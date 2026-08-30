# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# =============================================================================
# jobs/ingestion_job.py
#
# Requer que notebooks/00_setup_control_tables.py já tenha sido executado ao
# menos uma vez (cria control_ingestion_log e control_watermark).
#
# Etapa 1 da arquitetura (docs/ARQUITETURA.md):
#   MongoDB Atlas (sample_mflix) --extração--> Landing Volume (arquivos NDJSON)
#
# Um único componente genérico e parametrizado (R1) roda para TODAS as coleções
# listadas em config/collections.json — sem bloco copiado/colado por coleção.
#
# Cobre:
#   R1 - pipeline parametrizada e genérica (config externa, OOP, extract/load/control separados)
#   R2 - boas práticas de recursos:
#        * leitura em lotes via cursor.batch_size (não list(cursor) da coleção inteira)
#        * projection pushdown (exclui campos sensíveis/largos na origem, não no destino)
#        * reuso de conexão (1 MongoClient para todas as coleções da execução)
#        * retry com backoff exponencial em falha de rede
#   R3 - full load / incremental com watermark PERSISTIDA entre execuções (control_watermark)
#   R5 - grava uma linha em control_ingestion_log por coleção, stage='extract'
# =============================================================================

# COMMAND ----------

# MAGIC %pip install pymongo pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_telegram_notifier

# COMMAND ----------

import datetime
import json
import os
import time
import uuid

import bson
import yaml
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# COMMAND ----------

# --------------------------------------------------------------------------- #
# Carga de configuração externa (nunca hardcoded no corpo do código — R1)
# --------------------------------------------------------------------------- #
with open("/Workspace/Users/daviblfarias@gmail.com/T3-DE-INGESTAO/config/pipeline_config.yaml", "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

with open("/Workspace/Users/daviblfarias@gmail.com/T3-DE-INGESTAO/config/collections.json", "r", encoding="utf-8") as f:
    COLLECTIONS_CFG = json.load(f)

CATALOG = CFG["catalog"]
LANDING_VOLUME = CFG["landing"]["volume_path"].format(catalog=CATALOG)
DOCS_PER_FILE = CFG["landing"]["docs_per_file"]
CONTROL_LOG_TABLE = CFG["control"]["ingestion_log_table"].format(catalog=CATALOG)
WATERMARK_TABLE = CFG["control"]["watermark_table"].format(catalog=CATALOG)
MAX_RETRIES = CFG["resilience"]["max_retries"]
BACKOFF_BASE = CFG["resilience"]["backoff_base_seconds"]

RUN_ID = str(uuid.uuid4())
RUN_START = datetime.datetime.utcnow()
print(f"run_id={RUN_ID} | catalog={CATALOG} | landing={LANDING_VOLUME}")

# Pipeline Sentinel — notificador Telegram (best-effort, nunca derruba o pipeline)
notifier = TelegramNotifier(
    secret_scope=CFG["telegram"]["secret_scope"],
    token_key=CFG["telegram"]["token_key"],
    chat_id_key=CFG["telegram"]["chat_id_key"],
    enabled=CFG["telegram"].get("enabled", True),
)

# COMMAND ----------

# DBTITLE 1,Setup landing schema and volume
# --------------------------------------------------------------------------- #
# Setup: cria o schema landing e o volume mflix se não existirem
# --------------------------------------------------------------------------- #
schema_landing = f"{CATALOG}.landing"
volume_name = "mflix"

print(f"Verificando/criando schema e volume...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_landing}")
print(f"  ✓ schema {schema_landing} disponível")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema_landing}.{volume_name}")
print(f"  ✓ volume {schema_landing}.{volume_name} disponível")
print(f"  Landing path: {LANDING_VOLUME}")

# COMMAND ----------

# --------------------------------------------------------------------------- #
# Camada CONTROL — leitura/escrita de watermark e do log de execução.
# Separada da lógica de extração (R1 - separação clara extract / load / control).
# --------------------------------------------------------------------------- #
class ControlStore:

    def __init__(self, spark, watermark_table: str, log_table: str):
        self.spark = spark
        self.watermark_table = watermark_table
        self.log_table = log_table

    def get_last_watermark(self, collection: str):
        try:
            df = self.spark.table(self.watermark_table).filter(f"collection = '{collection}'")
            row = df.orderBy(df.updated_at.desc()).first()
            return row["last_watermark_value"] if row else None
        except Exception:
            # tabela pode não ter linha ainda para essa coleção na primeira execução
            return None

    def upsert_watermark(self, collection: str, watermark_field: str, value: str):
        if value is None:
            return
        now = datetime.datetime.utcnow()
        novo = self.spark.createDataFrame(
            [(collection, watermark_field, str(value), now)],
            schema="collection STRING, watermark_field STRING, last_watermark_value STRING, updated_at TIMESTAMP",
        )
        novo.createOrReplaceTempView("_novo_watermark")
        self.spark.sql(f"""
            MERGE INTO {self.watermark_table} AS destino
            USING _novo_watermark AS origem
            ON destino.collection = origem.collection
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def log_execution(self, **kwargs):
        row = self.spark.createDataFrame([kwargs])
        row.write.format("delta").mode("append").saveAsTable(self.log_table)


# COMMAND ----------

# --------------------------------------------------------------------------- #
# Camada EXTRACT — conexão Mongo, leitura em lotes, gravação em Landing.
# --------------------------------------------------------------------------- #
class MongoLandingExtractor:

    def __init__(self, database: str, secret_scope: str, secret_key: str,
                 batch_size: int, server_selection_timeout_ms: int, socket_timeout_ms: int):
        uri = dbutils.secrets.get(scope=secret_scope, key=secret_key)
        # Conexão única reaproveitada por todas as coleções da execução (R2 - connection pooling)
        self.client = MongoClient(
            uri,
            serverSelectionTimeoutMS=server_selection_timeout_ms,
            socketTimeoutMS=socket_timeout_ms,
            appName="databricks-mongodb-extractor",
        )
        self.database = database
        self.batch_size = batch_size

    @staticmethod
    def _encode(o):
        if isinstance(o, bson.ObjectId):
            return str(o)
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        if isinstance(o, bson.Decimal128):
            return str(o)
        if isinstance(o, bytes):
            return o.hex()
        return str(o)

    def _with_retry(self, func, *args, **kwargs):
        """Retry com backoff exponencial em falha de rede da origem (R2)."""
        last_exc = None
        for tentativa in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except PyMongoError as exc:
                last_exc = exc
                espera = BACKOFF_BASE * (2 ** (tentativa - 1))
                print(f"[retry {tentativa}/{MAX_RETRIES}] falha de rede: {exc} -> aguardando {espera}s")
                time.sleep(espera)
        raise last_exc

    def build_filter(self, watermark_field: str | None, last_value: str | None) -> dict:
        if watermark_field is None or last_value is None:
            return {}
        return {watermark_field: {"$gt": last_value}}

    def build_projection(self, exclude_fields: list[str]) -> dict | None:
        # Projection pushdown (R2): campos excluídos NA ORIGEM, não depois em Spark.
        if not exclude_fields:
            return None
        return {campo: 0 for campo in exclude_fields}

    def count(self, collection: str, filtro: dict) -> int:
        return self._with_retry(
            self.client[self.database][collection].count_documents, filtro
        )

    def extract_to_landing(self, collection: str, filtro: dict, projecao: dict | None,
                            volume_path: str, docs_per_file: int) -> dict:
        """
        Lê a coleção em lotes via cursor (nunca list(cursor) da coleção inteira - R2) e
        grava arquivos NDJSON na Landing, agrupando `docs_per_file` documentos por arquivo
        para evitar o problema de small files (um arquivo por doc seria inviável para
        coleções como `comments`, ~50k docs).
        Retorna métricas da extração (qtd lida, maior valor de watermark visto).
        """
        os.makedirs(volume_path, exist_ok=True)

        def _open_cursor():
            return self.client[self.database][collection].find(
                filter=filtro, projection=projecao, batch_size=self.batch_size
            )

        cursor = self._with_retry(_open_cursor)

        qtd_lida = 0
        maior_watermark = None
        buffer = []
        arquivos_gravados = 0

        def _flush(buf):
            nonlocal arquivos_gravados
            if not buf:
                return
            ns = time.time_ns()
            segundos, nanos = divmod(ns, 1_000_000_000)
            carimbo = datetime.datetime.fromtimestamp(segundos).strftime("%Y%m%dT%H%M%S")
            nome = f"{self.database}-{collection}-{carimbo}-{segundos}-{nanos:09d}.ndjson"
            caminho = os.path.join(volume_path, nome)
            with open(caminho, "w", encoding="utf-8") as arquivo:
                for doc in buf:
                    arquivo.write(json.dumps(doc, default=self._encode, ensure_ascii=False))
                    arquivo.write("\n")
            arquivos_gravados += 1

        try:
            for doc in cursor:
                qtd_lida += 1
                buffer.append(doc)

                if len(buffer) >= docs_per_file:
                    self._with_retry(_flush, buffer)
                    buffer = []

            # último lote parcial
            self._with_retry(_flush, buffer)

        finally:
            cursor.close()

        return {
            "qtd_lida": qtd_lida,
            "arquivos_gravados": arquivos_gravados,
        }

    def close(self):
        self.client.close()


# COMMAND ----------

# --------------------------------------------------------------------------- #
# Orquestração da extração — itera todas as coleções configuradas, sem
# nenhum bloco específico por coleção (R1).
# --------------------------------------------------------------------------- #
try:
    extractor = MongoLandingExtractor(
        database=CFG["mongo"]["database"],
        secret_scope=CFG["mongo"]["secret_scope"],
        secret_key=CFG["mongo"]["secret_key"],
        batch_size=CFG["mongo"]["batch_size"],
        server_selection_timeout_ms=CFG["mongo"]["server_selection_timeout_ms"],
        socket_timeout_ms=CFG["mongo"]["socket_timeout_ms"],
    )
except Exception as exc:
    # Falha antes de processar qualquer coleção (ex.: secret ausente, Mongo fora do ar).
    notifier.notify_failure(job_name="ingestion_job", run_id=RUN_ID, erro=str(exc))
    raise

control = ControlStore(spark, WATERMARK_TABLE, CONTROL_LOG_TABLE)

resultados = []

for cfg_colecao in COLLECTIONS_CFG:
    collection = cfg_colecao["collection"]
    load_mode = cfg_colecao["load_mode"]
    watermark_field = cfg_colecao.get("watermark_field")
    exclude_fields = cfg_colecao.get("exclude_fields", [])

    start_time = datetime.datetime.utcnow()
    status = "SUCCESS"
    mensagem_erro = None
    qtd_lida = 0
    watermark_inicial = None
    watermark_final = None

    print(f"\n=== extraindo '{collection}' (modo={load_mode}) ===")

    try:
        watermark_inicial = (
            control.get_last_watermark(collection) if load_mode == "incremental" else None
        )
        filtro = extractor.build_filter(watermark_field, watermark_inicial)
        projecao = extractor.build_projection(exclude_fields)

        # count() usa o mesmo filtro da extração -> é o "qtd_lida_origem" real da execução
        qtd_origem_filtro = extractor.count(collection, filtro)
        print(f"  filtro={filtro} | documentos a extrair nesta execução={qtd_origem_filtro}")

        if qtd_origem_filtro == 0:
            # Coleções vazias (ex.: sessions) ou incremental sem novidade -> não é erro (R3/SAMPLE_MFLIX)
            print(f"  '{collection}': 0 documentos novos/pendentes — nada a gravar nesta execução.")
            qtd_lida = 0
        else:
            volume_colecao = os.path.join(LANDING_VOLUME, collection)
            metricas = extractor.extract_to_landing(
                collection=collection,
                filtro=filtro,
                projecao=projecao,
                volume_path=volume_colecao,
                docs_per_file=DOCS_PER_FILE,
            )
            qtd_lida = metricas["qtd_lida"]
            print(f"  '{collection}': {qtd_lida} documentos gravados em {metricas['arquivos_gravados']} arquivo(s).")

            if watermark_field and load_mode == "incremental":
                # maior valor do campo watermark efetivamente extraído nesta execução
                max_doc = extractor.client[extractor.database][collection].find(
                    filtro, projection={watermark_field: 1}
                ).sort(watermark_field, -1).limit(1)
                max_doc = list(max_doc)
                watermark_final = str(max_doc[0][watermark_field]) if max_doc else watermark_inicial
                control.upsert_watermark(collection, watermark_field, watermark_final)

    except Exception as exc:
        status = "FAILED"
        mensagem_erro = str(exc)
        print(f"  [ERRO] '{collection}': {mensagem_erro}")

    end_time = datetime.datetime.utcnow()

    control.log_execution(
        _ingestion_id=RUN_ID,
        collection=collection,
        stage="extract",
        load_type=load_mode,
        watermark_inicial=watermark_inicial if watermark_inicial is not None else "",
        watermark_final=watermark_final if watermark_final is not None else "",
        qtd_lida_origem=qtd_lida,
        qtd_gravada_destino=qtd_lida,  # nesta fase, "destino" = arquivos na Landing
        start_time=start_time,
        end_time=end_time,
        duracao_seg=(end_time - start_time).total_seconds(),
        status=status,
        mensagem_erro=mensagem_erro if mensagem_erro is not None else "",
    )

    resultados.append({
        "collection": collection,
        "status": status,
        "load_type": load_mode,
        "qtd_lida": qtd_lida,
        "mensagem_erro": mensagem_erro,
    })

extractor.close()

# COMMAND ----------

RUN_END = datetime.datetime.utcnow()

print("\n=== resumo da execução ===")
for r in resultados:
    print(r)

# Sentinel: uma mensagem consolidada por execução, com status de cada coleção,
# enviada ANTES do raise abaixo — assim o alerta sai mesmo quando o job vai falhar.
notifier.notify_run_summary(
    job_name="ingestion_job", run_id=RUN_ID, resultados=resultados,
    start_time=RUN_START, end_time=RUN_END,
)

falhas = [r for r in resultados if r["status"] == "FAILED"]
if falhas:
    # Task do Job falha de verdade -> Databricks Workflows aciona o retry/alerta nativo também
    erros_detalhados = "\n".join([f"  - {r['collection']}: {r['mensagem_erro']}" for r in falhas])
    raise RuntimeError(f"Extração falhou para {len(falhas)} coleção(ões):\n{erros_detalhados}")