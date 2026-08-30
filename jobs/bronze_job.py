# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# =============================================================================
# jobs/bronze_job.py
#
# Requer que notebooks/00_setup_control_tables.py e jobs/ingestion_job.py já
# tenham rodado ao menos uma vez (tabelas de controle criadas e arquivos
# NDJSON disponíveis na Landing).
#
# Etapa 2 da arquitetura (docs/ARQUITETURA.md):
#   Landing Volume (NDJSON) --carga--> Camada Bronze (Delta, Unity Catalog)
#
# Um único componente genérico e parametrizado (R1) roda para TODAS as
# coleções listadas em config/collections.json — sem bloco copiado/colado
# por coleção.
#
# Cobre:
#   R4 - colunas de rastreabilidade em toda tabela Bronze
#   R6 - Bronze append-only, fiel à origem, particionada, nomenclatura padronizada
#   R7 - tratamento de schema drift (schema evolution via mergeSchema + coluna
#        de rescue para registros que não casam com o schema inferido)
#   R8 - reconciliação: contagem origem x destino, nulos em _source_id,
#        duplicidade de _source_id, execução vira PARTIAL acima do limiar
#   R5 - grava uma linha em control_ingestion_log por coleção, stage='bronze_load'
# =============================================================================

# COMMAND ----------

# MAGIC %pip install pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_telegram_notifier

# COMMAND ----------

import datetime
import json
import os
import uuid

import yaml
from pyspark.sql import functions as F

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
BRONZE_SCHEMA = CFG["bronze"]["schema"]
CONTROL_LOG_TABLE = CFG["control"]["ingestion_log_table"].format(catalog=CATALOG)
MAX_DIFF_PCT = CFG["reconciliation"]["max_diff_pct"]

RUN_ID = str(uuid.uuid4())
RUN_START = datetime.datetime.utcnow()
print(f"run_id={RUN_ID} | catalog={CATALOG} | bronze_schema={BRONZE_SCHEMA}")

notifier = TelegramNotifier(
    secret_scope=CFG["telegram"]["secret_scope"],
    token_key=CFG["telegram"]["token_key"],
    chat_id_key=CFG["telegram"]["chat_id_key"],
    enabled=CFG["telegram"].get("enabled", True),
)

# COMMAND ----------

# --------------------------------------------------------------------------- #
# Camada CONTROL — grava o log de execução desta fase (bronze_load).
# Reaproveita a MESMA tabela control_ingestion_log do ingestion_job.py,
# diferenciando pela coluna 'stage'.
# --------------------------------------------------------------------------- #
def log_execution(**kwargs):
    row = spark.createDataFrame([kwargs])
    row.write.format("delta").mode("append").saveAsTable(CONTROL_LOG_TABLE)


# COMMAND ----------

# --------------------------------------------------------------------------- #
# Camada LOAD — lê os NDJSON pendentes de uma coleção na Landing e grava na
# Bronze. Move os arquivos processados para uma subpasta _processed/ dentro
# da própria Landing: isso é o que garante IDEMPOTÊNCIA nesta fase — rodar o
# job duas vezes seguidas não duplica nada na Bronze, porque na segunda vez
# não sobra nenhum arquivo pendente para processar (R3).
# --------------------------------------------------------------------------- #
def carregar_colecao(collection: str, load_type: str) -> dict:
    origem_path = os.path.join(LANDING_VOLUME, collection)
    processed_path = os.path.join(origem_path, "_processed")
    os.makedirs(processed_path, exist_ok=True)

    arquivos_pendentes = [
        os.path.join(origem_path, nome)
        for nome in os.listdir(origem_path)
        if nome.endswith(".ndjson") and os.path.isfile(os.path.join(origem_path, nome))
    ]

    if not arquivos_pendentes:
        # Nada novo desde a última carga Bronze — não é erro (mesma lógica do
        # ingestion_job.py para incremental sem novidade).
        return {"qtd_origem": 0, "qtd_destino": 0, "status": "SUCCESS", "mensagem_erro": None}

    # rescuedDataColumn: registros/campos que não batem com o schema inferido
    # (schema drift típico de Mongo/NoSQL) vão para essa coluna em vez de
    # serem descartados silenciosamente — atende R7 (quarentena / rescue).
    try:
        df_raw = (
            spark.read.option("rescuedDataColumn", "_rescued_data")
            .json(arquivos_pendentes)
        )
    except Exception:
        # fallback para clusters/runtimes sem suporte a rescuedDataColumn no
        # leitor batch de JSON — ainda funciona, só sem a coluna de rescue.
        df_raw = spark.read.json(arquivos_pendentes)
        df_raw = df_raw.withColumn("_rescued_data", F.lit(None).cast("string"))

    qtd_origem = df_raw.count()

    # Colunas de rastreabilidade obrigatórias (R4)
    agora = datetime.datetime.utcnow()
    df_bronze = (
        df_raw
        .withColumn("_source_id", F.col("_id"))  # _id do Mongo já veio como string (ObjectId serializado)
        .withColumn("_ingestion_id", F.lit(RUN_ID))
        .withColumn("_ingestion_timestamp", F.lit(agora).cast("timestamp"))
        .withColumn("_source_path", F.lit("mongodb_atlas"))
        .withColumn("_load_type", F.lit(load_type))
        .withColumn("_ingestion_date", F.lit(agora.date()).cast("date"))
    )

    # R8 — reconciliação, ANTES de gravar: nulos e duplicidade de chave
    qtd_nulos = df_bronze.filter(F.col("_source_id").isNull()).count()
    qtd_distintos = df_bronze.select("_source_id").distinct().count()
    qtd_duplicados = qtd_origem - qtd_distintos

    tabela_bronze = f"{CATALOG}.{BRONZE_SCHEMA}.{collection}"

    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")  # R7 — schema evolution explícito
        .partitionBy("_ingestion_date")  # R6 — particionamento padronizado
        .saveAsTable(tabela_bronze)
    )

    qtd_destino = qtd_origem  # append-only: tudo que foi lido foi gravado

    # só move para _processed DEPOIS do append ter sido confirmado — se o
    # write falhar, os arquivos continuam pendentes para a próxima tentativa
    for caminho in arquivos_pendentes:
        os.replace(caminho, os.path.join(processed_path, os.path.basename(caminho)))

    diff_pct = 0.0 if qtd_origem == 0 else abs(qtd_origem - qtd_destino) / qtd_origem * 100
    status = "SUCCESS"
    mensagem_erro = None
    if qtd_nulos > 0 or qtd_duplicados > 0:
        status = "PARTIAL"
        mensagem_erro = (
            f"{qtd_nulos} registro(s) com _source_id nulo; "
            f"{qtd_duplicados} _source_id duplicado(s) no lote."
        )
    if diff_pct > MAX_DIFF_PCT:
        status = "PARTIAL"
        mensagem_erro = (mensagem_erro or "") + f" divergência origem x destino de {diff_pct:.2f}%."

    return {
        "qtd_origem": qtd_origem,
        "qtd_destino": qtd_destino,
        "status": status,
        "mensagem_erro": mensagem_erro,
    }


# COMMAND ----------

# --------------------------------------------------------------------------- #
# Orquestração — itera todas as coleções configuradas, sem nenhum bloco
# específico por coleção (R1).
# --------------------------------------------------------------------------- #
resultados = []

for colecao_cfg in COLLECTIONS_CFG:
    collection = colecao_cfg["collection"]
    load_type = colecao_cfg["load_mode"]

    start_time = datetime.datetime.utcnow()
    print(f"\n=== carregando '{collection}' na Bronze (load_type={load_type}) ===")

    try:
        r = carregar_colecao(collection, load_type)
        status = r["status"]
        mensagem_erro = r["mensagem_erro"]
        qtd_origem = r["qtd_origem"]
        qtd_destino = r["qtd_destino"]
        print(f"  '{collection}': origem={qtd_origem} destino={qtd_destino} status={status}")
    except Exception as exc:
        status = "FAILED"
        mensagem_erro = str(exc)
        qtd_origem = 0
        qtd_destino = 0
        print(f"  [ERRO] '{collection}': {mensagem_erro}")

    end_time = datetime.datetime.utcnow()

    log_execution(
        _ingestion_id=RUN_ID,
        collection=collection,
        stage="bronze_load",
        load_type=load_type,
        watermark_inicial="",
        watermark_final="",
        qtd_lida_origem=qtd_origem,
        qtd_gravada_destino=qtd_destino,
        start_time=start_time,
        end_time=end_time,
        duracao_seg=(end_time - start_time).total_seconds(),
        status=status,
        mensagem_erro=mensagem_erro if mensagem_erro is not None else "",
    )

    resultados.append({
        "collection": collection,
        "status": status,
        "load_type": load_type,
        "qtd_lida": qtd_origem,
        "mensagem_erro": mensagem_erro,
    })

# COMMAND ----------

RUN_END = datetime.datetime.utcnow()

print("\n=== resumo da execução (bronze_load) ===")
for r in resultados:
    print(r)

notifier.notify_run_summary(
    job_name="bronze_job", run_id=RUN_ID, resultados=resultados,
    start_time=RUN_START, end_time=RUN_END,
)

falhas = [r for r in resultados if r["status"] == "FAILED"]
if falhas:
    raise RuntimeError(f"Carga Bronze falhou para: {[r['collection'] for r in falhas]}")