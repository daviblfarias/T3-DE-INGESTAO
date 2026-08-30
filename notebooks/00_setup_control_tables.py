# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# =============================================================================
# jobs/00_create_control_tables.py
#
# Cria o schema bronze e as tabelas de controle:
#   - control_ingestion_log  (R5 do enunciado)
#   - control_watermark      (persistência de watermark entre execuções, R3)
#
# Rodar UMA VEZ manualmente (ou como primeiro task do Job) antes da extração.
# Idempotente: usa CREATE ... IF NOT EXISTS, pode rodar várias vezes sem efeito colateral.
# =============================================================================

# COMMAND ----------

# MAGIC %pip install pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import yaml

with open("/Workspace/Users/daviblfarias@gmail.com/T3-DE-INGESTAO/config/pipeline_config.yaml", "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

CATALOG = CFG["catalog"]
BRONZE_SCHEMA = CFG["bronze"]["schema"]

print(f"catalog={CATALOG} | bronze_schema={BRONZE_SCHEMA}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# control_ingestion_log — fonte de verdade de "o que foi carregado, quando, por qual
# execução e com qual resultado" (R5). Colunas do enunciado + 'stage', que separa a
# fase de extração (Mongo -> Landing) da fase de carga Bronze (Landing -> Bronze),
# já que a arquitetura documentada tem as duas fases desacopladas.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.control_ingestion_log (
    _ingestion_id         STRING      COMMENT 'UUID da execução (run id)',
    collection             STRING      COMMENT 'nome da coleção Mongo',
    stage                  STRING      COMMENT 'extract | bronze_load',
    load_type              STRING      COMMENT 'full | incremental',
    watermark_inicial      STRING      COMMENT 'valor do watermark antes da execução',
    watermark_final        STRING      COMMENT 'maior valor de watermark visto nesta execução',
    qtd_lida_origem        BIGINT      COMMENT 'contagem na origem (Mongo) para o filtro aplicado',
    qtd_gravada_destino    BIGINT      COMMENT 'contagem efetivamente gravada no destino desta fase',
    start_time             TIMESTAMP,
    end_time                TIMESTAMP,
    duracao_seg             DOUBLE,
    status                   STRING     COMMENT 'SUCCESS | FAILED | PARTIAL',
    mensagem_erro            STRING
)
USING DELTA
COMMENT 'Tabela de controle de execuções da pipeline sample_mflix (R5)'
""")

# COMMAND ----------

# control_watermark — guarda o ÚLTIMO watermark confirmado por coleção, para que a
# próxima execução incremental saiba de onde continuar (R3 - watermark persistida).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.control_watermark (
    collection              STRING,
    watermark_field          STRING,
    last_watermark_value     STRING,
    updated_at                TIMESTAMP
)
USING DELTA
COMMENT 'Último watermark confirmado por coleção (checkpoint lógico da carga incremental)'
""")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}"))