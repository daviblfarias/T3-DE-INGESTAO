# Databricks notebook source
# =============================================================================
# notebooks/create_telegram_secret.py
#
# Cria/atualiza os secrets do bot do Telegram no scope 'conn-db' (mesmo scope
# do Mongo). Rode manualmente UMA VEZ, preenchendo os widgets na hora — nunca
# deixe o token nem o chat_id como valor literal neste arquivo antes de commitar.
# =============================================================================

# COMMAND ----------

dbutils.widgets.text("telegram_bot_token", "")
dbutils.widgets.text("telegram_chat_id", "")

# COMMAND ----------

import requests

token = dbutils.widgets.get("telegram_bot_token")
chat_id = dbutils.widgets.get("telegram_chat_id")

assert token, "Preencha o widget telegram_bot_token antes de rodar este comando."
assert chat_id, "Preencha o widget telegram_chat_id antes de rodar este comando."

# COMMAND ----------

# Cria o scope se ainda não existir (idempotente)
existing_scopes = [s.name for s in dbutils.secrets.listScopes()]
if "conn-db" not in existing_scopes:
    dbutils.secrets.createScope(scope="conn-db")

dbutils.secrets.put(scope="conn-db", key="telegram-bot-token", string_value=token)
dbutils.secrets.put(scope="conn-db", key="telegram-chat-id", string_value=chat_id)

print("Secrets 'telegram-bot-token' e 'telegram-chat-id' gravados no scope 'conn-db'.")

# COMMAND ----------

# Validação: envia uma mensagem de teste (não expõe o token no output)
resp = requests.post(
    f"https://api.telegram.org/bot{token}/sendMessage",
    data={"chat_id": chat_id, "text": "✅ Bot conectado — secret criado com sucesso via Databricks."},
    timeout=10,
)
print(f"status_code={resp.status_code}")
assert resp.status_code == 200, f"Falha ao enviar mensagem de teste: {resp.text}"

# COMMAND ----------

# Limpa os widgets para não deixar valores residuais visíveis na UI do notebook
dbutils.widgets.removeAll()
