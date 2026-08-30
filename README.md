# T3 — Ingestão Moderna: MongoDB (sample_mflix) → Databricks Bronze

Pipeline de ingestão que extrai as 6 coleções do banco `sample_mflix` (MongoDB
Atlas) e carrega na camada Bronze (Delta / Unity Catalog) do Databricks, com
controle de execução, watermark incremental e reconciliação origem×destino.

## Arquitetura

```
MongoDB Atlas (sample_mflix)
        │  jobs/ingestion_job.py  (extração parametrizada, batch, retry)
        ▼
Landing Volume (NDJSON)  ── /Volumes/{catalog}/landing/mflix/{collection}/
        │  jobs/bronze_job.py  (leitura, lineage, schema evolution, reconciliação)
        ▼
Bronze (Delta, Unity Catalog)  ── {catalog}.bronze.{collection}
        │
        ├── control_ingestion_log   (1 linha por coleção por execução, por stage)
        └── control_watermark       (watermark persistida entre execuções)
```

Detalhamento completo em [`docs/ARQUITETURA.md`](docs/ARQUITETURA.md) e do
dataset em [`docs/SAMPLE_MFLIX.md`](docs/SAMPLE_MFLIX.md).

## Como executar

1. `notebooks/00_setup_control_tables.py` — cria o schema Bronze e as tabelas
   de controle (`control_ingestion_log`, `control_watermark`). Idempotente.
2. `create-secret.py` — cria o secret do Mongo no scope `conn-db` (pede a URI
   via widget em tempo de execução; nunca fica salva em texto no arquivo).
3. `jobs/ingestion_job.py` — extrai as coleções configuradas em
   `config/collections.json` para a Landing.
4. `jobs/bronze_job.py` — carrega os arquivos pendentes da Landing na Bronze.

Toda a configuração (banco, coleções, modo de carga, campo de watermark,
campos excluídos, limiares) vive em `config/pipeline_config.yaml` e
`config/collections.json` — nenhum parâmetro específico de coleção é
hardcoded no código.

## Decisões e limitações conhecidas

- **PostgreSQL/Neon (Lakehouse Federation) não faz parte do escopo
  obrigatório deste trabalho.** A fonte exigida é exclusivamente o MongoDB
  `sample_mflix` (ver `trabalho_final_ingestao_moderna.md` e
  `docs/ARQUITETURA.md`). Os notebooks relacionados em `code-samples/` são
  material de estudo de uma etapa anterior da disciplina, não parte da
  entrega.
- **Credencial do MongoDB compartilhada pela disciplina.** O usuário/senha
  do MongoDB foi fornecido pelo professor para toda a turma acessar o mesmo
  ambiente — não é uma credencial individual deste grupo e não foi gerada
  por nós. Ela está armazenada como Databricks Secret (`conn-db` /
  `cnn-mongodb-sampleflix`) e nunca é lida em texto puro em nenhum notebook
  (`dbutils.secrets.get(...)` em `jobs/ingestion_job.py`).
- **Campos excluídos na origem** (projection pushdown, não filtro na
  Bronze): `password` (hash bcrypt de `users`), `jwt` (token ativo de
  `sessions`), `plot_embedding` (vetor ~1536 floats de `embedded_movies`),
  `fullplot`/`poster` (campos largos sem valor analítico direto de
  `movies`). Justificativa completa em `config/collections.json`.
- **Reconciliação:** divergência de contagem origem×destino acima de 0.5%
  (configurável) faz a execução ser marcada como `PARTIAL` no
  `control_ingestion_log`, em vez de falhar silenciosamente.

## Evidências de execução

Três execuções documentadas em `docs/evidencias/`:

1. `execucao_01_full_load.png` — carga inicial completa.
2. `execucao_02_incremental_sem_novidades.png` — reexecução sem dados novos
   na origem (`qtd_lida_origem = 0`).
3. `execucao_03_incremental_com_dados.png` — reexecução após inserção de
   novos documentos em `comments` (watermark incremental capturando só o
   delta).

## Bônus implementados

- **Pipeline Sentinel via Telegram** (`jobs/_telegram_notifier.py`): notifica
  o resumo de cada execução (e falhas) direto no Telegram, usando a Jobs API
  para contexto do run.
