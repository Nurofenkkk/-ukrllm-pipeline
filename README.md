# UkrLLM Pipeline

Automated data pipeline for building a Ukrainian-language text dataset from the Verkhovna Rada open data portal ([data.rada.gov.ua](https://data.rada.gov.ua/open/main)). The pipeline collects legislative documents (laws, decrees, resolutions), converts them from HTML to clean Markdown, validates quality, and stores them in a format optimized for further use by data scientists during LLM (Large Language Model) training.

## Requirements

- Python 3.12+
- Docker + Docker Compose
- Poetry

## Installation

### 1. Install dependencies
```bash
poetry install --no-root
```

### 2. Start infrastructure
```bash
docker compose -f docker/docker-compose.yaml up -d --build
```
Wait ~60 seconds for Airflow to initialize.

### 3. Configure
Edit `configs/config.local.yaml` for local runs, `configs/config.yaml` for Docker/Airflow:
```yaml
api:
  max_docs: 10  # number of documents to collect (524+ available)
  delay_min: 5  # min delay between API requests (sec)
  delay_max: 7  # max delay between API requests (sec)
```

## Running the Pipeline

### Option 1: Manual run (local)
```bash
poetry run python scripts/001-run-pipeline-entrypoint.py
```

### Option 2: Airflow DAG (automated daily)
1. Open http://localhost:8080 (admin / admin)
2. Find DAG `ukrllm_pipeline`
3. Unpause it (toggle on the left)
4. Trigger manually or wait for daily schedule

### Expected output
```
Pipeline complete!
Total collected : 10
Quality passed  : 10
Total in DB     : 10
```

## Services

| Service | URL | Login |
|---------|-----|-------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO UI | http://localhost:9001 | minioadmin / minioadmin123 |
| PostgreSQL | localhost:5433 | ukrllm_user / password123 / db: ukrllm_db |

## Project Structure
```
ukrllm-pipeline/
├── pipeline/
│   ├── collect.py      # Data collection from Rada API (with dictionaries, CAPTCHA bypass)
│   ├── transform.py    # HTML → Markdown (BeautifulSoup + markdownify)
│   ├── quality.py      # Data quality checks
│   ├── load.py         # Save to PostgreSQL + MinIO
│   └── analyze.py      # Dataset statistics
├── configs/
│   ├── config.yaml     # Configuration for Docker/Airflow
│   └── config.local.yaml # Configuration for local runs
├── dags/
│   └── pipeline.py     # Airflow DAG (daily schedule)
├── docker/
│   ├── Dockerfile.airflow
│   └── docker-compose.yaml
├── scripts/
│   └── 001-run-pipeline-entrypoint.py  # CLI entry point
├── outputs/
│   ├── analysis_report.json  # Dataset statistics (auto-updated)
│   ├── data_dump.csv         # Full data dump (auto-updated)
│   └── logs/pipeline.log     # Execution logs
├── pyproject.toml
├── poetry.lock
├── requirements.txt
├── README.md
└── REPORT.md
```

## Data Storage

- **PostgreSQL** — document metadata (title, status, type, issuer, date, word/char counts)
- **MinIO** — document texts as `.md` files in `documents/` bucket

## Check Results
```bash
# PostgreSQL
docker exec -it ukrllm_postgres psql -U ukrllm_user -d ukrllm_db \
  -c "SELECT nreg, nazva, doc_type, status, word_count FROM documents;"

# Analysis report
cat outputs/analysis_report.json

# MinIO (via UI)
# Open http://localhost:9001 → bucket "documents" → folder "documents/"

# Idempotency check (second run should skip all existing documents)
poetry run python scripts/001-run-pipeline-entrypoint.py
```

## Key Features

- **Official dictionaries** for status and document types from Rada open data portal
- **CAPTCHA bypass** via session cookies (automatic)
- **Retry on 503** with exponential backoff
- **Idempotent** — safe to re-run, no duplicate data
- **Separate databases** for pipeline data and Airflow metadata
