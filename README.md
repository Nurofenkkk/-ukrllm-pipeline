# UkrLLM Pipeline

Data pipeline for collecting and processing Verkhovna Rada documents for LLM training.

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
docker compose -f docker/docker-compose.yaml up -d
```

### 3. Configure
Edit `configs/config.yaml`:
```yaml
api:
  max_docs: 10  # increase for more documents
```

## Running the Pipeline

### Full pipeline
```bash
poetry run python scripts/001-run-pipeline-entrypoint.py
```

### Expected output
```
✅ Collected:  10 documents
✅ QC passed:  10/10
✅ Saved:      10 documents
```

## Services

| Service | URL | Login |
|---------|-----|-------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO UI | http://localhost:9001 | minioadmin / minioadmin123 |
| PostgreSQL | localhost:5433 | ukrllm_user / password123 |

## Project Structure
```
ukrllm-pipeline/
├── pipeline/
│   ├── collect.py      # Data collection from Rada API
│   ├── transform.py    # HTML → Markdown conversion
│   ├── load.py         # Save to PostgreSQL + MinIO
│   └── quality.py      # Data quality checks
├── configs/
│   └── config.yaml     # All configuration
├── dags/
│   └── pipeline.py     # Airflow DAG
├── scripts/
│   └── 001-run-pipeline-entrypoint.py
├── docker/
│   └── docker-compose.yaml
├── pyproject.toml
├── poetry.lock
├── README.md
└── REPORT.md
```

## Check Results
```bash
# Check PostgreSQL
docker exec -it ukrllm_postgres psql -U ukrllm_user -d ukrllm_db -c "SELECT nreg, nazva, status FROM documents;"

# Check logs
cat outputs/logs/pipeline.log
```
