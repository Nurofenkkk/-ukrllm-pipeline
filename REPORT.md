# REPORT — UkrLLM Pipeline

## 1. Solution Design

### Architecture
```
API Verkhovna Rada (data.rada.gov.ua)
        ↓
  [collect.py]         — data collection: token → dictionaries → TSV list → HTML texts
        ↓
  [transform.py]       — HTML → Markdown (markdownify)
        ↓
  [quality.py]         — quality checks (nreg, nazva, status, min text length)
        ↓
  [load.py]
    ↙        ↘
PostgreSQL    MinIO
(metadata)   (texts .md)
        ↓
  [analyze.py]         — dataset statistics → outputs/
```

### Components
- **RadaCollector** — obtains token once (`/api/token`), loads official dictionaries for statuses and document types from open data portal (`/ogd/zak/laws/data/csv/stan.txt`, `typ.txt`). Downloads document list via `/laws/main/r.tsv`, parses `card` field to extract `issuer`, `doc_type`, `date_revision`. Fetches HTML text for each document via `/laws/show/{nreg}`. Handles CAPTCHA verification pages automatically (session cookies), retries on 503 errors, auto-refreshes token on 401/403.
- **DocumentTransformer** — converts HTML to Markdown (`markdownify`), counts words and characters, removes raw HTML.
- **QualityChecker** — validates presence of `nreg`, `nazva`, `status`, minimum text length (100 chars, 20 words).
- **DataLoader** — saves document text to MinIO as `.md` file, metadata to PostgreSQL. Idempotent: checks duplicates by `nreg`. Sanitizes `nreg` (replaces `/` with `-`) to prevent nested folder creation in MinIO.
- **DataAnalyzer** — loads all documents from PostgreSQL, computes statistics, saves `outputs/analysis_report.json` and `outputs/data_dump.csv`.

### DB Schema (PostgreSQL)
```sql
CREATE TABLE documents (
    nreg          VARCHAR PRIMARY KEY,  -- unique document ID
    num           VARCHAR,              -- sequential number in registry
    nazva         TEXT,                 -- document title
    status        VARCHAR,              -- status (resolved via dictionary)
    card          TEXT,                 -- raw card string
    issuer        TEXT,                 -- issuing authority (from card)
    doc_type      VARCHAR,              -- document type (resolved via dictionary)
    date_revision VARCHAR,              -- revision date (DD.MM.YYYY from card)
    publics       TEXT,                 -- publication status
    link          TEXT,                 -- link to zakon.rada.gov.ua
    size          VARCHAR,              -- document size
    word_count    INTEGER,              -- word count in Markdown
    char_count    INTEGER,              -- character count in Markdown
    minio_path    TEXT,                 -- path in MinIO (documents/{nreg}.md)
    created_at    TIMESTAMP DEFAULT NOW()
);
```

### Data Flow
1. Token obtained once from `/api/token` (valid until 23:59)
2. Official dictionaries loaded from open data portal (10 statuses, 134 document types)
3. Document list downloaded from `/laws/main/r.tsv` (524 documents available)
4. `card` field parsed via regex: `"Authority; Type від DD.MM.YYYY"` → `issuer`, `doc_type`, `date_revision`
5. `nreg` extracted from link, `/edYYYYMMDD` suffix stripped
6. For each document: HTML fetched from `/laws/show/{nreg}` (delay 5–7 sec, CAPTCHA bypass via session cookies)
7. HTML converted to Markdown
8. Quality checks applied
9. Text → MinIO (`documents/{nreg}.md`), metadata → PostgreSQL
10. Statistics → `outputs/analysis_report.json`, dump → `outputs/data_dump.csv`

### Data Quality Controls
- Presence of `nreg` (unique ID)
- Presence of title (`nazva`)
- Presence of status (`status`)
- Minimum text length: 100 characters
- Minimum word count: 20
- Idempotency: SELECT before INSERT by `nreg` (re-runs don't duplicate rows)
- Status and doc_type validated against official dictionaries

### Storage Design Decisions
- **MinIO for texts**: Object storage is optimal for data scientists — each document is a separate `.md` file, easily iterable for LLM training. Markdown preserves document structure (headings, lists, tables) without HTML overhead.
- **PostgreSQL for metadata**: Structured storage enables filtering, aggregation, and SQL queries for dataset analysis. Data scientists can select subsets by `doc_type`, `status`, `issuer`, etc.
- **Separate databases**: Pipeline data (PostgreSQL) and Airflow metadata use separate database instances to avoid conflicts.

---

## 2. Automation Recommendation

### Scheduling
- DAG runs daily (`@daily`) to collect updated documents
- New documents are added, duplicates skipped automatically
- On failure: 2 retries with 5-minute delay

### Deployment
```bash
docker compose -f docker/docker-compose.yaml up -d --build
# Airflow UI: http://localhost:8080  (admin / admin)
# MinIO UI:   http://localhost:9001  (minioadmin / minioadmin123)
# PostgreSQL: localhost:5433         (ukrllm_user / password123)
```

### Scaling
- Increase `max_docs` in `config.yaml` to collect all 524+ documents
- Switch to `CeleryExecutor` + Redis for parallel task execution
- Add indexes: `CREATE INDEX ON documents(doc_type); CREATE INDEX ON documents(date_revision);`
- Partition `documents` table by `date_revision` for large volumes

### Monitoring
- Logs: `outputs/logs/pipeline.log` (rotation 10 MB)
- Airflow UI: status of each run and task
- Recommended: Slack/email alerts with `email_on_failure: true`

---

## 3. Analysis of Data Collected

| Metric | Value |
|--------|-------|
| Total documents in TSV list | 524 |
| Collected (max_docs=50) | 44 (6 failed due to 503) |
| QC passed | 44/44 (100%) |
| QC failed | 0/44 (0%) |
| Total words | 10,674 |
| Total characters | 82,277 |
| Avg words/doc | 242.6 |
| Avg chars/doc | 1,869.9 |
| Min words | 66 |
| Max words | 1,188 |
| Published | 15 |
| Not published | 29 |

### Distribution by Status
| Status | Count |
|--------|-------|
| Чинний (Active) | 31 |
| Не визначено (Undefined) | 13 |

### Distribution by Document Type (`doc_type`)
| Type | Count |
|------|-------|
| Розпорядження Кабінету Міністрів України | 12 |
| Повідомлення (НБУ) | 11 |
| Постанова Кабінету Міністрів України | 8 |
| Розпорядження Президента України | 6 |
| Указ Президента України | 4 |
| Порядок, Форма типового документа | 1 |
| Порядок | 1 |
| Постанова Національного банку України | 1 |

### Distribution by Issuer (`issuer`)
| Issuer | Count |
|--------|-------|
| Національний банк | 11 |
| (not extracted from card) | 31 |
| Постанова Кабінету Міністрів України | 2 |

### Observations
- The dataset contains a diverse mix of document types: cabinet resolutions, presidential decrees/orders, and NBU (National Bank) notifications
- NBU notifications are short (~66 words) with a template format; cabinet/presidential documents are significantly longer (up to 1,188 words)
- 70% of collected documents have "Чинний" (Active) status
- The `issuer` field is only extracted when the `card` uses semicolon-separated format (NBU documents); for other documents, the issuing authority is embedded in the `doc_type` string
- For full dataset analysis, `max_docs` should be increased to 524+

---

## 4. Failure Cases

| Problem | Cause | Solution |
|---------|-------|----------|
| CAPTCHA verification page instead of document text | API anti-bot protection | Session-based cookies + redirect following (implemented) |
| Token expires at 23:59 | API limitation | Automatic refresh on 401/403 (implemented) |
| 503 Service Unavailable | Server overload | Retry with exponential backoff (implemented, 3 retries) |
| Nested folders in MinIO | `nreg` contains `/` characters | Sanitize `nreg`: replace `/` with `-` (implemented) |
| `/edYYYYMMDD` suffix in nreg | API link format includes edition date | Regex strip of edition date suffix (implemented) |
| Empty `issuer` for non-NBU documents | `card` field has no semicolon separator | Doc type extracted correctly; issuer not separable from doc_type in this format |
| XCom exceeds limit (~1 MB) for large batches | Airflow stores data in DB | Data passed via temp files in `outputs/tmp/` (implemented) |
| SQLAlchemy 2.0 incompatible with Airflow 2.9 | Version conflict | Pinned `sqlalchemy<2.0` in `requirements.txt` for Airflow |

### What Can Be Improved
- Use `/laws/card/{nreg}.json` endpoint for structured metadata (instead of parsing `card` string)
- Store `Last-Modified` header for incremental updates
- Implement `If-Modified-Since` to save API requests
- Add NeMo-Curator for heuristic filtering of low-quality texts
- Decode URL-encoded Cyrillic in `nreg` (`urllib.parse.unquote`) for better readability
- Improve `issuer` extraction for non-semicolon card formats

---

## 5. Reproducibility Checklist

### Installation (fresh machine)
```bash
# Prerequisites: Python 3.12+, Docker, pip

# 1. Install Poetry
pip install poetry

# 2. Clone & install dependencies
git clone <repo>
cd ukrllm-pipeline
poetry install --no-root

# 3. Start infrastructure
docker compose -f docker/docker-compose.yaml up -d --build
# Wait ~60 seconds for Airflow to initialize

# 4. Run full pipeline
poetry run python scripts/001-run-pipeline-entrypoint.py
```

### Configuration (`configs/config.yaml`)
```yaml
api:
  max_docs: 50       # number of documents to collect (524 available)
  delay_min: 5       # min delay between requests (sec)
  delay_max: 7       # max delay between requests (sec)
database:
  url: "postgresql://ukrllm_user:password123@postgres:5432/ukrllm_db"
minio:
  endpoint: "minio:9000"
  access_key: "minioadmin"
  secret_key: "minioadmin123"
  bucket: "documents"
```

For local execution (outside Docker), use `configs/config.local.yaml`:
```yaml
database:
  url: "postgresql://ukrllm_user:password123@127.0.0.1:5433/ukrllm_db"
minio:
  endpoint: "localhost:9000"
```

### Expected Output
```
outputs/
├── logs/pipeline.log         # detailed execution logs
├── data_dump.csv             # dump of documents table from PostgreSQL
└── analysis_report.json      # dataset statistics
```

PostgreSQL: table `documents` (localhost:5433)
MinIO: bucket `documents`, files `documents/{nreg}.md` (localhost:9001)

### Verification
```bash
# Logs
cat outputs/logs/pipeline.log

# PostgreSQL
docker exec -it ukrllm_postgres psql -U ukrllm_user -d ukrllm_db \
  -c "SELECT nreg, nazva, doc_type, date_revision, status FROM documents;"

# Analysis
cat outputs/analysis_report.json

# Idempotency check (second run should show 0 saved, N skipped)
poetry run python scripts/001-run-pipeline-entrypoint.py
```

### Random Seeds
- `random.uniform(delay_min, delay_max)` — intentionally not fixed to avoid API rate-limiting patterns
