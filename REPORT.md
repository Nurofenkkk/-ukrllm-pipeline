# REPORT — UkrLLM Pipeline

## 1. Solution Design

### Architecture
```
API Верховної Ради (data.rada.gov.ua)
        ↓
  [collect.py]         — збір даних: токен → TSV список → HTML тексти
        ↓
  [transform.py]       — HTML → Markdown (markdownify)
        ↓
  [quality.py]         — перевірка якості (nreg, nazva, status, мін. текст)
        ↓
  [load.py]
    ↙        ↘
PostgreSQL    MinIO
(метадані)   (тексти .md)
        ↓
  [analyze.py]         — статистика датасету → outputs/
```

### Components
- **RadaCollector** — отримує токен один раз (`/api/token`), завантажує список документів через `/laws/main/r.tsv`, парсить поле `card` для вилучення `issuer`, `doc_type`, `date_revision`. Потім завантажує HTML текст кожного документа через `/laws/show/{nreg}`. Автоматично оновлює токен при відповіді 401/403.
- **DocumentTransformer** — конвертує HTML у Markdown (`markdownify`), рахує кількість слів та символів, видаляє сирий HTML.
- **QualityChecker** — перевіряє наявність `nreg`, `nazva`, `status`, мінімальну довжину тексту (100 символів, 20 слів).
- **DataLoader** — зберігає текст документа у MinIO як `.md` файл, метадані — у PostgreSQL. Ідемпотентний: перевіряє дублікати за `nreg`.
- **DataAnalyzer** — завантажує всі документи з PostgreSQL, рахує статистику, зберігає `outputs/analysis_report.json` та `outputs/data_dump.csv`.

### DB Schema (PostgreSQL)
```sql
CREATE TABLE documents (
    nreg          VARCHAR PRIMARY KEY,  -- унікальний ID документа
    num           VARCHAR,              -- порядковий номер у реєстрі
    nazva         TEXT,                 -- назва документа
    status        VARCHAR,              -- статус (Чинний / Не визначено / ...)
    card          TEXT,                 -- сирий рядок реквізитів
    issuer        TEXT,                 -- орган-видавець (з card)
    doc_type      VARCHAR,              -- тип документа (з card)
    date_revision VARCHAR,              -- дата поновлення (з card, ДД.ММ.РРРР)
    publics       TEXT,                 -- статус публікації
    link          TEXT,                 -- посилання на zakon.rada.gov.ua
    size          VARCHAR,              -- розмір документа
    word_count    INTEGER,              -- кількість слів у Markdown
    char_count    INTEGER,              -- кількість символів у Markdown
    minio_path    TEXT,                 -- шлях у MinIO (documents/{nreg}.md)
    created_at    TIMESTAMP DEFAULT NOW()
);
```

### Data Flow
1. Токен отримується один раз з `/api/token` (діє до 23:59)
2. Список документів завантажується з `/laws/main/r.tsv` (330 документів станом на 13.03.2026)
3. Поле `card` парситься regex: `"Орган; Тип від ДД.ММ.РРРР"` → `issuer`, `doc_type`, `date_revision`
4. Для кожного документа завантажується HTML з `/laws/show/{nreg}` (затримка 5–7 сек між запитами)
5. HTML конвертується у Markdown
6. Перевіряється якість
7. Текст → MinIO (`documents/{nreg}.md`), метадані → PostgreSQL
8. Статистика → `outputs/analysis_report.json`, дамп → `outputs/data_dump.csv`

### Data Quality Controls
- Перевірка наявності `nreg` (унікальний ID)
- Перевірка наявності назви (`nazva`)
- Перевірка наявності статусу (`status`)
- Мінімальна довжина тексту: 100 символів
- Мінімальна кількість слів: 20
- Ідемпотентність: SELECT перед INSERT за `nreg` (повторний запуск не дублює рядки)

---

## 2. Automation Recommendation

### Scheduling
- DAG запускається щодня (`@daily`) для збору поновлених документів
- Нові документи додаються, дублікати пропускаються автоматично
- При помилці: 2 повторні спроби з паузою 5 хвилин

### Deployment
```bash
docker compose -f docker/docker-compose.yaml up -d --build
# Airflow UI: http://localhost:8080  (admin / admin)
# MinIO UI:   http://localhost:9001  (minioadmin / minioadmin123)
# PostgreSQL: localhost:5433         (ukrllm_user / password123)
```

### Scaling
- Збільшити `max_docs` у `config.yaml` для збору всіх 330+ документів
- Перейти на `CeleryExecutor` + Redis для паралельного виконання тасків
- Додати індекси: `CREATE INDEX ON documents(doc_type); CREATE INDEX ON documents(date_revision);`
- Партиціонування таблиці `documents` за `date_revision` при великих обсягах

### Monitoring
- Логи: `outputs/logs/pipeline.log` (ротація 10 МБ)
- Airflow UI: статус кожного запуску і таску
- Рекомендується: Slack/email сповіщення при `email_on_failure: true`

---

## 3. Analysis of Data Collected

| Метрика | Значення |
|---------|----------|
| Всього документів у TSV списку | 330 |
| Зібрано (тест, max_docs=10) | 10 |
| QC passed | 10/10 (100%) |
| QC failed | 0/10 (0%) |
| Всього слів | 660 |
| Всього символів | 6 748 |
| Середня кількість слів/документ | 66.0 |
| Середня кількість символів/документ | 674.8 |
| Мін. слів | 66 |
| Макс. слів | 66 |
| Опубліковано | 3 |
| Не опубліковано | 7 |

### Розподіл за статусом
| Статус | Кількість |
|--------|-----------|
| Чинний | 4 |
| Не визначено | 6 |

### Розподіл за типом документа (`doc_type`)
| Тип | Кількість |
|-----|-----------|
| Повідомлення | 6 |
| Склад колегіального органу | 2 |
| Інші | 2 |

### Розподіл за органом-видавцем (`issuer`)
| Орган | Кількість |
|-------|-----------|
| Національний банк | 6 |
| Постанова Верховної Ради України | 2 |
| Рішення Конституційного суду | 1 |
| Указ Президента України | 1 |

### Спостереження
- Тестова вибірка (10 документів) складається переважно з повідомлень НБУ — короткі документи (~66 слів)
- Всі документи мають однакову кількість слів (66) через схожий шаблонний формат повідомлень НБУ
- Для повноцінного аналізу рекомендується зібрати всі 330 документів (`max_docs: 330`)

---

## 4. Failure Cases

| Проблема | Причина | Рішення |
|----------|---------|---------|
| Токен діє лише до 23:59 | Обмеження API | Автоматичне оновлення при 401/403 (реалізовано) |
| XCom перевищує ліміт (~1 МБ) при великих вибірках | Airflow зберігає дані в БД | Передача через файли в `outputs/tmp/` (реалізовано) |
| Конфлікт порту 5432 з локальним PostgreSQL | Два сервіси на одному порту | Docker Postgres перенесено на порт 5433 |
| SQLAlchemy 2.0 несумісна з Airflow 2.9 | Версійний конфлікт | Закріплено `sqlalchemy<2.0` у `requirements.txt` для Airflow |
| URL-encoded nreg (кирилиця) | `/go/п...` кодується в URL | `urllib.parse.unquote` при потребі |
| `card` без стандартного формату | Не всі документи мають "Орган; Тип від Дата" | Regex повертає часткове значення, не критично |
| Короткі документи (~66 слів) | Повідомлення НБУ дуже лаконічні | Знизити поріг або фільтрувати за `doc_type` |

### Що можна покращити
- Зберігати `Last-Modified` заголовок для інкрементальних оновлень
- Реалізувати `If-Modified-Since` для економії API запитів
- Додати NeMo-Curator для фільтрації низькоякісних текстів
- Окрема база даних для Airflow metadata (зараз спільна `ukrllm_db`)
- Декодування URL у nreg (`urllib.parse.unquote`) для документів з кирилицею

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
python -m poetry install --no-root

# 3. Start infrastructure
docker compose -f docker/docker-compose.yaml up -d --build
# Wait ~60 seconds for Airflow to initialize

# 4. Run full pipeline
python -m poetry run python scripts/001-run-pipeline-entrypoint.py
```

### Configuration (`configs/config.yaml`)
```yaml
api:
  max_docs: 10       # кількість документів для збору (330 доступно)
  delay_min: 5       # мін. затримка між запитами (сек)
  delay_max: 7       # макс. затримка між запитами (сек)
database:
  url: "postgresql://ukrllm_user:password123@127.0.0.1:5433/ukrllm_db"
minio:
  endpoint: "localhost:9000"
```

### Expected Output
```
outputs/
├── logs/pipeline.log         # детальні логи виконання
├── data_dump.csv             # дамп таблиці documents з PostgreSQL
└── analysis_report.json      # статистика датасету
```

PostgreSQL: таблиця `documents` (localhost:5433)
MinIO: bucket `documents`, файли `{nreg}.md` (localhost:9001)

### Verification
```bash
# Logs
cat outputs/logs/pipeline.log

# PostgreSQL
docker exec -it ukrllm_postgres psql -U ukrllm_user -d ukrllm_db \
  -c "SELECT nreg, nazva, doc_type, date_revision, status FROM documents;"

# Analysis
cat outputs/analysis_report.json

# Idempotency check (second run should show 0 saved, 10 skipped)
python -m poetry run python scripts/001-run-pipeline-entrypoint.py
```

### Random Seeds
- `random.uniform(delay_min, delay_max)` — не фіксований навмисно для уникнення блокування API
