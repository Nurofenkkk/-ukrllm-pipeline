import sys
import json
import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

CONFIG_PATH = "/opt/airflow/configs/config.yaml"
TMP_DIR = "/opt/airflow/outputs/tmp"


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _tmp_path(run_id: str, stage: str) -> str:
    safe = run_id.replace(":", "_").replace("+", "_").replace(" ", "_")
    os.makedirs(TMP_DIR, exist_ok=True)
    return f"{TMP_DIR}/{safe}_{stage}.json"


def task_collect(**ctx):
    from pipeline.collect import RadaCollector
    config = load_config()
    docs = RadaCollector(config).collect_all()

    path = _tmp_path(ctx["run_id"], "collect")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False)

    ctx["ti"].xcom_push(key="docs_path", value=path)
    return len(docs)


def task_transform(**ctx):
    from pipeline.transform import DocumentTransformer
    src = ctx["ti"].xcom_pull(key="docs_path", task_ids="collect")
    with open(src, encoding="utf-8") as f:
        docs = json.load(f)

    transformed = DocumentTransformer().transform_all(docs)

    path = _tmp_path(ctx["run_id"], "transform")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(transformed, f, ensure_ascii=False)

    ctx["ti"].xcom_push(key="docs_path", value=path)
    return len(transformed)


def task_quality(**ctx):
    from pipeline.quality import QualityChecker
    src = ctx["ti"].xcom_pull(key="docs_path", task_ids="transform")
    with open(src, encoding="utf-8") as f:
        docs = json.load(f)

    good, bad = QualityChecker().filter_all(docs)
    if len(good) == 0:
        raise ValueError(f"All {len(bad)} documents failed quality check!")

    path = _tmp_path(ctx["run_id"], "quality")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(good, f, ensure_ascii=False)

    ctx["ti"].xcom_push(key="docs_path", value=path)
    return len(good)


def task_load(**ctx):
    from pipeline.load import DataLoader
    config = load_config()
    src = ctx["ti"].xcom_pull(key="docs_path", task_ids="quality_check")
    with open(src, encoding="utf-8") as f:
        docs = json.load(f)

    DataLoader(config).save_all(docs)

    # Clean up temporary files for this run
    for stage in ("collect", "transform", "quality"):
        p = _tmp_path(ctx["run_id"], stage)
        if os.path.exists(p):
            os.remove(p)

    return len(docs)


def task_analyze(**ctx):
    from pipeline.analyze import DataAnalyzer
    config = load_config()
    analyzer = DataAnalyzer(config)
    stats = analyzer.analyze()
    analyzer.save_report(stats, "/opt/airflow/outputs/analysis_report.json")
    analyzer.save_dump("/opt/airflow/outputs/data_dump.csv")
    return stats.get("total_docs", 0)


default_args = {
    "owner": "ukrllm",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="ukrllm_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ukrllm"],
    description="Daily pipeline for Verkhovna Rada documents",
) as dag:

    collect = PythonOperator(
        task_id="collect",
        python_callable=task_collect,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    analyze = PythonOperator(
        task_id="analyze",
        python_callable=task_analyze,
    )

    collect >> transform >> quality_check >> load >> analyze
