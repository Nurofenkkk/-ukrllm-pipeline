import os
import json
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger


class DataAnalyzer:
    def __init__(self, config: dict):
        self.engine = create_engine(config["database"]["url"], future=True)

    def analyze(self) -> dict:
        """Compute dataset statistics from PostgreSQL."""
        with self.engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM documents", conn)

        if df.empty:
            logger.warning("No documents found for analysis")
            return {}

        stats = {
            "total_docs":     int(len(df)),
            "total_words":    int(df["word_count"].sum()),
            "total_chars":    int(df["char_count"].sum()),
            "avg_words":      round(float(df["word_count"].mean()), 1),
            "avg_chars":      round(float(df["char_count"].mean()), 1),
            "min_words":      int(df["word_count"].min()),
            "max_words":      int(df["word_count"].max()),
            "by_status":      df["status"].value_counts().to_dict(),
            "by_doc_type":    df["doc_type"].value_counts().to_dict() if "doc_type" in df.columns else {},
            "by_issuer":      df["issuer"].value_counts().head(10).to_dict() if "issuer" in df.columns else {},
            "published":      int((df["publics"] != "Не опубліковано").sum()) if "publics" in df.columns else 0,
            "not_published":  int((df["publics"] == "Не опубліковано").sum()) if "publics" in df.columns else 0,
        }

        logger.info(f"Analysis complete: {stats['total_docs']} docs, "
                    f"{stats['total_words']} words, {stats['total_chars']} chars")
        logger.info(f"  avg words/doc: {stats['avg_words']}, avg chars/doc: {stats['avg_chars']}")
        logger.info(f"  doc types: {stats['by_doc_type']}")
        return stats

    def save_report(self, stats: dict, path: str = "outputs/analysis_report.json"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        logger.info(f"Analysis report saved: {path}")

    def save_dump(self, path: str = "outputs/data_dump.csv"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with self.engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM documents", conn)
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info(f"Data dump saved: {path} ({len(df)} rows)")
        return df
