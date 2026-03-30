import sys
import yaml
from loguru import logger

sys.path.insert(0, ".")

from pipeline.collect import RadaCollector
from pipeline.transform import DocumentTransformer
from pipeline.quality import QualityChecker
from pipeline.load import DataLoader
from pipeline.analyze import DataAnalyzer


def main():
    # Загружаем конфиг (локальный для запуска вне Docker)
    import os
    config_path = os.environ.get("PIPELINE_CONFIG", "configs/config.local.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Настройка логов
    logger.add("outputs/logs/pipeline.log", rotation="10 MB")

    logger.info("=" * 50)
    logger.info("Starting UkrLLM Pipeline")
    logger.info("=" * 50)

    # Инициализация компонентов
    collector   = RadaCollector(config)
    transformer = DocumentTransformer()
    checker     = QualityChecker()
    loader      = DataLoader(config)
    analyzer    = DataAnalyzer(config)

    # ШАГ 1 - Сбор данных
    logger.info("Step 1: Collecting documents...")
    documents = collector.collect_all()
    logger.info(f"Collected: {len(documents)} documents")

    # ШАГ 2 - Трансформация HTML -> Markdown
    logger.info("Step 2: Transforming documents...")
    documents = transformer.transform_all(documents)

    # ШАГ 3 - Проверка качества
    logger.info("Step 3: Quality check...")
    good_docs, bad_docs = checker.filter_all(documents)
    logger.info(f"Passed: {len(good_docs)}, Failed: {len(bad_docs)}")

    # ШАГ 4 - Сохранение
    logger.info("Step 4: Saving documents...")
    loader.save_all(good_docs)

    # ШАГ 5 - Анализ данных
    logger.info("Step 5: Analyzing data...")
    stats = analyzer.analyze()
    analyzer.save_report(stats)
    analyzer.save_dump()

    # Итог
    logger.info("=" * 50)
    logger.info(f"Pipeline complete!")
    logger.info(f"Total collected : {len(documents)}")
    logger.info(f"Quality passed  : {len(good_docs)}")
    logger.info(f"Quality failed  : {len(bad_docs)}")
    logger.info(f"Total in DB     : {stats.get('total_docs', '?')}")
    logger.info(f"Total words     : {stats.get('total_words', '?')}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
