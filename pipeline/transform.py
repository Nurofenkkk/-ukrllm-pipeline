import re
from markdownify import markdownify as md
from loguru import logger


class DocumentTransformer:
    def html_to_markdown(self, html: str) -> str:
        """Конвертируем HTML в чистый Markdown"""
        # Конвертация HTML -> Markdown
        markdown = md(html, heading_style="ATX", strip=["script", "style"])

        # Убираем лишние пустые строки (больше 2 подряд)
        markdown = re.sub(r'\n{3,}', '\n\n', markdown)

        # Убираем пробелы в начале и конце
        markdown = markdown.strip()

        return markdown

    def transform_document(self, doc: dict) -> dict:
        """Трансформируем один документ"""
        html = doc.get("text_html", "")

        if not html:
            doc["text_markdown"] = ""
            doc["word_count"] = 0
            doc["char_count"] = 0
            return doc

        doc["text_markdown"] = self.html_to_markdown(html)
        doc["char_count"] = len(doc["text_markdown"])
        doc["word_count"] = len(doc["text_markdown"].split())

        # Удаляем сырой HTML - он больше не нужен
        doc.pop("text_html", None)

        logger.info(f"Transformed doc {doc.get('id', '')}: {doc['word_count']} words, {doc['char_count']} chars")
        return doc

    def transform_all(self, docs: list[dict]) -> list[dict]:
        """Трансформируем все документы"""
        transformed = []
        for doc in docs:
            try:
                transformed.append(self.transform_document(doc))
            except Exception as e:
                logger.error(f"Transform failed for {doc.get('id', '')}: {e}")
        logger.info(f"Transformed {len(transformed)} documents")
        return transformed
