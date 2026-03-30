import re
from markdownify import markdownify as md
from loguru import logger


class DocumentTransformer:
    def html_to_markdown(self, html: str) -> str:
        """Convert HTML to clean Markdown"""
        # Convert HTML -> Markdown
        markdown = md(html, heading_style="ATX", strip=["script", "style"])

        # Remove excessive blank lines (more than 2 in a row)
        markdown = re.sub(r'\n{3,}', '\n\n', markdown)

        # Strip leading and trailing whitespace
        markdown = markdown.strip()

        return markdown

    def transform_document(self, doc: dict) -> dict:
        """Transform a single document"""
        html = doc.get("text_html", "")

        if not html:
            doc["text_markdown"] = ""
            doc["word_count"] = 0
            doc["char_count"] = 0
            return doc

        doc["text_markdown"] = self.html_to_markdown(html)
        doc["char_count"] = len(doc["text_markdown"])
        doc["word_count"] = len(doc["text_markdown"].split())

        # Remove raw HTML - no longer needed
        doc.pop("text_html", None)

        logger.info(f"Transformed doc {doc.get('id', '')}: {doc['word_count']} words, {doc['char_count']} chars")
        return doc

    def transform_all(self, docs: list[dict]) -> list[dict]:
        """Transform all documents"""
        transformed = []
        for doc in docs:
            try:
                transformed.append(self.transform_document(doc))
            except Exception as e:
                logger.error(f"Transform failed for {doc.get('id', '')}: {e}")
        logger.info(f"Transformed {len(transformed)} documents")
        return transformed
