from loguru import logger


class QualityChecker:
    MIN_TEXT_LENGTH = 100
    MIN_WORD_COUNT = 20

    def check(self, doc: dict) -> tuple[bool, list[str]]:
        errors = []

        # Check document ID
        if not doc.get("nreg"):
            errors.append("Missing document nreg")

        # Check title - in TSV the field is called "nazva"
        if not doc.get("nazva") and not doc.get("title") and not doc.get("name"):
            errors.append("Missing document title")

        # Check status
        if not doc.get("status"):
            errors.append("Missing document status")

        # Check text length
        text = doc.get("text_markdown", "")
        if len(text) < self.MIN_TEXT_LENGTH:
            errors.append(f"Text too short: {len(text)} chars")

        # Check word count
        word_count = doc.get("word_count", 0)
        if word_count < self.MIN_WORD_COUNT:
            errors.append(f"Too few words: {word_count}")

        is_valid = len(errors) == 0

        if is_valid:
            logger.info(f"QC passed: {doc.get('nreg')}")
        else:
            logger.warning(f"QC failed for {doc.get('nreg')}: {errors}")

        return is_valid, errors

    def filter_all(self, docs: list[dict]) -> tuple[list[dict], list[dict]]:
        good = []
        bad = []
        for doc in docs:
            is_valid, errors = self.check(doc)
            if is_valid:
                good.append(doc)
            else:
                bad.append({**doc, "_errors": errors})
        logger.info(f"QC results: {len(good)} passed, {len(bad)} failed")
        return good, bad
