import re
import requests
import time
import random
import io
import csv
from loguru import logger


class RadaCollector:
    BASE = "https://data.rada.gov.ua"

    def __init__(self, config: dict):
        self.delay_min = config["api"]["delay_min"]
        self.delay_max = config["api"]["delay_max"]
        self.max_docs  = config["api"].get("max_docs", 50)

        # Получаем токен ОДИН РАЗ при старте
        self.token = self._get_fresh_token()
        self.headers = {"User-Agent": self.token}
        logger.info(f"Token: {self.token[:8]}...")

    def _get_fresh_token(self) -> str:
        """Получить токен - только один раз при старте!"""
        resp = requests.get(f"{self.BASE}/api/token")
        resp.raise_for_status()
        token = resp.json().get("token")
        logger.info("Got fresh token")
        return token

    def _delay(self):
        """Пауза 5-7 секунд между запросами"""
        t = random.uniform(self.delay_min, self.delay_max)
        logger.debug(f"Sleeping {t:.1f}s...")
        time.sleep(t)

    def _get(self, url: str) -> requests.Response:
        """GET с автоматическим обновлением токена при 401/403."""
        resp = requests.get(url, headers=self.headers)
        if resp.status_code in (401, 403):
            logger.warning(f"Token rejected ({resp.status_code}), refreshing...")
            self.token = self._get_fresh_token()
            self.headers = {"User-Agent": self.token}
            resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp

    @staticmethod
    def _parse_card(card: str) -> dict:
        """
        Parse card field: 'Орган; Тип від ДД.ММ.РРРР'
        Returns: issuer, doc_type, date_revision
        """
        result = {"issuer": "", "doc_type": "", "date_revision": ""}
        if not card:
            return result
        parts = card.split(";", 1)
        result["issuer"] = parts[0].strip()
        if len(parts) >= 2:
            m = re.match(r"^(.*?)\s+від\s+(\d{2}\.\d{2}\.\d{4})$", parts[1].strip())
            if m:
                result["doc_type"] = m.group(1).strip()
                result["date_revision"] = m.group(2).strip()
            else:
                result["doc_type"] = parts[1].strip()
        return result

    def get_updated_list_tsv(self) -> list[dict]:
        """
        Список поновлених документів з реквізитами у TSV форматі.
        URL: /laws/main/r.tsv
        Поля: num, card, nazva, status, publics, link, size
        """
        url = f"{self.BASE}/laws/main/r.tsv"
        resp = self._get(url)
        self._delay()

        # Парсим TSV
        reader = csv.DictReader(
            io.StringIO(resp.text),
            delimiter="\t"
        )
        docs = []
        for row in reader:
            link = row.get("link", "")
            nreg = link.split("/go/")[-1] if "/go/" in link else ""
            card = row.get("card", "").strip()
            parsed = self._parse_card(card)
            docs.append({
                "nreg":          nreg,
                "num":           row.get("num", "").strip(),
                "nazva":         row.get("nazva", "").strip(),
                "status":        row.get("status", "").strip(),
                "card":          card,
                "issuer":        parsed["issuer"],
                "doc_type":      parsed["doc_type"],
                "date_revision": parsed["date_revision"],
                "publics":       row.get("publics", "").strip(),
                "link":          link.strip(),
                "size":          row.get("size", "").strip(),
            })
        logger.info(f"Got {len(docs)} documents from TSV list")
        return docs

    def get_document_text_html(self, nreg: str) -> str:
        """
        Текст документа у HTML форматі.
        URL: /laws/show/nreg
        """
        url = f"{self.BASE}/laws/show/{nreg}"
        resp = self._get(url)
        self._delay()
        return resp.text

    def collect_all(self) -> list[dict]:
        """Головний метод - збирає всі документи"""
        all_docs = []

        # Шаг 1 - получаем список документов с метаданными
        logger.info("Fetching updated documents list...")
        items = self.get_updated_list_tsv()

        # Шаг 2 - для каждого документа получаем текст
        for i, item in enumerate(items[:self.max_docs]):
            nreg = item.get("nreg")
            if not nreg:
                logger.warning(f"No nreg for item {i}, skipping")
                continue
            try:
                logger.info(f"[{i+1}/{min(len(items), self.max_docs)}] Fetching text: {nreg}")
                item["text_html"] = self.get_document_text_html(nreg)
                all_docs.append(item)
                logger.info(f"OK: {nreg} - {item['nazva'][:60]}")
            except Exception as e:
                logger.error(f"Failed {nreg}: {e}")

        logger.info(f"Total collected: {len(all_docs)} documents")
        return all_docs
