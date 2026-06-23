import re
import requests
import time
import random
import io
import csv
from loguru import logger


class RadaCollector:
    BASE = "https://data.rada.gov.ua"
    DICT_URLS = {
        "status": f"{BASE}/ogd/zak/laws/data/csv/stan.txt",
        "doc_type": f"{BASE}/ogd/zak/laws/data/csv/typ.txt",
    }

    def __init__(self, config: dict):
        self.delay_min = config["api"]["delay_min"]
        self.delay_max = config["api"]["delay_max"]
        self.max_docs  = config["api"].get("max_docs", 50)

        # Use a session to maintain cookies (required for CAPTCHA bypass)
        self.session = requests.Session()

        # Get token once at startup
        self.token = self._get_fresh_token()
        self.session.headers.update({"User-Agent": self.token})
        logger.info(f"Token: {self.token[:8]}...")

        # Load official dictionaries
        self.dict_status = self._load_dictionary("status")
        self.dict_doc_type = self._load_dictionary("doc_type")
        logger.info(f"Dictionaries loaded: {len(self.dict_status)} statuses, "
                    f"{len(self.dict_doc_type)} doc types")

    def _load_dictionary(self, name: str) -> dict:
        """Load official dictionary from Rada open data portal.
        Files are tab-separated: code<TAB>value, encoding windows-1251.
        """
        url = self.DICT_URLS.get(name)
        if not url:
            return {}
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            # Try windows-1251 first (common for Rada), fallback to utf-8
            for enc in ("windows-1251", "utf-8", "cp1251"):
                try:
                    text = resp.content.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                text = resp.content.decode("utf-8", errors="replace")

            mapping = {}
            for line in text.strip().splitlines():
                parts = line.split("\t", 1)
                if len(parts) == 2:
                    mapping[parts[0].strip()] = parts[1].strip()
            logger.info(f"Dictionary '{name}': {len(mapping)} entries")
            return mapping
        except Exception as e:
            logger.warning(f"Failed to load dictionary '{name}': {e}")
            return {}

    def _resolve_status(self, status_text: str) -> str:
        """Validate status against official dictionary."""
        if not self.dict_status:
            return status_text
        # If status_text is already a known value, return it
        if status_text in self.dict_status.values():
            return status_text
        # If it's a code, resolve it
        if status_text in self.dict_status:
            return self.dict_status[status_text]
        return status_text

    def _resolve_doc_type(self, doc_type_text: str) -> str:
        """Validate doc_type against official dictionary."""
        if not self.dict_doc_type:
            return doc_type_text
        if doc_type_text in self.dict_doc_type.values():
            return doc_type_text
        if doc_type_text in self.dict_doc_type:
            return self.dict_doc_type[doc_type_text]
        return doc_type_text

    def _get_fresh_token(self) -> str:
        """Get a fresh API token"""
        resp = self.session.get(f"{self.BASE}/api/token")
        resp.raise_for_status()
        token = resp.json().get("token")
        logger.info("Got fresh token")
        return token

    def _delay(self):
        """Delay between requests to avoid rate limiting"""
        t = random.uniform(self.delay_min, self.delay_max)
        logger.debug(f"Sleeping {t:.1f}s...")
        time.sleep(t)

    def _get(self, url: str, retries: int = 3) -> requests.Response:
        """GET with automatic token refresh on 401/403, retry on 503, and CAPTCHA bypass."""
        for attempt in range(retries):
            resp = self.session.get(url)

            if resp.status_code in (401, 403):
                logger.warning(f"Token rejected ({resp.status_code}), refreshing...")
                self.token = self._get_fresh_token()
                self.session.headers.update({"User-Agent": self.token})
                resp = self.session.get(url)

            # Handle CAPTCHA verification page
            if resp.status_code == 200 and "<title>Перевірка</title>" in resp.text:
                m = re.search(r'location\.href="(.*?)"', resp.text)
                if m:
                    redirect_path = m.group(1)
                    redirect_url = redirect_path if redirect_path.startswith("http") else f"{self.BASE}{redirect_path}"
                    logger.debug(f"CAPTCHA bypass: following redirect")
                    time.sleep(1)
                    self.session.get(redirect_url)
                    time.sleep(1)
                    resp = self.session.get(url)

            if resp.status_code == 503:
                wait = (attempt + 1) * 5
                logger.warning(f"503 error, retrying in {wait}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        resp.raise_for_status()
        return resp

    @staticmethod
    def _parse_card(card: str) -> dict:
        """
        Parse card field. Two known formats:
          1) 'Issuer; DocType від DD.MM.YYYY'
          2) 'DocType від DD.MM.YYYY № NNN' (no semicolon, doc_type includes issuer)
        Returns: issuer, doc_type, date_revision
        """
        result = {"issuer": "", "doc_type": "", "date_revision": ""}
        if not card:
            return result

        if ";" in card:
            # Format: "Issuer; DocType від DD.MM.YYYY"
            parts = card.split(";", 1)
            result["issuer"] = parts[0].strip()
            rest = parts[1].strip()
        else:
            # Format: "DocType від DD.MM.YYYY № NNN"
            rest = card.strip()

        # Extract date and optional number suffix
        m = re.match(r"^(.*?)\s+від\s+(\d{2}\.\d{2}\.\d{4})(?:\s+№\s+.*)?$", rest)
        if m:
            result["doc_type"] = m.group(1).strip()
            result["date_revision"] = m.group(2).strip()
        else:
            result["doc_type"] = rest

        return result

    def get_updated_list_tsv(self) -> list[dict]:
        """
        Fetch updated documents list with metadata in TSV format.
        URL: /laws/main/r.tsv
        Fields: num, card, nazva, status, publics, link, size
        """
        url = f"{self.BASE}/laws/main/r.tsv"
        resp = self._get(url)
        self._delay()

        # Parse TSV
        reader = csv.DictReader(
            io.StringIO(resp.text),
            delimiter="\t"
        )
        docs = []
        for row in reader:
            link = row.get("link", "")
            raw_nreg = link.split("/go/")[-1] if "/go/" in link else ""
            # Strip /edYYYYMMDD suffix (edition date) from nreg
            nreg = re.sub(r"/ed\d{8}$", "", raw_nreg)
            card = row.get("card", "").strip()
            parsed = self._parse_card(card)
            status_raw = row.get("status", "").strip()
            doc_type_raw = parsed["doc_type"]
            docs.append({
                "nreg":          nreg,
                "nreg_full":     raw_nreg,  # with /edYYYYMMDD for API calls
                "num":           row.get("num", "").strip(),
                "nazva":         row.get("nazva", "").strip(),
                "status":        self._resolve_status(status_raw),
                "card":          card,
                "issuer":        parsed["issuer"],
                "doc_type":      self._resolve_doc_type(doc_type_raw),
                "date_revision": parsed["date_revision"],
                "publics":       row.get("publics", "").strip(),
                "link":          link.strip(),
                "size":          row.get("size", "").strip(),
            })
        logger.info(f"Got {len(docs)} documents from TSV list")
        return docs

    def get_document_text_html(self, nreg: str) -> str:
        """
        Fetch document text in HTML format.
        URL: /laws/show/nreg
        """
        url = f"{self.BASE}/laws/show/{nreg}"
        resp = self._get(url)
        self._delay()
        return resp.text

    def collect_all(self) -> list[dict]:
        """Main method - collect all documents"""
        all_docs = []

        # Fetch documents list with metadata
        logger.info("Fetching updated documents list...")
        items = self.get_updated_list_tsv()

        # Fetch text for each document
        for i, item in enumerate(items[:self.max_docs]):
            nreg = item.get("nreg")
            nreg_full = item.get("nreg_full", nreg)
            if not nreg:
                logger.warning(f"No nreg for item {i}, skipping")
                continue
            try:
                logger.info(f"[{i+1}/{min(len(items), self.max_docs)}] Fetching text: {nreg}")
                item["text_html"] = self.get_document_text_html(nreg_full)
                all_docs.append(item)
                logger.info(f"OK: {nreg} - {item['nazva'][:60]}")
            except Exception as e:
                logger.error(f"Failed {nreg}: {e}")

        logger.info(f"Total collected: {len(all_docs)} documents")
        return all_docs
