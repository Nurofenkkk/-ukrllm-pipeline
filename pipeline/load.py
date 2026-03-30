from sqlalchemy import create_engine, text
from minio import Minio
from io import BytesIO
from loguru import logger


class DataLoader:
    def __init__(self, config: dict):
        self.engine = create_engine(config["database"]["url"], future=True)
        self.minio = Minio(
            config["minio"]["endpoint"],
            access_key=config["minio"]["access_key"],
            secret_key=config["minio"]["secret_key"],
            secure=False
        )
        self.bucket = config["minio"]["bucket"]
        self._setup()

    def _setup(self):
        if not self.minio.bucket_exists(self.bucket):
            self.minio.make_bucket(self.bucket)
            logger.info(f"Created MinIO bucket: {self.bucket}")

        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS documents (
                    nreg          VARCHAR PRIMARY KEY,
                    num           VARCHAR,
                    nazva         TEXT,
                    status        VARCHAR,
                    card          TEXT,
                    issuer        TEXT,
                    doc_type      VARCHAR,
                    date_revision VARCHAR,
                    publics       TEXT,
                    link          TEXT,
                    size          VARCHAR,
                    word_count    INTEGER,
                    char_count    INTEGER,
                    minio_path    TEXT,
                    created_at    TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.commit()
        logger.info("Database ready")

    def is_duplicate(self, nreg: str) -> bool:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM documents WHERE nreg = :nreg"),
                {"nreg": nreg}
            ).fetchone()
        return result is not None

    def save(self, doc: dict):
        nreg = doc.get("nreg", "")
        if not nreg:
            logger.error("Cannot save document without nreg")
            return

        if self.is_duplicate(nreg):
            logger.info(f"Skipping duplicate: {nreg}")
            return

        # Text -> MinIO
        minio_path = f"documents/{nreg}.md"
        content = doc.get("text_markdown", "").encode("utf-8")
        self.minio.put_object(
            self.bucket, minio_path,
            BytesIO(content), len(content),
            content_type="text/markdown"
        )

        # Metadata -> PostgreSQL
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO documents
                    (nreg, num, nazva, status, card, issuer, doc_type, date_revision,
                     publics, link, size, word_count, char_count, minio_path)
                VALUES
                    (:nreg, :num, :nazva, :status, :card, :issuer, :doc_type, :date_revision,
                     :publics, :link, :size, :word_count, :char_count, :minio_path)
            """), {
                "nreg":          nreg,
                "num":           doc.get("num"),
                "nazva":         doc.get("nazva"),
                "status":        doc.get("status"),
                "card":          doc.get("card"),
                "issuer":        doc.get("issuer"),
                "doc_type":      doc.get("doc_type"),
                "date_revision": doc.get("date_revision"),
                "publics":       doc.get("publics"),
                "link":          doc.get("link"),
                "size":          doc.get("size"),
                "word_count":    doc.get("word_count", 0),
                "char_count":    doc.get("char_count", 0),
                "minio_path":    minio_path
            })
            conn.commit()
        logger.info(f"Saved: {nreg}")

    def save_all(self, docs: list[dict]):
        saved = 0
        skipped = 0
        for doc in docs:
            if self.is_duplicate(doc.get("nreg", "")):
                skipped += 1
                continue
            self.save(doc)
            saved += 1
        logger.info(f"Save complete: {saved} saved, {skipped} skipped")
