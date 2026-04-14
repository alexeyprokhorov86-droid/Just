"""
Базовый класс для всех чанкеров.
Общая логика: подключение к БД, запись чанков, подсчёт токенов.
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional

import psycopg2
import psycopg2.extras

from chunkers.config import DB_CONFIG, DB_BATCH_SIZE, LOG_EVERY

logger = logging.getLogger(__name__)


@dataclass
class Chunk:
    """Единица данных для записи в source_chunks."""
    chunk_text: str
    chunk_type: str          # envelope | body | structured | distilled
    source_kind: str         # email | 1c_customer_order | km_fact | ...
    parent_document_id: Optional[int] = None
    parent_1c_ref: Optional[str] = None
    chunk_date: Optional[str] = None       # YYYY-MM-DD
    confidence: float = 0.5
    token_count: Optional[int] = None
    document_id: Optional[int] = None      # legacy field (= parent_document_id for compat)
    chunk_no: int = 0


class BaseChunker(ABC):
    """Базовый класс. Каждый chunker реализует generate_chunks()."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._conn = None
        self._write_conn = None

    @property
    def conn(self):
        """Read connection (for SELECT / server-side cursors)."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**DB_CONFIG)
        return self._conn

    @property
    def write_conn(self):
        """Separate write connection (for INSERT/commit without killing read cursors)."""
        if self._write_conn is None or self._write_conn.closed:
            self._write_conn = psycopg2.connect(**DB_CONFIG)
        return self._write_conn

    def close(self):
        for c in (self._conn, self._write_conn):
            if c and not c.closed:
                c.close()

    @abstractmethod
    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        """
        Генерирует список чанков.
        full=True  — полная пересборка
        full=False — инкрементальная (только новые/изменённые)
        """
        ...

    def estimate_tokens(self, text: str) -> int:
        """Грубая оценка токенов (для русского ~1.5 символа = 1 токен)."""
        return max(1, len(text) // 3)

    def save_chunks(self, chunks: List[Chunk]) -> int:
        """Записывает чанки в source_chunks. Возвращает количество записанных."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would save {len(chunks)} chunks")
            return 0

        if not chunks:
            return 0

        saved = 0
        cur = self.write_conn.cursor()
        try:
            for i in range(0, len(chunks), DB_BATCH_SIZE):
                batch = chunks[i:i + DB_BATCH_SIZE]
                values = []
                for c in batch:
                    doc_id = c.document_id or c.parent_document_id or None
                    tok = c.token_count or self.estimate_tokens(c.chunk_text)
                    values.append((
                        doc_id, c.chunk_no, c.chunk_text, tok,
                        c.confidence, c.chunk_type, c.source_kind,
                        c.parent_document_id, c.parent_1c_ref, c.chunk_date,
                        c.confidence,
                    ))
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO source_chunks
                        (document_id, chunk_no, chunk_text, token_count,
                         importance_score, chunk_type, source_kind,
                         parent_document_id, parent_1c_ref, chunk_date,
                         confidence)
                    VALUES %s
                    """,
                    values,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                )
                saved += len(batch)
                if saved % LOG_EVERY == 0:
                    logger.info(f"  saved {saved}/{len(chunks)} chunks...")

            self.write_conn.commit()
            logger.info(f"Saved {saved} chunks total")
        except Exception:
            self.write_conn.rollback()
            raise
        finally:
            cur.close()

        return saved
