"""
KM chunker — km_facts, km_decisions, km_tasks, km_policies → distilled chunks.
Этап 3 плана.
"""
import logging
from typing import List

from chunkers.base_chunker import BaseChunker, Chunk
from chunkers.config import CONFIDENCE

logger = logging.getLogger(__name__)

KM_TABLES = [
    ("km_facts", "km_fact"),
    ("km_decisions", "km_decision"),
    ("km_tasks", "km_task"),
    ("km_policies", "km_policy"),
]


class KMChunker(BaseChunker):

    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        chunks = []
        for table, source_kind in KM_TABLES:
            chunks.extend(self._generate_from_table(table, source_kind, full))
        return chunks

    def _generate_from_table(self, table: str, source_kind: str, full: bool) -> List[Chunk]:
        """
        Каждая запись km_* → один distilled-чанк.
        Формат: [{type}: {source_chat}, {date}, автор: {author}] {text}
        """
        # TODO: SELECT из km_* таблиц + форматирование (секция 5.1 плана)
        logger.info(f"KMChunker: {table} — NOT IMPLEMENTED YET")
        return []
