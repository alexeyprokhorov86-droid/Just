"""
Email chunker — envelope + body чанки из source_documents.
Этап 1 плана.
"""
import logging
from typing import List

from chunkers.base_chunker import BaseChunker, Chunk
from chunkers.config import (
    CONFIDENCE, BODY_SPLIT_THRESHOLDS,
    BODY_MIN_PARAGRAPH_TOKENS, BODY_OVERLAP_TOKENS,
)

logger = logging.getLogger(__name__)


class EmailChunker(BaseChunker):

    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        chunks = []
        chunks.extend(self._generate_envelopes(full))
        chunks.extend(self._generate_bodies(full))
        return chunks

    # ------------------------------------------------------------------
    # Envelope chunks
    # ------------------------------------------------------------------
    def _generate_envelopes(self, full: bool) -> List[Chunk]:
        """Один envelope-чанк на каждое email."""
        # TODO: SELECT из source_documents WHERE source_kind='email'
        #       Шаблон: date | От: from → Кому: to | Тема: subject
        #       Суть: первые 2-3 предложения body
        logger.info("EmailChunker: generating envelopes — NOT IMPLEMENTED YET")
        return []

    # ------------------------------------------------------------------
    # Body chunks
    # ------------------------------------------------------------------
    def _generate_bodies(self, full: bool) -> List[Chunk]:
        """Body-чанки с разбивкой по абзацам."""
        # TODO: логика разбивки из плана (секция 3.2)
        #   - < 300 tokens → один чанк
        #   - 300-800 → split по абзацам
        #   - > 800 → split + overlap
        #   - Цитаты (>) — убираем
        #   - Prefix с метаданными для самодостаточности
        logger.info("EmailChunker: generating bodies — NOT IMPLEMENTED YET")
        return []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _strip_quotes(text: str) -> str:
        """Убирает строки-цитаты (начинающиеся с '>')."""
        lines = text.split("\n")
        return "\n".join(l for l in lines if not l.lstrip().startswith(">"))

    @staticmethod
    def _split_paragraphs(text: str) -> List[str]:
        """Разбивает текст по двойному переносу строки."""
        parts = [p.strip() for p in text.split("\n\n") if p.strip()]
        return parts
