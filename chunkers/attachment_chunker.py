"""
Attachment chunker — вложения из email (PDF, Excel, изображения).
Этап 5 плана.
"""
import logging
from typing import List

from chunkers.base_chunker import BaseChunker, Chunk
from chunkers.config import CONFIDENCE

logger = logging.getLogger(__name__)


class AttachmentChunker(BaseChunker):

    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        """
        Вложения:
        - AI-анализ как envelope-чанк (confidence=0.6)
        - PDF: текст по страницам с overlap
        - Excel/CSV: группы строк с заголовками
        """
        # TODO: SELECT из source_documents WHERE source_kind LIKE '%attachment%'
        #       + извлечение содержимого (секции 6.1, 6.2 плана)
        logger.info("AttachmentChunker: generate_chunks — NOT IMPLEMENTED YET")
        return []
