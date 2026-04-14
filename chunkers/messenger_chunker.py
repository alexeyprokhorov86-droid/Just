"""
Messenger chunker — диалоговые блоки из Telegram/Matrix.
Этап 4 плана (низкий приоритет).
"""
import logging
from typing import List

from chunkers.base_chunker import BaseChunker, Chunk
from chunkers.config import CONFIDENCE, MESSENGER_SESSION_GAP_MINUTES

logger = logging.getLogger(__name__)


class MessengerChunker(BaseChunker):

    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        """
        Группировка сообщений по сессиям (перерыв > 30 мин = новая сессия).
        Внутри сессии: если > 400 токенов — разбить по авторским блокам.
        """
        # TODO: SELECT из source_documents WHERE source_kind IN ('telegram', 'matrix')
        #       Группировка по chat + time gap (секция 5.2 плана)
        logger.info("MessengerChunker: generate_chunks — NOT IMPLEMENTED YET")
        return []
