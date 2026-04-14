"""
Embedder — генерация embeddings для чанков.
Пока заглушка. Qwen3-Embedding-0.6B будет подключён отдельной задачей.
"""
import logging
from typing import List, Optional

import numpy as np

from chunkers.config import EMBEDDING_DIM, EMBEDDING_BATCH_SIZE

logger = logging.getLogger(__name__)


class Embedder:
    """Интерфейс для embedding модели."""

    def __init__(self):
        self.model = None
        self.dim = EMBEDDING_DIM

    def load_model(self):
        """Загрузка модели Qwen3-Embedding-0.6B."""
        # TODO: загрузка модели (отдельная задача — Фаза 2.5)
        raise NotImplementedError(
            "Embedding model not configured yet. "
            "Run setup for Qwen3-Embedding-0.6B first."
        )

    def embed_texts(self, texts: List[str]) -> Optional[np.ndarray]:
        """
        Генерирует embeddings для списка текстов.
        Возвращает np.ndarray shape (len(texts), EMBEDDING_DIM).
        """
        if self.model is None:
            logger.warning("Embedder: model not loaded, skipping embedding")
            return None

        # TODO: batch inference
        raise NotImplementedError

    def embed_single(self, text: str) -> Optional[np.ndarray]:
        """Embedding для одного текста."""
        result = self.embed_texts([text])
        if result is not None:
            return result[0]
        return None
