"""
Embedder — Qwen3-Embedding-0.6B для генерации embeddings чанков.
Используется ТОЛЬКО через build_chunks_v2.py (старый e5-base остаётся в embedding_service.py).
"""
import logging
import time
from typing import List, Optional

import numpy as np

from chunkers.config import EMBEDDING_MODEL, EMBEDDING_DIM, EMBEDDING_BATCH_SIZE

logger = logging.getLogger(__name__)

_model = None  # singleton


def load_model():
    """
    Lazy singleton загрузка Qwen3-Embedding-0.6B.
    ~1.2GB модель, ~2GB RAM при inference.
    """
    global _model
    if _model is not None:
        return _model

    logger.info(f"Loading embedding model: {EMBEDDING_MODEL}...")
    t0 = time.time()

    from sentence_transformers import SentenceTransformer
    _model = SentenceTransformer(EMBEDDING_MODEL)

    dim = _model.get_sentence_embedding_dimension()
    elapsed = time.time() - t0
    logger.info(f"Model loaded in {elapsed:.1f}s, dimension={dim}")

    if dim != EMBEDDING_DIM:
        logger.warning(
            f"Expected dim={EMBEDDING_DIM}, got {dim}. "
            f"Update EMBEDDING_DIM in config.py!"
        )

    return _model


class Embedder:
    """Обёртка для batch embedding с Qwen3-Embedding-0.6B."""

    def __init__(self, batch_size: int = EMBEDDING_BATCH_SIZE):
        self.batch_size = batch_size
        self.model = None
        self.dim = EMBEDDING_DIM

    def ensure_loaded(self):
        if self.model is None:
            self.model = load_model()
            self.dim = self.model.get_sentence_embedding_dimension()

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Encode список текстов → list of float vectors.
        Qwen3-Embedding использует prompt_name='passage' для документов.
        """
        self.ensure_loaded()

        if not texts:
            return []

        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            show_progress_bar=False,
            normalize_embeddings=True,
            prompt_name="document",
        )

        # numpy → list[list[float]] для совместимости с psycopg2/pgvector
        if isinstance(embeddings, np.ndarray):
            return embeddings.tolist()
        return [e.tolist() for e in embeddings]

    def embed_single(self, text: str) -> Optional[List[float]]:
        """Embedding для одного текста."""
        result = self.embed_batch([text])
        return result[0] if result else None

    def embed_query(self, query: str) -> Optional[List[float]]:
        """
        Embedding для поискового запроса (prompt_name='query').
        Используется при retrieval, не при индексации.
        """
        self.ensure_loaded()
        emb = self.model.encode(
            [query],
            batch_size=1,
            show_progress_bar=False,
            normalize_embeddings=True,
            prompt_name="query",
        )
        if isinstance(emb, np.ndarray):
            return emb[0].tolist()
        return emb[0].tolist()
