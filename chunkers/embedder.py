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

# Обрезка текста до ~512 токенов (~2000 символов для русского).
# Qwen3 поддерживает 8192, но длинные тексты сильно замедляют CPU inference.
MAX_TEXT_CHARS = 1000


def load_model(backend: str = "torch"):
    """
    Lazy singleton загрузка Qwen3-Embedding-0.6B.
    backend: "torch" (default) или "onnx" (быстрее на CPU).
    """
    global _model
    if _model is not None:
        return _model

    logger.info(f"Loading embedding model: {EMBEDDING_MODEL} (backend={backend})...")
    t0 = time.time()

    from sentence_transformers import SentenceTransformer

    kwargs = {}
    if backend == "onnx":
        kwargs["backend"] = "onnx"

    _model = SentenceTransformer(EMBEDDING_MODEL, **kwargs)

    dim = _model.get_sentence_embedding_dimension()
    elapsed = time.time() - t0
    logger.info(f"Model loaded in {elapsed:.1f}s, dimension={dim}")

    if dim != EMBEDDING_DIM:
        logger.warning(
            f"Expected dim={EMBEDDING_DIM}, got {dim}. "
            f"Update EMBEDDING_DIM in config.py!"
        )

    return _model


def _truncate(text: str, max_chars: int = MAX_TEXT_CHARS) -> str:
    """Обрезка текста до max_chars символов."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


class Embedder:
    """Обёртка для batch embedding с Qwen3-Embedding-0.6B."""

    def __init__(self, batch_size: int = EMBEDDING_BATCH_SIZE, backend: str = "torch"):
        self.batch_size = batch_size
        self.backend = backend
        self.model = None
        self.dim = EMBEDDING_DIM

    def ensure_loaded(self):
        if self.model is None:
            self.model = load_model(backend=self.backend)
            self.dim = self.model.get_sentence_embedding_dimension()

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Encode список текстов → list of float vectors.
        Тексты обрезаются до MAX_TEXT_CHARS символов для скорости.
        """
        self.ensure_loaded()

        if not texts:
            return []

        truncated = [_truncate(t) for t in texts]

        embeddings = self.model.encode(
            truncated,
            batch_size=self.batch_size,
            show_progress_bar=len(truncated) > 100,
            normalize_embeddings=True,
            convert_to_numpy=True,
            prompt_name="document",
        )

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
            convert_to_numpy=True,
            prompt_name="query",
        )
        if isinstance(emb, np.ndarray):
            return emb[0].tolist()
        return emb[0].tolist()
