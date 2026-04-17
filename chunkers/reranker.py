"""
Reranker — Qwen3-Reranker-0.6B для переранжирования retrieval результатов.

Qwen3-Reranker — это causal LM (Qwen3ForCausalLM), а не классификатор.
Работает по паттерну: system prompt + <Instruct>/<Query>/<Document> →
ассистент должен ответить "yes" или "no". Score = P(yes) через softmax
по логитам последнего токена на позиции [no_token, yes_token].

Используется ПОСЛЕ retrieval по embedding_v2 (top-30 → rerank → top-5).
Singleton; модель ~1.5GB RAM, latency ~200-400мс/пара на CPU.

Активация в rag_agent через .env USE_RERANKER=true.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_state = {
    "model": None,
    "tokenizer": None,
    "prefix_tokens": None,
    "suffix_tokens": None,
    "token_true_id": None,
    "token_false_id": None,
    "max_length": 4096,  # режем агрессивно для CPU (модель поддерживает 32k)
}

RERANKER_MODEL = "Qwen/Qwen3-Reranker-0.6B"
MAX_DOC_CHARS = 2000

DEFAULT_INSTRUCTION = (
    "Given a Russian business question about the Frumelad confectionery "
    "company, judge whether the document is relevant and would help answer the query"
)

_SYSTEM_PROMPT = (
    '<|im_start|>system\nJudge whether the Document meets the requirements '
    'based on the Query and the Instruct provided. Note that the answer can '
    'only be "yes" or "no".<|im_end|>\n<|im_start|>user\n'
)
_ASSIST_SUFFIX = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"


def load_reranker():
    """Lazy singleton загрузка через AutoModelForCausalLM."""
    if _state["model"] is not None:
        return _state

    import time
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    t0 = time.time()
    logger.info(f"Loading reranker: {RERANKER_MODEL}...")

    tokenizer = AutoTokenizer.from_pretrained(RERANKER_MODEL, padding_side="left")
    model = AutoModelForCausalLM.from_pretrained(RERANKER_MODEL).eval()

    _state["tokenizer"] = tokenizer
    _state["model"] = model
    _state["prefix_tokens"] = tokenizer.encode(_SYSTEM_PROMPT, add_special_tokens=False)
    _state["suffix_tokens"] = tokenizer.encode(_ASSIST_SUFFIX, add_special_tokens=False)
    _state["token_true_id"] = tokenizer.convert_tokens_to_ids("yes")
    _state["token_false_id"] = tokenizer.convert_tokens_to_ids("no")

    elapsed = time.time() - t0
    logger.info(
        f"Reranker loaded in {elapsed:.1f}s "
        f"(yes_id={_state['token_true_id']}, no_id={_state['token_false_id']})"
    )
    return _state


def _format_pair(instruction: str, query: str, doc: str) -> str:
    return f"<Instruct>: {instruction}\n<Query>: {query}\n<Document>: {doc}"


def _score_batch(pairs: List[str], batch_size: int = 4) -> List[float]:
    import torch

    s = load_reranker()
    tokenizer = s["tokenizer"]
    model = s["model"]
    prefix = s["prefix_tokens"]
    suffix = s["suffix_tokens"]
    max_len = s["max_length"]
    yes_id = s["token_true_id"]
    no_id = s["token_false_id"]

    scores: List[float] = []
    for start in range(0, len(pairs), batch_size):
        chunk = pairs[start : start + batch_size]
        inputs = tokenizer(
            chunk,
            padding=False,
            truncation="longest_first",
            return_attention_mask=False,
            max_length=max_len - len(prefix) - len(suffix),
        )
        for i, ids in enumerate(inputs["input_ids"]):
            inputs["input_ids"][i] = prefix + ids + suffix
        inputs = tokenizer.pad(inputs, padding=True, return_tensors="pt", max_length=max_len)
        inputs = {k: v.to(model.device) for k, v in inputs.items()}

        with torch.no_grad():
            logits = model(**inputs).logits[:, -1, :]
            yes_logits = logits[:, yes_id]
            no_logits = logits[:, no_id]
            stacked = torch.stack([no_logits, yes_logits], dim=1)
            probs = torch.nn.functional.log_softmax(stacked, dim=1)
            batch_scores = probs[:, 1].exp().tolist()

        scores.extend(batch_scores)

    return scores


def rerank(
    query: str,
    candidates: List[Dict[str, Any]],
    text_key: str = "content",
    top_k: Optional[int] = None,
    instruction: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Пере-ранжировать кандидаты по релевантности запросу.

    Возвращает копию списка, отсортированную по rerank_score (P(yes) ∈ [0,1]).
    Каждому dict добавляется rerank_score (float). Исходное similarity
    сохраняется для отладки.
    """
    if not candidates:
        return []

    instr = instruction or DEFAULT_INSTRUCTION
    pairs = [
        _format_pair(instr, query, (c.get(text_key, "") or "")[:MAX_DOC_CHARS])
        for c in candidates
    ]

    scores = _score_batch(pairs)

    ranked = []
    for c, s in zip(candidates, scores):
        enriched = dict(c)
        enriched["rerank_score"] = float(s)
        ranked.append(enriched)
    ranked.sort(key=lambda x: x["rerank_score"], reverse=True)
    if top_k is not None:
        ranked = ranked[:top_k]

    logger.info(
        f"rerank: {len(candidates)} → top-{len(ranked)}, "
        f"P(yes) range [{min(scores):.3f} .. {max(scores):.3f}]"
    )
    return ranked


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    query = "Продажи тортов в апреле 2026"
    docs = [
        {"content": "Продажи торта Медовик в апреле 2026 составили 1200 штук."},
        {"content": "План производства на май 2026 — 3500 тортов."},
        {"content": "Remington razor blades are on sale this week."},
        {"content": "Отгрузка тортов Магнит с 26.02 по 11.03.2026 — 1120 штук промо."},
        {"content": "Сотрудники бухгалтерии: Ирина, Мария, Анна."},
    ]
    ranked = rerank(query, docs, top_k=5)
    print(f"\nQuery: {query}\n")
    for i, r in enumerate(ranked, 1):
        print(f"[{i}] P(yes)={r['rerank_score']:.4f}  {r['content']}")
