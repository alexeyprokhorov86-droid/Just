"""
Smoke-тест embedding consistency для Qwen3-Embedding-0.6B.

Проверяет:
1. Инструкция-чувствительность модели (asymmetric document↔query encoding).
2. Self-similarity doc×query cosine для одной и той же фразы — должна быть
   > 0.85 (sanity check, что prompts не ломают семантику).

Запускать:
  - Вручную перед переключением RAG на embedding_v2.
  - Периодически через auto_agent_cron.py (drift detection) — exit code 0/1/2.

Exit codes:
  0 — PASS (min doc↔query ≥ 0.85)
  1 — WARN (0.70 ≤ min < 0.85; работает, но стоит глянуть prompts)
  2 — FAIL (min < 0.70; модель ломается при инструкциях)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np

from chunkers.embedder import _get_shared_embedder


# Фиксированные русские фразы из домена Фрумелад (воспроизводимость результата).
TEST_TEXTS = [
    "Продажи за апрель 2026 года по группе тортов",
    "Сотрудники отдела бухгалтерии и их контакты",
    "Рецептура шоколадного мусса для пирожного Картошка",
    "Остатки сырья на складе Фрумелад и НФ",
    "План производства на следующую неделю",
]


def main() -> int:
    emb = _get_shared_embedder()
    emb.ensure_loaded()
    model = emb.model

    print("=" * 72)
    print("Embedding Consistency Smoke-Test")
    print("=" * 72)
    print(f"Model dim: {emb.dim}")
    print()

    doc_vecs = model.encode(
        TEST_TEXTS,
        prompt_name="document",
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    )
    query_vecs = model.encode(
        TEST_TEXTS,
        prompt_name="query",
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    )

    print(f"{'#':<3} {'doc↔query cos':<16} text")
    print("-" * 72)
    sims = []
    for i, (dv, qv) in enumerate(zip(doc_vecs, query_vecs)):
        s = float(np.dot(dv, qv))
        sims.append(s)
        print(f"{i + 1:<3} {s:<16.4f} {TEST_TEXTS[i][:50]}")

    avg = sum(sims) / len(sims)
    mn = min(sims)
    mx = max(sims)
    print()
    print(f"doc↔query self-similarity:  avg={avg:.4f}  min={mn:.4f}  max={mx:.4f}")

    # Классификация instruction-awareness
    if all(s > 0.99 for s in sims):
        awareness = "SYMMETRIC (prompts не влияют)"
    elif avg > 0.90:
        awareness = "WEAK AWARENESS (слабая асимметрия)"
    elif avg > 0.70:
        awareness = "INSTRUCTION-AWARE (нормальная асимметрия, дисциплина критична)"
    else:
        awareness = "STRONG AWARENESS (сильное влияние инструкций)"
    print(f"Verdict: {awareness}")
    print()

    if mn >= 0.85:
        print(f"[PASS] min doc↔query = {mn:.4f} ≥ 0.85")
        return 0
    if mn >= 0.70:
        print(f"[WARN] min doc↔query = {mn:.4f} < 0.85 — проверьте prompts в config_sentence_transformers.json")
        return 1
    print(f"[FAIL] min doc↔query = {mn:.4f} < 0.70 — модель ломается при инструкциях")
    return 2


if __name__ == "__main__":
    sys.exit(main())
