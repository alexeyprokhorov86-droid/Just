#!/usr/bin/env python3
"""Фаза 0.5 — Claude Vision smoke-test на PDF УПД.

Прогоняет `.tmp_input/Русагриком 17,03,2026.pdf` через Anthropic API,
просит вернуть структурированный JSON по полям из ТЗ.

Запуск: python3 scripts/vision_probe.py [--pdf <path>]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import pathlib
import sys

import anthropic
import httpx
from dotenv import load_dotenv

REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env")

DEFAULT_PDF = REPO / ".tmp_input" / "Русагриком 17,03,2026.pdf"
# Opus 4.7 — максимальное качество для таблиц УПД
MODEL = "claude-opus-4-7"

PROMPT = """Перед тобой УПД (универсальный передаточный документ) от поставщика.
Извлеки ВСЕ указанные поля и верни СТРОГО JSON без обёртки markdown.

Структура:
{
  "supplier": {
    "name": "…",
    "inn": "…",
    "kpp": "…"
  },
  "buyer": {
    "name": "…",
    "inn": "…",
    "kpp": "…"
  },
  "document": {
    "type": "УПД/Счёт-фактура/Накладная/…",
    "number": "…",
    "date": "YYYY-MM-DD",
    "total_amount": 0.0,
    "nds_total": 0.0,
    "currency": "RUB"
  },
  "items": [
    {
      "line": 1,
      "name": "…",                   // номенклатура как в документе
      "unit": "шт/кг/л",
      "quantity": 0.0,
      "price": 0.0,
      "sum_without_nds": 0.0,
      "nds_rate_percent": 20,        // или 10, 0, "Без НДС"
      "nds_sum": 0.0,
      "sum_with_nds": 0.0
    }
  ],
  "seals_and_signatures": {
    "supplier_seal_present": true,
    "supplier_signature_present": true,
    "buyer_seal_present": true,
    "buyer_signature_present": true,
    "supplier_name_from_seal": "…",   // организация как написана на печати
    "buyer_name_from_seal": "…"
  },
  "notes": "…"  // любые замечания, если видна битая/нечёткая часть
}

Если поле не видно — ставь null (не пропускай ключ). Количество строк в items — сколько реально в документе.
"""


def pdf_to_base64(path: pathlib.Path) -> str:
    return base64.standard_b64encode(path.read_bytes()).decode("ascii")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", type=pathlib.Path, default=DEFAULT_PDF)
    args = ap.parse_args()

    pdf = args.pdf
    if not pdf.exists():
        print(f"❌ Not found: {pdf}")
        return 1
    print(f"PDF: {pdf} ({pdf.stat().st_size} bytes)")

    api_key = os.environ["ANTHROPIC_API_KEY"]
    # Прокси: api.anthropic.com блокируется на RU IP. Используем Privoxy.
    http_client = httpx.Client(proxy="http://127.0.0.1:8118", timeout=300)
    client = anthropic.Anthropic(api_key=api_key, http_client=http_client)

    print(f"Model: {MODEL}")
    print("Encoding PDF…")
    b64 = pdf_to_base64(pdf)
    print(f"  base64 size: {len(b64)} chars")

    print("\nSending to Claude…")
    resp = client.messages.create(
        model=MODEL,
        max_tokens=8192,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": b64,
                    },
                },
                {"type": "text", "text": PROMPT},
            ],
        }],
    )

    text = resp.content[0].text.strip()
    print(f"\nUsage: in={resp.usage.input_tokens} out={resp.usage.output_tokens}")
    print(f"\n── Raw response ──\n{text[:3000]}\n")

    # Попытка распарсить
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.rsplit("```", 1)[0]
    try:
        parsed = json.loads(text)
    except Exception as e:
        print(f"⚠ JSON parse error: {e}")
        return 2

    out_path = REPO / ".tmp_input" / f"{pdf.stem}.vision.json"
    out_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ parsed, saved → {out_path}")

    # Быстрая summary
    print("\n── Summary ──")
    doc = parsed.get("document", {}) or {}
    sup = parsed.get("supplier", {}) or {}
    buy = parsed.get("buyer", {}) or {}
    print(f"  type: {doc.get('type')} № {doc.get('number')} от {doc.get('date')}")
    print(f"  supplier: {sup.get('name')} ИНН {sup.get('inn')}")
    print(f"  buyer: {buy.get('name')} ИНН {buy.get('inn')}")
    print(f"  total: {doc.get('total_amount')} руб (НДС {doc.get('nds_total')})")
    items = parsed.get("items") or []
    print(f"  items: {len(items)}")
    for it in items[:5]:
        print(f"    {it.get('line')}. {it.get('name')} — {it.get('quantity')} × {it.get('price')} = {it.get('sum_with_nds')}")
    seals = parsed.get("seals_and_signatures") or {}
    print(f"  supplier seal: {seals.get('supplier_seal_present')}, "
          f"buyer seal: {seals.get('buyer_seal_present')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
