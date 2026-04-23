"""
tools/vision_label — распознавание этикетки товара для формирования серии.

Извлекает из фото этикетки: номер партии, дата изготовления, срок годности
(или «годен в течение N месяцев» → вычисляется), масса нетто/брутто.

Формат вывода подстраивается под `Catalog_СерииНоменклатуры` в 1С:
  - Номер партии → Code / Наименование серии
  - Дата изготовления → ДатаПроизводства
  - Срок годности → ГоденДо
"""
from __future__ import annotations

import base64
import datetime
import json
import logging
import os
import pathlib
from typing import Optional

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field

_REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(_REPO / ".env")

logger = logging.getLogger("vision_label")

MODEL = os.getenv("VISION_MODEL", "anthropic/claude-opus-4.7")


class LabelExtractResult(BaseModel):
    nomenclature_name: Optional[str] = None
    manufacturer: Optional[str] = None
    batch_number: Optional[str] = None
    production_date: Optional[str] = None  # ISO YYYY-MM-DD
    expiry_date: Optional[str] = None      # ISO YYYY-MM-DD
    shelf_life_months: Optional[int] = None
    weight_net_kg: Optional[float] = None
    weight_gross_kg: Optional[float] = None
    gost: Optional[str] = None
    storage_conditions: Optional[str] = None
    raw_notes: Optional[str] = None

    model: Optional[str] = None
    input_tokens: int = 0
    output_tokens: int = 0


_PROMPT = """Ты анализируешь фото этикетки товара для формирования серии в учётной системе 1С.

Из этикетки извлеки поля в JSON:
{
  "nomenclature_name": "название товара как на этикетке",
  "manufacturer": "изготовитель (название организации)",
  "batch_number": "номер партии (строка, например '758')",
  "production_date": "YYYY-MM-DD дата изготовления",
  "expiry_date": "YYYY-MM-DD срок годности до (если прямо написан)",
  "shelf_life_months": число_месяцев_срока_годности (если написано 'годен N месяцев'),
  "weight_net_kg": масса_нетто_кг (число),
  "weight_gross_kg": масса_брутто_кг (число),
  "gost": "номер ГОСТ / ТУ если есть",
  "storage_conditions": "условия хранения одной строкой",
  "raw_notes": "любая важная для учёта инфа которая не уложилась в поля выше"
}

Правила:
- Если expiry_date напрямую не написан, а есть shelf_life_months — верни shelf_life_months, сам
  не считай expiry_date. Вычисление сделаем на стороне кода (production_date + N месяцев).
- Если какое-то поле не видно — верни null (не пиши «не указано»).
- batch_number — строкой, даже если это цифры (ведущие нули важны).
- Масса в кг: если написано «г» или «мг» — переведи в кг.
- Только валидный JSON, без markdown-обёрток.
"""


def extract_label(image_bytes: bytes) -> LabelExtractResult:
    """Прогоняет фото этикетки через Claude Vision (RouterAI)."""
    key = os.getenv("ROUTERAI_API_KEY") or os.getenv("ROUTER_AI_API_KEY")
    base_url = os.getenv("ROUTERAI_BASE_URL") or "https://router.requesty.ai/v1"
    if not key:
        raise RuntimeError("ROUTERAI_API_KEY not set")

    client = OpenAI(api_key=key, base_url=base_url)
    b64 = base64.b64encode(image_bytes).decode()
    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": _PROMPT},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
        ],
    }]
    resp = client.chat.completions.create(
        model=MODEL, messages=messages, temperature=0, max_tokens=1000,
    )
    raw = resp.choices[0].message.content.strip()
    # Удалим возможные markdown fenced-блоки
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.rsplit("```", 1)[0]
    data = json.loads(raw.strip())

    out = LabelExtractResult(**data)
    out.model = MODEL
    out.input_tokens = getattr(resp.usage, "prompt_tokens", 0) or 0
    out.output_tokens = getattr(resp.usage, "completion_tokens", 0) or 0

    # Вычислим expiry_date если есть production_date + shelf_life_months
    if not out.expiry_date and out.production_date and out.shelf_life_months:
        try:
            prod = datetime.date.fromisoformat(out.production_date)
            out.expiry_date = (prod + relativedelta(months=out.shelf_life_months)).isoformat()
        except Exception as e:
            logger.warning("expiry computation failed: %s", e)

    return out


__all__ = ["extract_label", "LabelExtractResult"]
