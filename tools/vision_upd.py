"""
tools/vision_upd — извлечение структурированных данных из УПД через Claude Vision.

Две основные функции:
  - extract_upd(sources: list[bytes|Path]) -> UpdExtractResult
  - validate_upd(result) -> list[UpdWarning]   # блокеры + предупреждения

Backend: RouterAI (OpenAI-compatible API, модель anthropic/claude-opus-4.7).
PDF — растеризуется через pdf2image (poppler) в JPEG и отдаётся как image.
Для PDF > 3 страниц берём только первые 3 (УПД обычно 1-2 стр., больше — мусор).
"""
from __future__ import annotations

import base64
import io
import json
import logging
import os
import pathlib
from typing import Literal, Optional

import psycopg2
from dotenv import load_dotenv
from openai import OpenAI
from pdf2image import convert_from_bytes
from pydantic import BaseModel, Field

_REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(_REPO / ".env")

logger = logging.getLogger("vision_upd")

MODEL = os.getenv("VISION_MODEL", "anthropic/claude-opus-4.7")
PDF_MAX_PAGES = 3
PDF_DPI = 200  # баланс качество/размер, 200 достаточно для таблиц УПД
ALLOWED_VAT_RATES: set[int | None] = {0, 10, 20, 22, None}  # None = «Без НДС»
SUM_TOLERANCE_RUB = 1.0  # разница sum(items) vs total ≤ 1 руб — ок


# ─── Pydantic модели ────────────────────────────────────────────────────

class UpdParty(BaseModel):
    name: Optional[str] = None
    inn: Optional[str] = None
    kpp: Optional[str] = None


class UpdDocument(BaseModel):
    type: Optional[str] = None
    number: Optional[str] = None
    date: Optional[str] = None
    total_amount: Optional[float] = None
    nds_total: Optional[float] = None
    currency: Optional[str] = "RUB"


class UpdItem(BaseModel):
    line: Optional[int] = None
    name: Optional[str] = None
    unit: Optional[str] = None
    quantity: Optional[float] = None
    price: Optional[float] = None
    sum_without_nds: Optional[float] = None
    nds_rate_percent: Optional[int | float | str] = None  # 20 / "Без НДС"
    nds_sum: Optional[float] = None
    sum_with_nds: Optional[float] = None


class UpdSeals(BaseModel):
    supplier_seal_present: Optional[bool] = None
    supplier_signature_present: Optional[bool] = None
    buyer_seal_present: Optional[bool] = None
    buyer_signature_present: Optional[bool] = None
    supplier_name_from_seal: Optional[str] = None
    buyer_name_from_seal: Optional[str] = None


class UpdExtractResult(BaseModel):
    supplier: UpdParty = Field(default_factory=UpdParty)
    buyer: UpdParty = Field(default_factory=UpdParty)
    document: UpdDocument = Field(default_factory=UpdDocument)
    items: list[UpdItem] = Field(default_factory=list)
    seals_and_signatures: UpdSeals = Field(default_factory=UpdSeals)
    notes: Optional[str] = None
    # метаданные OCR
    model: Optional[str] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None


class UpdWarning(BaseModel):
    level: Literal["error", "warning"]
    code: str
    message: str
    details: dict = Field(default_factory=dict)


# ─── OCR-запрос ─────────────────────────────────────────────────────────

_PROMPT = """Перед тобой УПД (универсальный передаточный документ) от поставщика.
Если несколько страниц — объедини данные в один результат.

Извлеки поля и верни СТРОГО JSON без markdown-обёрток. Ключи — латиницей:

{
  "supplier": {"name": "…", "inn": "…", "kpp": "…"},
  "buyer":    {"name": "…", "inn": "…", "kpp": "…"},
  "document": {
    "type": "УПД|Счёт-фактура|Накладная|…",
    "number": "…",
    "date": "YYYY-MM-DD",
    "total_amount": 0.0,
    "nds_total": 0.0,
    "currency": "RUB"
  },
  "items": [
    {
      "line": 1,
      "name": "…",
      "unit": "шт|кг|л|упак",
      "quantity": 0.0,
      "price": 0.0,
      "sum_without_nds": 0.0,
      "nds_rate_percent": 20,
      "nds_sum": 0.0,
      "sum_with_nds": 0.0
    }
  ],
  "seals_and_signatures": {
    "supplier_seal_present": true,
    "supplier_signature_present": true,
    "buyer_seal_present": true,
    "buyer_signature_present": true,
    "supplier_name_from_seal": "…",
    "buyer_name_from_seal": "…"
  },
  "notes": "…"
}

Правила:
- Если поле не видно — null (не пропускать ключ).
- items — ВСЕ строки товаров. Количество строк в items = в документе.
- nds_rate_percent — число (20, 22, 10, 0) или строка "Без НДС".
- Даты — строго YYYY-MM-DD."""


def _source_to_images(src: bytes | pathlib.Path) -> list[tuple[str, str]]:
    """Разворачивает источник в список (media_type, base64) картинок.
    PDF → pdf2image (первые PDF_MAX_PAGES страниц). Картинка → как есть.
    """
    if isinstance(src, pathlib.Path):
        data = src.read_bytes()
    else:
        data = src

    if data[:4] == b"%PDF":
        pages = convert_from_bytes(data, dpi=PDF_DPI, first_page=1, last_page=PDF_MAX_PAGES)
        out = []
        for page in pages:
            buf = io.BytesIO()
            page.save(buf, format="JPEG", quality=85)
            out.append(("image/jpeg", base64.standard_b64encode(buf.getvalue()).decode()))
        return out
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return [("image/png", base64.standard_b64encode(data).decode())]
    # JPEG или fallback
    return [("image/jpeg", base64.standard_b64encode(data).decode())]


def _get_client() -> OpenAI:
    return OpenAI(
        base_url=os.environ["ROUTERAI_BASE_URL"],
        api_key=os.environ["ROUTERAI_API_KEY"],
    )


def extract_upd(sources: list[bytes | pathlib.Path]) -> UpdExtractResult:
    """Основная функция. Принимает 1+ фото/PDF УПД, возвращает структурированный JSON.

    Raises:
        ValueError: если Claude вернул невалидный JSON.
    """
    if not sources:
        raise ValueError("sources пуст")

    client = _get_client()

    content: list[dict] = [{"type": "text", "text": _PROMPT}]
    images_count = 0
    for src in sources:
        for media_type, b64 in _source_to_images(src):
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:{media_type};base64,{b64}"},
            })
            images_count += 1

    logger.info("extract_upd: %d source(s) → %d images, model %s",
                len(sources), images_count, MODEL)
    resp = client.chat.completions.create(
        model=MODEL,
        max_tokens=8192,
        messages=[{"role": "user", "content": content}],
    )
    text = (resp.choices[0].message.content or "").strip()

    # Снять markdown-обёртку если вдруг
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.rsplit("```", 1)[0].strip()

    try:
        raw = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error("extract_upd: bad JSON: %s — raw=%r", e, text[:500])
        raise ValueError(f"Claude вернул невалидный JSON: {e}") from e

    result = UpdExtractResult(**raw)
    result.model = MODEL
    result.input_tokens = getattr(resp.usage, "prompt_tokens", None) if resp.usage else None
    result.output_tokens = getattr(resp.usage, "completion_tokens", None) if resp.usage else None
    logger.info(
        "extract_upd done: supplier=%s buyer=%s total=%s items=%d tok=%s/%s",
        result.supplier.inn, result.buyer.inn,
        result.document.total_amount, len(result.items),
        result.input_tokens, result.output_tokens,
    )
    return result


# ─── Валидатор ──────────────────────────────────────────────────────────

def _our_inns() -> set[str]:
    """ИНН всех наших организаций (из c1_organizations)."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "172.20.0.2"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "knowledge_base"),
        user=os.getenv("DB_USER", "knowledge"),
        password=os.getenv("DB_PASSWORD", ""),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT inn FROM c1_organizations WHERE inn IS NOT NULL AND inn<>''")
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def _inn_near_our(inn: str, our: set[str], max_diff: int = 1) -> str | None:
    """Fuzzy-match ИНН: если OCR ошибся в 1 цифре среди наших 8 организаций —
    возвращает правильный ИНН. Длина должна совпадать (10 или 12)."""
    if not inn:
        return None
    matches = []
    for o in our:
        if len(o) != len(inn):
            continue
        diff = sum(1 for a, b in zip(inn, o) if a != b)
        if 0 < diff <= max_diff:
            matches.append((diff, o))
    if not matches:
        return None
    matches.sort()
    # Возвращаем только если ближайший match уникален (diff меньше второго)
    if len(matches) == 1 or matches[0][0] < matches[1][0]:
        return matches[0][1]
    return None


def _norm_vat(value) -> int | None:
    """Нормализует nds_rate_percent в int или None для «Без НДС»."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip().lower()
        if "без" in s:
            return None
        try:
            return int(float(s.rstrip("%").strip()))
        except ValueError:
            return -1  # сигнал о нераспарсенном значении
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def validate_upd(result: UpdExtractResult) -> list[UpdWarning]:
    """Возвращает список предупреждений/ошибок. level='error' = блокер для создания ПТУ."""
    warnings: list[UpdWarning] = []
    our = _our_inns()

    # 1. Swap: supplier в УПД = наша организация → блокер
    supplier_own = result.supplier.inn and result.supplier.inn in our
    # Fuzzy: OCR мог ошибиться на 1 цифру (у нас всего 8 своих ИНН, коллизии крайне редки)
    supplier_own_fuzzy = None
    if not supplier_own and result.supplier.inn:
        supplier_own_fuzzy = _inn_near_our(result.supplier.inn, our)
    if supplier_own or supplier_own_fuzzy:
        shown_inn = supplier_own_fuzzy or result.supplier.inn
        warnings.append(UpdWarning(
            level="error",
            code="supplier_is_own_org",
            message=(
                "В УПД поставщиком указана НАША организация "
                f"(ИНН {shown_inn}, «{result.supplier.name}»). "
                "Документ оформлен неправильно — приём товара невозможен без согласования."
            ),
            details={"inn": shown_inn, "name": result.supplier.name,
                     "ocr_inn": result.supplier.inn, "fuzzy_matched": bool(supplier_own_fuzzy)},
        ))

    # 2. Покупатель (buyer) должен быть наша организация
    if result.buyer.inn and result.buyer.inn not in our:
        fuzzy = _inn_near_our(result.buyer.inn, our)
        if fuzzy:
            # OCR ошибся на 1 цифру → корректируем, НЕ блокируем
            warnings.append(UpdWarning(
                level="warning",
                code="buyer_inn_ocr_corrected",
                message=(
                    f"ИНН покупателя распознан как {result.buyer.inn}, "
                    f"но ближайший ИНН нашей организации — {fuzzy} (отличие в 1 знаке). "
                    "Скорее всего, это OCR-ошибка. Использую скорректированное значение."
                ),
                details={"ocr_inn": result.buyer.inn, "corrected_inn": fuzzy,
                         "name": result.buyer.name},
            ))
            # Корректируем in-place, чтобы downstream-матчер видел правильный ИНН
            result.buyer.inn = fuzzy
        else:
            warnings.append(UpdWarning(
                level="error",
                code="buyer_not_own_org",
                message=(
                    "В УПД покупателем указана НЕ наша организация "
                    f"(ИНН {result.buyer.inn}, «{result.buyer.name}»). "
                    "Возможно, это УПД не для нас."
                ),
                details={"inn": result.buyer.inn, "name": result.buyer.name},
            ))

    # 3. Ни supplier.inn, ни buyer.inn не распознаны
    if not result.supplier.inn and not result.buyer.inn:
        warnings.append(UpdWarning(
            level="error",
            code="no_inn",
            message="Не распознан ни ИНН поставщика, ни ИНН покупателя. Нужны более чёткие фото.",
        ))

    # 4. Ставка НДС — только {0, 10, 20, 22, «Без НДС»}
    for it in result.items:
        rate = _norm_vat(it.nds_rate_percent)
        if rate is not None and rate not in ALLOWED_VAT_RATES:
            warnings.append(UpdWarning(
                level="warning" if rate == -1 else "warning",
                code="unusual_vat_rate",
                message=(
                    f"Строка {it.line} «{(it.name or '')[:40]}…»: "
                    f"необычная ставка НДС {it.nds_rate_percent!r}. "
                    f"Допустимы: 0, 10, 20, 22, «Без НДС»."
                ),
                details={"line": it.line, "rate_raw": it.nds_rate_percent},
            ))

    # 5. Сумма документа ≈ sum(items.sum_with_nds)
    if result.document.total_amount is not None and result.items:
        rows_total = sum((it.sum_with_nds or 0) for it in result.items)
        if abs(rows_total - result.document.total_amount) > SUM_TOLERANCE_RUB:
            warnings.append(UpdWarning(
                level="warning",
                code="sum_mismatch",
                message=(
                    f"Сумма по строкам ({rows_total:.2f}) не совпадает с суммой документа "
                    f"({result.document.total_amount:.2f})."
                ),
                details={"rows_sum": rows_total, "doc_total": result.document.total_amount},
            ))

    # 6. Нет товаров
    if not result.items:
        warnings.append(UpdWarning(
            level="error", code="no_items",
            message="Не распознано ни одной товарной строки.",
        ))

    # 7. Нет печатей/подписей с одной из сторон (не блокер, но предупреждение)
    s = result.seals_and_signatures
    if s.supplier_seal_present is False or s.supplier_signature_present is False:
        warnings.append(UpdWarning(
            level="warning", code="supplier_attestation_missing",
            message="На документе не видно печати/подписи поставщика.",
        ))
    if s.buyer_seal_present is False or s.buyer_signature_present is False:
        warnings.append(UpdWarning(
            level="warning", code="buyer_attestation_missing",
            message="На документе не видно печати/подписи покупателя.",
        ))

    return warnings


# ─── Форматирование для TG ──────────────────────────────────────────────

def format_extract_for_tg(result: UpdExtractResult, warnings: list[UpdWarning]) -> str:
    """Красиво оформленный обзор для кладовщика."""
    doc = result.document
    sup = result.supplier
    buy = result.buyer
    lines = [
        f"📄 <b>{doc.type or '?'} № {doc.number or '?'} от {doc.date or '?'}</b>",
        "",
        f"🏭 Поставщик: {sup.name or '?'} ИНН {sup.inn or '?'}",
        f"🏬 Покупатель: {buy.name or '?'} ИНН {buy.inn or '?'}",
        f"💰 Сумма: {doc.total_amount or 0:.2f} {doc.currency or 'RUB'} (НДС {doc.nds_total or 0:.2f})",
        "",
        f"📦 Позиций: {len(result.items)}",
    ]
    for it in result.items[:10]:
        lines.append(
            f"  {it.line}. {(it.name or '')[:60]} — "
            f"{it.quantity or 0:g} × {it.price or 0:.2f} = {it.sum_with_nds or 0:.2f} "
            f"(НДС {it.nds_rate_percent})"
        )
    if len(result.items) > 10:
        lines.append(f"  … и ещё {len(result.items) - 10}")

    if warnings:
        lines.append("")
        errors = [w for w in warnings if w.level == "error"]
        warns = [w for w in warnings if w.level == "warning"]
        if errors:
            lines.append(f"🚫 <b>Ошибки ({len(errors)}):</b>")
            for w in errors:
                lines.append(f"  • {w.message}")
        if warns:
            lines.append(f"⚠️ <b>Предупреждения ({len(warns)}):</b>")
            for w in warns:
                lines.append(f"  • {w.message}")
    else:
        lines.append("")
        lines.append("✅ Валидация пройдена.")

    return "\n".join(lines)
