"""
PDF handler — извлечение текста + анализ через Vision с anti-hallucination.

Архитектура:
1. Text-extract через PyPDF2 (быстро, работает на text-PDF).
2. Качественный критерий: если после PyPDF2 текст <100 символов ИЛИ
   содержит только ЭДО-протокол (Диадок/Контур/подписи) — это сканированный
   PDF, переключаемся на Vision.
3. Vision: pdf2image → PNG per page → multi-image GPT-4.1 запрос.
4. LLM-summary на extracted_text с anti-hallucination prompt'ом.

Это исправляет конкретную боль: id=464 (УПД от Рахата) был image-based PDF,
где PyPDF2 извлёк только страницу 3 (протокол Диадок). Vision видит все
страницы включая таблицу УПД, а anti-hallucination промпт не даёт выдумать
цифры.
"""
from __future__ import annotations

import base64
import io
import logging

from .._prompts import build_analysis_prompt

log = logging.getLogger("tools.attachments.pdf")

# Маркеры ЭДО-протокола — если это единственное что извлеклось, значит
# содержимое документа в image-based страницах.
_EDO_PROTOCOL_MARKERS = (
    "передан через диадок",
    "оператора эдо",
    "подписи отправителя",
    "подписи получателя",
    "идентификатор документа",
    "сертификат",
)


def _is_only_edo_protocol(text: str) -> bool:
    """Текст состоит только из служебных страниц оператора ЭДО?"""
    if not text:
        return True
    low = text.lower()
    markers_hit = sum(1 for m in _EDO_PROTOCOL_MARKERS if m in low)
    # Если встретились >=3 маркера протокола, а общая длина < 1500 символов —
    # это именно протокольный лист без содержимого.
    return markers_hit >= 3 and len(text) < 1500


def _extract_text_pypdf2(pdf_data: bytes) -> str:
    try:
        import PyPDF2
    except ImportError:
        log.warning("PyPDF2 not installed")
        return ""
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_data))
        parts: list[str] = []
        for page_num, page in enumerate(reader.pages[:50], 1):
            text = (page.extract_text() or "").strip()
            if text:
                parts.append(f"=== Страница {page_num} ===\n{text}")
        return "\n\n".join(parts)
    except Exception as e:
        log.warning(f"PyPDF2 extract failed: {e}")
        return ""


def _render_pdf_pages(pdf_data: bytes, last_page: int = 20) -> list[bytes]:
    try:
        from pdf2image import convert_from_bytes
    except ImportError:
        log.warning("pdf2image not installed")
        return []
    try:
        images = convert_from_bytes(pdf_data, first_page=1, last_page=last_page)
    except Exception as e:
        log.warning(f"pdf2image convert failed: {e}")
        return []
    result: list[bytes] = []
    for img in images:
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        result.append(buf.getvalue())
    return result


def _vision_extract_text(
    page_images: list[bytes],
    gpt_client,
    model: str = "openai/gpt-4.1",
) -> str:
    """Vision-OCR: каждая страница отдельным запросом → склейка текстом."""
    if not gpt_client or not page_images:
        return ""
    parts: list[str] = []
    for i, png in enumerate(page_images, 1):
        b64 = base64.standard_b64encode(png).decode()
        try:
            response = gpt_client.chat.completions.create(
                model=model,
                max_tokens=4000,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{b64}"},
                            },
                            {
                                "type": "text",
                                "text": (
                                    "Извлеки ВЕСЬ видимый текст с этой страницы PDF-документа. "
                                    "Таблицы оформи как строки с разделителем ' | '. Сохрани "
                                    "цифры, номера, даты, суммы ровно как в документе. Если "
                                    "страница сильно размыта — напиши '[страница плохо читается]'. "
                                    "Не добавляй комментариев от себя."
                                ),
                            },
                        ],
                    }
                ],
            )
            text = (response.choices[0].message.content or "").strip()
            if text:
                parts.append(f"=== Страница {i} ===\n{text}")
        except Exception as e:
            log.warning(f"Vision OCR page {i} failed: {e}")
            parts.append(f"=== Страница {i} ===\n[OCR ошибка: {e}]")
    return "\n\n".join(parts)


def _analyze_with_vision(
    page_images: list[bytes],
    prompt: str,
    gpt_client,
    model: str = "openai/gpt-4.1",
    max_tokens: int = 3000,
) -> str:
    """Анализ всех страниц одним multi-image запросом (так LLM видит документ
    целиком и может связать данные между страницами)."""
    if not gpt_client or not page_images:
        return ""
    content_parts: list[dict] = []
    for png in page_images:
        b64 = base64.standard_b64encode(png).decode()
        content_parts.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{b64}"},
            }
        )
    content_parts.append({"type": "text", "text": prompt})
    try:
        response = gpt_client.chat.completions.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": content_parts}],
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        log.warning(f"Vision analysis failed: {e}")
        return f"[Vision-анализ не выполнился: {e}]"


def analyze_pdf(
    *,
    file_bytes: bytes,
    filename: str,
    chat_context: str,
    gpt_client,
    company_profile: str,
    model: str = "openai/gpt-4.1",
) -> dict:
    """Главный entry point. Возвращает структурированный dict."""
    errors: list[str] = []

    # Шаг 1: быстрое извлечение через PyPDF2.
    pypdf2_text = _extract_text_pypdf2(file_bytes)

    # Шаг 2: оценка качества. Если pypdf2 дал мало или только ЭДО-протокол —
    # поднимаем Vision.
    need_vision = (
        len(pypdf2_text) < 100
        or _is_only_edo_protocol(pypdf2_text)
    )

    extracted_text = pypdf2_text
    page_images: list[bytes] = []
    if need_vision:
        page_images = _render_pdf_pages(file_bytes, last_page=20)
        if page_images:
            vision_text = _vision_extract_text(page_images, gpt_client, model=model)
            if vision_text:
                # Если PyPDF2 что-то нашёл (протокол) — сохраняем его отдельно.
                if pypdf2_text:
                    extracted_text = (
                        f"{vision_text}\n\n"
                        f"=== Извлечено из текстового слоя (PyPDF2) ===\n"
                        f"{pypdf2_text}"
                    )
                else:
                    extracted_text = vision_text
            else:
                errors.append("PyPDF2 вернул мало/протокол, Vision-OCR тоже не дал текста")
        else:
            errors.append("pdf2image/poppler недоступны — fallback на Vision невозможен")

    # Шаг 3: финальный LLM-анализ с anti-hallucination guardrails.
    summary = ""
    if gpt_client is not None:
        doc_type_label = "PDF документ (image-based, через Vision)" if need_vision else "PDF документ"
        if page_images and need_vision:
            # Multi-image analysis — LLM смотрит прямо на картинки + extracted_text как backup.
            prompt = build_analysis_prompt(
                company_profile=company_profile,
                doc_type=doc_type_label,
                doc_content=f"[Прикреплено {len(page_images)} страниц как изображения. "
                            f"Также извлечённый OCR-текст:]\n\n{extracted_text[:4000]}",
                chat_context=chat_context,
                filename=filename,
            )
            summary = _analyze_with_vision(page_images, prompt, gpt_client, model=model)
        else:
            # Text-only analysis
            prompt = build_analysis_prompt(
                company_profile=company_profile,
                doc_type=doc_type_label,
                doc_content=extracted_text,
                chat_context=chat_context,
                filename=filename,
            )
            try:
                response = gpt_client.chat.completions.create(
                    model=model,
                    max_tokens=2500,
                    messages=[{"role": "user", "content": prompt}],
                )
                summary = (response.choices[0].message.content or "").strip()
            except Exception as e:
                errors.append(f"LLM analysis failed: {e}")

    return {
        "document_type": "pdf",
        "extracted_text": extracted_text,
        "structured_fields": {
            "used_vision": need_vision,
            "pages_rendered": len(page_images),
            "pypdf2_text_len": len(pypdf2_text),
        },
        "summary": summary,
        "confidence": 1.0 if summary and not errors else (0.5 if summary else 0.0),
        "errors": errors,
    }
