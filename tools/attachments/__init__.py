"""
Tool: analyze_attachment — единый анализатор вложений.

Заменяет распылённую логику `bot.py:analyze_media` (~90 строк dispatch по
media_type) единым входом. Detection формата по magic-bytes (не доверяем
Telegram mime_type или расширению — опыт показал что ЭДО шлёт файлы без
расширения). Специализированные handler-ы в handlers/.

Возвращает структурированный dict:
  {
    "document_type": str,       # 'pdf'|'xml'|'image'|'docx'|'xlsx'|'pptx'|'unknown'
    "extracted_text": str,      # полное содержимое для БД content_text
    "structured_fields": dict,  # метаданные извлечения (pages, kind, root_tag)
    "summary": str,             # LLM-анализ для БД media_analysis
    "confidence": float,        # 0..1
    "errors": list[str],        # предупреждения/ошибки (не бросает)
  }
"""
from __future__ import annotations

import base64
import logging

from pydantic import BaseModel, Field

from ..registry import tool
from ._detect import detect_format, mime_for_format
from .handlers import image_handler, ooxml_handler, pdf_handler, xml_handler

log = logging.getLogger("tools.attachments")


class AnalyzeAttachmentInput(BaseModel):
    file_b64: str = Field(
        description=(
            "Содержимое файла в base64. При вызове через invoke() — стандартный "
            "base64; при прямом import можно удобно передать bytes через хелпер "
            "analyze_attachment_bytes (см. ниже)."
        ),
        min_length=1,
    )
    filename: str = Field(
        default="",
        description="Имя файла (опционально, используется для подсказки типа и в prompt).",
    )
    mime_type: str = Field(
        default="",
        description=(
            "MIME от источника (опционально, игнорируется если magic-bytes "
            "показывают другой формат — хеш отправителя не доверяем)."
        ),
    )
    chat_context: str = Field(
        default="",
        description="Текст обсуждения в чате вокруг документа (для справки, НЕ источник данных).",
    )


@tool(
    name="analyze_attachment",
    domain="attachments",
    description=(
        "Анализирует любое вложение (PDF, XML ЭДО, image, docx, xlsx, pptx). "
        "Определяет реальный формат по magic-bytes, диспатчит в соответствующий "
        "handler. Возвращает {document_type, extracted_text, structured_fields, "
        "summary, confidence, errors}. Для PDF использует PyPDF2→Vision fallback "
        "(критично для ЭДО-сканов). Для XML — lxml + human-readable сериализация. "
        "Anti-hallucination промпт: LLM не додумывает цифры/даты/имена, а "
        "честно пишет 'не удалось извлечь' если данные не видны."
    ),
    input_model=AnalyzeAttachmentInput,
)
def analyze_attachment(
    file_b64: str,
    filename: str,
    mime_type: str,
    chat_context: str,
) -> dict:
    file_bytes = base64.b64decode(file_b64)
    return _analyze_attachment_impl(file_bytes, filename, mime_type, chat_context)


def analyze_attachment_bytes(
    *,
    file_bytes: bytes,
    filename: str = "",
    mime_type: str = "",
    chat_context: str = "",
) -> dict:
    """Удобный helper для прямого вызова из Python-кода (без base64 туда-сюда).
    bot.py/cron зовут именно его. LLM через invoke → analyze_attachment."""
    return _analyze_attachment_impl(file_bytes, filename, mime_type, chat_context)


def _analyze_attachment_impl(
    file_bytes: bytes,
    filename: str,
    mime_type: str,
    chat_context: str,
) -> dict:
    from company_context import get_company_profile
    company_profile = get_company_profile()

    # gpt_client берём из того же места что и bot.py — уже инициализированный.
    from bot import gpt_client  # lazy import (bot.py тяжёлый)

    fmt = detect_format(file_bytes)
    detected_mime = mime_for_format(fmt)
    log.info(
        "analyze_attachment: filename=%r size=%d bytes, declared_mime=%r, detected=%s",
        filename, len(file_bytes), mime_type, fmt,
    )

    if fmt == "pdf":
        result = pdf_handler.analyze_pdf(
            file_bytes=file_bytes, filename=filename, chat_context=chat_context,
            gpt_client=gpt_client, company_profile=company_profile,
        )
    elif fmt == "xml_upd":
        result = xml_handler.analyze_xml(
            file_bytes=file_bytes, filename=filename, chat_context=chat_context,
            gpt_client=gpt_client, company_profile=company_profile,
        )
    elif fmt.startswith("image_"):
        result = image_handler.analyze_image(
            file_bytes=file_bytes, filename=filename, mime_type=detected_mime,
            chat_context=chat_context, gpt_client=gpt_client, company_profile=company_profile,
        )
    elif fmt == "zip_ooxml":
        result = ooxml_handler.analyze_ooxml(
            file_bytes=file_bytes, filename=filename, chat_context=chat_context,
            gpt_client=gpt_client, company_profile=company_profile,
        )
    else:
        result = {
            "document_type": "unknown",
            "extracted_text": "",
            "structured_fields": {"detected_format": fmt},
            "summary": "",
            "confidence": 0.0,
            "errors": [f"Формат {fmt!r} не поддерживается"],
        }

    result.setdefault("detected_format", fmt)
    return result
