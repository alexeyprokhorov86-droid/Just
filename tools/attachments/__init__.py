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
import os
import tempfile

from pydantic import BaseModel, Field

from ..registry import tool
from ._detect import detect_format, mime_for_format
from .handlers import image_handler, ooxml_handler, pdf_handler, video_handler, xml_handler

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


# ----------------------------------------------------------------------------
# Video: отдельный путь (MTProto-скачивание + adaptive frame sampling).
#
# Не участвует в analyze_attachment_bytes потому что:
#   1. Видео может быть до 2 GB — не держим в RAM, работаем с путём к файлу.
#   2. Нужны дополнительные параметры (chat_id, message_id) для MTProto.
#   3. focus_query — специфично для RAG-сценария (личка с ботом, без chat_context).
# ----------------------------------------------------------------------------


class AnalyzeVideoInput(BaseModel):
    chat_id: int = Field(description="Telegram chat_id где лежит сообщение с видео (для супергрупп — с -100 префиксом).")
    message_id: int = Field(description="message_id сообщения-источника видео.")
    filename: str = Field(default="video.mp4", description="Имя файла для логов/prompt (опционально).")
    chat_context: str = Field(default="", description="Контекст обсуждения в чате. Для attachments-пути.")
    focus_query: str = Field(default="", description="Вопрос пользователя про видео. Для RAG-пути. Если задан — превалирует над chat_context.")


@tool(
    name="analyze_video",
    domain="attachments",
    description=(
        "Анализирует видео из Telegram (совещания, собеседования, демо) любого "
        "размера до 2 GB. Скачивает через MTProto user-client, извлекает "
        "транскрипт Whisper, делает adaptive frame sampling (scene-detection "
        "+ LLM-классификация static/mixed/dynamic + опциональный deep-scan) и "
        "возвращает summary. Вызывается из двух мест: (1) bot.download_and_"
        "analyze_media для ingestion чатов, (2) RAG-агент когда в личке "
        "присылают видео — в этом случае передавать focus_query вместо "
        "chat_context. Возвращает {document_type='video', extracted_text="
        "транскрипт, summary, structured_fields (duration/frames/density), "
        "errors}. Требует чтобы user 805598873 был в чате chat_id — иначе "
        "MtprotoUnavailable."
    ),
    input_model=AnalyzeVideoInput,
)
def analyze_video(
    chat_id: int,
    message_id: int,
    filename: str = "video.mp4",
    chat_context: str = "",
    focus_query: str = "",
) -> dict:
    """Sync-обёртка для совместимости с registry.invoke (тот синхронный).
    Внутри делает asyncio.run по новому event loop — годится для CLI/tests.
    Для bot.py используй `analyze_video_from_telegram` (async, работает в
    существующем loop'е)."""
    import asyncio
    return asyncio.run(
        analyze_video_from_telegram(
            chat_id=chat_id,
            message_id=message_id,
            filename=filename,
            chat_context=chat_context,
            focus_query=focus_query,
        )
    )


async def analyze_video_from_telegram(
    *,
    chat_id: int,
    message_id: int,
    filename: str = "video.mp4",
    chat_context: str = "",
    focus_query: str = "",
) -> dict:
    """Async entrypoint для bot.py и других уже-в-event-loop вызывов.

    Fallback-поведение (session нет / user не в чате) — возвращает dict с
    errors=[...] и пустым summary, НЕ бросает. bot.py сохранит запись,
    пользователь получит honest message что видео недоступно.
    """
    from ._mtproto import MtprotoUnavailable, download_from_telegram
    from company_context import get_company_profile
    company_profile = get_company_profile()
    from bot import gpt_client

    tmp_dir = tempfile.mkdtemp(prefix="video_dl_")
    dest_path = os.path.join(tmp_dir, filename if filename else "video.mp4")

    try:
        try:
            actual_path, size = await download_from_telegram(chat_id, message_id, dest_path)
        except MtprotoUnavailable as e:
            log.warning("video download skipped: %s", e)
            return {
                "document_type": "video",
                "extracted_text": "",
                "structured_fields": {"reason": "mtproto_unavailable"},
                "summary": "",
                "confidence": 0.0,
                "errors": [str(e)],
            }

        import asyncio
        result = await asyncio.to_thread(
            video_handler.analyze_video,
            video_path=actual_path,
            filename=filename,
            chat_context=chat_context,
            focus_query=focus_query,
            gpt_client=gpt_client,
            company_profile=company_profile,
        )
        result.setdefault("structured_fields", {})
        result["structured_fields"]["downloaded_bytes"] = size
        return result
    finally:
        import shutil as _shutil
        _shutil.rmtree(tmp_dir, ignore_errors=True)
