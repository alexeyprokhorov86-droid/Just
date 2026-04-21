"""Image handler — Vision-анализ + OCR для content_text."""
from __future__ import annotations

import base64
import logging

from .._prompts import build_analysis_prompt

log = logging.getLogger("tools.attachments.image")


def _ocr(file_bytes: bytes, media_type: str, gpt_client, model: str) -> str:
    if not gpt_client:
        return ""
    b64 = base64.standard_b64encode(file_bytes).decode()
    try:
        response = gpt_client.chat.completions.create(
            model=model,
            max_tokens=4000,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{b64}"}},
                        {
                            "type": "text",
                            "text": (
                                "Извлеки весь текст с изображения дословно. Таблицы — "
                                "строками через ' | '. Если текста нет — верни пустую "
                                "строку. Без комментариев от себя."
                            ),
                        },
                    ],
                }
            ],
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        log.warning(f"Image OCR failed: {e}")
        return ""


def analyze_image(
    *,
    file_bytes: bytes,
    filename: str,
    mime_type: str,
    chat_context: str,
    gpt_client,
    company_profile: str,
    model: str = "openai/gpt-4.1",
) -> dict:
    errors: list[str] = []
    extracted_text = _ocr(file_bytes, mime_type or "image/jpeg", gpt_client, model)

    summary = ""
    if gpt_client is not None:
        b64 = base64.standard_b64encode(file_bytes).decode()
        prompt = build_analysis_prompt(
            company_profile=company_profile,
            doc_type="Изображение",
            doc_content="[Vision]",
            chat_context=chat_context,
            filename=filename,
        )
        try:
            response = gpt_client.chat.completions.create(
                model=model,
                max_tokens=1500,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": f"data:{mime_type or 'image/jpeg'};base64,{b64}"}},
                            {"type": "text", "text": prompt},
                        ],
                    }
                ],
            )
            summary = (response.choices[0].message.content or "").strip()
        except Exception as e:
            errors.append(f"LLM analysis failed: {e}")

    return {
        "document_type": "image",
        "extracted_text": extracted_text,
        "structured_fields": {"mime_type": mime_type},
        "summary": summary,
        "confidence": 1.0 if summary and not errors else 0.0,
        "errors": errors,
    }
