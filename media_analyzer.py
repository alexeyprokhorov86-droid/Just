"""Media analysis API — единая точка для анализа вложений (фото/PDF/XLSX/Word/PPTX/audio/video).

Использование: импортировать функции отсюда вместо прямого импорта из bot.py.
Применяется в:
- bot.py (рантайм Telegram-бота, обрабатывает сообщения live)
- analyze_tg_media_backlog.py (батч-обработка backlog медиа из БД)
- analyze_attachments.py (email-вложения)

Сейчас это facade — реальные реализации в bot.py. Followup: физически
переместить код сюда, в bot.py оставить только хэндлеры Telegram.

Все функции async кроме `has_meaningful_text` (pre-filter для OCR-режима).
"""

import base64
import logging

# Re-exports из bot.py (lazy import чтобы не тянуть bot side-effects при простом import media_analyzer)
from bot import (
    # Контекст и промпт
    get_full_chat_context,
    build_analysis_prompt,
    # Извлечение сырого текста
    extract_text_from_image,
    extract_text_from_pdf,
    extract_text_from_word,
    extract_csv_from_excel,
    extract_text_from_pptx,
    extract_transcript_from_audio,
    # LLM-анализ
    analyze_image_with_gpt,
    analyze_pdf_with_gpt,
    analyze_excel_with_gpt,
    analyze_word_with_gpt,
    analyze_pptx_with_gpt,
    analyze_video_with_gemini,
    analyze_video_with_whisper,
    # LLM-клиент (нужен для has_meaningful_text)
    gpt_client,
)

logger = logging.getLogger(__name__)


async def has_meaningful_text(image_data: bytes, media_type: str = "image/jpeg") -> bool:
    """Cheap pre-filter: содержит ли изображение значимый текст?

    Использует gpt-4.1-mini с коротким yes/no промптом. ~$0.005 за чек.
    Применяется в analyze_tg_media_backlog для чатов где per-photo анализ
    включается выборочно (Торты Отгрузки): фото без текста (просто паллеты,
    кузов) пропускаются, фото с текстом (накладная, ТТН, маркировка,
    рукописная пометка) уходят в полный анализ.

    Returns: True если на изображении есть значимый текст.
    На ошибке/no client → True (не блокируем дальнейший pipeline).
    """
    if not gpt_client:
        return True

    try:
        b64 = base64.standard_b64encode(image_data).decode('utf-8')
        resp = gpt_client.chat.completions.create(
            model="openai/gpt-4.1-mini",
            max_tokens=16,  # gpt-4.1-mini требует >=16
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{media_type};base64,{b64}"}
                    },
                    {
                        "type": "text",
                        "text": (
                            "Есть ли на этом изображении значимый текст "
                            "(накладная, ТТН, таблица, маркировка, подпись, "
                            "рукописная пометка, ценник, документ)? "
                            "Ответь одним словом: yes или no."
                        )
                    }
                ],
            }],
        )
        answer = resp.choices[0].message.content.strip().lower()
        return answer.startswith('yes')
    except Exception as e:
        logger.warning(f"has_meaningful_text fallback to True: {e}")
        return True
