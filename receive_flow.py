"""
receive_flow — Telegram-флоу приёмки УПД.

ConversationHandler на 3 состояния:
  WAITING_PHOTOS  → кладовщик грузит 1+ фото/PDF УПД
  PROCESSING      → идёт OCR (новые фото игнорятся)
  SHOWN           → показали результат, ждём «Далее» или «Переснять»

Фаза 1 (эта): останавливаемся на SHOWN — показали распознанный УПД
и предупреждения. Создание ПТУ — Фаза 2+.

Регистрация:
  from receive_flow import receive_conversation
  application.add_handler(receive_conversation())
"""
from __future__ import annotations

import io
import logging
import os

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode, ChatAction
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from tools.vision_upd import (
    extract_upd,
    validate_upd,
    format_extract_for_tg,
    UpdExtractResult,
    UpdWarning,
)

logger = logging.getLogger("receive_flow")

# States
WAITING_PHOTOS, PROCESSING, SHOWN = range(3)

# Admin-only на Фазу 1. В Фазе 2+ расширим на всех сотрудников склада
# (через c1_staff_history и роли).
_ADMIN_ID = int(os.getenv("ADMIN_USER_ID", "805598873"))


def _is_authorized(user_id: int) -> bool:
    # MVP: только админ. TODO Фаза 2: список user_id кладовщиков
    return user_id == _ADMIN_ID


async def cmd_receive(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not user or not _is_authorized(user.id):
        await update.message.reply_text(
            "Команда доступна только сотрудникам склада. Обратитесь к админу."
        )
        return ConversationHandler.END

    ctx.user_data["upd_photos"] = []
    ctx.user_data["upd_result"] = None
    ctx.user_data["upd_warnings"] = None

    await update.message.reply_text(
        "📸 <b>Приёмка товара — сканирование УПД</b>\n\n"
        "Пришлите фото УПД (универсальный передаточный документ).\n"
        "Можно несколько фото — они будут склеены в один документ.\n\n"
        "Когда закончите — нажмите <b>«Готово, распознать»</b>.\n"
        "Отмена: /cancel",
        parse_mode=ParseMode.HTML,
    )
    return WAITING_PHOTOS


def _keyboard_done(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Готово, распознать ({count} фото)", callback_data="upd_done")],
        [InlineKeyboardButton("❌ Отменить", callback_data="upd_cancel")],
    ])


async def on_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not _is_authorized(update.effective_user.id):
        return ConversationHandler.END

    msg = update.message
    photos: list[bytes] = ctx.user_data.setdefault("upd_photos", [])

    # Извлекаем file_id из photo (массив размеров) или document
    file_id = None
    if msg.photo:
        file_id = msg.photo[-1].file_id  # максимальный размер
    elif msg.document and (
        msg.document.mime_type in ("application/pdf", "image/jpeg", "image/png")
        or (msg.document.file_name or "").lower().endswith((".pdf", ".jpg", ".jpeg", ".png"))
    ):
        file_id = msg.document.file_id
    else:
        await msg.reply_text("Это не похоже на фото/PDF УПД. Пришлите изображение или PDF.")
        return WAITING_PHOTOS

    try:
        tg_file = await ctx.bot.get_file(file_id)
        buf = io.BytesIO()
        await tg_file.download_to_memory(buf)
        data = buf.getvalue()
    except Exception as e:
        logger.exception("download file failed: %s", e)
        await msg.reply_text(f"Не удалось скачать файл: {e}")
        return WAITING_PHOTOS

    photos.append(data)
    logger.info("received %d bytes, total %d photos", len(data), len(photos))

    await msg.reply_text(
        f"📥 Принято фото {len(photos)}. "
        "Пришлите ещё или нажмите «Готово».",
        reply_markup=_keyboard_done(len(photos)),
    )
    return WAITING_PHOTOS


async def on_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    photos: list[bytes] = ctx.user_data.get("upd_photos", [])
    if not photos:
        await query.edit_message_text("Нет фото для распознавания. Пришлите фото или /cancel.")
        return WAITING_PHOTOS

    await query.edit_message_text(
        f"🧠 Распознаю {len(photos)} фото через Claude Vision… (~10-30 сек)"
    )

    try:
        await query.message.chat.send_action(ChatAction.TYPING)
    except Exception:
        pass

    try:
        result: UpdExtractResult = extract_upd(photos)
    except Exception as e:
        logger.exception("extract_upd failed: %s", e)
        await query.message.reply_text(
            f"❌ Ошибка распознавания: {e}\n\n"
            "Попробуйте более чёткие фото или /cancel."
        )
        return WAITING_PHOTOS

    warnings: list[UpdWarning] = validate_upd(result)
    ctx.user_data["upd_result"] = result.model_dump()
    ctx.user_data["upd_warnings"] = [w.model_dump() for w in warnings]

    text = format_extract_for_tg(result, warnings)
    blocker = any(w.level == "error" for w in warnings)

    if blocker:
        # В Фазе 1 просто показываем и останавливаемся. В Фазе 5 → алерт в Закупки
        # и ожидание ответа ответственных.
        buttons = [
            [InlineKeyboardButton("🔄 Переснять", callback_data="upd_redo")],
            [InlineKeyboardButton("❌ Завершить", callback_data="upd_cancel")],
        ]
    else:
        buttons = [
            [InlineKeyboardButton("➡️ Далее: подобрать заказ", callback_data="upd_match")],
            [InlineKeyboardButton("🔄 Переснять", callback_data="upd_redo")],
            [InlineKeyboardButton("❌ Отмена", callback_data="upd_cancel")],
        ]
    await query.message.reply_text(
        text, parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return SHOWN


async def on_redo(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer("Начинаем заново")
    ctx.user_data["upd_photos"] = []
    ctx.user_data["upd_result"] = None
    ctx.user_data["upd_warnings"] = None
    await query.message.reply_text(
        "📸 Пришлите фото УПД заново. /cancel — отменить.",
    )
    return WAITING_PHOTOS


async def on_match(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Фаза 1 заглушка. Реальный матчинг — Фаза 2."""
    query = update.callback_query
    await query.answer()
    result_dict = ctx.user_data.get("upd_result") or {}
    supplier = (result_dict.get("supplier") or {}).get("name") or "?"
    amount = (result_dict.get("document") or {}).get("total_amount")
    await query.message.reply_text(
        "🚧 <b>Фаза 1 MVP</b>: распознавание работает, создание ПТУ — в Фазе 2+.\n\n"
        f"Сохранено в user_data:\n"
        f"• Поставщик: {supplier}\n"
        f"• Сумма: {amount}\n"
        f"• Позиций: {len(result_dict.get('items') or [])}\n\n"
        "Для новой приёмки: /receive",
        parse_mode=ParseMode.HTML,
    )
    return ConversationHandler.END


async def on_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if q:
        await q.answer()
        await q.message.reply_text("Отменено. Для новой приёмки — /receive.")
    elif update.message:
        await update.message.reply_text("Отменено. Для новой приёмки — /receive.")
    ctx.user_data.pop("upd_photos", None)
    ctx.user_data.pop("upd_result", None)
    ctx.user_data.pop("upd_warnings", None)
    return ConversationHandler.END


def receive_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CommandHandler("receive", cmd_receive)],
        states={
            WAITING_PHOTOS: [
                MessageHandler(
                    (filters.PHOTO | filters.Document.IMAGE | filters.Document.PDF),
                    on_photo,
                ),
                CallbackQueryHandler(on_done,   pattern=r"^upd_done$"),
                CallbackQueryHandler(on_cancel, pattern=r"^upd_cancel$"),
            ],
            SHOWN: [
                CallbackQueryHandler(on_match,  pattern=r"^upd_match$"),
                CallbackQueryHandler(on_redo,   pattern=r"^upd_redo$"),
                CallbackQueryHandler(on_cancel, pattern=r"^upd_cancel$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", on_cancel)],
        per_message=False,
        name="receive_upd",
    )
