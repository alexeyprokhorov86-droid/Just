"""fix_flow — /fix conversation для членов чата Руководство.

Собирает текст + фото + видео + документы, складывает в /tmp/fix_queue/<id>.json.
Очередь обрабатывает worker в claude_tg_bridge (см. claude_tg_bridge/fix_worker.py):
запускает `claude -p` с full toolset, результат возвращает пользователю в DM
через main bot API (сам bot этот не отвечает по этой задаче).

Регистрация:
  from fix_flow import fix_conversation
  application.add_handler(fix_conversation())
"""
from __future__ import annotations

import json
import logging
import os
import pathlib
import shutil
import uuid
from datetime import datetime

from telegram import Update
from telegram.error import TelegramError
from telegram.ext import (
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logger = logging.getLogger(__name__)

# Чат "Руководство (bridged)" — единственный, чьи члены могут вызывать /fix.
# Можно переопределить через .env если переезд на другой чат.
FIX_AUTH_CHAT_ID = int(os.getenv("FIX_AUTH_CHAT_ID", "-1003596439983"))

QUEUE_DIR = pathlib.Path("/tmp/fix_queue")
SESSIONS_DIR = pathlib.Path("/tmp/fix_sessions")

# Bot API ограничение на скачивание — 20 MB.
MAX_FILE_BYTES = 20 * 1024 * 1024

COLLECTING = 1


async def _is_rukovodstvo_member(app, user_id: int) -> bool:
    try:
        member = await app.bot.get_chat_member(FIX_AUTH_CHAT_ID, user_id)
        return member.status in ("member", "administrator", "creator", "owner")
    except TelegramError as e:
        logger.warning("get_chat_member(%s, %s) failed: %s", FIX_AUTH_CHAT_ID, user_id, e)
        return False


def _new_session(user) -> dict:
    sid = str(uuid.uuid4())
    sess_dir = SESSIONS_DIR / sid
    sess_dir.mkdir(parents=True, exist_ok=True)
    return {
        "session_id": sid,
        "user_id": user.id,
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "session_dir": str(sess_dir),
        "texts": [],
        "photos": [],
        "videos": [],
        "documents": [],
    }


async def cmd_fix(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.effective_user:
        return ConversationHandler.END
    if update.effective_chat.type != "private":
        await update.message.reply_text("Команда /fix работает только в личке с ботом.")
        return ConversationHandler.END

    user = update.effective_user
    if not await _is_rukovodstvo_member(ctx.application, user.id):
        await update.message.reply_text("⛔ Доступ только для членов чата Руководство.")
        return ConversationHandler.END

    if "fix" in ctx.user_data:
        old = ctx.user_data["fix"]
        old_dir = pathlib.Path(old.get("session_dir", ""))
        if old_dir.exists():
            shutil.rmtree(old_dir, ignore_errors=True)
        await update.message.reply_text(
            f"⚠️ Прежняя /fix сессия ({old['session_id'][:8]}) сброшена — начинаем новую."
        )

    ctx.user_data["fix"] = _new_session(user)
    await update.message.reply_text(
        "🔧 *Fix session*\n\n"
        "Присылай текст, фото, видео и документы — что нужно исправить.\n"
        "Можно много сообщений подряд.\n\n"
        "/done — отправить задачу в Claude Code\n"
        "/cancel — отменить",
        parse_mode="Markdown",
    )
    return COLLECTING


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    sess = ctx.user_data.get("fix")
    if not sess:
        return ConversationHandler.END
    sess["texts"].append(update.message.text or "")
    await update.message.reply_text(f"📝 текст #{len(sess['texts'])}")
    return COLLECTING


async def on_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    sess = ctx.user_data.get("fix")
    if not sess:
        return ConversationHandler.END

    photo = update.message.photo[-1]
    if photo.file_size and photo.file_size > MAX_FILE_BYTES:
        await update.message.reply_text(
            f"⚠️ Фото >{MAX_FILE_BYTES // 1024 // 1024}МБ — Bot API не скачает. "
            f"Сожми или разрежь."
        )
        return COLLECTING

    file = await photo.get_file()
    n = len(sess["photos"]) + 1
    dest = pathlib.Path(sess["session_dir"]) / f"photo_{n:02d}.jpg"
    await file.download_to_drive(str(dest))
    sess["photos"].append(str(dest))
    size_kb = dest.stat().st_size // 1024
    await update.message.reply_text(f"🖼 фото #{n} ({size_kb} KB)")
    return COLLECTING


async def on_video(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    sess = ctx.user_data.get("fix")
    if not sess:
        return ConversationHandler.END

    video = update.message.video
    if video.file_size and video.file_size > MAX_FILE_BYTES:
        await update.message.reply_text(
            f"⚠️ Видео >{MAX_FILE_BYTES // 1024 // 1024}МБ — Bot API не скачает. "
            f"Опиши проблему текстом или разрежь видео."
        )
        return COLLECTING

    file = await video.get_file()
    n = len(sess["videos"]) + 1
    ext = "mp4"
    if video.mime_type:
        sub = video.mime_type.split("/")[-1].lower()
        ext = "mov" if sub == "quicktime" else (sub if sub.isalnum() else "mp4")
    dest = pathlib.Path(sess["session_dir"]) / f"video_{n:02d}.{ext}"
    await file.download_to_drive(str(dest))
    sess["videos"].append({
        "path": str(dest),
        "filename": dest.name,
        "duration": video.duration or 0,
        "mime_type": video.mime_type or "",
    })
    size_mb = dest.stat().st_size / 1024 / 1024
    await update.message.reply_text(f"🎬 видео #{n} ({size_mb:.1f} МБ, {video.duration or 0}s)")
    return COLLECTING


async def on_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    sess = ctx.user_data.get("fix")
    if not sess:
        return ConversationHandler.END

    doc = update.message.document
    if doc.file_size and doc.file_size > MAX_FILE_BYTES:
        await update.message.reply_text(
            f"⚠️ Файл >{MAX_FILE_BYTES // 1024 // 1024}МБ — пропустил."
        )
        return COLLECTING

    file = await doc.get_file()
    n = len(sess["documents"]) + 1
    safe_name = (doc.file_name or f"doc_{n}").replace("/", "_")[:80]
    dest = pathlib.Path(sess["session_dir"]) / f"doc_{n:02d}_{safe_name}"
    await file.download_to_drive(str(dest))
    sess["documents"].append({
        "path": str(dest),
        "filename": doc.file_name or safe_name,
        "mime_type": doc.mime_type or "",
    })
    size_kb = dest.stat().st_size // 1024
    await update.message.reply_text(f"📎 документ #{n}: {doc.file_name or safe_name} ({size_kb} KB)")
    return COLLECTING


async def cmd_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    sess = ctx.user_data.get("fix")
    if not sess:
        await update.message.reply_text("Нет активной /fix сессии.")
        return ConversationHandler.END

    if not (sess["texts"] or sess["photos"] or sess["videos"] or sess["documents"]):
        await update.message.reply_text(
            "⚠️ Ничего не приложено. Пришли хоть что-то или /cancel."
        )
        return COLLECTING

    sess["dm_chat_id"] = update.effective_chat.id
    sess["finalized_at"] = datetime.now().isoformat(timespec="seconds")
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    queue_file = QUEUE_DIR / f"{sess['session_id']}.json"
    queue_file.write_text(json.dumps(sess, ensure_ascii=False, indent=2))

    parts = []
    if sess["texts"]:     parts.append(f"{len(sess['texts'])} текст(ов)")
    if sess["photos"]:    parts.append(f"{len(sess['photos'])} фото")
    if sess["videos"]:    parts.append(f"{len(sess['videos'])} видео")
    if sess["documents"]: parts.append(f"{len(sess['documents'])} док.")

    await update.message.reply_text(
        f"🚀 Принято: {', '.join(parts)}\n"
        f"ID: `{sess['session_id'][:8]}`\n\n"
        f"Claude Code обрабатывает задачу — отвечу когда готово (5–15 мин).",
        parse_mode="Markdown",
    )
    logger.info(
        "fix queued: %s by user %s (texts=%d photos=%d videos=%d docs=%d)",
        sess["session_id"], sess["user_id"],
        len(sess["texts"]), len(sess["photos"]),
        len(sess["videos"]), len(sess["documents"]),
    )
    ctx.user_data.pop("fix", None)
    return ConversationHandler.END


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    sess = ctx.user_data.pop("fix", None)
    if not sess:
        await update.message.reply_text("Нет активной /fix сессии.")
        return ConversationHandler.END

    sess_dir = pathlib.Path(sess.get("session_dir", ""))
    if sess_dir.exists():
        shutil.rmtree(sess_dir, ignore_errors=True)
    await update.message.reply_text(f"❌ /fix сессия {sess['session_id'][:8]} отменена.")
    return ConversationHandler.END


def fix_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CommandHandler("fix", cmd_fix)],
        states={
            COLLECTING: [
                CommandHandler("done", cmd_done),
                CommandHandler("cancel", cmd_cancel),
                MessageHandler(filters.PHOTO, on_photo),
                MessageHandler(filters.VIDEO, on_video),
                MessageHandler(filters.Document.ALL, on_document),
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_text),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        per_message=False,
        name="fix_session",
    )
