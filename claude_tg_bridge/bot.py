"""Claude Code Bridge bot — hook-mode, только transport/control.

Единственная роль: long-poll, команды /status /pause /resume для управления
трансляцией из ~/.claude/settings.json hooks → hook_bridge.py. Сам ничего
не отправляет в TG и не вызывает claude — всё делает hook_bridge.py.
"""

import logging
import os
import pathlib

from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest

_REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(_REPO / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("claude_bridge.bot")

BOT_TOKEN = os.environ["CLAUDE_BRIDGE_BOT_TOKEN"]
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", "0"))

PAUSE_FILE = pathlib.Path("/tmp/claude_hook_paused")


def _is_admin(update: Update) -> bool:
    return update.effective_user and update.effective_user.id == ADMIN_ID


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        return
    paused = "paused ⏸" if PAUSE_FILE.exists() else "active ✅"
    text = (
        "👁 *Claude Code Bridge (hook mode)*\n\n"
        f"status: {paused}\n\n"
        "Транслирую живые действия Claude Code из терминала:\n"
        "• 🎤 твои промпты\n"
        "• 💬 текст ассистента (между тулами и финальный ответ)\n"
        "• 🔹 Read/Grep/Glob и пр. (тихий лог)\n"
        "• 📤 stdout Bash\n"
        "• ⏰ напоминалка если approve в терминале висит >5 мин\n\n"
        "Approve делается в терминале — отсюда в stdin не проинжектишь.\n\n"
        "Команды:\n"
        "/status — состояние\n"
        "/pause — выключить трансляцию (хуки тихо пропускают)\n"
        "/resume — включить обратно"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        return
    paused = "paused" if PAUSE_FILE.exists() else "active"
    await update.message.reply_text(f"status: {paused}")


async def cmd_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        return
    PAUSE_FILE.touch()
    await update.message.reply_text("⏸ transliteration paused")


async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        return
    try:
        PAUSE_FILE.unlink()
    except FileNotFoundError:
        pass
    await update.message.reply_text("▶️ resumed")


def main():
    proxy = os.getenv("PROXY_URL")
    req_kwargs = dict(read_timeout=120, write_timeout=120, connect_timeout=30)
    if proxy:
        req_kwargs["proxy"] = proxy

    request = HTTPXRequest(**req_kwargs)
    get_updates_request = HTTPXRequest(**req_kwargs)

    app = (Application.builder()
           .token(BOT_TOKEN)
           .request(request)
           .get_updates_request(get_updates_request)
           .build())

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))

    logger.info("Claude Code Bridge bot (hook mode) started")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
