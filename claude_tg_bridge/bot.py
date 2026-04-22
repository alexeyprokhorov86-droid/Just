"""Claude Code Bridge bot — hook-mode + /ask (headless claude -p).

Два режима в одном боте:
  1. Hook-mode: long-poll, /status /pause /resume для управления трансляцией
     действий Claude из SSH-сессии (hooks → hook_bridge.py).
  2. /ask mode: любой текст от админа → subprocess `claude -p <prompt>` →
     ответ в TG. Headless, stateless (каждый раз новая сессия). Идёт через
     Claude Max подписку (CLAUDE_CODE_OAUTH_TOKEN), не API.
"""

import asyncio
import logging
import os
import pathlib
import shlex

from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
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
CLAUDE_OAUTH_TOKEN = os.getenv("CLAUDE_CODE_OAUTH_TOKEN", "")
CLAUDE_BIN = "/home/admin/.local/bin/claude"
REPO_DIR = str(_REPO)

PAUSE_FILE = pathlib.Path("/tmp/claude_hook_paused")

# Timeout одной /ask-сессии. Claude может делать многошаговые операции
# (читать несколько файлов, запускать Bash, коммитить) — 15 мин с запасом.
ASK_TIMEOUT_SEC = 900

# Prompt-обёртка: просим прочитать контекст (CLAUDE.md Claude Code тянет сам,
# но последние session-файлы — нет) и действовать без уточнений.
ASK_PROMPT_PREFIX = """Ты вызван из Telegram-бота — пользователь задал вопрос ниже.

Перед ответом прочитай для контекста:
1. `CLAUDE.md` в корне проекта (project instructions)
2. Последние 3 файла в `.claude/sessions/` (по mtime) — что было в недавних сессиях

Правила работы:
- Ты в режиме acceptEdits. Полный доступ: Read/Grep/Glob/Bash/Edit/Write/Task.
- Действуй самостоятельно, не задавай уточняющих вопросов. Если не уверен —
  выбери лучший вариант и упомяни альтернативы в конце.
- Пользователь читает с телефона: отвечай кратко, по делу. Без воды.

Вопрос пользователя:
"""


def _is_admin(update: Update) -> bool:
    return update.effective_user and update.effective_user.id == ADMIN_ID


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        return
    paused = "paused ⏸" if PAUSE_FILE.exists() else "active ✅"
    text = (
        "👁 *Claude Code Bridge*\n\n"
        f"hook-status: {paused}\n\n"
        "*Режимы:*\n\n"
        "1. *Hook-трансляция* — живые действия Claude из SSH-сессии:\n"
        "   • 🎤 твои промпты\n"
        "   • 💬 текст ассистента\n"
        "   • 🔹 Read/Grep/Glob и пр.\n"
        "   • 📤 stdout Bash\n"
        "   • ⏰ напоминалка если approve висит >5 мин\n\n"
        "2. */ask через TG* — просто напиши сообщение (не команду), "
        "и Claude проснётся как headless-агент. Stateless, каждый раз новая "
        "сессия, но читает CLAUDE.md + последние 3 session-файла.\n\n"
        "*Команды:*\n"
        "/status — состояние hooks\n"
        "/pause /resume — выключить/включить hook-трансляцию\n"
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


# ─── /ask headless Claude ────────────────────────────────────────────────


def _build_ask_env() -> dict:
    """env для claude -p: OAuth token (подписка) + HTTPS_PROXY (обход гео-блока).

    Снимаем CLAUDECODE/CLAUDE_CODE_*: иначе дочерний claude пытается подключиться
    к parent-сессии и получает 403. ANTHROPIC_API_KEY тоже снимаем, чтобы точно
    шло через OAuth (Max-подписка), а не API billing.
    """
    env = os.environ.copy()
    for var in (
        "CLAUDECODE",
        "CLAUDE_CODE_SSE_PORT",
        "CLAUDE_CODE_ENTRYPOINT",
        "CLAUDE_CODE_EXECPATH",
        "ANTHROPIC_API_KEY",
    ):
        env.pop(var, None)
    env["CLAUDE_CODE_OAUTH_TOKEN"] = CLAUDE_OAUTH_TOKEN
    env["HTTPS_PROXY"] = "http://127.0.0.1:8118"
    env["HTTP_PROXY"] = "http://127.0.0.1:8118"
    return env


async def _run_claude(prompt: str) -> tuple[int, str, str]:
    """Запускает `claude -p <prompt>` асинхронно. Возвращает (rc, stdout, stderr)."""
    full_prompt = ASK_PROMPT_PREFIX + prompt
    proc = await asyncio.create_subprocess_exec(
        CLAUDE_BIN,
        "-p",
        full_prompt,
        "--permission-mode",
        "acceptEdits",
        "--allowedTools",
        "Bash,Edit,Read,Write,Grep,Glob,Task",
        cwd=REPO_DIR,
        env=_build_ask_env(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=ASK_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return -1, "", f"timeout после {ASK_TIMEOUT_SEC}s"
    return proc.returncode, stdout.decode("utf-8", "replace"), stderr.decode(
        "utf-8", "replace"
    )


def _chunks(text: str, size: int = 3800) -> list[str]:
    """Режем длинный ответ на куски ≤ size (TG limit 4096). Граница — \\n\\n."""
    if len(text) <= size:
        return [text]
    parts: list[str] = []
    rest = text
    while len(rest) > size:
        cut = rest.rfind("\n\n", 0, size)
        if cut <= 0:
            cut = rest.rfind("\n", 0, size)
        if cut <= 0:
            cut = size
        parts.append(rest[:cut])
        rest = rest[cut:].lstrip("\n")
    if rest:
        parts.append(rest)
    return parts


async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Любое текстовое сообщение от admin → headless claude -p."""
    if not _is_admin(update):
        return
    if not update.message or not update.message.text:
        return
    if not CLAUDE_OAUTH_TOKEN:
        await update.message.reply_text("❌ CLAUDE_CODE_OAUTH_TOKEN не задан в .env")
        return

    prompt = update.message.text.strip()
    if not prompt:
        return

    thinking = await update.message.reply_text("🧠 думаю…")
    try:
        await update.message.chat.send_action(ChatAction.TYPING)
    except Exception:
        pass

    logger.info("ask: %r (%d chars)", prompt[:80], len(prompt))
    rc, stdout, stderr = await _run_claude(prompt)
    logger.info("ask done: rc=%s stdout=%d stderr=%d", rc, len(stdout), len(stderr))

    try:
        await thinking.delete()
    except Exception:
        pass

    if rc != 0:
        err = (stderr or stdout or "empty output")[-3500:]
        await update.message.reply_text(f"❌ claude rc={rc}\n\n{err}")
        return

    answer = stdout.strip() or "(пустой ответ)"
    for i, part in enumerate(_chunks(answer)):
        await update.message.reply_text(part)


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
    # /ask — любой не-командный текст от админа
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("Claude Code Bridge bot started (hook-mode + /ask)")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
