"""Claude Code Bridge bot — hook-mode + /ask (headless claude -p).

Два режима в одном боте:
  1. Hook-mode: long-poll, /status /pause /resume для управления трансляцией
     действий Claude из SSH-сессии (hooks → hook_bridge.py).
  2. /ask mode: любой текст от админа → subprocess `claude -p <prompt>` →
     ответ в TG. Headless, stateless (каждый раз новая сессия). Идёт через
     Claude Max подписку (CLAUDE_CODE_OAUTH_TOKEN), не API.
"""

import asyncio
import json
import logging
import os
import pathlib
import uuid
from datetime import datetime

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
SESSIONS_FILE = pathlib.Path(__file__).resolve().parent / "sessions.json"

# Timeout одной /ask-сессии. Claude может делать многошаговые операции
# (читать несколько файлов, запускать Bash, коммитить) — 15 мин с запасом.
ASK_TIMEOUT_SEC = 900

# Prompt-обёртка: просим прочитать контекст (CLAUDE.md Claude Code тянет сам,
# но последние session-файлы — нет) и действовать без уточнений.
ASK_PROMPT_PREFIX = """Ты вызван из Telegram-бота — пользователь задал вопрос ниже.

Перед ответом прочитай для контекста:
1. `CLAUDE.md` в корне проекта (project instructions)
2. Последние 3 файла в `logs/sessions/` (по mtime) — что было в недавних сессиях

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
        "и Claude проснётся как headless-агент. Есть память между вопросами "
        "(через --resume session_id). Первый вопрос создаёт сессию "
        "(читает CLAUDE.md + последние 3 session-файла), следующие её продолжают.\n\n"
        "*Команды:*\n"
        "/new — начать новый разговор (сбросить память)\n"
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


# ─── Session persistence (stateful chat через --session-id/--resume) ────

def _load_sessions() -> dict:
    if not SESSIONS_FILE.exists():
        return {}
    try:
        return json.loads(SESSIONS_FILE.read_text())
    except Exception as e:
        logger.warning("sessions.json parse error: %s — resetting", e)
        return {}


def _save_sessions(data: dict) -> None:
    SESSIONS_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def _get_or_create_session(user_id: int) -> tuple[str, bool]:
    """Возвращает (session_id, is_new). is_new=True если только что создали."""
    data = _load_sessions()
    key = str(user_id)
    now = datetime.now().isoformat(timespec="seconds")
    if key in data and data[key].get("session_id"):
        data[key]["last_used"] = now
        _save_sessions(data)
        return data[key]["session_id"], False
    session_id = str(uuid.uuid4())
    data[key] = {"session_id": session_id, "created_at": now, "last_used": now}
    _save_sessions(data)
    return session_id, True


def _reset_session(user_id: int) -> str | None:
    """Сбрасывает session_id для user_id. Возвращает прежний id (или None)."""
    data = _load_sessions()
    key = str(user_id)
    if key not in data:
        return None
    prev = data[key].get("session_id")
    del data[key]
    _save_sessions(data)
    return prev


async def _run_claude(prompt: str, session_id: str, is_new: bool) -> tuple[int, str, str]:
    """Запускает `claude -p <prompt>` асинхронно. Возвращает (rc, stdout, stderr).

    is_new=True → --session-id (создать сессию с этим UUID).
    is_new=False → --resume (продолжить существующую).
    """
    # При продолжении сессии Claude уже прочитал CLAUDE.md / sessions — не дублируем
    # обёртку, просто передаём вопрос. При новой сессии — полная инструкция.
    full_prompt = ASK_PROMPT_PREFIX + prompt if is_new else prompt
    session_flag = "--session-id" if is_new else "--resume"
    proc = await asyncio.create_subprocess_exec(
        CLAUDE_BIN,
        "-p",
        full_prompt,
        session_flag,
        session_id,
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

    session_id, is_new = _get_or_create_session(update.effective_user.id)
    marker = "🆕" if is_new else "🧠"
    thinking = await update.message.reply_text(f"{marker} думаю…")
    try:
        await update.message.chat.send_action(ChatAction.TYPING)
    except Exception:
        pass

    logger.info("ask: session=%s new=%s prompt=%r (%d chars)",
                session_id[:8], is_new, prompt[:80], len(prompt))
    rc, stdout, stderr = await _run_claude(prompt, session_id, is_new)
    logger.info("ask done: rc=%s stdout=%d stderr=%d", rc, len(stdout), len(stderr))

    try:
        await thinking.delete()
    except Exception:
        pass

    # Если --resume упал (сессия исчезла / уголовная ошибка), пробуем с новой
    if rc != 0 and not is_new:
        logger.warning("resume failed (rc=%s) — retrying with fresh session", rc)
        _reset_session(update.effective_user.id)
        session_id, is_new = _get_or_create_session(update.effective_user.id)
        rc, stdout, stderr = await _run_claude(prompt, session_id, is_new)
        logger.info("ask retry: rc=%s stdout=%d", rc, len(stdout))

    if rc != 0:
        err = (stderr or stdout or "empty output")[-3500:]
        await update.message.reply_text(f"❌ claude rc={rc}\n\n{err}")
        return

    answer = stdout.strip() or "(пустой ответ)"
    for i, part in enumerate(_chunks(answer)):
        await update.message.reply_text(part)


async def cmd_new(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Сбросить память — следующий вопрос создаст свежую сессию."""
    if not _is_admin(update):
        return
    prev = _reset_session(update.effective_user.id)
    if prev:
        await update.message.reply_text(
            f"🔄 сессия сброшена (была: `{prev[:8]}…`)\n"
            f"Следующее сообщение — новый разговор с нуля.",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await update.message.reply_text("нет активной сессии — следующее сообщение и так создаст новую")


async def _post_init(app: Application) -> None:
    """После инициализации бота — поднять fix_worker на фоне.

    Worker читает /tmp/fix_queue/*.json (от main bot fix_flow), запускает
    `claude -p` и шлёт ответ пользователю в DM через main bot Bot API.
    """
    main_bot_token = os.getenv("BOT_TOKEN", "")
    if not main_bot_token:
        logger.warning("BOT_TOKEN не задан — /fix worker не сможет отвечать пользователям, не запускаю")
        return
    from claude_tg_bridge.fix_worker import fix_worker_loop
    asyncio.create_task(fix_worker_loop(main_bot_token))
    logger.info("fix_worker spawned")


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
           .post_init(_post_init)
           .build())

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("new", cmd_new))
    # /ask — любой не-командный текст от админа
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("Claude Code Bridge bot started (hook-mode + /ask + fix_worker)")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
