#!/home/admin/telegram_logger_bot/venv/bin/python3
"""Claude Code hook bridge → Telegram (variant 2a).

Режим: терминал рулит approve'ами сам (стандартный permission-flow
Claude Code). Хук только:
  - пересылает в TG UserPromptSubmit / не-блокирующие tools (Read/Grep/…)
    и PostToolUse stdout для Bash
  - для блокирующих tools (Bash/Write/Edit/NotebookEdit) форкает фоновый
    "nag" процесс: через 5 минут, если approve не был дан в терминале
    (нет PostToolUse-маркера), шлёт в TG напоминалку.

Никаких approve-кнопок в TG: из хука ответ в терминальный stdin не
проинжектить, поэтому кнопки честно бесполезны — оставляем пустое
уведомление как "вернись к терминалу".
"""

import hashlib
import json
import logging
import os
import pathlib
import subprocess
import sys
import time
import urllib.request
import uuid
from urllib.parse import quote

from dotenv import load_dotenv
load_dotenv("/home/admin/telegram_logger_bot/.env")

BOT_TOKEN = os.getenv("CLAUDE_BRIDGE_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", "0"))
PROXY = "http://127.0.0.1:8118"

BLOCKING_TOOLS = {"Bash", "Write", "Edit", "NotebookEdit"}
NAG_TIMEOUT_SEC = 300  # 5 минут — после этого шлём TG-напоминалку

DONE_DIR = pathlib.Path("/tmp/claude_hook_done")
PAUSE_FILE = pathlib.Path("/tmp/claude_hook_paused")
OFFSET_DIR = pathlib.Path("/tmp/claude_hook_offsets")
DONE_DIR.mkdir(parents=True, exist_ok=True)
OFFSET_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = "/home/admin/telegram_logger_bot/claude_hook_bridge.log"
logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("hook_bridge")


# ─── Telegram Bot API (urllib + Privoxy) ──────────────────────────────

def _tg_request(method: str, payload: dict) -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode()
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"https": PROXY, "http": PROXY})
    )
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with opener.open(req, timeout=30) as resp:
        return json.loads(resp.read())


def tg_send(text: str, parse_mode: str = "Markdown"):
    if not BOT_TOKEN or not ADMIN_ID:
        logger.warning("TG token/admin_id missing, skip send")
        return None
    payload = {"chat_id": ADMIN_ID, "text": text[:4000]}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        return _tg_request("sendMessage", payload)
    except Exception as e:
        logger.warning(f"tg_send markdown failed: {e}; retry plain")
        payload.pop("parse_mode", None)
        try:
            return _tg_request("sendMessage", payload)
        except Exception as e2:
            logger.error(f"tg_send plain failed: {e2}")
            return None


# ─── formatters ───────────────────────────────────────────────────────

def fmt_tool_brief(tool_name: str, tool_input: dict) -> str:
    if tool_name == "Read":
        return str(tool_input.get("file_path", ""))[:200]
    if tool_name in ("Grep", "Glob"):
        return str(tool_input.get("pattern", ""))[:200]
    if tool_name in ("Edit", "Write"):
        return str(tool_input.get("file_path", ""))[:200]
    if tool_name == "Bash":
        desc = tool_input.get("description") or ""
        cmd = tool_input.get("command") or ""
        return (f"{desc} — " if desc else "") + cmd[:300]
    if tool_name == "WebFetch":
        return str(tool_input.get("url", ""))[:200]
    if tool_name == "WebSearch":
        return str(tool_input.get("query", ""))[:200]
    if tool_name == "TodoWrite":
        return f"{len(tool_input.get('todos', []))} tasks"
    return str(tool_input)[:200]


def _tool_key(payload: dict) -> str:
    """Stable id для корреляции Pre ↔ Post-ToolUse."""
    tuid = payload.get("tool_use_id")
    if tuid:
        return str(tuid)
    sid = str(payload.get("session_id", ""))
    ti = json.dumps(payload.get("tool_input") or {}, sort_keys=True)
    return hashlib.sha1((sid + ti).encode()).hexdigest()[:16]


# ─── transcript reader: assistant text-блоки → TG ────────────────────

def _offset_file(session_id: str) -> pathlib.Path:
    sid = session_id or "default"
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in sid)[:64]
    return OFFSET_DIR / f"{safe}.txt"


def _read_offset(session_id: str) -> int:
    try:
        return int(_offset_file(session_id).read_text().strip())
    except Exception:
        return 0


def _write_offset(session_id: str, offset: int):
    try:
        _offset_file(session_id).write_text(str(offset))
    except Exception as e:
        logger.warning(f"offset write failed: {e}")


def _chunk_send(prefix: str, text: str, chunk_sz: int = 3800):
    """Длинные тексты режем на куски ≤ chunk_sz, первый с prefix."""
    text = text.strip()
    if not text:
        return
    first = True
    for i in range(0, len(text), chunk_sz):
        part = text[i:i + chunk_sz]
        tg_send(f"{prefix}{part}" if first else part)
        first = False


def flush_assistant_texts(transcript_path: str, session_id: str):
    """Дочитывает transcript JSONL с сохранённого оффсета, шлёт в TG все
    новые assistant text-блоки (и thinking если есть)."""
    if not transcript_path:
        return
    tp = pathlib.Path(transcript_path)
    if not tp.exists():
        return
    # на первом вызове в этой сессии отсекаем всю историю до последнего
    # user-сообщения — чтобы не флудить старой перепиской, но показать
    # текущий ответ ассистента полностью
    if not _offset_file(session_id).exists():
        try:
            data = tp.read_bytes()
        except Exception as e:
            logger.warning(f"transcript initial read failed: {e}")
            return
        cum = 0
        last_user_end = 0
        for line in data.splitlines(keepends=True):
            cum += len(line)
            try:
                entry = json.loads(line.strip())
            except Exception:
                continue
            msg = entry.get("message") if isinstance(entry.get("message"), dict) else entry
            if msg.get("role") == "user":
                last_user_end = cum
        _write_offset(session_id, last_user_end)
    offset = _read_offset(session_id)
    try:
        with tp.open("rb") as f:
            f.seek(offset)
            new_data = f.read()
            new_offset = f.tell()
    except Exception as e:
        logger.warning(f"transcript read failed: {e}")
        return
    if not new_data:
        return

    for line in new_data.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except Exception:
            continue
        # формат Claude Code transcript: либо {"role":...} либо
        # {"type":"assistant","message":{"role":"assistant","content":[...]}}
        msg = entry.get("message") if isinstance(entry.get("message"), dict) else entry
        if msg.get("role") != "assistant":
            continue
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype == "text":
                text = (block.get("text") or "").strip()
                if text:
                    _chunk_send("💬 ", text)
            elif btype == "thinking":
                think = (block.get("thinking") or "").strip()
                if think:
                    _chunk_send("🧠 _thinking:_\n", think)

    _write_offset(session_id, new_offset)


# ─── nag: фоновый "пни в TG через 5 мин если не ответил" ──────────────

def spawn_nag(tool_use_id: str, tool_name: str, tool_input: dict):
    """Форкнуть детач-процесс — тот же hook_bridge.py с --nag."""
    brief = fmt_tool_brief(tool_name, tool_input)
    try:
        subprocess.Popen(
            [sys.executable, __file__, "--nag", tool_use_id, tool_name, brief],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as e:
        logger.warning(f"spawn_nag failed: {e}")


def run_nag(tool_use_id: str, tool_name: str, brief: str):
    time.sleep(NAG_TIMEOUT_SEC)
    marker = DONE_DIR / f"{tool_use_id}.marker"
    if marker.exists():
        try:
            marker.unlink()
        except Exception:
            pass
        return
    # approve всё ещё висит в терминале → пинок в TG
    tg_send(
        f"⏰ *approve висит >5 мин в терминале:*\n"
        f"🔧 *{tool_name}* `{brief[:400]}`"
    )


# ─── hook event handlers ──────────────────────────────────────────────

def handle_user_prompt_submit(payload: dict):
    prompt = (payload.get("prompt") or "").strip()
    if not prompt:
        return
    tg_send(f"🎤 *Алексей → Claude*\n{prompt[:3500]}")


def handle_pre_tool_use(payload: dict):
    # ассистент-текст перед tool-call → в TG
    flush_assistant_texts(payload.get("transcript_path", ""),
                          payload.get("session_id", ""))

    tool_name = payload.get("tool_name") or ""
    tool_input = payload.get("tool_input") or {}

    if tool_name in BLOCKING_TOOLS:
        # Терминал сам обработает permission. Мы только ставим nag.
        tool_use_id = _tool_key(payload)
        spawn_nag(tool_use_id, tool_name, tool_input)
        return  # exit 0 + пустой stdout → Claude использует дефолт

    # Не-блокирующие тулы — тихий лог в TG (наблюдение с телефона)
    brief = fmt_tool_brief(tool_name, tool_input)
    if brief:
        tg_send(f"🔹 *{tool_name}* `{brief}`")


def handle_post_tool_use(payload: dict):
    tool_name = payload.get("tool_name") or ""

    # маркер "tool отработал" → nag отменяется
    if tool_name in BLOCKING_TOOLS:
        tool_use_id = _tool_key(payload)
        try:
            (DONE_DIR / f"{tool_use_id}.marker").touch()
        except Exception as e:
            logger.warning(f"marker write failed: {e}")

    # для Bash — показываем stdout/stderr в TG
    if tool_name != "Bash":
        return
    resp = payload.get("tool_response") or {}
    stdout = (resp.get("stdout") or "")[:1500]
    stderr = (resp.get("stderr") or "")[:500]
    parts = []
    if stdout:
        parts.append(f"📤\n```\n{stdout}\n```")
    if stderr:
        parts.append(f"⚠️ stderr:\n```\n{stderr}\n```")
    if parts:
        tg_send("\n".join(parts))


def handle_stop(payload: dict):
    # финальный ответ ассистента (без последующих tool-call) → в TG
    flush_assistant_texts(payload.get("transcript_path", ""),
                          payload.get("session_id", ""))
    tg_send("✅ *сессия завершена*")


EVENT_HANDLERS = {
    "UserPromptSubmit": handle_user_prompt_submit,
    "PreToolUse": handle_pre_tool_use,
    "PostToolUse": handle_post_tool_use,
    "Stop": handle_stop,
    "SessionEnd": handle_stop,
}


# ─── entry ────────────────────────────────────────────────────────────

def main():
    # nag-режим: запуск детач-процессом от spawn_nag
    if len(sys.argv) > 1 and sys.argv[1] == "--nag":
        try:
            run_nag(sys.argv[2], sys.argv[3],
                    sys.argv[4] if len(sys.argv) > 4 else "")
        except Exception as e:
            logger.exception(f"nag failed: {e}")
        sys.exit(0)

    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
    except Exception as e:
        logger.error(f"invalid stdin JSON: {e}")
        sys.exit(0)

    event = payload.get("hook_event_name") or ""
    logger.info(f"event={event} tool={payload.get('tool_name')}")

    if PAUSE_FILE.exists():
        sys.exit(0)

    handler = EVENT_HANDLERS.get(event)
    if handler:
        try:
            handler(payload)
        except Exception as e:
            logger.exception(f"handler {event} failed: {e}")
    sys.exit(0)


if __name__ == "__main__":
    main()
