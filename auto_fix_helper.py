"""
auto_fix_helper.py — вспомогательные команды для auto_fix.sh.

Использование:
    rate_check <trigger> <max_per_24h> <cooldown_min>
        rc=0 если разрешено, rc=1 если rate-limit/cooldown сработал.
        stdout: однострочное объяснение.
    log_event <trigger> <ctx_file> <claude_out_path|""> <status> <revert_reason>
              <git_sha> <health_before> <health_after> [reverted]
    tg_report <trigger> <status> <reason> <git_sha> <session_log> <summary_file>
    target_hint <trigger> <ctx_file>
        выводит имя сервиса для health-check (если можно вывести из контекста).
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from dotenv import load_dotenv

ENV_PATH = pathlib.Path(__file__).parent / ".env"
load_dotenv(dotenv_path=ENV_PATH if ENV_PATH.exists() else None)

DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

WHITELIST_SERVICES = {
    "telegram-logger", "email-sync", "matrix-listener",
    "auth-bom", "nkt-dashboard",
}

STATUS_EMOJI = {
    "success": "✅",
    "failed": "❌",
    "reverted": "⏪",
    "rate_limited": "🚧",
    "escalated": "🆘",
    "dry_run": "🧪",
}


def db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, connect_timeout=10,
    )


def cmd_rate_check(trigger: str, max_per_24h: int, cooldown_min: int) -> int:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FILTER (
                WHERE status NOT IN ('rate_limited','dry_run')
            ),
            MAX(started_at) FILTER (
                WHERE status NOT IN ('rate_limited','dry_run')
            )
            FROM auto_fix_log
            WHERE trigger_name = %s
              AND started_at > NOW() - INTERVAL '24 hours'
            """,
            (trigger,),
        )
        cnt, last_at = cur.fetchone()
    cnt = cnt or 0
    if cnt >= max_per_24h:
        print(f"rate-limit hit: {cnt}/{max_per_24h} attempts in last 24h for trigger={trigger}")
        return 1
    if last_at is not None:
        age_min = (datetime.now(timezone.utc) - last_at).total_seconds() / 60.0
        if age_min < cooldown_min:
            print(f"cooldown active: last attempt {age_min:.1f} min ago (need {cooldown_min})")
            return 1
    print(f"ok: {cnt}/{max_per_24h} attempts in last 24h, no cooldown")
    return 0


def _read_text(path: str, max_bytes: int = 100_000) -> str:
    if not path or not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        return f.read(max_bytes).decode("utf-8", errors="replace")


def _parse_actions(claude_output: str) -> list[str]:
    actions: list[str] = []
    in_block = False
    in_actions = False
    for line in claude_output.splitlines():
        if line.strip() == "=== AUTO-FIX SUMMARY ===":
            in_block = True; continue
        if line.strip() == "=== END SUMMARY ===":
            break
        if not in_block:
            continue
        if line.startswith("ACTIONS:"):
            in_actions = True; continue
        if in_actions:
            stripped = line.lstrip()
            if stripped.startswith("- "):
                actions.append(stripped[2:].strip())
            elif stripped.startswith(("GIT_SHA:", "TARGET_SERVICE:", "STATUS:", "TRIGGER:")):
                in_actions = False
    return actions


def cmd_log_event(args: list[str]) -> int:
    (trigger, ctx_file, claude_out_path, status,
     revert_reason, git_sha, health_before, health_after) = args[:8]
    reverted = (args[8].lower() == "true") if len(args) > 8 else False
    ctx_text = _read_text(ctx_file)
    try:
        ctx_json = json.loads(ctx_text) if ctx_text.strip() else {}
    except Exception:
        ctx_json = {"_raw": ctx_text[:5000]}
    claude_text = _read_text(claude_out_path) if claude_out_path else ""
    actions = _parse_actions(claude_text)
    hb = None if health_before in ("", "unknown") else (health_before == "true")
    ha = None if health_after in ("", "unknown") else (health_after == "true")
    sha = git_sha if git_sha and git_sha != "unknown" else None
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO auto_fix_log
              (trigger_name, trigger_context, claude_output, actions_taken,
               git_commit_sha, reverted, revert_reason,
               health_check_before, health_check_after,
               telegram_reported, started_at, finished_at, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
            RETURNING id
            """,
            (trigger, json.dumps(ctx_json), claude_text[:200_000],
             actions or None, sha, reverted, revert_reason or None,
             hb, ha, False, status),
        )
        row_id = cur.fetchone()[0]
    print(row_id)
    return 0


def cmd_tg_report(args: list[str]) -> int:
    trigger, status, reason, git_sha, session_log, summary_file = (
        args + [""] * 6
    )[:6]
    if not BOT_TOKEN or not ADMIN_USER_ID:
        print("TG creds missing", file=sys.stderr)
        return 0
    emoji = STATUS_EMOJI.get(status, "❔")
    actions = ""
    if summary_file and os.path.exists(summary_file):
        text = _read_text(summary_file, 8000)
        in_actions = False
        lines = []
        for line in text.splitlines():
            if line.startswith("ACTIONS:"):
                in_actions = True; continue
            if in_actions:
                if line.startswith(("GIT_SHA:", "TARGET_SERVICE:", "STATUS:", "===")):
                    break
                if line.strip().startswith("- "):
                    lines.append(line.strip())
        actions = "\n".join(lines[:6])
    parts = [
        f"🤖 <b>Auto-fix: {trigger}</b>",
        f"Статус: {emoji} <b>{status}</b>",
    ]
    if actions:
        parts.append(f"Действия:\n{actions}")
    if git_sha and git_sha not in ("", "unknown", "none"):
        parts.append(f"Git: <code>{git_sha[:10]}</code>")
    if reason:
        parts.append(f"Причина: {reason[:300]}")
    if session_log:
        parts.append(f"Лог: <code>{session_log}</code>")
    msg = "\n".join(parts)[:3800]
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, data={
            "chat_id": ADMIN_USER_ID, "text": msg, "parse_mode": "HTML",
        }, timeout=10)
    except Exception as e:  # noqa: BLE001
        print(f"TG send error: {e}", file=sys.stderr)
    return 0


def cmd_target_hint(trigger: str, ctx_file: str) -> int:
    """Подсказка для health-check: выводим имя сервиса, если ясно."""
    ctx_text = _read_text(ctx_file)
    try:
        ctx = json.loads(ctx_text) if ctx_text.strip() else {}
    except Exception:
        ctx = {}
    cand = ctx.get("service") if isinstance(ctx, dict) else None
    if cand and cand in WHITELIST_SERVICES:
        print(cand); return 0
    if trigger == "service_down" and cand:
        print(cand); return 0
    return 0


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr); return 2
    cmd = sys.argv[1]
    args = sys.argv[2:]
    if cmd == "rate_check":
        return cmd_rate_check(args[0], int(args[1]), int(args[2]))
    if cmd == "log_event":
        return cmd_log_event(args)
    if cmd == "tg_report":
        return cmd_tg_report(args)
    if cmd == "target_hint":
        return cmd_target_hint(args[0], args[1])
    print(f"unknown cmd: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
