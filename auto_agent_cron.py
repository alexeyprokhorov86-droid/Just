"""
auto_agent_cron.py — плановые триггеры автономного агента (раз в час).

Каждый триггер:
  1. проверяет своё условие (БД / лог / df),
  2. при срабатывании пишет JSON-контекст в /tmp/agent_<trigger>_ctx.json,
  3. вызывает auto_fix.sh <trigger> <ctx_path>.

Дедупликация — на стороне auto_fix.sh (rate-limit 2/24ч + cooldown 15 мин).
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import re
import subprocess
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

SCRIPT_DIR = pathlib.Path(__file__).parent
load_dotenv(dotenv_path=SCRIPT_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("auto_agent_cron")

DB = dict(
    host=os.getenv("DB_HOST", "172.20.0.2"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "knowledge_base"),
    user=os.getenv("DB_USER", "knowledge"),
    password=os.getenv("DB_PASSWORD"),
    connect_timeout=10,
)

AUTO_FIX_SH = str(SCRIPT_DIR / "auto_fix.sh")
CALL_TIMEOUT_SEC = 660  # auto_fix.sh: claude=600s + overhead

SYNC_LOGS = [
    "/home/admin/knowledge-base/sync_quick.log",
    "/home/admin/knowledge-base/sync_hourly.log",
    "/home/admin/knowledge-base/sync_daily.log",
    "/home/admin/knowledge-base/sync_full.log",
    "/home/admin/knowledge-base/sync_weekly.log",
]
REVIEW_LOG = str(SCRIPT_DIR / "review_knowledge.log")
BUILD_CHUNKS_LOG = str(SCRIPT_DIR / "build_chunks.log")


def write_ctx(trigger: str, payload: dict) -> str:
    payload.setdefault("trigger", trigger)
    payload.setdefault("detected_at", datetime.now().isoformat())
    path = f"/tmp/agent_{trigger}_ctx.json"
    with open(path, "w") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)
    return path


def call_auto_fix(trigger: str, ctx_path: str) -> None:
    log.info("→ auto_fix.sh %s %s", trigger, ctx_path)
    try:
        rc = subprocess.run(
            [AUTO_FIX_SH, trigger, ctx_path],
            cwd=str(SCRIPT_DIR), timeout=CALL_TIMEOUT_SEC,
        ).returncode
        log.info("← auto_fix.sh %s exit=%s", trigger, rc)
    except subprocess.TimeoutExpired:
        log.error("auto_fix.sh %s TIMEOUT", trigger)
    except Exception as e:  # noqa: BLE001
        log.error("auto_fix.sh %s ERROR: %s", trigger, e)


# ── Триггер 1: pending_rules ────────────────────────────────────────────
def trigger_pending_rules() -> None:
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM km_filter_rules WHERE approval_status='pending'"
        )
        cnt = cur.fetchone()[0]
        if cnt <= 5:
            log.info("pending_rules: %s ≤ 5 — пропуск", cnt); return
        cur.execute(
            """
            SELECT id, rule_type, value, reason, created_at
            FROM km_filter_rules
            WHERE approval_status='pending'
            ORDER BY created_at DESC
            LIMIT 20
            """
        )
        rows = [
            dict(id=r[0], rule_type=r[1], value=r[2], reason=r[3], created_at=r[4])
            for r in cur.fetchall()
        ]
    ctx = write_ctx("pending_rules", {"count": cnt, "top_20": rows})
    call_auto_fix("pending_rules", ctx)


# ── Триггер 2: sync_1c_error ────────────────────────────────────────────
ERR_RE = re.compile(r"\b(ERROR|Traceback|HTTP\s+[45]\d{2}|Exception|FAILED)\b")


def _tail_recent_errors(path: str, since_sec: int = 3600) -> list[str]:
    """Берём последние ~500 строк, фильтруем по паттерну. Время не учитываем —
    sync-логи редко ротируются, и mtime файла даёт верхнюю границу."""
    if not os.path.exists(path):
        return []
    if time.time() - os.path.getmtime(path) > since_sec * 3:
        return []  # лог давно не трогали — пропускаем
    try:
        out = subprocess.run(
            ["tail", "-n", "500", path],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception:
        return []
    return [ln for ln in out.splitlines() if ERR_RE.search(ln)][-30:]


def trigger_sync_1c_error() -> None:
    found: dict[str, list[str]] = {}
    for path in SYNC_LOGS:
        errs = _tail_recent_errors(path, since_sec=3600)
        if errs:
            found[path] = errs
    if not found:
        log.info("sync_1c_error: чисто"); return
    ctx = write_ctx("sync_1c_error", {
        "logs_with_errors": list(found.keys()),
        "errors_per_log": found,
    })
    call_auto_fix("sync_1c_error", ctx)


# ── Триггер 3: disk_high ────────────────────────────────────────────────
def trigger_disk_high() -> None:
    out = subprocess.run(
        ["df", "/", "--output=pcent,avail,used,size"],
        capture_output=True, text=True, timeout=10,
    ).stdout
    lines = out.strip().splitlines()
    if len(lines) < 2:
        return
    pct = int(lines[1].split()[0].rstrip("%"))
    if pct <= 85:
        log.info("disk_high: %s%% ≤ 85 — пропуск", pct); return
    df_h = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=10).stdout
    try:
        du_top = subprocess.run(
            "du -sh /home/admin/* /home/admin/.* 2>/dev/null | sort -h | tail -10",
            shell=True, capture_output=True, text=True, timeout=120,
        ).stdout
    except Exception:
        du_top = ""
    var_log = subprocess.run(
        ["sudo", "du", "-sh", "/var/log"],
        capture_output=True, text=True, timeout=30,
    ).stdout
    ctx = write_ctx("disk_high", {
        "percent": pct,
        "df_h": df_h,
        "du_top10_home_admin": du_top,
        "du_var_log": var_log,
    })
    call_auto_fix("disk_high", ctx)


# ── Триггер 4: embeddings_stalled ───────────────────────────────────────
def trigger_embeddings_stalled() -> None:
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT MAX(created_at) FROM source_chunks")
        last_chunk = cur.fetchone()[0]
        cur.execute("SELECT MAX(created_at) FROM source_documents")
        last_doc = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*) FROM source_documents sd
            WHERE NOT EXISTS (
                SELECT 1 FROM source_chunks sc WHERE sc.document_id = sd.id
            )
            AND sd.created_at > NOW() - INTERVAL '24 hours'
            """
        )
        unindexed_24h = cur.fetchone()[0]
    if last_chunk is None or last_doc is None:
        log.info("embeddings_stalled: нет данных"); return
    age_chunk_min = (datetime.now(last_chunk.tzinfo) - last_chunk).total_seconds() / 60
    age_doc_min = (datetime.now(last_doc.tzinfo) - last_doc).total_seconds() / 60
    if age_chunk_min <= 120 or age_doc_min > 30:
        log.info(
            "embeddings_stalled: chunk_age=%.0fm doc_age=%.0fm — пропуск",
            age_chunk_min, age_doc_min,
        )
        return
    tail_log = ""
    if os.path.exists(BUILD_CHUNKS_LOG):
        tail_log = subprocess.run(
            ["tail", "-n", "30", BUILD_CHUNKS_LOG],
            capture_output=True, text=True, timeout=10,
        ).stdout
    ctx = write_ctx("embeddings_stalled", {
        "last_chunk_at": last_chunk.isoformat(),
        "last_doc_at": last_doc.isoformat(),
        "age_chunk_min": round(age_chunk_min, 1),
        "age_doc_min": round(age_doc_min, 1),
        "unindexed_docs_24h": unindexed_24h,
        "build_chunks_log_tail": tail_log,
    })
    call_auto_fix("embeddings_stalled", ctx)


# ── Триггер 5: json_parse_errors ────────────────────────────────────────
JSON_ERR_RE = re.compile(r"(JSON parse error|JSONDecodeError|json\.decoder)", re.I)


def trigger_json_parse_errors() -> None:
    if not os.path.exists(REVIEW_LOG):
        log.info("json_parse_errors: %s нет — создаю пустой", REVIEW_LOG)
        pathlib.Path(REVIEW_LOG).touch()
        return
    if time.time() - os.path.getmtime(REVIEW_LOG) > 86400 * 3:
        return  # лог давно не трогали
    try:
        out = subprocess.run(
            ["tail", "-n", "2000", REVIEW_LOG],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception:
        return
    lines = out.splitlines()
    err_indices = [i for i, ln in enumerate(lines) if JSON_ERR_RE.search(ln)]
    if len(err_indices) < 3:
        log.info("json_parse_errors: %s ошибок < 3 — пропуск", len(err_indices)); return
    samples = []
    for i in err_indices[-5:]:
        samples.append("\n".join(lines[max(0, i - 3): i + 4]))
    ctx = write_ctx("json_parse_errors", {
        "log_file": REVIEW_LOG,
        "error_count_in_tail": len(err_indices),
        "last_5_with_context": samples,
    })
    call_auto_fix("json_parse_errors", ctx)


TRIGGERS = [
    ("pending_rules", trigger_pending_rules),
    ("sync_1c_error", trigger_sync_1c_error),
    ("disk_high", trigger_disk_high),
    ("embeddings_stalled", trigger_embeddings_stalled),
    ("json_parse_errors", trigger_json_parse_errors),
]


def main() -> int:
    log.info("=== auto_agent_cron start ===")
    for name, fn in TRIGGERS:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            log.exception("trigger %s упал: %s", name, e)
        time.sleep(5)
    log.info("=== auto_agent_cron done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
