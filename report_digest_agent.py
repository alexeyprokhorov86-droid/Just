"""
report_digest_agent.py — парсинг ночных отчётов и вызов auto_fix.sh.

Запуск по cron в 08:15 (после daily_report.py в 08:00).
Парсит сегодняшние логи 3 скриптов:
  1. audit_pipeline.log  (01:00) — аудит пайплайна
  2. review_knowledge.log (05:00) — ревизия знаний
  3. daily_report.log     (08:00) — ежедневный отчёт

Выделяет actionable проблемы, формирует JSON-контекст и вызывает
auto_fix.sh report_digest <ctx_path>.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import re
import subprocess
import time
from datetime import datetime

from dotenv import load_dotenv

SCRIPT_DIR = pathlib.Path(__file__).parent
load_dotenv(dotenv_path=SCRIPT_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("report_digest_agent")

AUTO_FIX_SH = str(SCRIPT_DIR / "auto_fix.sh")
CALL_TIMEOUT_SEC = 660

AUDIT_LOG = str(SCRIPT_DIR / "audit_pipeline.log")
REVIEW_LOG = str(SCRIPT_DIR / "review_knowledge.log")
DAILY_LOG = str(SCRIPT_DIR / "daily_report.log")

TODAY = datetime.now().strftime("%Y-%m-%d")


def _read_today_block(log_path: str) -> str:
    """Извлекает из лога строки за сегодня."""
    if not os.path.exists(log_path):
        return ""
    lines = []
    try:
        with open(log_path, "r", errors="replace") as f:
            for line in f:
                if TODAY in line[:30] or (lines and not re.match(r"^\d{4}-\d{2}-\d{2}", line)):
                    lines.append(line)
    except Exception:
        return ""
    return "".join(lines[-3000:])  # max ~3000 строк


def _extract_report_html(log_text: str) -> str:
    """Извлекает финальный HTML-отчёт из лога (после 'Отчёт:' или '<b>')."""
    report_lines = []
    in_report = False
    for line in log_text.splitlines():
        # Начало отчёта — строка с <b>🔍 или <b>🔬 или <b>📊
        if "<b>" in line and any(m in line for m in ["Ночной аудит", "Ревизия знаний", "Ежедневный отчёт"]):
            in_report = True
            idx = line.find("<b>")
            report_lines = [line[idx:]]
            continue
        if in_report:
            stripped = line.strip()
            # Конец отчёта — строка с timestamp лога
            if re.match(r"^\d{4}-\d{2}-\d{2}.*\[(INFO|WARNING|ERROR)\]", line):
                break
            # Конец — маркеры завершения
            if stripped in ("АУДИТ ЗАВЕРШЁН", "РЕВИЗИЯ ЗАВЕРШЕНА"):
                break
            # Пустые строки внутри отчёта — пропускаем, но не ломаем
            report_lines.append(line)
    return "\n".join(report_lines)


# ── Парсеры проблем ──────────────────────────────────────────────────────

def parse_audit_issues(report: str) -> list[dict]:
    """Парсит проблемы из ночного аудита пайплайна."""
    issues = []

    # TG attachments errors
    m = re.search(r"❌ Ошибок:\s*(\d+)", report)
    if m and int(m.group(1)) > 0:
        errors = int(m.group(1))
        remaining = 0
        mr = re.search(r"⏳ Осталось:\s*(\d+)", report)
        if mr:
            remaining = int(mr.group(1))
        issues.append({
            "source": "audit_pipeline",
            "type": "tg_attachment_errors",
            "severity": "medium",
            "description": f"TG-вложения: {errors} ошибок при обработке, {remaining} необработанных",
            "errors": errors,
            "remaining": remaining,
        })

    # Email attachment issues
    m = re.search(r"Email-вложения без анализа:\s*(\d+)", report)
    if m and int(m.group(1)) > 0:
        issues.append({
            "source": "audit_pipeline",
            "type": "email_attachment_gaps",
            "severity": "low",
            "description": f"Email-вложения без анализа: {m.group(1)}",
        })

    # Distillation — telegram backlog (matrix OK to ignore, too many)
    m = re.search(r"telegram_message:\s*(\d+)", report)
    if m and int(m.group(1)) > 100:
        issues.append({
            "source": "audit_pipeline",
            "type": "distillation_telegram_backlog",
            "severity": "medium",
            "description": f"Telegram-сообщений без distillation: {m.group(1)}",
            "count": int(m.group(1)),
        })

    # Source chunks exit code
    m = re.search(r"Без chunks.*?exit=(\d+)", report, re.DOTALL)
    if m and m.group(1) != "0":
        issues.append({
            "source": "audit_pipeline",
            "type": "build_chunks_error",
            "severity": "high",
            "description": f"build_source_chunks завершился с exit={m.group(1)}",
        })

    return issues


def parse_review_issues(report: str) -> list[dict]:
    """Парсит проблемы из ревизии знаний."""
    issues = []

    # High error count in review
    m = re.search(r"⚠️ Ошибок:\s*(\d+)", report)
    if m and int(m.group(1)) > 0:
        issues.append({
            "source": "review_knowledge",
            "type": "review_llm_errors",
            "severity": "high",
            "description": f"LLM-ревью: {m.group(1)} ошибок",
            "count": int(m.group(1)),
        })

    # Many rejected — possible data quality issue
    m_rejected = re.search(r"❌ Отклонено:\s*(\d+)", report)
    m_total = re.search(r"Проверено:\s*(\d+)", report)
    if m_rejected and m_total:
        rejected = int(m_rejected.group(1))
        total = int(m_total.group(1))
        if total > 0 and rejected / total > 0.3:
            issues.append({
                "source": "review_knowledge",
                "type": "high_rejection_rate",
                "severity": "medium",
                "description": f"Высокий процент отклонений: {rejected}/{total} ({rejected/total:.0%})",
            })

    return issues


def parse_daily_issues(report: str) -> list[dict]:
    """Парсит проблемы из ежедневного отчёта."""
    issues = []

    # RAG evaluator insufficient
    m = re.search(r"Evaluator insufficient:\s*(\d+)\s+из\s+(\d+)", report)
    if m:
        insuff = int(m.group(1))
        total = int(m.group(2))
        if total > 0 and insuff / total > 0.5:
            issues.append({
                "source": "daily_report",
                "type": "rag_quality_low",
                "severity": "medium",
                "description": f"RAG качество: {insuff}/{total} insufficient ({insuff/total:.0%})",
            })

    # Services down
    for svc in ["telegram-logger", "email-sync", "matrix-listener", "auth-bom"]:
        if re.search(rf"{svc}:.*❌", report):
            issues.append({
                "source": "daily_report",
                "type": "service_down",
                "severity": "critical",
                "description": f"Сервис {svc} не работает",
                "service": svc,
            })

    # Email errors
    m = re.search(r"Ящиков с ошибками:\s*(\d+)", report)
    if m and int(m.group(1)) > 5:
        issues.append({
            "source": "daily_report",
            "type": "email_mailbox_errors",
            "severity": "medium",
            "description": f"Почтовых ящиков с ошибками: {m.group(1)}",
        })

    # 1C sync errors
    for m in re.finditer(r"❌\s+(\w+):\s*(.+)", report):
        entity = m.group(1)
        detail = m.group(2).strip()
        # Не дублировать сервисы (они тоже ❌)
        if entity not in ("telegram", "email", "matrix", "auth"):
            issues.append({
                "source": "daily_report",
                "type": "sync_1c_error",
                "severity": "high",
                "description": f"1С sync ошибка: {entity} — {detail}",
            })

    # Suspicious junk rules — needs checking
    m = re.search(r"Проверь:\s*(.+)", report)
    if m:
        rules_text = m.group(1).strip()
        issues.append({
            "source": "daily_report",
            "type": "suspicious_junk_rules",
            "severity": "medium",
            "description": f"Подозрительные junk-правила: {rules_text}",
            "rules_text": rules_text,
        })

    # Disk high (>80%)
    m = re.search(r"Диск:\s*(\d+)%", report)
    if m and int(m.group(1)) > 80:
        issues.append({
            "source": "daily_report",
            "type": "disk_usage_high",
            "severity": "high",
            "description": f"Диск: {m.group(1)}% занято",
        })

    return issues


def parse_audit_log_errors(log_text: str) -> list[dict]:
    """Проверяет лог audit_pipeline на ошибки в процессе работы."""
    issues = []

    # Telegram download failures
    download_fails = log_text.count("Telegram download failed")
    if download_fails > 50:
        issues.append({
            "source": "audit_pipeline_process",
            "type": "tg_download_failures",
            "severity": "medium",
            "description": f"Telegram download failed: {download_fails} раз (возможно file_id устарели)",
            "count": download_fails,
        })

    # SQL column errors (e.g. "storage_path" does not exist)
    col_errors = set()
    for m in re.finditer(r'column "(\w+)" does not exist', log_text):
        col_errors.add(m.group(1))
    if col_errors:
        # Extract affected tables
        tables = set()
        for m in re.finditer(r"Ошибка (tg_chat_\S+):", log_text):
            tables.add(m.group(1))
        issues.append({
            "source": "audit_pipeline_process",
            "type": "sql_column_missing",
            "severity": "high",
            "description": f"Отсутствуют колонки в БД: {', '.join(col_errors)} (таблиц: {len(tables)})",
            "missing_columns": list(col_errors),
            "affected_tables_sample": list(tables)[:5],
        })

    return issues


def call_auto_fix(ctx_path: str) -> None:
    log.info("→ auto_fix.sh report_digest %s", ctx_path)
    try:
        rc = subprocess.run(
            [AUTO_FIX_SH, "report_digest", ctx_path],
            cwd=str(SCRIPT_DIR), timeout=CALL_TIMEOUT_SEC,
        ).returncode
        log.info("← auto_fix.sh report_digest exit=%s", rc)
    except subprocess.TimeoutExpired:
        log.error("auto_fix.sh report_digest TIMEOUT")
    except Exception as e:
        log.error("auto_fix.sh report_digest ERROR: %s", e)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--unconditional", action="store_true",
        help="Будить агента каждый день, даже если проблем не найдено (режим scheduled_review).",
    )
    parser.add_argument(
        "--until", type=str, default=None,
        help="Дата окончания для --unconditional (YYYY-MM-DD включительно). После — обычное поведение.",
    )
    args = parser.parse_args()

    log.info("=== report_digest_agent start ===")

    unconditional_active = args.unconditional
    if args.unconditional and args.until:
        try:
            until_dt = datetime.strptime(args.until, "%Y-%m-%d").date()
            today_dt = datetime.now().date()
            if today_dt > until_dt:
                unconditional_active = False
                log.info("--unconditional истёк (today=%s > until=%s) — обычный режим", today_dt, until_dt)
        except ValueError:
            log.warning("--until не распарсился (%s), игнорирую", args.until)

    # Читаем сегодняшние блоки из логов
    audit_text = _read_today_block(AUDIT_LOG)
    review_text = _read_today_block(REVIEW_LOG)
    daily_text = _read_today_block(DAILY_LOG)

    log.info("Прочитано: audit=%d, review=%d, daily=%d символов",
             len(audit_text), len(review_text), len(daily_text))

    # Извлекаем HTML-отчёты
    audit_report = _extract_report_html(audit_text)
    review_report = _extract_report_html(review_text)
    daily_report = _extract_report_html(daily_text)

    log.info("Отчёты: audit=%d, review=%d, daily=%d символов",
             len(audit_report), len(review_report), len(daily_report))

    # Парсим проблемы
    all_issues = []
    if audit_report:
        all_issues.extend(parse_audit_issues(audit_report))
    if audit_text:
        all_issues.extend(parse_audit_log_errors(audit_text))
    if review_report:
        all_issues.extend(parse_review_issues(review_report))
    if daily_report:
        all_issues.extend(parse_daily_issues(daily_report))

    if not all_issues and not unconditional_active:
        log.info("Проблем не найдено — всё ок")
        log.info("=== report_digest_agent done ===")
        return 0

    # Сортируем по severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_issues.sort(key=lambda x: severity_order.get(x.get("severity", "low"), 3))

    if all_issues:
        log.info("Найдено %d проблем:", len(all_issues))
        for iss in all_issues:
            log.info("  [%s] %s — %s", iss["severity"], iss["type"], iss["description"])
    else:
        log.info("Проблем не найдено, но --unconditional активен — будим агента для обзора (no_action ожидаем)")

    # Формируем контекст для auto_fix.sh
    mode = "scheduled_review" if unconditional_active and not all_issues else "issues_found"
    ctx = {
        "trigger": "report_digest",
        "mode": mode,
        "detected_at": datetime.now().isoformat(),
        "date": TODAY,
        "issues_count": len(all_issues),
        "issues": all_issues,
        "reports_summary": {
            "audit_pipeline": audit_report[:2000] if audit_report else "не найден",
            "review_knowledge": review_report[:2000] if review_report else "не найден",
            "daily_report": daily_report[:2000] if daily_report else "не найден",
        },
    }
    if mode == "scheduled_review":
        ctx["note"] = (
            "Плановое ежедневное пробуждение: парсер не нашёл явных проблем. "
            "Прочитай 3 отчёта (audit_pipeline / review_knowledge / daily_report), "
            "оцени общее состояние. Если всё ок — верни STATUS: no_action (ничего не правь). "
            "Если увидел что-то, что парсер пропустил — действуй по правилам."
        )
    ctx_path = f"/tmp/agent_report_digest_ctx.json"
    with open(ctx_path, "w") as f:
        json.dump(ctx, f, ensure_ascii=False, default=str, indent=2)

    log.info("Контекст записан: %s (%d байт, mode=%s)", ctx_path, os.path.getsize(ctx_path), mode)

    call_auto_fix(ctx_path)

    log.info("=== report_digest_agent done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
