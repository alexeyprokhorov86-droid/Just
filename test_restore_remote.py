#!/usr/bin/env python3
"""
test_restore_remote.py — еженедельный реальный pg_restore последнего бекапа на Helsinki VPS.

Что делает:
    1. SSH на Helsinki, находит последний backup_*.sql.gz в /root/db_backups
    2. Поднимает временный postgres:15 контейнер (`pg-test-restore`)
    3. Восстанавливает дамп в БД test_restore через psql
    4. Проверяет минимальные пороги (count из ключевых таблиц)
    5. Удаляет контейнер + временные данные
    6. При FAIL — TG-алерт админу

Минимумы (если меньше — БД повреждена / неполная):
    source_chunks: ≥ 500_000
    km_facts:      ≥ 30_000
    source_documents: ≥ 200_000
    c1_employees:  ≥ 1_000

Длительность ~30-50 мин на Helsinki (4 vCPU, 3.7 GB RAM, ~17 GB raw БД).
Запускать в часы низкой нагрузки.
"""

import os
import subprocess
import sys
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [test_restore] %(levelname)s %(message)s",
)
log = logging.getLogger("test_restore")

HELSINKI_HOST = "root@77.42.83.103"
HELSINKI_KEY = "/home/admin/.ssh/id_rsa"
REMOTE_DIR = "/root/db_backups"
CONTAINER = "pg-test-restore"

# Минимумы — если меньше → дамп битый
MIN_COUNTS = {
    "source_chunks": 500_000,
    "km_facts": 30_000,
    "source_documents": 200_000,
    "c1_employees": 1_000,
}

REMOTE_SCRIPT = r"""
set -e
cd """ + REMOTE_DIR + r"""

# Найти последний дамп
LATEST=$(ls -t backup_*.sql.gz 2>/dev/null | head -1)
if [ -z "$LATEST" ]; then
    echo "FAIL: no backups in """ + REMOTE_DIR + r"""" >&2
    exit 1
fi
echo "INFO latest: $LATEST"
SIZE=$(stat -c '%s' "$LATEST")
echo "INFO size: $((SIZE/1024/1024)) MB"

# Очистить старый контейнер если остался от прошлого прогона
docker rm -f """ + CONTAINER + r""" 2>/dev/null || true

# Поднять pgvector/pgvector:pg15 (тот же image что у prod knowledge_db, pgvector уже встроен)
echo "INFO starting pgvector/pgvector:pg15 container"
docker run -d --name """ + CONTAINER + r""" \
    -e POSTGRES_PASSWORD=test_restore_only \
    -e POSTGRES_DB=test_restore \
    --shm-size=512m \
    --memory=2g --memory-swap=4g \
    pgvector/pgvector:pg15 >/dev/null

# Ждём готовности (до 60 секунд)
for i in $(seq 1 30); do
    if docker exec """ + CONTAINER + r""" pg_isready -U postgres -q; then
        echo "INFO postgres ready"
        break
    fi
    sleep 2
done

# pgvector уже есть в image — просто включаем extension в test_restore
docker exec """ + CONTAINER + r""" psql -U postgres -d test_restore -c "CREATE EXTENSION IF NOT EXISTS vector;" 2>&1 | head -1

# Restore (plain SQL, гонка через psql --single-transaction= нет — pg_dump ставит SET для миграций)
echo "INFO starting restore — это займёт 20-40 минут"
START=$(date +%s)
zcat "$LATEST" | docker exec -i """ + CONTAINER + r""" psql -U postgres -d test_restore -v ON_ERROR_STOP=0 -q > /tmp/restore_log 2>&1 || true
END=$(date +%s)
echo "INFO restore took $((END-START)) seconds"

# Errors summary (ON_ERROR_STOP=0 → ошибки в лог, но не падает)
ERR_COUNT=$(grep -c '^ERROR' /tmp/restore_log || echo 0)
echo "INFO restore ERROR lines: $ERR_COUNT"
if [ "$ERR_COUNT" -gt 100 ]; then
    echo "WARN too many restore errors, sample:"
    grep '^ERROR' /tmp/restore_log | head -5
fi

# Проверка count'ов
echo "RESULT BEGIN"
docker exec """ + CONTAINER + r""" psql -U postgres -d test_restore -tAc "
    SELECT 'source_chunks=' || count(*) FROM source_chunks
    UNION ALL SELECT 'km_facts=' || count(*) FROM km_facts
    UNION ALL SELECT 'source_documents=' || count(*) FROM source_documents
    UNION ALL SELECT 'c1_employees=' || count(*) FROM c1_employees
" 2>&1
echo "RESULT END"

# Cleanup
docker rm -f """ + CONTAINER + r""" >/dev/null
echo "INFO container removed"
"""


def alert_admin(text: str):
    try:
        import httpx
        token = os.getenv("BOT_TOKEN")
        chat = os.getenv("ADMIN_USER_ID")
        proxy = os.getenv("PROXY_URL")
        if not (token and chat):
            return
        with httpx.Client(proxy=proxy, timeout=15) as c:
            c.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": int(chat), "text": text, "disable_web_page_preview": True},
            )
    except Exception as e:
        log.warning(f"alert failed: {e}")


def parse_counts(stdout: str) -> dict[str, int]:
    """Извлекает 'table=N' пары между RESULT BEGIN/END."""
    result = {}
    capture = False
    for line in stdout.splitlines():
        if line.strip() == "RESULT BEGIN":
            capture = True
            continue
        if line.strip() == "RESULT END":
            capture = False
            continue
        if capture and "=" in line:
            k, v = line.strip().split("=", 1)
            try:
                result[k] = int(v)
            except ValueError:
                pass
    return result


def main():
    log.info("Running pg_restore test on Helsinki")
    cmd = [
        "ssh", "-i", HELSINKI_KEY,
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=20",
        "-o", "ServerAliveInterval=60",
        HELSINKI_HOST,
        "bash", "-s",
    ]
    try:
        r = subprocess.run(
            cmd, input=REMOTE_SCRIPT,
            capture_output=True, text=True, timeout=4500,  # 75 мин
        )
    except subprocess.TimeoutExpired:
        msg = "test_restore TIMEOUT (>75 мин на Helsinki)"
        log.error(msg)
        alert_admin(f"BACKUP TEST RESTORE: {msg}")
        sys.exit(1)

    log.info(f"ssh exit code: {r.returncode}")
    log.info(f"stdout (last 30 lines):\n{chr(10).join(r.stdout.splitlines()[-30:])}")
    if r.stderr:
        log.warning(f"stderr (last 20 lines):\n{chr(10).join(r.stderr.splitlines()[-20:])}")

    counts = parse_counts(r.stdout)
    log.info(f"counts parsed: {counts}")

    failures = []
    if r.returncode != 0:
        failures.append(f"ssh script exit code {r.returncode}")

    for table, threshold in MIN_COUNTS.items():
        actual = counts.get(table)
        if actual is None:
            failures.append(f"{table}: count not returned (table missing?)")
        elif actual < threshold:
            failures.append(f"{table}: {actual} < {threshold} threshold")

    if failures:
        msg = "BACKUP TEST RESTORE FAILED\n" + "\n".join(f"  - {f}" for f in failures)
        msg += f"\n\nCounts: {counts}"
        log.error(msg)
        alert_admin(msg)
        sys.exit(1)

    summary = ", ".join(f"{k}={v}" for k, v in counts.items())
    log.info(f"PASSED — {summary}")
    # Не шлём успех в TG чтобы не шуметь — только при FAIL


if __name__ == "__main__":
    main()
