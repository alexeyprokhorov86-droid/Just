#!/usr/bin/env python3
"""
backup_to_remote.py — выгружает дамп БД на оба прокси-VPS (Amsterdam + Helsinki).

Каждый VPS — отдельный носитель + offsite в другой юрисдикции.
Дополняет S3-копию (Cloud.ru), для полного 3-2-1+ покрытия.

Использование:
    python3 backup_to_remote.py /path/to/backup_YYYY-MM-DD_HH-MM.sql.gz

ENV (.env):
    REMOTE_BACKUP_RETENTION_DAYS — сколько дней хранить на каждом VPS (default: 5)
    BOT_TOKEN, ADMIN_USER_ID, PROXY_URL — для TG-алертов

Не падает если один VPS недоступен — пытается оба и репортит частичный успех.
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
    format="%(asctime)s [backup_to_remote] %(levelname)s %(message)s",
)
log = logging.getLogger("backup_to_remote")

RETENTION_DAYS = int(os.getenv("REMOTE_BACKUP_RETENTION_DAYS", "5"))
REMOTE_DIR = "/root/db_backups"

REMOTES = [
    {
        "name": "Amsterdam",
        "host": "root@109.234.38.39",
        "key": "/home/admin/.ssh/amsterdam_proxy",
    },
    {
        "name": "Helsinki",
        "host": "root@77.42.83.103",
        "key": "/home/admin/.ssh/id_rsa",
    },
]


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


def ssh_opts(key: str) -> list[str]:
    return [
        "-i", key,
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=15",
        "-o", "ServerAliveInterval=30",
    ]


def push_to(remote: dict, local_path: Path) -> tuple[bool, str]:
    """rsync на VPS + cleanup старых файлов. (success, message)."""
    name = remote["name"]
    host = remote["host"]
    key = remote["key"]
    target = f"{host}:{REMOTE_DIR}/"

    try:
        # rsync с прогрессом отключён (cron-friendly), -t (preserve mtime → корректный retention),
        # -W (whole-file: faster для одноразовой передачи без diff)
        rsync_cmd = [
            "rsync", "-tW",
            "-e", "ssh " + " ".join(ssh_opts(key)),
            str(local_path),
            target,
        ]
        log.info(f"{name}: rsync → {target}")
        r = subprocess.run(rsync_cmd, capture_output=True, text=True, timeout=900)
        if r.returncode != 0:
            return False, f"rsync failed: {r.stderr.strip()}"

        # Cleanup старых через ssh find -mtime
        cleanup_cmd = (
            f"find {REMOTE_DIR} -name 'backup_*.sql.gz' -mtime +{RETENTION_DAYS} -delete -print"
        )
        r = subprocess.run(
            ["ssh"] + ssh_opts(key) + [host, cleanup_cmd],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            log.warning(f"{name}: cleanup non-zero exit: {r.stderr.strip()}")
        deleted = [line for line in r.stdout.splitlines() if line.strip()]
        if deleted:
            log.info(f"{name}: deleted {len(deleted)} old: {', '.join(Path(p).name for p in deleted)}")

        # Проверка что файл реально на месте + размер
        verify_cmd = f"stat -c '%s' {REMOTE_DIR}/{local_path.name}"
        r = subprocess.run(
            ["ssh"] + ssh_opts(key) + [host, verify_cmd],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode != 0:
            return False, f"verify failed: {r.stderr.strip()}"
        remote_size = int(r.stdout.strip())
        local_size = local_path.stat().st_size
        if remote_size != local_size:
            return False, f"size mismatch: local={local_size} remote={remote_size}"

        return True, f"{remote_size/1024/1024:.0f} MB ok"
    except subprocess.TimeoutExpired:
        return False, "ssh/rsync timeout"
    except Exception as e:
        return False, f"exception: {e}"


def main():
    if len(sys.argv) != 2:
        print("usage: backup_to_remote.py <local_backup.sql.gz>", file=sys.stderr)
        sys.exit(2)

    local = Path(sys.argv[1])
    if not local.is_file():
        log.error(f"file not found: {local}")
        sys.exit(1)

    results = []
    for remote in REMOTES:
        ok, msg = push_to(remote, local)
        results.append((remote["name"], ok, msg))
        if ok:
            log.info(f"{remote['name']}: {msg}")
        else:
            log.error(f"{remote['name']}: {msg}")

    failed = [n for n, ok, _ in results if not ok]
    if failed:
        details = "\n".join(f"  - {n}: {msg}" for n, ok, msg in results if not ok)
        alert_admin(f"REMOTE BACKUP partial failure for {local.name}\n{details}")
        # exit 1 если ВСЕ упали; если хотя бы один ОК — это soft-success
        if len(failed) == len(REMOTES):
            sys.exit(1)
    log.info(f"done: {len(results)-len(failed)}/{len(results)} remote copies")


if __name__ == "__main__":
    main()
