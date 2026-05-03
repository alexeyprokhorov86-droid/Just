#!/usr/bin/env python3
"""
backup_to_s3.py — загружает локальный pg_dump в S3 и чистит старые S3-объекты.

Вызывается из backup.sh после успешного pg_dump:
    python3 backup_to_s3.py /path/to/backup_YYYY-MM-DD_HH-MM.sql.gz

ENV:
    ATTACHMENTS_BUCKET_*       — креды + endpoint Cloud.ru S3
    S3_BACKUP_PREFIX           — префикс ключа (default: db_backups/)
    S3_BACKUP_RETENTION_DAYS   — сколько дней держать в S3 (default: 30)
    BOT_TOKEN, ADMIN_USER_ID   — для алерта в TG при сбое
    PROXY_URL                  — для сообщения в TG (S3 — direct, без прокси)

Exit codes:
    0  — успех
    1  — upload failed
    2  — bad args
"""

import hashlib
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [backup_to_s3] %(levelname)s %(message)s",
)
log = logging.getLogger("backup_to_s3")

PREFIX = os.getenv("S3_BACKUP_PREFIX", "db_backups/").rstrip("/") + "/"
RETENTION_DAYS = int(os.getenv("S3_BACKUP_RETENTION_DAYS", "30"))


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("ATTACHMENTS_BUCKET_ENDPOINT"),
        aws_access_key_id=os.getenv("ATTACHMENTS_BUCKET_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("ATTACHMENTS_BUCKET_SECRET_KEY"),
        region_name=os.getenv("ATTACHMENTS_BUCKET_REGION"),
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def alert_admin(text: str):
    """Шлёт сообщение админу через PROXY_URL (best-effort)."""
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


def upload(local_path: Path) -> str:
    """Загружает файл в S3 с metadata sha256+size. Возвращает S3-ключ."""
    bucket = os.getenv("ATTACHMENTS_BUCKET_NAME")
    if not bucket:
        raise RuntimeError("ATTACHMENTS_BUCKET_NAME is empty")

    digest = sha256_file(local_path)
    size = local_path.stat().st_size
    key = PREFIX + local_path.name
    s3 = s3_client()

    log.info(f"uploading {local_path.name} ({size/1024/1024:.1f} MB, sha256={digest[:12]}…) → s3://{bucket}/{key}")
    s3.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={"Metadata": {"sha256": digest, "source-host": os.uname().nodename}},
    )

    # Verify
    head = s3.head_object(Bucket=bucket, Key=key)
    remote_size = head["ContentLength"]
    if remote_size != size:
        raise RuntimeError(f"size mismatch: local={size} remote={remote_size}")
    log.info(f"verified: ContentLength={remote_size}, ETag={head['ETag']}")
    return key


def cleanup(retention_days: int) -> int:
    """Удаляет S3-объекты старше retention_days. Возвращает число удалённых."""
    bucket = os.getenv("ATTACHMENTS_BUCKET_NAME")
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    s3 = s3_client()

    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                log.info(f"deleting {obj['Key']} (modified {obj['LastModified'].date()}, age >{retention_days}d)")
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
                deleted += 1
    log.info(f"cleanup: deleted {deleted} objects older than {retention_days}d from s3://{bucket}/{PREFIX}")
    return deleted


def main():
    if len(sys.argv) != 2:
        print("usage: backup_to_s3.py <local_backup.sql.gz>", file=sys.stderr)
        sys.exit(2)

    local = Path(sys.argv[1])
    if not local.is_file():
        log.error(f"file not found: {local}")
        alert_admin(f"BACKUP S3: file not found {local}")
        sys.exit(1)

    try:
        key = upload(local)
        cleanup(RETENTION_DAYS)
        log.info("done")
    except (ClientError, Exception) as e:
        log.error(f"upload failed: {e}")
        alert_admin(f"BACKUP S3 FAILED: {local.name}\nerror: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
