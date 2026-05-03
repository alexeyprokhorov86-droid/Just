#!/usr/bin/env python3
"""
verify_backup.py — еженедельная проверка целостности последнего S3-бекапа БД.

Что делает (lightweight, без полного restore — у нас только 33 GB free):
    1. Находит самый свежий S3-объект в s3://<bucket>/db_backups/
    2. Скачивает в /tmp
    3. Проверяет:
       - sha256 совпадает с metadata.sha256 (если есть)
       - gzip целостность (gzip -t)
       - SQL-сигнатуру pg_dump (zcat | head, ищем "PostgreSQL database dump")
       - наличие COPY/INSERT и CREATE TABLE строк (формат plain SQL)
    4. Удаляет временный файл
    5. Шлёт TG алерт при провале

Полный test-restore в отдельную БД сейчас невозможен (диск 33 GB free, дамп ~5 GB,
восстановленная БД займёт +15-20 GB). TODO когда расширим диск.
"""

import gzip
import hashlib
import os
import subprocess
import sys
import logging
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [verify_backup] %(levelname)s %(message)s",
)
log = logging.getLogger("verify_backup")

PREFIX = os.getenv("S3_BACKUP_PREFIX", "db_backups/").rstrip("/") + "/"
TMP_DIR = Path("/tmp/backup_verify")
SQL_SIGNATURE = b"PostgreSQL database dump"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("ATTACHMENTS_BUCKET_ENDPOINT"),
        aws_access_key_id=os.getenv("ATTACHMENTS_BUCKET_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("ATTACHMENTS_BUCKET_SECRET_KEY"),
        region_name=os.getenv("ATTACHMENTS_BUCKET_REGION"),
    )


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


def latest_object(s3, bucket: str) -> dict | None:
    """Ищет последний по LastModified объект в S3 под префиксом."""
    latest = None
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if latest is None or obj["LastModified"] > latest["LastModified"]:
                latest = obj
    return latest


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def verify(local: Path, expected_sha256: str | None) -> list[str]:
    """Возвращает список ошибок (пустой = OK)."""
    errors: list[str] = []

    # 1. sha256
    if expected_sha256:
        actual = sha256_file(local)
        if actual != expected_sha256:
            errors.append(f"sha256 mismatch: expected {expected_sha256[:12]}…, got {actual[:12]}…")
        else:
            log.info(f"sha256 ✓ ({actual[:12]}…)")
    else:
        log.warning("no expected sha256 in S3 metadata, skip")

    # 2. gzip integrity
    r = subprocess.run(["gzip", "-t", str(local)], capture_output=True, text=True)
    if r.returncode != 0:
        errors.append(f"gzip -t failed: {r.stderr.strip()}")
    else:
        log.info("gzip integrity ✓")

    # 3. SQL signature (read first ~10 KB after decompress)
    try:
        with gzip.open(local, "rb") as f:
            head = f.read(10 * 1024)
        if SQL_SIGNATURE not in head:
            errors.append(f"no '{SQL_SIGNATURE.decode()}' in first 10 KB")
        else:
            log.info("SQL signature ✓")
    except Exception as e:
        errors.append(f"cannot decompress head: {e}")

    # 4. structural sanity — ищем CREATE TABLE и COPY где-то в первых 1 MB
    try:
        with gzip.open(local, "rb") as f:
            chunk = f.read(1024 * 1024)
        if b"CREATE TABLE" not in chunk:
            errors.append("no 'CREATE TABLE' in first 1 MB — schema missing?")
        if b"COPY " not in chunk and b"INSERT " not in chunk:
            errors.append("no 'COPY' or 'INSERT' in first 1 MB — data missing?")
        if not errors:
            log.info("schema+data signatures ✓")
    except Exception as e:
        errors.append(f"cannot decompress 1 MB: {e}")

    return errors


def main():
    bucket = os.getenv("ATTACHMENTS_BUCKET_NAME")
    s3 = s3_client()

    obj = latest_object(s3, bucket)
    if obj is None:
        msg = f"BACKUP VERIFY: no objects under s3://{bucket}/{PREFIX}"
        log.error(msg)
        alert_admin(msg)
        sys.exit(1)

    log.info(f"verifying latest: {obj['Key']} ({obj['Size']/1024/1024:.1f} MB, modified {obj['LastModified']})")

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    local = TMP_DIR / Path(obj["Key"]).name

    try:
        head = s3.head_object(Bucket=bucket, Key=obj["Key"])
        expected_sha = head.get("Metadata", {}).get("sha256")

        s3.download_file(bucket, obj["Key"], str(local))
        log.info(f"downloaded → {local}")

        errors = verify(local, expected_sha)

        if errors:
            msg = "BACKUP VERIFY FAILED\n" + obj["Key"] + "\n" + "\n".join(f"  - {e}" for e in errors)
            log.error(msg)
            alert_admin(msg)
            sys.exit(1)
        log.info("ALL CHECKS PASSED")
    finally:
        try:
            local.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
