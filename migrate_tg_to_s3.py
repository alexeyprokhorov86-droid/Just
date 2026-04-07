#!/usr/bin/env python3
"""
migrate_tg_to_s3.py — миграция старых TG-вложений на Object Storage (S3).

Скачивает файлы из Telegram по file_id и загружает в S3 bucket.
Обновляет storage_path в таблицах tg_chat_*.

Запуск:
  python migrate_tg_to_s3.py              # все без storage_path
  python migrate_tg_to_s3.py --batch 100  # ограничить
  python migrate_tg_to_s3.py --dry-run    # только посчитать
"""

import os
import sys
import asyncio
import logging
import hashlib
import argparse
import fcntl
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv('/home/admin/telegram_logger_bot/.env')

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '172.20.0.2'),
    'port': 5432,
    'dbname': 'knowledge_base',
    'user': 'knowledge',
    'password': os.getenv('DB_PASSWORD'),
}

BOT_TOKEN = os.getenv('BOT_TOKEN', '')
BUCKET_NAME = os.getenv('ATTACHMENTS_BUCKET_NAME', '')


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def get_s3_client():
    try:
        import boto3
        from botocore.config import Config as BotoConfig
        return boto3.client(
            service_name='s3',
            endpoint_url=os.getenv('ATTACHMENTS_BUCKET_ENDPOINT', 'https://s3.cloud.ru'),
            region_name=os.getenv('ATTACHMENTS_BUCKET_REGION', 'ru-central-1'),
            aws_access_key_id=os.getenv('ATTACHMENTS_BUCKET_ACCESS_KEY', ''),
            aws_secret_access_key=os.getenv('ATTACHMENTS_BUCKET_SECRET_KEY', ''),
            config=BotoConfig(s3={"addressing_style": "path"})
        )
    except Exception as e:
        logger.error(f"S3 client error: {e}")
        return None


def find_unmigrated(conn):
    """Найти все TG-вложения без storage_path."""
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'tg_chat_%%'
          AND table_name != 'tg_chats_metadata'
    """)
    tables = [r[0] for r in cur.fetchall()]
    
    items = []
    for tbl in tables:
        try:
            cur.execute(f"""
                SELECT message_id, media_file_id, timestamp
                FROM {tbl}
                WHERE media_file_id IS NOT NULL AND media_file_id != ''
                  AND (storage_path IS NULL OR storage_path = '')
                ORDER BY timestamp DESC
            """)
            for row in cur.fetchall():
                items.append({
                    'table': tbl,
                    'message_id': row[0],
                    'file_id': row[1],
                    'timestamp': row[2],
                })
        except Exception as e:
            logger.warning(f"Ошибка чтения {tbl}: {e}")
            conn.rollback()
    
    cur.close()
    return items


async def migrate_batch(conn, items, s3, dry_run=False):
    """Скачать из Telegram и загрузить на S3."""
    from telegram import Bot
    from telegram.request import HTTPXRequest
    
    request = HTTPXRequest(read_timeout=120, connect_timeout=30, proxy="socks5://127.0.0.1:1080")
    bot = Bot(token=BOT_TOKEN, request=request)
    
    migrated = 0
    errors = 0
    
    for i, item in enumerate(items):
        try:
            if dry_run:
                logger.info(f"[DRY-RUN] {item['table']} msg={item['message_id']} date={item['timestamp']}")
                migrated += 1
                continue
            
            # Скачиваем из Telegram
            f = await bot.get_file(item['file_id'])
            file_data = await f.download_as_bytearray()
            
            filename = f.file_path.split('/')[-1] if f.file_path else f"file_{item['message_id']}"
            
            # Формируем S3 ключ
            digest = hashlib.sha256(bytes(file_data)).hexdigest()[:16]
            safe_fn = filename.replace("/", "_").replace("\\", "_")
            s3_key = f"tg_attachments/{item['table']}/{item['message_id']}/{digest}_{safe_fn}"
            
            # Загружаем на S3
            s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=bytes(file_data))
            storage_path = f"s3://{BUCKET_NAME}/{s3_key}"
            
            # Обновляем БД
            cur = conn.cursor()
            cur.execute(f"UPDATE {item['table']} SET storage_path = %s WHERE message_id = %s",
                       (storage_path, item['message_id']))
            conn.commit()
            cur.close()
            
            migrated += 1
            if migrated % 10 == 0:
                logger.info(f"Прогресс: {migrated}/{len(items)} ({errors} ошибок)")
            
            await asyncio.sleep(0.3)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Ошибка {item['table']} msg={item['message_id']}: {e}")
            errors += 1
            conn.rollback()
            await asyncio.sleep(1)
    
    return migrated, errors


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', type=int, default=0, help='Макс файлов (0=все)')
    parser.add_argument('--dry-run', action='store_true', help='Только посчитать')
    args = parser.parse_args()
    
    # Lock
    lock_file = open('/tmp/migrate_tg_s3.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("Миграция уже запущена")
        sys.exit(0)
    
    conn = get_conn()
    
    items = find_unmigrated(conn)
    logger.info(f"Найдено {len(items)} вложений без S3")
    
    if not items:
        logger.info("Нечего мигрировать")
        return
    
    if args.batch:
        items = items[:args.batch]
        logger.info(f"Ограничено до {args.batch}")
    
    s3 = None
    if not args.dry_run:
        s3 = get_s3_client()
        if not s3:
            logger.error("Не удалось создать S3 клиент")
            return
    
    migrated, errors = await migrate_batch(conn, items, s3, dry_run=args.dry_run)
    
    logger.info(f"ГОТОВО: мигрировано {migrated}, ошибок {errors}")
    conn.close()


if __name__ == '__main__':
    asyncio.run(main())
