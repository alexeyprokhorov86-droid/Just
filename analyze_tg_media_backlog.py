"""Backfill анализа медиа в Telegram-чатах: download → analyze → UPDATE → canonical.

Идёт по всем tg_chat_* таблицам, находит сообщения с storage_path и пустым
media_analysis. Скачивает из S3, анализирует через media_analyzer (LLM), пишет
обратно в БД и триггерит canonical_helper.

Для чатов в TORTY_OTGRUZKI применяется LLM-prefilter (gpt-4.1-mini):
анализируются только фото с текстом (накладные, маркировка), пустые кадры
паллет/кузова пропускаются (значимый smart save: ~70% фото без текста).

Использование:
    python analyze_tg_media_backlog.py [--batch N] [--chat TABLE_NAME] [--dry-run]
"""

import argparse
import asyncio
import io
import json
import logging
import os
import pathlib
import sys

import boto3
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(pathlib.Path(__file__).parent / '.env')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

import media_analyzer
from canonical_helper import insert_source_document_tg

S3_ENDPOINT = os.getenv('ATTACHMENTS_BUCKET_ENDPOINT')
S3_ACCESS_KEY = os.getenv('ATTACHMENTS_BUCKET_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('ATTACHMENTS_BUCKET_SECRET_KEY')
S3_REGION = os.getenv('ATTACHMENTS_BUCKET_REGION')
S3_BUCKET = os.getenv('ATTACHMENTS_BUCKET_NAME')

TORTY_OTGRUZKI_TABLES = {
    'tg_chat_1003360907471_torty_otgruzki',
    'tg_chat_5052540061_torty_otgruzki',
}


def get_db():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', '172.20.0.2'), port=5432,
        dbname='knowledge_base', user='knowledge', password=os.getenv('DB_PASSWORD'),
    )


def get_s3():
    return boto3.client(
        's3', endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )


def parse_s3_path(storage_path: str) -> tuple[str, str] | None:
    """s3://bucket/key → (bucket, key)."""
    if not storage_path or not storage_path.startswith('s3://'):
        return None
    rest = storage_path[5:]
    bucket, _, key = rest.partition('/')
    return bucket, key if key else None


def download_from_s3(s3, storage_path: str) -> bytes | None:
    parsed = parse_s3_path(storage_path)
    if not parsed:
        return None
    bucket, key = parsed
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read()
    except Exception as e:
        logger.warning(f"S3 download failed {storage_path}: {e}")
        return None


def list_chat_tables(conn) -> list[str]:
    cur = conn.cursor()
    cur.execute("""
        SELECT tablename FROM pg_tables
        WHERE tablename LIKE 'tg_chat_%'
          AND tablename NOT LIKE '%bridged%'
          AND tablename NOT LIKE '%metadata%'
        ORDER BY tablename
    """)
    return [r[0] for r in cur.fetchall()]


def get_chat_meta(conn, table_name: str) -> tuple[int, str]:
    cur = conn.cursor()
    cur.execute(
        "SELECT chat_id, chat_title FROM tg_chats_metadata WHERE table_name=%s",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return 0, table_name
    return row[0], row[1] or table_name


def get_pending(conn, table: str, limit: int) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""
        SELECT message_id, message_text, message_type, storage_path, media_file_id,
               first_name, last_name, username, user_id, timestamp,
               reply_to_message_id, forward_from_user_id
        FROM {table}
        WHERE storage_path IS NOT NULL
          AND (media_analysis IS NULL OR media_analysis = '')
          AND message_type IN ('photo','pdf','excel','word','powerpoint','video','voice')
        ORDER BY timestamp DESC
        LIMIT %s
    """, (limit,))
    return cur.fetchall()


CONTENT_TYPE_MAP = {
    'photo': 'image/jpeg',
    'pdf': 'application/pdf',
    'excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'word': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'powerpoint': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'video': 'video/mp4',
    'voice': 'audio/ogg',
}


async def analyze_one(file_data: bytes, message_type: str, filename: str, context: str) -> tuple[str, str]:
    """Возвращает (media_analysis, content_text)."""
    ma, ct = "", ""
    try:
        if message_type == 'photo':
            ma = await media_analyzer.analyze_image_with_gpt(file_data, 'image/jpeg', context, filename)
            ct = await media_analyzer.extract_text_from_image(file_data, 'image/jpeg')
        elif message_type == 'pdf':
            ma = await media_analyzer.analyze_pdf_with_gpt(file_data, filename, context)
            ct = await media_analyzer.extract_text_from_pdf(file_data)
        elif message_type == 'excel':
            ma = await media_analyzer.analyze_excel_with_gpt(file_data, filename, context)
            ct = await media_analyzer.extract_csv_from_excel(file_data, filename)
        elif message_type == 'word':
            ma = await media_analyzer.analyze_word_with_gpt(file_data, filename, context)
            ct = await media_analyzer.extract_text_from_word(file_data)
        elif message_type == 'powerpoint':
            ma = await media_analyzer.analyze_pptx_with_gpt(file_data, filename, context)
            ct = await media_analyzer.extract_text_from_pptx(file_data)
        elif message_type == 'video':
            ma = await media_analyzer.analyze_video_with_gemini(file_data, filename, context)
        elif message_type == 'voice':
            # Voice = транскрипт через whisper (нужен путь к файлу)
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
                tmp.write(file_data)
                tmp_path = tmp.name
            try:
                ct = await media_analyzer.extract_transcript_from_audio(tmp_path)
                ma = f"Транскрипция: {ct}" if ct else ""
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
    except Exception as e:
        logger.error(f"  analyze error ({message_type}): {e}")
    return ma or "", ct or ""


def update_message(conn, table: str, message_id: int, ma: str, ct: str):
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE {table}
        SET media_analysis = %s, content_text = %s
        WHERE message_id = %s
    """, (ma, ct, message_id))
    conn.commit()


def trigger_canonical(conn, table: str, chat_title: str, msg: dict, ma: str, ct: str):
    """Обновить source_documents через canonical_helper (с media_analysis + content_text)."""
    msg_data = dict(msg)
    msg_data['media_analysis'] = ma
    msg_data['content_text'] = ct
    cur = conn.cursor()
    insert_source_document_tg(cur, table, chat_title, msg_data)
    conn.commit()


async def process_table(conn, s3, table: str, batch: int, dry_run: bool, ocr_filter: bool):
    chat_id, chat_title = get_chat_meta(conn, table)
    pending = get_pending(conn, table, batch)
    if not pending:
        return 0, 0

    logger.info(f"=== {table} ({chat_title}) — {len(pending)} pending, ocr_filter={ocr_filter}")

    # Контекст за 8 дней (как в bot.py)
    try:
        context = media_analyzer.get_full_chat_context(table, chat_id, chat_title, hours=192)
    except Exception as e:
        logger.warning(f"  context fetch failed: {e}")
        context = ""

    analyzed = skipped_filter = errors = 0
    for msg in pending:
        sp = msg['storage_path']
        mid = msg['message_id']
        mt = msg['message_type']

        file_data = download_from_s3(s3, sp)
        if not file_data:
            errors += 1
            continue

        # OCR-prefilter для Торты-Отгрузки: только фото с текстом
        if ocr_filter and mt == 'photo':
            has_text = await media_analyzer.has_meaningful_text(file_data, 'image/jpeg')
            if not has_text:
                skipped_filter += 1
                # Пометим что чекали но текста нет — чтобы не пере-чекать
                if not dry_run:
                    update_message(conn, table, mid, '[no_text]', '')
                logger.info(f"  msg {mid}: no text → skip")
                continue

        if dry_run:
            logger.info(f"  [dry] msg {mid} type={mt} would analyze")
            analyzed += 1
            continue

        filename = sp.rsplit('/', 1)[-1]
        ma, ct = await analyze_one(file_data, mt, filename, context)

        if not ma and not ct:
            errors += 1
            logger.warning(f"  msg {mid}: empty analysis")
            continue

        update_message(conn, table, mid, ma, ct)
        trigger_canonical(conn, table, chat_title, msg, ma, ct)
        analyzed += 1
        if analyzed % 10 == 0:
            logger.info(f"  progress: analyzed={analyzed} skipped={skipped_filter} errors={errors}")

    logger.info(f"  done: analyzed={analyzed} skipped_filter={skipped_filter} errors={errors}")
    return analyzed, skipped_filter


async def main():
    p = argparse.ArgumentParser()
    p.add_argument('--batch', type=int, default=100, help='Макс сообщений на чат за запуск')
    p.add_argument('--chat', type=str, help='Только этот tg_chat_*')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    conn = get_db()
    s3 = get_s3()

    tables = [args.chat] if args.chat else list_chat_tables(conn)

    total_analyzed = total_skipped = 0
    for t in tables:
        ocr_filter = t in TORTY_OTGRUZKI_TABLES
        try:
            a, s = await process_table(conn, s3, t, args.batch, args.dry_run, ocr_filter)
            total_analyzed += a
            total_skipped += s
        except Exception as e:
            logger.error(f"{t}: {e}")
            conn.rollback()

    logger.info(f"GLOBAL: analyzed={total_analyzed}, skipped_filter={total_skipped}")


if __name__ == '__main__':
    asyncio.run(main())
