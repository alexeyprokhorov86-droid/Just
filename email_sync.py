#!/usr/bin/env python3
"""
Email Sync Service - Синхронизация почтовых ящиков.
Запускается отдельным процессом параллельно с ботом.

Использование:
    python email_sync.py              # обычный запуск
    python email_sync.py --once       # одна синхронизация и выход
    python email_sync.py --initial    # первичная загрузка истории
"""

import os
import sys
import re
import time
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Generator
from dataclasses import dataclass
import pathlib

from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# Загружаем переменные окружения
env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")

IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.nicmail.ru")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))

SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "5"))
SYNC_BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "50"))
INITIAL_LOAD_DAYS = int(os.getenv("INITIAL_LOAD_DAYS", "30"))

ATTACHMENTS_PATH = os.getenv("ATTACHMENTS_PATH", "/var/email_logger/attachments")


# ============================================================
# РАБОТА С БД
# ============================================================

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, 
        user=DB_USER, password=DB_PASSWORD
    )


def get_email_credentials() -> dict:
    """Загружает учётные данные email из .env."""
    credentials = {}
    i = 1
    while True:
        env_key = f"EMAIL_{i}"
        value = os.getenv(env_key)
        if not value:
            break
        parts = value.split(",", 1)
        if len(parts) == 2:
            email_addr, password = parts
            credentials[email_addr.strip()] = password.strip()
        i += 1
    return credentials


# ============================================================
# ПАРСИНГ EMAIL
# ============================================================

@dataclass
class ParsedEmail:
    uid: int
    message_id: str
    in_reply_to: Optional[str]
    references: List[str]
    from_address: str
    to_addresses: List[str]
    cc_addresses: List[str]
    subject: str
    subject_normalized: str
    body_text: str
    body_html: str
    received_at: datetime
    has_attachments: bool


# Паттерн для нормализации темы
SUBJECT_CLEANUP_PATTERN = re.compile(
    r'^(re|fwd|fw|отв|ответ|пересл|переслано)[\s]*:[\s]*',
    re.IGNORECASE | re.UNICODE
)


def decode_email_header(header: str) -> str:
    """Декодирует заголовок письма."""
    if not header:
        return ""
    try:
        decoded_parts = decode_header(header)
        result = []
        for part, charset in decoded_parts:
            if isinstance(part, bytes):
                charset = charset or 'utf-8'
                try:
                    result.append(part.decode(charset, errors='replace'))
                except:
                    result.append(part.decode('utf-8', errors='replace'))
            else:
                result.append(str(part))
        return ' '.join(result).strip()
    except:
        return str(header)


def parse_email_address(header: str) -> str:
    """Извлекает email адрес из заголовка."""
    decoded = decode_email_header(header)
    match = re.search(r'[\w\.-]+@[\w\.-]+', decoded)
    return match.group(0).lower() if match else decoded.lower()


def parse_email_addresses(header: str) -> List[str]:
    """Извлекает список email адресов."""
    if not header:
        return []
    decoded = decode_email_header(header)
    return [addr.lower() for addr in re.findall(r'[\w\.-]+@[\w\.-]+', decoded)]


def normalize_subject(subject: str) -> str:
    """Нормализует тему письма."""
    normalized = subject
    while True:
        new = SUBJECT_CLEANUP_PATTERN.sub('', normalized)
        if new == normalized:
            break
        normalized = new
    return normalized.strip()[:500]


def extract_body(msg) -> tuple:
    """Извлекает текст и HTML из письма."""
    body_text = ""
    body_html = ""
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))
            
            if "attachment" in content_disposition:
                continue
            
            try:
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                charset = part.get_content_charset() or 'utf-8'
                text = payload.decode(charset, errors='replace')
                
                if content_type == "text/plain" and not body_text:
                    body_text = text
                elif content_type == "text/html" and not body_html:
                    body_html = text
            except:
                pass
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or 'utf-8'
                text = payload.decode(charset, errors='replace')
                if msg.get_content_type() == "text/html":
                    body_html = text
                else:
                    body_text = text
        except:
            pass
    
    # Конвертируем HTML в текст если нет текстовой версии
    if not body_text and body_html:
        body_text = re.sub(r'<br\s*/?>', '\n', body_html, flags=re.IGNORECASE)
        body_text = re.sub(r'<[^>]+>', '', body_text)
        import html
        body_text = html.unescape(body_text)
    
    return body_text, body_html


def has_attachments(msg) -> bool:
    """Проверяет наличие вложений."""
    if not msg.is_multipart():
        return False
    for part in msg.walk():
        if "attachment" in str(part.get("Content-Disposition", "")):
            return True
        if part.get_filename():
            return True
    return False


def parse_email_message(uid: int, raw_data: bytes) -> Optional[ParsedEmail]:
    """Парсит сырые данные письма."""
    try:
        msg = email.message_from_bytes(raw_data)
        
        message_id = decode_email_header(msg.get('Message-ID', ''))
        in_reply_to = decode_email_header(msg.get('In-Reply-To', ''))
        references_raw = decode_email_header(msg.get('References', ''))
        references = references_raw.split() if references_raw else []
        
        from_addr = parse_email_address(msg.get('From', ''))
        to_addrs = parse_email_addresses(msg.get('To', ''))
        cc_addrs = parse_email_addresses(msg.get('Cc', ''))
        
        subject = decode_email_header(msg.get('Subject', ''))
        subject_norm = normalize_subject(subject)
        
        date_str = msg.get('Date', '')
        try:
            received_at = parsedate_to_datetime(date_str)
        except:
            received_at = datetime.now()
        
        body_text, body_html = extract_body(msg)
        
        return ParsedEmail(
            uid=uid,
            message_id=message_id,
            in_reply_to=in_reply_to if in_reply_to else None,
            references=references,
            from_address=from_addr,
            to_addresses=to_addrs,
            cc_addresses=cc_addrs,
            subject=subject,
            subject_normalized=subject_norm,
            body_text=body_text,
            body_html=body_html,
            received_at=received_at,
            has_attachments=has_attachments(msg)
        )
    except Exception as e:
        logger.error(f"Error parsing email UID {uid}: {e}")
        return None


# ============================================================
# IMAP СИНХРОНИЗАЦИЯ
# ============================================================

def connect_imap(email_addr: str, password: str) -> Optional[imaplib.IMAP4_SSL]:
    """Подключается к IMAP серверу."""
    try:
        conn = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        conn.login(email_addr, password)
        logger.info(f"Connected to {email_addr}")
        return conn
    except Exception as e:
        logger.error(f"IMAP connection failed for {email_addr}: {e}")
        return None


def fetch_uids(conn: imaplib.IMAP4_SSL, folder: str, since_uid: int = 0) -> List[int]:
    """Получает список UID писем."""
    try:
        status, _ = conn.select(folder, readonly=True)
        if status != "OK":
            return []
        
        if since_uid > 0:
            status, data = conn.uid('search', None, f'UID {since_uid}:*')
        else:
            status, data = conn.uid('search', None, 'ALL')
        
        if status != "OK":
            return []
        
        uids = data[0].split()
        return sorted([int(uid) for uid in uids if int(uid) > since_uid])
    except Exception as e:
        logger.error(f"Error fetching UIDs: {e}")
        return []


def fetch_messages(conn: imaplib.IMAP4_SSL, folder: str, uids: List[int]) -> Generator[ParsedEmail, None, None]:
    """Загружает и парсит сообщения."""
    if not uids:
        return
    
    try:
        conn.select(folder, readonly=True)
        
        for i in range(0, len(uids), SYNC_BATCH_SIZE):
            batch = uids[i:i + SYNC_BATCH_SIZE]
            uid_str = ','.join(str(uid) for uid in batch)
            
            status, data = conn.uid('fetch', uid_str, '(RFC822)')
            if status != "OK":
                continue
            
            for item in data:
                if isinstance(item, tuple) and len(item) >= 2:
                    uid_match = re.search(rb'UID (\d+)', item[0])
                    if uid_match:
                        uid = int(uid_match.group(1))
                        parsed = parse_email_message(uid, item[1])
                        if parsed:
                            yield parsed
            
            time.sleep(1)  # Пауза между батчами
    except Exception as e:
        logger.error(f"Error fetching messages: {e}")


def find_sent_folder(conn: imaplib.IMAP4_SSL) -> Optional[str]:
    """Находит папку Отправленные."""
    variants = ["Sent", "INBOX.Sent", "Отправленные", "INBOX.Отправленные"]
    
    try:
        status, folders = conn.list()
        if status != "OK":
            return None
        
        folder_names = []
        for f in folders:
            if isinstance(f, bytes):
                f = f.decode('utf-8', errors='replace')
            match = re.search(r'"([^"]+)"$', str(f))
            if match:
                folder_names.append(match.group(1))
        
        for variant in variants:
            if variant in folder_names:
                return variant
        
        for folder in folder_names:
            if 'sent' in folder.lower() or 'отправ' in folder.lower():
                return folder
    except:
        pass
    
    return None


# ============================================================
# РАБОТА С ВЕТКАМИ
# ============================================================

def find_or_create_thread(cur, parsed: ParsedEmail) -> int:
    """Находит или создаёт ветку для письма."""
    
    # 1. Ищем по References
    if parsed.references:
        cur.execute("""
            SELECT t.id FROM email_threads t
            JOIN email_messages m ON m.thread_id = t.id
            WHERE m.message_id = ANY(%s)
            LIMIT 1
        """, (parsed.references,))
        row = cur.fetchone()
        if row:
            return row[0]
    
    # 2. Ищем по In-Reply-To
    if parsed.in_reply_to:
        cur.execute("""
            SELECT thread_id FROM email_messages
            WHERE message_id = %s AND thread_id IS NOT NULL
        """, (parsed.in_reply_to,))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    
    # 3. Ищем по нормализованной теме
    if len(parsed.subject_normalized) > 10:
        cur.execute("""
            SELECT id FROM email_threads
            WHERE subject_normalized = %s
            AND last_message_at > NOW() - INTERVAL '7 days'
            ORDER BY last_message_at DESC
            LIMIT 1
        """, (parsed.subject_normalized,))
        row = cur.fetchone()
        if row:
            return row[0]
    
    # 4. Создаём новую ветку
    cur.execute("""
        INSERT INTO email_threads (
            thread_id, subject_normalized, started_at, last_message_at, message_count
        )
        VALUES (%s, %s, %s, %s, 1)
        RETURNING id
    """, (parsed.message_id, parsed.subject_normalized, parsed.received_at, parsed.received_at))
    
    return cur.fetchone()[0]


def update_thread_stats(cur, thread_id: int, parsed: ParsedEmail):
    """Обновляет статистику ветки."""
    cur.execute("""
        UPDATE email_threads
        SET 
            message_count = message_count + 1,
            last_message_at = GREATEST(last_message_at, %s),
            participant_emails = array_append(
                CASE WHEN NOT %s = ANY(COALESCE(participant_emails, '{}'))
                THEN participant_emails ELSE participant_emails END,
                CASE WHEN NOT %s = ANY(COALESCE(participant_emails, '{}'))
                THEN %s ELSE NULL END
            ),
            updated_at = NOW()
        WHERE id = %s
    """, (parsed.received_at, parsed.from_address, parsed.from_address, parsed.from_address, thread_id))


# ============================================================
# СИНХРОНИЗАЦИЯ
# ============================================================

def sync_mailbox(mailbox_id: int, email_addr: str, password: str, last_uid_inbox: int, last_uid_sent: int) -> dict:
    """Синхронизирует один почтовый ящик."""
    
    stats = {'inbox': 0, 'sent': 0, 'errors': 0}
    
    conn_imap = connect_imap(email_addr, password)
    if not conn_imap:
        return stats
    
    conn_db = get_db_connection()
    
    try:
        with conn_db.cursor() as cur:
            # Синхронизируем INBOX
            uids = fetch_uids(conn_imap, "INBOX", last_uid_inbox)
            logger.info(f"{email_addr}: found {len(uids)} new messages in INBOX")
            
            new_uid_inbox = last_uid_inbox
            for parsed in fetch_messages(conn_imap, "INBOX", uids):
                try:
                    process_email(cur, parsed, mailbox_id, "INBOX", "inbound")
                    new_uid_inbox = max(new_uid_inbox, parsed.uid)
                    stats['inbox'] += 1
                except Exception as e:
                    logger.error(f"Error processing inbox {parsed.uid}: {e}")
                    stats['errors'] += 1
            
            # Синхронизируем Sent
            sent_folder = find_sent_folder(conn_imap)
            new_uid_sent = last_uid_sent
            
            if sent_folder:
                uids = fetch_uids(conn_imap, sent_folder, last_uid_sent)
                logger.info(f"{email_addr}: found {len(uids)} new messages in {sent_folder}")
                
                for parsed in fetch_messages(conn_imap, sent_folder, uids):
                    try:
                        process_email(cur, parsed, mailbox_id, "Sent", "outbound")
                        new_uid_sent = max(new_uid_sent, parsed.uid)
                        stats['sent'] += 1
                    except Exception as e:
                        logger.error(f"Error processing sent {parsed.uid}: {e}")
                        stats['errors'] += 1
            
            # Обновляем статус ящика
            cur.execute("""
                UPDATE monitored_mailboxes
                SET last_sync_at = NOW(), last_uid_inbox = %s, last_uid_sent = %s,
                    sync_status = 'idle', last_error = NULL
                WHERE id = %s
            """, (new_uid_inbox, new_uid_sent, mailbox_id))
            
            conn_db.commit()
    
    except Exception as e:
        logger.error(f"Sync error for {email_addr}: {e}")
        try:
            with conn_db.cursor() as cur:
                cur.execute("""
                    UPDATE monitored_mailboxes
                    SET sync_status = 'error', last_error = %s
                    WHERE id = %s
                """, (str(e), mailbox_id))
                conn_db.commit()
        except:
            pass
    
    finally:
        conn_db.close()
        try:
            conn_imap.logout()
        except:
            pass
    
    logger.info(f"{email_addr}: synced inbox={stats['inbox']}, sent={stats['sent']}, errors={stats['errors']}")
    return stats


def process_email(cur, parsed: ParsedEmail, mailbox_id: int, folder: str, direction: str):
    """Обрабатывает и сохраняет одно письмо."""
    
    # Проверяем дубликат
    cur.execute("""
        SELECT id FROM email_messages
        WHERE mailbox_id = %s AND folder = %s AND message_uid = %s
    """, (mailbox_id, folder, parsed.uid))
    
    if cur.fetchone():
        return
    
    # Находим или создаём ветку
    thread_id = find_or_create_thread(cur, parsed)
    
    # Сохраняем сообщение
    cur.execute("""
        INSERT INTO email_messages (
            message_uid, message_id, in_reply_to, references_list,
            thread_id, mailbox_id, folder, direction,
            from_address, to_addresses, cc_addresses,
            subject, subject_normalized, body_text, body_html,
            has_attachments, received_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (mailbox_id, folder, message_uid) DO NOTHING
    """, (
        parsed.uid, parsed.message_id, parsed.in_reply_to, parsed.references,
        thread_id, mailbox_id, folder, direction,
        parsed.from_address, parsed.to_addresses, parsed.cc_addresses,
        parsed.subject, parsed.subject_normalized, parsed.body_text, parsed.body_html,
        parsed.has_attachments, parsed.received_at
    ))
    
    # Обновляем статистику ветки
    update_thread_stats(cur, thread_id, parsed)


def sync_all_mailboxes():
    """Синхронизирует все активные почтовые ящики."""
    
    credentials = get_email_credentials()
    if not credentials:
        logger.error("No email credentials found in .env")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, last_uid_inbox, last_uid_sent
                FROM monitored_mailboxes
                WHERE is_active = true
            """)
            mailboxes = cur.fetchall()
    finally:
        conn.close()
    
    logger.info(f"Starting sync for {len(mailboxes)} mailboxes")
    
    total_stats = {'inbox': 0, 'sent': 0, 'errors': 0, 'skipped': 0}
    
    for mailbox_id, email_addr, last_uid_inbox, last_uid_sent in mailboxes:
        password = credentials.get(email_addr)
        if not password:
            logger.warning(f"No password for {email_addr}, skipping")
            total_stats['skipped'] += 1
            continue
        
        stats = sync_mailbox(
            mailbox_id=mailbox_id,
            email_addr=email_addr,
            password=password,
            last_uid_inbox=last_uid_inbox or 0,
            last_uid_sent=last_uid_sent or 0
        )
        
        total_stats['inbox'] += stats['inbox']
        total_stats['sent'] += stats['sent']
        total_stats['errors'] += stats['errors']
        
        time.sleep(2)  # Пауза между ящиками
    
    logger.info(f"Sync complete: inbox={total_stats['inbox']}, sent={total_stats['sent']}, "
                f"errors={total_stats['errors']}, skipped={total_stats['skipped']}")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Email Sync Service')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--initial', action='store_true', help='Initial load (not implemented yet)')
    args = parser.parse_args()
    
    logger.info("=" * 50)
    logger.info("Email Sync Service starting...")
    logger.info(f"IMAP Server: {IMAP_SERVER}:{IMAP_PORT}")
    logger.info(f"Sync interval: {SYNC_INTERVAL_MINUTES} minutes")
    logger.info("=" * 50)
    
    if args.once:
        sync_all_mailboxes()
        return
    
    # Бесконечный цикл синхронизации
    while True:
        try:
            sync_all_mailboxes()
        except KeyboardInterrupt:
            logger.info("Interrupted, exiting...")
            break
        except Exception as e:
            logger.error(f"Sync loop error: {e}")
        
        logger.info(f"Sleeping for {SYNC_INTERVAL_MINUTES} minutes...")
        time.sleep(SYNC_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    main()
