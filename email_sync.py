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
import hashlib
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
from email import policy
from email.parser import BytesParser
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Generator
from dataclasses import dataclass, field
import pathlib
from company_context import get_company_profile
from fact_extractor import extract_facts_from_thread_summary_sync

from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import requests
import json
from embedding_service_e5 import index_email_chunk
from email_text_processing import build_email_chunks

try:
    import boto3
    from botocore.config import Config as BotoConfig
except Exception:
    boto3 = None
    BotoConfig = None

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
MAX_ATTACHMENT_SIZE_MB = int(os.getenv("MAX_ATTACHMENT_SIZE_MB", "25"))
MAX_ATTACHMENT_SIZE_BYTES = MAX_ATTACHMENT_SIZE_MB * 1024 * 1024

# S3-compatible Object Storage для вложений (Yandex/GCS S3/AWS)
ATTACHMENTS_BUCKET_NAME = os.getenv("ATTACHMENTS_BUCKET_NAME", "").strip()
ATTACHMENTS_BUCKET_ENDPOINT = os.getenv("ATTACHMENTS_BUCKET_ENDPOINT", "").strip()
ATTACHMENTS_BUCKET_REGION = os.getenv("ATTACHMENTS_BUCKET_REGION", "ru-central1").strip()
ATTACHMENTS_BUCKET_ACCESS_KEY = os.getenv("ATTACHMENTS_BUCKET_ACCESS_KEY", "").strip()
ATTACHMENTS_BUCKET_SECRET_KEY = os.getenv("ATTACHMENTS_BUCKET_SECRET_KEY", "").strip()
ATTACHMENTS_BUCKET_PREFIX = os.getenv("ATTACHMENTS_BUCKET_PREFIX", "email_attachments").strip("/")
ATTACHMENTS_BUCKET_FORCE_PATH_STYLE = os.getenv(
    "ATTACHMENTS_BUCKET_FORCE_PATH_STYLE", "true"
).lower() in ("1", "true", "yes")

# RouterAI для анализа цепочек
ROUTERAI_API_KEY = os.getenv("ROUTERAI_API_KEY")
ROUTERAI_BASE_URL = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")

# Telegram бот для уведомлений
BOT_TOKEN = os.getenv("BOT_TOKEN")

_s3_client = None

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


def map_legacy_thread_status(lifecycle_status: str, resolution_outcome: Optional[str]) -> str:
    """Сопоставляет 2-слойную модель в legacy status для совместимости."""
    if lifecycle_status == "open":
        return "open"
    if lifecycle_status == "pending_resolution":
        return "pending_resolution"
    if lifecycle_status == "archived":
        return "archived"
    if lifecycle_status == "closed":
        return "cancelled" if resolution_outcome == "cancelled" else "resolved"
    return "open"


def get_s3_client():
    """Ленивая инициализация S3-compatible клиента."""
    global _s3_client

    if _s3_client is not None:
        return _s3_client

    if not ATTACHMENTS_BUCKET_NAME:
        return None

    if not (ATTACHMENTS_BUCKET_ACCESS_KEY and ATTACHMENTS_BUCKET_SECRET_KEY):
        logger.warning("Bucket name задан, но не заданы ключи ATTACHMENTS_BUCKET_ACCESS_KEY/SECRET_KEY")
        return None

    if not boto3:
        logger.warning("boto3 недоступен — загрузка email вложений в bucket отключена")
        return None

    client_kwargs = {
        "service_name": "s3",
        "aws_access_key_id": ATTACHMENTS_BUCKET_ACCESS_KEY,
        "aws_secret_access_key": ATTACHMENTS_BUCKET_SECRET_KEY,
        "region_name": ATTACHMENTS_BUCKET_REGION,
    }
    if ATTACHMENTS_BUCKET_ENDPOINT:
        client_kwargs["endpoint_url"] = ATTACHMENTS_BUCKET_ENDPOINT
    if BotoConfig:
        client_kwargs["config"] = BotoConfig(
            s3={"addressing_style": "path" if ATTACHMENTS_BUCKET_FORCE_PATH_STYLE else "auto"}
        )

    _s3_client = boto3.client(**client_kwargs)
    return _s3_client


def sanitize_filename(filename: str) -> str:
    """Очищает имя файла для безопасного хранения."""
    base = (filename or "").strip()
    if not base:
        return "attachment.bin"
    base = base.replace("\x00", "")
    base = re.sub(r"[^\w\-.() ]+", "_", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base[:180] or "attachment.bin"


def build_attachment_key(email_id: int, filename: str, received_at: Optional[datetime], payload: bytes) -> str:
    """Формирует ключ объекта в bucket/local storage."""
    dt = received_at or datetime.now()
    safe_filename = sanitize_filename(filename)
    digest = hashlib.sha256(payload).hexdigest()[:16]
    prefix = ATTACHMENTS_BUCKET_PREFIX or "email_attachments"
    return f"{prefix}/{dt.strftime('%Y/%m/%d')}/{email_id}/{digest}_{safe_filename}"


def upload_attachment_to_bucket(email_id: int, filename: str, payload: bytes,
                                content_type: str = "", received_at: Optional[datetime] = None) -> tuple[str, str]:
    """Загружает вложение в bucket. Возвращает (storage_path, error)."""
    client = get_s3_client()
    if not client:
        return "", "bucket_not_configured"

    key = build_attachment_key(email_id=email_id, filename=filename, received_at=received_at, payload=payload)
    try:
        put_kwargs = {
            "Bucket": ATTACHMENTS_BUCKET_NAME,
            "Key": key,
            "Body": payload,
        }
        if content_type:
            put_kwargs["ContentType"] = content_type
        client.put_object(**put_kwargs)
        return f"s3://{ATTACHMENTS_BUCKET_NAME}/{key}", ""
    except Exception as e:
        return "", str(e)


def save_attachment_locally(email_id: int, filename: str, payload: bytes,
                            received_at: Optional[datetime] = None) -> tuple[str, str]:
    """Fallback: сохраняет вложение на локальный диск."""
    rel_key = build_attachment_key(email_id=email_id, filename=filename, received_at=received_at, payload=payload)
    full_path = pathlib.Path(ATTACHMENTS_PATH) / rel_key
    try:
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(payload)
        return str(full_path), ""
    except Exception as e:
        return "", str(e)


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
    attachments: List["EmailAttachment"] = field(default_factory=list)


@dataclass
class EmailAttachment:
    filename: str
    content_type: str
    payload: bytes
    size_bytes: int


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
    """Извлекает текст и HTML из письма (стараемся корректно декодировать)."""
    body_text = ""
    body_html = ""

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition", "")).lower()
            if "attachment" in disp:
                continue

            try:
                if ctype == "text/plain" and not body_text:
                    body_text = part.get_content()
                elif ctype == "text/html" and not body_html:
                    body_html = part.get_content()
            except Exception:
                # fallback на старый decode (на всякий случай)
                try:
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue
                    charset = part.get_content_charset() or 'utf-8'
                    decoded = payload.decode(charset, errors='replace')
                    if ctype == "text/plain" and not body_text:
                        body_text = decoded
                    elif ctype == "text/html" and not body_html:
                        body_html = decoded
                except Exception:
                    pass
    else:
        ctype = msg.get_content_type()
        try:
            if ctype == "text/html":
                body_html = msg.get_content()
            else:
                body_text = msg.get_content()
        except Exception:
            try:
                payload = msg.get_payload(decode=True)
                if payload:
                    charset = msg.get_content_charset() or 'utf-8'
                    decoded = payload.decode(charset, errors='replace')
                    if ctype == "text/html":
                        body_html = decoded
                    else:
                        body_text = decoded
            except Exception:
                pass

    return str(body_text or ""), str(body_html or "")



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


def extract_attachments(msg) -> List[EmailAttachment]:
    """Извлекает вложения из email сообщения."""
    attachments: List[EmailAttachment] = []
    if not msg.is_multipart():
        return attachments

    for part in msg.walk():
        content_disposition = str(part.get("Content-Disposition", "")).lower()
        raw_filename = part.get_filename()

        # Обрабатываем явные attachment и части с именем файла
        if "attachment" not in content_disposition and not raw_filename:
            continue

        try:
            payload = part.get_payload(decode=True) or b""
        except Exception:
            payload = b""

        if not payload:
            continue

        if len(payload) > MAX_ATTACHMENT_SIZE_BYTES:
            logger.warning(
                f"Пропуск вложения >{MAX_ATTACHMENT_SIZE_MB}MB: "
                f"{decode_email_header(raw_filename or 'attachment.bin')}"
            )
            continue

        filename = decode_email_header(raw_filename or "attachment.bin")
        content_type = part.get_content_type() or "application/octet-stream"
        attachments.append(
            EmailAttachment(
                filename=filename[:255],
                content_type=content_type[:100],
                payload=payload,
                size_bytes=len(payload),
            )
        )

    return attachments


def parse_email_message(uid: int, raw_data: bytes) -> Optional[ParsedEmail]:
    """Парсит сырые данные письма."""
    try:
        msg = BytesParser(policy=policy.default).parsebytes(raw_data)
        
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
        attachments = extract_attachments(msg)
        
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
            has_attachments=has_attachments(msg),
            attachments=attachments,
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

# ── Email & Attachment Classification ─────────────────────────────────────

def classify_email_category(from_address: str, to_addresses: str, mailbox_email: str) -> str:
    """Определяет категорию письма при вставке."""
    from_lower = (from_address or "").lower()
    our_domains = ['totsamiy.com', 'lacannelle.ru']
    
    # own_notifications — наш noreply
    if from_lower == 'noreply@totsamiy.com':
        return 'own_notifications'
    
    # system — mailer-daemon, postmaster, ediweb
    system_patterns = ['mailer-daemon@', 'postmaster@', 'do_not_reply@ediweb']
    if any(p in from_lower for p in system_patterns):
        return 'system'
    
    # external_auto — внешние noreply/no-reply
    auto_patterns = ['noreply@', 'no-reply@', 'no_reply@', 'donotreply@', 'do-not-reply@']
    is_our_domain = any(d in from_lower for d in our_domains)
    if not is_our_domain and any(p in from_lower for p in auto_patterns):
        return 'external_auto'
    
    # internal — от наших доменов
    if is_our_domain:
        return 'internal'
    
    # external_business — всё остальное
    return 'external_business'


def classify_attachment_status(filename: str, size_bytes: int, content_type: str) -> str:
    """Определяет analysis_status вложения: pending или skip_junk."""
    fname_lower = (filename or "").lower()
    ctype_lower = (content_type or "").lower()
    
    # Мусорные расширения
    junk_extensions = ['.gif', '.bmp', '.mso', '.dat', '.htm']
    if any(fname_lower.endswith(ext) for ext in junk_extensions):
        return 'skip_junk'
    
    # image001/image002 и т.д. < 50KB — подписи в письмах
    if fname_lower.startswith('image00') and (size_bytes or 0) < 50000:
        return 'skip_junk'
    
    # Файлы без имени или без расширения < 1KB
    if not filename or (not '.' in filename and (size_bytes or 0) < 1000):
        return 'skip_junk'
    
    return 'pending'

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
    
    # 4. Создаём новую ветку (или берём существующую при гонке/дубликате Message-ID)
    cur.execute("""
        INSERT INTO email_threads (
            thread_id, subject_normalized, started_at, last_message_at, message_count,
            lifecycle_status, resolution_outcome, status
        )
        VALUES (%s, %s, %s, %s, 1, 'open', NULL, 'open')
        RETURNING id
    """, (parsed.message_id, parsed.subject_normalized, parsed.received_at, parsed.received_at))

    row = cur.fetchone()
    if row and len(row) > 0:
        return row[0]

    # Очень редкий fallback
    cur.execute("SELECT id FROM email_threads WHERE thread_id = %s", (parsed.message_id,))
    row = cur.fetchone()
    if row and len(row) > 0:
        return row[0]

    raise RuntimeError(f"Не удалось создать или получить thread для message_id={parsed.message_id}")


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
# АНАЛИЗ ЗАКРЫТИЯ ЦЕПОЧЕК
# ============================================================

CLOSURE_MARKERS = [
    # Оплата
    "оплачено", "оплатили", "оплата прошла", "оплата поступила", "деньги поступили",
    "платёж получен", "платеж получен", "средства поступили", "оплата подтверждена",
    # Отгрузка/доставка
    "отгружено", "отгрузили", "товар отправлен", "заказ отправлен", "доставлено",
    "получили товар", "товар получен", "груз доставлен", "отгрузка выполнена",
    # Подтверждение/согласование
    "подтверждаю", "подтверждено", "согласовано", "договорились", "принято",
    "заказ подтверждён", "заказ подтвержден", "всё верно", "все верно",
    # Завершение
    "вопрос закрыт", "вопрос решён", "вопрос решен", "спасибо за сотрудничество",
    "благодарим за заказ", "ждём следующий заказ", "ждем следующий заказ",
    # Отказ/отмена
    "отказ", "отменено", "заказ отменён", "заказ отменен", "не актуально"
]


def check_thread_closure(body_text: str, subject: str = "") -> tuple[bool, str]:
    """
    Проверяет, содержит ли письмо маркеры закрытия цепочки.
    Возвращает (is_closed, marker_found).
    """
    if not body_text:
        return False, ""
    
    text_lower = body_text.lower()
    subject_lower = (subject or "").lower()
    combined = f"{subject_lower} {text_lower}"
    
    for marker in CLOSURE_MARKERS:
        if marker in combined:
            return True, marker
    
    return False, ""


def get_thread_messages(cur, thread_id: int, limit: int = 20) -> list:
    """Получает последние сообщения цепочки для генерации сводки."""
    cur.execute("""
        SELECT from_address, to_addresses, subject, body_text, received_at
        FROM email_messages
        WHERE thread_id = %s
        ORDER BY received_at DESC
        LIMIT %s
    """, (thread_id, limit))
    
    messages = []
    for row in cur.fetchall():
        messages.append({
            "from": row[0],
            "to": row[1],
            "subject": row[2],
            "body": (row[3] or "")[:1000],  # Ограничиваем длину
            "date": row[4].strftime("%d.%m.%Y %H:%M") if row[4] else ""
        })
    
    return list(reversed(messages))  # Хронологический порядок


def generate_thread_summary(thread_id: int, messages: list, closure_marker: str) -> dict:
    """Генерирует сводку цепочки через GPT-4.1-mini."""
    if not ROUTERAI_API_KEY or not messages:
        return {}
    
    # Формируем текст переписки
    conversation = []
    for msg in messages:
        conversation.append(f"[{msg['date']}] От: {msg['from']}\nТема: {msg['subject']}\n{msg['body']}\n")
    
    conversation_text = "\n---\n".join(conversation)
    
    company_profile = get_company_profile()
    
    prompt = f"""{company_profile}

Проанализируй эту email-переписку компании Фрумелад и создай краткую сводку.
Используй знания о компании из профиля выше: учитывай кто такие контрагенты, какие бренды, кто из сотрудников участвует.

ПЕРЕПИСКА:
{conversation_text}

ОБНАРУЖЕННЫЙ МАРКЕР ЗАКРЫТИЯ: "{closure_marker}"

Ответь в формате JSON:
{{
    "summary_short": "Краткий итог в 1-2 предложения (что заказали, чем закончилось)",
    "summary_detailed": "Подробная сводка: участники, предмет обсуждения, ключевые даты и суммы, итог",
    "key_decisions": ["решение 1", "решение 2"],
    "action_items": ["задача 1 если есть", "задача 2"],
    "status": "closed_success" или "closed_cancelled" или "closed_other",
    "topic_tags": ["закупка", "оплата", "доставка"]
}}

Только JSON, без пояснений:"""

    try:
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {ROUTERAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "openai/gpt-4.1-mini",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1000,
                "temperature": 0.3
            },
            timeout=60
        )
        
        result = response.json()
        if "choices" not in result:
            logger.error(f"Thread summary: нет choices в ответе")
            return {}
        
        answer = result["choices"][0]["message"]["content"].strip()
        
        # Убираем markdown если есть
        if answer.startswith("```"):
            answer = answer.split("```")[1]
            if answer.startswith("json"):
                answer = answer[4:]
        
        summary_data = json.loads(answer)
        logger.info(f"Thread {thread_id}: сводка сгенерирована")
        return summary_data
        
    except json.JSONDecodeError as e:
        logger.error(f"Thread summary: ошибка парсинга JSON: {e}")
        return {}
    except Exception as e:
        logger.error(f"Thread summary: ошибка: {e}")
        return {}


def save_thread_summary(cur, thread_id: int, summary_data: dict, closure_marker: str):
    """Сохраняет сводку в БД."""
    if not summary_data:
        return

    outcome_map = {
        "closed_success": "resolved",
        "closed_cancelled": "cancelled",
        "closed_other": "other",
    }
    resolution_outcome = outcome_map.get(summary_data.get("status", ""), "other")
    legacy_status = map_legacy_thread_status("closed", resolution_outcome)
    
    cur.execute("""
        UPDATE email_threads
        SET 
            lifecycle_status = 'closed',
            resolution_outcome = %s,
            status = %s,
            resolution_detected_at = NOW(),
            summary_short = %s,
            summary_detailed = %s,
            key_decisions = %s,
            action_items = %s,
            topic_tags = %s,
            summary_generated_at = NOW(),
            summary_model = 'gpt-4.1-mini',
            updated_at = NOW()
        WHERE id = %s
    """, (
        resolution_outcome,
        legacy_status,
        summary_data.get("summary_short", ""),
        summary_data.get("summary_detailed", ""),
        summary_data.get("key_decisions", []),
        json.dumps(summary_data.get("action_items", []), ensure_ascii=False),
        summary_data.get("topic_tags", []),
        thread_id
    ))

    logger.info(
        f"Thread {thread_id}: сводка сохранена, lifecycle=closed, outcome={resolution_outcome}"
    )


def notify_thread_closed(thread_id: int, subject: str, summary_short: str, resolution_outcome: str):
    """Отправляет уведомление в Telegram о закрытии цепочки."""
    if not BOT_TOKEN:
        return
    
    # Получаем список пользователей с включённой рассылкой
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id FROM tg_full_analysis_settings
                WHERE send_full_analysis = TRUE AND user_id = %s
            """, (int(os.getenv("ADMIN_USER_ID", "0")),))
            users = [row[0] for row in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error(f"Notify thread closed: ошибка получения пользователей: {e}")
        return
    
    if not users:
        return
    
    # Формируем сообщение
    status_emoji = "✅" if resolution_outcome == "resolved" else "❌" if resolution_outcome == "cancelled" else "📧"
    message = (
        f"{status_emoji} Цепочка писем закрыта\n\n"
        f"📌 {subject[:100]}\n\n"
        f"📝 {summary_short}"
    )
    
    # Отправляем каждому пользователю
    for user_id in users:
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": user_id,
                    "text": message
                },
                timeout=10
            )
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
    
    logger.info(f"Thread {thread_id}: уведомления отправлены {len(users)} пользователям")


def process_thread_closure(cur, thread_id: int, body_text: str, subject: str, from_address: str = ""):
    """
    Основная функция: проверяет закрытие, генерирует сводку, уведомляет.
    Вызывается после каждого нового письма.
    """
    # Фильтр 1: пропускаем noreply и автоуведомления
    skip_senders = ['noreply@', 'no-reply@', 'robot@', 'donotreply@', 'автоответ']
    if any(s in (from_address or "").lower() for s in skip_senders):
        logger.info(f"Thread {thread_id}: пропускаем — отправитель noreply/автоуведомление")
        return

    # Фильтр 2: внешние отправители — проверяем есть ли ответ от наших сотрудников
    our_domains = ['totsamiy.com', 'lacannelle.ru']
    is_internal = any(d in (from_address or "").lower() for d in our_domains)
    if not is_internal:
        cur.execute("""
            SELECT COUNT(*) FROM email_messages
            WHERE thread_id = %s
            AND (from_address ILIKE %s OR from_address ILIKE %s)
        """, (thread_id, "%@totsamiy.com", "%@lacannelle.ru"))
        our_replies = cur.fetchone()[0]
        if our_replies == 0:
            logger.info(f"Thread {thread_id}: пропускаем — внешний отправитель, нет ответов от сотрудников")
            return

    # Проверяем, не обработана ли уже эта цепочка
    cur.execute("""
        SELECT lifecycle_status, resolution_detected_at FROM email_threads WHERE id = %s
    """, (thread_id,))
    row = cur.fetchone()
    
    if not row:
        return
    
    lifecycle_status, resolution_at = row
    
    # Если уже закрыта — пропускаем
    if lifecycle_status in ('closed', 'archived') and resolution_at:
        return
    
    # Проверяем маркеры закрытия
    is_closed, marker = check_thread_closure(body_text, subject)
    
    if not is_closed:
        return
    
    logger.info(f"Thread {thread_id}: обнаружен маркер закрытия '{marker}'")

    # Помечаем ветку как "нуждается в подтверждении" до генерации финального итога
    cur.execute("""
        UPDATE email_threads
        SET lifecycle_status = 'pending_resolution',
            status = 'pending_resolution',
            resolution_detected_at = COALESCE(resolution_detected_at, NOW()),
            updated_at = NOW()
        WHERE id = %s
    """, (thread_id,))
    
    # Получаем сообщения цепочки
    messages = get_thread_messages(cur, thread_id)
    
    if not messages:
        return
    
    # Генерируем сводку
    summary_data = generate_thread_summary(thread_id, messages, marker)
    
    if not summary_data:
        # Если не удалось сгенерировать — закрываем с outcome=other
        fallback_outcome = "other"
        cur.execute("""
            UPDATE email_threads
            SET lifecycle_status = 'closed',
                resolution_outcome = %s,
                status = %s,
                resolution_detected_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
        """, (fallback_outcome, map_legacy_thread_status("closed", fallback_outcome), thread_id))
        return
    
    # Сохраняем сводку
    save_thread_summary(cur, thread_id, summary_data, marker)
    
    # Автоизвлечение фактов из сводки
    try:
        extract_facts_from_thread_summary_sync(summary_data, thread_id, subject)
    except Exception as e:
        logger.debug(f"Fact extraction from thread error: {e}")
    
    # Отправляем уведомление
    outcome_map = {
        "closed_success": "resolved",
        "closed_cancelled": "cancelled",
        "closed_other": "other",
    }
    resolution_outcome = outcome_map.get(summary_data.get("status", ""), "other")
    notify_thread_closed(
        thread_id,
        subject,
        summary_data.get("summary_short", ""),
        resolution_outcome
    )


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
                    conn_db.commit()
                    new_uid_inbox = max(new_uid_inbox, parsed.uid)
                    stats['inbox'] += 1
                except Exception as e:
                    logger.error(f"Error processing inbox {parsed.uid}: {e}")
                    stats['errors'] += 1
                    conn_db.rollback()
            
            # Синхронизируем Sent
            sent_folder = find_sent_folder(conn_imap)
            new_uid_sent = last_uid_sent
            
            if sent_folder:
                uids = fetch_uids(conn_imap, sent_folder, last_uid_sent)
                logger.info(f"{email_addr}: found {len(uids)} new messages in {sent_folder}")
                
                for parsed in fetch_messages(conn_imap, sent_folder, uids):
                    try:
                        process_email(cur, parsed, mailbox_id, "Sent", "outbound")
                        conn_db.commit()
                        new_uid_sent = max(new_uid_sent, parsed.uid)
                        stats['sent'] += 1
                    except Exception as e:
                        logger.error(f"Error processing sent {parsed.uid}: {e}")
                        stats['errors'] += 1
                        conn_db.rollback()
            
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


def store_email_attachments(cur, email_id: int, parsed: ParsedEmail) -> tuple[int, int]:
    """Сохраняет вложения письма в bucket/local storage и пишет метаданные в БД."""
    if not parsed.attachments:
        return 0, 0

    saved = 0
    failed = 0

    for attachment in parsed.attachments:
        storage_path = ""
        storage_error = ""

        # 1) Пытаемся загрузить в bucket (при наличии конфигурации)
        bucket_path, bucket_error = upload_attachment_to_bucket(
            email_id=email_id,
            filename=attachment.filename,
            payload=attachment.payload,
            content_type=attachment.content_type,
            received_at=parsed.received_at,
        )
        if bucket_path:
            storage_path = bucket_path
        else:
            storage_error = "" if bucket_error == "bucket_not_configured" else bucket_error

        # 2) Fallback на локальное хранилище
        if not storage_path:
            local_path, local_error = save_attachment_locally(
                email_id=email_id,
                filename=attachment.filename,
                payload=attachment.payload,
                received_at=parsed.received_at,
            )
            if local_path:
                storage_path = local_path
                if storage_error:
                    storage_error = f"bucket upload failed: {storage_error}"
            else:
                if storage_error and local_error:
                    storage_error = f"bucket: {storage_error}; local: {local_error}"
                elif local_error:
                    storage_error = local_error

        if storage_path:
            analysis_status = classify_attachment_status(
                attachment.filename, attachment.size_bytes, attachment.content_type
            )
        else:
            analysis_status = "failed"

        cur.execute(
            """
            INSERT INTO email_attachments (
                message_id, filename, content_type, size_bytes, storage_path,
                analysis_status, analysis_error
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                email_id,
                attachment.filename[:255],
                attachment.content_type[:100],
                attachment.size_bytes,
                storage_path[:500] if storage_path else None,
                analysis_status,
                (storage_error or None),
            ),
        )

        if storage_path:
            saved += 1
        else:
            failed += 1
            logger.warning(f"Attachment storage failed for email_id={email_id}: {attachment.filename} ({storage_error})")

    cur.execute(
        "UPDATE email_messages SET attachment_count = %s WHERE id = %s",
        (saved + failed, email_id),
    )

    return saved, failed


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
            has_attachments, received_at, category
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (mailbox_id, folder, message_uid) DO NOTHING
        RETURNING id
    """, (
        parsed.uid, parsed.message_id, parsed.in_reply_to, parsed.references,
        thread_id, mailbox_id, folder, direction,
        parsed.from_address, parsed.to_addresses, parsed.cc_addresses,
        parsed.subject, parsed.subject_normalized, parsed.body_text, parsed.body_html,
        parsed.has_attachments, parsed.received_at,
        classify_email_category(parsed.from_address, str(parsed.to_addresses), '')
    ))
    
    # Получаем ID нового сообщения
    row = cur.fetchone()
    if row:
        email_id = row[0]
        # Canonical zone
        try:
            from canonical_helper import insert_source_document_email
            cur.execute("SELECT email FROM monitored_mailboxes WHERE id = %s", (mailbox_id,))
            mb_row = cur.fetchone()
            mb_email = mb_row[0] if mb_row else f'mailbox_{mailbox_id}'
            insert_source_document_email(cur, email_id, parsed, mb_email, direction)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Canonical email insert error: {e}")

        # Сохраняем вложения (bucket/local)
        if parsed.has_attachments and parsed.attachments:
            saved, failed = store_email_attachments(cur, email_id, parsed)
            logger.info(
                f"Email {email_id}: attachments saved={saved}, failed={failed}"
            )

        # Коммитим email перед индексацией, чтобы избежать сирот-embeddings
        cur.connection.commit()

        # Индексируем для векторного поиска
        chunks = build_email_chunks(
            subject=parsed.subject,
            body_text=parsed.body_text,
            body_html=parsed.body_html
        )

        for idx, chunk in enumerate(chunks):
            try:
                index_email_chunk(email_id=email_id, chunk_index=idx, content=chunk)
            except Exception as e:
                logger.warning(f"Email chunk indexing failed for email_id={email_id}, chunk={idx}: {e}")
        logger.debug(f"Indexed email {email_id} into {len(chunks)} chunks")
    
    # Обновляем статистику ветки
    update_thread_stats(cur, thread_id, parsed)
    
    # Проверяем закрытие цепочки
    process_thread_closure(cur, thread_id, parsed.body_text, parsed.subject, parsed.from_address)

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
