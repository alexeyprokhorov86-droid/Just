"""
Telegram Bot для логирования сообщений из групповых чатов.
Версия 2.0 - с ролями пользователей и расширенным контекстом.

Функции:
- Логирование всех сообщений в PostgreSQL
- Анализ изображений, PDF, Excel, Word, PowerPoint через Claude Vision
- Учёт ролей пользователей
- Контекст чата за 3 дня с учётом связанных сообщений
"""

import os
import re
import logging
import base64
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import anthropic
from telegram.ext import CallbackQueryHandler
from rag_agent import process_rag_query, index_new_message
from telegram.helpers import escape_markdown
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Загружаем переменные окружения
# Ищем .env в директории скрипта или в текущей директории
import pathlib
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

BOT_TOKEN = os.getenv("BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Подключение к БД
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ID администратора для запросов ролей (твой Telegram ID)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

# Название группы для отложенного анализа документов
DELAYED_ANALYSIS_CHAT = "Торты Отгрузки"

# Инициализация Claude клиента
claude_client = None
if ANTHROPIC_API_KEY:
    import httpx
    proxy_url = os.getenv("PROXY_URL")
    if proxy_url:
        http_client = httpx.Client(proxy=proxy_url)
        claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, http_client=http_client)
        logger.info(f"Claude Vision активирован через прокси {proxy_url}")
    else:
        claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        logger.info("Claude Vision активирован")
else:
    logger.warning("ANTHROPIC_API_KEY не установлен - анализ изображений отключён")

# Хранение состояния для назначения ролей
pending_role_assignments = {}


# ============================================================
# РАБОТА С БД
# ============================================================

def get_db_connection():
    """Создаёт подключение к PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def sanitize_table_name(chat_id: int, chat_title: str) -> str:
    """Создаёт безопасное имя таблицы из ID и названия чата."""
    translit_map = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
    }
    
    title_lower = chat_title.lower()
    transliterated = ''.join(translit_map.get(c, c) for c in title_lower)
    safe_title = re.sub(r'[^a-z0-9]+', '_', transliterated)
    safe_title = re.sub(r'_+', '_', safe_title).strip('_')
    safe_title = safe_title[:30] if safe_title else "unnamed"
    
    return f"tg_chat_{abs(chat_id)}_{safe_title}"


def ensure_table_exists(chat_id: int, chat_title: str) -> str:
    """Создаёт таблицу для чата, если она не существует."""
    table_name = sanitize_table_name(chat_id, chat_title)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    user_id BIGINT,
                    username VARCHAR(255),
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    message_text TEXT,
                    message_type VARCHAR(50) DEFAULT 'text',
                    reply_to_message_id BIGINT,
                    forward_from_user_id BIGINT,
                    media_file_id TEXT,
                    media_analysis TEXT,
                    content_text TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(message_id)
                )
            """).format(sql.Identifier(table_name)))
            
            cur.execute(sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (timestamp)
            """).format(
                sql.Identifier(f"idx_{table_name}_timestamp"),
                sql.Identifier(table_name)
            ))
            
            cur.execute(sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (user_id)
            """).format(
                sql.Identifier(f"idx_{table_name}_user_id"),
                sql.Identifier(table_name)
            ))
            
            cur.execute(sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} USING gin(to_tsvector('russian', COALESCE(message_text, '') || ' ' || COALESCE(media_analysis, '') || ' ' || COALESCE(content_text, '')))
            """).format(
                sql.Identifier(f"idx_{table_name}_fts"),
                sql.Identifier(table_name)
            ))
            
            # Таблица метаданных чатов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tg_chats_metadata (
                    chat_id BIGINT PRIMARY KEY,
                    chat_title VARCHAR(255),
                    table_name VARCHAR(100),
                    chat_type VARCHAR(50),
                    added_at TIMESTAMPTZ DEFAULT NOW(),
                    last_message_at TIMESTAMPTZ,
                    total_messages INTEGER DEFAULT 0
                )
            """)
            
            # Таблица ролей пользователей
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tg_user_roles (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    username VARCHAR(255),
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    role VARCHAR(255),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(user_id, chat_id)
                )
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_roles_user ON tg_user_roles(user_id)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_roles_chat ON tg_user_roles(chat_id)
            """)
            
            cur.execute("""
                INSERT INTO tg_chats_metadata (chat_id, chat_title, table_name, chat_type, last_message_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (chat_id) DO UPDATE SET
                    chat_title = EXCLUDED.chat_title,
                    last_message_at = NOW()
            """, (chat_id, chat_title, table_name, "group"))
            
            conn.commit()
            logger.info(f"Таблица {table_name} готова для чата '{chat_title}'")
            
    finally:
        conn.close()
    
    return table_name


def save_message(table_name: str, message_data: dict):
    """Сохраняет сообщение в таблицу чата."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                INSERT INTO {} (
                    message_id, user_id, username, first_name, last_name,
                    message_text, message_type, reply_to_message_id,
                    forward_from_user_id, media_file_id, media_analysis, content_text, timestamp
                ) VALUES (
                    %(message_id)s, %(user_id)s, %(username)s, %(first_name)s, %(last_name)s,
                    %(message_text)s, %(message_type)s, %(reply_to_message_id)s,
                    %(forward_from_user_id)s, %(media_file_id)s, %(media_analysis)s, %(content_text)s, %(timestamp)s
                )
                ON CONFLICT (message_id) DO UPDATE SET
                    message_text = EXCLUDED.message_text,
                    media_analysis = EXCLUDED.media_analysis,
                    content_text = EXCLUDED.content_text
            """).format(sql.Identifier(table_name)), message_data)
            conn.commit()
    finally:
        conn.close()


# ============================================================
# РАБОТА С РОЛЯМИ ПОЛЬЗОВАТЕЛЕЙ
# ============================================================

def get_user_role(user_id: int, chat_id: int) -> str | None:
    """Получает роль пользователя в чате."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT role FROM tg_user_roles
                WHERE user_id = %s AND chat_id = %s
            """, (user_id, chat_id))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"Ошибка получения роли: {e}")
        return None
    finally:
        conn.close()


def set_user_role(user_id: int, chat_id: int, role: str, username: str = None, first_name: str = None, last_name: str = None):
    """Устанавливает роль пользователя в чате."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tg_user_roles (user_id, chat_id, username, first_name, last_name, role, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, chat_id) DO UPDATE SET
                    role = EXCLUDED.role,
                    username = COALESCE(EXCLUDED.username, tg_user_roles.username),
                    first_name = COALESCE(EXCLUDED.first_name, tg_user_roles.first_name),
                    last_name = COALESCE(EXCLUDED.last_name, tg_user_roles.last_name),
                    updated_at = NOW()
            """, (user_id, chat_id, username, first_name, last_name, role))
            conn.commit()
            logger.info(f"Роль '{role}' установлена для пользователя {user_id} в чате {chat_id}")
    except Exception as e:
        logger.error(f"Ошибка установки роли: {e}")
    finally:
        conn.close()

# ============================================================
# НАСТРОЙКИ РАССЫЛКИ ПОЛНОГО АНАЛИЗА В ЛИЧКУ
# ============================================================

def get_user_analysis_setting(user_id: int) -> bool:
    """Проверяет, включена ли у пользователя рассылка полного анализа."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT send_full_analysis 
                FROM tg_full_analysis_settings 
                WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            return row[0] if row else False
    except Exception as e:
        logger.error(f"Ошибка получения настройки анализа: {e}")
        return False
    finally:
        conn.close()


def set_user_analysis_setting(user_id: int, username: str, first_name: str, enabled: bool):
    """Устанавливает настройку рассылки полного анализа."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tg_full_analysis_settings 
                    (user_id, username, first_name, send_full_analysis, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    send_full_analysis = EXCLUDED.send_full_analysis,
                    username = COALESCE(EXCLUDED.username, tg_full_analysis_settings.username),
                    first_name = COALESCE(EXCLUDED.first_name, tg_full_analysis_settings.first_name),
                    updated_at = NOW()
            """, (user_id, username, first_name, enabled))
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка установки настройки анализа: {e}")
    finally:
        conn.close()


def get_users_with_full_analysis_enabled() -> list:
    """Возвращает список user_id с включённой рассылкой."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, username, first_name
                FROM tg_full_analysis_settings 
                WHERE send_full_analysis = TRUE
            """)
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения списка пользователей: {e}")
        return []
    finally:
        conn.close()


def get_users_without_roles(chat_id: int, table_name: str) -> list:
    """Получает список пользователей без ролей в чате."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                SELECT DISTINCT m.user_id, m.username, m.first_name, m.last_name
                FROM {} m
                LEFT JOIN tg_user_roles r ON m.user_id = r.user_id AND r.chat_id = %s
                WHERE m.user_id IS NOT NULL 
                AND r.role IS NULL
                AND m.timestamp > NOW() - INTERVAL '30 days'
                ORDER BY m.first_name
            """).format(sql.Identifier(table_name)), (chat_id,))
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения пользователей без ролей: {e}")
        return []
    finally:
        conn.close()


# ============================================================
# КОНТЕКСТ ЧАТА
# ============================================================

def get_full_chat_context(table_name: str, chat_id: int, chat_title: str, hours: int = 192) -> str:
    """Получает полный контекст чата с ролями и связанными сообщениями за 8 дней."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Получаем участников с ролями
            cur.execute(sql.SQL("""
                SELECT DISTINCT 
                    m.user_id,
                    COALESCE(m.first_name, m.username, 'Неизвестный') as name,
                    m.last_name,
                    r.role
                FROM {} m
                LEFT JOIN tg_user_roles r ON m.user_id = r.user_id AND r.chat_id = %s
                WHERE m.timestamp > NOW() - INTERVAL '%s hours'
                AND m.user_id IS NOT NULL
            """).format(sql.Identifier(table_name)), (chat_id, hours))
            
            participants = cur.fetchall()
            
            # Получаем сообщения за период
            cur.execute(sql.SQL("""
                SELECT 
                    m.message_id,
                    m.user_id,
                    COALESCE(m.first_name, m.username, 'Неизвестный') as first_name,
                    m.last_name,
                    r.role,
                    m.message_text,
                    m.media_analysis,
                    m.message_type,
                    m.reply_to_message_id,
                    m.timestamp
                FROM {} m
                LEFT JOIN tg_user_roles r ON m.user_id = r.user_id AND r.chat_id = %s
                WHERE m.timestamp > NOW() - INTERVAL '%s hours'
                AND (m.message_text IS NOT NULL AND m.message_text != '' 
                     OR m.media_analysis IS NOT NULL AND m.media_analysis != '')
                ORDER BY m.timestamp ASC
            """).format(sql.Identifier(table_name)), (chat_id, hours))
            
            messages = cur.fetchall()
            
            # Собираем ID для поиска связанных сообщений
            reply_ids = [m[8] for m in messages if m[8] is not None]
            message_ids = [m[0] for m in messages]
            
            # Находим связанные сообщения за пределами периода
            missing_ids = [rid for rid in reply_ids if rid not in message_ids]
            linked_messages = {}
            
            if missing_ids:
                placeholders = ','.join(['%s'] * len(missing_ids))
                cur.execute(sql.SQL(f"""
                    SELECT 
                        m.message_id,
                        COALESCE(m.first_name, m.username, 'Неизвестный') as first_name,
                        m.last_name,
                        r.role,
                        m.message_text,
                        m.media_analysis,
                        m.message_type,
                        m.timestamp
                    FROM {{}} m
                    LEFT JOIN tg_user_roles r ON m.user_id = r.user_id AND r.chat_id = %s
                    WHERE m.message_id IN ({placeholders})
                """).format(sql.Identifier(table_name)), [chat_id] + missing_ids)
                
                for row in cur.fetchall():
                    linked_messages[row[0]] = row
            
            if not messages and not participants:
                return ""
            
            # Формируем контекст
            context_parts = []
            
            # Информация о чате
            context_parts.append(f"=== ЧАТ: {chat_title} ===\n")
            
            # Участники с ролями
            context_parts.append("УЧАСТНИКИ ЧАТА:")
            for user_id, name, last_name, role in participants:
                full_name = f"{name} {last_name}" if last_name else name
                role_str = f" — {role}" if role else " — роль не указана"
                context_parts.append(f"  • {full_name}{role_str}")
            context_parts.append("")
            
            # Сообщения
            context_parts.append("=== ИСТОРИЯ СООБЩЕНИЙ (последние 8 дней) ===\n")
            
            for msg_id, user_id, first_name, last_name, role, text, analysis, msg_type, reply_to, ts in messages:
                date_str = ts.strftime("%d.%m.%Y")
                time_str = ts.strftime("%H:%M")
                full_name = f"{first_name} {last_name}" if last_name else first_name
                role_str = f" [{role}]" if role else ""
                
                msg_parts = [f"[{date_str} {time_str}] {full_name}{role_str}:"]
                
                # Если это ответ на другое сообщение
                if reply_to:
                    linked = None
                    # Сначала ищем в связанных сообщениях за пределами периода
                    if reply_to in linked_messages:
                        linked = linked_messages[reply_to]
                        linked_name = f"{linked[1]} {linked[2]}" if linked[2] else linked[1]
                        linked_role = f" [{linked[3]}]" if linked[3] else ""
                        linked_date = linked[7].strftime("%d.%m.%Y %H:%M")
                        linked_content = linked[4] if linked[4] else linked[5] if linked[5] else "[медиа]"
                        linked_content = linked_content[:300] + "..." if len(linked_content) > 300 else linked_content
                    else:
                        # Ищем в текущих сообщениях
                        for m in messages:
                            if m[0] == reply_to:
                                linked_name = f"{m[2]} {m[3]}" if m[3] else m[2]
                                linked_role = f" [{m[4]}]" if m[4] else ""
                                linked_date = m[9].strftime("%d.%m.%Y %H:%M")
                                linked_content = m[5] if m[5] else m[6] if m[6] else "[медиа]"
                                linked_content = linked_content[:300] + "..." if len(linked_content) > 300 else linked_content
                                linked = True
                                break
                    
                    if linked:
                        msg_parts.append(f"  ↳ В ОТВЕТ НА ({linked_date}, {linked_name}{linked_role}):")
                        msg_parts.append(f"    \"{linked_content}\"")
                
                if text and text.strip():
                    msg_parts.append(f"  {text[:3000]}")
                
                if analysis and analysis.strip():
                    analysis_short = analysis[:1600] + "..." if len(analysis) > 1600 else analysis
                    msg_parts.append(f"  [АНАЛИЗ {msg_type.upper()}]: {analysis_short}")
                
                context_parts.append("\n".join(msg_parts))
                context_parts.append("")
            
            return "\n".join(context_parts)
            
    except Exception as e:
        logger.error(f"Ошибка получения контекста чата: {e}")
        return ""
    finally:
        conn.close()


# ============================================================
# ПОСТРОЕНИЕ ПРОМПТА
# ============================================================

def build_analysis_prompt(doc_type: str, doc_content: str, context: str, filename: str = "") -> str:
    """Создаёт промпт для анализа документа с учётом контекста чата."""
    
    prompt = f"""Ты — участник рабочего чата, который получил документ от коллеги.
Твоя задача — проанализировать документ так, как его воспримут участники чата, учитывая контекст обсуждения.

"""
    
    if context:
        prompt += f"""{context}

"""
    
    prompt += f"""=== ПОЛУЧЕННЫЙ ДОКУМЕНТ ===
Тип: {doc_type}
Файл: {filename}
{doc_content if doc_content != "[Изображение прикреплено]" else ""}

=== ИНСТРУКЦИИ ПО АНАЛИЗУ ===

1. ПРИОРИТЕТ КОНТЕКСТА (ВАЖНО!):
   Если из истории чата понятно, зачем был отправлен этот документ — анализируй его ИМЕННО в этом контексте:
   - Если обсуждали проблему с поставщиком → фокусируйся на данных этого поставщика
   - Если просили проверить цены → сравни с тем, что обсуждалось
   - Если ждали отчёт по конкретной теме → выдели именно эту информацию
   - Если документ — ответ на вопрос → дай ответ на этот вопрос

2. УЧИТЫВАЙ РОЛИ УЧАСТНИКОВ:
   - Кто отправил документ и какая у него роль
   - Кому предназначен документ
   - Какие вопросы могут возникнуть у участников с разными ролями

3. ЕСЛИ КОНТЕКСТ НЕ ЯСЕН:
   Только тогда делай стандартный анализ:
   - Тип документа
   - Ключевые данные (даты, суммы, контрагенты, товары)
   - Основное содержание

4. ФОРМАТ ОТВЕТА:
   - Начни с главного: что этот документ значит для участников чата
   - Выдели ключевые данные в контексте обсуждения
   - Укажи, если что-то требует внимания или действий
   - Кратко, по делу, без воды

Проанализируй документ:"""
    
    return prompt


# ============================================================
# ИЗВЛЕЧЕНИЕ ТЕКСТОВОГО СОДЕРЖИМОГО
# ============================================================

async def extract_text_from_image(image_data: bytes, media_type: str) -> str:
    """Извлекает текст из изображения с помощью OCR через Claude Vision."""
    if not claude_client:
        return ""

    try:
        base64_image = base64.standard_b64encode(image_data).decode("utf-8")

        # Используем Claude Vision для OCR
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": base64_image,
                            },
                        },
                        {
                            "type": "text",
                            "text": "Извлеки весь текст, который видишь на этом изображении. Верни только текст, без дополнительных комментариев. Если текста нет, верни пустую строку."
                        }
                    ],
                }
            ],
        )

        extracted_text = response.content[0].text.strip()
        logger.info(f"Текст извлечен из изображения: {len(extracted_text)} символов")
        return extracted_text

    except Exception as e:
        logger.error(f"Ошибка извлечения текста из изображения: {e}")
        return ""


async def extract_text_from_pdf(pdf_data: bytes) -> str:
    """Извлекает текст из PDF файла."""
    try:
        # Пробуем извлечь текст напрямую
        try:
            import PyPDF2
            import io

            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_data))
            text_parts = []

            for page_num, page in enumerate(pdf_reader.pages[:50], 1):  # Максимум 50 страниц
                page_text = page.extract_text()
                if page_text.strip():
                    text_parts.append(f"=== Страница {page_num} ===\n{page_text}")

            if text_parts:
                return "\n\n".join(text_parts)
        except Exception as e:
            logger.warning(f"PyPDF2 не смог извлечь текст: {e}")

        # Если PyPDF2 не сработал, используем Claude Vision через pdf2image
        try:
            from pdf2image import convert_from_bytes
            images = convert_from_bytes(pdf_data, first_page=1, last_page=20)

            all_text = []
            for i, image in enumerate(images, 1):
                import io
                img_byte_arr = io.BytesIO()
                image.save(img_byte_arr, format='PNG')
                img_bytes = img_byte_arr.getvalue()

                page_text = await extract_text_from_image(img_bytes, "image/png")
                if page_text:
                    all_text.append(f"=== Страница {i} ===\n{page_text}")

            return "\n\n".join(all_text)
        except Exception as e:
            logger.error(f"Не удалось извлечь текст через OCR: {e}")
            return ""

    except Exception as e:
        logger.error(f"Ошибка извлечения текста из PDF: {e}")
        return ""


async def extract_text_from_word(file_data: bytes) -> str:
    """Извлекает текст из Word документа."""
    try:
        import io
        from docx import Document

        doc = Document(io.BytesIO(file_data))

        text_parts = []

        # Извлекаем параграфы
        for para in doc.paragraphs:
            if para.text.strip():
                text_parts.append(para.text)

        # Извлекаем таблицы
        for table in doc.tables:
            table_text = []
            for row in table.rows:
                row_text = " | ".join(cell.text.strip() for cell in row.cells)
                if row_text.strip():
                    table_text.append(row_text)
            if table_text:
                text_parts.append("\n" + "\n".join(table_text))

        return "\n".join(text_parts)

    except Exception as e:
        logger.error(f"Ошибка извлечения текста из Word: {e}")
        return ""


async def extract_csv_from_excel(file_data: bytes, filename: str = "") -> str:
    """Извлекает данные из Excel в формате CSV."""
    try:
        import io
        is_xls = filename.lower().endswith('.xls') and not filename.lower().endswith('.xlsx')

        csv_parts = []

        if is_xls:
            # Старый формат .xls
            try:
                import xlrd
                wb = xlrd.open_workbook(file_contents=file_data)

                for sheet_name in wb.sheet_names():
                    sheet = wb.sheet_by_name(sheet_name)
                    csv_parts.append(f"=== {sheet_name} ===")

                    for row_idx in range(sheet.nrows):
                        row_values = []
                        for col_idx in range(sheet.ncols):
                            cell_value = sheet.cell_value(row_idx, col_idx)
                            # Экранируем значения для CSV
                            if isinstance(cell_value, str) and (',' in cell_value or '"' in cell_value or '\n' in cell_value):
                                cell_value = f'"{cell_value.replace(chr(34), chr(34)+chr(34))}"'
                            row_values.append(str(cell_value) if cell_value else "")
                        csv_parts.append(",".join(row_values))
                    csv_parts.append("")  # Пустая строка между листами

            except Exception as e:
                logger.error(f"Ошибка чтения .xls: {e}")
                return ""
        else:
            # Новый формат .xlsx
            try:
                from openpyxl import load_workbook
                wb = load_workbook(io.BytesIO(file_data), read_only=True, data_only=True)

                for sheet_name in wb.sheetnames:
                    sheet = wb[sheet_name]
                    csv_parts.append(f"=== {sheet_name} ===")

                    for row in sheet.iter_rows(values_only=True):
                        row_values = []
                        for cell in row:
                            # Экранируем значения для CSV
                            cell_str = str(cell) if cell is not None else ""
                            if ',' in cell_str or '"' in cell_str or '\n' in cell_str:
                                cell_str = f'"{cell_str.replace(chr(34), chr(34)+chr(34))}"'
                            row_values.append(cell_str)
                        csv_parts.append(",".join(row_values))
                    csv_parts.append("")  # Пустая строка между листами

                wb.close()
            except Exception as e:
                logger.warning(f"openpyxl не смог открыть, пробуем xlrd: {e}")
                try:
                    import xlrd
                    wb = xlrd.open_workbook(file_contents=file_data)

                    for sheet_name in wb.sheet_names():
                        sheet = wb.sheet_by_name(sheet_name)
                        csv_parts.append(f"=== {sheet_name} ===")

                        for row_idx in range(sheet.nrows):
                            row_values = []
                            for col_idx in range(sheet.ncols):
                                cell_value = sheet.cell_value(row_idx, col_idx)
                                if isinstance(cell_value, str) and (',' in cell_value or '"' in cell_value or '\n' in cell_value):
                                    cell_value = f'"{cell_value.replace(chr(34), chr(34)+chr(34))}"'
                                row_values.append(str(cell_value) if cell_value else "")
                            csv_parts.append(",".join(row_values))
                        csv_parts.append("")
                except Exception as e2:
                    logger.error(f"Ошибка чтения Excel обоими методами: {e2}")
                    return ""

        return "\n".join(csv_parts)

    except Exception as e:
        logger.error(f"Ошибка извлечения CSV из Excel: {e}")
        return ""


async def extract_text_from_pptx(file_data: bytes) -> str:
    """Извлекает текст из PowerPoint презентации."""
    try:
        import io
        from pptx import Presentation

        prs = Presentation(io.BytesIO(file_data))

        text_parts = []

        for slide_num, slide in enumerate(prs.slides, 1):
            slide_text = [f"=== Слайд {slide_num} ==="]

            for shape in slide.shapes:
                if hasattr(shape, "text") and shape.text.strip():
                    slide_text.append(shape.text)

                # Извлекаем текст из таблиц
                if hasattr(shape, "table"):
                    table = shape.table
                    for row in table.rows:
                        row_text = " | ".join(cell.text.strip() for cell in row.cells)
                        if row_text:
                            slide_text.append(row_text)

            if len(slide_text) > 1:  # Если есть текст помимо заголовка
                text_parts.append("\n".join(slide_text))

        return "\n\n".join(text_parts)

    except Exception as e:
        logger.error(f"Ошибка извлечения текста из PowerPoint: {e}")
        return ""


async def extract_transcript_from_audio(audio_path: str) -> str:
    """Извлекает транскрипт из аудио файла с помощью Whisper."""
    try:
        import whisper

        model = whisper.load_model("base")
        result = model.transcribe(audio_path, language="ru", fp16=False)

        transcript = result.get("text", "").strip()
        logger.info(f"Аудио транскрибировано: {len(transcript)} символов")
        return transcript

    except Exception as e:
        logger.error(f"Ошибка транскрибирования аудио: {e}")
        return ""


# ============================================================
# АНАЛИЗ ДОКУМЕНТОВ
# ============================================================

async def analyze_image_with_claude(image_data: bytes, media_type: str, context: str = "", filename: str = "") -> str:
    """Анализирует изображение через Claude Vision."""
    if not claude_client:
        return ""
    
    try:
        base64_image = base64.standard_b64encode(image_data).decode("utf-8")
        
        prompt = build_analysis_prompt("Изображение", "[Изображение прикреплено]", context, filename)
        
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2500,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": base64_image,
                            },
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ],
                }
            ],
        )
        
        analysis = response.content[0].text
        logger.info(f"Изображение проанализировано: {len(analysis)} символов")
        return analysis
        
    except Exception as e:
        logger.error(f"Ошибка анализа изображения: {e}")
        return ""


async def analyze_pdf_with_claude(pdf_data: bytes, filename: str = "", context: str = "") -> str:
    """Анализирует PDF через Claude."""
    if not claude_client:
        return ""
    
    try:
        try:
            from pdf2image import convert_from_bytes
            images = convert_from_bytes(pdf_data, first_page=1, last_page=10)
        except Exception as e:
            logger.warning(f"Не удалось конвертировать PDF в изображения: {e}")
            base64_pdf = base64.standard_b64encode(pdf_data).decode("utf-8")
            
            prompt = build_analysis_prompt("PDF документ", "[PDF документ прикреплён]", context, filename)
            
            response = claude_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2500,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "document",
                                "source": {
                                    "type": "base64",
                                    "media_type": "application/pdf",
                                    "data": base64_pdf,
                                },
                            },
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ],
                    }
                ],
            )
            return response.content[0].text
        
        # Анализируем каждую страницу
        all_analysis = []
        for i, image in enumerate(images):
            import io
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_bytes = img_byte_arr.getvalue()
            
            page_context = f"Страница {i+1} документа {filename}"
            if context:
                page_context = context + f"\n\nТекущая страница: {i+1} из {len(images)}"
            
            analysis = await analyze_image_with_claude(img_bytes, "image/png", page_context, filename)
            if analysis:
                all_analysis.append(f"[Страница {i+1}]\n{analysis}")
        
        return "\n\n".join(all_analysis)
        
    except Exception as e:
        logger.error(f"Ошибка анализа PDF: {e}")
        return ""


async def analyze_excel_with_claude(file_data: bytes, filename: str = "", context: str = "") -> str:
    """Анализирует Excel файл через Claude. Поддерживает .xlsx и .xls форматы."""
    if not claude_client:
        return ""
    
    try:
        import io
        all_text = []
        
        # Определяем формат по расширению или пробуем оба
        is_xls = filename.lower().endswith('.xls') and not filename.lower().endswith('.xlsx')
        
        if is_xls:
            # Старый формат .xls
            try:
                import xlrd
                wb = xlrd.open_workbook(file_contents=file_data)
                
                for sheet_name in wb.sheet_names()[:5]:
                    sheet = wb.sheet_by_name(sheet_name)
                    all_text.append(f"=== Лист: {sheet_name} ===")
                    
                    rows_count = 0
                    for row_idx in range(min(sheet.nrows, 200)):
                        row_values = [str(sheet.cell_value(row_idx, col_idx)) if sheet.cell_value(row_idx, col_idx) else "" for col_idx in range(sheet.ncols)]
                        if any(row_values):
                            all_text.append(" | ".join(row_values))
                            rows_count += 1
                    
                    if rows_count == 200:
                        all_text.append("... (данные обрезаны)")
                        
            except Exception as e:
                logger.error(f"Ошибка чтения .xls: {e}")
                return ""
        else:
            # Новый формат .xlsx
            try:
                from openpyxl import load_workbook
                wb = load_workbook(io.BytesIO(file_data), read_only=True, data_only=True)
                
                for sheet_name in wb.sheetnames[:5]:
                    sheet = wb[sheet_name]
                    all_text.append(f"=== Лист: {sheet_name} ===")
                    
                    rows_count = 0
                    for row in sheet.iter_rows(max_row=200, values_only=True):
                        row_values = [str(cell) if cell is not None else "" for cell in row]
                        if any(row_values):
                            all_text.append(" | ".join(row_values))
                            rows_count += 1
                    
                    if rows_count == 200:
                        all_text.append("... (данные обрезаны)")
                
                wb.close()
            except Exception as e:
                # Может это .xls с неправильным расширением - пробуем xlrd
                logger.warning(f"openpyxl не смог открыть, пробуем xlrd: {e}")
                try:
                    import xlrd
                    wb = xlrd.open_workbook(file_contents=file_data)
                    
                    for sheet_name in wb.sheet_names()[:5]:
                        sheet = wb.sheet_by_name(sheet_name)
                        all_text.append(f"=== Лист: {sheet_name} ===")
                        
                        for row_idx in range(min(sheet.nrows, 200)):
                            row_values = [str(sheet.cell_value(row_idx, col_idx)) if sheet.cell_value(row_idx, col_idx) else "" for col_idx in range(sheet.ncols)]
                            if any(row_values):
                                all_text.append(" | ".join(row_values))
                except Exception as e2:
                    logger.error(f"Ошибка чтения Excel обоими методами: {e2}")
                    return ""
        
        excel_content = "\n".join(all_text)
        
        if len(excel_content) > 15000:
            excel_content = excel_content[:15000] + "\n... (данные обрезаны)"
        
        if not excel_content.strip():
            return "Файл пуст или не удалось прочитать содержимое."
        
        prompt = build_analysis_prompt("Excel таблица", f"Содержимое файла:\n{excel_content}", context, filename)
        
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        
        return response.content[0].text
        
    except Exception as e:
        logger.error(f"Ошибка анализа Excel: {e}")
        return ""


async def analyze_word_with_claude(file_data: bytes, filename: str = "", context: str = "") -> str:
    """Анализирует Word файл через Claude."""
    if not claude_client:
        return ""
    
    try:
        import io
        from docx import Document
        
        doc = Document(io.BytesIO(file_data))
        
        paragraphs = []
        for para in doc.paragraphs[:500]:
            if para.text.strip():
                paragraphs.append(para.text)
        
        for table in doc.tables[:10]:
            for row in table.rows:
                row_text = " | ".join(cell.text.strip() for cell in row.cells if cell.text.strip())
                if row_text:
                    paragraphs.append(row_text)
        
        word_content = "\n".join(paragraphs)
        
        if len(word_content) > 15000:
            word_content = word_content[:15000] + "\n... (текст обрезан)"
        
        prompt = build_analysis_prompt("Word документ", f"Содержимое документа:\n{word_content}", context, filename)
        
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        
        return response.content[0].text
        
    except Exception as e:
        logger.error(f"Ошибка анализа Word: {e}")
        return ""


async def analyze_pptx_with_claude(file_data: bytes, filename: str = "", context: str = "") -> str:
    """Анализирует PowerPoint файл через Claude."""
    if not claude_client:
        return ""
    
    try:
        import io
        from pptx import Presentation
        
        prs = Presentation(io.BytesIO(file_data))
        
        slides_text = []
        for i, slide in enumerate(prs.slides[:30], 1):
            slide_content = [f"=== Слайд {i} ==="]
            
            for shape in slide.shapes:
                if hasattr(shape, "text") and shape.text.strip():
                    slide_content.append(shape.text.strip())
            
            if len(slide_content) > 1:
                slides_text.append("\n".join(slide_content))
        
        pptx_content = "\n\n".join(slides_text)
        
        if len(pptx_content) > 15000:
            pptx_content = pptx_content[:15000] + "\n... (текст обрезан)"
        
        prompt = build_analysis_prompt("PowerPoint презентация", f"Содержимое презентации:\n{pptx_content}", context, filename)
        
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        
        return response.content[0].text
        
    except Exception as e:
        logger.error(f"Ошибка анализа PowerPoint: {e}")
        return ""

async def analyze_video_with_gemini(file_data: bytes, filename: str = "", context: str = "") -> str:
    """Анализирует видео через Gemini 3 Flash (поддерживает видео напрямую)."""
    import requests
    import base64
    
    ROUTERAI_API_KEY = os.getenv("ROUTERAI_API_KEY")
    ROUTERAI_BASE_URL = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")
    
    if not ROUTERAI_API_KEY:
        logger.warning("ROUTERAI_API_KEY не установлен — анализ видео через Gemini недоступен")
        # Fallback на старый метод с Whisper + Claude
        return await analyze_video_with_whisper(file_data, filename, context)
    
    try:
        # Кодируем видео в base64
        video_base64 = base64.standard_b64encode(file_data).decode("utf-8")
        
        # Определяем mime type
        ext = filename.lower().split('.')[-1] if filename else 'mp4'
        mime_types = {
            'mp4': 'video/mp4',
            'avi': 'video/x-msvideo',
            'mov': 'video/quicktime',
            'mkv': 'video/x-matroska',
            'webm': 'video/webm'
        }
        mime_type = mime_types.get(ext, 'video/mp4')
        
        # Строим промпт
        prompt = build_analysis_prompt("Видео", "[Видео прикреплено]", context, filename)
        
        # Запрос к Gemini через RouterAI
        url = f"{ROUTERAI_BASE_URL}/chat/completions"
        headers = {
            "Authorization": f"Bearer {ROUTERAI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "google/gemini-3-flash-preview",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{video_base64}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 2000
        }
        
        response = requests.post(url, headers=headers, json=data, timeout=120)
        result = response.json()
        
        if "choices" in result and len(result["choices"]) > 0:
            analysis = result["choices"][0]["message"]["content"]
            logger.info(f"Видео проанализировано через Gemini: {len(analysis)} символов")
            return analysis
        else:
            logger.error(f"Ошибка Gemini API: {result}")
            return ""
            
    except Exception as e:
        logger.error(f"Ошибка анализа видео через Gemini: {e}")
        return ""


async def analyze_video_with_whisper(file_data: bytes, filename: str = "", context: str = "") -> str:
    """Fallback: анализирует видео через Whisper (только аудио) + Claude."""
    if not claude_client:
        return ""
    
    try:
        import tempfile
        import subprocess
        import whisper
        
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as video_file:
            video_file.write(file_data)
            video_path = video_file.name
        
        audio_path = video_path.replace('.mp4', '.wav')
        subprocess.run([
            'ffmpeg', '-i', video_path, '-vn', '-acodec', 'pcm_s16le',
            '-ar', '16000', '-ac', '1', audio_path, '-y'
        ], capture_output=True, timeout=120)
        
        transcript = ""
        if os.path.exists(audio_path) and os.path.getsize(audio_path) > 0:
            model = whisper.load_model("base")
            result = model.transcribe(audio_path, language="ru")
            transcript = result["text"]
        
        os.unlink(video_path)
        if os.path.exists(audio_path):
            os.unlink(audio_path)
        
        if not transcript.strip():
            return "Видео без речи или речь не распознана."
        
        if len(transcript) > 10000:
            transcript = transcript[:10000] + "... (транскрипция обрезана)"
        
        prompt = build_analysis_prompt("Видео (транскрипция аудио)", f"Транскрипция:\n{transcript}", context, filename)
        
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        
        return response.content[0].text
        
    except Exception as e:
        logger.error(f"Ошибка анализа видео через Whisper: {e}")
        return ""


# ============================================================
# ОБРАБОТКА МЕДИАФАЙЛОВ
# ============================================================

async def download_and_analyze_media(bot, message, table_name: str = None) -> tuple[str, str, str]:
    """Скачивает и анализирует медиафайл с учётом контекста чата.

    Возвращает: (media_type_str, media_analysis, content_text)
    """
    media_analysis = ""
    content_text = ""
    media_type_str = "media"

    try:
        file = None
        media_type = None
        filename = ""

        if message.photo:
            file = await bot.get_file(message.photo[-1].file_id)
            media_type = "image/jpeg"
            media_type_str = "photo"
            filename = "photo.jpg"
        elif message.voice:
            file = await bot.get_file(message.voice.file_id)
            media_type = "voice"
            media_type_str = "voice"
            filename = "voice.ogg"
        elif message.audio:
            file = await bot.get_file(message.audio.file_id)
            media_type = "audio"
            media_type_str = "audio"
            filename = message.audio.file_name or "audio.mp3"
        elif message.video:
            if message.video.file_size and message.video.file_size < 40 * 1024 * 1024:
                file = await bot.get_file(message.video.file_id)
                media_type = "video"
                media_type_str = "video"
                filename = "video.mp4"
            else:
                logger.warning("Видео слишком большое для анализа")
                return "video", "", ""
        elif message.document:
            doc = message.document
            filename = doc.file_name or ""
            filename_lower = filename.lower()

            if doc.mime_type and doc.mime_type.startswith("image/"):
                file = await bot.get_file(doc.file_id)
                media_type = doc.mime_type
                media_type_str = "image"
            elif doc.mime_type == "application/pdf" or filename_lower.endswith(".pdf"):
                file = await bot.get_file(doc.file_id)
                media_type = "application/pdf"
                media_type_str = "pdf"
            elif filename_lower.endswith(('.xlsx', '.xls')):
                file = await bot.get_file(doc.file_id)
                media_type = "excel"
                media_type_str = "excel"
            elif filename_lower.endswith(('.docx', '.doc')):
                file = await bot.get_file(doc.file_id)
                media_type = "word"
                media_type_str = "word"
            elif filename_lower.endswith(('.pptx', '.ppt')):
                file = await bot.get_file(doc.file_id)
                media_type = "powerpoint"
                media_type_str = "powerpoint"
            elif filename_lower.endswith(('.mp4', '.avi', '.mov', '.mkv')):
                if doc.file_size and doc.file_size < 40 * 1024 * 1024:
                    file = await bot.get_file(doc.file_id)
                    media_type = "video"
                    media_type_str = "video"
                else:
                    logger.warning("Видео слишком большое для анализа")
                    return "video", "", ""
            else:
                media_type_str = "document"
                return media_type_str, "", ""
        else:
            return media_type_str, "", ""

        if not file:
            return media_type_str, "", ""

        # Скачиваем файл
        file_data = await file.download_as_bytearray()

        # Получаем полный контекст чата (8 дней = 192 часа)
        context = ""
        if table_name and message.chat:
            chat_context = get_full_chat_context(
                table_name,
                message.chat.id,
                message.chat.title or "Чат",
                192  # 8 дней
            )
            if chat_context:
                context = chat_context

        # Добавляем подпись если есть
        caption = message.caption or ""
        if caption:
            context += f"\n\n=== ПОДПИСЬ К ТЕКУЩЕМУ ФАЙЛУ ===\n{caption}"

        # Анализируем И извлекаем содержимое в зависимости от типа
        if media_type == "voice" or media_type == "audio":
            # Для голосовых сообщений и аудио - транскрибируем
            import tempfile
            suffix = '.ogg' if media_type == "voice" else '.mp3'
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(bytes(file_data))
                audio_path = tmp.name
            try:
                content_text = await extract_transcript_from_audio(audio_path)

                # Создаем анализ на основе транскрипта
                if content_text:
                    prompt = build_analysis_prompt(
                        "Голосовое сообщение" if media_type == "voice" else "Аудио файл",
                        f"Транскрипция:\n{content_text}",
                        context,
                        filename
                    )

                    if claude_client:
                        response = claude_client.messages.create(
                            model="claude-sonnet-4-20250514",
                            max_tokens=2500,
                            messages=[{"role": "user", "content": prompt}],
                        )
                        media_analysis = response.content[0].text
                    else:
                        media_analysis = f"Транскрипция: {content_text}"
                else:
                    media_analysis = "Не удалось распознать речь в аудио."
            finally:
                if os.path.exists(audio_path):
                    os.unlink(audio_path)
        elif media_type == "application/pdf":
            media_analysis = await analyze_pdf_with_claude(bytes(file_data), filename, context)
            content_text = await extract_text_from_pdf(bytes(file_data))
        elif media_type and media_type.startswith("image/"):
            media_analysis = await analyze_image_with_claude(bytes(file_data), media_type, context, filename)
            content_text = await extract_text_from_image(bytes(file_data), media_type)
        elif media_type == "excel":
            media_analysis = await analyze_excel_with_claude(bytes(file_data), filename, context)
            content_text = await extract_csv_from_excel(bytes(file_data), filename)
        elif media_type == "word":
            media_analysis = await analyze_word_with_claude(bytes(file_data), filename, context)
            content_text = await extract_text_from_word(bytes(file_data))
        elif media_type == "powerpoint":
            media_analysis = await analyze_pptx_with_claude(bytes(file_data), filename, context)
            content_text = await extract_text_from_pptx(bytes(file_data))
        elif media_type == "video":
            media_analysis = await analyze_video_with_gemini(bytes(file_data), filename, context)
            # Для видео извлекаем транскрипт аудио
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as tmp:
                tmp.write(bytes(file_data))
                video_path = tmp.name
            try:
                content_text = await extract_transcript_from_audio(video_path)
            finally:
                if os.path.exists(video_path):
                    os.unlink(video_path)

    except Exception as e:
        logger.error(f"Ошибка обработки медиа: {e}")

    return media_type_str, media_analysis, content_text


def determine_message_type(message) -> tuple[str, str | None]:
    """Определяет тип сообщения и file_id если есть медиа."""
    if message.photo:
        return "photo", message.photo[-1].file_id
    elif message.video:
        return "video", message.video.file_id
    elif message.audio:
        return "audio", message.audio.file_id
    elif message.voice:
        return "voice", message.voice.file_id
    elif message.video_note:
        return "video_note", message.video_note.file_id
    elif message.document:
        return "document", message.document.file_id
    elif message.sticker:
        return "sticker", message.sticker.file_id
    elif message.animation:
        return "animation", message.animation.file_id
    elif message.location:
        return "location", None
    elif message.contact:
        return "contact", None
    elif message.poll:
        return "poll", None
    else:
        return "text", None


async def analyze_daily_documents(bot, chat_id: int, chat_title: str):
    """Анализирует все документы за день для указанного чата.

    Вызывается планировщиком в конце дня (23:55).
    """
    logger.info(f"Запуск анализа документов за день для чата {chat_title} ({chat_id})")

    table_name = ensure_table_exists(chat_id, chat_title)

    # Получаем все документы за сегодня без анализа
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Ищем документы за сегодня, которые не имеют анализа
            cur.execute(f"""
                SELECT message_id, media_file_id, message_text, message_type, timestamp
                FROM {table_name}
                WHERE DATE(timestamp) = CURRENT_DATE
                AND message_type IN ('pdf', 'excel', 'word', 'powerpoint', 'document', 'photo')
                AND (media_analysis IS NULL OR media_analysis = '')
                ORDER BY timestamp
            """)
            documents = cur.fetchall()
    finally:
        conn.close()

    if not documents:
        logger.info(f"Нет документов для анализа в чате {chat_title}")
        return

    logger.info(f"Найдено {len(documents)} документов для анализа в чате {chat_title}")

    # Формируем сводный анализ всех документов за день
    all_docs_info = []
    for idx, (msg_id, file_id, caption, msg_type, timestamp) in enumerate(documents, 1):
        time_str = timestamp.strftime("%H:%M")
        doc_info = f"{idx}. Документ {msg_type} в {time_str}"
        if caption:
            doc_info += f": {caption[:100]}"
        all_docs_info.append(doc_info)

    # Получаем контекст чата за день
    context = get_full_chat_context(table_name, chat_id, chat_title, 24)  # 24 часа

    # Создаем сводный анализ
    summary_prompt = f"""Проанализируй все документы, отправленные в чат "{chat_title}" за сегодня.

=== СПИСОК ДОКУМЕНТОВ ЗА ДЕНЬ ===
{chr(10).join(all_docs_info)}

=== КОНТЕКСТ ЧАТА ЗА ДЕНЬ ===
{context}

Создай краткий сводный анализ всех документов:
1. Общая тематика документов
2. Ключевые данные и цифры
3. Важные моменты и выводы
4. Связь между документами (если есть)

Анализ должен быть структурированным и информативным."""

    try:
        if claude_client:
            response = claude_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=3000,
                messages=[{"role": "user", "content": summary_prompt}],
            )
            summary_analysis = response.content[0].text

            # Сохраняем сводный анализ в БД для последнего документа дня
            # (или можно создать отдельную таблицу для дневных отчетов)
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    # Обновляем последний документ дня со сводным анализом
                    last_msg_id = documents[-1][0]
                    cur.execute(f"""
                        UPDATE {table_name}
                        SET media_analysis = %s
                        WHERE message_id = %s
                    """, (f"📊 СВОДНЫЙ АНАЛИЗ ДОКУМЕНТОВ ЗА ДЕНЬ\n\n{summary_analysis}", last_msg_id))
                    conn.commit()
            finally:
                conn.close()

            # Отправляем сводный анализ в чат
            await bot.send_message(
                chat_id=chat_id,
                text=f"📊 *Сводный анализ документов за {datetime.now().strftime('%d.%m.%Y')}*\n\n"
                     f"Проанализировано документов: {len(documents)}\n\n"
                     f"{summary_analysis}",
                parse_mode="Markdown"
            )

            logger.info(f"Сводный анализ отправлен в чат {chat_title}")

    except Exception as e:
        logger.error(f"Ошибка при создании сводного анализа: {e}")


# ============================================================
# ОБРАБОТЧИКИ СООБЩЕНИЙ
# ============================================================

async def log_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик для логирования всех сообщений."""
    message = update.message or update.edited_message
    
    if not message or not message.chat:
        return
    
    if message.chat.type not in ["group", "supergroup"]:
        return
    
    chat_id = message.chat.id
    chat_title = message.chat.title or f"Chat_{abs(chat_id)}"
    
    table_name = ensure_table_exists(chat_id, chat_title)
    
    message_type, media_file_id = determine_message_type(message)

    # Проверяем, нужно ли отложить анализ для этой группы
    is_delayed_chat = chat_title == DELAYED_ANALYSIS_CHAT

    # Анализируем медиа если есть (кроме группы с отложенным анализом)
    media_analysis = ""
    content_text = ""
    if message.photo or message.video or message.voice or message.audio or (message.document and (message.document.mime_type or message.document.file_name)):
        # Для группы "Торты Отгрузки" не анализируем сразу - анализ будет в конце дня
        if not is_delayed_chat:
            analyzed_type, media_analysis, content_text = await download_and_analyze_media(context.bot, message, table_name)
            if analyzed_type != "media":
                message_type = analyzed_type
        else:
            # Для отложенного анализа просто определяем тип документа
            if message.document:
                doc = message.document
                filename = doc.file_name or ""
                filename_lower = filename.lower()
                if doc.mime_type == "application/pdf" or filename_lower.endswith(".pdf"):
                    message_type = "pdf"
                elif filename_lower.endswith(('.xlsx', '.xls')):
                    message_type = "excel"
                elif filename_lower.endswith(('.docx', '.doc')):
                    message_type = "word"
                elif filename_lower.endswith(('.pptx', '.ppt')):
                    message_type = "powerpoint"

            logger.info(f"Документ {message_type} отложен для анализа в конце дня (чат: {chat_title})")

        # Отправляем результат анализа в чат (только если анализ был выполнен)
        if media_analysis:
            try:
                # Формируем краткий анализ (2-3 строки)
                lines = [l for l in media_analysis.split('\n') if l.strip()]
                summary = '\n'.join(lines[:3])
                if len(summary) > 350:
                    summary = summary[:350] + "..."
                
                # В чат — только краткий анализ
                filename = ""
                if message.document and message.document.file_name:
                    filename = f" ({message.document.file_name})"
                
                await message.reply_text(f"📄 Анализ{filename}:\n\n{summary}")
                
                # Рассылка полного анализа в личку тем, кто включил
                if len(media_analysis) > 400:  # Только если есть что добавить
                    chat_title = message.chat.title or "Чат"
                    sender_name = message.from_user.first_name if message.from_user else "Неизвестный"
                    
                    full_message = (
                        f"📄 *Полный анализ документа*\n\n"
                        f"📍 Чат: {chat_title}\n"
                        f"👤 Отправил: {sender_name}\n"
                        f"📎 Файл: {filename.strip(' ()') or message_type}\n\n"
                        f"{media_analysis}"
                    )
                    
                    # Получаем список всех пользователей с включённой рассылкой
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as cur:
                            # Все пользователи с включённой рассылкой (независимо от чата)
                            cur.execute("""
                                SELECT user_id
                                FROM tg_full_analysis_settings
                                WHERE send_full_analysis = TRUE
                            """)
                            users_to_notify = [row[0] for row in cur.fetchall()]
                    finally:
                        conn.close()
                    
                    # Отправляем в личку
                    for uid in users_to_notify:
                        try:
                            # Разбиваем если слишком длинный
                            if len(full_message) > 4000:
                                parts = [full_message[i:i+4000] for i in range(0, len(full_message), 4000)]
                                for i, part in enumerate(parts):
                                    await context.bot.send_message(
                                        chat_id=uid,
                                        text=part if i == 0 else f"...продолжение:\n\n{part}",
                                        parse_mode="Markdown"
                                    )
                            else:
                                await context.bot.send_message(
                                    chat_id=uid,
                                    text=full_message,
                                    parse_mode="Markdown"
                                )
                        except Exception as e:
                            logger.warning(f"Не удалось отправить анализ пользователю {uid}: {e}")
                            # Если бот заблокирован — отключаем рассылку
                            if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                                set_user_analysis_setting(uid, "", "", False)
                    
            except Exception as e:
                logger.error(f"Ошибка отправки анализа: {e}")
    
    text = message.text or message.caption or ""
    
    message_data = {
        "message_id": message.message_id,
        "user_id": message.from_user.id if message.from_user else None,
        "username": message.from_user.username if message.from_user else None,
        "first_name": message.from_user.first_name if message.from_user else None,
        "last_name": message.from_user.last_name if message.from_user else None,
        "message_text": text,
        "message_type": message_type,
        "reply_to_message_id": message.reply_to_message.message_id if message.reply_to_message else None,
        "forward_from_user_id": None,
        "media_file_id": media_file_id,
        "media_analysis": media_analysis,
        "content_text": content_text,
        "timestamp": message.date
    }

    save_message(table_name, message_data)

    # Индексируем для векторного поиска (включая извлеченное содержимое)
    content_for_index = (message_data.get("message_text") or "") + " " + (message_data.get("media_analysis") or "") + " " + (message_data.get("content_text") or "")
    if content_for_index.strip():
        await index_new_message(table_name, message_data["message_id"], content_for_index.strip())
    logger.info(f"Сохранено сообщение {message.message_id} ({message_type}) в {table_name}")
    
    # Проверяем, есть ли у пользователя роль
    if message.from_user and ADMIN_USER_ID and message.from_user.id != ADMIN_USER_ID:
        user_role = get_user_role(message.from_user.id, chat_id)
        if not user_role:
            # Запрашиваем роль у администратора (только для новых пользователей)
            await request_user_role(context.bot, message, chat_id, chat_title)


async def request_user_role(bot, message, chat_id: int, chat_title: str):
    """Запрашивает роль пользователя у администратора."""
    if not ADMIN_USER_ID:
        return
    
    user = message.from_user
    user_name = f"{user.first_name or ''} {user.last_name or ''}".strip() or user.username or f"User_{user.id}"
    
    try:
        await bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=f"👤 Новый пользователь без роли в чате \"{chat_title}\":\n\n"
                 f"Имя: {user_name}\n"
                 f"Username: @{user.username or 'нет'}\n"
                 f"ID: {user.id}\n\n"
                 f"Ответьте на это сообщение, указав роль пользователя.\n"
                 f"Например: Бухгалтер, Менеджер, Директор и т.д.",
            parse_mode="HTML"
        )
        
        # Сохраняем ожидание ответа
        pending_role_assignments[f"admin_{user.id}_{chat_id}"] = {
            "user_id": user.id,
            "chat_id": chat_id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name
        }
    except Exception as e:
        logger.error(f"Ошибка запроса роли: {e}")


async def handle_admin_role_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ответ администратора с ролью пользователя."""
    message = update.message
    
    if not message.reply_to_message or message.chat.type != "private":
        return
    
    if message.from_user.id != ADMIN_USER_ID:
        return
    
    # Ищем ожидающее назначение
    reply_text = message.reply_to_message.text or ""
    
    for key, pending in list(pending_role_assignments.items()):
        if key.startswith("admin_") and f"ID: {pending['user_id']}" in reply_text:
            role = message.text.strip()
            
            set_user_role(
                pending["user_id"],
                pending["chat_id"],
                role,
                pending["username"],
                pending["first_name"],
                pending["last_name"]
            )
            
            await message.reply_text(
                f"✅ Роль \"{role}\" назначена пользователю {pending['first_name'] or pending['username']}"
            )
            
            del pending_role_assignments[key]
            return


# ============================================================
# КОМАНДЫ
# ============================================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    if update.message.chat.type == "private":
        await update.message.reply_text(
            "👋 Привет! Я бот для логирования сообщений.\n\n"
            "📝 Сохраняю все сообщения в базу данных\n"
            "🖼 Анализирую документы через AI с учётом контекста чата\n"
            "👥 Учитываю роли участников\n"
            "🔍 Поддерживаю поиск по истории\n\n"
            "Команды:\n"
            "/roles - показать пользователей без ролей\n"
            "/stats - статистика чата\n"
            "/search <запрос> - поиск по сообщениям\n"
            "/analysis - настройка рассылки полного анализа документов\n\n"
            "Добавь меня в групповой чат!"
        )
    else:
        await update.message.reply_text(
            "✅ Бот активирован в этом чате.\n"
            "📝 Логирование сообщений\n"
            "🖼 Анализ документов с контекстом\n"
            "👥 Учёт ролей участников\n\n"
            "Команды: /roles, /stats, /search, /analysis"
        )


async def roles_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает пользователей без ролей."""
    message = update.message
    
    if message.chat.type not in ["group", "supergroup"]:
        await message.reply_text("Эта команда работает только в групповых чатах.")
        return
    
    chat_id = message.chat.id
    chat_title = message.chat.title or f"Chat_{abs(chat_id)}"
    table_name = sanitize_table_name(chat_id, chat_title)
    
    users_without_roles = get_users_without_roles(chat_id, table_name)
    
    if not users_without_roles:
        # Показываем всех пользователей с ролями
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT first_name, last_name, role 
                    FROM tg_user_roles 
                    WHERE chat_id = %s 
                    ORDER BY role, first_name
                """, (chat_id,))
                users_with_roles = cur.fetchall()
        finally:
            conn.close()
        
        if users_with_roles:
            response = "✅ Все пользователи имеют роли:\n\n"
            for first_name, last_name, role in users_with_roles:
                name = f"{first_name or ''} {last_name or ''}".strip() or "Без имени"
                response += f"• {name} — {role}\n"
            await message.reply_text(response)
        else:
            await message.reply_text("В этом чате пока нет пользователей с назначенными ролями.")
        return
    
    response = "👥 **Пользователи без ролей:**\n\n"
    for i, (user_id, username, first_name, last_name) in enumerate(users_without_roles, 1):
        name = f"{first_name or ''} {last_name or ''}".strip() or username or f"User_{user_id}"
        response += f"{i}. {name} (@{username or 'нет'})\n"
    
    response += "\n**Чтобы назначить роль**, ответьте на это сообщение в формате:\n"
    response += "`1 Директор`\n`2 Бухгалтер`\n`3 Менеджер`"
    
    sent_message = await message.reply_text(response, parse_mode="Markdown")
    
    pending_role_assignments[chat_id] = {
        "message_id": sent_message.message_id,
        "users": users_without_roles,
        "table_name": table_name
    }


async def handle_role_assignment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает назначение ролей в групповом чате."""
    message = update.message
    
    if not message.reply_to_message:
        return
    
    chat_id = message.chat.id
    
    # Обработка в личных сообщениях (ответ администратора)
    if message.chat.type == "private":
        await handle_admin_role_reply(update, context)
        return
    
    # Обработка в групповом чате
    if chat_id not in pending_role_assignments:
        return
    
    pending = pending_role_assignments[chat_id]
    
    if message.reply_to_message.message_id != pending["message_id"]:
        return
    
    lines = message.text.strip().split('\n')
    assigned = []
    
    for line in lines:
        parts = line.strip().split(' ', 1)
        if len(parts) != 2:
            continue
        
        try:
            index = int(parts[0]) - 1
            role = parts[1].strip()
            
            if 0 <= index < len(pending["users"]):
                user_id, username, first_name, last_name = pending["users"][index]
                set_user_role(user_id, chat_id, role, username, first_name, last_name)
                name = f"{first_name or ''} {last_name or ''}".strip() or username or f"User_{user_id}"
                assigned.append(f"{name} → {role}")
        except (ValueError, IndexError):
            continue
    
    if assigned:
        response = "✅ **Роли назначены:**\n" + "\n".join(assigned)
        await message.reply_text(response, parse_mode="Markdown")
        
        remaining = get_users_without_roles(chat_id, pending["table_name"])
        if remaining:
            await message.reply_text(f"Осталось пользователей без ролей: {len(remaining)}\nИспользуйте /roles чтобы продолжить.")
        else:
            del pending_role_assignments[chat_id]
    else:
        await message.reply_text("Не удалось распознать. Используйте формат:\n`1 Директор`", parse_mode="Markdown")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику по текущему чату."""
    message = update.message
    
    if message.chat.type not in ["group", "supergroup"]:
        await message.reply_text("Эта команда работает только в групповых чатах.")
        return
    
    chat_id = message.chat.id
    chat_title = message.chat.title or f"Chat_{abs(chat_id)}"
    table_name = sanitize_table_name(chat_id, chat_title)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            
            if not cur.fetchone()[0]:
                await message.reply_text("📊 Пока нет данных для этого чата.")
                return
            
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
            total_messages = cur.fetchone()[0]
            
            cur.execute(sql.SQL("SELECT COUNT(DISTINCT user_id) FROM {}").format(sql.Identifier(table_name)))
            unique_users = cur.fetchone()[0]
            
            cur.execute(sql.SQL("""
                SELECT COUNT(*) FROM {} WHERE message_type IN ('photo', 'pdf', 'image', 'excel', 'word', 'powerpoint', 'video')
            """).format(sql.Identifier(table_name)))
            media_count = cur.fetchone()[0]
            
            cur.execute(sql.SQL("""
                SELECT COUNT(*) FROM {} WHERE media_analysis IS NOT NULL AND media_analysis != ''
            """).format(sql.Identifier(table_name)))
            analyzed_count = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM tg_user_roles WHERE chat_id = %s AND role IS NOT NULL
            """, (chat_id,))
            roles_count = cur.fetchone()[0]
            
            cur.execute(sql.SQL("""
                SELECT COUNT(*) FROM {} WHERE timestamp::date = CURRENT_DATE
            """).format(sql.Identifier(table_name)))
            today_messages = cur.fetchone()[0]
            
            stats_text = (
                f"📊 **Статистика чата**\n\n"
                f"📝 Всего сообщений: {total_messages:,}\n"
                f"👥 Участников: {unique_users}\n"
                f"🏷 С ролями: {roles_count}\n"
                f"📅 Сегодня: {today_messages}\n"
                f"📎 Медиафайлов: {media_count}\n"
                f"🤖 Проанализировано: {analyzed_count}"
            )
            
            await message.reply_text(stats_text, parse_mode="Markdown")
            
    finally:
        conn.close()


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск по сообщениям в текущем чате."""
    message = update.message
    
    if message.chat.type not in ["group", "supergroup"]:
        await message.reply_text("Эта команда работает только в групповых чатах.")
        return
    
    query = ' '.join(context.args) if context.args else None
    
    if not query:
        await message.reply_text(
            "🔍 Использование: /search <запрос>\n\n"
            "Пример: /search накладная сахар"
        )
        return
    
    chat_id = message.chat.id
    chat_title = message.chat.title or f"Chat_{abs(chat_id)}"
    table_name = sanitize_table_name(chat_id, chat_title)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                SELECT 
                    m.timestamp,
                    COALESCE(m.first_name, m.username, 'Неизвестный') as author,
                    r.role,
                    m.message_type,
                    LEFT(COALESCE(m.message_text, '') || ' ' || COALESCE(m.media_analysis, ''), 300) as content
                FROM {} m
                LEFT JOIN tg_user_roles r ON m.user_id = r.user_id AND r.chat_id = %s
                WHERE to_tsvector('russian', COALESCE(m.message_text, '') || ' ' || COALESCE(m.media_analysis, '')) 
                      @@ plainto_tsquery('russian', %s)
                ORDER BY m.timestamp DESC
                LIMIT 10
            """).format(sql.Identifier(table_name)), (chat_id, query,))
            
            results = cur.fetchall()
            
            if not results:
                await message.reply_text(f"🔍 По запросу «{query}» ничего не найдено.")
                return
            
            response = f"🔍 **Результаты поиска:** «{query}»\n\n"
            for ts, author, role, msg_type, content in results:
                date_str = ts.strftime("%d.%m.%Y %H:%M")
                role_str = f" [{role}]" if role else ""
                type_emoji = {"photo": "🖼", "pdf": "📄", "document": "📎", "excel": "📊", "word": "📝", "powerpoint": "📽", "video": "🎬"}.get(msg_type, "💬")
                content_preview = content[:150] + "..." if len(content) > 150 else content
                response += f"{type_emoji} {date_str} | **{author}**{role_str}\n{content_preview}\n\n"
            
            await message.reply_text(response, parse_mode="Markdown")
            
    finally:
        conn.close()


async def analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /analysis — управление рассылкой полного анализа документов в личку."""
    user = update.effective_user
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    
    args = context.args
    
    if not args:
        current = get_user_analysis_setting(user_id)
        status = "✅ включена" if current else "❌ выключена"
        await update.message.reply_text(
            f"📄 *Рассылка полного анализа документов:* {status}\n\n"
            f"Когда кто-то отправляет документ в чат, бот анализирует его.\n"
            f"В чат приходит краткий анализ (2-3 строки).\n"
            f"Если включено — полный анализ приходит вам в личку.\n\n"
            f"Команды:\n"
            f"`/analysis on` — включить\n"
            f"`/analysis off` — выключить",
            parse_mode="Markdown"
        )
        return
    
    action = args[0].lower()
    
    if action == 'on':
        set_user_analysis_setting(user_id, username, first_name, True)
        await update.message.reply_text(
            "✅ Готово! Теперь полный анализ документов будет приходить вам в личные сообщения.\n\n"
            "⚠️ Убедитесь, что вы начали диалог с ботом (напишите /start в личку боту)."
        )
        logger.info(f"Пользователь {first_name} ({user_id}) включил рассылку анализа")
    
    elif action == 'off':
        set_user_analysis_setting(user_id, username, first_name, False)
        await update.message.reply_text("❌ Рассылка полного анализа отключена.")
        logger.info(f"Пользователь {first_name} ({user_id}) отключил рассылку анализа")
    
    else:
        await update.message.reply_text(
            "Используйте:\n"
            "`/analysis on` — включить\n"
            "`/analysis off` — выключить",
            parse_mode="Markdown"
        )


async def analysis_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /analysis_list — показать кто включил рассылку (только для админа)."""
    user_id = update.effective_user.id
    
    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Эта команда доступна только администратору.")
        return
    
    users = get_users_with_full_analysis_enabled()
    
    if not users:
        await update.message.reply_text("📭 Никто не включил рассылку полного анализа.")
        return
    
    response = "📄 *Пользователи с включённой рассылкой анализа:*\n\n"
    for uid, username, first_name in users:
        name = first_name or username or str(uid)
        username_str = f" (@{username})" if username else ""
        response += f"• {name}{username_str}\n"
    
    response += f"\n*Всего:* {len(users)}"
    
    await update.message.reply_text(response, parse_mode="Markdown")
    

async def chats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список всех логируемых чатов."""
    if update.message.chat.type != "private":
        await update.message.reply_text("Эта команда доступна только в личных сообщениях.")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT chat_title, total_messages, last_message_at
                FROM tg_chats_metadata
                ORDER BY last_message_at DESC NULLS LAST
                LIMIT 20
            """)
            chats = cur.fetchall()
            
            if not chats:
                await update.message.reply_text("📭 Пока нет подключенных чатов.")
                return
            
            response = "📋 **Логируемые чаты:**\n\n"
            for title, total, last_msg in chats:
                last_str = last_msg.strftime("%d.%m.%Y %H:%M") if last_msg else "—"
                total = total or 0
                response += f"• **{title}**\n  Сообщений: {total:,} | {last_str}\n\n"
            
            await update.message.reply_text(response, parse_mode="Markdown")
            
    finally:
        conn.close()


# ============================================================
# ЗАПУСК
# ============================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик ошибок — логирует и продолжает работу."""
    import traceback
    
    logger.error(f"Ошибка при обработке обновления: {context.error}")
    
    # Логируем traceback
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)
    logger.error(f"Traceback:\n{tb_string[:1000]}")
    
    # Отправляем алерт администратору (не чаще раза в час)
    if ADMIN_USER_ID:
        try:
            error_text = str(context.error)[:200]
            await context.bot.send_message(
                chat_id=ADMIN_USER_ID,
                text=f"⚠️ Ошибка бота:\n\n{error_text}"
            )
        except:
            pass  # Не падаем если не можем отправить алерт


async def handle_mention(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик упоминания бота — RAG агент."""
    message = update.message
    if not message or not message.text:
        return
    
    # Проверяем, упомянут ли бот
    bot_username = (await context.bot.get_me()).username
    
    if f"@{bot_username}" not in message.text:
        return
    
    # Извлекаем вопрос (убираем упоминание бота)
    question = message.text.replace(f"@{bot_username}", "").strip()
    
    if not question:
        await message.reply_text("Задайте вопрос после упоминания бота.\n\nПример: @имя_бота какой курс доллара?")
        return
    
    # Отправляем индикатор "печатает"
    await context.bot.send_chat_action(chat_id=message.chat.id, action="typing")
    
    try:

        # Обрабатываем RAG запрос
        response = await process_rag_query(question, "")
        
        # Отправляем ответ
        if len(response) > 4000:
            # Разбиваем на части
            parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
            for part in parts:
                await message.reply_text(part)
        else:
            await message.reply_text(response)
        
        logger.info(f"RAG ответ отправлен: {len(response)} символов")
        
    except Exception as e:
        logger.error(f"Ошибка RAG агента: {e}")
        await message.reply_text(f"Произошла ошибка при обработке запроса. Попробуйте позже.")

# ============================================================
# EMAIL LOGGER: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def format_email_age(dt) -> str:
    """Форматирует возраст для email."""
    if not dt:
        return "никогда"
    
    from datetime import datetime
    if dt.tzinfo:
        dt = dt.replace(tzinfo=None)
    
    delta = datetime.now() - dt
    
    if delta.days > 30:
        return f"{delta.days // 30} мес."
    elif delta.days > 0:
        return f"{delta.days} дн."
    elif delta.seconds > 3600:
        return f"{delta.seconds // 3600} ч."
    elif delta.seconds > 60:
        return f"{delta.seconds // 60} мин."
    else:
        return "сейчас"


def truncate_text(text: str, max_len: int = 100) -> str:
    """Обрезает текст."""
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len-3] + "..."


# ============================================================
# EMAIL LOGGER: КОМАНДЫ
# ============================================================

async def open_threads_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает открытые ветки email переписки."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Нет доступа к этой команде")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    t.id,
                    t.subject_normalized,
                    t.message_count,
                    t.last_message_at,
                    t.priority,
                    t.status
                FROM email_threads t
                WHERE t.status IN ('open', 'pending_resolution')
                ORDER BY 
                    CASE t.priority 
                        WHEN 'high' THEN 1 
                        WHEN 'medium' THEN 2 
                        ELSE 3 
                    END,
                    t.last_message_at DESC
                LIMIT 20
            """)
            threads = cur.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения веток: {e}")
        await update.message.reply_text(
            "❌ Таблицы email логгера не найдены.\n\n"
            "Примените миграцию:\n"
            "`psql -d knowledge_base -f 001_init_email_logger.sql`",
            parse_mode="Markdown"
        )
        return
    finally:
        conn.close()
    
    if not threads:
        await update.message.reply_text("✅ Нет открытых веток email переписки")
        return
    
    text = "📬 *Открытые ветки переписки:*\n\n"
    
    for thread_id, subject, msg_count, last_msg_at, priority, status in threads:
        priority_icon = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(priority or 'medium', '⚪')
        status_icon = '⏳' if status == 'pending_resolution' else '📨'
        age = format_email_age(last_msg_at)
        subject_short = truncate_text(subject or "Без темы", 45)
        
        text += (
            f"{priority_icon}{status_icon} *{subject_short}*\n"
            f"   📨 {msg_count or 0} писем • {age}\n"
            f"   /emailthread\\_{thread_id}\n\n"
        )
    
    await update.message.reply_text(text, parse_mode="Markdown")


async def show_email_thread_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает детали ветки по команде /emailthread_N."""
    import json
    
    text = update.message.text
    match = re.search(r'/emailthread_(\d+)', text)
    if not match:
        await update.message.reply_text("❌ Укажите ID ветки: /emailthread_123")
        return
    
    thread_id = int(match.group(1))
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    id, subject_normalized, message_count, last_message_at,
                    priority, status, participant_emails, topic_tags,
                    summary_short, key_decisions, action_items
                FROM email_threads WHERE id = %s
            """, (thread_id,))
            row = cur.fetchone()
            
            if not row:
                await update.message.reply_text("❌ Ветка не найдена")
                return
            
            (tid, subject, msg_count, last_msg_at, priority, status, 
             participants, tags, summary, decisions, actions) = row
             
            # Получаем последние сообщения
            cur.execute("""
                SELECT from_address, body_text, received_at
                FROM email_messages
                WHERE thread_id = %s
                ORDER BY received_at DESC
                LIMIT 3
            """, (thread_id,))
            messages = cur.fetchall()
    finally:
        conn.close()
    
    # Статус
    status_map = {
        'open': '📬 Открыта',
        'pending_resolution': '⏳ Ожидает подтверждения',
        'resolved': '✅ Решена',
        'archived': '📦 В архиве'
    }
    status_str = status_map.get(status, status or 'unknown')
    
    # Приоритет
    priority_map = {'high': '🔴 Высокий', 'medium': '🟡 Средний', 'low': '🟢 Низкий'}
    priority_str = priority_map.get(priority, priority or 'medium')
    
    # Формируем ответ
    response = (
        f"📧 *{truncate_text(subject or 'Без темы', 50)}*\n\n"
        f"*Статус:* {status_str}\n"
        f"*Приоритет:* {priority_str}\n"
        f"*Сообщений:* {msg_count or 0}\n"
        f"*Последнее:* {format_email_age(last_msg_at)}\n"
    )
    
    if participants:
        p_list = participants[:3] if isinstance(participants, list) else []
        if p_list:
            response += f"*Участники:* {', '.join(p_list)}\n"
    
    if tags and isinstance(tags, list):
        response += f"*Теги:* {', '.join(tags)}\n"
    
    if summary:
        response += f"\n📝 *Саммари:*\n{summary}\n"
        
        if decisions and isinstance(decisions, list):
            response += "\n*Решения:*\n"
            for d in decisions[:5]:
                response += f"✓ {d}\n"
        
        if actions:
            items = actions if isinstance(actions, list) else json.loads(actions) if isinstance(actions, str) else []
            if items:
                response += "\n*Задачи:*\n"
                for item in items[:5]:
                    if isinstance(item, dict):
                        assignee = item.get('assignee', '?')
                        task = item.get('task', '')
                        response += f"• {assignee}: {task}\n"
    
    # Последние сообщения
    if messages:
        response += "\n📜 *Последние сообщения:*\n"
        for from_addr, body, received_at in messages:
            date_str = received_at.strftime('%d.%m %H:%M') if received_at else ""
            body_short = truncate_text(body or "", 150)
            response += f"\n_{from_addr}_ ({date_str}):\n{body_short}\n"
    
    # Кнопки
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Решена", callback_data=f"email_resolve:{thread_id}"),
            InlineKeyboardButton("📦 Архив", callback_data=f"email_archive:{thread_id}"),
        ]
    ])
    
    await update.message.reply_text(response[:4000], parse_mode="Markdown", reply_markup=keyboard)


async def email_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-кнопок для email."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith("email_resolve:"):
        thread_id = int(data.split(":")[1])
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE email_threads
                    SET status = 'resolved', resolution_confirmed = true, updated_at = NOW()
                    WHERE id = %s
                """, (thread_id,))
                conn.commit()
        finally:
            conn.close()
        await query.answer("✅ Ветка отмечена как решённая")
        await query.edit_message_reply_markup(reply_markup=None)
    
    elif data.startswith("email_archive:"):
        thread_id = int(data.split(":")[1])
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE email_threads SET status = 'archived', updated_at = NOW() WHERE id = %s
                """, (thread_id,))
                conn.commit()
        finally:
            conn.close()
        await query.answer("📦 Ветка перемещена в архив")
        await query.edit_message_reply_markup(reply_markup=None)


async def email_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику email логгера."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Проверяем существование таблиц
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'email_messages'
                )
            """)
            if not cur.fetchone()[0]:
                await update.message.reply_text(
                    "📊 Email логгер ещё не настроен.\n\n"
                    "Примените миграцию:\n"
                    "`psql -d knowledge_base -f 001_init_email_logger.sql`",
                    parse_mode="Markdown"
                )
                return
            
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM monitored_mailboxes WHERE is_active = true),
                    (SELECT COUNT(*) FROM email_messages),
                    (SELECT COUNT(*) FROM email_threads),
                    (SELECT COUNT(*) FROM email_threads WHERE status = 'open'),
                    (SELECT COUNT(*) FROM email_attachments),
                    (SELECT COUNT(*) FROM email_attachments WHERE analysis_status = 'pending')
            """)
            mailboxes, messages, threads, open_threads, attachments, pending = cur.fetchone()
            
            cur.execute("""
                SELECT email, last_sync_at, sync_status
                FROM monitored_mailboxes
                WHERE last_sync_at IS NOT NULL
                ORDER BY last_sync_at DESC
                LIMIT 1
            """)
            last_sync = cur.fetchone()
            
            cur.execute("""
                SELECT COUNT(*) FROM monitored_mailboxes WHERE sync_status = 'error'
            """)
            error_count = cur.fetchone()[0]
    finally:
        conn.close()
    
    text = (
        "📊 *Статистика Email Логгера:*\n\n"
        f"📬 Ящиков: {mailboxes or 0}\n"
        f"📨 Сообщений: {messages or 0:,}\n"
        f"🔗 Веток: {threads or 0} (открытых: {open_threads or 0})\n"
        f"📎 Вложений: {attachments or 0} (в очереди: {pending or 0})\n"
    )
    
    if last_sync:
        email, sync_at, status = last_sync
        text += f"\n*Последняя синхронизация:*\n{email} — {format_email_age(sync_at)}\n"
    
    if error_count:
        text += f"\n⚠️ Ящиков с ошибками: {error_count}"
    
    await update.message.reply_text(text, parse_mode="Markdown")


async def sync_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статус синхронизации ящиков."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Только для администраторов")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT email, sync_status, last_sync_at
                FROM monitored_mailboxes
                WHERE is_active = true
                ORDER BY last_sync_at DESC NULLS LAST
                LIMIT 30
            """)
            mailboxes = cur.fetchall()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return
    finally:
        conn.close()
    
    if not mailboxes:
        await update.message.reply_text("📬 Нет активных почтовых ящиков")
        return
    
    status_icons = {'idle': '✅', 'syncing': '🔄', 'initial_load': '📥', 'error': '❌'}
    
    text = "📬 *Статус синхронизации:*\n\n"
    
    for email, status, last_sync in mailboxes:
        icon = status_icons.get(status or 'idle', '❓')
        age = format_email_age(last_sync) if last_sync else "—"
        mailbox_name = email.split('@')[0] if email else "?"
        text += f"{icon} `{mailbox_name}` {age}\n"
    
    await update.message.reply_text(text[:4000], parse_mode="Markdown")


async def search_email_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск по email сообщениям."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    if not context.args:
        await update.message.reply_text(
            "🔍 *Поиск по email:*\n\n"
            "`/search_email накладная сахар`",
            parse_mode="Markdown"
        )
        return
    
    query_text = ' '.join(context.args)
    
    if len(query_text) < 3:
        await update.message.reply_text("Запрос слишком короткий")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    m.subject,
                    m.from_address,
                    m.received_at,
                    t.id as thread_id
                FROM email_messages m
                LEFT JOIN email_threads t ON t.id = m.thread_id
                WHERE 
                    m.subject ILIKE %s OR
                    m.body_text ILIKE %s OR
                    m.from_address ILIKE %s
                ORDER BY m.received_at DESC
                LIMIT 10
            """, (f"%{query_text}%", f"%{query_text}%", f"%{query_text}%"))
            results = cur.fetchall()
    finally:
        conn.close()
    
    if not results:
        await update.message.reply_text(f"❌ По запросу «{query_text}» ничего не найдено")
        return
    
    text = f"🔍 *Результаты «{query_text}»:*\n\n"
    
    for subject, from_addr, received_at, thread_id in results:
        subject_short = truncate_text(subject or "Без темы", 40)
        date = received_at.strftime('%d.%m.%Y') if received_at else ""
        thread_link = f"/emailthread\\_{thread_id}" if thread_id else ""
        
        text += f"📧 *{subject_short}*\n"
        text += f"   {from_addr or '?'} • {date}\n"
        if thread_link:
            text += f"   {thread_link}\n"
        text += "\n"
    
    await update.message.reply_text(text[:4000], parse_mode="Markdown")


async def add_employee_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет сотрудника."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Только для администраторов")
        return
    
    if not context.args:
        await update.message.reply_text(
            "👤 *Добавление сотрудника:*\n\n"
            "`/add_employee Иванов Иван | Бухгалтерия | Бухгалтер`\n"
            "`/add_employee Петрова Мария | Производство`\n"
            "`/add_employee Сидоров Пётр`",
            parse_mode="Markdown"
        )
        return
    
    full_text = ' '.join(context.args)
    parts = [p.strip() for p in full_text.split('|')]
    
    full_name = parts[0] if len(parts) > 0 else None
    department = parts[1] if len(parts) > 1 else None
    position = parts[2] if len(parts) > 2 else None
    
    if not full_name:
        await update.message.reply_text("❌ Укажите имя")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO employees (full_name, department, position, is_active)
                VALUES (%s, %s, %s, true)
                RETURNING id
            """, (full_name, department, position))
            emp_id = cur.fetchone()[0]
            conn.commit()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return
    finally:
        conn.close()
    
    text = f"✅ *Сотрудник добавлен:*\n\n👤 {full_name}\n"
    if department:
        text += f"🏢 {department}\n"
    if position:
        text += f"💼 {position}\n"
    text += f"\nID: {emp_id}"
    
    await update.message.reply_text(text, parse_mode="Markdown")


async def assign_email_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Назначает email сотруднику."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Только для администраторов")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "📧 *Назначение email:*\n\n"
            "`/assign_email <ID сотрудника> <email>`\n\n"
            "Пример:\n"
            "`/assign_email 1 accountant@totsamiy.com`\n\n"
            "Список сотрудников: /list\\_employees",
            parse_mode="Markdown"
        )
        return
    
    try:
        employee_id = int(context.args[0])
        email = context.args[1].lower()
    except:
        await update.message.reply_text("❌ Формат: /assign_email <ID> <email>")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Проверяем сотрудника
            cur.execute("SELECT full_name FROM employees WHERE id = %s", (employee_id,))
            emp = cur.fetchone()
            if not emp:
                await update.message.reply_text(f"❌ Сотрудник ID {employee_id} не найден")
                return
            
            # Назначаем email
            cur.execute("""
                INSERT INTO employee_emails (employee_id, email, is_primary, assigned_by)
                VALUES (%s, %s, true, %s)
                ON CONFLICT (employee_id, email) DO NOTHING
            """, (employee_id, email, update.effective_user.id))
            conn.commit()
    finally:
        conn.close()
    
    await update.message.reply_text(
        f"✅ *Email назначен:*\n\n"
        f"👤 {emp[0]}\n"
        f"📧 {email}",
        parse_mode="Markdown"
    )


async def list_employees_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список сотрудников."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT e.id, e.full_name, e.department, 
                       array_agg(ee.email) FILTER (WHERE ee.email IS NOT NULL) as emails
                FROM employees e
                LEFT JOIN employee_emails ee ON ee.employee_id = e.id
                WHERE e.is_active = true
                GROUP BY e.id, e.full_name, e.department
                ORDER BY e.full_name
                LIMIT 30
            """)
            employees = cur.fetchall()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return
    finally:
        conn.close()
    
    if not employees:
        await update.message.reply_text("👤 Нет сотрудников. Добавьте через /add\\_employee", parse_mode="Markdown")
        return
    
    text = "👥 *Сотрудники:*\n\n"
    
    for emp_id, name, dept, emails in employees:
        dept_str = f" ({dept})" if dept else ""
        email_str = f"\n   📧 {', '.join(emails)}" if emails and emails[0] else ""
        text += f"*{emp_id}.* {name}{dept_str}{email_str}\n"
    
    await update.message.reply_text(text[:4000], parse_mode="Markdown")

def get_chat_id_by_title(chat_title: str) -> int | None:
    """Получает chat_id по названию чата из БД."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Получаем список всех таблиц чатов
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name LIKE 'chat_%'
            """)
            tables = [row[0] for row in cur.fetchall()]

            # Ищем таблицу по названию чата
            for table in tables:
                try:
                    # Проверяем название чата
                    cur.execute(f"SELECT chat_id FROM {table} LIMIT 1")
                    result = cur.fetchone()
                    if result:
                        chat_id = result[0]
                        # Проверяем, соответствует ли название
                        cur.execute("""
                            SELECT EXISTS(
                                SELECT 1 FROM chat_info
                                WHERE chat_id = %s AND chat_title = %s
                            )
                        """, (chat_id, chat_title))
                        if cur.fetchone()[0]:
                            return chat_id
                except Exception as e:
                    logger.debug(f"Ошибка при проверке таблицы {table}: {e}")
                    continue
    finally:
        conn.close()

    return None


async def scheduled_daily_analysis(application):
    """Запланированный анализ документов в конце дня."""
    chat_id = get_chat_id_by_title(DELAYED_ANALYSIS_CHAT)

    if chat_id:
        await analyze_daily_documents(application.bot, chat_id, DELAYED_ANALYSIS_CHAT)
    else:
        logger.warning(f"Чат '{DELAYED_ANALYSIS_CHAT}' не найден для анализа документов")


def main():
    """Запуск бота."""
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не установлен в .env!")
        return

    if not DB_PASSWORD:
        logger.error("DB_PASSWORD не установлен в .env!")
        return

    application = Application.builder().token(BOT_TOKEN).build()

    # Инициализация планировщика для отложенного анализа документов
    scheduler = AsyncIOScheduler()

    # Добавляем задачу на 23:55 каждый день
    scheduler.add_job(
        scheduled_daily_analysis,
        CronTrigger(hour=23, minute=55),
        args=[application],
        id='daily_document_analysis',
        name='Ежедневный анализ документов для группы "Торты Отгрузки"',
        replace_existing=True
    )

    scheduler.start()
    logger.info(f"🕐 Планировщик запущен. Анализ документов для '{DELAYED_ANALYSIS_CHAT}' будет проводиться в 23:55")
    
    # Команды
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("roles", roles_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("chats", chats_command))
    application.add_handler(CommandHandler("analysis", analysis_command))
    application.add_handler(CommandHandler("analysis_list", analysis_list_command))
    # Email Logger команды
    application.add_handler(CommandHandler("threads", open_threads_command))
    application.add_handler(CommandHandler("email_stats", email_stats_command))
    application.add_handler(CommandHandler("sync_status", sync_status_command))
    application.add_handler(CommandHandler("search_email", search_email_command))
    application.add_handler(CommandHandler("add_employee", add_employee_command))
    application.add_handler(CommandHandler("assign_email", assign_email_command))
    application.add_handler(CommandHandler("list_employees", list_employees_command))

    application.add_handler(MessageHandler(
        filters.Regex(r'^/emailthread_\d+'),
        show_email_thread_command
    ))

    application.add_handler(CallbackQueryHandler(
        email_callback_handler,
        pattern=r'^email_'
    ))

    # RAG агент — обработка упоминаний бота (ПЕРЕД log_message!)
    application.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'@\w+'),
        handle_mention
    ))
    
    # Обработчик ответов с ролями (в группах и личных сообщениях)
    application.add_handler(MessageHandler(
        filters.TEXT & filters.REPLY & ~filters.COMMAND,
        handle_role_assignment
    ))
    
    # Обработчик всех остальных сообщений
    application.add_handler(MessageHandler(
        filters.ALL & ~filters.COMMAND,
        log_message
    ))
    
    logger.info("🚀 Бот запущен. Логирование + анализ медиа + роли активны.")
    application.add_error_handler(error_handler)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
