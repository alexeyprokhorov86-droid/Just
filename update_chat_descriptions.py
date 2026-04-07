#!/usr/bin/env python3
"""
Автоматическая генерация описаний чатов на основе анализа контента.

Для каждого чата:
1. Берёт участников и их активность за 3 месяца
2. Берёт 60 последних сообщений (включая media_analysis)
3. Отправляет на GPT-4.1 для генерации описания
4. Сохраняет в tg_chats_metadata.description + description_updated_at

Использование:
    python3 update_chat_descriptions.py          # обновить все чаты
    python3 update_chat_descriptions.py --force   # обновить даже если описание свежее (<7 дней)
    python3 update_chat_descriptions.py --chat tg_chat_1003653024997_hr_frumelad_nf  # один конкретный чат

Для регулярного запуска добавить в cron (раз в неделю):
    0 6 * * 1 cd /home/admin/telegram_logger_bot && /home/admin/telegram_logger_bot/venv/bin/python3 update_chat_descriptions.py >> /var/log/chat_descriptions.log 2>&1
"""
import os
import sys
import json
import time
import argparse
import requests
import psycopg2
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv('/home/admin/telegram_logger_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

ROUTERAI_API_KEY = os.getenv("ROUTERAI_API_KEY")
ROUTERAI_BASE_URL = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '172.20.0.2'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'dbname': os.getenv('DB_NAME', 'knowledge_base'),
    'user': os.getenv('DB_USER', 'knowledge'),
    'password': os.getenv('DB_PASSWORD'),
}

# Сколько месяцев анализировать
ANALYSIS_MONTHS = 3
# Сколько сообщений брать для анализа
SAMPLE_SIZE = 60
# Не обновлять описание чаще чем раз в N дней
FRESHNESS_DAYS = 7


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def call_gpt(prompt: str, max_tokens: int = 1200) -> str:
    """Вызов GPT-4.1 через RouterAI."""
    if not ROUTERAI_API_KEY:
        logger.error("ROUTERAI_API_KEY не задан")
        return ""
    try:
        resp = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {ROUTERAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "openai/gpt-4.1",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": 0.1
            },
            timeout=90
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error(f"GPT API error: {e}")
        return ""


def ensure_columns(conn):
    """Добавляем колонки description и description_updated_at если их нет."""
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE tg_chats_metadata ADD COLUMN IF NOT EXISTS description TEXT")
        cur.execute("ALTER TABLE tg_chats_metadata ADD COLUMN IF NOT EXISTS description_updated_at TIMESTAMP")
    conn.commit()


def get_chat_tables(conn):
    """Все активные чаты из metadata."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT chat_id, chat_title, table_name, last_message_at, total_messages,
                   description, description_updated_at
            FROM tg_chats_metadata
            WHERE table_name IS NOT NULL
            ORDER BY last_message_at DESC NULLS LAST
        """)
        return [
            {
                "chat_id": r[0],
                "title": r[1] or "",
                "table_name": r[2],
                "last_msg": r[3],
                "total_msgs": r[4] or 0,
                "description": r[5],
                "desc_updated": r[6]
            }
            for r in cur.fetchall()
        ]


def get_chat_participants(conn, table_name: str) -> list:
    """Участники чата с количеством сообщений (за всё время)."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')) as name,
                    username,
                    COUNT(*) as msg_count,
                    MAX(timestamp) as last_msg
                FROM {table_name}
                WHERE first_name IS NOT NULL
                AND first_name NOT IN ('Group', 'Bot', 'Telegram')
                AND COALESCE(username, '') != 'GroupAnonymousBot'
                GROUP BY first_name, last_name, username
                ORDER BY msg_count DESC
                LIMIT 15
            """)
            return [
                {
                    "name": r[0],
                    "username": r[1] or "",
                    "msgs": r[2],
                    "last_active": r[3].strftime("%d.%m.%Y") if r[3] else ""
                }
                for r in cur.fetchall()
            ]
    except Exception as e:
        logger.warning(f"  Ошибка participants для {table_name}: {e}")
        return []


def get_chat_sample(conn, table_name: str, limit: int = SAMPLE_SIZE) -> list:
    """Последние N сообщений включая media_analysis и content_text (без фильтра по дате)."""
    try:
        with conn.cursor() as cur:
            # Проверяем наличие колонок
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s
                AND column_name IN ('message_text', 'media_analysis', 'content_text',
                                    'first_name', 'timestamp', 'message_type')
            """, (table_name,))
            cols = {r[0] for r in cur.fetchall()}

            if 'timestamp' not in cols:
                return []

            # Формируем SELECT — берём media_analysis и content_text полностью (до 1500 символов)
            select_parts = []
            select_parts.append("first_name" if 'first_name' in cols else "'' as first_name")
            select_parts.append("message_text" if 'message_text' in cols else "'' as message_text")
            select_parts.append("LEFT(media_analysis, 1500) as media_analysis" if 'media_analysis' in cols else "'' as media_analysis")
            select_parts.append("LEFT(content_text, 1000) as content_text" if 'content_text' in cols else "'' as content_text")
            select_parts.append("message_type" if 'message_type' in cols else "'text' as message_type")
            select_parts.append("timestamp")

            # WHERE — хотя бы что-то непустое
            where_parts = []
            if 'message_text' in cols:
                where_parts.append("(message_text IS NOT NULL AND message_text != '')")
            if 'media_analysis' in cols:
                where_parts.append("(media_analysis IS NOT NULL AND media_analysis != '')")
            if 'content_text' in cols:
                where_parts.append("(content_text IS NOT NULL AND content_text != '')")

            if not where_parts:
                return []

            where_clause = " OR ".join(where_parts)

            # Берём последние N сообщений БЕЗ фильтра по дате
            cur.execute(f"""
                SELECT {', '.join(select_parts)}
                FROM {table_name}
                WHERE ({where_clause})
                ORDER BY timestamp DESC
                LIMIT {limit}
            """)

            results = []
            for r in cur.fetchall():
                msg = {
                    "from": r[0] or "?",
                    "text": (r[1] or "").strip(),
                    "analysis": (r[2] or "").strip(),
                    "content_text": (r[3] or "").strip(),
                    "type": r[4] or "text",
                    "date": r[5].strftime("%d.%m.%Y %H:%M") if r[5] else ""
                }
                results.append(msg)

            return results

    except Exception as e:
        logger.warning(f"  Ошибка sample для {table_name}: {e}")
        return []


def format_messages_for_prompt(messages: list) -> str:
    """Форматирует сообщения для промпта."""
    lines = []
    for m in reversed(messages):  # хронологический порядок
        sender = m["from"]
        date = m["date"]
        text = m["text"]
        analysis = m["analysis"]
        content_text = m.get("content_text", "")

        parts = []
        if text:
            parts.append(text[:400])
        if analysis:
            parts.append(f"[Анализ вложения: {analysis[:800]}]")
        if content_text and content_text != text:
            parts.append(f"[Текст документа: {content_text[:500]}]")

        content = " | ".join(parts) if parts else "[пустое сообщение]"
        lines.append(f"[{date}] {sender}: {content}")

    return "\n".join(lines)


def format_participants_for_prompt(participants: list) -> str:
    """Форматирует список участников."""
    if not participants:
        return "Нет данных об участниках"
    lines = []
    for p in participants:
        username_str = f" (@{p['username']})" if p['username'] else ""
        lines.append(f"- {p['name']}{username_str}: {p['msgs']} сообщ., посл. активность {p['last_active']}")
    return "\n".join(lines)


def generate_description(chat_info: dict, participants: list, messages: list) -> str:
    """Генерирует описание чата через GPT-4.1."""

    participants_text = format_participants_for_prompt(participants)
    messages_text = format_messages_for_prompt(messages)

    prompt = f"""Ты — аналитик бизнес-коммуникаций кондитерской компании "Фрумелад" (ООО "Фрумелад" — продажи/администрирование, ООО "НФ"/"Новэл Фуд" — производство).

Проанализируй Telegram-чат и составь его описание для системы маршрутизации запросов.

НАЗВАНИЕ ЧАТА: {chat_info['title']}
ИДЕНТИФИКАТОР: {chat_info['table_name']}
ВСЕГО СООБЩЕНИЙ: {chat_info['total_msgs']}
ПОСЛЕДНЕЕ СООБЩЕНИЕ: {chat_info['last_msg'].strftime('%d.%m.%Y') if chat_info['last_msg'] else 'нет'}

УЧАСТНИКИ (за {ANALYSIS_MONTHS} мес.):
{participants_text}

ПОСЛЕДНИЕ {len(messages)} СООБЩЕНИЙ:
{messages_text}

ЗАДАЧА: На основе участников и содержания сообщений напиши описание чата.

ФОРМАТ ОТВЕТА — строго JSON без markdown:
{{
  "description": "2-4 предложения: основная тематика чата, какие вопросы обсуждаются, какие решения принимаются",
  "keywords": ["ключевое_слово_1", "ключевое_слово_2", ...],
  "roles": ["роль_участника_1", "роль_участника_2", ...],
  "topics": ["тема_1", "тема_2", ...]
}}

ПРАВИЛА:
- description должен быть полезен для маршрутизации поисковых запросов — чтобы система понимала какие вопросы направлять в этот чат
- Укажи конкретные типы документов, процессов, решений которые обсуждаются
- Если в чате обсуждаются офферы, найм, увольнения — обязательно укажи "job offer", "оффер", "найм"
- Если обсуждаются закупки — укажи типы закупок (сырьё, упаковка, оборудование)
- Если обсуждается производство — укажи что именно (выпуск, брак, рецептуры, смены)
- keywords — слова по которым пользователь может искать информацию из этого чата
- roles — роли/должности участников (HR-менеджер, технолог, бухгалтер и т.д.), определи по контексту сообщений
- topics — основные темы обсуждений"""

    response = call_gpt(prompt)
    if not response:
        return ""

    # Парсим JSON
    try:
        # Убираем возможные markdown-обёртки
        clean = response.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
            if clean.endswith("```"):
                clean = clean[:-3]
            clean = clean.strip()

        data = json.loads(clean)

        # Формируем итоговое описание для metadata
        desc = data.get("description", "")
        keywords = data.get("keywords", [])
        roles = data.get("roles", [])
        topics = data.get("topics", [])

        parts = [desc]
        if keywords:
            parts.append(f"Ключевые слова: {', '.join(keywords)}")
        if roles:
            parts.append(f"Роли участников: {', '.join(roles)}")
        if topics:
            parts.append(f"Темы: {', '.join(topics)}")

        return "\n".join(parts)

    except json.JSONDecodeError:
        # Если не JSON — используем как есть
        logger.warning("  GPT вернул не-JSON, используем как текст")
        return response[:500]


def save_description(conn, chat_id: int, description: str):
    """Сохраняет описание в metadata."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE tg_chats_metadata
            SET description = %s, description_updated_at = NOW()
            WHERE chat_id = %s
        """, (description, chat_id))
    conn.commit()


def update_all_descriptions(force: bool = False, single_chat: str = None):
    """Основная функция обновления описаний."""
    conn = get_conn()
    ensure_columns(conn)

    chats = get_chat_tables(conn)
    logger.info(f"Найдено {len(chats)} чатов в metadata")

    updated = 0
    skipped = 0
    errors = 0

    for chat in chats:
        table_name = chat["table_name"]

        # Фильтр по конкретному чату
        if single_chat and table_name != single_chat:
            continue

        # Пропускаем свежие описания (если не --force)
        if not force and chat["desc_updated"]:
            age_days = (datetime.now() - chat["desc_updated"]).days
            if age_days < FRESHNESS_DAYS:
                logger.info(f"  {chat['title']}: описание свежее ({age_days}д), пропускаем")
                skipped += 1
                continue

        logger.info(f"\n{'='*60}")
        logger.info(f"Анализ: {chat['title']} [{table_name}]")

        # Собираем данные
        participants = get_chat_participants(conn, table_name)
        messages = get_chat_sample(conn, table_name)

        if not messages:
            logger.warning(f"  Нет сообщений с текстом/вложениями, пропускаем")
            skipped += 1
            continue

        logger.info(f"  Участников: {len(participants)}, сообщений для анализа: {len(messages)}")

        # Генерируем описание
        description = generate_description(chat, participants, messages)

        if not description:
            logger.error(f"  Не удалось сгенерировать описание")
            errors += 1
            continue

        # Сохраняем
        save_description(conn, chat["chat_id"], description)
        updated += 1

        logger.info(f"  ✅ Описание сохранено ({len(description)} символов)")
        logger.info(f"  Превью: {description[:200]}...")

        # Пауза между запросами к API
        time.sleep(1)

    conn.close()

    logger.info(f"\n{'='*60}")
    logger.info(f"ИТОГО: {updated} обновлено, {skipped} пропущено, {errors} ошибок")
    logger.info(f"{'='*60}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Обновление описаний Telegram-чатов')
    parser.add_argument('--force', action='store_true', help='Обновить даже свежие описания')
    parser.add_argument('--chat', type=str, help='Обновить конкретный чат (table_name)')
    args = parser.parse_args()

    update_all_descriptions(force=args.force, single_chat=args.chat)
