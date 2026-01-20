"""
Экспорт истории сообщений из Telegram чатов в PostgreSQL
Использует Telethon (userbot) для доступа к истории
"""

import asyncio
import re
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User
import psycopg2
from psycopg2 import sql

# ============================================================
# НАСТРОЙКИ — ЗАМЕНИ НА СВОИ
# ============================================================

API_ID = 34361670
API_HASH = "2cbde1edc0755c956bc90b47cf5ec45b"

# БД
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "knowledge_base"
DB_USER = "knowledge"
DB_PASSWORD = "Prokhorov2025Secure"

# ============================================================

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

def sanitize_table_name(chat_id: int, chat_title: str) -> str:
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
    safe_title = re.sub(r'_+', '_', safe_title).strip('_')[:30] or "unnamed"
    return f"tg_chat_{abs(chat_id)}_{safe_title}"

def ensure_table_exists(chat_id: int, chat_title: str) -> str:
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
                    timestamp TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(message_id)
                )
            """).format(sql.Identifier(table_name)))
            
            cur.execute(sql.SQL(
                "CREATE INDEX IF NOT EXISTS {} ON {} (timestamp)"
            ).format(sql.Identifier(f"idx_{table_name}_ts"), sql.Identifier(table_name)))
            
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
            
            cur.execute("""
                INSERT INTO tg_chats_metadata (chat_id, chat_title, table_name, chat_type)
                VALUES (%s, %s, %s, 'group')
                ON CONFLICT (chat_id) DO UPDATE SET chat_title = EXCLUDED.chat_title
            """, (chat_id, chat_title, table_name))
            
            conn.commit()
    finally:
        conn.close()
    return table_name

def save_message(table_name: str, msg_data: dict):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                INSERT INTO {} (message_id, user_id, username, first_name, last_name,
                    message_text, message_type, reply_to_message_id, timestamp)
                VALUES (%(message_id)s, %(user_id)s, %(username)s, %(first_name)s, 
                    %(last_name)s, %(message_text)s, %(message_type)s, 
                    %(reply_to_message_id)s, %(timestamp)s)
                ON CONFLICT (message_id) DO NOTHING
            """).format(sql.Identifier(table_name)), msg_data)
            conn.commit()
    finally:
        conn.close()

async def export_chat(client, dialog, limit=None):
    """Экспортирует историю одного чата"""
    chat = dialog.entity
    chat_id = dialog.id
    chat_title = dialog.title or "Unnamed"
    
    print(f"\n📥 Экспорт: {chat_title}")
    table_name = ensure_table_exists(chat_id, chat_title)
    
    count = 0
    async for message in client.iter_messages(chat, limit=limit):
        if message.text or message.media:
            sender = await message.get_sender()
            msg_data = {
                "message_id": message.id,
                "user_id": sender.id if sender else None,
                "username": getattr(sender, 'username', None),
                "first_name": getattr(sender, 'first_name', None),
                "last_name": getattr(sender, 'last_name', None),
                "message_text": message.text or "",
                "message_type": "text" if message.text else "media",
                "reply_to_message_id": message.reply_to_msg_id if message.reply_to else None,
                "timestamp": message.date
            }
            save_message(table_name, msg_data)
            count += 1
            if count % 100 == 0:
                print(f"  ... {count} сообщений")
    
    print(f"  ✅ Сохранено: {count} сообщений")
    return count

async def main():
    print("🔐 Подключение к Telegram...")
    client = TelegramClient('session', API_ID, API_HASH)
    await client.start()
    
    print("📋 Загрузка списка чатов...")
    dialogs = await client.get_dialogs()
    
    # Фильтруем только группы
    groups = [d for d in dialogs if d.is_group or d.is_channel]
    
    print(f"\n📊 Найдено групп: {len(groups)}")
    print("-" * 40)
    
    for i, d in enumerate(groups, 1):
        print(f"{i}. {d.title}")
    
    print("-" * 40)
    choice = input("\nВведи номера через запятую (или 'all' для всех): ").strip()
    
    if choice.lower() == 'all':
        selected = groups
    else:
        indices = [int(x.strip()) - 1 for x in choice.split(',')]
        selected = [groups[i] for i in indices if 0 <= i < len(groups)]
    
    limit_input = input("Лимит сообщений на чат (Enter = все): ").strip()
    limit = int(limit_input) if limit_input else None
    
    total = 0
    for dialog in selected:
        total += await export_chat(client, dialog, limit)
    
    print(f"\n🎉 Готово! Всего экспортировано: {total} сообщений")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
