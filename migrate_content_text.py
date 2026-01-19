#!/usr/bin/env python3
"""
Скрипт миграции для добавления поля content_text в существующие таблицы.
Обновляет:
1. Все таблицы Telegram чатов (tg_chat_*)
2. Таблицу email_attachments
3. Индексы для полнотекстового поиска
"""

import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


def get_db_connection():
    """Создает подключение к БД."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def migrate_telegram_tables():
    """Добавляет поле content_text во все таблицы Telegram чатов."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Получаем список всех таблиц чатов
            cur.execute("""
                SELECT table_name FROM tg_chats_metadata
            """)
            tables = [row[0] for row in cur.fetchall()]

            print(f"Найдено {len(tables)} таблиц чатов для миграции")

            for table_name in tables:
                print(f"Миграция таблицы {table_name}...")

                # Добавляем колонку content_text если её нет
                cur.execute(sql.SQL("""
                    ALTER TABLE {}
                    ADD COLUMN IF NOT EXISTS content_text TEXT
                """).format(sql.Identifier(table_name)))

                # Удаляем старый индекс
                cur.execute(sql.SQL("""
                    DROP INDEX IF EXISTS {}
                """).format(sql.Identifier(f"idx_{table_name}_fts")))

                # Создаем новый индекс с включением content_text
                cur.execute(sql.SQL("""
                    CREATE INDEX IF NOT EXISTS {} ON {}
                    USING gin(to_tsvector('russian',
                        COALESCE(message_text, '') || ' ' ||
                        COALESCE(media_analysis, '') || ' ' ||
                        COALESCE(content_text, '')
                    ))
                """).format(
                    sql.Identifier(f"idx_{table_name}_fts"),
                    sql.Identifier(table_name)
                ))

                print(f"✓ {table_name} обновлена")

            conn.commit()
            print(f"\n✓ Все {len(tables)} таблиц Telegram успешно обновлены")

    except Exception as e:
        conn.rollback()
        print(f"✗ Ошибка миграции Telegram таблиц: {e}")
        raise
    finally:
        conn.close()


def migrate_email_attachments():
    """Добавляет поле content_text в таблицу email_attachments."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            print("Миграция таблицы email_attachments...")

            # Проверяем существует ли таблица
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'email_attachments'
                )
            """)

            if not cur.fetchone()[0]:
                print("⚠ Таблица email_attachments не существует, пропускаем")
                return

            # Добавляем колонку content_text
            cur.execute("""
                ALTER TABLE email_attachments
                ADD COLUMN IF NOT EXISTS content_text TEXT
            """)

            # Создаем индекс для полнотекстового поиска
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_attachments_content_text_fts
                ON email_attachments
                USING gin(to_tsvector('russian', COALESCE(content_text, '')))
            """)

            # Удаляем старый комбинированный индекс если есть
            cur.execute("""
                DROP INDEX IF EXISTS idx_attachments_combined_fts
            """)

            # Создаем новый комбинированный индекс
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_attachments_combined_fts
                ON email_attachments
                USING gin(to_tsvector('russian',
                    COALESCE(analysis_text, '') || ' ' ||
                    COALESCE(content_text, '')
                ))
            """)

            # Добавляем комментарий
            cur.execute("""
                COMMENT ON COLUMN email_attachments.content_text IS
                'Извлеченное текстовое содержимое файла: транскрипты, OCR, текст из документов, CSV из Excel и т.д.'
            """)

            conn.commit()
            print("✓ Таблица email_attachments успешно обновлена")

    except Exception as e:
        conn.rollback()
        print(f"✗ Ошибка миграции email_attachments: {e}")
        raise
    finally:
        conn.close()


def main():
    """Основная функция миграции."""
    print("=" * 60)
    print("Миграция: добавление поля content_text")
    print("=" * 60)
    print()

    try:
        # Миграция таблиц Telegram
        migrate_telegram_tables()
        print()

        # Миграция таблицы email_attachments
        migrate_email_attachments()
        print()

        print("=" * 60)
        print("✓ Миграция успешно завершена!")
        print("=" * 60)

    except Exception as e:
        print()
        print("=" * 60)
        print(f"✗ Миграция не удалась: {e}")
        print("=" * 60)
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
