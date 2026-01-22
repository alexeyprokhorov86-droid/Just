"""
Сервис для создания и поиска векторных эмбеддингов.
Использует intfloat/multilingual-e5-base для русского языка.
"""

import os
import logging
from typing import List, Dict, Optional
import psycopg2
from psycopg2 import sql
from sentence_transformers import SentenceTransformer
import numpy as np

# Загружаем переменные окружения
from dotenv import load_dotenv
load_dotenv('/home/admin/telegram_logger_bot/.env')

logger = logging.getLogger(__name__)

# Настройки БД
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Глобальная модель (загружается один раз)
_model = None


def get_model() -> SentenceTransformer:
    """Загружает модель e5-base (один раз, потом из кэша)."""
    global _model
    if _model is None:
        logger.info("Загружаем модель intfloat/multilingual-e5-base...")
        _model = SentenceTransformer('intfloat/multilingual-e5-base')
        logger.info("Модель загружена")
    return _model


def get_db_connection():
    """Создаёт подключение к PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def create_embedding(text: str) -> List[float]:
    """
    Создаёт эмбеддинг для текста.
    Для e5 моделей нужен префикс 'query: ' или 'passage: '
    """
    model = get_model()
    # Для индексации документов используем 'passage: '
    # Для поисковых запросов используем 'query: '
    embedding = model.encode(f"passage: {text}", normalize_embeddings=True)
    return embedding.tolist()


def create_query_embedding(text: str) -> List[float]:
    """Создаёт эмбеддинг для поискового запроса."""
    model = get_model()
    embedding = model.encode(f"query: {text}", normalize_embeddings=True)
    return embedding.tolist()


def index_telegram_message(source_table: str, source_id: int, content: str) -> bool:
    """Индексирует одно сообщение из Telegram."""
    if not content or len(content.strip()) < 10:
        return False  # Слишком короткий текст
    
    conn = get_db_connection()
    try:
        embedding = create_embedding(content)
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO embeddings (source_type, source_table, source_id, content, embedding)
                VALUES ('telegram', %s, %s, %s, %s)
                ON CONFLICT (source_table, source_id) 
                DO UPDATE SET content = EXCLUDED.content, embedding = EXCLUDED.embedding
            """, (source_table, source_id, content[:5000], embedding))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Ошибка индексации: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def index_all_telegram_chats(batch_size: int = 100) -> Dict[str, int]:
    """
    Индексирует все сообщения из всех Telegram чатов.
    Возвращает статистику.
    """
    stats = {"total": 0, "indexed": 0, "skipped": 0, "errors": 0}
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            # Получаем список всех таблиц чатов
            cur.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'tg_chat_%'
                AND table_name != 'tg_chats_metadata'
                AND table_name != 'tg_user_roles'
            """)
            chat_tables = [row[0] for row in cur.fetchall()]
        
        logger.info(f"Найдено {len(chat_tables)} таблиц чатов")
        
        for table_name in chat_tables:
            logger.info(f"Индексируем {table_name}...")
            indexed_in_table = 0
            
            try:
                with conn.cursor() as cur:
                    # Получаем сообщения, которые ещё не проиндексированы
                    cur.execute(sql.SQL("""
                        SELECT t.id, 
                               COALESCE(t.message_text, '') || ' ' || COALESCE(t.media_analysis, '') as content
                        FROM {} t
                        LEFT JOIN embeddings e 
                            ON e.source_table = %s AND e.source_id = t.id
                        WHERE e.id IS NULL
                        AND (t.message_text IS NOT NULL OR t.media_analysis IS NOT NULL)
                        AND LENGTH(COALESCE(t.message_text, '') || COALESCE(t.media_analysis, '')) > 10
                    """).format(sql.Identifier(table_name)), (table_name,))
                    
                    rows = cur.fetchall()
                    stats["total"] += len(rows)
                    
                    for row_id, content in rows:
                        content = content.strip()
                        if len(content) < 10:
                            stats["skipped"] += 1
                            continue
                        
                        if index_telegram_message(table_name, row_id, content):
                            stats["indexed"] += 1
                            indexed_in_table += 1
                        else:
                            stats["skipped"] += 1
                        
                        # Прогресс каждые 100 записей
                        if stats["indexed"] % 100 == 0:
                            logger.info(f"Проиндексировано: {stats['indexed']}")
                            
            except Exception as e:
                logger.error(f"Ошибка в таблице {table_name}: {e}")
                stats["errors"] += 1
                continue
            
            logger.info(f"  {table_name}: проиндексировано {indexed_in_table}")
    
    finally:
        conn.close()
    
    return stats


def vector_search(query: str, limit: int = 10, source_type: Optional[str] = None) -> List[Dict]:
    """
    Семантический поиск по эмбеддингам.
    
    Args:
        query: Поисковый запрос
        limit: Максимум результатов
        source_type: Фильтр по типу источника ('telegram', 'email', '1c_nomenclature')
    
    Returns:
        Список результатов с полями: source_type, source_table, source_id, content, similarity
    """
    query_embedding = create_query_embedding(query)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if source_type:
                cur.execute("""
                    SELECT source_type, source_table, source_id, content,
                           1 - (embedding <=> %s::vector) as similarity
                    FROM embeddings
                    WHERE source_type = %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (query_embedding, source_type, query_embedding, limit))
            else:
                cur.execute("""
                    SELECT source_type, source_table, source_id, content,
                           1 - (embedding <=> %s::vector) as similarity
                    FROM embeddings
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (query_embedding, query_embedding, limit))
            
            results = []
            for row in cur.fetchall():
                results.append({
                    "source_type": row[0],
                    "source_table": row[1],
                    "source_id": row[2],
                    "content": row[3],
                    "similarity": float(row[4])
                })
            
            return results
    finally:
        conn.close()


def get_index_stats() -> Dict:
    """Возвращает статистику индекса."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_type, COUNT(*) as count
                FROM embeddings
                GROUP BY source_type
            """)
            by_type = {row[0]: row[1] for row in cur.fetchall()}
            
            cur.execute("SELECT COUNT(*) FROM embeddings")
            total = cur.fetchone()[0]
            
            return {"total": total, "by_type": by_type}
    finally:
        conn.close()

def vector_search_weighted(query: str, limit: int = 10, source_type: Optional[str] = None, 
                           freshness_weight: float = 0.25, decay_days: int = 90) -> List[Dict]:
    """
    Семантический поиск с учётом свежести (для email).
    
    Args:
        query: Поисковый запрос
        limit: Максимум результатов
        source_type: Фильтр по типу источника
        freshness_weight: Вес свежести (0.0-1.0), остальное - similarity
        decay_days: За сколько дней freshness падает до ~0.37
    
    Returns:
        Список результатов с полями: source_type, source_table, source_id, content, 
        similarity, freshness, final_score, received_at
    """
    query_embedding = create_query_embedding(query)
    similarity_weight = 1.0 - freshness_weight
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if source_type == 'email':
                # Для email — взвешенный скоринг с учётом даты
                cur.execute("""
                    SELECT 
                        e.source_type, 
                        e.source_table, 
                        e.source_id, 
                        e.content,
                        1 - (e.embedding <=> %s::vector) as similarity,
                        EXP(-EXTRACT(EPOCH FROM (NOW() - em.received_at)) / (%s * 86400)) as freshness,
                        (1 - (e.embedding <=> %s::vector)) * %s + 
                        EXP(-EXTRACT(EPOCH FROM (NOW() - em.received_at)) / (%s * 86400)) * %s as final_score,
                        em.received_at,
                        em.subject,
                        em.from_address
                    FROM embeddings e
                    JOIN email_messages em ON e.source_id = em.id
                    WHERE e.source_type = 'email'
                    ORDER BY final_score DESC
                    LIMIT %s
                """, (query_embedding, decay_days, query_embedding, similarity_weight, 
                      decay_days, freshness_weight, limit))
                
                results = []
                for row in cur.fetchall():
                    results.append({
                        "source_type": row[0],
                        "source_table": row[1],
                        "source_id": row[2],
                        "content": row[3],
                        "similarity": float(row[4]),
                        "freshness": float(row[5]) if row[5] else 0,
                        "final_score": float(row[6]) if row[6] else 0,
                        "received_at": row[7],
                        "subject": row[8],
                        "from_address": row[9]
                    })
                return results
            else:
                # Для остальных — обычный поиск
                return vector_search(query, limit, source_type)
    finally:
        conn.close()

# CLI для запуска индексации
if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) > 1 and sys.argv[1] == "index":
        print("Запускаем индексацию всех Telegram чатов...")
        print("Это может занять время при первом запуске (загрузка модели ~1.1GB)")
        print()
        
        stats = index_all_telegram_chats()
        
        print()
        print("=" * 50)
        print("РЕЗУЛЬТАТ ИНДЕКСАЦИИ:")
        print(f"  Всего записей: {stats['total']}")
        print(f"  Проиндексировано: {stats['indexed']}")
        print(f"  Пропущено (короткие): {stats['skipped']}")
        print(f"  Ошибок: {stats['errors']}")
        print("=" * 50)
    
    elif len(sys.argv) > 1 and sys.argv[1] == "stats":
        stats = get_index_stats()
        print(f"Всего эмбеддингов: {stats['total']}")
        print(f"По типам: {stats['by_type']}")
    
    elif len(sys.argv) > 2 and sys.argv[1] == "search":
        query = " ".join(sys.argv[2:])
        print(f"Поиск: {query}")
        print()
        
        results = vector_search(query, limit=5)
        for i, r in enumerate(results, 1):
            print(f"{i}. [{r['similarity']:.3f}] {r['source_type']}: {r['content'][:100]}...")
            print()
    
    else:
        print("Использование:")
        print("  python embedding_service.py index   - индексировать все чаты")
        print("  python embedding_service.py stats   - статистика индекса")
        print("  python embedding_service.py search <запрос>  - тестовый поиск")
