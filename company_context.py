"""
Модуль для подгрузки Company Profile из agent_memory.
Импортируется в bot.py, rag_agent.py, email_sync.py.

Использование:
    from company_context import get_company_profile
    
    profile = get_company_profile()
    prompt = f"{profile}\n\n{your_prompt}"
"""

import os
import logging
import psycopg2
from functools import lru_cache
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Настройки БД
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def _get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


# Кэш: профиль обновляется раз в час
_profile_cache = {"text": None, "loaded_at": None}
CACHE_TTL = timedelta(hours=1)


def get_company_profile() -> str:
    """
    Возвращает Company Profile из agent_memory.
    Кэшируется на 1 час чтобы не дёргать БД при каждом запросе.
    """
    now = datetime.now()
    
    # Проверяем кэш
    if (_profile_cache["text"] is not None 
        and _profile_cache["loaded_at"] is not None
        and now - _profile_cache["loaded_at"] < CACHE_TTL):
        return _profile_cache["text"]
    
    try:
        conn = _get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT fact FROM agent_memory 
            WHERE category = 'company_profile' AND subject = 'Company Profile' AND is_active = true
            ORDER BY updated_at DESC LIMIT 1
        """)
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row and row[0]:
            _profile_cache["text"] = row[0]
            _profile_cache["loaded_at"] = now
            logger.info(f"Company Profile загружен ({len(row[0])} символов)")
            return row[0]
        else:
            logger.warning("Company Profile не найден в agent_memory")
            return ""
    
    except Exception as e:
        logger.error(f"Ошибка загрузки Company Profile: {e}")
        # Возвращаем кэш если есть, даже если устарел
        if _profile_cache["text"]:
            return _profile_cache["text"]
        return ""


def get_relevant_facts(query: str, limit: int = 10) -> str:
    """
    Подтягивает релевантные факты из agent_memory по ключевым словам.
    Используется для динамического обогащения контекста.
    """
    if not query:
        return ""
    
    try:
        conn = _get_db_connection()
        cur = conn.cursor()
        
        # Полнотекстовый поиск по фактам
        cur.execute("""
            SELECT category, subject, fact 
            FROM agent_memory 
            WHERE is_active = true 
              AND category != 'company_profile'
              AND (
                  to_tsvector('russian', fact) @@ plainto_tsquery('russian', %s)
                  OR subject ILIKE %s
                  OR fact ILIKE %s
              )
            ORDER BY confidence DESC
            LIMIT %s
        """, (query, f"%{query}%", f"%{query}%", limit))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        if not rows:
            return ""
        
        facts = []
        for cat, subj, fact in rows:
            facts.append(f"[{cat}] {subj}: {fact}")
        
        return "\n".join(facts)
    
    except Exception as e:
        logger.error(f"Ошибка поиска фактов: {e}")
        return ""


def invalidate_cache():
    """Сбрасывает кэш (вызывать после обновления профиля)."""
    _profile_cache["text"] = None
    _profile_cache["loaded_at"] = None
