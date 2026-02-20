"""
Автоизвлечение фактов из результатов анализа.
Вызывается после анализа документов и email threads.

Использование:
    from fact_extractor import extract_and_save_facts
    
    # После получения результата анализа:
    await extract_and_save_facts(analysis_text, source="document:накладная.pdf")
"""

import os
import re
import json
import logging
import requests
import psycopg2
from datetime import datetime

logger = logging.getLogger(__name__)

# Настройки
ROUTERAI_API_KEY = os.getenv("ROUTERAI_API_KEY", "")
ROUTERAI_BASE_URL = os.getenv("ROUTERAI_BASE_URL", "https://api.routerai.com/v1")

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


def _save_fact(category: str, subject: str, fact: str, source: str) -> bool:
    """Сохраняет факт в agent_memory, если похожего нет."""
    try:
        conn = _get_db_connection()
        cur = conn.cursor()
        
        # Проверяем дубликат (по subject + начало факта)
        cur.execute("""
            SELECT id FROM agent_memory
            WHERE subject = %s AND LEFT(fact, 100) = LEFT(%s, 100) AND is_active = true
            LIMIT 1
        """, (subject, fact))
        
        existing = cur.fetchone()
        
        if existing:
            # Обновляем confidence
            cur.execute("""
                UPDATE agent_memory 
                SET updated_at = NOW(), confidence = LEAST(confidence + 0.1, 1.0)
                WHERE id = %s
            """, (existing[0],))
            conn.commit()
            cur.close()
            conn.close()
            return False  # не новый
        
        cur.execute("""
            INSERT INTO agent_memory (category, subject, fact, source, confidence)
            VALUES (%s, %s, %s, %s, 0.7)
        """, (category, subject, fact, source))
        
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения факта: {e}")
        return False


async def extract_and_save_facts(analysis_text: str, source: str = "auto"):
    """
    Извлекает факты из текста анализа и сохраняет в agent_memory.
    Вызывается после анализа документа или email thread.
    
    Args:
        analysis_text: Результат анализа (то что LLM вернул)
        source: Откуда факт (например "document:накладная.pdf" или "email_thread:123")
    """
    if not analysis_text or len(analysis_text) < 50:
        return
    
    if not ROUTERAI_API_KEY:
        return
    
    try:
        prompt = f"""Из текста анализа извлеки ТОЛЬКО НОВЫЕ ВАЖНЫЕ ФАКТЫ о бизнесе.

ПРАВИЛА:
- Извлекай только конкретные факты: суммы, даты, решения, условия, контакты
- НЕ извлекай общие фразы типа "документ содержит данные"
- НЕ извлекай факты которые очевидны (что компания продаёт кондитерские изделия)
- Максимум 3 факта из одного анализа (только самые важные)
- Если важных фактов нет — верни пустой массив

КАТЕГОРИИ: клиент, поставщик, продукция, цена, решение, процесс, проблема, контакт, договор

ТЕКСТ АНАЛИЗА:
{analysis_text[:3000]}

Ответь ТОЛЬКО JSON массивом (без пояснений):
[
  {{"category": "категория", "subject": "о ком/чём", "fact": "конкретный факт"}},
  ...
]

Если фактов нет: []"""

        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {ROUTERAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "google/gemini-2.0-flash-001",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,
                "temperature": 0.1
            },
            timeout=30
        )
        
        result = response.json()
        if "choices" not in result:
            return
        
        answer = result["choices"][0]["message"]["content"].strip()
        
        # Убираем markdown если есть
        if answer.startswith("```"):
            answer = answer.split("```")[1]
            if answer.startswith("json"):
                answer = answer[4:]
        
        # Пробуем извлечь JSON массив
        match = re.search(r'\[.*\]', answer, re.DOTALL)
        if not match:
            return
        
        facts = json.loads(match.group())
        
        if not isinstance(facts, list):
            return
        
        saved_count = 0
        for fact_data in facts[:3]:  # максимум 3 факта
            if not isinstance(fact_data, dict):
                continue
            
            category = fact_data.get("category", "").strip()
            subject = fact_data.get("subject", "").strip()
            fact = fact_data.get("fact", "").strip()
            
            if not category or not subject or not fact:
                continue
            
            if len(fact) < 10:
                continue
            
            if _save_fact(category, subject, fact, source):
                saved_count += 1
                logger.info(f"Новый факт: [{category}] {subject}: {fact[:80]}...")
        
        if saved_count > 0:
            logger.info(f"Извлечено и сохранено {saved_count} новых фактов из {source}")
    
    except json.JSONDecodeError:
        logger.debug(f"Fact extraction: не удалось распарсить JSON")
    except Exception as e:
        logger.error(f"Ошибка извлечения фактов: {e}")


async def extract_facts_from_thread_summary(summary_data: dict, thread_id: int, subject: str = ""):
    """
    Извлекает факты из сводки email thread (async версия).
    """
    if not summary_data:
        return
    
    parts = []
    if summary_data.get("summary_detailed"):
        parts.append(summary_data["summary_detailed"])
    if summary_data.get("key_decisions"):
        parts.append("Решения: " + "; ".join(summary_data["key_decisions"]))
    if summary_data.get("action_items"):
        parts.append("Задачи: " + "; ".join(summary_data["action_items"]))
    
    if not parts:
        return
    
    combined = "\n".join(parts)
    source = f"email_thread:{thread_id}"
    if subject:
        source += f":{subject[:50]}"
    
    await extract_and_save_facts(combined, source=source)


def extract_facts_from_thread_summary_sync(summary_data: dict, thread_id: int, subject: str = ""):
    """
    Синхронная версия для email_sync.py (который не использует async).
    """
    if not summary_data:
        return
    
    parts = []
    if summary_data.get("summary_detailed"):
        parts.append(summary_data["summary_detailed"])
    if summary_data.get("key_decisions"):
        parts.append("Решения: " + "; ".join(summary_data["key_decisions"]))
    if summary_data.get("action_items"):
        parts.append("Задачи: " + "; ".join(summary_data["action_items"]))
    
    if not parts:
        return
    
    combined = "\n".join(parts)
    source = f"email_thread:{thread_id}"
    if subject:
        source += f":{subject[:50]}"
    
    _extract_and_save_facts_sync(combined, source=source)


def _extract_and_save_facts_sync(analysis_text: str, source: str = "auto"):
    """Синхронная версия extract_and_save_facts."""
    if not analysis_text or len(analysis_text) < 50:
        return
    
    if not ROUTERAI_API_KEY:
        return
    
    try:
        prompt = f"""Из текста анализа извлеки ТОЛЬКО НОВЫЕ ВАЖНЫЕ ФАКТЫ о бизнесе.

ПРАВИЛА:
- Извлекай только конкретные факты: суммы, даты, решения, условия, контакты
- НЕ извлекай общие фразы типа "документ содержит данные"
- НЕ извлекай факты которые очевидны (что компания продаёт кондитерские изделия)
- Максимум 3 факта из одного анализа (только самые важные)
- Если важных фактов нет — верни пустой массив

КАТЕГОРИИ: клиент, поставщик, продукция, цена, решение, процесс, проблема, контакт, договор

ТЕКСТ АНАЛИЗА:
{analysis_text[:3000]}

Ответь ТОЛЬКО JSON массивом (без пояснений):
[
  {{"category": "категория", "subject": "о ком/чём", "fact": "конкретный факт"}},
  ...
]

Если фактов нет: []"""

        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {ROUTERAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "google/gemini-2.0-flash-001",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,
                "temperature": 0.1
            },
            timeout=30
        )
        
        result = response.json()
        if "choices" not in result:
            return
        
        answer = result["choices"][0]["message"]["content"].strip()
        
        if answer.startswith("```"):
            answer = answer.split("```")[1]
            if answer.startswith("json"):
                answer = answer[4:]
        
        match = re.search(r'\[.*\]', answer, re.DOTALL)
        if not match:
            return
        
        facts = json.loads(match.group())
        
        if not isinstance(facts, list):
            return
        
        saved_count = 0
        for fact_data in facts[:3]:
            if not isinstance(fact_data, dict):
                continue
            
            category = fact_data.get("category", "").strip()
            subject = fact_data.get("subject", "").strip()
            fact = fact_data.get("fact", "").strip()
            
            if not category or not subject or not fact or len(fact) < 10:
                continue
            
            if _save_fact(category, subject, fact, source):
                saved_count += 1
                logger.info(f"Новый факт: [{category}] {subject}: {fact[:80]}...")
        
        if saved_count > 0:
            logger.info(f"Извлечено и сохранено {saved_count} новых фактов из {source}")
    
    except json.JSONDecodeError:
        logger.debug(f"Fact extraction: не удалось распарсить JSON")
    except Exception as e:
        logger.error(f"Ошибка извлечения фактов: {e}")
