#!/usr/bin/env python3
"""
review_knowledge.py — LLM-ревизор базы знаний.

Проверяет новые факты/решения, удаляет мусор, предлагает новые правила фильтрации.
Запускается раз в сутки после distillation.

Cron: 0 4 * * * cd /home/admin/telegram_logger_bot && .../python review_knowledge.py >> .../review_knowledge.log 2>&1
"""

import os
import sys
import json
import time
import logging
import fcntl
import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv('/home/admin/telegram_logger_bot/.env')

ROUTER_AI_KEY = os.getenv('ROUTERAI_API_KEY', '')
ROUTER_AI_URL = os.getenv('ROUTERAI_BASE_URL', 'https://routerai.ru/api/v1')
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
ADMIN_USER_ID = os.getenv('ADMIN_USER_ID', '')

DB_CONFIG = {
    'host': '172.17.0.2',
    'port': 5432,
    'dbname': 'knowledge_base',
    'user': 'knowledge',
    'password': os.getenv('DB_PASSWORD', 'Prokhorov2025Secure')
}

# Сколько фактов проверять за запуск
MAX_FACTS_PER_RUN = 200
# Размер батча для LLM
LLM_BATCH_SIZE = 20


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def call_llm(messages, model="openai/gpt-4.1", temperature=0.1):
    """Вызов LLM через RouterAI."""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {ROUTER_AI_KEY}'
    }
    payload = {
        'model': model,
        'messages': messages,
        'temperature': temperature,
        'max_tokens': 4000
    }
    resp = requests.post(
        f'{ROUTER_AI_URL}/chat/completions',
        headers=headers,
        json=payload,
        timeout=120
    )
    resp.raise_for_status()
    return resp.json()['choices'][0]['message']['content']


def get_company_context(conn):
    """Загружает контекст компании для ревизора."""
    cur = conn.cursor()
    
    # Ключевые факты из agent_memory
    cur.execute("""
        SELECT category, subject, fact 
        FROM agent_memory 
        WHERE is_active = true AND category IN ('компания', 'персонал', 'бренд', 'продукция', 'клиент', 'поставщик')
        ORDER BY category, subject
        LIMIT 100
    """)
    memory_facts = cur.fetchall()
    
    # Текущие правила фильтрации
    cur.execute("""
        SELECT rule_type, value, reason, hit_count 
        FROM km_filter_rules 
        WHERE is_active = true 
        ORDER BY hit_count DESC
    """)
    rules = cur.fetchall()
    
    cur.close()
    
    # Формируем контекст
    context = "=== О КОМПАНИИ ===\n"
    current_cat = ""
    for cat, subj, fact in memory_facts:
        if cat != current_cat:
            context += f"\n[{cat.upper()}]\n"
            current_cat = cat
        context += f"- {subj}: {fact[:200]}\n"
    
    context += "\n=== ТЕКУЩИЕ ПРАВИЛА ФИЛЬТРАЦИИ ===\n"
    for rtype, value, reason, hits in rules:
        context += f"- {rtype}: '{value}' ({reason}, срабатываний: {hits})\n"
    
    return context


def get_items_for_review(conn, limit=MAX_FACTS_PER_RUN):
    """Получает сущности для проверки — факты, решения, задачи, политики."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    items = []
    
    # Факты (приоритет — без subject)
    cur.execute("""
        SELECT f.id, 'fact' as item_type, f.fact_type as subtype, f.fact_text as text,
               e.canonical_name as subject_name
        FROM km_facts f
        LEFT JOIN km_entities e ON e.id = f.subject_entity_id
        WHERE f.created_at > NOW() - INTERVAL '48 hours'
          AND f.verification_status = 'extracted'
        ORDER BY CASE WHEN f.subject_entity_id IS NULL THEN 0 ELSE 1 END, f.confidence ASC
        LIMIT %s
    """, (limit // 2,))
    items.extend(cur.fetchall())
    
    # Решения
    cur.execute("""
        SELECT id, 'decision' as item_type, scope_type as subtype, decision_text as text,
               NULL as subject_name
        FROM km_decisions
        WHERE created_at > NOW() - INTERVAL '48 hours'
          AND verification_status = 'extracted'
        ORDER BY created_at DESC
        LIMIT %s
    """, (limit // 6,))
    items.extend(cur.fetchall())
    
    # Задачи
    cur.execute("""
        SELECT id, 'task' as item_type, NULL as subtype, task_text as text,
               NULL as subject_name
        FROM km_tasks
        WHERE created_at > NOW() - INTERVAL '48 hours'
          AND verification_status = 'extracted'
        ORDER BY created_at DESC
        LIMIT %s
    """, (limit // 6,))
    items.extend(cur.fetchall())
    
    # Политики
    cur.execute("""
        SELECT id, 'policy' as item_type, NULL as subtype, policy_text as text,
               NULL as subject_name
        FROM km_policies
        WHERE created_at > NOW() - INTERVAL '48 hours'
          AND verification_status = 'extracted'
        ORDER BY created_at DESC
        LIMIT %s
    """, (limit // 6,))
    items.extend(cur.fetchall())
    
    cur.close()
    return items


REVIEW_SYSTEM_PROMPT = """Ты — ревизор базы знаний кондитерской компании Фрумелад (ООО "Фрумелад" и ООО "НФ").

Компания производит торты, пирожные, печенье для крупных розничных сетей (Лента, Окей, Ozon, Wildberries).
Бренды: Кондитерская Прохорова, Пинкис, Майти Боллз, Пачка Благородная.

Твоя задача — проверить извлечённые факты и определить для каждого:
- keep — факт полезен для компании (информация о поставщиках, клиентах, ценах, производстве, сотрудниках, логистике)
- delete — факт не относится к компании (рассылки, спам, чужие компании, персональные данные, общие фразы)
- uncertain — непонятно, нужна дополнительная проверка

Также предложи новые слова/паттерны для автоматической фильтрации если заметишь повторяющийся мусор.

Отвечай ТОЛЬКО валидным JSON:
{
  "verdicts": [
    {"id": 123, "type": "fact|decision|task|policy", "verdict": "keep|delete|uncertain", "reason": "краткая причина"}
  ],
  "new_rules": [
    {"value": "слово или паттерн", "reason": "почему это мусор"}
  ],
  "remove_rules": [
    {"value": "слово или паттерн", "reason": "почему это правило ошибочно фильтрует полезное"}
  ]
}"""


def review_batch(facts_batch, company_context):
    """Проверяет батч фактов через LLM."""
    facts_text = "\n".join([
        f"[{f['item_type'].upper()} ID:{f['id']}] subtype={f['subtype'] or '-'} subject={f['subject_name'] or 'НЕТ'} | {f['text'][:200]}"
        for f in facts_batch
    ])
    
    user_content = f"""{company_context}

=== ФАКТЫ ДЛЯ ПРОВЕРКИ ({len(facts_batch)} шт) ===
{facts_text}

Проверь каждый факт. JSON:"""
    
    result = call_llm([
        {'role': 'system', 'content': REVIEW_SYSTEM_PROMPT},
        {'role': 'user', 'content': user_content}
    ])
    
    # Парсим JSON
    result = result.strip()
    if result.startswith('```'):
        result = result.split('\n', 1)[1].rsplit('```', 1)[0]
    
    return json.loads(result)


def apply_verdicts(conn, verdicts):
    """Применяет вердикты ревизора ко всем типам сущностей."""
    cur = conn.cursor()
    stats = {'kept': 0, 'deleted': 0, 'uncertain': 0}
    
    table_map = {
        'fact': ('km_facts', 'id'),
        'decision': ('km_decisions', 'id'),
        'task': ('km_tasks', 'id'),
        'policy': ('km_policies', 'id'),
    }
    
    for v in verdicts:
        item_id = v.get('id')
        item_type = v.get('type', 'fact')
        verdict = v.get('verdict', 'uncertain')
        
        table, id_col = table_map.get(item_type, ('km_facts', 'id'))
        
        if verdict == 'delete':
            cur.execute(f"DELETE FROM {table} WHERE {id_col} = %s", (item_id,))
            stats['deleted'] += 1
        elif verdict == 'keep':
            cur.execute(f"UPDATE {table} SET verification_status = 'verified', updated_at = NOW() WHERE {id_col} = %s", (item_id,))
            stats['kept'] += 1
        else:
            cur.execute(f"UPDATE {table} SET verification_status = 'uncertain', updated_at = NOW() WHERE {id_col} = %s", (item_id,))
            stats['uncertain'] += 1
    
    conn.commit()
    cur.close()
    return stats


def apply_new_rules(conn, new_rules, remove_rules):
    """Добавляет/удаляет правила фильтрации."""
    cur = conn.cursor()
    added = 0
    removed = 0
    
    for rule in new_rules:
        value = rule.get('value', '').lower().strip()
        reason = rule.get('reason', '')
        if not value or len(value) < 3:
            continue
        # Проверяем что такого правила ещё нет
        cur.execute("SELECT id FROM km_filter_rules WHERE value = %s AND is_active = true", (value,))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO km_filter_rules (rule_type, target, value, reason, added_by)
                VALUES ('junk_word', 'all', %s, %s, 'llm_reviewer')
            """, (value, reason))
            added += 1
            logger.info(f"  Новое правило: '{value}' — {reason}")
    
    for rule in remove_rules:
        value = rule.get('value', '').lower().strip()
        reason = rule.get('reason', '')
        if not value:
            continue
        cur.execute("""
            UPDATE km_filter_rules SET is_active = false, updated_at = NOW()
            WHERE value = %s AND is_active = true
        """, (value,))
        if cur.rowcount > 0:
            removed += 1
            logger.info(f"  Правило отключено: '{value}' — {reason}")
    
    conn.commit()
    cur.close()
    return added, removed


def send_report(text):
    """Отправляет отчёт в Telegram."""
    if not ADMIN_USER_ID or not BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for i in range(0, len(text), 4000):
        try:
            requests.post(url, json={
                'chat_id': ADMIN_USER_ID,
                'text': text[i:i+4000],
                'parse_mode': 'HTML'
            }, timeout=30, proxies={'https': 'socks5h://127.0.0.1:1080'})
        except Exception as e:
            logger.error(f"Отправка отчёта: {e}")


def main():
    # Lock
    lock_file = open('/tmp/review_knowledge.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("review_knowledge уже запущен")
        sys.exit(0)
    
    logger.info("=" * 60)
    logger.info("РЕВИЗИЯ ЗНАНИЙ — СТАРТ")
    logger.info("=" * 60)
    
    conn = get_conn()
    
    # Загружаем контекст компании
    company_context = get_company_context(conn)
    logger.info(f"Контекст компании: {len(company_context)} символов")
    
    # Получаем факты для проверки
    facts = get_items_for_review(conn)
    logger.info(f"Фактов для проверки: {len(facts)}")
    
    if not facts:
        logger.info("Нечего проверять")
        send_report("🔬 Ревизия знаний: нечего проверять ✅")
        conn.close()
        return
    
    total_stats = {'kept': 0, 'deleted': 0, 'uncertain': 0}
    total_new_rules = 0
    total_removed_rules = 0
    errors = 0
    
    # Обрабатываем батчами
    for i in range(0, len(facts), LLM_BATCH_SIZE):
        batch = facts[i:i+LLM_BATCH_SIZE]
        batch_num = i // LLM_BATCH_SIZE + 1
        logger.info(f"Батч {batch_num}: {len(batch)} фактов")
        
        try:
            result = review_batch(batch, company_context)
            
            # Применяем вердикты
            verdicts = result.get('verdicts', [])
            stats = apply_verdicts(conn, verdicts)
            for k in stats:
                total_stats[k] += stats[k]
            
            # Применяем новые правила
            new_rules = result.get('new_rules', [])
            remove_rules = result.get('remove_rules', [])
            added, removed = apply_new_rules(conn, new_rules, remove_rules)
            total_new_rules += added
            total_removed_rules += removed
            
            logger.info(f"  kept={stats['kept']}, deleted={stats['deleted']}, "
                       f"uncertain={stats['uncertain']}, +rules={added}, -rules={removed}")
            
            time.sleep(1)  # Rate limiting
            
        except json.JSONDecodeError as e:
            logger.error(f"  JSON parse error: {e}")
            errors += 1
        except Exception as e:
            logger.error(f"  Ошибка: {e}")
            errors += 1
    
    # Статистика
    conn2 = get_conn()
    cur = conn2.cursor()
    cur.execute("SELECT COUNT(*) FROM km_facts")
    total_facts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM km_filter_rules WHERE is_active = true")
    active_rules = cur.fetchone()[0]
    cur.close()
    conn2.close()
    
    # Отчёт
    report = (
        f"<b>🔬 Ревизия знаний</b>\n\n"
        f"Проверено: {len(facts)} фактов\n"
        f"✅ Оставлено: {total_stats['kept']}\n"
        f"❌ Удалено: {total_stats['deleted']}\n"
        f"❓ Неопределённо: {total_stats['uncertain']}\n"
        f"⚠️ Ошибок: {errors}\n\n"
        f"📏 Правила фильтрации:\n"
        f"  + Добавлено: {total_new_rules}\n"
        f"  - Отключено: {total_removed_rules}\n"
        f"  Всего активных: {active_rules}\n\n"
        f"📊 Всего фактов в базе: {total_facts}"
    )
    
    logger.info(f"Отчёт:\n{report}")
    send_report(report)
    
    conn.close()
    logger.info("РЕВИЗИЯ ЗАВЕРШЕНА")


if __name__ == '__main__':
    main()
