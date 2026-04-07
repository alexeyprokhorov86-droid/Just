#!/usr/bin/env python3
"""
review_knowledge.py — LLM-ревизор базы знаний.

Step 0: Дедупликация по embedding similarity (cosine > 0.95)
Step 1: LLM-ревью новых фактов/решений/задач/политик

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
    'host': os.getenv('DB_HOST', '172.20.0.2'),
    'port': 5432,
    'dbname': 'knowledge_base',
    'user': 'knowledge',
    'password': os.getenv('DB_PASSWORD')
}

# Сколько фактов проверять за запуск
MAX_FACTS_PER_RUN = 200
# Размер батча для LLM
LLM_BATCH_SIZE = 20

# === Дедупликация ===
DEDUP_SIMILARITY_THRESHOLD = 0.95
DEDUP_BATCH_SIZE = 500
DEDUP_MAX_PER_RUN = 2000


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


# ============================================================
# STEP 0: ДЕДУПЛИКАЦИЯ ПО EMBEDDING SIMILARITY
# ============================================================

# Таблицы для дедупликации: (table_name, text_column)
DEDUP_TABLES = [
    ('km_facts', 'fact_text'),
    ('km_decisions', 'decision_text'),
    ('km_tasks', 'task_text'),
    ('km_policies', 'policy_text'),
]


def deduplicate_table(conn, table, text_col, max_dupes=DEDUP_MAX_PER_RUN):
    """
    Универсальная дедупликация для любой km_* таблицы.
    Находит дубли через KNN-поиск (HNSW index).
    Из пары оставляет более длинный текст, короткий -> 'duplicate'.
    """
    cur = conn.cursor()

    cur.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE embedding IS NOT NULL
          AND verification_status NOT IN ('rejected', 'duplicate')
    """)
    total_active = cur.fetchone()[0]
    logger.info(f"[DEDUP] {table}: активных с embedding: {total_active}")

    if total_active == 0:
        return 0

    offset = 0
    total_dupes = 0
    seen_pairs = set()

    while offset < total_active and total_dupes < max_dupes:
        cur.execute(f"""
            SELECT id, embedding, LENGTH({text_col}) as len
            FROM {table}
            WHERE embedding IS NOT NULL
              AND verification_status NOT IN ('rejected', 'duplicate')
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, (DEDUP_BATCH_SIZE, offset))
        batch = cur.fetchall()

        if not batch:
            break

        batch_dupes = 0
        for fid, emb, flen in batch:
            if total_dupes >= max_dupes:
                break

            cur.execute(f"""
                SELECT b.id, 1 - (b.embedding <=> %s::vector) as sim, LENGTH(b.{text_col}) as len
                FROM {table} b
                WHERE b.id != %s
                  AND b.embedding IS NOT NULL
                  AND b.verification_status NOT IN ('rejected', 'duplicate')
                ORDER BY b.embedding <=> %s::vector
                LIMIT 1
            """, (emb, fid, emb))
            row = cur.fetchone()

            if row and row[1] >= DEDUP_SIMILARITY_THRESHOLD:
                neighbor_id, sim, neighbor_len = row

                pair_key = tuple(sorted([fid, neighbor_id]))
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                if flen >= neighbor_len:
                    remove_id = neighbor_id
                else:
                    remove_id = fid

                cur.execute(f"""
                    UPDATE {table}
                    SET verification_status = 'duplicate', updated_at = NOW()
                    WHERE id = %s
                      AND verification_status NOT IN ('rejected', 'duplicate')
                """, (remove_id,))

                if cur.rowcount > 0:
                    total_dupes += 1
                    batch_dupes += 1

        conn.commit()
        logger.info(f"[DEDUP] {table} offset={offset}: {batch_dupes} дублей")
        offset += DEDUP_BATCH_SIZE

    logger.info(f"[DEDUP] {table}: итого помечено дублями: {total_dupes}")
    return total_dupes


def deduplicate_all(conn):
    """Дедупликация по всем km_* таблицам."""
    results = {}
    for table, text_col in DEDUP_TABLES:
        try:
            count = deduplicate_table(conn, table, text_col)
            results[table] = count
        except Exception as e:
            logger.error(f"[DEDUP] Ошибка {table}: {e}")
            results[table] = 0
    return results


# ============================================================
# STEP 1: LLM-РЕВЬЮ (без изменений)
# ============================================================

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
            cur.execute(f"""
                UPDATE {table} SET verification_status = 'rejected', updated_at = NOW()
                WHERE {id_col} = %s
            """, (item_id,))
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
                INSERT INTO km_filter_rules (rule_type, target, value, reason, added_by, approval_status)
                VALUES ('junk_word', 'all', %s, %s, 'llm_reviewer', 'pending')
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

    # ============================================================
    # STEP 0: ДЕДУПЛИКАЦИЯ
    # ============================================================
    logger.info("--- STEP 0: Дедупликация по embedding similarity ---")
    try:
        dedup_results = deduplicate_all(conn)
        dedup_count = sum(dedup_results.values())
    except Exception as e:
        logger.error(f"Ошибка дедупликации: {e}")
        dedup_results = {}
        dedup_count = 0

    # ============================================================
    # STEP 1: LLM-РЕВЬЮ
    # ============================================================
    logger.info("--- STEP 1: LLM-ревью новых фактов ---")

    # Загружаем контекст компании
    company_context = get_company_context(conn)
    logger.info(f"Контекст компании: {len(company_context)} символов")

    # Получаем факты для проверки
    facts = get_items_for_review(conn)
    # Лимит — не более 30% от проверенных
    max_delete = max(int(len(facts) * 0.3), 5)  # минимум 5
    logger.info(f"Фактов для проверки: {len(facts)}")

    if not facts:
        logger.info("Нечего проверять (LLM)")
        dedup_lines = "\n".join([f"  {t}: {c}" for t, c in dedup_results.items() if c > 0]) or "  нет дублей"
        report = (
            f"<b>🔬 Ревизия знаний</b>\n\n"
            f"<b>Step 0 — Дедупликация ({dedup_count}):</b>\n"
            f"{dedup_lines}\n\n"
            f"<b>Step 1 — LLM-ревью:</b>\n"
            f"Нечего проверять ✅"
        )
        send_report(report)
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

    # Проверка лимита
    limit_warning = ""
    if total_stats['deleted'] > max_delete:
        limit_warning = f"\n⚠️ ВНИМАНИЕ: удалено {total_stats['deleted']} > лимит {max_delete} (30%)! Проверьте правила!"
        logger.warning(limit_warning)

    # Сохраняем метрики здоровья
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO km_health_metrics (metric_date, facts_rejected, facts_verified, facts_deduplicated, rules_added, rules_disabled)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s)
            ON CONFLICT (metric_date) DO UPDATE SET
                facts_rejected = km_health_metrics.facts_rejected + EXCLUDED.facts_rejected,
                facts_verified = km_health_metrics.facts_verified + EXCLUDED.facts_verified,
                facts_deduplicated = km_health_metrics.facts_deduplicated + EXCLUDED.facts_deduplicated,
                rules_added = km_health_metrics.rules_added + EXCLUDED.rules_added,
                rules_disabled = km_health_metrics.rules_disabled + EXCLUDED.rules_disabled
        """, (total_stats['deleted'], total_stats['kept'], dedup_count, total_new_rules, total_removed_rules))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"Ошибка сохранения метрик: {e}")

    # Статистика
    conn2 = get_conn()
    cur = conn2.cursor()
    cur.execute("SELECT COUNT(*) FROM km_facts WHERE verification_status NOT IN ('rejected','duplicate')")
    active_facts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM km_filter_rules WHERE is_active = true")
    active_rules = cur.fetchone()[0]
    cur.close()
    conn2.close()

    # Отчёт
    dedup_lines = "\n".join([f"  {t}: {c}" for t, c in dedup_results.items() if c > 0]) or "  нет дублей"
    report = (
        f"<b>🔬 Ревизия знаний</b>\n\n"
        f"<b>Step 0 — Дедупликация ({dedup_count}):</b>\n"
        f"{dedup_lines}\n\n"
        f"<b>Step 1 — LLM-ревью:</b>\n"
        f"Проверено: {len(facts)}\n"
        f"✅ Оставлено: {total_stats['kept']}\n"
        f"❌ Отклонено: {total_stats['deleted']}\n"
        f"❓ Неопределённо: {total_stats['uncertain']}\n"
        f"⚠️ Ошибок: {errors}\n\n"
        f"📏 Правила: +{total_new_rules} / -{total_removed_rules} (активных: {active_rules})\n"
        f"📊 Активных фактов: {active_facts}"
        f"{limit_warning}"
    )

    logger.info(f"Отчёт:\n{report}")
    send_report(report)

    conn.close()
    logger.info("РЕВИЗИЯ ЗАВЕРШЕНА")


if __name__ == '__main__':
    main()
