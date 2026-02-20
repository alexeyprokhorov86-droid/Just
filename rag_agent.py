"""
RAG Agent для поиска по базе знаний и интернету.
Включает SQL-поиск и векторный (семантический) поиск с учётом свежести.
"""

import os
import pathlib
from dotenv import load_dotenv
from company_context import get_company_profile

env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

import json
import logging
import requests
import psycopg2
from psycopg2 import sql
import re
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ROUTERAI_API_KEY = os.getenv("ROUTERAI_API_KEY")
ROUTERAI_BASE_URL = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")

# Импорт векторного поиска
try:
    from embedding_service import vector_search, vector_search_weighted, index_telegram_message
    VECTOR_SEARCH_ENABLED = True
    logger.info("Векторный поиск включен")
except ImportError:
    VECTOR_SEARCH_ENABLED = False
    logger.warning("embedding_service не найден, векторный поиск отключен")


def get_db_connection():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


def clean_keywords(query: str) -> list:
    """Очищает ключевые слова от пунктуации."""
    clean_query = re.sub(r'[,.:;!?()"\']', ' ', query)
    keywords = [w.strip() for w in clean_query.split() if len(w.strip()) > 2]
    return keywords if keywords else [query]

def extract_time_context(question: str) -> dict:
    """
    Извлекает временной контекст из запроса.
    
    Если в запросе указан период (за последний месяц, вчера, в январе) —
    настраивает параметры поиска под этот период.
    
    Если период не указан — использует decay_days=90 по умолчанию.
    """
    question_lower = question.lower()
    now = datetime.now()
    
    result = {
        "has_time_filter": False,
        "date_from": None,
        "date_to": None,
        "decay_days": 90,  # По умолчанию 90 дней
        "freshness_weight": 0.25  # По умолчанию
    }
    
    # Паттерны для "за последний/последние N дней/недель/месяцев"
    patterns = [
        # "за последний месяц", "за последние 2 месяца"
        (r'за последн(?:ий|ие|юю|ее)?\s*(\d+)?\s*месяц', lambda m: int(m.group(1) or 1) * 30),
        (r'за (\d+)\s*месяц', lambda m: int(m.group(1)) * 30),
        
        # "за последнюю неделю", "за последние 2 недели"  
        (r'за последн(?:ий|ие|юю|ее)?\s*(\d+)?\s*недел', lambda m: int(m.group(1) or 1) * 7),
        (r'за (\d+)\s*недел', lambda m: int(m.group(1)) * 7),
        
        # "за последний день", "за последние 3 дня"
        (r'за последн(?:ий|ие|юю|ее)?\s*(\d+)?\s*(?:день|дня|дней)', lambda m: int(m.group(1) or 1)),
        (r'за (\d+)\s*(?:день|дня|дней)', lambda m: int(m.group(1))),
        
        # "за последний год"
        (r'за последн(?:ий|ие|юю|ее)?\s*год', lambda m: 365),
        (r'за год', lambda m: 365),
        
        # "за последний квартал"
        (r'за последн(?:ий|ие|юю|ее)?\s*квартал', lambda m: 90),
        (r'за квартал', lambda m: 90),
        
        # "вчера"
        (r'\bвчера\b', lambda m: 2),
        
        # "сегодня"
        (r'\bсегодня\b', lambda m: 1),
        
        # "на этой неделе"
        (r'на этой неделе', lambda m: 7),
        (r'на прошлой неделе', lambda m: 14),
        
        # "в этом месяце"
        (r'в этом месяце', lambda m: now.day),
        (r'в прошлом месяце', lambda m: 60),
        
        # "недавно" - используем 14 дней
        (r'\bнедавно\b', lambda m: 14),
        
        # "в последнее время" - 30 дней
        (r'в последнее время', lambda m: 30),
    ]
    
    for pattern, days_func in patterns:
        match = re.search(pattern, question_lower)
        if match:
            result["has_time_filter"] = True
            result["decay_days"] = days_func(match)
            result["date_from"] = now - timedelta(days=result["decay_days"])
            result["date_to"] = now
            # Если указан конкретный период — увеличиваем вес свежести
            result["freshness_weight"] = 0.4
            break
    
    # Паттерны для конкретных месяцев: "в январе", "в январе 2025"
    months = {
        'январ': 1, 'феврал': 2, 'март': 3, 'апрел': 4,
        'мае': 5, 'мая': 5, 'май': 5, 'июн': 6, 'июл': 7, 'август': 8,
        'сентябр': 9, 'октябр': 10, 'ноябр': 11, 'декабр': 12
    }
    
    if not result["has_time_filter"]:
        for month_pattern, month_num in months.items():
            match = re.search(rf'в\s+{month_pattern}\w*\s*(\d{{4}})?', question_lower)
            if match:
                year = int(match.group(1)) if match.group(1) else now.year
                # Если месяц в будущем этого года — берём прошлый год
                if month_num > now.month and year == now.year:
                    year -= 1
                
                # Первый день месяца
                result["date_from"] = datetime(year, month_num, 1)
                # Последний день месяца
                if month_num == 12:
                    result["date_to"] = datetime(year + 1, 1, 1) - timedelta(days=1)
                else:
                    result["date_to"] = datetime(year, month_num + 1, 1) - timedelta(days=1)
                
                result["has_time_filter"] = True
                result["decay_days"] = (now - result["date_from"]).days or 30
                result["freshness_weight"] = 0.5  # Точный период — высокий вес
                break
    
    return result

def diversify_by_source_id(
    items: list,
    total_limit: int,
    max_per_source: int = 2,
    score_key: str = "final_score",
    source_id_key: str = "source_id",
) -> list:
    """
    Ограничивает число результатов от одного источника (source_id).
    Нужна для email, где один email = несколько чанков => много попаданий из одного письма.

    Логика:
    - ожидаем, что items уже отсортированы по score desc (или мы сортируем сами)
    - берём по max_per_source на один source_id
    - останавливаемся на total_limit
    """
    if not items:
        return []

    # На всякий случай сортируем (чтобы не зависеть от поведения БД/индекса)
    items = sorted(items, key=lambda x: x.get(score_key, 0), reverse=True)

    per_source_count = {}
    out = []

    for it in items:
        sid = it.get(source_id_key)
        if sid is None:
            # если source_id отсутствует — считаем как уникальный
            out.append(it)
            if len(out) >= total_limit:
                break
            continue

        cnt = per_source_count.get(sid, 0)
        if cnt >= max_per_source:
            continue

        per_source_count[sid] = cnt + 1
        out.append(it)

        if len(out) >= total_limit:
            break

    return out

def search_telegram_chats_sql(query: str, limit: int = 30) -> list:
    """SQL-поиск по чатам (точное совпадение слов)."""
    results = []
    conn = get_db_connection()
    keywords = clean_keywords(query)
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'tg_chat_%' AND table_name != 'tg_chats_metadata' AND table_name != 'tg_user_roles'""")
            chat_tables = [row[0] for row in cur.fetchall()]
            for table_name in chat_tables:
                for keyword in keywords[:2]:
                    try:
                        cur.execute(sql.SQL("SELECT timestamp, first_name, message_text, media_analysis, message_type FROM {} WHERE message_text ILIKE %s OR media_analysis ILIKE %s ORDER BY timestamp DESC LIMIT %s").format(sql.Identifier(table_name)), (f"%{keyword}%", f"%{keyword}%", limit))
                        for row in cur.fetchall():
                            chat_name = table_name.replace('tg_chat_', '').split('_', 1)[-1].replace('_', ' ').title()
                            content = row[2] or ""
                            if row[3]:
                                content += f"\n[Анализ]: {row[3][:500]}"
                            result = {"source": f"Чат: {chat_name}", "date": row[0].strftime("%d.%m.%Y %H:%M") if row[0] else "", "author": row[1] or "", "content": content[:1000], "type": row[4] or "text"}
                            if result not in results:
                                results.append(result)
                    except:
                        continue
    finally:
        conn.close()
    return results[:limit]


def search_telegram_chats_vector(query: str, limit: int = 30, time_context: dict = None) -> list:
    """Векторный (семантический) поиск по чатам с учётом свежести."""
    if not VECTOR_SEARCH_ENABLED:
        return []
    
    # Получаем параметры времени
    if time_context is None:
        time_context = extract_time_context(query)
    
    decay_days = time_context.get("decay_days", 90)
    freshness_weight = time_context.get("freshness_weight", 0.25)
    
    try:
        # Используем взвешенный поиск с учётом свежести
        vector_results = vector_search_weighted(
            query, 
            limit=limit, 
            source_type='telegram',
            freshness_weight=freshness_weight,
            decay_days=decay_days
        )
        
        results = []
        for r in vector_results:
            chat_name = r['source_table'].replace('tg_chat_', '').split('_', 1)[-1].replace('_', ' ').title()
            
            result = {
                "source": f"Чат: {chat_name}",
                "content": r['content'][:1000],
                "type": "text",
                "similarity": r.get('similarity', 0),
                "freshness": r.get('freshness', 0),
                "final_score": r.get('final_score', r.get('similarity', 0)),
                "search_type": "vector"
            }
            
            # Добавляем дату если есть
            if r.get('timestamp'):
                result["date"] = r['timestamp'].strftime("%d.%m.%Y %H:%M")
            
            results.append(result)
        
        logger.info(f"Векторный поиск (decay={decay_days}d, fw={freshness_weight}): {len(results)} результатов")
        return results
        
    except Exception as e:
        logger.error(f"Ошибка векторного поиска: {e}")
        return []

def search_emails_sql(query: str, limit: int = 30) -> list:
    """SQL/keyword поиск по email — для точных совпадений (артикулы, номера, ИНН)."""
    results = []
    conn = get_db_connection()
    keywords = clean_keywords(query)
    
    try:
        with conn.cursor() as cur:
            # FTS поиск
            fts_query = ' | '.join(keywords[:3])
            cur.execute("""
                SELECT id, subject, body_text, from_address, received_at
                FROM email_messages
                WHERE to_tsvector('russian', COALESCE(subject, '') || ' ' || COALESCE(body_text, ''))
                      @@ to_tsquery('russian', %s)
                ORDER BY received_at DESC
                LIMIT %s
            """, (fts_query, limit * 2))
            
            fts_results = cur.fetchall()
            
            # Если FTS не дал результатов — ILIKE fallback
            if not fts_results:
                for keyword in keywords[:2]:
                    cur.execute("""
                        SELECT id, subject, body_text, from_address, received_at
                        FROM email_messages
                        WHERE subject ILIKE %s OR body_text ILIKE %s
                        ORDER BY received_at DESC
                        LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", limit))
                    fts_results.extend(cur.fetchall())
            
            seen_ids = set()
            for row in fts_results:
                if row[0] in seen_ids:
                    continue
                seen_ids.add(row[0])
                
                content = f"Тема: {row[1] or ''}\n{(row[2] or '')[:800]}"
                received_str = row[4].strftime("%d.%m.%Y") if row[4] else ""
                
                results.append({
                    "source": "Email",
                    "content": content,
                    "subject": row[1] or "",
                    "from_address": row[3] or "",
                    "date": received_str,
                    "similarity": 0.5,
                    "final_score": 0.5,
                    "search_type": "email_sql",
                    "source_id": row[0],
                })
                
                if len(results) >= limit:
                    break
                    
    except Exception as e:
        logger.error(f"Ошибка SQL поиска email: {e}")
    finally:
        conn.close()
    
    logger.info(f"Email SQL поиск: {len(results)} результатов")
    return results

def search_emails_vector(query: str, limit: int = 30, time_context: dict = None) -> list:
    """Семантический поиск по email с учётом свежести + diversity по source_id (чанки одного письма)."""
    if not VECTOR_SEARCH_ENABLED:
        return []

    if time_context is None:
        time_context = extract_time_context(query)

    decay_days = time_context.get("decay_days", 90)
    freshness_weight = time_context.get("freshness_weight", 0.25)

    # Сколько кандидатов взять до группировки:
    #  - если max_per_email=2 и нужно limit=10, то кандидатов лучше 50-80,
    #    чтобы после отбрасывания дублей не остаться без результатов.
    pre_limit = max(limit * 6, 50)
    max_chunks_per_email = 2

    results = []
    try:
        email_candidates = vector_search_weighted(
            query,
            limit=pre_limit,
            source_type='email',
            freshness_weight=freshness_weight,
            decay_days=decay_days
        )

        # Ключевой шаг пункта 1: ограничиваем чанки одного письма
        diversified = diversify_by_source_id(
            email_candidates,
            total_limit=limit,
            max_per_source=max_chunks_per_email,
            score_key="final_score",
            source_id_key="source_id",
        )

        for r in diversified:
            received_str = ""
            if r.get("received_at"):
                received_str = r["received_at"].strftime("%d.%m.%Y")

            results.append({
                "source": "Email",
                "content": r.get("content", ""),
                "subject": r.get("subject", ""),
                "from_address": r.get("from_address", ""),
                "date": received_str,
                "similarity": r.get("similarity", 0),
                "freshness": r.get("freshness", 0),
                "final_score": r.get("final_score", r.get("similarity", 0)),
                "search_type": "email_vector",
                # полезно для дальнейших шагов и отладки
                "source_id": r.get("source_id"),
            })

        logger.info(
            f"Email vector search: pre_limit={pre_limit}, diversified={len(results)} "
            f"(max_per_email={max_chunks_per_email}, decay={decay_days}d, fw={freshness_weight})"
        )

    except Exception as e:
        logger.error(f"Ошибка поиска email: {e}")

    return results

def search_emails(query: str, limit: int = 30, time_context: dict = None) -> list:
    """
    Комбинированный поиск по email:
    1. Векторный поиск (семантический) — находит по смыслу
    2. SQL поиск — находит точные совпадения (артикулы, номера, ИНН)
    3. Объединяем и дедуплицируем
    """
    results = []
    seen_ids = set()
    
    # Сначала векторный поиск
    vector_results = search_emails_vector(query, limit=limit, time_context=time_context)
    for r in vector_results:
        source_id = r.get('source_id')
        if source_id and source_id in seen_ids:
            continue
        if source_id:
            seen_ids.add(source_id)
        results.append(r)
    
    # Затем SQL поиск для точных совпадений
    sql_results = search_emails_sql(query, limit=limit)
    for r in sql_results:
        source_id = r.get('source_id')
        if source_id and source_id in seen_ids:
            continue
        if source_id:
            seen_ids.add(source_id)
        results.append(r)
    
    # Сортируем по final_score
    results.sort(key=lambda x: x.get('final_score', 0), reverse=True)
    
    logger.info(f"Поиск email: {len(results)} результатов (vector + sql)")
    return results[:limit]

def search_telegram_chats(query: str, limit: int = 30, time_context: dict = None) -> list:
    """
    Комбинированный поиск по чатам:
    1. Векторный поиск (семантический) — находит по смыслу с учётом свежести
    2. SQL поиск — находит точные совпадения
    3. Объединяем и дедуплицируем
    """
    results = []
    seen_content = set()
    
    # Сначала векторный поиск (с учётом временного контекста)
    vector_results = search_telegram_chats_vector(query, limit=limit, time_context=time_context)
    for r in vector_results:
        content_hash = hash(r['content'][:200])
        if content_hash not in seen_content:
            seen_content.add(content_hash)
            results.append(r)
    
    # Затем SQL поиск для точных совпадений
    sql_results = search_telegram_chats_sql(query, limit=limit)
    for r in sql_results:
        content_hash = hash(r['content'][:200])
        if content_hash not in seen_content:
            seen_content.add(content_hash)
            results.append(r)
    
    # Сортируем по final_score (если есть) или similarity
    results.sort(key=lambda x: x.get('final_score', x.get('similarity', 0)), reverse=True)
    
    logger.info(f"Поиск в чатах: {len(results)} результатов (vector + sql)")
    return results[:limit]


def search_1c_data(query: str, limit: int = 30) -> list:
    """Универсальный поиск по данным 1С с JOIN-ами по справочникам.
    
    Приоритет результатов:
    1. Закупочные цены (purchase_prices)
    2. Продажи (sales) 
    3. Заказы клиентов (c1_customer_orders + items)
    4. Заказы поставщикам (c1_supplier_orders + items)
    5. Производство (c1_production + items)
    6. Банковские расходы (c1_bank_expenses)
    7. Внутреннее потребление (c1_internal_consumption + items)
    8. Инвентаризация (c1_inventory_count + items)
    9. Номенклатура справочник
    10. Клиенты справочник
    """
    # Категории результатов с приоритетами
    results_by_category = {
        "prices": [],        # приоритет 1
        "sales": [],         # приоритет 2
        "cust_orders": [],   # приоритет 3
        "supp_orders": [],   # приоритет 4
        "production": [],    # приоритет 5
        "bank": [],          # приоритет 6
        "consumption": [],   # приоритет 7
        "inventory": [],     # приоритет 8
        "nomenclature": [],  # приоритет 9
        "clients": [],       # приоритет 10
    }
    
    conn = get_db_connection()
    keywords = clean_keywords(query)
    
    if not keywords:
        return []
    
    # Строим условие ILIKE для нескольких ключевых слов
    def ilike_conditions(columns: list, keyword: str) -> tuple:
        """Возвращает (SQL условие, параметры) для поиска по нескольким колонкам."""
        parts = []
        params = []
        for col in columns:
            parts.append(f"{col} ILIKE %s")
            params.append(f"%{keyword}%")
        return " OR ".join(parts), params
    
    try:
        with conn.cursor() as cur:
            for keyword in keywords[:3]:
                
                # ═══════════════════════════════════════
                # 1. ЗАКУПОЧНЫЕ ЦЕНЫ (purchase_prices)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT doc_date, doc_number, contractor_name, 
                               nomenclature_name, quantity, price, sum_total 
                        FROM purchase_prices 
                        WHERE nomenclature_name ILIKE %s 
                           OR contractor_name ILIKE %s 
                        ORDER BY doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {
                            "source": "1С: ЗАКУПОЧНЫЕ ЦЕНЫ",
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "",
                            "content": f"{row[3]} от {row[2]}: {row[5]} руб./ед., "
                                       f"кол-во: {row[4]}, сумма: {row[6]} руб. (док. {row[1]})",
                            "type": "price"
                        }
                        if result not in results_by_category["prices"]:
                            results_by_category["prices"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка закупочных цен: {e}")
                
                # ═══════════════════════════════════════
                # 2. ПРОДАЖИ (sales) — уже денормализована
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT doc_date, doc_number, doc_type, client_name, 
                               nomenclature_name, quantity, price, sum_with_vat
                        FROM sales 
                        WHERE client_name ILIKE %s 
                           OR nomenclature_name ILIKE %s
                           OR consignee_name ILIKE %s
                        ORDER BY doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {
                            "source": f"1С: ПРОДАЖИ ({row[2]})",
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "",
                            "content": f"{row[4]} → {row[3]}: {row[6]} руб./ед., "
                                       f"кол-во: {row[5]}, сумма: {row[7]} руб. (док. {row[1]})",
                            "type": "sales"
                        }
                        if result not in results_by_category["sales"]:
                            results_by_category["sales"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка продаж: {e}")
                
                # ═══════════════════════════════════════
                # 3. ЗАКАЗЫ КЛИЕНТОВ (c1_customer_orders + items)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT co.doc_date, co.doc_number, c.name as client,
                               n.name as product, coi.quantity, coi.price, coi.sum_total,
                               co.status, co.shipment_date
                        FROM c1_customer_orders co
                        JOIN c1_customer_order_items coi ON coi.order_key = co.ref_key
                        LEFT JOIN clients c ON co.partner_key = c.id::text
                        LEFT JOIN nomenclature n ON coi.nomenclature_key = n.id::text
                        WHERE (c.name ILIKE %s OR n.name ILIKE %s OR co.doc_number ILIKE %s)
                          AND co.is_deleted = false
                        ORDER BY co.doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        shipment = f", отгрузка: {row[8].strftime('%d.%m.%Y')}" if row[8] else ""
                        result = {
                            "source": "1С: ЗАКАЗЫ КЛИЕНТОВ",
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "",
                            "content": f"{row[3] or '?'} → {row[2] or '?'}: {row[5]} руб., "
                                       f"кол-во: {row[4]}, сумма: {row[6]} руб. "
                                       f"(док. {row[1]}, статус: {row[7] or '?'}{shipment})",
                            "type": "customer_order"
                        }
                        if result not in results_by_category["cust_orders"]:
                            results_by_category["cust_orders"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка заказов клиентов: {e}")
                
                # ═══════════════════════════════════════
                # 4. ЗАКАЗЫ ПОСТАВЩИКАМ (c1_supplier_orders + items)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT so.doc_date, so.doc_number, c.name as supplier,
                               n.name as product, soi.quantity, soi.price, soi.sum_total,
                               so.status
                        FROM c1_supplier_orders so
                        JOIN c1_supplier_order_items soi ON soi.order_key = so.ref_key
                        LEFT JOIN clients c ON so.partner_key = c.id::text
                        LEFT JOIN nomenclature n ON soi.nomenclature_key = n.id::text
                        WHERE (c.name ILIKE %s OR n.name ILIKE %s OR so.doc_number ILIKE %s)
                          AND so.is_deleted = false
                        ORDER BY so.doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {
                            "source": "1С: ЗАКАЗЫ ПОСТАВЩИКАМ",
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "",
                            "content": f"{row[3] or '?'} от {row[2] or '?'}: {row[5]} руб., "
                                       f"кол-во: {row[4]}, сумма: {row[6]} руб. "
                                       f"(док. {row[1]}, статус: {row[7] or '?'})",
                            "type": "supplier_order"
                        }
                        if result not in results_by_category["supp_orders"]:
                            results_by_category["supp_orders"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка заказов поставщикам: {e}")
                
                # ═══════════════════════════════════════
                # 5. ПРОИЗВОДСТВО (c1_production + items)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT p.doc_date, p.doc_number, 
                               n.name as product, pi.quantity, pi.price, pi.sum_total
                        FROM c1_production p
                        JOIN c1_production_items pi ON pi.production_key = p.ref_key
                        LEFT JOIN nomenclature n ON pi.nomenclature_key = n.id::text
                        WHERE (n.name ILIKE %s OR p.doc_number ILIKE %s OR p.comment ILIKE %s)
                          AND p.is_deleted = false
                        ORDER BY p.doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {
                            "source": "1С: ПРОИЗВОДСТВО",
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "",
                            "content": f"{row[2] or '?'}: кол-во: {row[3]}, "
                                       f"цена: {row[4]} руб., сумма: {row[5]} руб. (док. {row[1]})",
                            "type": "production"
                        }
                        if result not in results_by_category["production"]:
                            results_by_category["production"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка производства: {e}")
                
                # ═══════════════════════════════════════
                # 6. БАНКОВСКИЕ РАСХОДЫ (c1_bank_expenses)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT be.doc_date, be.doc_number, c.name as counterparty,
                               be.amount, be.purpose, be.comment
                        FROM c1_bank_expenses be
                        LEFT JOIN clients c ON be.counterparty_key = c.id::text
                        WHERE (c.name ILIKE %s OR be.purpose ILIKE %s 
                               OR be.comment ILIKE %s OR be.doc_number ILIKE %s)
                          AND be.is_deleted = false
                        ORDER BY be.doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        purpose = row[4][:100] if row[4] else ""
                        result = {
                            "source": "1С: БАНКОВСКИЕ РАСХОДЫ",
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "",
                            "content": f"{row[2] or '?'}: {row[3]} руб. "
                                       f"Назначение: {purpose} (док. {row[1]})",
                            "type": "bank_expense"
                        }
                        if result not in results_by_category["bank"]:
                            results_by_category["bank"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка банковских расходов: {e}")
                
                # ═══════════════════════════════════════
                # 7. ВНУТРЕННЕЕ ПОТРЕБЛЕНИЕ (c1_internal_consumption + items)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT ic.doc_date, ic.doc_number,
                               n.name as product, ici.quantity, ici.sum_total
                        FROM c1_internal_consumption ic
                        JOIN c1_internal_consumption_items ici ON ici.doc_key = ic.ref_key
                        LEFT JOIN nomenclature n ON ici.nomenclature_key = n.id::text
                        WHERE (n.name ILIKE %s OR ic.doc_number ILIKE %s OR ic.comment ILIKE %s)
                          AND ic.is_deleted = false
                        ORDER BY ic.doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {
                            "source": "1С: ВНУТРЕННЕЕ ПОТРЕБЛЕНИЕ",
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "",
                            "content": f"{row[2] or '?'}: кол-во: {row[3]}, "
                                       f"сумма: {row[4]} руб. (док. {row[1]})",
                            "type": "consumption"
                        }
                        if result not in results_by_category["consumption"]:
                            results_by_category["consumption"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка внутреннего потребления: {e}")
                
                # ═══════════════════════════════════════
                # 8. ИНВЕНТАРИЗАЦИЯ (c1_inventory_count + items)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT inv.doc_date, inv.doc_number,
                               n.name as product, ii.quantity_fact, 
                               ii.quantity_account, ii.deviation
                        FROM c1_inventory_count inv
                        JOIN c1_inventory_count_items ii ON ii.doc_key = inv.ref_key
                        LEFT JOIN nomenclature n ON ii.nomenclature_key = n.id::text
                        WHERE (n.name ILIKE %s OR inv.doc_number ILIKE %s)
                          AND inv.is_deleted = false
                        ORDER BY inv.doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        deviation = row[5] if row[5] else 0
                        dev_str = f"+{deviation}" if deviation > 0 else str(deviation)
                        result = {
                            "source": "1С: ИНВЕНТАРИЗАЦИЯ",
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "",
                            "content": f"{row[2] or '?'}: факт: {row[3]}, учёт: {row[4]}, "
                                       f"отклонение: {dev_str} (док. {row[1]})",
                            "type": "inventory"
                        }
                        if result not in results_by_category["inventory"]:
                            results_by_category["inventory"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка инвентаризации: {e}")
                
                # ═══════════════════════════════════════
                # 9. НОМЕНКЛАТУРА (справочник)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT name, code, unit FROM nomenclature 
                        WHERE name ILIKE %s OR code ILIKE %s 
                        LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {
                            "source": "1С: Номенклатура",
                            "content": f"{row[0]} (код: {row[1]}, ед.: {row[2]})",
                            "type": "nomenclature"
                        }
                        if result not in results_by_category["nomenclature"]:
                            results_by_category["nomenclature"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка номенклатуры: {e}")
                
                # ═══════════════════════════════════════
                # 10. КЛИЕНТЫ (справочник)
                # ═══════════════════════════════════════
                try:
                    cur.execute("""
                        SELECT name, inn FROM clients 
                        WHERE name ILIKE %s OR inn ILIKE %s 
                        LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {
                            "source": "1С: Клиенты",
                            "content": f"{row[0]} (ИНН: {row[1]})",
                            "type": "client"
                        }
                        if result not in results_by_category["clients"]:
                            results_by_category["clients"].append(result)
                except Exception as e:
                    logger.debug(f"Ошибка клиентов: {e}")
    
    finally:
        conn.close()
    
    # ═══════════════════════════════════════
    # СБОРКА РЕЗУЛЬТАТОВ ПО ПРИОРИТЕТУ
    # ═══════════════════════════════════════
    # Порядок категорий определяет приоритет
    category_order = [
        "prices", "sales", "cust_orders", "supp_orders",
        "production", "bank", "consumption", "inventory",
        "nomenclature", "clients"
    ]
    
    final_results = []
    for cat in category_order:
        items = results_by_category[cat]
        remaining = limit - len(final_results)
        if remaining <= 0:
            break
        final_results.extend(items[:remaining])
    
    # Логирование
    counts = {cat: len(items) for cat, items in results_by_category.items() if items}
    logger.info(f"Поиск 1С по {keywords}: {counts}, итого: {len(final_results)}")
    
    return final_results[:limit]


def search_internet(query: str) -> tuple:
    """Поиск в интернете через Perplexity. Возвращает (текст, список_ссылок)."""
    if not ROUTERAI_API_KEY:
        return "", []
    try:
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": "perplexity/sonar", "messages": [{"role": "user", "content": query}]},
            timeout=60
        )
        result = response.json()
        
        if "choices" not in result:
            return "", []
        
        text = result["choices"][0]["message"]["content"]
        citations = result.get("citations", [])
        
        return text, citations
        
    except Exception as e:
        logger.error(f"Ошибка интернет: {e}")
        return "", []


def generate_response(question: str, db_results: list, web_results: str, web_citations: list = None, chat_context: str = "") -> str:
    """Генерация ответа на основе найденных данных."""
    if not ROUTERAI_API_KEY:
        return "API ключ не настроен"
    try:
        context_parts = []
        
        # Группируем результаты по типу
        prices = [r for r in db_results if r.get('type') == 'price']
        other_1c = [r for r in db_results if r.get('source', '').startswith('1С') and r.get('type') != 'price']
        chats = [r for r in db_results if r.get('source', '').startswith('Чат')]
        emails = [r for r in db_results if r.get('source', '').startswith('Email')]
        
        # Сначала закупочные цены (ПРИОРИТЕТ!)
        if prices:
            context_parts.append("=== ЗАКУПОЧНЫЕ ЦЕНЫ КОМПАНИИ (данные 1С) ===")
            for i, res in enumerate(prices, 1):
                context_parts.append(f"{i}. {res.get('date', '')} {res['content']}")
        
        # Потом справочники 1С
        if other_1c:
            context_parts.append("\n=== СПРАВОЧНИКИ 1С ===")
            for i, res in enumerate(other_1c, 1):
                context_parts.append(f"{i}. [{res['source']}] {res['content'][:300]}")
        
        # Потом чаты
        if chats:
            context_parts.append("\n=== ИЗ ЧАТОВ ===")
            for i, res in enumerate(chats[:5], 1):
                score_info = ""
                if 'final_score' in res:
                    score_info = f" [релевантность: {res['final_score']:.0%}]"
                elif 'similarity' in res:
                    score_info = f" [релевантность: {res['similarity']:.0%}]"
                date_info = f" ({res['date']})" if res.get('date') else ""
                context_parts.append(f"{i}.{score_info}{date_info} {res['content'][:300]}")
        
        # Потом email
        if emails:
            context_parts.append("\n=== ИЗ EMAIL ===")
            for i, res in enumerate(emails[:5], 1):
                score_info = f" [релевантность: {res.get('final_score', res.get('similarity', 0)):.0%}]"
                date_info = f" ({res['date']})" if res.get('date') else ""
                subj = (res.get("subject") or "").strip()
                frm = (res.get("from_address") or "").strip()
                header = ""
                if subj or frm:
                    header = f"{subj} | {frm}".strip(" |")

                context_parts.append(
                    f"{i}.{score_info}{date_info} {header}\n{res['content'][:400]}"
                )

        
        # Интернет
        if web_results:
            context_parts.append("\n=== ИНТЕРНЕТ ===")
            context_parts.append(web_results[:2000])
        
        context = "\n".join(context_parts)
        
        company_profile = get_company_profile()
        
        prompt = f"""{company_profile}

Ты — RAG-агент компании Фрумелад. Отвечай на русском.

ВОПРОС: {question}

НАЙДЕННЫЕ ДАННЫЕ:
{context if context else "Ничего не найдено."}

ИНСТРУКЦИИ:
1. Используй знания из профиля компании и найденные данные для ответа
2. Данные из 1С (закупки, продажи, номенклатура) — это реальные данные компании
3. Данные из ЧАТОВ и EMAIL — внутренняя переписка сотрудников
4. Указывай конкретные цифры, даты, имена — если они есть в данных
5. Если данных недостаточно — скажи об этом, не придумывай
6. Отвечай по существу вопроса, кратко и структурированно

Ответ:"""

        response = requests.post(f"{ROUTERAI_BASE_URL}/chat/completions", headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"}, json={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": prompt}], "max_tokens": 2000}, timeout=60)
        result = response.json()
        if "choices" in result:
            response_text = result["choices"][0]["message"]["content"]
            
            if web_citations:
                response_text += "\n\n📎 **Источники:**"
                for i, url in enumerate(web_citations[:5], 1):
                    response_text += f"\n{i}. {url}"
            
            return response_text
        return "Ошибка генерации"
    except Exception as e:
        return f"Ошибка: {e}"


def classify_question(question: str) -> dict:
    """Классификация вопроса для выбора источников поиска."""
    if not ROUTERAI_API_KEY:
        return {"search_1c": True, "search_chats": True, "search_email": True, "search_web": False, "keywords": question, "priority": "1c"}
    try:
        prompt = f"""Определи где искать информацию.
Источники: 1С (цены, закупки, товары), Чаты (обсуждения в Telegram), Email (переписка по почте), Интернет (внешняя информация).
Извлеки 1-3 КЛЮЧЕВЫХ СЛОВА (существительные без запятых: сахар мука торт)

Вопрос: {question}

JSON: {{"search_1c": true/false, "search_chats": true/false, "search_email": true/false, "search_web": true/false, "keywords": "слово1 слово2", "priority": "1c/chats/email/web"}}"""
        response = requests.post(f"{ROUTERAI_BASE_URL}/chat/completions", headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"}, json={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": prompt}], "max_tokens": 200}, timeout=30)
        result = response.json()
        if "choices" in result:
            match = re.search(r'\{[^}]+\}', result["choices"][0]["message"]["content"])
            if match:
                return json.loads(match.group())
        return {"search_1c": True, "search_chats": True, "search_email": True, "search_web": False, "keywords": question, "priority": "1c"}
    except:
        return {"search_1c": True, "search_chats": True, "search_email": True, "search_web": False, "keywords": question, "priority": "1c"}

def rerank_results(question: str, results: list, top_k: int = 10) -> list:
    """
    Переранжирование результатов через LLM.
    Берёт до 60 кандидатов, просит GPT оценить релевантность, возвращает top_k лучших.
    """
    if not results or not ROUTERAI_API_KEY:
        return results[:top_k]
    
    # Берём максимум 60 кандидатов для reranking
    candidates = results[:60]
    
    if len(candidates) <= top_k:
        return candidates
    
    # Формируем список для оценки
    docs_text = []
    for i, r in enumerate(candidates):
        source = r.get('source', 'Unknown')
        content = r.get('content', '')[:300]
        date = r.get('date', '')
        docs_text.append(f"[{i}] ({source}, {date}) {content}")
    
    docs_joined = "\n".join(docs_text)
    
    prompt = f"""Оцени релевантность документов для вопроса.

ВОПРОС: {question}

ДОКУМЕНТЫ:
{docs_joined}

Верни ТОЛЬКО номера {top_k} самых релевантных документов через запятую, от лучшего к худшему.
Пример ответа: 3,7,1,4,9,2,0,5,8,6

Номера:"""

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
                "max_tokens": 2000,
                "temperature": 0
            },
            timeout=30
        )
        
        result = response.json()
        if "choices" not in result:
            logger.warning(f"Rerank: нет choices в ответе")
            return candidates[:top_k]
        
        answer = result["choices"][0]["message"]["content"].strip()
        
        # Парсим номера
        indices = []
        for part in answer.replace(" ", "").split(","):
            try:
                idx = int(part.strip())
                if 0 <= idx < len(candidates) and idx not in indices:
                    indices.append(idx)
            except ValueError:
                continue
        
        if not indices:
            logger.warning(f"Rerank: не удалось распарсить ответ '{answer}'")
            return candidates[:top_k]
        
        # Собираем результаты в новом порядке
        reranked = [candidates[i] for i in indices[:top_k]]
        
        # Добавляем оставшиеся если не хватает
        if len(reranked) < top_k:
            for r in candidates:
                if r not in reranked:
                    reranked.append(r)
                if len(reranked) >= top_k:
                    break
        
        logger.info(f"Rerank: {len(candidates)} -> {len(reranked)} (top {top_k})")
        return reranked
        
    except Exception as e:
        logger.error(f"Ошибка reranking: {e}")
        return candidates[:top_k]

async def process_rag_query(question: str, chat_context: str = "") -> str:
    """Основная функция обработки RAG-запроса с учётом временного контекста."""
    logger.info(f"RAG запрос: {question}")
    
    # Извлекаем временной контекст из вопроса
    time_context = extract_time_context(question)
    if time_context["has_time_filter"]:
        logger.info(f"Временной контекст: decay_days={time_context['decay_days']}, fw={time_context['freshness_weight']}")
    
    # Классифицируем вопрос
    classification = classify_question(question)
    logger.info(f"Классификация: {classification}")
    
    keywords = classification.get("keywords", question)
    db_results = []
    
    # Поиск в 1С (SQL) — всегда первым
    if classification.get("search_1c", True):
        c1_results = search_1c_data(keywords, limit=30)
        db_results.extend(c1_results)
        logger.info(f"Найдено в 1С: {len(c1_results)}")
    
    # Поиск в чатах (векторный с учётом свежести + SQL)
    if classification.get("search_chats", True):
        chat_results = search_telegram_chats(keywords, limit=30, time_context=time_context)
        db_results.extend(chat_results)
        logger.info(f"Найдено в чатах: {len(chat_results)}")
    
    # Поиск в email (векторный с учётом свежести)
    if classification.get("search_email", True):
        email_results = search_emails(keywords, limit=30, time_context=time_context)
        db_results.extend(email_results)
        logger.info(f"Найдено в email: {len(email_results)}")
    
    logger.info(f"Всего в БД: {len(db_results)}")
    
    # Reranking — переранжируем результаты через LLM
    if len(db_results) > 10:
        db_results = rerank_results(question, db_results, top_k=15)
    
    # Поиск в интернете
    web_results = ""
    web_citations = []
    if classification.get("search_web", False):
        web_results, web_citations = search_internet(question)
    
    return generate_response(question, db_results, web_results, web_citations, chat_context)

async def index_new_message(table_name: str, message_id: int, content: str):
    """Индексирует новое сообщение для векторного поиска."""
    if not VECTOR_SEARCH_ENABLED:
        return
    
    if not content or len(content.strip()) < 10:
        return
    
    try:
        index_telegram_message(table_name, message_id, content)
        logger.debug(f"Проиндексировано сообщение {message_id} из {table_name}")
    except Exception as e:
        logger.error(f"Ошибка индексации сообщения: {e}")
