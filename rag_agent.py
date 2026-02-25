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
from datetime import datetime, timedelta, date

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


def _resolve_period(period_str):
    """Преобразует строку периода из Router в (date_from, date_to)."""
    if not period_str or period_str == "null":
        return None, None
    
    today = date.today()
    
    # Календарная неделя (пн-вс)
    if period_str == "week":
        monday = today - timedelta(days=today.weekday())
        return monday, today
    
    if period_str == "last_week":
        monday = today - timedelta(days=today.weekday() + 7)
        sunday = monday + timedelta(days=6)
        return monday, sunday
    
    # Календарный месяц
    if period_str == "month":
        return date(today.year, today.month, 1), today
    
    if period_str == "last_month":
        first_this = date(today.year, today.month, 1)
        last_prev = first_this - timedelta(days=1)
        first_prev = date(last_prev.year, last_prev.month, 1)
        return first_prev, last_prev
    
    # Календарный квартал
    if period_str == "quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        return date(today.year, q_month, 1), today
    
    if period_str == "last_quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_month, 1)
        last_q_end = q_start - timedelta(days=1)
        last_q_month = ((last_q_end.month - 1) // 3) * 3 + 1
        return date(last_q_end.year, last_q_month, 1), last_q_end
    
    # Не календарные — просто N дней назад, без верхней границы
    simple_map = {
        "today": today,
        "yesterday": today - timedelta(days=1),
        "2weeks": today - timedelta(weeks=2),
        "half_year": today - timedelta(days=180),
        "year": today - timedelta(days=365),
    }
    
    if period_str in simple_map:
        return simple_map[period_str], None
    
    # Конкретный месяц (january, february, ...)
    months = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
    if period_str in months:
        month_num = months[period_str]
        year = today.year
        if month_num > today.month:
            year -= 1
        first_day = date(year, month_num, 1)
        if month_num == 12:
            last_day = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = date(year, month_num + 1, 1) - timedelta(days=1)
        return first_day, last_day
    
    return None, None


def search_1c_analytics(analytics_type, keywords="", period_date=None, 
                         entities=None, limit=20):
    """Агрегированные запросы по данным 1С (топ клиентов, товаров, поставщиков)."""
    results = []
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            
            # ТОП КЛИЕНТОВ ПО ПРОДАЖАМ
            if analytics_type in ("top_clients", "sales_summary"):
                try:
                    sql = """
                        SELECT client_name, 
                               COUNT(*) as positions,
                               SUM(sum_with_vat) as revenue,
                               MIN(doc_date) as first_date,
                               MAX(doc_date) as last_date,
                               COUNT(DISTINCT doc_number) as docs_count
                        FROM sales 
                        WHERE doc_type = 'Реализация'
                    """
                    params = []
                    if period_date:
                        sql += " AND doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        sql += " AND doc_date <= %s"
                        params.append(period_end)
                    if entities and entities.get("clients"):
                        client_filters = []
                        for client in entities["clients"]:
                            client_filters.append("client_name ILIKE %s")
                            params.append(f"%{client}%")
                        sql += " AND (" + " OR ".join(client_filters) + ")"
                    sql += " GROUP BY client_name ORDER BY revenue DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(sql, params)
                    
                    for row in cur.fetchall():
                        revenue = f"{row[2]:,.0f}" if row[2] else "0"
                        period = ""
                        if row[3] and row[4]:
                            period = f" (период: {row[3].strftime('%d.%m.%Y')} — {row[4].strftime('%d.%m.%Y')})"
                        results.append({
                            "source": "1С: АНАЛИТИКА ПРОДАЖ ПО КЛИЕНТАМ",
                            "date": row[4].strftime("%d.%m.%Y") if row[4] else "",
                            "content": f"{row[0]}: выручка {revenue} руб., "
                                       f"{row[1]} позиций, {row[5]} документов{period}",
                            "type": "analytics_sales_client"
                        })
                except Exception as e:
                    logger.debug(f"Ошибка аналитики клиентов: {e}")
            
            # ТОП ТОВАРОВ ПО ПРОДАЖАМ
            if analytics_type in ("top_products", "sales_summary"):
                try:
                    sql = """
                        SELECT nomenclature_name,
                               SUM(quantity) as total_qty,
                               SUM(sum_with_vat) as revenue,
                               AVG(price) as avg_price,
                               COUNT(DISTINCT client_name) as clients_count
                        FROM sales
                        WHERE doc_type = 'Реализация'
                    """
                    params = []
                    if period_date:
                        sql += " AND doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        sql += " AND doc_date <= %s"
                        params.append(period_end)
                    if entities and entities.get("products"):
                        prod_filters = []
                        for prod in entities["products"]:
                            prod_filters.append("nomenclature_name ILIKE %s")
                            params.append(f"%{prod}%")
                        sql += " AND (" + " OR ".join(prod_filters) + ")"
                    sql += " GROUP BY nomenclature_name ORDER BY revenue DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(sql, params)
                    
                    for row in cur.fetchall():
                        revenue = f"{row[2]:,.0f}" if row[2] else "0"
                        avg_price = f"{row[3]:,.2f}" if row[3] else "?"
                        results.append({
                            "source": "1С: АНАЛИТИКА ПРОДАЖ ПО ТОВАРАМ",
                            "date": "",
                            "content": f"{row[0]}: выручка {revenue} руб., "
                                       f"кол-во: {row[1]}, ср.цена: {avg_price} руб., "
                                       f"клиентов: {row[4]}",
                            "type": "analytics_sales_product"
                        })
                except Exception as e:
                    logger.debug(f"Ошибка аналитики товаров: {e}")
            
            # ТОП ПОСТАВЩИКОВ ПО ЗАКУПКАМ
            if analytics_type in ("top_suppliers", "purchase_summary"):
                try:
                    sql = """
                        SELECT contractor_name,
                               COUNT(*) as positions,
                               SUM(sum_total) as total_sum,
                               COUNT(DISTINCT nomenclature_name) as products_count,
                               MAX(doc_date) as last_date
                        FROM purchase_prices
                    """
                    params = []
                    if period_date:
                        sql += " WHERE doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        sql += " AND doc_date <= %s"
                        params.append(period_end)
                    if entities and entities.get("suppliers"):
                        prefix = " AND " if period_date else " WHERE "
                        supp_filters = []
                        for supp in entities["suppliers"]:
                            supp_filters.append("contractor_name ILIKE %s")
                            params.append(f"%{supp}%")
                        sql += prefix + "(" + " OR ".join(supp_filters) + ")"
                    sql += " GROUP BY contractor_name ORDER BY total_sum DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(sql, params)
                    
                    for row in cur.fetchall():
                        total = f"{row[2]:,.0f}" if row[2] else "0"
                        results.append({
                            "source": "1С: АНАЛИТИКА ЗАКУПОК ПО ПОСТАВЩИКАМ",
                            "date": row[4].strftime("%d.%m.%Y") if row[4] else "",
                            "content": f"{row[0]}: сумма закупок {total} руб., "
                                       f"{row[1]} позиций, {row[3]} наименований",
                            "type": "analytics_purchases"
                        })
                except Exception as e:
                    logger.debug(f"Ошибка аналитики закупок: {e}")
            
            # АНАЛИТИКА ПРОИЗВОДСТВА
            if analytics_type == "production_summary":
                try:
                    sql = """
                        SELECT n.name as product,
                               SUM(pi.quantity) as total_qty,
                               SUM(pi.sum_total) as total_sum,
                               COUNT(DISTINCT p.ref_key) as docs_count,
                               MAX(p.doc_date) as last_date
                        FROM c1_production p
                        JOIN c1_production_items pi ON pi.production_key = p.ref_key
                        LEFT JOIN nomenclature n ON pi.nomenclature_key = n.id::text
                        WHERE p.is_deleted = false
                    """
                    params = []
                    if period_date:
                        sql += " AND p.doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        sql += " AND p.doc_date <= %s"
                        params.append(period_end)
                    sql += " GROUP BY n.name ORDER BY total_sum DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(sql, params)
                    
                    for row in cur.fetchall():
                        total = f"{row[2]:,.0f}" if row[2] else "0"
                        results.append({
                            "source": "1С: АНАЛИТИКА ПРОИЗВОДСТВА",
                            "date": row[4].strftime("%d.%m.%Y") if row[4] else "",
                            "content": f"{row[0] or '?'}: произведено {row[1]}, "
                                       f"сумма: {total} руб., документов: {row[3]}",
                            "type": "analytics_production"
                        })
                except Exception as e:
                    logger.debug(f"Ошибка аналитики производства: {e}")
    
    finally:
        conn.close()
    
    logger.info(f"Аналитика 1С [{analytics_type}]: {len(results)} результатов")
    return results


def search_1c_data(query, limit=30, period_date=None, entities=None):
    """Универсальный поиск по данным 1С с JOIN-ами по справочникам."""
    results_by_category = {
        "prices": [], "sales": [], "cust_orders": [], "supp_orders": [],
        "production": [], "bank": [], "consumption": [], "inventory": [],
        "nomenclature": [], "clients": [],
    }
    
    conn = get_db_connection()
    keywords = clean_keywords(query)
    
    if not keywords:
        return []
    
    try:
        with conn.cursor() as cur:
            for keyword in keywords[:3]:
                
                # 1. ЗАКУПОЧНЫЕ ЦЕНЫ
                try:
                    q = """
                        SELECT doc_date, doc_number, contractor_name, 
                               nomenclature_name, quantity, price, sum_total 
                        FROM purchase_prices 
                        WHERE (nomenclature_name ILIKE %s OR contractor_name ILIKE %s)
                    """
                    params = [f"%{keyword}%", f"%{keyword}%"]
                    if period_date:
                        q += " AND doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        q += " AND doc_date <= %s"
                        params.append(period_end)
                    q += " ORDER BY doc_date DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
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
                
                # 2. ПРОДАЖИ
                try:
                    q = """
                        SELECT doc_date, doc_number, doc_type, client_name, 
                               nomenclature_name, quantity, price, sum_with_vat
                        FROM sales 
                        WHERE (client_name ILIKE %s OR nomenclature_name ILIKE %s OR consignee_name ILIKE %s)
                    """
                    params = [f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"]
                    if period_date:
                        q += " AND doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        q += " AND doc_date <= %s"
                        params.append(period_end)
                    q += " ORDER BY doc_date DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
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
                
                # 3. ЗАКАЗЫ КЛИЕНТОВ
                try:
                    q = """
                        SELECT co.doc_date, co.doc_number, c.name as client,
                               n.name as product, coi.quantity, coi.price, coi.sum_total,
                               co.status, co.shipment_date
                        FROM c1_customer_orders co
                        JOIN c1_customer_order_items coi ON coi.order_key = co.ref_key
                        LEFT JOIN clients c ON co.partner_key = c.id::text
                        LEFT JOIN nomenclature n ON coi.nomenclature_key = n.id::text
                        WHERE (c.name ILIKE %s OR n.name ILIKE %s OR co.doc_number ILIKE %s)
                          AND co.is_deleted = false
                    """
                    params = [f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"]
                    if period_date:
                        q += " AND co.doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        q += " AND co.doc_date <= %s"
                        params.append(period_end)
                    q += " ORDER BY co.doc_date DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
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
                
                # 4. ЗАКАЗЫ ПОСТАВЩИКАМ
                try:
                    q = """
                        SELECT so.doc_date, so.doc_number, c.name as supplier,
                               n.name as product, soi.quantity, soi.price, soi.sum_total,
                               so.status
                        FROM c1_supplier_orders so
                        JOIN c1_supplier_order_items soi ON soi.order_key = so.ref_key
                        LEFT JOIN clients c ON so.partner_key = c.id::text
                        LEFT JOIN nomenclature n ON soi.nomenclature_key = n.id::text
                        WHERE (c.name ILIKE %s OR n.name ILIKE %s OR so.doc_number ILIKE %s)
                          AND so.is_deleted = false
                    """
                    params = [f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"]
                    if period_date:
                        q += " AND so.doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        q += " AND so.doc_date <= %s"
                        params.append(period_end)
                    q += " ORDER BY so.doc_date DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
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
                
                # 5. ПРОИЗВОДСТВО
                try:
                    q = """
                        SELECT p.doc_date, p.doc_number, 
                               n.name as product, pi.quantity, pi.price, pi.sum_total
                        FROM c1_production p
                        JOIN c1_production_items pi ON pi.production_key = p.ref_key
                        LEFT JOIN nomenclature n ON pi.nomenclature_key = n.id::text
                        WHERE (n.name ILIKE %s OR p.doc_number ILIKE %s OR p.comment ILIKE %s)
                          AND p.is_deleted = false
                    """
                    params = [f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"]
                    if period_date:
                        q += " AND p.doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        q += " AND p.doc_date <= %s"
                        params.append(period_end)
                    q += " ORDER BY p.doc_date DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
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
                
                # 6. БАНКОВСКИЕ РАСХОДЫ
                try:
                    q = """
                        SELECT be.doc_date, be.doc_number, c.name as counterparty,
                               be.amount, be.purpose, be.comment
                        FROM c1_bank_expenses be
                        LEFT JOIN clients c ON be.counterparty_key = c.id::text
                        WHERE (c.name ILIKE %s OR be.purpose ILIKE %s 
                               OR be.comment ILIKE %s OR be.doc_number ILIKE %s)
                          AND be.is_deleted = false
                    """
                    params = [f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"]
                    if period_date:
                        q += " AND be.doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        q += " AND be.doc_date <= %s"
                        params.append(period_end)
                    q += " ORDER BY be.doc_date DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
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
                
                # 7. ВНУТРЕННЕЕ ПОТРЕБЛЕНИЕ
                try:
                    q = """
                        SELECT ic.doc_date, ic.doc_number,
                               n.name as product, ici.quantity, ici.sum_total
                        FROM c1_internal_consumption ic
                        JOIN c1_internal_consumption_items ici ON ici.doc_key = ic.ref_key
                        LEFT JOIN nomenclature n ON ici.nomenclature_key = n.id::text
                        WHERE (n.name ILIKE %s OR ic.doc_number ILIKE %s OR ic.comment ILIKE %s)
                          AND ic.is_deleted = false
                    """
                    params = [f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"]
                    if period_date:
                        q += " AND ic.doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        q += " AND ic.doc_date <= %s"
                        params.append(period_end)
                    q += " ORDER BY ic.doc_date DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
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
                
                # 8. ИНВЕНТАРИЗАЦИЯ
                try:
                    q = """
                        SELECT inv.doc_date, inv.doc_number,
                               n.name as product, ii.quantity_fact, 
                               ii.quantity_account, ii.deviation
                        FROM c1_inventory_count inv
                        JOIN c1_inventory_count_items ii ON ii.doc_key = inv.ref_key
                        LEFT JOIN nomenclature n ON ii.nomenclature_key = n.id::text
                        WHERE (n.name ILIKE %s OR inv.doc_number ILIKE %s)
                          AND inv.is_deleted = false
                    """
                    params = [f"%{keyword}%", f"%{keyword}%"]
                    if period_date:
                        q += " AND inv.doc_date >= %s"
                        params.append(period_date)
                    if period_end:
                        q += " AND inv.doc_date <= %s"
                        params.append(period_end)
                    q += " ORDER BY inv.doc_date DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
                    for row in cur.fetchall():
                        deviation = row[5] if row[5] else 0
                        dev_str = f"+{deviation}" if deviation and deviation > 0 else str(deviation)
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
                
                # 9. НОМЕНКЛАТУРА
                try:
                    cur.execute("""
                        SELECT name, code, unit FROM nomenclature 
                        WHERE name ILIKE %s OR code ILIKE %s LIMIT %s
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
                
                # 10. КЛИЕНТЫ
                try:
                    cur.execute("""
                        SELECT name, inn FROM clients 
                        WHERE name ILIKE %s OR inn ILIKE %s LIMIT %s
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
    
    # Сборка по приоритету
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


def generate_response(question, db_results, web_results, web_citations=None, chat_context=""):
    """Генерация ответа на основе найденных данных."""
    if not ROUTERAI_API_KEY:
        return "API ключ не настроен"
    try:
        context_parts = []
        
        # Группируем результаты по типу
        analytics = [r for r in db_results if r.get('type', '').startswith('analytics_')]
        prices = [r for r in db_results if r.get('type') == 'price']
        sales = [r for r in db_results if r.get('type') == 'sales']
        orders = [r for r in db_results if r.get('type') in ('customer_order', 'supplier_order')]
        production = [r for r in db_results if r.get('type') in ('production', 'consumption')]
        finance = [r for r in db_results if r.get('type') == 'bank_expense']
        inventory = [r for r in db_results if r.get('type') == 'inventory']
        refs = [r for r in db_results if r.get('type') in ('nomenclature', 'client')]
        chats = [r for r in db_results if r.get('source', '').startswith('Чат')]
        emails = [r for r in db_results if r.get('source', '').startswith('Email')]
        
        if analytics:
            context_parts.append("=== АНАЛИТИКА (агрегированные данные) ===")
            for i, res in enumerate(analytics, 1):
                context_parts.append(f"{i}. [{res['source']}] {res['content']}")
        
        if prices:
            context_parts.append("\n=== ЗАКУПОЧНЫЕ ЦЕНЫ ===")
            for i, res in enumerate(prices[:10], 1):
                context_parts.append(f"{i}. {res.get('date', '')} {res['content']}")
        
        if sales:
            context_parts.append("\n=== ПРОДАЖИ (документы) ===")
            for i, res in enumerate(sales[:10], 1):
                context_parts.append(f"{i}. {res.get('date', '')} {res['content']}")
        
        if orders:
            context_parts.append("\n=== ЗАКАЗЫ ===")
            for i, res in enumerate(orders[:10], 1):
                context_parts.append(f"{i}. [{res['source']}] {res.get('date', '')} {res['content']}")
        
        if production:
            context_parts.append("\n=== ПРОИЗВОДСТВО ===")
            for i, res in enumerate(production[:10], 1):
                context_parts.append(f"{i}. [{res['source']}] {res.get('date', '')} {res['content']}")
        
        if finance:
            context_parts.append("\n=== ФИНАНСЫ ===")
            for i, res in enumerate(finance[:10], 1):
                context_parts.append(f"{i}. {res.get('date', '')} {res['content']}")
        
        if inventory:
            context_parts.append("\n=== ИНВЕНТАРИЗАЦИЯ ===")
            for i, res in enumerate(inventory[:5], 1):
                context_parts.append(f"{i}. {res.get('date', '')} {res['content']}")
        
        if refs:
            context_parts.append("\n=== СПРАВОЧНИКИ ===")
            for i, res in enumerate(refs[:5], 1):
                context_parts.append(f"{i}. [{res['source']}] {res['content']}")
        
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
                context_parts.append(f"{i}.{score_info}{date_info} {header}\n{res['content'][:400]}")
        
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
2. Секция АНАЛИТИКА содержит агрегированные итоги — используй их для ответов про "топ", "основные", "сколько всего"
3. Данные из 1С (закупки, продажи, заказы, производство) — это реальные данные компании
4. Данные из ЧАТОВ и EMAIL — внутренняя переписка сотрудников
5. Указывай конкретные цифры, даты, имена — если они есть в данных
6. Если данных недостаточно — скажи об этом, не придумывай
7. Отвечай по существу вопроса, кратко и структурированно

Ответ:"""

        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": prompt}], "max_tokens": 2000, "temperature": 0},
            timeout=60
        )
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

def route_query(question, chat_context=""):
    """Router на GPT-4.1-mini — анализирует вопрос и строит план выполнения."""
    if not ROUTERAI_API_KEY:
        return _default_plan(question)
    
    try:
        prompt = f"""Ты — маршрутизатор запросов для бизнес-ассистента кондитерской компании "Фрумелад".

Доступные источники данных:
- 1С_ANALYTICS: агрегированные данные (топ клиентов, суммы продаж за период, рейтинги товаров, объёмы производства, суммы закупок). Используй когда нужны ИТОГИ, СУММЫ, РЕЙТИНГИ, СРАВНЕНИЯ.
- 1С_SEARCH: поиск конкретных документов (найти заказ, посмотреть цену товара, конкретная закупка). Используй когда нужен КОНКРЕТНЫЙ документ или запись.
- CHATS: переписка сотрудников в Telegram (обсуждения, договорённости, решения).
- EMAIL: деловая переписка по почте (с клиентами, поставщиками, подрядчиками).
- WEB: интернет-поиск. Только для внешней информации.

Типы аналитики (для 1С_ANALYTICS):
- top_clients: топ клиентов по продажам
- top_products: топ товаров по продажам
- sales_summary: сводка продаж
- top_suppliers: топ поставщиков по закупкам
- production_summary: сводка производства
- purchase_summary: сводка закупок

Вопрос: {question}

Верни ТОЛЬКО JSON без markdown:
{{"query_type": "analytics|search|lookup|chat_search|web|mixed", "steps": [{{"source": "1С_ANALYTICS|1С_SEARCH|CHATS|EMAIL|WEB", "action": "что искать", "analytics_type": "top_clients|top_products|sales_summary|top_suppliers|production_summary|purchase_summary|null", "keywords": "ключевые слова через пробел"}}], "entities": {{"clients": [], "products": [], "suppliers": []}}, "period": "today|yesterday|week|2weeks|month|quarter|half_year|year|january|february|march|april|may|june|july|august|september|october|november|december|null", "keywords": "основные ключевые слова"}}

Правила:
- Для аналитических вопросов (топ, основные, сколько всего, с цифрами) — 1С_ANALYTICS первым
- Для конкретных поисков (найди заказ, цена на X) — 1С_SEARCH
- Можно комбинировать шаги
- keywords — существительные БЕЗ запятых
- period из контекста: "за 2 недели" = "2weeks", "в январе" = "january"
"""
        
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": "openai/gpt-4.1-mini", "messages": [{"role": "user", "content": prompt}], "max_tokens": 500, "temperature": 0},
            timeout=15
        )
        
        result = response.json()
        if "choices" in result:
            content = result["choices"][0]["message"]["content"].strip()
            content = re.sub(r'^```(?:json)?\s*', '', content)
            content = re.sub(r'\s*```$', '', content)
            plan = json.loads(content)
            
            if "steps" not in plan or not plan["steps"]:
                plan["steps"] = [{"source": "1С_SEARCH", "action": "поиск", "keywords": plan.get("keywords", question)}]
            if "keywords" not in plan:
                plan["keywords"] = question
            
            logger.info(f"Router: type={plan.get('query_type')}, steps={len(plan['steps'])}, period={plan.get('period')}")
            return plan
        
        return _default_plan(question)
    
    except Exception as e:
        logger.error(f"Router error: {e}")
        return _default_plan(question)


def _default_plan(question):
    """План по умолчанию если Router недоступен."""
    return {
        "query_type": "mixed",
        "steps": [
            {"source": "1С_SEARCH", "action": "поиск", "keywords": question},
            {"source": "CHATS", "action": "поиск", "keywords": question},
            {"source": "EMAIL", "action": "поиск", "keywords": question}
        ],
        "entities": {"clients": [], "products": [], "suppliers": []},
        "period": None,
        "keywords": question
    }

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

async def process_rag_query(question, chat_context=""):
    """Основная функция обработки RAG-запроса с Router."""
    logger.info(f"RAG запрос: {question}")
    
    # Шаг 1: Router определяет план выполнения
    plan = route_query(question, chat_context)
    logger.info(f"Query plan: {plan.get('query_type')}, steps: {len(plan.get('steps', []))}")
    
    # Извлекаем параметры из плана
    period_date, period_end = _resolve_period(plan.get("period"))
    entities = plan.get("entities", {})
    keywords = plan.get("keywords", question)
    
    # Временной контекст для векторного поиска
    time_context = extract_time_context(question)
    if time_context["has_time_filter"]:
        logger.info(f"Временной контекст: decay_days={time_context['decay_days']}")
    
    db_results = []
    web_results = ""
    web_citations = []
    
    # Шаг 2: Выполняем шаги плана
    for step in plan.get("steps", []):
        source = step.get("source", "")
        step_keywords = step.get("keywords", keywords)
        analytics_type = step.get("analytics_type")
        
        if source == "1С_ANALYTICS" and analytics_type:
            results = search_1c_analytics(
                analytics_type=analytics_type,
                keywords=step_keywords,
                period_date=period_date,
                entities=entities,
                limit=20
            )
            db_results.extend(results)
            logger.info(f"Step [{source}/{analytics_type}]: {len(results)} результатов")
        
        elif source == "1С_SEARCH":
            results = search_1c_data(
                query=step_keywords,
                limit=30,
                period_date=period_date,
                entities=entities
            )
            db_results.extend(results)
            logger.info(f"Step [{source}]: {len(results)} результатов")
        
        elif source == "CHATS":
            results = search_telegram_chats(step_keywords, limit=30, time_context=time_context)
            db_results.extend(results)
            logger.info(f"Step [{source}]: {len(results)} результатов")
        
        elif source == "EMAIL":
            results = search_emails(step_keywords, limit=30, time_context=time_context)
            db_results.extend(results)
            logger.info(f"Step [{source}]: {len(results)} результатов")
        
        elif source == "WEB":
            web_results, web_citations = search_internet(step_keywords)
            logger.info(f"Step [{source}]: получен ответ")
    
    logger.info(f"Всего в БД: {len(db_results)}")
    
    # Шаг 3: Reranking
    if len(db_results) > 10:
        db_results = rerank_results(question, db_results, top_k=15)
    
    # Шаг 4: Генерация ответа
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
