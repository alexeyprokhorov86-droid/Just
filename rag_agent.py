"""
RAG Agent для поиска по базе знаний и интернету.
Включает SQL-поиск и векторный (семантический) поиск с учётом свежести.
"""

import os
import pathlib
from dotenv import load_dotenv

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

def search_telegram_chats_sql(query: str, limit: int = 10) -> list:
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


def search_telegram_chats_vector(query: str, limit: int = 10, time_context: dict = None) -> list:
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

def search_emails_vector(query: str, limit: int = 10, time_context: dict = None) -> list:
    """Семантический поиск по email с учётом свежести."""
    if not VECTOR_SEARCH_ENABLED:
        return []
    
    # Получаем параметры времени
    if time_context is None:
        time_context = extract_time_context(query)
    
    decay_days = time_context.get("decay_days", 90)
    freshness_weight = time_context.get("freshness_weight", 0.25)
    
    results = []
    try:
        email_results = vector_search_weighted(
            query, 
            limit=limit, 
            source_type='email',
            freshness_weight=freshness_weight,
            decay_days=decay_days
        )
        
        for r in email_results:
            received_str = ""
            if r.get("received_at"):
                received_str = r["received_at"].strftime("%d.%m.%Y")
            
            results.append({
                "source": "Email",
                "content": r["content"],
                "subject": r.get("subject", ""),
                "from_address": r.get("from_address", ""),
                "date": received_str,
                "similarity": r.get("similarity", 0),
                "freshness": r.get("freshness", 0),
                "final_score": r.get("final_score", r.get("similarity", 0)),
                "search_type": "email_vector"
            })
            
        logger.info(f"Email поиск (decay={decay_days}d): {len(results)} результатов")
        
    except Exception as e:
        logger.error(f"Ошибка поиска email: {e}")
    
    return results


def search_telegram_chats(query: str, limit: int = 10, time_context: dict = None) -> list:
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


def search_1c_data(query: str, limit: int = 10) -> list:
    """SQL-поиск по данным 1С (цены, номенклатура, контрагенты)."""
    prices = []
    nomenclature = []
    contractors = []
    
    conn = get_db_connection()
    keywords = clean_keywords(query)
    
    try:
        with conn.cursor() as cur:
            # 1. ЗАКУПОЧНЫЕ ЦЕНЫ (приоритет!)
            for keyword in keywords[:3]:
                try:
                    cur.execute("""
                        SELECT doc_date, doc_number, contractor_name, nomenclature_name, quantity, price, sum_total 
                        FROM purchase_prices 
                        WHERE nomenclature_name ILIKE %s OR contractor_name ILIKE %s 
                        ORDER BY doc_date DESC LIMIT %s
                    """, (f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {
                            "source": "1С: ЗАКУПОЧНЫЕ ЦЕНЫ", 
                            "date": row[0].strftime("%d.%m.%Y") if row[0] else "", 
                            "content": f"{row[3]} от {row[2]}: {row[5]} руб./ед., кол-во: {row[4]}, сумма: {row[6]} руб. (док. {row[1]})", 
                            "type": "price"
                        }
                        if result not in prices:
                            prices.append(result)
                except Exception as e:
                    logger.debug(f"Ошибка закупочных цен: {e}")
            
            # 2. Номенклатура (справочно)
            for keyword in keywords[:3]:
                try:
                    cur.execute("SELECT name, code, unit FROM nomenclature WHERE name ILIKE %s OR code ILIKE %s LIMIT %s", (f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {"source": "1С: Номенклатура", "content": f"{row[0]} (код: {row[1]}, ед.: {row[2]})", "type": "nomenclature"}
                        if result not in nomenclature:
                            nomenclature.append(result)
                except:
                    pass
            
            # 3. Контрагенты (справочно)
            for keyword in keywords[:3]:
                try:
                    cur.execute("SELECT name, inn, full_name FROM contractors WHERE name ILIKE %s OR inn ILIKE %s LIMIT %s", (f"%{keyword}%", f"%{keyword}%", limit))
                    for row in cur.fetchall():
                        result = {"source": "1С: Контрагенты", "content": f"{row[0]} (ИНН: {row[1]})", "type": "contractor"}
                        if result not in contractors:
                            contractors.append(result)
                except:
                    pass
    finally:
        conn.close()
    
    results = prices[:limit]
    remaining = limit - len(results)
    if remaining > 0:
        results.extend(nomenclature[:remaining])
        remaining = limit - len(results)
    if remaining > 0:
        results.extend(contractors[:remaining])
    
    logger.info(f"Поиск 1С по {keywords}: цены={len(prices)}, номенклатура={len(nomenclature)}, контрагенты={len(contractors)}")
    return results[:limit]


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
                context_parts.append(f"{i}.{score_info}{date_info} {res['content'][:400]}")
        
        # Интернет
        if web_results:
            context_parts.append("\n=== ИНТЕРНЕТ ===")
            context_parts.append(web_results[:2000])
        
        context = "\n".join(context_parts)
        
        prompt = f"""Ты — RAG-агент компании. Отвечай на русском.

ВОПРОС: {question}

НАЙДЕННЫЕ ДАННЫЕ:
{context if context else "Ничего не найдено."}

ВАЖНЫЕ ИНСТРУКЦИИ:
1. ЗАКУПОЧНЫЕ ЦЕНЫ КОМПАНИИ — это РЕАЛЬНЫЕ цены по которым мы покупаем товар. ВСЕГДА указывай их в ответе!
2. Если спрашивают о цене "у нас" — это закупочные цены из 1С
3. Если спрашивают о рыночных ценах — используй данные из интернета
4. Данные из ЧАТОВ и EMAIL — это внутренняя переписка компании
5. Указывай конкретные цифры: цену, дату, поставщика
6. Не придумывай данные

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
        c1_results = search_1c_data(keywords, limit=15)
        db_results.extend(c1_results)
        logger.info(f"Найдено в 1С: {len(c1_results)}")
    
    # Поиск в чатах (векторный с учётом свежести + SQL)
    if classification.get("search_chats", True):
        chat_results = search_telegram_chats(keywords, limit=10, time_context=time_context)
        db_results.extend(chat_results)
        logger.info(f"Найдено в чатах: {len(chat_results)}")
    
    # Поиск в email (векторный с учётом свежести)
    if classification.get("search_email", True):
        email_results = search_emails_vector(keywords, limit=10, time_context=time_context)
        db_results.extend(email_results)
        logger.info(f"Найдено в email: {len(email_results)}")
    
    logger.info(f"Всего в БД: {len(db_results)}")
    
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
