"""
RAG Agent для поиска по базе знаний и интернету.
Включает SQL-поиск и векторный (семантический) поиск.
"""

import os
from dotenv import load_dotenv
load_dotenv('/home/admin/telegram_logger_bot/.env')

import json
import logging
import requests
import psycopg2
from psycopg2 import sql
import re

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
    from embedding_service import vector_search, index_telegram_message
    VECTOR_SEARCH_ENABLED = True
    logger.info("Векторный поиск включен")
except ImportError:
    VECTOR_SEARCH_ENABLED = False
    logger.warning("embedding_service не найден, векторный поиск отключен")


def get_db_connection():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


def clean_keywords(query: str) -> list:
    """Очищает ключевые слова от пунктуации."""
    # Убираем запятые, точки и другую пунктуацию
    clean_query = re.sub(r'[,.:;!?()"\']', ' ', query)
    keywords = [w.strip() for w in clean_query.split() if len(w.strip()) > 2]
    return keywords if keywords else [query]


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


def search_telegram_chats_vector(query: str, limit: int = 10) -> list:
    """Векторный (семантический) поиск по чатам."""
    if not VECTOR_SEARCH_ENABLED:
        return []
    
    try:
        vector_results = vector_search(query, limit=limit, source_type='telegram')
        results = []
        for r in vector_results:
            chat_name = r['source_table'].replace('tg_chat_', '').split('_', 1)[-1].replace('_', ' ').title()
            results.append({
                "source": f"Чат: {chat_name}",
                "content": r['content'][:1000],
                "type": "text",
                "similarity": r['similarity'],
                "search_type": "vector"
            })
        logger.info(f"Векторный поиск: {len(results)} результатов")
        return results
    except Exception as e:
        logger.error(f"Ошибка векторного поиска: {e}")
        return []


def search_telegram_chats(query: str, limit: int = 10) -> list:
    """Комбинированный поиск по чатам."""
    results = []
    seen_contents = set()
    
    vector_results = search_telegram_chats_vector(query, limit=limit)
    for r in vector_results:
        content_key = r['content'][:100]
        if content_key not in seen_contents:
            seen_contents.add(content_key)
            results.append(r)
    
    if len(results) < limit:
        sql_results = search_telegram_chats_sql(query, limit=limit - len(results))
        for r in sql_results:
            content_key = r['content'][:100]
            if content_key not in seen_contents:
                seen_contents.add(content_key)
                r['search_type'] = 'sql'
                results.append(r)
    
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
    
    # Собираем результаты: сначала цены, потом остальное
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
                similarity_info = f" [релевантность: {res['similarity']:.0%}]" if 'similarity' in res else ""
                context_parts.append(f"{i}.{similarity_info} {res['content'][:300]}")
        
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
1. ЗАКУПОЧНЫЕ ЦЕНЫ КОМПАНИИ — это РЕАЛЬНЫЕ цены по которым мы покупаем товар. Они в разделе "ЗАКУПОЧНЫЕ ЦЕНЫ КОМПАНИИ". ВСЕГДА указывай их в ответе!
2. Если спрашивают о цене "у нас" — это закупочные цены из 1С
3. Если спрашивают о рыночных ценах — используй данные из интернета
4. Указывай конкретные цифры: цену, дату, поставщика
5. Не придумывай данные

Ответ:"""

        response = requests.post(f"{ROUTERAI_BASE_URL}/chat/completions", headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"}, json={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": prompt}], "max_tokens": 2000}, timeout=60)
        result = response.json()
        if "choices" in result:
            response_text = result["choices"][0]["message"]["content"]
            
            # Добавляем ссылки в конец если есть
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
        return {"search_1c": True, "search_chats": True, "search_web": False, "keywords": question, "priority": "1c"}
    try:
        prompt = f"""Определи где искать информацию.
Источники: 1С (цены, закупки, товары), Чаты (обсуждения), Интернет (внешнее).
Извлеки 1-3 КЛЮЧЕВЫХ СЛОВА (существительные без запятых: сахар мука торт)

Вопрос: {question}

JSON: {{"search_1c": true/false, "search_chats": true/false, "search_web": true/false, "keywords": "слово1 слово2", "priority": "1c/chats/web"}}"""
        response = requests.post(f"{ROUTERAI_BASE_URL}/chat/completions", headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"}, json={"model": "google/gemini-3-flash-preview", "messages": [{"role": "user", "content": prompt}], "max_tokens": 200}, timeout=30)
        result = response.json()
        if "choices" in result:
            import re
            match = re.search(r'\{[^}]+\}', result["choices"][0]["message"]["content"])
            if match:
                return json.loads(match.group())
        return {"search_1c": True, "search_chats": True, "search_web": False, "keywords": question, "priority": "1c"}
    except:
        return {"search_1c": True, "search_chats": True, "search_web": False, "keywords": question, "priority": "1c"}


async def process_rag_query(question: str, chat_context: str = "") -> str:
    """Основная функция обработки RAG-запроса."""
    logger.info(f"RAG запрос: {question}")
    classification = classify_question(question)
    logger.info(f"Классификация: {classification}")
    keywords = classification.get("keywords", question)
    db_results = []
    
    # Поиск в 1С (SQL) — всегда первым
    if classification.get("search_1c", True):
        c1_results = search_1c_data(keywords, limit=15)
        db_results.extend(c1_results)
        logger.info(f"Найдено в 1С: {len(c1_results)}")
    
    # Поиск в чатах (векторный + SQL)
    if classification.get("search_chats", True):
        chat_results = search_telegram_chats(keywords, limit=10)
        db_results.extend(chat_results)
        logger.info(f"Найдено в чатах: {len(chat_results)}")
    
    logger.info(f"Всего в БД: {len(db_results)}")
    
    # Поиск в интернете
    web_results = ""
    web_citations = []
    if classification.get("search_web", False):
        web_results, web_citations = search_internet(question)
    
    return generate_response(question, db_results, web_results, web_citations, chat_context)


# Функция для индексации новых сообщений (вызывается из bot.py)
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
