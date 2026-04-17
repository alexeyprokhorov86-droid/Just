"""
RAG Agent для поиска по базе знаний и интернету.
Включает SQL-поиск и векторный (семантический) поиск с учётом свежести.
ReAct архитектура: Smart Router → Поиск → Evaluator → (повтор) → Генерация.
"""

import os
import pathlib
from dotenv import load_dotenv
from company_context import get_company_profile

env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

import json
import logging
import time
import requests
import psycopg2
from psycopg2 import sql
import re
import math
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
    from embedding_service_e5 import vector_search, vector_search_weighted, index_telegram_message
    VECTOR_SEARCH_ENABLED = True
    logger.info("Векторный поиск включен")
except ImportError:
    VECTOR_SEARCH_ENABLED = False
    logger.warning("embedding_service не найден, векторный поиск отключен")


# === Кэш списка чатов из metadata ===
_chat_list_cache = {"data": None, "ts": 0}

# Ограничители качества retrieval/generation
TELEGRAM_VECTOR_MIN_SCORE = 0.72
EMAIL_VECTOR_MIN_SCORE = 0.55
TELEGRAM_SQL_MIN_SCORE = 0.55
EMAIL_SQL_MIN_SCORE = 0.42

EVIDENCE_MAX_ITEMS = 12
EVIDENCE_QUOTAS = {
    "analytics": 3,
    "1c": 4,
    "chat": 3,
    "email": 3,
    "other": 2,
}

INTENT_EVIDENCE_QUOTAS = {
    "staffing": {"chat": 6, "email": 2, "1c": 1, "analytics": 0, "other": 1},
    "documents": {"chat": 6, "email": 2, "1c": 1, "analytics": 0, "other": 1},
    "finance": {"chat": 3, "email": 4, "1c": 4, "analytics": 2, "other": 1},
    "production": {"chat": 4, "email": 2, "1c": 4, "analytics": 2, "other": 1},
    "procurement": {"chat": 4, "email": 2, "1c": 4, "analytics": 2, "other": 1},
}


def get_db_connection():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


def clean_keywords(query: str) -> list:
    """Очищает ключевые слова от пунктуации."""
    clean_query = re.sub(r'[,.:;!?()"\']', ' ', query)
    keywords = [w.strip() for w in clean_query.split() if len(w.strip()) > 2]
    return keywords if keywords else [query]


SEARCH_STOP_WORDS = {
    "и", "или", "а", "но", "в", "во", "на", "по", "из", "с", "со", "за", "для", "к", "ко", "у",
    "о", "об", "от", "до", "про", "над", "под", "при", "между", "что", "кто", "где", "когда",
    "как", "какой", "какая", "какие", "каких", "который", "которого", "которых", "это", "этот",
    "эта", "эти", "меня", "мне", "нас", "нам", "наши", "ваши", "нужно", "надо", "можно",
    "покажи", "скажи", "расскажи", "дай", "есть", "был", "была", "были", "будет", "вопрос",
}

KEYWORD_PRIORITY_PATTERNS = (
    (r"ндс|налог|фнс|счет|сч[её]т|оплат|плат[её]ж|договор|акт|инн|кпп", 1.3),
    (r"производ|технолог|рецепт|брак|выпуск|смен|себесто", 1.3),
    (r"закуп|постав|сыр[ья]|материал|цена|прайс", 1.2),
    (r"выруч|марж|продаж|клиент|контрагент|банк|касс", 1.1),
    (r"кадр|персонал|сотруд|оффер|должност|назнач|принят|нанят|уволен", 1.1),
    (r"документ|накладн|счет[-_ ]?фактур|приложен|вложен", 0.8),
)

INTENT_EXPANSIONS = [
    {
        "name": "staffing_events",
        "triggers": (
            "принят", "приняли", "наняли", "новый", "вышел", "вышла", "оффер", "должност",
            "взяли", "взят", "взята", "устро", "уволен", "уволили", "кро", "кандидат",
            "кто отвечает", "ответственный", "offer", "hiring", "hire", "fired", "candidate", "position",
        ),
        "terms": (
            "назначен", "принят на работу", "выход на работу", "фио сотрудника",
            "ответственный", "руководитель", "начальник отдела", "согласование оффера",
        ),
    },
    {
        "name": "finance_tax",
        "triggers": ("ндс", "налог", "фнс", "счет", "оплата", "платеж", "договор", "акт", "vat", "tax", "invoice", "payment"),
        "terms": ("налоговый учет", "оплата счета", "договорные условия", "бухгалтерия"),
    },
    {
        "name": "production",
        "triggers": ("производ", "технолог", "рецепт", "брак", "выпуск", "смена", "production", "technolog"),
        "terms": ("производственный процесс", "технологические требования", "контроль качества"),
    },
    {
        "name": "procurement_supply",
        "triggers": ("закуп", "постав", "сырье", "материал", "цена", "прайс", "procure", "supplier", "price"),
        "terms": ("условия поставки", "закупочные цены", "поставщик"),
    },
]

EN_TO_RU_LAYOUT = str.maketrans(
    "`qwertyuiop[]asdfghjkl;'zxcvbnm,./QWERTYUIOP{}ASDFGHJKL:\"ZXCVBNM<>?",
    "ёйцукенгшщзхъфывапролджэячсмитьбю.ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ,"
)

TERM_ALIASES = {
    "offer": "оффер",
    "offers": "офферы",
    "offer letter": "оффер",
    "hiring": "найм",
    "hire": "найм",
    "hired": "принят",
    "fired": "уволен",
    "dismissal": "увольнение",
    "candidate": "кандидат",
    "position": "должность",
    "chief technologist": "главный технолог",
    "technologist": "технолог",
    "resume": "резюме",
    "production": "производство",
    "procurement": "закупки",
    "supplier": "поставщик",
    "suppliers": "поставщики",
    "invoice": "счет",
    "invoices": "счета",
    "payment": "оплата",
    "payments": "платежи",
    "vat": "ндс",
    "tax": "налог",
    "taxes": "налоги",
    "kro": "кро",
    "hr": "кадры",
}

INTENT_CONTENT_MARKERS = {
    "staffing": [
        "оффер", "offer", "job offer", "кандидат", "должност", "прием", "приём", "принят",
        "найм", "hiring", "hire", "уволен", "увольн", "резюме", "hr", "кадр", "персонал",
        "подбор", "согласование оффера", "выход на работу",
    ],
    "finance": [
        "ндс", "налог", "tax", "vat", "счет", "счёт", "invoice", "оплат", "payment",
        "платеж", "платёж", "фнс", "договор", "акт", "реестр", "факторинг",
    ],
    "production": [
        "производ", "production", "технолог", "technolog", "брак", "выпуск",
        "смен", "рецепт", "качест", "haccp",
    ],
    "procurement": [
        "закуп", "procure", "постав", "supplier", "сырь", "материал",
        "цена", "прайс", "price", "заказ поставщику",
    ],
    "documents": [
        "документ", "document", "pdf", "excel", "word", "ppt", "приложен",
        "вложен", "файл", "накладн", "акт", "договор",
    ],
}


def swap_en_to_ru_layout(text: str) -> str:
    """Преобразует текст, набранный в EN-раскладке, в RU-раскладку."""
    return (text or "").translate(EN_TO_RU_LAYOUT)


def maybe_add_layout_variant(query: str) -> list:
    """
    Если запрос целиком набран латиницей, добавляет EN->RU вариант.
    Это ловит кейсы типа 'jaaths' вместо 'офферы'.
    """
    q = (query or "").strip()
    if not q:
        return []

    en_letters = len(re.findall(r"[A-Za-z]", q))
    ru_letters = len(re.findall(r"[А-Яа-яЁё]", q))
    if en_letters > 0 and ru_letters == 0:
        swapped = swap_en_to_ru_layout(q)
        if swapped and swapped.lower() != q.lower():
            return [swapped]
    return []


def extract_alias_terms(query: str) -> list:
    """Добавляет языковые alias-термы для смешанных RU/EN запросов."""
    q_low = (query or "").lower()
    extras = []
    for src, dst in TERM_ALIASES.items():
        if src in q_low:
            extras.append(dst)
    return extras


def has_recent_intent(question: str) -> bool:
    """Определяет, что пользователю важна свежесть данных."""
    q = (question or "").lower()
    return any(
        re.search(p, q) for p in (
            r"\bнедавно\b",
            r"\bпоследн(?:ий|яя|ее|ие|их)\b",
            r"\bсегодня\b",
            r"\bвчера\b",
            r"\bнов(?:ый|ая|ое|ые)\b",
            r"\bтекущ(?:ий|ая|ее|ие)\b",
            r"\bсвеж(?:ий|ая|ие)\b",
        )
    )


def tokenize_query(query: str) -> list:
    """Токенизация запроса для retrieval-пайплайна."""
    return re.findall(r"[A-Za-zА-Яа-яЁё0-9_]{2,}", (query or "").lower())


def keyword_variants(token: str) -> list:
    """
    Простая нормализация словоформ для SQL ILIKE.
    Нужна для случаев: "офферы" -> "оффер", "приняли" -> "приня".
    """
    t = (token or "").strip().lower()
    if not t:
        return []

    variants = {t}
    # Базовые русские окончания (без жёсткого стемминга)
    endings = ("ами", "ями", "ого", "ему", "ыми", "ими", "ая", "яя", "ое", "ее",
               "ые", "ие", "ов", "ев", "ей", "ам", "ям", "ах", "ях", "ой", "ий",
               "ый", "ую", "юю", "а", "я", "ы", "и", "е", "у", "ю", "о")
    for end in endings:
        if len(t) > len(end) + 3 and t.endswith(end):
            variants.add(t[:-len(end)])

    # Частый кейс с англицизмами/рус-транслитом
    if t.endswith("ы") or t.endswith("и"):
        variants.add(t[:-1])

    # Убираем слишком короткие обрезки
    cleaned = [v for v in variants if len(v) >= 3]
    # Длинные/более конкретные сначала
    cleaned.sort(key=len, reverse=True)
    return cleaned[:4]


def expand_query_for_retrieval(query: str) -> str:
    """Расширяет запрос синонимичными формулировками по интенту."""
    q = (query or "").strip()
    if not q:
        return q

    q_lower = q.lower()
    extras = []
    for rule in INTENT_EXPANSIONS:
        if any(trigger in q_lower for trigger in rule["triggers"]):
            extras.extend(rule["terms"])

    # Поддержка английских терминов/смешанной лексики
    extras.extend(extract_alias_terms(q))

    # Автокоррекция EN->RU раскладки
    extras.extend(maybe_add_layout_variant(q))

    # Сохраняем аббревиатуры (КРО, НДС, ФНС) как отдельные важные термины
    abbreviations = re.findall(r"\b[A-ZА-ЯЁ]{2,8}\b", q)
    extras.extend(abbreviations)

    uniq = []
    seen = set()
    for term in extras:
        t = term.strip()
        if not t:
            continue
        key = t.lower()
        if key in seen:
            continue
        if key in q_lower:
            continue
        seen.add(key)
        uniq.append(t)

    return f"{q} {' '.join(uniq)}".strip() if uniq else q


def select_search_keywords(query: str, max_keywords: int = 8) -> list:
    """Умный выбор keywords: убираем шум и поднимаем доменные сущности."""
    tokens = tokenize_query(query)
    if not tokens:
        return clean_keywords(query)[:max_keywords]

    scored = []
    seen = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)

        if token in SEARCH_STOP_WORDS:
            continue
        if len(token) < 2:
            continue

        score = 0.25 + min(len(token) * 0.06, 0.9)
        if any(ch.isdigit() for ch in token):
            score += 0.4
        for pattern, bonus in KEYWORD_PRIORITY_PATTERNS:
            if re.search(pattern, token):
                score += bonus

        scored.append((score, token))

    if not scored:
        return clean_keywords(query)[:max_keywords]

    scored.sort(key=lambda x: x[0], reverse=True)
    return [token for _, token in scored[:max_keywords]]


def detect_query_intents(question: str) -> set:
    """Лёгкая intent-классификация для страховки роутинга."""
    q = (question or "").lower()
    q_exp = expand_query_for_retrieval(question).lower()
    merged_q = f"{q} {q_exp}"
    intents = set()

    if re.search(r"принят|приняли|нанял|наняли|взяли|уволен|оффер|offer|hire|hiring|dismiss|должност|position|кто отвечает|ответствен", merged_q):
        intents.add("staffing")
    if re.search(r"ндс|налог|tax|vat|invoice|счет|сч[её]т|оплат|payment|плат[её]ж|фнс|договор|акт", merged_q):
        intents.add("finance")
    if re.search(r"производ|production|технолог|technolog|брак|выпуск|смен", merged_q):
        intents.add("production")
    if re.search(r"закуп|procure|supplier|постав|сырь|материал|цена|прайс", merged_q):
        intents.add("procurement")
    if re.search(r"документ|document|pdf|excel|word|вложен|приложен|накладн", merged_q):
        intents.add("documents")
    if re.search(r"кто|кого|какие|какой|когда|были ли|what|which|who|when", merged_q):
        intents.add("lookup")

    return intents


def get_primary_intent(question: str) -> str:
    """Возвращает приоритетный intent для настройки retrieval/quotas."""
    intents = detect_query_intents(question)
    for name in ("staffing", "finance", "production", "procurement", "documents", "lookup"):
        if name in intents:
            return name
    return "lookup"


def suggest_target_chats_by_intent(question: str, max_items: int = 8) -> list:
    """
    Локальная подстраховка выбора чатов по названию.
    Универсально: опирается на intent и ключевые маркеры.
    """
    chats = get_chat_list()
    if not chats:
        return []

    q = (question or "").lower()
    intents = detect_query_intents(question)

    markers = []
    if "staffing" in intents:
        markers += ["hr", "кадр", "подбор", "персонал", "оффер", "кро", "рекрут"]
    if "finance" in intents:
        markers += ["бухгалтер", "налог", "ндс", "финанс", "априори", "аутсорсинг"]
    if "production" in intents:
        markers += ["производ", "технолог", "качест", "цех", "выпуск"]
    if "procurement" in intents:
        markers += ["закуп", "постав", "сырье", "снабжен"]
    if "documents" in intents:
        markers += ["документ", "бз", "отгруз", "скан", "торты отгрузки"]

    # если intent слабый — добавим токены запроса
    if not markers:
        markers += select_search_keywords(expand_query_for_retrieval(q), max_keywords=4)

    scored = []
    for chat in chats:
        title = (chat.get("title") or "").lower()
        table = chat.get("table")
        if not table:
            continue

        score = 0
        for m in markers:
            if m and m in title:
                score += 2
        for t in tokenize_query(q):
            if t in title:
                score += 1

        if score > 0:
            scored.append((score, table))

    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    seen = set()
    for _, table in scored:
        if table in seen:
            continue
        seen.add(table)
        out.append(table)
        if len(out) >= max_items:
            break
    return out


def ensure_plan_sources(plan: dict, question: str) -> dict:
    """
    Подстраховка: если router выбрал нерелевантные источники,
    добавляем необходимые шаги по типу вопроса.
    """
    if not isinstance(plan, dict):
        return plan

    intents = detect_query_intents(question)
    steps = list(plan.get("steps") or [])
    existing = {s.get("source") for s in steps if isinstance(s, dict)}
    keywords = plan.get("keywords") or question

    def _add_step(src: str):
        if src in existing:
            return
        steps.append({"source": src, "action": "поиск", "keywords": keywords})
        existing.add(src)

    if "staffing" in intents:
        _add_step("CHATS")
        _add_step("EMAIL")
    if "finance" in intents:
        _add_step("1С_SEARCH")
        _add_step("CHATS")
    if "production" in intents or "procurement" in intents:
        _add_step("1С_SEARCH")
        _add_step("CHATS")
    if "documents" in intents:
        _add_step("CHATS")
    if "lookup" in intents and "CHATS" not in existing:
        _add_step("CHATS")

    plan["steps"] = steps
    return plan


def parse_result_datetime(result: dict):
    """Извлекает datetime из результата retrieval."""
    dt_raw = result.get("timestamp") or result.get("received_at")
    if isinstance(dt_raw, datetime):
        return dt_raw

    date_str = (result.get("date") or "").strip()
    if not date_str:
        return None

    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def freshness_by_time(dt_value, decay_days: int) -> float:
    """Экспоненциальная свежесть 0..1."""
    if not dt_value or not isinstance(dt_value, datetime):
        return 0.5
    age_seconds = max((datetime.now() - dt_value).total_seconds(), 0)
    return float(math.exp(-age_seconds / max(decay_days * 86400, 1)))


def get_chat_list() -> list:
    """Возвращает список чатов из metadata с кэшем 5 минут."""
    now = time.time()
    if _chat_list_cache["data"] and (now - _chat_list_cache["ts"]) < 300:
        return _chat_list_cache["data"]

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT chat_id, chat_title, table_name, last_message_at, description
                FROM tg_chats_metadata
                WHERE table_name IS NOT NULL
                ORDER BY last_message_at DESC NULLS LAST
            """)
            chats = []
            for row in cur.fetchall():
                chats.append({
                    "chat_id": row[0],
                    "title": row[1] or "",
                    "table": row[2],
                    "last_msg": row[3].strftime("%d.%m.%Y") if row[3] else "нет сообщений",
                    "description": row[4] or ""
                })
        conn.close()
        _chat_list_cache["data"] = chats
        _chat_list_cache["ts"] = now
        logger.info(f"Загружен список чатов: {len(chats)} шт")
        return chats
    except Exception as e:
        logger.error(f"Ошибка загрузки списка чатов: {e}")
        return []


def format_chat_list_for_llm() -> str:
    """Форматирует список чатов для передачи в LLM Router."""
    chats = get_chat_list()
    lines = []
    for c in chats:
        if c["last_msg"] != "нет сообщений":
            desc = c.get("description", "")
            # Берём только первую строку описания (основное описание без keywords/roles)
            short_desc = desc.split("\n")[0].strip() if desc else ""
            if short_desc:
                lines.append(f"- {c['title']} [{c['table']}] (посл.: {c['last_msg']}) — {short_desc}")
            else:
                lines.append(f"- {c['title']} [{c['table']}] (посл.: {c['last_msg']})")
    return "\n".join(lines)


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
        (r'за последн(?:ий|ие|юю|ее)?\s*(\d+)?\s*месяц', lambda m: int(m.group(1) or 1) * 30),
        (r'за (\d+)\s*месяц', lambda m: int(m.group(1)) * 30),
        (r'за последн(?:ий|ие|юю|ее)?\s*(\d+)?\s*недел', lambda m: int(m.group(1) or 1) * 7),
        (r'за (\d+)\s*недел', lambda m: int(m.group(1)) * 7),
        (r'за последн(?:ий|ие|юю|ее)?\s*(\d+)?\s*(?:день|дня|дней)', lambda m: int(m.group(1) or 1)),
        (r'за (\d+)\s*(?:день|дня|дней)', lambda m: int(m.group(1))),
        (r'за последн(?:ий|ие|юю|ее)?\s*год', lambda m: 365),
        (r'за год', lambda m: 365),
        (r'за последн(?:ий|ие|юю|ее)?\s*квартал', lambda m: 90),
        (r'за квартал', lambda m: 90),
        (r'\bвчера\b', lambda m: 2),
        (r'\bсегодня\b', lambda m: 1),
        (r'на этой неделе', lambda m: 7),
        (r'на прошлой неделе', lambda m: 14),
        (r'в этом месяце', lambda m: now.day),
        (r'в прошлом месяце', lambda m: 60),
        (r'\bнедавно\b', lambda m: 14),
        (r'в последнее время', lambda m: 30),
    ]
    
    for pattern, days_func in patterns:
        match = re.search(pattern, question_lower)
        if match:
            result["has_time_filter"] = True
            result["decay_days"] = days_func(match)
            result["date_from"] = now - timedelta(days=result["decay_days"])
            result["date_to"] = now
            result["freshness_weight"] = 0.4
            break
    
    # Паттерны для конкретных месяцев
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
                if month_num > now.month and year == now.year:
                    year -= 1
                result["date_from"] = datetime(year, month_num, 1)
                if month_num == 12:
                    result["date_to"] = datetime(year + 1, 1, 1) - timedelta(days=1)
                else:
                    result["date_to"] = datetime(year, month_num + 1, 1) - timedelta(days=1)
                result["has_time_filter"] = True
                result["decay_days"] = (now - result["date_from"]).days or 30
                result["freshness_weight"] = 0.5
                break
    
    return result

def diversify_by_source_id(
    items: list,
    total_limit: int,
    max_per_source: int = 2,
    score_key: str = "final_score",
    source_id_key: str = "source_id",
) -> list:
    """Ограничивает число результатов от одного источника (source_id)."""
    if not items:
        return []

    items = sorted(items, key=lambda x: x.get(score_key, 0), reverse=True)

    per_source_count = {}
    out = []

    for it in items:
        sid = it.get(source_id_key)
        if sid is None:
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


# =============================================================================
# ПОИСК ПО TELEGRAM-ЧАТАМ
# =============================================================================

def _group_messages(messages: list, window_minutes: int = 3) -> list:
    """
    Группирует сообщения одного автора в окне ±N минут в один блок.
    Входной формат: list of dict с ключами timestamp, first_name, content, ...
    """
    if not messages:
        return messages

    # Сортируем по времени
    sorted_msgs = sorted(messages, key=lambda m: m.get("_ts") or datetime.min)

    groups = []
    current_group = None

    for msg in sorted_msgs:
        ts = msg.get("_ts")
        author = msg.get("author", "")

        if (current_group
                and current_group["author"] == author
                and ts and current_group["_last_ts"]
                and (ts - current_group["_last_ts"]).total_seconds() <= window_minutes * 60):
            # Добавляем к текущей группе
            current_group["content"] += "\n" + msg.get("content", "")
            current_group["_last_ts"] = ts
        else:
            # Новая группа
            if current_group:
                groups.append(current_group)
            current_group = {
                **msg,
                "content": msg.get("content", ""),
                "_last_ts": ts,
            }

    if current_group:
        groups.append(current_group)

    # Убираем служебное поле и обрезаем контент
    for g in groups:
        g.pop("_last_ts", None)
        g.pop("_ts", None)
        g["content"] = g["content"][:1500]

    return groups

def search_knowledge(query: str, limit: int = 30) -> list:
    """Поиск по базе знаний: km_facts, km_decisions, km_tasks, km_policies."""
    from embedding_service_e5 import create_query_embedding
    query_embedding = create_query_embedding(query)
    emb_str = "[" + ",".join(str(x) for x in query_embedding) + "]"
    
    conn = get_db_connection()
    results = []
    
    tables = [
        ("km_facts", "fact_text", "fact_type"),
        ("km_decisions", "decision_text", "decision_type"),
        ("km_tasks", "task_text", "task_type"),
        ("km_policies", "policy_text", "policy_type"),
    ]
    
    per_table = max(limit // len(tables), 5)
    
    try:
        with conn.cursor() as cur:
            for table, text_col, type_col in tables:
                try:
                    cur.execute(f"""
                        SELECT id, {text_col}, {type_col}, confidence, created_at,
                               1 - (embedding <=> %s::vector) as similarity
                        FROM {table}
                        WHERE verification_status NOT IN ('rejected', 'duplicate')
                          AND embedding IS NOT NULL
                        ORDER BY embedding <=> %s::vector
                        LIMIT %s
                    """, (emb_str, emb_str, per_table))
                    
                    for row in cur.fetchall():
                        sim = float(row[5]) if row[5] else 0
                        if sim < 0.3:
                            continue
                        results.append({
                            "source": f"knowledge:{table}",
                            "source_type": "knowledge",
                            "content": row[1],
                            "type": row[2] or "",
                            "confidence": float(row[3]) if row[3] else 0.8,
                            "similarity": sim,
                            "created_at": str(row[4]) if row[4] else "",
                            "search_type": "vector",
                        })
                except Exception as e:
                    logger.warning(f"search_knowledge {table}: {e}")
    finally:
        conn.close()
    
    results.sort(key=lambda x: x["similarity"], reverse=True)
    logger.info(f"search_knowledge: {len(results)} результатов по запросу '{query[:50]}'")
    return results[:limit]


def search_source_chunks(query: str, limit: int = 30, min_similarity: float = 0.3) -> list:
    """
    Поиск по source_chunks через Qwen3 embedding_v2 (HNSW индекс).

    В отличие от search_knowledge (km_*, дистиллированные факты на e5), эта
    функция возвращает сырые документные чанки: telegram-сообщения, email,
    matrix-события, 1С-документы. Формат результатов совместим с
    search_knowledge для объединения в общий RAG pipeline.

    Активируется через .env `USE_EMBEDDING_V2=true` (в вызывающем коде).
    """
    from chunkers.embedder import embed_query_v2

    query_vec = embed_query_v2(query)
    if query_vec is None:
        logger.warning("search_source_chunks: embed_query_v2 вернул None")
        return []

    emb_str = "[" + ",".join(str(x) for x in query_vec) + "]"

    conn = get_db_connection()
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    sc.id, sc.chunk_text, sc.chunk_type, sc.source_kind,
                    sc.confidence, sc.created_at, sc.chunk_date,
                    sd.title, sd.doc_date, sd.author_name, sd.channel_name,
                    sd.source_ref,
                    1 - (sc.embedding_v2 <=> %s::vector) AS similarity
                FROM source_chunks sc
                LEFT JOIN source_documents sd ON sd.id = sc.document_id
                WHERE sc.embedding_v2 IS NOT NULL
                  AND (sd.is_deleted IS NULL OR sd.is_deleted = false)
                ORDER BY sc.embedding_v2 <=> %s::vector
                LIMIT %s
            """, (emb_str, emb_str, limit))

            for row in cur.fetchall():
                sim = float(row[12]) if row[12] is not None else 0.0
                if sim < min_similarity:
                    continue
                source_kind = row[3] or "unknown"
                results.append({
                    "source": f"source_chunks:{source_kind}",
                    "source_type": "source_chunks",
                    "content": row[1],
                    "type": row[2] or source_kind,
                    "confidence": float(row[4]) if row[4] is not None else 0.5,
                    "similarity": sim,
                    "created_at": str(row[5]) if row[5] else "",
                    "chunk_date": str(row[6]) if row[6] else "",
                    "title": row[7] or "",
                    "doc_date": str(row[8]) if row[8] else "",
                    "author": row[9] or "",
                    "channel": row[10] or "",
                    "source_ref": row[11] or "",
                    "search_type": "vector_v2",
                })
    finally:
        conn.close()

    logger.info(
        f"search_source_chunks: {len(results)} результатов по запросу "
        f"'{query[:50]}'"
    )
    return results


def search_source_chunks_reranked(
    query: str,
    retrieve_limit: int = 30,
    top_k: int = 5,
    min_similarity: float = 0.3,
) -> list:
    """
    Retrieval + rerank: embedding_v2 cosine top-N → Qwen3-Reranker top-K.

    retrieve_limit: сколько взять из HNSW (рекомендовано 20-30)
    top_k:          сколько оставить после reranker (рекомендовано 3-10)

    Добавляет в каждый result поле `rerank_score` (P(yes) ∈ [0,1]).
    Активируется через .env USE_RERANKER=true (проверяет вызывающий код).
    """
    from chunkers.reranker import rerank

    retrieved = search_source_chunks(query, limit=retrieve_limit, min_similarity=min_similarity)
    if not retrieved:
        return []
    reranked = rerank(query, retrieved, text_key="content", top_k=top_k)
    return reranked


def search_telegram_chats_sql(query: str, limit: int = 30, target_tables: list = None,
                              time_context: dict = None) -> list:
    """SQL-поиск по Telegram чатам с time-aware scoring и keyword expansion."""
    if time_context is None:
        time_context = extract_time_context(query)

    decay_days = time_context.get("decay_days", 90)
    freshness_weight = time_context.get("freshness_weight", 0.25)
    date_from = time_context.get("date_from")
    date_to = time_context.get("date_to")

    retrieval_query = expand_query_for_retrieval(query)
    keywords = select_search_keywords(retrieval_query, max_keywords=8)
    keyword_terms = []
    seen_kw = set()
    for kw in keywords:
        for variant in keyword_variants(kw):
            if variant in seen_kw:
                continue
            seen_kw.add(variant)
            keyword_terms.append(variant)
    if not keyword_terms:
        keyword_terms = keywords

    results = []
    conn = get_db_connection()
    found_anchors = {}
    try:
        with conn.cursor() as cur:
            if target_tables:
                chat_tables = target_tables
            else:
                cur.execute("""SELECT table_name FROM information_schema.tables
                              WHERE table_schema = 'public' AND table_name LIKE 'tg_chat_%'
                              AND table_name != 'tg_chats_metadata' AND table_name != 'tg_user_roles'""")
                chat_tables = [row[0] for row in cur.fetchall()]

            for table_name in chat_tables:
                for keyword in keyword_terms:
                    try:
                        query_sql = (
                            "SELECT id, timestamp, first_name, message_text, media_analysis, "
                            "message_type, content_text FROM {} "
                            "WHERE (message_text ILIKE %s OR media_analysis ILIKE %s OR content_text ILIKE %s) "
                        )
                        params = [f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"]
                        if date_from:
                            query_sql += "AND timestamp >= %s "
                            params.append(date_from)
                        if date_to:
                            query_sql += "AND timestamp <= %s "
                            params.append(date_to)
                        query_sql += "ORDER BY timestamp DESC LIMIT %s"
                        params.append(limit)

                        cur.execute(sql.SQL(query_sql).format(sql.Identifier(table_name)), params)
                        for row in cur.fetchall():
                            chat_name = table_name.replace('tg_chat_', '').split('_', 1)[-1].replace('_', ' ').title()
                            row_id = row[0]
                            ts = row[1]
                            content = row[3] or ""
                            if row[6]:
                                content += f"\n[Документ]: {row[6][:500]}"
                            if row[4]:
                                content += f"\n[Анализ]: {row[4][:500]}"

                            lexical_score = 0.75
                            freshness = freshness_by_time(ts, decay_days)
                            final_score = lexical_score * (1 - freshness_weight) + freshness * freshness_weight

                            result = {
                                "source": f"Чат: {chat_name}",
                                "date": ts.strftime("%d.%m.%Y %H:%M") if ts else "",
                                "author": row[2] or "",
                                "content": content[:1500],
                                "type": row[5] or "text",
                                "_ts": ts,
                                "similarity": lexical_score,
                                "freshness": freshness,
                                "final_score": final_score,
                                "search_type": "chat_sql",
                                "source_id": f"{table_name}:{row_id}",
                                "source_table": table_name,
                            }
                            if result not in results:
                                results.append(result)
                                if ts and row[2]:
                                    if table_name not in found_anchors:
                                        found_anchors[table_name] = []
                                    found_anchors[table_name].append((ts, row[2]))
                    except Exception:
                        continue

            # Контекстное окно: соседние сообщения ±5 минут того же автора
            seen_content = {hash(r['content'][:200]) for r in results}
            for table_name, anchors in found_anchors.items():
                unique_anchors = {}
                for ts, author in anchors:
                    key = (author, ts.strftime("%Y-%m-%d %H:%M"))
                    if key not in unique_anchors:
                        unique_anchors[key] = (ts, author)

                for ts, author in list(unique_anchors.values())[:10]:
                    try:
                        cur.execute(sql.SQL(
                            "SELECT id, timestamp, first_name, message_text, media_analysis, "
                            "message_type, content_text FROM {} "
                            "WHERE first_name = %s "
                            "AND timestamp BETWEEN %s AND %s "
                            "ORDER BY timestamp"
                        ).format(sql.Identifier(table_name)),
                            (author, ts - timedelta(minutes=5), ts + timedelta(minutes=5)))

                        chat_name = table_name.replace('tg_chat_', '').split('_', 1)[-1].replace('_', ' ').title()
                        for row in cur.fetchall():
                            row_id = row[0]
                            row_ts = row[1]
                            content = row[3] or ""
                            if row[6]:
                                content += f"\n[Документ]: {row[6][:500]}"
                            if row[4]:
                                content += f"\n[Анализ]: {row[4][:500]}"
                            content_hash = hash(content[:200])
                            if content_hash in seen_content:
                                continue

                            seen_content.add(content_hash)
                            lexical_score = 0.68
                            freshness = freshness_by_time(row_ts, decay_days)
                            final_score = lexical_score * (1 - freshness_weight) + freshness * freshness_weight

                            results.append({
                                "source": f"Чат: {chat_name}",
                                "date": row_ts.strftime("%d.%m.%Y %H:%M") if row_ts else "",
                                "author": row[2] or "",
                                "content": content[:1500],
                                "type": row[5] or "text",
                                "_ts": row_ts,
                                "similarity": lexical_score,
                                "freshness": freshness,
                                "final_score": final_score,
                                "search_type": "chat_sql",
                                "source_id": f"{table_name}:{row_id}",
                                "source_table": table_name,
                            })
                    except Exception:
                        continue

    finally:
        conn.close()

    results = _group_messages(results, window_minutes=3)
    return results[:limit]

def search_telegram_chats_vector(query: str, limit: int = 30, time_context: dict = None,
                                 target_tables: list = None) -> list:
    """Векторный (семантический) поиск по чатам с учётом свежести."""
    if not VECTOR_SEARCH_ENABLED:
        return []
    
    if time_context is None:
        time_context = extract_time_context(query)
    
    decay_days = time_context.get("decay_days", 90)
    freshness_weight = time_context.get("freshness_weight", 0.25)
    
    retrieval_query = expand_query_for_retrieval(query)

    try:
        vector_results = vector_search_weighted(
            retrieval_query,
            limit=limit, 
            source_type='telegram',
            freshness_weight=freshness_weight,
            decay_days=decay_days,
            source_tables=target_tables if target_tables else None
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
                "search_type": "vector",
                "source_id": f"{r.get('source_table')}:{r.get('source_id')}",
                "source_table": r.get('source_table'),
            }
            
            if r.get('timestamp'):
                result["date"] = r['timestamp'].strftime("%d.%m.%Y %H:%M")
            
            results.append(result)
        
        logger.info(
            f"Векторный поиск (decay={decay_days}d, fw={freshness_weight}, "
            f"targets={len(target_tables) if target_tables else 'all'}): {len(results)} результатов"
        )
        return results
        
    except Exception as e:
        logger.error(f"Ошибка векторного поиска: {e}")
        return []


def search_emails_sql(query: str, limit: int = 30, time_context: dict = None) -> list:
    """SQL/keyword поиск по email с query expansion и time-aware scoring."""
    if time_context is None:
        time_context = extract_time_context(query)

    decay_days = time_context.get("decay_days", 90)
    freshness_weight = time_context.get("freshness_weight", 0.25)
    date_from = time_context.get("date_from")
    date_to = time_context.get("date_to")

    retrieval_query = expand_query_for_retrieval(query)
    keywords = select_search_keywords(retrieval_query, max_keywords=8)
    keyword_terms = []
    seen_kw = set()
    for kw in keywords:
        for variant in keyword_variants(kw):
            if variant in seen_kw:
                continue
            seen_kw.add(variant)
            keyword_terms.append(variant)
    if not keyword_terms:
        keyword_terms = keywords

    results = []
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            fts_query = ' | '.join(keyword_terms[:8]) if keyword_terms else retrieval_query
            fts_sql = """
                SELECT id, subject, body_text, from_address, received_at
                FROM email_messages
                WHERE to_tsvector('russian', COALESCE(subject, '') || ' ' || COALESCE(body_text, ''))
                      @@ to_tsquery('russian', %s)
            """
            fts_params = [fts_query]
            if date_from:
                fts_sql += " AND received_at >= %s"
                fts_params.append(date_from)
            if date_to:
                fts_sql += " AND received_at <= %s"
                fts_params.append(date_to)
            fts_sql += " ORDER BY received_at DESC LIMIT %s"
            fts_params.append(limit * 3)
            cur.execute(fts_sql, fts_params)

            fts_results = cur.fetchall()

            # Fallback по ILIKE если FTS ничего не дал
            if not fts_results:
                for keyword in keyword_terms[:8]:
                    kw_sql = """
                        SELECT id, subject, body_text, from_address, received_at
                        FROM email_messages
                        WHERE (subject ILIKE %s OR body_text ILIKE %s)
                    """
                    kw_params = [f"%{keyword}%", f"%{keyword}%"]
                    if date_from:
                        kw_sql += " AND received_at >= %s"
                        kw_params.append(date_from)
                    if date_to:
                        kw_sql += " AND received_at <= %s"
                        kw_params.append(date_to)
                    kw_sql += " ORDER BY received_at DESC LIMIT %s"
                    kw_params.append(limit)
                    cur.execute(kw_sql, kw_params)
                    fts_results.extend(cur.fetchall())

            seen_ids = set()
            for row in fts_results:
                if row[0] in seen_ids:
                    continue
                seen_ids.add(row[0])

                content = f"Тема: {row[1] or ''}\n{(row[2] or '')[:800]}"
                received_str = row[4].strftime("%d.%m.%Y") if row[4] else ""

                lexical_score = 0.62
                freshness = freshness_by_time(row[4], decay_days)
                final_score = lexical_score * (1 - freshness_weight) + freshness * freshness_weight

                results.append({
                    "source": "Email",
                    "content": content,
                    "subject": row[1] or "",
                    "from_address": row[3] or "",
                    "date": received_str,
                    "similarity": lexical_score,
                    "freshness": freshness,
                    "final_score": final_score,
                    "search_type": "email_sql",
                    "source_id": row[0],
                    "received_at": row[4],
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
    """Семантический поиск по email с учётом свежести + diversity."""
    if not VECTOR_SEARCH_ENABLED:
        return []

    if time_context is None:
        time_context = extract_time_context(query)

    decay_days = time_context.get("decay_days", 90)
    freshness_weight = time_context.get("freshness_weight", 0.25)

    pre_limit = max(limit * 3, 30)
    max_chunks_per_email = 2

    results = []
    try:
        retrieval_query = expand_query_for_retrieval(query)
        email_candidates = vector_search_weighted(
            retrieval_query,
            limit=pre_limit,
            source_type='email',
            freshness_weight=freshness_weight,
            decay_days=decay_days
        )

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
    """Комбинированный поиск по email: вектор + SQL."""
    results = []
    seen_ids = set()
    
    vector_results = search_emails_vector(query, limit=limit, time_context=time_context)
    for r in vector_results:
        source_id = r.get('source_id')
        if source_id and source_id in seen_ids:
            continue
        if source_id:
            seen_ids.add(source_id)
        results.append(r)
    
    sql_results = search_emails_sql(query, limit=limit, time_context=time_context)
    for r in sql_results:
        source_id = r.get('source_id')
        if source_id and source_id in seen_ids:
            continue
        if source_id:
            seen_ids.add(source_id)
        results.append(r)
    
    results.sort(key=lambda x: x.get('final_score', 0), reverse=True)
    
    logger.info(f"Поиск email: {len(results)} результатов (vector + sql)")
    return results[:limit]


def search_telegram_chats(query: str, limit: int = 30, time_context: dict = None,
                          target_tables: list = None) -> list:
    """
    Комбинированный поиск по чатам: вектор + SQL.
    target_tables: если задан — SQL ищет ТОЛЬКО в этих таблицах.
    """
    results = []
    seen_content = set()
    
    # Векторный поиск (пока по всем чатам, фильтрация по source_table)
    vector_results = search_telegram_chats_vector(
        query, limit=limit, time_context=time_context, target_tables=target_tables
    )
    for r in vector_results:
        content_hash = hash(r['content'][:200])
        if content_hash not in seen_content:
            seen_content.add(content_hash)
            results.append(r)
    
    # SQL поиск — в target_tables если заданы
    sql_results = search_telegram_chats_sql(
        query, limit=limit, target_tables=target_tables, time_context=time_context
    )
    for r in sql_results:
        content_hash = hash(r['content'][:200])
        if content_hash not in seen_content:
            seen_content.add(content_hash)
            results.append(r)
    
    results.sort(key=lambda x: x.get('final_score', x.get('similarity', 0)), reverse=True)

    # Fallback: если target_chats были заданы, но recall низкий — доищем по всем чатам
    min_target_hits = max(4, min(8, limit // 3))
    if target_tables and len(results) < min_target_hits:
        logger.info(
            f"CHATS fallback: low recall in target chats ({len(results)} < {min_target_hits}), "
            "searching across all chats"
        )
        extra_results = []
        extra_vector = search_telegram_chats_vector(
            query, limit=max(limit // 2, 12), time_context=time_context, target_tables=None
        )
        extra_sql = search_telegram_chats_sql(
            query, limit=max(limit // 2, 12), target_tables=None, time_context=time_context
        )
        extra_results.extend(extra_vector)
        extra_results.extend(extra_sql)

        seen_content = {hash((r.get("content") or "")[:200]) for r in results}
        for r in extra_results:
            h = hash((r.get("content") or "")[:200])
            if h in seen_content:
                continue
            seen_content.add(h)
            results.append(r)

        results.sort(key=lambda x: x.get('final_score', x.get('similarity', 0)), reverse=True)

    logger.info(
        f"Поиск в чатах: {len(results)} результатов "
        f"(vector + sql, target={len(target_tables) if target_tables else 'all'})"
    )
    return results[:limit]


def _resolve_period(period_str):
    """Преобразует строку периода из Router в (date_from, date_to)."""
    if not period_str or period_str == "null":
        return None, None
    
    today = date.today()
    
    if period_str == "week":
        monday = today - timedelta(days=today.weekday())
        sunday = monday + timedelta(days=6)
        return monday, sunday
    
    if period_str == "last_week":
        monday = today - timedelta(days=today.weekday() + 7)
        sunday = monday + timedelta(days=6)
        return monday, sunday
    
    if period_str == "month":
        first = date(today.year, today.month, 1)
        if today.month == 12:
            last = date(today.year + 1, 1, 1) - timedelta(days=1)
        else:
            last = date(today.year, today.month + 1, 1) - timedelta(days=1)
        return first, last
    
    if period_str == "last_month":
        first_this = date(today.year, today.month, 1)
        last_prev = first_this - timedelta(days=1)
        first_prev = date(last_prev.year, last_prev.month, 1)
        return first_prev, last_prev
    
    if period_str == "quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_month, 1)
        q_end_month = q_month + 2
        q_end = date(today.year, q_end_month + 1, 1) - timedelta(days=1) if q_end_month < 12 else date(today.year, 12, 31)
        return q_start, q_end
    
    if period_str == "last_quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_month, 1)
        last_q_end = q_start - timedelta(days=1)
        last_q_month = ((last_q_end.month - 1) // 3) * 3 + 1
        return date(last_q_end.year, last_q_month, 1), last_q_end
    
    simple_map = {
        "today": today,
        "yesterday": today - timedelta(days=1),
        "2weeks": today - timedelta(weeks=2),
        "half_year": today - timedelta(days=180),
        "year": today - timedelta(days=365),
    }
    
    if period_str in simple_map:
        return simple_map[period_str], None
    
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
            last_day = date(year, 12, 31)
        else:
            last_day = date(year, month_num + 1, 1) - timedelta(days=1)
        return first_day, last_day
    
    return None, None


# =============================================================================
# АНАЛИТИКА 1С
# =============================================================================

def search_1c_analytics(analytics_type, keywords="", period_date=None,
                         period_end=None, entities=None, limit=20):
    """Агрегированные запросы по данным 1С."""
    results = []
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            
            if analytics_type in ("top_clients", "sales_summary"):
                try:
                    q = """
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
                        q += " AND doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND doc_date <= %s"; params.append(period_end)
                    if entities and entities.get("clients"):
                        client_filters = []
                        for client in entities["clients"]:
                            client_filters.append("client_name ILIKE %s")
                            params.append(f"%{client}%")
                        q += " AND (" + " OR ".join(client_filters) + ")"
                    q += " GROUP BY client_name ORDER BY revenue DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
                    
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
            
            if analytics_type in ("top_products", "sales_summary"):
                try:
                    q = """
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
                        q += " AND doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND doc_date <= %s"; params.append(period_end)
                    if entities and entities.get("products"):
                        prod_filters = []
                        for prod in entities["products"]:
                            prod_filters.append("nomenclature_name ILIKE %s")
                            params.append(f"%{prod}%")
                        q += " AND (" + " OR ".join(prod_filters) + ")"
                    q += " GROUP BY nomenclature_name ORDER BY revenue DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
                    
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
            
            if analytics_type in ("top_suppliers", "purchase_summary"):
                try:
                    q = """
                        SELECT contractor_name,
                               COUNT(*) as positions,
                               SUM(sum_total) as total_sum,
                               COUNT(DISTINCT nomenclature_name) as products_count,
                               MAX(doc_date) as last_date
                        FROM purchase_prices
                    """
                    params = []
                    if period_date:
                        q += " WHERE doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND doc_date <= %s"; params.append(period_end)
                    if entities and entities.get("suppliers"):
                        prefix = " AND " if period_date else " WHERE "
                        supp_filters = []
                        for supp in entities["suppliers"]:
                            supp_filters.append("contractor_name ILIKE %s")
                            params.append(f"%{supp}%")
                        q += prefix + "(" + " OR ".join(supp_filters) + ")"
                    q += " GROUP BY contractor_name ORDER BY total_sum DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
                    
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
            
            if analytics_type == "production_summary":
                try:
                    q = """
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
                        q += " AND p.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND p.doc_date <= %s"; params.append(period_end)
                    q += " GROUP BY n.name ORDER BY total_sum DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)
                    
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


# =============================================================================
# ПОИСК ПО 1С (документы)
# =============================================================================

def search_1c_data(query, limit=30, period_date=None, period_end=None, entities=None):
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
                        q += " AND doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND doc_date <= %s"; params.append(period_end)
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
                        q += " AND doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND doc_date <= %s"; params.append(period_end)
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
                        q += " AND co.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND co.doc_date <= %s"; params.append(period_end)
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
                        q += " AND so.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND so.doc_date <= %s"; params.append(period_end)
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
                        q += " AND p.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND p.doc_date <= %s"; params.append(period_end)
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
                        q += " AND be.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND be.doc_date <= %s"; params.append(period_end)
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
                        q += " AND ic.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND ic.doc_date <= %s"; params.append(period_end)
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
                        q += " AND inv.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND inv.doc_date <= %s"; params.append(period_end)
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
    """Поиск в интернете через Perplexity."""
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


# =============================================================================
# QUALITY GATES + EVIDENCE PACK
# =============================================================================

def _source_bucket(result: dict) -> str:
    source = result.get("source", "")
    item_type = result.get("type", "")

    if isinstance(item_type, str) and item_type.startswith("analytics_"):
        return "analytics"
    if isinstance(source, str) and source.startswith("Email"):
        return "email"
    if isinstance(source, str) and source.startswith("Чат"):
        return "chat"
    if isinstance(source, str) and source.startswith("1С"):
        return "1c"
    return "other"


def _result_score(result: dict) -> float:
    raw = result.get("final_score", result.get("similarity", 0))
    try:
        score = float(raw)
    except Exception:
        score = 0.0

    if score <= 0:
        bucket = _source_bucket(result)
        if bucket == "analytics":
            return 0.70
        if bucket == "1c":
            return 0.62
    return score


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def deduplicate_results(results: list) -> list:
    """Дедупликация по source_id и по близкому контенту."""
    out = []
    seen_source_ids = set()
    seen_signatures = set()

    for r in results:
        source_id = r.get("source_id")
        source = r.get("source", "")
        content = _normalize_text(r.get("content", ""))
        signature = f"{source}|{content[:220]}"

        if source_id is not None:
            sid_key = f"{source}|{source_id}"
            if sid_key in seen_source_ids:
                continue
            seen_source_ids.add(sid_key)

        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        out.append(r)

    return out


def apply_relevance_filters(results: list) -> list:
    """Отсеивает слабые результаты для chat/email и сортирует по score."""
    filtered = []

    for r in results:
        bucket = _source_bucket(r)
        search_type = r.get("search_type", "")
        score = _result_score(r)

        if bucket == "chat":
            min_score = TELEGRAM_VECTOR_MIN_SCORE if search_type == "vector" else TELEGRAM_SQL_MIN_SCORE
            if score < min_score:
                continue
        elif bucket == "email":
            min_score = EMAIL_VECTOR_MIN_SCORE if search_type == "email_vector" else EMAIL_SQL_MIN_SCORE
            if score < min_score:
                continue

        rr = dict(r)
        rr["_bucket"] = bucket
        rr["_score"] = score
        filtered.append(rr)

    filtered.sort(key=lambda x: x.get("_score", 0), reverse=True)
    return filtered


def apply_recent_intent_boost(results: list, question: str, time_context: dict = None) -> list:
    """Для запросов о свежих событиях поднимает более новые документы."""
    if not results:
        return []
    if not has_recent_intent(question):
        return results

    if time_context is None:
        time_context = extract_time_context(question)
    decay_days = time_context.get("decay_days", 30)

    boosted = []
    for r in results:
        rr = dict(r)
        base_score = rr.get("_score", _result_score(rr))
        dt_value = parse_result_datetime(rr)
        recency = freshness_by_time(dt_value, decay_days)
        rr["_score"] = base_score * 0.75 + recency * 0.25
        rr["_recentness"] = recency
        boosted.append(rr)

    boosted.sort(key=lambda x: x.get("_score", 0), reverse=True)
    return boosted


def apply_intent_source_boost(results: list, question: str) -> list:
    """
    Универсальный prior по источникам в зависимости от intent.
    Нужен, чтобы системные email-уведомления не перебивали профильные чаты.
    """
    if not results:
        return []

    primary = get_primary_intent(question)
    priors_by_intent = {
        "staffing": {"chat": 1.22, "email": 0.92, "1c": 0.78, "analytics": 0.75, "other": 0.9},
        "documents": {"chat": 1.16, "email": 0.96, "1c": 0.85, "analytics": 0.8, "other": 0.9},
        "finance": {"chat": 1.00, "email": 1.06, "1c": 1.14, "analytics": 1.10, "other": 0.9},
        "production": {"chat": 1.05, "email": 0.95, "1c": 1.12, "analytics": 1.06, "other": 0.9},
        "procurement": {"chat": 1.05, "email": 0.96, "1c": 1.12, "analytics": 1.06, "other": 0.9},
        "lookup": {"chat": 1.03, "email": 1.00, "1c": 1.00, "analytics": 1.00, "other": 0.95},
    }
    priors = priors_by_intent.get(primary, priors_by_intent["lookup"])

    boosted = []
    for r in results:
        rr = dict(r)
        bucket = rr.get("_bucket", _source_bucket(rr))
        base = rr.get("_score", _result_score(rr))
        prior = priors.get(bucket, 1.0)

        # Системные уведомления по staffing часто создают шум
        if primary == "staffing" and bucket == "email":
            sender = (rr.get("from_address") or "").lower()
            subject = (rr.get("subject") or "").lower()
            if (
                sender.startswith("no-reply@")
                or sender.startswith("noreply@")
                or sender.startswith("reply@")
                or "gosuslugi" in sender
                or "factorin" in sender
            ):
                if not re.search(r"оффер|offer|должност|кандидат|принят|прием|найм|hiring", subject):
                    prior *= 0.72

        rr["_score"] = base * prior
        rr["_intent_prior"] = prior
        rr["_bucket"] = bucket
        boosted.append(rr)

    boosted.sort(key=lambda x: x.get("_score", 0), reverse=True)
    return boosted


def apply_intent_content_relevance(results: list, question: str) -> list:
    """
    Дополнительный контентный prior по интенту:
    - усиливает документы с терминологией нужного домена;
    - ослабляет документы без доменных маркеров.
    """
    if not results:
        return []

    primary = get_primary_intent(question)
    markers = INTENT_CONTENT_MARKERS.get(primary)
    if not markers:
        return results

    boosted = []
    for r in results:
        rr = dict(r)
        base = rr.get("_score", _result_score(rr))
        bucket = rr.get("_bucket", _source_bucket(rr))

        haystack = " ".join([
            str(rr.get("source", "")),
            str(rr.get("subject", "")),
            str(rr.get("from_address", "")),
            str(rr.get("content", ""))[:1200],
        ]).lower()
        hits = sum(1 for m in markers if m in haystack)

        # Бонус за тематические попадания
        bonus = min(hits * 0.06, 0.30)

        # Штраф за отсутствие signal в chat/email (где шума больше)
        penalty = 1.0
        if hits == 0 and bucket in ("chat", "email"):
            penalty = 0.60 if primary in ("staffing", "documents") else 0.72

        rr["_score"] = base * penalty + bonus
        rr["_intent_hits"] = hits
        rr["_bucket"] = bucket
        boosted.append(rr)

    boosted.sort(key=lambda x: x.get("_score", 0), reverse=True)
    return boosted


def get_effective_evidence_quotas(question: str) -> tuple[dict, bool]:
    """Возвращает квоты evidence и режим строгих caps по primary intent."""
    primary = get_primary_intent(question)
    if primary in INTENT_EVIDENCE_QUOTAS:
        return dict(INTENT_EVIDENCE_QUOTAS[primary]), True
    return dict(EVIDENCE_QUOTAS), False


def _make_citation(result: dict) -> str:
    source = result.get("source", "Источник")
    date_value = result.get("date", "")
    author = result.get("author", "")
    subject = result.get("subject", "")
    from_address = result.get("from_address", "")

    parts = [source]
    if date_value:
        parts.append(str(date_value))
    if author:
        parts.append(f"автор: {author}")
    if subject:
        parts.append(f"тема: {subject[:80]}")
    if from_address:
        parts.append(f"от: {from_address}")
    return " | ".join(parts)


def select_evidence_for_generation(results: list, max_items: int = EVIDENCE_MAX_ITEMS,
                                   quotas: dict = None, strict_caps: bool = False) -> list:
    """Финальный отбор evidence: квоты по источникам + заполнение до max_items."""
    ranked = sorted(results, key=lambda x: x.get("_score", _result_score(x)), reverse=True)
    quotas = dict(quotas or EVIDENCE_QUOTAS)
    counts = {}
    selected = []
    deferred = []

    for r in ranked:
        bucket = r.get("_bucket", _source_bucket(r))
        quota = quotas.get(bucket, quotas.get("other", 2))
        used = counts.get(bucket, 0)
        if used < quota:
            counts[bucket] = used + 1
            selected.append(r)
            if len(selected) >= max_items:
                break
        else:
            deferred.append(r)

    if len(selected) < max_items:
        for r in deferred:
            if strict_caps:
                bucket = r.get("_bucket", _source_bucket(r))
                quota = quotas.get(bucket, quotas.get("other", 2))
                used = counts.get(bucket, 0)
                if used >= quota:
                    continue
                counts[bucket] = used + 1
            selected.append(r)
            if len(selected) >= max_items:
                break

    final = []
    for idx, r in enumerate(selected[:max_items], 1):
        rr = dict(r)
        rr["evidence_id"] = idx
        rr["citation"] = _make_citation(rr)
        rr["evidence_snippet"] = _normalize_text(rr.get("content", ""))[:360]
        final.append(rr)
    return final


def build_evidence_context(evidence_items: list) -> str:
    """Компактный контекст для генератора (8-12 доказательств)."""
    lines = []
    for item in evidence_items:
        score = item.get("_score", _result_score(item))
        lines.append(
            f"[{item['evidence_id']}] {item.get('citation', '')} | score={score:.2f}\n"
            f"{item.get('evidence_snippet', '')}"
        )
    return "\n\n".join(lines)


# =============================================================================
# ГЕНЕРАЦИЯ ОТВЕТА (GPT-4.1)
# =============================================================================

def generate_response(question, db_results, web_results, web_citations=None, chat_context=""):
    """Генерация grounded-ответа с обязательными ссылками на evidence."""
    if not ROUTERAI_API_KEY:
        return "API ключ не настроен"
    try:
        evidence_items = db_results or []
        if evidence_items and "evidence_id" not in evidence_items[0]:
            prepared = apply_relevance_filters(deduplicate_results(evidence_items))
            evidence_items = select_evidence_for_generation(prepared, max_items=EVIDENCE_MAX_ITEMS)

        evidence_context = build_evidence_context(evidence_items)
        sources_map = "\n".join(
            f"[{item['evidence_id']}] {item.get('citation', '')}"
            for item in evidence_items
        )
        company_profile = get_company_profile()

        prompt = f"""{company_profile}

Ты — RAG-агент компании Фрумелад. Отвечай на русском.

ВОПРОС: {question}

ДОКАЗАТЕЛЬСТВА (evidence):
{evidence_context if evidence_context else "Нет релевантных доказательств."}

ДОП. ДАННЫЕ ИЗ ИНТЕРНЕТА:
{(web_results or "")[:1500]}

ПРАВИЛА:
1) Используй только факты из evidence. Не придумывай.
2) Каждый утверждаемый тезис обязан иметь ссылку в формате [n], где n — номер evidence.
3) Если данных недостаточно — явно напиши "Недостаточно данных" и что именно нужно уточнить.
4) Предпочитай конкретику: суммы, даты, документы, имена.
5) Не делай тезисов без ссылки [n].
6) Если вопрос про свежие события — делай приоритет на самых новых доказательствах.

ФОРМАТ ОТВЕТА:
- Краткий вывод (1-2 предложения)
- Ключевые факты (маркированный список, каждый пункт с [n])
- Что не найдено/риски (если есть)
- Источники (список [n] -> краткое описание)

СПИСОК ИСТОЧНИКОВ:
{sources_map if sources_map else "Нет"}

Ответ:"""

        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "openai/gpt-4.1",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1800,
                "temperature": 0
            },
            timeout=60
        )
        result = response.json()
        if "choices" not in result:
            return "Ошибка генерации"

        response_text = result["choices"][0]["message"]["content"]

        # Гарантированно добавляем карту источников внизу, даже если LLM её не вывел
        if sources_map:
            response_text += "\n\n📎 Источники (карта evidence):\n" + sources_map

        if web_citations:
            response_text += "\n\n🌐 Внешние ссылки:"
            for i, url in enumerate(web_citations[:5], 1):
                response_text += f"\n{i}. {url}"

        return response_text
    except Exception as e:
        return f"Ошибка: {e}"


# =============================================================================
# SMART ROUTER (GPT-4.1-mini) — с выбором чатов
# =============================================================================

def route_query(question, chat_context=""):
    """Smart Router: анализирует вопрос, выбирает чаты, строит план."""
    if not ROUTERAI_API_KEY:
        return _default_plan(question)
    
    try:
        chat_list = format_chat_list_for_llm()
        
        prompt = f"""Ты — маршрутизатор запросов для бизнес-ассистента кондитерской компании "Фрумелад".

ДОСТУПНЫЕ ЧАТЫ TELEGRAM:
{chat_list}

ИСТОЧНИКИ ДАННЫХ:
- 1С_ANALYTICS: агрегированные данные (топ клиентов, суммы продаж, рейтинги товаров, объёмы производства). Для ИТОГОВ, СУММ, РЕЙТИНГОВ.
- 1С_SEARCH: поиск конкретных документов (заказ, цена товара, закупка). Для КОНКРЕТНЫХ записей.
- CHATS: переписка сотрудников в Telegram.
- EMAIL: деловая переписка по почте.
- WEB: интернет-поиск (только внешняя информация).
- KNOWLEDGE: база знаний компании (факты, решения, задачи, политики). Для вопросов про правила, процессы, решения, кто за что отвечает, что было решено/сделано.

ТИПЫ АНАЛИТИКИ (для 1С_ANALYTICS):
top_clients, top_products, sales_summary, top_suppliers, production_summary, purchase_summary

ВОПРОС: {question}

ЗАДАЧА: Проанализируй вопрос и определи:
1. Какие КОНКРЕТНЫЕ чаты из списка выше наиболее релевантны (по названию)
2. Какие источники данных нужны
3. Какие ключевые слова использовать для поиска

РАССУЖДАЙ: например "НДС" → бухгалтерия → чаты с "бухгалтерия" и "априори" в названии.
"Закупки сахара" → чаты "закупки" + 1С закупочные цены.

Верни ТОЛЬКО JSON без markdown:
{{"query_type": "analytics|search|lookup|chat_search|web|mixed",
"reasoning": "краткое объяснение логики выбора",
"target_chats": ["tg_chat_xxx", "tg_chat_yyy"],
"steps": [{{"source": "1С_ANALYTICS|1С_SEARCH|CHATS|EMAIL|WEB", "action": "описание", "analytics_type": "тип|null", "keywords": "слова через пробел"}}],
"entities": {{"clients": [], "products": [], "suppliers": []}},
"period": "today|yesterday|week|2weeks|month|quarter|half_year|year|january|...|december|null",
"keywords": "основные ключевые слова"}}

ПРАВИЛА:
- target_chats: выбери 3-7 НАИБОЛЕЕ релевантных чатов из списка. Используй точные имена таблиц [tg_chat_...].
- НЕ включай источники "на всякий случай": добавляй CHATS/EMAIL только когда это действительно нужно по вопросу
- Если вопрос про обсуждения/согласования/переписку — добавляй CHATS и/или EMAIL
- Если вопрос про агрегированные итоги/выручку/топы — приоритет 1С_ANALYTICS
- Если вопрос про конкретные документы/операции — приоритет 1С_SEARCH
- Для бухгалтерских вопросов (НДС, налог, счёт, оплата) — обязательно чаты с "бухгалтерия", "априори", "отчеты по аутсорсингу"
- Для вопросов про закупки — чаты с "закупки"
- Для вопросов про производство — чаты с "производство"
- Для вопросов про внутренние правила, решения, процессы, задачи — добавляй KNOWLEDGE
- KNOWLEDGE хорош для "что было решено", "как мы делаем X", "кто отвечает за Y"
- Минимум 2-3 шага, keywords — существительные без запятых
- period: "за 2 недели" = "2weeks", "в январе" = "january", "недавно"/"в последний раз" = "2weeks"
"""
        
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": "openai/gpt-4.1-mini", "messages": [{"role": "user", "content": prompt}], "max_tokens": 800, "temperature": 0},
            timeout=(5, 15)
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
            if "target_chats" not in plan:
                plan["target_chats"] = []
            
            logger.info(f"Router: type={plan.get('query_type')}, steps={len(plan['steps'])}, "
                       f"target_chats={len(plan.get('target_chats', []))}, period={plan.get('period')}, "
                       f"reasoning={plan.get('reasoning', '')[:100]}")
            return plan
        
        return _default_plan(question)
    
    except Exception as e:
        logger.error(f"Router error: {e}")
        return _default_plan(question)


def _infer_default_sources(question: str) -> list:
    """Heuristic fallback: выбирает источники без обязательного CHATS+EMAIL."""
    q = (question or "").lower()

    chat_signals = ("чат", "telegram", "телеграм", "обсуж", "соглас", "кто писал", "переписк")
    email_signals = ("email", "емейл", "почт", "письм", "в переписке")
    web_signals = ("интернет", "рынок", "новости", "внешн", "курсы валют", "конкурент")
    one_c_signals = (
        "1с", "выручк", "продаж", "закуп", "постав", "клиент", "номенклатур",
        "документ", "заказ", "производ", "банк", "сумм", "сколько"
    )

    sources = []
    if any(sig in q for sig in web_signals):
        sources.append("WEB")
    if any(sig in q for sig in one_c_signals):
        sources.append("1С_SEARCH")
    if any(sig in q for sig in chat_signals):
        sources.append("CHATS")
    if any(sig in q for sig in email_signals):
        sources.append("EMAIL")

    if not sources:
        sources.append("1С_SEARCH")
    return sources


def _default_plan(question):
    """План по умолчанию если Router недоступен."""
    stop_words = {
        'сколько', 'какой', 'какая', 'какие', 'каких', 'когда', 'где', 'кто', 'что',
        'как', 'почему', 'зачем', 'наши', 'наших', 'наша', 'наше', 'нашим',
        'последний', 'последние', 'последняя', 'последнюю', 'последних',
        'который', 'которая', 'которые', 'которых',
        'этот', 'этой', 'этих', 'этом', 'того', 'тому',
        'можно', 'нужно', 'надо', 'есть', 'было', 'будет', 'были',
        'очень', 'более', 'менее', 'самый', 'самые',
        'внешней', 'внутренней', 'основные', 'основной',
        'покажи', 'найди', 'скажи', 'расскажи', 'дай',
        'раз', 'раза', 'разу', 'всего', 'итого',
    }
    clean_query = re.sub(r'[,.:;!?()"\']', ' ', question.lower())
    words = [w.strip() for w in clean_query.split() if len(w.strip()) > 2 and w.strip() not in stop_words]
    keywords = " ".join(words[:5]) if words else question
    
    inferred_sources = _infer_default_sources(question)
    steps = []
    for src in inferred_sources:
        steps.append({"source": src, "action": "поиск", "keywords": keywords})

    if len(inferred_sources) > 1:
        query_type = "mixed"
    elif inferred_sources[0] == "WEB":
        query_type = "web"
    elif inferred_sources[0] == "CHATS":
        query_type = "chat_search"
    else:
        query_type = "search"

    return {
        "query_type": query_type,
        "steps": steps,
        "entities": {"clients": [], "products": [], "suppliers": []},
        "period": None,
        "keywords": keywords,
        "target_chats": [],  # fallback — искать везде
    }


# =============================================================================
# EVALUATOR (GPT-4.1-mini) — оценка достаточности результатов
# =============================================================================

def evaluate_results(question: str, results: list, plan: dict) -> dict:
    """
    Evaluator: оценивает достаточность найденных результатов.
    Возвращает: {"sufficient": True/False, "missing": "...", "retry_keywords": "...", "retry_chats": [...]}
    """
    if not ROUTERAI_API_KEY or not results:
        return {"sufficient": len(results) > 0, "missing": "", "retry_keywords": "", "retry_chats": []}
    
    # Краткое summary результатов для Evaluator
    summary_parts = []
    sources_found = set()
    has_numbers = False
    
    for r in results[:20]:
        source = r.get("source", "")
        sources_found.add(source.split(":")[0].strip())
        content = r.get("content", "")[:200]
        if any(c.isdigit() for c in content):
            has_numbers = True
        summary_parts.append(f"[{source}] {r.get('date', '')} {content}")
    
    summary = "\n".join(summary_parts[:15])
    chat_list = format_chat_list_for_llm()
    
    prompt = f"""Ты — оценщик качества поиска для бизнес-ассистента.

ВОПРОС пользователя: {question}

НАЙДЕННЫЕ РЕЗУЛЬТАТЫ ({len(results)} шт, источники: {', '.join(sources_found)}):
{summary}

ДОСТУПНЫЕ ЧАТЫ (для retry):
{chat_list}

ЗАДАЧА: Оцени, достаточно ли найденных данных для ПОЛНОГО ответа на вопрос.

Критерии НЕДОСТАТОЧНОСТИ:
- Вопрос про конкретные цифры/суммы, но цифр в результатах нет
- Вопрос про согласование/решение, но найдены только общие упоминания без деталей
- Вопрос про конкретный документ/событие, но найдены нерелевантные данные
- Результаты из нерелевантных чатов (например, вопрос про бухгалтерию, а результаты из чата производства)

Верни ТОЛЬКО JSON:
{{"sufficient": true/false, "missing": "что не хватает (кратко)", "retry_keywords": "уточнённые ключевые слова для повторного поиска", "retry_chats": ["tg_chat_xxx"]}}

Если sufficient=true, остальные поля пустые.
Если sufficient=false, в retry_chats укажи чаты из списка где СТОИТ поискать дополнительно.
"""

    try:
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": "openai/gpt-4.1-mini", "messages": [{"role": "user", "content": prompt}], "max_tokens": 300, "temperature": 0},
            timeout=(5, 10)
        )
        
        result = response.json()
        if "choices" in result:
            content = result["choices"][0]["message"]["content"].strip()
            content = re.sub(r'^```(?:json)?\s*', '', content)
            content = re.sub(r'\s*```$', '', content)
            evaluation = json.loads(content)
            
            logger.info(f"Evaluator: sufficient={evaluation.get('sufficient')}, "
                       f"missing={evaluation.get('missing', '')[:80]}")
            return evaluation
        
        return {"sufficient": True, "missing": "", "retry_keywords": "", "retry_chats": []}
    
    except Exception as e:
        logger.error(f"Evaluator error: {e}")
        return {"sufficient": True, "missing": "", "retry_keywords": "", "retry_chats": []}


# =============================================================================
# RERANKING
# =============================================================================

def rerank_results(question: str, results: list, top_k: int = 10) -> list:
    """Переранжирование результатов через LLM."""
    if not results or not ROUTERAI_API_KEY:
        return results[:top_k]
    
    candidates = results[:60]
    
    if len(candidates) <= top_k:
        return candidates
    
    docs_text = []
    for i, r in enumerate(candidates):
        source = r.get('source', 'Unknown')
        content = r.get('content', '')[:300]
        date_str = r.get('date', '')
        docs_text.append(f"[{i}] ({source}, {date_str}) {content}")
    
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
        
        reranked = [candidates[i] for i in indices[:top_k]]
        
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


# =============================================================================
# ОСНОВНОЙ ReAct ЦИКЛ
# =============================================================================

async def process_rag_query(question, chat_context="", user_info: dict = None):
    """
    ReAct цикл обработки RAG-запроса:
    1. Smart Router (выбор чатов + план)
    2. Поиск по источникам
    3. Evaluator (достаточно ли?)
    4. Если нет — повторный поиск (макс 2 итерации)
    5. Reranking
    6. Генерация ответа (GPT-4.1)
    """
    logger.info(f"RAG запрос: {question}")
    start_time = time.time()
    if not user_info:
        user_info = {}

    # === Шаг 1: Smart Router ===
    plan = route_query(question, chat_context)
    plan = ensure_plan_sources(plan, question)
    router_time_ms = int((time.time() - start_time) * 1000)
    logger.info(f"Query plan: type={plan.get('query_type')}, steps={len(plan.get('steps', []))}, "
               f"target_chats={plan.get('target_chats', [])}")
    
    period_date, period_end = _resolve_period(plan.get("period"))
    entities = plan.get("entities", {})
    keywords = plan.get("keywords", question)
    target_chats = plan.get("target_chats", [])
    suggested_chats = suggest_target_chats_by_intent(question, max_items=8)
    if suggested_chats:
        merged_targets = []
        seen_targets = set()
        for t in (target_chats or []) + suggested_chats:
            if not t or t in seen_targets:
                continue
            seen_targets.add(t)
            merged_targets.append(t)
        target_chats = merged_targets[:10]

    primary_intent = get_primary_intent(question)
    
    time_context = extract_time_context(question)
    if time_context["has_time_filter"]:
        logger.info(f"Временной контекст: decay_days={time_context['decay_days']}")
    
    db_results = []
    web_results = ""
    web_citations = []
    
    # === Шаг 2: Выполняем шаги плана ===
    for step in plan.get("steps", []):
        source = step.get("source", "")
        step_keywords = step.get("keywords", keywords)
        analytics_type = step.get("analytics_type")
        
        if source == "1С_ANALYTICS" and analytics_type:
            results = search_1c_analytics(
                analytics_type=analytics_type,
                keywords=step_keywords,
                period_date=period_date,
                period_end=period_end,
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
                period_end=period_end,
                entities=entities
            )
            db_results.extend(results)
            logger.info(f"Step [{source}]: {len(results)} результатов")
        
        elif source == "CHATS":
            # Используем target_chats из Router
            results = search_telegram_chats(
                step_keywords, limit=30, time_context=time_context,
                target_tables=target_chats if target_chats else None
            )
            db_results.extend(results)
            logger.info(f"Step [{source}]: {len(results)} результатов (target={len(target_chats)} чатов)")
        
        elif source == "EMAIL":
            results = search_emails(step_keywords, limit=30, time_context=time_context)
            db_results.extend(results)
            logger.info(f"Step [{source}]: {len(results)} результатов")
        
        elif source == "KNOWLEDGE":
            results = search_knowledge(step_keywords, limit=30)
            db_results.extend(results)
            logger.info(f"Step [{source}]: {len(results)} результатов")
    
    executed_sources = [step.get("source") for step in plan.get("steps", [])]
    db_results = deduplicate_results(db_results)
    db_results = apply_relevance_filters(db_results)
    db_results = apply_recent_intent_boost(db_results, question, time_context=time_context)
    db_results = apply_intent_source_boost(db_results, question)
    db_results = apply_intent_content_relevance(db_results, question)

    logger.info(
        f"Поиск завершён: {len(db_results)} релевантных результатов "
        f"за {time.time() - start_time:.1f}с"
    )

    # Global fallback: если мало релевантных данных, делаем широкое доизвлечение
    if len(db_results) < 4:
        logger.info("Global fallback retrieval: too few results, running broad chats+email search")
        fallback_query = expand_query_for_retrieval(question)
        fallback_chat_results = search_telegram_chats(
            fallback_query, limit=24, time_context=time_context, target_tables=None
        )
        fallback_email_results = search_emails(
            fallback_query, limit=18, time_context=time_context
        )
        db_results.extend(fallback_chat_results)
        db_results.extend(fallback_email_results)
        db_results = apply_relevance_filters(deduplicate_results(db_results))
        db_results = apply_recent_intent_boost(db_results, question, time_context=time_context)
        db_results = apply_intent_source_boost(db_results, question)
        db_results = apply_intent_content_relevance(db_results, question)
        logger.info(f"Global fallback done: now {len(db_results)} results")

    # Intent fallback: если для кадров/документных вопросов мало чатовых подтверждений
    intents = detect_query_intents(question)
    chat_hits = sum(1 for r in db_results if _source_bucket(r) == "chat")
    if (("staffing" in intents) or ("documents" in intents)) and chat_hits < 2:
        logger.info(
            f"Intent fallback: low chat hits ({chat_hits}) for intents={sorted(list(intents))}, "
            "running wide chat retrieval"
        )
        wide_query = expand_query_for_retrieval(question)
        wide_chat = search_telegram_chats(
            wide_query, limit=30, time_context=time_context, target_tables=None
        )
        db_results.extend(wide_chat)
        db_results = apply_relevance_filters(deduplicate_results(db_results))
        db_results = apply_recent_intent_boost(db_results, question, time_context=time_context)
        db_results = apply_intent_source_boost(db_results, question)
        db_results = apply_intent_content_relevance(db_results, question)
        logger.info(f"Intent fallback done: total={len(db_results)} chat_hits={sum(1 for r in db_results if _source_bucket(r) == 'chat')}")
    
    # === Шаг 3: Evaluator — проверяем достаточность (макс 1 итерация) ===
    for retry_num in range(1):
        evaluation = evaluate_results(question, db_results, plan)
        
        if evaluation.get("sufficient", True):
            logger.info(f"Evaluator: данные достаточны (итерация {retry_num})")
            break
        
        # Повторный поиск с уточнёнными параметрами
        retry_keywords = evaluation.get("retry_keywords", "")
        retry_chats = evaluation.get("retry_chats", [])
        
        if not retry_keywords and not retry_chats:
            logger.info(f"Evaluator: insufficient но нет retry параметров, пропускаем")
            break
        
        logger.info(f"Evaluator retry {retry_num + 1}: keywords='{retry_keywords}', chats={retry_chats}")
        
        # Дополнительный поиск
        if retry_keywords:
            if retry_chats:
                extra_chat_results = search_telegram_chats(
                    retry_keywords, limit=20, time_context=time_context,
                    target_tables=retry_chats
                )
                db_results.extend(extra_chat_results)
                logger.info(f"Retry CHATS: {len(extra_chat_results)} результатов из {len(retry_chats)} чатов")

            # EMAIL retry только если email участвует в intent
            q_low = (question or "").lower()
            need_email_retry = (
                "EMAIL" in executed_sources
                or "почт" in q_low
                or "email" in q_low
                or "письм" in q_low
            )
            if need_email_retry:
                extra_email_results = search_emails(retry_keywords, limit=15, time_context=time_context)
                db_results.extend(extra_email_results)
                logger.info(f"Retry EMAIL: {len(extra_email_results)} результатов")

        db_results = apply_relevance_filters(deduplicate_results(db_results))
        db_results = apply_recent_intent_boost(db_results, question, time_context=time_context)
        db_results = apply_intent_source_boost(db_results, question)
        db_results = apply_intent_content_relevance(db_results, question)

    logger.info(f"Итого после ReAct: {len(db_results)} результатов за {time.time() - start_time:.1f}с")

    # === Шаг 4: Reranking ===
    if len(db_results) > 12:
        db_results = rerank_results(question, db_results, top_k=24)

    db_results = apply_relevance_filters(deduplicate_results(db_results))
    db_results = apply_recent_intent_boost(db_results, question, time_context=time_context)
    db_results = apply_intent_source_boost(db_results, question)
    db_results = apply_intent_content_relevance(db_results, question)

    evidence_quotas, strict_caps = get_effective_evidence_quotas(question)
    evidence_results = select_evidence_for_generation(
        db_results,
        max_items=EVIDENCE_MAX_ITEMS,
        quotas=evidence_quotas,
        strict_caps=strict_caps,
    )

    source_counts = {}
    for r in evidence_results:
        b = r.get("_bucket", _source_bucket(r))
        source_counts[b] = source_counts.get(b, 0) + 1
    logger.info(
        f"Evidence pack: {len(evidence_results)} шт, intent={primary_intent}, "
        f"quotas={source_counts}"
    )

    search_time_ms = int((time.time() - start_time) * 1000) - router_time_ms

    # === Шаг 5: Генерация ответа (GPT-4.1) ===
    gen_start = time.time()
    response = generate_response(question, evidence_results, web_results, web_citations, chat_context)
    generation_time_ms = int((time.time() - gen_start) * 1000)

    # === Логирование ===
    _log_rag_query({
        "user_id": user_info.get("user_id"),
        "username": user_info.get("username"),
        "first_name": user_info.get("first_name"),
        "chat_id": user_info.get("chat_id"),
        "chat_type": user_info.get("chat_type"),
        "question": question,
        "primary_intent": primary_intent,
        "detected_intents": list(detect_query_intents(question)),
        "router_query_type": plan.get("query_type"),
        "router_target_chats": plan.get("target_chats"),
        "sources_used": executed_sources,
        "evidence_count": len(evidence_results),
        "evidence_sources": json.dumps(source_counts),
        "evaluator_sufficient": evaluation.get("sufficient", True),
        "retry_count": 0,
        "rerank_applied": len(db_results) > 12,
        "response_length": len(response),
        "response_time_ms": int((time.time() - start_time) * 1000),
        "router_time_ms": router_time_ms,
        "search_time_ms": search_time_ms,
        "generation_time_ms": generation_time_ms,
        "web_search_used": bool(web_results),
        "error": None,
    })

    return response


def _log_rag_query(data: dict):
    """Записывает RAG-запрос в лог. Не блокирует основной поток."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rag_query_log (
                    user_id, username, first_name, chat_id, chat_type,
                    question, primary_intent, detected_intents,
                    router_query_type, router_target_chats, sources_used,
                    evidence_count, evidence_sources, evaluator_sufficient,
                    retry_count, rerank_applied, response_length,
                    response_time_ms, router_time_ms, search_time_ms,
                    generation_time_ms, web_search_used, error
                ) VALUES (
                    %(user_id)s, %(username)s, %(first_name)s, %(chat_id)s, %(chat_type)s,
                    %(question)s, %(primary_intent)s, %(detected_intents)s,
                    %(router_query_type)s, %(router_target_chats)s, %(sources_used)s,
                    %(evidence_count)s, %(evidence_sources)s, %(evaluator_sufficient)s,
                    %(retry_count)s, %(rerank_applied)s, %(response_length)s,
                    %(response_time_ms)s, %(router_time_ms)s, %(search_time_ms)s,
                    %(generation_time_ms)s, %(web_search_used)s, %(error)s
                )
            """, data)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"RAG log error: {e}")


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
