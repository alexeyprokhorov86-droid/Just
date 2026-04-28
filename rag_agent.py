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


# get_chat_list переехал в tools.chats (кэш живёт там же).
from tools.chats import get_chat_list

# Ограничители качества retrieval/generation
TELEGRAM_VECTOR_MIN_SCORE = 0.72
EMAIL_VECTOR_MIN_SCORE = 0.55
TELEGRAM_SQL_MIN_SCORE = 0.55
EMAIL_SQL_MIN_SCORE = 0.42

EVIDENCE_MAX_ITEMS = 16
EVIDENCE_QUOTAS = {
    "analytics": 10,  # top_* / *_by_nomenclature часто возвращают 10-20 строк — не резать
    "1c": 4,
    "chat": 3,
    "email": 3,
    "other": 2,
}

INTENT_EVIDENCE_QUOTAS = {
    "staffing": {"chat": 6, "email": 2, "1c": 1, "analytics": 1, "other": 1},
    "documents": {"chat": 6, "email": 2, "1c": 1, "analytics": 1, "other": 1},
    "finance": {"chat": 3, "email": 4, "1c": 4, "analytics": 10, "other": 1},
    "production": {"chat": 3, "email": 2, "1c": 4, "analytics": 10, "other": 1},
    "procurement": {"chat": 3, "email": 2, "1c": 4, "analytics": 10, "other": 1},
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
    # Дайджест: "что обсуждали вчера/сегодня/за неделю" без конкретной темы
    if re.search(
        r"(что|чем|о\s+чём|как).{0,20}(обсужд|происход|было|говорил|писал|делал|нов|случил)"
        r"|что\s+нового|дайджест|новости\s+компани|итог[иа]\s+(дня|недели|вчера|сегодня)",
        merged_q
    ):
        intents.add("recent_activity")

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

    if "recent_activity" in intents:
        _add_step("CHATS")
        _add_step("EMAIL")
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
        "freshness_weight": 0.25,  # По умолчанию
        "find_earliest": False,  # True для вопросов "когда впервые/первое упоминание/когда началось"
    }

    # Детекция "первого упоминания" — инвертирует порядок сортировки
    # и отключает freshness bias (ищем старые записи).
    earliest_patterns = [
        r"\bвперв(?:ые|ой|ыми)\b",
        r"перв(?:ое|ого|ый)\s+(?:упомина|раз|сообщение|письмо)",
        r"когда\s+(?:начал|появил|стартова|пошл)",
        r"самое\s+ранне",
        r"с\s+каких\s+пор",
        r"с\s+какой\s+даты",
    ]
    for pat in earliest_patterns:
        if re.search(pat, question_lower):
            result["find_earliest"] = True
            # Для earliest ищем по всем данным без decay
            result["decay_days"] = 3650  # 10 лет
            result["freshness_weight"] = 0.0
            break
    
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
    
    # "вчера"/"сегодня" — точные границы дня, не "за N дней до сейчас"
    if re.search(r'\bвчера\b', question_lower) and not result["find_earliest"]:
        yesterday = (now - timedelta(days=1)).date()
        result["has_time_filter"] = True
        result["date_from"] = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
        result["date_to"] = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
        result["decay_days"] = 2
        result["freshness_weight"] = 0.5
    elif re.search(r'\bсегодня\b', question_lower) and not result["find_earliest"]:
        today_d = now.date()
        result["has_time_filter"] = True
        result["date_from"] = datetime(today_d.year, today_d.month, today_d.day, 0, 0, 0)
        result["date_to"] = now
        result["decay_days"] = 1
        result["freshness_weight"] = 0.5
    else:
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
    conn.autocommit = True  # ошибка в одной таблице не должна ронять остальные
    results = []

    # (table, text_col, type_col, has_verification_status)
    # km_decisions не имеет verification_status и *_type; используем scope_type.
    # km_tasks имеет status (не task_type).
    # km_policies имеет scope_type + verification_status.
    tables = [
        ("km_facts", "fact_text", "fact_type", True),
        ("km_decisions", "decision_text", "scope_type", False),
        ("km_tasks", "task_text", "status", True),
        ("km_policies", "policy_text", "scope_type", True),
    ]

    per_table = max(limit // len(tables), 5)

    try:
        with conn.cursor() as cur:
            for table, text_col, type_col, has_verif in tables:
                try:
                    where_verif = (
                        "verification_status NOT IN ('rejected', 'duplicate') AND "
                        if has_verif else ""
                    )
                    cur.execute(f"""
                        SELECT id, {text_col}, {type_col}, confidence, created_at,
                               1 - (embedding <=> %s::vector) as similarity
                        FROM {table}
                        WHERE {where_verif}embedding IS NOT NULL
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
            # fetch_limit больше, чем нужно — чтобы после dedup осталось достаточно
            fetch_limit = min(limit * 5, 200)
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
            """, (emb_str, emb_str, fetch_limit))

            seen_content = set()  # content-based dedup против повторяющихся email
            for row in cur.fetchall():
                if len(results) >= limit:
                    break
                sim = float(row[12]) if row[12] is not None else 0.0
                if sim < min_similarity:
                    continue
                content_text = row[1] or ""
                content_key = content_text[:300].strip().lower()
                if content_key in seen_content:
                    continue
                seen_content.add(content_key)
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


def search_unified(query: str, limit: int = 30) -> list:
    """
    Гибридный поиск: km_* (legacy e5) + source_chunks (Qwen3) + dedup + opt rerank.

    Логика:
    1) Параллельно search_knowledge (km_facts/decisions/tasks/policies через e5)
       и search_source_chunks (source_chunks.embedding_v2 через Qwen3 HNSW).
    2) Дедуп по первым 200 символам content (режет повторы между km_* и
       source_chunks:km_* — дистиллированные факты часто дублируются).
    3) Если USE_RERANKER=true → пересортировка через Qwen3-Reranker; иначе
       сортировка по исходному cosine similarity.

    Вызывается из основного RAG pipeline когда USE_EMBEDDING_V2=true.
    По данным A/B на 10 фикс-вопросах Qwen3 выигрывает 6/10, дополняет 2/10.
    """
    km_results = search_knowledge(query, limit=limit)
    sc_results = search_source_chunks(query, limit=limit)

    seen = set()
    unique = []
    for r in km_results + sc_results:
        content = (r.get("content", "") or "").strip().lower()
        key = content[:200]
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(r)

    use_reranker = os.getenv("USE_RERANKER", "false").lower() == "true"
    if use_reranker and unique:
        try:
            from chunkers.reranker import rerank
            unique = rerank(query, unique, top_k=limit)
        except Exception as e:
            logger.warning(f"search_unified rerank failed: {e}")
            unique.sort(key=lambda x: x.get("similarity", 0), reverse=True)
    else:
        unique.sort(key=lambda x: x.get("similarity", 0), reverse=True)

    logger.info(
        f"search_unified: km={len(km_results)} sc={len(sc_results)} "
        f"→ uniq={len(unique)} → top={min(limit, len(unique))} "
        f"(reranker={'on' if use_reranker else 'off'})"
    )
    return unique[:limit]


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
    find_earliest = time_context.get("find_earliest", False)

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

            # Дайджест-режим: пустые keywords + date filter → берём все сообщения за период
            if not keyword_terms and date_from and chat_tables:
                for table_name in chat_tables[:20]:
                    try:
                        dq_sql = (
                            "SELECT id, timestamp, first_name, message_text, media_analysis, "
                            "message_type, content_text FROM {} "
                            "WHERE message_text IS NOT NULL AND LENGTH(message_text) > 10 "
                        )
                        dq_params: list = []
                        if date_from:
                            dq_sql += "AND timestamp >= %s "
                            dq_params.append(date_from)
                        if date_to:
                            dq_sql += "AND timestamp <= %s "
                            dq_params.append(date_to)
                        dq_sql += f"ORDER BY timestamp {'ASC' if find_earliest else 'DESC'} LIMIT %s"
                        dq_params.append(min(limit, 20))
                        cur.execute(sql.SQL(dq_sql).format(sql.Identifier(table_name)), dq_params)
                        for row in cur.fetchall():
                            chat_name = table_name.replace('tg_chat_', '').split('_', 1)[-1].replace('_', ' ').title()
                            content = row[3] or ""
                            if row[6]:
                                content += f"\n[Документ]: {row[6][:300]}"
                            ts = row[1]
                            freshness = freshness_by_time(ts, decay_days)
                            results.append({
                                "source": f"Чат: {chat_name}",
                                "content": content,
                                "timestamp": ts,
                                "date": ts.strftime("%d.%m.%Y") if ts else "",
                                "similarity": 0.6,
                                "freshness": freshness,
                                "final_score": 0.6 * (1 - freshness_weight) + freshness * freshness_weight,
                                "search_type": "chat_sql_digest",
                                "source_id": f"{table_name}_{row[0]}",
                            })
                    except Exception:
                        conn.rollback()
                logger.info(f"Chat SQL digest: {len(results)} сообщений за {date_from.date() if date_from else '?'}")
                return results[:limit]

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
                        query_sql += f"ORDER BY timestamp {'ASC' if find_earliest else 'DESC'} LIMIT %s"
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
    find_earliest = time_context.get("find_earliest", False)
    sort_order = "ASC" if find_earliest else "DESC"

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

            # Для дайджест-запросов (пустые keywords + есть date filter) — возвращаем все
            # внутренние/бизнесовые письма за период без FTS-фильтра по тексту.
            if not keyword_terms and date_from:
                date_sql = """
                    SELECT id, subject, body_text, from_address, received_at
                    FROM email_messages
                    WHERE email_category IN ('internal', 'external_business')
                """
                date_params: list = []
                date_sql += " AND received_at >= %s"
                date_params.append(date_from)
                if date_to:
                    date_sql += " AND received_at <= %s"
                    date_params.append(date_to)
                date_sql += f" ORDER BY received_at {sort_order} LIMIT %s"
                date_params.append(limit * 3)
                cur.execute(date_sql, date_params)
                fts_results = cur.fetchall()
            else:
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
                fts_sql += f" ORDER BY received_at {sort_order} LIMIT %s"
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
                    kw_sql += f" ORDER BY received_at {sort_order} LIMIT %s"
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

    # Конкретные кварталы: q1_2025, q4_2025, q2_2026 и т.п.
    m = re.match(r"^q([1-4])_(\d{4})$", period_str.lower().strip())
    if m:
        q_num = int(m.group(1))
        y = int(m.group(2))
        q_start_month = (q_num - 1) * 3 + 1
        q_end_month = q_num * 3
        q_start = date(y, q_start_month, 1)
        if q_end_month == 12:
            q_end = date(y, 12, 31)
        else:
            q_end = date(y, q_end_month + 1, 1) - timedelta(days=1)
        return q_start, q_end

    # Конкретные месяцы с годом: january_2025, march_2026 и т.п.
    m = re.match(r"^([a-z]+)_(\d{4})$", period_str.lower().strip())
    if m:
        months_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        if m.group(1) in months_map:
            mn = months_map[m.group(1)]
            y = int(m.group(2))
            first_day = date(y, mn, 1)
            if mn == 12:
                last_day = date(y, 12, 31)
            else:
                last_day = date(y, mn + 1, 1) - timedelta(days=1)
            return first_day, last_day

    # Прямые даты: period_str = "2025-10-01..2025-12-31"
    m = re.match(r"^(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})$", period_str.strip())
    if m:
        try:
            return date.fromisoformat(m.group(1)), date.fromisoformat(m.group(2))
        except ValueError:
            pass

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

# ============================================================
# Text-to-SQL fallback (Фаза 3): Claude Opus 4.7 + safe read-only runner
# ============================================================

_SQL_BLACKLIST = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|GRANT|REVOKE|CREATE|COPY|MERGE|CALL|EXECUTE|DO|VACUUM|ANALYZE|REINDEX|LOCK|REFRESH)\b",
    re.IGNORECASE,
)
_SQL_START = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def _run_safe_sql(sql: str, timeout_sec: int = 15) -> tuple:
    """
    Выполняет SELECT/WITH в read-only транзакции с timeout'ом.

    Безопасность:
    - Только SELECT/WITH в начале
    - Blacklist ключевых слов DDL/DML
    - SET LOCAL TRANSACTION READ ONLY внутри транзакции
    - statement_timeout ограничивает время
    - auto-LIMIT 200 если не указан

    Returns: (rows, column_names)
    Raises RuntimeError на нарушениях безопасности / SQL ошибках.
    """
    sql_clean = sql.strip().rstrip(";").strip()
    if not _SQL_START.match(sql_clean):
        raise RuntimeError("SQL должен начинаться с SELECT или WITH")
    if _SQL_BLACKLIST.search(sql_clean):
        raise RuntimeError("SQL содержит запрещённое ключевое слово (DDL/DML)")
    if ";" in sql_clean:
        raise RuntimeError("SQL не должен содержать ';' (только один statement)")
    if not re.search(r"\bLIMIT\s+\d+\s*$", sql_clean, re.IGNORECASE):
        sql_clean = sql_clean + " LIMIT 200"

    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET LOCAL TRANSACTION READ ONLY")
            cur.execute(f"SET LOCAL statement_timeout = {int(timeout_sec * 1000)}")
            cur.execute(sql_clean)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description] if cur.description else []
        conn.rollback()
        return rows, cols
    finally:
        try:
            conn.close()
        except Exception:
            pass


_SQL_SCHEMA_HINT = """Схема БД (PostgreSQL, кондитерская компания "Фрумелад" + "НФ/Новэл Фуд"):

-- Materialized views (обновляются каждые 10 мин) — ПРЕДПОЧТИТЕЛЬНЫ для аналитики:

mart_sales (продажи, денормализованные):
  id, doc_date DATE, doc_number, doc_type ('Реализация'/'Возврат от покупателя'),
  client_name, consignee_name, nomenclature_name, nomenclature_type,
  quantity NUMERIC, price, sum_with_vat, sum_without_vat,
  year, month, week_start DATE, year_month TEXT (YYYY-MM)

mart_purchases (закупки):
  id, doc_date DATE, doc_number, contractor_name,
  nomenclature_name, quantity, price, sum_total,
  year, month, year_month

mart_production (производство):
  id, doc_date, doc_number, department_key, nomenclature_key,
  quantity, price, sum_total, year_month
  → для nomenclature.name нужен JOIN nomenclature ON id::text = nomenclature_key

mart_customer_orders (заказы клиентов), mart_supplier_orders (заказы поставщикам) — похожая структура.

-- Справочники:

nomenclature: id UUID, name, article, weight NUMERIC, weight_unit ('кг'/'г'/'шт'), full_name
  — для JOIN с c1_* и mart_production используй n.id::text = <nomenclature_key>
  — для JOIN с mart_sales/purchases используй n.name = nomenclature_name

c1_warehouses: ref_key, name, warehouse_type, is_folder

c1_stock_balance: warehouse_key, nomenclature_key, quantity, in_shipment
  JOIN: nomenclature ON id::text = nomenclature_key; c1_warehouses ON ref_key = warehouse_key

c1_employees: ref_key, last_name, first_name, middle_name
v_current_staff: актуальный штат

-- Полезные views:

v_plan_fact_weekly: "Неделя" DATE, "Заказы (план)", "Ордера (факт)", "Передачи ФМ", "Отклонение", "Выполнение %"
v_consumption_vs_purchases_monthly: period TEXT (YYYY-MM), nom_name, consumed, purchased, diff
v_sales_adjusted: скорректированные продажи (с учётом возвратов):
  effective_date, actual_date, client_name, nomenclature_name, quantity, price, sum_with_vat

-- Дополнительно есть c1_purchases, c1_purchase_items, c1_sales_plan, c1_production_items — обычно mart-views закрывают.

-- Email переписка:
email_messages: id, message_id, thread_id, mailbox_id, folder,
  direction ('inbound'/'outbound'), from_address, to_addresses text[], cc_addresses text[],
  subject, subject_normalized, body_text, body_html, has_attachments, attachment_count,
  received_at TIMESTAMP, processed_at, category
  — поля времени: received_at (НЕ sent_at, которой НЕТ).
  — для поиска адреса в to/cc: array_to_string(to_addresses, ',') ILIKE '%X%'
  — from_address это plain varchar

  ⚡ ВАЖНО для вопросов "кто контакт / ФИО / менеджер":
  body_text содержит ПОЛНЫЙ исходный текст письма, включая ПОДПИСЬ автора
  (ФИО, должность, телефон, email). В подписи обычно есть "С уважением,"
  и затем 2-5 строк с персональной информацией.
  Для извлечения подписи используй:
    substring(body_text FROM '(?s)С уважением[,!][^\n]*\n([^|]{20,500}?)(?:\n\n|$)')
  Или включай body_text в результат и Answerer выделит ФИО в ответе.
  Предпочитай outbound письма (direction='outbound') — они содержат ПОЛНУЮ
  подпись отправителя, а inbound могут быть cut'нутыми нашим клиентом.
  Также исключай служебные адреса: noreply@, no-reply@, svc_*, *service*,
  edi@, pretenz*, accounting@ — они не содержат персон.

email_threads: id, subject, first_at, last_at, status
email_attachments: id, message_id (FK на email_messages), filename, content_text, media_kind

-- Telegram чаты:
tg_chat_<id>_<name>: id, timestamp, user_id, message (text), from_user_name, message_type
  (таблицы динамические, по одной на чат; имена через information_schema.tables)
tg_chats_metadata: chat_id, chat_title, table_name
tg_user_roles: user_id, first_name, username, role (ILIKE '%технолог%' и т.п.), chat_id, is_active

-- База знаний (для бизнес-вопросов типа "что мы решили о X"):
km_facts, km_decisions, km_tasks, km_policies — text+embedding. Для них используй retrieval, не SQL.
"""


def _generate_sql_via_llm(question: str, entities: dict = None,
                           period_date=None, period_end=None) -> str:
    """
    LLM (Claude Opus 4.7) пишет SQL по бизнес-вопросу.
    Возвращает готовый SELECT (без ';' и markdown).
    """
    entities = entities or {}
    extras = []
    if period_date:
        extras.append(f"Период начало: {period_date}")
    if period_end:
        extras.append(f"Период конец: {period_end}")
    if entities.get("clients"):
        extras.append(f"Клиенты (ILIKE match): {entities['clients']}")
    if entities.get("suppliers"):
        extras.append(f"Поставщики (ILIKE match): {entities['suppliers']}")
    if entities.get("products"):
        extras.append(f"Номенклатура (ILIKE match): {entities['products']}")
    if entities.get("warehouses"):
        extras.append(f"Склады (ILIKE match): {entities['warehouses']}")

    prompt = f"""Ты — SQL-ассистент. Сгенерируй ОДИН безопасный SELECT-запрос, отвечающий на бизнес-вопрос.

{_SQL_SCHEMA_HINT}

ВОПРОС: {question}

КОНТЕКСТ:
{chr(10).join(extras) if extras else '(нет дополнительных сущностей или периода)'}

ПРАВИЛА:
- ТОЛЬКО SELECT или WITH (никаких INSERT/UPDATE/DELETE/DDL)
- ILIKE '%...%' для текстовых фильтров (клиент, поставщик, номенклатура)
- Даты — через doc_date >= 'YYYY-MM-DD' AND doc_date < 'YYYY-MM-DD'
- Агрегаты (SUM, COUNT, AVG) + GROUP BY для аналитики
- ORDER BY для осмысленной сортировки (revenue DESC, qty DESC, date DESC)
- LIMIT (≤ 50 строк для аналитики, ≤ 200 для детальной выборки)
- Один запрос, БЕЗ ';' в конце
- Используй mart_* вместо c1_* где возможно

Верни ТОЛЬКО SQL, без пояснений и markdown-блоков:"""

    sql_text = _call_answerer(prompt, "anthropic/claude-opus-4.7", max_tokens=800, timeout=60)
    sql_text = re.sub(r'^```(?:sql)?\s*', '', sql_text.strip())
    sql_text = re.sub(r'\s*```$', '', sql_text)
    return sql_text.strip()


def _format_qty_with_unit(qty, weight, weight_unit) -> str:
    """
    Форматирует количество с единицей измерения из nomenclature.
    Примеры:
        qty=10000, weight=1.0, unit='кг'  → '10 000 кг'
        qty=1200,  weight=0.4, unit='кг'  → '1 200 ед. × 0.4 кг = 480 кг'
        qty=50,    weight=None            → '50 ед.'
    """
    if qty is None:
        return "0 ед."
    qty_fmt = f"{qty:,.3f}".replace(",", " ").rstrip("0").rstrip(".")
    if not weight_unit:
        return f"{qty_fmt} ед."
    w = float(weight or 0)
    if abs(w - 1.0) < 1e-6:
        # Единица = 1 {weight_unit}: quantity уже в {weight_unit}
        return f"{qty_fmt} {weight_unit}"
    if w > 0:
        total = float(qty) * w
        total_fmt = f"{total:,.0f}".replace(",", " ")
        return f"{qty_fmt} ед. × {w:g} {weight_unit} = {total_fmt} {weight_unit}"
    return f"{qty_fmt} ед."


def _format_volume_row(verb, nom, qty, rub, weight, weight_unit,
                        first_date, last_date, docs,
                        rub_label="сумма", docs_label="документ(ов)") -> str:
    """Общий форматтер content-строки для purchases/sales/production."""
    qty_str = _format_qty_with_unit(qty, weight, weight_unit)
    rub_s = f"{rub:,.0f}".replace(",", " ") if rub else "0"
    period_s = f"{first_date}..{last_date}" if first_date and last_date else ""
    return (
        f"{verb} '{nom}': {qty_str}, {rub_label} {rub_s} руб., "
        f"{docs} {docs_label}"
        + (f", период {period_s}" if period_s else "")
    )


def search_1c_analytics(analytics_type, keywords="", period_date=None,
                         period_end=None, entities=None, limit=20):
    """Агрегированные запросы по данным 1С."""
    results = []
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            
            if analytics_type in ("top_clients", "sales_summary"):
                try:
                    # mart_sales уже построен над v_sales_adjusted (effective_date AS doc_date,
                    # Корректировки включены, ФРУМЕЛАД отфильтрован). Убрали WHERE doc_type='Реализация'
                    # чтобы net-выручка учитывала возвраты (отрицательные Корректировки).
                    q = """
                        SELECT client_name,
                               COUNT(*) as positions,
                               SUM(sum_with_vat) as revenue,
                               MIN(doc_date) as first_date,
                               MAX(doc_date) as last_date,
                               COUNT(DISTINCT doc_number) as docs_count
                        FROM mart_sales
                        WHERE 1=1
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
                    # mart_sales — net с учётом Корректировок. AVG(price) считаем только
                    # по Реализациям (иначе avg искажается ценами возвратов).
                    q = """
                        SELECT nomenclature_name,
                               SUM(quantity) as total_qty,
                               SUM(sum_with_vat) as revenue,
                               AVG(NULLIF(price,0)) FILTER (WHERE doc_type='Реализация') as avg_price,
                               COUNT(DISTINCT client_name) as clients_count
                        FROM mart_sales
                        WHERE 1=1
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
                        FROM mart_purchases
                        WHERE 1=1
                    """
                    params = []
                    if period_date:
                        q += " AND doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND doc_date <= %s"; params.append(period_end)
                    if entities and entities.get("suppliers"):
                        supp_filters = []
                        for supp in entities["suppliers"]:
                            supp_filters.append("contractor_name ILIKE %s")
                            params.append(f"%{supp}%")
                        q += " AND (" + " OR ".join(supp_filters) + ")"
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

            # ---------- Новые SQL-tools (keyword-driven) ----------

            if analytics_type == "purchases_by_nomenclature":
                try:
                    q = """
                        SELECT mp.nomenclature_name,
                               SUM(mp.quantity) AS qty,
                               SUM(mp.sum_total) AS rub,
                               MIN(mp.doc_date) AS first_date,
                               MAX(mp.doc_date) AS last_date,
                               COUNT(DISTINCT mp.doc_number) AS docs,
                               MAX(n.weight) AS w,
                               MAX(n.weight_unit) AS wu
                        FROM mart_purchases mp
                        LEFT JOIN nomenclature n ON n.name = mp.nomenclature_name
                        WHERE 1=1
                    """
                    params = []
                    if period_date:
                        q += " AND mp.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND mp.doc_date <= %s"; params.append(period_end)
                    kw_set = set((clean_keywords(keywords) or [])[:3])
                    for kw in kw_set:
                        q += " AND mp.nomenclature_name ILIKE %s"; params.append(f"%{kw}%")
                    # products из entities как fallback если Router не положил в keywords
                    for pr in (entities or {}).get("products", [])[:3]:
                        if pr and pr.lower() not in " ".join(kw_set).lower():
                            q += " AND mp.nomenclature_name ILIKE %s"; params.append(f"%{pr}%")
                    for sup in (entities or {}).get("suppliers", [])[:3]:
                        q += " AND mp.contractor_name ILIKE %s"; params.append(f"%{sup}%")
                    q += " GROUP BY mp.nomenclature_name ORDER BY qty DESC NULLS LAST LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)

                    for row in cur.fetchall():
                        nom, qty, rub, fd, ld, docs, w, wu = row
                        content = _format_volume_row("Закуплено", nom, qty, rub, w, wu, fd, ld, docs)
                        results.append({
                            "source": "1С: ЗАКУПКИ ПО НОМЕНКЛАТУРЕ",
                            "date": ld.strftime("%d.%m.%Y") if ld else "",
                            "content": content,
                            "type": "analytics_purchases_by_nomenclature",
                        })
                except Exception as e:
                    logger.debug(f"purchases_by_nomenclature: {e}")

            if analytics_type == "sales_by_nomenclature":
                try:
                    # mart_sales: net-количество и сумма с учётом Корректировок
                    q = """
                        SELECT ms.nomenclature_name,
                               SUM(ms.quantity) AS qty,
                               SUM(ms.sum_with_vat) AS rub,
                               MIN(ms.doc_date) AS first_date,
                               MAX(ms.doc_date) AS last_date,
                               COUNT(DISTINCT ms.doc_number) AS docs,
                               MAX(n.weight) AS w,
                               MAX(n.weight_unit) AS wu
                        FROM mart_sales ms
                        LEFT JOIN nomenclature n ON n.name = ms.nomenclature_name
                        WHERE 1=1
                    """
                    params = []
                    if period_date:
                        q += " AND ms.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND ms.doc_date <= %s"; params.append(period_end)
                    kw_set = set((clean_keywords(keywords) or [])[:3])
                    for kw in kw_set:
                        q += " AND ms.nomenclature_name ILIKE %s"; params.append(f"%{kw}%")
                    for pr in (entities or {}).get("products", [])[:3]:
                        if pr and pr.lower() not in " ".join(kw_set).lower():
                            q += " AND ms.nomenclature_name ILIKE %s"; params.append(f"%{pr}%")
                    for cl in (entities or {}).get("clients", [])[:3]:
                        q += " AND (ms.client_name ILIKE %s OR ms.consignee_name ILIKE %s)"
                        params.append(f"%{cl}%"); params.append(f"%{cl}%")
                    q += " GROUP BY ms.nomenclature_name ORDER BY rub DESC NULLS LAST LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)

                    for row in cur.fetchall():
                        nom, qty, rub, fd, ld, docs, w, wu = row
                        content = _format_volume_row(
                            "Продано", nom, qty, rub, w, wu, fd, ld, docs,
                            rub_label="выручка", docs_label="реализаций",
                        )
                        results.append({
                            "source": "1С: ПРОДАЖИ ПО НОМЕНКЛАТУРЕ",
                            "date": ld.strftime("%d.%m.%Y") if ld else "",
                            "content": content,
                            "type": "analytics_sales_by_nomenclature",
                        })
                except Exception as e:
                    logger.debug(f"sales_by_nomenclature: {e}")

            if analytics_type == "stock_balance":
                try:
                    q = """
                        SELECT n.name AS nom, w.name AS warehouse,
                               SUM(sb.quantity) AS qty, SUM(sb.in_shipment) AS in_ship,
                               MAX(n.weight) AS w, MAX(n.weight_unit) AS wu
                        FROM c1_stock_balance sb
                        JOIN nomenclature n ON n.id::text = sb.nomenclature_key
                        LEFT JOIN c1_warehouses w ON w.ref_key = sb.warehouse_key
                        WHERE sb.quantity > 0
                    """
                    params = []
                    kw_set = set((clean_keywords(keywords) or [])[:3])
                    for kw in kw_set:
                        q += " AND n.name ILIKE %s"; params.append(f"%{kw}%")
                    for pr in (entities or {}).get("products", [])[:3]:
                        if pr and pr.lower() not in " ".join(kw_set).lower():
                            q += " AND n.name ILIKE %s"; params.append(f"%{pr}%")
                    for wh_name in (entities or {}).get("warehouses", [])[:3]:
                        q += " AND w.name ILIKE %s"; params.append(f"%{wh_name}%")
                    q += " GROUP BY n.name, w.name ORDER BY qty DESC LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)

                    for row in cur.fetchall():
                        nom, wh, qty, in_ship, w, wu = row
                        unit_str = _format_qty_with_unit(qty, w, wu)
                        ship_s = ""
                        if in_ship and in_ship > 0:
                            ship_s = f" (в пути: {_format_qty_with_unit(in_ship, w, wu)})"
                        content = (
                            f"Остаток '{nom}' на складе '{wh or '?'}': {unit_str}{ship_s}"
                        )
                        results.append({
                            "source": "1С: ОСТАТКИ НА СКЛАДЕ",
                            "date": "",
                            "content": content,
                            "type": "analytics_stock_balance",
                        })
                except Exception as e:
                    logger.debug(f"stock_balance: {e}")

            if analytics_type == "production_by_nomenclature":
                try:
                    q = """
                        SELECT n.name AS nom,
                               SUM(mp.quantity) AS qty,
                               SUM(mp.sum_total) AS rub,
                               MIN(mp.doc_date) AS first_date,
                               MAX(mp.doc_date) AS last_date,
                               COUNT(DISTINCT mp.doc_number) AS docs,
                               MAX(n.weight) AS w,
                               MAX(n.weight_unit) AS wu
                        FROM mart_production mp
                        LEFT JOIN nomenclature n ON n.id::text = mp.nomenclature_key
                        WHERE 1=1
                    """
                    params = []
                    if period_date:
                        q += " AND mp.doc_date >= %s"; params.append(period_date)
                    if period_end:
                        q += " AND mp.doc_date <= %s"; params.append(period_end)
                    for kw in (clean_keywords(keywords) or [])[:3]:
                        q += " AND n.name ILIKE %s"; params.append(f"%{kw}%")
                    # products из entities — дополнительный фильтр (если Router указал продукт, но не вставил в keywords)
                    for pr in (entities or {}).get("products", [])[:3]:
                        if pr and (not keywords or pr.lower() not in (keywords or "").lower()):
                            q += " AND n.name ILIKE %s"; params.append(f"%{pr}%")
                    q += " GROUP BY n.name ORDER BY qty DESC NULLS LAST LIMIT %s"
                    params.append(limit)
                    cur.execute(q, params)

                    for row in cur.fetchall():
                        nom, qty, rub, fd, ld, docs, w, wu = row
                        content = _format_volume_row(
                            "Произведено", nom or "?", qty, rub, w, wu, fd, ld, docs,
                            rub_label="сумма",
                        )
                        results.append({
                            "source": "1С: ПРОИЗВОДСТВО ПО НОМЕНКЛАТУРЕ",
                            "date": ld.strftime("%d.%m.%Y") if ld else "",
                            "content": content,
                            "type": "analytics_production_by_nomenclature",
                        })
                except Exception as e:
                    logger.debug(f"production_by_nomenclature: {e}")

            if analytics_type == "custom_sql":
                # Text-to-SQL через Claude Opus 4.7: LLM пишет SQL по вопросу.
                # Используется когда стандартные *_by_nomenclature / top_* не
                # покрывают. Router должен класть текст вопроса в keywords.
                try:
                    question_for_sql = (keywords or "").strip()
                    if not question_for_sql:
                        logger.warning("custom_sql: пустой keywords — пропуск")
                    else:
                        sql = _generate_sql_via_llm(
                            question_for_sql, entities,
                            period_date=period_date, period_end=period_end,
                        )
                        logger.info(f"custom_sql generated: {sql[:250]}")
                        rows, cols = _run_safe_sql(sql, timeout_sec=15)
                        logger.info(f"custom_sql: {len(rows)} строк")
                        for row in rows[:limit]:
                            parts = []
                            for i, v in enumerate(row):
                                if v is None:
                                    continue
                                label = cols[i] if i < len(cols) else f"col{i}"
                                s = str(v)
                                if len(s) > 80:
                                    s = s[:77] + "..."
                                parts.append(f"{label}: {s}")
                            content = "; ".join(parts)
                            results.append({
                                "source": "1С: КАСТОМНЫЙ SQL",
                                "date": "",
                                "content": content,
                                "type": "analytics_custom_sql",
                                "sql": sql,
                            })
                except Exception as e:
                    logger.warning(f"custom_sql failed: {e}")

            if analytics_type == "plan_vs_fact":
                try:
                    q = """
                        SELECT "Неделя", "Заказы (план)", "Ордера (факт)",
                               "Передачи ФМ", "Отклонение", "Выполнение %"
                        FROM v_plan_fact_weekly
                        WHERE 1=1
                    """
                    params = []
                    if period_date:
                        q += ' AND "Неделя" >= %s'; params.append(period_date)
                    if period_end:
                        q += ' AND "Неделя" <= %s'; params.append(period_end)
                    q += ' ORDER BY "Неделя" DESC LIMIT %s'
                    params.append(limit)
                    cur.execute(q, params)

                    for row in cur.fetchall():
                        week, plan, fact, transfers, dev, pct = row
                        plan_s = f"{plan:,.0f}".replace(",", " ") if plan else "0"
                        fact_s = f"{fact:,.0f}".replace(",", " ") if fact else "0"
                        pct_s = f"{pct:.1f}%" if pct is not None else "?"
                        content = (
                            f"Неделя {week}: план {plan_s}, факт {fact_s}, "
                            f"передачи ФМ {transfers}, отклонение {dev}, выполнение {pct_s}"
                        )
                        results.append({
                            "source": "1С: ПЛАН-ФАКТ ПО НЕДЕЛЯМ",
                            "date": week.strftime("%d.%m.%Y") if week else "",
                            "content": content,
                            "type": "analytics_plan_vs_fact",
                        })
                except Exception as e:
                    logger.debug(f"plan_vs_fact: {e}")

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

def _fixate_answer_as_source_chunk(question: str, answer: str,
                                     evidence_items: list, meta: dict) -> None:
    """
    Сохраняет удачный RAG-ответ в source_documents + source_chunks (Qwen3).
    Критерии:
    - evaluator признал ответ хорошим
    - retry_count ≤ 1 (одно escalation — ещё ок)
    - в evidence есть хотя бы 1 источник из "1С:..." (иначе менее надёжно)
    - этот вопрос ещё не зафиксирован (dedup по хэшу вопроса)

    Source_chunk попадает в search_source_chunks на следующем запросе и
    retrieval подхватит его через Qwen3 cosine.
    """
    try:
        evaluator = meta.get("evaluator") or {}
        if not evaluator.get("good", False):
            return
        if meta.get("retry_count", 0) > 1:
            return
        has_1c = any("1С" in (ev.get("source", "") or "") for ev in (evidence_items or []))
        if not has_1c:
            return
        q_norm = (question or "").strip()
        if len(q_norm) < 15 or len(q_norm) > 500:
            return

        import hashlib
        q_hash = hashlib.md5(q_norm.lower().encode()).hexdigest()[:16]
        source_ref = f"rag:{q_hash}"

        # Очистка ответа от служебных блоков (карта evidence, внешние ссылки)
        clean_answer = re.sub(r"\n+📎 Источники.*?(?=\n\n|\Z)", "", answer, flags=re.DOTALL)
        clean_answer = re.sub(r"\n+🌐 Внешние ссылки.*?(?=\n\n|\Z)", "", clean_answer, flags=re.DOTALL)
        clean_answer = clean_answer.strip()
        if len(clean_answer) > 2500:
            clean_answer = clean_answer[:2500]

        body = f"Вопрос: {q_norm}\n\nОтвет: {clean_answer}"

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Dedup: не создаём второй раз тот же вопрос
                cur.execute(
                    "SELECT id FROM source_documents WHERE source_ref = %s LIMIT 1",
                    (source_ref,),
                )
                existing = cur.fetchone()
                if existing:
                    return

                cur.execute(
                    """
                    INSERT INTO source_documents
                      (source_kind, source_ref, title, body_text, doc_date,
                       language, is_deleted, confidence, meta, created_at, updated_at)
                    VALUES
                      ('rag_answer', %s, %s, %s, now()::date, 'ru', false, 0.95,
                       %s::jsonb, now(), now())
                    RETURNING id
                    """,
                    (
                        source_ref,
                        q_norm[:200],
                        body,
                        json.dumps({
                            "retry_count": meta.get("retry_count", 0),
                            "model": meta.get("model_used"),
                            "evidence_count": len(evidence_items or []),
                        }, ensure_ascii=False),
                    ),
                )
                doc_id = cur.fetchone()[0]

                try:
                    from chunkers.embedder import embed_document_v2
                    emb = embed_document_v2(body[:2000])
                except Exception as e:
                    logger.warning(f"fixate: embed_document_v2 failed: {e}")
                    conn.rollback()
                    return
                if not emb:
                    conn.rollback()
                    return
                emb_str = "[" + ",".join(str(x) for x in emb) + "]"

                cur.execute(
                    """
                    INSERT INTO source_chunks
                      (document_id, chunk_no, chunk_text, embedding_v2,
                       chunk_type, source_kind, confidence, importance_score, created_at)
                    VALUES (%s, 0, %s, %s::vector, 'rag_answer', 'rag_answer', 0.95, 0.9, now())
                    """,
                    (doc_id, body[:2000], emb_str),
                )
                conn.commit()
                logger.info(f"RAG answer fixated: doc_id={doc_id}, ref={source_ref}")
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"fixate_answer failed: {e}")


def _call_answerer(prompt: str, model: str = "openai/gpt-4.1",
                    max_tokens: int = 1800, timeout: int = 60) -> str:
    """Один вызов LLM для генерации ответа. Raises RuntimeError на ошибке."""
    response = requests.post(
        f"{ROUTERAI_BASE_URL}/chat/completions",
        headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0,
        },
        timeout=timeout,
    )
    result = response.json()
    if "choices" not in result:
        raise RuntimeError(f"answerer {model}: {result.get('error', result)}")
    return result["choices"][0]["message"]["content"]


def evaluate_answer_quality(question: str, answer: str, evidence_items: list) -> dict:
    """
    Post-answer critique на gpt-4.1. Проверяет наличие прямого ответа, 1С-источников
    для количественных вопросов, корректность цитирований.
    Возвращает {"good": bool, "reasoning": str, "issues": [str]}.
    """
    if not ROUTERAI_API_KEY or not answer or len(answer) < 20:
        return {"good": True, "reasoning": "evaluator skipped", "issues": []}

    sources_short = "\n".join(
        f"[{i.get('evidence_id','?')}] {i.get('citation','')}"
        for i in evidence_items[:12]
    )
    has_1c_evidence = any(
        (i.get("source", "") or "").startswith("1С") for i in evidence_items
    )

    prompt = f"""Оцени качество ответа RAG-агента по бизнес-вопросу.

ВОПРОС: {question}

ОТВЕТ АГЕНТА:
{answer[:3000]}

ИСТОЧНИКИ EVIDENCE ({len(evidence_items)} шт, 1С-данные доступны: {has_1c_evidence}):
{sources_short}

КРИТЕРИИ ХОРОШЕГО ОТВЕТА:
1) Есть прямой ответ (цифра/имя/факт/дата), не "недостаточно данных" на основной вопрос
2) Если вопрос количественный (сколько/объём/остаток/сумма/топ/выручка) — ДОЛЖЕН быть хотя бы 1 источник из "1С:..."
3) Каждый ключевой тезис имеет ссылку [n]
4) Отвечает по-человечески, а не сухо перечисляет
5) "Риски/пробелы" не написаны про мелочи (ед.изм. если они ясны из контекста, детализация не спрошенного)

Верни ТОЛЬКО JSON без markdown:
{{"good": true|false, "reasoning": "краткое объяснение оценки", "issues": ["список конкретных проблем если есть"]}}"""

    try:
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "openai/gpt-4.1",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 400,
                "temperature": 0,
            },
            timeout=30,
        )
        result = response.json()
        if "choices" not in result:
            logger.warning(f"evaluate_answer_quality: {result}")
            return {"good": True, "reasoning": "evaluator http error", "issues": []}
        content = result["choices"][0]["message"]["content"].strip()
        content = re.sub(r'^```(?:json)?\s*', '', content)
        content = re.sub(r'\s*```$', '', content)
        parsed = json.loads(content)
        logger.info(
            f"AnswerEval: good={parsed.get('good')}, "
            f"reason={(parsed.get('reasoning') or '')[:100]}"
        )
        return parsed
    except Exception as e:
        logger.warning(f"evaluate_answer_quality error: {e}")
        return {"good": True, "reasoning": f"evaluator exc: {e}", "issues": []}


def generate_response(question, db_results, web_results, web_citations=None, chat_context=""):
    """Генерация grounded-ответа с обязательными ссылками на evidence.
    Возвращает tuple (text, meta) где meta = {retry_count, model_used, evaluator}.
    """
    meta = {"retry_count": 0, "model_used": "openai/gpt-4.1", "evaluator": {}}
    if not ROUTERAI_API_KEY:
        return "API ключ не настроен", meta
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
{(chat_context + chr(10) + chr(10)) if chat_context else ''}
ВОПРОС: {question}

ДОКАЗАТЕЛЬСТВА (evidence):
{evidence_context if evidence_context else "Нет релевантных доказательств."}

ДОП. ДАННЫЕ ИЗ ИНТЕРНЕТА:
{(web_results or "")[:1500]}

ПРАВИЛА:
1) Используй только факты из evidence. Не придумывай. Каждый тезис — со ссылкой [n].
2) Отвечай как живой коллега, а не как базой данных. Прямо и конкретно.
3) Единицы измерения: в evidence от 1С обычно уже указаны единицы (кг, шт, руб).
   Если в тексте написано "10 000 кг" — так и пиши в ответе. Не спрашивай "что значит
   ед.", если единица явно указана в evidence. Для сырья по умолчанию кг, для готовой
   продукции — штуки (если не указано иное — можно предположить, но отметить это).
4) "Недостаточно данных" пиши ТОЛЬКО если не хватает основного ответа на вопрос.
   Не выводи как риск уточнения, которые не меняют ответа (детализация поставщиков,
   возможные корректировки, сроки годности — только если спросили).
5) Конкретика > общие формулировки: суммы, даты, документы, имена.
6) Если вопрос про свежие события — приоритет самым новым доказательствам.
7) Если в evidence есть цифра-ответ и ед.изм. — сформулируй по-человечески:
   "Купили 10 тонн муки на 255 тыс ₽ — один раз, 15 февраля" лучше чем
   "Было закуплено 10000 ед. муки на 255000 руб."

ФОРМАТ ОТВЕТА:
- Краткий ответ (1-2 предложения, по-человечески, с главной цифрой/фактом и ссылкой [n])
- Детали (маркированный список только если есть что добавить к краткому ответу; каждый пункт [n])
- Риски/пробелы (ТОЛЬКО если реально не хватает чего-то важного для ответа)
- Источники (список [n] — краткое описание)

СПИСОК ИСТОЧНИКОВ:
{sources_map if sources_map else "Нет"}

Ответ:"""

        # Попытка 1: gpt-4.1
        try:
            response_text = _call_answerer(prompt, "openai/gpt-4.1", max_tokens=1800, timeout=60)
            meta["model_used"] = "openai/gpt-4.1"
        except Exception as e:
            logger.error(f"answerer gpt-4.1: {e}")
            return f"Ошибка генерации: {e}", meta

        # Post-answer evaluator → escalation
        quality = evaluate_answer_quality(question, response_text, evidence_items)
        meta["evaluator"] = quality
        if not quality.get("good", True):
            issues = quality.get("issues") or [quality.get("reasoning", "")]
            logger.info(f"Escalation: answer weak ({issues[:2]}), retry with Claude Opus 4.7")
            escalate_prompt = (
                prompt
                + "\n\n⚠️ ЗАМЕЧАНИЯ к предыдущему ответу (исправь их):\n- "
                + "\n- ".join(str(i)[:200] for i in issues[:5])
                + "\nПерепиши ответ с учётом этих замечаний. Тот же формат, те же источники [n]."
            )
            try:
                escalated = _call_answerer(
                    escalate_prompt, "anthropic/claude-opus-4.7",
                    max_tokens=3000, timeout=180
                )
                response_text = escalated
                meta["model_used"] = "anthropic/claude-opus-4.7"
                meta["retry_count"] = 1
            except Exception as e:
                logger.warning(f"Claude Opus escalation failed: {e} — keeping gpt-4.1 answer")

        # Гарантированно добавляем карту источников внизу, даже если LLM её не вывел
        if sources_map:
            response_text += "\n\n📎 Источники (карта evidence):\n" + sources_map

        if web_citations:
            response_text += "\n\n🌐 Внешние ссылки:"
            for i, url in enumerate(web_citations[:5], 1):
                response_text += f"\n{i}. {url}"

        return response_text, meta
    except Exception as e:
        return f"Ошибка: {e}", meta


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
- 1С_ANALYTICS: агрегированные SQL-запросы к 1С (суммы, количества, остатки, план/факт). Для вопросов "сколько/объём/выручка/остаток/итого/топ ...".
- 1С_SEARCH: поиск конкретных документов в 1С (отдельный заказ, цена, закупочный счёт). Для КОНКРЕТНЫХ записей по ключевым словам.
- CHATS: переписка сотрудников в Telegram.
- EMAIL: деловая переписка по почте.
- WEB: интернет-поиск (только внешняя информация).
- KNOWLEDGE: база знаний компании (факты, решения, задачи, политики). Для вопросов про правила, процессы, решения, кто за что отвечает, что было решено/сделано.

ТИПЫ АНАЛИТИКИ (analytics_type для 1С_ANALYTICS):
- top_clients / sales_summary — топ клиентов по выручке за период
- top_products — топ продукции по выручке
- top_suppliers / purchase_summary — топ поставщиков по суммам
- production_summary — сводка производства (без фильтра)
- purchases_by_nomenclature — СКОЛЬКО куплено номенклатуры X за период (mart_purchases; keyword фильтр по названию номенклатуры). Для "сколько муки/сахара/упаковки купили в феврале".
- sales_by_nomenclature — СКОЛЬКО продано номенклатуры X за период (mart_sales). Для "сколько Медовика продали в марте".
- stock_balance — текущие ОСТАТКИ номенклатуры X на складах. Для "остатки муки", "сколько сахара на складе".
- production_by_nomenclature — СКОЛЬКО произведено номенклатуры X за период (mart_production). Для "сколько тортов Медовик произвели в феврале".
- plan_vs_fact — недельный план/факт (v_plan_fact_weekly). Для "план/факт за март", "выполнение плана".
- custom_sql — универсальный text-to-SQL через Claude Opus 4.7 (дорого, медленно). Использовать ТОЛЬКО когда никакой из *_by_nomenclature/top_*/stock_balance/plan_vs_fact не подходит. Типичные кейсы: "средний чек клиента X", "динамика продаж помесячно за год", "маржинальность SKU", "сравнение двух периодов", "расход vs закупки по категориям", агрегаты по 2+ сущностям одновременно. В keywords клади полную формулировку вопроса на русском (не ключевые слова).

ВАЖНО: если в вопросе есть конкретное название товара/сырья И слова количества/суммы/остатка — используй *_by_nomenclature или stock_balance с keywords = название номенклатуры.

ВОПРОС: {question}

ЗАДАЧА: Проанализируй вопрос и определи:
1. Какие КОНКРЕТНЫЕ чаты из списка выше наиболее релевантны (по названию)
2. Какие источники данных нужны
3. Какие ключевые слова использовать для поиска

ЭТАЛОННЫЕ ПРИМЕРЫ (изучи и следуй этим паттернам):

# Количественные вопросы по номенклатуре (товар + период + количество/сумма)
Q: "Сколько муки мы купили в феврале 2026?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"purchases_by_nomenclature","keywords":"мука"}}],"entities":{{"products":["мука"]}},"period":"february"}}

Q: "Сколько сахара закупили за последний месяц?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"purchases_by_nomenclature","keywords":"сахар"}}],"entities":{{"products":["сахар"]}},"period":"month"}}

Q: "Сколько тортов Медовик произвели в марте?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"production_by_nomenclature","keywords":"медовик"}}],"entities":{{"products":["медовик"]}},"period":"march"}}

Q: "Сколько Наполеона продали в первом квартале?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"sales_by_nomenclature","keywords":"наполеон"}}],"entities":{{"products":["наполеон"]}},"period":"quarter"}}

Q: "Выручка по пирожным за апрель"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"sales_by_nomenclature","keywords":"пирожн"}}],"period":"april"}}

# Остатки (текущее состояние склада)
Q: "Остатки сахара на складе?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"stock_balance","keywords":"сахар"}}],"entities":{{"products":["сахар"]}}}}

Q: "Сколько муки на складе СЫРЬЯ?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"stock_balance","keywords":"мука"}}],"entities":{{"products":["мука"],"warehouses":["СЫРЬЯ"]}}}}

Q: "Остатки упаковки по складу УПАКОВКИ"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"stock_balance","keywords":"упаковк"}}],"entities":{{"products":["упаковка"],"warehouses":["УПАКОВКИ"]}}}}

# С конкретным клиентом или поставщиком
Q: "Что мы продали клиенту Дикси за 4 квартал 2025?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"sales_by_nomenclature","keywords":""}}],"entities":{{"clients":["Дикси"]}},"period":"q4_2025"}}

Q: "Что купили у ИП Кутабаевой в марте?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"purchases_by_nomenclature","keywords":""}}],"entities":{{"suppliers":["Кутабаева"]}},"period":"march"}}

# Топы
Q: "Топ 5 поставщиков за март 2026 по сумме"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"top_suppliers","keywords":""}}],"period":"march"}}

Q: "Топ 10 клиентов за прошлый год"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"top_clients","keywords":""}}],"period":"year"}}

Q: "Самые продаваемые товары за квартал"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"top_products","keywords":""}}],"period":"quarter"}}

# План-факт
Q: "Выполнение плана за март"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"plan_vs_fact","keywords":""}}],"period":"march"}}

# Кастомный SQL (когда стандартные tools не подходят)
Q: "Средний чек продаж клиенту Магнит за 2 квартал 2025?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"custom_sql","keywords":"Средний чек продаж клиенту Магнит за 2 квартал 2025"}}],"entities":{{"clients":["Магнит"]}},"period":"q2_2025"}}

Q: "Какие SKU выросли в продажах больше всего между январём и мартом 2026?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"custom_sql","keywords":"Какие SKU выросли в продажах больше всего между январём и мартом 2026? Сравни sum_with_vat помесячно."}}],"period":"2026-01-01..2026-03-31"}}

# Конкретные документы
Q: "По какой цене покупали муку в последний раз?"
A: {{"query_type":"search","steps":[{{"source":"1С_SEARCH","keywords":"мука"}}],"entities":{{"products":["мука"]}},"period":"2weeks"}}

# Чаты / переписка / процесс / решение
Q: "Как решили вопрос по доп соглашению с Магнит?"
A: {{"query_type":"chat_search","steps":[{{"source":"CHATS","keywords":"магнит доп соглашение"}},{{"source":"EMAIL","keywords":"магнит соглашение"}},{{"source":"KNOWLEDGE","keywords":"магнит соглашение"}}],"entities":{{"clients":["Магнит"]}}}}

Q: "Что в бухгалтерии обсуждали про НДС?"
A: {{"query_type":"chat_search","target_chats":["tg_chat_1003492830147_buhgalteriya_frumelad_nf","tg_chat_apriori_frumelad_nf"],"steps":[{{"source":"CHATS","keywords":"ндс"}}]}}

Q: "Кто отвечает за качество упаковки?"
A: {{"query_type":"lookup","steps":[{{"source":"KNOWLEDGE","keywords":"упаковка ответственный"}}]}}

# Смешанный (количество + обсуждения)
Q: "Сколько муки купили в феврале и что обсуждали по поставщикам?"
A: {{"query_type":"mixed","steps":[{{"source":"1С_ANALYTICS","analytics_type":"purchases_by_nomenclature","keywords":"мука"}},{{"source":"CHATS","keywords":"мука поставщик"}}],"entities":{{"products":["мука"]}},"period":"february"}}

# Дайджест дня/периода — "что обсуждали/происходило" без конкретной темы → CHATS + EMAIL
Q: "Что вчера обсуждали в компании?"
A: {{"query_type":"chat_search","steps":[{{"source":"CHATS","keywords":""}},{{"source":"EMAIL","keywords":""}}],"period":"yesterday"}}

Q: "Что сегодня происходит?"
A: {{"query_type":"chat_search","steps":[{{"source":"CHATS","keywords":""}},{{"source":"EMAIL","keywords":""}}],"period":"today"}}

Q: "Что обсуждали на этой неделе?"
A: {{"query_type":"chat_search","steps":[{{"source":"CHATS","keywords":""}},{{"source":"EMAIL","keywords":""}}],"period":"week"}}

# "Когда впервые" / "первое упоминание" — ищем САМОЕ РАННЕЕ (обратная хронология)
Q: "Когда впервые упоминается меренга в переписке с ВкусВиллом?"
A: {{"query_type":"chat_search","steps":[{{"source":"EMAIL","keywords":"меренга ВкусВилл"}}],"entities":{{"clients":["ВкусВилл"],"products":["меренга"]}}}}

Q: "С каких пор мы работаем с Магнитом?"
A: {{"query_type":"chat_search","steps":[{{"source":"EMAIL","keywords":"Магнит"}},{{"source":"CHATS","keywords":"Магнит"}}],"entities":{{"clients":["Магнит"]}}}}

# Поиск КОНТАКТОВ (email-адресов + ФИО) клиента/поставщика — ЧЕРЕЗ custom_sql.
# Обычный retrieval не найдёт ни список адресов, ни ФИО из подписей.
Q: "С какими контактами ВкусВилл велась переписка?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"custom_sql","keywords":"Найди все уникальные email-адреса с домена vkusvill, с которыми велась переписка. SELECT DISTINCT email-адрес, COUNT писем, диапазон дат. Искать в from_address, to_addresses, cc_addresses (ILIKE %vkusvill%). Исключи служебные адреса (noreply, no-reply, service, svc_, edi@, pretenz, accounting)."}}],"entities":{{"clients":["ВкусВилл"]}}}}

Q: "Кто у Магнита наш основной контакт / байер / категорийщик?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"custom_sql","keywords":"Найди ФИО и должности КЛИЕНТСКИХ контактов Магнита (НЕ служебных адресов). Включи from_address, subject и body_text в SELECT для последних 20 писем от персональных адресов magnit.ru (исключи noreply/no-reply/svc_/edi@/pretenz/accounting). Ответу нужен не только список адресов, но и ФИО из подписей в body_text (обычно после 'С уважением,'). SELECT from_address, subject, LEFT(body_text, 1500) FROM email_messages WHERE from_address ILIKE '%magnit%' AND from_address NOT ILIKE '%noreply%' AND from_address NOT ILIKE '%no-reply%' AND from_address NOT ILIKE '%svc_%' AND from_address NOT ILIKE '%edi@%' AND from_address NOT ILIKE '%service%' ORDER BY received_at DESC LIMIT 30."}}],"entities":{{"clients":["Магнит"]}}}}

Q: "С кем из Дикси переписывались в 2025 году?"
A: {{"query_type":"analytics","steps":[{{"source":"1С_ANALYTICS","analytics_type":"custom_sql","keywords":"Найди ФИО контактных лиц Дикси за 2025 год (январь-декабрь). Читай body_text для извлечения подписей. SELECT from_address, subject, LEFT(body_text, 1500) FROM email_messages WHERE (from_address ILIKE '%dixy%' OR from_address ILIKE '%dicy%') AND received_at BETWEEN '2025-01-01' AND '2025-12-31' AND from_address NOT ILIKE '%noreply%' AND from_address NOT ILIKE '%no-reply%' AND from_address NOT ILIKE '%svc_%' AND from_address NOT ILIKE '%edi@%' ORDER BY received_at DESC LIMIT 30. В ответе верни список ФИО с должностями и email."}}],"entities":{{"clients":["Дикси"]}},"period":"2025-01-01..2025-12-31"}}

КРИТИЧНО:
- Если в вопросе есть название товара + "сколько/объём/купили/продали/произвели" →
  1С_ANALYTICS с *_by_nomenclature (НЕ 1С_SEARCH, НЕ только CHATS)
- Если "остатки"/"остаток"/"запас" → stock_balance
- Если "топ N"/"лучшие"/"самые" → top_*
- Если есть название клиента (ИП, ООО, название сети) → entities.clients + sales_*
- Если есть название поставщика → entities.suppliers + purchases_*
- Если есть название склада → entities.warehouses + stock_balance
- Если "когда впервые"/"первое упоминание"/"когда началось"/"с каких пор"/"самое раннее" →
  EMAIL + CHATS (retrieval автоматически перейдёт в режим ASC-сортировки)
- Если "что обсуждали/происходило/было вчера/сегодня/на этой неделе" (дайджест без конкретной темы) →
  ОБЯЗАТЕЛЬНО CHATS + EMAIL, period = yesterday/today/week
- CHATS/EMAIL — ТОЛЬКО когда вопрос о переписке/решении/обсуждении

Верни ТОЛЬКО JSON без markdown:
{{"query_type": "analytics|search|lookup|chat_search|web|mixed",
"reasoning": "краткое объяснение логики выбора",
"target_chats": ["tg_chat_xxx", "tg_chat_yyy"],
"steps": [{{"source": "1С_ANALYTICS|1С_SEARCH|CHATS|EMAIL|WEB", "action": "описание", "analytics_type": "тип|null", "keywords": "слова через пробел"}}],
"entities": {{"clients": [], "products": [], "suppliers": []}},
"period": "today|yesterday|week|2weeks|month|quarter|half_year|year|january..december|q1_2025|q2_2025|q3_2025|q4_2025|q1_2026|january_2025|YYYY-MM-DD..YYYY-MM-DD|null",
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
- period: "за 2 недели" = "2weeks", "в январе" = "january" (если без года, подразумевается ближайший прошедший), "недавно"/"в последний раз" = "2weeks"
- period с КОНКРЕТНЫМ годом: "в январе 2025" = "january_2025", "4 квартал 25" = "q4_2025", "Q2 2026" = "q2_2026"
- если период не шаблонный — возвращай диапазон YYYY-MM-DD..YYYY-MM-DD напрямую, напр. "с 1 июня по 15 июля 2025" = "2025-06-01..2025-07-15"
"""
        
        # 3 попытки с exponential backoff при таймауте/5xx
        response = None
        last_err = None
        for attempt in range(3):
            try:
                response = requests.post(
                    f"{ROUTERAI_BASE_URL}/chat/completions",
                    headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
                    json={
                        "model": "openai/gpt-4.1",
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 2000,
                        "temperature": 0,
                    },
                    timeout=(5, 45),
                )
                if response.status_code >= 500:
                    last_err = f"HTTP {response.status_code}"
                    time.sleep(2 ** attempt)  # 1s, 2s, 4s
                    continue
                break
            except (requests.Timeout, requests.ConnectionError) as e:
                last_err = str(e)
                logger.warning(f"Router attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** attempt)
        if response is None or response.status_code >= 500:
            logger.error(f"Router failed after 3 attempts: {last_err}")
            return _default_plan(question)

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

async def process_rag_query(question, chat_context="", user_info: dict = None,
                              prev_context: dict = None, meta_out: dict = None):
    """
    ReAct цикл обработки RAG-запроса:
    0. Если prev_context (reply-chain) — встраиваем предыдущий Q/A в chat_context
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

    # === Шаг 0: Reply-chain контекст (поддержка deep chain до корня) ===
    if prev_context and isinstance(prev_context, dict):
        # Собираем нормализованную цепочку Q/A (от старых к новым)
        chain = prev_context.get("chain")
        if chain and isinstance(chain, list):
            pairs = [(p.get("question") or "", p.get("answer") or "") for p in chain if p]
        else:
            # Обратная совместимость: старый формат {"question","answer"}
            pairs = [(
                prev_context.get("question") or "",
                prev_context.get("answer") or "",
            )]
        pairs = [(q.strip()[:500], a.strip()[:1200]) for q, a in pairs if q.strip() and a.strip()]

        if pairs:
            lines = ["[FOLLOW-UP CHAIN] (от старого к новому — текущий вопрос продолжает тему)"]
            for i, (q, a) in enumerate(pairs, 1):
                lines.append(f"Q{i}: {q}")
                lines.append(f"A{i}: {a}")
            lines.append("[/FOLLOW-UP CHAIN]")
            chat_context = (
                (chat_context + "\n\n" if chat_context else "") + "\n".join(lines)
            )
            logger.info(
                f"Follow-up chain depth={len(pairs)}, root Q: '{pairs[0][0][:60]}'"
            )

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
    # Если Router явно вернул period — дополняем/уточняем time_context его данными.
    # extract_time_context парсит текст и может промахнуться (напр. "вчера" → wrong range),
    # а Router видит семантику запроса точнее.
    _router_period = plan.get("period")
    if _router_period and _router_period not in (None, "null") and period_date:
        if _router_period == "yesterday":
            d = period_date  # date object
            time_context["date_from"] = datetime(d.year, d.month, d.day, 0, 0, 0)
            time_context["date_to"] = datetime(d.year, d.month, d.day, 23, 59, 59)
            time_context["has_time_filter"] = True
            time_context["decay_days"] = 2
            time_context["freshness_weight"] = 0.5
        elif _router_period == "today":
            d = period_date
            time_context["date_from"] = datetime(d.year, d.month, d.day, 0, 0, 0)
            time_context["date_to"] = datetime.now()
            time_context["has_time_filter"] = True
            time_context["decay_days"] = 1
            time_context["freshness_weight"] = 0.5
        elif not time_context["has_time_filter"]:
            # Для остальных периодов — подставляем если extract_time_context ничего не нашёл
            time_context["date_from"] = datetime(period_date.year, period_date.month, period_date.day)
            if period_end:
                time_context["date_to"] = datetime(period_end.year, period_end.month, period_end.day, 23, 59, 59)
            time_context["has_time_filter"] = True
    if time_context["has_time_filter"]:
        logger.info(f"Временной контекст: decay_days={time_context['decay_days']}, "
                    f"date_from={time_context.get('date_from')}, date_to={time_context.get('date_to')}")
    
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
            if os.getenv("USE_EMBEDDING_V2", "false").lower() == "true":
                results = search_unified(step_keywords, limit=30)
            else:
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

    # === Шаг 5: Генерация ответа (gpt-4.1 → escalate Claude Opus 4.7 если слабо) ===
    gen_start = time.time()
    response, gen_meta = generate_response(
        question, evidence_results, web_results, web_citations, chat_context
    )
    generation_time_ms = int((time.time() - gen_start) * 1000)

    answer_eval = gen_meta.get("evaluator") or {}

    # === Фиксация ответа в source_chunks (Фаза 4.1) ===
    # Ранний retrieval на повторных вопросах — через Qwen3 HNSW.
    try:
        _fixate_answer_as_source_chunk(question, response, evidence_results, gen_meta)
    except Exception as e:
        logger.warning(f"fixate wrapper: {e}")

    # === Логирование ===
    log_id = _log_rag_query({
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
        "retry_count": gen_meta.get("retry_count", 0),
        "rerank_applied": len(db_results) > 12,
        "response_length": len(response),
        "response_time_ms": int((time.time() - start_time) * 1000),
        "router_time_ms": router_time_ms,
        "search_time_ms": search_time_ms,
        "generation_time_ms": generation_time_ms,
        "web_search_used": bool(web_results),
        "error": None,
        "answer_model": gen_meta.get("model_used"),
        "answer_retry_count": gen_meta.get("retry_count", 0),
        "answer_eval_good": answer_eval.get("good") if answer_eval else None,
        "answer_eval_issues": "; ".join(answer_eval.get("issues", [])[:5]) if answer_eval else None,
    })

    if meta_out is not None and log_id is not None:
        meta_out["log_id"] = log_id

    return response


def _log_rag_query(data: dict):
    """Записывает RAG-запрос в лог, возвращает новый id (или None)."""
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
                    generation_time_ms, web_search_used, error,
                    answer_model, answer_retry_count, answer_eval_good, answer_eval_issues
                ) VALUES (
                    %(user_id)s, %(username)s, %(first_name)s, %(chat_id)s, %(chat_type)s,
                    %(question)s, %(primary_intent)s, %(detected_intents)s,
                    %(router_query_type)s, %(router_target_chats)s, %(sources_used)s,
                    %(evidence_count)s, %(evidence_sources)s, %(evaluator_sufficient)s,
                    %(retry_count)s, %(rerank_applied)s, %(response_length)s,
                    %(response_time_ms)s, %(router_time_ms)s, %(search_time_ms)s,
                    %(generation_time_ms)s, %(web_search_used)s, %(error)s,
                    %(answer_model)s, %(answer_retry_count)s, %(answer_eval_good)s, %(answer_eval_issues)s
                ) RETURNING id
            """, data)
            row = cur.fetchone()
        conn.commit()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.warning(f"RAG log error: {e}")
        return None


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
