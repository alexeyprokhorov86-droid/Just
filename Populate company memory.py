#!/usr/bin/env python3
"""
Скрипт первичного наполнения agent_memory знаниями о компании.
Запускается один раз на VPS.

Использование:
    python populate_company_memory.py
"""

import psycopg2
import os
from datetime import datetime

# === НАСТРОЙКИ БД ===
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "knowledge_base"),
    "user": os.getenv("DB_USER", "knowledge"),
    "password": os.getenv("DB_PASSWORD", "ProhKnowledge2024"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def save_fact(cur, category, subject, fact, source="auto_populate"):
    """Сохраняет факт в agent_memory, если похожего нет."""
    cur.execute("""
        SELECT id FROM agent_memory
        WHERE subject = %s AND fact = %s AND is_active = true
        LIMIT 1
    """, (subject, fact))
    if cur.fetchone():
        return False  # уже есть
    cur.execute("""
        INSERT INTO agent_memory (category, subject, fact, source, confidence)
        VALUES (%s, %s, %s, %s, 1.0)
    """, (category, subject, fact, source))
    return True


def populate_static_facts(cur):
    """Базовые факты о компании, которые невозможно извлечь из БД."""
    facts = [
        # === СТРУКТУРА КОМПАНИИ ===
        ("компания", "Фрумелад", 
         "Группа компаний состоит из двух юрлиц: ООО 'Фрумелад' (ФР) и ООО 'НФ'. "
         "НФ занимается только производством и продаёт ВСЮ продукцию через ФР. "
         "ФР владеет торговыми марками и занимается продажей. "
         "ФР содержит административный персонал: HR, бухгалтерия, реклама, операционный директорат. "
         "Доставка продукции клиентам — на ФР (статья ДДС 'Доставка до клиента')."),
        
        ("компания", "ИП Прохоров",
         "ИП Прохоров — используется для вывода наличных средств."),
        
        ("компания", "Форма Мода",
         "Компания 'Форма Мода' числится среди поставщиков как поставщик упаковки. "
         "По факту через неё получаются наличные за минусом комиссии для выплаты зарплат или дивидендов."),

        ("компания", "Сотрудники",
         "Общее количество сотрудников — около 80 человек. "
         "Из них ~20 в администрации, остальные ~60 — контур трудозатрат "
         "(непосредственно выпускают готовую продукцию)."),
        
        # === КЛЮЧЕВЫЕ ЛЮДИ ===
        ("персонал", "Алексей Прохоров",
         "Основатель и владелец компании. Самый главный человек, знает всё о бизнесе. "
         "Принимает стратегические решения, занимается развитием новых продуктов и направлений. "
         "Telegram user_id: 805598873. Email: alexey@lacannelle.ru"),
        
        ("персонал", "Ирина Прохорова",
         "Жена Алексея. Официально — Генеральный директор в ФР и НФ. "
         "По факту отвечает за текущие продажи. "
         "Не отвечает за стратегию или новые продукты, но участвует в их создании. "
         "Email: irina@lacannelle.ru"),
        
        # === БРЕНДЫ ===
        ("бренд", "Кондитерская Прохорова",
         "Основной и старейший бренд компании. Раньше был единственным. "
         "Под этим брендом выпускается большинство продукции: торты, пирожные."),
        
        ("бренд", "Птичка Благородная",
         "Один из брендов компании. Линейка суфлейных изделий."),
        
        ("бренд", "Пинкис",
         "Один из брендов компании."),
        
        ("бренд", "Майти Боллз",
         "Один из брендов компании (Mighty Bollz / MIGHTY BOLLZ DARK CHOCOLATE)."),
        
        ("бренд", "Протеин Лаб",
         "Один из брендов компании (Protein Lab)."),
        
        ("бренд", "ТимТим",
         "Один из брендов компании."),
        
        ("бренд", "La Cannelle",
         "Бывший бренд, сейчас не используется как торговая марка. "
         "Домен lacannelle.ru используется для части корпоративной почты."),
        
        # === ПРОДУКЦИЯ ===
        ("продукция", "Основная продукция",
         "Компания производит кондитерские изделия (торты, пирожные) для крупного сетевого ритейла. "
         "Главное отличие — длинный срок хранения (от 30 дней) в холодильных условиях (+4–8°С)."),
        
        ("продукция", "Топ-продукты",
         "Самые продаваемые позиции: "
         "Торт суфлейный 'Благородная птичка' 400г, "
         "Пирожное слоёное 'Медовик на гречишном мёду' 120г, "
         "Пирожное суфлейное 'Благородная Птичка' 70г, "
         "Пирожное 'Картошка' 70г, "
         "Торт слоёный Медовик 500г, "
         "Пирожное суфлейное 'Птичка Прохорова' 65г."),
        
        # === ВНЕШНИЕ ПАРТНЁРЫ ===
        ("партнёр", "BSG",
         "Внешняя IT-компания, занимается поддержкой и доработкой 1С. "
         "Основатель — Станислав (Стас) Березовский. Аббревиатура BSG — от его ФИО. "
         "Telegram-чаты: 'Фрумелад задачи на разработку BSG', 'Фрумелад поддержка BSG'."),
        
        ("партнёр", "KELIN",
         "Внешняя юридическая компания. "
         "Чат 'KELIN - ФНС' — обсуждение конфиденциальных вопросов (не для общего доступа). "
         "Чат 'KELIN - Кондитерская Прохорова' — текущие юридические вопросы."),
        
        # === ДОМЕНЫ И ПОЧТА ===
        ("инфраструктура", "Email домены",
         "Компания использует два email-домена: totsamiy.com (основной, ~70 ящиков) "
         "и lacannelle.ru (~11 ящиков). Всего 81 почтовый ящик. "
         "Ящики распределены по функциям: sale@, zakupki@, tehnolog@, sklad@, hr@, glavbuh@ и т.д."),
        
        ("инфраструктура", "Email мониторинг",
         "Все 81 почтовый ящик логируются в PostgreSQL (таблица email_messages). "
         "На февраль 2026 — более 205 000 проиндексированных писем. "
         "Эмбеддинги создаются через multilingual-e5-base для семантического поиска."),
        
        # === СТРАТЕГИЯ И ПРИОРИТЕТЫ ===
        ("стратегия", "Текущая ситуация",
         "Отрасль стагнирует, компания практически не растёт в основном направлении. "
         "Ведётся активная работа по поиску новых рынков и продуктов."),
        
        ("стратегия", "Новые направления",
         "1) Разработка продукции для HoReCa в заморозке. "
         "2) Расширение ассортимента для ритейла: новые вкусы (солёная карамель, манго-маракуйя, вишня). "
         "3) Цель — начать поставки во ВкусВилл (требуется качественная продукция с короткими сроками хранения)."),
        
        ("стратегия", "Кадровая проблема",
         "Острая проблема с подбором персонала: "
         "любого уровня на производство (трудозатраты), качественного — в администрацию. "
         "Решение — автоматизация процессов и применение ИИ."),
        
        # === ИНФРАСТРУКТУРА IT ===
        ("инфраструктура", "IT-системы",
         "ERP: 1С:КА 2.5 (Комплексная автоматизация). Поддержка и доработка — BSG. "
         "VPS: 95.174.92.209 (Ubuntu, Docker). "
         "Telegram-бот для логирования сообщений, анализа документов и RAG. "
         "PostgreSQL с pgvector для хранения данных и семантического поиска. "
         "Metabase для бизнес-аналитики и дашбордов. "
         "Синхронизация 1С через OData API."),
    ]
    
    count = 0
    for category, subject, fact in facts:
        if save_fact(cur, category, subject, fact, "manual_populate"):
            count += 1
    return count


def populate_suppliers_from_db(cur):
    """Извлекает топ поставщиков из purchase_prices (исключая внутренние компании)."""
    cur.execute("""
        SELECT contractor_name, COUNT(*) as purchases, 
               MAX(doc_date) as last_purchase,
               COUNT(DISTINCT nomenclature_name) as unique_items,
               SUM(sum_total) as total_sum
        FROM purchase_prices
        WHERE contractor_name IS NOT NULL AND contractor_name != ''
          AND contractor_name NOT ILIKE '%%ФРУМЕЛАД%%'
          AND contractor_name NOT ILIKE '%%ФОРМА МОДА%%'
          AND contractor_name NOT ILIKE '%%НФ ООО%%'
          AND contractor_name NOT ILIKE '%%Прохоров%%ИП%%'
        GROUP BY contractor_name
        ORDER BY purchases DESC
        LIMIT 30
    """)
    
    count = 0
    for row in cur.fetchall():
        name, purchases, last_date, unique_items, total_sum = row
        fact = (f"Поставщик с {purchases} позициями закупок. "
                f"Последняя закупка: {last_date}. "
                f"Уникальных товаров: {unique_items}. "
                f"Общая сумма: {total_sum:,.0f} руб." if total_sum else 
                f"Поставщик с {purchases} позициями закупок. Последняя закупка: {last_date}.")
        if save_fact(cur, "поставщик", name, fact, "auto_from_1c"):
            count += 1
    return count


def populate_clients_from_db(cur):
    """Извлекает топ клиентов из sales (исключая внутренние перемещения)."""
    cur.execute("""
        SELECT client_name, COUNT(*) as sales_count,
               MAX(doc_date) as last_sale,
               SUM(CASE WHEN doc_type = 'Реализация' THEN sum_with_vat ELSE 0 END) as total_revenue
        FROM sales
        WHERE client_name IS NOT NULL AND client_name != ''
          AND client_name NOT ILIKE '%%ФРУМЕЛАД%%'
          AND client_name NOT ILIKE '%%НФ ООО%%'
        GROUP BY client_name
        ORDER BY sales_count DESC
        LIMIT 30
    """)
    
    count = 0
    for row in cur.fetchall():
        name, sales_count, last_date, total_revenue = row
        
        # Добавляем контекст для ключевых клиентов
        extra = ""
        name_upper = name.upper() if name else ""
        if "ФРУМЕЛАД" in name_upper:
            extra = " (внутренние перемещения между ФР и НФ)"
        elif "ТАНДЕР" in name_upper or "МАГНИТ" in name_upper:
            extra = " (сеть Магнит)"
        elif "АГРОТОРГ" in name_upper or "X5" in name_upper or "ПЕРЕКРЁСТОК" in name_upper or "ПЕРЕКРЕСТОК" in name_upper:
            extra = " (X5 Group: Пятёрочка, Перекрёсток)"
        elif "МЕТРО" in name_upper:
            extra = " (METRO Cash & Carry)"
        elif "ДИКСИ" in name_upper:
            extra = " (сеть Дикси)"
        elif "OZON" in name_upper or "ОЗОН" in name_upper:
            extra = " (маркетплейс Ozon)"
        elif "ЛЕНТА" in name_upper:
            extra = " (сеть Лента)"
        
        fact = (f"Клиент с {sales_count} позициями продаж{extra}. "
                f"Последняя продажа: {last_date}.")
        if total_revenue and total_revenue > 0:
            fact += f" Общая выручка: {total_revenue:,.0f} руб."
        
        if save_fact(cur, "клиент", name, fact, "auto_from_1c"):
            count += 1
    return count


def populate_chats_from_db(cur):
    """Извлекает информацию о Telegram чатах."""
    cur.execute("""
        SELECT chat_id, chat_title, chat_type, total_messages, last_message_at
        FROM tg_chats_metadata
        WHERE chat_title IS NOT NULL
        ORDER BY last_message_at DESC NULLS LAST
    """)
    
    # Классификация чатов по назначению
    chat_categories = {
        "Производство": "производство, выпуск продукции",
        "Закупки": "закупки сырья и материалов",
        "Бухгалтерия": "бухгалтерский учёт, финансы",
        "R&D": "разработка новых продуктов, рецептуры",
        "HR": "кадры, подбор персонала",
        "Руководство": "стратегические вопросы, управление",
        "Дизайн": "дизайн упаковки",
        "Склад": "складская логистика",
        "Продажи": "продажи, отгрузки",
        "Торты Отгрузки": "отгрузки тортов, фото документов",
        "БЗ": "База Знаний — хранение инструкций и документов",
        "BSG": "задачи для IT-подрядчика BSG (поддержка 1С)",
        "KELIN": "юридические вопросы (внешняя юр.компания)",
        "Отчеты по аутсорсингу": "контроль аутсорсинговых работников",
        "Подбор Персонала": "внешний подбор персонала",
        "Новые продукты": "мониторинг конкурентов и новых продуктов",
        "Апримари": "взаимодействие с партнёром Апримари",
        "План": "планирование производства",
    }
    
    count = 0
    for row in cur.fetchall():
        chat_id, title, chat_type, total_msgs, last_msg = row
        if not title:
            continue
        
        # Определяем категорию
        purpose = "рабочий чат"
        for key, desc in chat_categories.items():
            if key.lower() in title.lower():
                purpose = desc
                break
        
        last_activity = f", последняя активность: {last_msg.strftime('%d.%m.%Y')}" if last_msg else ""
        
        fact = f"Telegram-чат '{title}' (ID: {chat_id}). Назначение: {purpose}{last_activity}."
        if save_fact(cur, "telegram_чат", title, fact, "auto_from_db"):
            count += 1
    return count


def populate_mailboxes_from_db(cur):
    """Извлекает информацию о почтовых ящиках."""
    cur.execute("SELECT id, email FROM monitored_mailboxes ORDER BY email")
    
    # Классификация ящиков по функциям
    mailbox_roles = {
        "sale": "продажи",
        "zakupki": "закупки",
        "zakaz": "заказы",
        "tehnolog": "технологи",
        "sklad": "склад",
        "glavbuh": "главный бухгалтер",
        "zambuh": "зам.главбуха",
        "accountant": "бухгалтерия",
        "hr": "кадры",
        "podbor": "подбор персонала",
        "cadri": "кадры",
        "chef": "шеф-повар/главный технолог",
        "directorprod": "директор по производству",
        "rukprod": "руководитель производства",
        "executive": "руководство",
        "operating": "операционный директор",
        "kachestvo": "контроль качества",
        "sb": "служба безопасности",
        "security": "безопасность",
        "it": "IT",
        "alexey": "Алексей Прохоров (основатель)",
        "irina": "Ирина Прохорова (ген.директор)",
        "business": "бизнес-вопросы",
        "prescription": "рецептуры",
        "proizvodstvo": "производство",
        "fasovka": "фасовка",
        "brigadir": "бригадир производства",
        "shiftsupervisor": "начальник смены",
        "controlling": "контроллинг",
        "factoring": "факторинг",
        "sverka": "сверки с контрагентами",
        "document": "документооборот",
        "office": "офис",
        "od": "операционный директор",
        "aho": "АХО (хозяйственное обеспечение)",
        "scan": "сканирование документов",
        "mm": "маркетинг",
        "education": "обучение",
        "1c-its": "1С:ИТС (поддержка 1С)",
        "bot": "бот (автоматические уведомления)",
        "noreply": "системные уведомления (не отвечать)",
        "postmaster": "администрирование почты",
    }
    
    count = 0
    for row in cur.fetchall():
        mb_id, email = row
        if not email:
            continue
        
        local_part = email.split("@")[0].lower()
        domain = email.split("@")[1] if "@" in email else ""
        
        role = "общего назначения"
        for key, desc in mailbox_roles.items():
            if local_part.startswith(key) or local_part == key:
                role = desc
                break
        
        fact = f"Почтовый ящик {email} (ID: {mb_id}). Функция: {role}. Домен: {domain}."
        if save_fact(cur, "email_ящик", email, fact, "auto_from_db"):
            count += 1
    return count


def generate_company_profile(cur):
    """
    Генерирует сводный Company Profile — текстовый блок,
    который будет вставляться в system prompt при каждом вызове LLM.
    Сохраняется как отдельная запись в agent_memory с category='company_profile'.
    """
    profile = """=== ПРОФИЛЬ КОМПАНИИ ФРУМЕЛАД ===

СТРУКТУРА: Группа из двух юрлиц — ООО «Фрумелад» (ФР, продажи и администрация) и ООО «НФ» (производство). НФ производит, ФР продаёт. ~80 сотрудников (~20 администрация, ~60 производство).

РУКОВОДСТВО: Алексей Прохоров — основатель и владелец. Ирина Прохорова — генеральный директор ФР и НФ, отвечает за текущие продажи.

БРЕНДЫ: Кондитерская Прохорова (основной), Птичка Благородная, Пинкис, Майти Боллз, Протеин Лаб, ТимТим.

ПРОДУКЦИЯ: Кондитерские изделия (торты, пирожные) для сетевого ритейла. Отличие — длинный срок хранения (30+ дней) при +4–8°С. Топ-продукты: Благородная птичка, Медовик, Картошка, Птичка Прохорова.

КЛИЕНТЫ: Тандер (Магнит), X5 (Агроторг, Перекрёсток), Милкбокс Ритейл, Фуд Майнз, Метро, Дикси, Ozon, Лента, Городской супермаркет, ТД Флагман.

КЛЮЧЕВЫЕ ПОСТАВЩИКИ: СОВЭКС ФУД (крупнейший), Ресурс Маркет, Импреторг, Карнов, Промсервис, Белый Город, Шоколандия, Оптиком.

ВНЕШНИЕ ПАРТНЁРЫ: BSG (Стас Березовский) — поддержка и доработка 1С. KELIN — юридическое сопровождение. Форма Мода — упаковка (формально).

ДОМЕНЫ: totsamiy.com (основной, ~70 ящиков), lacannelle.ru (дополнительный, ~11 ящиков). Всего 81 почтовый ящик.

IT-ИНФРАСТРУКТУРА: 1С:КА 2.5 (ERP), PostgreSQL + pgvector (данные и семантический поиск), Telegram-бот (логирование, анализ документов, RAG), Metabase (дашборды), VPS на Ubuntu с Docker.

ТЕКУЩИЕ ПРИОРИТЕТЫ:
1. Отрасль стагнирует → поиск новых рынков и продуктов
2. Разработка продукции для HoReCa в заморозке
3. Новые вкусы для ритейла: солёная карамель, манго-маракуйя, вишня
4. Цель — начать поставки во ВкусВилл (короткие сроки хранения)
5. Кадровая проблема → автоматизация и ИИ

=== КОНЕЦ ПРОФИЛЯ ==="""

    # Удаляем старый профиль если есть
    cur.execute("DELETE FROM agent_memory WHERE category = 'company_profile' AND subject = 'Company Profile'")
    
    cur.execute("""
        INSERT INTO agent_memory (category, subject, fact, source, confidence)
        VALUES ('company_profile', 'Company Profile', %s, 'auto_generate', 1.0)
    """, (profile,))
    
    return profile


def main():
    conn = get_conn()
    cur = conn.cursor()
    
    print("=" * 60)
    print("НАПОЛНЕНИЕ agent_memory ЗНАНИЯМИ О КОМПАНИИ")
    print("=" * 60)
    print()
    
    # 1. Статические факты
    print("1. Загрузка базовых фактов о компании...")
    n = populate_static_facts(cur)
    print(f"   ✅ Загружено: {n} фактов")
    
    # 2. Поставщики из 1С
    print("2. Извлечение поставщиков из 1С...")
    n = populate_suppliers_from_db(cur)
    print(f"   ✅ Загружено: {n} поставщиков")
    
    # 3. Клиенты из 1С
    print("3. Извлечение клиентов из 1С...")
    n = populate_clients_from_db(cur)
    print(f"   ✅ Загружено: {n} клиентов")
    
    # 4. Telegram чаты
    print("4. Извлечение Telegram чатов...")
    n = populate_chats_from_db(cur)
    print(f"   ✅ Загружено: {n} чатов")
    
    # 5. Почтовые ящики
    print("5. Извлечение почтовых ящиков...")
    n = populate_mailboxes_from_db(cur)
    print(f"   ✅ Загружено: {n} ящиков")
    
    # 6. Company Profile
    print("6. Генерация Company Profile...")
    profile = generate_company_profile(cur)
    print(f"   ✅ Профиль сгенерирован ({len(profile)} символов)")
    
    conn.commit()
    
    # Итоговая статистика
    cur.execute("SELECT category, COUNT(*) FROM agent_memory WHERE is_active = true GROUP BY category ORDER BY count DESC")
    stats = cur.fetchall()
    
    print()
    print("=" * 60)
    print("ИТОГО В agent_memory:")
    print("=" * 60)
    total = 0
    for cat, cnt in stats:
        print(f"  {cat}: {cnt}")
        total += cnt
    print(f"  --- ВСЕГО: {total} записей ---")
    
    cur.close()
    conn.close()
    print()
    print("✅ Готово! Теперь agent_memory содержит базовые знания о компании.")


if __name__ == "__main__":
    main()
