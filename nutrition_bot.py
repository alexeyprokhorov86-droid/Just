"""
Модуль запроса БЖУ у технологов через Telegram-бот.

Workflow:
1. fill_nutrition.py помечает позиции с low/medium confidence → nutrition_requests (status=pending)
2. Этот скрипт находит актуальных технологов из tg_user_roles
3. Отправляет запросы технологам в Telegram
4. Технолог отвечает: ввод данных / фото этикетки / пропустить / отложить / отклонить
5. Данные верифицируются LLM и записываются в 1С
"""
import json
import os
import re
import time
import logging
import base64
import requests
import psycopg2
from urllib.parse import quote
from dotenv import load_dotenv
from datetime import datetime

load_dotenv('/home/admin/telegram_logger_bot/.env')

# === Конфигурация из .env ===
BOT_TOKEN = os.environ["BOT_TOKEN"]
ODATA_BASE_URL = os.environ["ODATA_BASE_URL"].rstrip("/")
ODATA_USER = os.environ["ODATA_USERNAME"]
ODATA_PASS = os.environ["ODATA_PASSWORD"]
ROUTERAI_BASE_URL = os.environ["ROUTERAI_BASE_URL"]
ROUTERAI_API_KEY = os.environ["ROUTERAI_API_KEY"]
DB_HOST = os.environ.get("DB_HOST", "172.20.0.2")
DB_NAME = os.environ.get("DB_NAME", "knowledge_base")
DB_USER = os.environ.get("DB_USER", "knowledge")
DB_PASSWORD = os.environ["DB_PASSWORD"]
ADMIN_USER_ID = int(os.environ.get("TELEGRAM_ADMIN_ID", "0"))

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
from proxy_config import get_proxy_url
SOCKS_PROXY = os.environ.get("SOCKS_PROXY", get_proxy_url())

VERIFY_MODEL = "openai/gpt-5.4"
VISION_MODEL = "google/gemini-2.5-pro"

# Виды номенклатуры Сырья
SYRYE_TYPE_IDS = [
    '59718fc5-64a8-11eb-8106-005056a759ff',
    'e6fc1a75-64a0-11eb-8106-005056a759ff',
    '773e4cfa-179f-11ec-bf1d-000c29247c35',
    '7389a6bb-17af-11ec-bf1d-000c29247c35',
    'd503d3f5-ce32-11ed-8e18-000c299cc968',
    '97e5cba0-17af-11ec-bf1d-000c29247c35',
    'b63360b4-17af-11ec-bf1d-000c29247c35',
]

# Маппинг полей → Свойство_Key в 1С
NUTRITION_PROP_KEYS = {
    'protein': '89f344f6-8e2b-11f0-8e2c-000c299cc968',
    'fat': '6415fc48-8e55-11f0-8e2c-000c299cc968',
    'carbs': '72c01a93-8e2c-11f0-8e2c-000c299cc968',
    'sugar': '9f465a41-8e2c-11f0-8e2c-000c299cc968',
    'calories': 'c4c3da14-8e2d-11f0-8e2c-000c299cc968',
    'moisture': 'ec65aa99-8e2c-11f0-8e2c-000c299cc968',
    'fiber': '11fe0378-8e2d-11f0-8e2c-000c299cc968',
    'lactose': '3cdacac8-8e2d-11f0-8e2c-000c299cc968',
    'sweetness': '87d8874e-8e2d-11f0-8e2c-000c299cc968',
}

ALLERGEN_PROP_KEYS = {
    'has_allergens': '87e46ae6-8e56-11f0-8e2c-000c299cc968',
    'глютен': 'd68a8efc-8e56-11f0-8e2c-000c299cc968',
    'ракообразные': 'e15c2ff6-8e56-11f0-8e2c-000c299cc968',
    'яйца': 'ef1fea8c-8e56-11f0-8e2c-000c299cc968',
    'рыба': 'fe76964e-8e56-11f0-8e2c-000c299cc968',
    'арахис': '0e9dc99e-8e57-11f0-8e2c-000c299cc968',
    'соя': '1aa12403-8e57-11f0-8e2c-000c299cc968',
    'молоко': '286d6d77-8e57-11f0-8e2c-000c299cc968',
    'орехи': '35ad29ff-8e57-11f0-8e2c-000c299cc968',
    'сельдерей': '42c06b6c-8e57-11f0-8e2c-000c299cc968',
    'горчица': '4aef2c22-8e57-11f0-8e2c-000c299cc968',
    'кунжут': '596e8fcb-8e57-11f0-8e2c-000c299cc968',
    'диоксид_серы': '70229b66-8e57-11f0-8e2c-000c299cc968',
    'люпин': '7b4560cb-8e57-11f0-8e2c-000c299cc968',
    'моллюски': '86c00960-8e57-11f0-8e2c-000c299cc968',
}

ALL_ALLERGEN_NAMES = ['глютен', 'ракообразные', 'яйца', 'рыба', 'арахис', 'соя',
                      'молоко', 'орехи', 'сельдерей', 'горчица', 'кунжут', 'диоксид_серы', 'люпин', 'моллюски']

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def get_db():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


# === ДИНАМИЧЕСКИЙ ПОИСК ТЕХНОЛОГОВ ===

def find_technologists(conn):
    """Найти актуальных технологов из tg_user_roles."""
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, first_name, last_name, role
        FROM tg_user_roles
        WHERE role ILIKE '%технолог%'
        ORDER BY 
            CASE WHEN role ILIKE '%главн%' THEN 0 ELSE 1 END,
            role
    """)
    rows = cur.fetchall()
    cur.close()
    
    # Дедупликация по user_id (берём наивысшую роль)
    seen = {}
    for user_id, first_name, last_name, role in rows:
        if user_id not in seen:
            name = f"{first_name or ''} {last_name or ''}".strip() or str(user_id)
            seen[user_id] = {"user_id": user_id, "name": name, "role": role}
    
    technologists = list(seen.values())
    if not technologists:
        logger.warning("Технологи не найдены в tg_user_roles! Запросы будут назначены на админа.")
        technologists = [{"user_id": ADMIN_USER_ID, "name": "Администратор", "role": "admin"}]
    
    return technologists


def get_primary_technologist(conn):
    """Получить основного технолога (Главный технолог, или первый доступный)."""
    techs = find_technologists(conn)
    # Приоритет: Главный технолог > Технолог > Админ
    for t in techs:
        if 'главн' in t['role'].lower():
            return t
    return techs[0]


# === TELEGRAM API ===

def tg_request(method, data=None, files=None):
    """Отправка запроса в Telegram API через прокси."""
    url = f"{TELEGRAM_API}/{method}"
    proxies = {"https": SOCKS_PROXY, "http": SOCKS_PROXY}
    try:
        if files:
            r = requests.post(url, data=data, files=files, proxies=proxies, timeout=30)
        else:
            r = requests.post(url, json=data, proxies=proxies, timeout=30)
        return r.json()
    except Exception as e:
        logger.error(f"TG API error: {e}")
        return None


# === LLM ===

def llm_call(model, messages, temperature=0.1):
    try:
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": model, "messages": messages, "max_tokens": 1500, "temperature": temperature},
            timeout=90
        )
        if response.status_code != 200:
            return None
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"LLM error: {e}")
        return None


def parse_json(text):
    if not text:
        return None
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    try:
        return json.loads(text)
    except:
        start = text.find('{')
        end = text.rfind('}')
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end+1])
            except:
                pass
    return None


# === ОЧЕРЕДЬ ЗАПРОСОВ ===

def populate_pending_requests():
    """Заполнить nutrition_requests позициями без БЖУ."""
    conn = get_db()
    cur = conn.cursor()
    tech = get_primary_technologist(conn)
    
    placeholders = ','.join(['%s'] * len(SYRYE_TYPE_IDS))
    cur.execute(f"""
        SELECT n.id, n.name FROM nomenclature n
        LEFT JOIN nutrition_requests nr ON nr.nom_id = n.id
        WHERE n.type_id IN ({placeholders})
          AND n.is_folder = false
          AND (n.protein IS NULL AND n.fat IS NULL AND n.carbs IS NULL AND n.calories IS NULL)
          AND nr.id IS NULL
        ORDER BY n.name
    """, SYRYE_TYPE_IDS)
    
    new_items = cur.fetchall()
    added = 0
    for nom_id, name in new_items:
        cur.execute("""
            INSERT INTO nutrition_requests (nom_id, nom_name, status, assigned_to, assigned_name)
            VALUES (%s, %s, 'pending', %s, %s)
            ON CONFLICT (nom_id) DO NOTHING
        """, (str(nom_id), name, tech["user_id"], tech["name"]))
        added += 1
    
    conn.commit()
    logger.info(f"Добавлено {added} запросов, назначено на: {tech['name']} ({tech['role']})")
    cur.close()
    conn.close()
    return added


# === ОТПРАВКА ЗАПРОСОВ ===

def send_next_request(user_id=None):
    """Отправить следующий запрос технологу."""
    conn = get_db()
    cur = conn.cursor()
    
    target_user = user_id
    if not target_user:
        tech = get_primary_technologist(conn)
        target_user = tech["user_id"]
    
    cur.execute("""
        SELECT id, nom_id, nom_name, search_data
        FROM nutrition_requests
        WHERE status = 'pending' AND assigned_to = %s
        ORDER BY created_at LIMIT 1
    """, (target_user,))
    
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return None
    
    req_id, nom_id, nom_name, search_data = row
    
    cur.execute("SELECT COUNT(*) FROM nutrition_requests WHERE status = 'pending' AND assigned_to = %s", (target_user,))
    remaining = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM nutrition_requests WHERE status = 'written'")
    done = cur.fetchone()[0]
    
    text = f"📋 *Заполнение БЖУ сырья* ({remaining} в очереди, {done} заполнено)\n\n"
    text += f"*{nom_name}*\n\n"
    
    if search_data:
        sd = search_data if isinstance(search_data, dict) else json.loads(search_data)
        text += f"🔍 Найдено в интернете (не подтверждено):\n"
        text += f"Б={sd.get('protein','?')} Ж={sd.get('fat','?')} У={sd.get('carbs','?')} Кал={sd.get('calories','?')}\n\n"
    
    text += "Выберите действие:"
    
    keyboard = {"inline_keyboard": [
        [{"text": "✏️ Ввести данные", "callback_data": f"nutr_input:{req_id}"},
         {"text": "📷 Фото этикетки", "callback_data": f"nutr_photo:{req_id}"}],
        [{"text": "⏭ Пропустить", "callback_data": f"nutr_skip:{req_id}"},
         {"text": "⏰ Отложить", "callback_data": f"nutr_defer:{req_id}"}],
        [{"text": "❌ Отклонить", "callback_data": f"nutr_reject:{req_id}"}]
    ]}
    
    if search_data:
        keyboard["inline_keyboard"].insert(1, [
            {"text": "✅ Подтвердить найденные данные", "callback_data": f"nutr_confirm:{req_id}"}
        ])
    
    result = tg_request("sendMessage", {
        "chat_id": target_user, "text": text,
        "parse_mode": "Markdown", "reply_markup": keyboard
    })
    
    if result and result.get("ok"):
        msg_id = result["result"]["message_id"]
        cur.execute("UPDATE nutrition_requests SET status='sent', message_id=%s, updated_at=NOW() WHERE id=%s",
                   (msg_id, req_id))
        conn.commit()
        logger.info(f"Отправлен запрос #{req_id}: {nom_name} → user {target_user}")
    
    cur.close()
    conn.close()
    return req_id


def send_batch_requests(count=5, user_id=None):
    # Раз в день проверяем, все ли технологи написали /start
    invite_flag = "/tmp/nutrition_invite_done"
    today = time.strftime("%Y-%m-%d")
    need_invite = True
    try:
        with open(invite_flag) as f:
            if f.read().strip() == today:
                need_invite = False
    except FileNotFoundError:
        pass
    if need_invite:
        try:
            invite_technologists_to_bot()
        except Exception as e:
            logger.warning(f"invite_technologists error: {e}")
        with open(invite_flag, "w") as f:
            f.write(today)

    sent = 0
    for _ in range(count):
        if send_next_request(user_id) is None:
            break
        sent += 1
        time.sleep(0.5)
    return sent

def invite_technologists_to_bot():
    """Отправить приглашение в групповой чат технологам, которые не начали диалог с ботом."""
    conn = get_db()
    techs = find_technologists(conn)
    
    # Проверяем кто может получать личные сообщения
    uninvited = []
    for tech in techs:
        result = tg_request("sendMessage", {
            "chat_id": tech["user_id"],
            "text": "✓"
        })
        if result and result.get("ok"):
            # Удаляем тестовое сообщение
            tg_request("deleteMessage", {
                "chat_id": tech["user_id"],
                "message_id": result["result"]["message_id"]
            })
        else:
            uninvited.append(tech)
    
    if not uninvited:
        logger.info("Все технологи уже доступны для бота")
        conn.close()
        return
    
    # Находим общий групповой чат
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT r.chat_id, m.chat_title
        FROM tg_user_roles r
        JOIN tg_chats_metadata m ON m.chat_id = r.chat_id
        WHERE r.user_id = ANY(%s) AND r.role ILIKE '%%технолог%%'
        LIMIT 1
    """, ([t["user_id"] for t in uninvited],))
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    if not row:
        logger.warning("Не найден общий чат с технологами")
        return
    
    chat_id, chat_title = row
    
    mentions = []
    for tech in uninvited:
        mentions.append(f"[{tech['name']}](tg://user?id={tech['user_id']})")
    
    text = (f"👋 {', '.join(mentions)}\n\n"
            f"Для заполнения данных о пищевой ценности сырья, "
            f"пожалуйста, напишите боту в личные сообщения команду /start\n\n"
            f"После этого бот сможет отправлять вам запросы на заполнение БЖУ.")
    
    tg_request("sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    })
    logger.info(f"Приглашение отправлено в '{chat_title}' для {len(uninvited)} технологов")

# === ОБРАБОТКА ОТВЕТОВ ===

def handle_callback(callback_query):
    """Обработка нажатия кнопки."""
    data = callback_query.get("data", "")
    user_id = callback_query["from"]["id"]
    callback_id = callback_query["id"]
    
    parts = data.split(":")
    if len(parts) != 2 or not parts[0].startswith("nutr_"):
        return
    
    action, req_id = parts[0], int(parts[1])
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("SELECT nom_id, nom_name, search_data, status FROM nutrition_requests WHERE id = %s", (req_id,))
    row = cur.fetchone()
    if not row:
        tg_request("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Запрос не найден"})
        cur.close(); conn.close()
        return
    
    nom_id, nom_name, search_data, status = row
    
    if action == "nutr_input":
        tg_request("answerCallbackQuery", {"callback_query_id": callback_id})
        tg_request("sendMessage", {
            "chat_id": user_id,
            "text": f"Введите данные для *{nom_name}*:\n\n"
                    f"`Б=значение Ж=значение У=значение Сахар=значение Кал=значение`\n"
                    f"`Влажность=14 Клетчатка=2.5 Лактоза=0`\n"
                    f"`Аллергены=глютен,соя,молоко`\n\n"
                    f"Пример: `Б=10.5 Ж=1.2 У=72 Кал=340`\n\n"
                    f"_ID:{req_id}_",
            "parse_mode": "Markdown"
        })
        cur.execute("UPDATE nutrition_requests SET status='awaiting_input', updated_at=NOW() WHERE id=%s", (req_id,))
    
    elif action == "nutr_photo":
        tg_request("answerCallbackQuery", {"callback_query_id": callback_id})
        tg_request("sendMessage", {
            "chat_id": user_id,
            "text": f"📷 Отправьте фото этикетки для *{nom_name}*\n\n_ID:{req_id}_",
            "parse_mode": "Markdown"
        })
        cur.execute("UPDATE nutrition_requests SET status='awaiting_photo', updated_at=NOW() WHERE id=%s", (req_id,))
    
    elif action == "nutr_confirm":
        tg_request("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Подтверждено!"})
        sd = search_data if isinstance(search_data, dict) else json.loads(search_data) if search_data else None
        if sd:
            success = write_to_1c_and_db(conn, nom_id, nom_name, sd)
            if success:
                cur.execute("UPDATE nutrition_requests SET status='written', verified_data=%s, updated_at=NOW() WHERE id=%s",
                           (json.dumps(sd, ensure_ascii=False), req_id))
                tg_request("sendMessage", {"chat_id": user_id, "text": f"✅ *{nom_name}* — записано в 1С!", "parse_mode": "Markdown"})
            else:
                tg_request("sendMessage", {"chat_id": user_id, "text": f"❌ Ошибка записи в 1С"})
        send_next_request(user_id)
    
    elif action == "nutr_skip":
        tg_request("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Пропущено"})
        cur.execute("UPDATE nutrition_requests SET status='skipped', updated_at=NOW() WHERE id=%s", (req_id,))
        send_next_request(user_id)
    
    elif action == "nutr_defer":
        tg_request("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Отложено на 7 дней"})
        cur.execute("UPDATE nutrition_requests SET status='deferred', defer_until=NOW()+INTERVAL '7 days', updated_at=NOW() WHERE id=%s", (req_id,))
        send_next_request(user_id)
    
    elif action == "nutr_reject":
        tg_request("answerCallbackQuery", {"callback_query_id": callback_id})
        tg_request("sendMessage", {
            "chat_id": user_id,
            "text": f"Укажите причину отклонения для *{nom_name}*:\n\n_ID:{req_id}_",
            "parse_mode": "Markdown"
        })
        cur.execute("UPDATE nutrition_requests SET status='awaiting_reject_reason', updated_at=NOW() WHERE id=%s", (req_id,))
    
    conn.commit()
    cur.close()
    conn.close()


def handle_text_reply(message):
    """Обработка текстового ответа."""
    user_id = message["from"]["id"]
    text = message.get("text", "")
    
    conn = get_db()
    cur = conn.cursor()
    
    # Ищем ID запроса в тексте сообщения или reply
    req_id = None
    for source in [text, message.get("reply_to_message", {}).get("text", "")]:
        match = re.search(r'ID:(\d+)', source)
        if match:
            req_id = int(match.group(1))
            break
    
    if not req_id:
        cur.execute("""
            SELECT id FROM nutrition_requests 
            WHERE assigned_to = %s AND status IN ('awaiting_input', 'awaiting_reject_reason')
            ORDER BY updated_at DESC LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        if row:
            req_id = row[0]
    
    if not req_id:
        cur.close(); conn.close()
        return
    
    cur.execute("SELECT nom_id, nom_name, status FROM nutrition_requests WHERE id = %s", (req_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return
    
    nom_id, nom_name, status = row
    
    if status == 'awaiting_reject_reason':
        cur.execute("UPDATE nutrition_requests SET status='rejected', reject_reason=%s, updated_at=NOW() WHERE id=%s", (text, req_id))
        conn.commit()
        tg_request("sendMessage", {"chat_id": user_id, "text": f"❌ *{nom_name}* — отклонено.\nПричина: {text}", "parse_mode": "Markdown"})
        send_next_request(user_id)
    
    elif status == 'awaiting_input':
        parsed = parse_nutrition_text(text)
        if not parsed:
            tg_request("sendMessage", {
                "chat_id": user_id,
                "text": "⚠️ Не удалось распознать. Формат:\n`Б=10.5 Ж=1.2 У=72 Кал=340`",
                "parse_mode": "Markdown"
            })
            cur.close(); conn.close()
            return
        
        verify_result = verify_technologist_data(nom_name, parsed)
        
        if verify_result and verify_result.get("verified"):
            final_data = verify_result.get("final_data", parsed)
            success = write_to_1c_and_db(conn, nom_id, nom_name, final_data)
            if success:
                cur.execute("""
                    UPDATE nutrition_requests SET status='written', 
                        technologist_data=%s, verified_data=%s, updated_at=NOW()
                    WHERE id=%s
                """, (json.dumps(parsed, ensure_ascii=False), json.dumps(final_data, ensure_ascii=False), req_id))
                conn.commit()
                tg_request("sendMessage", {"chat_id": user_id,
                    "text": f"✅ *{nom_name}* — записано!\n"
                            f"Б={final_data.get('protein')} Ж={final_data.get('fat')} "
                            f"У={final_data.get('carbs')} Кал={final_data.get('calories')}",
                    "parse_mode": "Markdown"})
            else:
                tg_request("sendMessage", {"chat_id": user_id, "text": f"❌ Ошибка записи в 1С"})
        else:
            issues = verify_result.get("issues", ["Неизвестная проблема"]) if verify_result else ["Верификация не удалась"]
            tg_request("sendMessage", {
                "chat_id": user_id,
                "text": f"⚠️ *{nom_name}* — не прошло проверку:\n" + "\n".join(f"• {i}" for i in issues) +
                        f"\n\nПопробуйте заново или отправьте фото.",
                "parse_mode": "Markdown"
            })
        
        send_next_request(user_id)
    
    cur.close()
    conn.close()


def handle_photo(message):
    """Обработка фото этикетки."""
    user_id = message["from"]["id"]
    
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT id, nom_id, nom_name FROM nutrition_requests 
        WHERE assigned_to = %s AND status = 'awaiting_photo'
        ORDER BY updated_at DESC LIMIT 1
    """, (user_id,))
    row = cur.fetchone()
    
    if not row:
        cur.close(); conn.close()
        return
    
    req_id, nom_id, nom_name = row
    photos = message.get("photo", [])
    if not photos:
        cur.close(); conn.close()
        return
    
    file_id = photos[-1]["file_id"]
    
    # Скачиваем фото
    file_info = tg_request("getFile", {"file_id": file_id})
    if not file_info or not file_info.get("ok"):
        tg_request("sendMessage", {"chat_id": user_id, "text": "❌ Не удалось получить фото"})
        cur.close(); conn.close()
        return
    
    file_path = file_info["result"]["file_path"]
    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    proxies = {"https": SOCKS_PROXY, "http": SOCKS_PROXY}
    photo_response = requests.get(file_url, proxies=proxies, timeout=30)
    
    if photo_response.status_code != 200:
        tg_request("sendMessage", {"chat_id": user_id, "text": "❌ Не удалось скачать фото"})
        cur.close(); conn.close()
        return
    
    photo_b64 = base64.b64encode(photo_response.content).decode('utf-8')
    tg_request("sendMessage", {"chat_id": user_id, "text": f"🔍 Анализирую фото для *{nom_name}*...", "parse_mode": "Markdown"})
    
    extracted = analyze_photo_nutrition(nom_name, photo_b64)
    
    if not extracted:
        tg_request("sendMessage", {"chat_id": user_id, "text": "❌ Не удалось извлечь данные. Попробуйте ввести вручную."})
        cur.execute("UPDATE nutrition_requests SET status='awaiting_input', updated_at=NOW() WHERE id=%s", (req_id,))
        conn.commit(); cur.close(); conn.close()
        return
    
    text = f"📷 Из фото для *{nom_name}*:\n\n"
    text += f"Б={extracted.get('protein','?')} Ж={extracted.get('fat','?')} У={extracted.get('carbs','?')} Кал={extracted.get('calories','?')}\n"
    allergens_found = [k for k, v in extracted.get('allergens', {}).items() if v]
    if allergens_found:
        text += f"Аллергены: {', '.join(allergens_found)}\n"
    
    cur.execute("UPDATE nutrition_requests SET photo_file_id=%s, search_data=%s, status='sent', updated_at=NOW() WHERE id=%s",
               (file_id, json.dumps(extracted, ensure_ascii=False), req_id))
    conn.commit()
    
    keyboard = {"inline_keyboard": [
        [{"text": "✅ Подтвердить", "callback_data": f"nutr_confirm:{req_id}"},
         {"text": "✏️ Исправить", "callback_data": f"nutr_input:{req_id}"}],
        [{"text": "❌ Отклонить", "callback_data": f"nutr_reject:{req_id}"}]
    ]}
    
    tg_request("sendMessage", {"chat_id": user_id, "text": text, "parse_mode": "Markdown", "reply_markup": keyboard})
    cur.close(); conn.close()


# === ПАРСИНГ И ВЕРИФИКАЦИЯ ===

def parse_nutrition_text(text):
    mapping = {
        'б': 'protein', 'белки': 'protein', 'белок': 'protein',
        'ж': 'fat', 'жиры': 'fat', 'жир': 'fat',
        'у': 'carbs', 'углеводы': 'carbs',
        'сахар': 'sugar',
        'кал': 'calories', 'калории': 'calories', 'ккал': 'calories', 'калорийность': 'calories',
        'влажность': 'moisture', 'влага': 'moisture',
        'клетчатка': 'fiber', 'лактоза': 'lactose', 'сладость': 'sweetness',
    }
    result = {}
    pairs = re.findall(r'(\w+)\s*=\s*([\d.,]+)', text, re.IGNORECASE)
    for key, value in pairs:
        key_lower = key.lower()
        if key_lower in mapping:
            try:
                result[mapping[key_lower]] = float(value.replace(',', '.'))
            except ValueError:
                pass
    
    allergen_match = re.search(r'аллергены\s*=\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
    if allergen_match:
        allergen_list = [a.strip().lower() for a in allergen_match.group(1).split(',')]
        result['allergens'] = {a: (a in allergen_list) for a in ALL_ALLERGEN_NAMES}
        result['has_allergens'] = any(result['allergens'].values())
    else:
        result['allergens'] = {a: False for a in ALL_ALLERGEN_NAMES}
        result['has_allergens'] = False
    
    if not result.get('protein') and not result.get('calories'):
        return None
    return result


def verify_technologist_data(product_name, data):
    prompt = f"""Проверь пищевую ценность для сырья "{product_name}".
Данные: {json.dumps(data, ensure_ascii=False, indent=2)}
Правила: Б+Ж+У+влажность+клетчатка≈100%(±15%), сахар≤углеводы, калорийность≈Б*4+Ж*9+У*4(±15%), диапазоны 0-100/0-900.
Верни JSON: {{"verified": true/false, "confidence": "high/medium/low", "issues": [], "final_data": {{...}}}}"""
    text = llm_call(VERIFY_MODEL, [
        {"role": "system", "content": "Эксперт по пищевой химии. Только JSON."},
        {"role": "user", "content": prompt}
    ])
    return parse_json(text)


def analyze_photo_nutrition(product_name, photo_b64):
    prompt = f"""Извлеки пищевую ценность из этикетки на фото. Продукт: "{product_name}".
Верни JSON: {{"protein":число,"fat":число,"carbs":число,"sugar":число,"calories":число,
"moisture":null,"fiber":null,"lactose":null,"sweetness":null,"has_allergens":bool,
"allergens":{{"глютен":bool,"ракообразные":bool,"яйца":bool,"рыба":bool,"арахис":bool,
"соя":bool,"молоко":bool,"орехи":bool,"сельдерей":bool,"горчица":bool,"кунжут":bool,
"диоксид_серы":bool,"люпин":bool,"моллюски":bool}}}}"""
    try:
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": VISION_MODEL, "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{photo_b64}"}}
                ]}
            ], "max_tokens": 1000, "temperature": 0.1},
            timeout=90
        )
        if response.status_code != 200:
            return None
        return parse_json(response.json()["choices"][0]["message"]["content"])
    except Exception as e:
        logger.error(f"Vision error: {e}")
        return None


# === ЗАПИСЬ В 1С ===

def write_to_1c_and_db(conn, nom_id, nom_name, nutrition_data):
    encoded = quote("Catalog_Номенклатура", safe='_')
    dp = quote("ДополнительныеРеквизиты", safe='')
    url = f"{ODATA_BASE_URL}/{encoded}(guid'{nom_id}')/{dp}?$format=json"
    r = requests.get(url, auth=(ODATA_USER, ODATA_PASS), timeout=30)
    existing = r.json().get('value', []) if r.status_code == 200 else []
    
    new_props = []
    line_number = 1
    all_known = set(NUTRITION_PROP_KEYS.values()) | set(ALLERGEN_PROP_KEYS.values())
    
    for prop in existing:
        if prop.get('Свойство_Key') not in all_known:
            new_props.append({'LineNumber': str(line_number), 'Свойство_Key': prop['Свойство_Key'],
                'Значение': prop.get('Значение',''), 'Значение_Type': prop.get('Значение_Type',''),
                'ТекстоваяСтрока': prop.get('ТекстоваяСтрока','')})
            line_number += 1
    
    for field, prop_key in NUTRITION_PROP_KEYS.items():
        value = nutrition_data.get(field)
        if value is not None:
            new_props.append({'LineNumber': str(line_number), 'Свойство_Key': prop_key,
                'Значение': float(value), 'Значение_Type': 'Edm.Double', 'ТекстоваяСтрока': ''})
            line_number += 1
    
    allergens = nutrition_data.get('allergens', {})
    for field, prop_key in ALLERGEN_PROP_KEYS.items():
        value = nutrition_data.get('has_allergens', False) if field == 'has_allergens' else allergens.get(field, False)
        new_props.append({'LineNumber': str(line_number), 'Свойство_Key': prop_key,
            'Значение': 'true' if value else 'false', 'Значение_Type': 'Edm.Boolean', 'ТекстоваяСтрока': ''})
        line_number += 1
    
    patch_url = f"{ODATA_BASE_URL}/{encoded}(guid'{nom_id}')"
    r = requests.patch(patch_url, json={'ДополнительныеРеквизиты': new_props},
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
        auth=(ODATA_USER, ODATA_PASS), timeout=30)
    
    if r.status_code != 200:
        logger.error(f"1C PATCH error for {nom_name}: {r.status_code}")
        return False
    
    cur = conn.cursor()
    cur.execute("""
        UPDATE nomenclature SET protein=%s, fat=%s, carbs=%s, sugar=%s, calories=%s,
            moisture=%s, fiber=%s, lactose=%s, sweetness=%s, has_allergens=%s, allergens=%s::jsonb
        WHERE id = %s::uuid
    """, (nutrition_data.get('protein'), nutrition_data.get('fat'), nutrition_data.get('carbs'),
          nutrition_data.get('sugar'), nutrition_data.get('calories'), nutrition_data.get('moisture'),
          nutrition_data.get('fiber'), nutrition_data.get('lactose'), nutrition_data.get('sweetness'),
          nutrition_data.get('has_allergens', False),
          json.dumps(allergens, ensure_ascii=False), str(nom_id)))
    conn.commit()
    cur.close()
    logger.info(f"Written: {nom_name}")
    return True


# === УТИЛИТЫ ===

def check_deferred():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE nutrition_requests SET status='pending', updated_at=NOW() WHERE status='deferred' AND defer_until<=NOW() RETURNING id")
    rows = cur.fetchall()
    conn.commit(); cur.close(); conn.close()
    if rows:
        logger.info(f"Возвращено из отложенных: {len(rows)}")
    return len(rows)


def get_stats():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT status, COUNT(*) FROM nutrition_requests GROUP BY status ORDER BY status")
    stats = dict(cur.fetchall())
    cur.close(); conn.close()
    return stats


def list_technologists():
    conn = get_db()
    techs = find_technologists(conn)
    conn.close()
    return techs


# === CLI ===

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Бот запроса БЖУ у технологов')
    parser.add_argument('--populate', action='store_true', help='Заполнить очередь')
    parser.add_argument('--send', type=int, default=0, help='Отправить N запросов')
    parser.add_argument('--stats', action='store_true', help='Статистика')
    parser.add_argument('--check-deferred', action='store_true', help='Проверить отложенные')
    parser.add_argument('--technologists', action='store_true', help='Показать найденных технологов')
    parser.add_argument('--user', type=int, default=None, help='User ID (переопределить технолога)')
    parser.add_argument('--invite', action='store_true', help='Пригласить технологов написать боту /start')
    args = parser.parse_args()
    
    if args.technologists:
        techs = list_technologists()
        print("\nТехнологи из tg_user_roles:")
        for t in techs:
            primary = " ← основной" if t == techs[0] else ""
            print(f"  {t['name']} (user_id={t['user_id']}, роль={t['role']}){primary}")
    
    if args.populate:
        populate_pending_requests()
    
    if args.stats:
        stats = get_stats()
        print("\nСтатистика:")
        for status, count in stats.items():
            print(f"  {status}: {count}")
    
    if args.send > 0:
        sent = send_batch_requests(args.send, args.user)
        print(f"Отправлено: {sent}")
    
    if args.check_deferred:
        check_deferred()

    if args.invite:
        invite_technologists_to_bot()
