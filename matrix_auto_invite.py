"""
matrix_auto_invite.py — Автоматическое приглашение сотрудников в Element.

Логика:
1. Берёт активных сотрудников из tg_user_roles
2. Сопоставляет TG username → Matrix аккаунт
3. Для тех у кого есть Matrix-аккаунт:
   a) Сбрасывает пароль (если ещё не сброшен) → сохраняет в matrix_invites
   b) Отправляет приглашение через TG-бота в групповой чат (inline mention)
   c) Приглашает в рабочие Matrix-комнаты тех, кто уже зашёл
4. Не спамит — отправляет по N приглашений за запуск, помнит кому уже отправлено

Запуск:
  python3 matrix_auto_invite.py --dry-run       # посмотреть что сделает
  python3 matrix_auto_invite.py                  # выполнить (до 7 приглашений)
  python3 matrix_auto_invite.py --batch 15       # до 15 приглашений за раз
  python3 matrix_auto_invite.py --resend         # повторить тем, кто не зашёл
  python3 matrix_auto_invite.py --invite-rooms   # только пригласить в комнаты (для уже зарег.)
"""

import os
import sys
import string
import random
import logging
import argparse
import time
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("matrix_auto_invite")

# ── Конфигурация ──────────────────────────────────────────────

MATRIX_URL = os.environ.get("MATRIX_URL", "http://localhost:8008")
MATRIX_ADMIN_USER = os.environ.get("MATRIX_ADMIN_USER", "aleksei")
MATRIX_ADMIN_PASSWORD = os.environ.get("MATRIX_ADMIN_PASSWORD", "TempPass2026!")

DB_HOST = os.environ.get("DB_HOST", "172.20.0.2")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "knowledge_base")
DB_USER = os.environ.get("DB_USER", "knowledge")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

DEFAULT_BATCH = 7

# Рабочие Matrix-комнаты
WORK_ROOMS = {
    "Бухгалтерия Фрумелад/НФ", "Руководство (bridged)", "Производство",
    "Априори & Фрумелад/НФ", "Секретариат", "HR-Фрумелад/НФ",
    "Фрумелад задачи на разработку BSG", "Торты Отгрузки",
    "Фрумелад поддержка BSG", "Дизайн упаковки",
    "Новые продукты и конкуренты", "БЗ Производство Chat", "БЗ Производство",
    "БЗ R&D", "БЗ R&D Chat", "БЗ Бухгалтерия", "БЗ Бухгалтерия Chat",
    "БЗ Закупки Chat", "БЗ Склад", "БЗ Склад Chat",
    "Подбор Персонала Внешний", "Отчеты по аутсорсингу",
    "R&D ~ общая рабочая группа", "KELIN - ФНС",
    "KELIN - кондитерская Прохорова", "БЗ инструкции производство",
    "Закупки", "Закупки - Упаковка", "Продажи на ярды",
    "Производство Кондитерская Прохорова",
    "Фрумелад (НФ) Кадровые задачи по IT и 1С",
}

# Исключения в маппинге TG-чат → Matrix-комната
TG_TO_MATRIX_NAME = {
    "Руководство": "Руководство (bridged)",
    "Дизайн упаковки Кондитерская Прохорова": "Дизайн упаковки",
}

# Ручной маппинг TG user_id → Matrix localpart
MANUAL_USER_MAP = {
    805598873: "aleksei",
    1058481218: "irina.prokhorova",
}

SKIP_MATRIX_USERS = {"@bot:frumelad.ru", "@aleksei:frumelad.ru"}

ELEMENT_ANDROID = "https://play.google.com/store/apps/details?id=io.element.android.x"
ELEMENT_IOS = "https://apps.apple.com/app/element-x-secure-chat-call/id1631335820"
ELEMENT_WEB = "https://app.element.io"


# ── Утилиты ──────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


def generate_password(length=10):
    chars = string.ascii_letters + string.digits
    pwd = [random.choice(string.ascii_uppercase), random.choice(string.ascii_lowercase), random.choice(string.digits)]
    pwd += [random.choice(chars) for _ in range(length - 3)]
    random.shuffle(pwd)
    return "".join(pwd)


# ── Telegram Bot API ─────────────────────────────────────────

def tg_send_message(chat_id, text, parse_mode="Markdown"):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    proxies = {}
    proxy_url = os.environ.get("PROXY_URL")
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}
    resp = requests.post(url, json={
        "chat_id": chat_id, "text": text,
        "parse_mode": parse_mode, "disable_web_page_preview": True,
    }, proxies=proxies, timeout=15)
    return resp.json()


# ── Matrix API ───────────────────────────────────────────────

def matrix_login():
    resp = requests.post(f"{MATRIX_URL}/_matrix/client/v3/login",
        json={"type": "m.login.password", "user": MATRIX_ADMIN_USER, "password": MATRIX_ADMIN_PASSWORD}, timeout=10)
    data = resp.json()
    if "access_token" not in data:
        logger.error(f"Matrix login failed: {data}")
        sys.exit(1)
    return data["access_token"]


def matrix_get_real_users(token):
    users = {}
    _from = "0"
    while True:
        resp = requests.get(f"{MATRIX_URL}/_synapse/admin/v2/users",
            headers={"Authorization": f"Bearer {token}"}, params={"limit": 100, "from": _from}, timeout=15)
        data = resp.json()
        for u in data.get("users", []):
            name = u["name"]
            if not name.startswith("@telegram_"):
                localpart = name.split(":")[0].lstrip("@")
                users[localpart.lower()] = name
        if not data.get("next_token"):
            break
        _from = data["next_token"]
    return users


def matrix_reset_password(token, matrix_user_id, new_password):
    resp = requests.put(f"{MATRIX_URL}/_synapse/admin/v2/users/{matrix_user_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"password": new_password, "logout_devices": False}, timeout=10)
    return resp.status_code == 200


def matrix_get_rooms(token):
    rooms = {}
    _from = 0
    while True:
        resp = requests.get(f"{MATRIX_URL}/_synapse/admin/v1/rooms",
            headers={"Authorization": f"Bearer {token}"}, params={"limit": 100, "from": _from}, timeout=15)
        data = resp.json()
        for r in data.get("rooms", []):
            name = r.get("name")
            if name and name in WORK_ROOMS:
                rooms[name] = r["room_id"]
        if len(data.get("rooms", [])) < 100:
            break
        _from += 100
    return rooms


def matrix_get_room_members(token, room_id):
    resp = requests.get(f"{MATRIX_URL}/_synapse/admin/v1/rooms/{room_id}/members",
        headers={"Authorization": f"Bearer {token}"}, timeout=10)
    return set(resp.json().get("members", []))


def matrix_invite_to_room(token, room_id, user_id):
    resp = requests.post(f"{MATRIX_URL}/_matrix/client/v3/rooms/{room_id}/invite",
        headers={"Authorization": f"Bearer {token}"}, json={"user_id": user_id}, timeout=10)
    return resp.status_code in (200, 403)


# ── БД: matrix_invites ──────────────────────────────────────

def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS matrix_invites (
            tg_user_id BIGINT PRIMARY KEY,
            tg_username VARCHAR(255),
            tg_first_name VARCHAR(255),
            matrix_id VARCHAR(255),
            matrix_password VARCHAR(255),
            invite_sent_at TIMESTAMP,
            invite_chat_id BIGINT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()


def get_already_invited(conn):
    cur = conn.cursor()
    cur.execute("SELECT tg_user_id FROM matrix_invites WHERE invite_sent_at IS NOT NULL")
    result = {row[0] for row in cur.fetchall()}
    cur.close()
    return result


def save_invite(conn, tg_user_id, tg_username, tg_first_name, matrix_id, password, chat_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO matrix_invites (tg_user_id, tg_username, tg_first_name, matrix_id, matrix_password, invite_sent_at, invite_chat_id)
        VALUES (%s, %s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (tg_user_id) DO UPDATE SET
            matrix_password = EXCLUDED.matrix_password,
            invite_sent_at = NOW(),
            invite_chat_id = EXCLUDED.invite_chat_id
    """, (tg_user_id, tg_username, tg_first_name, matrix_id, password, chat_id))
    conn.commit()
    cur.close()


# ── Основная логика ──────────────────────────────────────────

def get_tg_employees(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT ur.user_id, ur.username, ur.first_name,
               array_agg(DISTINCT ur.chat_id) as chat_ids,
               array_agg(DISTINCT m.chat_title) as chat_titles
        FROM tg_user_roles ur
        JOIN tg_chats_metadata m ON m.chat_id = ur.chat_id
        WHERE ur.is_active = true
          AND ur.first_name NOT IN ('Group')
          AND COALESCE(ur.username, '') != 'GroupAnonymousBot'
        GROUP BY ur.user_id, ur.username, ur.first_name
        ORDER BY ur.first_name
    """)
    employees = []
    for row in cur.fetchall():
        employees.append({
            "user_id": row[0], "username": row[1], "first_name": row[2],
            "chat_ids": row[3], "chat_titles": row[4],
        })
    cur.close()
    return employees


def resolve_matrix_id(tg_user_id, tg_username, matrix_users):
    if tg_user_id in MANUAL_USER_MAP:
        localpart = MANUAL_USER_MAP[tg_user_id]
        return matrix_users.get(localpart)
    if tg_username:
        return matrix_users.get(tg_username.lower())
    return None


def pick_best_chat(chat_ids, chat_titles):
    priority = ["Производство", "Руководство", "Закупки", "Бухгалтерия", "HR"]
    for prio in priority:
        for i, title in enumerate(chat_titles):
            if title and prio.lower() in title.lower():
                return chat_ids[i], chat_titles[i]
    return chat_ids[0], chat_titles[0]


def format_invite_message(first_name, tg_user_id, matrix_login, password):
    mention = f"[{first_name}](tg://user?id={tg_user_id})"
    return (
        f"👋 {mention}\n\n"
        f"Мы подключаем корпоративный мессенджер Element.\n"
        f"Вам создан аккаунт, вот ваши данные для входа:\n\n"
        f"📱 *Android:* [скачать Element X]({ELEMENT_ANDROID})\n"
        f"📱 *iPhone:* [скачать Element X]({ELEMENT_IOS})\n"
        f"💻 *На компьютере:* откройте {ELEMENT_WEB}\n\n"
        f"При входе нажмите *«Изменить сервер»* и введите:\n"
        f"Сервер: `matrix.frumelad.ru`\n"
        f"Логин: `{matrix_login}`\n"
        f"Пароль: `{password}`\n\n"
        f"По вопросам → @GreyArea8"
    )


def run_invite(args):
    conn = get_db()
    ensure_table(conn)

    logger.info("Подключаюсь к Matrix...")
    token = matrix_login()

    logger.info("Загружаю Matrix-пользователей...")
    matrix_users = matrix_get_real_users(token)
    logger.info(f"  Реальных аккаунтов: {len(matrix_users)}")

    logger.info("Загружаю сотрудников из TG...")
    employees = get_tg_employees(conn)
    logger.info(f"  Активных сотрудников: {len(employees)}")

    already_invited = set()
    if not args.resend:
        already_invited = get_already_invited(conn)
        logger.info(f"  Уже приглашённых: {len(already_invited)}")

    sent = 0
    no_matrix = 0

    for emp in employees:
        if sent >= args.batch:
            logger.info(f"Лимит {args.batch} достигнут, остальные — в следующий запуск")
            break

        tg_uid = emp["user_id"]
        if tg_uid in already_invited:
            continue
        if tg_uid in MANUAL_USER_MAP and MANUAL_USER_MAP[tg_uid] == "aleksei":
            continue

        matrix_id = resolve_matrix_id(tg_uid, emp["username"], matrix_users)
        if not matrix_id:
            logger.info(f"  ⚠ {emp['first_name']} ({emp['username']}) — нет Matrix-аккаунта")
            no_matrix += 1
            continue
        if matrix_id in SKIP_MATRIX_USERS:
            continue

        localpart = matrix_id.split(":")[0].lstrip("@")
        password = generate_password(10)

        if not args.dry_run:
            ok = matrix_reset_password(token, matrix_id, password)
            if not ok:
                logger.warning(f"  Не удалось сбросить пароль для {matrix_id}")
                continue

        chat_id, chat_title = pick_best_chat(emp["chat_ids"], emp["chat_titles"])
        msg = format_invite_message(emp["first_name"], tg_uid, localpart, password)

        if args.dry_run:
            logger.info(f"  [DRY-RUN] {emp['first_name']} → {matrix_id}, чат: '{chat_title}'")
            logger.info(f"    пароль: {password}")
        else:
            result = tg_send_message(chat_id, msg)
            if result.get("ok"):
                save_invite(conn, tg_uid, emp["username"], emp["first_name"], matrix_id, password, chat_id)
                logger.info(f"  ✅ {emp['first_name']} → {matrix_id}, чат: '{chat_title}'")
            else:
                logger.warning(f"  ❌ {emp['first_name']}: {result.get('description')}")
                continue
            time.sleep(1)

        sent += 1

    logger.info(f"\nИтого: отправлено {sent}, без Matrix-аккаунта {no_matrix}")
    conn.close()


def run_invite_rooms(args):
    conn = get_db()
    logger.info("Подключаюсь к Matrix...")
    token = matrix_login()

    room_map = matrix_get_rooms(token)
    logger.info(f"  Рабочих комнат: {len(room_map)}")

    matrix_users = matrix_get_real_users(token)
    employees = get_tg_employees(conn)
    invited_count = 0

    for emp in employees:
        matrix_id = resolve_matrix_id(emp["user_id"], emp["username"], matrix_users)
        if not matrix_id or matrix_id in SKIP_MATRIX_USERS:
            continue

        for title in emp["chat_titles"]:
            if not title:
                continue
            matrix_room_name = TG_TO_MATRIX_NAME.get(title, title)
            if matrix_room_name not in room_map:
                continue

            room_id = room_map[matrix_room_name]
            members = matrix_get_room_members(token, room_id)
            if matrix_id in members:
                continue

            if args.dry_run:
                logger.info(f"  [DRY-RUN] invite {matrix_id} → {matrix_room_name}")
            else:
                ok = matrix_invite_to_room(token, room_id, matrix_id)
                status = "✅" if ok else "❌"
                logger.info(f"  {status} {matrix_id} → {matrix_room_name}")
                invited_count += 1
            time.sleep(0.3)

    logger.info(f"\nПриглашений в комнаты: {invited_count}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Matrix auto-invite через TG-бота")
    parser.add_argument("--dry-run", action="store_true", help="Показать что сделает, без действий")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH, help=f"Макс. приглашений (default: {DEFAULT_BATCH})")
    parser.add_argument("--resend", action="store_true", help="Повторить приглашения")
    parser.add_argument("--invite-rooms", action="store_true", help="Только пригласить в комнаты")
    args = parser.parse_args()

    if args.invite_rooms:
        run_invite_rooms(args)
    else:
        run_invite(args)
