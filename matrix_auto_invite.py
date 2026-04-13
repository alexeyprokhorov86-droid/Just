"""
matrix_auto_invite.py - Invite employees to Element via TG bot.

Groups by TG chat, sends one message per chat with all credentials,
then invites each person to ALL their Matrix rooms at once.

Usage:
  python3 matrix_auto_invite.py --dry-run
  python3 matrix_auto_invite.py
  python3 matrix_auto_invite.py --resend
  python3 matrix_auto_invite.py --invite-rooms
"""

import os, sys, string, random, logging, argparse, time
from collections import defaultdict
import psycopg2, requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("matrix_auto_invite")

MATRIX_URL = os.environ.get("MATRIX_URL", "http://localhost:8008")
MATRIX_ADMIN_USER = os.environ.get("MATRIX_ADMIN_USER", "aleksei")
MATRIX_ADMIN_PASSWORD = os.environ.get("MATRIX_ADMIN_PASSWORD", "TempPass2026!")
DB_HOST = os.environ.get("DB_HOST", "172.20.0.2")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "knowledge_base")
DB_USER = os.environ.get("DB_USER", "knowledge")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

WORK_ROOMS = {
    "Бухгалтерия Фрумелад/НФ","Руководство (bridged)","Производство",
    "Априори & Фрумелад/НФ","Секретариат","HR-Фрумелад/НФ",
    "Фрумелад задачи на разработку BSG","Торты Отгрузки",
    "Фрумелад поддержка BSG","Дизайн упаковки",
    "Новые продукты и конкуренты","БЗ Производство Chat","БЗ Производство",
    "БЗ R&D","БЗ R&D Chat","БЗ Бухгалтерия","БЗ Бухгалтерия Chat",
    "БЗ Закупки Chat","БЗ Склад","БЗ Склад Chat",
    "Подбор Персонала Внешний","Отчеты по аутсорсингу",
    "R&D ~ общая рабочая группа",
    "KELIN - кондитерская Прохорова","БЗ инструкции производство",
    "Закупки","Закупки - Упаковка","Продажи на ярды",
}
TG_TO_MATRIX_NAME = {"Руководство":"Руководство (bridged)","Дизайн упаковки Кондитерская Прохорова":"Дизайн упаковки"}
MANUAL_USER_MAP = {805598873:"aleksei", 1058481218:"irina.prokhorova"}
SKIP_MATRIX_USERS = {"@bot:frumelad.ru","@aleksei:frumelad.ru"}
SPACE_ROOM_ID = "!hRnxoPZwyiPRobHsCy:frumelad.ru"
ELEMENT_ANDROID = "https://play.google.com/store/apps/details?id=io.element.android.x"
ELEMENT_IOS = "https://apps.apple.com/app/element-x-secure-chat-call/id1631335820"
ELEMENT_WEB = "https://app.element.io"
CHAT_PRIORITY = ["Производство","Руководство","Закупки","Бухгалтерия","HR","Секретариат","R&D","Торты","Дизайн","Продажи"]

def get_db():
    return psycopg2.connect(host=DB_HOST,port=DB_PORT,dbname=DB_NAME,user=DB_USER,password=DB_PASSWORD)

def generate_password(length=10):
    chars = string.ascii_letters + string.digits
    pwd = [random.choice(string.ascii_uppercase),random.choice(string.ascii_lowercase),random.choice(string.digits)]
    pwd += [random.choice(chars) for _ in range(length-3)]
    random.shuffle(pwd)
    return "".join(pwd)

def tg_send_message(chat_id, text, parse_mode="Markdown"):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    proxies = {}
    proxy_url = os.environ.get("PROXY_URL")
    if proxy_url:
        proxies = {"http":proxy_url,"https":proxy_url}
    resp = requests.post(url, json={"chat_id":chat_id,"text":text,"parse_mode":parse_mode,"disable_web_page_preview":True}, proxies=proxies, timeout=15)
    return resp.json()

def matrix_login():
    resp = requests.post(f"{MATRIX_URL}/_matrix/client/v3/login",json={"type":"m.login.password","user":MATRIX_ADMIN_USER,"password":MATRIX_ADMIN_PASSWORD},timeout=10)
    data = resp.json()
    if "access_token" not in data:
        logger.error(f"Matrix login failed: {data}"); sys.exit(1)
    return data["access_token"]

def matrix_get_real_users(token):
    users = {}; _from = "0"
    while True:
        resp = requests.get(f"{MATRIX_URL}/_synapse/admin/v2/users",headers={"Authorization":f"Bearer {token}"},params={"limit":100,"from":_from},timeout=15)
        data = resp.json()
        for u in data.get("users",[]):
            name = u["name"]
            if not name.startswith("@telegram_"):
                users[name.split(":")[0].lstrip("@").lower()] = name
        if not data.get("next_token"): break
        _from = data["next_token"]
    return users

def matrix_reset_password(token, mid, pw):
    return requests.put(f"{MATRIX_URL}/_synapse/admin/v2/users/{mid}",headers={"Authorization":f"Bearer {token}"},json={"password":pw,"logout_devices":False},timeout=10).status_code==200

def matrix_get_rooms(token):
    rooms = {}; _from = 0
    while True:
        resp = requests.get(f"{MATRIX_URL}/_synapse/admin/v1/rooms",headers={"Authorization":f"Bearer {token}"},params={"limit":100,"from":_from},timeout=15)
        data = resp.json()
        for r in data.get("rooms",[]):
            name = r.get("name")
            if name and name in WORK_ROOMS: rooms[name] = r["room_id"]
        if len(data.get("rooms",[]))<100: break
        _from += 100
    return rooms

def matrix_get_room_members(token, room_id):
    return set(requests.get(f"{MATRIX_URL}/_synapse/admin/v1/rooms/{room_id}/members",headers={"Authorization":f"Bearer {token}"},timeout=10).json().get("members",[]))

def matrix_invite_to_room(token, room_id, user_id):
    return requests.post(f"{MATRIX_URL}/_matrix/client/v3/rooms/{room_id}/invite",headers={"Authorization":f"Bearer {token}"},json={"user_id":user_id},timeout=10).status_code in (200,403)

def matrix_user_has_devices(token, matrix_user_id):
    """Проверить, заходил ли пользователь (есть ли у него устройства)."""
    resp = requests.get(f"{MATRIX_URL}/_synapse/admin/v2/users/{matrix_user_id}/devices",
        headers={"Authorization":f"Bearer {token}"},timeout=10)
    return len(resp.json().get("devices",[])) > 0

def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS matrix_invites (
        tg_user_id BIGINT PRIMARY KEY, tg_username VARCHAR(255), tg_first_name VARCHAR(255),
        matrix_id VARCHAR(255), matrix_password VARCHAR(255),
        invite_sent_at TIMESTAMP, invite_chat_id BIGINT, created_at TIMESTAMP DEFAULT NOW())""")
    conn.commit(); cur.close()

def get_already_invited(conn):
    cur = conn.cursor()
    cur.execute("SELECT tg_user_id FROM matrix_invites WHERE invite_sent_at IS NOT NULL")
    r = {row[0] for row in cur.fetchall()}; cur.close(); return r

def save_invite(conn, uid, uname, fname, mid, pw, cid):
    cur = conn.cursor()
    cur.execute("""INSERT INTO matrix_invites (tg_user_id,tg_username,tg_first_name,matrix_id,matrix_password,invite_sent_at,invite_chat_id)
        VALUES (%s,%s,%s,%s,%s,NOW(),%s) ON CONFLICT (tg_user_id) DO UPDATE SET
        matrix_password=EXCLUDED.matrix_password, invite_sent_at=NOW(), invite_chat_id=EXCLUDED.invite_chat_id""",(uid,uname,fname,mid,pw,cid))
    conn.commit(); cur.close()

def get_tg_employees_by_chat(conn):
    cur = conn.cursor()
    cur.execute("""SELECT ur.user_id,ur.username,ur.first_name,ur.chat_id,m.chat_title
        FROM tg_user_roles ur JOIN tg_chats_metadata m ON m.chat_id=ur.chat_id
        WHERE ur.is_active=true AND ur.first_name NOT IN ('Group') AND COALESCE(ur.username,'')!='GroupAnonymousBot'
        ORDER BY m.chat_title,ur.first_name""")
    employees = {}; chat_members = defaultdict(set); chat_titles_map = {}
    for uid,username,first_name,chat_id,chat_title in cur.fetchall():
        if uid not in employees:
            employees[uid] = {"username":username,"first_name":first_name,"chat_ids":set(),"chat_titles":set()}
        employees[uid]["chat_ids"].add(chat_id)
        employees[uid]["chat_titles"].add(chat_title)
        chat_members[chat_id].add(uid)
        chat_titles_map[chat_id] = chat_title
    cur.close()
    return employees, chat_members, chat_titles_map

def resolve_matrix_id(uid, username, matrix_users):
    if uid in MANUAL_USER_MAP: return matrix_users.get(MANUAL_USER_MAP[uid])
    if username: return matrix_users.get(username.lower())
    return None

def sort_chats(chat_ids, titles_map):
    def key(cid):
        t = titles_map.get(cid,"")
        for i,p in enumerate(CHAT_PRIORITY):
            if p.lower() in t.lower(): return i
        return 100
    return sorted(chat_ids, key=key)

def format_group_invite(people_data):
    mentions = [f"[{fn}](tg://user?id={uid})" for fn,uid,_,_ in people_data]
    creds = [f"• {fn} — логин: `{lp}`, пароль: `{pw}`" for fn,_,lp,pw in people_data]
    return (f"👋 {', '.join(mentions)}\n\n"
        f"Мы подключаем корпоративный мессенджер Element.\n"
        f"Вам созданы аккаунты:\n\n" + "\n".join(creds) + "\n\n"
        f"📱 *Android:* [скачать Element X]({ELEMENT_ANDROID})\n"
        f"📱 *iPhone:* [скачать Element X]({ELEMENT_IOS})\n"
        f"💻 *На компьютере:* откройте {ELEMENT_WEB}\n\n"
        f"При входе нажмите *«Изменить сервер»* и введите:\n"
        f"Сервер: `matrix.frumelad.ru`\n\n"
        f"По вопросам → @GreyArea8")

def run_invite(args):
    conn = get_db(); ensure_table(conn)
    token = matrix_login()
    matrix_users = matrix_get_real_users(token)
    room_map = matrix_get_rooms(token)
    employees, chat_members, chat_titles_map = get_tg_employees_by_chat(conn)
    already_invited = set() if args.resend else get_already_invited(conn)

    logger.info(f"Matrix аккаунтов: {len(matrix_users)}, комнат: {len(room_map)}, сотрудников TG: {len(employees)}, уже приглашено: {len(already_invited)}")

    to_invite = {}; already_active = {}; no_matrix = []
    for uid, emp in employees.items():
        if uid in already_invited: continue
        if uid in MANUAL_USER_MAP and MANUAL_USER_MAP[uid]=="aleksei": continue
        mid = resolve_matrix_id(uid, emp["username"], matrix_users)
        if not mid or mid in SKIP_MATRIX_USERS:
            if not mid and emp["username"]: no_matrix.append(f"{emp['first_name']}({emp['username']})")
            continue
        lp = mid.split(":")[0].lstrip("@")
        entry = {"matrix_id":mid,"localpart":lp,"first_name":emp["first_name"],"username":emp["username"],"chat_ids":emp["chat_ids"],"chat_titles":emp["chat_titles"]}

        # Проверяем: уже заходил в Matrix?
        if matrix_user_has_devices(token, mid):
            already_active[uid] = entry
            logger.info(f"  ✓ {emp['first_name']} ({lp}) — уже в Matrix, только invite в комнаты")
        else:
            entry["password"] = generate_password()
            to_invite[uid] = entry

    if no_matrix: logger.info(f"Без Matrix-аккаунта: {', '.join(no_matrix)}")

    # Сначала обрабатываем тех, кто УЖЕ в Matrix — Space + комнаты
    total_rooms = 0
    space_members = matrix_get_room_members(token, SPACE_ROOM_ID)
    if already_active:
        logger.info(f"\n{'='*50}\nУже в Matrix ({len(already_active)} чел.) — приглашаю в Space и комнаты:")
        for uid, d in already_active.items():
            # Space invite
            if d["matrix_id"] not in space_members:
                if args.dry_run:
                    logger.info(f"  [DRY-RUN] {d['first_name']} → Space Фрумелад")
                else:
                    ok = matrix_invite_to_room(token, SPACE_ROOM_ID, d["matrix_id"])
                    logger.info(f"  {'✅' if ok else '❌'} {d['first_name']} → Space Фрумелад")
                    total_rooms += 1; time.sleep(0.2)
            # Комнаты
            for t in d["chat_titles"]:
                if not t: continue
                mn = TG_TO_MATRIX_NAME.get(t, t)
                if mn not in room_map: continue
                rid = room_map[mn]
                members = matrix_get_room_members(token, rid)
                if d["matrix_id"] in members: continue
                if args.dry_run:
                    logger.info(f"  [DRY-RUN] {d['first_name']} → {mn}")
                else:
                    ok = matrix_invite_to_room(token, rid, d["matrix_id"])
                    logger.info(f"  {'✅' if ok else '❌'} {d['first_name']} → {mn}")
                    total_rooms += 1; time.sleep(0.2)

    if not to_invite:
        logger.info(f"\nНовых для приглашения нет. В комнаты: {total_rooms}")
        conn.close(); return

    # Группируем НОВЫХ по чатам
    chat_to_uninvited = defaultdict(list)
    for uid in to_invite:
        for cid in to_invite[uid]["chat_ids"]:
            chat_to_uninvited[cid].append(uid)

    sorted_chats = sort_chats(list(chat_to_uninvited.keys()), chat_titles_map)
    invited_users = set(); total_sent = 0

    for chat_id in sorted_chats:
        chat_title = chat_titles_map.get(chat_id, str(chat_id))
        matrix_room_name = TG_TO_MATRIX_NAME.get(chat_title, chat_title)
        if matrix_room_name not in room_map:
            continue
        people = [uid for uid in chat_to_uninvited[chat_id] if uid not in invited_users]
        if not people: continue
        logger.info(f"\n── {chat_title} ({len(people)} чел.) ──")

        # Сброс паролей
        people_data = []
        for uid in people:
            d = to_invite[uid]
            if not args.dry_run:
                if not matrix_reset_password(token, d["matrix_id"], d["password"]):
                    logger.warning(f"  ❌ пароль {d['matrix_id']}"); continue
            people_data.append((d["first_name"], uid, d["localpart"], d["password"]))
        if not people_data: continue

        # Отправка в TG
        msg = format_group_invite(people_data)
        if args.dry_run:
            logger.info(f"  [DRY-RUN] Сообщение в '{chat_title}':")
            for fn,uid,lp,pw in people_data: logger.info(f"    {fn} → @{lp}:frumelad.ru, пароль: {pw}")
        else:
            result = tg_send_message(chat_id, msg)
            if result.get("ok"):
                logger.info(f"  ✅ Отправлено в '{chat_title}' ({len(people_data)} чел.)")
                for fn,uid,lp,pw in people_data:
                    save_invite(conn, uid, to_invite[uid]["username"], fn, to_invite[uid]["matrix_id"], pw, chat_id)
            else:
                logger.warning(f"  ❌ '{chat_title}': {result.get('description')}"); continue
            time.sleep(2)

        for fn,uid,lp,pw in people_data: invited_users.add(uid)
        total_sent += len(people_data)

        # Приглашение в Space + ВСЕ комнаты
        for fn,uid,lp,pw in people_data:
            d = to_invite[uid]
            # Space
            if d["matrix_id"] not in space_members:
                if args.dry_run:
                    logger.info(f"    [DRY-RUN] {fn} → Space Фрумелад")
                else:
                    ok = matrix_invite_to_room(token, SPACE_ROOM_ID, d["matrix_id"])
                    logger.info(f"    {'✅' if ok else '❌'} {fn} → Space Фрумелад")
                    total_rooms += 1; time.sleep(0.2)
            # Комнаты
            for t in d["chat_titles"]:
                if not t: continue
                mn = TG_TO_MATRIX_NAME.get(t, t)
                if mn not in room_map: continue
                rid = room_map[mn]
                if args.dry_run:
                    logger.info(f"    [DRY-RUN] {fn} → {mn}")
                else:
                    ok = matrix_invite_to_room(token, rid, d["matrix_id"])
                    logger.info(f"    {'✅' if ok else '❌'} {fn} → {mn}")
                    total_rooms += 1; time.sleep(0.2)

    logger.info(f"\n{'='*50}\nИтого: TG-приглашений {total_sent}, в комнаты {total_rooms}")
    conn.close()

def run_invite_rooms(args):
    conn = get_db(); token = matrix_login()
    room_map = matrix_get_rooms(token); matrix_users = matrix_get_real_users(token)
    employees,_,_ = get_tg_employees_by_chat(conn); cnt = 0

    # Сначала invite всех в Space Фрумелад
    space_members = matrix_get_room_members(token, SPACE_ROOM_ID)
    logger.info(f"Space Фрумелад: {len(space_members)} участников")
    for uid,emp in employees.items():
        mid = resolve_matrix_id(uid, emp["username"], matrix_users)
        if not mid or mid in SKIP_MATRIX_USERS: continue
        if mid not in space_members:
            if args.dry_run: logger.info(f"  [DRY-RUN] {mid} → Space Фрумелад")
            else:
                ok = matrix_invite_to_room(token, SPACE_ROOM_ID, mid)
                logger.info(f"  {'✅' if ok else '❌'} {mid} → Space Фрумелад"); cnt += 1
            time.sleep(0.3)

    # Затем invite в комнаты
    for uid,emp in employees.items():
        mid = resolve_matrix_id(uid, emp["username"], matrix_users)
        if not mid or mid in SKIP_MATRIX_USERS: continue
        for t in emp["chat_titles"]:
            if not t: continue
            mn = TG_TO_MATRIX_NAME.get(t,t)
            if mn not in room_map: continue
            members = matrix_get_room_members(token, room_map[mn])
            if mid in members: continue
            if args.dry_run: logger.info(f"  [DRY-RUN] {mid} → {mn}")
            else:
                ok = matrix_invite_to_room(token, room_map[mn], mid)
                logger.info(f"  {'✅' if ok else '❌'} {mid} → {mn}"); cnt += 1
            time.sleep(0.3)
    logger.info(f"\nВсего приглашений: {cnt}"); conn.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run",action="store_true")
    p.add_argument("--resend",action="store_true")
    p.add_argument("--invite-rooms",action="store_true")
    a = p.parse_args()
    run_invite_rooms(a) if a.invite_rooms else run_invite(a)
