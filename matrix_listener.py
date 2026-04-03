"""Matrix event listener — пишет сообщения из Matrix/Element в source_documents."""
import asyncio
import json
import os
import sys
import time
import psycopg2
import aiohttp

# Конфигурация
MATRIX_URL = "https://matrix.frumelad.ru"
MATRIX_USER = "@aleksei:frumelad.ru"
MATRIX_PASSWORD = os.environ.get("MATRIX_BOT_PASSWORD", "TempPass2026!")
DB_HOST = os.environ.get("DB_HOST", "172.20.0.2")
DB_NAME = "knowledge_base"
DB_USER = "knowledge"
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Prokhorov2025Secure")
SYNC_TOKEN_FILE = "/home/admin/synapse-data/matrix_sync_token"


def get_db():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


def save_message(conn, room_id, room_name, event):
    """Сохранить сообщение в source_documents."""
    sender = event.get("sender", "")
    content = event.get("content", {})
    body = content.get("body", "")
    event_id = event.get("event_id", "")
    ts = event.get("origin_server_ts", 0)

    if not body or not body.strip():
        return False

    source_ref = f"matrix:{room_id}:{event_id}"
    channel_ref = f"matrix_room_{room_id}"
    from_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts / 1000)) if ts else None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO source_documents
                    (source_kind, source_ref, title, body_text, doc_date,
                     author_name, author_ref, channel_ref, channel_name,
                     language, is_deleted, confidence, meta)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_kind, source_ref) DO NOTHING
            """, (
                'matrix_message',
                source_ref,
                None,
                body.strip(),
                from_ts,
                sender.split(":")[0].lstrip("@") if sender else "",
                sender,
                channel_ref,
                room_name or room_id,
                'ru',
                False,
                1.0,
                json.dumps({
                    'msgtype': content.get('msgtype'),
                    'room_id': room_id,
                    'event_id': event_id
                })
            ))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"[matrix] DB error: {e}")
        return False


def load_sync_token():
    try:
        with open(SYNC_TOKEN_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def save_sync_token(token):
    with open(SYNC_TOKEN_FILE, "w") as f:
        f.write(token)


async def main():
    print("[matrix_listener] Starting...")

    async with aiohttp.ClientSession() as session:
        # Логин
        login_resp = await session.post(
            f"{MATRIX_URL}/_matrix/client/v3/login",
            json={
                "type": "m.login.password",
                "user": MATRIX_USER.split(":")[0].lstrip("@"),
                "password": MATRIX_PASSWORD
            },
            ssl=False
        )
        login_data = await login_resp.json()
        if "access_token" not in login_data:
            print(f"[matrix_listener] Login failed: {login_data}")
            sys.exit(1)

        access_token = login_data["access_token"]
        print(f"[matrix_listener] Logged in as {MATRIX_USER}")

        conn = get_db()
        sync_token = load_sync_token()
        room_names = {}
        saved_count = 0

        while True:
            try:
                params = {
                    "timeout": "30000",
                    "filter": json.dumps({
                        "room": {
                            "timeline": {"limit": 50},
                            "state": {"types": ["m.room.name"]}
                        }
                    })
                }
                if sync_token:
                    params["since"] = sync_token

                resp = await session.get(
                    f"{MATRIX_URL}/_matrix/client/v3/sync",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params=params,
                    ssl=False,
                    timeout=aiohttp.ClientTimeout(total=60)
                )
                data = await resp.json()

                new_token = data.get("next_batch")
                if new_token:
                    # Обработка комнат
                    rooms = data.get("rooms", {}).get("join", {})
                    for room_id, room_data in rooms.items():
                        # Получаем название комнаты из state events
                        for event in room_data.get("state", {}).get("events", []):
                            if event.get("type") == "m.room.name":
                                room_names[room_id] = event.get("content", {}).get("name", room_id)

                        room_name = room_names.get(room_id, room_id)

                        # Обрабатываем сообщения
                        for event in room_data.get("timeline", {}).get("events", []):
                            if event.get("type") == "m.room.message":
                                if save_message(conn, room_id, room_name, event):
                                    saved_count += 1
                                    sender = event.get("sender", "")
                                    body_preview = event.get("content", {}).get("body", "")[:50]
                                    print(f"[matrix] #{saved_count}: {sender} in {room_name}: {body_preview}")

                    save_sync_token(new_token)
                    sync_token = new_token

            except Exception as e:
                print(f"[matrix_listener] Sync error: {e}")
                await asyncio.sleep(5)
                # Переподключаем БД
                try:
                    conn.close()
                except:
                    pass
                conn = get_db()


if __name__ == "__main__":
    asyncio.run(main())
