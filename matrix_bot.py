"""
Matrix Bot для Element X — обработка вопросов (RAG) и анализ вложений.

Импортирует функции анализа из bot.py, использует process_rag_query из rag_agent.py.
Работает как отдельный systemd-сервис параллельно с Telegram-ботом.
"""
import asyncio
import json
import os
import sys
import time
import logging
import threading
import tempfile
import psycopg2
import aiohttp

# Добавляем директорию проекта в path для импорта
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
import pathlib

env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

# Импорт функций анализа из bot.py
from bot import (
    analyze_pdf_with_gpt,
    analyze_image_with_gpt,
    analyze_excel_with_gpt,
    analyze_word_with_gpt,
    analyze_pptx_with_gpt,
    analyze_video_with_gemini,
    extract_text_from_pdf,
    extract_text_from_image,
    extract_csv_from_excel,
    extract_text_from_word,
    extract_text_from_pptx,
    extract_transcript_from_audio,
    build_analysis_prompt,
    gpt_client,
    upload_to_s3_background,
    S3_BUCKET,
)
from rag_agent import process_rag_query
from fact_extractor import extract_and_save_facts

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("matrix_bot")

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

MATRIX_URL = "http://127.0.0.1:8008"
MATRIX_USER = os.environ.get("MATRIX_BOT_USER", "@aleksei:frumelad.ru")
MATRIX_PASSWORD = os.environ.get("MATRIX_BOT_PASSWORD")
DB_HOST = os.environ.get("DB_HOST", "172.20.0.2")
DB_NAME = "knowledge_base"
DB_USER = "knowledge"
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SYNC_TOKEN_FILE = "/home/admin/matrix-data/matrix_bot_sync_token"

# BOM конфигурация
BOM_SERVER_URL = os.environ.get("BOM_SERVER_URL", "http://95.174.92.209")

# Bridged rooms — загружаем при старте, пропускаем их в listener-режиме
MAUTRIX_DB_NAME = "mautrix_telegram"
MAUTRIX_DB_USER = "mautrix_tg"
MAUTRIX_DB_PASSWORD = os.environ.get("MAUTRIX_DB_PASSWORD", "MautrixTG2026")

# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================


def get_db():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


def load_sync_token():
    try:
        with open(SYNC_TOKEN_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def save_sync_token(token):
    os.makedirs(os.path.dirname(SYNC_TOKEN_FILE), exist_ok=True)
    with open(SYNC_TOKEN_FILE, "w") as f:
        f.write(token)


def get_bridged_rooms():
    """Получить room_id забриджённых комнат."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=MAUTRIX_DB_NAME,
            user=MAUTRIX_DB_USER, password=MAUTRIX_DB_PASSWORD
        )
        with conn.cursor() as cur:
            cur.execute("SELECT mxid FROM portal WHERE mxid IS NOT NULL")
            rooms = {row[0] for row in cur.fetchall()}
        conn.close()
        logger.info(f"Loaded {len(rooms)} bridged rooms")
        return rooms
    except Exception as e:
        logger.warning(f"Could not load bridged rooms: {e}")
        return set()


# ============================================================
# ОТПРАВКА СООБЩЕНИЙ В MATRIX
# ============================================================


async def send_message(session, access_token, room_id, text, reply_to=None):
    """Отправить текстовое сообщение в Matrix-комнату."""
    txn_id = f"matrix_bot_{int(time.time() * 1000)}_{id(text)}"

    content = {
        "msgtype": "m.text",
        "body": text,
    }

    if reply_to:
        content["m.relates_to"] = {
            "m.in_reply_to": {"event_id": reply_to}
        }

    # Разбиваем длинные сообщения
    if len(text) > 30000:
        parts = [text[i:i+30000] for i in range(0, len(text), 30000)]
        for i, part in enumerate(parts):
            await send_message(session, access_token, room_id, part)
        return

    url = f"{MATRIX_URL}/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}"
    async with session.put(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        json=content,
        ssl=False
    ) as resp:
        if resp.status != 200:
            err = await resp.text()
            logger.error(f"Failed to send message: {resp.status} {err}")


async def send_typing(session, access_token, room_id, typing=True):
    """Отправить индикатор 'печатает'."""
    url = f"{MATRIX_URL}/_matrix/client/v3/rooms/{room_id}/typing/{MATRIX_USER}"
    await session.put(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        json={"typing": typing, "timeout": 30000},
        ssl=False
    )


# ============================================================
# СКАЧИВАНИЕ ВЛОЖЕНИЙ ИЗ MATRIX
# ============================================================


async def download_matrix_media(session, access_token, mxc_url):
    """Скачать файл по mxc:// URL."""
    if not mxc_url or not mxc_url.startswith("mxc://"):
        return None

    # mxc://server/media_id -> /_matrix/media/v3/download/server/media_id
    parts = mxc_url[6:].split("/", 1)
    if len(parts) != 2:
        return None

    server, media_id = parts
    # Synapse 1.150+ требует authenticated media endpoint
    url = f"{MATRIX_URL}/_matrix/client/v1/media/download/{server}/{media_id}"

    async with session.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        ssl=False
    ) as resp:
        if resp.status == 200:
            return await resp.read()
        else:
            logger.error(f"Failed to download media: {resp.status}")
            return None


# ============================================================
# ОБРАБОТКА ВЛОЖЕНИЙ
# ============================================================


async def handle_media_message(session, access_token, room_id, event):
    """Обработка сообщения с вложением."""
    content = event.get("content", {})
    msgtype = content.get("msgtype", "")
    body = content.get("body", "")
    url = content.get("url", "")
    info = content.get("info", {})
    mimetype = info.get("mimetype", "")
    event_id = event.get("event_id", "")

    if not url:
        return

    # Определяем тип файла
    filename = body or ""
    filename_lower = filename.lower()
    media_type = None
    media_type_str = "document"

    if msgtype == "m.image" or mimetype.startswith("image/"):
        media_type = mimetype or "image/jpeg"
        media_type_str = "image"
    elif mimetype == "application/pdf" or filename_lower.endswith(".pdf"):
        media_type = "application/pdf"
        media_type_str = "pdf"
    elif filename_lower.endswith(('.xlsx', '.xls')):
        media_type = "excel"
        media_type_str = "excel"
    elif filename_lower.endswith(('.docx', '.doc')):
        media_type = "word"
        media_type_str = "word"
    elif filename_lower.endswith(('.pptx', '.ppt')):
        media_type = "powerpoint"
        media_type_str = "powerpoint"
    elif msgtype == "m.video" or filename_lower.endswith(('.mp4', '.avi', '.mov', '.mkv')):
        file_size = info.get("size", 0)
        if file_size > 40 * 1024 * 1024:
            await send_message(session, access_token, room_id,
                             "⚠️ Видео слишком большое для анализа (макс. 40MB).", reply_to=event_id)
            return
        media_type = "video"
        media_type_str = "video"
    elif msgtype == "m.audio":
        media_type = "audio"
        media_type_str = "audio"
    else:
        # Неподдерживаемый тип
        return

    # Скачиваем файл
    await send_typing(session, access_token, room_id)
    file_data = await download_matrix_media(session, access_token, url)

    if not file_data:
        await send_message(session, access_token, room_id,
                         "❌ Не удалось скачать файл.", reply_to=event_id)
        return

    # S3 upload в фоне
    if S3_BUCKET and len(file_data) > 0:
        threading.Thread(
            target=upload_to_s3_background,
            args=(bytes(file_data), "matrix_media", 0, filename, media_type_str),
            daemon=True
        ).start()

    # Анализируем файл
    try:
        media_analysis = ""
        content_text = ""

        if media_type in ("audio",) or msgtype == "m.audio":
            suffix = '.ogg' if 'ogg' in mimetype else '.mp3'
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(file_data)
                audio_path = tmp.name
            try:
                content_text = await extract_transcript_from_audio(audio_path)
                if content_text and gpt_client:
                    prompt = build_analysis_prompt(
                        "Голосовое сообщение", f"Транскрипция:\n{content_text}",
                        "", filename
                    )
                    response = gpt_client.chat.completions.create(
                        model="openai/gpt-4.1",
                        max_tokens=4500,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    media_analysis = response.choices[0].message.content
                elif content_text:
                    media_analysis = f"Транскрипция: {content_text}"
                else:
                    media_analysis = "Не удалось распознать речь."
            finally:
                if os.path.exists(audio_path):
                    os.unlink(audio_path)

        elif media_type == "application/pdf":
            media_analysis = await analyze_pdf_with_gpt(file_data, filename, "")
            content_text = await extract_text_from_pdf(file_data)

        elif media_type and media_type.startswith("image/"):
            media_analysis = await analyze_image_with_gpt(file_data, media_type, "", filename)
            content_text = await extract_text_from_image(file_data, media_type)

        elif media_type == "excel":
            media_analysis = await analyze_excel_with_gpt(file_data, filename, "")
            content_text = await extract_csv_from_excel(file_data, filename)

        elif media_type == "word":
            media_analysis = await analyze_word_with_gpt(file_data, filename, "")
            content_text = await extract_text_from_word(file_data)

        elif media_type == "powerpoint":
            media_analysis = await analyze_pptx_with_gpt(file_data, filename, "")
            content_text = await extract_text_from_pptx(file_data)

        elif media_type == "video":
            media_analysis = await analyze_video_with_gemini(file_data, filename, "")
            with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as tmp:
                tmp.write(file_data)
                video_path = tmp.name
            try:
                content_text = await extract_transcript_from_audio(video_path)
            finally:
                if os.path.exists(video_path):
                    os.unlink(video_path)

        # Отправляем результат анализа
        if media_analysis:
            header = f"📎 **Анализ: {filename}**\n\n" if filename else "📎 **Анализ файла:**\n\n"
            await send_message(session, access_token, room_id,
                             header + media_analysis, reply_to=event_id)

            # Автоизвлечение фактов
            if len(media_analysis) > 50:
                try:
                    await extract_and_save_facts(
                        media_analysis, source=f"matrix_document:{filename or media_type_str}"
                    )
                except Exception as e:
                    logger.debug(f"Fact extraction error: {e}")
        else:
            await send_message(session, access_token, room_id,
                             f"⚠️ Не удалось проанализировать {filename or 'файл'}.",
                             reply_to=event_id)

    except Exception as e:
        logger.error(f"Media analysis error: {e}")
        await send_message(session, access_token, room_id,
                         f"❌ Ошибка при анализе: {str(e)[:200]}", reply_to=event_id)


# ============================================================
# ОБРАБОТКА КОМАНД
# ============================================================


async def handle_command(session, access_token, room_id, event, command, args):
    """Обработка команд бота."""
    event_id = event.get("event_id", "")
    sender = event.get("sender", "")

    if command == "help":
        help_text = (
            "🤖 **Команды бота:**\n\n"
            "• Просто напишите вопрос — RAG-поиск по базе знаний\n"
            "• `/search <запрос>` — поиск по ключевым словам\n"
            "• `/bom` — ссылка на отчёт BOM\n"
            "• `/help` — эта справка\n\n"
            "📎 Отправьте файл (PDF, Excel, Word, фото, видео) — получите анализ"
        )
        await send_message(session, access_token, room_id, help_text, reply_to=event_id)

    elif command == "search":
        if not args:
            await send_message(session, access_token, room_id,
                             "🔍 Использование: /search <запрос>", reply_to=event_id)
            return

        await send_typing(session, access_token, room_id)
        try:
            response = await process_rag_query(args, "")
            await send_message(session, access_token, room_id, response, reply_to=event_id)
        except Exception as e:
            logger.error(f"Search error: {e}")
            await send_message(session, access_token, room_id,
                             "❌ Ошибка поиска.", reply_to=event_id)

    elif command == "bom":
        try:
            from auth_bom import generate_token
            # Для Matrix используем hash sender как user_id
            user_hash = abs(hash(sender)) % (10 ** 9)
            token = generate_token(user_hash)
            url = f"{BOM_SERVER_URL}/bom_login?token={token}"
            await send_message(session, access_token, room_id,
                             f"📋 **Состав продукции**\n\n"
                             f"Ваша ссылка (действует 7 дней):\n{url}",
                             reply_to=event_id)
        except Exception as e:
            logger.error(f"BOM error: {e}")
            await send_message(session, access_token, room_id,
                             "❌ Ошибка генерации ссылки BOM.", reply_to=event_id)

    else:
        await send_message(session, access_token, room_id,
                         f"Неизвестная команда: /{command}\nНапишите /help для справки.",
                         reply_to=event_id)


# ============================================================
# ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ (RAG)
# ============================================================


async def handle_text_message(session, access_token, room_id, event):
    """Обработка текстового сообщения — RAG-поиск."""
    content = event.get("content", {})
    body = content.get("body", "").strip()
    event_id = event.get("event_id", "")
    sender = event.get("sender", "")

    if not body:
        return

    # Пропускаем команды бриджа
    if body.startswith("!tg "):
        return

    # Пропускаем короткие приветствия
    if body.lower() in ('привет', 'hi', 'hello', 'старт'):
        await send_message(session, access_token, room_id,
                         "👋 Привет! Задайте вопрос — я поищу ответ в базе данных.\n"
                         "Или отправьте файл для анализа.\n"
                         "Команда /help — список команд.",
                         reply_to=event_id)
        return

    # Определяем: это личное сообщение боту или упоминание?
    # В Matrix проверяем — это DM (2 участника) или групповая комната
    # В групповых комнатах реагируем только на упоминание
    # Пока для простоты: реагируем на все сообщения в комнатах где бот есть

    await send_typing(session, access_token, room_id)

    try:
        response = await process_rag_query(body, "")

        if response:
            await send_message(session, access_token, room_id, response, reply_to=event_id)
            logger.info(f"RAG response: {len(response)} chars to {sender}")
        else:
            await send_message(session, access_token, room_id,
                             "Не удалось найти ответ.", reply_to=event_id)

    except Exception as e:
        logger.error(f"RAG error: {e}")
        await send_message(session, access_token, room_id,
                         "❌ Ошибка при обработке запроса.", reply_to=event_id)


# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================


async def main():
    """Главный цикл Matrix-бота."""
    logger.info("Matrix bot starting...")

    # Загружаем bridged rooms — в них не отвечаем на обычные сообщения
    # (там работает Telegram-бот через бридж)
    bridged_rooms = get_bridged_rooms()

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
            logger.error(f"Login failed: {login_data}")
            sys.exit(1)

        access_token = login_data["access_token"]
        logger.info(f"Logged in as {MATRIX_USER}")

        sync_token = load_sync_token()
        room_names = {}

        # Если нет sync_token — делаем initial sync без обработки сообщений
        if not sync_token:
            logger.info("Initial sync (skipping old messages)...")
            resp = await session.get(
                f"{MATRIX_URL}/_matrix/client/v3/sync",
                headers={"Authorization": f"Bearer {access_token}"},
                params={
                    "timeout": "1000",
                    "filter": json.dumps({
                        "room": {
                            "timeline": {"limit": 0},
                            "state": {"types": ["m.room.name"]}
                        }
                    })
                },
                ssl=False,
                timeout=aiohttp.ClientTimeout(total=30)
            )
            data = await resp.json()
            sync_token = data.get("next_batch")
            if sync_token:
                save_sync_token(sync_token)
                logger.info("Initial sync complete, listening for new messages...")

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
                if not new_token:
                    continue

                rooms = data.get("rooms", {}).get("join", {})
                for room_id, room_data in rooms.items():
                    # Обновляем имена комнат
                    for event in room_data.get("state", {}).get("events", []):
                        if event.get("type") == "m.room.name":
                            room_names[room_id] = event.get("content", {}).get("name", room_id)

                    room_name = room_names.get(room_id, room_id)

                    for event in room_data.get("timeline", {}).get("events", []):
                        sender = event.get("sender", "")
                        event_type = event.get("type", "")

                        # Пропускаем собственные сообщения
                        if sender == MATRIX_USER:
                            continue

                        # Пропускаем ghost-пользователей (bridged из Telegram)
                        if sender.startswith("@telegram_") and sender.endswith(":frumelad.ru"):
                            continue

                        # Пропускаем бота бриджа
                        if sender == "@telegrambot:frumelad.ru":
                            continue

                        if event_type == "m.room.message":
                            content = event.get("content", {})
                            msgtype = content.get("msgtype", "")
                            body = content.get("body", "")

                            # Обработка вложений
                            if msgtype in ("m.image", "m.file", "m.video", "m.audio"):
                                logger.info(f"Media from {sender} in {room_name}: {body}")
                                asyncio.create_task(
                                    handle_media_message(session, access_token, room_id, event)
                                )
                                continue

                            # Обработка команд
                            if body.startswith("/"):
                                parts = body[1:].split(None, 1)
                                command = parts[0].lower()
                                args = parts[1] if len(parts) > 1 else ""
                                logger.info(f"Command /{command} from {sender} in {room_name}")
                                asyncio.create_task(
                                    handle_command(session, access_token, room_id, event, command, args)
                                )
                                continue

                            # Текстовые сообщения — RAG
                            # В bridged-комнатах не отвечаем на обычные сообщения
                            if room_id in bridged_rooms:
                                continue

                            if msgtype == "m.text" and body:
                                logger.info(f"RAG query from {sender} in {room_name}: {body[:50]}")
                                asyncio.create_task(
                                    handle_text_message(session, access_token, room_id, event)
                                )

                save_sync_token(new_token)
                sync_token = new_token

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Sync error: {e}")
                await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
