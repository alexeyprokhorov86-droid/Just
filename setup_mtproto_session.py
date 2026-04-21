#!/usr/bin/env python3
"""
Одноразовая авторизация pyrogram user-client — чтобы бот мог скачивать
файлы > 20 MB (совещания, собеседования) через MTProto под твоим аккаунтом.

Запуск (интерактивно, через SSH):
    cd /home/admin/telegram_logger_bot
    python3 setup_mtproto_session.py

Скрипт:
1. Подхватывает TELEGRAM_API_ID / TELEGRAM_API_HASH из .env.
2. Использует SOCKS5 прокси 127.0.0.1:1080 (Amsterdam), чтобы достучаться
   до Telegram MTProto DC (напрямую в РФ заблокированы).
3. Запросит номер телефона, SMS-код, 2FA пароль (если включён).
4. Сохранит session в `mtproto.session` (в корне проекта, gitignored).

После «Session saved» можно запускать ботов — они подхватывают session
без повторной авторизации.
"""
import os
import sys
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from pyrogram import Client

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
PROXY = {
    "scheme": "socks5",
    "hostname": "127.0.0.1",
    "port": 1080,
}

print("=" * 60)
print("pyrogram user-client setup для Frumelad bot (длинные видео)")
print("=" * 60)
print(f"API_ID: {API_ID}")
print(f"Proxy: socks5 127.0.0.1:1080 (Amsterdam)")
print()
print("Введи свой номер телефона (как в Telegram, с +, напр. +7900...) —")
print("придёт SMS-код, введи его. Если включена 2FA — попросит cloud-пароль.")
print()

app = Client(
    "mtproto",                      # имя файла session (mtproto.session)
    api_id=API_ID,
    api_hash=API_HASH,
    proxy=PROXY,
    workdir=os.path.dirname(__file__),
)

with app:
    me = app.get_me()
    print()
    print("=" * 60)
    print(f"✅ Session saved: mtproto.session")
    print(f"   user: {me.first_name} (@{me.username or '-'}, id={me.id})")
    print(f"   phone: {me.phone_number}")
    print("=" * 60)
    print()
    print("Теперь бот может скачивать файлы до 2 GB через этот аккаунт.")
    print("Дальше я (Claude) напишу video-handler в tools/attachments — он")
    print("автоматически подхватит mtproto.session и станет качать большие")
    print("видео совещаний и собеседований.")
