#!/usr/bin/env python3
"""
Сервис авторизации для /bom/ страницы.
Проверяет HMAC-токен из cookie или query-параметра.
Запуск: python3 auth_bom.py (порт 5555)
Nginx: auth_request /auth_bom;
"""
import hmac
import hashlib
import time
import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

load_dotenv()

PORT = 5555
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
# Секрет для HMAC — берём из BOT_TOKEN
SECRET = hashlib.sha256(f"bom_auth_{BOT_TOKEN}".encode()).digest()
TOKEN_TTL = 7 * 24 * 3600  # 7 дней


def generate_token(user_id: int) -> str:
    """Генерация токена: user_id.timestamp.hmac"""
    ts = int(time.time())
    msg = f"{user_id}.{ts}"
    sig = hmac.new(SECRET, msg.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{user_id}.{ts}.{sig}"


def verify_token(token: str) -> dict:
    """Проверка токена. Возвращает {'valid': bool, 'user_id': int, 'reason': str}"""
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return {'valid': False, 'reason': 'bad_format'}

        user_id = int(parts[0])
        ts = int(parts[1])
        sig = parts[2]

        # Проверяем HMAC
        msg = f"{user_id}.{ts}"
        expected = hmac.new(SECRET, msg.encode(), hashlib.sha256).hexdigest()[:32]
        if not hmac.compare_digest(sig, expected):
            return {'valid': False, 'reason': 'bad_signature'}

        # Проверяем срок
        if time.time() - ts > TOKEN_TTL:
            return {'valid': False, 'reason': 'expired'}

        return {'valid': True, 'user_id': user_id, 'reason': 'ok'}

    except (ValueError, IndexError):
        return {'valid': False, 'reason': 'parse_error'}


# HTML страница для неавторизованных
LOGIN_PAGE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Доступ ограничен</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
       background: #f0f2f5; display: flex; justify-content: center; align-items: center;
       min-height: 100vh; color: #1a1a2e; }
.card { background: #fff; border-radius: 12px; padding: 40px; max-width: 420px;
        box-shadow: 0 2px 16px rgba(0,0,0,0.08); text-align: center; }
.card h1 { font-size: 20px; margin-bottom: 12px; }
.card p { font-size: 14px; color: #666; line-height: 1.6; margin-bottom: 16px; }
.card .bot-link { display: inline-block; background: #e94560; color: #fff;
                  padding: 12px 24px; border-radius: 8px; text-decoration: none;
                  font-weight: 600; font-size: 14px; }
.card .bot-link:hover { background: #c73050; }
.card .reason { font-size: 12px; color: #999; margin-top: 12px; }
</style>
</head>
<body>
<div class="card">
    <h1>🔒 Доступ ограничен</h1>
    <p>Для просмотра состава продукции<br>запросите ссылку у бота.</p>
    <a class="bot-link" href="https://t.me/AI_FRUM_NF_bot?start=bom">Открыть бота → /bom</a>
    <div class="reason">REASON_TEXT</div>
</div>
</body>
</html>"""


class AuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        # Nginx auth_request — проверяем заголовки
        if parsed.path == '/auth_check':
            # Получаем оригинальный URL из заголовка nginx
            original_uri = self.headers.get('X-Original-URI', '')
            cookie_header = self.headers.get('Cookie', '')

            token = None

            # 1. Ищем токен в query оригинального URI
            if '?' in original_uri:
                orig_qs = parse_qs(urlparse(original_uri).query)
                if 'token' in orig_qs:
                    token = orig_qs['token'][0]

            # 2. Ищем в cookie
            if not token and cookie_header:
                for part in cookie_header.split(';'):
                    part = part.strip()
                    if part.startswith('bom_token='):
                        token = part[len('bom_token='):]
                        break

            if not token:
                self.send_response(401)
                self.end_headers()
                return

            result = verify_token(token)
            if result['valid']:
                self.send_response(200)
                self.end_headers()
            else:
                self.send_response(401)
                self.end_headers()
            return

        # Страница установки cookie из ?token= и редиректа
        if parsed.path == '/bom_login':
            qs = parse_qs(parsed.query)
            token = qs.get('token', [None])[0]

            if token:
                result = verify_token(token)
                if result['valid']:
                    # Устанавливаем cookie и редиректим на /bom/
                    self.send_response(302)
                    self.send_header('Set-Cookie',
                                     f'bom_token={token}; Path=/bom; Max-Age={TOKEN_TTL}; HttpOnly; SameSite=Lax')
                    self.send_header('Location', '/bom/')
                    self.end_headers()
                    return
                else:
                    reason = result.get('reason', 'unknown')
                    reason_text = {
                        'expired': 'Ссылка истекла. Запросите новую командой /bom',
                        'bad_signature': 'Невалидная ссылка',
                        'bad_format': 'Невалидная ссылка',
                    }.get(reason, 'Ошибка авторизации')
            else:
                reason_text = 'Токен не указан'

            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(LOGIN_PAGE.replace('REASON_TEXT', reason_text).encode())
            return

        # Страница "нет доступа"
        if parsed.path == '/bom_denied':
            reason = parse_qs(parsed.query).get('reason', [''])[0]
            reason_text = {
                'expired': 'Ссылка истекла. Запросите новую командой /bom',
                'no_token': 'Запросите ссылку у бота командой /bom',
            }.get(reason, 'Запросите доступ у бота')

            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(LOGIN_PAGE.replace('REASON_TEXT', reason_text).encode())
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        # Тихий лог — не спамим в stdout
        pass


def main():
    server = HTTPServer(('127.0.0.1', PORT), AuthHandler)
    print(f"Auth BOM сервис запущен на 127.0.0.1:{PORT}")
    server.serve_forever()


if __name__ == '__main__':
    main()
