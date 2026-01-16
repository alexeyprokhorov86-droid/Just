"""
Watchdog для мониторинга Telegram Logger Bot.
Запускается через cron каждые 5 минут.
"""

import os
import subprocess
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('/home/admin/telegram_logger_bot/.env')

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Файл для хранения состояния
STATE_FILE = "/home/admin/telegram_logger_bot/watchdog_state.txt"
LOG_FILE = "/home/admin/telegram_logger_bot/watchdog.log"

def log(message: str):
    """Записывает в лог."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"
    print(log_line)
    with open(LOG_FILE, "a") as f:
        f.write(log_line + "\n")

def send_alert(message: str):
    """Отправляет уведомление в Telegram."""
    if not BOT_TOKEN or not ADMIN_USER_ID:
        log(f"Не могу отправить алерт: {message}")
        return
    
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": ADMIN_USER_ID,
            "text": f"🚨 WATCHDOG ALERT\n\n{message}",
            "parse_mode": "HTML"
        }
        requests.post(url, data=data, timeout=10)
        log(f"Алерт отправлен: {message}")
    except Exception as e:
        log(f"Ошибка отправки алерта: {e}")

def check_service_running() -> bool:
    """Проверяет что сервис telegram-logger запущен."""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "telegram-logger"],
            capture_output=True, text=True, timeout=10
        )
        return result.stdout.strip() == "active"
    except Exception as e:
        log(f"Ошибка проверки сервиса: {e}")
        return False

def check_disk_space() -> tuple[bool, int]:
    """Проверяет свободное место на диске. Возвращает (ok, процент_использования)."""
    try:
        result = subprocess.run(
            ["df", "/", "--output=pcent"],
            capture_output=True, text=True, timeout=10
        )
        # Парсим вывод типа "Use%\n 59%"
        lines = result.stdout.strip().split('\n')
        if len(lines) >= 2:
            percent = int(lines[1].strip().replace('%', ''))
            return percent < 85, percent
        return True, 0
    except Exception as e:
        log(f"Ошибка проверки диска: {e}")
        return True, 0

def check_db_connection() -> bool:
    """Проверяет подключение к базе данных."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=10
        )
        conn.close()
        return True
    except Exception as e:
        log(f"Ошибка подключения к БД: {e}")
        return False

def check_recent_messages() -> tuple[bool, int]:
    """Проверяет были ли сообщения за последние 30 минут."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=10
        )
        with conn.cursor() as cur:
            # Считаем сообщения за последние 30 минут из всех чатов
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT 1 FROM tg_chats_metadata 
                    WHERE last_message_at > NOW() - INTERVAL '30 minutes'
                ) t
            """)
            count = cur.fetchone()[0]
        conn.close()
        return count > 0, count
    except Exception as e:
        log(f"Ошибка проверки сообщений: {e}")
        return True, 0  # Не алертим если не можем проверить

def check_service_errors() -> list[str]:
    """Проверяет логи на критические ошибки за последние 10 минут."""
    try:
        result = subprocess.run(
            ["journalctl", "-u", "telegram-logger", "--since", "10 minutes ago", 
             "--no-pager", "-p", "err"],
            capture_output=True, text=True, timeout=30
        )
        errors = []
        for line in result.stdout.split('\n'):
            if 'CRITICAL' in line or 'Error while parsing' in line:
                errors.append(line[:200])
        return errors[:3]  # Максимум 3 ошибки
    except Exception as e:
        log(f"Ошибка проверки логов: {e}")
        return []

def restart_service():
    """Перезапускает сервис."""
    try:
        subprocess.run(["sudo", "systemctl", "restart", "telegram-logger"], timeout=30)
        log("Сервис перезапущен")
        return True
    except Exception as e:
        log(f"Ошибка перезапуска: {e}")
        return False

def skip_stuck_update():
    """Пропускает застрявшее обновление."""
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset=-1"
        requests.get(url, timeout=10)
        log("Очередь обновлений сброшена")
        return True
    except Exception as e:
        log(f"Ошибка сброса очереди: {e}")
        return False

def get_state() -> dict:
    """Читает состояние из файла."""
    state = {"last_alert": {}, "restart_count": 0, "last_restart": None}
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                import json
                state = json.load(f)
    except:
        pass
    return state

def save_state(state: dict):
    """Сохраняет состояние в файл."""
    try:
        import json
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log(f"Ошибка сохранения состояния: {e}")

def should_alert(alert_type: str, state: dict, cooldown_minutes: int = 30) -> bool:
    """Проверяет нужно ли отправлять алерт (с учётом cooldown)."""
    last_alerts = state.get("last_alert", {})
    last_time = last_alerts.get(alert_type)
    
    if not last_time:
        return True
    
    try:
        last_dt = datetime.fromisoformat(last_time)
        return datetime.now() - last_dt > timedelta(minutes=cooldown_minutes)
    except:
        return True

def mark_alerted(alert_type: str, state: dict):
    """Отмечает что алерт отправлен."""
    if "last_alert" not in state:
        state["last_alert"] = {}
    state["last_alert"][alert_type] = datetime.now().isoformat()

def main():
    log("=== Watchdog запущен ===")
    state = get_state()
    alerts = []
    need_restart = False
    
    # 1. Проверяем сервис
    if not check_service_running():
        alerts.append("❌ Сервис telegram-logger не запущен!")
        need_restart = True
    else:
        log("✅ Сервис запущен")
    
    # 2. Проверяем диск
    disk_ok, disk_percent = check_disk_space()
    if not disk_ok:
        if should_alert("disk", state):
            alerts.append(f"💾 Диск заполнен на {disk_percent}%!")
            mark_alerted("disk", state)
    else:
        log(f"✅ Диск: {disk_percent}% использовано")
    
    # 3. Проверяем БД
    if not check_db_connection():
        if should_alert("db", state):
            alerts.append("🗄 База данных недоступна!")
            mark_alerted("db", state)
    else:
        log("✅ База данных доступна")
    
    # 4. Проверяем ошибки в логах
    errors = check_service_errors()
    if errors:
        log(f"⚠️ Найдено {len(errors)} ошибок в логах")
        if "Error while parsing" in str(errors):
            log("Обнаружена ошибка парсинга — пробуем сбросить очередь")
            skip_stuck_update()
            need_restart = True
        if should_alert("errors", state, cooldown_minutes=60):
            alerts.append(f"⚠️ Ошибки в логах:\n{errors[0][:100]}...")
            mark_alerted("errors", state)
    else:
        log("✅ Критических ошибок нет")
    
    # 5. Перезапускаем если нужно
    if need_restart:
        log("Требуется перезапуск...")
        state["restart_count"] = state.get("restart_count", 0) + 1
        state["last_restart"] = datetime.now().isoformat()
        
        skip_stuck_update()
        if restart_service():
            alerts.append(f"🔄 Бот перезапущен (перезапусков сегодня: {state['restart_count']})")
        else:
            alerts.append("❌ Не удалось перезапустить бота!")
    
    # 6. Отправляем алерты
    if alerts:
        send_alert("\n\n".join(alerts))
    
    # 7. Сохраняем состояние
    save_state(state)
    
    log("=== Watchdog завершён ===\n")

if __name__ == "__main__":
    main()
