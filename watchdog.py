"""
Watchdog для мониторинга Telegram Logger Bot.
Запускается через cron каждые 5 минут.
"""

import json
import os
import subprocess
import time
import requests
import psycopg2
import pathlib
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Загружаем переменные окружения
# Ищем .env в директории скрипта или в текущей директории
env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Файлы для хранения состояния (настраиваемые через env или в директории скрипта)
SCRIPT_DIR = pathlib.Path(__file__).parent
STATE_FILE = os.getenv("WATCHDOG_STATE_FILE", str(SCRIPT_DIR / "watchdog_state.txt"))
LOG_FILE = os.getenv("WATCHDOG_LOG_FILE", str(SCRIPT_DIR / "watchdog.log"))

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

def check_service_running(service_name: str = "telegram-logger") -> bool:
    """Проверяет что systemd сервис запущен."""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service_name],
            capture_output=True, text=True, timeout=10
        )
        return result.stdout.strip() == "active"
    except Exception as e:
        log(f"Ошибка проверки сервиса {service_name}: {e}")
        return False


def restart_service(service_name: str = "telegram-logger") -> bool:
    """Перезапускает сервис."""
    try:
        subprocess.run(["sudo", "systemctl", "restart", service_name], timeout=30)
        log(f"Сервис {service_name} перезапущен")
        return True
    except Exception as e:
        log(f"Ошибка перезапуска {service_name}: {e}")
        return False


AUTO_FIX_SH = str(SCRIPT_DIR / "auto_fix.sh")


def trigger_auto_fix_service_down(service_name: str) -> None:
    """Если обычный рестарт не помог — вызываем автономного Claude-агента.

    Собираем контекст (последние 50 строк journalctl + последние 5 коммитов)
    и запускаем auto_fix.sh в фоне, чтобы watchdog не блокировал cron-слот.
    """
    ctx_path = f"/tmp/watchdog_{service_name}_ctx.json"
    try:
        journal = subprocess.run(
            ["journalctl", "-u", service_name, "-n", "50", "--no-pager"],
            capture_output=True, text=True, timeout=30,
        ).stdout
    except Exception as e:
        journal = f"(journalctl failed: {e})"
    try:
        gitlog = subprocess.run(
            ["git", "log", "-5", "--oneline"],
            capture_output=True, text=True, timeout=10, cwd=str(SCRIPT_DIR),
        ).stdout
    except Exception as e:
        gitlog = f"(git log failed: {e})"
    ctx = {
        "trigger": "service_down",
        "service": service_name,
        "journal_tail_50": journal[-15000:],
        "git_log_5": gitlog,
        "detected_at": datetime.now().isoformat(),
    }
    try:
        with open(ctx_path, "w") as f:
            json.dump(ctx, f, ensure_ascii=False)
    except Exception as e:
        log(f"Не смог записать контекст {ctx_path}: {e}")
        return
    try:
        bg_log = str(SCRIPT_DIR / "auto_fix_bg.log")
        subprocess.Popen(
            f"nohup {AUTO_FIX_SH} service_down {ctx_path} >> {bg_log} 2>&1 &",
            shell=True, start_new_session=True,
        )
        log(f"auto_fix.sh запущен в фоне для service_down/{service_name}")
    except Exception as e:
        log(f"Не смог запустить auto_fix.sh: {e}")


def restart_with_auto_fix(service_name: str) -> bool:
    """Расширенный рестарт: обычный → wait 60s → если всё ещё down → auto_fix."""
    ok = restart_service(service_name)
    if not ok:
        trigger_auto_fix_service_down(service_name)
        return False
    time.sleep(60)
    if check_service_running(service_name):
        return True
    log(f"Сервис {service_name} всё ещё не active через 60с после restart — вызываем auto_fix")
    trigger_auto_fix_service_down(service_name)
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
    
    # 1. Проверяем telegram-logger
    if not check_service_running("telegram-logger"):
        alerts.append("❌ Сервис telegram-logger не запущен!")
        state["restart_count"] = state.get("restart_count", 0) + 1
        skip_stuck_update()
        if restart_with_auto_fix("telegram-logger"):
            alerts.append(f"🔄 telegram-logger перезапущен (перезапусков: {state['restart_count']})")
        else:
            alerts.append("❌ telegram-logger не поднялся, передан auto_fix")
    else:
        log("✅ telegram-logger запущен")

    # 2. Проверяем email-sync
    if not check_service_running("email-sync"):
        alerts.append("❌ Сервис email-sync не запущен!")
        if restart_with_auto_fix("email-sync"):
            alerts.append("🔄 email-sync перезапущен")
        else:
            alerts.append("❌ email-sync не поднялся, передан auto_fix")
    else:
        log("✅ email-sync запущен")
    
    # 3. Проверяем диск
    disk_ok, disk_percent = check_disk_space()
    if not disk_ok:
        if should_alert("disk", state):
            alerts.append(f"💾 Диск заполнен на {disk_percent}%!")
            mark_alerted("disk", state)
    else:
        log(f"✅ Диск: {disk_percent}% использовано")
    
    # 4. Проверяем БД
    if not check_db_connection():
        if should_alert("db", state):
            alerts.append("🗄 База данных недоступна!")
            mark_alerted("db", state)
    else:
        log("✅ База данных доступна")
    
    # 5. Проверяем ошибки в логах telegram-logger
    errors = check_service_errors()
    if errors:
        log(f"⚠️ Найдено {len(errors)} ошибок в логах")
        if "Error while parsing" in str(errors):
            log("Обнаружена ошибка парсинга — пробуем сбросить очередь")
            skip_stuck_update()
            restart_service("telegram-logger")
        if should_alert("errors", state, cooldown_minutes=60):
            alerts.append(f"⚠️ Ошибки в логах:\n{errors[0][:100]}...")
            mark_alerted("errors", state)
    else:
        log("✅ Критических ошибок нет")
    
    # 6. Отправляем алерты
    if alerts:
        send_alert("\n\n".join(alerts))
    
    # 7. Сохраняем состояние
    save_state(state)
    
    log("=== Watchdog завершён ===\n")

if __name__ == "__main__":
    main()
