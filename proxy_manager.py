#!/usr/bin/env python3
"""
proxy_manager.py — Proxy health checker & failover manager.

Каждые CHECK_INTERVAL секунд проверяет все SOCKS5-прокси через curl к api.telegram.org.
Пишет активный URL прокси в ACTIVE_PROXY_FILE.
При смене активного прокси — перезапускает telegram-logger через systemctl.

Логика:
  1. Проверяем все прокси по порядку приоритета (Helsinki первый — быстрее).
  2. Первый рабочий становится активным.
  3. Если активный изменился — пишем в файл и рестартим бота.
  4. Если ни один не работает — пишем "NONE", бот не рестартится (ждём восстановления).
"""

import subprocess
import time
import logging
import sys
import json
from pathlib import Path
from datetime import datetime

# ─── Настройки ──────────────────────────────────────────────

PROXIES = [
    {
        "name": "Helsinki",
        "url": "socks5h://127.0.0.1:1081",
        "port": 1081,
        "service": "proxy-tunnel-helsinki",
    },
    {
        "name": "Amsterdam",
        "url": "socks5h://127.0.0.1:1080",
        "port": 1080,
        "service": "proxy-tunnel-amsterdam",
    },
]

CHECK_INTERVAL = 30          # секунды между проверками
CURL_TIMEOUT = 10            # таймаут curl в секундах
CHECK_URL = "https://api.telegram.org"
ACTIVE_PROXY_FILE = Path("/tmp/active_proxy.json")
BOT_SERVICE = "telegram-logger"
STATUS_FILE = Path("/tmp/proxy_status.json")

# ─── Логирование ────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [proxy_manager] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("proxy_manager")

# ─── Функции ────────────────────────────────────────────────


def check_proxy(proxy: dict) -> bool:
    """Проверяет прокси через curl. Возвращает True если работает."""
    try:
        result = subprocess.run(
            [
                "curl", "-s", "-o", "/dev/null",
                "-w", "%{http_code}",
                "--proxy", proxy["url"],
                "--connect-timeout", str(CURL_TIMEOUT),
                "--max-time", str(CURL_TIMEOUT),
                CHECK_URL,
            ],
            capture_output=True,
            text=True,
            timeout=CURL_TIMEOUT + 5,
        )
        http_code = result.stdout.strip()
        ok = http_code in ("200", "301", "302", "404")  # 404 тоже ОК — Telegram отвечает
        if ok:
            log.debug(f"  {proxy['name']}:{proxy['port']} — OK (HTTP {http_code})")
        else:
            log.warning(f"  {proxy['name']}:{proxy['port']} — FAIL (HTTP {http_code})")
        return ok
    except (subprocess.TimeoutExpired, Exception) as e:
        log.warning(f"  {proxy['name']}:{proxy['port']} — FAIL ({e})")
        return False


def read_active_proxy() -> dict | None:
    """Читает текущий активный прокси из файла."""
    try:
        data = json.loads(ACTIVE_PROXY_FILE.read_text())
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_active_proxy(proxy: dict | None):
    """Записывает активный прокси в файл."""
    if proxy is None:
        data = {"url": "NONE", "port": 0, "name": "NONE", "updated": datetime.now().isoformat()}
    else:
        data = {
            "url": proxy["url"],
            "port": proxy["port"],
            "name": proxy["name"],
            "updated": datetime.now().isoformat(),
        }
    ACTIVE_PROXY_FILE.write_text(json.dumps(data, ensure_ascii=False))


def write_status(statuses: list[dict]):
    """Записывает статус всех прокси для мониторинга."""
    data = {
        "checked_at": datetime.now().isoformat(),
        "proxies": statuses,
    }
    STATUS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def restart_bot():
    """Перезапускает бота через systemctl."""
    log.info(f"Restarting {BOT_SERVICE}...")
    try:
        result = subprocess.run(
            ["sudo", "systemctl", "restart", BOT_SERVICE],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            log.info(f"{BOT_SERVICE} restarted successfully")
        else:
            log.error(f"Failed to restart {BOT_SERVICE}: {result.stderr}")
    except Exception as e:
        log.error(f"Exception restarting {BOT_SERVICE}: {e}")


def try_restart_tunnel(proxy: dict):
    """Пытается перезапустить упавший SSH-туннель."""
    service = proxy["service"]
    log.info(f"Attempting to restart tunnel {service}...")
    try:
        subprocess.run(
            ["sudo", "systemctl", "restart", service],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except Exception as e:
        log.error(f"Failed to restart {service}: {e}")


# ─── Основной цикл ──────────────────────────────────────────


def main():
    log.info("Starting proxy_manager")
    log.info(f"Proxies: {', '.join(p['name'] + ':' + str(p['port']) for p in PROXIES)}")
    log.info(f"Check interval: {CHECK_INTERVAL}s")
    log.info(f"Active proxy file: {ACTIVE_PROXY_FILE}")

    consecutive_all_down = 0

    while True:
        best_proxy = None
        statuses = []

        for proxy in PROXIES:
            ok = check_proxy(proxy)
            statuses.append({"name": proxy["name"], "port": proxy["port"], "alive": ok})
            if ok and best_proxy is None:
                best_proxy = proxy

        write_status(statuses)

        current = read_active_proxy()
        current_url = current["url"] if current else None

        if best_proxy is None:
            # Все прокси мёртвы
            consecutive_all_down += 1
            log.error(f"ALL proxies DOWN (count={consecutive_all_down})")
            if current_url != "NONE":
                write_active_proxy(None)
            # Каждые 3 цикла пытаемся перезапустить туннели
            if consecutive_all_down % 3 == 0:
                for proxy in PROXIES:
                    try_restart_tunnel(proxy)
        else:
            consecutive_all_down = 0
            if current_url != best_proxy["url"]:
                old_name = current.get("name", "none") if current else "none"
                log.info(f"SWITCHING: {old_name} → {best_proxy['name']} ({best_proxy['url']})")
                write_active_proxy(best_proxy)
                restart_bot()
            else:
                log.debug(f"Active proxy unchanged: {best_proxy['name']}")

            # Попробовать перезапустить мёртвые туннели (фоново)
            for proxy, status in zip(PROXIES, statuses):
                if not status["alive"]:
                    try_restart_tunnel(proxy)

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
