"""
proxy_config.py — Читает активный прокси из /tmp/active_proxy.json.
Используется в bot.py и daily_report.py вместо хардкода.

Usage:
    from proxy_config import get_proxy_url

    proxy = get_proxy_url()  # "socks5h://127.0.0.1:1081" или fallback
"""

import json
import logging
from pathlib import Path

ACTIVE_PROXY_FILE = Path("/tmp/active_proxy.json")
FALLBACK_PROXY = "socks5h://127.0.0.1:1080"  # Amsterdam как fallback

log = logging.getLogger(__name__)


def get_proxy_url() -> str:
    """Возвращает URL активного прокси. При ошибке — fallback."""
    try:
        data = json.loads(ACTIVE_PROXY_FILE.read_text())
        url = data.get("url", "")
        if url and url != "NONE":
            return url
        log.warning("Active proxy is NONE, using fallback")
        return FALLBACK_PROXY
    except FileNotFoundError:
        log.warning(f"{ACTIVE_PROXY_FILE} not found, using fallback")
        return FALLBACK_PROXY
    except (json.JSONDecodeError, Exception) as e:
        log.warning(f"Error reading proxy config: {e}, using fallback")
        return FALLBACK_PROXY


def get_proxy_dict() -> dict:
    """Возвращает dict для httpx/requests: {'https': url, 'http': url}."""
    url = get_proxy_url()
    return {"https": url, "http": url}
