"""
Tool: get_chat_list — единый справочник Telegram-чатов из tg_chats_metadata.

Заменяет:
- rag_agent.py:get_chat_list() (был кэш 5 мин, ordered by last_message_at DESC)
- notifications.py:get_available_chats() (без кэша, ordered by chat_title ASC)

Возвращает rich shape со всеми полями, callers фильтруют/сортируют под свои нужды.
Кэш 5 минут живёт внутри модуля.
"""
from __future__ import annotations

import time
from typing import Literal

import psycopg2.extras
from pydantic import BaseModel, Field

from ._db import get_conn
from .registry import tool

_CACHE_TTL_SEC = 300
_cache: dict = {"data": None, "ts": 0.0}


class GetChatListInput(BaseModel):
    order_by: Literal["recent", "title"] = Field(
        default="recent",
        description=(
            "'recent' — по last_message_at DESC nulls last (для RAG/Router "
            "чтобы свежие чаты шли первыми); 'title' — по алфавиту (для UI-клавиатур)."
        ),
    )


@tool(
    name="get_chat_list",
    domain="chats",
    description=(
        "Возвращает список известных Telegram-чатов из tg_chats_metadata (только чаты "
        "с непустым table_name, т.е. с настроенной ingestion). Каждый элемент: "
        "chat_id (int), title (str), table (имя telegram_messages_*), "
        "last_message_at (datetime|None), last_msg (str formatted 'dd.mm.yyyy' "
        "или 'нет сообщений'), description (str). Кэш 5 минут."
    ),
    input_model=GetChatListInput,
)
def get_chat_list(order_by: str = "recent") -> list[dict]:
    now = time.time()
    if _cache["data"] is not None and (now - _cache["ts"]) < _CACHE_TTL_SEC:
        rows = _cache["data"]
    else:
        conn = get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT chat_id, chat_title, table_name, last_message_at, description
                    FROM tg_chats_metadata
                    WHERE table_name IS NOT NULL
                    """
                )
                rows = [
                    {
                        "chat_id": r["chat_id"],
                        "title": r["chat_title"] or "",
                        "table": r["table_name"],
                        "last_message_at": r["last_message_at"],
                        "last_msg": (
                            r["last_message_at"].strftime("%d.%m.%Y")
                            if r["last_message_at"]
                            else "нет сообщений"
                        ),
                        "description": r["description"] or "",
                    }
                    for r in cur.fetchall()
                ]
        finally:
            conn.close()
        _cache["data"] = rows
        _cache["ts"] = now

    if order_by == "title":
        return sorted(rows, key=lambda r: r["title"].lower())
    # "recent" — nulls last, затем DESC
    return sorted(
        rows,
        key=lambda r: (
            r["last_message_at"] is None,
            -(r["last_message_at"].timestamp() if r["last_message_at"] else 0),
        ),
    )


def invalidate_cache() -> None:
    """Сбросить кэш (использовать после ingestion нового чата в tg_chats_metadata)."""
    _cache["data"] = None
    _cache["ts"] = 0.0
