"""
Tools: TG-уведомления.

Три tool-а, разделяющие бизнес (БД) от доставки (PTB/Matrix/HTTPS — зависит от
вызывающего):

- resolve_notification_recipients — кого затронет рассылка (для preview).
- prepare_notification — создать запись в notifications + notification_recipients,
  вернуть id и список получателей.
- finalize_notification — batch-обновление статусов после цикла доставки.

PTB `context.bot.send_message` остаётся в handler'е: там знают про reply_markup,
inline-клавиатуры подтверждения и т.п. Для cron/headless-surfaces будущая
обёртка `send_via_telegram_api(bot_token, ...)` в том же модуле (backlog).

Схема notifications:
  id, text, notification_type ('info'|'confirm'|...), target_type ('all'|'chats'|'users'),
  target_filter (jsonb), created_by (tg user_id), status ('sending'|'sent'|'failed'), created_at.

Схема notification_recipients:
  notification_id, user_id, first_name, sent_at, delivered (bool),
  error (text), confirmed_at.
"""
from __future__ import annotations

import json
from typing import Literal

import psycopg2.extras
from pydantic import BaseModel, Field

from ._db import get_conn
from .registry import tool


# ── 1. Resolve recipients (preview) ────────────────────────────────────

class ResolveRecipientsInput(BaseModel):
    target_type: Literal["all", "chats", "users"] = Field(
        description=(
            "'all' — все активные сотрудники в tg_user_roles; "
            "'chats' — сотрудники из указанных chat_ids; "
            "'users' — явный список user_ids."
        )
    )
    target_filter: dict = Field(
        default_factory=dict,
        description=(
            "Для target_type='chats': {'chat_ids': [int, ...]}; "
            "для 'users': {'user_ids': [int, ...]}; для 'all' — игнорируется."
        ),
    )


@tool(
    name="resolve_notification_recipients",
    domain="notifications",
    description=(
        "Возвращает список получателей рассылки по target_type/target_filter "
        "(читает tg_user_roles, DISTINCT ON user_id). Формат: "
        "[{user_id:int, first_name:str}, ...]. Используется для preview в UI "
        "(«отправка X людям») и для подсчёта охвата перед фактическим prepare."
    ),
    input_model=ResolveRecipientsInput,
)
def resolve_notification_recipients(target_type: str, target_filter: dict) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if target_type == "all":
                cur.execute(
                    "SELECT DISTINCT ON (user_id) user_id, first_name "
                    "FROM tg_user_roles WHERE is_active = TRUE "
                    "ORDER BY user_id, id"
                )
            elif target_type == "chats":
                chat_ids = target_filter.get("chat_ids", [])
                cur.execute(
                    "SELECT DISTINCT ON (user_id) user_id, first_name "
                    "FROM tg_user_roles WHERE is_active = TRUE AND chat_id = ANY(%s) "
                    "ORDER BY user_id, id",
                    (chat_ids,),
                )
            elif target_type == "users":
                user_ids = target_filter.get("user_ids", [])
                cur.execute(
                    "SELECT DISTINCT ON (user_id) user_id, first_name "
                    "FROM tg_user_roles WHERE user_id = ANY(%s) "
                    "ORDER BY user_id, id",
                    (user_ids,),
                )
            else:
                return []
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── 2. Prepare notification ────────────────────────────────────────────

class PrepareNotificationInput(BaseModel):
    text: str = Field(description="Текст рассылки", min_length=1)
    target_type: Literal["all", "chats", "users"]
    target_filter: dict = Field(default_factory=dict)
    notification_type: Literal["info", "warning", "alert", "confirm"] = "info"
    created_by: int = Field(description="tg user_id инициатора (для аудита)")


@tool(
    name="prepare_notification",
    domain="notifications",
    description=(
        "Создаёт запись в notifications (status='sending') и в "
        "notification_recipients для каждого получателя (разрешённого из "
        "target_type/filter). Возвращает {notification_id:int, "
        "recipients:[{user_id,first_name}, ...], recipient_count:int}. "
        "После этого caller должен фактически разослать сообщения (через PTB "
        "context.bot.send_message, Matrix, или direct HTTPS) и вызвать "
        "finalize_notification с результатами."
    ),
    input_model=PrepareNotificationInput,
)
def prepare_notification(
    text: str,
    target_type: str,
    target_filter: dict,
    notification_type: str,
    created_by: int,
) -> dict:
    recipients = resolve_notification_recipients(
        target_type=target_type, target_filter=target_filter
    )
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO notifications "
                "(text, notification_type, target_type, target_filter, created_by, status) "
                "VALUES (%s, %s, %s, %s, %s, 'sending') RETURNING id",
                (
                    text,
                    notification_type,
                    target_type,
                    json.dumps(target_filter) if target_filter else None,
                    created_by,
                ),
            )
            notif_id = cur.fetchone()[0]
            for r in recipients:
                cur.execute(
                    "INSERT INTO notification_recipients "
                    "(notification_id, user_id, first_name) VALUES (%s, %s, %s)",
                    (notif_id, r["user_id"], r["first_name"]),
                )
            conn.commit()
    finally:
        conn.close()
    return {
        "notification_id": notif_id,
        "recipients": recipients,
        "recipient_count": len(recipients),
    }


# ── 3. Finalize notification ───────────────────────────────────────────

class DeliveryResult(BaseModel):
    user_id: int
    delivered: bool
    error: str = ""


class FinalizeNotificationInput(BaseModel):
    notification_id: int
    results: list[DeliveryResult] = Field(
        description="Результаты доставки по каждому получателю",
        min_length=0,
    )


@tool(
    name="finalize_notification",
    domain="notifications",
    description=(
        "После цикла доставки caller вызывает этот tool со списком результатов "
        "по каждому recipient'у. Помечает notification_recipients.sent_at=NOW(), "
        "delivered/error соответственно, и устанавливает notifications.status='sent'. "
        "Возвращает {sent:int, errors:int, total:int, status:str}."
    ),
    input_model=FinalizeNotificationInput,
)
def finalize_notification(notification_id: int, results: list[dict]) -> dict:
    sent = errors = 0
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for r in results:
                if r["delivered"]:
                    cur.execute(
                        "UPDATE notification_recipients "
                        "SET sent_at = NOW(), delivered = TRUE "
                        "WHERE notification_id = %s AND user_id = %s",
                        (notification_id, r["user_id"]),
                    )
                    sent += 1
                else:
                    cur.execute(
                        "UPDATE notification_recipients "
                        "SET sent_at = NOW(), error = %s "
                        "WHERE notification_id = %s AND user_id = %s",
                        (r.get("error", ""), notification_id, r["user_id"]),
                    )
                    errors += 1
            cur.execute(
                "UPDATE notifications SET status = 'sent' WHERE id = %s",
                (notification_id,),
            )
            conn.commit()
    finally:
        conn.close()
    return {
        "sent": sent,
        "errors": errors,
        "total": len(results),
        "status": "sent",
    }
