"""tasks_flow — Telegram-флоу для задач (Phase 2 payment_audit).

Команда /tasks → одна открытая задача из km_tasks → кнопки:
  [✅ Сделано] [⏰ Отложить] [👥 Передать] [❌ Отклонить]

Snooze: пресеты (1д/3д/неделя/своя дата).
Decline: запрашивает причину текстом.
Transfer: показывает список активных TG-юзеров → передача.

Регистрация:
  from tasks_flow import tasks_conversation
  application.add_handler(tasks_conversation())
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logger = logging.getLogger("tasks_flow")

DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASS = os.getenv("DB_PASSWORD")


def _conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


# Conversation states
CHOOSING_ACTION = 1
SNOOZE_PICK    = 2
SNOOZE_CUSTOM  = 3
DECLINE_REASON = 4
TRANSFER_PICK  = 5
RESULT_TEXT    = 6


PRIORITY_LABELS = {0: "🟢 низкий", 1: "🔵 нормальный", 2: "🟠 высокий", 3: "🔴 критический"}


# ─────────────────────────────────────────────────────────────────────
#  DB
# ─────────────────────────────────────────────────────────────────────

def fetch_next_task(tg_user_id: int) -> dict | None:
    with _conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, kind, title, task_text, context_data, assignee_entity_id,
                   assignee_tg_user_id, status, snoozed_until, escalation_level,
                   created_at, source_table, source_id, priority, deadline,
                   result_text
            FROM km_tasks
            WHERE assignee_tg_user_id = %s
              AND status = 'open'
              AND kind != 'extracted_from_text'
              AND (snoozed_until IS NULL OR snoozed_until <= NOW())
            ORDER BY priority DESC, deadline ASC NULLS LAST, created_at ASC LIMIT 1
        """, (tg_user_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def count_pending(tg_user_id: int) -> int:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM km_tasks
            WHERE assignee_tg_user_id = %s
              AND status = 'open'
              AND kind != 'extracted_from_text'
        """, (tg_user_id,))
        return cur.fetchone()[0]


def get_task(task_id: int) -> dict | None:
    with _conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM km_tasks WHERE id=%s", (task_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def update_task(task_id: int, **kwargs) -> None:
    cols = []; vals = []
    for k, v in kwargs.items():
        cols.append(f"{k} = %s"); vals.append(v)
    cols.append("updated_at = NOW()")
    vals.append(task_id)
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE km_tasks SET {', '.join(cols)} WHERE id=%s", vals)


def list_active_tg_assignees() -> list[dict]:
    """Активные TG-юзеры, у которых есть km_entity (можно передать задачу)."""
    with _conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT cu.tg_user_id, cu.display_name, cu.km_entity_id,
                   e.canonical_name AS person_name
            FROM comm_users cu
            LEFT JOIN km_entities e ON e.id = cu.km_entity_id
            WHERE cu.km_entity_id IS NOT NULL
              AND COALESCE(cu.is_external, false) = false
            ORDER BY e.canonical_name NULLS LAST, cu.display_name
        """)
        return [dict(r) for r in cur.fetchall()]


def log_reminder(task_id: int, level: int, channel: str, chat_id: int | None,
                 message_id: int | None, text: str | None, response: str | None = None) -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO task_reminders (task_id, level, channel, chat_id, message_id,
                                        text, user_response, responded_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,
                    CASE WHEN %s IS NOT NULL THEN NOW() END)
        """, (task_id, level, channel, chat_id, message_id, text, response, response))


# ─────────────────────────────────────────────────────────────────────
#  Render
# ─────────────────────────────────────────────────────────────────────

def render_task(task: dict) -> tuple[str, InlineKeyboardMarkup]:
    ctx = task.get("context_data") or {}
    if isinstance(ctx, str):
        ctx = json.loads(ctx)

    pri = task.get("priority") if task.get("priority") is not None else 1
    pri_str = PRIORITY_LABELS.get(pri, "🔵 нормальный")

    header_meta = [pri_str]
    if task.get("deadline"):
        from datetime import date as _d
        try:
            ddl = task["deadline"]
            days_left = (ddl - _d.today()).days
            if days_left < 0:
                header_meta.append(f"⚠ просрочка на {-days_left} д")
            elif days_left == 0:
                header_meta.append("⚠ дедлайн сегодня")
            else:
                header_meta.append(f"дедлайн через {days_left} д ({ddl})")
        except Exception:
            pass

    lines = [f"<b>{task.get('title') or 'Задача'}</b>"]
    if header_meta:
        lines.append(" · ".join(header_meta))
    lines.append("")
    if task.get("task_text"):
        lines.append(task["task_text"])
        lines.append("")

    if ctx.get("partner_name"):
        lines.append(f"Поставщик: <b>{ctx['partner_name']}</b>")
    if ctx.get("organization_name"):
        lines.append(f"Организация: {ctx['organization_name']}")
    if ctx.get("gap_amount") is not None:
        lines.append(f"Расхождение: <b>{float(ctx['gap_amount']):,.0f} ₽</b>".replace(",", " "))
    if ctx.get("total_paid") is not None:
        lines.append(
            f"Оплачено {float(ctx['total_paid']):,.0f} ₽, "
            f"принято {float(ctx['total_acquired']):,.0f} ₽".replace(",", " ")
        )
    if ctx.get("supplier_order_number"):
        lines.append(f"Заказ поставщику: <code>{ctx['supplier_order_number']}</code>")
        if ctx.get("supplier_order_desired_arrival"):
            lines.append(f"Желаемая дата поставки: {ctx['supplier_order_desired_arrival']}")

    lines.append("")
    if task["kind"] == "payment_no_acquisition":
        lines.append("Что сделать:")
        lines.append("— Оформить недостающие приёмные документы")
        lines.append("— Или подтвердить аванс / отклонить с причиной")

    text = "\n".join(lines)
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Сделано",  callback_data=f"task_done_{task['id']}"),
            InlineKeyboardButton("⏰ Отложить", callback_data=f"task_snooze_{task['id']}"),
        ],
        [
            InlineKeyboardButton("👥 Передать", callback_data=f"task_transfer_{task['id']}"),
            InlineKeyboardButton("❌ Отклонить",callback_data=f"task_decline_{task['id']}"),
        ],
    ])
    return text, kb


# ─────────────────────────────────────────────────────────────────────
#  Handlers
# ─────────────────────────────────────────────────────────────────────

async def _show_one_task(update_or_q, context, user_id: int, prefix: str = "") -> int:
    task = fetch_next_task(user_id)
    if not task:
        text = "✨ Открытых задач нет."
        if hasattr(update_or_q, "message") and update_or_q.message:
            await update_or_q.message.reply_text(text)
        else:
            await update_or_q.callback_query.edit_message_text(text)
        return ConversationHandler.END
    text, kb = render_task(task)
    pending = count_pending(user_id)
    if pending > 1:
        text = f"<i>{prefix}Задача 1 из {pending}</i>\n\n" + text
    if hasattr(update_or_q, "message") and update_or_q.message:
        msg = await update_or_q.message.reply_text(text, reply_markup=kb,
                                                   parse_mode=ParseMode.HTML)
    else:
        await update_or_q.callback_query.edit_message_text(
            text, reply_markup=kb, parse_mode=ParseMode.HTML)
        msg = update_or_q.callback_query.message
    log_reminder(task["id"], level=0, channel="dm",
                 chat_id=msg.chat_id, message_id=msg.message_id, text=text)
    context.user_data["current_task_id"] = task["id"]
    return CHOOSING_ACTION


async def cmd_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _show_one_task(update, context, update.effective_user.id)


async def on_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    context.user_data["done_task_id"] = task_id
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Без комментария", callback_data=f"done_skip_{task_id}")],
        [InlineKeyboardButton("← Назад", callback_data=f"done_cancel_{task_id}")],
    ])
    await q.edit_message_text(
        "Кратко напиши, что сделано (одним сообщением).\n"
        "Например: «Оформил ПТУ #234 на 200к», «Аванс по договору, поставка 10.05».",
        reply_markup=kb,
    )
    return RESULT_TEXT


async def on_done_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    result = (update.message.text or "").strip()
    if len(result) < 2:
        await update.message.reply_text("Слишком коротко. Напиши хотя бы пару слов.")
        return RESULT_TEXT
    task_id = context.user_data.get("done_task_id")
    if not task_id:
        await update.message.reply_text("Слетел контекст. /tasks ещё раз.")
        return ConversationHandler.END
    update_task(task_id, status="resolved", resolved_at=datetime.now(),
                result_text=result)
    log_reminder(task_id, 0, "dm", update.message.chat_id, update.message.message_id,
                 f"resolved: {result}", response="resolved")
    await update.message.reply_text("✅ Сделано. Зафиксировано.")
    return await _show_next_or_end(update, context)


async def on_done_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    update_task(task_id, status="resolved", resolved_at=datetime.now())
    log_reminder(task_id, 0, "dm", q.message.chat_id, q.message.message_id,
                 "resolved (no comment)", response="resolved")
    await q.edit_message_text("✅ Сделано. Спасибо.")
    return await _show_next_or_end(update, context)


async def on_done_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    task = get_task(task_id)
    if task:
        text, kb = render_task(task)
        await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        return CHOOSING_ACTION
    return ConversationHandler.END


async def _show_next_or_end(update_or_q, context) -> int:
    """После resolve — показать следующую задачу или END."""
    user_id = update_or_q.effective_user.id
    next_t = fetch_next_task(user_id)
    if not next_t:
        return ConversationHandler.END
    text, kb = render_task(next_t)
    chat_id = (update_or_q.message.chat_id if update_or_q.message
               else update_or_q.callback_query.message.chat_id)
    msg = await context.bot.send_message(chat_id=chat_id, text=text,
                                          reply_markup=kb, parse_mode=ParseMode.HTML)
    log_reminder(next_t["id"], 0, "dm", msg.chat_id, msg.message_id, text)
    context.user_data["current_task_id"] = next_t["id"]
    return CHOOSING_ACTION


SNOOZE_OPTIONS = [("1 день", 1), ("3 дня", 3), ("Неделя", 7)]


async def on_snooze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    context.user_data["snooze_task_id"] = task_id
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(label, callback_data=f"snz_d_{days}_{task_id}")
         for label, days in SNOOZE_OPTIONS],
        [InlineKeyboardButton("Своя дата…", callback_data=f"snz_custom_{task_id}")],
        [InlineKeyboardButton("← Назад", callback_data=f"snz_cancel_{task_id}")],
    ])
    await q.edit_message_reply_markup(reply_markup=kb)
    return SNOOZE_PICK


async def on_snooze_preset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    parts = q.data.split("_")
    days = int(parts[2]); task_id = int(parts[3])
    until = datetime.now() + timedelta(days=days)
    update_task(task_id, snoozed_until=until)
    log_reminder(task_id, 0, "dm", q.message.chat_id, q.message.message_id,
                 f"snoozed +{days}d", response=f"snoozed_until_{until.date().isoformat()}")
    await q.edit_message_text(f"⏰ Отложено до {until.strftime('%Y-%m-%d %H:%M')}")
    return ConversationHandler.END


async def on_snooze_custom(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    await q.edit_message_text(
        "Введи дату <code>YYYY-MM-DD</code> (например <code>2026-05-15</code>).\n/cancel — отмена.",
        parse_mode=ParseMode.HTML)
    return SNOOZE_CUSTOM


async def on_snooze_custom_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
    if not m:
        await update.message.reply_text("Не понял дату. YYYY-MM-DD или /cancel.")
        return SNOOZE_CUSTOM
    try:
        until = datetime(int(m[1]), int(m[2]), int(m[3]), 9, 0)
    except ValueError:
        await update.message.reply_text("Неверная дата. Попробуй ещё раз.")
        return SNOOZE_CUSTOM
    task_id = context.user_data.get("snooze_task_id")
    if not task_id:
        await update.message.reply_text("Слетел контекст. /tasks ещё раз.")
        return ConversationHandler.END
    update_task(task_id, snoozed_until=until)
    log_reminder(task_id, 0, "dm", update.message.chat_id, update.message.message_id,
                 f"snoozed to {until}", response=f"snoozed_until_{until.date().isoformat()}")
    await update.message.reply_text(f"⏰ Отложено до {until.strftime('%Y-%m-%d %H:%M')}")
    return ConversationHandler.END


async def on_snooze_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    task = get_task(task_id)
    if task:
        text, kb = render_task(task)
        await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        return CHOOSING_ACTION
    return ConversationHandler.END


async def on_decline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    context.user_data["decline_task_id"] = task_id
    await q.edit_message_text(
        "Напиши причину одним сообщением. Например:\n"
        "«Это аванс по договору №123, поставка к 15.05»\n"
        "«Платёж ошибочный, инициирован возврат»\n"
        "/cancel — отмена."
    )
    return DECLINE_REASON


async def on_decline_reason(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    reason = (update.message.text or "").strip()
    if len(reason) < 5:
        await update.message.reply_text("Слишком короткая причина. Опиши подробнее.")
        return DECLINE_REASON
    task_id = context.user_data.get("decline_task_id")
    if not task_id:
        await update.message.reply_text("Слетел контекст. /tasks ещё раз.")
        return ConversationHandler.END
    update_task(task_id, status="declined", declined_at=datetime.now(), decline_reason=reason)
    log_reminder(task_id, 0, "dm", update.message.chat_id, update.message.message_id,
                 f"declined: {reason}", response="declined")
    await update.message.reply_text("❌ Отклонено. Зафиксировано.")
    return ConversationHandler.END


async def on_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    context.user_data["transfer_task_id"] = task_id
    candidates = list_active_tg_assignees()
    if not candidates:
        await q.edit_message_text("Нет других активных TG-юзеров для передачи.")
        return ConversationHandler.END
    # Build keyboard — 2 in row, max 18 candidates (9 rows)
    rows = []
    cur_row = []
    for c in candidates[:18]:
        if c["tg_user_id"] == update.effective_user.id:
            continue
        label = (c["person_name"] or c["display_name"] or str(c["tg_user_id"]))[:25]
        cur_row.append(InlineKeyboardButton(label, callback_data=f"tr_{task_id}_{c['tg_user_id']}"))
        if len(cur_row) == 2:
            rows.append(cur_row); cur_row = []
    if cur_row: rows.append(cur_row)
    rows.append([InlineKeyboardButton("← Назад", callback_data=f"tr_cancel_{task_id}")])
    await q.edit_message_text("Кому передать задачу?", reply_markup=InlineKeyboardMarkup(rows))
    return TRANSFER_PICK


async def on_transfer_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    parts = q.data.split("_")
    task_id = int(parts[1]); new_tg = int(parts[2])
    # find km_entity for new assignee
    with _conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT km_entity_id, display_name, "
                    "  (SELECT canonical_name FROM km_entities WHERE id=km_entity_id) AS person_name "
                    "FROM comm_users WHERE tg_user_id=%s", (new_tg,))
        c = cur.fetchone()
    if not c:
        await q.edit_message_text("Не нашёл получателя.")
        return ConversationHandler.END

    # Read current task to record transferred_from
    task = get_task(task_id)
    update_task(
        task_id,
        assignee_tg_user_id=new_tg,
        assignee_entity_id=c["km_entity_id"],
        transferred_from_entity_id=task.get("assignee_entity_id"),
        transferred_at=datetime.now(),
    )
    log_reminder(task_id, 0, "dm", q.message.chat_id, q.message.message_id,
                 f"transferred to tg={new_tg}", response=f"transferred_to_{new_tg}")

    name = c["person_name"] or c["display_name"] or str(new_tg)
    await q.edit_message_text(f"👥 Передано: {name}")

    # DM new assignee
    try:
        text, kb = render_task(get_task(task_id))
        text = f"<i>Тебе передали задачу</i>\n\n" + text
        msg = await context.bot.send_message(chat_id=new_tg, text=text,
                                              reply_markup=kb, parse_mode=ParseMode.HTML)
        log_reminder(task_id, 0, "dm", msg.chat_id, msg.message_id,
                     "transferred-in DM", response=None)
    except Exception as e:
        logger.warning(f"Could not DM new assignee {new_tg}: {e}")
    return ConversationHandler.END


async def on_transfer_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    task = get_task(task_id)
    if task:
        text, kb = render_task(task)
        await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        return CHOOSING_ACTION
    return ConversationHandler.END


async def on_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("Окей, отменил.")
    elif update.callback_query:
        q = update.callback_query; await q.answer()
        await q.edit_message_text("Отменил.")
    return ConversationHandler.END


def tasks_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CommandHandler("tasks", cmd_tasks)],
        states={
            CHOOSING_ACTION: [
                CallbackQueryHandler(on_done,     pattern=r"^task_done_\d+$"),
                CallbackQueryHandler(on_snooze,   pattern=r"^task_snooze_\d+$"),
                CallbackQueryHandler(on_decline,  pattern=r"^task_decline_\d+$"),
                CallbackQueryHandler(on_transfer, pattern=r"^task_transfer_\d+$"),
            ],
            SNOOZE_PICK: [
                CallbackQueryHandler(on_snooze_preset, pattern=r"^snz_d_\d+_\d+$"),
                CallbackQueryHandler(on_snooze_custom, pattern=r"^snz_custom_\d+$"),
                CallbackQueryHandler(on_snooze_cancel, pattern=r"^snz_cancel_\d+$"),
            ],
            SNOOZE_CUSTOM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_snooze_custom_text),
            ],
            DECLINE_REASON: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_decline_reason),
            ],
            TRANSFER_PICK: [
                CallbackQueryHandler(on_transfer_pick,   pattern=r"^tr_\d+_\d+$"),
                CallbackQueryHandler(on_transfer_cancel, pattern=r"^tr_cancel_\d+$"),
            ],
            RESULT_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_done_text),
                CallbackQueryHandler(on_done_skip,   pattern=r"^done_skip_\d+$"),
                CallbackQueryHandler(on_done_cancel, pattern=r"^done_cancel_\d+$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", on_cancel)],
        per_message=False, per_chat=True, per_user=True,
        name="tasks_conv",
    )
