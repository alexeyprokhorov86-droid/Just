"""tasks_flow — задачи payment_audit (Phase 2/3).

Top-level handlers (без ConversationHandler), чтобы callback-кнопки работали
и на сообщениях, отправленных из cron-эскалатора (через HTTP API), не только
на ответах /tasks.

Состояние ожидания текстового ввода — в context.user_data['await']:
  {'kind': 'done|snooze_custom|decline', 'task_id': N}

Регистрация:
  from tasks_flow import register_tasks_handlers
  register_tasks_handlers(application)
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
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logger = logging.getLogger("tasks_flow")

DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASS = os.getenv("DB_PASSWORD")

PRIORITY_LABELS = {0: "🟢 низкий", 1: "🔵 нормальный", 2: "🟠 высокий", 3: "🔴 критический"}


def _conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


# ─────────────────────────────────────────────────────────────────────
#  DB helpers
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
    """Кандидаты на передачу задачи: все TG-юзера с km_entity, у которых есть
    активная запись в v_current_staff (не уволены, не архив).

    JOIN по canonical_name = full_name ловит человека даже если
    comm_users.employee_ref_key указывает на старую увольнительную карточку
    (бывает у тех, кого приняли повторно — у них несколько c1_employees).
    """
    with _conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            WITH active_persons AS (
                SELECT DISTINCT TRIM(full_name) AS full_name
                FROM v_current_staff
                WHERE dismissal_date IS NULL AND is_archived = false
            )
            SELECT cu.tg_user_id, cu.display_name, cu.km_entity_id,
                   e.canonical_name AS person_name
            FROM comm_users cu
            JOIN km_entities e ON e.id = cu.km_entity_id
            JOIN active_persons ap ON ap.full_name = TRIM(e.canonical_name)
            WHERE COALESCE(cu.is_external, false) = false
            ORDER BY e.canonical_name, cu.display_name
        """)
        return [dict(r) for r in cur.fetchall()]


def log_reminder(task_id: int, level: int, channel: str, chat_id: int | None,
                 message_id: int | None, text: str | None,
                 response: str | None = None) -> None:
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
                header_meta.append(f"⚠ просрочка {-days_left} д")
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

async def cmd_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    task = fetch_next_task(user_id)
    if not task:
        await update.message.reply_text("✨ Открытых задач нет.")
        return
    text, kb = render_task(task)
    pending = count_pending(user_id)
    if pending > 1:
        text = f"<i>Задача 1 из {pending}</i>\n\n" + text
    msg = await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    log_reminder(task["id"], 0, "dm", msg.chat_id, msg.message_id, text)


async def on_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    context.user_data["await"] = {"kind": "done", "task_id": task_id}
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Без комментария", callback_data=f"done_skip_{task_id}")],
        [InlineKeyboardButton("← Назад", callback_data=f"done_cancel_{task_id}")],
    ])
    await q.edit_message_text(
        "Кратко напиши, что сделано (одним сообщением).\n"
        "Например: «Оформил ПТУ #234 на 200к», «Аванс по договору, поставка 10.05».",
        reply_markup=kb,
    )


async def on_task_snooze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("1 день", callback_data=f"snz_d_1_{task_id}"),
         InlineKeyboardButton("3 дня",  callback_data=f"snz_d_3_{task_id}"),
         InlineKeyboardButton("Неделя", callback_data=f"snz_d_7_{task_id}")],
        [InlineKeyboardButton("Своя дата…", callback_data=f"snz_custom_{task_id}")],
        [InlineKeyboardButton("← Назад", callback_data=f"snz_cancel_{task_id}")],
    ])
    await q.edit_message_reply_markup(reply_markup=kb)


async def on_task_decline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    context.user_data["await"] = {"kind": "decline", "task_id": task_id}
    await q.edit_message_text(
        "Напиши причину одним сообщением. Например:\n"
        "«Это аванс по договору №123, поставка 15.05»\n"
        "«Платёж ошибочный, инициирован возврат»\n"
        "/cancel — отмена."
    )


async def on_task_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    candidates = list_active_tg_assignees()
    candidates = [c for c in candidates if c["tg_user_id"] != update.effective_user.id]
    if not candidates:
        await q.edit_message_text("Нет других активных юзеров для передачи.")
        return
    rows = []
    cur_row = []
    for c in candidates:
        # Если у одного km_entity несколько TG (рабочий+личный) — показываем все,
        # различая по display_name. Сейчас 17 активных юзеров → ≤9 строк по 2 кнопки,
        # запас до TG-лимита (~100 кнопок) большой.
        person = c.get("person_name") or ""
        display = c.get("display_name") or ""
        if person and display and display.lower() not in person.lower():
            label = f"{person} ({display})"[:30]
        else:
            label = (person or display or str(c["tg_user_id"]))[:30]
        cur_row.append(InlineKeyboardButton(label, callback_data=f"tr_{task_id}_{c['tg_user_id']}"))
        if len(cur_row) == 2:
            rows.append(cur_row); cur_row = []
    if cur_row: rows.append(cur_row)
    rows.append([InlineKeyboardButton("← Назад", callback_data=f"tr_cancel_{task_id}")])
    await q.edit_message_text("Кому передать задачу?", reply_markup=InlineKeyboardMarkup(rows))


# Snooze flow

async def on_snooze_preset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    parts = q.data.split("_")
    days = int(parts[2]); task_id = int(parts[3])
    until = datetime.now() + timedelta(days=days)
    update_task(task_id, snoozed_until=until)
    log_reminder(task_id, 0, "dm", q.message.chat_id, q.message.message_id,
                 f"snoozed +{days}d", response=f"snoozed_until_{until.date()}")
    await q.edit_message_text(f"⏰ Отложено до {until.strftime('%Y-%m-%d %H:%M')}")


async def on_snooze_custom(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    context.user_data["await"] = {"kind": "snooze_custom", "task_id": task_id}
    await q.edit_message_text(
        "Введи дату <code>YYYY-MM-DD</code> (например <code>2026-05-15</code>). /cancel — отмена.",
        parse_mode=ParseMode.HTML)


async def on_snooze_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    task = get_task(task_id)
    if task:
        text, kb = render_task(task)
        await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)


# Done flow (skip / cancel)

async def on_done_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    update_task(task_id, status="resolved", resolved_at=datetime.now())
    log_reminder(task_id, 0, "dm", q.message.chat_id, q.message.message_id,
                 "resolved (no comment)", response="resolved")
    context.user_data.pop("await", None)
    await q.edit_message_text("✅ Сделано. Спасибо.")
    await _maybe_show_next(update, context, update.effective_user.id, q.message.chat_id)


async def on_done_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    context.user_data.pop("await", None)
    task = get_task(task_id)
    if task:
        text, kb = render_task(task)
        await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)


# Transfer

async def on_transfer_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    parts = q.data.split("_")
    task_id = int(parts[1]); new_tg = int(parts[2])
    with _conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT km_entity_id, display_name, "
                    "(SELECT canonical_name FROM km_entities WHERE id=km_entity_id) AS pname "
                    "FROM comm_users WHERE tg_user_id=%s", (new_tg,))
        c = cur.fetchone()
    if not c:
        await q.edit_message_text("Не нашёл получателя.")
        return
    task = get_task(task_id)
    update_task(task_id, assignee_tg_user_id=new_tg, assignee_entity_id=c["km_entity_id"],
                transferred_from_entity_id=task.get("assignee_entity_id"),
                transferred_at=datetime.now())
    log_reminder(task_id, 0, "dm", q.message.chat_id, q.message.message_id,
                 f"transferred to tg={new_tg}", response=f"transferred_to_{new_tg}")
    name = c["pname"] or c["display_name"] or str(new_tg)
    await q.edit_message_text(f"👥 Передано: {name}")
    try:
        text, kb = render_task(get_task(task_id))
        text = "<i>Тебе передали задачу</i>\n\n" + text
        msg = await context.bot.send_message(chat_id=new_tg, text=text,
                                              reply_markup=kb, parse_mode=ParseMode.HTML)
        log_reminder(task_id, 0, "dm", msg.chat_id, msg.message_id,
                     "transferred-in DM", response=None)
    except Exception as e:
        logger.warning(f"Could not DM new assignee {new_tg}: {e}")


async def on_transfer_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; await q.answer()
    task_id = int(q.data.split("_")[-1])
    task = get_task(task_id)
    if task:
        text, kb = render_task(task)
        await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)


# ─────────────────────────────────────────────────────────────────────
#  Text input dispatcher
# ─────────────────────────────────────────────────────────────────────

async def on_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глобальный обработчик текста — диспетчер по user_data['await']."""
    aw = context.user_data.get("await")
    if not aw:
        return  # не наше — ничего не делаем
    kind = aw["kind"]
    task_id = aw["task_id"]
    text = (update.message.text or "").strip()
    if text.startswith("/"):
        return  # команда — не обрабатываем

    if kind == "done":
        if len(text) < 2:
            await update.message.reply_text("Слишком коротко. Опиши хотя бы пару слов.")
            return
        update_task(task_id, status="resolved", resolved_at=datetime.now(),
                    result_text=text)
        log_reminder(task_id, 0, "dm", update.message.chat_id, update.message.message_id,
                     f"resolved: {text}", response="resolved")
        context.user_data.pop("await", None)
        await update.message.reply_text("✅ Сделано. Зафиксировано.")
        await _maybe_show_next(update, context, update.effective_user.id,
                                update.message.chat_id)
        return

    if kind == "snooze_custom":
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
        if not m:
            await update.message.reply_text("Не понял дату. YYYY-MM-DD или /cancel.")
            return
        try:
            until = datetime(int(m[1]), int(m[2]), int(m[3]), 9, 0)
        except ValueError:
            await update.message.reply_text("Неверная дата.")
            return
        update_task(task_id, snoozed_until=until)
        log_reminder(task_id, 0, "dm", update.message.chat_id, update.message.message_id,
                     f"snoozed to {until}", response=f"snoozed_until_{until.date()}")
        context.user_data.pop("await", None)
        await update.message.reply_text(f"⏰ Отложено до {until.strftime('%Y-%m-%d %H:%M')}")
        return

    if kind == "decline":
        if len(text) < 5:
            await update.message.reply_text("Слишком коротко. Опиши подробнее.")
            return
        update_task(task_id, status="declined", declined_at=datetime.now(),
                    decline_reason=text)
        log_reminder(task_id, 0, "dm", update.message.chat_id, update.message.message_id,
                     f"declined: {text}", response="declined")
        context.user_data.pop("await", None)
        await update.message.reply_text("❌ Отклонено. Зафиксировано.")
        return


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.pop("await", None):
        await update.message.reply_text("Окей, отменил.")


async def _maybe_show_next(update, context, user_id: int, chat_id: int) -> None:
    nxt = fetch_next_task(user_id)
    if not nxt:
        return
    text, kb = render_task(nxt)
    msg = await context.bot.send_message(chat_id=chat_id, text=text,
                                          reply_markup=kb, parse_mode=ParseMode.HTML)
    log_reminder(nxt["id"], 0, "dm", msg.chat_id, msg.message_id, text)


def register_tasks_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("tasks", cmd_tasks))
    application.add_handler(CommandHandler("cancel", cmd_cancel))

    application.add_handler(CallbackQueryHandler(on_task_done,    pattern=r"^task_done_\d+$"))
    application.add_handler(CallbackQueryHandler(on_task_snooze,  pattern=r"^task_snooze_\d+$"))
    application.add_handler(CallbackQueryHandler(on_task_decline, pattern=r"^task_decline_\d+$"))
    application.add_handler(CallbackQueryHandler(on_task_transfer,pattern=r"^task_transfer_\d+$"))

    application.add_handler(CallbackQueryHandler(on_snooze_preset, pattern=r"^snz_d_\d+_\d+$"))
    application.add_handler(CallbackQueryHandler(on_snooze_custom, pattern=r"^snz_custom_\d+$"))
    application.add_handler(CallbackQueryHandler(on_snooze_cancel, pattern=r"^snz_cancel_\d+$"))

    application.add_handler(CallbackQueryHandler(on_done_skip,   pattern=r"^done_skip_\d+$"))
    application.add_handler(CallbackQueryHandler(on_done_cancel, pattern=r"^done_cancel_\d+$"))

    application.add_handler(CallbackQueryHandler(on_transfer_pick,   pattern=r"^tr_\d+_\d+$"))
    application.add_handler(CallbackQueryHandler(on_transfer_cancel, pattern=r"^tr_cancel_\d+$"))

    # Глобальный текстовый обработчик — приоритет ниже команд (group=10)
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_input),
        group=10,
    )
