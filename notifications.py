"""
Система оповещений через Telegram-бот.
Команды: /notify, /notify_status, /notify_remind
Доступ: только ADMIN_USER_ID (805598873).
"""

import os
import json
import asyncio
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

load_dotenv()
logger = logging.getLogger(__name__)

from tools.chats import get_chat_list

ADMIN_USER_ID = 805598873

# Conversation states
ENTER_TEXT, SELECT_TARGET, SELECT_CHATS, SELECT_USERS, SELECT_TYPE, CONFIRM_SEND = range(6)


def get_db_connection():
    return psycopg2.connect(
        host="172.20.0.2",
        port=5432,
        dbname="knowledge_base",
        user="knowledge",
        password=os.getenv("DB_PASSWORD"),
    )


def resolve_recipients(target_type: str, target_filter: dict = None) -> list:
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        if target_type == "all":
            cur.execute(
                "SELECT DISTINCT ON (user_id) user_id, first_name "
                "FROM tg_user_roles WHERE is_active = TRUE ORDER BY user_id, id"
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
        cur.close()
        conn.close()


def get_active_users() -> list:
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT DISTINCT ON (user_id) user_id, first_name "
            "FROM tg_user_roles WHERE is_active = TRUE ORDER BY user_id, id"
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


# ── ConversationHandler steps ──


async def notify_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Команда доступна только администратору.")
        return ConversationHandler.END
    context.user_data["notify"] = {}
    await update.message.reply_text("Введите текст оповещения:")
    return ENTER_TEXT


async def enter_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["notify"]["text"] = update.message.text
    kb = [
        [InlineKeyboardButton("📢 Всем сотрудникам", callback_data="notify_target_all")],
        [InlineKeyboardButton("💬 По чатам...", callback_data="notify_target_chats")],
        [InlineKeyboardButton("👤 Конкретным людям...", callback_data="notify_target_users")],
    ]
    await update.message.reply_text("Выберите получателей:", reply_markup=InlineKeyboardMarkup(kb))
    return SELECT_TARGET


async def select_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "notify_target_all":
        context.user_data["notify"]["target_type"] = "all"
        context.user_data["notify"]["target_filter"] = None
        recipients = resolve_recipients("all")
        context.user_data["notify"]["recipients"] = recipients
        return await _ask_type(query, context)

    elif data == "notify_target_chats":
        context.user_data["notify"]["target_type"] = "chats"
        context.user_data["notify"]["selected_chats"] = []
        chats = get_chat_list(order_by="title")
        context.user_data["notify"]["all_chats"] = chats
        await _render_chat_buttons(query, context)
        return SELECT_CHATS

    elif data == "notify_target_users":
        context.user_data["notify"]["target_type"] = "users"
        context.user_data["notify"]["selected_users"] = []
        users = get_active_users()
        context.user_data["notify"]["all_users"] = users
        await _render_user_buttons(query, context)
        return SELECT_USERS


async def _render_chat_buttons(query, context):
    chats = context.user_data["notify"]["all_chats"]
    selected = context.user_data["notify"]["selected_chats"]
    kb = []
    for c in chats:
        mark = "✅" if c["chat_id"] in selected else "⬜"
        kb.append([InlineKeyboardButton(
            f'{mark} {c["title"]}',
            callback_data=f'notify_chat_{c["chat_id"]}',
        )])
    kb.append([InlineKeyboardButton("✅ Готово", callback_data="notify_chats_done")])
    await query.edit_message_text("Выберите чаты (нажмите для выбора):", reply_markup=InlineKeyboardMarkup(kb))


async def select_chats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "notify_chats_done":
        selected = context.user_data["notify"]["selected_chats"]
        if not selected:
            await query.answer("Выберите хотя бы один чат", show_alert=True)
            return SELECT_CHATS
        context.user_data["notify"]["target_filter"] = {"chat_ids": selected}
        recipients = resolve_recipients("chats", {"chat_ids": selected})
        context.user_data["notify"]["recipients"] = recipients
        return await _ask_type(query, context)

    # Toggle chat selection
    chat_id = int(data.replace("notify_chat_", ""))
    selected = context.user_data["notify"]["selected_chats"]
    if chat_id in selected:
        selected.remove(chat_id)
    else:
        selected.append(chat_id)
    await _render_chat_buttons(query, context)
    return SELECT_CHATS


async def _render_user_buttons(query, context):
    users = context.user_data["notify"]["all_users"]
    selected = context.user_data["notify"]["selected_users"]
    kb = []
    for u in users:
        mark = "✅" if u["user_id"] in selected else "⬜"
        name = u["first_name"] or str(u["user_id"])
        kb.append([InlineKeyboardButton(
            f'{mark} {name}',
            callback_data=f'notify_user_{u["user_id"]}',
        )])
    kb.append([InlineKeyboardButton("✅ Готово", callback_data="notify_users_done")])
    await query.edit_message_text("Выберите сотрудников:", reply_markup=InlineKeyboardMarkup(kb))


async def select_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "notify_users_done":
        selected = context.user_data["notify"]["selected_users"]
        if not selected:
            await query.answer("Выберите хотя бы одного", show_alert=True)
            return SELECT_USERS
        context.user_data["notify"]["target_filter"] = {"user_ids": selected}
        recipients = resolve_recipients("users", {"user_ids": selected})
        context.user_data["notify"]["recipients"] = recipients
        return await _ask_type(query, context)

    uid = int(data.replace("notify_user_", ""))
    selected = context.user_data["notify"]["selected_users"]
    if uid in selected:
        selected.remove(uid)
    else:
        selected.append(uid)
    await _render_user_buttons(query, context)
    return SELECT_USERS


async def _ask_type(query, context):
    kb = [
        [InlineKeyboardButton("ℹ️ Просто оповещение", callback_data="notify_type_info")],
        [InlineKeyboardButton("✋ С подтверждением", callback_data="notify_type_confirm")],
    ]
    await query.edit_message_text("Выберите тип оповещения:", reply_markup=InlineKeyboardMarkup(kb))
    return SELECT_TYPE


async def select_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    ntype = "info" if query.data == "notify_type_info" else "confirm"
    context.user_data["notify"]["notification_type"] = ntype

    nd = context.user_data["notify"]
    recipients = nd["recipients"]
    count = len(recipients)
    type_label = "Просто оповещение" if ntype == "info" else "С подтверждением"

    if count <= 10:
        names = ", ".join(r["first_name"] or str(r["user_id"]) for r in recipients)
    else:
        names = f"{count} сотрудников"

    preview = (
        f"📋 Оповещение:\n{nd['text']}\n\n"
        f"📨 Получатели: {count} чел.\n{names}\n\n"
        f"Тип: {type_label}"
    )
    kb = [
        [InlineKeyboardButton("🚀 Отправить", callback_data="notify_confirm_send")],
        [InlineKeyboardButton("❌ Отмена", callback_data="notify_confirm_cancel")],
    ]
    await query.edit_message_text(preview, reply_markup=InlineKeyboardMarkup(kb))
    return CONFIRM_SEND


async def confirm_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "notify_confirm_cancel":
        await query.edit_message_text("❌ Оповещение отменено.")
        context.user_data.pop("notify", None)
        return ConversationHandler.END

    nd = context.user_data["notify"]
    recipients = nd["recipients"]

    # Save to DB
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO notifications (text, notification_type, target_type, target_filter, created_by, status) "
            "VALUES (%s, %s, %s, %s, %s, 'sending') RETURNING id",
            (nd["text"], nd["notification_type"], nd["target_type"],
             json.dumps(nd["target_filter"]) if nd["target_filter"] else None,
             ADMIN_USER_ID),
        )
        notif_id = cur.fetchone()[0]

        for r in recipients:
            cur.execute(
                "INSERT INTO notification_recipients (notification_id, user_id, first_name) "
                "VALUES (%s, %s, %s)",
                (notif_id, r["user_id"], r["first_name"]),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    await query.edit_message_text(f"📤 Отправка оповещения #{notif_id}...")

    # Send messages
    sent = 0
    errors = 0
    for r in recipients:
        try:
            text = nd["text"]
            reply_markup = None
            if nd["notification_type"] == "confirm":
                reply_markup = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Ознакомлен(а)", callback_data=f"notify_ack_{notif_id}")
                ]])

            await context.bot.send_message(
                chat_id=r["user_id"],
                text=text,
                reply_markup=reply_markup,
            )
            _mark_delivered(notif_id, r["user_id"])
            sent += 1
        except Exception as e:
            err_msg = str(e)
            logger.warning(f"notify #{notif_id}: failed to send to {r['user_id']}: {err_msg}")
            _mark_error(notif_id, r["user_id"], err_msg)
            errors += 1

        await asyncio.sleep(0.05)

    # Update status
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE notifications SET status = 'sent' WHERE id = %s", (notif_id,))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    await context.bot.send_message(
        chat_id=ADMIN_USER_ID,
        text=f"✅ Оповещение #{notif_id} отправлено.\nДоставлено: {sent}, Ошибки: {errors}",
    )
    context.user_data.pop("notify", None)
    return ConversationHandler.END


def _mark_delivered(notif_id, user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE notification_recipients SET sent_at = NOW(), delivered = TRUE "
            "WHERE notification_id = %s AND user_id = %s",
            (notif_id, user_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _mark_error(notif_id, user_id, error):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE notification_recipients SET sent_at = NOW(), error = %s "
            "WHERE notification_id = %s AND user_id = %s",
            (error, notif_id, user_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("notify", None)
    await update.message.reply_text("❌ Оповещение отменено.")
    return ConversationHandler.END


# ── Acknowledge callback ──


async def handle_ack(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    notif_id = int(query.data.replace("notify_ack_", ""))
    user_id = query.from_user.id

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE notification_recipients SET confirmed_at = NOW() "
            "WHERE notification_id = %s AND user_id = %s AND confirmed_at IS NULL",
            (notif_id, user_id),
        )
        updated = cur.rowcount
        conn.commit()
    finally:
        cur.close()
        conn.close()

    if updated:
        await query.answer("✅ Вы подтвердили ознакомление")
        await query.edit_message_reply_markup(reply_markup=None)
    else:
        await query.answer("Вы уже подтвердили ранее")


# ── /notify_status ──


async def notify_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Команда доступна только администратору.")
        return

    args = context.args
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        if args:
            notif_id = int(args[0])
            cur.execute("SELECT * FROM notifications WHERE id = %s", (notif_id,))
            notif = cur.fetchone()
            if not notif:
                await update.message.reply_text(f"Оповещение #{notif_id} не найдено.")
                return

            cur.execute(
                "SELECT first_name, user_id, delivered, confirmed_at, error "
                "FROM notification_recipients WHERE notification_id = %s ORDER BY first_name",
                (notif_id,),
            )
            recs = cur.fetchall()

            lines = [f"📋 Оповещение #{notif_id} ({notif['notification_type']})"]
            lines.append(f"Текст: {notif['text'][:100]}...")
            lines.append(f"Статус: {notif['status']}")
            lines.append(f"Создано: {notif['created_at'].strftime('%d.%m %H:%M')}")
            lines.append("")

            delivered = sum(1 for r in recs if r["delivered"])
            confirmed = sum(1 for r in recs if r["confirmed_at"])
            errored = sum(1 for r in recs if r["error"])

            lines.append(f"Доставлено: {delivered}/{len(recs)}, Ошибки: {errored}")
            if notif["notification_type"] == "confirm":
                lines.append(f"Подтвердили: {confirmed}/{delivered}")
                lines.append("")
                not_confirmed = [r for r in recs if r["delivered"] and not r["confirmed_at"]]
                if not_confirmed:
                    lines.append("Не подтвердили:")
                    for r in not_confirmed:
                        name = r["first_name"] or str(r["user_id"])
                        lines.append(f"  - {name}")

            await update.message.reply_text("\n".join(lines))
        else:
            cur.execute(
                "SELECT n.id, n.notification_type, n.status, n.created_at, "
                "LEFT(n.text, 30) as text_short, "
                "count(nr.id) as total, "
                "count(nr.id) FILTER (WHERE nr.delivered) as delivered, "
                "count(nr.id) FILTER (WHERE nr.confirmed_at IS NOT NULL) as confirmed "
                "FROM notifications n LEFT JOIN notification_recipients nr ON nr.notification_id = n.id "
                "GROUP BY n.id ORDER BY n.id DESC LIMIT 10"
            )
            rows = cur.fetchall()
            if not rows:
                await update.message.reply_text("Оповещений пока нет.")
                return

            lines = ["Последние оповещения:\n"]
            for r in rows:
                dt = r["created_at"].strftime("%d.%m %H:%M")
                conf = ""
                if r["notification_type"] == "confirm":
                    conf = f", подтв: {r['confirmed']}/{r['delivered']}"
                lines.append(
                    f"#{r['id']} [{dt}] {r['text_short']}... "
                    f"({r['delivered']}/{r['total']}{conf}) — {r['status']}"
                )
            await update.message.reply_text("\n".join(lines))
    finally:
        cur.close()
        conn.close()


# ── /notify_remind ──


async def notify_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Команда доступна только администратору.")
        return

    args = context.args
    if not args:
        await update.message.reply_text("Использование: /notify_remind <id>")
        return

    notif_id = int(args[0])
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM notifications WHERE id = %s", (notif_id,))
        notif = cur.fetchone()
        if not notif:
            await update.message.reply_text(f"Оповещение #{notif_id} не найдено.")
            return
        if notif["notification_type"] != "confirm":
            await update.message.reply_text("Напоминание доступно только для оповещений с подтверждением.")
            return

        cur.execute(
            "SELECT user_id, first_name FROM notification_recipients "
            "WHERE notification_id = %s AND delivered = TRUE AND confirmed_at IS NULL",
            (notif_id,),
        )
        to_remind = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not to_remind:
        await update.message.reply_text(f"Все получатели оповещения #{notif_id} уже подтвердили.")
        return

    sent = 0
    for r in to_remind:
        try:
            reply_markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Ознакомлен(а)", callback_data=f"notify_ack_{notif_id}")
            ]])
            await context.bot.send_message(
                chat_id=r["user_id"],
                text=f"⏰ Напоминание:\n\n{notif['text']}",
                reply_markup=reply_markup,
            )
            sent += 1
        except Exception as e:
            logger.warning(f"notify_remind #{notif_id}: failed for {r['user_id']}: {e}")
        await asyncio.sleep(0.05)

    await update.message.reply_text(f"Напомнил {sent} сотрудникам (из {len(to_remind)} неподтвердивших).")


# ── Export ──


def get_notify_conversation_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CommandHandler("notify", notify_start)],
        states={
            ENTER_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_text)],
            SELECT_TARGET: [CallbackQueryHandler(select_target, pattern=r"^notify_target_")],
            SELECT_CHATS: [CallbackQueryHandler(select_chats, pattern=r"^notify_chat(s_done|_-?\d+)$")],
            SELECT_USERS: [CallbackQueryHandler(select_users, pattern=r"^notify_user(s_done|_\d+)$")],
            SELECT_TYPE: [CallbackQueryHandler(select_type, pattern=r"^notify_type_")],
            CONFIRM_SEND: [CallbackQueryHandler(confirm_send, pattern=r"^notify_confirm_")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_user=True,
        per_chat=True,
    )
