# ============================================================
# EMAIL LOGGER - ДОБАВИТЬ В bot.py
# ============================================================
#
# ИНСТРУКЦИЯ:
# 1. Добавь этот код в bot.py ПЕРЕД функцией main()
# 2. Добавь регистрацию команд в main() - см. конец файла
# ============================================================


# ============================================================
# EMAIL LOGGER: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def format_email_age(dt) -> str:
    """Форматирует возраст для email."""
    if not dt:
        return "никогда"
    
    from datetime import datetime
    if dt.tzinfo:
        dt = dt.replace(tzinfo=None)
    
    delta = datetime.now() - dt
    
    if delta.days > 30:
        return f"{delta.days // 30} мес."
    elif delta.days > 0:
        return f"{delta.days} дн."
    elif delta.seconds > 3600:
        return f"{delta.seconds // 3600} ч."
    elif delta.seconds > 60:
        return f"{delta.seconds // 60} мин."
    else:
        return "сейчас"


def truncate_text(text: str, max_len: int = 100) -> str:
    """Обрезает текст."""
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len-3] + "..."


# ============================================================
# EMAIL LOGGER: КОМАНДЫ
# ============================================================

async def open_threads_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает открытые ветки email переписки."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Нет доступа к этой команде")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    t.id,
                    t.subject_normalized,
                    t.message_count,
                    t.last_message_at,
                    t.priority,
                    t.status
                FROM email_threads t
                WHERE t.status IN ('open', 'pending_resolution')
                ORDER BY 
                    CASE t.priority 
                        WHEN 'high' THEN 1 
                        WHEN 'medium' THEN 2 
                        ELSE 3 
                    END,
                    t.last_message_at DESC
                LIMIT 20
            """)
            threads = cur.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения веток: {e}")
        await update.message.reply_text(
            "❌ Таблицы email логгера не найдены.\n\n"
            "Примените миграцию:\n"
            "`psql -d knowledge_base -f 001_init_email_logger.sql`",
            parse_mode="Markdown"
        )
        return
    finally:
        conn.close()
    
    if not threads:
        await update.message.reply_text("✅ Нет открытых веток email переписки")
        return
    
    text = "📬 *Открытые ветки переписки:*\n\n"
    
    for thread_id, subject, msg_count, last_msg_at, priority, status in threads:
        priority_icon = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(priority or 'medium', '⚪')
        status_icon = '⏳' if status == 'pending_resolution' else '📨'
        age = format_email_age(last_msg_at)
        subject_short = truncate_text(subject or "Без темы", 45)
        
        text += (
            f"{priority_icon}{status_icon} *{subject_short}*\n"
            f"   📨 {msg_count or 0} писем • {age}\n"
            f"   /emailthread\\_{thread_id}\n\n"
        )
    
    await update.message.reply_text(text, parse_mode="Markdown")


async def show_email_thread_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает детали ветки по команде /emailthread_N."""
    import json
    
    text = update.message.text
    match = re.search(r'/emailthread_(\d+)', text)
    if not match:
        await update.message.reply_text("❌ Укажите ID ветки: /emailthread_123")
        return
    
    thread_id = int(match.group(1))
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    id, subject_normalized, message_count, last_message_at,
                    priority, status, participant_emails, topic_tags,
                    summary_short, key_decisions, action_items
                FROM email_threads WHERE id = %s
            """, (thread_id,))
            row = cur.fetchone()
            
            if not row:
                await update.message.reply_text("❌ Ветка не найдена")
                return
            
            (tid, subject, msg_count, last_msg_at, priority, status, 
             participants, tags, summary, decisions, actions) = row
             
            # Получаем последние сообщения
            cur.execute("""
                SELECT from_address, body_text, received_at
                FROM email_messages
                WHERE thread_id = %s
                ORDER BY received_at DESC
                LIMIT 3
            """, (thread_id,))
            messages = cur.fetchall()
    finally:
        conn.close()
    
    # Статус
    status_map = {
        'open': '📬 Открыта',
        'pending_resolution': '⏳ Ожидает подтверждения',
        'resolved': '✅ Решена',
        'archived': '📦 В архиве'
    }
    status_str = status_map.get(status, status or 'unknown')
    
    # Приоритет
    priority_map = {'high': '🔴 Высокий', 'medium': '🟡 Средний', 'low': '🟢 Низкий'}
    priority_str = priority_map.get(priority, priority or 'medium')
    
    # Формируем ответ
    response = (
        f"📧 *{truncate_text(subject or 'Без темы', 50)}*\n\n"
        f"*Статус:* {status_str}\n"
        f"*Приоритет:* {priority_str}\n"
        f"*Сообщений:* {msg_count or 0}\n"
        f"*Последнее:* {format_email_age(last_msg_at)}\n"
    )
    
    if participants:
        p_list = participants[:3] if isinstance(participants, list) else []
        if p_list:
            response += f"*Участники:* {', '.join(p_list)}\n"
    
    if tags and isinstance(tags, list):
        response += f"*Теги:* {', '.join(tags)}\n"
    
    if summary:
        response += f"\n📝 *Саммари:*\n{summary}\n"
        
        if decisions and isinstance(decisions, list):
            response += "\n*Решения:*\n"
            for d in decisions[:5]:
                response += f"✓ {d}\n"
        
        if actions:
            items = actions if isinstance(actions, list) else json.loads(actions) if isinstance(actions, str) else []
            if items:
                response += "\n*Задачи:*\n"
                for item in items[:5]:
                    if isinstance(item, dict):
                        assignee = item.get('assignee', '?')
                        task = item.get('task', '')
                        response += f"• {assignee}: {task}\n"
    
    # Последние сообщения
    if messages:
        response += "\n📜 *Последние сообщения:*\n"
        for from_addr, body, received_at in messages:
            date_str = received_at.strftime('%d.%m %H:%M') if received_at else ""
            body_short = truncate_text(body or "", 150)
            response += f"\n_{from_addr}_ ({date_str}):\n{body_short}\n"
    
    # Кнопки
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Решена", callback_data=f"email_resolve:{thread_id}"),
            InlineKeyboardButton("📦 Архив", callback_data=f"email_archive:{thread_id}"),
        ]
    ])
    
    await update.message.reply_text(response[:4000], parse_mode="Markdown", reply_markup=keyboard)


async def email_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-кнопок для email."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith("email_resolve:"):
        thread_id = int(data.split(":")[1])
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE email_threads
                    SET status = 'resolved', resolution_confirmed = true, updated_at = NOW()
                    WHERE id = %s
                """, (thread_id,))
                conn.commit()
        finally:
            conn.close()
        await query.answer("✅ Ветка отмечена как решённая")
        await query.edit_message_reply_markup(reply_markup=None)
    
    elif data.startswith("email_archive:"):
        thread_id = int(data.split(":")[1])
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE email_threads SET status = 'archived', updated_at = NOW() WHERE id = %s
                """, (thread_id,))
                conn.commit()
        finally:
            conn.close()
        await query.answer("📦 Ветка перемещена в архив")
        await query.edit_message_reply_markup(reply_markup=None)


async def email_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику email логгера."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Проверяем существование таблиц
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'email_messages'
                )
            """)
            if not cur.fetchone()[0]:
                await update.message.reply_text(
                    "📊 Email логгер ещё не настроен.\n\n"
                    "Примените миграцию:\n"
                    "`psql -d knowledge_base -f 001_init_email_logger.sql`",
                    parse_mode="Markdown"
                )
                return
            
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM monitored_mailboxes WHERE is_active = true),
                    (SELECT COUNT(*) FROM email_messages),
                    (SELECT COUNT(*) FROM email_threads),
                    (SELECT COUNT(*) FROM email_threads WHERE status = 'open'),
                    (SELECT COUNT(*) FROM email_attachments),
                    (SELECT COUNT(*) FROM email_attachments WHERE analysis_status = 'pending')
            """)
            mailboxes, messages, threads, open_threads, attachments, pending = cur.fetchone()
            
            cur.execute("""
                SELECT email, last_sync_at, sync_status
                FROM monitored_mailboxes
                WHERE last_sync_at IS NOT NULL
                ORDER BY last_sync_at DESC
                LIMIT 1
            """)
            last_sync = cur.fetchone()
            
            cur.execute("""
                SELECT COUNT(*) FROM monitored_mailboxes WHERE sync_status = 'error'
            """)
            error_count = cur.fetchone()[0]
    finally:
        conn.close()
    
    text = (
        "📊 *Статистика Email Логгера:*\n\n"
        f"📬 Ящиков: {mailboxes or 0}\n"
        f"📨 Сообщений: {messages or 0:,}\n"
        f"🔗 Веток: {threads or 0} (открытых: {open_threads or 0})\n"
        f"📎 Вложений: {attachments or 0} (в очереди: {pending or 0})\n"
    )
    
    if last_sync:
        email, sync_at, status = last_sync
        text += f"\n*Последняя синхронизация:*\n{email} — {format_email_age(sync_at)}\n"
    
    if error_count:
        text += f"\n⚠️ Ящиков с ошибками: {error_count}"
    
    await update.message.reply_text(text, parse_mode="Markdown")


async def sync_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статус синхронизации ящиков."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Только для администраторов")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT email, sync_status, last_sync_at
                FROM monitored_mailboxes
                WHERE is_active = true
                ORDER BY last_sync_at DESC NULLS LAST
                LIMIT 30
            """)
            mailboxes = cur.fetchall()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return
    finally:
        conn.close()
    
    if not mailboxes:
        await update.message.reply_text("📬 Нет активных почтовых ящиков")
        return
    
    status_icons = {'idle': '✅', 'syncing': '🔄', 'initial_load': '📥', 'error': '❌'}
    
    text = "📬 *Статус синхронизации:*\n\n"
    
    for email, status, last_sync in mailboxes:
        icon = status_icons.get(status or 'idle', '❓')
        age = format_email_age(last_sync) if last_sync else "—"
        mailbox_name = email.split('@')[0] if email else "?"
        text += f"{icon} `{mailbox_name}` {age}\n"
    
    await update.message.reply_text(text[:4000], parse_mode="Markdown")


async def search_email_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск по email сообщениям."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    if not context.args:
        await update.message.reply_text(
            "🔍 *Поиск по email:*\n\n"
            "`/search_email накладная сахар`",
            parse_mode="Markdown"
        )
        return
    
    query_text = ' '.join(context.args)
    
    if len(query_text) < 3:
        await update.message.reply_text("Запрос слишком короткий")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    m.subject,
                    m.from_address,
                    m.received_at,
                    t.id as thread_id
                FROM email_messages m
                LEFT JOIN email_threads t ON t.id = m.thread_id
                WHERE 
                    m.subject ILIKE %s OR
                    m.body_text ILIKE %s OR
                    m.from_address ILIKE %s
                ORDER BY m.received_at DESC
                LIMIT 10
            """, (f"%{query_text}%", f"%{query_text}%", f"%{query_text}%"))
            results = cur.fetchall()
    finally:
        conn.close()
    
    if not results:
        await update.message.reply_text(f"❌ По запросу «{query_text}» ничего не найдено")
        return
    
    text = f"🔍 *Результаты «{query_text}»:*\n\n"
    
    for subject, from_addr, received_at, thread_id in results:
        subject_short = truncate_text(subject or "Без темы", 40)
        date = received_at.strftime('%d.%m.%Y') if received_at else ""
        thread_link = f"/emailthread\\_{thread_id}" if thread_id else ""
        
        text += f"📧 *{subject_short}*\n"
        text += f"   {from_addr or '?'} • {date}\n"
        if thread_link:
            text += f"   {thread_link}\n"
        text += "\n"
    
    await update.message.reply_text(text[:4000], parse_mode="Markdown")


async def add_employee_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет сотрудника."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Только для администраторов")
        return
    
    if not context.args:
        await update.message.reply_text(
            "👤 *Добавление сотрудника:*\n\n"
            "`/add_employee Иванов Иван | Бухгалтерия | Бухгалтер`\n"
            "`/add_employee Петрова Мария | Производство`\n"
            "`/add_employee Сидоров Пётр`",
            parse_mode="Markdown"
        )
        return
    
    full_text = ' '.join(context.args)
    parts = [p.strip() for p in full_text.split('|')]
    
    full_name = parts[0] if len(parts) > 0 else None
    department = parts[1] if len(parts) > 1 else None
    position = parts[2] if len(parts) > 2 else None
    
    if not full_name:
        await update.message.reply_text("❌ Укажите имя")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO employees (full_name, department, position, is_active)
                VALUES (%s, %s, %s, true)
                RETURNING id
            """, (full_name, department, position))
            emp_id = cur.fetchone()[0]
            conn.commit()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return
    finally:
        conn.close()
    
    text = f"✅ *Сотрудник добавлен:*\n\n👤 {full_name}\n"
    if department:
        text += f"🏢 {department}\n"
    if position:
        text += f"💼 {position}\n"
    text += f"\nID: {emp_id}"
    
    await update.message.reply_text(text, parse_mode="Markdown")


async def assign_email_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Назначает email сотруднику."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Только для администраторов")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "📧 *Назначение email:*\n\n"
            "`/assign_email <ID сотрудника> <email>`\n\n"
            "Пример:\n"
            "`/assign_email 1 accountant@totsamiy.com`\n\n"
            "Список сотрудников: /list\\_employees",
            parse_mode="Markdown"
        )
        return
    
    try:
        employee_id = int(context.args[0])
        email = context.args[1].lower()
    except:
        await update.message.reply_text("❌ Формат: /assign_email <ID> <email>")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Проверяем сотрудника
            cur.execute("SELECT full_name FROM employees WHERE id = %s", (employee_id,))
            emp = cur.fetchone()
            if not emp:
                await update.message.reply_text(f"❌ Сотрудник ID {employee_id} не найден")
                return
            
            # Назначаем email
            cur.execute("""
                INSERT INTO employee_emails (employee_id, email, is_primary, assigned_by)
                VALUES (%s, %s, true, %s)
                ON CONFLICT (employee_id, email) DO NOTHING
            """, (employee_id, email, update.effective_user.id))
            conn.commit()
    finally:
        conn.close()
    
    await update.message.reply_text(
        f"✅ *Email назначен:*\n\n"
        f"👤 {emp[0]}\n"
        f"📧 {email}",
        parse_mode="Markdown"
    )


async def list_employees_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список сотрудников."""
    
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT e.id, e.full_name, e.department, 
                       array_agg(ee.email) FILTER (WHERE ee.email IS NOT NULL) as emails
                FROM employees e
                LEFT JOIN employee_emails ee ON ee.employee_id = e.id
                WHERE e.is_active = true
                GROUP BY e.id, e.full_name, e.department
                ORDER BY e.full_name
                LIMIT 30
            """)
            employees = cur.fetchall()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return
    finally:
        conn.close()
    
    if not employees:
        await update.message.reply_text("👤 Нет сотрудников. Добавьте через /add\\_employee", parse_mode="Markdown")
        return
    
    text = "👥 *Сотрудники:*\n\n"
    
    for emp_id, name, dept, emails in employees:
        dept_str = f" ({dept})" if dept else ""
        email_str = f"\n   📧 {', '.join(emails)}" if emails and emails[0] else ""
        text += f"*{emp_id}.* {name}{dept_str}{email_str}\n"
    
    await update.message.reply_text(text[:4000], parse_mode="Markdown")


# ============================================================
# ДОБАВИТЬ В main() после существующих команд:
# ============================================================
#
# from telegram.ext import CallbackQueryHandler
#
# # Email Logger команды
# application.add_handler(CommandHandler("threads", open_threads_command))
# application.add_handler(CommandHandler("open_threads", open_threads_command))
# application.add_handler(CommandHandler("email_stats", email_stats_command))
# application.add_handler(CommandHandler("sync_status", sync_status_command))
# application.add_handler(CommandHandler("search_email", search_email_command))
# application.add_handler(CommandHandler("add_employee", add_employee_command))
# application.add_handler(CommandHandler("assign_email", assign_email_command))
# application.add_handler(CommandHandler("list_employees", list_employees_command))
#
# # Обработчик /emailthread_N
# application.add_handler(MessageHandler(
#     filters.Regex(r'^/emailthread_\d+'),
#     show_email_thread_command
# ))
#
# # Callback для email кнопок
# application.add_handler(CallbackQueryHandler(
#     email_callback_handler,
#     pattern=r'^email_'
# ))
#
# ============================================================
