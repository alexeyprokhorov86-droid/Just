"""
Миграция данных в Canonical Zone (source_documents + source_chunks).
Фаза 1: Telegram + Email -> source_documents
"""
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv('/home/admin/telegram_logger_bot/.env')

CONFIG_PG = {
    'host': '172.17.0.2',
    'port': 5432,
    'dbname': 'knowledge_base',
    'user': 'knowledge',
    'password': os.getenv('PG_PASSWORD', 'Prokhorov2025Secure')
}

def get_tg_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT tablename FROM pg_tables 
        WHERE schemaname='public' AND tablename LIKE 'tg_chat_%' 
        AND tablename != 'tg_chats_metadata'
        ORDER BY tablename
    """)
    tables = [r[0] for r in cur.fetchall()]
    cur.close()
    return tables

def get_chat_metadata(conn):
    cur = conn.cursor()
    cur.execute("SELECT table_name, chat_title, description FROM tg_chats_metadata")
    meta = {}
    for row in cur.fetchall():
        meta[row[0]] = {'title': row[1], 'description': row[2]}
    cur.close()
    return meta

def migrate_telegram(conn, batch_size=1000):
    tables = get_tg_tables(conn)
    chat_meta = get_chat_metadata(conn)
    
    total = 0
    for table in tables:
        meta = chat_meta.get(table, {})
        chat_title = meta.get('title', table)
        
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM source_documents 
            WHERE source_kind = 'telegram_message' AND channel_ref = %s
        """, (table,))
        existing = cur.fetchone()[0]
        
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        total_in_table = cur.fetchone()[0]
        
        if existing >= total_in_table:
            print(f"  ok {table}: {existing} уже мигрировано")
            cur.close()
            continue
        
        cur.execute(f"""
            SELECT id, timestamp, first_name, last_name, username, user_id,
                   message_text, media_analysis, content_text, message_type
            FROM "{table}"
            ORDER BY id
        """)
        
        rows = cur.fetchall()
        inserted = 0
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            values = []
            for row in batch:
                msg_id, ts, first_name, last_name, username, user_id, \
                    msg_text, media_analysis, content_text, msg_type = row
                
                body = content_text or ''
                if not body:
                    parts = []
                    if msg_text:
                        parts.append(msg_text)
                    if media_analysis:
                        parts.append(media_analysis)
                    body = '\n'.join(parts)
                
                if not body or not body.strip():
                    continue
                
                author = ' '.join(filter(None, [first_name, last_name])) or username or str(user_id)
                source_ref = f"{table}:{msg_id}"
                
                values.append((
                    'telegram_message',
                    source_ref,
                    None,
                    body.strip(),
                    ts,
                    author,
                    str(user_id) if user_id else None,
                    table,
                    chat_title,
                    'ru',
                    False,
                    1.0,
                    psycopg2.extras.Json({
                        'message_type': msg_type,
                        'username': username,
                        'has_media': bool(media_analysis)
                    })
                ))
            
            if values:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO source_documents 
                       (source_kind, source_ref, title, body_text, doc_date,
                        author_name, author_ref, channel_ref, channel_name,
                        language, is_deleted, confidence, meta)
                       VALUES %s
                       ON CONFLICT (source_kind, source_ref) DO NOTHING""",
                    values,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                inserted += len(values)
        
        conn.commit()
        total += inserted
        print(f"  +{inserted} {table} (было {existing}, всего {total_in_table})")
        cur.close()
    
    print(f"\nTelegram: мигрировано {total}")
    return total

def migrate_email(conn, batch_size=2000):
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM source_documents WHERE source_kind = 'email_message'")
    existing = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM email_messages")
    total_emails = cur.fetchone()[0]
    
    print(f"\nEmail: {existing} уже из {total_emails}")
    
    if existing >= total_emails:
        print("  Все email уже мигрированы")
        cur.close()
        return 0
    
    cur.execute("SELECT id, email_address FROM monitored_mailboxes")
    mailboxes = {r[0]: r[1] for r in cur.fetchall()}
    
    total_inserted = 0
    
    while True:
        cur.execute("""
            SELECT em.id, em.subject, em.body_text, em.received_at,
                   em.from_address, em.mailbox_id, em.direction,
                   em.has_attachments, em.thread_id, em.folder
            FROM email_messages em
            WHERE NOT EXISTS (
                SELECT 1 FROM source_documents sd
                WHERE sd.source_kind = 'email_message' 
                AND sd.source_ref = 'email:' || em.id::text
            )
            ORDER BY em.id
            LIMIT %s
        """, (batch_size,))
        
        rows = cur.fetchall()
        if not rows:
            break
        
        values = []
        for row in rows:
            em_id, subject, body, received_at, from_addr, mailbox_id, \
                direction, has_attach, thread_id, folder = row
            
            if not body or not body.strip():
                continue
            
            mailbox_email = mailboxes.get(mailbox_id, f'mailbox_{mailbox_id}')
            
            values.append((
                'email_message',
                f'email:{em_id}',
                subject,
                body.strip(),
                received_at,
                from_addr,
                from_addr,
                mailbox_email,
                mailbox_email,
                'ru',
                False,
                1.0,
                psycopg2.extras.Json({
                    'direction': direction,
                    'has_attachments': has_attach,
                    'thread_id': thread_id,
                    'folder': folder
                })
            ))
        
        if values:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO source_documents 
                   (source_kind, source_ref, title, body_text, doc_date,
                    author_name, author_ref, channel_ref, channel_name,
                    language, is_deleted, confidence, meta)
                   VALUES %s
                   ON CONFLICT (source_kind, source_ref) DO NOTHING""",
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            total_inserted += len(values)
        
        conn.commit()
        print(f"  ... {total_inserted}...")
    
    cur.close()
    print(f"  Email: +{total_inserted}")
    return total_inserted


if __name__ == '__main__':
    conn = psycopg2.connect(**CONFIG_PG)
    
    print("=" * 60)
    print("МИГРАЦИЯ В CANONICAL ZONE")
    print("=" * 60)
    
    print("\n[1] Telegram")
    tg_count = migrate_telegram(conn)
    
    print("\n[2] Email")
    em_count = migrate_email(conn)
    
    print("\n" + "=" * 60)
    print(f"ИТОГО: Telegram={tg_count}, Email={em_count}")
    print("=" * 60)
    
    conn.close()
