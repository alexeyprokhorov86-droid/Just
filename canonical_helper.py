"""Вспомогательные функции для записи в Canonical Zone (source_documents)."""
import psycopg2.extras
import json


def insert_source_document_tg(cur, table_name: str, chat_title: str, message_data: dict):
    """Вставка Telegram сообщения в source_documents.

    body_text = подпись пользователя + полный LLM-анализ вложения + сырой extract.
    Порядок важен: подпись/анализ дают семантику для embedding, content_text — для цитирования.
    """
    parts = []
    if message_data.get('message_text'):
        parts.append(message_data['message_text'])
    if message_data.get('media_analysis'):
        parts.append(f"[Анализ вложения]\n{message_data['media_analysis']}")
    if message_data.get('content_text'):
        parts.append(f"[Содержимое файла]\n{message_data['content_text']}")
    body = '\n\n'.join(parts)

    if not body or not body.strip():
        return

    first_name = message_data.get('first_name') or ''
    last_name = message_data.get('last_name') or ''
    author = f"{first_name} {last_name}".strip() or message_data.get('username') or str(message_data.get('user_id', ''))
    source_ref = f"{table_name}:{message_data.get('message_id', 0)}"
    user_id = message_data.get('user_id')

    try:
        cur.execute("""
            INSERT INTO source_documents
                (source_kind, source_ref, title, body_text, doc_date,
                 author_name, author_ref, channel_ref, channel_name,
                 language, is_deleted, confidence, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_kind, source_ref) DO UPDATE SET
                body_text = EXCLUDED.body_text,
                updated_at = NOW()
        """, (
            'telegram_message',
            source_ref,
            None,
            body.strip(),
            message_data.get('timestamp'),
            author,
            str(user_id) if user_id else None,
            table_name,
            chat_title or table_name,
            'ru',
            False,
            1.0,
            json.dumps({
                'message_type': message_data.get('message_type'),
                'username': message_data.get('username'),
                'has_media': bool(message_data.get('media_analysis'))
            })
        ))
    except Exception as e:
        print(f"[canonical] TG insert error: {e}")


def insert_source_document_email(cur, email_id: int, parsed, mailbox_email: str, direction: str):
    """Вставка email сообщения в source_documents."""
    body = parsed.body_text
    if not body or not body.strip():
        return

    try:
        cur.execute("""
            INSERT INTO source_documents
                (source_kind, source_ref, title, body_text, doc_date,
                 author_name, author_ref, channel_ref, channel_name,
                 language, is_deleted, confidence, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_kind, source_ref) DO NOTHING
        """, (
            'email_message',
            f'email:{email_id}',
            parsed.subject,
            body.strip(),
            parsed.received_at,
            parsed.from_address,
            parsed.from_address,
            mailbox_email,
            mailbox_email,
            'ru',
            False,
            1.0,
            json.dumps({
                'direction': direction,
                'has_attachments': parsed.has_attachments,
                'thread_id': None,
                'folder': None
            })
        ))
    except Exception as e:
        print(f"[canonical] Email insert error: {e}")
