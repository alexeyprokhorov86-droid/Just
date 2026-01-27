#!/usr/bin/env python3
import os
import pathlib
from dotenv import load_dotenv
import psycopg2

from embedding_service import index_email_chunk
from email_text_processing import build_email_chunks

# env
env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

def main(limit: int | None = None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, subject, body_text, body_html
                FROM email_messages
                ORDER BY id ASC
            """)
            rows = cur.fetchall()

        processed = 0
        for email_id, subject, body_text, body_html in rows:
            chunks = build_email_chunks(subject or "", body_text or "", body_html or "")
            if not chunks:
                continue

            # удаляем старые чанки этого письма (чтобы не оставались хвосты)
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM embeddings
                    WHERE source_type='email'
                      AND source_table='email_messages'
                      AND source_id=%s
                """, (email_id,))
            conn.commit()

            for idx, ch in enumerate(chunks):
                index_email_chunk(email_id=email_id, chunk_index=idx, content=ch)

            processed += 1
            if processed % 200 == 0:
                print(f"Reindexed emails: {processed}")

            if limit and processed >= limit:
                break

        print(f"Done. Total reindexed emails: {processed}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
