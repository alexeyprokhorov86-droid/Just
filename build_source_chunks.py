#!/usr/bin/env python3
"""
Разбивает source_documents на чанки и генерирует эмбеддинги.
Сохраняет в source_chunks с vector(768) для семантического поиска.

Использование:
  python build_source_chunks.py              # обработать всё новое
  python build_source_chunks.py --batch 1000 # ограничить количество документов
  python build_source_chunks.py --reset      # пересоздать все чанки
"""

import os
import sys
import logging
import psycopg2
import psycopg2.extras
import argparse
import re
import fcntl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# --- Настройки ---
CHUNK_SIZE = 500        # символов на чанк
CHUNK_OVERLAP = 100     # перекрытие
MIN_CHUNK_LEN = 30      # минимальная длина чанка
EMBED_BATCH_SIZE = 64   # батч для эмбеддингов
MIN_DOC_LEN = 25        # минимальная длина документа

DB_CONFIG = {
    'host': '172.17.0.2',
    'port': 5432,
    'dbname': 'knowledge_base',
    'user': 'knowledge',
    'password': os.environ.get('DB_PASSWORD', 'Prokhorov2025Secure')
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def split_into_chunks(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list:
    """Разбивает текст на чанки по границам предложений/абзацев."""
    if not text or len(text.strip()) < MIN_CHUNK_LEN:
        return []

    text = text.strip()

    # Короткий текст — один чанк
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    # Разбиваем на абзацы
    paragraphs = re.split(r'\n\s*\n', text)

    current_chunk = ""
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Если абзац сам по себе больше chunk_size — разбиваем по предложениям
        if len(para) > chunk_size:
            # Сначала сохраняем текущий чанк если есть
            if current_chunk:
                chunks.append(current_chunk.strip())
                # Берём overlap из конца текущего чанка
                current_chunk = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk

            # Разбиваем длинный абзац по предложениям
            sentences = re.split(r'(?<=[.!?;])\s+', para)
            for sent in sentences:
                if len(current_chunk) + len(sent) + 1 <= chunk_size:
                    current_chunk = (current_chunk + " " + sent).strip()
                else:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                        current_chunk = current_chunk[-overlap:] if len(current_chunk) > overlap else ""
                    # Если предложение само больше chunk_size — режем жёстко
                    if len(sent) > chunk_size:
                        for i in range(0, len(sent), chunk_size - overlap):
                            piece = sent[i:i + chunk_size]
                            if len(piece) >= MIN_CHUNK_LEN:
                                chunks.append(piece.strip())
                    else:
                        current_chunk = sent
        else:
            # Обычный абзац — добавляем к текущему чанку
            if len(current_chunk) + len(para) + 2 <= chunk_size:
                current_chunk = (current_chunk + "\n\n" + para).strip()
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                    current_chunk = current_chunk[-overlap:] if len(current_chunk) > overlap else ""
                current_chunk = (current_chunk + "\n\n" + para).strip() if current_chunk else para

    # Последний чанк
    if current_chunk and len(current_chunk.strip()) >= MIN_CHUNK_LEN:
        chunks.append(current_chunk.strip())

    return chunks


def load_embedding_model():
    """Загружает модель эмбеддингов."""
    from sentence_transformers import SentenceTransformer
    logger.info("Загружаем модель intfloat/multilingual-e5-base...")
    model = SentenceTransformer('intfloat/multilingual-e5-base')
    logger.info("Модель загружена")
    return model


def generate_embeddings(model, texts: list) -> list:
    """Генерирует эмбеддинги для списка текстов."""
    # e5 модели требуют префикс passage: для индексации
    prefixed = [f"passage: {t[:512]}" for t in texts]
    embeddings = model.encode(prefixed, normalize_embeddings=True, batch_size=EMBED_BATCH_SIZE)
    return embeddings.tolist()


def get_unprocessed_docs(conn, batch_size=500):
    """Получает документы без чанков."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sd.id, sd.body_text, sd.source_kind
            FROM source_documents sd
            LEFT JOIN source_chunks sc ON sc.document_id = sd.id
            WHERE sc.id IS NULL
              AND sd.body_text IS NOT NULL
              AND LENGTH(sd.body_text) >= %s
            ORDER BY sd.id
            LIMIT %s
        """, (MIN_DOC_LEN, batch_size))
        return cur.fetchall()


def process_batch(conn, model, docs):
    """Обрабатывает батч документов: чанкирует + эмбеддинги + вставка."""
    all_chunks = []  # (document_id, chunk_no, chunk_text, token_count)

    for doc_id, body_text, source_kind in docs:
        chunks = split_into_chunks(body_text)
        if not chunks:
            # Документ слишком короткий для чанков — вставляем целиком
            chunks = [body_text.strip()] if len(body_text.strip()) >= MIN_CHUNK_LEN else []

        for i, chunk_text in enumerate(chunks):
            token_count = len(chunk_text.split())
            all_chunks.append((doc_id, i, chunk_text, token_count))

    if not all_chunks:
        return 0

    # Генерируем эмбеддинги батчами
    chunk_texts = [c[2] for c in all_chunks]
    all_embeddings = []

    for i in range(0, len(chunk_texts), EMBED_BATCH_SIZE):
        batch_texts = chunk_texts[i:i + EMBED_BATCH_SIZE]
        batch_embs = generate_embeddings(model, batch_texts)
        all_embeddings.extend(batch_embs)

    # Вставляем в БД
    with conn.cursor() as cur:
        insert_data = []
        for idx, (doc_id, chunk_no, chunk_text, token_count) in enumerate(all_chunks):
            emb = all_embeddings[idx]
            insert_data.append((doc_id, chunk_no, chunk_text, str(emb), token_count))

        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO source_chunks (document_id, chunk_no, chunk_text, embedding, token_count)
               VALUES %s ON CONFLICT DO NOTHING""",
            insert_data,
            template="(%s, %s, %s, %s::vector, %s)"
        )
    conn.commit()
    return len(all_chunks)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', type=int, default=0, help='Макс документов (0=все)')
    parser.add_argument('--reset', action='store_true', help='Удалить все чанки и пересоздать')
    args = parser.parse_args()

    # Lock
    lock_file = open('/tmp/build_chunks.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("build_source_chunks уже запущен, выходим")
        sys.exit(0)

    conn = get_conn()

    if args.reset:
        logger.info("RESET: удаляем все чанки...")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE source_chunks CASCADE")
        conn.commit()
        logger.info("Все чанки удалены")

    # Загружаем модель
    model = load_embedding_model()

    total_chunks = 0
    total_docs = 0
    doc_batch_size = 200  # документов за один проход в БД

    while True:
        docs = get_unprocessed_docs(conn, batch_size=doc_batch_size)
        if not docs:
            break

        chunks_created = process_batch(conn, model, docs)
        total_docs += len(docs)
        total_chunks += chunks_created

        logger.info(f"Обработано: {total_docs} docs, создано: {total_chunks} chunks")

        if args.batch and total_docs >= args.batch:
            logger.info(f"Достигнут лимит --batch {args.batch}")
            break

    logger.info(f"ГОТОВО: {total_docs} документов -> {total_chunks} чанков")

    # Статистика
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(embedding) FROM source_chunks")
        total, with_emb = cur.fetchone()
        logger.info(f"Всего в source_chunks: {total} чанков, {with_emb} с эмбеддингами")

    conn.close()


if __name__ == '__main__':
    main()
