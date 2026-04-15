#!/usr/bin/env python3
"""
Backfill embedding_v2 for all source_chunks using Qwen3-Embedding-0.6B.
Supports resume — skips rows that already have embedding_v2.

Usage:
  nohup python3 backfill_embeddings_v2.py > backfill_embed.log 2>&1 &

Benchmark results (8-core CPU, 16GB RAM):
  - batch_size=32, max_chars=512: ~1.6 chunks/sec, ~51h for 292k
  - RAM: ~7GB peak, no OOM
"""
import os
import sys
import time
import logging

os.environ["TOKENIZERS_PARALLELISM"] = "false"

import psycopg2
import psycopg2.extras
from chunkers.config import DB_CONFIG, EMBEDDING_MODEL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# --- Config ---
BATCH_SIZE = 32        # encode batch (tested: no OOM on 16GB)
FETCH_SIZE = 500       # rows per DB fetch
MAX_TEXT_CHARS = 512   # truncation (cosine sim 0.99 vs 1000 chars)

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    # Count remaining
    cur.execute("SELECT count(*) FROM source_chunks WHERE embedding_v2 IS NULL")
    total = cur.fetchone()[0]
    log.info(f"Chunks to process: {total}")

    if total == 0:
        log.info("Nothing to do.")
        return

    # Load model
    log.info(f"Loading model: {EMBEDDING_MODEL}")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(EMBEDDING_MODEL)
    dim = model.get_sentence_embedding_dimension()
    log.info(f"Model loaded, dim={dim}")

    # Warmup
    model.encode(["warmup"], normalize_embeddings=True, convert_to_numpy=True, prompt_name="document")

    # Process in FETCH_SIZE batches using simple SELECT with LIMIT/OFFSET-like
    # approach via WHERE id > last_id (keyset pagination). This avoids the
    # server-side cursor issue where COMMIT invalidates named cursors.
    processed = 0
    t_start = time.time()
    t_last_log = t_start
    last_id = 0

    while True:
        cur.execute(
            "SELECT id, chunk_text FROM source_chunks "
            "WHERE embedding_v2 IS NULL AND id > %s ORDER BY id LIMIT %s",
            (last_id, FETCH_SIZE),
        )
        rows = cur.fetchall()
        if not rows:
            break

        # Encode in sub-batches of BATCH_SIZE
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            ids = [r[0] for r in batch]
            texts = [r[1][:MAX_TEXT_CHARS] if r[1] else "" for r in batch]

            _encode_and_queue(model, ids, texts, pending_updates := [])
            _flush_updates(conn, cur, pending_updates)
            processed += len(ids)

            # Log progress
            now = time.time()
            if now - t_last_log >= 30:
                elapsed = now - t_start
                rate = processed / elapsed if elapsed > 0 else 0
                eta_h = (total - processed) / rate / 3600 if rate > 0 else 0
                log.info(
                    f"Progress: {processed}/{total} ({100*processed/total:.1f}%) "
                    f"| {rate:.1f} chunks/sec | ETA: {eta_h:.1f}h"
                )
                t_last_log = now

        last_id = rows[-1][0]

    elapsed = time.time() - t_start
    rate = processed / elapsed if elapsed > 0 else 0
    log.info(f"Done! {processed} chunks in {elapsed/3600:.1f}h ({rate:.1f} chunks/sec)")

    cur.close()
    conn.close()


def _encode_and_queue(model, ids, texts, pending):
    vectors = model.encode(
        texts,
        batch_size=BATCH_SIZE,
        show_progress_bar=False,
        normalize_embeddings=True,
        convert_to_numpy=True,
        prompt_name="document",
    )
    for i, chunk_id in enumerate(ids):
        pending.append((vectors[i].tolist(), chunk_id))


def _flush_updates(conn, cur, updates):
    psycopg2.extras.execute_batch(
        cur,
        "UPDATE source_chunks SET embedding_v2 = %s::vector WHERE id = %s",
        updates,
        page_size=100,
    )
    conn.commit()


if __name__ == "__main__":
    main()
