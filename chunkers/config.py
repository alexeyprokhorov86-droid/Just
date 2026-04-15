"""
Конфигурация для build_chunks_v2 pipeline.
Параметры чанкинга, embedding, confidence levels.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# --- Database ---
DB_CONFIG = {
    "host": "172.20.0.2",
    "port": 5432,
    "dbname": "knowledge_base",
    "user": "knowledge",
    "password": os.getenv("DB_PASSWORD"),
}

# --- Chunk size targets (tokens) ---
CHUNK_SIZES = {
    "envelope": {"min": 50, "max": 100},
    "body": {"min": 100, "max": 400},
    "structured": {"min": 100, "max": 300},
    "distilled": {"min": 50, "max": 200},
}

BODY_SPLIT_THRESHOLDS = {
    "small": 300,   # < 300 tokens → one chunk
    "medium": 800,  # 300-800 → split by paragraphs
    # > 800 → split by paragraphs + overlap
}

BODY_MIN_PARAGRAPH_TOKENS = 50   # merge paragraphs smaller than this
BODY_OVERLAP_TOKENS = 50         # overlap between chunks for large bodies

# --- Confidence levels ---
CONFIDENCE = {
    "1c": 0.95,
    "email_envelope": 0.8,
    "email_body": 0.7,
    "km_fact": 0.7,
    "km_decision": 0.7,
    "km_task": 0.7,
    "km_policy": 0.7,
    "messenger_raw": 0.4,
    "attachment": 0.6,
}

# --- Messenger session grouping ---
MESSENGER_SESSION_GAP_MINUTES = 30

# --- Embedding ---
# Qwen3-Embedding-0.6B (будет настроено позже)
EMBEDDING_MODEL = "Qwen/Qwen3-Embedding-0.6B"
EMBEDDING_DIM = 1024
EMBEDDING_BATCH_SIZE = 128

# --- Batch processing ---
DB_BATCH_SIZE = 1000  # rows per INSERT batch
LOG_EVERY = 5000      # log progress every N chunks
