-- Migration 005: RAG query log + button log
CREATE TABLE IF NOT EXISTS rag_query_log (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    username VARCHAR(255),
    first_name VARCHAR(255),
    chat_id BIGINT,
    chat_type VARCHAR(20),
    question TEXT NOT NULL,
    primary_intent VARCHAR(50),
    detected_intents TEXT[],
    router_query_type VARCHAR(50),
    router_target_chats TEXT[],
    sources_used TEXT[],
    evidence_count INTEGER DEFAULT 0,
    evidence_sources JSONB,
    evaluator_sufficient BOOLEAN,
    retry_count INTEGER DEFAULT 0,
    rerank_applied BOOLEAN DEFAULT FALSE,
    response_length INTEGER,
    response_time_ms INTEGER,
    router_time_ms INTEGER,
    search_time_ms INTEGER,
    generation_time_ms INTEGER,
    web_search_used BOOLEAN DEFAULT FALSE,
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_rag_log_user ON rag_query_log(user_id);
CREATE INDEX idx_rag_log_created ON rag_query_log(created_at);
CREATE INDEX idx_rag_log_intent ON rag_query_log(primary_intent);

CREATE TABLE IF NOT EXISTS bot_button_log (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    button_type VARCHAR(50),
    context_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_button_log_created ON bot_button_log(created_at);
