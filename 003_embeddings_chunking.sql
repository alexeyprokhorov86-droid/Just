-- 003_embeddings_chunking.sql

ALTER TABLE embeddings
ADD COLUMN IF NOT EXISTS chunk_index integer NOT NULL DEFAULT 0;

-- старый unique constraint из вашего вывода:
-- "embeddings_source_table_source_id_key" UNIQUE (source_table, source_id)
ALTER TABLE embeddings
DROP CONSTRAINT IF EXISTS embeddings_source_table_source_id_key;

-- новый unique: теперь можно хранить несколько чанков на один source_id
CREATE UNIQUE INDEX IF NOT EXISTS embeddings_source_table_source_id_chunk_key
ON embeddings (source_table, source_id, chunk_index);

-- (необязательно, но полезно) индекс по source_type + source_table
CREATE INDEX IF NOT EXISTS embeddings_source_type_table_idx
ON embeddings (source_type, source_table);

-- Векторный HNSW индекс у вас уже есть, его не трогаем.
