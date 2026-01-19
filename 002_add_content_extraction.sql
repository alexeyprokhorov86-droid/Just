-- =====================================================
-- Content Extraction Migration
-- Миграция: 002_add_content_extraction.sql
-- Дата: 2026-01-19
-- Описание: Добавление поля для хранения извлеченного текстового содержимого
-- =====================================================

-- Добавление поля content_text в email_attachments
-- Это поле будет хранить:
-- - Транскрипты аудио/видео (через Whisper)
-- - Распознанный текст из изображений (OCR)
-- - Текстовое содержимое Word документов
-- - CSV представление Excel таблиц
-- - Текстовое содержимое PDF файлов
-- - Текстовое содержимое PowerPoint презентаций
ALTER TABLE email_attachments
ADD COLUMN IF NOT EXISTS content_text TEXT;

-- Создаем индекс для полнотекстового поиска по извлеченному содержимому
CREATE INDEX IF NOT EXISTS idx_attachments_content_text_fts
ON email_attachments
USING gin(to_tsvector('russian', COALESCE(content_text, '')));

-- Комбинированный индекс для поиска по анализу и содержимому
CREATE INDEX IF NOT EXISTS idx_attachments_combined_fts
ON email_attachments
USING gin(to_tsvector('russian', COALESCE(analysis_text, '') || ' ' || COALESCE(content_text, '')));

COMMENT ON COLUMN email_attachments.content_text IS 'Извлеченное текстовое содержимое файла: транскрипты, OCR, текст из документов, CSV из Excel и т.д.';

-- =====================================================
-- ГОТОВО!
-- =====================================================
