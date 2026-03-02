-- =====================================================
-- Email threads status model migration (2-layer)
-- Миграция: 004_email_thread_status_model.sql
-- Дата: 2026-03-02
-- =====================================================

-- 1) Новые колонки статусной модели
ALTER TABLE email_threads
ADD COLUMN IF NOT EXISTS lifecycle_status VARCHAR(50);

ALTER TABLE email_threads
ADD COLUMN IF NOT EXISTS resolution_outcome VARCHAR(50);

-- 2) Backfill lifecycle_status из legacy status
UPDATE email_threads
SET lifecycle_status = CASE
    WHEN status = 'open' THEN 'open'
    WHEN status = 'pending_resolution' THEN 'pending_resolution'
    WHEN status = 'archived' THEN 'archived'
    WHEN status IN ('resolved', 'closed', 'cancelled') THEN 'closed'
    ELSE 'open'
END
WHERE lifecycle_status IS NULL;

-- 3) Backfill resolution_outcome из legacy status
UPDATE email_threads
SET resolution_outcome = CASE
    WHEN status IN ('resolved', 'closed') THEN 'resolved'
    WHEN status = 'cancelled' THEN 'cancelled'
    ELSE NULL
END
WHERE resolution_outcome IS NULL;

-- 4) Нормализация и ограничения
UPDATE email_threads
SET lifecycle_status = 'open'
WHERE lifecycle_status IS NULL;

ALTER TABLE email_threads
ALTER COLUMN lifecycle_status SET DEFAULT 'open';

ALTER TABLE email_threads
ALTER COLUMN lifecycle_status SET NOT NULL;

ALTER TABLE email_threads
DROP CONSTRAINT IF EXISTS email_threads_lifecycle_status_chk;

ALTER TABLE email_threads
ADD CONSTRAINT email_threads_lifecycle_status_chk
CHECK (lifecycle_status IN ('open', 'pending_resolution', 'closed', 'archived'));

ALTER TABLE email_threads
DROP CONSTRAINT IF EXISTS email_threads_resolution_outcome_chk;

ALTER TABLE email_threads
ADD CONSTRAINT email_threads_resolution_outcome_chk
CHECK (resolution_outcome IS NULL OR resolution_outcome IN ('resolved', 'cancelled', 'other'));

-- 5) Синхронизация legacy status (обратная совместимость)
UPDATE email_threads
SET status = CASE
    WHEN lifecycle_status = 'open' THEN 'open'
    WHEN lifecycle_status = 'pending_resolution' THEN 'pending_resolution'
    WHEN lifecycle_status = 'archived' THEN 'archived'
    WHEN lifecycle_status = 'closed' AND resolution_outcome = 'cancelled' THEN 'cancelled'
    WHEN lifecycle_status = 'closed' THEN 'resolved'
    ELSE COALESCE(status, 'open')
END;

-- 6) Индексы
CREATE INDEX IF NOT EXISTS idx_threads_lifecycle_status
ON email_threads(lifecycle_status);

CREATE INDEX IF NOT EXISTS idx_threads_resolution_outcome
ON email_threads(resolution_outcome);

COMMENT ON COLUMN email_threads.lifecycle_status IS
'Текущий этап ветки: open/pending_resolution/closed/archived';

COMMENT ON COLUMN email_threads.resolution_outcome IS
'Результат закрытия: resolved/cancelled/other (NULL пока ветка не закрыта)';
