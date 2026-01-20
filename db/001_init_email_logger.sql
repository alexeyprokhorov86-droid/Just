-- =====================================================
-- Email Logger Database Schema
-- Миграция: 001_init_email_logger.sql
-- Дата: 2025-01-18
-- =====================================================

-- Расширение для векторного поиска (если ещё не установлено)
CREATE EXTENSION IF NOT EXISTS vector;

-- =====================================================
-- 1. СОТРУДНИКИ (центральная таблица)
-- =====================================================
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    name_1c VARCHAR(255),                   -- наименование в 1С
    telegram_id BIGINT UNIQUE,
    department VARCHAR(100),
    position VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_employees_telegram ON employees(telegram_id);
CREATE INDEX IF NOT EXISTS idx_employees_name ON employees(full_name);
CREATE INDEX IF NOT EXISTS idx_employees_name_1c ON employees(name_1c);

-- =====================================================
-- 2. СВЯЗЬ СОТРУДНИК <-> EMAIL
-- =====================================================
CREATE TABLE IF NOT EXISTS employee_emails (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    is_primary BOOLEAN DEFAULT false,       -- основной email сотрудника
    assigned_at TIMESTAMP DEFAULT NOW(),
    assigned_by BIGINT,                     -- telegram_id админа
    UNIQUE(employee_id, email)
);

CREATE INDEX IF NOT EXISTS idx_employee_emails_email ON employee_emails(email);
CREATE INDEX IF NOT EXISTS idx_employee_emails_employee ON employee_emails(employee_id);

-- =====================================================
-- 3. МОНИТОРИМЫЕ ПОЧТОВЫЕ ЯЩИКИ
-- =====================================================
CREATE TABLE IF NOT EXISTS monitored_mailboxes (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    imap_server VARCHAR(255) DEFAULT 'imap.nicmail.ru',
    imap_port INTEGER DEFAULT 993,
    use_ssl BOOLEAN DEFAULT true,
    is_active BOOLEAN DEFAULT true,
    last_sync_at TIMESTAMP,
    last_uid_inbox INTEGER DEFAULT 0,       -- последний обработанный UID во Входящих
    last_uid_sent INTEGER DEFAULT 0,        -- последний обработанный UID в Отправленных
    sync_status VARCHAR(50) DEFAULT 'idle', -- idle/syncing/error
    last_error TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mailboxes_active ON monitored_mailboxes(is_active);

-- =====================================================
-- 4. ВЕТКИ ПЕРЕПИСКИ
-- =====================================================
CREATE TABLE IF NOT EXISTS email_threads (
    id SERIAL PRIMARY KEY,
    thread_id VARCHAR(500) UNIQUE,          -- первый Message-ID в цепочке
    subject_normalized VARCHAR(500),        -- тема без Re:/Fwd:
    started_at TIMESTAMP,                   -- время первого письма
    last_message_at TIMESTAMP,              -- время последнего письма
    message_count INTEGER DEFAULT 1,
    participant_emails TEXT[],              -- все участники ветки
    participant_employee_ids INTEGER[],     -- связанные сотрудники
    
    -- Статус и решение
    status VARCHAR(50) DEFAULT 'open',      -- open/pending_resolution/resolved/archived
    resolution_detected_at TIMESTAMP,
    resolution_confirmed BOOLEAN DEFAULT false,
    
    -- AI-саммари
    summary_short TEXT,                     -- краткое саммари (2-3 предложения)
    summary_detailed TEXT,                  -- подробное саммари
    key_decisions TEXT[],                   -- массив ключевых решений
    action_items JSONB,                     -- задачи: [{assignee, task, deadline}]
    summary_generated_at TIMESTAMP,
    summary_model VARCHAR(50),
    
    -- Метаданные
    topic_tags TEXT[],                      -- теги темы
    priority VARCHAR(20) DEFAULT 'medium',  -- high/medium/low
    sentiment VARCHAR(20),                  -- positive/neutral/negative/conflict
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_threads_status ON email_threads(status);
CREATE INDEX IF NOT EXISTS idx_threads_last_message ON email_threads(last_message_at DESC);
CREATE INDEX IF NOT EXISTS idx_threads_subject ON email_threads(subject_normalized);

-- =====================================================
-- 5. EMAIL СООБЩЕНИЯ
-- =====================================================
CREATE TABLE IF NOT EXISTS email_messages (
    id SERIAL PRIMARY KEY,
    message_uid INTEGER,                    -- IMAP UID
    message_id VARCHAR(500),                -- Message-ID заголовок
    in_reply_to VARCHAR(500),               -- ссылка на родительское письмо
    references_list TEXT[],                 -- массив всех Message-ID в цепочке
    
    thread_id INTEGER REFERENCES email_threads(id) ON DELETE SET NULL,
    mailbox_id INTEGER REFERENCES monitored_mailboxes(id) ON DELETE CASCADE,
    folder VARCHAR(50),                     -- INBOX / Sent
    direction VARCHAR(10),                  -- inbound / outbound
    
    from_address VARCHAR(255),
    from_employee_id INTEGER REFERENCES employees(id) ON DELETE SET NULL,
    to_addresses TEXT[],
    cc_addresses TEXT[],
    bcc_addresses TEXT[],
    
    subject TEXT,
    subject_normalized VARCHAR(500),
    body_text TEXT,
    body_html TEXT,
    
    has_attachments BOOLEAN DEFAULT false,
    attachment_count INTEGER DEFAULT 0,
    
    received_at TIMESTAMP,
    processed_at TIMESTAMP DEFAULT NOW(),
    
    -- Для RAG
    embedding vector(1536),
    
    UNIQUE(mailbox_id, folder, message_uid)
);

CREATE INDEX IF NOT EXISTS idx_messages_thread ON email_messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_received ON email_messages(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_message_id ON email_messages(message_id);
CREATE INDEX IF NOT EXISTS idx_messages_in_reply_to ON email_messages(in_reply_to);
CREATE INDEX IF NOT EXISTS idx_messages_mailbox ON email_messages(mailbox_id);
CREATE INDEX IF NOT EXISTS idx_messages_from ON email_messages(from_address);
CREATE INDEX IF NOT EXISTS idx_messages_from_employee ON email_messages(from_employee_id);

-- =====================================================
-- 6. ВЛОЖЕНИЯ
-- =====================================================
CREATE TABLE IF NOT EXISTS email_attachments (
    id SERIAL PRIMARY KEY,
    message_id INTEGER REFERENCES email_messages(id) ON DELETE CASCADE,
    filename VARCHAR(255),
    content_type VARCHAR(100),
    size_bytes INTEGER,
    storage_path VARCHAR(500),              -- путь к файлу
    
    -- Анализ
    analysis_text TEXT,                     -- результат OCR/Vision
    analysis_status VARCHAR(20) DEFAULT 'pending', -- pending/processing/completed/failed
    analysis_model VARCHAR(50),
    analyzed_at TIMESTAMP,
    analysis_error TEXT,
    
    -- Для RAG
    embedding vector(1536),
    
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_attachments_message ON email_attachments(message_id);
CREATE INDEX IF NOT EXISTS idx_attachments_status ON email_attachments(analysis_status);

-- =====================================================
-- 7. ЛОГ ГЕНЕРАЦИИ САММАРИ
-- =====================================================
CREATE TABLE IF NOT EXISTS thread_summary_log (
    id SERIAL PRIMARY KEY,
    thread_id INTEGER REFERENCES email_threads(id) ON DELETE CASCADE,
    trigger_type VARCHAR(50),               -- auto_detected/manual/scheduled
    trigger_message_id INTEGER REFERENCES email_messages(id) ON DELETE SET NULL,
    prompt_used TEXT,
    response_raw TEXT,
    tokens_used INTEGER,
    model VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_summary_log_thread ON thread_summary_log(thread_id);

-- =====================================================
-- 8. ЛОГ СИНХРОНИЗАЦИИ
-- =====================================================
CREATE TABLE IF NOT EXISTS sync_log (
    id SERIAL PRIMARY KEY,
    mailbox_id INTEGER REFERENCES monitored_mailboxes(id) ON DELETE CASCADE,
    folder VARCHAR(50),
    started_at TIMESTAMP DEFAULT NOW(),
    finished_at TIMESTAMP,
    messages_processed INTEGER DEFAULT 0,
    messages_new INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    last_error TEXT,
    status VARCHAR(20) DEFAULT 'running'    -- running/completed/failed
);

CREATE INDEX IF NOT EXISTS idx_sync_log_mailbox ON sync_log(mailbox_id, started_at DESC);

-- =====================================================
-- 9. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
-- =====================================================

-- Функция обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Триггеры для автообновления updated_at
DROP TRIGGER IF EXISTS update_employees_updated_at ON employees;
CREATE TRIGGER update_employees_updated_at
    BEFORE UPDATE ON employees
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_threads_updated_at ON email_threads;
CREATE TRIGGER update_threads_updated_at
    BEFORE UPDATE ON email_threads
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 10. НАЧАЛЬНЫЕ ДАННЫЕ - ПОЧТОВЫЕ ЯЩИКИ
-- =====================================================
INSERT INTO monitored_mailboxes (email) VALUES
    ('1c-its@totsamiy.com'),
    ('accountant_1@totsamiy.com'),
    ('aho@totsamiy.com'),
    ('assistant@totsamiy.com'),
    ('brigadir1@totsamiy.com'),
    ('brigadir2@totsamiy.com'),
    ('business@totsamiy.com'),
    ('cadri@totsamiy.com'),
    ('chef@totsamiy.com'),
    ('chop@totsamiy.com'),
    ('control@totsamiy.com'),
    ('controlling@totsamiy.com'),
    ('directorprod@totsamiy.com'),
    ('document1@totsamiy.com'),
    ('document2@totsamiy.com'),
    ('education@totsamiy.com'),
    ('executive@totsamiy.com'),
    ('factoring@totsamiy.com'),
    ('fasovka@totsamiy.com'),
    ('gaskov@totsamiy.com'),
    ('glavbuh@totsamiy.com'),
    ('glavtehnolog@totsamiy.com'),
    ('grechkina@totsamiy.com'),
    ('headzakupki@totsamiy.com'),
    ('hr@totsamiy.com'),
    ('ip@totsamiy.com'),
    ('it@totsamiy.com'),
    ('kachestvo@totsamiy.com'),
    ('kivanov@totsamiy.com'),
    ('konditer@totsamiy.com'),
    ('kro@totsamiy.com'),
    ('manager3@totsamiy.com'),
    ('mm@totsamiy.com'),
    ('nebula-zyxel@totsamiy.com'),
    ('noreply@totsamiy.com'),
    ('od@totsamiy.com'),
    ('office@totsamiy.com'),
    ('oper@totsamiy.com'),
    ('operating@totsamiy.com'),
    ('operator@totsamiy.com'),
    ('podbor@totsamiy.com'),
    ('postmaster@totsamiy.com'),
    ('prescription@totsamiy.com'),
    ('proizvodstvo@totsamiy.com'),
    ('rukprod@totsamiy.com'),
    ('sale@totsamiy.com'),
    ('sale1@totsamiy.com'),
    ('sale2@totsamiy.com'),
    ('sb@totsamiy.com'),
    ('scan@totsamiy.com'),
    ('security@totsamiy.com'),
    ('shiftsupervisor@totsamiy.com'),
    ('shiftsupervisor1@totsamiy.com'),
    ('sklad@totsamiy.com'),
    ('sklad1@totsamiy.com'),
    ('sverka@totsamiy.com'),
    ('tehnolog@totsamiy.com'),
    ('tehnolog1@totsamiy.com'),
    ('tehnolog2@totsamiy.com'),
    ('tehnolog3@totsamiy.com'),
    ('tehnolog4@totsamiy.com'),
    ('tehnolog5@totsamiy.com'),
    ('tehnologg2@totsamiy.com'),
    ('tikhonova@totsamiy.com'),
    ('v.ryabov@totsamiy.com'),
    ('zakaz1@totsamiy.com'),
    ('zakupki1@totsamiy.com'),
    ('zakupki2@totsamiy.com'),
    ('zambuh@totsamiy.com'),
    ('zavsklad@totsamiy.com'),
    -- lacannelle.ru (11 ящиков)
    ('aho@lacannelle.ru'),
    ('alexey@lacannelle.ru'),
    ('bot@lacannelle.ru'),
    ('document@lacannelle.ru'),
    ('irina@lacannelle.ru'),
    ('postmaster@lacannelle.ru'),
    ('sale@lacannelle.ru'),
    ('tehnolog@lacannelle.ru'),
    ('zakaz@lacannelle.ru'),
    ('zakupki@lacannelle.ru'),
    ('zakupki1@lacannelle.ru')
ON CONFLICT (email) DO NOTHING;

-- =====================================================
-- ГОТОВО!
-- =====================================================
