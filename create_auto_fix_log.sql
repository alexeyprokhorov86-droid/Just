-- Лог автономных фиксов Claude-агента (см. TASK_autonomous_agent.md, шаг 1)
CREATE TABLE IF NOT EXISTS auto_fix_log (
    id SERIAL PRIMARY KEY,
    trigger_name TEXT NOT NULL,
    trigger_context JSONB,
    claude_output TEXT,
    actions_taken TEXT[],
    git_commit_sha TEXT,
    reverted BOOLEAN DEFAULT false,
    revert_reason TEXT,
    health_check_before BOOLEAN,
    health_check_after BOOLEAN,
    telegram_reported BOOLEAN DEFAULT false,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT  -- 'success' | 'failed' | 'reverted' | 'rate_limited' | 'escalated' | 'dry_run'
);

CREATE INDEX IF NOT EXISTS idx_auto_fix_trigger_time
    ON auto_fix_log(trigger_name, started_at DESC);
