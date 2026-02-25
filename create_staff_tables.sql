-- ==========================================================
-- Таблицы и представление для кадровых данных
-- Выполнить: docker exec -i knowledge_db psql -U knowledge -d knowledge_base < create_staff_tables.sql
-- ==========================================================

-- 1. Кадровая история сотрудников
CREATE TABLE IF NOT EXISTS c1_staff_history (
    id SERIAL PRIMARY KEY,
    recorder VARCHAR(50),
    recorder_type VARCHAR(200),
    period TIMESTAMP,
    line_number INTEGER,
    active BOOLEAN DEFAULT TRUE,
    employee_key VARCHAR(50),
    organization_key VARCHAR(50),
    department_key VARCHAR(50),
    position_key VARCHAR(50),
    position_staff_key VARCHAR(50),
    event_type VARCHAR(100),
    head_employee_key VARCHAR(50),
    contract_type VARCHAR(100),
    valid_until TIMESTAMP,
    is_head_employee BOOLEAN DEFAULT FALSE,
    num_rates NUMERIC(10,2) DEFAULT 1,
    physical_person_key VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(recorder, line_number)
);

CREATE INDEX IF NOT EXISTS idx_staff_history_employee ON c1_staff_history(employee_key);
CREATE INDEX IF NOT EXISTS idx_staff_history_period ON c1_staff_history(period DESC);
CREATE INDEX IF NOT EXISTS idx_staff_history_department ON c1_staff_history(department_key);
CREATE INDEX IF NOT EXISTS idx_staff_history_event ON c1_staff_history(event_type);

-- 2. Плановые начисления (оклад/тариф)
CREATE TABLE IF NOT EXISTS c1_planned_accruals (
    id SERIAL PRIMARY KEY,
    recorder VARCHAR(50),
    recorder_type VARCHAR(200),
    period TIMESTAMP,
    line_number INTEGER,
    active BOOLEAN DEFAULT TRUE,
    employee_key VARCHAR(50),
    accrual_key VARCHAR(50),
    physical_person_key VARCHAR(50),
    organization_key VARCHAR(50),
    is_used BOOLEAN DEFAULT TRUE,
    amount NUMERIC(15,2) DEFAULT 0,
    valid_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(recorder, line_number)
);

CREATE INDEX IF NOT EXISTS idx_planned_accruals_employee ON c1_planned_accruals(employee_key);
CREATE INDEX IF NOT EXISTS idx_planned_accruals_period ON c1_planned_accruals(period DESC);

-- 3. Добавить новые колонки в c1_employees
ALTER TABLE c1_employees ADD COLUMN IF NOT EXISTS is_nkt BOOLEAN DEFAULT FALSE;
ALTER TABLE c1_employees ADD COLUMN IF NOT EXISTS is_piece_work BOOLEAN DEFAULT FALSE;
ALTER TABLE c1_employees ADD COLUMN IF NOT EXISTS is_paid_by_shift BOOLEAN DEFAULT FALSE;

-- 4. Представление: текущие сотрудники с полной информацией
CREATE OR REPLACE VIEW v_current_staff AS
WITH latest_event AS (
    SELECT DISTINCT ON (employee_key)
        employee_key,
        period as event_date,
        event_type,
        organization_key,
        department_key,
        position_key,
        contract_type,
        num_rates,
        valid_until
    FROM c1_staff_history
    WHERE active = true
    ORDER BY employee_key, period DESC
),
latest_salary AS (
    SELECT DISTINCT ON (employee_key)
        employee_key,
        amount as salary,
        period as salary_date,
        is_used
    FROM c1_planned_accruals
    WHERE active = true
      AND amount > 0
    ORDER BY employee_key, period DESC
),
first_hire AS (
    SELECT DISTINCT ON (employee_key)
        employee_key,
        period as hire_date
    FROM c1_staff_history
    WHERE active = true
      AND event_type ILIKE '%прием%'
    ORDER BY employee_key, period ASC
),
dismissal AS (
    SELECT DISTINCT ON (employee_key)
        employee_key,
        period as dismissal_date
    FROM c1_staff_history
    WHERE active = true
      AND event_type ILIKE '%увольнен%'
    ORDER BY employee_key, period DESC
)
SELECT 
    e.ref_key,
    e.code as tab_number,
    e.name as full_name,
    e.is_archived,
    e.is_nkt,
    e.is_piece_work,
    e.is_paid_by_shift,
    le.event_type as last_event,
    le.event_date as last_event_date,
    le.contract_type,
    le.num_rates,
    fh.hire_date,
    dm.dismissal_date,
    ls.salary,
    ls.salary_date,
    dep.name as department_name,
    pos.name as position_name,
    le.organization_key,
    le.department_key,
    le.position_key
FROM c1_employees e
LEFT JOIN latest_event le ON le.employee_key = e.ref_key
LEFT JOIN latest_salary ls ON ls.employee_key = e.ref_key
LEFT JOIN first_hire fh ON fh.employee_key = e.ref_key
LEFT JOIN dismissal dm ON dm.employee_key = e.ref_key
LEFT JOIN c1_departments dep ON le.department_key = dep.ref_key
LEFT JOIN c1_positions pos ON le.position_key = pos.ref_key
WHERE NOT e.is_deleted;

-- 5. Проверка
SELECT 'Таблицы и представление созданы!' as status;
