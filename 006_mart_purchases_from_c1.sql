-- Миграция 006 (2026-04-17): перестройка mart_purchases на свежие c1_* таблицы
--
-- ПРОБЛЕМА: mart_purchases был MV поверх purchase_prices (legacy таблица),
-- которая перестала обновляться после 2026-03-17. Все analytics tools
-- (purchases_by_nomenclature, top_suppliers, purchase_summary) работают
-- через mart_purchases — поэтому все закупки за апрель были потеряны.
--
-- c1_purchases + c1_purchase_items + contractors + nomenclature — свежие
-- (обновляются через sync_1c_full). Перестраиваем MV на них.
--
-- REFRESH уже в cron каждые 10 мин для этого имени MV, ничего дополнительно не нужно.

DROP MATERIALIZED VIEW IF EXISTS mart_purchases CASCADE;

CREATE MATERIALIZED VIEW mart_purchases AS
SELECT
    pi.id AS id,
    p.doc_date,
    p.doc_number,
    COALESCE(c.name, p.partner_key)::varchar(500) AS contractor_name,
    n.name AS nomenclature_name,
    pi.quantity,
    pi.price,
    pi.sum_total,
    EXTRACT(year FROM p.doc_date)::int AS year,
    EXTRACT(month FROM p.doc_date)::int AS month,
    to_char(p.doc_date, 'YYYY-MM') AS year_month
FROM c1_purchases p
JOIN c1_purchase_items pi ON pi.doc_key = p.ref_key
LEFT JOIN nomenclature n ON n.id::text = pi.nomenclature_key
LEFT JOIN contractors c ON c.id::text = p.partner_key
WHERE p.is_deleted = false
  AND p.doc_date IS NOT NULL;

-- Unique index для поддержки REFRESH CONCURRENTLY если понадобится
CREATE UNIQUE INDEX mart_purchases_pk ON mart_purchases(id);
CREATE INDEX idx_mart_purch_date ON mart_purchases(doc_date);
CREATE INDEX idx_mart_purch_contr ON mart_purchases(contractor_name);
CREATE INDEX idx_mart_purch_ym ON mart_purchases(year_month);
CREATE INDEX idx_mart_purch_nom ON mart_purchases(nomenclature_name);
