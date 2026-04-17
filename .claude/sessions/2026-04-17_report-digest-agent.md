# Сессия: 2026-04-17 — Report Digest Agent

## Что сделано
- Создан `report_digest_agent.py` — парсит логи 3 ночных отчётов (audit_pipeline, review_knowledge, daily_report), выделяет actionable проблемы, вызывает `auto_fix.sh report_digest`
- Добавлен в cron: `15 8 * * *` (08:15, после daily_report в 08:00)
- Dry-run test пройден: промпт 11KB, контекст корректный

## Парсеры проблем

### audit_pipeline (01:00)
- `tg_attachment_errors` — ошибки при обработке TG-вложений (medium)
- `email_attachment_gaps` — email без анализа (low)
- `distillation_telegram_backlog` — telegram без distillation >100 (medium)
- `build_chunks_error` — exit code != 0 (high)
- `tg_download_failures` — массовые Telegram download failed (medium)
- `sql_column_missing` — отсутствующие колонки в БД (high)

### review_knowledge (05:00)
- `review_llm_errors` — ошибки LLM при ревью (high)
- `high_rejection_rate` — >30% отклонений (medium)

### daily_report (08:00)
- `rag_quality_low` — >50% insufficient (medium)
- `service_down` — сервис не работает (critical)
- `email_mailbox_errors` — >5 ящиков с ошибками (medium)
- `sync_1c_error` — ошибки синхронизации 1С (high)
- `suspicious_junk_rules` — подозрительные junk-правила (medium)
- `disk_usage_high` — >80% диска (high)

## Тестирование на данных 17.04
- Найдено 2 проблемы:
  - [high] sql_column_missing: storage_path в 3 таблицах (sekretariat, rukovodstvo_bridged)
  - [medium] tg_attachment_errors: 23 ошибки, 318 необработанных
- Dry-run auto_fix.sh: rate-check passed, промпт сформирован

## Изменённые файлы
- `report_digest_agent.py` — новый файл
- Cron: добавлена строка `15 8 * * * ... report_digest_agent.py >> report_digest.log 2>&1`

## Незавершённое
- Первый реальный запуск: завтра в 08:15
- Или можно запустить прямо сейчас (без --dry-run) для проверки на текущих данных

## Заметки
- Все 3 ночных скрипта пишут HTML-отчёт в свой лог через logger.info — парсер извлекает HTML по маркерам (<b>🔍, <b>🔬, <b>📊)
- audit_pipeline.log содержит ошибку `column "storage_path" does not exist` — 3 таблицы секретариат/руководство. Это реальный баг в audit_pipeline.py
