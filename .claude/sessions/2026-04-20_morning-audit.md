# Сессия: 2026-04-20 — Утренний аудит + приоритизация

## Что сделано
- [11:20] Старт: прочитаны CLAUDE.md, MEMORY.md, 3 последних session-лога. Git up-to-date (main = 7a99d6e). Untracked только `tests/full_rag_battery_result.json`.
- [11:22] Пользователь попросил самому выставить приоритеты и провести полный аудит.

## Приоритеты (самостоятельные, по риску × свежести)
1. **auto_fix.sh exit=1 + TG unreachable (08:15)** — автономный агент не работает, это блокирует self-healing. Смотрю первым.
2. **TG-Торты media backlog** — процесс кончился, но неизвестно прогрессом завершён или упал. Надо проверить coverage в БД.
3. **embedding_v2 coverage** — последние 12h добавили ~19k chunks, нужно убедиться что все пишутся в embedding_v2 (проверка прошлого бага c legacy).
4. **Здоровье сервисов** — last bot restart, queue, journalctl errors.
5. **Ночные cron** — audit_pipeline/review_knowledge/daily_report прошли? Есть ли обнаруженные проблемы?

## Аудит результаты

### ✅ Зелёное
- **embedding_v2**: 317 432 / 317 432 = 100% (e5 legacy 8 852 оставлены как исторические)
- **OOM**: dmesg чист — батч 128→32 держит
- **TG-Торты медиа**: 2 347 / 2 347 analyzed, 0 pending, 8 без S3 (историч.)
- **build_source_chunks cron**: работает, 500 docs / 10 мин на Qwen3 v2
- **Сервисы**: telegram-logger / email-sync / matrix-listener / auth-bom — все active
- **Daily Report 09:00**: чистый, диск 57%, RouterAI баланс 9544P

### 🔴 Красное — починено
- **auto_fix.sh в cron падает с 403 "Request not allowed"** третий день подряд (18/19/20.04 08:15).
  - Корень: `claude -p` в cron использует OAuth-токен из `~/.claude/.credentials.json`, он протухает за сутки и cron не триггерит refresh.
  - Тест: `env -u CLAUDECODE ... claude -p "..."` → 403. С `ANTHROPIC_API_KEY` из .env → rc=0, Anthropic API отвечает.
  - Фикс в `auto_fix.sh`: подтягиваю `ANTHROPIC_API_KEY` из .env и прокидываю в env для claude. API key не истекает.
  - Dry-run smoke_test: rc=0, prompt 6062 байт.
  - Валидация вживую: завтра 08:15 авто-запуск report_digest → проверить `auto_fix_log`.

### 🟡 Жёлтое — наблюдаем
- **review_knowledge 05:00 — все 10 батчей 402 Payment Required от RouterAI** за одну секунду. Сейчас RouterAI отвечает нормально (тест gpt-4.1 прошёл). Похоже на одноразовый всплеск у провайдера. Нет retry в `review_knowledge.py` — если повторится, добавить exponential backoff.
- **distillation telegram_message** в audit_pipeline — timeout 600s на батч 50 сообщений. 408 pending накапливается. Понизить батч до 20 или поднять timeout — на следующую сессию.
- **276 TG-вложений без анализа/текста** (не Торты) — хвост по мелким чатам (novye_produkty 72, apriori 67, proizvodstvo 63). Аналог analyze_tg_media_backlog работы, но для других чатов.
- **matrix-listener**: повторяет «Loaded 108 bridged rooms to skip» каждую минуту — косметика в логе.

## Изменённые файлы
- `auto_fix.sh` — экспорт `ANTHROPIC_API_KEY` из `.env` при вызове `claude -p` (фикс 403 в cron).

## Незавершённое / Следующие шаги
- **Наблюдать за auto_fix завтра 08:15** — должен отработать на API key без 403. Проверить `SELECT * FROM auto_fix_log ORDER BY started_at DESC LIMIT 1`.
- **TASK_canonical_attachments.md**: осталось #2 c1_event, #6 Matrix media, #3 v_messages_unified.
- **distillation telegram_message timeout** — понизить `--batch 50` → 20 в crontab или в скрипте, либо добавить `--timeout 1800`.
- **Мелкие TG-вложения backfill** — аналогично analyze_tg_media_backlog, но для novye_produkty / apriori / proizvodstvo (276 шт). По необходимости.

## Заметки
- RouterAI 402 за одну секунду по всем 10 батчам — это не rate-limit клиента (тогда бы не сразу), а сбой на стороне провайдера. Одиночный инцидент, не фиксим до повторения.
- OAuth-токен Claude Code обновляется только при интерактивном запуске (`.credentials.json` mtime совпал с моим логином сегодня в 11:20). В cron refresh не триггерится — отсюда систематические 403.
- Коммит фикса `auto_fix.sh` — по окончанию сессии, после user-approve.
