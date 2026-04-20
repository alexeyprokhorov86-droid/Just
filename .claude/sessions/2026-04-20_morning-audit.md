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

## [12:30] Зачистка мусорного чата antony_nut
- Пользователь: «antony_nut вычеркни отовсюду, затесался и не удаляется до конца».
- Инвентаризация: `tg_chats_metadata` (1 запись), `tg_chat_1001199207547_antony_nut` (1000 сообщений, без медиа, 2025-09-15..12-31), **в source_documents / km_* / коде — 0 следов**.
- `DROP TABLE tg_chat_1001199207547_antony_nut; DELETE FROM tg_chats_metadata WHERE chat_id=-1001199207547;` — успешно. Verify: meta_left=0, table_exists=0.

## [12:15] Distillation timeout fix
- `audit_pipeline.py:462` — timeout 600 → 1800. Дать distillation.py telegram_message шанс переварить 408 pending за один ночной прогон audit.

## [12:00] Продвижение по очереди (по запросу «делай всё по очереди»)
1. ✅ `git push` commit 555a16e (auto_fix ANTHROPIC_API_KEY fix) → origin/main. Feedback: `git push` теперь часть стандартного flow, не спрашиваю.
2. ✅ distillation timeout 600→1800 в audit_pipeline.
3. ✅ Хвост TG-вложений: проверка показала 0 pending с S3 по всем чатам (33 без storage_path — проблема миграции S3, не analyze).
4. ✅ c1_event MVP (2026-04-20 13:52):
   - Новый скрипт `canonize_1c_events.py`: 3 категории (purchase_large 300k / sale_large 200k / payment_large 500k)
   - Backfill 180 дней: **3531 событий** (payment 2234 / purchase 893 / sale 404), 33 мин
   - Cron `*/30 * * * *` incremental (updated_at > 1h)
   - 3531 chunks с embedding_v2 = 100% покрытие
   - Smoke-тест «крупная закупка у Калкулэйт» → 5 реальных закупок у ПК Калкулэйт, sim 0.5+
   - Первый прогон завис (58 мин CPU 99%, 0 вставок) — единый `c.commit()` в конце + конфликт с cron. Фикс: commit каждые 20 событий + прогресс-лог 30с + временно отключил cron на backfill
   - Коммит 6cfda03, push
   - V2 план зафиксирован: +4 категории в `TASK_canonical_attachments.md` (dispatch_large c проверкой JOIN на 6-мес данных — 361 dispatch >200k, 58% match — требуется fallback)

## [13:25] Параллельная задача: .tmp_input/ setup для пользователя
- Пользователь спросил как скармливать файлы в Claude Code.
- Создана `/home/admin/telegram_logger_bot/.tmp_input/` + `.gitignore: .tmp_input/`.
- Memory reference: `reference_tmp_input.md`.
- В чат — инструкции для Linux (sshfs) + Windows (WinSCP GUI / PowerShell scp / WSL).

## Коммиты за сессию
1. `555a16e` fix(auto_fix): ANTHROPIC_API_KEY из .env → стоп 403 в cron
2. `6cfda03` feat(canonical): c1_event MVP + distillation timeout + .tmp_input + antony cleanup

## Промежуточное состояние (сессия продолжается)
- ✅ auto_fix.sh устойчив к OAuth-протуханию
- ✅ distillation timeout 1800s
- ✅ c1_event MVP в проде (3531 events, cron работает)
- ✅ antony_nut удалён полностью
- ✅ .tmp_input/ готов для подкидывания файлов
- embedding_v2: 320 963 / 320 963 = 100% (прирост +3 531 от c1_event)

## Незавершённое / Следующая сессия
- Наблюдать `auto_fix_log` завтра в 08:15 — должен отработать на API key
- c1_event V2: реализовать 4 категории (dispatch_large — с fallback для 42% нематченных строк, inventory_discrepancy, production_issue, staff_change)
- Hand 1C: доделать `fetch_dispatch_events` через JOIN с customer_order_items + average price fallback
- `tests/full_rag_battery_result.json` — прогнать полную батарею на обновлённом RAG (теперь c c1_event), сравнить с прошлыми результатами

## [18:30] claude_tg_bridge — переход на hook-режим
- SDK-режим (`claude_runner.py`, ADK subprocess) убран. Причина: Algorithm 403 в subprocess без OAuth, плохая трансляция действий, не совпадает с поведением «увидеть что Claude делает в реальной сессии».
- Новый дизайн: `~/.claude/settings.json` hooks → `claude_tg_bridge/hook_bridge.py` → Telegram. Бот `claude_tg_bridge/bot.py` переделан в long-poll callback handler + `/pause` `/resume` `/status`.
- Hook events: UserPromptSubmit (echo «🙂 …»), PreToolUse (Read/Grep/Glob — тихий лог; Bash/Write/Edit — **блокирующий approve-запрос**, ждёт decision файл до 5 мин), PostToolUse (Bash — stdout-хвост), Stop (summary).
- Pause-флаг `/tmp/claude_hook_paused` — если существует, хуки `sys.exit(0)` сразу, не блокируют ничего.
- Deadlock при первом `systemctl start`: хуки применились раньше чем бот поднялся → Bash блок. Решение (ручные команды user-а через SSH): `touch /tmp/claude_hook_paused` + `systemctl enable/start claude-tg-bridge`.
- Service `claude-tg-bridge.service` включён и запущен. Бот в pause-режиме, готов принимать callback'и.

## Изменённые файлы (18:30)
- `claude_tg_bridge/hook_bridge.py` — новый, stdin hook payload → TG, ждёт decision для Bash/Write/Edit.
- `claude_tg_bridge/bot.py` — переписан: только callback handler (happrove/hdeny) + /pause /resume /status.
- `~/.claude/settings.json` — добавлены hooks UserPromptSubmit/PreToolUse/PostToolUse/Stop.
- `claude_tg_bridge/claude_runner.py`, `permission_gate.py`, `session_store.py` — устарели, но пока не удаляю (проверить после первой реальной сессии).

## [19:00] claude_tg_bridge — variant 2a (terminal-first + TG nag + assistant-text stream)
- Текущая итерация TG-only approve оказалась неудобной: если Алексей за терминалом, клик в TG — лишний шаг. Обсудили. Я объяснил что «reverse order» (терминал первый, TG fallback) структурно невозможен: hook — синхронный, терминальный prompt появляется только если hook вернул `ask`, после этого hook уже завершён и TG-клик не может достучаться до stdin терминала.
- Компромисс — **variant 2a**: hook сразу отпускает терминалу обычный permission flow, параллельно форкает detached nag-процесс. Через 5 мин (`NAG_TIMEOUT_SEC=300`) nag проверяет `/tmp/claude_hook_done/<tool_use_id>.marker` — если маркера нет (tool не отработал = approve не дан), шлёт в TG напоминалку «⏰ approve висит >5 мин». Кнопок нет сознательно (бесполезны).
- Корреляция Pre↔Post через `tool_use_id` из payload хука (подтвердил смоком: реально прилетает `toolu_01...`). Fallback на hash(session_id + tool_input) — на случай если payload без id.
- `settings.json`: `PostToolUse` matcher `""` (было `"Bash"`), чтобы маркер писался и для Write/Edit.
- Смоук `echo "smoke-test new hook"`: PreToolUse→PostToolUse 130ms → маркер записан → nag процесс через 5 мин тихо вышел. ✓

## [19:15] Streaming ассистент-текста в TG (через transcript_path)
- Алексей: «почему в TG не видны твои ответы и рассуждения, только команды?»
- Причина: у Claude Code нет hook'а на «ассистент сгенерировал текст» — только дискретные события (UserPromptSubmit / PreToolUse / PostToolUse / Stop). Но в каждом payload есть `transcript_path` — путь к JSONL где assistant-message с content-блоками text/thinking/tool_use пишется построчно.
- Реализовал `flush_assistant_texts(transcript_path, session_id)` в hook_bridge.py: дочитывает JSONL с сохранённого оффсета (`/tmp/claude_hook_offsets/<session>.txt`), извлекает `content[].type == "text"` и `"thinking"` блоки ассистента, шлёт в TG как 💬 / 🧠 (chunked до 3800 символов). Вызывается на PreToolUse и Stop.
- **Анти-флуд первого запуска**: если offset-файла нет, ищем байт-оффсет после последнего user-сообщения в transcript и ставим туда. Так старая история до текущего промпта не флудит, но текущий ход ассистента показывается полностью.
- **Был один флуд**: между Edit 4 (добавление flush в handle_stop) и Edit 5 (first_time-гард), хук успел сработать БЕЗ гарда → прилетело ~10-30 💬 с историей переписки. Алексей получил.

## [19:24] Cleanup + restart
- Удалены `claude_tg_bridge/claude_runner.py`, `permission_gate.py`, `session_store.py` — SDK-режим мёртв.
- `bot.py` упрощён: убран callback-handler (happrove/hdeny больше не шлются), только `/status /pause /resume`. /start обновлён под hook-mode описание с эмодзи-легендой.
- `sudo systemctl restart claude-tg-bridge` — PID 3034815, active.

## Bridge — итоговое состояние
- Transport: ~/.claude/settings.json hooks → claude_tg_bridge/hook_bridge.py → Telegram (через Privoxy 8118)
- Control: claude_tg_bridge/bot.py (long-poll, /pause /resume /status)
- Approve: всегда в терминале (родной permission-flow Claude Code)
- TG показывает: 🎤 user prompt, 💬 assistant text, 🧠 thinking, 🔹 Read/Grep/Glob/WebFetch/WebSearch/TodoWrite, 📤 Bash stdout, ⏰ nag если approve висит >5 мин, ✅ Stop
- Pause-флаг: `/tmp/claude_hook_paused` (touch = хук exit 0 сразу)
- Маркеры: `/tmp/claude_hook_done/<tool_use_id>.marker` — write on PostToolUse, read by nag
- Оффсеты: `/tmp/claude_hook_offsets/<session>.txt` — для incremental чтения transcript

## [19:38] RAG roadmap актуализирован + P0 работа стартовала
- Пересмотрели приоритеты по RAG. Фазы 1-6 TASK_rag_quality_v2 закрыты (2026-04-17), в проде + battery 2026-04-17 подтвердил 30/30 good_citations, 22/30 с 1С-evidence (73%).
- CLAUDE.md секция «Приоритеты» переписана — структура P0/P1/P2. Старые backlog-пункты (top_* v_sales_adjusted, дедуп km_facts) подняты в P0.
- Новый P0 финальный: (1) top_* net с учётом Корректировок, (2) подтвердить дедуп km_facts, (3) 👍/👎 inline-кнопки под RAG-ответами.
- Baseline battery запущен в BG (PID 3042690, start 19:38, ETA 20:10). Сохранил старый результат как `tests/full_rag_battery_result_2026-04-17.json`.

## [19:40] P0.1 top_* → включение Корректировок в net-выручку
- Исходная проблема не в смене источника: `mart_sales` УЖЕ построен как `FROM v_sales_adjusted WHERE client_name NOT ILIKE '%фрумелад%'` (т.е. effective_date, возвраты вкл., внутренние переводы исключены). Но RAG сам добавлял `WHERE doc_type='Реализация'` → отфильтровывал Корректировки → gross вместо net.
- `rag_agent.py` — убрал `WHERE doc_type='Реализация'` в трёх запросах: top_clients/sales_summary (L1772), top_products (L1814), sales_by_nomenclature (L1973). Для top_products AVG(price) оставил под FILTER (WHERE doc_type='Реализация') — чтобы средняя цена не искажалась.
- Проверил на март 2026: ТАНДЕР gross 14.49M → net 14.26M (−224k возвратов), Х5 АГРОТОРГ 8.45M → 8.30M, ПЕРЕКРЁСТОК сполз ниже КАМЕЛОТ (у Камелота нет возвратов). Метабаза и раньше была net — RAG теперь совпадает.

## [19:47] P0.2 km_facts dedupe — оказался уже в работе
- `review_knowledge.py` Step 0 (cosine ≥ 0.95, max 2000/run) в cron `0 5 * * *` — активен.
- Сегодня 05:04 дедуп помётил 183 записи: km_facts 92 / km_decisions 28 / km_tasks 24 / km_policies 39. Было 42k fact'ов (CLAUDE.md) → стало 25929 активных = дедуп реально сжал на ~38% за жизнь.
- Ничего не трогал. P0.2 считаем закрытым.
- Попутная бага: Step 1 (LLM-ревью) ловит 402 Payment Required от RouterAI в течение секунды по всем 10 батчам (17.04 и 20.04 подряд). retry с backoff отсутствует. В backlog на отдельный фикс.

## [19:50] P0.3 👍/👎 inline-кнопки под RAG
- DB: `ALTER TABLE rag_query_log ADD COLUMN user_feedback VARCHAR(10), feedback_at TIMESTAMP`; индекс partial по `user_feedback IS NOT NULL`.
- `rag_agent.py:_log_rag_query` — теперь `RETURNING id`, возвращает int. `process_rag_query` получила опциональный `meta_out: dict` — если передан, пишет туда `log_id`. Backward-compat (battery/matrix_bot не трогал).
- `bot.py` — `_rag_feedback_keyboard(log_id)` генерит кнопки с callback_data `rag_fb:<id>:up|down`; `rag_feedback_callback` на клике UPDATE'ит `rag_query_log.user_feedback/feedback_at` и меняет клавиатуру на «👍 принято». Привязано к handle_private_rag + handle_mention. Кнопки клеятся ТОЛЬКО к последнему чанку, если ответ режется на >4000 символов.
- Handler зарегистрирован с `pattern=r'^rag_fb:'` до `handle_full_analysis_button` (catch-all).
- `sudo systemctl restart telegram-logger` (PID 3049725). Smoke-check: python -c "import bot; import rag_agent" → ok. journalctl → "🚀 Бот запущен", никаких ошибок.
