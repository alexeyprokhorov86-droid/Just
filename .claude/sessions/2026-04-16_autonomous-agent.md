# Сессия: 2026-04-16 — Автономный Claude-агент (TASK_autonomous_agent.md)

## Цель
Реализовать self-healing систему: `claude -p` headless вызывается по триггерам watchdog/cron, фиксит проблемы, при провале health-check — auto-revert.

## Прогресс по шагам

### Шаг 0 — подготовка
- Прочитан `CLAUDE.md`, прочитан единственный лог `.claude/sessions/2026-04-15_audit-and-fixes.md`
- Прочитан `TASK_autonomous_agent.md`
- Создан этот файл лога
- Окружение: `claude` v2.1.109 в `/usr/bin/claude`, venv python OK, диск 80% (внимание!), cron активен
- TG-нотификация: `send_alert()` из `watchdog.py` (HTTP `api.telegram.org/sendMessage`, ADMIN_USER_ID из .env)

### Шаг 1 — таблица auto_fix_log ✅
- `create_auto_fix_log.sql` создан, DDL применён, индекс `idx_auto_fix_trigger_time` на месте

### Шаг 2 — .claude/AUTO_AGENT_RULES.md ✅
- Файл содержит: разрешённые команды/файлы/git, whitelist сервисов и контейнеров, blacklist, лимиты, алгоритм, формат финального блока для парсинга, правило эскалации

### Шаг 3 — auto_fix.sh (главный скрипт) ✅
- `auto_fix.sh` (bash, исп. set -uo pipefail) + `auto_fix_helper.py` (psycopg2, TG, парсинг)
- Helper.commands: `rate_check`, `log_event`, `tg_report`, `target_hint`
- Bash syntax OK, Python syntax OK, права +x выставлены, `.claude/auto_sessions/` создан

### Шаг 4 — интеграция с watchdog.py ✅
- Добавлены: `trigger_auto_fix_service_down()` (формирует JSON-контекст, запускает auto_fix.sh в фоне через `nohup ... &`) и `restart_with_auto_fix()` (обычный restart → wait 60s → если всё ещё down → вызвать агента)
- В `main()` блоки telegram-logger и email-sync переведены на `restart_with_auto_fix()`
- Syntax check OK

### Шаг 5 — auto_agent_cron.py ✅
- Схема одобрена пользователем. Уточнения: sync_1c_error смотрит все 5 sync-логов; `review_knowledge.log` уже существует (250KB)
- 5 триггеров реализованы. syntax+import OK
- Между триггерами 5с паузы; `try/except` чтобы один упавший триггер не валил остальные

### Шаг 6 — отчёты в Telegram ✅
- Уже встроено в `auto_fix_helper.py::cmd_tg_report` — HTML-формат, status-emoji, действия из summary, git SHA, путь к лог-файлу. Использует тот же endpoint `api.telegram.org/sendMessage` и `ADMIN_USER_ID` из .env, как `watchdog.send_alert`. Дополнительной работы не требуется.

### Шаг 7 — добавление cron-записи ✅
- Пользователь подтвердил. Запись добавлена в `crontab -u admin`:
  `0 * * * * /home/admin/telegram_logger_bot/venv/bin/python /home/admin/telegram_logger_bot/auto_agent_cron.py >> /home/admin/telegram_logger_bot/auto_agent.log 2>&1`

### Бонус — расширение root-диска (по просьбе пользователя)
- Cloud.ru расширил vda до 120GB, но vda2 (root, ext4) был 72GB
- `sudo growpart /dev/vda 2` → раздел расширен (с одновременным фиксом GPT-хвоста)
- `sudo resize2fs /dev/vda2` → online-grow ext4
- Результат: `/` теперь 118GB / 54GB used / 60GB avail = **48%** (было 80%)

### Финал — git, dry-run, лог ✅
- Коммит `be3713b` — feat: автономный Claude-агент для self-healing
- Изменения: 8 файлов / +1178 / -5
- `git push` → main (e8357f0..be3713b)
- Dry-run `auto_fix.sh --dry-run pending_rules` отработал: rate-check 0/2, промпт 6094 байт, запись `dry_run` (id=1) в auto_fix_log

## Итого создано/изменено

### Новые файлы
- `auto_fix.sh` — главный скрипт (rate-limit → claude -p → health-check → auto-revert → TG)
- `auto_fix_helper.py` — Python-helpers (psycopg2, парсинг summary, TG)
- `auto_agent_cron.py` — 5 плановых триггеров (pending_rules, sync_1c_error, disk_high, embeddings_stalled, json_parse_errors)
- `create_auto_fix_log.sql` — DDL таблицы auto_fix_log + индекс
- `.claude/AUTO_AGENT_RULES.md` — whitelist/blacklist/лимиты для агента
- `.claude/auto_sessions/` — папка для логов отдельных вызовов claude -p

### Изменённые файлы
- `watchdog.py` — добавлены `trigger_auto_fix_service_down()` и `restart_with_auto_fix()`; для telegram-logger и email-sync теперь вызывается расширенная логика (restart → wait 60s → если down → auto_fix.sh в фоне)

### Инфраструктура
- БД: создана таблица `auto_fix_log` + индекс `idx_auto_fix_trigger_time`
- Cron (admin): добавлена строка `0 * * * * auto_agent_cron.py >> auto_agent.log`
- Root-диск расширен: `/dev/vda2` 72GB → 118GB через `growpart` + `resize2fs` (online, без размонтирования). Использование `/` упало с 80% → 48%

## Незавершённое / Что наблюдать

- Первый плановый запуск cron: ближайший `:00` после установки. В `~/telegram_logger_bot/auto_agent.log` будет видно, какие триггеры сработают
- Если ни один триггер не сработает за час — проверить, что cron вообще выполнился: `grep "auto_agent_cron start" auto_agent.log`
- Auto-revert НЕ срабатывает для плановых триггеров (нет TARGET_SERVICE) — если агент закоммитит что-то странное, видно в `auto_fix_log.git_commit_sha` и `git log`, откатить вручную
- `daily_report.py` — пока не интегрирован блок "Auto-fix за сутки" из ТЗ (раздел "После деплоя", п.2). Сделать после первой недели наблюдений

## Заметки

- `claude -p` в headless-режиме использует OAuth Max-подписку пользователя (admin). Если sub истечёт — фиксы перестанут работать, alert придёт со status=`failed`
- Промпт ~6KB — в основном правила (AUTO_AGENT_RULES.md, ~3KB). Можно ужать, если будет дорого по токенам
- Контекст для service_down — журнал 50 строк (≤15KB) + 5 коммитов; для plan-триггеров — компактный JSON
- TG-сообщения через тот же endpoint и ADMIN_USER_ID, что и `watchdog.send_alert`

## Follow-up — фикс auto-approve в review_knowledge.py

После daily-отчёта пользователь сообщил о ложных авто-апрувах: `wireguard` и `файл зарплаты` помечены как junk. Расследование выявило **6 FP-правил** (id 190 vpn, 195/211/222 — regex по зарплате/договорам, 216/218 wireguard, 224 файл зарплаты).

**Корневые причины** (`apply_new_rules`):
1. дедуп только по `is_active=true` — отключённое правило можно re-add; LLM так и сделал с wireguard
2. любое предложение LLM сразу `approval_status='approved'`
3. нет защиты доменно-критичных терминов (IT, HR, finance, legal)

**Реализован 3-слойный фикс** (review_knowledge.py):
- Слой 1 — `SAFE_SUBSTRINGS` whitelist (~50 терминов: vpn/wireguard/ssh/.../зарплат/оклад/договор/банк/...). При совпадении — лог `[SKIP-WHITELIST]`, не вставлять.
- Слой 2 — history-check без фильтра по is_active. Если в БД уже есть запись с этим value — `[SKIP-HISTORY]`.
- Слой 3 — regex-метасимволы (`[`, `\d`, `.*`, и т.п.) → `approval_status='pending'` + `is_active=false`, в лог `[PENDING-REGEX]`. Plain word → старое поведение (`approved`/`is_active=true`).
- Бонус — в `REVIEW_SYSTEM_PROMPT` добавлен явный список «никогда не предлагай как junk_word: IT/HR/финансы/legal».

Self-test 8 кейсов прошёл (все 6 FP блокируются, 3 легитимных мусорных слова пропущены).

**Деактивированы вручную (UPDATE):**
- id=218 (wireguard), 224 (файл зарплаты) — по первой просьбе пользователя
- id=190 (vpn), 195, 211, 222 — после расследования, по подтверждению пользователя
- Всем выставлено `is_active=false`, `false_positive_count++`, `approval_status` оставлен `approved` для аудита

## Follow-up — боевая проверка авто-агента

Пользователь запросил end-to-end тест прямо сейчас. Найдены и исправлены 2 проблемы.

**Проблема: `claude -p` → 403 Forbidden.** Когда `auto_fix.sh` запускается из родительской Claude-сессии (env vars `CLAUDECODE=1`, `CLAUDE_CODE_SSE_PORT=...`, `CLAUDE_CODE_ENTRYPOINT=...`, `CLAUDE_CODE_EXECPATH=...`), дочерний `claude -p` пытается подключиться к родителю и получает 403. Cron запускает auto_fix.sh с чистым окружением, проблемы там нет, но защита нужна для ручных тестов и watchdog (который тоже может быть вызван иначе).

**Фикс:** перед `timeout claude -p` явно `env -u CLAUDECODE -u CLAUDE_CODE_SSE_PORT -u CLAUDE_CODE_ENTRYPOINT -u CLAUDE_CODE_EXECPATH ...`. После фикса E2E smoke test с триггером `smoke_test` прошёл за 8 секунд: Claude вернул `STATUS: no_action`, summary распарсен, запись id=2 с `status=success` в auto_fix_log, лог сессии в `.claude/auto_sessions/2026-04-16_14-30_smoke_test.log`.

**Ложная тревога:** пользовательский `crontab -l | grep auto_agent` показал пусто — это была ошибка терминала (вывод сцепился с предыдущей командой), запись в crontab на месте.

**Состояние плановых триггеров (ручной запуск через venv-python):**
- pending_rules: 2 ≤ 5 → пропуск
- sync_1c_error: чисто
- disk_high: 48% ≤ 85 → пропуск (благодаря расширению vda2)
- embeddings_stalled: chunk_age=2540m, doc_age=181m → пропуск (документы тоже стоят, оба больше пороговых интервалов)
- json_parse_errors: 1 < 3 → пропуск








