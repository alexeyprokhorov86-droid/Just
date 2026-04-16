# TASK: Автономный Claude-агент для self-healing инфраструктуры

## Цель
Создать систему, в которой Claude Code автоматически реагирует на проблемы инфраструктуры:
- Падение сервисов (telegram-logger, email-sync, matrix-listener, auth-bom)
- Ошибки в sync_1c (OData errors, timeout, credential issues)
- Новые pending-правила в km_filter_rules (авторевью)
- Аномалии из daily_report (диск >85%, embeddings не идут, sync error)
- JSON parse errors в review_knowledge.py

Агент использует `claude -p` (headless-режим) с OAuth Max-подпиской пользователя.

## Ограничения безопасности

**Разрешено без подтверждения:**
- `systemctl restart <service>` для известных сервисов (whitelist ниже)
- `docker restart <container>` для knowledge_db, metabase, synapse
- Правка `.py` файлов в `~/telegram_logger_bot/`
- `git add`, `git commit`, `git push`, `git revert HEAD`
- Чтение логов (`journalctl`, `docker logs`, файлы логов)

**Whitelist сервисов для рестарта:**
telegram-logger, email-sync, matrix-listener, auth-bom, nkt-dashboard

**Запрещено:**
- `rm -rf`, удаление файлов кроме временных в `/tmp/`
- Правка `.env`, `docker-compose.yml`, systemd-unit файлов
- `DROP TABLE`, `TRUNCATE`, `DELETE FROM` без `WHERE`
- `git push --force`, `git reset --hard`
- Правка cron (`crontab -e`)
- Установка новых пакетов (`apt install`, `pip install`)
- Открытие портов, изменение nginx конфигов
- Любые действия с Cloud.ru API

**Лимиты:**
- Максимум 2 попытки fix на один триггер за 24 часа
- Между попытками — минимум 15 минут
- Если после fix сервис упал снова в течение 10 мин — автооткат `git revert HEAD`

## Архитектура
┌─────────────────────────────────────────────────────────────┐
│  Два уровня детекции                                         │
├─────────────────────────────────────────────────────────────┤
│  watchdog.py (каждые 5 мин)  → срочное: сервисы              │
│  auto_agent_cron.py (раз в час) → плановое: rules, аномалии  │
└─────────────────────────────────────────────────────────────┘
│
▼
auto_fix.sh TRIGGER_NAME CONTEXT_FILE
│
▼
┌─────────────────────────────────────────────────────────────┐
│  auto_fix.sh:                                                │
│  1. Проверяет rate-limit (макс 2 попытки / 24ч)              │
│  2. Собирает контекст в prompt.txt                           │
│  3. Вызывает: claude -p "$(cat prompt.txt)"                  │
│         --permission-mode acceptEdits                        │
│         --allowedTools "Bash,Edit,Read,Write"                │
│  4. Сохраняет stdout в .claude/auto_sessions/                │
│  5. Health-check: systemctl is-active                        │
│  6. Если health-check упал — git revert HEAD                 │
│  7. Пишет отчёт в Telegram                                   │
│  8. Записывает в БД таблицу auto_fix_log                     │
└─────────────────────────────────────────────────────────────┘

## Реализация — пошагово

### Шаг 1: Таблица `auto_fix_log`

```sql
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
    status TEXT -- 'success', 'failed', 'reverted', 'rate_limited'
);

CREATE INDEX idx_auto_fix_trigger_time ON auto_fix_log(trigger_name, started_at DESC);
```

### Шаг 2: `.claude/AUTO_AGENT_RULES.md`

Создать файл с правилами, которые будут передаваться Claude Code в каждом вызове. Содержит whitelist/blacklist и лимиты из раздела "Ограничения безопасности" выше.

### Шаг 3: `auto_fix.sh` — главный скрипт

Параметры: `$1` = имя триггера, `$2` = путь к файлу с контекстом.

Логика:
1. Проверить в `auto_fix_log` — сколько попыток за последние 24 часа для этого триггера
2. Если >= 2 — выход с кодом 1, алерт в TG "достигнут лимит попыток"
3. Прочитать `AUTO_AGENT_RULES.md` + `CLAUDE.md` + контекст
4. Сформировать промпт:
Триггер: $TRIGGER_NAME
Контекст: $(cat $CONTEXT_FILE)
Правила: см. .claude/AUTO_AGENT_RULES.md
Задача: проанализируй ситуацию и исправь её.
Действуй в рамках разрешённых операций. Если задача выходит за рамки
разрешённого — выведи "ESCALATE: причина" и остановись.
Всегда фиксируй действия в лог .claude/auto_sessions/
5. Запустить `claude -p "$PROMPT" --permission-mode acceptEdits --allowedTools "Bash,Edit,Read,Write,Grep,Glob"` с таймаутом 10 минут
6. Захватить stdout/stderr в `.claude/auto_sessions/YYYY-MM-DD_HH-MM_TRIGGER.log`
7. Распарсить git log — был ли новый коммит
8. Health-check: ждать 30 сек, потом `systemctl is-active <relevant-service>` (или проверка по триггеру — для sync_1c проверить exit code повторного запуска)
9. Если health-check false И был коммит: `git revert --no-edit HEAD && git push` + restart сервиса + алерт "автооткат"
10. Записать результат в `auto_fix_log`
11. Отправить отчёт в Telegram через существующий механизм (используй функцию из daily_report.py или watchdog.py)

### Шаг 4: Интеграция с watchdog.py

В существующий `watchdog.py` при детекте падения сервиса (вместо/дополнительно к существующему restart):
- Сначала сделать обычный рестарт (как сейчас)
- Подождать 60 сек
- Если сервис снова down — вызвать `auto_fix.sh service_down /tmp/watchdog_context.json`
- В контексте: имя сервиса, последние 50 строк journalctl, последние 5 коммитов git log

### Шаг 5: `auto_agent_cron.py` — плановые триггеры

Отдельный скрипт по cron раз в час. Проверяет:

**Триггер pending_rules:**
```sql
SELECT COUNT(*) FROM km_filter_rules WHERE approval_status = 'pending';
```
Если > 5 — собрать топ-20 pending правил, вызвать `auto_fix.sh pending_rules /tmp/rules_context.json`.

**Триггер sync_1c_error:**
Проверить grep последних ошибок в `sync_full.log` за последний час. Если есть — вызвать `auto_fix.sh sync_1c_error /tmp/sync_context.json`.

**Триггер disk_high:**
Если `df /` > 85% — вызвать `auto_fix.sh disk_high /tmp/disk_context.json`.

**Триггер embeddings_stalled:**
Если за последние 2 часа `source_chunks` не пополнялись, но `source_documents` растут — вызвать `auto_fix.sh embeddings_stalled /tmp/emb_context.json`.

**Триггер json_parse_errors:**
Если в `review_knowledge.log` есть JSON parse errors за последние 24 часа — вызвать `auto_fix.sh json_parse_errors /tmp/json_context.json`.

### Шаг 6: Отчёты в Telegram

Формат отчёта:
🤖 <b>Auto-fix: TRIGGER_NAME</b>
Триггер: service_down (email-sync)
Действия:
• Проверил журнал, нашёл OOM
• Перезапустил сервис
• Health-check: ✅ active
Git: коммит abc1234
Статус: ✅ success
<a href="...">Полный лог</a>

Использовать существующий механизм отправки в TG (функция из watchdog.py или daily_report.py). Chat ID = ADMIN_USER_ID (805598873).

### Шаг 7: Cron
Плановые триггеры — раз в час
0 * * * * /home/admin/telegram_logger_bot/venv/bin/python /home/admin/telegram_logger_bot/auto_agent_cron.py >> /home/admin/telegram_logger_bot/auto_agent.log 2>&1

## Требования к коду

- Все скрипты в `~/telegram_logger_bot/`
- Логи сессий Claude: `.claude/auto_sessions/YYYY-MM-DD_HH-MM_TRIGGER.log`
- Использовать `get_db_connection()` паттерн как в `bot.py`
- Обработка ошибок: если `claude -p` упал — записать в auto_fix_log со статусом 'failed', алерт в TG
- Таймауты: `claude -p` — 10 мин, health-check — 2 мин
- Ничего не хардкодить — использовать `.env` переменные

## Тестирование

1. **Dry-run режим:** флаг `--dry-run` в `auto_fix.sh` — собирает контекст и показывает, что бы передал Claude, но не вызывает
2. **Тест rate-limit:** вручную создать 2 записи в auto_fix_log за последний час, запустить `auto_fix.sh` — должен отказаться
3. **Тест ручного триггера:** вручную остановить `auth-bom.service` и запустить watchdog.py — проверить что auto_fix.sh вызывается
4. **Тест авто-отката:** подменить healthcheck на `false`, запустить auto_fix.sh — должен откатить коммит

## Деплой

```bash
# 1. Создать таблицу
docker exec -i knowledge_db psql -U knowledge -d knowledge_base < create_auto_fix_log.sql

# 2. Создать папку логов
mkdir -p ~/telegram_logger_bot/.claude/auto_sessions

# 3. Права на исполнение
chmod +x ~/telegram_logger_bot/auto_fix.sh

# 4. Проверить что claude CLI доступен
which claude
claude --version

# 5. Добавить в cron
(crontab -l; echo "0 * * * * /home/admin/telegram_logger_bot/venv/bin/python /home/admin/telegram_logger_bot/auto_agent_cron.py >> /home/admin/telegram_logger_bot/auto_agent.log 2>&1") | crontab -

# 6. Первый dry-run
./auto_fix.sh --dry-run pending_rules /tmp/test_context.json

# 7. Коммит
git add auto_fix.sh auto_agent_cron.py .claude/AUTO_AGENT_RULES.md create_auto_fix_log.sql TASK_autonomous_agent.md
git commit -m "feat: автономный Claude-агент для self-healing"
git push
```

## Критические проверки перед деплоем

- [ ] `claude --version` работает под юзером admin
- [ ] OAuth-сессия активна (проверить `claude -p "echo test"`)
- [ ] Prod-сервисы не падают при тесте dry-run
- [ ] Git-репозиторий чистый, нет uncommitted changes в момент первого запуска
- [ ] Telegram-бот онлайн (для отчётов)
- [ ] Disk space > 5GB свободно (для логов и коммитов)

## После деплоя

1. Неделю наблюдать — не делать новых триггеров, смотреть auto_fix_log
2. Каждый вечер в daily_report добавить блок "Auto-fix за сутки": количество срабатываний, success rate
3. Если false positive > 20% — пересматривать триггеры
4. Через месяц добавлять новые триггеры (начиная с наименее рискованных)

## Лог сессии

После завершения работы записать лог в `.claude/sessions/2026-04-16_autonomous-agent.md` со всеми деталями реализации.