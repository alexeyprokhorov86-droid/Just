# Сессия: 2026-04-21 — auto_fix.sh фикс через CLAUDE_CODE_OAUTH_TOKEN

## Что сделано
- [10:30] Старт по запросу пользователя «как дела, видел что auto_fix вызывал тебя но ничего не сделал».
- [10:31] Проверил `.claude/auto_sessions/2026-04-21_08-15_report_digest.log`: claude rc=1, `Failed to authenticate. API Error: 403 {"error":{"type":"forbidden","message":"Request not allowed"}}`.
- [10:33] Диагностика: фикс от 20.04 (ANTHROPIC_API_KEY из .env) не сработал. Корень: `claude -p` при наличии `~/.claude/.credentials.json` использует OAuth-токен (Claude Max подписку) и ИГНОРИРУЕТ ANTHROPIC_API_KEY env. OAuth-токен протухает за ~24h и cron не триггерит refresh → 403.
- [10:34] Эксперимент: убрал credentials.json → `claude -p "say pong"` с ANTHROPIC_API_KEY rc=0, ответ "pong". Вернул credentials.json. Гипотеза подтверждена.
- [10:35] В `claude --help` нашёл флаг `--bare`: "Anthropic auth is strictly ANTHROPIC_API_KEY or apiKeyHelper (OAuth and keychain are never read)".
- [10:40] Сначала применил фикс `--bare` + ANTHROPIC_API_KEY. Пользователь поправил: `--bare` + API key означает double billing (Claude Max подписка + API usage). Откатил `--bare`.
- [10:42] Правильное решение — `claude setup-token`: long-lived token (~1 год), привязан к подписке, не к API billing. Заточен под cron/CI.
- [10:45] Пользователь запустил `claude setup-token` из SSH-терминала (из Claude Code через `!` не работает — требует TTY), получил токен `sk-ant-oat01-...` (len=108).
- [10:50] Добавил `CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-...` в `.env` (строка 140).
- [10:51] Обновил `auto_fix.sh`: читает `CLAUDE_CODE_OAUTH_TOKEN` из .env, прокидывает в `claude -p`; явно unset-ит `ANTHROPIC_API_KEY` чтобы не было коллизий и double billing; abort если токен не найден в .env.
- [10:52] Smoke-test: `env -u CLAUDECODE -u CLAUDE_CODE_SSE_PORT -u CLAUDE_CODE_ENTRYPOINT -u CLAUDE_CODE_EXECPATH -u ANTHROPIC_API_KEY CLAUDE_CODE_OAUTH_TOKEN=<...> claude -p "say pong"` → `pong`, rc=0. ✅

## Изменённые файлы
- `.env` — добавлен `CLAUDE_CODE_OAUTH_TOKEN` (строка 140, long-lived OAuth от setup-token)
- `auto_fix.sh` — строки 103-119: читаем `CLAUDE_CODE_OAUTH_TOKEN` из .env, `ANTHROPIC_API_KEY` больше не используется для claude-вызова (unset в env)

## Незавершённое / Следующие шаги
- Коммит + push (пользователь не просил пока; `.env` в gitignore, коммит только auto_fix.sh).
- Завтра 08:15 проверить `.claude/auto_sessions/` — должен отработать rc=0.
- Когда-то (через ~год) ротировать OAuth token: `claude setup-token` из SSH, обновить .env.
- Мониторить auto_fix_log на 403 — если появится, значит токен отозван/протух раньше заявленного года.

## Заметки
- `--bare` отвергнут: отключает OAuth и keychain, форсит ANTHROPIC_API_KEY → double billing (подписка + API). Пользователь поймал это, я исходно не предусмотрел.
- `claude setup-token` требует интерактивный TTY (ошибка "Raw mode is not supported" при запуске через `!` внутри Claude Code). Запускать ТОЛЬКО из чистого SSH-терминала.
- OAuth из `~/.claude/.credentials.json` (обычный интерактивный login) и OAuth от `setup-token` — разные механизмы: первый TTL ~24h без refresh в non-interactive, второй заявлен 1 год.
- Прошлый фикс (commit 555a16e, 20.04, ANTHROPIC_API_KEY) был бесполезен: CLI игнорирует env var когда есть credentials.json. Текущий фикс явно unset-ит ANTHROPIC_API_KEY для чистоты.
- `.env` permissions: 644 (rw-rw-r--) — секрет потенциально читаемый для group/others на VPS. Не менял в этой сессии, но стоит позже `chmod 600 .env`.
