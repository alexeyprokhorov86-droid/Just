# CLAUDE.md — Инструкция для Claude Code

## Проект

Платформа BI и knowledge management для кондитерской компании "Фрумелад" (~80 сотрудников).
Владелец и разработчик: Алексей.

## Стек

- **VPS**: 95.174.92.209 (Ubuntu 22.04, Cloud.ru)
- **БД**: PostgreSQL 16 + pgvector в Docker (`knowledge_db`, сеть `kb_network`, IP 172.20.0.2)
- **Database**: `knowledge_base`, user: `knowledge`, credentials в `.env`
- **Metabase**: Docker (172.20.0.3, порт 3000)
- **Matrix/Synapse**: Docker (порт 8008 внутри, 443/8448 снаружи), homeserver `matrix.frumelad.ru`
- **Bridge**: mautrix-telegram v0.15.2 + relay bot `@frumelad_bridge_bot`
- **Прокси**: Amsterdam SOCKS5 (порт 1080), Helsinki SOCKS5 (порт 1081)
- **HTTP-прокси**: Privoxy на порту 8118 (проксирует через SOCKS5 1080)
- **Python**: основной язык, все скрипты в этой директории
- **AI**: GPT-4.1-mini через RouterAI (prompt caching ~89%)

## Структура кода

```
~/telegram_logger_bot/     ← этот репозиторий (origin: Just.git на GitHub)
├── bot.py                 — основной Telegram бот (RAG-агент)
├── rag_agent.py           — RAG pipeline (роутер, retrieval, ответы)
├── company_context.py     — контекст компании для AI-промптов
├── sync_1c_full.py        — синхронизация данных из 1С OData
├── run_sync.sh            — обёртка для cron-запуска sync
├── build_source_chunks.py — чанкинг документов + embeddings
├── distill_*.py           — дистилляция km_facts/decisions/tasks/policies
├── matrix_auto_invite.py  — приглашение сотрудников в Matrix
├── matrix_listener.py     — ingestion Matrix → source_documents
├── email_sync.py          — синхронизация 81 почтового ящика
├── nutrition_bot.py        — бот для запроса КБЖУ данных
├── bom_*.py               — BOM Exploder (v2, версионирование)
├── .env                   — ВСЕ credentials (НЕ коммитить!)
└── docker-compose.yml     — конфиг Docker (PostgreSQL + Metabase)
```

## Критические правила

1. **Git flow: VPS → GitHub**. Редактируем на VPS, `git push` на GitHub. НЕ наоборот.
2. **Credentials**: всё в `.env`, НИКОГДА не хардкодить в коде.
3. **`.env` в .gitignore** — не коммитить.
4. **Docker сеть**: все контейнеры в `kb_network`. БД доступна по `172.20.0.2:5432`.
5. **Прокси для внешних API**: использовать `PROXY_URL` из `.env` для Telegram API.
6. **Privoxy** (порт 8118): для HTTPS-трафика через SOCKS5 (Claude Code использует его).
7. **Systemd сервисы**: `telegram-logger`, `email-sync`, `matrix-listener`, `auth-bom` — перезапуск через `sudo systemctl restart <service>`.
8. **OData пагинация**: всегда `$orderby=Ref_Key asc`.
9. **Тестирование**: перед деплоем проверять скрипты локально (`python3 script.py`).

## Ключевые таблицы БД

### Источники данных
- `source_documents` (256k+) — все документы (telegram, email, matrix)
- `source_chunks` (11.5k) — чанки с embeddings (HNSW индексы)
- `embeddings` (469k, legacy) — старые embeddings

### Knowledge Management (km_*)
- `km_facts` (~42k), `km_decisions` (~6.3k), `km_tasks` (~8k), `km_policies` (~2.3k)
- `km_entities` (~11.8k), `km_relations` (~7.5k)
- `km_filter_rules` — фильтрация мусора (junk_word/safe_word/min_length)

### 1С данные (c1_*)
- `c1_sales`, `c1_customer_orders`, `c1_dispatch_orders`
- `c1_specifications`, `c1_spec_materials`
- `c1_staff_history` (фильтр: `valid_until`, event_type "Перемещение")
- `c1_bank_balances` (3 счёта Фрумелад/НФ)
- `nomenclature` (7,774 записи, вес: ВесЧислитель/ВесЗнаменатель в кг)

### Materialized views (mart_*)
- `mart_sales` — обновляется каждые 10 мин
- `mart_customer_orders`, `v_plan_fact_weekly`

### Прочее
- `bom_expanded` (3,835 строк), `bom_calculations`
- `matrix_invites` (17 записей)
- `tg_user_roles`, `tg_full_analysis_settings`

## Подключение к БД

```python
import psycopg2
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host="172.20.0.2",
    dbname="knowledge_base",
    user="knowledge",
    password=os.getenv("DB_PASSWORD")
)
```

Или через psql:
```bash
docker exec -it knowledge_db psql -U knowledge -d knowledge_base
```

## Деплой

```bash
# После изменений:
sudo systemctl restart telegram-logger  # основной бот
sudo systemctl restart email-sync       # email
sudo systemctl restart matrix-listener  # Matrix ingestion

# Git:
git add -A && git commit -m "описание" && git push
```

## Текущие приоритеты (апрель 2026)

1. **Фаза 2** — переключение RAG на source_chunks (search_source_chunks в rag_agent.py)
2. **Фаза 2.5** — смена embedding модели на Qwen3-Embedding-0.6B + reranker
3. **Хвосты Фазы 6** — cron для matrix_auto_invite.py, iOS ссылка в /element
4. Мелкие задачи: --invite-rooms прогнать, sync_bank_balances проверить

## Полезные команды

```bash
# Логи сервисов
sudo journalctl -u telegram-logger -f
sudo journalctl -u email-sync -f

# Docker
docker ps
docker exec -it knowledge_db psql -U knowledge -d knowledge_base
docker exec -it synapse bash

# Synapse конфиг
docker exec synapse cat /data/homeserver.yaml

# Cron
crontab -l

# Проверить прокси
curl -x socks5h://127.0.0.1:1080 https://api.anthropic.com
```

## Организационный контекст

- **ООО "Фрумелад"** — продажи/администрация (Ирина — Генеральный Директор)
- **ООО "НФ" / "Новэл Фуд"** — производство
- Продукция: торты, пирожные, печенье для ритейл-сетей
- Admin Telegram user ID: 805598873
- Организация "Сириус" key: `7dac702d-dab7-11ec-bf30-000c29247c35`
## Логирование сессий (КРИТИЧЕСКОЕ ПРАВИЛО)

**ЭТО ПРАВИЛО ИМЕЕТ ВЫСШИЙ ПРИОРИТЕТ. Нарушение = потеря контекста для будущих сессий.**

Claude Code ведёт НЕПРЕРЫВНЫЙ лог в файле `.claude/sessions/YYYY-MM-DD_описание.md`:
- Создать файл лога В НАЧАЛЕ сессии, сразу после прочтения CLAUDE.md
- Дописывать в него ПО ХОДУ работы — каждое значимое действие (изменение файла, команда, решение)
- Финализировать в конце сессии

Формат файла:
```markdown
# Сессия: YYYY-MM-DD — Краткое описание

## Что сделано
- Пункт 1
- Пункт 2

## Изменённые файлы
- `file1.py` — что изменено
- `file2.py` — что изменено

## Незавершённое / Следующие шаги
- Что осталось доделать

## Заметки
- Важные наблюдения, баги, решения
```

### При старте сессии (ОБЯЗАТЕЛЬНАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ):
1. Прочитать CLAUDE.md (этот файл)
2. Прочитать последние 3 лога из `.claude/sessions/` для контекста:
```bash
ls -t .claude/sessions/ | head -3 | xargs -I {} cat .claude/sessions/{}
```
3. Создать новый файл лога для текущей сессии
4. Только после этого приступать к задаче

## Текущие задачи (обновлять вручную)

Файлы задач в корне репозитория:
- `TASK_rules_manage.md` — реализация /rules_find и /rules_off в боте
- `TASK_NOTIFICATIONS.md` — уведомления

### Очередь задач (апрель 2026):
1. **TASK_rules_manage.md** — поиск и деактивация правил фильтрации из бота
2. **RAG Фаза 2** — переключение на source_chunks (после завершения backfill)
3. **Qwen3-Reranker-0.6B** — локальный reranker
4. **Periodic Synthesis для 1С** — автосуммирование продаж/закупок/производства в km_facts

### Backlog:
- Дедупликация km_facts (embedding similarity в review_knowledge.py)
- Бот в Element X (Matrix-транспорт для /search, /analysis)
- matrix_auto_invite.py в cron
- sync_bank_balances — проверить деплой
- .well-known для frumelad.ru
