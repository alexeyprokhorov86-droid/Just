# CLAUDE.md — Инструкция для Claude Code

## Проект

Платформа BI и knowledge management для кондитерской компании "Фрумелад" (~80 сотрудников).
Владелец и разработчик: Алексей.

## Стек

- **VPS**: 95.174.92.209 (Ubuntu 22.04, Cloud.ru)
- **БД**: PostgreSQL 15 + pgvector 0.8.1 в Docker (image `pgvector/pgvector:pg15`, `shm_size: 2gb`, сеть `kb_network`, IP 172.20.0.2). docker-compose.yml лежит в `/home/admin/knowledge-base/` (НЕ в корне репо!)
- **Database**: `knowledge_base`, user: `knowledge`, credentials в `.env`
- **Metabase**: Docker (172.20.0.3, порт 3000)
- **Matrix/Synapse**: Docker (порт 8008 внутри, 443/8448 снаружи), homeserver `matrix.frumelad.ru`
- **Bridge**: mautrix-telegram v0.15.2 + relay bot `@frumelad_bridge_bot`
- **Прокси**: Amsterdam SOCKS5 (порт 1080), Helsinki SOCKS5 (порт 1081)
- **HTTP-прокси**: Privoxy на порту 8118 (проксирует через SOCKS5 1080)
- **Python**: основной язык, все скрипты в этой директории
- **AI модели (RouterAI)**:
  - Answerer: `openai/gpt-4.1` (default), escalation на `anthropic/claude-opus-4.7` если evaluator говорит "слабо"
  - Router: `openai/gpt-4.1` с 20 few-shot примерами + 3 retry
  - Text-to-SQL (custom_sql): `anthropic/claude-opus-4.7`
  - Embeddings: `Qwen/Qwen3-Embedding-0.6B` (1024-dim, HNSW `idx_sc_embedding_v2`)
  - Reranker: `Qwen/Qwen3-Reranker-0.6B` (native Causal LM API, batch_size=1)
  - Флаги в `.env`: `USE_EMBEDDING_V2=true`, `USE_RERANKER=true`

## Структура кода

```
~/telegram_logger_bot/        ← этот репозиторий (origin: Just.git на GitHub)
├── bot.py                    — основной Telegram бот (RAG-агент, /rooms, element_reminder)
├── rag_agent.py              — RAG pipeline (Router + 6 tools + search_unified + evaluator + escalation)
├── chunkers/
│   ├── embedder.py           — Qwen3-Embedding-0.6B (embed_query_v2, embed_document_v2)
│   ├── reranker.py           — Qwen3-Reranker-0.6B (native Causal LM)
│   └── ...
├── embedding_service_e5.py   — LEGACY e5-base (только для km_* поиска)
├── tests/
│   ├── check_embedding_consistency.py — Qwen3 doc↔query smoke-тест
│   └── ab_compare_retrieval.py        — 10 вопросов, сравнение веток
├── company_context.py        — контекст компании для AI-промптов
├── sync_1c_full.py           — синхронизация данных из 1С OData
├── synthesize_1c_facts.py    — ежедневный cron → km-like факты в source_chunks
├── build_source_chunks.py    — чанкинг документов + Qwen3 embeddings
├── distillation.py           — дистилляция km_facts/decisions/tasks/policies (e5 legacy)
├── matrix_auto_invite.py     — приглашение сотрудников в Matrix
├── matrix_listener.py        — ingestion Matrix → source_documents
├── email_sync.py             — синхронизация 81 почтового ящика
├── nutrition_bot.py          — бот для запроса КБЖУ данных
├── bom_*.py                  — BOM Exploder (v2, версионирование)
├── notifications.py          — /notify, /notify_status, /notify_remind
├── report_digest_agent.py    — парсер ночных отчётов → auto_fix триггеры
├── auto_fix.sh, auto_agent_cron.py — автономный Claude-агент self-healing
├── .env                      — ВСЕ credentials (НЕ коммитить!)
└── ... (docker-compose.yml живёт в /home/admin/knowledge-base/, не здесь)
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
- `source_documents` (~298k) — все документы (telegram, email, matrix, 1с, + новые kind: `rag_answer`, `synthesized_1c`)
- `source_chunks` (~293k) — чанки, embedding v2 (Qwen3 1024-dim) + HNSW `idx_sc_embedding_v2`
- `embeddings` (469k, legacy) — старые e5 embeddings для telegram_messages/email_messages

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

### Materialized views (mart_*) — обновляются каждые 10 мин через cron REFRESH
- `mart_sales`, `mart_purchases`, `mart_production`, `mart_customer_orders`, `mart_supplier_orders`
- Используются в Router-driven analytics tools (purchases_by_nomenclature, sales_by_nomenclature, production_by_nomenclature, stock_balance, top_* и т.д.)
- Views: `v_plan_fact_weekly`, `v_consumption_vs_purchases_monthly`, `v_sales_adjusted`, `v_current_staff`

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

## RAG архитектура (после TASK_rag_quality_v2, апрель 2026)

1. **Router** (`route_query`, gpt-4.1, 20 few-shot + retry x3) → plan со steps (CHATS/EMAIL/1С_ANALYTICS/1С_SEARCH/WEB/KNOWLEDGE) + entities (clients/suppliers/products/warehouses) + period.
2. **Tools** (`search_1c_analytics` / `search_1c_data` / `search_telegram_chats` / `search_emails` / `search_unified` для KNOWLEDGE). analytics_type: top_clients/top_products/top_suppliers, sales_summary/purchase_summary/production_summary, *_by_nomenclature, stock_balance, plan_vs_fact, custom_sql.
3. **search_unified** (при USE_EMBEDDING_V2=true): объединяет km_* (e5) + source_chunks (Qwen3) → dedup → Qwen3-Reranker → top-30.
4. **Evaluator (pre-answer)** — если evidence мало, retry поиск с расширенными chats/keywords.
5. **Answerer** (gpt-4.1) — generate с ссылками [n].
6. **Answer evaluator (post-answer)** — если ответ слабый → **escalation на Claude Opus 4.7**.
7. **Fixation** — удачные ответы с 1С-источником → source_documents(source_kind='rag_answer') + source_chunks. Повторные похожие вопросы ретривятся мгновенно.
8. **Reply-chain** — `message.reply_to_message.from_user.is_bot` → prev Q/A из chat_data → передаётся в process_rag_query как prev_context → встраивается в prompt Answerer'а.

## Приоритеты (апрель-май 2026)

1. ✅ **TASK_rag_quality_v2** — Router v2, Evaluator+Escalation, Text-to-SQL, km_fixation, Reply-chain, Periodic Synthesis (Фазы 1-6 готовы 2026-04-17).
2. **TASK_rules_manage.md** — поиск и деактивация правил фильтрации из бота
3. Хвосты: matrix_auto_invite.py в cron (✅ уже сделано), iOS ссылка в /element
4. Мелкие задачи: --invite-rooms прогнать, sync_bank_balances проверить
5. `v_plan_fact_weekly` в synthesize_1c_facts.py — починить 5-vs-6 колонок

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

Claude Code ведёт НЕПРЕРЫВНЫЙ лог сессии в `.claude/sessions/YYYY-MM-DD_описание.md`.

### При старте сессии (ОБЯЗАТЕЛЬНАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ):
1. Прочитать CLAUDE.md (этот файл)
2. Прочитать последние 3 лога из `.claude/sessions/` для контекста:
   ```bash
   ls -t .claude/sessions/ | head -3 | xargs -I {} cat .claude/sessions/{}
   ```
3. Создать новый файл лога для текущей сессии (даже если задача кажется быстрой!)
4. Только после этого приступать к задаче

### Когда дописывать в лог (ЖЁСТКИЕ ТРИГГЕРЫ — без интерпретации):

Дописывать в `## Что сделано` ОБЯЗАТЕЛЬНО:
- **Перед каждым текстовым ответом пользователю** — append события с момента прошлого ответа. Это самый важный триггер: если перед написанием ответа в чат не дописал лог — нарушение.
- **После каждого `Edit` / `Write`** в файл проекта — одна строка `файл.py — что изменено и зачем`
- **После каждой Bash-команды, изменяющей систему**: `sudo *`, `crontab`, `docker exec * INSERT/UPDATE/DELETE/CREATE/DROP`, `systemctl restart`, `git commit/push`, `pip install`, `npm install`, изменение .env — одна строка о факте и зачем
- **После каждого решения** «делаем X а не Y» — в `## Заметки` с обоснованием
- **После каждой ошибки или неожиданного поведения** — в `## Заметки`

НЕ дописывать (шум):
- Read, Glob, Grep — чисто исследовательские операции
- Bash без побочных эффектов: `ls`, `cat`, `git status`, `git log`, `psql -c "SELECT ..."`
- Промежуточные итерации одного и того же действия (одна итоговая строка вместо 5 промежуточных)

### Формат файла:
```markdown
# Сессия: YYYY-MM-DD — Краткое описание

## Что сделано
- [HH:MM] Пункт 1
- [HH:MM] Пункт 2

## Изменённые файлы
- `file1.py` — что и зачем
- `file2.py` — что и зачем

## Незавершённое / Следующие шаги
- Что осталось доделать (с конкретными командами/файлами, чтобы следующая сессия могла продолжить без раскопок)

## Заметки
- Решения, неочевидные находки, ошибки и их причины
```

Время в `[HH:MM]` — для понимания «что было до, что после» в долгих сессиях.

### Финализация:
- **Признаки конца сессии**: пользователь сказал «спасибо/всё/пока», 10+ минут тишины после видимого закрытия задачи, или явный «давай завершим».
- При признаке — сразу обновить `## Незавершённое / Следующие шаги` (даже если пусто — написать «всё закрыто»), убедиться что `## Изменённые файлы` соответствует git status.
- Если сессия прерывается на ожидании (фоновая команда, реиндекс, длинный билд) — записать в лог что ждём и ID/команду, чтобы next session могла подхватить.

### Самопроверка перед каждым ответом пользователю:
> «Что я сделал с момента прошлого ответа? Это в логе?» → если нет → дописать → потом отвечать.

## Текущие задачи (обновлять вручную)

Файлы задач в корне репозитория:
- `TASK_autonomous_agent.md` — автономный Claude-агент (✅ 2026-04-16)
- `TASK_qwen_consistency.md` — дисциплина instruction-aware Qwen3 (✅ 2026-04-17)
- `TASK_rag_quality_v2.md` — Router v2 + Evaluator + Text-to-SQL + km-fixation + Reply-chain (✅ Фазы 1-6 2026-04-17)
- `TASK_rules_manage.md` — ⚠️ файл не существует, но фича `/rules_find`/`/rules_off` уже в bot.py

### Backlog:
- Дедупликация km_facts (embedding similarity в review_knowledge.py)
- Бот в Element X (Matrix-транспорт для /search, /analysis)
- sync_bank_balances — проверить деплой
- .well-known для frumelad.ru
- Обновить `top_*` analytics tools — использовать флаг fresh данных (v_sales_adjusted для возвратов)
