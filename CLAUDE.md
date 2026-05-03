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
10. **izibo / vibebot — ОТДЕЛЬНЫЙ ПРОЕКТ**. Любые работы по @izibo_bot, конструктору ботов, wizard, discovery, compiler/runtime izibo делаются ТОЛЬКО в `~/vibebot/` (отдельный git repo, свой `CLAUDE.md`). НИКОГДА не править izibo-код в `~/telegram_logger_bot/`. ТЗ от Опуса по izibo (SPRINT*, WIZARD_*, RUNTIME_*, STOREFRONT_* и т.п.) кладутся в `.tmp_input/` этого репо как «приёмник», но имплементация — `cd ~/vibebot && ...`. Это правило 100% и не обсуждается.

## Ключевые таблицы БД

### Источники данных
- `source_documents` (~298k) — все документы (telegram, email, matrix, 1с, + новые kind: `rag_answer`, `synthesized_1c`)
- `source_chunks` (~293k) — чанки, embedding v2 (Qwen3 1024-dim) + HNSW `idx_sc_embedding_v2`
- `embeddings` (469k, legacy) — старые e5 embeddings для telegram_messages/email_messages

### Knowledge Management (km_*)
- `km_facts` (~42k), `km_decisions` (~6.3k), `km_tasks` (~8k), `km_policies` (~2.3k)
- `km_entities` (~13k) — +5 колонок kg_graph (centrality/degree_in/degree_out/community_id/kg_updated_at), rebuild `0 */6 * * *`
- `km_relations` (~7.9k) — типы: stored_at (3593), supplies (1316), works_in (1114), holds_position (1095), responsible_for (366), buys (225), collaborates_with (40), reports_to (40), complains_about (29), approves (23). Soft-связи (collaborates/reports) слабо извлекаются — compensated Фаза 4 (comm_edges)
- `km_filter_rules` — фильтрация мусора (junk_word/safe_word/min_length)

### Communication Graph (comm_*, 2026-04-21 KG Фаза 4)
- `comm_edges` — рёбра tg_user_id × tg_user_id по типам сигналов (reply weight 3.0, co_activity 0.2/неделя)
- `comm_users` — per-user community_id / betweenness / degree_weighted / matrix_joined / is_external / km_entity_id / employee_ref_key
- Rebuild `30 2 * * *` через `python3 -m tools.comm_graph rebuild` (~3 сек для 48 чатов)
- Цель: Волна 5 (Matrix-миграция по кластерам общения)

### ⚠ Унификация идентичностей — используй ГРАФ, а не новые таблицы
Когда нужно сматчить «одного человека» между разными системами (1С сотрудник, 1С пользователь, TG user_id, email, Matrix) —
**не создавай новые таблицы типа `entity_identities`**. Уже есть всё:
- `km_entities` (entity_type='person') — центральный узел персоны. Aliases + attrs ({employee_ref_key, c1_user_ref_key, tg_user_ids, emails, matrix_user_id, role}).
- `comm_users.km_entity_id` + `comm_users.employee_ref_key` — связь tg_user → KG → 1С сотрудник.
- `email_employee_mapping` (81 запись) — email + tg_user_id + c1_employee_key + employee_name_1c. Расширяй её, не создавай параллельную.
- `c1_employees` (1180), `c1_users` (246, Catalog_Пользователи 1С), `tg_user_roles` (37 user_id × роль).
Идея: один person — одна km_entity, у которой aliases = все имена/идентификаторы. RAG/агенты ходят через KG.

Скрипт-наполнитель: `populate_identities.py` (FIO match + RapidFuzz + LLM на ambiguous). Запускать после изменений в любом из источников.

### 1С данные (c1_*)
- `c1_sales`, `c1_customer_orders`, `c1_dispatch_orders`
- `c1_specifications`, `c1_spec_materials`
- `c1_staff_history` (фильтр: `valid_until`, event_type "Перемещение")
- `c1_bank_balances` (3 счёта Фрумелад/НФ)
- `c1_users` (Catalog_Пользователи 1С, 246) — на это ссылается `Автор_Key` документов. Sync через `sync_bank_expenses_authors.py`.
- `c1_bank_expenses` (29.7k) — расширено колонками `author_key, responsible_key, basis_doc_ref, basis_doc_type, bsg_order_ref, bsg_order_type, posted_by_bank, bank_post_date` (добавлены 2026-04-27 через `sync_bank_expenses_authors.py`).
- `nomenclature` (7,774 записи, вес: ВесЧислитель/ВесЗнаменатель в кг)

### Дополнительные приобретения (sync_acquisitions_extra.py, 2026-04-27)
- `c1_purchases_other_assets` + `_items` — Document_ПриобретениеУслугПрочихАктивов (аренда, коммуналка, IT, юр.услуги — то, что НЕ ПТУ). С 2025-01-01.
- `c1_advance_reports` + `c1_advance_report_other_expenses` + `c1_advance_report_supplier_pmts` — Document_АвансовыйОтчет (подотчётные расходы). На строках subtable есть прямой `cash_flow_item_key` — не нужна эвристика через поставщика.
- Cron: `--quick` каждые 30 мин (14 дней), `--daily` 5:00 (60 дней), `--full` ВС 3:00 (всё).

### Materialized views (mart_*) — обновляются каждые 10 мин через cron REFRESH
- `mart_sales`, `mart_purchases`, `mart_production`, `mart_customer_orders`, `mart_supplier_orders`
- Используются в Router-driven analytics tools (purchases_by_nomenclature, sales_by_nomenclature, production_by_nomenclature, stock_balance, top_* и т.д.)
- Views: `v_plan_fact_weekly`, `v_consumption_vs_purchases_monthly`, `v_sales_adjusted`, `v_current_staff`

### Прочее
- `bom_expanded` (15k строк, calc_id=4) — DAG продукт→материалы, используется tools/bom_graph (reachability upward/downward). В bom 197 finished + 48 полуфабрикатов + 418 raw.
- `bom_calculations`
- `matrix_invites` (17 записей)
- `tg_user_roles`, `tg_full_analysis_settings`
- `bot_settings` — key/value для рантайм-настроек бота (element_onboarding_video_file_id и т.п.)

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

## Бэкапы БД (3-2-1+, настроено 2026-04-29)

**Схема — 4 копии, 3 носителя, 3 offsite, 2 юрисдикции:**

| Локация | Retention | Скрипт | Cron |
|---|---|---|---|
| VPS-локал `~/telegram_logger_bot/backups/` | 1 день (`BACKUP_DAYS_TO_KEEP=1`) | `backup.sh` (pg_dump → gzip) | `30 3 * * *` |
| Cloud.ru S3 `s3://bucket-318efd/db_backups/` | 30 дней (`S3_BACKUP_RETENTION_DAYS=30`) | `backup_to_s3.py` (boto3 + sha256 verify + cleanup) | внутри backup.sh |
| Amsterdam VPS `root@109.234.38.39:/root/db_backups/` | 5 дней (`REMOTE_BACKUP_RETENTION_DAYS=5`) | `backup_to_remote.py` (rsync -tW + size verify) | внутри backup.sh |
| Helsinki VPS `root@77.42.83.103:/root/db_backups/` | 5 дней | тот же `backup_to_remote.py` | внутри backup.sh |

**Verify (раз в неделю):**
- `verify_backup.py` — суббота 7:00 — скачивает последний S3-объект, проверяет sha256/gzip integrity/SQL signature/CREATE TABLE+COPY signatures
- `test_restore_remote.py` — воскресенье 5:00 МСК — ssh на Helsinki → docker `pgvector/pgvector:pg15` → реальный pg_restore последнего дампа → count check (`source_chunks ≥ 500k`, `km_facts ≥ 30k`, `source_documents ≥ 200k`, `c1_employees ≥ 1k`) → cleanup container

**Правила:**
- Дамп ~5 GB compressed / ~17 GB raw. На VPS 33 GB free → полный pg_restore локально невозможен, делаем на Helsinki (67 GB free, 3.7 GB RAM, Docker).
- Перед предложением апгрейда диска/железа — проверь имеющиеся VPS (Amsterdam, Helsinki, любые другие платные ресурсы). Они часто простаивают и могут принять копии бесплатно.
- Soft-fail на удалённых: если один VPS недоступен — другие копии всё равно создаются, TG-алерт. Pipeline не падает целиком из-за одного недоступного offsite.
- Любое изменение `backup.sh` тестировать на существующем дампе (smoke-test через прямой вызов скрипта на готовом файле).

**Ключи ssh:**
- Amsterdam: `/home/admin/.ssh/amsterdam_proxy`
- Helsinki: `/home/admin/.ssh/id_rsa`
- Эти же ключи используются autossh в `proxy-tunnel-{amsterdam,helsinki}.service`.

**При восстановлении из бэкапа** (если prod БД умерла):
1. Скачать свежий дамп из любой из 4 локаций (приоритет: VPS-локал → S3 → Helsinki → Amsterdam).
2. `gunzip -c backup_*.sql.gz | docker exec -i knowledge_db psql -U knowledge -d knowledge_base`
3. Если pgvector отсутствует — `CREATE EXTENSION vector;` вручную.
4. Перезапустить сервисы: `sudo systemctl restart telegram-logger email-sync matrix-listener auth-bom`.

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

## Tool Registry (`tools/`, 2026-04-21)

Путь B (прагматичная стандартизация): декоратор `@tool(name, domain, description, input_model)` регистрирует функцию в `tools.registry.REGISTRY`. Прямой import и `invoke(name, params)` оба валидируют через pydantic InputModel (SSOT для defaults). `llm_schemas()` даёт JSON-schema в формате Anthropic/OpenAI tool_use — готово для native tool_use API.

**20 tools в registry** (состояние на 2026-04-21 после KG Фаз 1-4):
- `attachments` — analyze_attachment (PDF/XML/docx/xlsx/image, magic-byte detect, anti-hallucination), analyze_video (MTProto до 2GB + Whisper + adaptive frame sampling + 6-section prompt)
- `bom` — get_bom_report, bom_reachability (upward/downward в BOM DAG), bom_affected_by_supplier_delay (supplier→supplies→BOM upward)
- `c1` — synthesize_1c_snapshot (persist=True триггерит upsert в source_chunks)
- `chats` — get_chat_list
- `comm` — comm_neighbors, comm_community, matrix_migration_wave (communication graph из tg_chat reply + co_activity)
- `element_video` — generate_element_onboarding_video
- `identification` — identify_employee_by_text (LLM-match к v_current_staff)
- `kg` — graph_neighbors, graph_shortest_path, employee_responsibility (поверх km_entities+km_relations с PageRank/Louvain)
- `km_rules` — search_filter_rules, deactivate_filter_rule (с инвалидацией distillation cache через sys.modules)
- `notifications` — resolve_notification_recipients, prepare_notification, finalize_notification

**Добавить tool**: модуль в `tools/`, функция с декоратором, импорт в `tools/__init__.py`. Внутренний Python-код зовёт через прямой import (type-hints, нет лишних wrapper frames). LLM/slash/HTTP/Element-бот — через `invoke(name, dict_params)`.

Двойная регистрация при `python3 -m tools.X`: `tools/__init__.py` импортирует модуль, runner запускает как `__main__` — те же @tool срабатывают второй раз. `registry.py` это штатно обрабатывает (возвращает существующую Tool, не raise).

Детали контракта и проектные решения: `logs/sessions/2026-04-21_tools-registry.md`. Граф-слой: `TASK_kg_graph.md`.

## Приоритеты (апрель-май 2026)

### RAG (основной фокус):
1. ✅ **TASK_rag_quality_v2 Фазы 1-6** (готовы 2026-04-17): Router v2, Evaluator+Escalation, Text-to-SQL, km_fixation, Reply-chain, Periodic Synthesis.
2. ✅ **P0 — RAG quality улучшения** (закрыт 2026-04-20 + дополнен 2026-05-03):
   - ✅ `top_*` analytics tools → net-выручка с Корректировками (2026-04-20)
   - ✅ `sales_summary` / `purchase_summary` — добавлен ИТОГО (34.37M без НДС) как первый result (2026-05-03)
   - ✅ `bank_balance` — новый analytics_type, прямой запрос c1_bank_balances (643K точно) (2026-05-03)
   - ✅ Email search — entity-aware ILIKE + порог 0.42 (2026-05-03 dcf2ff6)
   - ✅ TG vector search — убран фильтр target_tables из HNSW (Q16 силикагель 0→16 ev) (2026-05-03)
   - ✅ Дедупликация `km_facts` по embedding-similarity (в `review_knowledge.py`) (2026-04-20)
   - ✅ 👍/👎 inline-кнопки под RAG-ответами → `rag_query_log.user_feedback` (2026-04-20)
3. **P1** — full_rag_battery прогон (30 вопросов) для оценки прогресса vs baseline 20.04 (63%, latency 52.8s)
4. **P2 backlog (отложено):**
   - Бот в Element X (Matrix-транспорт `/search`, `/analysis`)
   - Latency optimization (64s avg → цель ≤45s для Tier 3)

### Прочее:
5. ✅ **TASK_rules_manage** — /rules_find и /rules_off через tools/km_rules (2026-04-21).
6. Хвосты: iOS ссылка в /element, --invite-rooms прогнать, sync_bank_balances проверить
7. `v_plan_fact_weekly` в synthesize_1c_facts.py — починить 5-vs-6 колонок

### Tool Registry — следующие волны (после 2026-04-21 шага 1):
- **Волна 2** — approval workflow + review_knowledge: /rules_pending, rule_approve/rule_reject (bot.py:3365-3474), apply_verdicts/apply_new_rules из review_knowledge.py. Завершает домен `km_rules`.
- **Волна 3** — `send_via_telegram_api(bot_token, chat_id, text, reply_markup)`: прямой HTTPS POST для headless-сценариев. Разблокирует: auto_fix-алерты без PTB Application, Element-бота, любые cron-уведомления.
- **Волна 4** — RAG Router → native tool_use: миграция 6 RAG-tools (search_1c_analytics, search_1c_data, search_telegram_chats, search_emails, search_unified, search_source_chunks_reranked) на Anthropic/OpenAI tool_use API. Убрать ручной if/elif dispatch в `run_rag`. **Риск**: RAG-метрика нестабильна (±10% run-to-run), любое движение Router'а даёт шум. Делать только когда контракт registry обкатан на волнах 2-3 и есть baseline на стабильной метрике.
- Scope/period параметризация `synthesize_1c_snapshot` — разбить `build_synthesis_facts()` на sub-функции (sales_day/clients_month/...). Делать когда появится конкретный use-case (RAG on-demand recompute по entity).

### Волна 5 — Агент многоходовых поручений (на примере Matrix-миграции):

Долгоживущий LLM-агент который планирует, исполняет, мониторит и адаптирует стратегию по крупной организационной задаче (несколько дней/недель). Пример сценария: **миграция 80 сотрудников на Matrix** — агент разрабатывает план, критерии успешности, варианты А/Б; анализирует чаты TG (мертвые/активные), матчит пользователей с сотрудниками 1С (внешние → исключаем), формирует задачи для Claude Code (код/презентации/рассылки), мониторит реакцию людей, пересматривает стратегию, эскалирует тебе на критичных шагах. Работает пока задача не решена.

**Что нужно достроить (сейчас нет):**
- **Persistent goal/plan store** — таблица `agent_goals` с деревом подзадач, статусами, историей решений. Агент перечитывает состояние при каждом «пробуждении» (в отличие от LLM контекст-окна).
- **Scheduler с триггерами** — cron («проверь раз в день»), events («реакция в TG → реакция агента»), user-feedback listener. Оркестратор над Claude Agent SDK / Claude Code.
- **Safety framework** — бюджеты ($N/день), human-approval для критичных шагов (рассылка на 80+, удаление чатов), audit log всех действий, kill-switch.
- **Tools высшего порядка в registry**: `spawn_claude_code_task(prompt) → task_id`, `wait_for_user_approval(question) → bool`, `query_goal_state(goal_id) → progress`, `revise_plan(goal_id, new_rationale)`.

**Предпосылки** — сначала закрыть Волны 2-4 (полный tool-layer), желательно Волну 3 (`send_via_telegram_api` для headless-рассылок без PTB).

**Прототипирование перед Matrix**: начать с маленького агента (напр. «оптимизатор /element-напоминаний»: раз в день смотрит кто не в Matrix, адаптирует тон под сотрудника, мониторит реакцию, эскалирует после 3 ignored). Обкатывает persistent-state + triggers + safety на узкой задаче. Потом масштабировать до полного Matrix-агента.

**Стоимость ориентировочно**: $5-50/день Claude Opus в зависимости от активности. Реально мониторить.

**Существующий прототип концепции** — `auto_fix.sh` (узкая область, code self-healing по cron). Архитектурно совпадает, но для Волны 5 нужен более универсальный runtime.

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

## Дискуссия и несогласие (ПРАВИЛО)

Если пользователь технически заблуждается, неправильно понимает вопрос или
выбирает суб-оптимальный вариант — НЕ соглашаться сразу, чтобы не обидеть.

- Прямо назвать что не так: «нет, это не так потому что X», «вопрос на самом деле про Y, не Z».
- Привести доказательства: код/файл/строки/цифры, не «обычно» или «чаще всего».
- Если после аргументов пользователь всё равно настаивает — уважать его решение, выполнить, но **зафиксировать в session log** что компромисс был принят вопреки рекомендации (с причиной), чтобы потом можно было оценить результат.
- НЕ играть в "прав тот кто громче" — если у пользователя есть контекст (бизнес, история, опыт), которого Claude не видит, его выбор может быть верным даже если технически выглядит хуже. Сначала спросить «что я упускаю?», потом спорить.

Цель: пользователь нанимает Claude как инженера, а не как соглашателя.
Сэкономленное время > сохранённого лица.

## Логирование сессий (КРИТИЧЕСКОЕ ПРАВИЛО)

**ЭТО ПРАВИЛО ИМЕЕТ ВЫСШИЙ ПРИОРИТЕТ. Нарушение = потеря контекста для будущих сессий.**

Claude Code ведёт НЕПРЕРЫВНЫЙ лог сессии в `logs/sessions/YYYY-MM-DD_описание.md`.

### При старте сессии (ОБЯЗАТЕЛЬНАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ):
1. Прочитать CLAUDE.md (этот файл)
2. Прочитать последние 3 лога из `logs/sessions/` для контекста:
   ```bash
   ls -t logs/sessions/ | head -3 | xargs -I {} cat logs/sessions/{}
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
- `TASK_rules_manage` — ✅ /rules_find и /rules_off мигрированы на tools/km_rules (2026-04-21).

### Backlog:
- Бот в Element X (Matrix-транспорт для /search, /analysis)
- sync_bank_balances — проверить деплой
- .well-known для frumelad.ru
- RAG latency optimization (64s avg → ≤45s)
- **RAG latency: 2-мин gap между fixation и sendMessage** (обнаружен 2026-04-20 на первом пост-P0 запросе в личке). Между `logger.info("RAG answer fixated")` и HTTP sendMessage в /tmp logs 134 секунд с пустыми getUpdates — main thread чем-то занят. Подозрения: `_log_rag_query` с `RETURNING id` + `fetchone()` (новый код), или saturation DB-connection'ов при параллельном прогоне battery. Повторить без battery; если gap останется — инструментировать шаги между fixation и reply_text.
- review_knowledge.py Step 1: добавить retry с backoff для RouterAI 402 Payment Required (17.04 и 20.04 весь LLM-ревью упал одним махом; dedupe Step 0 работает, только LLM review страдает)
- 30 вопросов full_rag_battery расширить до 50+ (добавить сложные аналитические, цепочки)
- **Стабилизация метрики full_rag_battery**: прогоны 17.04 и 20.04 на одном корпусе вопросов показали run-to-run дрейф ±10% по `has_1c_evidence` (73%→63%, 3 вопроса потеряли 1С, 1 обрёл). gpt-4.1 temp=0 не детерминирован для Router'а. Нужна серия прогонов (3-5 подряд) + медиана/std вместо одиночной цифры. Иначе любая реальная регрессия утонет в шуме.
