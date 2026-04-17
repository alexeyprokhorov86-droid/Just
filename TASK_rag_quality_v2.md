# TASK: RAG Quality v2 — качественная выдача ответов из 1С

## Контекст

Сессия 2026-04-17 показала: после включения Qwen3 + Reranker + 5 SQL-tools
RAG корректно отвечает на 2 из 6 проверочных вопросов ("сколько муки",
"сколько Медовика произвели"). Остальные 4 — провал, потому что Router
не выбрал правильный 1С-tool:

| Вопрос | Ожидался tool | Фактический план | Результат |
|---|---|---|---|
| Сколько муки в феврале | purchases_by_nomenclature | ✅ | ✅ 10 т на 255к |
| Сколько Медовика в марте | production_by_nomenclature | ✅ | ✅ 4 SKU |
| Остатки сахара | stock_balance | ❌ только CHATS | ❌ "нет данных" |
| Топ 5 поставщиков март 26 | top_suppliers | частично | ❌ только 2 (старая таблица sales?) |
| Что продали Дикси в Q4 25 | sales_by_nomenclature + client | ❌ только CHATS | ❌ обсуждение из чата |
| Остатки упаковки "СЫРЬЯ СКЛАД" | stock_balance | ❌ только CHATS | ❌ "нет данных" |

**Корневая причина:** Router промахивается даже на очевидных паттернах
("остатки X" → stock_balance — не сработало дважды подряд). Причины
предположительно:
- Модель Router'а (GPT-4.1-mini) слишком слабая для маршрутизации
- Мало few-shot примеров в Router prompt
- Нет self-critique или escalation при слабом плане
- Нет fallback на text-to-SQL для сложных запросов

Пользовательские требования:
1. **Бюджет практически не ограничен** — приоритет качеству
2. **Рефлексия**: если ответ слабый — перезапустить с более продвинутой моделью
3. **Text-to-SQL** как мощный fallback (с валидацией безопасности)
4. **Фиксация удачных ответов** в km_facts с высоким confidence
5. **Контекстная цепочка** через Reply в Telegram (follow-up вопросы)

---

## План

### Фаза 1 — Router upgrade (КРИТИЧНО)

**1.1 Модель Router'а**
- Проверить текущую модель в `route_query` (ROUTERAI_API_KEY + какая модель)
- Переключить на сильную: GPT-4.1 (не mini) или Claude Sonnet 4.6 / Opus
- Параметр `temperature=0` для детерминизма

**1.2 Few-shot примеры в Router prompt**
Добавить 20+ фиксированных примеров с эталонными планами, покрывающих:
- "сколько <товар> купили/продали/произвели в <период>" → *_by_nomenclature
- "остатки <товар> [на складе X]" → stock_balance
- "что продали клиенту X [в период]" → sales_by_nomenclature + entities.clients
- "топ N поставщиков/клиентов/продуктов [в период]" → top_suppliers/clients/products
- "план-факт [в период]" → plan_vs_fact
- "остатки на складе Y [по всей номенклатуре]" → stock_balance с warehouse filter
- "какие поставки ждём [от поставщика X]" → 1С_SEARCH + CHATS
- "обсуждения по теме T" → CHATS/EMAIL + KNOWLEDGE

**1.3 Router self-critique**
После первичного plan — второй LLM-вызов:
"Проверь план. Есть ли tools, которые ты пропустил? Список tools: [полный список].
Если план неполный, верни расширенный."

**1.4 Валидация entities→filters**
Router часто возвращает `entities.clients=["Дикси"]`, но SQL-tools этим не
пользуются (нет JOIN по клиентам в новых *_by_nomenclature). Расширить:
- sales_by_nomenclature: добавить optional `client` параметр (фильтр по
  mart_sales.client_name ILIKE), пробросить из entities
- purchases_by_nomenclature: `supplier` параметр (mart_purchases.contractor_name)
- stock_balance: `warehouse` параметр (c1_warehouses.name ILIKE)

### Фаза 2 — Self-reflection и escalation

**2.1 Evaluator после первого ответа**
Новый LLM-вызов "проверь качество ответа":
- Есть ли в ответе прямой ответ на вопрос (цифра/факт/имя)?
- Источники покрывают все тезисы?
- Есть ли 1С-evidence, если вопрос количественный?

Если результат "слабый" (например, все источники — чаты при количественном
вопросе) → retry с более мощной моделью и forced 1С-tools.

**2.2 Escalation цепочка**
1. Tier 1: GPT-4.1-mini Router (быстро, дёшево) — 80% кейсов
2. Tier 2: GPT-4.1 Router + self-critique — 15%
3. Tier 3: Claude Opus Router + text-to-SQL fallback — 5%

Триггеры escalation:
- Evaluator сказал "слабо"
- В evidence нет источников из `1С:*` при количественном вопросе
- Пользователь прислал Reply "не то" или "дополни"

**2.3 Cost tracking**
`rag_query_log` расширить полями:
- tier_used (1/2/3)
- tokens_router, tokens_generator, tokens_evaluator
- total_cost_rub

### Фаза 3 — Text-to-SQL как tool

**3.1 Безопасный SQL-runner**
Новый модуль `chunkers/sql_runner.py`:
- Read-only connection (второй пользователь в БД с SELECT-only на mart_*, c1_*, nomenclature, v_*, km_*)
- Whitelist: SELECT, WITH, CASE, COALESCE, GROUP BY, ORDER BY, LIMIT
- Blacklist: любые DDL/DML
- AST parsing (через `sqlglot` или простой regex-гвард)
- Query timeout 15 сек
- LIMIT 1000 строк принудительно

**3.2 Text-to-SQL generator**
LLM с full DB schema в system prompt генерирует SQL. Схему подавать
компактно: только used tables для текущего запроса.

Новый analytics_type = `custom_sql`:
1. LLM составляет SQL
2. EXPLAIN проверяет query cost
3. SQL исполняется через read-only connection
4. Первые 50 строк идут как evidence с пометкой "SQL: <query>"

**3.3 Fallback-логика**
Router возвращает plan → если в plan нет ни одного 1С_ANALYTICS шага, а
вопрос похож на количественный (regex по keywords: "сколько", "остат",
"объём", "топ", "итого", "сумма") → добавить custom_sql шаг автоматически.

### Фаза 4 — Memory & context

**4.1 Фиксация успешных ответов в km_facts**
После генерации ответа — если evaluator оценил как "качественный" (все тезисы
со ссылками на 1С, нет "недостаточно данных"):
- Создать `km_fact` с `fact_text = "<кратко вопрос>: <кратко ответ>"`
- `fact_type = 'rag_answer'`
- `confidence = 0.95`
- `source_count` = число использованных evidence
- `embedding` через Qwen3 (чтобы попадать в retrieval)

Ретривер подхватит эти факты при повторных похожих вопросах.

**4.2 Conversation context через Reply**
В `bot.py` — обработчик `handle_message` должен:
- Распознавать `message.reply_to_message` и брать его text как предыдущий
  ответ бота
- Искать в `rag_query_log` запись по message_id бывшего ответа
- Передавать в `process_rag_query` параметр `prev_context={question, answer, evidence_ids}`
- В system prompt answerer'а добавлять: "Это follow-up к предыдущему
  диалогу. Предыдущий вопрос: ..., ответ: ..."

**4.3 rag_query_log расширение**
Поля:
- `parent_query_id` — follow-up chain
- `user_feedback` — 👍 / 👎 (inline-кнопки под ответом)
- `tier_used`, токены, стоимость

### Фаза 5 — Infrastructure fixes

**5.1 Router retry при таймауте**
3 попытки с exponential backoff (2/4/8 сек). Если все упали —
`_default_plan(question)` (без LLM).

**5.2 Дубли в source_chunks**
Разобраться почему в A/B Q2/Q9 одинаковые чанки повторяются 4 раза.
Скорее всего: `build_source_chunks.py` переиндексирует одно и то же.
Фикс: UPSERT по (document_id, chunk_no) или хэш по содержанию.

### Фаза 6 — Periodic Synthesis для 1С

**6.1 Ежедневный cron**
`synthesize_1c_facts.py` — cron @ 06:30 (после `sync_1c --daily` и до
`distillation`). Делает агрегаты за вчера/неделю/месяц по ключевым срезам:
- Продажи top-20 SKU за месяц
- Закупки top-10 номенклатур
- Остатки критичных позиций (мука, сахар, упаковка)
- План/факт за неделю
- Топ-5 клиентов/поставщиков

Пишет в `km_facts` с `confidence=0.98`, `fact_type='1c_synthesis'`,
embedding через Qwen3. Тогда retrieval мгновенно находит синтезированные
ответы без SQL.

### Фаза 7 — Documentation

**7.1 Обновить CLAUDE.md**
- docker-compose путь (`/home/admin/knowledge-base/`, не корень)
- image (`pgvector/pgvector:pg15`)
- Очередь задач: удалить выполненные, добавить RAG v2
- Новые таблицы/views в разделе "Ключевые таблицы БД"

**7.2 RAG architecture doc**
`docs/rag_architecture.md` — схема компонентов (Router → Tools → Evaluator →
Answerer → km_fixation → feedback), список всех analytics_type, примеры
запросов → ожидаемого плана.

---

## Приоритет реализации

1. **Фаза 1.1 + 1.2** — upgrade Router модель + 20+ few-shot примеров
   (быстро, большой effect). **1-2 часа.**
2. **Фаза 1.4** — entities→filters в SQL-tools (клиент, поставщик, склад).
   **1 час.**
3. **Фаза 2.1 + 2.2** — evaluator + escalation Tier 1→2→3. **3-4 часа.**
4. **Фаза 3.1 + 3.2 + 3.3** — text-to-SQL безопасный runner + integration.
   **3-4 часа.**
5. **Фаза 4.2** — Conversation context через Reply. **2 часа.**
6. **Фаза 4.1** — фиксация ответов в km_facts. **1 час.**
7. **Фаза 6** — Periodic Synthesis 1С. **3 часа.**
8. **Фаза 5.2** — Дубли source_chunks. **1 час.**
9. **Фаза 7** — Документация. **1 час.**

Итого: ~18 часов работы. Делать поэтапно, каждый этап = отдельный commit,
тестировать на тех же 6 эталонных вопросах + расширенный набор (30+
вопросов из `tests/ab_compare_retrieval.py` v2).

## Acceptance criteria

Баталь 30 эталонных вопросов. Для каждого:
- Ответ содержит прямой факт (не "недостаточно данных")
- В источниках минимум 1 запись из 1С/mart (если вопрос количественный)
- Пользователь оценивает 👍/👎; KPI ≥ 90% 👍 на батале
- Средняя latency ≤ 15 сек (Tier 1), ≤ 45 сек (Tier 3)

## Метрики после внедрения

- `rag_query_log`: доля ответов с 1С-источниками (было ~30% на старом
  промпте, цель ≥ 80%)
- Доля "недостаточно данных" в ответах (цель ≤ 10%)
- Среднее число evidence-тезисов (цель ≥ 3)
- Cost / ответ (приемлемо до ~1 руб при Tier 1, до ~10 руб при Tier 3)
