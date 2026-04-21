# TASK: Knowledge Graph — активация existing-данных через NetworkX

## Контекст

В БД уже лежит **скрытый граф**, который SQL-запросами эффективно не обработать:

| Источник | Размер | Что это |
|----------|--------|---------|
| `km_entities` | 13 153 узла | 7 703 SKU + 2 846 контрагентов + 1 289 сотрудников + 100 позиций + 91 процесс + 74 отдела + 59 складов + прочее |
| `km_relations` | 7 862 ребра | stored_at (3 593), supplies (1 316), works_in (1 114), holds_position (1 095), responsible_for (366), buys (225), collaborates_with (40), reports_to (40), complains_about (29), approves (23) |
| `bom_expanded` | 3 835 рёбер | DAG: готовый продукт → компоненты → сырьё |
| `tg_chat_*` + `matrix_invites` + `email_sync` | ~50k взаимодействий | Communication graph (не собран) |
| 1С cross: contractor → sku (supplies) + BOM + sku → client (sales) | — | Multi-hop reachability поставщик → клиент |

**Проблема:** `km_relations` и BOM сейчас — **мёртвый груз** для RAG. Ни один tool в `rag_agent.py` их не читает. Вопросы типа «кто отвечает за продукт X», «какие клиенты пострадают если поставщик Y задержит сырьё Z», «кто центральные коммуникаторы в проекте» — unanswerable, хотя данные есть.

**Почему SQL недостаточен:**
- Recursive CTE громоздки и медленны для multi-hop queries.
- Graph-алгоритмы (PageRank, centrality, community detection, shortest path) в SQL не выразимы.
- Графовые метрики полезны pre-computed: entity_centrality, community_id, degree — это колонки, которые RAG использует моментально.

**Почему не Neo4j:**
- Масштаб ~13k узлов и ~10k рёбер — NetworkX тянет в памяти за секунды.
- Neo4j = отдельный сервер + Cypher + синхронизация с pg — неделя работы при малой выгоде на текущем объёме.
- Когда граф вырастет до миллионов рёбер или понадобится hot-path графовые запросы на каждый RAG-ответ — вернёмся к Neo4j.

---

## Цели

1. **Разбудить существующие данные**: km_relations, BOM, 1С-связи превратить в graph-метрики + RAG-tools.
2. **Periodic recompute** (cron раз в 6 ч): строим NetworkX → считаем метрики → пишем обратно в pg как колонки.
3. **Новые RAG-tools**: `graph_neighbors`, `graph_shortest_path`, `bom_reachability`, `employee_responsibility`.
4. **Подготовка к Волне 5**: communication graph из tg_chat_* → community detection → кластеры сотрудников для поэтапной Matrix-миграции.

**НЕ делаем в этой задаче:**
- Установку Neo4j.
- Live in-memory граф в bot-процессе (только cron-обновление).
- Переписывание Router'а под KG_WALK — отдельная фаза после tools появятся.

---

## Фазы

### Фаза 1 — Graph Service MVP (1-2 дня)

**Цель:** крутящийся cron-скрипт, который строит NetworkX из `km_entities` + `km_relations`, считает базовые метрики, пишет в новые колонки `km_entities`.

**1.1 Миграция БД**
- `km_entities` + колонки:
  - `centrality` REAL — PageRank в общем графе km_relations
  - `degree_in` / `degree_out` INT — количество входящих/исходящих рёбер
  - `community_id` INT — cluster id (Louvain / Greedy modularity)
  - `kg_updated_at` TIMESTAMP — когда последний раз пересчитано
- Индекс на `community_id`.

**1.2 `tools/kg_graph.py`**
- Функция `rebuild_entity_graph()`:
  - Читает km_entities + km_relations → `nx.DiGraph`.
  - Рёбра взвешены по `weight` из km_relations (default 1.0).
  - Pre-filter: `status='active'`, `valid_to IS NULL OR valid_to >= today`.
  - Считает: `nx.pagerank`, `G.in_degree`, `G.out_degree`, community detection на undirected projection через `nx.algorithms.community.greedy_modularity_communities`.
  - UPDATE km_entities батчами по community.
- CLI-мод: `python3 -m tools.kg_graph rebuild` для ручного запуска.
- Логи: «Built graph: 13k nodes, 8k edges in 3s. Communities: 42. Centrality top-5: ...».

**1.3 Cron**
- `crontab -l` → добавить `0 */6 * * * cd /home/admin/telegram_logger_bot && python3 -m tools.kg_graph rebuild >> /var/log/kg_graph.log 2>&1`.

**Acceptance:**
- `SELECT count(*) FROM km_entities WHERE kg_updated_at IS NOT NULL` = 13k после первого прогона.
- Топ-5 entity по `centrality` — имена знакомые (ключевые сотрудники, основные SKU).
- `community_id` распределён (не всё в 1 кластере, не каждый узел сам себе).

---

### Фаза 2 — RAG Tools на графе (2-3 дня)

**Цель:** зарегистрировать в `tools/registry` 2-3 графовых инструмента, чтобы RAG-агент мог ими пользоваться через `invoke()` (и через native tool_use в Волне 4).

**2.1 `tools/kg_graph.py` — дополнить:**

- `graph_neighbors(entity_name: str, relation_types: list[str] = None, depth: int = 1, limit: int = 50) -> dict`
  - Находит entity по canonical_name (fuzzy) → возвращает соседей depth=1 или 2.
  - Фильтр по relation_types (опционально: `["supplies", "buys"]`).
  - Output: `{entity, neighbors: [{name, type, via_relation, distance, confidence}]}`.

- `graph_shortest_path(from_entity: str, to_entity: str, max_hops: int = 4) -> dict`
  - Путь в directed графе. Output: `{path: [entity1 → rel1 → entity2 → rel2 → ...], hops: N}`.
  - Для вопросов типа «как связаны клиент Магнит и поставщик Внуковское».

- `employee_responsibility(person_name: str) -> dict`
  - Ищет employee по имени → returns `{position, department, responsible_for: [SKU/processes], collaborates_with: [people], reports_to: person}`.
  - Собрано через многократные graph_neighbors с фильтром по relation_type.

**2.2 Регистрация в REGISTRY**
- Декоратор `@tool(name="graph_neighbors", domain="kg", ...)` в стиле attachments tools.
- Импорт в `tools/__init__.py`.

**Acceptance:**
- Ручной тест: `python3 -c "from tools import invoke; print(invoke('graph_neighbors', {'entity_name': 'Магнит', 'depth': 2}))"` возвращает SKU которые Магниту продаются + их компоненты.
- Три tools — green в smoke-тесте.

---

### Фаза 3 — BOM как полноценный граф (1-2 дня)

**Цель:** `bom_expanded` → BOM graph + reachability-tools.

**3.1 `tools/bom_graph.py`**
- `rebuild_bom_graph()` — строит DiGraph из `bom_expanded`.
- Pre-computed colums в `bom_expanded` / отдельной `bom_metrics`:
  - `depth_from_root` — расстояние от конечного продукта.
  - `reachable_finished_products` — для каждого сырья список готовых продуктов где используется.

**3.2 RAG-tools:**
- `bom_reachability(item_name: str, direction: "upward"|"downward") -> dict`
  - "upward" = из сырья → какие готовые продукты его используют (риск поставщика).
  - "downward" = из продукта → полная развёртка до сырья.
- `bom_affected_by_supplier_delay(supplier_name: str) -> dict`
  - Композит: `graph_neighbors(supplier)` → supplies → SKU → `bom_reachability(upward)` → готовые продукты.
  - Это тот самый вопрос: «что пострадает если поставщик Z задержит».

**Acceptance:**
- Ответ на вопрос «какая готовая продукция содержит сливки 30%» через RAG вернёт список SKU.

---

### Фаза 4 — Communication Graph (3-5 дней, пересекается с Волной 5)

**Цель:** построить граф взаимодействий сотрудников из tg_chat_* + matrix + email для использования в Matrix-миграции.

**4.1 `tools/comm_graph.py`**
- Периодический сбор рёбер:
  - tg-замены: `reply_to_message_id` → edge (A→B), `forward_from_user_id` → edge, co-participation в чате (weaker signal).
  - matrix: участники общих комнат.
  - email_sync: from/to пары.
- Weighting: частота + давность (decay).
- Сохранение в новую таблицу `comm_edges` (entity_a_id, entity_b_id, signal_type, weight, last_seen).

**4.2 Метрики:**
- Community detection (Louvain) → понять кластеры отделов/проектов.
- Betweenness centrality → кто «мосты» между кластерами (ключевые для миграции).
- Isolation score → кто редко взаимодействует.

**4.3 RAG-tools:**
- `comm_neighbors(person_name)` — с кем тесно общается.
- `comm_community(person_name)` — в каком кластере.
- `matrix_migration_wave(wave_number)` — возвращает следующую волну сотрудников для приглашения (на основе community + current matrix_user_mapping.is_joined).

**Acceptance:**
- Волна 5 (identification-agent + Matrix migration) использует `matrix_migration_wave()` для выбора кого приглашать следующим этапом.

---

### Фаза 5 (отложено) — Router integration

После фаз 1-3: добавить в `rag_agent.route_query` новый step-type `KG_WALK` и соответствующий dispatch в run_rag. Делать когда базовый tool-слой обкатан и есть стабильная метрика (full_rag_battery без ±10% дрейфа).

---

## Риски и открытые вопросы

1. **km_relations actuality**: как часто distillation.py сейчас обновляет relations? Нужен цикл: новые факты → новые relations → graph rebuild. Уточнить частоту.
2. **Naming mismatch**: km_entities canonical_name ≠ как люди пишут в чатах («Магнит» vs «АО Тандер»). Нужен alias resolver в `graph_neighbors` (уже есть поле `aliases` в km_entities — использовать).
3. **Communities stability**: Louvain не детерминирован. Либо seed + фиксированная версия, либо пост-обработка для стабильности id между прогонами.
4. **Embedding-based link prediction?** — идея на потом: находить рёбра которых нет в km_relations, но должны быть (по cosine distance embedding'ов). За рамками этого TASK.

---

## Что будет в конце

- **Новые колонки** в km_entities: `centrality`, `degree_in`, `degree_out`, `community_id`, `kg_updated_at`.
- **Новая таблица** `comm_edges` для Фазы 4.
- **5-6 новых RAG-tools** в registry (kg domain): graph_neighbors, graph_shortest_path, employee_responsibility, bom_reachability, bom_affected_by_supplier_delay, comm_neighbors.
- **Cron'ы**: rebuild_entity_graph (6ч), rebuild_bom_graph (сутки), rebuild_comm_graph (6ч).
- **km_relations и BOM — активированы**: RAG может отвечать на multi-hop вопросы за один tool-call вместо 3 SQL'ных.
