# TASK: Дисциплина instruction-aware Qwen3 embeddings (Phase 2 readiness)

## Контекст

Серия Qwen3-Embedding инструкция-чувствительна: `prompt_name="query"`
добавляет к запросу префикс вида `Instruct: <task>\nQuery: <text>`,
а документы кодируются "как есть". Несогласованность инструкций
между индексацией и retrieval смещает геометрию пространства и
тихо рушит качество.

Сейчас (2026-04-16) document-side согласован (`backfill_embeddings_v2.py`
и `chunkers/embedder.py:embed_batch` используют один `Embedder` класс
с `prompt_name="document"`). Query-side для Qwen3 написан
(`chunkers/embedder.py:embed_query`), но **нигде в проде не вызывается** —
RAG (`rag_agent.py:search_knowledge`) до сих пор гонит запросы через
`embedding_service.create_query_embedding` (старая модель e5-base).

**Риск:** при переключении RAG на `source_chunks.embedding_v2`
(после окончания backfill, ETA ~2026-04-17 17:00) машинальный
`from embedding_service import create_query_embedding` затолкает
e5-вектор в Qwen3-индекс. Размерность совпадает (1024), БД примет,
поиск технически отработает — но качество рухнет до уровня шума.
Защиты типов нет.

## Цель

Сделать так, чтобы перепутать модели было физически сложно, и чтобы
любая будущая правка query-encoding triggered проверку согласованности.

## Реализация

### Шаг 1 — Жёсткая изоляция legacy e5

- Переименовать `embedding_service.py` → `embedding_service_e5.py`
- В новом файле в module docstring явно: "LEGACY e5-base. Используется
  ТОЛЬКО для km_facts/km_decisions/km_tasks/km_policies/embeddings (старый
  индекс). Для source_chunks.embedding_v2 НЕ применять — там Qwen3."
- Поправить все импорты по grep `from embedding_service import` /
  `import embedding_service` (rag_agent.py, bot.py, audit_pipeline.py,
  analyze_attachments.py, auth_bom.py, sync_1c_full.py, build_source_chunks.py,
  fill_nutrition.py, email_embeddings.py, nutrition_bot.py)
- Запустить тесты / smoke-проверку, что ничего не упало

### Шаг 2 — Единая точка входа для Qwen3 query-side

- В `chunkers/embedder.py` уже есть `Embedder.embed_query()` — это и есть
  каноническая функция. Добавить module-level helper:
  ```python
  def embed_query_v2(text: str) -> list[float]:
      """Каноническое query-encoding для source_chunks.embedding_v2.
      Использовать ВЕЗДЕ при поиске по embedding_v2."""
      return _shared_embedder().embed_query(text)
  ```
- В docstring предупреждение: "Никогда не вызывай через legacy
  embedding_service_e5 — там другая модель."

### Шаг 3 — Проверить, какие prompts реально живут в модели

Перед всем — однократный аудит:
```python
from sentence_transformers import SentenceTransformer
m = SentenceTransformer("Qwen/Qwen3-Embedding-0.6B")
print(m.prompts)        # dict {name: prompt_text}
print(m.default_prompt_name)
```
Зафиксировать в комментарии в `chunkers/embedder.py` точные тексты
prompts, чтобы при апгрейде модели легко было сравнить.

Если в модели нет ключа `"document"` — sentence-transformers молча
использует пустой prompt. Это корректно для Qwen3 (документы без
инструкции), но факт нужно явно задокументировать, иначе через год
никто не вспомнит почему.

### Шаг 4 — Контракт через COMMENT ON COLUMN

```sql
COMMENT ON COLUMN source_chunks.embedding IS
  'LEGACY: intfloat/multilingual-e5-base, prefix "passage: "/"query: ". Не использовать в новом коде.';
COMMENT ON COLUMN source_chunks.embedding_v2 IS
  'Qwen/Qwen3-Embedding-0.6B, prompt_name="document" для индекса, "query" для поиска. Use chunkers.embedder.embed_query_v2().';
```
Видно прямо в `\d source_chunks`.

### Шаг 5 — Smoke-тест `check_embedding_consistency.py`

Скрипт:
1. Берёт 5 фиксированных тестовых текстов (хардкодом, чтобы результат
   был воспроизводим).
2. Кодирует каждый как `prompt_name="document"` и `prompt_name="query"`.
3. Считает cosine self-similarity (doc↔query) для каждой пары.
4. Печатает таблицу + summary:
   - Если все близки к 1.0 (>0.99) → модель симметрична, инструкции не
     влияют → флаг `INSTRUCTION_AWARE=False`.
   - Если разница ≥0.05 → асимметрична → `INSTRUCTION_AWARE=True`,
     дисциплина критична.
5. Дополнительно: считает, что Qwen3-document × Qwen3-query одной и
   той же фразы дают cosine > 0.85 (sanity check, что они про одно и то же).
6. Возвращает exit code 0/1, чтобы можно было звать из CI.

Хранить в репо: `tests/check_embedding_consistency.py`. Запускать
вручную перед переключением RAG, и затем периодически через
`auto_agent_cron.py` (раз в неделю) — добавить как 6-й триггер
`embedding_consistency_drift`, если cosine упал → ESCALATE.

### Шаг 6 — Pre-switch checklist (создать как чек-лист в этом файле)

Перед переключением `rag_agent.py` на `source_chunks.embedding_v2`:
- [ ] Backfill 100% (`SELECT COUNT(*) FILTER (WHERE embedding_v2 IS NULL) FROM source_chunks` = 0)
- [ ] HNSW индекс на `embedding_v2` создан
- [ ] `tests/check_embedding_consistency.py` проходит, doc×query
      similarity > 0.85
- [x] Шаги 1-4 этой задачи реализованы и закоммичены (2026-04-17)
- [x] Grep по репо `create_query_embedding` показывает только legacy
      пути (km_*, embeddings table) — 2026-04-17 после переименования
      осталась только rag_agent.py:716 внутри `search_knowledge` для km_facts/decisions/tasks/policies
- [ ] A/B сравнение качества: 20 фиксированных вопросов прогнать
      через старый RAG (e5) и новый (qwen) — top-5 hits, ручная оценка
      relevance
- [ ] Откат-план: переключение через переменную в .env
      (`USE_EMBEDDING_V2=true`), чтобы можно было быстро вернуть e5

### Шаг 7 — Опционально: канонический task instruction

Sentence-transformers по умолчанию использует встроенный `query` prompt
модели. Если хотим контролировать его независимо от версии модели —
зафиксировать в `chunkers/config.py`:
```python
QUERY_INSTRUCTION = "Given a Russian business question about Frumelad confectionery company, retrieve the most relevant internal knowledge"
```
И в `embed_query` передавать `prompt=QUERY_INSTRUCTION` вместо
`prompt_name="query"`. Тогда при обновлении модели поведение query-side
не "поплывёт" из-за изменения встроенных prompts.

⚠️ Если делать — нужно ПЕРЕБИЛДИТЬ также document-side с тем же
явным prompt'ом (или с пустым) — иначе симметрия снова сломается.
Решение: сейчас НЕ трогать, оставить sentence-transformers default.
Зафиксировать как future work, если найдём дрифт качества.

## Когда делать

После окончания backfill (ETA ~2026-04-17 17:00), но **до** того, как
кто-нибудь (Claude или человек) тронет `rag_agent.py:search_knowledge`
для перевода на `source_chunks.embedding_v2`. Шаги 1-5 — обязательны,
6 — обязательный чек-лист, 7 — на потом.

## Acceptance criteria

- `grep -r "from embedding_service " ~/telegram_logger_bot/` не находит
  ни одного импорта старого имени (только `embedding_service_e5`)
- В БД оба `COMMENT ON COLUMN` видны через `\d+ source_chunks`
- `python tests/check_embedding_consistency.py` отрабатывает с exit 0
- Pre-switch checklist в этом файле — все галочки проставлены
