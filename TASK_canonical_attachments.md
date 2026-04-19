# TASK: Canonical layer для вложений и 1С-событий

Статус: в работе. Создан 2026-04-19.

## Контекст

`source_documents` уже работает как canonical layer для текстов мессенджеров (email/telegram/matrix). Но:
- Email-вложения с LLM-анализом — НЕ в canonical, RAG их не видит
- TG-вложения частично попадали (был баг — теряли media_analysis или message_text)
- Matrix-медиа вообще не индексируется
- 1С — только 11 синтезированных агрегатов, нет точечных событий

## ✅ Пункт 4 — Фикс composition body_text для TG (СДЕЛАНО 2026-04-19)

**Проблема**: `canonical_helper.insert_source_document_tg` брал `content_text` ИЛИ `message_text+media_analysis`, не оба. Терял либо подпись пользователя, либо ценный LLM-анализ.

**Фикс**: всегда композит из 3 частей:
```
{message_text}

[Анализ вложения]
{media_analysis}

[Содержимое файла]
{content_text}
```

**Backfill результат**: 1693 TG-документов с медиа → 838 обновлено, 855 осиротевших (исходные tg_chat_* записи удалены), 0 ошибок. Запущен heredoc-ом без отдельного .py.

**Незавершённое после фикса #4**:
- ❗ Re-index: удалить старые `source_chunks WHERE document_id IN (838 updated_ids)` и пересобрать через `build_source_chunks.py`. Сделать ПОСЛЕ завершения backfill_embeddings_v2 (чтобы не наложиться).
- 855 осиротевших — отдельный аудит, чистить ли их или оставить как archive.

---

## Пункт 1 — Email-вложения в canonical (V1)

**Цель**: 11 760 проанализированных email-вложений (`email_attachments WHERE analysis_status='done'`) попадают в `source_documents` как `source_kind='email_attachment'`. RAG их видит наравне с письмами.

### Схема
```sql
INSERT INTO source_documents (
    source_kind='email_attachment',
    source_ref='email_att:' || a.id,           -- "email_att:27078"
    title=a.filename,
    body_text = a.analysis_text || E'\n\n---\n\n' || a.content_text,
                                                -- LLM summary первым (для embedding)
                                                -- + полный extract (для цитирования)
    doc_date=em.received_at,
    author_name=em.from_address,
    author_ref=em.from_address,
    channel_ref=mb.email_address,
    channel_name=mb.email_address,
    meta=jsonb_build_object(
        'parent_email_id', em.id,
        'parent_source_doc_id', sd.id,           -- FK на письмо-носитель в source_documents
        'content_type', a.content_type,
        'size_bytes', a.size_bytes,
        'storage_path', a.storage_path,           -- S3 ключ
        'analysis_model', a.analysis_model,
        'extraction_method', '...',               -- vision / pdf_text / extract
        'has_extracted_text', length(a.content_text) > 0,
        'has_llm_summary', length(a.analysis_text) > 0,
        'sha256', '...'                           -- хэш файла для дедупа
    )
);
```

### Правило body_text (КРИТИЧНО)
`analysis_text + "\n\n---\n\n" + content_text`. Summary ПЕРЕД сырым текстом — для качества embedding.
- Если `content_text` пуст (картинка) → только summary.
- Если `analysis_text` пуст → только текст.

### Чанкинг
- Стандартный (CHUNK_SIZE=500, OVERLAP=100).
- ПЕРВЫЙ чанк = полный analysis_text (даже если короче 500), даёт семантический якорь.
- XLSX по строкам — V2.

### Дедуп
- `UNIQUE(source_kind, source_ref)` уже защищает от двойной вставки одного email_att:N.
- Один файл в нескольких письмах: добавить `meta.sha256`, GIN-индекс `idx_sd_meta_sha256`. В RAG `search_unified` дедупим по хэшу.

### Связь «письмо ↔ вложения»
- В `meta` письма (`email_message`) добавить `attachment_ids: [27068, 27073, 27078]` — FK на вложения.
- В RAG: если найдено письмо → проверить attachment_ids → подтянуть как сопутствующие.
- Если найдено вложение → поднять родительское письмо для контекста.

### План внедрения
1. `backfill_email_attachments_to_canonical.py` — однократный, idempotent. ~5 мин на 12k записей.
2. `build_source_chunks.py` — добавить chunker для `email_attachment` (с правилом «первый чанк = analysis_text»).
3. `analyze_attachments.py` — после успешного анализа сразу INSERT в source_documents.
4. `rag_agent.search_unified` — добавить `email_attachment` в фильтр.
5. Backfill embedding v2 — догнать после основного.

### Что НЕ делать
- НЕ канонизировать `skip_junk` (11 266 спам/нотификации).
- НЕ канонизировать `empty` (1 931 пустых).
- НЕ удалять `email_attachments` — нужна для статуса/аудита/raw S3.

### Стоимость
- ~12k canonical-документов (4% к текущим 293k).
- ~30-50k чанков (~10-15% прирост).
- Embedding v2 backfill: 6-10 часов на новые чанки (можно догнать после основного).

---

## Пункт 5 — analyze_tg_media (LLM-анализ медиа в Telegram)

**Цель**: 95% TG-медиа сейчас БЕЗ анализа. На примере чата «Торты Отгрузки»: 2340 медиа, 101 с media_analysis (это сводки за день, не per-photo).

### Гибридная стратегия
1. Бесплатный OCR (PaddleOCR / Tesseract) на каждое фото.
2. Если есть текст (накладная, ТТН, маркировка, рукопись) → LLM-анализ через `gpt-4.1-mini` (дешевле full).
3. Если текста нет (чистый кузов, абстракт) → пропуск.
4. Per-photo media_analysis сохранять в `tg_chat_*.media_analysis` (та же колонка).

### Cron
- `analyze_tg_media.py --batch 100` каждый час.
- Обрабатывает `WHERE media_file_id IS NOT NULL AND media_analysis IS NULL`.

### Стоимость (расчёт для 1 чата 2340 фото)
- Full vision на всё: $200-400 backfill + ~$1-2/день поток.
- Гибрид (OCR-фильтр + mini): $50-80 backfill + ~$0.30/день поток. Покрытие важного контента ~80%.

### Особенно для «Торты Отгрузки»
Ежедневная сводка остаётся как есть (она дополняет, не заменяет). Per-photo нужен для точечных вопросов типа «когда последний раз отгружали Магниту?», «сколько коробок Наполеона ушло вчера?».

---

## Пункт 6 — Matrix media в canonical

**Цель**: сейчас `matrix_listener.py` ловит только `content.body` (текст). Файлы/картинки игнорируются.

### План
- В `matrix_listener.py` обрабатывать event_type `m.image`, `m.file`, `m.video`.
- Качать через media URL Synapse, гнать через тот же analyze_attachments-пайплайн.
- Сохранять как `source_kind='matrix_attachment'` (отдельный тип).

### Приоритет
Низкий — Matrix пока не основной канал. После #1, #4, #5.

---

## Пункт 2 — c1_event (селективная канонизация 1С-событий)

**Не про вложения, отдельная задача.** Перенести в `TASK_c1_canonical_events.md` или объединить.

### Идея
Не вся 1С (27k purchases, 30k orders, 50k dispatches), а только значимые события:
- Крупные платежи (>500к), продажи/отгрузки (>200к), закупки (>300к)
- Возвраты, недостачи, inventory_count расхождения
- Производственные shortage
- Новый клиент / новый поставщик
- Кадровые перемещения

### Схема `source_kind='c1_event'`
```sql
title='Закупка от {partner} на {amount} ₽'
body_text='Документ закупки № {doc_number} от {doc_date}.
Поставщик: {name}.
Сумма: {amount} ₽.
Склад: {name}.
Организация: {org}.
Позиции: {nomenclature_list}.
Комментарий: {comment}.'
meta={ event_type, doc_kind, ref_key (FK), amount, partner_key, ..., is_significant, significance_reason }
```

### Категории V1
| event_type | Источник | Порог |
|---|---|---|
| `purchase_large` | c1_purchases | amount > 300k ИЛИ новый поставщик |
| `sale_large` | mart_sales | amount > 200k ИЛИ новый клиент |
| `dispatch_large` | c1_dispatch_orders | amount > 200k |
| `payment_large` | c1_bank_expenses | amount > 500k |
| `inventory_discrepancy` | c1_inventory_count | любая ненулевая разница |
| `production_issue` | c1_shortage | любая запись |
| `staff_change` | c1_staff_history | event_type='Перемещение' |

### Чем отличается от `synthesized_1c` (11 шт)
| | `synthesized_1c` | `c1_event` |
|---|---|---|
| Что | Агрегат | Конкретный документ |
| Источник | Cron daily | Cron 30 мин (после --hourly) |
| Объём | Десятки | Тысячи |
| Назначение | Широкие вопросы | Точечные про конкретные сделки |

### План
1. `canonize_1c_events.py` — incremental по `updated_at > last_run`.
2. Cron каждые 30 мин.
3. Backfill за 6 мес — однократный, ~5-10k событий.
4. `build_source_chunks.py` — добавить `c1_event` (короткий текст, обычно 1 чанк).
5. `rag_agent.search_unified` — `c1_event` как ещё один источник.
6. Router few-shot: вопросы про конкретные сделки/контрагентов → `search_unified`.

### Чем НЕ заменяет SQL-аналитику
- Агрегации/тренды/группировки — это `search_1c_analytics`.
- `c1_event` для точечных запросов.

### Стоимость
- ~5-10k canonical-доков за 6 мес backfill.
- ~7k embeddings × 0.7 сек = 1-2 часа разово.
- Cron инкремент: ~50-100 событий/день — копейки.

---

## Пункт 3 — `v_messages_unified` VIEW (low priority удобство)

```sql
CREATE VIEW v_messages_unified AS
SELECT
    id, source_kind, doc_date, author_name, channel_name,
    title AS subject, body_text,
    meta->>'direction' AS direction,
    meta->>'folder' AS email_folder,
    (meta->>'has_attachments')::boolean AS has_attachments,
    (meta->>'has_media')::boolean AS has_media,
    meta->>'thread_id' AS thread_id
FROM source_documents
WHERE source_kind IN ('email_message','telegram_message','matrix_message');
```

Польза: Metabase дашборды без знания jsonb. Низкий приоритет.

---

## Приоритет внедрения

1. ✅ **#4 Fix TG body_text composition** — сделано 2026-04-19
2. **#1 Email attachments** — высокий (часто содержание накладной/счёта важнее тела письма)
3. **#5 analyze_tg_media** — средне-высокий (включая «Торты Отгрузки» в гибридном режиме)
4. **#2 c1_event** — средний (улучшает точечные 1С-вопросы)
5. **#6 Matrix media** — низкий (пока не основной канал)
6. **#3 v_messages_unified** — низкий (косметика для Metabase)

## После завершения backfill_embeddings_v2 (~17:00-19:00 19.04)

Обязательно перед #1, #5, #6:
1. CREATE INDEX CONCURRENTLY idx_sc_embedding_v2 ON source_chunks USING hnsw (embedding_v2 vector_cosine_ops) WITH (m=16, ef_construction=64);
2. Раскомментировать cron build_source_chunks.
3. Re-index 838 TG-документов из #4 (удалить старые chunks, перегенерить).
