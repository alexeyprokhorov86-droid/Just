# TASK: Canonical layer для вложений и 1С-событий

Статус: в работе. Создан 2026-04-19, обновлён 2026-04-19 (после #1).

## Контекст

`source_documents` уже работает как canonical layer для текстов мессенджеров (email/telegram/matrix). Но:
- ✅ ~~Email-вложения с LLM-анализом — НЕ в canonical, RAG их не видит~~ — закрыто #1
- ✅ ~~TG-вложения частично попадали (был баг — теряли media_analysis или message_text)~~ — закрыто #4
- Matrix-медиа вообще не индексируется
- 1С — только 11 синтезированных агрегатов, нет точечных событий

## Сводка прогресса 2026-04-20
| Пункт | Статус | Детали |
|---|---|---|
| #4 TG body composition | ✅ | 838 docs обновлено + 2699 chunks пересозданы |
| #1 Email attachments | ✅ | 11758 canonical docs + chunks embedded |
| #5 analyze_tg_media | ✅ | 2 347 / 2 347 (100%) в Торты-Отгрузки. 33 pending без S3 в других чатах — отдельная задача миграции S3 |
| #2 c1_event | ✅ MVP (2026-04-20) | 3 категории: purchase_large, sale_large, payment_large. Backfill 180 дней + cron `*/30 * * * *` |
| #2 c1_event V2 | ⏳ план | +4 категории: dispatch_large, inventory_discrepancy, production_issue, staff_change |
| #6 Matrix media | ⏳ план | низкий |
| #3 v_messages_unified | ⏳ план | низкий |

## Найденные побочные баги (зафиксированы)
1. ✅ `build_source_chunks.py` использовал legacy e5 вместо Qwen3 v2 → INSERT в `embedding`, не `embedding_v2`. Все чанки с момента c403107 (17.04) были невидимы для RAG. Фикс в коммите 9dfa1c7.
2. ✅ `build_source_chunks.py` `generate_embeddings` вызывал `embed_document_v2(t)` в list-comprehension (sequential, 1.3 ch/s). Фикс на `model.embed_batch(texts)` → 35 ch/s, ~27× ускорение.
3. ✅ `build_source_chunks.py` падал в cron с `fe_sendauth: no password supplied` — добавлен `load_dotenv()`.
4. Открыто: 855 «осиротевших» TG-документов (source_documents указывают на удалённые tg_chat_* строки) — мини-аудит, чистить ли.
5. Открыто: 177 411 авто-нотификаций 1С («Уведомление о не выполненных задачах») сидят в `source_documents` без чанков (не нужны для RAG, но засоряют queue в build_source_chunks). **Стоит** добавить в скрипт фильтр `WHERE meta->>'skip_reason' IS NULL OR meta->>'skip_reason' != 'auto_notification'`.
6. Открыто: после email_reindex 17.04 у 220k email_message в source_documents нет чанков (большинство — те самые auto_notification из п.5). После фильтра реальная очередь будет ~35k.

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

## ✅ Пункт 1 — Email-вложения в canonical (СДЕЛАНО 2026-04-19, чанкинг идёт)

**Результат**: 11 758 из 11 760 проанализированных вложений в `source_documents` (kind='email_attachment'). 2 пропущено — слишком короткий body после composition.

### Что сделано
1. ✅ **Backfill heredoc-ом** — INSERT 11758 attachments с composition `[Анализ файла: {filename}]\n{analysis_text}\n\n---ПОЛНЫЙ ТЕКСТ---\n\n{content_text}`. Метаданные: parent_email_id, parent_source_doc_id, content_type, size_bytes, storage_path, analysis_model, extraction_method, has_extracted_text, has_llm_summary.
2. ✅ **`analyze_attachments.py`** — добавлена функция `insert_email_attachment_to_canonical(conn, att_id)`. Вызывается из `update_attachment_status` при `status='done'`. Идемпотентная (ON CONFLICT DO NOTHING). Новые вложения попадают в canonical автоматически.
3. ✅ **`rag_agent.search_unified`** — изменений НЕ потребовалось. `search_source_chunks` ищет по всем source_kind через embedding_v2, новые `email_attachment` чанки автоматически в выдаче.
4. ⏳ **Чанкинг** — идёт фоном, ~85 минут (35 ch/s × ~3 chunks/doc × 11758 = ~1000 секунд только для email_attachment, но в очереди всего ~60k docs).

### Незакрытое (V1.1)
- `meta.attachment_ids: [27068, 27073, 27078]` в parent email_message canonical doc — для двусторонней связи письмо↔вложения. UPDATE single batch скрипт.
- В RAG: если найдено письмо → проверить attachment_ids → подтянуть как сопутствующие. И наоборот: найдено вложение → поднять родительское письмо.
- `meta.sha256` для дедупа одного файла в нескольких письмах. Сейчас одинаковый PDF, отправленный в 3 письмах = 3 разных canonical-документа. Не блокирующее, но в будущем стоит дедупить в `search_unified` по хэшу.
- Не канонизированы: 1931 `empty`, 11266 `skip_junk`, 4 `error`, 27 `skip_later` (и подобное) — это правильно.

### Цель оригинальная
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

**Реальные цифры по 48 TG-чатам (2026-04-19)**:
- 11 847 сообщений всего
- 4 108 с медиа (35%)
- 1 699 уже с media_analysis (41% от медиа)
- **Backlog: 2 409 медиа без анализа**

Картина оптимистичнее чем казалось — большая часть медиа в активных чатах уже анализируется ботом в момент получения через `download_and_analyze_media` (bot.py:1359). Backlog — преимущественно старые сообщения (до включения этой логики) и Торты-Отгрузки (где per-photo выключен).

### Что нужно решить сначала (архитектурно)
- **Какой пайплайн стандартизировать?** Сейчас два:
  - `bot.py:download_and_analyze_media` — сразу при приёме сообщения (gpt-4.1, max_tokens=4500).
  - `analyze_attachments.py` — для email-вложений из БД, batch, S3.
- Логика анализа TG медиа уже есть, нужен только **batch processor** для backlog.

### План
1. **`analyze_tg_media_backlog.py`** — однократный скрипт по аналогии с analyze_attachments.
   - Запрос: `SELECT message_id, message_type, storage_path, media_file_id FROM {tg_chat_*} WHERE media_file_id IS NOT NULL AND media_analysis IS NULL`
   - Скачивание из S3 по storage_path (как в analyze_attachments).
   - Использовать ТЕ ЖЕ функции что в bot.py: `analyze_image_with_gpt`, `analyze_pdf_with_gpt` и т.д. (вынести в отдельный модуль `media_analyzer.py` чтобы не дублировать).
   - UPDATE tg_chat_* SET media_analysis = ..., content_text = ...
   - Триггер обновления canonical (через canonical_helper.insert_source_document_tg).
2. **Решение по «Торты Отгрузки»**: per-photo OCR → если есть текст (этикетка, накладная, маркировка) → LLM-анализ; иначе пропуск. Реализовать как опцию `--mode=ocr_filter`.
3. **Cron**: после обработки backlog уже не нужен, поток покрывается bot.py. Возможно периодический check для пропущенных (раз в сутки).

### Стоимость
- 2 409 backlog × full vision (gpt-4.1) ~ $40-80 разово.
- Если включить «Торты Отгрузки» per-photo (~2200 фото) с OCR-фильтром: +$20-40.
- Поток: уже покрывается bot.py, доп. cost = 0.

### Что оставить как есть
- bot.py:download_and_analyze_media — рабочий механизм, не трогаем.
- Сводки за день в Торты-Отгрузки — оставить (они дополняют per-photo).

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
2. ✅ **#1 Email attachments** — сделано 2026-04-19
3. ✅ **#5 analyze_tg_media** — сделано 2026-04-19/20 (2347/2347 в Торты-Отгрузки)
4. ✅ **#2 c1_event MVP** — сделано 2026-04-20 (3 категории, backfill 180d + cron)
5. **#2 c1_event V2** — +4 категории: dispatch_large, inventory_discrepancy, production_issue, staff_change
6. **#6 Matrix media** — низкий (пока не основной канал)
7. **#3 v_messages_unified** — низкий (косметика для Metabase)

### План V2 c1_event (дополнительные категории)
| event_type | Источник | Порог | Примерный объём/год |
|---|---|---|---|
| `dispatch_large` | c1_dispatch_orders + c1_dispatch_order_items | amount > 200k | ~500-800 |
| `inventory_discrepancy` | c1_inventory_count + c1_inventory_count_items | любая ненулевая разница | ~50-100 |
| `production_issue` | c1_shortage + c1_shortage_items | любая запись | ~30-70 |
| `staff_change` | c1_staff_history | event_type='Перемещение' | ~20-40 |

**Реализация**: добавить 4 функции `fetch_*_events()` в `canonize_1c_events.py`, расширить choice=`--category`. Backfill ~1000 доков, embed ~15 мин.

Детали по таблицам (проверено 2026-04-20):
- `dispatch_large`: **решено JOIN**. `c1_dispatch_order_items` → `c1_customer_order_items` по `(customer_order_key, nomenclature_key)` (line_number между документами не согласован). Сумма dispatch = `SUM(price × dispatch_quantity)` где price берётся из customer_order_items по той же номенклатуре. Формула:
  ```sql
  SELECT doi.order_key AS dispatch_ref,
         SUM(COALESCE(coi.price,0) * doi.quantity) AS dispatch_sum
  FROM c1_dispatch_order_items doi
  LEFT JOIN c1_customer_order_items coi
    ON coi.doc_key = doi.customer_order_key
   AND coi.nomenclature_key = doi.nomenclature_key
  GROUP BY doi.order_key
  HAVING SUM(COALESCE(coi.price,0) * doi.quantity) >= 200000;
  ```
  **Проверено 2026-04-20 на 6-мес данных**: 2755 dispatches, 361 >200k. НО avg_match_ratio=0.58 — **42% строк не матчатся** с customer_order (прямые отгрузки без заказа, перемещения, регрейд). Нужен fallback: average price из mart_sales по той же nomenclature в ±30 дней от doc_date. Или через поле customer_order_key='' → отдельная ветка. Решить при реализации V2.
  Колонка в `c1_customer_order_items`: `order_key` (не `doc_key`).
- `inventory_discrepancy`: JOIN `c1_inventory_count` + `c1_inventory_count_items` с `WHERE deviation <> 0`. В body: список SKU и отклонений.
- `production_issue`: `c1_shortage` + `c1_shortage_items`, `sum_total` доступен. Аналогично purchase_large по структуре.
- `staff_change`: `c1_staff_history WHERE event_type='Перемещение' AND active=true`. JOIN на c1_employees/c1_positions/c1_departments для human body.

## ✅ После завершения backfill_embeddings_v2 (сделано 2026-04-19 19:00)

1. ✅ CREATE INDEX CONCURRENTLY idx_sc_embedding_v2 (HNSW, 2GB)
2. ✅ Раскомментирован cron build_source_chunks (`45 * * * *`)
3. ✅ Re-index 838 TG-документов из #4 (2699 chunks удалены, регенерированы через Qwen3 v2)

---

## Followup mini-задачи (после основных #5, #2)

### A. 855 «осиротевших» TG canonical-документов
- `source_documents` ссылаются на `tg_chat_*.message_id`, которого больше нет (старые удалённые сообщения).
- Решить: оставить как archive, пометить `is_deleted=true`, или удалить.
- Скрипт-проверка: `SELECT id, channel_ref, source_ref FROM source_documents WHERE source_kind='telegram_message' AND meta->>'has_media'='true' AND id NOT IN (...select where parent exists...)`.
- 30 минут работы.

### ✅ B. Фильтр auto_notification в build_source_chunks (СДЕЛАНО 2026-04-19)
- 185 393 авто-нотификаций 1С висели в queue, чанкер тратил время на их выгребание (батч 500 → 0 чанков → exit).
- Добавлен фильтр в `get_unprocessed_docs`: `AND (sd.meta->>'skip_reason' IS NULL OR sd.meta->>'skip_reason' != 'auto_notification')`.
- Они остаются в canonical (для архивных SQL-запросов), но в RAG векторный поиск не попадают.
- Бонус-фикс: `MIN_DOC_LEN=25 → 30` (=MIN_CHUNK_LEN), иначе 904 коротких docs (25-29 символов) вечно висели в queue без надежды попасть в чанки.

### C. Двусторонняя связь email ↔ attachments
- В `source_documents` (email_message) добавить `meta.attachment_ids: [27068, 27073]` — список FK на email_attachment-документы.
- Скрипт: `UPDATE source_documents sd SET meta = meta || jsonb_build_object('attachment_ids', (SELECT array_agg(att.id) FROM source_documents att WHERE att.source_kind='email_attachment' AND (att.meta->>'parent_source_doc_id')::int = sd.id))`.
- Бонус в RAG: `search_unified` находит письмо → подтягивает все вложения как контекст; и наоборот. Реализовать в `rag_agent` post-processing.
- 1 час работы.

### D. SHA256 дедуп для одинаковых файлов в разных письмах
- Один PDF, отправленный 3 раза = 3 канонических документа с одинаковым content_text.
- Добавить `meta.sha256` при insert (или пост-фактум backfill через BashFile.sha256(storage_path content)).
- В `search_unified` группировать выдачу по sha256, оставляя самый ранний.
- 2 часа.

### E. XLSX чанкинг по строкам (для V2)
- Для email_attachment с content_type=excel: текущий чанкинг режет CSV по символам, рвутся строки. Лучше: header + N rows = 1 chunk.
- Не блокирующее, но улучшит RAG для накладных/спецификаций.
- 3 часа.

### F. Привязка `analyze_attachments.py` к S3
- Сейчас работает — в analyze_attachments.py через S3 client. Но нужен мониторинг: сколько `done` vs `pending` за сутки.
- Добавить в `daily_report.py` строчку «вложения проанализировано: X done, Y pending».
- 30 минут.

### G. Coverage метрика «вложений в RAG»
- Дашборд в Metabase: % email_message где `meta.attachment_ids` есть, и сколько из них реально в RAG.
- 1 час.
