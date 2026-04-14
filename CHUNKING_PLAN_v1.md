# План перестройки чанкинга RAG — v1.0
# Дата: 14 апреля 2026

---

## 1. Архитектура: типы чанков

Все чанки хранятся в таблице `source_chunks` с новыми полями:

```sql
ALTER TABLE source_chunks ADD COLUMN IF NOT EXISTS chunk_type VARCHAR(20);
-- Значения: envelope | body | structured | distilled

ALTER TABLE source_chunks ADD COLUMN IF NOT EXISTS source_kind VARCHAR(30);
-- Значения: email | email_attachment | telegram | matrix |
--           1c_customer_order | 1c_supplier_order | 1c_sales |
--           1c_purchases | 1c_bank_expense | 1c_staff |
--           1c_specification | 1c_bom | 1c_stock |
--           1c_report_daily | 1c_report_weekly | 1c_report_monthly |
--           km_fact | km_decision | km_task | km_policy

ALTER TABLE source_chunks ADD COLUMN IF NOT EXISTS parent_document_id BIGINT;
-- Связь с source_documents (для email, telegram, matrix)

ALTER TABLE source_chunks ADD COLUMN IF NOT EXISTS parent_1c_ref VARCHAR(50);
-- Связь с 1С документом (Ref_Key)

ALTER TABLE source_chunks ADD COLUMN IF NOT EXISTS chunk_date DATE;
-- Дата документа/сообщения (для time-weighted retrieval)

ALTER TABLE source_chunks ADD COLUMN IF NOT EXISTS confidence FLOAT DEFAULT 0.5;
-- Уровень доверия: 1c=0.95, email_envelope=0.8, km_fact=0.7, messenger_raw=0.4
```

---

## 2. Embedding модель

**Модель**: Qwen3-Embedding-0.6B
- Контекст: 8192 токенов
- Размерность: 1024
- Мультиязычная (русский + английский)

**Целевой размер чанка**: 100-400 токенов (оптимум для retrieval)
- envelope: 50-100 токенов
- body: 200-400 токенов
- structured: 100-300 токенов
- distilled: 50-200 токенов (km_facts обычно короткие)

**Действие**: переиндексация ВСЕХ чанков после установки модели.
Старые embeddings (multilingual-e5-base, 768 dim) → удалить после миграции.

---

## 3. Этап 1 — Email (248k писем)

### 3.1. Envelope-чанки

**Источник**: таблица `source_documents` WHERE source_kind='email'

**Шаблон генерации** (без LLM, чистый SQL/Python):
```
{date} | От: {from_name} ({from_email}) → Кому: {to} | Тема: {subject}
Суть: {первые 2-3 предложения body или subject если body пустой}
Цепочка: {thread_id если есть} | Вложения: {количество и типы}
```

**Поля в source_chunks**:
- chunk_type = 'envelope'
- source_kind = 'email'
- parent_document_id = id из source_documents
- chunk_date = дата письма
- confidence = 0.8

**Объём**: ~248k чанков (один на письмо)
**Сложность**: низкая — шаблонная генерация
**Время**: 2-3 часа разработка + 1-2 часа генерация

### 3.2. Body-чанки

**Логика разбивки**:
1. Тело письма < 300 токенов → один body-чанк целиком
2. Тело письма 300-800 токенов → split по абзацам (двойной перенос строки), каждый абзац — чанк. Если абзац < 50 токенов — склеиваем со следующим.
3. Тело письма > 800 токенов → split по абзацам + overlap 50 токенов между соседними чанками.
4. Каждый body-чанк получает prefix с метаданными: "[Email от {from} к {to}, {date}, тема: {subject}]" — чтобы чанк был самодостаточным.

**Цитируемый текст**: строки начинающиеся с ">" — отделять. Можно либо убрать (чтобы не дублировать контент из оригинального письма), либо оставить как отдельный чанк с пометкой "цитата из предыдущего письма".

**Рекомендация**: убирать цитаты из body-чанков. Они уже существуют как отдельные письма в БД.

**Поля в source_chunks**:
- chunk_type = 'body'
- source_kind = 'email'
- parent_document_id = id
- chunk_date = дата письма
- confidence = 0.7

**Объём**: ~300-400k чанков (некоторые письма дадут несколько)
**Сложность**: средняя
**Время**: 3-4 часа разработка + 3-4 часа генерация

### 3.3. Оценка качества (после генерации email-чанков)

1. Составить 30 реальных вопросов, которые задавали боту по email
2. Для каждого вопроса вручную определить "золотой" ответ (какое письмо должно найтись)
3. Прогнать retrieval: вопрос → embedding → top-10 чанков
4. Метрика: Recall@5 и Recall@10 (нашёлся ли правильный чанк в топ-5/10?)
5. Если Recall@5 < 70% — анализировать промахи и точечно улучшать

---

## 4. Этап 2 — 1С Structured Chunks

### 4.1. Заказы клиентов (c1_customer_orders)

**Шаблон**:
```
Заказ клиента №{number} от {date}
Клиент: {customer_name}
Статус: {status}
Товары:
- {product_1}: {qty} шт × {price}₽ = {sum}₽
- {product_2}: {qty} шт × {price}₽ = {sum}₽
Итого: {total}₽
Склад: {warehouse}
Связанные отгрузки: {dispatch_numbers или "нет"}
```

**Поля**: chunk_type='structured', source_kind='1c_customer_order', confidence=0.95
**Обновление**: при каждом sync_1c_full.py — пересоздать чанки для изменённых документов

### 4.2. Заказы поставщикам (c1_supplier_orders)

**Шаблон** — аналогичный заказу клиента, но:
```
Заказ поставщику №{number} от {date}
Поставщик: {supplier_name}
Статус: {status}
Товары: ...
Итого: {total}₽
Связанные приходы: {purchase_numbers или "нет"}
```

### 4.3. Реализация товаров и услуг (c1_sales)

**Шаблон**:
```
Реализация №{number} от {date}
Клиент: {customer_name}
Организация: {organization} (Фрумелад/НФ)
Товары:
- {product}: {qty} шт, {sum}₽
Итого: {total}₽
Основание: заказ клиента №{order_number}
```

### 4.4. Приобретение товаров и услуг (c1_purchases)

**Шаблон** — зеркальный реализации.

### 4.5. Банковские расходы — агрегированные чанки

**Группировка**: по контрагенту × неделя

**Шаблон**:
```
Банковские расходы: {counterparty_name}
Период: {week_start} — {week_end}
Организация: {org}
Операции:
- {date_1}: {sum_1}₽ — {purpose_1} (ДДС: {dds_category})
- {date_2}: {sum_2}₽ — {purpose_2} (ДДС: {dds_category})
Итого за неделю: {total}₽
```

**Альтернатива**: если контрагентов слишком много и чанки мелкие — группировать по категории ДДС × неделя.

### 4.6. Кадровые документы (c1_staff_history)

**Один чанк на событие**:
```
Кадровое событие: {event_type} (приём/увольнение/перевод/изменение оклада)
Сотрудник: {full_name}
Дата: {date}
Подразделение: {department} {→ new_department если перевод}
Должность: {position} {→ new_position если перевод}
Оклад: {salary}₽ {→ new_salary если изменение}
```

**confidence = 0.95**

### 4.7. Спецификации (c1_specifications + c1_spec_materials)

**Два уровня чанков**:

**Уровень 1 — спецификация как рецептура**:
```
Спецификация: {product_name}
Выход: {output_qty} {unit}
Состав:
- {material_1}: {qty_1} {unit_1}
- {material_2}: {qty_2} {unit_2}
...
Категория: {product_type}
```

**Уровень 2 — BOM-раскрутка** (из bom_expanded):
```
BOM-раскрутка: {product_name}
Уровень раскрутки: полный (до сырья)
Сырьё:
- {raw_material_1}: {total_qty_1} {unit_1} (через: {path})
- {raw_material_2}: {total_qty_2} {unit_2} (через: {path})
Расчёт ID: {calculation_id}, версия: {version}
```

### 4.8. Складские остатки (c1_stock_balances)

**Группировка**: по дню × категория (ГП / сырьё / упаковка)

**Шаблон**:
```
Складские остатки на {date}
Категория: {category} (готовая продукция / сырьё / упаковка)
Склад: {warehouse}
Топ позиции:
- {product_1}: {qty_1} {unit}
- {product_2}: {qty_2} {unit}
...
Всего позиций: {count}
```

**Важно**: хранить не все позиции (их может быть 7000+), а топ-50 по остатку + агрегаты. Полный список доступен через прямой SQL-запрос RAG-агента.

### 4.9. Периодические отчёты (агрегированные)

**Ежедневный чанк**:
```
Сводка за {date}:
Продажи: {total_sales}₽ ({count_orders} заказов)
Отгрузки: {total_dispatches}₽
Производство: {total_production} ({count_items} позиций)
Топ-3 клиента: {client_1}, {client_2}, {client_3}
```

**Еженедельный чанк**: аналогично, плюс сравнение с предыдущей неделей и планом.

**Ежемесячный чанк**: аналогично, плюс накопленный итог с начала года.

**Генерация**: cron после sync_1c_full.py, формируем чанки из mart_sales и других mart_* views.

---

## 5. Этап 3 — Мессенджеры

### 5.1. km_facts / km_decisions / km_tasks / km_policies как чанки

**Каждая запись km_* → один чанк**.

**Формат**: текст факта/решения/задачи + метаданные:
```
[{type}: {source_chat}, {date}, автор: {author}]
{text факта/решения/задачи}
```

**Поля**: chunk_type='distilled', source_kind='km_fact'/'km_decision'/..., confidence=0.7

**Объём**: ~42k + 6.3k + 8k + 2.3k = ~59k чанков
**Сложность**: низкая — данные уже структурированы
**Время**: 1-2 часа

### 5.2. Сырые сообщения — диалоговые блоки (fallback)

**Группировка**:
1. Взять все сообщения одного чата, отсортировать по времени
2. Группировать по "сессиям": перерыв > 30 минут = новая сессия
3. Внутри сессии: если > 400 токенов — разбить по авторским блокам (все сообщения одного автора подряд = один подблок)
4. Если подблок < 50 токенов — склеить со следующим

**Формат чанка**:
```
[Чат: {chat_name} | {date} {time_start}-{time_end} | Участники: {authors}]
{author_1}: {message_1}
{author_1}: {message_2}
{author_2}: {message_3}
...
```

**Поля**: chunk_type='body', source_kind='telegram'/'matrix', confidence=0.4
**Объём**: ~1-2k чанков из 7871 сообщений (группировка сильно сжимает)
**Приоритет**: НИЗКИЙ — делаем после km_facts, только если retrieval по km_facts недостаточен

---

## 6. Этап 4 — Вложения

### 6.1. AI-анализ вложения как чанк

Из `source_documents` WHERE source_kind LIKE '%attachment%':
```
[Вложение: {filename}, тип: {mime_type}]
[Из email: от {from} к {to}, {date}, тема: {subject}]
Анализ: {ai_analysis_text}
```

**chunk_type = 'envelope'**, source_kind = 'email_attachment', confidence = 0.6

### 6.2. Содержимое вложения

**PDF**: извлечь текст → разбить по страницам → каждая страница с overlap = чанк.
**Excel/CSV**: группы строк с заголовками.
**Изображения**: только AI-анализ (6.1).

**Приоритет**: после этапов 1-3. Объём вложений в S3 нужно уточнить.

---

## 7. Reranker

**Модель**: Qwen3-Reranker-0.6B

**Как работает в pipeline**:
1. Вопрос → embedding → top-50 чанков из pgvector (быстрый поиск)
2. top-50 → reranker → переранжирование по релевантности → top-10
3. top-10 → в контекст LLM для генерации ответа

**Зачем**: embedding-поиск находит "похожие", а reranker находит "релевантные". Разница критична — embedding может поставить высоко чанк с похожими словами но другим смыслом.

**Дополнительные фильтры перед reranker**:
- confidence weighting: чанки с confidence=0.95 (1С) получают буст
- time decay: свежие чанки получают буст (настраиваемый)
- source diversity: в top-10 должны быть чанки из разных источников

---

## 8. Pipeline генерации чанков

### 8.1. Скрипт: build_chunks_v2.py

**Архитектура**:
```
build_chunks_v2.py
├── chunkers/
│   ├── email_chunker.py      — envelope + body для email
│   ├── onec_chunker.py       — structured чанки из 1С таблиц
│   ├── messenger_chunker.py  — диалоговые блоки
│   ├── km_chunker.py         — km_facts/decisions/tasks/policies
│   ├── attachment_chunker.py — вложения
│   └── base_chunker.py       — базовый класс с общей логикой
├── embedder.py               — Qwen3-Embedding-0.6B
├── config.py                 — параметры (размеры, overlap, confidence)
└── main.py                   — оркестратор
```

### 8.2. Режимы работы

- `--full` — полная пересборка всех чанков (первый запуск, смена модели)
- `--incremental` — только новые/изменённые документы (для cron)
- `--source email` — только email-чанки
- `--source 1c` — только 1С-чанки
- `--dry-run` — показать что будет сделано, не записывать

### 8.3. Инкрементальное обновление

Для cron (каждые 30 мин после sync_1c_full.py):
1. Проверить `source_documents` с `created_at > last_chunk_run`
2. Проверить 1С таблицы с `updated_at > last_chunk_run`
3. Сгенерировать чанки только для новых/изменённых
4. Записать embedding
5. Обновить `last_chunk_run` timestamp

---

## 9. Порядок реализации

```
Этап 0: Подготовка                              1 день
├── ALTER TABLE source_chunks (новые поля)
├── Установить Qwen3-Embedding-0.6B
├── Создать структуру build_chunks_v2.py
└── Базовый embedder.py

Этап 1: Email чанки                             2-3 дня
├── email_chunker.py (envelope + body)
├── Генерация envelope для всех 248k
├── Генерация body (с разбивкой по абзацам)
├── Embedding всех email-чанков
└── Тестирование: 30 вопросов, Recall@5

Этап 2: 1С structured чанки                     3-4 дня
├── onec_chunker.py
├── Шаблоны для каждого типа документа
├── Генерация чанков из c1_* таблиц
├── Периодические отчёты (daily/weekly/monthly)
├── Embedding
└── Тестирование

Этап 3: km_* как чанки                          1 день
├── km_chunker.py
├── Генерация из km_facts/decisions/tasks/policies
├── Embedding
└── Тестирование

Этап 4: Мессенджеры (сырые)                     1-2 дня
├── messenger_chunker.py
├── Группировка по сессиям
├── Embedding
└── Тестирование — нужны ли они поверх km_facts?

Этап 5: Вложения                                2-3 дня
├── attachment_chunker.py
├── PDF текст extraction + chunking
├── Excel/CSV structured chunks
├── Embedding
└── Тестирование

Этап 6: Reranker                                1-2 дня
├── Установить Qwen3-Reranker-0.6B
├── Встроить в rag_agent.py (top-50 → top-10)
├── Confidence weighting + time decay
└── A/B тестирование vs без reranker

Этап 7: Инкрементальное обновление              1 день
├── Incremental mode в build_chunks_v2.py
├── Cron настройка
└── Мониторинг
```

**Общий срок**: 12-17 дней при 2-4 часах в день

---

## 10. Метрики успеха

| Метрика | Цель | Как измерять |
|---------|------|-------------|
| Recall@5 | >70% | 30+ тестовых вопросов, ручная оценка |
| Recall@10 | >85% | то же |
| Latency retrieval | <2 сек | замер в rag_agent.py |
| Точность 1С данных | >95% | сверка с данными в 1С |
| Покрытие источников | 100% | все source_kinds представлены в чанках |

---

## 11. Открытые вопросы

1. **Ресурсы VPS**: хватит ли RAM для Qwen3-Embedding-0.6B? Модель ~1.2GB, inference ~2GB RAM. Сейчас 40% RAM занято + 40% swap. Возможно нужно увеличить RAM или использовать API.
2. **Объём чанков**: прикидка — 248k email envelope + ~350k email body + ~60k km + ~20k 1С + ~2k messenger = ~680k чанков. При 1024 dim float32 = ~2.6GB только embeddings. Проверить дисковое пространство.
3. **LLM для саммери envelope**: генерировать "суть" для envelope через LLM или брать первые 2-3 предложения? LLM качественнее но дороже (248k × ~$0.001 = ~$250). Начать с простого, потом точечно подключить LLM.
4. **Качество km_facts**: нужен аудит — сколько из 42k реально полезных? Если много мусора — чистить до чанкинга.
5. **Thread analysis fix**: промпт для анализа цепочек email — чинить сейчас или после?