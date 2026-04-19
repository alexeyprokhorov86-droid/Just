# Сессия: 2026-04-19 — Аудит после 1.5 суток молчания + фиксы

## Что сделано

### Аудит (11:00-11:15)
- Git log с 17.04 21:00 — пусто, новых коммитов нет
- journalctl telegram-logger — много рестартов парами (30-60 мин интервал)
- /var/log/auth.log — рестарты инициированы admin через sudo (ручные, пользователь сам деплоил/тестил)
- Crontab — build_source_chunks PAUSED 17.04 (правильно, не мешать backfill)
- Backfill embeddings v2 (PID 1565583) — running с 17.04, на 11:15 был 84%, ETA ~7ч
- HNSW idx_sc_embedding_v2 — отсутствует в БД (либо был дропнут перед reindex, либо не создавался)
- Покрытие embedding_v2: telegram/rag_answer/synthesized_1c = 100%, email_message = 86% (152749/177893)
- Корень проблемы 25144 чанков без v2 — email_reindex 17.04 22:57 пересоздал 108505 чанков

### Контекст-уточнение от пользователя
- build_source_chunks paused вручную в прошлой сессии для запуска повторного reindex эмбеддингов после фикса подписей в email-чанкинге

### Фиксы (13:30-13:45)
- `build_source_chunks.py` — добавлен `from dotenv import load_dotenv` + вызов с явным путём к `.env`. Раньше падал в cron с `fe_sendauth: no password supplied` потому что cron не имеет .env в окружении.
- crontab: добавлена строка `PATH=/home/admin/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin` в начало. Чинит `claude exit 127` в `report_digest_agent` (claude бинарник в `~/.local/bin`).
- crontab: watchdog cron сменил редирект `>> watchdog.log 2>&1` на `>> /dev/null 2>&1`. Раньше каждая запись дублировалась — log() и пишет в файл, и print(), который cron захватывал в тот же файл. Теперь одна запись на запуск.

### Фоновое ожидание
- Запущен background bash `b12ivw2ka`: `until grep -q "Done!" backfill_embed.log; do sleep 300; done` — поллит логи, уведомит когда backfill закончится.

## Изменённые файлы
- `build_source_chunks.py` — (1) load_dotenv для cron-окружения, (2) переход на Qwen3 v2 embedder + INSERT в embedding_v2 (раньше писал legacy e5 в embedding)
- crontab — (1) PATH=/home/admin/.local/bin:... сверху, (2) watchdog в /dev/null (был двойной вывод), (3) build_source_chunks раскомментирован
- `CLAUDE.md` — раздел «Логирование сессий» переписан с жёсткими триггерами (главный: «перед каждым текстовым ответом пользователю — append лог»)
- `canonical_helper.py` — TG insert: body_text = message_text + [Анализ вложения] media_analysis + [Содержимое файла] content_text. Раньше брался один из (content_text) ИЛИ (text+analysis), терялись данные.
- backfill TG canonical (heredoc): 1693 docs с медиа → 838 обновлено, 855 осиротевших (удалённые tg-сообщения).
- `TASK_canonical_attachments.md` — новый файл, полный план: ✅#4 (сделано), #1 email-вложения, #5 analyze_tg_media, #6 Matrix media, #2 c1_event, #3 v_messages_unified.

## Ссылки на смежные документы
- **План канонизации вложений и 1С-событий**: [`TASK_canonical_attachments.md`](../../TASK_canonical_attachments.md) — основной TODO для дальнейшей работы.

## Post-backfill (выполнено 2026-04-19 19:00-19:35)
1. ✅ [19:00] Backfill завершился: 198437 chunks за 44.0h, покрытие 100% (300387/300387)
2. ✅ [19:05] CREATE INDEX CONCURRENTLY idx_sc_embedding_v2 (HNSW, 2036 MB на диске)
3. ✅ [19:15] DELETE 2699 старых chunks для 838 TG-документов (фикс #4)
4. ✅ [19:25] Найден баг: build_source_chunks.py использовал legacy e5 (768-dim) вместо Qwen3 v2 → все новые chunks с момента c403107 уходили в `embedding`, а не `embedding_v2`. Это объясняет странное «8852 с эмбеддингами» в выводе скрипта.
5. ✅ [19:25] Фикс build_source_chunks.py: `from chunkers.embedder import embed_document_v2`, INSERT в `embedding_v2` вместо `embedding`. Удалил 435 неправильно созданных chunks, перегенерил с Qwen3 v2.
6. ✅ [19:30] Покрытие 298123/298123 = 100% Qwen3 v2 (включая re-indexed 838 TG)
7. ✅ [19:33] Раскомментировал cron build_source_chunks (`45 * * * *`)
8. ✅ [19:34] Smoke-тест: запрос «закупка стретч-плёнки» → top-5 релевантных email через HNSW, similarity 0.68

## Следующие шаги
- Коммит: build_source_chunks.py (load_dotenv + Qwen3 v2), canonical_helper.py (TG body composition), CLAUDE.md (логирование сессий), TASK_canonical_attachments.md (новый файл)
- Дальше по TASK_canonical_attachments.md: #1 email_attachment → #5 analyze_tg_media → #2 c1_event → #6 matrix media → #3 v_messages_unified

## ✅ Пункт #1 Email-вложения в canonical (20:00)
- [20:00] Backfill heredoc'ом: 11 758 из 11 760 attachments → source_documents (kind='email_attachment'). 2 пропущено (короткий body).
- [20:05] `analyze_attachments.py` — добавлена `insert_email_attachment_to_canonical()`, вызывается из `update_attachment_status` при status='done'. Идемпотентная.
- [20:10] `rag_agent.search_unified` — изменений не нужно, search_source_chunks ищет по всем kind через embedding_v2.
- [20:12] Запустил build_source_chunks --batch 0 (фон) — обнаружил 245k pending в очереди (включая 177k auto_notification 1С — спам, не надо чанковать).
- [20:15] Найден баг: `generate_embeddings` в build_source_chunks вызывал `embed_document_v2(t)` в list-comprehension (sequential) → 1.3 ch/s. Фикс на `model.embed_batch(texts)` → 35 ch/s, ускорение 27×.
- [20:18] Перезапустил с фиксом, но запрос `get_unprocessed_docs` (LEFT JOIN 269k×298k) висел 5 мин → kill, перезапуск с `--batch 500`.
- [20:20] TASK_canonical_attachments.md существенно расширен:
  - Сводная таблица прогресса
  - 6 найденных побочных багов с фиксами
  - Подробности по #1 (✅ сделано, V1.1 followups)
  - Подробности по #5 с реальными цифрами (2409 backlog, $40-80)
  - 7 follow-up mini-задач (A. orphans cleanup, B. фильтр auto_notification, C. attachment_ids в email meta, D. SHA256 дедуп, E. XLSX по строкам, F. мониторинг, G. дашборд coverage)

## Изменённые файлы (добавление)
- `analyze_attachments.py` — функция insert_email_attachment_to_canonical + триггер из update_attachment_status
- `build_source_chunks.py` — embed_batch() вместо list-comprehension (35× ускорение), MIN_DOC_LEN=30, фильтр auto_notification
- `TASK_canonical_attachments.md` — расширен с прогрессом, багами, follow-ups
- `media_analyzer.py` — НОВЫЙ. Facade для функций анализа медиа, re-export из bot.py + has_meaningful_text() (gpt-4.1-mini prefilter, max_tokens=16)
- `analyze_tg_media_backlog.py` — НОВЫЙ. Backfill media analysis через S3 + LLM. Для Торты-Отгрузки применяется OCR-prefilter.
- `CLAUDE.md` — добавлен раздел «Дискуссия и несогласие» (не соглашаться сразу при технических заблуждениях, аргументировать).
- memory `feedback_pushback.md` — то же самое в memory.

## ✅ Пункт #5 analyze_tg_media (20:30)
- Решение: variant B (LLM-prefilter без install tesseract). Пользователь сначала склонился к A, потом согласился на B после объяснения что LLM-OCR уже работает в pipeline.
- 2261 pending media (98.7% — Торты-Отгрузки 2233).
- Backfill запущен в фоне (`bod26dtpv`), batch=5000, ~7h wall, ожидаемая стоимость ~$100-140.
- OCR-prefilter работает (40% rate в smoke-тесте 5 фото).
- Чанкер `b73yc73r6` параллельно крутит ~1.5 ch/s, 60k docs очередь.

## Заметки
- HNSW мог быть «помечен как built» в логе сессии 17.04, но в БД его нет. На будущее — после CREATE INDEX делать verify через `SELECT indexname FROM pg_indexes WHERE indexname=...`, не доверять только успешному exit.
- watchdog запускался дважды на самом деле один раз — двойная запись была артефактом print() + log()
- Нет новых synthesized_1c с 17.04 — нормально, синтез работает только когда есть свежие 1С-данные
- backfill скорость 1.3 ch/s (на старте было 1.8), вероятно из-за нагрузки от email-sync/bot

## Ревизия инструкции по логированию (по запросу пользователя)
Раздел в CLAUDE.md «Логирование сессий» имеет хороший приоритет, но размыт:
- «значимое действие» — без определения
- «дописывать по ходу» — без триггера (после чего именно?)
- «финализировать в конце» — без признака конца сессии

Предложение к CLAUDE.md: добавить чёткие триггеры (перед каждым ответом user, после каждого Edit/Bash-mutator, после 10 мин тишины — финализировать).

## Фикс OOM build_source_chunks (22:30-22:35)
- [22:00, 22:05] dmesg: два OOM-kill подряд, python RSS 12-13GB (cron.service). Процесс build_source_chunks падал через 5 минут после старта из-за Qwen3 forward pass batch=128 на CPU.
- [22:33] Фикс: `chunkers/config.py` EMBEDDING_BATCH_SIZE 128→32, `build_source_chunks.py` EMBED_BATCH_SIZE 64→32, doc_batch_size 200→50.
- [22:33-22:35] Тест `--batch 100`: 89 сек, 100 docs → 121 chunks, пик RAM 4.2GB, без OOM.
- [22:27] Перезапущен `analyze_tg_media_backlog` (PID 2578815) на 2008 pending в Торты-Отгрузки (идемпотентный resume).
- Коммит f4ff9da: canonical fixes (email attachments, TG media, build_chunks Qwen3 v2, CLAUDE.md дискуссия).

## Незавершённое
- Проверить что cron в 22:45 отработает без OOM с новыми batch_size.
- Backlog TG-Торты продолжает идти (2008→0, ~7h wall, $100-140).
- По `TASK_canonical_attachments.md`: #2 c1_event, #6 Matrix media, #3 v_messages_unified.
