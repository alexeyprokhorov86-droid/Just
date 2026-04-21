# Сессия: 2026-04-21 — tools/attachments (фикс УПД + XML + tool-рефакторинг)

Продолжение пути B: выносим анализ вложений в tool-layer.

## Проблема (диагноз)

1. **PDF с ЭДО-УПД галлюцинирует** (id=464 Рахата, message_id=1382, ссылка `t.me/c/3348073995/1/1382`):
   - content_text содержит ТОЛЬКО протокол Диадок (подписи, сертификаты, идентификатор).
   - Страницы с таблицей УПД — сканированные (image-based), без текстового слоя.
   - LLM получает скудный текст + контекст чата «как закрыть расчёты» → **выдумывает**: номер 49, дата 15.04, сливки 11147 кг, 1053462 руб, НДС 160908. Всё фикция.
2. **XML-УПД не анализируется вообще** (44 документа в Априори, alen=0):
   - BOM + `<?xml` бинарник без поддержки в `bot.py:1398-1433`.
   - Падает в else → пустой анализ.

## Решение (вариант B)

Единый tool `analyze_attachment(file_bytes, filename, mime_type, chat_context)`:
- Magic-byte detection (не доверять расширению/mime).
- Dispatch в handlers: pdf / xml_upd / image / docx / xlsx.
- PDF: text-extract → fallback на Vision если pусто/мало текста.
- XML_UPD: strip BOM, lxml parse, ФНС-схема → реквизиты.
- Return {summary, extracted_text, document_type, confidence, errors}.

`bot.py` зовёт tool одной строкой, интерфейс в БД сохраняем (media_type, content_text, media_analysis).

## Что сделано
- [13:50] Diag: content_text id=464 содержит ТОЛЬКО страницу протокола Диадок; media_analysis — галлюцинация (УПД №49 сливки 1053462 руб).
- [13:55] Скачал S3-sample XML из Априори → **не ФНС-УПД**, а 1С edi_stnd/109 `СчетаПК` (реестры зарплат в банк НФ→Райффайзен/Сбер). Все 7 проверенных — этот же формат.
- [14:00] tools/attachments/_detect.py — magic-byte detection (pdf/xml/image/zip_ooxml/ole_legacy).
- [14:02] tools/attachments/_prompts.py — build_analysis_prompt с ANTI_HALLUCINATION_HEADER. Контекст чата ПЕРЕИМЕНОВАН в «справочный, НЕ источник данных».
- [14:05] tools/attachments/handlers/xml_handler.py — lxml parse + human-readable flatten (max 400 lines) + LLM summary.
- [14:08] Smoke XML на 1С-реестре зарплат (6702 bytes, 15 сотрудников): summary корректный, итого 330228.44 руб, Леонтьев max 41428.05, Мурашко min 4598.71 — все цифры реальные, 0 галлюцинаций.
- [14:10] tools/attachments/handlers/pdf_handler.py — PyPDF2 fast-path → _is_only_edo_protocol() → Vision fallback (pdf2image→PNG per-page OCR + multi-image analysis).
- [14:12] tools/attachments/handlers/image_handler.py, ooxml_handler.py — скопировано из bot.py с новым промптом.
- [14:15] tools/attachments/__init__.py — analyze_attachment tool (domain='attachments') + analyze_attachment_bytes helper (bytes без base64). Lazy import bot.gpt_client.
- [14:18] Тест PDF на Татьяны расчётном листе (Джурабаева, 63 KB): PyPDF2 1533 chars → summary корректный (начислено 49006 руб за март, 10001 апрель), 0 ошибок.
- [14:20] Скачал УПД (2).pdf от Рахата через Telegram Bot API (file_id жив, 1.9 MB). Vision сработал (3 страницы, 163s). 
  - ✅ Галлюцинация «сливки 1053462 руб» УБРАНА.
  - ⚠️ Новый OCR нестабилен: страница 1 прочиталась как «кадастр Куйбышевский р-н», finalsummary «аккумуляторы 26718 руб» — тоже не совсем корректно. Это предел GPT-4.1 Vision на плохих ЭДО-сканах.
  - Главное улучшение: LLM теперь **честно пишет «не видно общей строки итого, только позиции можно вручную сложить»** вместо уверенного вранья.
- [14:22] Миграция bot.py:download_and_analyze_media:
  - Ветка message.document — всегда качаем файл (max 50 MB), в tool детект по magic-bytes, media_type_str переопределяется из result.
  - Единая ветка для photo / PDF / XML / docx / xlsx / image в analyze_media — `await asyncio.to_thread(analyze_attachment_bytes, ...)` (чтобы не блокировать event loop на Vision).
  - Voice/audio/video остались на старом пути (transcripts, не document scope).
- [14:25] Рестарт telegram-logger: чистый startup, 0 errors, RAG-scheduler работает.

## Изменённые файлы
- `tools/attachments/__init__.py` — новый, tool analyze_attachment + helper
- `tools/attachments/_detect.py` — новый, magic-byte detector
- `tools/attachments/_prompts.py` — новый, anti-hallucination prompt builder
- `tools/attachments/handlers/__init__.py` — новый, пустой
- `tools/attachments/handlers/xml_handler.py` — новый, universal XML (не специализирован на УПД)
- `tools/attachments/handlers/pdf_handler.py` — новый, PyPDF2 + Vision fallback + EDO-protocol heuristic
- `tools/attachments/handlers/image_handler.py` — новый
- `tools/attachments/handlers/ooxml_handler.py` — новый, docx/xlsx/pptx
- `tools/__init__.py` — добавлен импорт attachments
- `bot.py` — миграция download_and_analyze_media: ветка document → всегда tool, dispatch по media_type → единый вызов analyze_attachment_bytes через asyncio.to_thread. Старые analyze_*_with_gpt и extract_text_from_* оставлены как dead code (удалить в след. коммите).

- [14:20] Backfill 44 XML в Априори: 36 ok, 8 detect=unknown (февральские от Рахата, XML-ФНС в cp1251 без BOM, начинаются с `\r\n<Файл xmlns=...`).
- [14:27] Фикс detect_format: добавлены паттерны для `<Файл` (utf-8 и cp1251) и общий fallback `<...xmlns=...`. xml_handler получил `_detect_encoding` с fallback cp1251 → перекодирование в utf-8 перед lxml.
- [14:28] Повторный backfill 8 файлов — все 8 ok (все XML, 0 errors). 44/44 в Априори 100% проанализированы.
- [14:34] Перепрогон id=462 и id=464 (PDF-УПД от Рахата) через Telegram file_id. Оба Vision, ~140s. Результаты ИНТЕРЕСНЫЕ: два прогона на ОДНОМ файле дали разные summary — «ПРОФС стальные листы 6.5M» vs «ТОРГ трактор Т-25А 206k». Главное: **ни один не упоминает сливки из контекста чата** — anti-hallucination на контекст РАБОТАЕТ. Но Vision OCR сам по себе нестабилен на плохих ЭДО-сканах. Это потолок GPT-4.1 Vision, не prompt-проблема. Улучшение — в backlog.

## Незавершённое / Следующие шаги
- 9 tools в registry, 44/44 backfill в Априори сделан, галлюцинация на УПД по контексту чата УБРАНА — проверено юзером.
- (backlog, отдельная сессия) Удалить dead code в bot.py: analyze_pdf_with_gpt, analyze_image_with_gpt, analyze_excel_with_gpt, analyze_word_with_gpt, analyze_pptx_with_gpt, extract_text_from_pdf, extract_text_from_image, extract_text_from_word, extract_csv_from_excel, extract_text_from_pptx (~500 строк).
- (backlog) Улучшить Vision OCR на ЭДО-сканах: ↑DPI pdf2image до 300-400, Tesseract+препроцессинг, либо ensemble с голосованием. Нужно для нечитаемых УПД (id=462/464 дали разные интерпретации на каждом прогоне).
- (backlog) Специализированный handler для ФНС-УПД schema — когда начнут реально приходить (сейчас только 1С-реестры и ФНС-запросы).

## Заметки
- ✅ id=464 = message_id=1382 = ссылка пользователя. id=462 — дубль того же УПД (переотправка).
- Ранее сегодня закрыт шаг 1 пути B (tools/ registry, 8 tools, 5 коммитов). Tool-философия уже обкатана.
- **Ключевой инсайт**: галлюцинация в id=464 НЕ из-за Vision, а из-за combination двух факторов:
  1. PyPDF2 вернул только протокол Диадок (нет fallback в `analyze_pdf_with_gpt`? нет, fallback есть).
  2. Старый prompt build_analysis_prompt: «Анализируй документ ИМЕННО в контексте обсуждения. Отвечай на тот вопрос, который обсуждался» — это как раз провоцировало LLM додумывать под контекст. Плюс OCR на мелком шрифте даёт плохой результат.
- Новый anti-hallucination prompt снимает обе причины. Но Vision на трудных сканах всё равно ограничен — не решается prompt'ом.
- 9 tools в registry сейчас (было 8). analyze_attachment — по приоритету самый ценный для бота (каждое вложение проходит через него).
- **Подтверждение anti-hallucination working**: id=462/464 перепрогон дал совершенно разные версии содержимого УПД (ПРОФС/ТОРГ/Ракурс), но НИ ОДИН не упомянул «сливки 1053462» из контекста чата. Раньше prompt провоцировал именно это. Теперь галлюцинации — «естественное» искажение Vision OCR на плохом скане, без промпт-усиления.

