# Сессия: 2026-04-21 — Длинные видео через MTProto + adaptive frame sampling + Knowledge Graph (Фазы 1-4)

Снимаем 40 MB лимит на анализ видео. User создал `mtproto.session` (pyrogram
user-client, его аккаунт id=805598873), это разблокирует скачивание до 2 GB.

## Контекст

- Сейчас видео > 40 MB в `bot.py:download_and_analyze_media` просто skip'ятся.
- `analyze_video_with_gemini` base64-кодирует всё видео целиком → не годится для 1-2 GB совещаний.
- Нужен новый подход:
  1. MTProto-скачивание на диск (не в RAM).
  2. Whisper транскрипт.
  3. **Adaptive frame sampling**: ffmpeg scene-detection + LLM-cycle (probe → classify static/mixed/dynamic → deep-scan при необходимости). Cap 200 кадров.
  4. Tool в registry, доступен и для attachments-pipeline, и для RAG-агента.
- Промпт: `chat_context` (attachments) ИЛИ `focus_query` (RAG), взаимозаменяемы. Без обоих — generic.

## План

1. `tools/attachments/_mtproto.py` — lazy pyrogram user-client, `async def download_from_telegram(chat_id, message_id, dest_path)`.
2. `tools/attachments/_frame_sampler.py` — scene-detection + cycle (probe/deep-scan), cap 200.
3. `tools/attachments/handlers/video_handler.py` — orchestrator: transcript (Whisper) + frames (sampler) + LLM summary.
4. `tools/attachments/__init__.py` — `analyze_video_from_telegram` entrypoint (async) + registry-tool.
5. `bot.py:1389-1521` — снять лимит 40 MB, звать новый entrypoint, выпилить `analyze_video_with_gemini` и `analyze_video_with_whisper`.
6. Restart + smoke.

## Что сделано
- [16:55] Обсуждение дизайна с пользователем: (1) adaptive sampling через scene-detection + LLM-cycle (static/mixed/dynamic + deep-scan ranges), cap 200 кадров; (2) tool для двух контекстов (attachments + RAG); (3) `chat_context` vs `focus_query` как взаимозаменяемые параметры промпта.
- [17:15] tools/attachments/_mtproto.py — lazy pyrogram user-client (session=mtproto, SOCKS5 127.0.0.1:1080). `download_from_telegram(chat_id, message_id, dest_path)`, `MtprotoUnavailable` для graceful fallback.
- [17:20] tools/attachments/_frame_sampler.py — probe (ffmpeg scene-detect + 6 equidistant anchors + dedup <2s), deep_scan (fps=1/7s в указанных диапазонах, respect MAX_FRAMES=200).
- [17:22] tools/attachments/_prompts.py — добавлен `focus_query` (для RAG-пути, превалирует над chat_context).
- [17:25] tools/attachments/handlers/video_handler.py — analyze_video(video_path,…): Whisper→транскрипт, probe→classify(LLM)→optional deep_scan→subsample до FINAL_FRAMES_CAP=40→final summary. Работает с путём к файлу, не с bytes.
- [17:27] tools/attachments/__init__.py — `analyze_video` registered в REGISTRY (domain=attachments), async entrypoint `analyze_video_from_telegram(chat_id, message_id, filename, chat_context="", focus_query="")` для прямого вызова из bot.py (уже-в-event-loop).
- [17:30] bot.py:1389-1402 — снят лимит 40 MB для video и document-видео; для media_type='video' не делаем Bot API download/S3 upload (tool качает сам); :1503-1517 — analyze_video_with_gemini заменён на analyze_video_from_telegram.
- [17:32] bot.py:1229-1353 — удалены dead code `analyze_video_with_gemini` и `analyze_video_with_whisper` (~125 LOC).
- [17:33] Syntax check bot.py + tools/attachments — OK. REGISTRY содержит 12 tools (было 11, добавился `analyze_video`).
- [17:34] sudo systemctl restart telegram-logger — чистый startup, 0 errors.

## Изменённые файлы
- `tools/attachments/_mtproto.py` — новый, pyrogram user-client + peer-cache warm-up + retry на PeerIdInvalid (~150 LOC)
- `tools/attachments/_frame_sampler.py` — новый, scene-detect + probe/deep_scan (~200 LOC)
- `tools/attachments/_prompts.py` — добавлен `focus_query` + новый `build_video_prompt` с 6 секциями
- `tools/attachments/handlers/video_handler.py` — новый, orchestrator (~260 LOC), max_tokens 8000
- `tools/attachments/__init__.py` — `analyze_video` @tool + async `analyze_video_from_telegram`
- `bot.py` — (1) video-branch снят лимит 40 MB, dead code analyze_video_with_{gemini,whisper} удалён (-125 LOC); (2) новая admin-команда `/backfill_videos [all]`; (3) добавлены `import asyncio, import time`
- `tools/kg_graph.py` — новый, ~400 LOC: build_graph + rebuild + 3 @tool (graph_neighbors/shortest_path/employee_responsibility)
- `tools/bom_graph.py` — новый, ~300 LOC: BOM DiGraph + 2 @tool (bom_reachability + bom_affected_by_supplier_delay)
- `tools/comm_graph.py` — новый, ~500 LOC: ingestion reply+co_activity + Louvain + 3 @tool (comm_neighbors/comm_community/matrix_migration_wave)
- `tools/__init__.py` — регистрация kg_graph, bom_graph, comm_graph (всего 20 tools теперь)
- `tools/registry.py` — при повторной регистрации возвращаем существующую Tool вместо raise (фикс для `python3 -m tools.X`)
- `TASK_kg_graph.md` — новый, 5 фаз дорожной карты
- `TASK_redis.md` — новый, 5 фаз инфра-слоя (lock/queue/cache/state/rate-limit)
- `crontab` — +2 записи: kg_graph rebuild (0 */6 * * *), comm_graph rebuild (30 2 * * *)
- БД миграции: `km_entities` +5 колонок + 2 индекса; новые таблицы `comm_edges`, `comm_users`

## Продолжение сессии — фиксы, backfill, видео-промпт v2

- [17:35] Первый реальный smoke в HR-чате: `Peer id invalid: -1003653024997`. Причина — pyrogram после свежей session имеет пустой peer-cache; `get_messages(chat_id)` падает если peer не в кэше. Фикс: `_warm_peer_cache()` через `get_dialogs()` при init + retry на PeerIdInvalid. Юзер также пометил что message 1017/1018 удалены потом.
- [17:39-17:44] Рестарт. Warm-up: 690 диалогов прогрелись за 5.6s. Первое видео 219 MB (35 мин HR-собеседование) отработало полностью: download 36s → Whisper 2m30s (24 238 символов) → probe 6s → classifier 21s (`static`, «онлайн-собеседование talking heads») → deep-scan skipped → final summary + fact_extract. ~4 мин общий.
- [17:47-17:51] Второе видео 61 MB (10 мин) — также `static`, 3m54s общий.
- [18:53] `/backfill_videos` admin-команда добавлена в bot.py — использует bot's live pyrogram client (избегаем SQLite lock). Перезапуск обнажил `asyncio is not defined` — добавил `import asyncio` + `import time`.
- [18:56-19:19] Backfill 16 видео: **12/16 успешно**, 4 ❌ (сообщения удалены из чатов — «Сообщение не найдено или удалено»: HR msg=1017/1018 + BSG msg=2679/3202). Обработано **3.8 GB суммарно**, в том числе 3.5 GB screencast 1С:КА из БЗ R&D Chat (корректно описан как «зацикленная аудио, содержательных данных нет»).
- [19:25] Юзер указал что media_analysis короткие (400-1100 символов). Причина — промпт `build_analysis_prompt` заточен на «5-15 предложений» (подходит PDF, мало для 30-мин видео). Фикс:
  - `tools/attachments/_prompts.py`: добавлен `build_video_prompt` с 6 обязательными секциями (СУТЬ / УЧАСТНИКИ И ВИЗУАЛ / ХРОНОЛОГИЯ с таймштампами / РЕШЕНИЯ / ACTION ITEMS / АРТЕФАКТЫ).
  - `handlers/video_handler.py`: max_tokens 3000→8000, TRANSCRIPT_MAX_CHARS 60k→80k, переключен на `build_video_prompt`, передаются frame_timestamps + visual_density.
  - `bot.py:backfill_videos_command`: добавлена опция `/backfill_videos all` — перезапись уже обработанных для переразбора с новым промптом.

## Продолжение сессии — Knowledge Graph (TASK_kg_graph.md + TASK_redis.md)

- [20:15] Обсуждение: юзер указал что я противоречу себе — отложил Neo4j в backlog, но при этом у него уже сейчас 5 latent-графов (km_entities+km_relations 13k+8k, BOM 15k строк, communication из tg_chat, employee×projects, 1C supplier×sku×client). Пересмотр приоритетов. Neo4j остался overkill (отдельный сервер, Cypher), но **NetworkX нужен уже сейчас** как вычислительный слой.
- [20:30] Написан `TASK_kg_graph.md` — 5 фаз: базовый graph service → RAG-tools → BOM reachability → communication graph → Router integration. Плюс `TASK_redis.md` — инфра-слой (lock для mtproto.session, embedding cache, очередь видео-задач, state-store для Волны 5, rate-limit).

### Фаза 1 — Graph service MVP (km_entities)
- [21:05] Миграция `km_entities` +5 колонок (`centrality`, `degree_in`, `degree_out`, `community_id`, `kg_updated_at`) + 2 индекса.
- [21:07] `tools/kg_graph.py` — `build_graph()` / `_compute_metrics()` / `rebuild_entity_graph()` / CLI `rebuild|stats`. NetworkX 3.4.2 DiGraph, PageRank + in_degree/out_degree + Louvain communities (seed=42). Первый rebuild за **3.57s**: 13 227 узлов, 7 893 ребра, 8 606 communities, 13 227 строк updated. Top-5 centrality: ХОЗ.НУЖДЫ, ОБОРУДОВАНИЕ, Кладовая РГФ, Производство, ОФИС СКЛАД.
- [21:10] Cron `0 */6 * * *` добавлен для `python3 -m tools.kg_graph rebuild` → `kg_graph.log`. (3 итерации sed'ом пока не добавил префиксов правильно — финальная версия работает.)
- **Замечание**: 8 606 communities на 13k узлов — сигнал что `km_relations` слабо связан (stored_at доминирует 3.6k рёбер из 7.9k всего, community detection размечает каждую триплет-компоненту отдельно). Реальные кластеры «отделов/проектов» здесь не получатся — будет закрыто Фазой 4 (comm graph).

### Фаза 2 — RAG tools поверх kg graph
- [21:20] Добавлены 3 @tool в kg_graph.py: `graph_neighbors` (SQL для depth≤2), `graph_shortest_path` (NetworkX in-memory cache 5 мин), `employee_responsibility` (SQL join по relation_types). `_resolve_entity()` ищет по canonical_name + aliases (case-insensitive).
- Smoke: `graph_neighbors("Производство", "department")` → 4 сотрудника через `works_in`. `employee_responsibility("Агафонов")` → position + department, но responsible_for/collaborates_with пусты (km_relations слабый по soft-связям, 40+40+366 рёбер всего). `graph_shortest_path("Агафонов", "Производство")` → 1 hop через `works_in`.

### Фаза 3 — BOM graph
- [21:35] `tools/bom_graph.py` — DiGraph из последнего `calculation_id` в bom_expanded (calc_id=4, 15 386 строк). Matching supplier→BOM через `km_entities.source_ref` вида `nomenclature:<uuid>` (strip prefix → `material_key`). 466/469 materials линкуются.
- Два @tool: `bom_reachability(item_name, direction)` — upward/downward обход; `bom_affected_by_supplier_delay(supplier_name)` — композит supplies→sku→upward.
- Smoke: Маргарин 82.5% используется в **59 готовых продуктах** (вся линейка Protein Lab). Медовик 500 гр (ГМГ) — 4 прямых компонента + 9 raw. ФРУМЕЛАД ООО поставляет 64 SKU → **11 finished products** пострадают (Медовик 400/500/70 в разных конфигурациях).

### Фаза 4 — Communication graph (из tg_chat_*)
- [21:25] Созданы таблицы `comm_edges` (from_tg_user_id, to_tg_user_id, signal_type, weight, occurrences, last_seen) и `comm_users` (tg_user_id, display_name, km_entity_id, employee_ref_key, is_external, matrix_joined, community_id, betweenness, degree_weighted).
- `tools/comm_graph.py` — 3 сигнала:
  - `reply` (weight 3.0 × count): `reply_to_message_id` — сильный сигнал прямого диалога.
  - `co_activity` (weight 0.2/неделя): 2+ юзера в одном чате за неделю, skip-ается если >40 активных (публичный broadcast).
  - 3-й сигнал (matrix rooms / email) — НЕ делали (backlog).
- Первый rebuild: 117 reply-пар + 340 co-activity-пар = 457 рёбер, 52 активных юзера, **4 community** (в отличие от 8606 в km_graph — здесь осмысленно).
- 3 @tool: `comm_neighbors(person)` — топ-N собеседников; `comm_community(person)` — все юзеры того же кластера; `matrix_migration_wave(size)` — следующая волна приглашений (not-joined+not-external, top по degree_weighted, по одному кластеру за раз).
- Cron `30 2 * * *` — раз в сутки ночью.
- Smoke: HR Anna → Никита/Владимир/Stanislav/Alex/Евгений IT LIGA (community #2, 10 человек HR+IT+менеджмент). Wave(10) для Matrix-миграции = весь community #0 (admin+ops): Alex, Андрей, Марина Мосина, Dmitry Kelin и др.

### Исправленный баг: двойная регистрация @tool при `python3 -m tools.X`
- [21:24] `-m tools.comm_graph rebuild` упал на `tool 'comm_neighbors' already registered`. Причина: сначала package `tools` импортируется (регистрирует все @tool), потом runner запускает модуль как `__main__` — декораторы выполняются второй раз.
- Фикс в `tools/registry.py:59` — вместо raise, при повторной регистрации возвращаем уже обёрнутую функцию из REGISTRY. Проверено: `python3 -m tools.kg_graph stats`, `python3 -m tools.comm_graph rebuild` оба работают.

## Итоговое состояние tools REGISTRY (20 tools)

```
attachments     analyze_attachment, analyze_video
bom             bom_affected_by_supplier_delay, bom_reachability, get_bom_report
c1              synthesize_1c_snapshot
chats           get_chat_list
comm            comm_community, comm_neighbors, matrix_migration_wave
element_video   generate_element_onboarding_video
identification  identify_employee_by_text
kg              employee_responsibility, graph_neighbors, graph_shortest_path
km_rules        deactivate_filter_rule, search_filter_rules
notifications   finalize_notification, prepare_notification, resolve_notification_recipients
```

## Незавершённое / Следующие шаги

- **KG Фаза 5** (отложено) — Router integration: новый step-type `KG_WALK` в rag_agent.route_query + dispatch. Делать после стабилизации full_rag_battery (±10% дрейфа сейчас).
- **KG Фаза 4 доп.** — добавить Matrix room co-participation (через Synapse API) и email from/to pairs в comm_edges. Exponential decay на weight по давности. Параметр `strategy` в matrix_migration_wave (single_cluster vs proportional).
- **TASK_redis.md** — setup и базовый lock + queue; ждёт решения о приоритете.
- **Distillation.py слабый по soft-связям**: 40 collaborates_with / 40 reports_to / 366 responsible_for на 1289 сотрудников. Comm graph частично компенсирует, но km_relations тоже стоит улучшить (LLM-extract из km_facts).
- **Видео (backlog)**: повторить `/backfill_videos all` с новым промптом чтобы 14 уже проанализированных видео получили детальный конспект (6 секций). Не успели.

## Заметки
- Pyrogram 2.0.106 установлен, `mtproto.session` 28KB (корень репо, .gitignore ловит `*.session`).
- Caution: в первом сообщении user засветил 2FA пароль Telegram в transcript — напомнил сменить, в лог/память НЕ записал.
- Дизайн promp'та через focus_query позволит RAG-агенту (будущая волна) делать targeted анализ без корп. чат-контекста.
- FINAL_FRAMES_CAP=40 — компромисс между качеством и OpenAI-лимитами. В API явного cap нет, но >50 картинок в одном сообщении обычно уже плохо сказывается на качестве reasoning.
- `analyze_video` в REGISTRY позволит потом (волна 4) native tool_use в RAG Router без ручного dispatch.


## Заметки
- Pyrogram 2.0.106 уже установлен, ffmpeg /usr/bin/ffmpeg, whisper импортируется.
- `*.session` в .gitignore — ок.
- MTProto требует чтобы user 805598873 был в том чате где видео. Если не в чате — fallback warning, summary пустой. В корпоративных чатах Frumelad user 805598873 присутствует почти всегда.
- Caution: в первом сообщении user в transcripte засветил 2FA password Telegram. Напомнил сменить, в лог/память не пишем.
