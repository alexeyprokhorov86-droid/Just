# TASK: Redis — инфраструктурный слой (кеш / очередь / lock / state)

## Контекст

В течение сессии 2026-04-21 (MTProto-video + backfill) всплыли 2 реальных проблемы, которые решаются Redis:

1. **SQLite lock на `mtproto.session`** — bot-процесс и backfill-worker не могут одновременно открыть session. Пришлось перенести backfill в сам bot-процесс через `/backfill_videos`. Это костыль: следующий долгий background-скрипт упрётся в то же ограничение.

2. **Тяжёлые задачи в event loop бота** — видео-анализ 4 мин, Element-video 1 мин, будущие BOM-пересборки. Если Whisper/ffmpeg залипает, весь бот теряет responsiveness. Нужна очередь + отдельный worker.

Плюс 3 идеи на перспективу, которые не срочны, но дадут заметную разницу:

3. **Embedding query cache** — Qwen3 embed_query_v2 ~200-400 ms на GPU, а RAG-лог показывает что похожие вопросы повторяются («продажи Магнита» в разных формулировках). Hash нормализованного вопроса → top-N chunks, TTL сутки.

4. **State store для Волны 5** (агент многоходовых поручений) — persistent state «ждём ответа Екатерины с 2026-04-19», «Иванов в процессе identification», без pg-коммитов на каждый чих.

5. **Rate-limit к внешним API** — защита от всплеска в RouterAI / Anthropic, сейчас нет.

---

## Цели

1. Redis как Docker-контейнер в `kb_network` (172.20.0.X).
2. Distributed lock для `mtproto.session` — любой процесс может качать, не блокируя bot.
3. Очередь видео-анализа: bot публикует job, отдельный `video_worker.py` забирает и обрабатывает.
4. Embedding query cache для RAG (ускорение повторных запросов).
5. State-store helper в `tools/redis_state.py` для будущего агента Волны 5.

**НЕ делаем сразу:**
- Redis Stack (vectors) — pgvector у нас уже работает на 300k chunks, не трогаем.
- Replica / Sentinel — single instance достаточно; при сбое бот fallback'ится на pg.
- Persistence RDB/AOF по-умолчанию хватит (данные — преимущественно кеш, потерять не страшно).

---

## Фазы

### Фаза 1 — Setup (пол дня)

- `docker-compose.yml` в `/home/admin/knowledge-base/` — сервис `redis:7-alpine` в `kb_network`, volume для persistence, maxmemory 512 MB, maxmemory-policy allkeys-lru.
- Пароль в `.env` как `REDIS_PASSWORD`.
- `tools/_redis.py` — singleton client (`redis.asyncio.Redis`) с ленивым init.

**Acceptance:** `docker exec redis redis-cli ping` → PONG. Python может писать/читать ключи.

### Фаза 2 — Distributed lock для mtproto (полдня)

- В `tools/attachments/_mtproto.py` — `@contextlib.asynccontextmanager _mtproto_lock()` использует Redis `SET lock:mtproto <uuid> NX EX 60`.
- `download_from_telegram` оборачивается в lock — два процесса не конфликтуют.
- Fallback: если Redis недоступен, работает как сейчас (bot-only).

**Acceptance:** одновременный запуск bot + backfill-скрипта не даёт `database is locked`.

### Фаза 3 — Очередь видео-задач (1-2 дня)

- `video_worker.py` — отдельный сервис (systemd unit `video-worker`):
  - Консюмит Redis Streams `videos:pending`.
  - Вызывает `analyze_video_from_telegram` и обновляет pg.
- `bot.py`: когда приходит видео — `XADD videos:pending chat_id=X message_id=Y filename=...` вместо синхронного вызова.
- Retry/DLQ для падающих jobs (Streams consumer groups).

**Acceptance:** bot отвечает моментально, видео-анализ идёт в worker, в логах worker'а по 1 job. При падении worker job сохраняется в Redis и забирается после restart.

### Фаза 4 — Embedding cache (1 день)

- `chunkers/embedder.py:embed_query_v2` — hash нормализованного query (lowercase, stripped), SHA1, ключ `emb:q:<hash>`. TTL 24h.
- Miss → compute + SETEX. Hit → return.
- Метрики cache hit rate в логах.

**Acceptance:** full_rag_battery показывает измеримое ускорение на повторных вопросах.

### Фаза 5 — State store для Волны 5 (отложено, делается когда Волна 5 стартует)

- `tools/redis_state.py` — API `agent_state.get(goal_id)`, `agent_state.update(goal_id, dict)`, TTL бесконечный или months.
- Подразумевает формат агента определён (персистент goal/plan store из CLAUDE.md Волны 5).

---

## Риски

- **Redis single point of failure**: если контейнер упал, без fallback — очередь не идёт, кеш не отдаёт. Решение: критичные tools (lock, queue) — fallback на прямой вызов или pg; кеш — fallback на compute.
- **Пароль в .env**: тот же путь что остальные credentials. Но Redis слушает только внутри docker network, public не открываем.
- **Persistence**: stream'ы с видео-задачами потерять нельзя. RDB snapshot каждые 5 мин + AOF = ok.

---

## Интеграция с другими задачами

- **TASK_kg_graph** — независим, но kg_graph metrics тоже можно кешировать в Redis (`kg:centrality:<eid>`) чтобы tools не дергали pg каждый раз.
- **Волна 5** — state store критичен для persistent goal/plan store.
- **RAG latency optimization** — embedding cache + hot queries cache прямой вклад.
