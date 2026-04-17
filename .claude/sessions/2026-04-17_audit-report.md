# Сессия: 2026-04-17 — Полный аудит + TASK_rag_quality_v2

## Что сделано

Начало сессии — запрос «изучи всё и дай отчёт что улучшить».
Дальше постепенно: исправлено 4 hotspot'а (Fase 1-3 аудита), затем реализован полный план TASK_rag_quality_v2 (Фазы 1-7) + доделки.

### Коммиты за сессию (17 в origin/main)

1. `2816463` bot.py /rooms + hardcode password → env
2. `cb89be0` cleanup корня (−16 файлов) + .gitignore + report_digest_agent
3. `c403107` embedding_service → _e5, Qwen3 canonical API
4. `0d73e4d` HNSW + search_source_chunks + consistency test
5. `3a21556` Qwen3-Reranker-0.6B
6. `877216e` nomenclature.weight_unit в SQL-tools + human answerer
7. `937340e` search_unified (km_* + source_chunks) + search_knowledge SQL-фикс
8. `ed8fdf8` Router на gpt-4.1 + 20 few-shot + entities→filters
9. `c2f7f23` period mapping (q4_2025/january_2026/YYYY-MM-DD), top_* на mart_*
10. `e52f104` post-answer evaluator + escalation на Claude Opus 4.7
11. `61593f9` text-to-SQL (Opus 4.7) + safe runner + custom_sql
12. `6e47908` fixate успешные ответы в source_chunks (Qwen3)
13. `53323d0` reply-chain контекст → answerer prompt
14. `f83e61e` Router retry + content dedup в source_chunks retrieval
15. `a2669a4` synthesize_1c_facts.py cron 06:00
16. `c19bbe5` CLAUDE.md обновлён (архитектура, модели, флаги)
17. `2520199` deep reply-chain + plan_fact %% escape
18. `065ff07` reply-chain в БД (переживает рестарт) + /rag_stats
19. `f04aea7` nutrition-цикл остановлен
20. `ac443b3` mart_purchases на c1_* + battery 30

### Инфраструктура
- logrotate для проекта
- knowledge_db container пересоздан: pgvector/pgvector:pg15 + shm_size 2gb
- HNSW idx_sc_embedding_v2 (m=16, ef=64) построен за ~3 мин
- mart_purchases пересоздан на c1_purchases (12816 → 27882, данные до 17.04)
- bot_message_chain таблица для reply-chain
- answer_model / answer_retry_count / answer_eval_good / answer_eval_issues в rag_query_log
- Cron: 06:00 synthesize_1c_facts.py, остальные без изменений

### Ключевые файлы (новые/крупно-правленные)
- rag_agent.py — Router prompt, 11 analytics_types, evaluator+escalation, text-to-SQL, search_unified, fixate, dedup
- bot.py — Matrix rooms, notifications (уже было), reply-chain DB, /rag_stats, element_reminder v2
- chunkers/embedder.py — embed_query_v2, embed_document_v2
- chunkers/reranker.py — native AutoModelForCausalLM API
- embedding_service_e5.py — legacy isolated
- tests/check_embedding_consistency.py, tests/ab_compare_retrieval.py, tests/full_rag_battery.py
- fill_nutrition.py — фильтр по nutrition_requests, auto_deferred для SKIP_TECHNOLOGIST
- synthesize_1c_facts.py — ежедневный cron с 11 категориями синтеза
- TASK_qwen_consistency.md (чеклист), TASK_rag_quality_v2.md (план)
- CLAUDE.md (обновлён)

## Metrics после всех работ

### Battery 30 вопросов (tests/full_rag_battery.py)
- 0 ошибок, 30/30 ≥3 цитирования
- 64 сек средняя латенция
- gpt-4.1: 18 запросов, eval_good=100%
- Claude Opus 4.7 escalation: 12 запросов (40%)

### Observed issue (не блокер)
**40% escalation — многовато.** Причина: evaluator_answer_quality штрафует
ответы "нельзя посчитать по evidence" (маржинальность, план-факт без
cost-данных), хотя такие ответы корректные. Система дважды наказывает
за объективно отсутствующие данные. Фикс для будущей сессии:
смягчить `evaluate_answer_quality` — если LLM явно признаёт что именно
отсутствует и запрашивает уточнение, это "good", а не "bad".

### Nutrition-цикл (fill_nutrition.py)
57 → 9 позиций/день после фикса (-84% LLM-вызовов).

### RAG за сутки (24h до 17:00)
- 28 запросов, 0 errors
- Средняя latency 43 сек (без battery это был бы обычный поток)
- Pre-answer evaluator строг (54% "insufficient"), post-answer gpt-4.1
  согласен с плохим только 2/7 новых — значит pre-eval перестарается,
  но retry не ломает качество

## Что стоит наблюдать в реальном использовании

1. `/rag_stats 24` раз в день → смотреть % escalation, avg latency
2. Реальные вопросы пользователей — какие провалятся
3. `rag_query_log` WHERE answer_eval_good=false — смотреть что Opus
   считает плохим, либо калибровать evaluator либо добавлять tools
4. Стоимость: если escalation >30% на Opus → серьёзный токен-расход.
   По прайсу RouterAI: Opus 494₽/1M input + 2472₽/1M output.
   Средний запрос ~2-3К input + 1-2К output → 5-10 ₽/escalation.
   При 100 запросах в день и 40% escalation → 200-400 ₽/день только
   на escalation. Это в пределах бюджета, но стоит мониторить.

## Незавершённое / Следующая сессия

- Tuning evaluator_answer_quality (смягчить "good" критерий)
- 4 вопроса которые ушли на escalation зря: "Наполеон Q1 2026",
  "коржи в феврале", "Магнит март", "Кутабаева последний месяц" —
  вероятно в БД реально нет данных, evaluator штрафует за честное
  "не нашёл", а в реальности это правильный ответ
- TASK_rules_manage.md — удалить упоминание из roadmap (фича уже в проде)
- Cost-дашборд через Metabase (отложено)

## Заметки
- Claude Opus 4.7 через RouterAI: `anthropic/claude-opus-4.7` (с точкой!)
- `%` в SQL-колонке → экранировать через `%%` в psycopg2-запросах
- Materialized views с UNIQUE index поддерживают REFRESH CONCURRENTLY
- PTB chat_data = in-memory, переносили на БД bot_message_chain для
  переживания рестарта
