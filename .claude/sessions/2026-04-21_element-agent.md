# Сессия: 2026-04-21 — Element Reminder → умный агент поручений

Первый прототип Волны 5 (агент многоходовых поручений) на узкой задаче —
миграция сотрудников на Element X. Вариант C: 5 шагов в одной сессии.

## Контекст (диагностика до старта)

- `matrix_user_mapping`: 35 записей (16 joined, 19 not_joined).
- Проблемы:
  - Екатерина (id=448723084) в 0 активных TG-чатах, но reminder ежедневно в личку.
  - Мусорные имена «😀», «fff», «@3loy81» — реальные люди, но кто? маппинга нет.
  - TG-имена латиницей (Alis Pashaeva, Daniil Grebenikov) — fuzzy-match к `v_current_staff` (204 чел.) не ловит.
  - Групповые теги в рабочие чаты → внешние видят «не подключились: X, Y» — навязчиво.
  - Нет колонки `employee_ref_key` → нельзя отличить внешнего от сотрудника.

## План

- **Шаг 1**: schema-миграция `matrix_user_mapping` (+exclude/frequency/count/escalated/employee_ref_key/is_external).
- **Шаг 2**: фильтры в `element_reminder`. Skip неактивных/уволенных/external. Adaptive frequency. Групповые → только админу.
- **Шаг 3**: identification-agent — бот опрашивает неопознанных, LLM матчит с 1С, admin approval, tool в registry.
- **Шаг 4**: slideshow-видео: open-source скрины Element X → Nano Banana (`google/gemini-2.5-flash-image` через RouterAI) редактирует text overlay → TTS → ffmpeg сборка.
- **Шаг 5**: per-user tone через GPT-4.1 (position_name из 1С → формальный/дружеский стиль).

## Что сделано

## Изменённые файлы

## Незавершённое / Следующие шаги

## Заметки
- Пользователь подсказал: RouterAI поддерживает любые модели, включая **Nano Banana** (Gemini 2.5 Flash Image) — это снимает проблему Android-эмуляции. Путь: статичные скриншоты + image-editing через Nano Banana → slideshow.
- Пользователь поправил меня про "overkill": для агента-поручений генерация видео — не overkill, а профильная задача.
- Мусорные имена (😀, fff, @3loy81) — это НЕ тесты, это реальные сотрудники которых не смогли идентифицировать. Идентификацию будет делать identification-agent через опрос.
