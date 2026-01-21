# Проверка обновлений bot.py на VPS

## Шаг 1: Подключитесь к VPS и перейдите в директорию проекта
```bash
cd /path/to/Just  # Замените на вашу директорию
```

## Шаг 2: Проверьте текущую ветку и статус
```bash
git status
git branch
```

## Шаг 3: Получите последние изменения с GitHub (без применения)
```bash
git fetch origin
```

## Шаг 4: Проверьте разницу между локальной и удаленной версией
```bash
# Если вы на ветке claude/add-daily-doc-analysis-OGNyV
git diff HEAD origin/claude/add-daily-doc-analysis-OGNyV -- bot.py

# Или проверьте все файлы
git diff HEAD origin/claude/add-daily-doc-analysis-OGNyV
```

## Шаг 5: Посмотрите последние коммиты на удаленной ветке
```bash
git log origin/claude/add-daily-doc-analysis-OGNyV --oneline -5
```

## Шаг 6: Переключитесь на нужную ветку и примените изменения
```bash
# Переключение на ветку с изменениями
git checkout claude/add-daily-doc-analysis-OGNyV

# Или если вы на другой ветке, сначала сделайте:
git fetch origin claude/add-daily-doc-analysis-OGNyV:claude/add-daily-doc-analysis-OGNyV
git checkout claude/add-daily-doc-analysis-OGNyV

# Получите последние изменения
git pull origin claude/add-daily-doc-analysis-OGNyV
```

## Шаг 7: Проверьте, что изменения применены
```bash
# Проверьте содержимое файла (ключевые строки)
grep -n "APScheduler" bot.py
grep -n "DELAYED_ANALYSIS_CHAT" bot.py
grep -n "analyze_daily_documents" bot.py
grep -n "scheduled_daily_analysis" bot.py
```

## Шаг 8: Проверьте последний коммит локально
```bash
git log -1 --stat
```

Должен быть коммит: "Добавлен анализ документов в конце дня для группы "Торты Отгрузки""

## Шаг 9: Установите необходимую зависимость
```bash
pip install apscheduler
# Или если используете pip3
pip3 install apscheduler
```

## Шаг 10: Перезапустите бота
```bash
# Если бот запущен через systemd
sudo systemctl restart your-bot-service

# Или если бот запущен через screen/tmux
# Остановите старый процесс и запустите заново
pkill -f bot.py
python3 bot.py

# Или если используется nohup
pkill -f bot.py
nohup python3 bot.py > bot.log 2>&1 &
```

## Быстрая проверка одной командой
```bash
# Проверить, есть ли изменения, которые нужно подтянуть
git fetch origin && git status
```

Если вывод показывает "Your branch is behind 'origin/...'", значит нужно сделать `git pull`.

## Проверка версии файла по хешу
```bash
# Посмотреть хеш последнего коммита
git rev-parse HEAD

# Сравнить с удаленной веткой
git rev-parse origin/claude/add-daily-doc-analysis-OGNyV
```

Если хеши совпадают - у вас актуальная версия!
