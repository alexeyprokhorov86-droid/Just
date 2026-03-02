# 📧 Email Logger

Система логирования корпоративной почты с AI-анализом для Frumelad (totsamiy.com).

## 🎯 Возможности

- **Синхронизация 70 почтовых ящиков** через IMAP (NIC.ru)
- **Автоматическое определение веток переписки** по References/In-Reply-To
- **AI-саммари** при завершении обсуждений (Claude/Gemini)
- **Анализ вложений** (OCR, Vision API)
- **Telegram-бот** для управления и мониторинга
- **Интеграция с RAG** (pgvector для семантического поиска)

## 📁 Структура проекта

```
email_logger/
├── config/
│   ├── .env.example      # Шаблон конфигурации
│   └── settings.py       # Загрузка настроек
├── db/
│   └── 001_init_email_logger.sql  # Миграция БД
├── services/
│   ├── imap_client.py         # IMAP клиент
│   ├── sync_service.py        # Синхронизация почты
│   ├── thread_manager.py      # Управление ветками + AI
│   └── attachment_processor.py # Обработка вложений
├── bot/
│   └── email_commands.py      # Команды Telegram бота
├── main.py               # Точка входа
└── requirements.txt
```

## 🚀 Установка

### 1. Клонирование и настройка окружения

```bash
cd /opt
git clone <repo> email_logger
cd email_logger

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Настройка базы данных

```bash
# Создаём базу
sudo -u postgres createdb email_logger

# Применяем миграцию
psql -d email_logger -f db/001_init_email_logger.sql
```

### 3. Конфигурация

```bash
cp config/.env.example config/.env
nano config/.env
```

Заполните:
- `DATABASE_URL` — строка подключения к PostgreSQL
- `EMAIL_N` — учётные данные почтовых ящиков
- `ROUTERAI_API_KEY` — ключ для AI API
- `TELEGRAM_BOT_TOKEN` — токен бота

### 4. Запуск

```bash
# Напрямую
python main.py

# Через systemd
sudo cp email_logger.service /etc/systemd/system/
sudo systemctl enable email_logger
sudo systemctl start email_logger
```

## 🤖 Команды Telegram бота

### Ветки переписки
- `/open_threads` — список открытых веток
- `/thread_<id>` — детали ветки
- Кнопки: 📜 Переписка, 🤖 Саммари, ✅ Решена, 📦 Архив

### Управление
- `/assign_email` — назначить email сотруднику
- `/email_stats` — статистика системы
- `/sync_status` — статус синхронизации
- `/force_sync` — принудительная синхронизация

### Поиск
- `/search_email` — поиск по письмам

## 📊 Схема БД

### Основные таблицы

| Таблица | Описание |
|---------|----------|
| `employees` | Сотрудники (связь с Telegram и 1С) |
| `employee_emails` | Связь сотрудников с email |
| `monitored_mailboxes` | Мониторимые почтовые ящики |
| `email_threads` | Ветки переписки с AI-саммари |
| `email_messages` | Письма |
| `email_attachments` | Вложения с анализом |

### Поля ветки (email_threads)

- `subject_normalized` — тема без Re:/Fwd:
- `lifecycle_status` — open/pending_resolution/closed/archived
- `resolution_outcome` — resolved/cancelled/other (nullable)
- `status` — legacy-поле для обратной совместимости
- `summary_short/detailed` — AI-саммари
- `key_decisions` — массив решений
- `action_items` — задачи (JSON)
- `priority` — high/medium/low
- `sentiment` — positive/neutral/negative/conflict

## 🔄 Логика работы

### Синхронизация
1. Каждые 5 минут проверяем все активные ящики
2. Загружаем новые письма по IMAP UID
3. Парсим заголовки, тело, вложения
4. Определяем ветку по References → In-Reply-To → теме
5. Сохраняем в БД

### AI-саммари
1. Ищем маркеры завершения: "договорились", "принято", "утверждаю"...
2. Если найдено — генерируем саммари через Claude
3. Извлекаем: решения, задачи, теги, приоритет
4. Уведомляем админов для подтверждения

### Вложения
1. Сохраняем файл в `YYYY/MM/DD/hash_filename`
2. Ставим в очередь на анализ
3. Изображения/PDF → Gemini Vision
4. Текстовые файлы → Claude Haiku
5. Результат анализа → в БД для RAG

## 🔧 Конфигурация .env

```env
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/email_logger

# IMAP (NIC.ru)
IMAP_SERVER=imap.nicmail.ru
IMAP_PORT=993

# Email credentials (EMAIL_N=address,password)
EMAIL_1=accountant@totsamiy.com,SecretPass
EMAIL_2=hr@totsamiy.com,SecretPass
# ... до EMAIL_70

# AI
ROUTERAI_API_KEY=your_key
ROUTERAI_BASE_URL=https://api.routerai.com/v1

# Telegram
TELEGRAM_BOT_TOKEN=123456:ABC...
TELEGRAM_ADMIN_IDS=123456789

# Storage
ATTACHMENTS_PATH=/var/email_logger/attachments

# Sync
SYNC_INTERVAL_MINUTES=5
INITIAL_LOAD_DAYS=30
```

## 📈 Мониторинг

### Логи
```bash
tail -f email_logger.log
journalctl -u email_logger -f
```

### Проверка статуса
```sql
-- Ящики с ошибками
SELECT email, last_error FROM monitored_mailboxes WHERE sync_status = 'error';

-- Статистика по дням
SELECT DATE(received_at), COUNT(*) FROM email_messages GROUP BY 1 ORDER BY 1 DESC;

-- Открытые ветки
SELECT subject_normalized, message_count, priority FROM email_threads WHERE status = 'open';
```

## 🛡️ Безопасность

- Пароли хранятся в `.env` (не в репозитории!)
- IMAP через SSL (порт 993)
- Доступ к боту ограничен ролями
- Вложения в отдельной директории с хэшами

## 📝 TODO

- [ ] IMAP IDLE для push-уведомлений
- [ ] Интеграция с 1С для синхронизации сотрудников
- [ ] Web-интерфейс для просмотра веток
- [ ] Экспорт в Outline Wiki
- [ ] Уведомления о важных письмах

---

*Разработано для Frumelad / Кондитерская Прохорова*
