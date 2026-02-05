#!/bin/bash

# Загружаем переменные окружения из .env файла
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
fi

# Настройки (берутся из .env или устанавливаются значения по умолчанию)
BACKUP_DIR="${BACKUP_DIR:-$SCRIPT_DIR/backups}"
DB_CONTAINER="${DB_CONTAINER:-knowledge_db}"
DB_NAME="${DB_NAME:-knowledge_base}"
DB_USER="${DB_USER:-knowledge}"
BOT_TOKEN="${BOT_TOKEN}"
ADMIN_USER_ID="${ADMIN_USER_ID}"
DAYS_TO_KEEP="${BACKUP_DAYS_TO_KEEP:-3}"

# Создаём папку если нет
mkdir -p $BACKUP_DIR

# Имя файла с датой
DATE=$(date +%Y-%m-%d_%H-%M)
BACKUP_FILE="$BACKUP_DIR/backup_$DATE.sql.gz"

# Делаем бэкап
echo "[$(date)] Начинаю бэкап..."

docker exec $DB_CONTAINER pg_dump -U $DB_USER $DB_NAME | gzip > $BACKUP_FILE

# Проверяем успешность
if [ -f "$BACKUP_FILE" ] && [ -s "$BACKUP_FILE" ]; then
    SIZE=$(du -h $BACKUP_FILE | cut -f1)
    echo "[$(date)] Бэкап создан: $BACKUP_FILE ($SIZE)"
    
    # Удаляем старые бэкапы
    find $BACKUP_DIR -name "backup_*.sql.gz" -mtime +$DAYS_TO_KEEP -delete
    echo "[$(date)] Старые бэкапы удалены"
    
    # Считаем количество бэкапов
    COUNT=$(ls -1 $BACKUP_DIR/backup_*.sql.gz 2>/dev/null | wc -l)
    
    # Отправляем уведомление об успехе (опционально, можно закомментировать)
    # curl -s -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
    #     -d "chat_id=$ADMIN_USER_ID" \
    #     -d "text=✅ Бэкап БД создан: $SIZE, всего бэкапов: $COUNT"
else
    echo "[$(date)] ОШИБКА: Бэкап не создан!"
    
    # Отправляем алерт
    curl -s -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
        -d "chat_id=$ADMIN_USER_ID" \
        -d "text=🚨 ОШИБКА: Бэкап базы данных не создан!"
fi

echo "[$(date)] Готово"
