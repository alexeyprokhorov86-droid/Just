#!/bin/bash

# Настройки
BACKUP_DIR="/home/admin/backups"
DB_CONTAINER="knowledge_db"
DB_NAME="knowledge_base"
DB_USER="knowledge"
BOT_TOKEN="8402954094:AAHV5LHFHO7w5ObkZqre9A0H3sMSBLuvXcQ"
ADMIN_ID="805598873"
DAYS_TO_KEEP=7

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
    #     -d "chat_id=$ADMIN_ID" \
    #     -d "text=✅ Бэкап БД создан: $SIZE, всего бэкапов: $COUNT"
else
    echo "[$(date)] ОШИБКА: Бэкап не создан!"
    
    # Отправляем алерт
    curl -s -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
        -d "chat_id=$ADMIN_ID" \
        -d "text=🚨 ОШИБКА: Бэкап базы данных не создан!"
fi

echo "[$(date)] Готово"
