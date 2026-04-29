#!/bin/bash

# Загружаем переменные окружения из .env файла
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
fi

# Настройки (берутся из .env или устанавливаются значения по умолчанию)
BACKUP_DIR="${BACKUP_DIR:-$SCRIPT_DIR/backups}"
DB_CONTAINER="${DB_CONTAINER:-knowledge_db}"
DB_USER="${DB_USER:-knowledge}"
BOT_TOKEN="${BOT_TOKEN}"
ADMIN_USER_ID="${ADMIN_USER_ID}"
DAYS_TO_KEEP="${BACKUP_DAYS_TO_KEEP:-3}"

# Список БД для бэкапа: frumelad (всегда) + saas (если БД существует)
DBS_TO_BACKUP="knowledge_base"
if docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='knowledge_base_saas'" 2>/dev/null | grep -q 1; then
    DBS_TO_BACKUP="$DBS_TO_BACKUP knowledge_base_saas"
fi

mkdir -p "$BACKUP_DIR"
DATE=$(date +%Y-%m-%d_%H-%M)
echo "[$(date)] Начинаю бэкап БД: $DBS_TO_BACKUP"

OVERALL_OK=1
for DB_NAME in $DBS_TO_BACKUP; do
    # frumelad (knowledge_base) — историческое имя файла без префикса БД
    # saas — с префиксом, чтобы verify_backup.py и retention различали
    if [ "$DB_NAME" = "knowledge_base" ]; then
        BACKUP_FILE="$BACKUP_DIR/backup_$DATE.sql.gz"
    else
        BACKUP_FILE="$BACKUP_DIR/backup_${DB_NAME}_$DATE.sql.gz"
    fi

    echo "[$(date)] [$DB_NAME] pg_dump → $BACKUP_FILE"
    docker exec "$DB_CONTAINER" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$BACKUP_FILE"

    if [ ! -f "$BACKUP_FILE" ] || [ ! -s "$BACKUP_FILE" ]; then
        echo "[$(date)] [$DB_NAME] ОШИБКА: Бэкап не создан!"
        OVERALL_OK=0
        curl -s -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
            -d "chat_id=$ADMIN_USER_ID" \
            -d "text=🚨 ОШИБКА: Бэкап БД $DB_NAME не создан!"
        continue
    fi

    SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    echo "[$(date)] [$DB_NAME] OK: $SIZE"

    # S3
    echo "[$(date)] [$DB_NAME] → S3"
    "$SCRIPT_DIR/venv/bin/python" "$SCRIPT_DIR/backup_to_s3.py" "$BACKUP_FILE"
    [ $? -ne 0 ] && echo "[$(date)] [$DB_NAME] S3 upload failed (локальная копия сохранена)"

    # Remote VPS
    echo "[$(date)] [$DB_NAME] → Amsterdam + Helsinki"
    "$SCRIPT_DIR/venv/bin/python" "$SCRIPT_DIR/backup_to_remote.py" "$BACKUP_FILE"
    [ $? -ne 0 ] && echo "[$(date)] [$DB_NAME] оба удалённых VPS недоступны (S3 + локальная живы)"
done

# Локальная ротация одна на все БД (один префикс backup_*.sql.gz покрывает оба варианта имён)
find "$BACKUP_DIR" -name "backup_*.sql.gz" -mtime +$DAYS_TO_KEEP -delete
COUNT=$(ls -1 "$BACKUP_DIR"/backup_*.sql.gz 2>/dev/null | wc -l)
echo "[$(date)] Локальная ротация: $COUNT файлов в $BACKUP_DIR (retention >$DAYS_TO_KEEP дней)"

[ "$OVERALL_OK" = "1" ] && echo "[$(date)] Готово" || echo "[$(date)] Готово (с ошибками)"
