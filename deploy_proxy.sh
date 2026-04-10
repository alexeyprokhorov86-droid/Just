#!/bin/bash
#
# deploy_proxy.sh — Устанавливает proxy failover систему на VPS.
#
# Запуск: sudo bash deploy_proxy.sh
#
# Что делает:
#   1. Устанавливает autossh (если нет)
#   2. Убивает старые SSH-туннели и cron-записи
#   3. Копирует systemd-юниты для туннелей
#   4. Копирует proxy_manager.py и proxy_config.py
#   5. Добавляет sudoers для admin (systemctl restart без пароля)
#   6. Запускает всё

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="/home/admin/Just"

echo "=== [1/7] Установка autossh ==="
if ! command -v autossh &>/dev/null; then
    apt-get update && apt-get install -y autossh
else
    echo "autossh уже установлен"
fi

echo ""
echo "=== [2/7] Убираем старые SSH-туннели ==="
# Убить все SSH-туннели к прокси-серверам
pkill -f 'ssh.*1080.*109.234' 2>/dev/null || true
pkill -f 'ssh.*1081.*77.42' 2>/dev/null || true
pkill -f 'autossh.*109.234' 2>/dev/null || true
pkill -f 'autossh.*77.42' 2>/dev/null || true
echo "Старые туннели остановлены"

echo ""
echo "=== [3/7] Убираем SSH-туннели из cron ==="
# Удаляем строки с SSH-туннелями из crontab admin
CRON_BACKUP="/tmp/crontab_backup_$(date +%Y%m%d_%H%M%S)"
crontab -u admin -l > "$CRON_BACKUP" 2>/dev/null || true
if [ -s "$CRON_BACKUP" ]; then
    grep -v 'ssh.*1080.*109.234\|ssh.*1081.*77.42\|autossh.*109.234\|autossh.*77.42' "$CRON_BACKUP" | crontab -u admin - 2>/dev/null || true
    echo "Cron очищен (бэкап: $CRON_BACKUP)"
else
    echo "Cron у admin пуст, пропускаем"
fi

echo ""
echo "=== [4/7] Копируем systemd-юниты ==="
cp "$REPO_DIR/proxy-tunnel-amsterdam.service" /etc/systemd/system/
cp "$REPO_DIR/proxy-tunnel-helsinki.service" /etc/systemd/system/
cp "$REPO_DIR/proxy-manager.service" /etc/systemd/system/
systemctl daemon-reload
echo "Юниты скопированы"

echo ""
echo "=== [5/7] Sudoers для admin ==="
SUDOERS_FILE="/etc/sudoers.d/proxy-manager"
cat > "$SUDOERS_FILE" << 'EOF'
# Разрешаем admin управлять туннелями и ботом без пароля
admin ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart proxy-tunnel-amsterdam
admin ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart proxy-tunnel-helsinki
admin ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart telegram-logger
admin ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart proxy-tunnel-amsterdam.service
admin ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart proxy-tunnel-helsinki.service
admin ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart telegram-logger.service
EOF
chmod 440 "$SUDOERS_FILE"
echo "Sudoers настроен: $SUDOERS_FILE"

echo ""
echo "=== [6/7] Запускаем туннели ==="
systemctl enable --now proxy-tunnel-amsterdam.service
systemctl enable --now proxy-tunnel-helsinki.service
sleep 5
echo "Статус Amsterdam:"
systemctl status proxy-tunnel-amsterdam.service --no-pager -l || true
echo ""
echo "Статус Helsinki:"
systemctl status proxy-tunnel-helsinki.service --no-pager -l || true

echo ""
echo "=== [7/7] Запускаем proxy_manager ==="
systemctl enable --now proxy-manager.service
sleep 3
echo "Статус proxy_manager:"
systemctl status proxy-manager.service --no-pager -l || true

echo ""
echo "=== ГОТОВО ==="
echo ""
echo "Проверки:"
echo "  systemctl status proxy-tunnel-amsterdam"
echo "  systemctl status proxy-tunnel-helsinki"
echo "  systemctl status proxy-manager"
echo "  cat /tmp/active_proxy.json"
echo "  cat /tmp/proxy_status.json"
echo "  journalctl -u proxy-manager -f"
echo ""
echo "ВАЖНО: Не забудь обновить bot.py — заменить хардкод прокси на:"
echo "  from proxy_config import get_proxy_url"
echo "  proxy_url = get_proxy_url()"
