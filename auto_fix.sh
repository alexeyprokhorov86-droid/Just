#!/usr/bin/env bash
# auto_fix.sh — главный скрипт автономного Claude-агента.
# См. TASK_autonomous_agent.md (шаг 3) и .claude/AUTO_AGENT_RULES.md
#
# Использование:
#   ./auto_fix.sh [--dry-run] <trigger_name> <context_file>

set -uo pipefail

REPO_DIR="/home/admin/telegram_logger_bot"
RULES_FILE="$REPO_DIR/.claude/AUTO_AGENT_RULES.md"
SESSIONS_DIR="$REPO_DIR/.claude/auto_sessions"
HELPER="$REPO_DIR/auto_fix_helper.py"
PY="$REPO_DIR/venv/bin/python"
CLAUDE_BIN="$(command -v claude || echo /usr/bin/claude)"
CLAUDE_TIMEOUT_SEC=600
HEALTHCHECK_WAIT_SEC=30
HEALTHCHECK_TIMEOUT_SEC=120
RATE_LIMIT_PER_24H=2
COOLDOWN_MIN=15

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
    shift
fi

TRIGGER_NAME="${1:-}"
CONTEXT_FILE="${2:-}"

if [[ -z "$TRIGGER_NAME" || -z "$CONTEXT_FILE" ]]; then
    echo "usage: $0 [--dry-run] <trigger_name> <context_file>" >&2
    exit 2
fi

if [[ ! -f "$CONTEXT_FILE" ]]; then
    echo "context file not found: $CONTEXT_FILE" >&2
    exit 2
fi

mkdir -p "$SESSIONS_DIR"
TS="$(date +%Y-%m-%d_%H-%M)"
SESSION_LOG="$SESSIONS_DIR/${TS}_${TRIGGER_NAME}.log"
PROMPT_FILE="$(mktemp /tmp/auto_fix_prompt.XXXXXX)"
SUMMARY_FILE="$(mktemp /tmp/auto_fix_summary.XXXXXX)"
trap 'rm -f "$PROMPT_FILE" "$SUMMARY_FILE"' EXIT

log() { echo "[$(date '+%F %T')] $*" | tee -a "$SESSION_LOG"; }

cd "$REPO_DIR" || { echo "cd $REPO_DIR failed"; exit 2; }

# ── Rate-limit & cooldown ──────────────────────────────────────────────
log "=== auto_fix.sh start trigger=$TRIGGER_NAME dry_run=$DRY_RUN ==="
RATE_CHECK="$("$PY" "$HELPER" rate_check "$TRIGGER_NAME" "$RATE_LIMIT_PER_24H" "$COOLDOWN_MIN" 2>&1)"
RATE_RC=$?
log "rate_check: $RATE_CHECK"
if [[ $RATE_RC -ne 0 ]]; then
    REASON="$RATE_CHECK"
    "$PY" "$HELPER" log_event "$TRIGGER_NAME" "$CONTEXT_FILE" "" "rate_limited" "$REASON" "" "" "" >/dev/null || true
    "$PY" "$HELPER" tg_report "$TRIGGER_NAME" "rate_limited" "$REASON" "" "" "" || true
    exit 1
fi

# ── Сборка промпта ─────────────────────────────────────────────────────
{
    echo "Триггер: $TRIGGER_NAME"
    echo
    echo "Контекст (JSON):"
    cat "$CONTEXT_FILE"
    echo
    echo "Правила (см. .claude/AUTO_AGENT_RULES.md):"
    cat "$RULES_FILE"
    echo
    echo "Задача: проанализируй ситуацию и исправь её строго в рамках разрешённых операций."
    echo "Если задача выходит за рамки — выведи 'ESCALATE: <причина>' и завершись."
    echo "Финальный stdout-блок ОБЯЗАТЕЛЕН (см. формат в правилах, секция 'Что писать в stdout')."
} > "$PROMPT_FILE"

GIT_SHA_BEFORE="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
log "git HEAD before: $GIT_SHA_BEFORE"

if [[ $DRY_RUN -eq 1 ]]; then
    log "DRY-RUN: prompt прочитан, claude НЕ вызывается. Размер промпта: $(wc -c < "$PROMPT_FILE") байт."
    log "Промпт сохранён: $PROMPT_FILE"
    cp "$PROMPT_FILE" "$SESSIONS_DIR/${TS}_${TRIGGER_NAME}.dryrun_prompt.txt"
    "$PY" "$HELPER" log_event "$TRIGGER_NAME" "$CONTEXT_FILE" "" "dry_run" "" "$GIT_SHA_BEFORE" "" "" >/dev/null || true
    log "=== DRY-RUN done ==="
    exit 0
fi

# ── Health-check before ────────────────────────────────────────────────
TARGET_HINT="$("$PY" "$HELPER" target_hint "$TRIGGER_NAME" "$CONTEXT_FILE" 2>/dev/null || echo "")"
HEALTH_BEFORE="unknown"
if [[ -n "$TARGET_HINT" ]]; then
    if systemctl is-active --quiet "$TARGET_HINT"; then HEALTH_BEFORE="true"; else HEALTH_BEFORE="false"; fi
    log "target hint: $TARGET_HINT, health_before=$HEALTH_BEFORE"
fi

# ── Вызов claude -p ─────────────────────────────────────────────────────
log "Вызываем claude -p (timeout=${CLAUDE_TIMEOUT_SEC}s) ..."
CLAUDE_OUT="$(mktemp /tmp/auto_fix_claude_out.XXXXXX)"
CLAUDE_ERR="$(mktemp /tmp/auto_fix_claude_err.XXXXXX)"
# Снимаем CLAUDE_CODE_* env vars: если auto_fix.sh запущен ИЗ другой Claude-
# сессии (или цепочкой), дочерний `claude -p` иначе пытается подключиться к
# родителю и получает 403 Forbidden. Cron-окружение чистое, но защита нужна
# для ручных запусков и watchdog (который тоже может быть вызван иначе).
set +e
env -u CLAUDECODE -u CLAUDE_CODE_SSE_PORT -u CLAUDE_CODE_ENTRYPOINT -u CLAUDE_CODE_EXECPATH \
    timeout "$CLAUDE_TIMEOUT_SEC" "$CLAUDE_BIN" -p "$(cat "$PROMPT_FILE")" \
    --permission-mode acceptEdits \
    --allowedTools "Bash,Edit,Read,Write,Grep,Glob" \
    > "$CLAUDE_OUT" 2> "$CLAUDE_ERR"
CLAUDE_RC=$?
set -e
log "claude exit code: $CLAUDE_RC"
{
    echo "----- claude stdout -----"
    cat "$CLAUDE_OUT"
    echo "----- claude stderr -----"
    cat "$CLAUDE_ERR"
} >> "$SESSION_LOG"

if [[ $CLAUDE_RC -ne 0 ]]; then
    REASON="claude exit=$CLAUDE_RC (timeout?): $(tail -c 500 "$CLAUDE_ERR")"
    "$PY" "$HELPER" log_event "$TRIGGER_NAME" "$CONTEXT_FILE" "$CLAUDE_OUT" "failed" "$REASON" "$GIT_SHA_BEFORE" "$HEALTH_BEFORE" "" >/dev/null || true
    "$PY" "$HELPER" tg_report "$TRIGGER_NAME" "failed" "$REASON" "$GIT_SHA_BEFORE" "$SESSION_LOG" "" || true
    rm -f "$CLAUDE_OUT" "$CLAUDE_ERR"
    exit 1
fi

# ── Парсинг финального блока ───────────────────────────────────────────
sed -n '/^=== AUTO-FIX SUMMARY ===$/,/^=== END SUMMARY ===$/p' "$CLAUDE_OUT" > "$SUMMARY_FILE"
PARSED_STATUS="$(grep -E '^STATUS:' "$SUMMARY_FILE" | head -1 | sed 's/^STATUS:[[:space:]]*//')"
PARSED_SHA="$(grep -E '^GIT_SHA:' "$SUMMARY_FILE" | head -1 | sed 's/^GIT_SHA:[[:space:]]*//')"
PARSED_TARGET="$(grep -E '^TARGET_SERVICE:' "$SUMMARY_FILE" | head -1 | sed 's/^TARGET_SERVICE:[[:space:]]*//')"
[[ -z "$PARSED_STATUS" ]] && PARSED_STATUS="unknown"
[[ -z "$PARSED_TARGET" || "$PARSED_TARGET" == "none" ]] && PARSED_TARGET="$TARGET_HINT"
log "parsed status=$PARSED_STATUS git_sha=$PARSED_SHA target=$PARSED_TARGET"

if grep -q '^ESCALATE:' "$CLAUDE_OUT"; then
    ESC_REASON="$(grep -m1 '^ESCALATE:' "$CLAUDE_OUT" | sed 's/^ESCALATE:[[:space:]]*//')"
    "$PY" "$HELPER" log_event "$TRIGGER_NAME" "$CONTEXT_FILE" "$CLAUDE_OUT" "escalated" "$ESC_REASON" "$GIT_SHA_BEFORE" "$HEALTH_BEFORE" "" >/dev/null || true
    "$PY" "$HELPER" tg_report "$TRIGGER_NAME" "escalated" "$ESC_REASON" "$GIT_SHA_BEFORE" "$SESSION_LOG" "" || true
    rm -f "$CLAUDE_OUT" "$CLAUDE_ERR"
    exit 0
fi

# ── Был ли новый коммит? ───────────────────────────────────────────────
GIT_SHA_AFTER="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
NEW_COMMIT="false"
if [[ "$GIT_SHA_AFTER" != "$GIT_SHA_BEFORE" && "$GIT_SHA_AFTER" != "unknown" ]]; then
    NEW_COMMIT="true"
    log "новый коммит: $GIT_SHA_AFTER"
fi

# ── Health-check after ─────────────────────────────────────────────────
HEALTH_AFTER="unknown"
if [[ -n "$PARSED_TARGET" && "$PARSED_TARGET" != "none" ]]; then
    log "ждём ${HEALTHCHECK_WAIT_SEC}s перед health-check..."
    sleep "$HEALTHCHECK_WAIT_SEC"
    DEADLINE=$(( $(date +%s) + HEALTHCHECK_TIMEOUT_SEC ))
    HEALTH_AFTER="false"
    while [[ $(date +%s) -lt $DEADLINE ]]; do
        if systemctl is-active --quiet "$PARSED_TARGET"; then
            HEALTH_AFTER="true"; break
        fi
        sleep 5
    done
    log "health_after($PARSED_TARGET) = $HEALTH_AFTER"
fi

# ── Auto-revert при провале health-check ───────────────────────────────
FINAL_STATUS="success"
REVERTED="false"
REVERT_REASON=""
if [[ "$HEALTH_AFTER" == "false" && "$NEW_COMMIT" == "true" ]]; then
    log "health-check FAIL → git revert HEAD"
    if git revert --no-edit HEAD >> "$SESSION_LOG" 2>&1 && git push >> "$SESSION_LOG" 2>&1; then
        REVERTED="true"
        REVERT_REASON="health_check_failed: $PARSED_TARGET not active after fix"
        if [[ -n "$PARSED_TARGET" ]]; then
            sudo systemctl restart "$PARSED_TARGET" >> "$SESSION_LOG" 2>&1 || true
        fi
        FINAL_STATUS="reverted"
    else
        REVERTED="false"
        REVERT_REASON="git revert FAILED — manual intervention required"
        FINAL_STATUS="failed"
    fi
elif [[ "$PARSED_STATUS" == "escalated" ]]; then
    FINAL_STATUS="escalated"
elif [[ "$HEALTH_AFTER" == "false" ]]; then
    FINAL_STATUS="failed"
fi

"$PY" "$HELPER" log_event "$TRIGGER_NAME" "$CONTEXT_FILE" "$CLAUDE_OUT" "$FINAL_STATUS" "$REVERT_REASON" "$GIT_SHA_AFTER" "$HEALTH_BEFORE" "$HEALTH_AFTER" "$REVERTED" >/dev/null || true
"$PY" "$HELPER" tg_report "$TRIGGER_NAME" "$FINAL_STATUS" "$REVERT_REASON" "$GIT_SHA_AFTER" "$SESSION_LOG" "$SUMMARY_FILE" || true

rm -f "$CLAUDE_OUT" "$CLAUDE_ERR"
log "=== auto_fix.sh done status=$FINAL_STATUS reverted=$REVERTED ==="
exit 0
