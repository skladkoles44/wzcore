#!/usr/bin/env bash
set -euo pipefail

APP_URL="http://127.0.0.1:8000/health"
UNIT="max-bot1"
HOST="$(hostname -f 2>/dev/null || hostname)"
ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# restart cooldown (prevents self-made restart storms)
RESTART_LOCK="/run/max-bot1.healthcheck.restart.ts"
RESTART_COOLDOWN_SEC="${RESTART_COOLDOWN_SEC:-300}"

tg_send() {
  # never break healthcheck flow
  if [[ "${TG_ALERT_ENABLED:-0}" != "1" ]]; then return 0; fi
  if [[ -z "${TG_ALERT_BOT_TOKEN:-}" || -z "${TG_ALERT_CHAT_ID:-}" ]]; then return 0; fi
  local text="$1"
  curl -fsS --max-time 8 -X POST "https://api.telegram.org/bot${TG_ALERT_BOT_TOKEN}/sendMessage" \
    -d "chat_id=${TG_ALERT_CHAT_ID}" --data-urlencode "text=${text}" >/dev/null 2>&1 || true
}

# load tg env (optional)
source /opt/max-bot1/healthcheck.env 2>/dev/null || true

# try health
if curl -fsS --max-time 3 "$APP_URL" >/dev/null 2>&1; then
  echo "HEALTH_OK"
  exit 0
fi

echo "HEALTH_FAIL -> considering restart $UNIT"
tg_send "🚨 ${UNIT} HEALTH_FAIL on ${HOST} at $(ts)."

now="$(date +%s)"
last="0"
[[ -f "$RESTART_LOCK" ]] && last="$(cat "$RESTART_LOCK" 2>/dev/null || echo 0)"

if [[ $((now-last)) -lt $RESTART_COOLDOWN_SEC ]]; then
  echo "COOLDOWN_SKIP_RESTART"
  tg_send "🧊 ${UNIT} cooldown active (${RESTART_COOLDOWN_SEC}s). Not restarting again. Host=${HOST} at $(ts)."
  exit 1
fi

echo "$now" > "$RESTART_LOCK"
echo "RESTARTING $UNIT"
systemctl restart "$UNIT" || true
sleep 2

if curl -fsS --max-time 3 "$APP_URL" >/dev/null 2>&1; then
  echo "OK_AFTER_RESTART"
  tg_send "✅ ${UNIT} recovered after restart on ${HOST} at $(ts)."
  exit 0
fi

echo "STILL_DOWN"
tg_send "🧨 ${UNIT} STILL_DOWN on ${HOST} at $(ts). Manual вмешательство."
exit 1
