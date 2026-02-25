#!/usr/bin/env bash
set -euo pipefail

LOCK="/run/max-bot1.startlimit.alert.ts"
COOLDOWN_SEC="${STARTLIMIT_ALERT_COOLDOWN_SEC:-600}"

now="$(date +%s)"
last="0"
[[ -f "$LOCK" ]] && last="$(cat "$LOCK" 2>/dev/null || echo 0)"

# anti-spam
if [[ $((now-last)) -lt $COOLDOWN_SEC ]]; then
  exit 0
fi
echo "$now" > "$LOCK"

# env gate
source /opt/max-bot1/healthcheck.env 2>/dev/null || true
if [[ "${TG_ALERT_ENABLED:-0}" != "1" ]]; then exit 0; fi
if [[ -z "${TG_ALERT_BOT_TOKEN:-}" || -z "${TG_ALERT_CHAT_ID:-}" ]]; then exit 0; fi

HOST="$(hostname -f 2>/dev/null || hostname)"
TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
MSG="🌀 max-bot1 restart storm / start-limit on ${HOST} at ${TS}. Check: systemctl status max-bot1 ; journalctl -u max-bot1 -n 200"

curl -fsS --max-time 8 -X POST "https://api.telegram.org/bot${TG_ALERT_BOT_TOKEN}/sendMessage" \
  -d "chat_id=${TG_ALERT_CHAT_ID}" --data-urlencode "text=${MSG}" >/dev/null 2>&1 || true
