#!/usr/bin/env bash
set -euo pipefail

APP_DIR=/opt/max-bot1/app
DB="/var/lib/max-bot1/bot.db"
DST_DIR=/opt/max-bot1/backups/db
TS="$(date +%Y%m%d_%H%M%S)"
OUT="$DST_DIR/bot.db.bak.$TS"

cd "$APP_DIR"

# backup (preserve mode/owner where possible; ensure readable by service user)
cp -a "$DB" "$OUT"

# retention: delete >30d
find "$DST_DIR" -maxdepth 1 -type f -name 'bot.db.bak.*' -mtime +30 -print -delete || true

# keep newest 10
ls -1t "$DST_DIR"/bot.db.bak.* 2>/dev/null | tail -n +11 | xargs -r rm -f

echo "OK_DB_BACKUP=$OUT"
