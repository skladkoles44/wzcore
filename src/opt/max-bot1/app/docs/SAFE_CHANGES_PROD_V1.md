# SAFE CHANGES (PROD) — max-bot1
Version: v1
Scope: SQLite schema + feature/options rollout without breaking production.

## Invariants
- Production must stay up (no intentional downtime beyond a restart).
- Any DB change MUST be backward compatible (old app can run on new DB).
- Any code change MUST be forward compatible (new app can run on old DB).
- Migrations run BEFORE app start via systemd ordering (max-bot1-migrate.service).

## Systemd topology (must remain true)
- max-bot1.service has drop-in ordering:
  - Requires=max-bot1-migrate.service
  - After=network-online.target max-bot1-migrate.service
- No HTTP/health checks inside systemd unit (no curl in ExecStartPre/Post).

## What counts as SAFE DB change (SQLite)
Allowed (additive only):
- CREATE TABLE IF NOT EXISTS ...
- CREATE INDEX IF NOT EXISTS ... (or guarded by sqlite_master check)
- ALTER TABLE ... ADD COLUMN ... (nullable or with DEFAULT; never drop/rename)
- Data backfill scripts are allowed ONLY if idempotent and bounded (safe to re-run).

Forbidden on PROD:
- DROP TABLE / DROP COLUMN / RENAME TABLE / destructive migrations
- Rewriting the whole DB file
- Long-running locks without a plan (VACUUM, huge UPDATE without batching)

## Rollout protocol (2-phase, compatible both ways)

### Phase A — Ship "compat code" (does NOT require new schema)
Goal: deploy code that works whether the new option exists or not.
Rules:
- When reading: if table/column missing -> behave as "feature off".
- When writing: do not write to new table/column unless feature flag is ON and schema is present.

### Phase B — Apply migrations (additive only)
- Implement migrations in migrate_b2c.py (idempotent).
- Confirm migrate unit logs show OK.

### Phase C — Enable feature
- Enable via env/flag after confirming:
  - max-bot1-migrate.service succeeded
  - app health OK
  - minimal functional test OK (one representative request)

## Verification checklist (each change)
1) Migrations:
   - systemctl start max-bot1-migrate.service
   - journalctl -u max-bot1-migrate.service --no-pager -n 100
2) App restart:
   - systemctl restart max-bot1
   - sleep 2
   - ss -ltnp | grep ':8000'
   - curl -fsS http://127.0.0.1:8000/health
3) DB sanity:
   - sqlite3 /var/lib/max-bot1/bot.db '.tables'
   - sqlite3 /var/lib/max-bot1/bot.db '.schema <changed_table>'

## Emergency rollback rules
- If new code misbehaves: rollback code only (DB is additive; keep it).
- Never attempt destructive DB rollback on PROD.
- Disable feature flag first, then restart app.

## Notes about false alarms
- Immediately after restart port 8000 may not be bound yet (tens–hundreds of ms).
- Always sleep ~2s before concluding "NO_LISTENER".
