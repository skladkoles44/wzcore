# System Overview (Bot1 + Bot2 + SQLite)

## 1) Components

### Bot1 — Producer / Ingest (MAX → DB)
**Role:** accepts inbound MAX messages, normalizes, and writes facts + delivery tasks into SQLite.

**Responsibilities (facts):**
- extract sender/user_id
- extract chat_id
- build `notify_text`
- generate `event_id`
- `INSERT` into `events`
- `INSERT` into `delivery` (tasks for dispatch)

**Non-responsibilities (facts):**
- no retry logic
- no sending to Telegram/MAX
- does not consume queue

---

### Bot2 — Dispatch Worker / Consumer (DB → Telegram + MAX)
**Role:** consumes delivery tasks from SQLite and performs dispatch.

**Executable (facts):**
- `/opt/max-bot2/app/bot2_dispatch_runner.py`
- runs as systemd service: `max-bot2-dispatch.service`

**Loop (facts):**
1) claim batch from SQLite  
2) send Telegram (Bot API)  
3) send MAX (platform API)  
4) update delivery status  
5) sleep

---

### SQLite — Contract Layer (facts)
**DB file:** `/var/lib/max-bot1/bot.db`

Acts as:
- persistent queue of delivery tasks
- state journal
- diagnostics source
- idempotency anchor (via unique keys / event_id)

---

## 2) Database Model (facts)

### 2.1 `events` table (fact snapshot from discovery)
- `event_id TEXT PRIMARY KEY`
- required: `source_channel`, `event_type`, `received_ts`, `source_user_id`, `source_chat_id`, `created_ts`
- optional fields exist (e.g. `text`, `meta_json`, `raw_json`, `reply_*`)
- additional convenience columns present: `user_id`, `chat_id`, `notify_text`

**Meaning:** immutable fact “what happened”.

---

### 2.2 `delivery` table (fact snapshot from discovery)
Columns (key ones):
- `id INTEGER PRIMARY KEY AUTOINCREMENT`
- `event_id TEXT NOT NULL`
- `kind TEXT NOT NULL`
- `dest_channel TEXT NOT NULL`
- `recipient_id TEXT NOT NULL`
- `payload TEXT` (nullable)
- `status TEXT NOT NULL`
- `attempts INTEGER NOT NULL DEFAULT 0`
- `last_error TEXT`
- `claim_id TEXT`
- `claim_ts REAL`
- `next_retry_ts REAL`
- `created_ts REAL NOT NULL`
- `updated_ts REAL NOT NULL`
- `channel TEXT NOT NULL DEFAULT 'bot2'`
- `error TEXT`

**Meaning:** tasks “what must be delivered”.

---

## 3) Delivery State Machine (facts)

States (documented and used operationally):
- `new`
- `processing` (claimed/in-flight)
- `sent`
- `dead`

Transitions (facts):
- `new` → `processing` (claim)
- `processing` → `sent` (success)
- `processing` → `new` (retry)
- `processing` → `dead` (max attempts)

---

## 4) Implemented Fixes & Verified Behaviors (facts from chat runs)

### 4.1 Claim ID correctness fix (Bot2)
**Issue observed:** `set_status affected 0 rows (expected 1)` spam due to mismatch between in-memory `claim_id` and DB `claim_id`.

**Fix applied (fact):**
- `claim_batch ... RETURNING ... claim_id ...`
- internal mapping uses returned `claim_id` when available

**Verification (fact):**
- controlled insert produced `sent` rows
- after service restart: `journal_set_status_0rows_matches=0` since service start

---

### 4.2 Lease-timeout release (Bot2)
**Behavior (fact):**
- Bot2 releases stuck `processing` rows whose `claim_ts` is older than configured lease window:
  - log: `lease_timeout released=1 lease_sec=300`
  - then row is claimed and delivered

**Verification (fact):**
- controlled test inserted `status=processing` with old `claim_ts`
- within seconds, status transitioned to `sent`
- logs show `lease_timeout`, `claim_batch`, `BOT2 done delivery_id=... sent`

---

## 5) Retention / Purge (facts)

### 5.1 Script
**Path (fact):** `/opt/max-bot2/app/retention_purge.py`

**What it does (facts from logs):**
- computes cutoff by `RETENTION_DAYS` (configured to 365 in service env)
- deletes old `delivery` rows eligible by policy (in runs shown: 0 candidates)
- deletes orphan `events` (in runs shown: 0 candidates)
- if `next_retry_ts` exists: script detects it (`HAS_NEXT_RETRY_TS=True`)
- rotates old backup files matching glob:
  - `BACKUP_GLOB=/var/lib/max-bot1/bot.db.bak.*`
  - uses backup retention window (365 in service env)
- writes stamp on success:
  - `/var/lib/max-bot1/retention.last_ok.ts`
- logs DB size before/after:
  - `db_size_before=...`
  - `db_size_after=...`
  - `db_size_delta=...`

**DRYRUN parameter (fact):**
- code: `DRYRUN = int(os.getenv("DRYRUN", "0"))`
- default is **0** (real mode), set `DRYRUN=1` for dry-run

### 5.2 systemd timer/service
**Installed (fact):**
- `max-bot-retention.service`
- `max-bot-retention.timer` enabled, daily schedule with randomized delay

**Guard (fact):**
- `MAX_STAMP_AGE_SEC=129600` (36 hours) logged by script in v4 install run
- first run logs: `WARN stamp_missing (first run?)`, `stamp_age_sec=0`

**Proof artifacts (fact):**
- timer active (waiting), next trigger shown
- stamp file exists: `/var/lib/max-bot1/retention.last_ok.ts`

---

## 6) Operational Commands (facts / observed usage)

- service status:
  - `systemctl status max-bot2-dispatch.service`
- dispatch logs:
  - `journalctl -u max-bot2-dispatch.service --no-pager`
  - `/var/log/max-bot2/notify_dispatch.log`
- retention logs:
  - `journalctl -u max-bot-retention.service --no-pager -n 50`

