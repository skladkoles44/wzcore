#!/usr/bin/env python3
import os
import sys
import time
import glob
import sqlite3
from typing import Optional

# === Config (env overridable) ===
DB_PATH = os.getenv("DB_PATH", "/var/lib/max-bot1/bot.db")
STAMP_PATH = os.getenv("STAMP_PATH", "/var/lib/max-bot1/retention.last_ok.ts")

RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "365"))
BACKUP_GLOB = os.getenv("BACKUP_GLOB", "/var/lib/max-bot1/bot.db.bak.*")
BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "365"))

# guard: alert if purge hasn't been successful for too long
MAX_STAMP_AGE_SEC = int(os.getenv("MAX_STAMP_AGE_SEC", "0"))  # 0 disables guard

# safety / ops
DRYRUN = int(os.getenv("DRYRUN", "1"))  # manual default is safe; systemd sets DRYRUN=0
SQLITE_TIMEOUT_SEC = float(os.getenv("SQLITE_TIMEOUT_SEC", "10"))
BUSY_TIMEOUT_MS = int(os.getenv("BUSY_TIMEOUT_MS", "10000"))
BEGIN_MODE = os.getenv("BEGIN_MODE", "IMMEDIATE").upper()  # IMMEDIATE|DEFERRED|EXCLUSIVE
DO_VACUUM = int(os.getenv("DO_VACUUM", "0"))  # expensive lock; off by default

SECONDS = 86400

def _log(msg: str) -> None:
    now = int(time.time())
    print(f"[{now}] {msg}", flush=True)

def _file_size(path: str) -> int:
    try:
        return int(os.stat(path).st_size)
    except FileNotFoundError:
        return 0

def _fmt_bytes(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    x = float(n)
    for u in units:
        if x < 1024.0 or u == units[-1]:
            return f"{x:.1f}{u}"
        x /= 1024.0
    return f"{x:.1f}B"

def _read_stamp_age(now: int) -> int:
    try:
        with open(STAMP_PATH, "r", encoding="utf-8") as f:
            last = int(f.read().strip())
        return max(0, now - last)
    except FileNotFoundError:
        _log("WARN stamp_missing (first run?)")
        return 0  # do NOT punish first run
    except Exception as e:
        _log(f"WARN stamp_read_error err={e!r}")
        return 10**9  # any other error -> guard can trigger

def _ensure_parent(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (name,)
    ).fetchone()
    return row is not None

def _has_col(conn: sqlite3.Connection, table: str, col: str) -> bool:
    q = "SELECT 1 FROM pragma_table_info(?) WHERE name=? LIMIT 1;"
    row = conn.execute(q, (table, col)).fetchone()
    return row is not None

def main() -> int:
    now = int(time.time())
    cutoff = now - (RETENTION_DAYS * SECONDS)
    backup_cutoff = now - (BACKUP_RETENTION_DAYS * SECONDS)

    _log("retention_purge start")
    _log(f"DB_PATH={DB_PATH}")
    _log(f"RETENTION_DAYS={RETENTION_DAYS} cutoff={cutoff}")
    _log(f"BACKUP_GLOB={BACKUP_GLOB}")
    _log(f"BACKUP_RETENTION_DAYS={BACKUP_RETENTION_DAYS} backup_cutoff={backup_cutoff}")
    _log(f"DRYRUN={DRYRUN}")
    _log(f"MAX_STAMP_AGE_SEC={MAX_STAMP_AGE_SEC}")

    # guard: detect "timer broken / purge not running"
    if MAX_STAMP_AGE_SEC > 0:
        age = _read_stamp_age(now)
        _log(f"stamp_age_sec={age}")
        if age > MAX_STAMP_AGE_SEC:
            _log(f"CRITICAL retention_guard_failed age_sec={age} max={MAX_STAMP_AGE_SEC}")
            return 2

    db_size_before = _file_size(DB_PATH)
    _log(f"db_size_before={db_size_before} ({_fmt_bytes(db_size_before)})")

    # connect (busy_timeout)
    try:
        conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT_SEC)
    except Exception as e:
        _log(f"CRITICAL db_connect_failed err={e!r}")
        return 3

    try:
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS};")

        # minimal schema validation
        if not _table_exists(conn, "delivery") or not _table_exists(conn, "events"):
            _log("CRITICAL missing_table delivery/events")
            return 4

        has_next_retry_ts = _has_col(conn, "delivery", "next_retry_ts")
        _log(f"HAS_NEXT_RETRY_TS={has_next_retry_ts}")

        # candidates count
        if has_next_retry_ts:
            delivery_candidates = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM delivery
                WHERE status IN ('sent','dead')
                  AND created_ts < ?
                  AND (next_retry_ts IS NULL OR next_retry_ts <= ?)
                """,
                (cutoff, now),
            ).fetchone()["n"]
        else:
            delivery_candidates = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM delivery
                WHERE status IN ('sent','dead')
                  AND created_ts < ?
                """,
                (cutoff,),
            ).fetchone()["n"]

        events_orphan_candidates = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM events
            WHERE created_ts < ?
              AND event_id NOT IN (SELECT event_id FROM delivery)
            """,
            (cutoff,),
        ).fetchone()["n"]

        _log(f"delivery candidates={delivery_candidates}")
        _log(f"events orphan candidates={events_orphan_candidates}")

        if DRYRUN == 1:
            _log("DRYRUN=1 -> no deletes executed")
        else:
            # one transaction; predictable lock behavior
            begin_sql = "BEGIN IMMEDIATE;" if BEGIN_MODE == "IMMEDIATE" else "BEGIN;"
            conn.execute(begin_sql)

            if has_next_retry_ts:
                cur = conn.execute(
                    """
                    DELETE FROM delivery
                    WHERE status IN ('sent','dead')
                      AND created_ts < ?
                      AND (next_retry_ts IS NULL OR next_retry_ts <= ?)
                    """,
                    (cutoff, now),
                )
            else:
                cur = conn.execute(
                    """
                    DELETE FROM delivery
                    WHERE status IN ('sent','dead')
                      AND created_ts < ?
                    """,
                    (cutoff,),
                )
            deleted_delivery = cur.rowcount
            _log(f"deleted delivery={deleted_delivery}")

            cur2 = conn.execute(
                """
                DELETE FROM events
                WHERE created_ts < ?
                  AND event_id NOT IN (SELECT event_id FROM delivery)
                """,
                (cutoff,),
            )
            deleted_events = cur2.rowcount
            _log(f"deleted events={deleted_events}")

            conn.commit()
            _log("DB commit OK")

            # optional: VACUUM (expensive exclusive lock) - disabled by default
            if DO_VACUUM == 1:
                _log("VACUUM start")
                conn.execute("VACUUM;")
                _log("VACUUM complete")

            # stamp only on successful real run
            _ensure_parent(STAMP_PATH)
            with open(STAMP_PATH, "w", encoding="utf-8") as f:
                f.write(str(now))
            _log(f"stamp_written={STAMP_PATH}")

    except sqlite3.OperationalError as e:
        # typical: SQLITE_BUSY, locked, missing columns, etc.
        try:
            conn.rollback()
        except Exception:
            pass
        _log(f"CRITICAL sqlite_operational_error err={e!r}")
        return 5
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _log(f"CRITICAL unexpected_error err={e!r}")
        return 6
    finally:
        try:
            conn.close()
        except Exception:
            pass

    db_size_after = _file_size(DB_PATH)
    _log(f"db_size_after={db_size_after} ({_fmt_bytes(db_size_after)})")
    if db_size_after <= db_size_before:
        _log(f"db_size_delta={(db_size_after - db_size_before)} ({_fmt_bytes(abs(db_size_after - db_size_before))})")
    else:
        _log(f"WARN db_grew_delta=+{(db_size_after - db_size_before)} ({_fmt_bytes(db_size_after - db_size_before)})")

    # rotate backups (mtime based)
    paths = sorted(glob.glob(BACKUP_GLOB))
    delete_candidates = []
    for p in paths:
        try:
            if int(os.stat(p).st_mtime) < backup_cutoff:
                delete_candidates.append(p)
        except FileNotFoundError:
            continue

    _log(f"backup files matched={len(paths)} delete_candidates={len(delete_candidates)}")

    if DRYRUN == 1:
        _log("DRYRUN=1 -> no backup deletes executed")
    else:
        deleted = 0
        for p in delete_candidates:
            try:
                os.remove(p)
                deleted += 1
            except Exception as e:
                _log(f"WARN backup_delete_failed path={p} err={e!r}")
        _log(f"deleted backups={deleted}")

    _log("retention_purge complete")
    return 0

if __name__ == "__main__":
    sys.exit(main())
