import asyncio
import re
import json
import logging
import os
import sys
import random
import sqlite3
import time
import uuid
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Any, Optional

import httpx

# =========================
# Config (env)
# =========================
DB_PATH = os.getenv("DB_PATH", "/var/lib/max-bot1/bot.db")

TG_ALERT_TOKEN = (os.getenv("TG_ALERT_TOKEN", "") or "").strip()
TG_ALERT_CHAT_ID_RAW = (os.getenv("TG_ALERT_CHAT_ID", "") or "").strip().strip('"').strip("'")

BOT2_TOKEN = (os.getenv("BOT2_TOKEN", "") or "").strip()  # only used for preflight sanity
MAX_NOTIFY_USER_ID_RAW = (os.getenv("MAX_NOTIFY_USER_ID", "") or "").strip()
MAX_SEND_URL = (os.getenv("MAX_SEND_URL", "") or "").strip()
BOT2_SEND_URL = (os.getenv("BOT2_SEND_URL", "") or "").strip()


LOG_DIR = (os.getenv("LOG_DIR", "/var/log/max-bot2") or "").strip()
LOG_FILE = (os.getenv("LOG_FILE", "notify_dispatch.log") or "").strip()
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "10485760"))  # 10MB
LOG_BACKUPS = int(os.getenv("LOG_BACKUPS", "10"))

MAX_BATCH = int(os.getenv("MAX_BATCH", "20"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "1.0"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "10"))
LEASE_TIMEOUT_SECONDS = int(os.getenv("LEASE_TIMEOUT_SECONDS", "300"))

SEND_TIMEOUT_SEC = float(os.getenv("SEND_TIMEOUT_SEC", "20.0"))  # for TG and send_max_func

# =========================
# Logging
# =========================
logger = logging.getLogger("max-bot.notify_dispatch")
logger.setLevel(logging.INFO)

class _DropIdleClaimBatch(logging.Filter):
    """Drop noisy idle logs: claim_batch ... n=0"""
    _rx_n0 = re.compile(r"\bn=0\b")

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        if "claim_batch" in msg and self._rx_n0.search(msg):
            return False
        return True

_LOG_READY = False

# =========================
# Globals
# =========================
_task: Optional[asyncio.Task] = None
_http: Optional[httpx.AsyncClient] = None

# Cache: sqlite RETURNING support
_SUPPORTS_RETURNING: Optional[bool] = None

# Parsed IDs
TG_ALERT_CHAT_ID_INT: Optional[int] = None
MAX_NOTIFY_USER_ID_INT: Optional[int] = None

# Table schema flags (detected at preflight)
_HAS_CLAIM_ID = False
_HAS_CLAIM_TS = False
_HAS_LAST_ERROR = False
_HAS_CREATED_TS = False
_HAS_UPDATED_TS = False
_HAS_ATTEMPTS = False
_HAS_PAYLOAD = False
_HAS_RECIPIENT_ID = False


_ALLOWED_STATUS = {"new", "processing", "sent", "dead"}

def _now_ts() -> float:
    return time.time()

def _jitter_sleep(base: float) -> float:
    return base + random.random() * min(0.25, base)

def _setup_logging_failfast() -> None:
    global _LOG_READY
    if _LOG_READY:
        return

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s:%(name)s:%(message)s"))
    logger.addHandler(ch)

    # File handler with rotation (fail-fast if cannot write)
    if not LOG_DIR:
        raise RuntimeError("LOG_DIR is empty")
    os.makedirs(LOG_DIR, exist_ok=True)

    test_path = os.path.join(LOG_DIR, ".write_test")
    try:
        with open(test_path, "w", encoding="utf-8") as f:
            f.write("ok\n")
        os.remove(test_path)
    except Exception as e:
        raise RuntimeError(f"LOG_DIR not writable: {LOG_DIR!r} ({e})")

    log_path = os.path.join(LOG_DIR, LOG_FILE)
    fh = RotatingFileHandler(
        log_path,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUPS,
        encoding="utf-8",
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s:%(name)s:%(message)s"))
    logger.addHandler(fh)

    # Drop idle claim_batch n=0 spam
    flt = _DropIdleClaimBatch()

    logger.propagate = False

    try:

        logger.addFilter(flt)

    except Exception:

        pass

    for _h in list(logger.handlers):
        try:
            _h.addFilter(flt)
        except Exception:
            pass

    _LOG_READY = True
    logger.info("logging ready log_path=%s maxBytes=%s backups=%s", log_path, LOG_MAX_BYTES, LOG_BACKUPS)

def _parse_ids_failfast() -> None:
    global TG_ALERT_CHAT_ID_INT, MAX_NOTIFY_USER_ID_INT

    if TG_ALERT_CHAT_ID_RAW:
        try:
            TG_ALERT_CHAT_ID_INT = int(TG_ALERT_CHAT_ID_RAW)
        except Exception:
            raise RuntimeError(f"TG_ALERT_CHAT_ID not int: {TG_ALERT_CHAT_ID_RAW!r}")
    else:
        TG_ALERT_CHAT_ID_INT = None

    if MAX_NOTIFY_USER_ID_RAW:
        try:
            MAX_NOTIFY_USER_ID_INT = int(MAX_NOTIFY_USER_ID_RAW)
        except Exception:
            raise RuntimeError(f"MAX_NOTIFY_USER_ID not int: {MAX_NOTIFY_USER_ID_RAW!r}")
    else:
        MAX_NOTIFY_USER_ID_INT = None

def _connect_db_retry() -> sqlite3.Connection:
    last_exc: Optional[BaseException] = None
    base = 0.05
    for i in range(10):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=5.0)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            last_exc = e
            time.sleep(_jitter_sleep(base))
            base = min(base * 2, 1.0)
    raise sqlite3.OperationalError(f"db open failed after retries: {last_exc}")

def _detect_delivery_columns(conn: sqlite3.Connection) -> None:
    global _HAS_CLAIM_ID, _HAS_CLAIM_TS, _HAS_LAST_ERROR, _HAS_CREATED_TS, _HAS_UPDATED_TS, _HAS_ATTEMPTS, _HAS_PAYLOAD, _HAS_RECIPIENT_ID
    cols = set()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(delivery)")
    for r in cur.fetchall():
        cols.add((r[1] or "").lower())

    _HAS_CLAIM_ID = "claim_id" in cols
    _HAS_CLAIM_TS = "claim_ts" in cols
    _HAS_LAST_ERROR = "last_error" in cols
    _HAS_CREATED_TS = "created_ts" in cols
    _HAS_UPDATED_TS = "updated_ts" in cols
    _HAS_ATTEMPTS = "attempts" in cols
    _HAS_PAYLOAD = "payload" in cols
    _HAS_RECIPIENT_ID = "recipient_id" in cols

    logger.info(
        "delivery schema flags claim_id=%s claim_ts=%s last_error=%s created_ts=%s updated_ts=%s attempts=%s payload=%s recipient_id=%s",
        _HAS_CLAIM_ID, _HAS_CLAIM_TS, _HAS_LAST_ERROR, _HAS_CREATED_TS, _HAS_UPDATED_TS, _HAS_ATTEMPTS, _HAS_PAYLOAD, _HAS_RECIPIENT_ID
    )

def _supports_returning(conn: sqlite3.Connection) -> bool:
    # SQLite RETURNING supported from 3.35.0
    cur = conn.cursor()
    cur.execute("SELECT sqlite_version()")
    v = cur.fetchone()[0]
    try:
        parts = tuple(int(x) for x in v.split(".")[:3])
    except Exception:
        logger.warning("cannot parse sqlite_version=%r; assume no RETURNING", v)
        return False
    return parts >= (3, 35, 0)

def _supports_returning_cached(conn: sqlite3.Connection) -> bool:
    global _SUPPORTS_RETURNING
    if _SUPPORTS_RETURNING is None:
        _SUPPORTS_RETURNING = _supports_returning(conn)
        logger.info("sqlite RETURNING supported=%s", _SUPPORTS_RETURNING)
    return _SUPPORTS_RETURNING

def _preflight_db_access_failfast() -> None:
    if not os.path.exists(DB_PATH):
        raise RuntimeError(f"DB_PATH does not exist: {DB_PATH}")

    # preflight: visibility for outbound send hooks
    if not MAX_SEND_URL:
        logger.warning("MAX_SEND_URL not set — MAX sends will fail")
    if not BOT2_SEND_URL:
        logger.warning("BOT2_SEND_URL not set — BOT2 sends will fail")

    conn = _connect_db_retry()
    try:
        cur = conn.cursor()

        # WAL improves concurrency (best-effort; don't crash if not supported)
        try:
            cur.execute("PRAGMA journal_mode=WAL")
            _ = cur.fetchone()
        except Exception as e:
            logger.warning("PRAGMA journal_mode=WAL failed: %s", e)

        # Basic read sanity
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='delivery'")
        if not cur.fetchone():
            raise RuntimeError("table 'delivery' not found in DB")
        # Fail-fast in prod if outbound channels are present in queue but URLs are missing (microfix #2/#3)
        # We intentionally check DB presence to avoid crashing in dev where channels aren't used.
        try:
            cur.execute("SELECT 1 FROM delivery WHERE dest_channel='max' LIMIT 1")
            has_max = cur.fetchone() is not None
            cur.execute("SELECT 1 FROM delivery WHERE dest_channel='bot2' LIMIT 1")
            has_bot2 = cur.fetchone() is not None
        except Exception as e:
            raise RuntimeError(f"preflight channel presence check failed: {e}")

        if has_max and not MAX_SEND_URL:
            raise RuntimeError("MAX_SEND_URL not set but delivery has dest_channel='max' rows")
        if has_bot2 and not BOT2_SEND_URL:
            raise RuntimeError("BOT2_SEND_URL not set but delivery has dest_channel='bot2' rows")

        # Write permission sanity: create/drop a temp table
        try:
            cur.execute("CREATE TABLE IF NOT EXISTS _preflight_test (id INTEGER)")
            cur.execute("DROP TABLE _preflight_test")
            conn.commit()
        except Exception as e:
            raise RuntimeError(f"DB not writable (preflight): {e}")

        _detect_delivery_columns(conn)

        # Helpful index (non-fatal if exists / cannot create)
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_pick ON delivery(dest_channel, status, created_ts, id)")
            conn.commit()
        except Exception as e:
            logger.warning("index create skipped/failed: %s", e)

    finally:
        conn.close()

def _ensure_http() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=httpx.Timeout(SEND_TIMEOUT_SEC))
    return _http

def _truncate_err(s: str, n: int = 800) -> str:
    if not s:
        return s
    s = s.strip().replace("\n", " ")
    if len(s) <= n:
        return s
    return s[:n] + "..."

def _validate_status(status: str) -> None:
    if status not in _ALLOWED_STATUS:
        raise ValueError(f"invalid status={status!r}, allowed={sorted(_ALLOWED_STATUS)}")

def _text_from_payload(payload: Any) -> str:
    # BLOB-safe + number-safe
    if payload is None:
        return ""

    # hard caps (avoid OOM / log bombs)
    MAX_DECODE_CHARS = int(os.getenv("PAYLOAD_MAX_DECODE_CHARS", "262144"))   # 256KB chars
    MAX_TEXT_CHARS   = int(os.getenv("PAYLOAD_MAX_TEXT_CHARS", "8192"))      # final text cap
    MAX_JSON_CHARS   = int(os.getenv("PAYLOAD_MAX_JSON_CHARS", "262144"))    # only try json.loads under this

    if isinstance(payload, (bytes, bytearray, memoryview)):
        try:
            payload = bytes(payload).decode("utf-8", errors="replace")
        except Exception:
            payload = str(payload)

    if isinstance(payload, (int, float)):
        return str(payload)

    if not isinstance(payload, str):
        payload = str(payload)

    s = payload.strip()
    if not s:
        return ""

    # Cap decoded size early
    if len(s) > MAX_DECODE_CHARS:
        s = s[:MAX_DECODE_CHARS]

    # BOM-safe
    s = s.lstrip("\ufeff").strip()
    if not s:
        return ""

    # If JSON, prefer notify_text if present
    if s.startswith("{") and s.endswith("}") and len(s) <= MAX_JSON_CHARS:
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                nt = obj.get("notify_text")
                if isinstance(nt, str) and nt.strip():
                    out = nt.strip()
                    return out[:MAX_TEXT_CHARS] if len(out) > MAX_TEXT_CHARS else out
        except Exception:
            pass

    # Plain text fallback
    return s[:MAX_TEXT_CHARS] if len(s) > MAX_TEXT_CHARS else s


def _extract_attempts(row: sqlite3.Row) -> int:

    if not _HAS_ATTEMPTS:
        return 0
    a = row["attempts"]
    if a is None:
        return 0
    try:
        return int(a)
    except Exception:
        return 0

def _extract_recipient_id(row: sqlite3.Row) -> Optional[int]:
    if not _HAS_RECIPIENT_ID:
        return None
    rid = row["recipient_id"]
    if rid is None:
        return None
    try:
        return int(rid)
    except Exception:
        return None

def _set_status(conn: sqlite3.Connection, delivery_id: int, status: str, attempts: Optional[int] = None,
                claim_id: Optional[str] = None, last_error: Optional[str] = None) -> None:
    _validate_status(status)

    if delivery_id is None:
        raise ValueError("delivery_id is None")

    fields = ["status=?"]
    params: list[Any] = [status]

    if _HAS_ATTEMPTS and attempts is not None:
        fields.append("attempts=?")
        params.append(int(attempts))

    if _HAS_UPDATED_TS:
        fields.append("updated_ts=?")
        params.append(_now_ts())

    if _HAS_LAST_ERROR:
        if last_error:
            last_error = _truncate_err(last_error, 800)
            fields.append("last_error=?")
            params.append(last_error)
        else:
            # clear on success/claim
            fields.append("last_error=NULL")

    where = "id=?"
    params.append(int(delivery_id))

    # claim safety if present
    if _HAS_CLAIM_ID and claim_id:
        where += " AND claim_id=?"
        params.append(claim_id)

    sql = f"UPDATE delivery SET {', '.join(fields)} WHERE {where}"
    cur = conn.cursor()
    cur.execute(sql, params)
    if cur.rowcount != 1:
        logger.warning("set_status affected %s rows (expected 1) id=%s status=%s claim_id=%s", cur.rowcount, delivery_id, status, claim_id)
    conn.commit()

def _claim_batch(channel: str, limit: int) -> list[dict]:
    if channel is None or str(channel).strip() == "":
        raise ValueError("channel is empty/None")

    conn = _connect_db_retry()
    try:
        cur = conn.cursor()
        claim = uuid.uuid4().hex

        # Atomic: lease release + claim in one write transaction
        cur.execute("BEGIN IMMEDIATE")

        if not _HAS_CLAIM_TS:
            logger.error("CRITICAL: claim_ts column missing in delivery table; lease-timeout disabled (continuing without lease)")
        else:
            lease_sec = int(LEASE_TIMEOUT_SECONDS)
            cutoff = _now_ts() - float(lease_sec)

            fields = ["status='new'", "claim_ts=NULL"]
            params_rel: list[Any] = []

            if _HAS_CLAIM_ID:
                fields.append("claim_id=NULL")

            if _HAS_UPDATED_TS:
                fields.append("updated_ts=?")
                params_rel.append(_now_ts())

            if _HAS_LAST_ERROR:
                fields.append(
                    "last_error = CASE "
                    "WHEN last_error IS NULL OR last_error='' THEN 'lease_expired' "
                    "WHEN instr(last_error,'lease_expired')>0 THEN last_error "
                    "ELSE last_error || '; lease_expired' END"
                )

            sql_rel = f"UPDATE delivery SET {', '.join(fields)} WHERE status='processing' AND claim_ts IS NOT NULL AND claim_ts < ?"
            params_rel.append(cutoff)
            cur.execute(sql_rel, params_rel)
            released = cur.rowcount
            if released:
                logger.warning("lease_timeout released=%s lease_sec=%s", released, lease_sec)

        # We only pick 'new' here (simpler). 'processing' stays for current worker.
        # Also enforce attempts < MAX_ATTEMPTS to avoid infinite loop.
        base_where = "dest_channel=? AND status='new'"
        params: list[Any] = [channel]

        if _HAS_ATTEMPTS:
            base_where += " AND COALESCE(attempts,0) < ?"
            params.append(int(MAX_ATTEMPTS))

        order = "created_ts ASC, id ASC" if _HAS_CREATED_TS else "id ASC"

        if _supports_returning_cached(conn) and _HAS_CLAIM_ID:
            sql = f"""
            UPDATE delivery
               SET status='processing',
                   claim_id=? {', claim_ts=?' if _HAS_CLAIM_TS else ''} {', updated_ts=?' if _HAS_UPDATED_TS else ''} {', last_error=NULL' if _HAS_LAST_ERROR else ''}
             WHERE id IN (
                   SELECT id
                     FROM delivery
                    WHERE {base_where}
                    ORDER BY {order}
                    LIMIT ?
             )
             RETURNING id, dest_channel, recipient_id, attempts, payload, claim_id {', created_ts' if _HAS_CREATED_TS else ''}
            """
            p = [claim]
            if _HAS_CLAIM_TS:
                p.append(_now_ts())
            if _HAS_UPDATED_TS:
                p.append(_now_ts())
            p.extend(params)
            p.append(int(limit))

            cur.execute(sql, p)
            rows = cur.fetchall()
            conn.commit()

            out = []
            for r in rows:
                out.append({
                    "id": int(r["id"]),
                    "dest_channel": channel,
                    "recipient_id": _extract_recipient_id(r),
                    "attempts": _extract_attempts(r),
                    "payload": r["payload"] if _HAS_PAYLOAD else None,
                    "created_ts": float(r["created_ts"]) if _HAS_CREATED_TS else None,
                    "claim_id": (r["claim_id"] if _HAS_CLAIM_ID else claim),
                })

            # FIFO cosmetic (RETURNING order not guaranteed)
            if _HAS_CREATED_TS:
                out.sort(key=lambda x: (x["created_ts"] or 0.0, x["id"]))
            else:
                out.sort(key=lambda x: x["id"])

            logger.info("claim_batch channel=%s n=%s claim_id=%s", channel, len(out), claim)
            return out

        # Fallback: one-shot UPDATE using subquery, then SELECT by claim_id if available, else by ids.
        # This still avoids SELECT+UPDATE race because UPDATE chooses ids internally.
        # If claim_id column missing, we still update to processing and then reselect ids via status='processing' heuristic (best-effort).
        upd_fields = ["status='processing'"]
        upd_params: list[Any] = []

        if _HAS_CLAIM_ID:
            upd_fields.append("claim_id=?")
            upd_params.append(claim)

        if _HAS_CLAIM_TS:
            upd_fields.append("claim_ts=?")
            upd_params.append(_now_ts())

        if _HAS_UPDATED_TS:
            upd_fields.append("updated_ts=?")
            upd_params.append(_now_ts())

        if _HAS_LAST_ERROR:
            upd_fields.append("last_error=NULL")

        upd_sql = f"""
        UPDATE delivery
           SET {', '.join(upd_fields)}
         WHERE id IN (
               SELECT id
                 FROM delivery
                WHERE {base_where}
                ORDER BY {order}
                LIMIT ?
         )
        """
        upd_params.extend(params)
        upd_params.append(int(limit))
        cur.execute(upd_sql, upd_params)

        # Reselect claimed rows
        if _HAS_CLAIM_ID:
            cur.execute(
                f"SELECT id, dest_channel, recipient_id, attempts, payload {', created_ts' if _HAS_CREATED_TS else ''} "
                f"FROM delivery WHERE status='processing' AND claim_id=? ORDER BY {order}",
                (claim,),
            )
        else:
            # best-effort (single worker assumed)
            cur.execute(
                f"SELECT id, dest_channel, recipient_id, attempts, payload {', created_ts' if _HAS_CREATED_TS else ''} "
                f"FROM delivery WHERE status='processing' AND dest_channel=? ORDER BY {order} LIMIT ?",
                (channel, int(limit)),
            )

        rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "id": int(r["id"]),
                "dest_channel": channel,
                "recipient_id": _extract_recipient_id(r),
                "attempts": _extract_attempts(r),
                "payload": r["payload"] if _HAS_PAYLOAD else None,
                "created_ts": float(r["created_ts"]) if _HAS_CREATED_TS else None,
                "claim_id": claim if _HAS_CLAIM_ID else None,
            })

        if _HAS_CREATED_TS:
            out.sort(key=lambda x: (x["created_ts"] or 0.0, x["id"]))
        else:
            out.sort(key=lambda x: x["id"])

        conn.commit()
        logger.info("claim_batch(fallback) channel=%s n=%s claim_id=%s", channel, len(out), claim)
        return out

    finally:
        conn.close()

async def _send_tg(text: str) -> None:
    if not TG_ALERT_TOKEN:
        raise RuntimeError("TG_ALERT_TOKEN empty")
    if TG_ALERT_CHAT_ID_INT is None:
        raise RuntimeError("TG_ALERT_CHAT_ID empty/invalid")

    http = _ensure_http()
    url = f"https://api.telegram.org/bot{TG_ALERT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_ALERT_CHAT_ID_INT, "text": text}
    r = await http.post(url, json=payload)
    r.raise_for_status()
    logger.info("TG sent ok chat_id=%s text_len=%s", TG_ALERT_CHAT_ID_INT, len(text))

async def _send_max(send_max_func, text: str, user_id: int) -> None:
    if not user_id:
        raise RuntimeError("MAX user_id empty/0")
    # send_max_func is provided by bot2_dispatch_runner (uses BOT2 token)
    await send_max_func(text, int(user_id))
    logger.info("MAX sent ok user_id=%s text_len=%s", user_id, len(text))

async def _deliver_dual(send_max_func, did: int, claim_id: Optional[str], payload: Any,
                        fallback_recipient_id: Optional[int], attempts0: int) -> None:
    """
    Variant A:
    - Always attempt TG and MAX independently.
    - recipient_id semantics for bot2:
        - MAX recipient is fixed MAX_NOTIFY_USER_ID from env (authoritative).
        - If missing, fallback to row.recipient_id (legacy) to avoid silent drop.
    - Status:
        - if at least one channel succeeded => sent
        - if both failed => retry/dead by MAX_ATTEMPTS
    """
    text = _text_from_payload(payload)
    if not text:
        text = "(empty payload)"

    # Decide MAX recipient
    max_uid = MAX_NOTIFY_USER_ID_INT or fallback_recipient_id
    tg_ok = False
    max_ok = False
    err_tg = None
    err_max = None

    # TG attempt
    try:
        await asyncio.wait_for(_send_tg(text), timeout=SEND_TIMEOUT_SEC)
        tg_ok = True
    except Exception as e:
        err_tg = repr(e)
        logger.exception("BOT2(TG) failed delivery_id=%s", did)

    # MAX attempt
    try:
        if send_max_func is None:
            raise RuntimeError("send_max_func not provided")
        if not max_uid:
            raise RuntimeError("MAX recipient unresolved (MAX_NOTIFY_USER_ID missing and row.recipient_id invalid)")
        await asyncio.wait_for(_send_max(send_max_func, text, int(max_uid)), timeout=SEND_TIMEOUT_SEC)
        max_ok = True
    except Exception as e:
        err_max = repr(e)
        logger.exception("BOT2(MAX) failed delivery_id=%s", did)

    # Commit outcome
    conn = _connect_db_retry()
    try:
        if tg_ok or max_ok:
            # success if at least one went out
            _set_status(conn, did, "sent", attempts=attempts0 + 1, claim_id=claim_id, last_error=None)
            logger.info("BOT2 done delivery_id=%s sent tg_ok=%s max_ok=%s", did, tg_ok, max_ok)
        else:
            attempts = attempts0 + 1
            last_error = f"tg={err_tg}; max={err_max}"
            if attempts >= MAX_ATTEMPTS:
                _set_status(conn, did, "dead", attempts=attempts, claim_id=claim_id, last_error=last_error)
                logger.error("BOT2 dead delivery_id=%s attempts=%s", did, attempts)
            else:
                _set_status(conn, did, "new", attempts=attempts, claim_id=claim_id, last_error=last_error)
                logger.warning("BOT2 retry delivery_id=%s attempts=%s", did, attempts)
    finally:
        conn.close()

async def _deliver_single_tg(did: int, claim_id: Optional[str], payload: Any, attempts0: int) -> None:
    text = _text_from_payload(payload)
    if not text:
        text = "(empty payload)"
    try:
        await asyncio.wait_for(_send_tg(text), timeout=SEND_TIMEOUT_SEC)
        conn = _connect_db_retry()
        try:
            _set_status(conn, did, "sent", attempts=attempts0 + 1, claim_id=claim_id)
        finally:
            conn.close()
    except Exception as e:
        conn = _connect_db_retry()
        try:
            attempts = attempts0 + 1
            if attempts >= MAX_ATTEMPTS:
                _set_status(conn, did, "dead", attempts=attempts, claim_id=claim_id, last_error=repr(e))
            else:
                _set_status(conn, did, "new", attempts=attempts, claim_id=claim_id, last_error=repr(e))
        finally:
            conn.close()
        logger.exception("TG failed delivery_id=%s attempts=%s", did, attempts0 + 1)

async def _deliver_single_max(send_max_func, did: int, claim_id: Optional[str], payload: Any,
                              recipient_id: Optional[int], attempts0: int) -> None:
    text = _text_from_payload(payload)
    if not text:
        text = "(empty payload)"
    try:
        if send_max_func is None:
            raise RuntimeError("send_max_func not provided")

        try:
            rid = int(recipient_id) if recipient_id is not None else 0
        except Exception:
            rid = 0
        if rid <= 0:
            raise RuntimeError("recipient_id missing/invalid for MAX channel row")

        await asyncio.wait_for(_send_max(send_max_func, text, rid), timeout=SEND_TIMEOUT_SEC)

        conn = _connect_db_retry()
        try:
            _set_status(conn, did, "sent", attempts=attempts0 + 1, claim_id=claim_id)
        finally:
            conn.close()
    except Exception as e:
        conn = _connect_db_retry()
        try:
            attempts = attempts0 + 1
            if attempts >= MAX_ATTEMPTS:
                _set_status(conn, did, "dead", attempts=attempts, claim_id=claim_id, last_error=repr(e))
            else:
                _set_status(conn, did, "new", attempts=attempts, claim_id=claim_id, last_error=repr(e))
        finally:
            conn.close()
        logger.exception("MAX failed delivery_id=%s attempts=%s", did, attempts0 + 1)

async def _loop(send_max_func) -> None:
    logger.info("notify_dispatch start max_batch=%s sleep=%s max_attempts=%s", MAX_BATCH, SLEEP_SEC, MAX_ATTEMPTS)
    try:
        while True:
            any_work = False

            # BOT2: dual channel (TG + MAX to personal MAX)
            for item in _claim_batch("bot2", MAX_BATCH):
                any_work = True
                did = item["id"]
                await _deliver_dual(
                    send_max_func=send_max_func,
                    did=did,
                    claim_id=item.get("claim_id"),
                    payload=item.get("payload"),
                    fallback_recipient_id=item.get("recipient_id"),
                    attempts0=int(item.get("attempts") or 0),
                )

            # TG only
            for item in _claim_batch("tg", MAX_BATCH):
                any_work = True
                await _deliver_single_tg(
                    did=item["id"],
                    claim_id=item.get("claim_id"),
                    payload=item.get("payload"),
                    attempts0=int(item.get("attempts") or 0),
                )

            # MAX only (legacy)
            for item in _claim_batch("max", MAX_BATCH):
                any_work = True
                await _deliver_single_max(
                    send_max_func=send_max_func,
                    did=item["id"],
                    claim_id=item.get("claim_id"),
                    payload=item.get("payload"),
                    recipient_id=item.get("recipient_id"),
                    attempts0=int(item.get("attempts") or 0),
                )

            await asyncio.sleep(SLEEP_SEC if any_work else max(SLEEP_SEC, 1.0))

    except asyncio.CancelledError:
        logger.info("notify_dispatch cancelled")
        raise
    except Exception:
        logger.exception("notify_dispatch loop error")
        await asyncio.sleep(1.0)

def start_notify_dispatch(send_max_func) -> None:
    global _task
    _setup_logging_failfast()
    _parse_ids_failfast()
    _preflight_db_access_failfast()

    if _task and not _task.done():
        logger.info("notify_dispatch already running")
        return

    # Helpful config warnings
    if not TG_ALERT_TOKEN or TG_ALERT_CHAT_ID_INT is None:
        logger.warning("TG not configured (TG_ALERT_TOKEN/CHAT_ID missing) -> TG sends will fail")
    if MAX_NOTIFY_USER_ID_INT is None:
        logger.warning("MAX_NOTIFY_USER_ID missing -> bot2 MAX recipient falls back to row.recipient_id (legacy)")

    _task = asyncio.create_task(_loop(send_max_func))

async def stop_notify_dispatch() -> None:
    global _task, _http
    if _task:
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
    if _http is not None:
        await _http.aclose()
        _http = None
