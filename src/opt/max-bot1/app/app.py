import os
import json
import time
import asyncio
import logging
import sqlite3
import hashlib
from typing import Any
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request, BackgroundTasks

# Project modules (must exist in /opt/max-bot1/app)
from notify_store import create_event_and_delivery, assert_tables_exist
from db_ext import (
    upsert_user_profile,
    upsert_conversation,
    log_message,
    build_deeplink,
)

# =============== CONFIG ===============
API_BASE = "https://platform-api.max.ru"

# IMPORTANT: in prod we expect systemd EnvironmentFile to provide this
BOT1_TOKEN = os.environ.get("BOT1_TOKEN", "").strip()

AUTO_REPLY_TEXT = os.getenv(
    "AUTO_REPLY_TEXT",
    "Здравствуйте 👋 Мы получили ваше сообщение. И свяжемся с вами в ближайшее время.",
)

# IMPORTANT: default must point to shared persistent DB
DB_PATH = os.getenv("DB_PATH", "/var/lib/max-bot1/bot.db")

DEDUP_TTL_HOURS = int(os.getenv("DEDUP_TTL_HOURS", "24"))
CLIENT_TTL_DAYS = int(os.getenv("CLIENT_TTL_DAYS", "180"))
PROCESSING_STALE_MINUTES = int(os.getenv("PROCESSING_STALE_MINUTES", "5"))
FAILED_RETRY_MINUTES = int(os.getenv("FAILED_RETRY_MINUTES", "30"))
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "3600"))
HANDLE_TIMEOUT = int(os.getenv("HANDLE_TIMEOUT", "30"))

RETRY_COUNT = int(os.getenv("RETRY_COUNT", "3"))
BASE_RETRY_DELAY = float(os.getenv("BASE_RETRY_DELAY", "2"))

NOTIFY_MAX_LEN = int(os.getenv("NOTIFY_MAX_LEN", "3500"))
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "3"))
WEBHOOK_BODY_TTL_HOURS = int(os.getenv("WEBHOOK_BODY_TTL_HOURS", "24"))

# =============== LOGGING ===============
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("max-bot")

# =============== TIME (MSK) ===============
try:
    from zoneinfo import ZoneInfo

    MSK = ZoneInfo("Europe/Moscow")
except Exception:
    MSK = None


def _to_utc_dt(ts_any: Any) -> datetime:
    now_utc = datetime.now(timezone.utc)

    if ts_any is None:
        return now_utc

    if isinstance(ts_any, (int, float)):
        v = float(ts_any)
        if v > 1e11:  # ms
            v /= 1000.0
        try:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return now_utc

    if isinstance(ts_any, str):
        t = ts_any.strip()
        if not t:
            return now_utc

        # numeric string
        try:
            v = float(t)
            if v > 1e11:  # ms
                v /= 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            pass

        # ISO string
        try:
            t2 = t.replace("Z", "+00:00")
            dt = datetime.fromisoformat(t2)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return now_utc

    return now_utc


def format_msk(ts_any: Any) -> str:
    dt_utc = _to_utc_dt(ts_any)
    if MSK is None:
        return dt_utc.strftime("%Y-%m-%d %H:%M:%S")
    return dt_utc.astimezone(MSK).strftime("%Y-%m-%d %H:%M:%S")


# =============== HTTP ===============
_timeout = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0)
client = httpx.AsyncClient(timeout=_timeout)


async def send_message(
    token: str,
    *,
    user_id: int | None = None,
    chat_id: int | None = None,
    text: str,
) -> bool:
    if (user_id is None) == (chat_id is None):
        raise ValueError("send_message: pass exactly one of user_id or chat_id")

    if not token:
        logger.error("BOT1_TOKEN is empty: cannot send")
        return False

    headers = {"Authorization": token}
    params = {"user_id": user_id} if user_id is not None else {"chat_id": chat_id}

    text = (text or "")
    if len(text) > NOTIFY_MAX_LEN:
        text = text[:NOTIFY_MAX_LEN]

    to_val = chat_id if chat_id is not None else user_id

    for attempt in range(RETRY_COUNT + 1):
        try:
            r = await client.post(
                f"{API_BASE}/messages",
                params=params,
                json={"text": text},
                headers=headers,
            )
            r.raise_for_status()
            logger.info(
                "sent_success to=%s text_len=%d attempt=%d",
                to_val,
                len(text),
                attempt + 1,
            )
            return True

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            body = e.response.text
            logger.error("MAX send HTTP %s body=%s", status, body)

            if status == 429:
                retry_after = int(e.response.headers.get("Retry-After", "5"))
                await asyncio.sleep(retry_after)
                continue

            if 500 <= status < 600:
                await asyncio.sleep(min(BASE_RETRY_DELAY * (2**attempt), 60.0))
                continue

            return False

        except httpx.RequestError as e:
            logger.error("MAX send network error: %s", str(e))
            await asyncio.sleep(min(BASE_RETRY_DELAY * (2**attempt), 60.0))

    return False


# =============== DB ===============
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    with get_conn() as conn:
        cur = conn.cursor()

        # dedupe (event_id is our canonical key)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dedupe (
                event_id   TEXT PRIMARY KEY,
                updated_ts REAL NOT NULL,
                status     TEXT NOT NULL,
                error      TEXT
            )
            """
        )

        # clients
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS clients (
                user_id       INTEGER PRIMARY KEY,
                last_seen_ts  REAL NOT NULL
            )
            """
        )

        # rate_limit (persistent)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rate_limit (
                user_id       INTEGER PRIMARY KEY,
                last_seen_ts  REAL NOT NULL
            )
            """
        )

        # webhook_seen (replay protection)
        # we keep BOTH created_ts and updated_ts for debug/retention
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_seen (
                body_sha256  TEXT PRIMARY KEY,
                created_ts   REAL NOT NULL,
                updated_ts   REAL NOT NULL
            )
            """
        )

        # indexes
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dedupe_status_updated ON dedupe(status, updated_ts)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_webhook_seen_updated ON webhook_seen(updated_ts)"
        )

        conn.commit()


def _sqlite_retry(op, max_retries: int = 5, base_delay: float = 0.05):
    for attempt in range(max_retries):
        try:
            return op()
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay * (2**attempt))


def seen_or_mark_body(body_bytes: bytes) -> bool:
    """
    True  => seen before (replay)
    False => first time
    """
    h = hashlib.sha256(body_bytes).hexdigest()
    now = time.time()

    def _op():
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT created_ts FROM webhook_seen WHERE body_sha256=?", (h,))
            existed = cur.fetchone() is not None

            if not existed:
                cur.execute(
                    "INSERT INTO webhook_seen (body_sha256, created_ts, updated_ts) VALUES (?, ?, ?)",
                    (h, now, now),
                )
            else:
                cur.execute(
                    "UPDATE webhook_seen SET updated_ts=? WHERE body_sha256=?",
                    (now, h),
                )

            conn.commit()
            return existed
        finally:
            conn.close()

    return bool(_sqlite_retry(_op))


def cleanup_tables() -> tuple[int, int, int, int]:
    """
    Return (dedupe_deleted, dedupe_marked_stale, webhook_seen_deleted, rate_limit_deleted).
    """
    now = time.time()
    done_thr = now - (DEDUP_TTL_HOURS * 3600)
    stale_thr = now - (PROCESSING_STALE_MINUTES * 60)
    wh_thr = now - (WEBHOOK_BODY_TTL_HOURS * 3600)

    def _op():
        conn = get_conn()
        try:
            cur = conn.cursor()

            # reclaim stale processing => mark failed
            cur.execute(
                "UPDATE dedupe SET status='failed', error='stale_timeout', updated_ts=? "
                "WHERE status='processing' AND updated_ts < ?",
                (now, stale_thr),
            )
            marked = cur.rowcount

            # delete done/failed older than TTL
            cur.execute(
                "DELETE FROM dedupe WHERE status IN ('done','failed') AND updated_ts < ?",
                (done_thr,),
            )
            deleted = cur.rowcount

            # webhook_seen retention by updated_ts
            cur.execute("DELETE FROM webhook_seen WHERE updated_ts < ?", (wh_thr,))
            wh_deleted = cur.rowcount

            # rate_limit retention (90 days)
            cur.execute("DELETE FROM rate_limit WHERE last_seen_ts < ?", (now - 90 * 86400,))
            rl_deleted = cur.rowcount

            conn.commit()
            return deleted, marked, wh_deleted, rl_deleted
        finally:
            conn.close()

    return _sqlite_retry(_op)


_cleanup_task: asyncio.Task | None = None


async def periodic_cleanup():
    try:
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL)
            try:
                d, m, w, r = cleanup_tables()
                logger.info(
                    "cleanup: dedupe_deleted=%s dedupe_stale_marked=%s webhook_seen_deleted=%s rate_limit_deleted=%s",
                    d,
                    m,
                    w,
                    r,
                )
            except Exception:
                logger.exception("cleanup failed")
    except asyncio.CancelledError:
        logger.info("cleanup task cancelled")


# =============== DEDUPE / IDs ===============
def get_message_id(update: dict[str, Any]) -> str:
    """
    Stable message id for dedupe.
    Prefer native mid. If absent, build deterministic surrogate from stable fields.
    """
    msg = update.get("message") or {}
    body = msg.get("body") or {}

    mid = body.get("mid")
    if mid:
        return str(mid)

    sender_id = (msg.get("sender") or {}).get("user_id", "")
    chat_id = (msg.get("recipient") or {}).get("chat_id", "")
    seq = body.get("seq", "")
    date = body.get("date", "")

    text = body.get("text") or ""
    text_h = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

    stable = f"{sender_id}|{chat_id}|{seq}|{date}|{text_h}"
    return "surr." + hashlib.sha256(stable.encode("utf-8")).hexdigest()


def acquire_processing(event_id: str) -> bool:
    """
    Two-phase acquire with BEGIN IMMEDIATE.
    True if acquired for processing.
    """
    now = time.time()
    stale_age = PROCESSING_STALE_MINUTES * 60
    failed_retry_age = FAILED_RETRY_MINUTES * 60

    def _op():
        conn = get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.cursor()

            cur.execute("SELECT status, updated_ts FROM dedupe WHERE event_id=?", (event_id,))
            row = cur.fetchone()

            if row:
                status, updated_ts = row
                updated_ts = float(updated_ts)

                if status == "done":
                    conn.rollback()
                    return False

                if status == "processing":
                    if now - updated_ts <= stale_age:
                        conn.rollback()
                        return False
                    logger.warning("reclaiming stale processing event_id=%s", event_id)
                    cur.execute(
                        "UPDATE dedupe SET updated_ts=?, status='processing', error=NULL WHERE event_id=?",
                        (now, event_id),
                    )
                    conn.commit()
                    return True

                if status == "failed":
                    if now - updated_ts <= failed_retry_age:
                        conn.rollback()
                        return False
                    logger.warning("reclaiming failed event_id=%s", event_id)
                    cur.execute(
                        "UPDATE dedupe SET updated_ts=?, status='processing', error=NULL WHERE event_id=?",
                        (now, event_id),
                    )
                    conn.commit()
                    return True

                conn.rollback()
                return False

            # insert new claim
            try:
                cur.execute(
                    "INSERT INTO dedupe (event_id, updated_ts, status, error) VALUES (?, ?, 'processing', NULL)",
                    (event_id, now),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                conn.rollback()
                return False
        finally:
            conn.close()

    return bool(_sqlite_retry(_op))


def mark_done(event_id: str, error: str | None = None) -> None:
    now = time.time()

    def _op():
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE dedupe SET status='done', error=?, updated_ts=? WHERE event_id=?",
                (error, now, event_id),
            )
            conn.commit()
        finally:
            conn.close()

    _sqlite_retry(_op)


def mark_failed(event_id: str, error: str | None = None) -> None:
    now = time.time()

    def _op():
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE dedupe SET status='failed', error=?, updated_ts=? WHERE event_id=? AND status='processing'",
                (error, now, event_id),
            )
            conn.commit()
        finally:
            conn.close()

    _sqlite_retry(_op)


# =============== CLIENT STATUS ===============
def is_new_client(user_id: int) -> bool:
    now = time.time()
    thr = CLIENT_TTL_DAYS * 86400

    def _op():
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT last_seen_ts FROM clients WHERE user_id=?", (user_id,))
            row = cur.fetchone()

            if not row:
                cur.execute(
                    "INSERT INTO clients (user_id, last_seen_ts) VALUES (?, ?)",
                    (user_id, now),
                )
                conn.commit()
                return True

            last_seen = float(row[0])
            is_new = (now - last_seen) > thr

            cur.execute("UPDATE clients SET last_seen_ts=? WHERE user_id=?", (now, user_id))
            conn.commit()
            return is_new
        finally:
            conn.close()

    return bool(_sqlite_retry(_op))


# =============== RATE LIMIT (persistent) ===============
def check_rate_limit(user_id: int) -> bool:
    # IMPORTANT: epoch seconds (time.time), NOT loop monotonic time
    now = time.time()

    def _op():
        conn = get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.cursor()

            cur.execute("SELECT last_seen_ts FROM rate_limit WHERE user_id=?", (user_id,))
            row = cur.fetchone()
            if row is not None:
                last = float(row[0])
                if now - last < RATE_LIMIT_SECONDS:
                    conn.rollback()
                    return False

            cur.execute(
                """
                INSERT INTO rate_limit (user_id, last_seen_ts)
                VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET last_seen_ts=excluded.last_seen_ts
                """,
                (user_id, now),
            )
            conn.commit()
            return True
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    return bool(_sqlite_retry(_op))


# =============== FASTAPI ===============
app = FastAPI()


@app.on_event("startup")
async def startup():
    init_db()
    # external tables used by Bot2 dispatcher contract
    assert_tables_exist()

    global _cleanup_task
    _cleanup_task = asyncio.create_task(periodic_cleanup())


@app.on_event("shutdown")
async def shutdown():
    global _cleanup_task
    if _cleanup_task:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
    await client.aclose()


@app.get("/health")
async def health():
    try:
        with get_conn() as conn:
            conn.execute("SELECT 1")
        return {
            "status": "ok",
            "db_path": DB_PATH,
            "db_connected": True,
            "token_present": bool(BOT1_TOKEN),
        }
    except Exception as e:
        logger.error("health db check failed: %s", e)
        return {
            "status": "error",
            "db_path": DB_PATH,
            "db_connected": False,
            "token_present": bool(BOT1_TOKEN),
            "err": str(e),
        }


@app.post("/webhook")
async def webhook(req: Request, bg: BackgroundTasks):
    body_bytes = await req.body()

    # Always try parse JSON, but never fail webhook
    try:
        update = json.loads(body_bytes.decode("utf-8"))
    except Exception as e:
        logger.exception("invalid_json")
        return {"ok": True, "invalid_json": True, "err": str(e), "db_path": DB_PATH}

    # replay protection on raw body
    try:
        if seen_or_mark_body(body_bytes):
            return {"ok": True, "replay": True}
    except Exception:
        logger.exception("webhook_seen failed")
        # IMPORTANT: still 200 to avoid upstream retry storm
        return {"ok": True, "webhook_seen_failed": True, "db_path": DB_PATH}

    bg.add_task(handle_update, update)
    return {"ok": True}


async def handle_update(update: dict[str, Any]):
    try:
        await asyncio.wait_for(_handle(update), timeout=HANDLE_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error("handle timeout")
    except Exception:
        logger.exception("unhandled update error")


async def _handle(update: dict[str, Any]):
    ut = update.get("update_type")
    if ut not in ("message_created", "message_new"):
        logger.info("ignore update_type=%s", ut)
        return

    msg = update.get("message") or {}
    sender = msg.get("sender") or {}
    recipient = msg.get("recipient") or {}
    body = msg.get("body") or {}

    sender_user_id = sender.get("user_id")
    sender_is_bot = bool(sender.get("is_bot", False))
    chat_id = recipient.get("chat_id")

    if not sender_user_id or not chat_id:
        return

    # ignore bots only (admin НЕ игнорим)
    if sender_is_bot:
        return

    # validate IDs early (avoid ValueError later)
    try:
        sender_uid_int = int(sender_user_id)
        chat_id_int = int(chat_id)
    except (ValueError, TypeError):
        logger.warning("invalid_id sender_user_id=%r chat_id=%r", sender_user_id, chat_id)
        return

    # persistent rate limit by sender_user_id (including admin)
    if not check_rate_limit(sender_uid_int):
        return

    event_id = get_message_id(update)

    # acquire AFTER validation + rate-limit
    if not acquire_processing(event_id):
        return

    text = (body.get("text") or "").strip()
    msg_ts = body.get("date") or update.get("timestamp")
    time_msk = format_msk(msg_ts)

    # ===== Persist: profile + conversation + incoming message =====
    label = None
    deep_link = ""

    try:
        now = time.time()
        is_first = upsert_user_profile(
            user_id=sender_uid_int,
            now=now,
            sender=sender,
            chat_id=chat_id_int,
            text=text or "",
        )

        label, _ttl_until = upsert_conversation(
            chat_id=chat_id_int,
            user_id=sender_uid_int,
            now=now,
            ttl_minutes=int(os.getenv("DIALOG_TTL_MINUTES", "60")),
        )

        if is_first:
            label = "NEW"

        log_message(
            chat_id=chat_id_int,
            user_id=sender_uid_int,
            now=now,
            direction="in",
            text=text or "",
            msg_id=event_id,
            raw_update=update,
        )

        deep_link = build_deeplink(user_id=sender_uid_int, chat_id=chat_id_int)
        if (not deep_link) or (not str(deep_link).strip()):
            deep_link = f"chat_id={chat_id_int}"

        # ===== MVP Notification v1 =====
        try:
            # normalize msg_ts -> REAL unix seconds
            try:
                ts = _to_utc_dt(msg_ts).timestamp()
            except Exception:
                ts = time.time()
                logger.warning("invalid msg_ts=%r, using now", msg_ts)

            notify_text = (
                f"[{time_msk}] "
                f"{(label or 'MSG')} "
                f"uid={sender_uid_int} chat={chat_id_int} "
                f"user={(sender.get('username') or '')} "
                f"login={(sender.get('login') or '')} "
                f"name={(sender.get('first_name') or '')} {(sender.get('last_name') or '')}\n"
                f"link={deep_link}\n"
                f"{text or ''}"
            )[:NOTIFY_MAX_LEN]

            create_event_and_delivery(
                event_id=str(event_id),
                user_id=sender_uid_int,
                chat_id=chat_id_int,
                notify_text=notify_text,
                is_new=1 if (label == "NEW") else 0,
                created_ts=ts,
            )
        except TypeError:
            # fallback if older notify_store expects updated_ts instead of created_ts
            create_event_and_delivery(
                event_id=str(event_id),
                user_id=sender_uid_int,
                chat_id=chat_id_int,
                notify_text=notify_text,
                is_new=1 if (label == "NEW") else 0,
                updated_ts=ts,
            )
        except Exception:
            logger.exception("notify persist failed")

    except Exception:
        logger.exception("db_ext persist failed")

    # ===== Auto-reply =====
    ok1 = await send_message(BOT1_TOKEN, chat_id=chat_id_int, text=AUTO_REPLY_TEXT)

    # log outgoing only if we tried to reply
    try:
        log_message(
            chat_id=chat_id_int,
            user_id=sender_uid_int,
            now=time.time(),
            direction="out",
            text=AUTO_REPLY_TEXT,
            msg_id=None,
            raw_update=None,
        )
    except Exception:
        logger.exception("db_ext outgoing log failed")

    if ok1:
        mark_done(event_id)
    else:
        mark_failed(event_id, error="auto_reply_failed")
