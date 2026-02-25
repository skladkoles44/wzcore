import os
import time
import json
import sqlite3
from typing import Any

DB_PATH = os.getenv("DB_PATH", "/var/lib/max-bot1/bot.db")

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {r[1] for r in cur.fetchall()}  # name

def assert_tables_exist() -> None:
    # Ничего не пересоздаём: у тебя уже прод-схема.
    return

def create_event_and_delivery(
    *,
    event_id: str,
    user_id: int,
    chat_id: int,
    notify_text: str,
    is_new: int = 0,
    created_ts: float | None = None,
    updated_ts: float | None = None,  # backward compat
    source_channel: str = "max",
    event_type: str = "message_received",
    meta_json: str | None = None,
    raw_json: str | None = None,
) -> None:
    """
    Совместимо с твоей текущей schema:

    events: NOT NULL source_channel, event_type, received_ts, source_user_id, source_chat_id, created_ts (unixepoch())
    delivery: NOT NULL kind, dest_channel, recipient_id, status, attempts, created_ts, updated_ts (+ payload)
    """

    now = time.time()
    ts = float(created_ts if created_ts is not None else (updated_ts if updated_ts is not None else now))
    received_ts = ts

    notify_text = (notify_text or "")
    if len(notify_text) > 3500:
        notify_text = notify_text[:3500]

    with _conn() as conn:
        assert_tables_exist()
        cur = conn.cursor()

        # -------- events --------
        ev_cols = _cols(conn, "events")
        ev: dict[str, Any] = {}

        # required in your schema
        ev["event_id"] = str(event_id)
        if "source_channel" in ev_cols: ev["source_channel"] = source_channel
        if "event_type" in ev_cols: ev["event_type"] = event_type
        if "received_ts" in ev_cols: ev["received_ts"] = received_ts
        if "source_user_id" in ev_cols: ev["source_user_id"] = int(user_id)
        if "source_chat_id" in ev_cols: ev["source_chat_id"] = int(chat_id)

        # payload/text fields
        if "text" in ev_cols: ev["text"] = notify_text
        if "notify_text" in ev_cols: ev["notify_text"] = notify_text

        # optional denorm
        if "user_id" in ev_cols: ev["user_id"] = int(user_id)
        if "chat_id" in ev_cols: ev["chat_id"] = int(chat_id)

        # flags/timestamps
        if "is_new" in ev_cols: ev["is_new"] = int(is_new)
        if "created_ts" in ev_cols: ev["created_ts"] = ts

        if "meta_json" in ev_cols and meta_json is not None: ev["meta_json"] = meta_json
        if "raw_json" in ev_cols and raw_json is not None: ev["raw_json"] = raw_json

        keys = list(ev.keys())
        placeholders = ",".join(["?"] * len(keys))
        collist = ",".join(keys)
        upd_keys = [k for k in keys if k != "event_id"]
        upd_expr = ",".join([f"{k}=excluded.{k}" for k in upd_keys])

        cur.execute(
            f"""
            INSERT INTO events ({collist})
            VALUES ({placeholders})
            ON CONFLICT(event_id) DO UPDATE SET {upd_expr}
            """,
            [ev[k] for k in keys],
        )

        # -------- delivery --------
        dl_cols = _cols(conn, "delivery")

        # В твоей схеме (по PRAGMA): kind, dest_channel, recipient_id, payload, status, attempts, last_error, claim_id, claim_ts, next_retry_ts, created_ts, updated_ts, channel, error
        payload_obj = {
            "event_id": str(event_id),
            "user_id": int(user_id),
            "chat_id": int(chat_id),
            "notify_text": notify_text,
            "is_new": int(is_new),
            "source_channel": source_channel,
            "event_type": event_type,
            "created_ts": ts,
        }
        payload_str = json.dumps(payload_obj, ensure_ascii=False)

        dl: dict[str, Any] = {}
        dl["event_id"] = str(event_id)
        if "payload" in dl_cols:
            dl["payload"] = payload_str

        if "kind" in dl_cols:
            dl["kind"] = "bot2"  # тип доставки для диспетчера
        if "dest_channel" in dl_cols:
            dl["dest_channel"] = "bot2"
        if "recipient_id" in dl_cols:
            # кому слать: admin user_id (MAX_NOTIFY_USER_ID/ADMIN_USER_ID), иначе fallback на user_id клиента
            uid_env = os.getenv("MAX_NOTIFY_USER_ID") or os.getenv("ADMIN_USER_ID")
            if uid_env and str(uid_env).strip():
                dl["recipient_id"] = str(int(uid_env))
            else:
                dl["recipient_id"] = str(int(user_id))
        if "status" in dl_cols:
            dl["status"] = "new"

        if "attempts" in dl_cols:
            dl["attempts"] = 0

        # ошибки/клеймы — пустые
        if "last_error" in dl_cols:
            dl["last_error"] = None
        if "claim_id" in dl_cols:
            dl["claim_id"] = None
        if "claim_ts" in dl_cols:
            dl["claim_ts"] = None
        if "next_retry_ts" in dl_cols:
            dl["next_retry_ts"] = None

        if "created_ts" in dl_cols:
            dl["created_ts"] = now
        if "updated_ts" in dl_cols:
            dl["updated_ts"] = now

        # новые поля, которые ты уже видишь (channel/error)
        if "channel" in dl_cols:
            dl["channel"] = "bot2"
        if "error" in dl_cols:
            dl["error"] = None

        dl_keys = list(dl.keys())
        dl_placeholders = ",".join(["?"] * len(dl_keys))
        dl_collist = ",".join(dl_keys)

        cur.execute(
            f"INSERT INTO delivery ({dl_collist}) VALUES ({dl_placeholders})",
            [dl[k] for k in dl_keys],
        )

        conn.commit()
