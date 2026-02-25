import os
import json
import time
import sqlite3
from typing import Any, Dict, Optional, Tuple

DB_PATH = os.getenv("DB_PATH", "/var/lib/max-bot1/bot.db")

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.execute("PRAGMA foreign_keys=ON;")
    return c

def upsert_user_profile(
    user_id: int,
    now: float,
    sender: Dict[str, Any],
    chat_id: Optional[int],
    text: Optional[str],
) -> bool:
    """
    Returns True if first time (NEW user), else False.
    """
    c = _conn()
    try:
        row = c.execute("SELECT 1 FROM users_profile WHERE user_id=?", (user_id,)).fetchone()
        if row is None:
            c.execute(
                """INSERT INTO users_profile(
                      user_id, first_seen_ts, last_seen_ts,
                      display_name, username,
                      last_chat_id, last_text
                   ) VALUES (?,?,?,?,?,?,?)""",
                (
                    user_id,
                    now,
                    now,
                    (sender.get("display_name") or sender.get("name")),
                    sender.get("username"),
                    chat_id,
                    text,
                ),
            )
            c.commit()
            return True

        c.execute(
            """UPDATE users_profile
               SET last_seen_ts=?,
                   display_name=COALESCE(?, display_name),
                   username=COALESCE(?, username),
                   last_chat_id=COALESCE(?, last_chat_id),
                   last_text=COALESCE(?, last_text)
               WHERE user_id=?""",
            (
                now,
                (sender.get("display_name") or sender.get("name")),
                sender.get("username"),
                chat_id,
                text,
                user_id,
            ),
        )
        c.commit()
        return False
    finally:
        c.close()

def upsert_conversation(
    chat_id: int,
    user_id: int,
    now: float,
    ttl_minutes: int = 60,
) -> Tuple[str, float]:
    """
    Returns (label, ttl_until_ts)
      label: NEW | CONTINUE | RETURN
    """
    ttl_until = now + ttl_minutes * 60

    c = _conn()
    try:
        row = c.execute(
            "SELECT ttl_until_ts FROM conversations WHERE chat_id=?",
            (chat_id,),
        ).fetchone()

        if row is None:
            c.execute(
                """INSERT INTO conversations(
                      chat_id, user_id, created_ts, updated_ts,
                      status, stage, ttl_until_ts
                   ) VALUES (?,?,?,?,?,?,?)""",
                (chat_id, user_id, now, now, "open", "start", ttl_until),
            )
            c.commit()
            return "NEW", ttl_until

        prev_ttl = float(row[0]) if row[0] is not None else 0.0
        label = "CONTINUE" if now <= prev_ttl else "RETURN"

        c.execute(
            """UPDATE conversations
               SET user_id=?, updated_ts=?, status='open', ttl_until_ts=?
               WHERE chat_id=?""",
            (user_id, now, ttl_until, chat_id),
        )
        c.commit()
        return label, ttl_until
    finally:
        c.close()

def log_message(
    chat_id: int,
    user_id: int,
    now: float,
    direction: str,          # in|out
    text: Optional[str],
    msg_id: Optional[str],
    raw_update: Optional[Dict[str, Any]],
) -> None:
    raw_json = json.dumps(raw_update, ensure_ascii=False) if raw_update is not None else None

    c = _conn()
    try:
        c.execute(
            """INSERT INTO messages(ts, user_id, chat_id, direction, msg_id, text, raw_json)
               VALUES (?,?,?,?,?,?,?)""",
            (now, user_id, chat_id, direction, msg_id, text, raw_json),
        )
        c.commit()
    finally:
        c.close()

def build_deeplink(user_id: int, chat_id: int) -> str:
    tpl = os.getenv("MAX_LINK_TEMPLATE", "").strip()
    if not tpl:
        return ""
    return tpl.format(user_id=user_id, chat_id=chat_id)
