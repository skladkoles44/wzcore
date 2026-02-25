import os
import sqlite3
from contextlib import closing

DB_PATH = os.getenv("DB_PATH", "/var/lib/max-bot1/bot.db")

DDL = [
    "PRAGMA foreign_keys=ON;",
    """
    CREATE TABLE IF NOT EXISTS users_profile (
      user_id        INTEGER PRIMARY KEY,
      first_seen_ts  REAL    NOT NULL,
      last_seen_ts   REAL    NOT NULL,
      display_name   TEXT,
      username       TEXT,
      phone          TEXT,
      city           TEXT,
      geo_lat        REAL,
      geo_lon        REAL,
      geo_ts         REAL,
      last_chat_id   INTEGER,
      last_text      TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS conversations (
      chat_id        INTEGER PRIMARY KEY,
      user_id        INTEGER NOT NULL,
      created_ts     REAL    NOT NULL,
      updated_ts     REAL    NOT NULL,
      status         TEXT    NOT NULL DEFAULT 'open',
      stage          TEXT    NOT NULL DEFAULT 'start',
      ttl_until_ts   REAL,
      FOREIGN KEY(user_id) REFERENCES users_profile(user_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
      msg_id         INTEGER PRIMARY KEY AUTOINCREMENT,
      chat_id        INTEGER NOT NULL,
      user_id        INTEGER NOT NULL,
      created_ts     REAL    NOT NULL,
      direction      TEXT    NOT NULL,          -- 'in' | 'out'
      text           TEXT,
      payload_json   TEXT,
      FOREIGN KEY(chat_id) REFERENCES conversations(chat_id),
      FOREIGN KEY(user_id) REFERENCES users_profile(user_id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON conversations(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id);",
    "CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id);",
]

def run() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA foreign_keys=ON;")
        cur = con.cursor()
        for stmt in DDL:
            cur.execute(stmt)
        con.commit()

if __name__ == "__main__":
    run()
    print("OK_MIGRATE_B2C")
