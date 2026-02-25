PRAGMA foreign_keys=ON;

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

CREATE TABLE IF NOT EXISTS conversations (
  chat_id        INTEGER PRIMARY KEY,
  user_id        INTEGER NOT NULL,
  created_ts     REAL    NOT NULL,
  updated_ts     REAL    NOT NULL,
  status         TEXT    NOT NULL DEFAULT "open",
  stage          TEXT    NOT NULL DEFAULT "start",
  ttl_until_ts   REAL,
  FOREIGN KEY(user_id) REFERENCES users_profile(user_id)
);

CREATE TABLE IF NOT EXISTS messages (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  ts             REAL    NOT NULL,
  user_id        INTEGER NOT NULL,
  chat_id        INTEGER NOT NULL,
  direction      TEXT    NOT NULL,
  msg_id         TEXT,
  text           TEXT,
  raw_json       TEXT,
  FOREIGN KEY(user_id) REFERENCES users_profile(user_id),
  FOREIGN KEY(chat_id) REFERENCES conversations(chat_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_user_ts ON messages(user_id, ts);
CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_id, ts);
