# PACK-S: inbound msg_id idempotency (MAX webhook)

Цель: защита от дублей входящих вебхуков на уровне БД + безопасная деградация в коде.

## Что сделали

База данных (partial UNIQUE index):

CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_in_msgid
ON messages(msg_id)
WHERE direction='in'
  AND msg_id IS NOT NULL
  AND trim(msg_id)<>'';

Код:

В /opt/max-bot1/app/db_ext.py:
INSERT INTO messages(...)
заменено на:
INSERT OR IGNORE INTO messages(...)

## Результат

- Повторная доставка webhook не создаёт дубли в messages.
- Inbound-история стала идемпотентной.
