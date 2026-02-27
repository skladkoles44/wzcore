# DELIVERY STRICT MODE

Date: 2026-02-27

## What changed

Delivery is strictly idempotent.

UNIQUE(event_id, dest_channel, recipient_id)

Producer insert:
INSERT ... ON CONFLICT(event_id, dest_channel, recipient_id) DO NOTHING

## Result

- Duplicate deliveries impossible at DB level
- Safe under retries
- Dispatcher remains pure consumer
- Verified: PRAGMA quick_check = ok
