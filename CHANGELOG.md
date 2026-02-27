# Changelog

## 1.0.0 – Initial Documentation Release (2026-02-25)

- System Overview added
- Delivery State Machine (RFC) added
- Sequence diagram added
- README rewritten
- License and versioning introduced

## 2026-02-27
- Delivery queue hardened (strict idempotency)
- UNIQUE(event_id, dest_channel, recipient_id)
- Producer uses ON CONFLICT DO NOTHING

## 2026-02-27
- Delivery queue hardened (strict idempotency)
- UNIQUE(event_id, dest_channel, recipient_id)
- Producer uses ON CONFLICT DO NOTHING
