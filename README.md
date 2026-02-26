# WZCore — Deterministic Message Processing & Dispatch (Bot1 + Bot2 + SQLite)

This repository contains:
- **facts-only docs** of the system behavior (state machine + flows)
- **deploy artifacts** (systemd units)
- **runtime snapshot** of the currently running implementation (Bot1/Bot2)

It is intentionally **domain-agnostic**: it documents *mechanics*, not *business*.

## What you will find here

- `src/opt/max-bot1/` — Bot1 runtime snapshot (ingest/normalize/persist)
- `src/opt/max-bot2/` — Bot2 runtime snapshot (claim/lease/retry/dispatch)
- `deploy_systemd/` — systemd units used in production
- `docs/` — authoritative docs (overview + RFC-style delivery state machine)

## Proof points (engineering)

- SQLite-backed durability (events + delivery tables)
- deterministic retry scheduling (e.g. `next_retry_ts`)
- lease/claim model for safe dispatch
- retention worker/timer

## Quick start

See: `QUICKSTART.md`

## Versioning / IP

- `VERSION` — current version marker
- `CHANGELOG.md` — change log
- `LICENSE` — IP/license statement

## Runtime Contract

See: docs/runtime/CONTRACT.md
