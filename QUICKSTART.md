# QUICKSTART (Operational)

This repo is a **runtime snapshot** + **deployment artifacts**.

## 1) Minimal requirements

- Linux host with systemd (for production-style run)
- Python 3.12+
- Network access to your inbound platform and delivery channels (configured via env)

## 2) Layout

- Bot1 code: `src/opt/max-bot1/app/app.py`
- Bot2 runner: `src/opt/max-bot2/app/bot2_dispatch_runner.py`
- systemd units: `deploy_systemd/*.service` and `deploy_systemd/*.timer`

## 3) Configure

Copy and edit:

- `.env.example` → `.env`

## 4) Run (development)

Example (adjust paths/env to your host):

- Bot1: run the FastAPI app (webhook receiver)
- Bot2: run the dispatch runner loop

This repository does **not** ship a one-command installer because it reflects a production host layout.
For a clean install path, use the systemd units in `deploy_systemd/` as the source of truth.

## 5) Docs

- `docs/System_Overview.md`
- `docs/Delivery_State_Machine.md`
- `docs/diagrams/sequence.mmd`
