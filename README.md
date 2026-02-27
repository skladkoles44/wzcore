WZCore — Deterministic Runtime Sandbox



This sandbox demonstrates deterministic, replayable, idempotent message processing mechanics.
WZCore is minimal and domain-agnostic. It contains no business logic — only runtime mechanics.


---

Supported Features

Deterministic replay

Idempotent event handling

Retry-aware duplicate detection

Zero-side-effect dry-run

Explicit state machine transitions



---

Repository Structure

src/
  wzcore_sandbox/
    app.py
    state_machine.py
tests/
docs/
deploy_systemd/
.github/workflows/

Runtime entrypoint: 👉 src/wzcore_sandbox/app.py
State machine implementation: 👉 src/wzcore_sandbox/state_machine.py


---

Quick Start

Supported Python: 3.12

python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pytest -q
uvicorn wzcore_sandbox.app:app --reload


---

Health Check

curl -s http://127.0.0.1:8000/health

Expected response:

{"status": "ok"}


---

Runtime API

POST /runtime/handle

Input:

{
  "event_id": "e1",
  "attempt": 1,
  "dry_run": false,
  "simulate_fail": false
}

Output:

{
  "event_id": "e1",
  "state": "SUCCESS",
  "is_new": 1,
  "is_duplicate": false,
  "transitions": [
    { "from": "INIT", "to": "PROCESSING" },
    { "from": "PROCESSING", "to": "SUCCESS" }
  ]
}


---

Deterministic Semantics (Proof Points)

1️⃣ Dry-run has zero side effects

curl -s -X POST http://127.0.0.1:8000/runtime/handle \
  -H "Content-Type: application/json" \
  -d '{"event_id":"dry1","attempt":1,"dry_run":true}'

Then real call:

curl -s -X POST http://127.0.0.1:8000/runtime/handle \
  -H "Content-Type: application/json" \
  -d '{"event_id":"dry1","attempt":1}'

Result: is_new = 1 — dry-run does not persist state.


---

2️⃣ Duplicate detection only on retry

First call:

{"event_id":"e2","attempt":1}

Retry:

{"event_id":"e2","attempt":2}

Second response:

{
  "is_new": 0,
  "is_duplicate": true
}

Duplicate detection requires:

stored state exists

previous state == SUCCESS

attempt > 1



---

3️⃣ Replay semantics (deterministic core)

curl -s -X POST http://127.0.0.1:8000/runtime/handle \
  -H "Content-Type: application/json" \
  -d '{"event_id":"x","attempt":1}'

Then:

curl -s -X POST http://127.0.0.1:8000/runtime/handle \
  -H "Content-Type: application/json" \
  -d '{"event_id":"x","attempt":2}'

Transitions are replayed from stored state.
Behavior is deterministic and stable across retries.


---

Architecture Overview

flowchart TD

Client -->|POST /runtime/handle| Runtime
Runtime -->|lookup| Store[_EVENTS]
Runtime -->|replay transitions| SM[StateMachine]
SM -->|step()| SM
Runtime -->|persist if not dry-run| Store
Runtime --> Client

subgraph Deterministic Core
SM
Store
end

Replay + deterministic transitions → predictable behavior.


---

State Machine

Defined in: 👉 src/wzcore_sandbox/state_machine.py

States:

INIT

PROCESSING

FAILED

SUCCESS


Valid transitions:

INIT → PROCESSING

PROCESSING → FAILED

FAILED → PROCESSING

PROCESSING → SUCCESS


No randomness.
No time-based logic.
No hidden state.


---

Testing

Run locally:

pytest -q

CI runs:

flake8

pytest


CI status badge is shown at the top.


---

Design Principles

1. Deterministic replay over mutation


2. Idempotency first


3. Dry-run must never mutate state


4. Duplicate detection must be explicit and retry-aware


5. No business logic inside runtime core




---

Contract

See: 👉 docs/runtime_contract.md

Defines:

input schema

output schema

invariants

retry semantics



---

Why This Matters

This sandbox models the mechanical core required for:

reliable message processing

retry-safe systems

exactly-once-ish semantics

deterministic recovery


It is a minimal, inspectable reference implementation.


---

Status

Deterministic runtime ✔

Dry-run stateless ✔

Retry-aware duplicate guard ✔

CI green ✔

Tests passing ✔



---

If you’re evaluating this repository: run it, call /runtime/handle, replay events, inspect transitions.
It should behave deterministically every time.

---

### Delivery strict mode

See: docs/runtime/DELIVERY_STRICT_MODE.md
Runtime notes
PACK-S — inbound webhook idempotency (msg_id)

Система защищена от дублей входящих вебхуков MAX на уровне базы данных и кода.

База данных

Создан partial UNIQUE index:

CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_in_msgid
ON messages(msg_id)
WHERE direction='in' 
  AND msg_id IS NOT NULL 
  AND trim(msg_id)<>'';

Это гарантирует, что один и тот же msg_id для входящего сообщения не может быть записан дважды.

Код

В /opt/max-bot1/app/db_ext.py вставка в messages выполняется через:

INSERT OR IGNORE INTO messages(...)

Если webhook приходит повторно, запись будет проигнорирована без падения процесса.

Результат

Повторная доставка webhook не создаёт дубли в истории.

История inbound сообщений стала идемпотентной.

Процесс устойчив к повторной доставке событий со стороны провайдера.

Документ:
PACK_S_INBOUND_MSGID_IDEMPOTENCY.md

- PACK-S: inbound webhook idempotency (msg_id) — docs/runtime/PACK_S_INBOUND_MSGID_IDEMPOTENCY.md
