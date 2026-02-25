# Delivery State Machine (RFC)

## 1. Scope

This document defines the authoritative state machine for records in SQLite table `delivery`.

The state machine is defined by the field `delivery.status` and related columns controlling claiming, retries, and lease recovery.

---

## 2. Entity

A delivery record is one row in table `delivery`.

### Observed fields

- id (INTEGER PRIMARY KEY)
- event_id (TEXT)
- dest_channel (TEXT)
- recipient_id (TEXT)
- status (TEXT)
- attempts (INTEGER)
- created_ts (REAL)
- updated_ts (REAL)
- claim_id (TEXT, optional)
- claim_ts (REAL, optional)
- next_retry_ts (REAL, optional)
- payload (TEXT, optional)
- last_error (TEXT, optional)

---

## 3. States

`status` MUST be one of:

- new
- processing
- sent
- dead

---

## 4. State Invariants

### new

Eligible for claim if:

- next_retry_ts IS NULL
  OR
- next_retry_ts <= now

### processing

- claim_id MUST be set (if column exists)
- claim_ts MUST be set (if column exists)

Lease expired if:

- claim_ts < (now - LEASE_TIMEOUT_SECONDS)

### sent

Terminal success state.

Must not be claimed again.

### dead

Terminal failure state.

May be manually requeued.

---

## 5. Allowed Transitions

[*] -> new

new -> processing
  condition:
    eligible AND claimed under write transaction
  action:
    status='processing'
    claim_id set
    claim_ts=now

processing -> sent
  condition:
    dispatch successful
  action:
    status='sent'
    attempts++

processing -> dead
  condition:
    terminal failure
  action:
    status='dead'
    attempts++
    last_error set

processing -> new
  condition:
    lease expired
  action:
    status='new'
    claim_ts=NULL

dead -> new
  condition:
    manual operator action

---

## 6. Forbidden Transitions

- sent -> processing
- dead -> processing
- new -> sent (without processing)
- sent -> new (unless future replay feature is defined)

---

## 7. Concurrency Model

Claim MUST occur inside a SQLite write transaction.

Recommended:

BEGIN IMMEDIATE

Busy timeout SHOULD be configured to avoid claim contention errors.

---

## 8. Formal Diagram

\`\`\`mermaid
stateDiagram-v2
    [*] --> new

    new --> processing: claim\nset claim_id, claim_ts
    processing --> sent: success
    processing --> dead: terminal fail
    processing --> new: lease timeout

    dead --> new: manual requeue
\`\`\`
