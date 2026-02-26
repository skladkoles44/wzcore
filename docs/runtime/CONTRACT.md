# WZCORE Runtime Contract (v0.2.0)

This document defines the public runtime contract for the sandbox package:
- Event envelope (inputs)
- Deterministic processing and invariants
- Idempotency and retry semantics
- Failure handling and recovery expectations
- Health semantics

## 1. Definitions

### 1.1 Event
An event is an immutable input message processed by the runtime.

### 1.2 State
State is the deterministic result of applying events via the state machine.

### 1.3 Determinism (hard requirement)
Given:
- Identical initial state
- Identical ordered sequence of valid events

The runtime MUST produce:
- Identical final state
- Identical emitted outputs (if any)
- Identical error classification for the same invalid event

## 2. Event Envelope (public contract)

### 2.1 Schema (canonical)
Fields:

- `event_id` (string, required): globally unique idempotency key for the event.
- `ts` (number, required): unix timestamp (seconds, float allowed).
- `type` (string, required): event kind (namespaced).
- `source` (object, required):
  - `system` (string, required)
  - `actor_id` (string, optional)
- `payload` (object, required): event-specific data (validated by `type`).
- `meta` (object, optional): reserved for forward compatibility, tracing, debug.

### 2.2 Invariants
- `event_id` MUST be stable across retries.
- `payload` MUST be JSON-serializable.
- Unknown fields MUST be ignored unless explicitly required by the specification.

### 2.3 Examples

Minimal:
`{"event_id":"evt_1","ts":1700000000,"type":"sandbox.ping","source":{"system":"demo"},"payload":{}}`

Full:
`{"event_id":"evt_2","ts":1700000001.25,"type":"sandbox.transition","source":{"system":"demo","actor_id":"user_123"},"payload":{"from":"INIT","to":"READY"},"meta":{"trace_id":"t_abc","version":"1"}}`

## 3. State machine guarantees (determinism + invariants)

### 3.1 Allowed transitions
Documented by the state machine implementation.

### 3.2 Forbidden transitions
A forbidden transition MUST raise `TransitionError` (or a subtype) and MUST NOT mutate state.

### 3.3 Validation order
1. Envelope validation
2. Event-specific validation
3. Transition validation
4. State mutation (atomic)

## 4. Idempotency and retry semantics

### 4.1 Idempotency key
`event_id` is the idempotency key.

### 4.2 Duplicate handling
If an event with the same `event_id` is re-processed:
- Runtime MUST return the same outcome as the original processing
- Runtime MUST NOT apply state mutation twice

### 4.3 Retry policy (contract)
- Retryable: transport errors, timeouts, temporary dependency errors
- Non-retryable: validation errors, forbidden transitions

(Implementation may provide helpers; contract is the minimum.)

## 5. Failure handling policy (isolation and recovery)

### 5.1 Isolation
One failing event MUST NOT corrupt the global runtime process state.

### 5.2 Recovery
After a crash or restart, re-processing the same ordered event stream MUST converge to the same state.

### 5.3 Observability minimum
- Stable error taxonomy
- Stable health endpoint semantics

## 6. Health semantics

`GET /health` MUST return:
- `200 OK` when runtime is alive and capable of processing
- JSON body with at least: `status`, `version`
