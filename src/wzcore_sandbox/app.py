from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .state_machine import DeterministicStateMachine, State, Transition
app = FastAPI(title="WZCore Sandbox", version="0.2.2")

_boot_ts = time.time()


# ----------------------------
# Runtime: minimal in-memory store
# ----------------------------

@dataclass
class StoredEvent:
    event_id: str
    transitions: List[Transition]
    updated_ts: float


_EVENTS: Dict[str, StoredEvent] = {}


class RuntimeEvent(BaseModel):
    event_id: str = Field(min_length=1)
    attempt: int = Field(ge=1, default=1)
    simulate_fail: bool = False


def _replay(transitions: List[Transition]) -> DeterministicStateMachine:
    sm = DeterministicStateMachine()
    if not transitions:
        return sm
    # Guard against corrupted store: first transition must originate from INIT
    if transitions[0].frm != State.INIT:
        raise ValueError("Corrupted stored transitions: first transition not from INIT")
    for t in transitions:
        sm.step(t.to)
    return sm


def _persist(event_id: str, sm: DeterministicStateMachine) -> StoredEvent:
    st = StoredEvent(event_id=event_id, transitions=list(sm.transitions), updated_ts=time.time())
    _EVENTS[event_id] = st
    return st


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "uptime_s": round(time.time() - _boot_ts, 3)}


@app.post("/demo/run")
def demo_run(simulate_fail: bool = False) -> dict:
    # deterministic demo path: INIT -> PROCESSING -> (FAILED -> PROCESSING) -> SUCCESS
    sm = DeterministicStateMachine()
    if sm.state == State.INIT:
        sm.step(State.PROCESSING)

    if simulate_fail and sm.state == State.PROCESSING:
        sm.step(State.FAILED)
        sm.step(State.PROCESSING)

    if sm.state == State.PROCESSING:
        sm.step(State.SUCCESS)

    return {
        "state": sm.state.value,
        "transitions": [{"from": t.frm.value, "to": t.to.value} for t in sm.transitions],
    }


@app.post("/runtime/handle")
def runtime_handle(ev: RuntimeEvent) -> dict:

    # DRY-RUN: must have zero side-effects (no store read/write)
    if getattr(ev, "dry_run", False):
        sm = DeterministicStateMachine()
        # emulate one attempt without persistence
        # (same transitions as a normal first run)
        sm.step(State.PROCESSING)
        try:
            sm.step(State.SUCCESS)
        except Exception:
            sm.step(State.FAILED)

        transitions = (
            [{"from": t.frm.name, "to": t.to.name} for t in getattr(sm, "transitions", [])]
            if hasattr(sm, "transitions")
            else []
        )
        state_name = getattr(getattr(sm, "state", None), "name", "SUCCESS")

        return {
            "event_id": ev.event_id,
            "state": state_name,
            "is_new": 1,
            "is_duplicate": False,
            "transitions": transitions,
        }

    stored = _EVENTS.get(ev.event_id)
    sm = _replay(stored.transitions) if stored else DeterministicStateMachine()

    # exactly-once-ish: if already SUCCESS -> return stable result
    if stored and sm.state == State.SUCCESS:
        res = {
            "event_id": ev.event_id,
            "state": sm.state.value,
            "is_new": 0,
            "transitions": [{"from": t.frm.value, "to": t.to.value} for t in sm.transitions],
            "is_duplicate": True,
        }
        return res
    # deterministic progression
    if sm.state == State.INIT:
        sm.step(State.PROCESSING)

    # optional deterministic fail path (caller-controlled)
    if ev.simulate_fail and sm.state == State.PROCESSING:
        sm.step(State.FAILED)

    # retry path: FAILED -> PROCESSING on next handle call
    if sm.state == State.FAILED:
        sm.step(State.PROCESSING)

    # complete if processing
    if sm.state == State.PROCESSING:
        sm.step(State.SUCCESS)

    _persist(ev.event_id, sm)

    res = {
        "event_id": ev.event_id,
        "state": sm.state.value,
        "is_new": 1,
        "transitions": [{"from": t.frm.value, "to": t.to.value} for t in sm.transitions],
        "is_duplicate": False,
    }
    return res
