from __future__ import annotations
import time
from fastapi import FastAPI
from .state_machine import DeterministicStateMachine, State

app = FastAPI(title="WZCore Sandbox", version="0.1.0")

_boot_ts = time.time()
_sm = DeterministicStateMachine()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "uptime_s": round(time.time() - _boot_ts, 3)}


@app.post("/demo/run")
def demo_run(simulate_fail: bool = False) -> dict:
    # deterministic demo path: INIT -> PROCESSING -> (FAILED -> PROCESSING) -> SUCCESS
    if _sm.state == State.INIT:
        _sm.step(State.PROCESSING)

    if simulate_fail and _sm.state == State.PROCESSING:
        _sm.step(State.FAILED)
        _sm.step(State.PROCESSING)

    if _sm.state == State.PROCESSING:
        _sm.step(State.SUCCESS)

    return {
        "state": _sm.state.value,
        "transitions": [{"from": t.frm.value, "to": t.to.value} for t in _sm.transitions],
    }
