from fastapi.testclient import TestClient

from wzcore_sandbox.app import app


def test_runtime_handle_invalid_event_id():
    c = TestClient(app)
    r = c.post("/runtime/handle", json={"event_id": "", "attempt": 1})
    # pydantic validation => 422
    assert r.status_code == 422


def test_runtime_handle_independent_events():
    c = TestClient(app)

    r1 = c.post("/runtime/handle", json={"event_id": "e1", "attempt": 1})
    r2 = c.post("/runtime/handle", json={"event_id": "e2", "attempt": 1})

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["state"] == "SUCCESS"
    assert r2.json()["state"] == "SUCCESS"


def test_runtime_handle_dry_run_no_side_effects():
    c = TestClient(app)

    # 1) dry-run should not persist anything
    r1 = c.post("/runtime/handle", json={"event_id": "dry1", "attempt": 1, "dry_run": True})
    assert r1.status_code == 200
    j1 = r1.json()
    assert j1["event_id"] == "dry1"
    assert j1["state"] in ("SUCCESS", "FAILED")

    # 2) real run after dry-run should behave like a first real run (still is_new == 1)
    r2 = c.post("/runtime/handle", json={"event_id": "dry1", "attempt": 1})
    assert r2.status_code == 200
    j2 = r2.json()
    assert j2["event_id"] == "dry1"
    assert j2.get("is_new") in (1, True)


def test_runtime_handle_dry_run_is_idempotent_and_stateless():
    c = TestClient(app)

    r1 = c.post("/runtime/handle", json={"event_id": "dry2", "attempt": 1, "dry_run": True})
    r2 = c.post("/runtime/handle", json={"event_id": "dry2", "attempt": 1, "dry_run": True})
    assert r1.status_code == 200
    assert r2.status_code == 200
    # should not "flip" behavior because it must not persist
    assert r1.json()["state"] == r2.json()["state"]
