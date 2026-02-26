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
