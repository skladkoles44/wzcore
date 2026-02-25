from fastapi.testclient import TestClient
from wzcore_sandbox.app import app


def test_health_ok():
    c = TestClient(app)
    r = c.get("/health")
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "ok"
