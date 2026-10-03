"""API tests: state/payload logic without a broker, and the visitor actions against a real PostgreSQL."""
import json
import os
import threading
import time

os.environ["HERMES_NO_KAFKA"] = "1"
os.environ.setdefault("UI_DIR", "/nonexistent")

import psycopg  # noqa: E402
import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from hermes import api, config, workload  # noqa: E402

N = [0]


def ip():
    N[0] += 1
    return {"x-forwarded-for": f"203.0.113.{N[0]}, 10.0.0.1"}


def metrics(ts):
    lat = {"p50": 80.0, "p95": 140.0, "max": 200}
    return {"ts": ts, "paused": False,
            "throughput": {"events_per_s": 9.1, "orders_per_s": 2.0, "payments_per_s": 1.0,
                           "revenue_per_min": 1200.0, "baseline_orders_per_s": 2.0},
            "latency_ms": {"commit_to_capture": lat, "capture_to_process": lat, "end_to_end": lat},
            "funnel_60s": {"placed": 120, "paid": 60, "shipped": 50, "deleted": 0},
            "totals": {"events": 10, "orders": 5, "snapshot_reads": 0},
            "db": {"blocked_count": 0, "commits_per_s": 4.0}}


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, value, key=None):
        self.sent.append((topic, json.loads(value)))

    def poll(self, _t):
        return 0

    def flush(self, _t):
        return 0


@pytest.fixture
def client():
    api.S = api.State()
    api.HITS.clear()
    with TestClient(api.app) as c:
        yield c


def test_state_payload_cursor_and_alert_tracking(client):
    now = int(time.time() * 1000)
    api.S.set_metrics(metrics(now))
    api.S.add("alerts", {"id": "a1", "rule": "blocking", "state": "open"})
    api.S.add("feed", {"table": "orders", "op": "c"})
    snap = client.get("/api/snapshot").json()
    assert snap["metrics"]["throughput"]["orders_per_s"] == 2.0
    assert len(snap["history"]) == 1 and snap["history"][0]["e2e_p95"] == 140.0
    assert [a["rule"] for a in snap["active_alerts"]] == ["blocking"]
    cursor = snap["seq"]
    api.S.add("alerts", {"id": "a1", "rule": "blocking", "state": "resolved"})
    inc = client.get(f"/api/snapshot?since={cursor}").json()
    assert "history" not in inc and inc["feed"] == [] and [a["state"] for a in inc["alerts"]] == ["resolved"]
    assert inc["active_alerts"] == []
    assert client.get("/api/health").json()["metrics_age_s"] < 5


def test_sse_stream_snapshot_then_ticks():
    """Runs a real uvicorn server: the in-process test client can't signal disconnects to an endless stream."""
    import socket

    import httpx
    import uvicorn

    api.S = api.State()
    api.S.set_metrics(metrics(int(time.time() * 1000)))
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = uvicorn.Server(uvicorn.Config(api.app, host="127.0.0.1", port=port, log_level="warning"))
    th = threading.Thread(target=server.run, daemon=True)
    th.start()
    try:
        for _ in range(50):
            if server.started:
                break
            time.sleep(0.1)
        events = []
        with httpx.stream("GET", f"http://127.0.0.1:{port}/api/stream", timeout=5) as r:
            assert r.headers["content-type"].startswith("text/event-stream")
            for line in r.iter_lines():
                if line.startswith("event: "):
                    events.append(line[7:])
                elif line.startswith("data: ") and events[-1] == "snapshot":
                    assert "history" in json.loads(line[6:])
                    api.S.add("feed", {"table": "orders", "op": "c"})
                elif line.startswith("data: ") and events[-1] == "tick":
                    tick = json.loads(line[6:])
                    assert [f["table"] for f in tick["feed"]] == ["orders"] and "history" not in tick
                    break
        assert events == ["snapshot", "tick"]
    finally:
        server.should_exit = True
        th.join(5)


def test_rate_limit_per_ip(client, monkeypatch):
    monkeypatch.setattr(api, "PRODUCER", FakeProducer())
    h = ip()
    assert client.post("/api/actions/resume", headers=h).status_code == 200
    r = client.post("/api/actions/resume", headers=h)
    assert r.status_code == 429
    assert client.post("/api/actions/resume", headers=ip()).status_code == 200   # other visitors unaffected


def test_pause_sends_control_message_and_is_single_flight(client, monkeypatch):
    fp = FakeProducer()
    monkeypatch.setattr(api, "PRODUCER", fp)
    r = client.post("/api/actions/pause", headers=ip())
    assert r.status_code == 200 and fp.sent == [(config.CONTROL_TOPIC, {"action": "pause", "seconds": 30})]
    assert client.get("/api/snapshot").json()["scenario"]["name"] == "pause"
    busy = client.post("/api/actions/blocking", headers=ip())
    assert busy.status_code == 409 and "pause" in busy.json()["detail"]
    client.post("/api/actions/resume", headers=ip())
    assert fp.sent[-1][1] == {"action": "resume"}


# ── against PostgreSQL ──────────────────────────────────────────────
@pytest.fixture
def db_client(client, shop_dsn, monkeypatch):
    monkeypatch.setattr(config, "PG_DSN", shop_dsn)
    return client


def test_visitor_order_is_tagged_for_tracing(db_client, shop_dsn):
    r = db_client.post("/api/actions/order", headers=ip())
    assert r.status_code == 200, r.text
    body = r.json()
    with psycopg.connect(shop_dsn) as c:
        row = c.execute("SELECT source, trace_id::text FROM orders WHERE id = %s", (body["order_id"],)).fetchone()
    assert row == ("visitor", body["trace_id"])
    assert db_client.get("/api/snapshot").json()["visitor_orders"] == 1


def test_blocking_scenario_stalls_hot_product_orders(db_client, shop_dsn, monkeypatch):
    monkeypatch.setitem(api.SCENARIO_SECONDS, "blocking", 7)
    monkeypatch.setattr(workload, "pick_product", lambda n: workload.HOT_PRODUCT)
    r = db_client.post("/api/actions/blocking", headers=ip())
    assert r.status_code == 200 and "7s" in r.json()["message"]
    time.sleep(1.5)
    with psycopg.connect(shop_dsn) as c:
        apps = {a for (a,) in c.execute("SELECT application_name FROM pg_stat_activity "
                                        "WHERE cardinality(pg_blocking_pids(pid)) > 0")}
    assert "demo-restock-job" in apps
    t0 = time.time()
    stuck = db_client.post("/api/actions/order", headers=ip())
    assert stuck.status_code == 409 and "lock_timeout" in stuck.json()["detail"]
    assert 3.5 < time.time() - t0 < 6
    time.sleep(3)
    assert db_client.post("/api/actions/order", headers=ip()).status_code == 200   # chain cleared
    assert db_client.get("/api/snapshot").json()["scenario"]["ends_at"] <= time.time()


def test_long_transaction_scenario_holds_a_snapshot(db_client, shop_dsn, monkeypatch):
    monkeypatch.setitem(api.SCENARIO_SECONDS, "longtx", 3)
    assert db_client.post("/api/actions/longtx", headers=ip()).status_code == 200
    time.sleep(1)
    with psycopg.connect(shop_dsn) as c:
        row = c.execute("SELECT state, backend_xmin IS NOT NULL FROM pg_stat_activity "
                        "WHERE application_name = 'demo-report-query'").fetchone()
    assert row == ("idle in transaction", True)
    time.sleep(3)
    with psycopg.connect(shop_dsn) as c:
        assert c.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'demo-report-query'"
                         ).fetchone()[0] == 0


def test_database_down_returns_503(client, monkeypatch):
    monkeypatch.setattr(config, "PG_DSN", "host=127.0.0.1 port=1 dbname=x user=x connect_timeout=1")
    r = client.post("/api/actions/order", headers=ip())
    assert r.status_code == 503 and "Database unavailable" in r.json()["detail"]
