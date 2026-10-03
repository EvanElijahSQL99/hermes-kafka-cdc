"""Runs the schema, workload and collector SQL against a real PostgreSQL (see conftest.py)."""
import threading
import time

import psycopg
import pytest

from hermes import workload
from hermes.collector import Collector


@pytest.fixture
def conn(shop_dsn):
    with psycopg.connect(shop_dsn) as c:
        yield c


def test_schema_seed_and_replica_identity(conn):
    cur = conn.cursor()
    assert cur.execute("SELECT count(*) FROM customers").fetchone()[0] == 200
    assert cur.execute("SELECT count(*) FROM inventory").fetchone()[0] == 40
    ident = dict(cur.execute("SELECT relname, relreplident FROM pg_class "
                             "WHERE relname IN ('orders','inventory','payments')").fetchall())
    assert ident == {"orders": "f", "inventory": "f", "payments": "d"}


def test_order_lifecycle(conn, monkeypatch):
    cur = conn.cursor()
    monkeypatch.setattr(workload, "pick_product", lambda n: 1)
    before = cur.execute("SELECT on_hand, price FROM inventory WHERE product_id = 1").fetchone()
    oid = workload.place_order(cur, 40, 200, source="visitor", trace_id="7d9c0e8e-6f43-4a39-9d43-6a0b4a6bd111", quantity=3)
    conn.commit()
    row = cur.execute("SELECT status, quantity, total, source, trace_id::text FROM orders WHERE id = %s", (oid,)).fetchone()
    assert row[:4] == ("placed", 3, before[1] * 3, "visitor") and row[4].startswith("7d9c0e8e")
    assert cur.execute("SELECT on_hand FROM inventory WHERE product_id = 1").fetchone()[0] == before[0] - 3

    cur.execute("UPDATE orders SET created_at = now() - interval '5 seconds'")   # pay_one ignores brand-new orders
    assert workload.pay_one(cur)
    conn.commit()
    paid = cur.execute("SELECT count(*) FROM orders WHERE status = 'paid'").fetchone()[0]
    assert paid == 1 and cur.execute("SELECT count(*) FROM payments").fetchone()[0] == 1

    cur.execute("UPDATE orders SET updated_at = now() - interval '5 seconds' WHERE status = 'paid'")
    assert workload.ship_one(cur)
    cur.execute("UPDATE orders SET updated_at = now() - interval '30 minutes' WHERE status = 'shipped'")
    assert workload.cleanup(cur) == 1
    conn.commit()
    assert cur.execute("SELECT count(*) FROM payments").fetchone()[0] == 0   # cascade

    cur.execute("UPDATE inventory SET on_hand = 10 WHERE product_id = 2")
    workload.restock(cur)
    assert cur.execute("SELECT on_hand FROM inventory WHERE product_id = 2").fetchone()[0] == 510
    conn.rollback()


def test_collector_snapshot_rates_slots_and_dead_tuples(shop_dsn):
    with psycopg.connect(shop_dsn, autocommit=True) as c:
        c.execute("SELECT pg_create_logical_replication_slot('hermes_test_slot', 'pgoutput')")
        try:
            cur = c.cursor()
            col = Collector()
            first = col.snapshot(cur)
            assert first["rates"] == {} and first["blocked"] == [] and first["xid_age"] > 0
            with psycopg.connect(shop_dsn) as w:
                for _ in range(30):
                    workload.place_order(w.cursor(), 40, 200)
                    w.commit()
                w.execute("UPDATE inventory SET on_hand = on_hand")
                w.commit()
            time.sleep(0.2)
            snap = col.snapshot(cur)
            assert snap["rates"]["commits_per_s"] > 0 and snap["rates"]["inserted_per_s"] > 0
            assert 0 <= snap["rates"]["cache_hit_pct"] <= 100
            slot = next(s for s in snap["slots"] if s["slot"] == "hermes_test_slot")
            assert slot["lag_bytes"] > 0 and slot["active"] is False
            assert snap["dead_tuples"] and {"table", "live", "dead"} <= set(snap["dead_tuples"][0])
        finally:
            c.execute("SELECT pg_drop_replication_slot('hermes_test_slot')")


def test_collector_sees_blocking_chain_and_long_transaction(shop_dsn):
    """The same SQL the demo scenarios rely on: a FOR UPDATE holder, a waiter, and an open snapshot."""
    ready, done = threading.Event(), threading.Event()

    def holder():
        with psycopg.connect(shop_dsn, application_name="demo-lock-holder") as h:
            h.execute("SELECT on_hand FROM inventory WHERE product_id = 1 FOR UPDATE")
            ready.set()
            done.wait(10)
            h.rollback()

    def waiter():
        ready.wait(5)
        with psycopg.connect(shop_dsn, application_name="demo-restock-job") as w:
            w.execute("SET lock_timeout = '10s'")
            w.execute("UPDATE inventory SET on_hand = on_hand WHERE product_id = 1")
            w.rollback()

    threads = [threading.Thread(target=holder), threading.Thread(target=waiter)]
    for t in threads:
        t.start()
    try:
        with psycopg.connect(shop_dsn, autocommit=True) as c, \
             psycopg.connect(shop_dsn, application_name="demo-report-query", autocommit=True) as rpt:
            rpt.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            rpt.execute("SELECT count(*) FROM orders")
            time.sleep(1.5)
            snap = Collector().snapshot(c.cursor())
            assert snap["blocked_count"] == 1
            b = snap["blocked"][0]
            assert b["app"] == "demo-restock-job" and len(b["blocked_by"]) == 1 and b["seconds"] >= 1
            assert "UPDATE inventory" in b["query"]
            assert snap["oldest_xact"]["seconds"] >= 1
            assert snap["sessions"].get("idle in transaction", 0) >= 1
            pinned = c.execute("SELECT backend_xmin IS NOT NULL FROM pg_stat_activity WHERE application_name = "
                               "'demo-report-query'").fetchone()[0]
            assert pinned, "a REPEATABLE READ transaction must hold its snapshot (that is what blocks vacuum)"
            rpt.execute("ROLLBACK")
    finally:
        done.set()
        for t in threads:
            t.join(15)
