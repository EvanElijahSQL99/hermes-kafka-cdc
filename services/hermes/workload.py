"""Background OLTP traffic for the shop database: orders → payments → shipments.

Product 1 is a deliberately "hot" item (about a third of orders), so a lock on its
inventory row — the dashboard's "Blocking chain" scenario — stalls real traffic,
exactly like hot-row contention in production.
"""
import logging
import random
import time

import psycopg

from . import config

log = logging.getLogger("hermes.workload")
HOT_PRODUCT = 1


def pick_product(n_products: int) -> int:
    return HOT_PRODUCT if random.random() < 0.33 else random.randint(2, n_products)


def place_order(cur, n_products, n_customers, source="workload", trace_id=None, quantity=None):
    product = pick_product(n_products)
    qty = quantity or random.randint(1, 4)
    cur.execute("UPDATE inventory SET on_hand = on_hand - %s, updated_at = now() "
                "WHERE product_id = %s RETURNING price", (qty, product))
    price = cur.fetchone()[0]
    cur.execute("INSERT INTO orders (customer_id, product_id, quantity, total, source, trace_id) "
                "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                (random.randint(1, n_customers), product, qty, price * qty, source, trace_id))
    return cur.fetchone()[0]


def pay_one(cur):
    cur.execute("SELECT id, total FROM orders WHERE status = 'placed' AND created_at < now() - interval '1 second' "
                "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED")
    row = cur.fetchone()
    if not row:
        return False
    cur.execute("INSERT INTO payments (order_id, amount, method) VALUES (%s, %s, %s)",
                (row[0], row[1], random.choice(["card", "card", "card", "ach", "wallet"])))
    cur.execute("UPDATE orders SET status = 'paid', updated_at = now() WHERE id = %s", (row[0],))
    return True


def ship_one(cur):
    cur.execute("UPDATE orders SET status = 'shipped', updated_at = now() WHERE id = ("
                "SELECT id FROM orders WHERE status = 'paid' AND updated_at < now() - interval '2 seconds' "
                "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED)")
    return cur.rowcount > 0


def restock(cur):
    cur.execute("UPDATE inventory SET on_hand = on_hand + 500, updated_at = now() WHERE on_hand < 100")


def cleanup(cur):
    # Keep the demo database small: shipped orders older than 20 minutes are deleted (CDC emits deletes too).
    cur.execute("DELETE FROM orders WHERE status = 'shipped' AND updated_at < now() - interval '20 minutes'")
    return cur.rowcount


def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    while True:
        try:
            with psycopg.connect(config.PG_DSN, application_name="shop-workload") as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM inventory")
                    n_products = cur.fetchone()[0]
                    cur.execute("SELECT count(*) FROM customers")
                    n_customers = cur.fetchone()[0]
                conn.commit()
                log.info("workload running at ~%.1f tx/s", config.WORKLOAD_TPS)
                last_cleanup = time.time()
                while True:
                    started = time.time()
                    with conn.cursor() as cur:
                        r = random.random()
                        if r < 0.5:
                            place_order(cur, n_products, n_customers)
                        elif r < 0.75:
                            pay_one(cur) or place_order(cur, n_products, n_customers)
                        elif r < 0.95:
                            ship_one(cur) or place_order(cur, n_products, n_customers)
                        else:
                            restock(cur)
                        if time.time() - last_cleanup > 60:
                            n = cleanup(cur)
                            last_cleanup = time.time()
                            if n:
                                log.info("cleanup deleted %d shipped orders", n)
                    conn.commit()
                    # Poisson-ish arrivals around the target rate.
                    delay = random.expovariate(config.WORKLOAD_TPS) - (time.time() - started)
                    if delay > 0:
                        time.sleep(min(delay, 2))
        except psycopg.OperationalError as e:
            log.warning("database unavailable (%s); retrying", e)
            time.sleep(3)


if __name__ == "__main__":
    run()
