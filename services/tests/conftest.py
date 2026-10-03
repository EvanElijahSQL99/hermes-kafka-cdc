"""Test fixtures.

Database tests need a disposable PostgreSQL (16+) started with wal_level=logical, e.g.

    docker run -d --rm -p 55432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17 -c wal_level=logical
    export TEST_PG_ADMIN_DSN="host=127.0.0.1 port=55432 user=postgres dbname=postgres"

They are skipped when TEST_PG_ADMIN_DSN is not set. Each session recreates a `shop_test` database from
postgres/init.sql.
"""
import os
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
ADMIN = os.environ.get("TEST_PG_ADMIN_DSN")


@pytest.fixture(scope="session")
def shop_dsn():
    if not ADMIN:
        pytest.skip("TEST_PG_ADMIN_DSN not set")
    import psycopg
    with psycopg.connect(ADMIN, autocommit=True) as c:
        c.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = 'shop_test'")
        c.execute("DROP DATABASE IF EXISTS shop_test WITH (FORCE)")
        c.execute("CREATE DATABASE shop_test")
    dsn = ADMIN.replace("dbname=postgres", "dbname=shop_test")
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute((ROOT / "postgres" / "init.sql").read_text())
    return dsn
