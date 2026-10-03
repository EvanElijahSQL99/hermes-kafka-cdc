"""Thin helpers around confluent-kafka so the services stay readable."""
import json
import logging
import time

log = logging.getLogger("hermes")


def wait_for_kafka(bootstrap: str, timeout_s: float = 180):
    """Block until the broker answers a metadata request (containers start in any order)."""
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.time() + timeout_s
    while True:
        try:
            admin.list_topics(timeout=5)
            return admin
        except Exception as e:  # noqa: BLE001
            if time.time() > deadline:
                raise
            log.info("waiting for Kafka at %s (%s)", bootstrap, e)
            time.sleep(3)


def ensure_topics(admin, topics: dict):
    """Create topics that don't exist yet. topics = {name: (partitions, {config})}."""
    from confluent_kafka.admin import NewTopic
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=p, replication_factor=1, config=c)
           for t, (p, c) in topics.items() if t not in existing]
    if not new:
        return
    for t, f in admin.create_topics(new).items():
        try:
            f.result()
            log.info("created topic %s", t)
        except Exception as e:  # noqa: BLE001  (already exists after a race is fine)
            log.info("topic %s: %s", t, e)


def producer(bootstrap: str, **extra):
    from confluent_kafka import Producer
    conf = {"bootstrap.servers": bootstrap, "linger.ms": 20, "acks": "all", "enable.idempotence": True}
    conf.update(extra)
    return Producer(conf)


def send_json(p, topic: str, value: dict, key: str | None = None):
    p.produce(topic, json.dumps(value, separators=(",", ":"), default=str).encode(), key=key.encode() if key else None)
    p.poll(0)


def loads(raw: bytes | None):
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None
