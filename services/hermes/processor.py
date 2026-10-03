"""Stream processor: consumes CDC + telemetry, publishes per-second metrics, alerts and traces.

Consumer group `hermes-processor`. A control topic lets the dashboard pause consumption of
the CDC topics (telemetry keeps flowing), so visitors can watch consumer lag build and drain.
"""
import logging
import threading
import time

from confluent_kafka import Consumer, KafkaError

from . import config
from .core import Processor
from .kafka_util import ensure_topics, loads, producer, send_json, wait_for_kafka

log = logging.getLogger("hermes.processor")
MAX_PAUSE_S = 45


class Control:
    """Reads pause/resume commands from hermes.control on its own consumer (latest only)."""

    def __init__(self):
        self.pause_until = 0.0
        self.lock = threading.Lock()

    def paused(self):
        with self.lock:
            return time.time() < self.pause_until

    def run(self):
        c = Consumer({"bootstrap.servers": config.BOOTSTRAP, "group.id": "hermes-processor-control",
                      "auto.offset.reset": "latest", "enable.auto.commit": True})
        c.subscribe([config.CONTROL_TOPIC])
        while True:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            cmd = loads(m.value()) or {}
            with self.lock:
                if cmd.get("action") == "pause":
                    secs = min(float(cmd.get("seconds", 30)), MAX_PAUSE_S)
                    self.pause_until = time.time() + secs
                    log.info("pausing CDC consumption for %.0fs", secs)
                elif cmd.get("action") == "resume":
                    self.pause_until = 0
                    log.info("resuming CDC consumption")


def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    admin = wait_for_kafka(config.BOOTSTRAP)
    six_h = {"retention.ms": str(6 * 3600 * 1000)}
    ensure_topics(admin, {config.METRICS_TOPIC: (1, six_h), config.ALERTS_TOPIC: (1, {}),
                          config.TRACE_TOPIC: (1, six_h), config.CONTROL_TOPIC: (1, six_h),
                          config.TELEMETRY_TOPIC: (1, six_h)})

    control = Control()
    threading.Thread(target=control.run, daemon=True).start()

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP,
        "group.id": config.PROCESSOR_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "topic.metadata.refresh.interval.ms": 10000,   # pick up Debezium's topics soon after they appear
    })
    # Regex subscription: Debezium creates shop.public.* topics on its first event.
    consumer.subscribe(["^shop\\.public\\..*", config.TELEMETRY_TOPIC])
    out = producer(config.BOOTSTRAP)
    proc = Processor()
    next_tick = time.time() + 1
    paused_now = False

    while True:
        # Pause/resume only the CDC partitions; telemetry keeps flowing so alerts stay live.
        want_pause = control.paused()
        if want_pause or paused_now:
            cdc = [tp for tp in consumer.assignment() if tp.topic.startswith("shop.public.")]
            if cdc:
                (consumer.pause if want_pause else consumer.resume)(cdc)   # idempotent; covers rebalances
            if want_pause != paused_now:
                log.info("CDC consumption %s", "paused" if want_pause else "resumed")
            paused_now = proc.paused = want_pause

        for m in consumer.consume(num_messages=500, timeout=0.2):
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("consumer error: %s", m.error())
                continue
            now_ms = int(time.time() * 1000)
            if m.topic() == config.TELEMETRY_TOPIC:
                snap = loads(m.value())
                if snap:
                    proc.handle_telemetry(snap)
                continue
            ts_type, ts = m.timestamp()
            trace = proc.handle_cdc(loads(m.value()), ts if ts_type else None, now_ms)
            if trace:
                send_json(out, config.TRACE_TOPIC, trace, key=str(trace["trace_id"]))

        if time.time() >= next_tick:
            next_tick += 1
            if next_tick < time.time():          # fell behind (e.g. long catch-up): don't burst ticks
                next_tick = time.time() + 1
            metrics, alerts = proc.tick(int(time.time() * 1000))
            send_json(out, config.METRICS_TOPIC, metrics, key=config.INSTANCE)
            for a in alerts:
                log.info("alert %s %s: %s", a["state"], a["rule"], a["detail"])
                send_json(out, config.ALERTS_TOPIC, a, key=a["rule"])
            out.poll(0)


if __name__ == "__main__":
    run()
