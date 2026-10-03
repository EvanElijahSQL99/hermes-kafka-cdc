"""Shared settings, read from the environment (see docker-compose.yml)."""
import os

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
PG_DSN = os.environ.get("PG_DSN", "host=postgres dbname=shop user=hermes password=hermes")
INSTANCE = os.environ.get("PG_INSTANCE", "shop-db")

# Topics
CDC_TOPICS = ["shop.public.orders", "shop.public.payments", "shop.public.inventory"]  # written by Debezium
TELEMETRY_TOPIC = "telemetry.postgres"   # written by the collector
METRICS_TOPIC = "hermes.metrics"         # written by the processor, once per second
ALERTS_TOPIC = "hermes.alerts"           # written by the processor when a rule opens/resolves
TRACE_TOPIC = "hermes.trace"             # written by the processor for orders placed from the dashboard
CONTROL_TOPIC = "hermes.control"         # pause/resume commands for the processor

PROCESSOR_GROUP = "hermes-processor"

# Demo guard rails
WORKLOAD_TPS = float(os.environ.get("WORKLOAD_TPS", "4"))   # background transactions per second
COLLECT_INTERVAL_S = float(os.environ.get("COLLECT_INTERVAL_S", "2"))
