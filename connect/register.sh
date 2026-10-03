#!/bin/sh
# Registers the Debezium PostgreSQL connector (idempotent: PUT replaces the config).
set -e
URL=http://connect:8083/connectors/shop-postgres/config
until curl -sf http://connect:8083/connector-plugins >/dev/null; do echo "waiting for Kafka Connect…"; sleep 3; done
curl -sf -X PUT -H "Content-Type: application/json" --data @/connect/shop-postgres.json "$URL"
echo
echo "connector registered"
sleep 5
curl -s http://connect:8083/connectors/shop-postgres/status
echo
