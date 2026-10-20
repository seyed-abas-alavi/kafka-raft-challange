#!/bin/sh
# One-shot job: creates the topic once all brokers are healthy.
set -eu
. /opt/scripts/common.sh
write_client_properties

BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
TOPIC="${TOPIC:-messages}"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPS" \
  --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPS" \
  --describe --topic "$TOPIC"
