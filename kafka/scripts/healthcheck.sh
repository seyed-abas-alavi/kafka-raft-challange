#!/bin/sh
# Healthy = the broker answers over TLS + SASL on its own client listener.
. /opt/scripts/common.sh
write_client_properties
exec /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --command-config "$CLIENT_PROPS" > /dev/null 2>&1
