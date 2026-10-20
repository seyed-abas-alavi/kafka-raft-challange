#!/bin/sh
# Writes a client config (SASL_SSL + PLAIN, PEM CA) used by the Kafka CLI tools.
CLIENT_PROPS="${CLIENT_PROPS:-/tmp/client.properties}"

write_client_properties() {
  cat > "$CLIENT_PROPS" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${ADMIN_USER}" password="${ADMIN_PASSWORD}";
ssl.truststore.type=PEM
ssl.truststore.location=/certs/ca.crt
PROPS
}
