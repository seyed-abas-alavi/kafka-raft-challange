#!/bin/sh
# Generates a private CA and one server certificate per broker (PEM format).
# Idempotent: skips if every node already has a certificate in the volume.
# If the node list changed (e.g. a 2-node volume reused for 3 nodes), all
# certificates are regenerated so they stay signed by the same CA.
set -eu

OUT="${CERT_DIR:-/certs}"
NODES="${NODES:-kafka-1 kafka-2 kafka-3}"
cd "$OUT"

complete=1
[ -f .done ] && [ -f ca.crt ] || complete=0
for node in $NODES; do
  [ -f "$node.keystore.pem" ] || complete=0
done

if [ "$complete" = 1 ]; then
  echo "[certs] already generated for: $NODES, skipping"
  exit 0
fi

if [ -f .done ]; then
  echo "[certs] existing certificates are incomplete for: $NODES, regenerating all"
fi
rm -f ./*.pem ./*.crt ./*.key ./*.csr ./*.ext ./*.srl .done

echo "[certs] creating CA"
openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
  -subj "/O=Padvish Challenge/CN=Padvish Challenge CA" -out ca.crt

for node in $NODES; do
  echo "[certs] creating certificate for $node"
  openssl genrsa -out "$node.key" 2048 2>/dev/null
  openssl req -new -key "$node.key" -subj "/O=Padvish Challenge/CN=$node" -out "$node.csr"
  cat > "$node.ext" <<EXT
subjectAltName=DNS:$node,DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
EXT
  openssl x509 -req -in "$node.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "$node.crt" -days 825 -sha256 -extfile "$node.ext" 2>/dev/null
  # Kafka PEM keystore: unencrypted PKCS#8 private key followed by the certificate
  openssl pkcs8 -topk8 -nocrypt -in "$node.key" -out "$node.pkcs8.key"
  cat "$node.pkcs8.key" "$node.crt" > "$node.keystore.pem"
  rm -f "$node.key" "$node.pkcs8.key" "$node.csr" "$node.ext"
done

# The CA private key is not needed after signing.
rm -f ca.key ca.srl

# Demo only: world-readable so the non-root Kafka user can read them.
chmod 644 ./*.pem ./*.crt
touch .done
echo "[certs] done"
