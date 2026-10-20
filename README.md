#  DevOps Challenge — Kafka (KRaft, 3 nodes) + TLS + SASL + Grafana 

A Docker Compose environment with a 3-node Apache Kafka cluster in **KRaft mode** , encrypted with **TLS** and authenticated with **SASL/PLAIN**, plus a Python message producer and consumer, and a Grafana dashboard showing the logs, the messages of every node and the messages of every node. Data is kept in Docker volumes.

author: seyed abas alavi

email: seyed.abas.alavii@gmail.com

## Run

```bash
docker compose up -d --build
docker compose ps                     
docker compose logs -f message-producer message-consumer
```

## monitoring

http://127.0.0.1:3000/d/kafka-challenge-logs/kafka-challenge-by-abas-alavi?orgId=1&from=now-5m&to=now&timezone=browser&refresh=5s


## Layout

```
.
├── docker-compose.yml        # main file
├── .env                      # demo credentials and settings
├── certs/                    # cert-generator service (private CA + per-broker certs)
├── kafka/scripts/            # healthcheck + topic-creation helpers for the brokers
├── message-producer/         # service: message-producer
├── message-consumer/         # service: message-consumer (also used for the observers)
├── monitoring/               # Grafana + Loki + Alloy (dashboard)
├── tests/                    # infrastructure tests (service tests live in each service's tests/)
└── pytest.ini, requirements-dev.txt
```

## Architecture

| Service | Role |
|---|---|
| `cert-generator` | One-shot: creates a CA and a certificate for `kafka-1`, `kafka-2`, `kafka-3` (SAN = hostname, `localhost`) in a shared volume |
| `volume-init` | One-shot: makes the data volumes writable for the non-root users of Kafka, Loki, Grafana and Alloy |
| `kafka-1`, `kafka-2`, `kafka-3` | Combined broker + controller nodes forming the Raft quorum (`1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093`) |
| `kafka-init` | One-shot: creates topic `messages` (3 partitions, replication factor 3, `min.insync.replicas=2`) once all brokers are healthy |
| `message-producer` | Sends a JSON message (with timestamp) every 5 s |
| `message-consumer` | Reads the topic and prints each message to stdout |
| `observer-kafka-1..3` | Extra consumers (one per node, own consumer group) that read from the replica stored on that node; used only for the per-node Grafana panels |
| `alloy` | Collects the logs of every container in the project and sends them to Loki |
| `loki` | Log storage |
| `grafana` | Dashboard on http://localhost:3000 |

Security:
- **Clients ↔ brokers and broker ↔ broker:** `SASL_SSL` listener (`:9092`), mechanism `PLAIN`, TLS certificates verified against the CA, hostname verification on.
- **Raft metadata traffic:** dedicated `CONTROLLER` listener (`:9093`) over TLS with mutual authentication.
- Users (see `.env`): `admin` (brokers, tooling) and `app` (producer/consumer/observers). Demo values — change them for real use.

- since the project preps demo version, .env file that contains secretes and env variables is exposed.
Resources: about 3 GB of free RAM is recommended (3 brokers + monitoring).



Expected output (consumer):

```
[consumer] partition=1 offset=0 value={"id": 1, "timestamp": "2026-...", "message": "hello from message-producer", ...}
```


## Grafana

Open **http://localhost:3000** — the *Kafka Challenge - Logs* dashboard is the home page (read-only without login; admin login is `admin` / `admin`, see `.env`). Refresh is every 5 s.

- **Top row:** messages sent by the producer, received by the consumer, and stored on each of `kafka-1`, `kafka-2`, `kafka-3` (last 5 min).
- **Producer / consumer panels:** live log lines of both services.
- **Per node (3 rows):** left, the messages stored on that node (from `observer-kafka-N`); right, the server log of that node.

Because every partition is replicated to all 3 nodes, each node holds every message, so the three "messages stored" panels show the same messages. If a node is stopped, its panel stops updating while the others continue.

Note: Kafka server logs do not contain message contents. That is why the observers exist: each one reads with `client.rack` set to its node's rack, so Kafka serves it from that node's replica (follower fetching, `RackAwareReplicaSelector`). If its node is down, it falls back to another node and keeps running.

Ad-hoc queries: *Explore* → Loki, e.g. `{service="kafka-2"}` or `{service="observer-kafka-3"} |= "id"`.

How it works: Alloy reads the Docker logs through `/var/run/docker.sock` (only containers of this Compose project), labels each line with `service` and `container`, and pushes it to Loki. Grafana's Loki datasource and the dashboard are provisioned from `monitoring/grafana/`.

## Tests

Unit tests run locally with pytest (no Docker needed; `openssl` is required for the certificate tests):

```bash
pip install -r requirements-dev.txt
pytest
```

| Location | What it covers |
|---|---|
| `message-producer/tests/` | message payload and timestamp, TLS/SASL client config, send loop and interval, error handling, graceful stop |
| `message-consumer/tests/` | client config (consumer and per-node observer), output format, error and end-of-partition handling, close on exit |
| `tests/test_infra.py` | Compose file (3 KRaft nodes, quorum, replication, TLS/SASL settings, volumes, dependencies), shell script syntax, certificate script (run for real), Grafana/Loki/Alloy configuration |

## Verify the cluster

All three nodes are voters:

```bash
docker compose exec kafka-1 sh -c '. /opt/scripts/common.sh && write_client_properties && \
  /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 \
  --command-config $CLIENT_PROPS describe --status'
```
(`CurrentVoters` must list nodes 1, 2 and 3.) Topic replicas / ISR:

```bash
docker compose logs kafka-init          # output of --describe: Isr: 1,2,3
```

Wrong credentials are rejected (authentication failure in the client logs), and a plaintext connection to port 9092 is refused.

## Failure test

```bash
docker compose stop kafka-2             # producer and consumer keep running
docker compose start kafka-2            # node rejoins and catches up
```
With one node down the quorum (2 of 3) and writes (`min.insync.replicas=2`) still work; Grafana shows the stopped node's panels going quiet. Stopping two nodes stops writes.

## Configuration

Everything is in `.env`: credentials, topic, consumer group, `SEND_INTERVAL_SECONDS` (default `5`; the task text says both "every second" and "every 5 seconds", so it is configurable).

## Notes / limitations

- The certs are world-readable inside a Docker volume and the CA key is deleted after signing — fine for a demo, not for production. Authorization (ACLs) is not configured; any authenticated user has full access.
- Grafana allows anonymous read-only access (demo); remove `GF_AUTH_ANONYMOUS_ENABLED` in `docker-compose.yml` to require login.
