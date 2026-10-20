"""Sends one JSON message to Kafka every SEND_INTERVAL_SECONDS (default 5)."""
import json
import os
import signal
import socket
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

running = True


def stop(signum, _frame):
    global running
    print(f"[producer] signal {signum} received, shutting down", flush=True)
    running = False


def build_config(env):
    """Kafka client settings (TLS + SASL/PLAIN) from environment variables."""
    return {
        "bootstrap.servers": env["BOOTSTRAP_SERVERS"],
        "client.id": "message-producer",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": env["KAFKA_USERNAME"],
        "sasl.password": env["KAFKA_PASSWORD"],
        "ssl.ca.location": env.get("KAFKA_CA_FILE", "/certs/ca.crt"),
        "enable.idempotence": True,
        "acks": "all",
    }


def build_payload(seq, host, now=None):
    """The JSON-serializable message body, including a UTC timestamp."""
    now = now or datetime.now(timezone.utc)
    return {
        "id": seq,
        "timestamp": now.isoformat(),
        "message": "hello from message-producer",
        "producer": host,
    }


def on_delivery(err, msg):
    if err is not None:
        print(f"[producer] delivery FAILED: {err}", flush=True)
    else:
        print(
            f"[producer] delivered to {msg.topic()} "
            f"[partition {msg.partition()}] @ offset {msg.offset()}",
            flush=True,
        )


def send_message(producer, topic, seq, host):
    """Queue one message. Returns True on success, False if it could not be queued."""
    payload = build_payload(seq, host)
    try:
        producer.produce(topic, value=json.dumps(payload).encode(), on_delivery=on_delivery)
    except (BufferError, KafkaException) as exc:
        print(f"[producer] produce error: {exc}", flush=True)
        return False
    print(f"[producer] sent {json.dumps(payload)}", flush=True)
    return True


def run(producer, topic, interval, host, max_messages=None):
    """Send a message every `interval` seconds until stopped. Returns messages attempted."""
    seq = 0
    next_tick = time.monotonic()
    while running:
        if max_messages is not None and seq >= max_messages:
            break
        seq += 1
        send_message(producer, topic, seq, host)

        # serve delivery callbacks, then sleep until the next tick (no drift)
        producer.poll(0)
        next_tick += interval
        while running and (delay := next_tick - time.monotonic()) > 0:
            producer.poll(min(delay, 0.5))

    producer.flush(10)
    print("[producer] stopped", flush=True)
    return seq


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    topic = os.environ.get("TOPIC", "messages")
    interval = float(os.environ.get("SEND_INTERVAL_SECONDS", "5"))
    producer = Producer(build_config(os.environ))
    print(
        f"[producer] sending to '{topic}' every {interval}s "
        f"via {os.environ['BOOTSTRAP_SERVERS']}",
        flush=True,
    )
    run(producer, topic, interval, socket.gethostname())


if __name__ == "__main__":
    main()
