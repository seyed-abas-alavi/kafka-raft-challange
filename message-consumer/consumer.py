"""Reads messages from the topic and prints them to stdout.
"""
import os
import signal

from confluent_kafka import Consumer, KafkaError

running = True


def stop(signum, _frame):
    global running
    print(f"signal {signum} received, shutting down", flush=True)
    running = False


def get_prefix(env):
    label = env.get("NODE_LABEL")
    return f"[observer {label}]" if label else "[consumer]"


def build_config(env):
    """Kafka client settings (TLS + SASL/PLAIN) from environment variables."""
    label = env.get("NODE_LABEL")
    conf = {
        "bootstrap.servers": env["BOOTSTRAP_SERVERS"],
        "group.id": env.get("GROUP_ID", "message-consumer-group"),
        "client.id": f"observer-{label}" if label else "message-consumer",
        "auto.offset.reset": "earliest",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": env["KAFKA_USERNAME"],
        "sasl.password": env["KAFKA_PASSWORD"],
        "ssl.ca.location": env.get("KAFKA_CA_FILE", "/certs/ca.crt"),
    }
    if env.get("CLIENT_RACK"):
        conf["client.rack"] = env["CLIENT_RACK"]  
    return conf


def format_message(prefix, msg):
    return (
        f"{prefix} partition={msg.partition()} offset={msg.offset()} "
        f"value={msg.value().decode('utf-8', errors='replace')}"
    )


def process_message(prefix, msg):
    """Print a polled message. Returns the printed line, or None if there was none."""
    if msg is None:
        return None
    if msg.error():
        if msg.error().code() != KafkaError._PARTITION_EOF:
            print(f"{prefix} error: {msg.error()}", flush=True)
        return None
    line = format_message(prefix, msg)
    print(line, flush=True)
    return line


def run(consumer, prefix):
    """Poll and print until stopped; always closes the consumer."""
    try:
        while running:
            process_message(prefix, consumer.poll(1.0))
    finally:
        consumer.close()
        print(f"{prefix} stopped", flush=True)


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    topic = os.environ.get("TOPIC", "messages")
    conf = build_config(os.environ)
    prefix = get_prefix(os.environ)

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"{prefix} subscribed to '{topic}' (group '{conf['group.id']}')", flush=True)
    run(consumer, prefix)


if __name__ == "__main__":
    main()
