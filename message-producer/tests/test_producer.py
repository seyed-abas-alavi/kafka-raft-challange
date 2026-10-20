import json
import time
from datetime import datetime, timezone

import pytest
from confluent_kafka import KafkaException

import producer

ENV = {
    "BOOTSTRAP_SERVERS": "kafka-1:9092,kafka-2:9092,kafka-3:9092",
    "KAFKA_USERNAME": "app",
    "KAFKA_PASSWORD": "app-secret",
}


class FakeProducer:
    def __init__(self, conf=None, fail_with=None):
        self.conf = conf
        self.fail_with = fail_with
        self.produced = []
        self.polls = 0
        self.flushed = None

    def produce(self, topic, value=None, on_delivery=None):
        if self.fail_with:
            raise self.fail_with
        self.produced.append((topic, value, on_delivery))

    def poll(self, timeout):
        self.polls += 1

    def flush(self, timeout):
        self.flushed = timeout


@pytest.fixture(autouse=True)
def reset_running():
    producer.running = True
    yield
    producer.running = True


class TestBuildPayload:
    def test_fields(self):
        now = datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc)
        payload = producer.build_payload(7, "host-1", now=now)
        assert payload == {
            "id": 7,
            "timestamp": "2026-10-17T12:00:00+00:00",
            "message": "hello from message-producer",
            "producer": "host-1",
        }

    def test_default_timestamp_is_utc_and_parseable(self):
        payload = producer.build_payload(1, "h")
        ts = datetime.fromisoformat(payload["timestamp"])
        assert ts.utcoffset().total_seconds() == 0

    def test_is_json_serializable(self):
        json.dumps(producer.build_payload(1, "h"))


class TestBuildConfig:
    def test_security_settings(self):
        conf = producer.build_config(ENV)
        assert conf["security.protocol"] == "SASL_SSL"
        assert conf["sasl.mechanisms"] == "PLAIN"
        assert conf["sasl.username"] == "app"
        assert conf["sasl.password"] == "app-secret"

    def test_ca_default_and_override(self):
        assert producer.build_config(ENV)["ssl.ca.location"] == "/certs/ca.crt"
        conf = producer.build_config({**ENV, "KAFKA_CA_FILE": "/tmp/ca.pem"})
        assert conf["ssl.ca.location"] == "/tmp/ca.pem"

    def test_reliability_settings(self):
        conf = producer.build_config(ENV)
        assert conf["acks"] == "all"
        assert conf["enable.idempotence"] is True

    def test_bootstrap_servers_passed_through(self):
        assert producer.build_config(ENV)["bootstrap.servers"] == ENV["BOOTSTRAP_SERVERS"]

    @pytest.mark.parametrize("missing", ["BOOTSTRAP_SERVERS", "KAFKA_USERNAME", "KAFKA_PASSWORD"])
    def test_missing_required_env_raises(self, missing):
        env = {k: v for k, v in ENV.items() if k != missing}
        with pytest.raises(KeyError):
            producer.build_config(env)


class TestSendMessage:
    def test_produces_json_bytes_to_topic(self):
        fake = FakeProducer()
        assert producer.send_message(fake, "messages", 3, "h") is True
        topic, value, callback = fake.produced[0]
        assert topic == "messages"
        assert isinstance(value, bytes)
        body = json.loads(value)
        assert body["id"] == 3 and body["producer"] == "h"
        assert callback is producer.on_delivery

    @pytest.mark.parametrize("error", [BufferError("queue full"), KafkaException("boom")])
    def test_produce_errors_are_handled(self, error, capsys):
        fake = FakeProducer(fail_with=error)
        assert producer.send_message(fake, "messages", 1, "h") is False
        assert "produce error" in capsys.readouterr().out

    def test_logs_sent_line(self, capsys):
        producer.send_message(FakeProducer(), "messages", 1, "h")
        assert "[producer] sent" in capsys.readouterr().out


class TestOnDelivery:
    def test_success(self, capsys):
        class Msg:
            def topic(self): return "messages"
            def partition(self): return 2
            def offset(self): return 41

        producer.on_delivery(None, Msg())
        out = capsys.readouterr().out
        assert "messages" in out and "partition 2" in out and "offset 41" in out

    def test_failure(self, capsys):
        producer.on_delivery("broker down", None)
        assert "delivery FAILED: broker down" in capsys.readouterr().out


class TestRun:
    def test_sends_requested_number_of_messages_in_order(self):
        fake = FakeProducer()
        sent = producer.run(fake, "messages", 0.01, "h", max_messages=3)
        assert sent == 3
        ids = [json.loads(v)["id"] for _, v, _ in fake.produced]
        assert ids == [1, 2, 3]

    def test_flushes_on_exit(self):
        fake = FakeProducer()
        producer.run(fake, "messages", 0.01, "h", max_messages=1)
        assert fake.flushed == 10

    def test_respects_interval(self):
        fake = FakeProducer()
        start = time.monotonic()
        producer.run(fake, "messages", 0.05, "h", max_messages=3)
        assert time.monotonic() - start >= 0.1

    def test_stop_signal_ends_loop(self):
        fake = FakeProducer()
        producer.stop(15, None)
        assert producer.run(fake, "messages", 0.01, "h") == 0
        assert fake.produced == [] and fake.flushed == 10

    def test_keeps_running_after_produce_error(self):
        fake = FakeProducer(fail_with=BufferError("full"))
        assert producer.run(fake, "messages", 0.01, "h", max_messages=2) == 2


class TestMain:
    def test_defaults(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(producer, "Producer", lambda conf: FakeProducer(conf))
        monkeypatch.setattr(producer.signal, "signal", lambda *a: None)
        monkeypatch.setattr(producer, "run", lambda p, topic, interval, host: captured.update(
            conf=p.conf, topic=topic, interval=interval))
        for key, value in ENV.items():
            monkeypatch.setenv(key, value)
        monkeypatch.delenv("TOPIC", raising=False)
        monkeypatch.delenv("SEND_INTERVAL_SECONDS", raising=False)

        producer.main()

        assert captured["topic"] == "messages"
        assert captured["interval"] == 5.0
        assert captured["conf"]["bootstrap.servers"] == ENV["BOOTSTRAP_SERVERS"]

    def test_env_overrides(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(producer, "Producer", lambda conf: FakeProducer(conf))
        monkeypatch.setattr(producer.signal, "signal", lambda *a: None)
        monkeypatch.setattr(producer, "run", lambda p, topic, interval, host: captured.update(
            topic=topic, interval=interval))
        for key, value in ENV.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setenv("TOPIC", "other")
        monkeypatch.setenv("SEND_INTERVAL_SECONDS", "1")

        producer.main()

        assert captured == {"topic": "other", "interval": 1.0}
