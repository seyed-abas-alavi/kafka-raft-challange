import pytest
from confluent_kafka import KafkaError

import consumer

ENV = {
    "BOOTSTRAP_SERVERS": "kafka-1:9092,kafka-2:9092,kafka-3:9092",
    "KAFKA_USERNAME": "app",
    "KAFKA_PASSWORD": "app-secret",
}


class FakeError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"FakeError({self._code})"


class FakeMsg:
    def __init__(self, value=b'{"id": 1}', partition=0, offset=0, error=None):
        self._value, self._partition, self._offset, self._error = value, partition, offset, error

    def value(self): return self._value
    def partition(self): return self._partition
    def offset(self): return self._offset
    def error(self): return self._error


class FakeConsumer:
    """poll() returns the queued items, then asks the run loop to stop."""

    def __init__(self, items, fail=None):
        self.items = list(items)
        self.fail = fail
        self.closed = False
        self.subscribed = None

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        if self.fail:
            raise self.fail
        if not self.items:
            consumer.running = False
            return None
        return self.items.pop(0)

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def reset_running():
    consumer.running = True
    yield
    consumer.running = True


class TestConfig:
    def test_default_consumer(self):
        conf = consumer.build_config(ENV)
        assert conf["group.id"] == "message-consumer-group"
        assert conf["client.id"] == "message-consumer"
        assert conf["auto.offset.reset"] == "earliest"
        assert "client.rack" not in conf
        assert consumer.get_prefix(ENV) == "[consumer]"

    def test_security_settings(self):
        conf = consumer.build_config(ENV)
        assert conf["security.protocol"] == "SASL_SSL"
        assert conf["sasl.mechanisms"] == "PLAIN"
        assert conf["sasl.username"] == "app" and conf["sasl.password"] == "app-secret"
        assert conf["ssl.ca.location"] == "/certs/ca.crt"

    def test_observer(self):
        env = {**ENV, "NODE_LABEL": "kafka-2", "CLIENT_RACK": "rack-2", "GROUP_ID": "observer-kafka-2"}
        conf = consumer.build_config(env)
        assert conf["client.rack"] == "rack-2"
        assert conf["client.id"] == "observer-kafka-2"
        assert conf["group.id"] == "observer-kafka-2"
        assert consumer.get_prefix(env) == "[observer kafka-2]"

    @pytest.mark.parametrize("missing", ["BOOTSTRAP_SERVERS", "KAFKA_USERNAME", "KAFKA_PASSWORD"])
    def test_missing_required_env_raises(self, missing):
        env = {k: v for k, v in ENV.items() if k != missing}
        with pytest.raises(KeyError):
            consumer.build_config(env)


class TestFormatMessage:
    def test_contains_partition_offset_and_value(self):
        line = consumer.format_message("[consumer]", FakeMsg(b'{"id": 5}', partition=2, offset=9))
        assert line == '[consumer] partition=2 offset=9 value={"id": 5}'

    def test_invalid_utf8_is_replaced_not_raised(self):
        line = consumer.format_message("[consumer]", FakeMsg(b"\xff\xfe"))
        assert "value=" in line


class TestProcessMessage:
    def test_none_message(self, capsys):
        assert consumer.process_message("[consumer]", None) is None
        assert capsys.readouterr().out == ""

    def test_prints_valid_message(self, capsys):
        line = consumer.process_message("[consumer]", FakeMsg())
        assert line in capsys.readouterr().out

    def test_partition_eof_is_silent(self, capsys):
        msg = FakeMsg(error=FakeError(KafkaError._PARTITION_EOF))
        assert consumer.process_message("[consumer]", msg) is None
        assert capsys.readouterr().out == ""

    def test_other_errors_are_reported(self, capsys):
        msg = FakeMsg(error=FakeError(KafkaError._TRANSPORT))
        assert consumer.process_message("[consumer]", msg) is None
        assert "error" in capsys.readouterr().out


class TestRun:
    def test_prints_all_messages_and_closes(self, capsys):
        fake = FakeConsumer([FakeMsg(b"a", offset=0), None, FakeMsg(b"b", offset=1)])
        consumer.run(fake, "[consumer]")
        out = capsys.readouterr().out
        assert "offset=0 value=a" in out and "offset=1 value=b" in out
        assert fake.closed
        assert "[consumer] stopped" in out

    def test_closes_when_poll_raises(self):
        fake = FakeConsumer([], fail=RuntimeError("boom"))
        with pytest.raises(RuntimeError):
            consumer.run(fake, "[consumer]")
        assert fake.closed

    def test_stop_signal_ends_loop(self):
        consumer.stop(15, None)
        fake = FakeConsumer([FakeMsg()])
        consumer.run(fake, "[consumer]")
        assert fake.closed and len(fake.items) == 1


class TestMain:
    def test_subscribes_to_topic_and_runs(self, monkeypatch):
        created = {}

        def make(conf):
            created["consumer"] = FakeConsumer([])
            created["conf"] = conf
            return created["consumer"]

        monkeypatch.setattr(consumer, "Consumer", make)
        monkeypatch.setattr(consumer.signal, "signal", lambda *a: None)
        for key, value in ENV.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setenv("TOPIC", "messages")
        monkeypatch.delenv("NODE_LABEL", raising=False)
        monkeypatch.delenv("CLIENT_RACK", raising=False)

        consumer.main()

        assert created["consumer"].subscribed == ["messages"]
        assert created["consumer"].closed
        assert created["conf"]["bootstrap.servers"] == ENV["BOOTSTRAP_SERVERS"]
