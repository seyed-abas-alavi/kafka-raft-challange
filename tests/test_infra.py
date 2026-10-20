"""Static checks of the Compose file, helper scripts, certificate script and Grafana setup.

No Docker is needed: the files are parsed and the certificate script is run for real
in a temporary directory (requires `openssl`).
"""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
NODES = ["kafka-1", "kafka-2", "kafka-3"]


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())


@pytest.fixture(scope="module")
def services(compose):
    return compose["services"]


def env_of(service):
    return service["environment"]


# ---------------------------------------------------------------- compose: services
class TestComposeServices:
    def test_required_service_names(self, services):
        for name in ["message-producer", "message-consumer", *NODES]:
            assert name in services

    def test_every_build_context_has_a_dockerfile(self, services):
        for name, svc in services.items():
            if "build" not in svc:
                continue
            build = svc["build"]
            context = build if isinstance(build, str) else build["context"]
            dockerfile = "Dockerfile" if isinstance(build, str) else build.get("dockerfile", "Dockerfile")
            assert (ROOT / context / dockerfile).is_file(), name

    def test_each_app_in_its_own_subfolder(self):
        assert (ROOT / "message-producer" / "producer.py").is_file()
        assert (ROOT / "message-consumer" / "consumer.py").is_file()


    def test_env_file_defines_used_variables(self):
        text = (ROOT / "docker-compose.yml").read_text()
        used = set(re.findall(r"\$\{([A-Z_]+):-", text))
        defined = {
            line.split("=", 1)[0]
            for line in (ROOT / ".env").read_text().splitlines()
            if "=" in line and not line.startswith("#")
        }
        assert used <= defined, f"missing from .env: {used - defined}"

    def test_apps_wait_for_topic_creation(self, services):
        for name in ["message-producer", "message-consumer"]:
            cond = services[name]["depends_on"]["kafka-init"]["condition"]
            assert cond == "service_completed_successfully"

    def test_init_waits_for_all_brokers_healthy(self, services):
        deps = services["kafka-init"]["depends_on"]
        for node in NODES:
            assert deps[node]["condition"] == "service_healthy"

    def test_brokers_have_healthchecks(self, services):
        for node in NODES:
            assert "healthcheck" in services[node]

    def test_observers_cover_every_node(self, services):
        for i, node in enumerate(NODES, start=1):
            env = env_of(services[f"observer-{node}"])
            assert env["NODE_LABEL"] == node
            assert env["CLIENT_RACK"] == f"rack-{i}"
            assert env["GROUP_ID"] == f"observer-{node}"

    def test_observer_groups_are_distinct_from_main_consumer(self, services):
        groups = {env_of(services[f"observer-{n}"])["GROUP_ID"] for n in NODES}
        assert len(groups) == 3
        assert env_of(services["message-consumer"])["GROUP_ID"] not in groups


# ---------------------------------------------------------------- compose: Kafka cluster
class TestKafkaCluster:
    def test_node_ids_unique_and_sequential(self, services):
        ids = [env_of(services[n])["KAFKA_NODE_ID"] for n in NODES]
        assert ids == [1, 2, 3]

    def test_kraft_roles(self, services):
        for n in NODES:
            assert env_of(services[n])["KAFKA_PROCESS_ROLES"] == "broker,controller"
            assert not any("ZOOKEEPER" in key for key in env_of(services[n]))

    def test_quorum_voters_match_nodes(self, services):
        for n in NODES:
            voters = env_of(services[n])["KAFKA_CONTROLLER_QUORUM_VOTERS"].split(",")
            assert voters == [f"{i}@kafka-{i}:9093" for i in (1, 2, 3)]

    def test_same_cluster_id_everywhere(self, services):
        ids = {env_of(services[n])["CLUSTER_ID"] for n in NODES}
        assert len(ids) == 1

    def test_advertised_listener_and_keystore_match_hostname(self, services):
        for n in NODES:
            env = env_of(services[n])
            assert env["KAFKA_ADVERTISED_LISTENERS"] == f"BROKER://{n}:9092"
            assert env["KAFKA_SSL_KEYSTORE_LOCATION"] == f"/certs/{n}.keystore.pem"
            assert services[n]["hostname"] == n

    def test_each_node_has_its_own_rack(self, services):
        racks = [env_of(services[n])["KAFKA_BROKER_RACK"] for n in NODES]
        assert racks == ["rack-1", "rack-2", "rack-3"]

    def test_replication_is_safe_for_one_node_down(self, services):
        env = env_of(services["kafka-1"])
        rf = env["KAFKA_DEFAULT_REPLICATION_FACTOR"]
        assert rf == 3
        assert env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == rf
        assert env["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"] == rf
        assert 1 < env["KAFKA_MIN_INSYNC_REPLICAS"] <= rf - 1

    def test_init_script_creates_topic_with_same_settings(self):
        script = (ROOT / "kafka/scripts/init-topics.sh").read_text()
        assert "--replication-factor 3" in script
        assert "min.insync.replicas=2" in script
        for n in NODES:
            assert f"{n}:9092" in script


# ---------------------------------------------------------------- compose: security
class TestSecurity:
    def test_listeners_use_tls_and_sasl_only(self, services):
        for n in NODES:
            env = env_of(services[n])
            assert env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] == "CONTROLLER:SSL,BROKER:SASL_SSL"
            assert "PLAINTEXT" not in json.dumps(env)
            assert env["KAFKA_SASL_ENABLED_MECHANISMS"] == "PLAIN"
            assert env["KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL"] == "PLAIN"

    def test_controller_uses_mutual_tls(self, services):
        env = env_of(services["kafka-1"])
        assert env["KAFKA_LISTENER_NAME_CONTROLLER_SSL_CLIENT_AUTH"] == "required"

    def test_hostname_verification_enabled(self, services):
        env = env_of(services["kafka-1"])
        assert env["KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM"] == "https"

    def test_jaas_defines_admin_and_app_users(self, services):
        jaas = env_of(services["kafka-1"])["KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG"]
        assert "PlainLoginModule required" in jaas
        assert "user_${ADMIN_USER" in jaas and "user_${APP_USER" in jaas

    def test_clients_connect_to_all_brokers_with_credentials_and_ca(self, services):
        for name in ["message-producer", "message-consumer"]:
            env = env_of(services[name])
            assert env["BOOTSTRAP_SERVERS"] == "kafka-1:9092,kafka-2:9092,kafka-3:9092"
            assert env["KAFKA_CA_FILE"] == "/certs/ca.crt"
            assert "KAFKA_USERNAME" in env and "KAFKA_PASSWORD" in env

    def test_broker_ports_not_published_to_host(self, services):
        for n in NODES:
            assert "ports" not in services[n]


# ---------------------------------------------------------------- scripts
class TestShellScripts:
    @pytest.mark.parametrize("path", sorted(ROOT.rglob("*.sh")), ids=lambda p: str(p.relative_to(ROOT)))
    def test_syntax(self, path):
        subprocess.run(["sh", "-n", str(path)], check=True)

    def test_scripts_use_unix_line_endings(self):
        for path in ROOT.rglob("*.sh"):
            assert b"\r" not in path.read_bytes(), path


# ---------------------------------------------------------------- certificate script
needs_openssl = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not installed")


def run_certs(directory, nodes=None):
    env = {"CERT_DIR": str(directory), "PATH": "/usr/bin:/bin:/usr/local/bin"}
    if nodes:
        env["NODES"] = nodes
    return subprocess.run(
        ["sh", str(ROOT / "certs/generate-certs.sh")],
        env=env, check=True, capture_output=True, text=True,
    ).stdout


@needs_openssl
class TestCertificates:
    def test_generates_ca_and_node_certs(self, tmp_path):
        run_certs(tmp_path)
        names = {p.name for p in tmp_path.iterdir()}
        assert {"ca.crt", ".done"} <= names
        for n in NODES:
            assert {f"{n}.crt", f"{n}.keystore.pem"} <= names

    def test_certs_verify_against_ca_for_their_hostname(self, tmp_path):
        run_certs(tmp_path)
        for n in NODES:
            result = subprocess.run(
                ["openssl", "verify", "-CAfile", str(tmp_path / "ca.crt"),
                 "-verify_hostname", n, str(tmp_path / f"{n}.crt")],
                capture_output=True, text=True,
            )
            assert result.returncode == 0, result.stdout + result.stderr

    def test_cert_does_not_verify_for_another_hostname(self, tmp_path):
        run_certs(tmp_path)
        result = subprocess.run(
            ["openssl", "verify", "-CAfile", str(tmp_path / "ca.crt"),
             "-verify_hostname", "kafka-2", str(tmp_path / "kafka-1.crt")],
            capture_output=True, text=True,
        )
        assert result.returncode != 0

    def test_keystore_is_pem_with_pkcs8_key_and_cert(self, tmp_path):
        run_certs(tmp_path)
        text = (tmp_path / "kafka-1.keystore.pem").read_text()
        assert text.count("-----BEGIN PRIVATE KEY-----") == 1
        assert text.count("-----BEGIN CERTIFICATE-----") == 1
        assert text.index("PRIVATE KEY") < text.index("BEGIN CERTIFICATE")

    def test_ca_private_key_is_removed(self, tmp_path):
        run_certs(tmp_path)
        assert not (tmp_path / "ca.key").exists()
        assert not list(tmp_path.glob("*.csr"))

    def test_second_run_is_a_noop(self, tmp_path):
        run_certs(tmp_path)
        before = (tmp_path / "ca.crt").read_bytes()
        out = run_certs(tmp_path)
        assert "skipping" in out
        assert (tmp_path / "ca.crt").read_bytes() == before

    def test_regenerates_when_a_node_is_missing(self, tmp_path):
        run_certs(tmp_path, nodes="kafka-1 kafka-2")
        assert not (tmp_path / "kafka-3.keystore.pem").exists()
        out = run_certs(tmp_path)
        assert "regenerating" in out
        for n in NODES:
            assert (tmp_path / f"{n}.keystore.pem").exists()
            result = subprocess.run(
                ["openssl", "verify", "-CAfile", str(tmp_path / "ca.crt"), str(tmp_path / f"{n}.crt")],
                capture_output=True, text=True,
            )
            assert result.returncode == 0

    def test_default_node_list_matches_compose(self):
        script = (ROOT / "certs/generate-certs.sh").read_text()
        assert 'NODES="${NODES:-kafka-1 kafka-2 kafka-3}"' in script


# ---------------------------------------------------------------- persistence (volumes)
class TestVolumes:
    def mounts(self, service):
        return {m.split(":")[1]: m.split(":")[0] for m in service.get("volumes", []) if ":" in m}

    def test_each_kafka_node_has_its_own_data_volume(self, services):
        volumes = []
        for n in NODES:
            mounts = self.mounts(services[n])
            assert "/var/lib/kafka/data" in mounts, n
            volumes.append(mounts["/var/lib/kafka/data"])
        assert len(set(volumes)) == 3

    def test_kafka_log_dir_is_the_mounted_path(self, services):
        for n in NODES:
            assert env_of(services[n])["KAFKA_LOG_DIRS"] == "/var/lib/kafka/data"

    def test_loki_grafana_and_alloy_persist_their_data(self, services):
        assert self.mounts(services["loki"])["/loki"] == "loki-data"
        assert self.mounts(services["grafana"])["/var/lib/grafana"] == "grafana-data"
        assert self.mounts(services["alloy"])["/var/lib/alloy/data"] == "alloy-data"

    def test_all_named_volumes_are_declared(self, compose, services):
        declared = set(compose["volumes"])
        used = {
            m.split(":")[0]
            for svc in services.values()
            for m in svc.get("volumes", [])
            if not m.startswith(("./", "/"))
        }
        assert used <= declared, used - declared

    def test_volume_init_prepares_every_data_volume(self, services):
        init_volumes = {m.split(":")[0] for m in services["volume-init"]["volumes"]}
        assert {"kafka-1-data", "kafka-2-data", "kafka-3-data",
                "loki-data", "grafana-data", "alloy-data"} <= init_volumes

    @pytest.mark.parametrize("name", ["kafka-1", "kafka-2", "kafka-3", "loki", "alloy", "grafana"])
    def test_services_wait_for_volume_init(self, services, name):
        assert services[name]["depends_on"]["volume-init"]["condition"] == "service_completed_successfully"

    def test_grafana_dashboards_are_not_inside_the_data_volume(self, services):
        targets = self.mounts(services["grafana"])
        assert not any(t.startswith("/var/lib/grafana/") for t in targets)


# ---------------------------------------------------------------- Grafana / Loki / Alloy
@pytest.fixture(scope="module")
def dashboard():
    return json.loads((ROOT / "monitoring/grafana/dashboards/kafka-logs.json").read_text())


class TestMonitoring:
    def test_datasource_provisioning_matches_dashboard_uid(self, dashboard):
        ds = yaml.safe_load((ROOT / "monitoring/grafana/provisioning/datasources/loki.yml").read_text())
        uid = ds["datasources"][0]["uid"]
        for panel in dashboard["panels"]:
            assert panel["datasource"]["uid"] == uid
            for target in panel["targets"]:
                assert target["datasource"]["uid"] == uid

    def test_dashboard_has_unique_panel_ids_and_fits_grid(self, dashboard):
        ids = [p["id"] for p in dashboard["panels"]]
        assert len(ids) == len(set(ids))
        for p in dashboard["panels"]:
            assert p["gridPos"]["x"] + p["gridPos"]["w"] <= 24

    def test_queries_reference_existing_services(self, dashboard, services):
        for panel in dashboard["panels"]:
            for target in panel["targets"]:
                for service in re.findall(r'service="([^"]+)"', target["expr"]):
                    assert service in services, f"{panel['title']}: {service}"

    def test_every_node_has_log_and_message_panels(self, dashboard):
        exprs = " ".join(t["expr"] for p in dashboard["panels"] for t in p["targets"])
        for n in NODES:
            assert f'service="{n}"' in exprs
            assert f'service="observer-{n}"' in exprs

    def test_dashboard_is_provisioned_and_set_as_home(self, compose):
        grafana = compose["services"]["grafana"]
        assert grafana["environment"]["GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH"].endswith("kafka-logs.json")

    def test_dashboard_only_shows_logs_and_message_counts(self, dashboard):
        for panel in dashboard["panels"]:
            assert panel["type"] in ("stat", "logs")
            assert "unit" not in panel["title"].lower()

    def test_no_error_lines_panel(self, dashboard):
        for panel in dashboard["panels"]:
            assert "error" not in panel["title"].lower()
            for target in panel["targets"]:
                assert "error|exception" not in target["expr"]

    def test_alloy_drops_tooling_containers(self):
        config = (ROOT / "monitoring/alloy/config.alloy").read_text()
        for name in ["alloy", "loki", "grafana", "volume-init"]:
            assert name in config

    def test_provisioned_dashboard_path_matches_mount(self, services):
        provider = yaml.safe_load(
            (ROOT / "monitoring/grafana/provisioning/dashboards/dashboards.yml").read_text())
        path = provider["providers"][0]["options"]["path"]
        targets = [m.split(":")[1] for m in services["grafana"]["volumes"] if ":" in m]
        assert path in targets

    def test_alloy_filters_on_compose_project_name(self, compose):
        config = (ROOT / "monitoring/alloy/config.alloy").read_text()
        assert f'com.docker.compose.project={compose["name"]}' in config
        assert config.count("{") == config.count("}")

    def test_alloy_pushes_to_loki_service(self):
        config = (ROOT / "monitoring/alloy/config.alloy").read_text()
        assert "http://loki:3100/loki/api/v1/push" in config

    def test_loki_config_parses(self):
        yaml.safe_load((ROOT / "monitoring/loki/loki-config.yml").read_text())
