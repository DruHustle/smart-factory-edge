import importlib
import json
import sys
import types
import unittest
import tempfile
from unittest.mock import patch
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _purge_modules(*names: str) -> None:
    for name in names:
        sys.modules.pop(name, None)


def _install_dotenv_stub() -> None:
    dotenv_mod = types.ModuleType("dotenv")
    dotenv_mod.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_mod


class GatewayConfigurationTests(unittest.TestCase):
    def test_telemetry_intervals_leave_headroom_for_dashboard_offline_timeout(self):
        _purge_modules("config")
        with patch.dict("os.environ", {"EDGE_ENV": "development"}, clear=True):
            from config import load_settings
            self.assertEqual(load_settings().heartbeat_interval_seconds, 60)
            self.assertEqual(load_settings().telemetry_snapshot_seconds, 60)
        for name in ("HEARTBEAT_INTERVAL_SECONDS", "TELEMETRY_SNAPSHOT_SECONDS"):
            for value in ("0", "121"):
                with patch.dict("os.environ", {"EDGE_ENV": "development", name: value}, clear=True):
                    with self.assertRaisesRegex(RuntimeError, "between 1 and 60"):
                        load_settings()

    def test_asset_polling_defaults_to_subsecond_for_motion_telemetry(self):
        _purge_modules("config")
        with patch.dict("os.environ", {"EDGE_ENV": "development"}, clear=True):
            from config import load_settings
            self.assertEqual(load_settings().asset_poll_interval_seconds, 0.5)


class FakeMqttClient:
    def __init__(self):
        self.published = []
        self.subscribed = []
        self.connected = None
        self.loop_started = False
        self.loop_stopped = False
        self.disconnected = False
        self.acknowledged = []

    def ack(self, mid, qos):
        self.acknowledged.append((mid, qos))
        return 0

    def username_pw_set(self, *_args, **_kwargs):
        pass

    def tls_set(self, *_args, **_kwargs):
        self.tls_options = (_args, _kwargs)

    def connect(self, host, port, keepalive=60):
        self.connected = (host, port, keepalive)

    def loop_start(self):
        self.loop_started = True

    def loop_stop(self):
        self.loop_stopped = True

    def disconnect(self):
        self.disconnected = True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))
        return types.SimpleNamespace(rc=0)

    def subscribe(self, topic, qos=0):
        self.subscribed.append(topic)


def _install_paho_stub() -> None:
    client_mod = types.ModuleType("paho.mqtt.client")
    client_mod.Client = lambda *args, **kwargs: FakeMqttClient()
    client_mod.MQTT_ERR_SUCCESS = 0
    client_mod.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)

    mqtt_mod = types.ModuleType("paho.mqtt")
    mqtt_mod.client = client_mod

    paho_mod = types.ModuleType("paho")
    paho_mod.mqtt = mqtt_mod

    sys.modules["paho"] = paho_mod
    sys.modules["paho.mqtt"] = mqtt_mod
    sys.modules["paho.mqtt.client"] = client_mod


class OneShotStop:
    def __init__(self):
        self._stopped = False

    def is_set(self):
        return self._stopped

    def set(self):
        self._stopped = True

    def wait(self, _seconds):
        self._stopped = True
        return True


class InlineThread:
    def __init__(self, target=None, args=(), daemon=False):
        self.target = target
        self.args = args
        self.daemon = daemon

    def start(self):
        if self.target is not None:
            if getattr(self.target, "__name__", "") == "asset_poll_loop" and not self.args[1]:
                return
            self.target(*self.args)


class GatewaySmokeTests(unittest.TestCase):
    def test_mqtt_tls_uses_configured_ca_certificate(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(
            mqtt_client_id="pi-edge-01",
            mqtt_username="gateway-user",
            mqtt_password="gateway-secret",
            mqtt_use_tls=True,
            mqtt_ca_cert="/etc/ssl/smart-factory/broker-ca.pem",
            site_id="factory-a",
            line_id="line-1",
            device_id="pi-edge-01",
        )
        client = sensor_gateway.make_mqtt_client(settings)
        self.assertEqual(client.tls_options[1]["ca_certs"], settings.mqtt_ca_cert)

    def setUp(self):
        _purge_modules("sensor_gateway", "dotenv", "paho", "paho.mqtt", "paho.mqtt.client")
        _install_dotenv_stub()
        _install_paho_stub()

    def _run_gateway_once(self, serial_enabled: bool, asset_configs=None, asset_reading=None):
        sensor_gateway = importlib.import_module("sensor_gateway")

        settings = types.SimpleNamespace(
            device_id="pi-edge-01",
            site_id="factory-a",
            line_id="line-1",
            mqtt_host="127.0.0.1",
            mqtt_port=1883,
            mqtt_username="",
            mqtt_password="",
            mqtt_use_tls=False,
            mqtt_ca_cert="",
            mqtt_client_id="pi-edge-01",
            serial_enabled=serial_enabled,
            serial_port="/dev/ttyUSB0",
            serial_baud=115200,
            asset_config_file="",
            synthetic_telemetry_enabled=not serial_enabled,
            asset_poll_interval_seconds=1,
            publish_interval_seconds=1,
            ada031_control_asset_ids=(),
        )

        fake_client = FakeMqttClient()

        sensor_gateway.load_settings = lambda: settings
        sensor_gateway.load_asset_connections = lambda _path, _gateway_id=None: asset_configs or []
        sensor_gateway.make_mqtt_client = lambda _settings, *_args: fake_client
        sensor_gateway.serial_reader = lambda *_args, **_kwargs: None
        sensor_gateway.heartbeat_loop = lambda *_args, **_kwargs: None
        sensor_gateway.signal.signal = lambda *_args, **_kwargs: None
        sensor_gateway.Thread = InlineThread
        sensor_gateway.STOP = OneShotStop()
        if asset_configs:
            sensor_gateway.read_asset = lambda _asset, *_args: asset_reading or {"temperature": 67.5, "pressure": 7.2}

        sensor_gateway.main()
        return fake_client, settings, sensor_gateway

    def test_gateway_applies_only_valid_configuration_command(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(mqtt_client_id="pi-edge-01", mqtt_username="", mqtt_password="", mqtt_use_tls=False, mqtt_ca_cert="", site_id="factory-a", line_id="line-1", device_id="pi-edge-01")
        applied = []
        client = sensor_gateway.make_mqtt_client(settings, lambda profile: applied.append(profile) or "profile-hash")
        profile = {"schemaVersion": 1, "gatewayDeviceId": settings.device_id, "assets": []}
        message = types.SimpleNamespace(topic=sensor_gateway.command_topic(settings.site_id, settings.line_id, settings.device_id), payload=json.dumps({"action": "replace_asset_configuration", "configuration": profile}).encode())
        client.on_message(client, None, message)
        self.assertEqual(applied, [profile])
        ack = json.loads(client.published[-1][1])
        self.assertEqual(ack["status"], "applied")
        self.assertEqual(ack["configurationHash"], "profile-hash")

    def test_gateway_rejects_unsupported_command_action(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(mqtt_client_id="pi-edge-01", mqtt_username="", mqtt_password="", mqtt_use_tls=False, mqtt_ca_cert="", site_id="factory-a", line_id="line-1", device_id="pi-edge-01")
        client = sensor_gateway.make_mqtt_client(settings, lambda _profile: "never")
        message = types.SimpleNamespace(topic=sensor_gateway.command_topic(settings.site_id, settings.line_id, settings.device_id), payload=b'{"action":"execute"}')
        client.on_message(client, None, message)
        self.assertEqual(json.loads(client.published[-1][1])["status"], "rejected")

    def test_gateway_routes_v2_ada031_profile_command(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(
            mqtt_client_id="pi-edge-01", mqtt_username="", mqtt_password="", mqtt_use_tls=False,
            mqtt_ca_cert="", site_id="factory-a", line_id="line-1", device_id="pi-edge-01",
        )
        relayed = []
        client = sensor_gateway.make_mqtt_client(
            settings,
            relay_ada031_control=lambda command: relayed.append(command) or {
                "commandId": command["commandId"], "status": "serial_write_accepted",
            },
        )
        command = {
            "schemaVersion": 2, "action": "set_profile", "profile": "pick_and_place_repeat",
            "commandId": "profile-0001", "targetAssetId": "urn:test:arm",
            "expiresAt": int(sensor_gateway.time.time() * 1000) + 5000,
        }
        topic = sensor_gateway.command_topic(settings.site_id, settings.line_id, settings.device_id)
        client.on_message(client, None, types.SimpleNamespace(topic=topic, payload=json.dumps(command).encode(), retain=False))
        self.assertEqual(relayed, [command])
        self.assertEqual(json.loads(client.published[-1][1])["status"], "serial_write_accepted")

    def test_gateway_relays_only_valid_short_lived_wrover_gpio_commands(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(
            mqtt_client_id="pi-edge-01", mqtt_username="", mqtt_password="", mqtt_use_tls=False,
            mqtt_ca_cert="", site_id="factory-a", line_id="line-1", device_id="pi-edge-01",
            local_mqtt_device_ids=("esp32-wrover-01",),
            local_mqtt_control_device_ids=("esp32-wrover-01",),
        )
        local = FakeMqttClient()
        client = sensor_gateway.make_mqtt_client(
            settings,
            relay_control=lambda command: sensor_gateway.relay_gpio_command(settings, local, command),
        )
        command = {
            "schemaVersion": 1, "action": "set_gpio", "commandId": "gpio-test-0001",
            "targetDeviceId": "esp32-wrover-01", "pin": 18, "value": 1,
            "holdMs": 250, "expiresAt": int(sensor_gateway.time.time() * 1000) + 5000,
        }
        message = types.SimpleNamespace(
            topic=sensor_gateway.command_topic(settings.site_id, settings.line_id, settings.device_id),
            payload=json.dumps(command).encode(), retain=False,
        )
        client.on_message(client, None, message)
        self.assertEqual(local.published[0][0], "factory/factory-a/line-1/esp32-wrover-01/commands")
        self.assertEqual(json.loads(local.published[0][1]), {key: command[key] for key in ("schemaVersion", "action", "commandId", "pin", "value", "holdMs", "expiresAt")})
        self.assertEqual(local.published[0][2:], (1, False))
        self.assertEqual(json.loads(client.published[-1][1])["status"], "queued")

    def test_gateway_rejects_gpio_commands_outside_control_allowlist_or_retained(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(
            mqtt_client_id="pi-edge-01", mqtt_username="", mqtt_password="", mqtt_use_tls=False,
            mqtt_ca_cert="", site_id="factory-a", line_id="line-1", device_id="pi-edge-01",
            local_mqtt_device_ids=("esp32-wrover-01",), local_mqtt_control_device_ids=(),
        )
        local = FakeMqttClient()
        client = sensor_gateway.make_mqtt_client(settings, relay_control=lambda command: sensor_gateway.relay_gpio_command(settings, local, command))
        command = {
            "schemaVersion": 1, "action": "set_gpio", "commandId": "gpio-test-0002",
            "targetDeviceId": "esp32-wrover-01", "pin": 18, "value": 1,
            "holdMs": 250, "expiresAt": int(sensor_gateway.time.time() * 1000) + 5000,
        }
        topic = sensor_gateway.command_topic(settings.site_id, settings.line_id, settings.device_id)
        client.on_message(client, None, types.SimpleNamespace(topic=topic, payload=json.dumps(command).encode(), retain=False))
        self.assertEqual(json.loads(client.published[-1][1])["status"], "rejected")
        self.assertEqual(local.published, [])

        settings.local_mqtt_control_device_ids = ("esp32-wrover-01",)
        command["pin"] = 25
        client.on_message(client, None, types.SimpleNamespace(topic=topic, payload=json.dumps(command).encode(), retain=False))
        self.assertEqual(json.loads(client.published[-1][1])["status"], "rejected")
        self.assertEqual(local.published, [])

        command["pin"] = 18
        client.on_message(client, None, types.SimpleNamespace(topic=topic, payload=json.dumps(command).encode(), retain=True))
        self.assertEqual(json.loads(client.published[-1][1])["status"], "rejected")
        self.assertEqual(local.published, [])

    def test_gateway_forwards_only_matching_wrover_control_acknowledgements(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(
            site_id="factory-a", line_id="line-1", device_id="pi-edge-01",
            local_mqtt_control_device_ids=("esp32-wrover-01",),
        )
        cloud = FakeMqttClient()
        topic = "factory/factory-a/line-1/esp32-wrover-01/control-ack"
        payload = json.dumps({
            "deviceId": "esp32-wrover-01", "commandId": "gpio-test-0003",
            "status": "applied", "timestamp": int(sensor_gateway.time.time() * 1000), "pin": 18, "value": 1,
        }).encode()
        self.assertTrue(sensor_gateway.forward_local_control_ack(settings, cloud, types.SimpleNamespace(topic=topic, payload=payload)))
        self.assertEqual(cloud.published[0][0], "factory/factory-a/line-1/pi-edge-01/commands/ack")
        self.assertEqual(json.loads(cloud.published[0][1])["gatewayDeviceId"], "pi-edge-01")
        self.assertFalse(sensor_gateway.forward_local_control_ack(settings, cloud, types.SimpleNamespace(topic=topic.replace("esp32-wrover-01", "unlisted-device"), payload=payload)))

    def test_local_wrover_telemetry_is_forwarded_only_when_allowlisted(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(site_id="factory-a", line_id="line-1", device_id="pi-edge-01", local_mqtt_device_ids=("esp32-wrover-01",))
        cloud = FakeMqttClient()
        topic = "factory/factory-a/line-1/esp32-wrover-01/telemetry"
        payload = json.dumps({
            "deviceId": "esp32-wrover-01", "timestamp": 1770000000000,
            "sensorType": "DHT11", "sensorStatus": "ok", "temperature": 25.5, "humidity": 55.0,
        }).encode()
        message = types.SimpleNamespace(topic=topic, payload=payload)
        self.assertTrue(sensor_gateway.forward_local_message(settings, cloud, message))
        forwarded = json.loads(cloud.published[0][1])
        self.assertEqual(forwarded["deviceId"], "esp32-wrover-01")
        self.assertEqual(forwarded["sensorType"], "DHT11")
        self.assertEqual(forwarded["sensorStatus"], "ok")
        self.assertEqual(forwarded["humidity"], 55.0)
        self.assertEqual(forwarded["gatewayId"], "pi-edge-01")
        self.assertEqual(cloud.published[0][0], topic)
        self.assertEqual(cloud.published[0][2:], (1, False))

        invalid_status = types.SimpleNamespace(
            topic=topic,
            payload=json.dumps({
                "deviceId": "esp32-wrover-01", "timestamp": 1770000000000,
                "sensorType": "DHT11", "sensorStatus": "connected_but_invented",
                "temperature": 25.5, "humidity": 55.0,
            }).encode(),
        )
        self.assertFalse(sensor_gateway.forward_local_message(settings, cloud, invalid_status))
        self.assertEqual(len(cloud.published), 1)

        missing_reading = types.SimpleNamespace(
            topic=topic,
            payload=json.dumps({
                "deviceId": "esp32-wrover-01", "timestamp": 1770000000000,
                "sensorType": "DHT11", "sensorStatus": "ok", "temperature": 25.5,
            }).encode(),
        )
        self.assertFalse(sensor_gateway.forward_local_message(settings, cloud, missing_reading))

        failed_with_stale_values = types.SimpleNamespace(
            topic=topic,
            payload=json.dumps({
                "deviceId": "esp32-wrover-01", "timestamp": 1770000000000,
                "sensorType": "DHT11", "sensorStatus": "read_error",
                "temperature": 25.5, "humidity": 55.0,
            }).encode(),
        )
        self.assertFalse(sensor_gateway.forward_local_message(settings, cloud, failed_with_stale_values))

    def test_local_wrover_ingress_rejects_unlisted_identity_and_bad_payload(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(site_id="factory-a", line_id="line-1", local_mqtt_device_ids=("esp32-wrover-01",))
        cloud = FakeMqttClient()
        topic = "factory/factory-a/line-1/unlisted-device/telemetry"
        message = types.SimpleNamespace(topic=topic, payload=b'{"deviceId":"unlisted-device","timestamp":1770000000000}')
        self.assertFalse(sensor_gateway.forward_local_message(settings, cloud, message))
        bad_topic = "factory/factory-a/line-1/esp32-wrover-01/telemetry"
        bad_payload = types.SimpleNamespace(topic=bad_topic, payload=b'{"deviceId":"esp32-wrover-01","timestamp":true}')
        self.assertFalse(sensor_gateway.forward_local_message(settings, cloud, bad_payload))
        self.assertEqual(cloud.published, [])

    def test_local_wrover_ingress_rejects_unbounded_integer_metrics(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(site_id="factory-a", line_id="line-1", local_mqtt_device_ids=("esp32-wrover-01",))
        topic = "factory/factory-a/line-1/esp32-wrover-01/telemetry"
        payload = (b'{"deviceId":"esp32-wrover-01","timestamp":1770000000000,"temperature":' + b"9" * 4000 + b"}")
        message = types.SimpleNamespace(topic=topic, payload=payload)
        self.assertFalse(sensor_gateway.forward_local_message(settings, FakeMqttClient(), message))

    def test_local_wrover_ingress_rejects_humidity_outside_percentage_range(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(
            site_id="factory-a", line_id="line-1",
            local_mqtt_device_ids=("esp32-wrover-01",),
        )
        topic = "factory/factory-a/line-1/esp32-wrover-01/telemetry"
        for humidity in (-0.1, 100.1):
            payload = json.dumps({
                "deviceId": "esp32-wrover-01", "timestamp": 1770000000000,
                "sensorType": "DHT11", "sensorStatus": "ok",
                "temperature": 25.5, "humidity": humidity,
            }).encode()
            message = types.SimpleNamespace(topic=topic, payload=payload)
            self.assertFalse(sensor_gateway.forward_local_message(settings, FakeMqttClient(), message))

    def test_local_broker_ingress_subscribes_only_to_gateway_scope(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(local_mqtt_client_id="pi-edge-ingress", local_mqtt_username="gateway", local_mqtt_password="secret", local_mqtt_use_tls=False, local_mqtt_ca_cert="", site_id="factory-a", line_id="line-1", local_mqtt_device_ids=("esp32-wrover-01",))
        cloud = FakeMqttClient()
        local = sensor_gateway.make_local_mqtt_client(settings, cloud)
        local.on_connect(local, None, None, 0)
        self.assertEqual(local.subscribed, ["factory/factory-a/line-1/+/telemetry", "factory/factory-a/line-1/+/heartbeat"])

    def test_local_ack_waits_for_durable_acceptance_and_failure_requests_restart(self):
        gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(local_mqtt_client_id="test", local_mqtt_username="", local_mqtt_use_tls=False,
            site_id="factory-a", line_id="line-1", local_mqtt_device_ids=("esp32-wrover-01",), device_id="pi-edge-01")
        cloud = FakeMqttClient()
        cloud.publish = lambda *a, **kw: types.SimpleNamespace(rc=1)
        local = gateway.make_local_mqtt_client(settings, cloud)
        msg = types.SimpleNamespace(topic="factory/factory-a/line-1/esp32-wrover-01/telemetry", mid=42, qos=1,
            payload=json.dumps({"deviceId": "esp32-wrover-01", "timestamp": 1800000000000, "temperature": 20}).encode())
        local.on_message(local, None, msg)
        self.assertEqual(local.acknowledged, [])
        self.assertTrue(gateway.STOP.is_set())
        self.assertTrue(gateway.INGRESS_FAILED.is_set())

    def test_local_ack_accepts_durable_and_permanently_invalid_messages(self):
        gateway = importlib.import_module("sensor_gateway")
        settings = types.SimpleNamespace(local_mqtt_client_id="test", local_mqtt_username="", local_mqtt_use_tls=False,
            site_id="factory-a", line_id="line-1", local_mqtt_device_ids=("esp32-wrover-01",), device_id="pi-edge-01")
        cloud = FakeMqttClient()
        local = gateway.make_local_mqtt_client(settings, cloud)
        msg = types.SimpleNamespace(topic="factory/factory-a/line-1/esp32-wrover-01/telemetry", mid=42, qos=1,
            payload=json.dumps({"deviceId": "esp32-wrover-01", "timestamp": 1800000000000, "temperature": 20}).encode())
        local.on_message(local, None, msg)
        self.assertEqual(len(cloud.published), 1)
        self.assertEqual(local.acknowledged, [(42, 1)])
        msg.mid = 43
        msg.payload = b"invalid-json"
        local.on_message(local, None, msg)
        self.assertEqual(local.acknowledged, [(42, 1), (43, 1)])
        self.assertFalse(gateway.INGRESS_FAILED.is_set())

    def test_gateway_does_not_queue_heartbeats_while_disconnected(self):
        gateway = importlib.import_module("sensor_gateway")
        client = FakeMqttClient()
        settings = types.SimpleNamespace(device_id="pi-edge-01", site_id="a", line_id="b", heartbeat_interval_seconds=60)
        for connected in (False, True):
            gateway.STOP = OneShotStop()
            client.is_connected = lambda: connected
            gateway.heartbeat_loop(settings, client)
        self.assertEqual(len(client.published), 1)

    def test_synthetic_telemetry_published_when_serial_disabled(self):
        fake_client, settings, sensor_gateway = self._run_gateway_once(serial_enabled=False)
        telemetry_topic = sensor_gateway.telemetry_topic(settings.site_id, settings.line_id, settings.device_id)
        telemetry_messages = [m for m in fake_client.published if m[0] == telemetry_topic]
        self.assertEqual(len(telemetry_messages), 1)
        payload = json.loads(telemetry_messages[0][1])
        self.assertEqual(payload["deviceId"], settings.device_id)

    def test_synthetic_telemetry_not_published_when_serial_enabled(self):
        fake_client, settings, sensor_gateway = self._run_gateway_once(serial_enabled=True)
        telemetry_topic = sensor_gateway.telemetry_topic(settings.site_id, settings.line_id, settings.device_id)
        telemetry_messages = [m for m in fake_client.published if m[0] == telemetry_topic]
        self.assertEqual(len(telemetry_messages), 0)

    def test_serial_worker_retries_when_usb_path_is_missing_at_startup(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        attempts = []

        class RetryStop:
            stopped = False
            waits = 0
            def is_set(self):
                return self.stopped
            def wait(self, _seconds):
                self.waits += 1
                if self.waits >= 2:
                    self.stopped = True

        sensor_gateway.STOP = RetryStop()
        sensor_gateway.serial = types.SimpleNamespace(
            Serial=lambda *_args, **_kwargs: attempts.append(True) or (_ for _ in ()).throw(FileNotFoundError("not enumerated")),
        )
        settings = types.SimpleNamespace(serial_enabled=True, serial_port="/dev/serial/by-id/arm", serial_baud=9600)
        sensor_gateway.serial_reader(settings, lambda _reading: None)
        self.assertEqual(len(attempts), 2)

    def test_downloaded_asset_profile_publishes_normalized_machine_reading(self):
        asset = {"assetId": "urn:test:compressor", "protocol": "opcua", "endpoint": "opc.tcp://plc:4840", "tagMappings": []}
        fake_client, settings, sensor_gateway = self._run_gateway_once(serial_enabled=False, asset_configs=[asset])
        topic = sensor_gateway.telemetry_topic(settings.site_id, settings.line_id, settings.device_id)
        message = next(item for item in fake_client.published if item[0] == topic)
        payload = json.loads(message[1])
        self.assertEqual(payload["assetId"], asset["assetId"])
        self.assertEqual(payload["temperature"], 67.5)
        self.assertEqual(payload["pressure"], 7.2)
        self.assertIsInstance(payload["timestamp"], int)

    def test_ada031_joint_signals_are_forwarded_in_asset_telemetry(self):
        asset = {
            "assetId": "urn:example:asset:adeept:ADA031:001",
            "protocol": "serial",
            "endpoint": "serial:///dev/serial/by-id/ada031?baudrate=115200",
            "tagMappings": [],
        }
        reading = {"assetSignals": {"joint1TargetDeg": 90.0, "joint5TargetDeg": 42.0}}
        fake_client, settings, sensor_gateway = self._run_gateway_once(
            serial_enabled=False,
            asset_configs=[asset],
            asset_reading=reading,
        )
        topic = sensor_gateway.telemetry_topic(settings.site_id, settings.line_id, settings.device_id)
        payload = json.loads(next(item for item in fake_client.published if item[0] == topic)[1])
        self.assertEqual(payload["assetId"], asset["assetId"])
        self.assertEqual(payload["assetSignals"], reading["assetSignals"])

    def test_ada031_asset_poll_forwards_connected_firmware_signals(self):
        sensor_gateway = importlib.import_module("sensor_gateway")
        asset = {
            "assetId": "urn:test:arm", "protocol": "ada031_v4_serial",
            "endpoint": "serial:///dev/serial/by-id/arm?baudrate=9600", "tagMappings": [],
        }
        reading = {"assetSignals": {"cycle_count": 7.0, "servo_1_deg": 90.0}}
        published = []
        active_sets = []
        generic_manager = types.SimpleNamespace(close_unused=lambda _ids: None, close=lambda: None)
        ada_manager = types.SimpleNamespace(
            close_unused=lambda ids: active_sets.append(ids),
            read_telemetry=lambda _asset: reading,
        )
        settings = types.SimpleNamespace(
            asset_poll_interval_seconds=1,
            ada031_control_asset_ids=(asset["assetId"],),
        )
        sensor_gateway.STOP = OneShotStop()
        sensor_gateway.asset_poll_loop(
            settings, [asset], published.append,
            serial_connection_manager=generic_manager,
            ada031_connection_manager=ada_manager,
        )
        self.assertEqual(active_sets, [{asset["assetId"]}])
        self.assertEqual(published[0]["assetId"], asset["assetId"])
        self.assertEqual(published[0]["assetSignals"], reading["assetSignals"])


class AssetAdapterTests(unittest.TestCase):
    def setUp(self):
        _purge_modules("asset_adapters", "ada031_control")
        self.adapters = importlib.import_module("asset_adapters")
        self.ada031 = importlib.import_module("ada031_control")

    def test_opcua_fails_closed_without_secure_channel_settings(self):
        class FakeClient:
            def set_security_string(self, _value):
                raise AssertionError("No security string should be installed")
        with patch.dict("os.environ", {"OPCUA_SECURITY_POLICY": "", "OPCUA_ALLOW_INSECURE": "false"}, clear=False):
            with self.assertRaisesRegex(ValueError, "secure channel is required"):
                self.adapters.configure_opcua_security(FakeClient())

    def test_opcua_uses_configured_sign_and_encrypt_certificate(self):
        class FakeClient:
            security = None
            def set_security_string(self, value):
                self.security = value
        env = {
            "OPCUA_SECURITY_POLICY": "Basic256Sha256",
            "OPCUA_SECURITY_MODE": "SignAndEncrypt",
            "OPCUA_CLIENT_CERT": "/etc/smart-factory-iot/certs/client.pem",
            "OPCUA_CLIENT_KEY": "/etc/smart-factory-iot/certs/client-key.pem",
            "OPCUA_SERVER_CERT": "/etc/smart-factory-iot/certs/server.pem",
        }
        client = FakeClient()
        with patch.dict("os.environ", env, clear=False):
            self.adapters.configure_opcua_security(client)
        self.assertEqual(client.security, "Basic256Sha256,SignAndEncrypt,/etc/smart-factory-iot/certs/client.pem,/etc/smart-factory-iot/certs/client-key.pem,/etc/smart-factory-iot/certs/server.pem")

    def test_asset_profile_is_saved_atomically_with_private_permissions(self):
        from asset_adapters import load_asset_connections, save_asset_profile
        profile = {"schemaVersion": 1, "gatewayDeviceId": "pi-edge-01", "assets": [{"assetId": "urn:test:pump", "protocol": "modbus_tcp", "endpoint": "modbus://plc:502", "tagMappings": [{"metric": "pressure", "address": 4, "registerType": "holding", "scale": 0.1}]}]}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "nested" / "assets.json"
            profile_hash = save_asset_profile(str(target), profile, "pi-edge-01")
            self.assertEqual(len(profile_hash), 64)
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            self.assertEqual(load_asset_connections(str(target), "pi-edge-01")[0]["assetId"], "urn:test:pump")

    def test_asset_profile_rejects_arbitrary_mapping_fields(self):
        from asset_adapters import validate_asset_profile
        profile = {"schemaVersion": 1, "gatewayDeviceId": "pi-edge-01", "assets": [{"assetId": "urn:test:pump", "protocol": "mqtt", "endpoint": "mqtt://broker", "tagMappings": [{"metric": "pressure", "password": "must-not-be-stored"}]}]}
        with self.assertRaisesRegex(ValueError, "unsupported fields"):
            validate_asset_profile(profile, "pi-edge-01")

    def test_ada031_serial_profile_is_valid_and_allows_named_source_mappings(self):
        profile = {
            "schemaVersion": 1,
            "gatewayDeviceId": "pi-edge-01",
            "assets": [{
                "assetId": "urn:example:asset:adeept:ADA031:001",
                "assetName": "Adeept ADA031 Robotic Arm",
                "protocol": "serial",
                "endpoint": "serial:///dev/serial/by-id/ada031?baudrate=115200",
                "tagMappings": [{"metric": "power", "source": "controllerPowerW", "scale": 1.0}],
            }],
        }
        validated = self.adapters.validate_asset_profile(profile, "pi-edge-01")
        self.assertEqual(validated[0]["protocol"], "serial")
        self.assertEqual(validated[0]["tagMappings"][0]["source"], "controllerPowerW")

    def test_serial_signal_values_are_forwarded_and_validated(self):
        values = self.adapters._serial_record_values(
            {
                "controllerPowerW": 3.5,
                "signals": {"joint1TargetDeg": 90, "gripperTargetDeg": 32.5},
            },
            [{"metric": "power", "source": "controllerPowerW", "scale": 2}],
        )
        self.assertEqual(values["power"], 7.0)
        self.assertEqual(values["assetSignals"]["joint1TargetDeg"], 90.0)
        with self.assertRaisesRegex(ValueError, "finite number"):
            self.adapters._serial_record_values({"signals": {"joint1TargetDeg": float("nan")}}, [])

    def test_serial_asset_claims_its_usb_device_against_standalone_reader(self):
        assets = [{
            "protocol": "serial",
            "endpoint": "serial:///dev/ttyUSB0?baudrate=115200",
        }]
        self.assertTrue(self.adapters.serial_asset_uses_port(assets, "/dev/ttyUSB0"))
        self.assertFalse(self.adapters.serial_asset_uses_port(assets, "/dev/ttyUSB1"))

    def test_stable_serial_symlink_and_kernel_alias_have_one_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            kernel_path = Path(directory) / "ttyUSB0"
            stable_path = Path(directory) / "usb-controller-if00-port0"
            kernel_path.touch()
            stable_path.symlink_to(kernel_path)
            assets = [{
                "protocol": "ada031_v4_serial",
                "endpoint": f"serial://{stable_path}?baudrate=9600",
            }]
            self.assertTrue(self.adapters.serial_asset_uses_port(assets, str(kernel_path)))

            profile = {
                "schemaVersion": 1,
                "gatewayDeviceId": "pi-edge-01",
                "assets": [
                    {"assetId": "urn:test:arm", "protocol": "ada031_v4_serial", "endpoint": f"serial://{stable_path}?baudrate=9600", "tagMappings": []},
                    {"assetId": "urn:test:serial", "protocol": "serial", "endpoint": f"serial://{kernel_path}?baudrate=115200", "tagMappings": []},
                ],
            }
            with self.assertRaisesRegex(ValueError, "one asset profile"):
                self.adapters.validate_asset_profile(profile, "pi-edge-01")

    def test_serial_json_connection_is_reused_across_polls(self):
        class FakePort:
            def __init__(self):
                self.lines = [
                    b"booting vendor firmware\n",
                    b'{"signals":{"joint1TargetDeg":90}}\n',
                    b'{"signals":{"joint1TargetDeg":91}}\n',
                ]
                self.closed = False

            def readline(self):
                return self.lines.pop(0) if self.lines else b""

            def close(self):
                self.closed = True

        opened = []
        fake_serial = types.ModuleType("serial")
        fake_serial.Serial = lambda *args, **kwargs: opened.append((args, kwargs)) or FakePort()
        manager = self.adapters.SerialJsonConnectionManager()
        endpoint = "serial:///dev/ttyACM0?baudrate=115200&timeout=0.1&settle=0"
        asset = {
            "assetId": "urn:example:asset:adeept:ADA031:001",
            "protocol": "serial",
            "endpoint": endpoint,
            "tagMappings": [],
        }
        with patch.dict(sys.modules, {"serial": fake_serial}):
            first = self.adapters.read_asset(asset, manager)
            second = self.adapters.read_asset(asset, manager)
        self.assertEqual(first["assetSignals"]["joint1TargetDeg"], 90.0)
        self.assertEqual(second["assetSignals"]["joint1TargetDeg"], 91.0)
        self.assertEqual(len(opened), 1)
        manager.close()
        profile = {
            "schemaVersion": 1,
            "gatewayDeviceId": "pi-edge-01",
            "assets": [{
                "assetId": "urn:test:compressor",
                "protocol": "modbus_tcp",
                "endpoint": "10.0.0.20:502",
                "tagMappings": [{"metric": "temperature", "address": 100}],
            }],
        }
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as file:
            json.dump(profile, file)
            file.flush()
            loaded = self.adapters.load_asset_connections(file.name)
        self.assertEqual(loaded[0]["assetId"], "urn:test:compressor")

    def test_ada031_example_profile_matches_the_edge_configuration_schema(self):
        example = Path(__file__).resolve().parents[1] / "examples" / "ada031-edge-profile.json"
        profile = json.loads(example.read_text(encoding="utf-8"))
        assets = self.adapters.validate_asset_profile(profile, "pi-edge-01")
        self.assertEqual(assets[0]["assetName"], "Adeept ADA031 5-DOF Robotic Arm")
        self.assertEqual(assets[0]["protocol"], "ada031_v4_serial")

    def test_ada031_vendor_key_protocol_maps_to_exact_ascii_bytes(self):
        expected = {
            ("base", "increase"): b"o", ("base", "decrease"): b"p",
            ("shoulder", "increase"): b"u", ("shoulder", "decrease"): b"i",
            ("elbow", "increase"): b"t", ("elbow", "decrease"): b"y",
            ("wrist_rotation", "increase"): b"e", ("wrist_rotation", "decrease"): b"r",
            ("gripper", "increase"): b"q", ("gripper", "decrease"): b"w",
        }
        for (joint, direction), byte in expected.items():
            self.assertEqual(self.ada031.resolve_command_byte(joint, direction), byte)
        with self.assertRaisesRegex(ValueError, "unsupported"):
            self.ada031.resolve_command_byte("emergency_stop", "increase")

    def test_ada031_commands_require_live_unretained_allowlisted_asset_and_expiry(self):
        settings = types.SimpleNamespace(ada031_control_asset_ids=("urn:test:arm",))
        assets = [{"assetId": "urn:test:arm", "protocol": "ada031_v4_serial", "endpoint": "serial:///dev/ttyACM0?baudrate=9600"}]
        command = {
            "schemaVersion": 1, "action": "ada031_control", "commandId": "command-0001",
            "targetAssetId": "urn:test:arm", "joint": "base", "direction": "increase",
            "expiresAt": int(__import__("time").time() * 1000) + 5000,
        }
        normalized, asset, byte = self.ada031.validate_command(settings, command, assets)
        self.assertEqual(normalized["targetAssetId"], asset["assetId"])
        self.assertEqual(byte, b"o")
        with self.assertRaisesRegex(ValueError, "must not be retained"):
            self.ada031.validate_command(settings, command, assets, retained=True)
        with self.assertRaisesRegex(ValueError, "not enabled"):
            self.ada031.validate_command(types.SimpleNamespace(ada031_control_asset_ids=()), command, assets)
        expired = {**command, "expiresAt": 1}
        with self.assertRaisesRegex(ValueError, "expired"):
            self.ada031.validate_command(settings, expired, assets)

    def test_ada031_v2_actions_map_to_connected_firmware_bytes(self):
        settings = types.SimpleNamespace(ada031_control_asset_ids=("urn:test:arm",))
        assets = [{"assetId": "urn:test:arm", "protocol": "ada031_v4_serial", "endpoint": "serial:///dev/ttyACM0?baudrate=9600"}]
        expires_at = int(__import__("time").time() * 1000) + 5000
        cases = [
            ({"action": "jog", "joint": "shoulder", "direction": "decrease"}, b"i"),
            ({"action": "set_profile", "profile": "pick_and_place_repeat"}, b"P"),
            ({"action": "set_profile", "profile": "demonstration_moves"}, b"D"),
            ({"action": "neutral"}, b"N"),
            ({"action": "stop_program"}, b"S"),
        ]
        for index, (fields, expected_byte) in enumerate(cases):
            command = {
                "schemaVersion": 2, "commandId": f"command-v2-{index:02d}",
                "targetAssetId": "urn:test:arm", "expiresAt": expires_at, **fields,
            }
            normalized, _asset, command_byte = self.ada031.validate_command(settings, command, assets)
            self.assertEqual(command_byte, expected_byte)
            self.assertEqual(normalized["action"], fields["action"])

        invalid = {
            "schemaVersion": 2, "action": "neutral", "commandId": "command-v2-bad",
            "targetAssetId": "urn:test:arm", "expiresAt": expires_at, "profile": "demonstration_moves",
        }
        with self.assertRaisesRegex(ValueError, "fields"):
            self.ada031.validate_command(settings, invalid, assets)

    def test_ada031_reads_latest_connected_firmware_telemetry_on_command_port(self):
        telemetry_one = {
            "cycle_count": 3, "successful_cycles": 2, "failed_cycles": 1,
            "interrupted_cycles": 4,
            "cycle_time_ms": 1700, "active_profile": 1, "sequence_step": 2,
            "movement_active": 1, "button_pressed": 0, "uptime_ms": 20000,
            "calibration_mode": 0, "pots_matched": 0,
            "servo_1_deg": 90, "servo_2_deg": 100, "servo_3_deg": 80,
            "servo_4_deg": 70, "servo_5_deg": 45,
        }
        telemetry_two = {**telemetry_one, "cycle_count": 4, "sequence_step": 3, "movement_active": 0}

        class FakePort:
            def __init__(self):
                self.lines = [
                    b'{"i2c_address":"0x3C"}\n',
                    (json.dumps(telemetry_one) + "\n").encode(),
                    (json.dumps(telemetry_two) + "\n").encode(),
                ]
                self.writes = []
            @property
            def in_waiting(self):
                return sum(len(line) for line in self.lines)
            def readline(self):
                return self.lines.pop(0) if self.lines else b""
            def write(self, value):
                self.writes.append(value)
                return len(value)
            def flush(self):
                pass
            def close(self):
                pass

        ports = []
        manager = self.ada031.Ada031SerialCommandManager(
            serial_factory=lambda *_args, **_kwargs: ports.append(FakePort()) or ports[-1],
            sleeper=lambda _seconds: None,
        )
        asset = {"assetId": "urn:test:arm", "protocol": "ada031_v4_serial", "endpoint": "serial:///dev/ttyACM0?baudrate=9600"}
        reading = manager.read_telemetry(asset)
        self.assertEqual(reading["assetSignals"]["cycle_count"], 4.0)
        self.assertEqual(reading["assetSignals"]["interrupted_cycles"], 4.0)
        self.assertEqual(reading["assetSignals"]["sequence_step"], 3.0)
        command = {
            "commandId": "command-v2-shared", "targetAssetId": asset["assetId"],
            "action": "neutral", "expiresAt": int(__import__("time").time() * 1000) + 5000,
        }
        manager.execute(asset, command, b"N")
        self.assertEqual(len(ports), 1)
        self.assertEqual(ports[0].writes, [b"N"])
        manager.close()

    def test_ada031_telemetry_rejects_impossible_servo_angle(self):
        with self.assertRaisesRegex(ValueError, "angle"):
            self.ada031.normalize_telemetry({"cycle_count": 1, "servo_1_deg": 999})

    def test_ada031_telemetry_reopens_after_usb_descriptor_fails(self):
        class FailingPort:
            closed = False
            def readline(self):
                raise OSError("USB device disconnected")
            def close(self):
                self.closed = True

        class RecoveredPort:
            closed = False
            in_waiting = 0
            def readline(self):
                return b'{"cycle_count":2,"movement_active":1}\n'
            def close(self):
                self.closed = True

        ports = [FailingPort(), RecoveredPort()]
        opened = []
        manager = self.ada031.Ada031SerialCommandManager(
            serial_factory=lambda *_args, **_kwargs: opened.append(ports[len(opened)]) or opened[-1],
            sleeper=lambda _seconds: None,
        )
        asset = {"assetId": "urn:test:arm", "protocol": "ada031_v4_serial", "endpoint": "serial:///dev/ttyUSB1?baudrate=9600"}
        with self.assertRaisesRegex(OSError, "disconnected"):
            manager.read_telemetry(asset)
        self.assertTrue(opened[0].closed)
        recovered = manager.read_telemetry(asset)
        self.assertEqual(recovered["assetSignals"]["cycle_count"], 2.0)
        self.assertEqual(recovered["assetSignals"]["movement_active"], 1.0)
        self.assertEqual(len(opened), 2)
        manager.close()

    def test_ada031_serial_manager_writes_one_byte_after_one_controller_open(self):
        class FakePort:
            def __init__(self, *_args, **kwargs):
                self.options = kwargs
                self.writes = []
                self.closed = False
            def write(self, value):
                self.writes.append(value)
                return len(value)
            def flush(self):
                pass
            def close(self):
                self.closed = True

        ports = []
        fake_serial = types.ModuleType("serial")
        fake_serial.Serial = lambda *args, **kwargs: ports.append(FakePort(*args, **kwargs)) or ports[-1]
        manager = self.ada031.Ada031SerialCommandManager(serial_factory=fake_serial.Serial, sleeper=lambda _seconds: None)
        asset = {"assetId": "urn:test:arm", "protocol": "ada031_v4_serial", "endpoint": "serial:///dev/ttyACM0?baudrate=9600"}
        command = {"commandId": "command-0001", "targetAssetId": asset["assetId"], "joint": "base", "direction": "increase", "expiresAt": int(__import__("time").time() * 1000) + 5000}
        with patch.dict(sys.modules, {"serial": fake_serial}):
            result = manager.execute(asset, command, b"o")
            duplicate = manager.execute(asset, command, b"o")
        self.assertEqual(result["status"], "serial_write_accepted")
        self.assertEqual(result["feedback"], "controller_telemetry_pending")
        self.assertEqual(duplicate["status"], "duplicate_ignored")
        self.assertEqual(ports[0].options["baudrate"], 9600)
        self.assertEqual(ports[0].writes, [b"o"])
        manager.close()

    def test_routes_opcua_and_modbus_profiles_to_their_pollers(self):
        with patch.object(self.adapters, "read_opcua", return_value={"temperature": 42.0}) as opcua:
            result = self.adapters.read_asset({"protocol": "opcua", "endpoint": "opc.tcp://plc:4840", "tagMappings": []})
        self.assertEqual(result, {"temperature": 42.0})
        opcua.assert_called_once()

        with patch.object(self.adapters, "read_modbus_tcp", return_value={"pressure": 7.1}) as modbus:
            result = self.adapters.read_asset({"protocol": "modbus_tcp", "endpoint": "plc:502", "tagMappings": []})
        self.assertEqual(result, {"pressure": 7.1})
        modbus.assert_called_once()

    def test_applies_register_scale_to_engineering_value(self):
        value = self.adapters._decode_registers([742], {"scale": 0.01})
        self.assertEqual(value, 7.42)


if __name__ == "__main__":
    unittest.main()
