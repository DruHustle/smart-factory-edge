"""Opt-in real network tests: CONNECTIVITY_PROTOCOL_TESTS=1.

Requires the pinned requirements and an isolated MQTT v5 broker on loopback
(CONNECTIVITY_TEST_MQTT_PORT, default18884). Never addresses plant devices.
"""
import copy
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import socket
import struct
import sys
import threading
import time
import unittest
import uuid
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from connectivity_schema import validate_connection
from connectivity_values import LocalCommissioning, utc_now
from fx_tsn import decode_uadp, fx_model_snapshot
from industrial_drivers import MqttDriver, ModbusDriver, OpcUaDriver, UadpDriver
from connectivity_fixtures import connection, encoded_uadp, tag


def free_port(kind=socket.SOCK_STREAM):
    with socket.socket(socket.AF_INET, kind) as port:
        port.bind(("127.0.0.1", 0))
        return port.getsockname()[1]


def until(condition, seconds=5):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("Expected protocol observation did not arrive")


@unittest.skipUnless(os.getenv("CONNECTIVITY_PROTOCOL_TESTS") == "1", "Set CONNECTIVITY_PROTOCOL_TESTS=1 with an isolated MQTT v5 broker")
class ProtocolIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from connectivity_protocol_bench import ProtocolBench
        logging.getLogger("asyncua").setLevel(logging.ERROR)
        cls.environment = patch.dict(os.environ, {"EDGE_ENV": "development", "OPCUA_ALLOW_INSECURE": "true"})
        cls.environment.start()
        cls.bench = ProtocolBench(free_port(), free_port(), free_port(socket.SOCK_DGRAM), int(os.getenv("CONNECTIVITY_TEST_MQTT_PORT", "18884"))).start()
        cls.connections = {item["connectionId"]: item for item in cls.bench.configuration()["connections"]}

    @classmethod
    def tearDownClass(cls):
        cls.bench.close()
        cls.environment.stop()

    def driver(self, cls, config):
        driver = cls(validate_connection(config), LocalCommissioning({}))
        self.addCleanup(driver.close)
        driver.connect()
        return driver

    def test_real_modbus_tcp_typed_registers_coils_quality_and_exception(self):
        config = copy.deepcopy(self.connections["modbus-bench"])
        config["tags"].append(tag("uint16", "invalid-register", {"address": 50000, "registerType": "holding_register", "unitId": 1}))
        result = self.driver(ModbusDriver, config).collect()
        tags = {item["tagId"]: item for item in result["tags"]}
        self.assertEqual(tags["pressure"]["value"], 12.5)
        self.assertEqual(tags["counter"]["value"], "18446744073709551615")
        self.assertIs(tags["running"]["value"], True)
        self.assertEqual(tags["pressure"]["quality"], "unknown")
        self.assertEqual(tags["invalid-register"]["quality"], "bad")
        self.assertEqual(result["state"], "degraded")

    def test_legacy_asset_profile_still_reads_pinned_modbus_and_opcua_clients(self):
        from asset_adapters import read_modbus_tcp, read_opcua
        modbus = read_modbus_tcp(self.connections["modbus-bench"]["endpoint"], [{"metric": "pressure", "address": 0, "count": 2, "unitId": 1, "dataType": "float32", "unit": "bar"}])
        self.assertEqual(modbus["pressure"], 12.5)
        self.assertEqual(modbus["mapping"]["pressure"]["rawUnit"], "register")
        opcua = read_opcua(self.connections["opcua-bench"]["endpoint"], [{"metric": "pressure", "nodeId": f"ns={self.bench.namespace};s=Pressure", "unit": "bar"}])
        self.assertEqual(opcua["pressure"], 12.5)
        self.assertEqual(opcua["mapping"]["pressure"]["rawUnit"], "native")

    def test_real_modbus_boolean_registers_preserve_zero_and_nonzero_values(self):
        config = copy.deepcopy(self.connections["modbus-bench"])
        config["tags"] = [
            tag("boolean", "holding-on", {"address": 0, "registerType": "holding_register", "unitId": 1}),
            tag("boolean", "holding-off", {"address": 1, "registerType": "holding_register", "unitId": 1}),
            tag("boolean", "input-on", {"address": 0, "registerType": "input_register", "unitId": 1}),
        ]
        result = self.driver(ModbusDriver, config).collect()
        values = {item["tagId"]: item for item in result["tags"]}
        self.assertIs(values["holding-on"]["value"], True)
        self.assertIs(values["holding-off"]["value"], False)
        self.assertIs(values["input-on"]["value"], True)
        self.assertTrue(all(item["quality"] == "unknown" for item in values.values()))
        self.assertEqual(result["state"], "connected")

    @unittest.skipUnless(os.name == "posix", "Pseudo-terminal RTU fixture requires POSIX")
    def test_real_modbus_rtu_over_commissioned_pseudo_terminal(self):
        import pty
        import select
        master, slave = pty.openpty()
        path = os.ttyname(slave)
        requests, finished = [], threading.Event()
        def crc(frame):
            value = 0xFFFF
            for byte in frame:
                value ^= byte
                for _ in range(8):
                    value = (value >> 1) ^ 0xA001 if value & 1 else value >> 1
            return struct.pack("<H", value)
        def responder():
            pending = b""
            while not finished.is_set():
                try:
                    if not select.select([master], [], [], 0.1)[0]:
                        continue
                    pending += os.read(master, 256)
                    if len(pending) >= 8:
                        request, pending = pending[:8], pending[8:]
                        requests.append(request)
                        if request[:6] == b"\x01\x03\x00\x00\x00\x02" and crc(request[:6]) == request[6:]:
                            response = b"\x01\x03\x04" + struct.pack(">f", 12.5)
                            os.write(master, response + crc(response))
                except OSError:
                    return
        worker = threading.Thread(target=responder, daemon=True)
        worker.start()
        driver = ModbusDriver(connection("modbus_rtu", endpoint="serial://" + path + "?baudrate=19200"), LocalCommissioning({"CONNECTIVITY_ALLOWED_SERIAL_PORTS": path}))
        try:
            driver.connect()
            observed = driver.collect()["tags"][0]
            self.assertEqual(observed["value"], 12.5)
            self.assertEqual(observed["quality"], "unknown")
            self.assertEqual(len(requests), 1)
        finally:
            finished.set()
            driver.close()
            worker.join(1)
            os.close(master)
            os.close(slave)

    def test_real_opcua_monitored_change_preserves_status_and_timestamp(self):
        from asyncua import ua
        driver = self.driver(OpcUaDriver, self.connections["opcua-bench"])
        initial = {item["tagId"]: item for item in driver.collect()["tags"]}
        self.assertEqual(initial["counter"]["value"], "18446744073709551615")
        self.assertIs(initial["running"]["value"], True)
        self.assertEqual(initial["label"]["value"], "simulation-running")
        stamp = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        self.bench.pressure.write_value(ua.DataValue(ua.Variant(27.25, ua.VariantType.Float), SourceTimestamp=stamp))
        def observed():
            item = driver.collect()["tags"][0]
            return item if item["value"] == 27.25 else None
        changed = until(observed)
        self.assertEqual(changed["sourceTimestamp"], "2026-01-02T03:04:05.000Z")
        self.assertEqual(changed["quality"], "good")
        self.bench.pressure.write_value(ua.DataValue(ua.Variant(27.25, ua.VariantType.Float), StatusCode=ua.StatusCode(ua.StatusCodes.BadSensorFailure), SourceTimestamp=stamp))
        bad = until(lambda: driver.collect()["tags"][0] if driver.collect()["tags"][0]["quality"] == "bad" else None)
        self.assertIsNone(bad["value"])
        self.assertEqual(bad["nativeStatus"], "0x808c0000")
        self.bench.pressure.write_value(ua.Variant(12.5, ua.VariantType.Float))

    def test_real_fx_information_model_requires_namespace_and_type(self):
        driver = self.driver(OpcUaDriver, self.connections["fx-diagnostics"])
        value = driver.collect()["fx"]
        self.assertTrue(value["modelVerified"])
        self.assertEqual(value["determinism"], "unverified")
        self.assertEqual({c["kind"] for c in value["components"]}, {"automation_component", "functional_entity", "connection_manager"})
        self.assertNotIn("fx", self.driver(OpcUaDriver, self.connections["opcua-bench"]).collect())

    def test_ordinary_server_never_becomes_an_fx_model(self):
        from asyncua.sync import Server, Client
        from asyncua import ua
        server = Server()
        self.addCleanup(server.stop)
        url = f"opc.tcp://127.0.0.1:{free_port()}/ordinary/"
        server.set_endpoint(url)
        server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        server.start()
        client = Client(url)
        client.connect()
        try:
            result = fx_model_snapshot(client)
            self.assertFalse(result["modelVerified"])
            self.assertEqual(result["warnings"], ["fx_namespaces_not_present"])
        finally:
            client.disconnect()

    def test_real_mqtt_json_retained_downgrade_bad_payload_and_staleness(self):
        topic = "bench/tests/" + uuid.uuid4().hex
        config = connection("mqtt", endpoint=f"mqtt://127.0.0.1:{self.bench.mqtt_port}")
        config["tags"][0]["source"]["topic"] = topic
        payload = json.dumps({"value": 19.5, "quality": "good", "time": utc_now()})
        self.bench.mqtt.publish(topic, payload, qos=1, retain=True).wait_for_publish(3)
        driver = self.driver(MqttDriver, config)
        retained = until(lambda: driver.collect()["tags"][0] if driver.collect()["tags"][0]["value"] == 19.5 else None)
        self.assertEqual(retained["quality"], "uncertain")
        self.assertEqual(retained["nativeStatus"], "mqtt_retained")
        self.bench.mqtt.publish(topic, json.dumps({"value": 20.5, "quality": "good", "time": utc_now()}), qos=1).wait_for_publish(3)
        live = until(lambda: driver.collect()["tags"][0] if driver.collect()["tags"][0]["value"] == 20.5 else None)
        self.assertEqual(live["quality"], "good")
        self.bench.mqtt.publish(topic, json.dumps({"value": None, "quality": "good", "time": utc_now()}), qos=1).wait_for_publish(3)
        missing = until(lambda: driver.collect()["tags"][0] if driver.collect()["tags"][0].get("nativeStatus") == "source_value_missing" else None)
        self.assertIsNone(missing["value"])
        self.assertEqual(missing["quality"], "bad")
        self.bench.mqtt.publish(topic, '{"value":1,"value":2}', qos=1).wait_for_publish(3)
        invalid = until(lambda: driver.collect()["tags"][0] if driver.collect()["tags"][0].get("nativeStatus") == "mqtt_payload_invalid" else None)
        self.assertEqual(invalid["quality"], "bad")
        with driver.lock:
            driver.timestamps["pressure"] = time.monotonic() - 31
        self.assertEqual(driver.collect()["tags"][0]["nativeStatus"], "mqtt_source_stale")
        self.bench.mqtt.publish(topic, b"", qos=1, retain=True).wait_for_publish(3)

    def test_independent_uadp_encoder_raw_variant_datavalue_and_pinned_metadata(self):
        from asyncua import ua
        for encoding in ("variant", "raw"):
            with self.subTest(encoding=encoding):
                value = decode_uadp(encoded_uadp(encoding=encoding), connection("opcua_fx"))
                self.assertEqual(value["tags"][0]["value"], 12.5)
                self.assertEqual(value["tags"][0]["quality"], "unknown")
        stamp = datetime(2026, 1, 2, tzinfo=timezone.utc)
        data = ua.DataValue(ua.Variant(4.0, ua.VariantType.Float), StatusCode=ua.StatusCode(ua.StatusCodes.UncertainDataSubNormal), SourceTimestamp=stamp, ServerTimestamp=stamp)
        value = decode_uadp(encoded_uadp(values=[data], encoding="datavalue"), connection("opcua_fx"))
        self.assertEqual(value["tags"][0]["quality"], "uncertain")
        self.assertEqual(value["tags"][0]["sourceTimestamp"], "2026-01-02T00:00:00.000Z")
        # A more severe DataSet header must dominate a field's status.
        bad_dataset = decode_uadp(encoded_uadp(values=[data], encoding="datavalue", Status=0x8000), connection("opcua_fx"))
        self.assertEqual(bad_dataset["tags"][0]["quality"], "bad")
        self.assertIsNone(bad_dataset["tags"][0]["value"])
        with self.assertRaisesRegex(ValueError, "keyframe_required"):
            decode_uadp(encoded_uadp(Valid=False), connection("opcua_fx"))
        bad_metadata = connection("opcua_fx")
        bad_metadata["fx"]["metadataMajorVersion"] = 2
        with self.assertRaisesRegex(ValueError, "metadata_version_mismatch"):
            decode_uadp(encoded_uadp(), bad_metadata)

    def test_actual_udp_receive_sequence_duplicate_rejection_and_staleness(self):
        port = free_port(socket.SOCK_DGRAM)
        driver = self.driver(UadpDriver, connection("opcua_fx", endpoint=f"opc.udp://127.0.0.1:{port}"))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            data = encoded_uadp(sequence=65535)
            sender.sendto(data, ("127.0.0.1", port))
            first = until(lambda: driver.collect() if driver.collect()["fx"]["receivedMessages"] == 1 else None)
            self.assertEqual(first["tags"][0]["value"], 12.5)
            sender.sendto(data, ("127.0.0.1", port))
            until(lambda: driver.collect()["fx"]["droppedMessages"] == 1)
            sender.sendto(encoded_uadp(sequence=0), ("127.0.0.1", port))
            until(lambda: driver.collect()["fx"]["receivedMessages"] == 2)
        with driver.lock:
            driver.last_received = time.monotonic() - 6
        stale = driver.collect()
        self.assertEqual(stale["state"], "degraded")
        self.assertEqual(stale["tags"][0]["quality"], "bad")
        self.assertEqual(stale["fx"]["determinism"], "unverified")


if __name__ == "__main__":
    unittest.main()
