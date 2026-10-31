import copy
import json
import os
from pathlib import Path
import stat
import struct
import sys
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from connectivity_schema import json_pointer, load_configuration, save_configuration, validate_configuration, validate_connection
from connectivity_values import LocalCommissioning, normalize_value, sample
from fx_tsn import collect_tsn, decode_uadp
from industrial_connectivity import ConnectivityRuntime, _comparison, observation_batches
from industrial_drivers import ModbusDriver
from connectivity_fixtures import connection, configuration, tag


class ContractTests(unittest.TestCase):
    def test_shared_dotnet_node_python_contract_fixture(self):
        fixture = json.loads((Path(__file__).resolve().parents[2] / "fixtures/connectivity/industrial-v1.json").read_text())
        profile = fixture["configuration"]
        self.assertEqual(validate_configuration(profile, profile["gatewayDeviceId"]), profile)
        self.assertEqual(len(profile["connections"]), 6)

    def test_protocol_sources_and_independent_types(self):
        profile = configuration(*(connection(p, p) for p in ("modbus_tcp", "modbus_rtu", "opcua", "mqtt", "opcua_fx")))
        self.assertEqual(validate_configuration(profile, "pi-edge-01"), profile)
        validated = validate_configuration(profile, "pi-edge-01")
        validated["connections"][0]["tags"].clear()
        self.assertEqual(len(profile["connections"][0]["tags"]), 1)

    def test_unknown_fields_secrets_methods_and_protocol_mismatch_rejected(self):
        for field in ("password", "username", "methodId", "write", "securityMode", "tsnSocket"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_connection(connection(**{field: "forbidden"}))
        for source in ({"nodeId": "i=42"}, {"address": 0, "registerType": "coil", "unitId": 1}):
            bad = connection()
            bad["tags"][0]["source"] = source
            with self.assertRaises(ValueError):
                validate_connection(bad)
        with self.assertRaises(ValueError):
            validate_connection(connection(fx=None))

    def test_bounds_duplicate_tags_reserved_ack_and_identity(self):
        invalid = [configuration(connection(), connection()), configuration(revision=True), configuration(revision=0), configuration(connection(connection_id="gateway-configuration"))]
        duplicate = connection()
        duplicate["tags"] *= 2
        invalid.append(configuration(duplicate))
        for profile in invalid:
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                validate_configuration(profile, "pi-edge-01")
        with self.assertRaises(ValueError):
            validate_configuration(configuration(), "another-gateway")

    def test_endpoint_secrets_queries_path_and_schemes_rejected(self):
        for endpoint in ("tcp://user:pass@127.0.0.1:502", "tcp://127.0.0.1:502/path", "tcp://127.0.0.1:502?secret=x", "http://127.0.0.1", "tcp://127.0.0.1:0"):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                validate_connection(connection(endpoint=endpoint))
        with self.assertRaises(ValueError):
            validate_connection(connection("modbus_rtu", endpoint="serial:///dev/../etc/passwd"))

    def test_stable_node_ids_and_serial_ownership(self):
        good = connection("opcua")
        validate_connection(good)
        good["tags"][0]["source"]["nodeId"] = "ns=2;s=Pressure"
        with self.assertRaises(ValueError):
            validate_connection(good)
        with self.assertRaises(ValueError):
            validate_configuration(configuration(connection("modbus_rtu", "a"), connection("modbus_rtu", "b")), "pi-edge-01")

    def test_udp_requires_explicit_identity_and_complete_metadata(self):
        original = connection("opcua_fx")
        for key in ("publisherId", "publisherIdType", "writerGroupId", "dataSetWriterId", "metadataMajorVersion", "metadataMinorVersion", "allowUnsecured"):
            bad = copy.deepcopy(original)
            del bad["fx"][key]
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_connection(bad)
        original["tags"][0]["source"]["fieldIndex"] = 1
        with self.assertRaises(ValueError):
            validate_connection(original)

    def test_scaling_only_supported_numeric_encodings(self):
        for kind in ("boolean", "string", "int64", "uint64"):
            value = connection("mqtt", tags=[tag(kind, source={"topic": "test", "path": ""})])
            value["tags"][0]["scale"] = 2
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                validate_connection(value)
        value = tag("int16")
        value.update(scale=0.1, offset=0.25)
        self.assertEqual(normalize_value(value, 12), 1.4500000000000002)

    def test_exact_int64_and_invalid_numeric_quality(self):
        self.assertEqual(normalize_value(tag("uint64"), 18446744073709551615), "18446744073709551615")
        self.assertEqual(normalize_value(tag("int64"), "-9223372036854775808"), "-9223372036854775808")
        for value in (18446744073709551616, -1, 1.0, True):
            self.assertEqual(sample(tag("uint64"), value)["quality"], "bad")
        for value in (float("nan"), float("inf"), True):
            self.assertEqual(sample(tag(), value)["value"], None)
        self.assertEqual(sample(tag("boolean"), True)["value"], True)
        self.assertEqual(sample(tag("string"), "running")["value"], "running")

    def test_atomic_persistence_and_duplicate_json_detection(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "config.json"
            digest = save_configuration(target, configuration(connection()), "pi-edge-01")
            self.assertEqual(len(digest), 64)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            self.assertEqual(load_configuration(target, "pi-edge-01"), configuration(connection()))
            original = target.read_bytes()
            with self.assertRaises(ValueError):
                save_configuration(target, configuration(revision=-1), "pi-edge-01")
            self.assertEqual(target.read_bytes(), original)
            target.write_text(original.decode().replace('"revision":1', '"revision":1,"revision":2'))
            with self.assertRaisesRegex(ValueError, "duplicate_json_field"):
                load_configuration(target, "pi-edge-01")

    def test_unicode_configuration_preserves_compact_utf8_size_on_save_reload(self):
        tags = [{"tagId": f"t{i}", "name": "雪" * 120, "dataType": "float32", "source": {"topic": "bench/" + "x" * 160, "path": "/value"}} for i in range(100)]
        profile = configuration(connection("mqtt", tags=tags))
        payload = json.dumps(profile, ensure_ascii=False, separators=(",", ":")).encode()
        self.assertGreater(len(payload), 60000)
        self.assertLessEqual(len(payload), 64000)
        self.assertGreater(len(json.dumps(profile).encode()), 65536)
        validate_configuration(profile, "pi-edge-01")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile.json"
            save_configuration(path, profile, "pi-edge-01")
            self.assertEqual(path.stat().st_size, len(payload))
            self.assertEqual(load_configuration(path, "pi-edge-01"), profile)

    def test_json_pointer_escape_and_literal_topics(self):
        self.assertEqual(json_pointer({"a/b": {"~": [42]}}, "/a~1b/~0/0"), 42)
        with self.assertRaises(ValueError):
            json_pointer([42], "/01")
        bad = connection("mqtt")
        bad["tags"][0]["source"]["topic"] = "factory/#"
        with self.assertRaises(ValueError):
            validate_connection(bad)


class CommissioningTests(unittest.TestCase):
    def test_production_destinations_fail_closed_without_local_allowlist(self):
        local = LocalCommissioning({"EDGE_ENV": "production"})
        with self.assertRaisesRegex(ValueError, "not_commissioned"):
            local.endpoint(connection(endpoint="tcp://127.0.0.1:502"))
        local = LocalCommissioning({"EDGE_ENV": "production", "CONNECTIVITY_ALLOWED_NETWORKS": "127.0.0.0/8,169.254.0.0/16"})
        self.assertEqual(local.endpoint(connection()), "127.0.0.1")
        with self.assertRaises(ValueError):
            local.endpoint(connection(endpoint="tcp://169.254.169.254:80"))
        with self.assertRaises(ValueError):
            local.endpoint(connection("modbus_rtu"))

    def test_insecure_remote_mqtt_rejected_in_production(self):
        local = LocalCommissioning({"EDGE_ENV": "production", "CONNECTIVITY_ALLOWED_NETWORKS": "192.0.2.0/24"})
        with self.assertRaisesRegex(ValueError, "mqtt_tls_required"):
            local.endpoint(connection("mqtt", endpoint="mqtt://192.0.2.10:1883"))

    def test_secret_and_collector_refs_resolved_only_locally(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "secrets.json"
            path.write_text(json.dumps({"credentials": {"plc": {"password": "local-only"}}, "collectors": {"ptp": {"interface": "eth0", "pmcSocket": "/run/ptp4l"}}}))
            env = {"EDGE_ENV": "production", "CONNECTIVITY_SECRETS_FILE": str(path), "CONNECTIVITY_ALLOWED_INTERFACES": "eth0"}
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "permissions"):
                LocalCommissioning(env)
            path.chmod(0o600)
            local = LocalCommissioning(env)
            self.assertEqual(local.credentials({"credentialRef": "plc"})["password"], "local-only")
            self.assertEqual(local.collector({"interface": "eth0", "collectorRef": "ptp"})["pmcSocket"], "/run/ptp4l")
            with self.assertRaises(ValueError):
                local.collector({"interface": "eth1", "collectorRef": "ptp"})


class RuntimeTests(unittest.TestCase):
    def test_revision_idempotence_conflict_restart_and_empty_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            reports = []
            target = Path(directory) / "configuration.json"
            runtime = ConnectivityRuntime(target, "pi-edge-01", reports.append, threading.Event())
            config = configuration(revision=5)
            self.assertEqual(runtime.apply(config), runtime.apply(config))
            with self.assertRaisesRegex(ValueError, "conflict"):
                runtime.apply(configuration(connection(), revision=5))
            with self.assertRaisesRegex(ValueError, "stale"):
                runtime.apply(configuration(revision=4))
            restored = ConnectivityRuntime(target, "pi-edge-01", reports.append, threading.Event())
            restored._receipt()
            receipt = reports[0]["connectivity"]
            self.assertEqual(receipt["connectionId"], "gateway-configuration")
            self.assertEqual(receipt["configurationRevision"], 5)
            self.assertEqual(receipt["state"], "disabled")
            self.assertEqual(receipt["tags"], [])
            self.assertFalse(set(receipt) & {"fx", "tsn", "commandId", "errorCode", "discoveredTags"})

    def test_chunking_preserves_all_long_string_tags_and_unique_event_ids(self):
        snapshot = {"state": "connected", "tags": [sample(tag("string", "t" + str(i)), "雪" * 1024) for i in range(100)]}
        reports = list(observation_batches("bench", 7, snapshot))
        self.assertEqual(sum(len(report["tags"]) for report in reports), 100)
        self.assertEqual(len({report["eventId"] for report in reports}), len(reports))
        for report in reports:
            self.assertLessEqual(len(report["tags"]), 20)
            self.assertLessEqual(len(json.dumps(report, ensure_ascii=False, separators=(",", ":")).encode()), 14500)

    def test_cached_probe_keeps_envelope_and_outer_acquisition_time_equal(self):
        from datetime import datetime, timezone
        with tempfile.TemporaryDirectory() as directory:
            reports = []
            runtime = ConnectivityRuntime(Path(directory) / "config.json", "pi-edge-01", reports.append, threading.Event())
            when = datetime.fromtimestamp(time.time() - 4, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            runtime._emit(connection(), 1, {"state": "connected", "tags": [], "_observedAt": when}, str(uuid.uuid4()))
            envelope = reports[0]["connectivity"]
            self.assertEqual(envelope["observedAt"], when)
            self.assertEqual(reports[0]["timestamp"], int(datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp() * 1000))
            self.assertNotEqual(reports[0]["integration"]["gatewayReceivedAt"], when)

    def test_tsn_evidence_is_not_newer_than_its_completed_observation(self):
        stop, reports = threading.Event(), []
        class FakeDriver:
            def connect(self): pass
            def collect(self): return {"state": "connected", "tags": []}
            def close(self): pass
        def publish(payload): reports.append(payload); stop.set()
        with tempfile.TemporaryDirectory() as directory:
            runtime = ConnectivityRuntime(Path(directory) / "config.json", "pi-edge-01", publish, stop, driver_factory=lambda *_: FakeDriver())
            def delayed_collection(_config, _commissioning):
                time.sleep(0.02)
                return collect_tsn({"interface": "lo"}, LocalCommissioning({}), runner=lambda _: None, sysfs_root="/nonexistent")
            with patch("industrial_connectivity.collect_tsn", delayed_collection):
                runtime._worker(connection(tsn={"interface": "lo"}), 1, stop)
            observation = reports[0]["connectivity"]
            self.assertLessEqual(observation["tsn"]["observedAt"], observation["observedAt"])

    def test_observation_changes_ignore_only_volatile_timestamps_and_counters(self):
        initial = {"state": "connected", "tags": [sample(tag(), 1, server_timestamp="2026-01-01T00:00:00Z")], "tsn": {"observedAt": "a", "counters": {"rxDropped": 1}, "clock": {"state": "unknown"}}}
        changed = copy.deepcopy(initial)
        changed["tags"][0]["serverTimestamp"] = "2026-01-02T00:00:00Z"
        changed["tsn"]["counters"]["rxDropped"] = 2
        self.assertEqual(_comparison(initial), _comparison(changed))
        changed["tags"][0]["value"] = 2
        self.assertNotEqual(_comparison(initial), _comparison(changed))
        self.assertGreater(ConnectivityRuntime.connection_wait(connection(), 6), 29)

    def test_legacy_resource_conflicts_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = ConnectivityRuntime(Path(directory) / "profile.json", "pi-edge-01", lambda _: None, threading.Event(), legacy_serial_paths=lambda: {"/dev/ttyUSB9"}, legacy_connections=lambda: {"tcp://127.0.0.1:15020"})
            for c in (connection(), connection("modbus_rtu")):
                with self.assertRaisesRegex(ValueError, "owned_by_asset_profile"):
                    runtime.apply(configuration(c))

    def test_serial_lease_survives_configuration_replacement_until_close(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = ConnectivityRuntime(Path(directory) / "profile.json", "pi-edge-01", lambda _: None, threading.Event())
            first, second = object(), object()
            serial = connection("modbus_rtu")
            path = runtime._acquire_serial(serial, first)
            runtime.apply(configuration(revision=2))
            self.assertIn("/dev/ttyUSB9", runtime.serial_paths())
            with self.assertRaisesRegex(ValueError, "serial_port_busy"):
                runtime._acquire_serial(serial, second)
            runtime._release_serial(path, second)
            self.assertIn("/dev/ttyUSB9", runtime.serial_paths())
            runtime._release_serial(path, first)
            self.assertEqual(runtime._acquire_serial(serial, second), path)

    def test_spooled_event_identity_and_unicode_survive_process_restart(self):
        from durable_state import DurableState, DurablePublisher
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "delivery.sqlite3"
            state = DurableState(path)
            runtime = ConnectivityRuntime(Path(directory) / "config.json", "pi-edge-01", lambda payload: DurablePublisher(None, state).publish("factory/site/line/pi-edge-01/telemetry", json.dumps(payload, ensure_ascii=False, separators=(",", ":"))), threading.Event())
            runtime._emit(connection(), 1, {"state": "connected", "tags": [sample(tag("string", "label"), "雪" * 1024)]})
            before = state.first()
            event_id = json.loads(before[2])["connectivity"]["eventId"]
            state.close()
            restarted = DurableState(path)
            try:
                self.assertEqual(restarted.first(), before)
                self.assertEqual(json.loads(restarted.first()[2])["connectivity"]["eventId"], event_id)
                self.assertLess(len(restarted.first()[2].encode()), 16384)
            finally:
                restarted.close()

    def test_probe_idempotency_expiry_unknown_fields_and_queue_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = ConnectivityRuntime(Path(directory) / "profile.json", "pi-edge-01", lambda _: None, threading.Event())
            cmd = {"action": "probe_connectivity", "gatewayDeviceId": "pi-edge-01", "commandId": str(uuid.uuid4()), "expiresAt": int(time.time() * 1000) + 45000, "connection": connection()}
            self.assertEqual(runtime.queue_probe(cmd), runtime.queue_probe(cmd))
            self.assertEqual(runtime.pending_probes.qsize(), 1)
            with self.assertRaises(ValueError):
                runtime.queue_probe({**cmd, "expiresAt": 0})
            with self.assertRaises(ValueError):
                runtime.queue_probe({**cmd, "write": True})
            for _ in range(3):
                runtime.queue_probe({**cmd, "commandId": str(uuid.uuid4())})
            with self.assertRaisesRegex(ValueError, "queue_full"):
                runtime.queue_probe({**cmd, "commandId": str(uuid.uuid4())})

    def test_a_failed_driver_does_not_stop_an_independent_connection(self):
        reports, stop = [], threading.Event()
        class FakeDriver:
            def __init__(self, config): self.config = config
            def connect(self):
                if self.config["connectionId"] == "failed": raise ConnectionError("connection_failed")
            def collect(self): return {"state": "connected", "tags": [sample(self.config["tags"][0], 42)]}
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory:
            runtime = ConnectivityRuntime(Path(directory) / "profile.json", "pi-edge-01", reports.append, stop, driver_factory=lambda c, *_: FakeDriver(c))
            runtime.apply(configuration(connection(connection_id="failed"), connection(connection_id="healthy")))
            worker = threading.Thread(target=runtime.run)
            worker.start()
            try:
                deadline = time.monotonic() + 3
                while len(reports) < 3 and time.monotonic() < deadline:
                    time.sleep(0.01)
                states = {r["connectivity"]["connectionId"]: r["connectivity"]["state"] for r in reports}
                self.assertEqual(states["failed"], "disconnected")
                self.assertEqual(states["healthy"], "connected")
                self.assertEqual(reports[-1]["integration"]["mode"], "simulated")
            finally:
                stop.set(); worker.join(4)
            self.assertFalse(worker.is_alive())

    def test_delivery_failure_is_not_accepted_and_retry_is_rate_bounded(self):
        stop = threading.Event()
        attempts = []
        class FakeDriver:
            def connect(self): pass
            def collect(self): return {"state": "connected", "tags": []}
            def close(self): pass
        def reject(payload):
            attempts.append(payload)
            raise RuntimeError("spool_full")
        with tempfile.TemporaryDirectory() as directory:
            runtime = ConnectivityRuntime(Path(directory) / "profile.json", "pi-edge-01", reject, stop, driver_factory=lambda *_: FakeDriver())
            worker = threading.Thread(target=runtime._worker, args=(connection(), 1, stop))
            worker.start(); stop.wait(0.35); stop.set(); worker.join(2)
            self.assertEqual(len(attempts), 1)

    def test_udp_connection_failure_keeps_unauthenticated_quality_basis(self):
        stop, reports = threading.Event(), []
        class FailedDriver:
            def connect(self): raise OSError("uadp_bind_failed")
            def close(self): pass
        def publish(value):
            reports.append(value)
            stop.set()
        with tempfile.TemporaryDirectory() as directory:
            runtime = ConnectivityRuntime(Path(directory) / "profile.json", "pi-edge-01", publish, stop, driver_factory=lambda *_: FailedDriver())
            runtime._worker(connection("opcua_fx"), 1, stop)
            observed = reports[0]["connectivity"]["tags"][0]
            self.assertEqual(observed["qualityBasis"], "unauthenticated_udp")
            self.assertEqual(observed["quality"], "bad")
            self.assertIsNone(observed["value"])


class EvidenceAndEncodingTests(unittest.TestCase):
    def test_modbus_word_byte_order_and_uint64_precision(self):
        t = tag()
        self.assertEqual(ModbusDriver.decode([0x4148, 0], t), 12.5)
        t["source"]["wordOrder"] = "little"
        self.assertEqual(ModbusDriver.decode([0, 0x4148], t), 12.5)
        t["source"]["byteOrder"] = "little"
        self.assertEqual(ModbusDriver.decode([0, 0x4841], t), 12.5)
        self.assertEqual(normalize_value(tag("uint64"), ModbusDriver.decode([65535] * 4, tag("uint64"))), "18446744073709551615")

    def test_missing_tsn_tools_and_socket_never_imply_sync_or_capability(self):
        value = collect_tsn({"interface": "lo"}, LocalCommissioning({}), runner=lambda _: None, sysfs_root="/nonexistent")
        self.assertEqual(value["clock"]["state"], "unknown")
        self.assertEqual(value["capabilities"]["hardwareTimestamping"], "unknown")
        self.assertEqual(value["determinism"], "unverified")
        self.assertTrue(all(item["status"] == "unavailable" for item in value["evidence"]))

    def test_commissioned_tsn_get_only_evidence(self):
        calls = []
        class Commissioned:
            def collector(self, _): return {"pmcSocket": "/run/ptp4l", "interface": "eth0"}
        def runner(args):
            calls.append(args)
            return {"ethtool": "Capabilities:\n hardware-transmit\n hardware-receive\n hardware-raw-clock\nPTP Hardware Clock: 0\n", "tc": '[{"kind":"taprio","handle":"100:","root":true}]', "pmc": "master_offset -12\ngmPresent true\ngmIdentity 001122.fffe.334455\nportState SLAVE\nmeanPathDelay 104.5\n"}[args[0]]
        result = collect_tsn({"interface": "eth0", "ptpDomain": 24}, Commissioned(), runner=runner, sysfs_root="/nonexistent")
        self.assertEqual(result["clock"]["state"], "synchronized")
        self.assertEqual(result["clock"]["offsetNs"], -12)
        self.assertEqual(result["capabilities"]["hardwareTimestamping"], "supported")
        self.assertEqual(result["qdiscs"], [{"kind": "taprio", "handle": "100:"}])
        self.assertEqual(result["determinism"], "unverified")
        self.assertEqual(calls[-1][-3:], ["GET TIME_STATUS_NP", "GET PORT_DATA_SET", "GET CURRENT_DATA_SET"])
        self.assertFalse(any("SET " in arg for call in calls for arg in call))

    def test_uadp_explicit_wire_fixture_and_malformed_boundaries(self):
        # OPC 10000-14: UInt64 publisher, group/payload headers, keyframe with
        # sequence + pinned major/minor versions, one Float variant.
        data = bytes.fromhex("f103ffffffffffffffff0964000100014df4690100010000000000000001000a00004841")
        result = decode_uadp(data, connection("opcua_fx"))
        self.assertEqual(result["tags"][0]["value"], 12.5)
        self.assertEqual(result["tags"][0]["quality"], "unknown")
        self.assertEqual(result["tags"][0]["qualityBasis"], "unauthenticated_udp")
        self.assertTrue(result["metadataVerified"])
        for size in range(len(data)):
            with self.subTest(size=size), self.assertRaises(ValueError):
                decode_uadp(data[:size], connection("opcua_fx"))
        for offset, mask in ((0, 0x40), (1, 0x10), (1, 0x80), (18, 1)):
            mutated = bytearray(data); mutated[offset] ^= mask
            with self.subTest(offset=offset, mask=mask), self.assertRaises(ValueError):
                decode_uadp(bytes(mutated), connection("opcua_fx"))
        with self.assertRaises(ValueError):
            decode_uadp(data + b"\x00", connection("opcua_fx"))


if __name__ == "__main__":
    unittest.main()
