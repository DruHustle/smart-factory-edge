"""Gateway runtime for commissioned, independently scheduled protocol drivers."""
from __future__ import annotations

import copy
from datetime import datetime
import hashlib
import json
import os
import queue
import random
import re
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

from connectivity_schema import OBSERVATION_VERSION, endpoint_resource, load_configuration, save_configuration, validate_configuration, validate_connection
from connectivity_values import LocalCommissioning, sample, utc_now
from fx_tsn import collect_tsn
from industrial_drivers import create_driver


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _error_code(error):
    value = str(error)
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:-]{0,127}", value):
        return value
    return "driver_" + type(error).__name__[:64]


def _comparison(snapshot):
    value = copy.deepcopy(snapshot)
    value.pop("_observedAt", None)
    for tag in value.get("tags", []):
        tag.pop("sourceTimestamp", None)
        tag.pop("serverTimestamp", None)
    if "fx" in value:
        for key in ("receivedMessages", "droppedMessages", "lastSequenceNumber"):
            value["fx"].pop(key, None)
    if "tsn" in value:
        value["tsn"].pop("observedAt", None)
        value["tsn"].pop("counters", None)
    return _digest(value)


def _close(driver):
    if driver is not None:
        try:
            driver.close()
        except Exception:
            pass


def observation_batches(connection_id, revision, snapshot, command_id=None):
    """Bound every message including long string values and discovery metadata."""
    observed_at = snapshot.get("_observedAt", utc_now())
    base = {"schemaVersion": OBSERVATION_VERSION, "connectionId": connection_id, "configurationRevision": revision, "observedAt": observed_at, "state": snapshot["state"]}
    if command_id:
        base["commandId"] = command_id
    extras = {k: copy.deepcopy(snapshot[k]) for k in ("errorCode", "discoveredTags", "fx", "tsn") if k in snapshot}
    # Discovery is a bounded preview. Preserve full tag values by splitting them;
    # truncate only optional browsing previews if their node IDs are very long.
    while len(json.dumps(extras, ensure_ascii=False).encode()) > 7000:
        if extras.get("discoveredTags"):
            extras["discoveredTags"].pop()
        elif extras.get("fx", {}).get("components"):
            extras["fx"]["components"].pop()
            warnings = extras["fx"]["warnings"]
            if "report_discovery_truncated" not in warnings and len(warnings) < 8:
                warnings.append("report_discovery_truncated")
        else:
            raise ValueError("connectivity_diagnostics_size")
    batch = {**base, **extras, "eventId": str(uuid.uuid4()), "tags": []}
    for tag in snapshot.get("tags", []):
        candidate = {**batch, "tags": [*batch["tags"], tag]}
        if len(batch["tags"]) >= 20 or len(json.dumps(candidate, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()) > 14500:
            yield batch
            batch = {**base, "eventId": str(uuid.uuid4()), "tags": []}
        batch["tags"].append(tag)
    yield batch


class ConnectivityRuntime:
    def __init__(self, path, gateway_id, publish, stop, serial_lock=None, legacy_serial_paths=None, command_state=None, commissioning_factory=LocalCommissioning, driver_factory=create_driver, legacy_connections=None):
        self.path, self.gateway_id, self.publish, self.stop = path, gateway_id, publish, stop
        self.serial_lock = serial_lock or threading.RLock()
        self.legacy_serial_paths = legacy_serial_paths or (lambda: set())
        self.legacy_connections = legacy_connections or (lambda: set())
        self.command_state = command_state
        self.commissioning_factory, self.driver_factory = commissioning_factory, driver_factory
        self.lock = threading.RLock()
        self.configured = Path(path).is_file()
        self.configuration = load_configuration(path, gateway_id)
        self.configuration_digest = _digest(self.configuration)
        self.jobs = {}
        self.serial_owners = {}
        self.last_observations = {}
        self.pending_probes = queue.Queue(maxsize=4)
        self.seen_probes = {}
        self.session_id = str(uuid.uuid4())
        self.sequence = 0
        self.receipt_pending = self.configured
        self.report_seconds = max(5, min(300, float(os.getenv("CONNECTIVITY_REPORT_INTERVAL_SECONDS", "10"))))
        self.snapshot_seconds = max(self.report_seconds, min(300, float(os.getenv("CONNECTIVITY_SNAPSHOT_SECONDS", "30"))))

    def apply(self, configuration):
        validated = validate_configuration(configuration, self.gateway_id)
        digest = _digest(validated)
        serial_paths = {os.path.realpath(urlparse(c["endpoint"]).path) for c in validated["connections"] if c["enabled"] and c["protocol"] == "modbus_rtu"}
        if serial_paths & self.legacy_serial_paths():
            raise ValueError("serial_port_owned_by_asset_profile")
        endpoints = {endpoint_resource(c["endpoint"]) for c in validated["connections"] if c["enabled"] and c["protocol"] in {"opcua", "opcua_fx", "modbus_tcp"}}
        if endpoints & {endpoint_resource(endpoint) for endpoint in self.legacy_connections()}:
            raise ValueError("endpoint_owned_by_asset_profile")
        with self.lock:
            if self.configured and validated["revision"] < self.configuration["revision"]:
                raise ValueError("connectivity_revision_stale")
            if self.configured and validated["revision"] == self.configuration["revision"] and digest != self.configuration_digest:
                raise ValueError("connectivity_revision_conflict")
            saved = save_configuration(self.path, validated, self.gateway_id)
            self.configuration, self.configuration_digest, self.configured = validated, digest, True
            self.receipt_pending = True
            return {"configurationHash": saved, "configurationRevision": validated["revision"]}

    def queue_probe(self, command):
        if set(command) != {"action", "gatewayDeviceId", "commandId", "expiresAt", "connection"} or command["action"] != "probe_connectivity" or command["gatewayDeviceId"] != self.gateway_id:
            raise ValueError("invalid_connectivity_probe")
        try:
            if str(uuid.UUID(command["commandId"])) != command["commandId"]:
                raise ValueError()
        except (ValueError, AttributeError, TypeError):
            raise ValueError("invalid_probe_identity") from None
        now = time.time() * 1000
        expires = command["expiresAt"]
        if type(expires) is not int or not now < expires <= now + 60000:
            raise ValueError("probe_expired")
        connection = validate_connection(command["connection"])
        with self.lock:
            self.seen_probes = {key: expiry for key, expiry in self.seen_probes.items() if expiry > now}
            if command["commandId"] in self.seen_probes:
                return {"commandId": command["commandId"], "status": "queued"}
            if len(self.seen_probes) >= 1024 or self.pending_probes.full():
                raise ValueError("probe_queue_full")
            if self.command_state and not self.command_state.reserve_command(command["commandId"], expires):
                return {"commandId": command["commandId"], "status": "queued"}
            self.pending_probes.put_nowait({**command, "connection": connection})
            self.seen_probes[command["commandId"]] = expires
        return {"commandId": command["commandId"], "status": "queued"}

    def _emit(self, connection, revision, snapshot, command_id=None):
        for report in observation_batches(connection["connectionId"], revision, snapshot, command_id):
            qualities = {t["quality"] for t in report["tags"]}
            quality = "bad" if "bad" in qualities else "uncertain" if "uncertain" in qualities else "good" if qualities == {"good"} else "unknown"
            with self.lock:
                sequence = self.sequence
                self.sequence += 1
            integration = {"mode": connection.get("mode", "live"), "sessionId": self.session_id, "sequence": sequence, "sourceQuality": quality, "sourceConnection": "online" if snapshot["state"] in {"connected", "degraded"} else "offline" if snapshot["state"] == "disconnected" else "unknown", "timestampOrigin": "gateway", "retained": False, "gatewayReceivedAt": utc_now()}
            if integration["mode"] == "simulated":
                integration["scenarioRunId"] = connection["scenarioRunId"]
            observed_ms = int(datetime.fromisoformat(report["observedAt"].replace("Z", "+00:00")).timestamp() * 1000)
            payload = {"timestamp": observed_ms, "integration": integration, "connectivity": report}
            if len(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()) > 15700:
                raise ValueError("connectivity_message_size")
            self.publish(payload)

    def _receipt(self):
        with self.lock:
            revision = self.configuration["revision"]
            state = "waiting" if self.configuration["connections"] else "disabled"
        self._emit({"connectionId": "gateway-configuration"}, revision, {"state": state, "tags": []})

    def _sample_tsn(self, connection, snapshot, commissioning, previous, sampled_at):
        if "tsn" not in connection:
            return None, sampled_at
        if previous is not None and time.monotonic() - sampled_at < 10:
            snapshot["tsn"] = previous
            return previous, sampled_at
        try:
            value = collect_tsn(connection["tsn"], commissioning)
        except Exception:
            value = {"interface": connection["tsn"]["interface"], "observedAt": utc_now(), "clock": {"state": "unknown"}, "capabilities": {"hardwareTimestamping": "unknown"}, "qdiscs": [], "counters": {}, "evidence": [{"source": s, "status": "unavailable"} for s in ("pmc", "ethtool", "tc", "sysfs")], "determinism": "unverified"}
        snapshot["tsn"] = value
        return value, time.monotonic()

    def _acquire_serial(self, connection, owner):
        if connection["protocol"] != "modbus_rtu":
            return None
        path = os.path.realpath(urlparse(connection["endpoint"]).path)
        if path in self.legacy_serial_paths():
            raise ValueError("serial_port_owned_by_asset_profile")
        with self.lock:
            if path in self.serial_owners:
                raise ValueError("serial_port_busy")
            self.serial_owners[path] = owner
        return path

    def _release_serial(self, path, owner):
        if path is not None:
            with self.lock:
                if self.serial_owners.get(path) is owner:
                    del self.serial_owners[path]

    def _worker(self, connection, revision, local_stop):
        driver = None
        serial_path = None
        failures = 0
        last_report = -float("inf")
        last_attempt = -float("inf")
        last_comparison = None
        tsn, tsn_time = None, 0
        try:
            while not self.stop.is_set() and not local_stop.is_set():
                cycle = time.monotonic()
                try:
                    if driver is None:
                        if connection["protocol"] in {"opcua", "opcua_fx", "modbus_tcp"} and endpoint_resource(connection["endpoint"]) in {endpoint_resource(endpoint) for endpoint in self.legacy_connections()}:
                            raise ValueError("endpoint_owned_by_asset_profile")
                        serial_path = self._acquire_serial(connection, local_stop)
                        commissioning = self.commissioning_factory()
                        driver = self.driver_factory(connection, commissioning, self.serial_lock)
                        driver.connect()
                    snapshot = driver.collect()
                    tsn, tsn_time = self._sample_tsn(connection, snapshot, commissioning, tsn, tsn_time)
                    snapshot["_observedAt"] = utc_now()
                    failures = 0
                except Exception as error:
                    _close(driver)
                    self._release_serial(serial_path, local_stop)
                    serial_path = None
                    driver = None
                    failures += 1
                    basis = "unauthenticated_udp" if connection.get("fx", {}).get("mode") == "udp_uadp" else "communication"
                    snapshot = {"state": "disconnected", "tags": [sample(t, quality="bad", native_status="connection_unavailable", basis=basis) for t in connection["tags"]], "errorCode": _error_code(error), "_observedAt": utc_now()}
                with self.lock:
                    if self.stop.is_set() or local_stop.is_set() or self.configuration["revision"] != revision:
                        break
                    self.last_observations[connection["connectionId"]] = (revision, time.monotonic(), copy.deepcopy(snapshot))
                comparison = _comparison(snapshot)
                elapsed = time.monotonic() - last_report
                if time.monotonic() - last_attempt >= self.report_seconds and (elapsed >= self.snapshot_seconds or comparison != last_comparison and elapsed >= self.report_seconds):
                    last_attempt = time.monotonic()
                    try:
                        self._emit(connection, revision, snapshot)
                        last_report, last_comparison = time.monotonic(), comparison
                    except Exception as error:
                        # A failed durable enqueue is never treated as accepted.
                        if failures < 2:
                            print("[CONNECTIVITY] delivery unavailable: " + _error_code(error))
                interval = self.connection_wait(connection, failures)
                local_stop.wait(max(0.01, interval - (time.monotonic() - cycle)))
        finally:
            _close(driver)
            self._release_serial(serial_path, local_stop)

    @staticmethod
    def connection_wait(connection, failures):
        if failures:
            return min(30, 2 ** min(failures - 1, 5)) + random.random() * 0.25
        return connection["intervalMs"] / 1000

    def _probe_worker(self):
        while not self.stop.is_set():
            try:
                command = self.pending_probes.get(timeout=0.25)
            except queue.Empty:
                continue
            connection = command["connection"]
            driver = None
            serial_path = None
            with self.lock:
                revision = self.configuration["revision"]
                current = self.last_observations.get(connection["connectionId"])
                active = next((c for c in self.configuration["connections"] if c["connectionId"] == connection["connectionId"]), None)
            try:
                if command["expiresAt"] <= time.time() * 1000:
                    raise ValueError("probe_expired")
                if active == connection and current and len(current) == 3 and time.monotonic() - current[1] <= 5:
                    snapshot = copy.deepcopy(current[2])
                else:
                    if connection["protocol"] == "modbus_rtu":
                        path = os.path.realpath(urlparse(connection["endpoint"]).path)
                        active_paths = {os.path.realpath(urlparse(c["endpoint"]).path) for c in self.configuration["connections"] if c["enabled"] and c["protocol"] == "modbus_rtu"}
                        if path in active_paths or path in self.legacy_serial_paths():
                            raise ValueError("serial_connection_active_use_status")
                    commissioning = self.commissioning_factory()
                    serial_path = self._acquire_serial(connection, command)
                    driver = self.driver_factory(connection, commissioning, self.serial_lock)
                    driver.connect()
                    snapshot = driver.collect()
                    end = min(time.monotonic() + 3, time.monotonic() + max(0, (command["expiresAt"] - time.time() * 1000) / 1000))
                    while snapshot["state"] == "waiting" and time.monotonic() < end and not self.stop.wait(0.1):
                        snapshot = driver.collect()
                    self._sample_tsn(connection, snapshot, commissioning, None, 0)
                    snapshot["_observedAt"] = utc_now()
            except Exception as error:
                snapshot = {"state": "disconnected", "tags": [], "errorCode": _error_code(error)}
            finally:
                _close(driver)
                self._release_serial(serial_path, command)
            try:
                self._emit(connection, revision, snapshot, command["commandId"])
            except Exception as error:
                print("[CONNECTIVITY] probe delivery unavailable: " + _error_code(error))
            self.pending_probes.task_done()

    def run(self):
        probe_thread = threading.Thread(target=self._probe_worker, daemon=True)
        probe_thread.start()
        try:
            while not self.stop.is_set():
                with self.lock:
                    revision = self.configuration["revision"]
                    desired = {c["connectionId"]: copy.deepcopy(c) for c in self.configuration["connections"]}
                    receipt = self.receipt_pending
                if receipt:
                    try:
                        self._receipt()
                        with self.lock:
                            if self.configuration["revision"] == revision:
                                self.receipt_pending = False
                    except Exception:
                        pass
                for key, (fingerprint, stopped, worker) in list(self.jobs.items()):
                    target = desired.get(key)
                    if not worker.is_alive() or not target or fingerprint != _digest([revision, target]) or not target["enabled"]:
                        stopped.set()
                        worker.join(timeout=0.05)
                        if not worker.is_alive():
                            del self.jobs[key]
                            if key not in desired:
                                with self.lock:
                                    self.last_observations.pop(key, None)
                for key, connection in desired.items():
                    if key in self.jobs:
                        continue
                    if not connection["enabled"]:
                        marker = (revision, "disabled")
                        if self.last_observations.get(key) != marker:
                            try:
                                self._emit(connection, revision, {"state": "disabled", "tags": []})
                                self.last_observations[key] = marker
                            except Exception:
                                pass
                        continue
                    stopped = threading.Event()
                    worker = threading.Thread(target=self._worker, args=(connection, revision, stopped), daemon=True)
                    self.jobs[key] = (_digest([revision, connection]), stopped, worker)
                    worker.start()
                self.stop.wait(0.25)
        finally:
            for _, stopped, _ in self.jobs.values():
                stopped.set()
            for _, _, worker in self.jobs.values():
                worker.join(timeout=10)
            probe_thread.join(timeout=10)

    def serial_paths(self):
        with self.lock:
            return set(self.serial_owners) | {os.path.realpath(urlparse(c["endpoint"]).path) for c in self.configuration["connections"] if c["enabled"] and c["protocol"] == "modbus_rtu"}
