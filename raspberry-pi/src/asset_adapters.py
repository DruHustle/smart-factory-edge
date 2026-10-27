"""Read industrial assets using mappings exported by the AAS page."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import struct
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


from measurement_units import SOURCE_UNITS, mapped_value, valid_identity

SUPPORTED_METRICS = {"temperature", "humidity", "vibration", "power", "pressure", "rpm"}
SERIAL_SIGNAL_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
MAX_SERIAL_RECORD_BYTES = 4096
MAX_SERIAL_SIGNALS = 32


def validate_asset_profile(profile: dict, expected_gateway_id: str) -> list[dict]:
    """Validate a complete, versioned desired-state profile before it reaches polling."""
    if not isinstance(profile, dict) or profile.get("schemaVersion") != 1:
        raise ValueError("Unsupported edge configuration schema version")
    if not valid_identity(expected_gateway_id) or profile.get("gatewayDeviceId") != expected_gateway_id:
        raise ValueError("Edge configuration targets a different gateway")
    assets = profile.get("assets")
    if not isinstance(assets, list) or len(assets) > 25:
        raise ValueError("Edge profile must contain at most 25 assets")
    validated = []
    serial_device_paths: set[str] = set()
    for asset in assets:
        if not isinstance(asset, dict):
            raise ValueError("Edge asset entry must be an object")
        asset_id = asset.get("assetId")
        protocol = str(asset.get("protocol", "")).lower()
        endpoint = asset.get("endpoint") or ""
        mappings = asset.get("tagMappings") or []
        if not isinstance(asset_id, str) or not asset_id or len(asset_id) > 128 or any(char in asset_id for char in "\r\n+#"):
            raise ValueError("Edge asset identifier is invalid")
        if protocol not in {"mqtt", "opcua", "modbus_tcp", "modbus_rtu", "serial", "ada031_v4_serial"}:
            raise ValueError("Edge protocol is unsupported")
        if not isinstance(endpoint, str) or len(endpoint) > 512 or ("@" in endpoint.split("://", 1)[-1].split("/", 1)[0]):
            raise ValueError("Machine endpoint is invalid or contains credentials")
        if not isinstance(mappings, list) or len(mappings) > 100 or any(not isinstance(item, dict) for item in mappings):
            raise ValueError("Tag mappings must be an array of at most 100 objects")
        if protocol == "ada031_v4_serial":
            parsed_endpoint = urlparse(endpoint)
            options = parse_qs(parsed_endpoint.query)
            if parsed_endpoint.scheme != "serial" or not parsed_endpoint.path or options.get("baudrate", ["9600"])[0] != "9600":
                raise ValueError("ADA031 V4 requires a USB serial endpoint at 9600 baud")
        if protocol in {"serial", "modbus_rtu", "ada031_v4_serial"}:
            parsed_endpoint = urlparse(endpoint)
            if parsed_endpoint.scheme == "serial" and parsed_endpoint.path:
                device_path = os.path.realpath(parsed_endpoint.path)
                if device_path in serial_device_paths:
                    raise ValueError("Only one asset profile may own a physical serial device")
                serial_device_paths.add(device_path)
        mapped_metrics = [str(m.get("metric")).lower() for m in mappings if m.get("metric")]
        if len(mapped_metrics) != len(set(mapped_metrics)):
            raise ValueError("Map each scalar metric once per asset")
        for mapping in mappings:
            if set(mapping) - {
                "metric", "address", "nodeId", "registerType", "scale", "offset", "unit", "name",
                "source", "count", "deviceId", "unitId", "dataType", "wordOrder",
            }:
                raise ValueError("Tag mapping contains unsupported fields")
            metric = mapping.get("metric")
            if metric is not None and str(metric).lower() not in SUPPORTED_METRICS:
                raise ValueError("Tag mapping contains an unsupported metric")
            if mapping.get("unit") and (metric not in SOURCE_UNITS or mapping["unit"] not in SOURCE_UNITS[metric]):
                raise ValueError("Unsupported engineering unit for metric")
            count = mapping.get("count", 1)
            if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 2:
                raise ValueError("Modbus count must be one or two registers for supported data types")
            offset = mapping.get("offset", 0)
            if isinstance(offset, bool) or not isinstance(offset, (int, float)) or not math.isfinite(offset):
                raise ValueError("Tag mapping offset must be a finite number")
            scale = mapping.get("scale", 1)
            if isinstance(scale, bool) or not isinstance(scale, (int, float)) or not math.isfinite(scale):
                raise ValueError("Tag mapping scale must be a finite number")
            if "address" in mapping and (isinstance(mapping["address"], bool) or not isinstance(mapping["address"], int) or not 0 <= mapping["address"] <= 65535):
                raise ValueError("Modbus address is outside the supported range")
            if "nodeId" in mapping and (not isinstance(mapping["nodeId"], str) or len(mapping["nodeId"]) > 512):
                raise ValueError("OPC UA node id is invalid")
        validated.append({
            "assetId": asset_id,
            "assetName": str(asset.get("assetName") or asset_id)[:255],
            "protocol": protocol,
            "endpoint": endpoint,
            "tagMappings": mappings,
        })
    return validated


def save_asset_profile(path: str, profile: dict, expected_gateway_id: str) -> str:
    """Atomically persist validated configuration so it survives gateway restarts."""
    assets = validate_asset_profile(profile, expected_gateway_id)
    normalized = {"schemaVersion": 1, "gatewayDeviceId": expected_gateway_id, "assets": assets}
    payload = json.dumps(normalized, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(payload) > 64 * 1024:
        raise ValueError("Edge configuration exceeds the 64 KB limit")
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    fd, temporary = tempfile.mkstemp(prefix=".assets-", suffix=".json", dir=str(destination.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return hashlib.sha256(payload).hexdigest()


def load_asset_connections(path: str, expected_gateway_id: str | None = None) -> list[dict]:
    if not path or not Path(path).is_file():
        return []
    with open(path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
    gateway_id = expected_gateway_id or config.get("gatewayDeviceId")
    if not isinstance(gateway_id, str) or not gateway_id:
        raise ValueError("Edge profile has no gateway identity")
    return validate_asset_profile(config, gateway_id)


def _metric_name(mapping: dict) -> str | None:
    metric = str(mapping.get("metric") or "").strip().lower()
    return metric if metric in SUPPORTED_METRICS else None


def _decode_registers(registers: list[int], mapping: dict, raw_value=False) -> float:
    if not registers:
        raise ValueError("Modbus returned no registers")
    if len(registers) == 1:
        value = registers[0]
        if mapping.get("dataType") == "int16" and value >= 0x8000:
            value -= 0x10000
    else:
        words = list(registers)
        if mapping.get("wordOrder", "big").lower() == "little":
            words.reverse()
        raw = 0
        for word in words:
            raw = (raw << 16) | word
        encoding = str(mapping.get("dataType", "uint32")).lower()
        if encoding == "float32":
            value = struct.unpack(">f", struct.pack(">I", raw))[0]
        elif encoding == "int32" and raw >= 0x80000000:
            value = raw - 0x100000000
        else:
            value = raw
    return float(value) if raw_value else float(value) * float(mapping.get("scale", 1)) + float(mapping.get("offset", 0))


def read_modbus_tcp(endpoint: str, mappings: list[dict]) -> dict[str, float]:
    from pymodbus.client import ModbusTcpClient

    parsed = urlparse(endpoint if "://" in endpoint else f"tcp://{endpoint}")
    if not parsed.hostname:
        raise ValueError("Modbus TCP endpoint must be host:port")
    client = ModbusTcpClient(parsed.hostname, port=parsed.port or 502, timeout=3)
    try:
        if not client.connect():
            raise ConnectionError(f"Could not connect to Modbus TCP endpoint {parsed.hostname}:{parsed.port or 502}")
        return _read_modbus_mappings(client, mappings)
    finally:
        client.close()


def read_modbus_rtu(endpoint: str, mappings: list[dict]) -> dict[str, float]:
    from pymodbus.client import ModbusSerialClient

    parsed = urlparse(endpoint)
    if parsed.scheme != "serial" or not parsed.path:
        raise ValueError("Modbus RTU endpoint must look like serial:///dev/ttyUSB0?baudrate=9600")
    options = parse_qs(parsed.query)
    client = ModbusSerialClient(
        port=parsed.path,
        baudrate=int(options.get("baudrate", ["9600"])[0]),
        bytesize=int(options.get("bytesize", ["8"])[0]),
        parity=options.get("parity", ["N"])[0],
        stopbits=int(options.get("stopbits", ["1"])[0]),
        timeout=3,
    )
    try:
        if not client.connect():
            raise ConnectionError(f"Could not open Modbus RTU port {parsed.path}")
        return _read_modbus_mappings(client, mappings)
    finally:
        client.close()


def _read_modbus_mappings(client, mappings: list[dict]) -> dict[str, float]:
    values: dict[str, float] = {}
    for mapping in mappings:
        metric = _metric_name(mapping)
        if metric is None or "address" not in mapping:
            continue
        address = int(mapping["address"])
        count = max(1, min(4, int(mapping.get("count", 1))))
        device_id = int(mapping.get("deviceId", mapping.get("unitId", 1)))
        register_type = str(mapping.get("registerType", "holding_register")).lower()
        if register_type in {"input", "input_register", "input_registers"}:
            response = client.read_input_registers(address, count=count, device_id=device_id)
        else:
            response = client.read_holding_registers(address, count=count, device_id=device_id)
        if response.isError():
            raise IOError(f"Modbus read failed at register {address}")
        mapped_value(values, metric, _decode_registers(response.registers, mapping, raw_value=True), mapping, "register")
    return values


def configure_opcua_security(client) -> None:
    production = os.getenv("EDGE_ENV", "development").strip().lower() == "production"
    if production and (os.getenv("OPCUA_ALLOW_INSECURE", "false").lower() == "true" or
        os.getenv("OPCUA_SECURITY_MODE", "SignAndEncrypt") != "SignAndEncrypt" or not os.getenv("OPCUA_SERVER_CERT")):
        raise ValueError("Production OPC UA requires SignAndEncrypt and a commissioned trusted server certificate")
    policy = os.getenv("OPCUA_SECURITY_POLICY", "").strip()
    if not policy:
        if os.getenv("OPCUA_ALLOW_INSECURE", "false").strip().lower() == "true":
            return
        raise ValueError("OPC UA secure channel is required; configure policy, client certificate, and private key")
    mode = os.getenv("OPCUA_SECURITY_MODE", "SignAndEncrypt").strip()
    if mode not in {"Sign", "SignAndEncrypt"}:
        raise ValueError("OPCUA_SECURITY_MODE must be Sign or SignAndEncrypt")
    certificate = os.getenv("OPCUA_CLIENT_CERT", "").strip()
    private_key = os.getenv("OPCUA_CLIENT_KEY", "").strip()
    if not certificate or not private_key:
        raise ValueError("OPC UA policy requires OPCUA_CLIENT_CERT and OPCUA_CLIENT_KEY")
    security_string = f"{policy},{mode},{certificate},{private_key}"
    server_certificate = os.getenv("OPCUA_SERVER_CERT", "").strip()
    if server_certificate:
        security_string += f",{server_certificate}"
    client.set_security_string(security_string)


def read_opcua(endpoint: str, mappings: list[dict]) -> dict[str, float]:
    from asyncua.sync import Client
    client = Client(endpoint, timeout=5)
    configure_opcua_security(client)
    username = os.getenv("OPCUA_USERNAME")
    password = os.getenv("OPCUA_PASSWORD")
    if username:
        client.set_user(username)
        client.set_password(password or "")
    values: dict[str, float] = {}
    with client:
        for mapping in mappings:
            metric = _metric_name(mapping)
            node_id = mapping.get("nodeId")
            if metric is None or not node_id:
                continue
            raw_value = client.get_node(str(node_id)).read_value()
            mapped_value(values, metric, raw_value, mapping)
    return values


class SerialJsonConnectionManager:
    """Keep USB serial devices open between polls so Arduino boards are not reset repeatedly."""

    def __init__(self):
        self._ports: dict[str, tuple[str, Any]] = {}

    def read_record(self, asset_id: str, endpoint: str) -> dict:
        import serial

        parsed = urlparse(endpoint)
        if parsed.scheme != "serial" or not parsed.path:
            raise ValueError("Serial JSON endpoint must look like serial:///dev/ttyUSB0?baudrate=115200")

        options = parse_qs(parsed.query)
        baudrate = int(options.get("baudrate", ["115200"])[0])
        timeout = float(options.get("timeout", ["1"])[0])
        settle = float(options.get("settle", ["2"])[0])
        if not 300 <= baudrate <= 1_000_000:
            raise ValueError("Serial baud rate is outside the supported range")
        if not math.isfinite(timeout) or not 0.1 <= timeout <= 5:
            raise ValueError("Serial timeout must be between 0.1 and 5 seconds")
        if not math.isfinite(settle) or not 0 <= settle <= 10:
            raise ValueError("Serial settle time must be between 0 and 10 seconds")

        existing = self._ports.get(asset_id)
        if existing and existing[0] != endpoint:
            existing[1].close()
            del self._ports[asset_id]

        if asset_id not in self._ports:
            port = serial.Serial(parsed.path, baudrate, timeout=timeout)
            self._ports[asset_id] = (endpoint, port)
            # Opening an Arduino Uno USB serial port may reset its bootloader.
            if settle:
                time.sleep(settle)

        port = self._ports[asset_id][1]
        # Skip vendor startup banners until a JSON record arrives.
        for _ in range(8):
            raw = port.readline()
            if not raw:
                continue
            if len(raw) > MAX_SERIAL_RECORD_BYTES:
                raise ValueError("Serial JSON record exceeds the supported size")
            try:
                record = json.loads(raw.decode("utf-8", errors="strict").strip())
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(record, dict):
                return record
        raise TimeoutError("No JSON telemetry record received from the serial asset")

    def close_unused(self, active_asset_ids: set[str]) -> None:
        for asset_id in set(self._ports) - active_asset_ids:
            self._ports.pop(asset_id)[1].close()

    def close(self) -> None:
        for _endpoint, port in self._ports.values():
            port.close()
        self._ports.clear()


def _serial_record_values(record: dict, mappings: list[dict]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for mapping in mappings:
        metric = _metric_name(mapping)
        source = mapping.get("source") or mapping.get("name") or metric
        if metric and source in record and record[source] is not None:
            raw_value = record[source]
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                raise ValueError(f"Serial telemetry field {source} must be numeric")
            value = float(raw_value) * float(mapping.get("scale", 1)) + float(mapping.get("offset", 0))
            if not math.isfinite(value):
                raise ValueError("Serial telemetry values must be finite numbers")
            mapped_value(values, metric, raw_value, mapping)

    signals = record.get("signals")
    if signals is not None:
        if not isinstance(signals, dict) or len(signals) > MAX_SERIAL_SIGNALS:
            raise ValueError("Serial asset signals must be an object with at most 32 values")
        normalized_signals = {}
        for name, raw_value in signals.items():
            if not isinstance(name, str) or not SERIAL_SIGNAL_NAME.fullmatch(name):
                raise ValueError("Serial asset signal name is invalid")
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)) or not math.isfinite(raw_value):
                raise ValueError(f"Serial asset signal {name} must be a finite number")
            normalized_signals[name] = float(raw_value)
        if normalized_signals:
            values["assetSignals"] = normalized_signals
    return values


def read_serial_json(
    endpoint: str,
    mappings: list[dict],
    connection_manager: SerialJsonConnectionManager | None = None,
    asset_id: str | None = None,
) -> dict[str, Any]:
    parsed = urlparse(endpoint)
    if parsed.scheme != "serial" or not parsed.path:
        raise ValueError("Serial JSON endpoint must look like serial:///dev/ttyUSB0?baudrate=115200")

    if connection_manager is not None:
        record = connection_manager.read_record(asset_id or endpoint, endpoint)
        return _serial_record_values(record, mappings)

    # One-shot mode remains useful to callers outside the long-running gateway.
    import serial
    options = parse_qs(parsed.query)
    baudrate = int(options.get("baudrate", ["115200"])[0])
    timeout = float(options.get("timeout", ["1"])[0])
    with serial.Serial(parsed.path, baudrate, timeout=timeout) as port:
        raw = port.readline()
    if len(raw) > MAX_SERIAL_RECORD_BYTES:
        raise ValueError("Serial JSON record exceeds the supported size")
    if not raw:
        return {}
    record = json.loads(raw.decode("utf-8", errors="strict").strip())
    if not isinstance(record, dict):
        raise ValueError("Serial telemetry record must be a JSON object")
    return _serial_record_values(record, mappings)

def read_asset(
    asset: dict,
    serial_connection_manager: SerialJsonConnectionManager | None = None,
) -> dict[str, Any]:
    protocol = str(asset.get("protocol", "")).lower()
    endpoint = str(asset.get("endpoint") or "")
    mappings = asset.get("tagMappings") or []
    if protocol == "opcua":
        return read_opcua(endpoint, mappings)
    if protocol == "modbus_tcp":
        return read_modbus_tcp(endpoint, mappings)
    if protocol == "modbus_rtu":
        return read_modbus_rtu(endpoint, mappings)
    if protocol == "serial":
        return read_serial_json(endpoint, mappings, serial_connection_manager, asset.get("assetId"))
    if protocol == "mqtt":
        return {}
    if protocol == "ada031_v4_serial":
        # Connected-firmware telemetry is handled by the synchronized ADA031
        # command manager. The legacy stock firmware emits no records.
        return {}
    raise ValueError(f"Unsupported asset protocol: {protocol}")


def serial_asset_uses_port(assets: list[dict], device_path: str) -> bool:
    """Avoid opening an ADA031 USB controller twice through two serial readers."""
    target = os.path.realpath(device_path)
    for asset in assets:
        if str(asset.get("protocol", "")).lower() not in {"serial", "modbus_rtu", "ada031_v4_serial"}:
            continue
        parsed = urlparse(str(asset.get("endpoint") or ""))
        if parsed.scheme == "serial" and parsed.path and os.path.realpath(parsed.path) == target:
            return True
    return False
