"""Independent, bounded industrial connectivity wire validation.

The existing asset profile remains schema 1. This contract uses its own textual
version and never carries protocol credentials or permission to write devices.
"""
from __future__ import annotations

import copy
import hashlib
import ipaddress
import json
import math
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from measurement_units import valid_identity

CONFIG_VERSION = "industrial-connectivity/v1"
OBSERVATION_VERSION = "industrial-observation/v1"
PROTOCOLS = {"opcua", "modbus_tcp", "modbus_rtu", "mqtt", "opcua_fx"}
DATA_TYPES = {"boolean", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32", "float64", "string"}
REF = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", re.ASCII)
PROPERTY = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", re.ASCII)
INTERFACE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,14}", re.ASCII)
SAFE_INTEGER = 9007199254740991
REGISTER_COUNTS = {"int16": 1, "uint16": 1, "int32": 2, "uint32": 2, "float32": 2, "int64": 4, "uint64": 4, "float64": 4}


def fail(code):
    raise ValueError("invalid_connectivity:" + code)


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail("duplicate_json_field")
        result[key] = value
    return result


def fields(value, required, optional=()):
    if not isinstance(value, dict) or not set(required) <= set(value) or set(value) - set(required) - set(optional):
        fail("fields")


def text(value, limit, empty=False):
    if not isinstance(value, str) or len(value) > limit or (not empty and not value) or any(ord(c) < 32 or ord(c) == 127 for c in value):
        fail("text")


def integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        fail("integer")


def enum(value, choices):
    if not isinstance(value, str) or value not in choices:
        fail("enum")


def reference(value):
    if not isinstance(value, str) or not REF.fullmatch(value):
        fail("reference")


def pointer(value):
    text(value, 256, empty=True)
    if value and (not value.startswith("/") or re.search(r"~(?![01])", value)):
        fail("json_pointer")


def json_pointer(value, path):
    for part in path.split("/")[1:] if path else []:
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(value, list):
            if not re.fullmatch(r"0|[1-9][0-9]{0,5}", part):
                raise ValueError("payload_path")
            value = value[int(part)]
        elif isinstance(value, dict):
            value = value[part]
        else:
            raise ValueError("payload_path")
    return value


def endpoint_resource(endpoint):
    """Normalize default ports/case when detecting legacy polling owners."""
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"tcp", "opc.tcp"}:
        return endpoint
    return (parsed.scheme, (parsed.hostname or "").lower(), parsed.port or (502 if parsed.scheme == "tcp" else 4840))


def _endpoint(connection):
    endpoint = connection["endpoint"]
    text(endpoint, 512)
    try:
        parsed = urlparse(endpoint)
        if parsed.username or parsed.password or parsed.fragment:
            fail("endpoint")
        protocol = connection["protocol"]
        if protocol == "modbus_rtu":
            if parsed.scheme != "serial" or parsed.netloc or not parsed.path.startswith("/dev/") or ".." in Path(parsed.path).parts:
                fail("serial_endpoint")
            opts = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
            if set(opts) - {"baudrate", "parity", "bytesize", "stopbits"} or any(len(v) != 1 for v in opts.values()):
                fail("serial_options")
            integer(int(opts.get("baudrate", ["9600"])[0]), 300, 1000000)
            enum(opts.get("parity", ["N"])[0], {"N", "E", "O"})
            integer(int(opts.get("bytesize", ["8"])[0]), 7, 8)
            integer(int(opts.get("stopbits", ["1"])[0]), 1, 2)
            return
        schemes = {"opcua": {"opc.tcp"}, "mqtt": {"mqtt", "mqtts"}, "modbus_tcp": {"tcp"}}
        allowed = schemes.get(protocol, {"opc.udp"} if connection.get("fx", {}).get("mode") == "udp_uadp" else {"opc.tcp"})
        if parsed.scheme not in allowed or not parsed.hostname or parsed.query:
            fail("endpoint")
        if parsed.port is not None:
            integer(parsed.port, 1, 65535)
        if protocol in {"mqtt", "modbus_tcp"} and parsed.path not in {"", "/"}:
            fail("endpoint_path")
        if parsed.scheme == "opc.udp" and (not parsed.port or parsed.path not in {"", "/"}):
            fail("udp_endpoint")
    except (ValueError, TypeError, OverflowError) as error:
        if str(error).startswith("invalid_connectivity:"):
            raise
        fail("endpoint")


def _fx(connection):
    fx = connection.get("fx")
    if connection["protocol"] != "opcua_fx":
        if "fx" in connection:
            fail("fx_protocol")
        return
    fields(fx, {"mode"}, {"publisherId", "publisherIdType", "writerGroupId", "dataSetWriterId", "metadataMajorVersion", "metadataMinorVersion", "interfaceAddress", "allowUnsecured"})
    enum(fx["mode"], {"diagnostics", "udp_uadp"})
    if fx["mode"] == "diagnostics":
        if set(fx) != {"mode"}:
            fail("fx_diagnostics_fields")
        return
    required = {"publisherId", "publisherIdType", "writerGroupId", "dataSetWriterId", "metadataMajorVersion", "metadataMinorVersion", "allowUnsecured"}
    if not required <= set(fx) or fx["allowUnsecured"] is not True:
        fail("fx_udp_commissioning")
    enum(fx["publisherIdType"], {"Byte", "UInt16", "UInt32", "UInt64", "String"})
    text(fx["publisherId"], 128)
    if fx["publisherIdType"] != "String":
        limits = {"Byte": 255, "UInt16": 65535, "UInt32": 4294967295, "UInt64": 18446744073709551615}
        if not re.fullmatch(r"0|[1-9][0-9]{0,19}", fx["publisherId"]):
            fail("publisher_id")
        integer(int(fx["publisherId"]), 0, limits[fx["publisherIdType"]])
    for key in ("writerGroupId", "dataSetWriterId"):
        integer(fx[key], 1, 65535)
    for key in ("metadataMajorVersion", "metadataMinorVersion"):
        integer(fx[key], 0, 4294967295)
    if "interfaceAddress" in fx:
        try:
            ipaddress.IPv4Address(fx["interfaceAddress"])
        except (ValueError, TypeError):
            fail("interface_address")


def validate_connection(value):
    fields(value, {"connectionId", "name", "protocol", "endpoint", "enabled", "intervalMs", "tags"}, {"credentialRef", "mode", "scenarioRunId", "fx", "tsn"})
    if not valid_identity(value["connectionId"]) or value["connectionId"] == "gateway-configuration":
        fail("connection_id")
    text(value["name"], 120)
    enum(value["protocol"], PROTOCOLS)
    if type(value["enabled"]) is not bool:
        fail("enabled")
    integer(value["intervalMs"], 100, 60000)
    mode = value.get("mode", "live")
    enum(mode, {"live", "simulated"})
    if mode == "simulated":
        from provenance import IDENTIFIER
        run = value.get("scenarioRunId")
        if not isinstance(run, str) or not IDENTIFIER.fullmatch(run):
            fail("scenario_run")
    elif "scenarioRunId" in value:
        fail("scenario_mode")
    if "credentialRef" in value:
        reference(value["credentialRef"])
    _fx(value)
    _endpoint(value)
    if "tsn" in value:
        tsn = value["tsn"]
        fields(tsn, {"interface"}, {"ptpDomain", "collectorRef"})
        if not isinstance(tsn["interface"], str) or not INTERFACE.fullmatch(tsn["interface"]):
            fail("tsn_interface")
        if "ptpDomain" in tsn:
            integer(tsn["ptpDomain"], 0, 127)
        if "collectorRef" in tsn:
            reference(tsn["collectorRef"])
    tags = value["tags"]
    if not isinstance(tags, list) or len(tags) > 100:
        fail("tags")
    ids = set()
    udp = value.get("fx", {}).get("mode") == "udp_uadp"
    for tag in tags:
        fields(tag, {"tagId", "name", "dataType", "source"}, {"unit", "assetId", "property", "scale", "offset"})
        if not valid_identity(tag["tagId"]) or tag["tagId"] in ids:
            fail("tag_id")
        ids.add(tag["tagId"])
        text(tag["name"], 120)
        enum(tag["dataType"], DATA_TYPES)
        for key, limit in (("unit", 32), ("assetId", 128)):
            if key in tag:
                text(tag[key], limit)
        if "property" in tag and (not isinstance(tag["property"], str) or not PROPERTY.fullmatch(tag["property"])):
            fail("property")
        for key, default in (("scale", 1), ("offset", 0)):
            number = tag.get(key, default)
            if type(number) not in {int, float} or not math.isfinite(number):
                fail("scaling")
            if tag["dataType"] in {"boolean", "string", "int64", "uint64"} and number != default:
                fail("scaling_type")
        source = tag["source"]
        if udp:
            fields(source, {"fieldIndex"})
            integer(source["fieldIndex"], 0, 99)
        elif value["protocol"] in {"opcua", "opcua_fx"}:
            fields(source, {"nodeId"}, {"namespaceUri"})
            text(source["nodeId"], 512)
            if "namespaceUri" in source:
                text(source["namespaceUri"], 256)
                if not re.fullmatch(r"(?:i=[0-9]{1,10}|s=.{1,510}|g=[0-9a-fA-F-]{36}|b=[A-Za-z0-9+/=]+)", source["nodeId"]):
                    fail("stable_node_id")
            elif not re.fullmatch(r"(?:ns=[0-9]{1,5};)?(?:i=[0-9]{1,10}|s=.{1,510}|g=[0-9a-fA-F-]{36}|b=[A-Za-z0-9+/=]+)", source["nodeId"]):
                fail("node_id")
        elif value["protocol"] in {"modbus_tcp", "modbus_rtu"}:
            fields(source, {"address", "registerType", "unitId"}, {"count", "byteOrder", "wordOrder"})
            integer(source["address"], 0, 65535)
            integer(source["unitId"], 1, 247)
            enum(source["registerType"], {"coil", "discrete_input", "input_register", "holding_register"})
            count = 1 if source["registerType"] in {"coil", "discrete_input"} else REGISTER_COUNTS.get(tag["dataType"])
            if count is None or (source["registerType"] in {"coil", "discrete_input"} and tag["dataType"] != "boolean"):
                fail("register_data_type")
            if type(source.get("count", count)) is not int or source.get("count", count) != count or source["address"] + count > 65536:
                fail("register_count")
            for key in ("byteOrder", "wordOrder"):
                enum(source.get(key, "big"), {"big", "little"})
        else:
            fields(source, {"topic", "path"}, {"timestampPath", "qualityPath"})
            text(source["topic"], 256)
            if any(char in source["topic"] for char in "+#"):
                fail("mqtt_topic")
            for key in ("path", "timestampPath", "qualityPath"):
                if key in source:
                    pointer(source[key])
    if udp and (not tags or sorted(t["source"]["fieldIndex"] for t in tags) != list(range(len(tags)))):
        fail("udp_metadata_fields")
    return copy.deepcopy(value)


def validate_configuration(value, gateway_id):
    fields(value, {"schemaVersion", "gatewayDeviceId", "revision", "connections"})
    if value["schemaVersion"] != CONFIG_VERSION or not valid_identity(gateway_id) or value["gatewayDeviceId"] != gateway_id:
        fail("gateway_or_version")
    integer(value["revision"], 1, SAFE_INTEGER)
    connections = value["connections"]
    if not isinstance(connections, list) or len(connections) > 16:
        fail("connections")
    result = {**value, "connections": [validate_connection(c) for c in connections]}
    ids = [c["connectionId"] for c in result["connections"]]
    if len(ids) != len(set(ids)):
        fail("duplicate_connection")
    serial_paths = [os.path.realpath(urlparse(c["endpoint"]).path) for c in connections if c["protocol"] == "modbus_rtu" and c["enabled"]]
    if len(serial_paths) != len(set(serial_paths)):
        fail("duplicate_serial_owner")
    # Leave room for the action wrapper inside the 64 KiB MQTT command.
    if len(json.dumps(result, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()) > 64000:
        fail("configuration_bytes")
    return result


def save_configuration(path, value, gateway_id):
    validated = validate_configuration(value, gateway_id)
    payload = json.dumps(validated, separators=(",", ":"), ensure_ascii=False, sort_keys=True, allow_nan=False).encode()
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".connectivity-", dir=destination.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return hashlib.sha256(payload).hexdigest()


def load_configuration(path, gateway_id):
    if not path or not Path(path).is_file():
        return {"schemaVersion": CONFIG_VERSION, "gatewayDeviceId": gateway_id, "revision": 1, "connections": []}
    with open(path, "rb") as source:
        data = source.read(64001)
    if len(data) > 64000:
        fail("configuration_bytes")
    return validate_configuration(json.loads(data, object_pairs_hook=unique_fields), gateway_id)
