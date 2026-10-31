"""Read-only UAFX observation and TSN evidence.

UADP support is a deliberately bounded observer profile: scalar keyframes with
explicit publisher/group/writer identity and payload headers. It cannot mutate
an FX ConnectionManager, handle encrypted frames, or establish TSN guarantees.
Protocol layout: OPC 10000-14 7.2.4.4 and 7.2.4.5.
"""
from __future__ import annotations

import json
import math
import os
import re
import struct
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from connectivity_values import sample, utc_now, utc_timestamp

FX_AC = "http://opcfoundation.org/UA/FX/AC/"
FX_CM = "http://opcfoundation.org/UA/FX/CM/"
FX_DATA = "http://opcfoundation.org/UA/FX/Data/"
FX_TYPES = {
    (FX_AC, "AutomationComponentType"): "automation_component",
    (FX_AC, "FunctionalEntityType"): "functional_entity",
    (FX_AC, "ConnectionEndpointType"): "connection_endpoint",
    (FX_AC, "PubSubConnectionEndpointType"): "connection_endpoint",
    (FX_CM, "ConnectionManagerType"): "connection_manager",
}


class Cursor:
    def __init__(self, data):
        self.data = memoryview(data)
        self.position = 0

    @property
    def remaining(self):
        return len(self.data) - self.position

    def take(self, size):
        if size < 0 or size > self.remaining:
            raise ValueError("uadp_truncated")
        result = self.data[self.position:self.position + size]
        self.position += size
        return result

    def unpack(self, fmt):
        return struct.unpack("<" + fmt, self.take(struct.calcsize("<" + fmt)))[0]

    def string(self):
        size = self.unpack("i")
        if not 0 <= size <= 4096:
            raise ValueError("uadp_string_length")
        return bytes(self.take(size)).decode("utf-8", errors="strict")

    def timestamp(self):
        ticks = self.unpack("q")
        if ticks <= 0:
            return None
        try:
            return utc_timestamp(datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=ticks // 10))
        except OverflowError:
            raise ValueError("uadp_timestamp") from None


UA_TYPES = {"boolean": 1, "int16": 4, "uint16": 5, "int32": 6, "uint32": 7, "int64": 8, "uint64": 9, "float32": 10, "float64": 11, "string": 12}
UA_FORMAT = {1: "B", 4: "h", 5: "H", 6: "i", 7: "I", 8: "q", 9: "Q", 10: "f", 11: "d", 19: "I"}


def _raw_scalar(cursor, ua_type):
    if ua_type == 12:
        return cursor.string()
    if ua_type not in UA_FORMAT:
        raise ValueError("uadp_scalar_type")
    value = cursor.unpack(UA_FORMAT[ua_type])
    if ua_type == 1:
        if value not in {0, 1}:
            raise ValueError("uadp_boolean")
        return bool(value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("uadp_nonfinite")
    return value


def _variant(cursor, expected_type):
    kind = cursor.unpack("B")
    if kind & 0xC0:
        raise ValueError("uadp_arrays_unsupported")
    if kind == 0:
        return None, 0x80000000
    if kind == 19:
        return None, cursor.unpack("I")
    if kind != expected_type:
        raise ValueError("uadp_metadata_type_mismatch")
    return _raw_scalar(cursor, kind), 0


def _worst_status(header, field):
    # A field's Good/Uncertain code cannot clear a Bad DataSet status.
    return header if header >> 30 > field >> 30 else field or header


def decode_uadp(data, connection):
    """Decode a commissioned scalar keyframe without trusting wire lengths.

No message-level crypto is implied. Unknown flags, encrypted/signed frames,
chunks, deltas, omitted identities and schema changes fail closed.
"""
    if not isinstance(data, bytes) or not 4 <= len(data) <= 8192:
        raise ValueError("uadp_packet_size")
    fx = connection["fx"]
    cursor = Cursor(data)
    flags = cursor.unpack("B")
    if flags & 0x0F != 1 or flags & 0x70 != 0x70:
        raise ValueError("uadp_explicit_identity_required")
    extended = cursor.unpack("B") if flags & 0x80 else 0
    extended2 = cursor.unpack("B") if extended & 0x80 else 0
    if extended & 0x10 or extended2:
        raise ValueError("uadp_security_or_extension_unsupported")
    publisher_kind = extended & 7
    expected_kind = {"Byte": 0, "UInt16": 1, "UInt32": 2, "UInt64": 3, "String": 4}[fx["publisherIdType"]]
    if publisher_kind != expected_kind:
        raise ValueError("uadp_publisher_type_mismatch")
    publisher = cursor.string() if publisher_kind == 4 else cursor.unpack({0: "B", 1: "H", 2: "I", 3: "Q"}[publisher_kind])
    if str(publisher) != fx["publisherId"]:
        raise ValueError("uadp_publisher_mismatch")
    if extended & 8:
        cursor.take(16)  # DataSetClassId is not used to infer a mapping.
    group_flags = cursor.unpack("B")
    if group_flags & 0xF0 or not group_flags & 1:
        raise ValueError("uadp_group_header")
    if cursor.unpack("H") != fx["writerGroupId"]:
        raise ValueError("uadp_writer_group_mismatch")
    if group_flags & 2:
        cursor.unpack("I")
    if group_flags & 4:
        cursor.unpack("H")
    network_sequence = cursor.unpack("H") if group_flags & 8 else None
    count = cursor.unpack("B")
    if not 1 <= count <= 100:
        raise ValueError("uadp_dataset_count")
    writers = [cursor.unpack("H") for _ in range(count)]
    if len(set(writers)) != len(writers) or fx["dataSetWriterId"] not in writers:
        raise ValueError("uadp_writer_mismatch")
    network_timestamp = cursor.timestamp() if extended & 0x20 else None
    if extended & 0x40:
        cursor.unpack("H")
    sizes = [cursor.unpack("H") for _ in range(count)] if count > 1 else [cursor.remaining]
    if sum(sizes) != cursor.remaining or any(size < 1 for size in sizes):
        raise ValueError("uadp_dataset_sizes")
    payload = None
    for writer, size in zip(writers, sizes):
        raw = cursor.take(size)
        if writer == fx["dataSetWriterId"]:
            payload = Cursor(raw)
    ds_flags = payload.unpack("B")
    ds_flags2 = payload.unpack("B") if ds_flags & 0x80 else 0
    if not ds_flags & 1 or ds_flags2 & 0xCF:
        raise ValueError("uadp_keyframe_required")
    encoding = (ds_flags >> 1) & 3
    if encoding == 3:
        raise ValueError("uadp_encoding_unsupported")
    sequence = payload.unpack("H") if ds_flags & 8 else network_sequence
    stamp = payload.timestamp() if ds_flags2 & 0x10 else network_timestamp
    if ds_flags2 & 0x20:
        payload.unpack("H")
    header_status = payload.unpack("H") << 16 if ds_flags & 0x10 else 0
    major = payload.unpack("I") if ds_flags & 0x20 else None
    minor = payload.unpack("I") if ds_flags & 0x40 else None
    if major is not None and major != fx["metadataMajorVersion"] or minor is not None and minor != fx["metadataMinorVersion"]:
        raise ValueError("uadp_metadata_version_mismatch")
    tags = sorted(connection["tags"], key=lambda item: item["source"]["fieldIndex"])
    if encoding != 1 and payload.unpack("H") != len(tags):
        raise ValueError("uadp_metadata_field_count")
    values = []
    for tag in tags:
        status = header_status
        source_stamp = None
        server_stamp = stamp
        kind = UA_TYPES[tag["dataType"]]
        if encoding == 1:
            value = _raw_scalar(payload, kind)
        elif encoding == 0:
            value, field_status = _variant(payload, kind)
            status = _worst_status(status, field_status)
        else:
            dv_flags = payload.unpack("B")
            if dv_flags & 0xC0:
                raise ValueError("uadp_datavalue_flags")
            value, field_status = _variant(payload, kind) if dv_flags & 1 else (None, 0x80000000)
            field_status = payload.unpack("I") if dv_flags & 2 else field_status
            status = _worst_status(status, field_status)
            source_stamp = payload.timestamp() if dv_flags & 4 else None
            if dv_flags & 0x10:
                payload.unpack("H")
            server_stamp = payload.timestamp() if dv_flags & 8 else stamp
            if dv_flags & 0x20:
                payload.unpack("H")
        severity = status >> 30
        quality = "bad" if severity >= 2 else "uncertain" if severity == 1 else "unknown"
        values.append(sample(tag, value, quality, f"0x{status:08x}", source_stamp, server_stamp, "unauthenticated_udp"))
    if payload.remaining:
        raise ValueError("uadp_trailing_payload")
    return {"tags": values, "sequence": sequence, "metadataVerified": major is not None and minor is not None}


def fx_model_snapshot(client, max_nodes=160):
    """Browse only an existing configured server. Never invokes UA methods."""
    from asyncua import ua
    namespaces = client.get_namespace_array()
    result = {"mode": "diagnostics", "modelVerified": False, "components": [], "warnings": [], "determinism": "unverified"}
    if FX_AC not in namespaces and FX_CM not in namespaces:
        result["warnings"].append("fx_namespaces_not_present")
        return result
    queue = [(client.nodes.objects, 0)]
    seen = set()
    type_cache = {}
    deadline = time.monotonic() + 5

    def kind_for(node_id):
        key = node_id.to_string()
        if key in type_cache:
            return type_cache[key]
        current = client.get_node(node_id)
        type_seen = set()
        for _ in range(12):
            if time.monotonic() >= deadline:
                raise TimeoutError("fx_browse_budget")
            current_key = current.nodeid.to_string()
            if current_key in type_seen:
                break
            type_seen.add(current_key)
            browse = current.read_browse_name()
            uri = namespaces[browse.NamespaceIndex] if browse.NamespaceIndex < len(namespaces) else ""
            found = FX_TYPES.get((uri, browse.Name))
            if found:
                type_cache[key] = found
                return found
            parents = current.get_referenced_nodes(refs=ua.ObjectIds.HasSubtype, direction=ua.BrowseDirection.Inverse, includesubtypes=False)
            if not parents:
                break
            current = parents[0]
        type_cache[key] = None
        return None

    try:
        while queue and len(seen) < max_nodes and time.monotonic() < deadline:
            node, depth = queue.pop(0)
            key = node.nodeid.to_string()
            if key in seen or (node.nodeid.NamespaceIndex == 0 and node.nodeid.Identifier == ua.ObjectIds.Server):
                continue
            seen.add(key)
            if node.read_node_class() != ua.NodeClass.Object:
                continue
            definition = node.read_type_definition()
            kind = kind_for(definition) if definition is not None else None
            if kind:
                index = node.nodeid.NamespaceIndex
                uri = namespaces[index] if index < len(namespaces) else ""
                entry = {"name": str(node.read_display_name().Text or key)[:120], "nodeId": key[:512], "namespaceUri": uri[:256], "kind": kind}
                if len(result["components"]) < 20:
                    result["components"].append(entry)
                result["modelVerified"] = True
            if depth < 8:
                queue.extend((child, depth + 1) for child in node.get_children()[:max_nodes])
        if queue:
            result["warnings"].append("fx_discovery_truncated")
    except Exception:
        result["warnings"].append("fx_discovery_incomplete")
    if not result["modelVerified"]:
        result["warnings"].append("fx_components_not_observed")
    return result


def _command(args):
    """No shell, no stdin, bounded output/time, and an immutable GET-only argv."""
    with __import__("tempfile").TemporaryFile() as output:
        try:
            completed = subprocess.run(args, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL, timeout=2, check=False, env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"})
            if completed.returncode:
                return None
            output.seek(0)
            data = output.read(16385)
            return data.decode("utf-8", errors="replace") if len(data) <= 16384 else None
        except (OSError, subprocess.TimeoutExpired):
            return None


def collect_tsn(config, commissioning, runner=_command, sysfs_root="/sys/class/net"):
    interface = config["interface"]
    settings = commissioning.collector(config)
    result = {"interface": interface, "observedAt": utc_now(), "clock": {"state": "unknown"}, "capabilities": {"hardwareTimestamping": "unknown"}, "qdiscs": [], "counters": {}, "evidence": [], "determinism": "unverified"}
    def evidence(source, observed):
        result["evidence"].append({"source": source, "status": "observed" if observed else "unavailable"})
    ethtool = runner(["ethtool", "-T", interface])
    evidence("ethtool", ethtool is not None)
    if ethtool is not None:
        hardware = "hardware-transmit" in ethtool and "hardware-receive" in ethtool and "hardware-raw-clock" in ethtool
        result["capabilities"]["hardwareTimestamping"] = "supported" if hardware else "unsupported"
        clock = re.search(r"PTP Hardware Clock:\s*(-?\d+)", ethtool)
        if clock and int(clock[1]) >= 0:
            result["capabilities"]["ptpHardwareClock"] = "ptp" + clock[1]
    tc = runner(["tc", "-j", "qdisc", "show", "dev", interface])
    observed = False
    if tc is not None:
        try:
            qdiscs = json.loads(tc)
            if not isinstance(qdiscs, list):
                raise ValueError()
            for item in qdiscs[:8]:
                if not isinstance(item, dict) or not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", item.get("kind", "")):
                    continue
                normalized = {"kind": item["kind"]}
                for field in ("handle", "parent"):
                    if isinstance(item.get(field), str) and re.fullmatch(r"[A-Za-z0-9:_-]{1,32}", item[field]):
                        normalized[field] = item[field]
                result["qdiscs"].append(normalized)
            observed = True
        except (ValueError, TypeError):
            pass
    evidence("tc", observed)
    for filename, field in (("rx_dropped", "rxDropped"), ("tx_dropped", "txDropped"), ("rx_errors", "rxErrors"), ("tx_errors", "txErrors")):
        try:
            number = (Path(sysfs_root) / interface / "statistics" / filename).read_text()[:64].strip()
            if re.fullmatch(r"\d{1,16}", number) and int(number) <= 9007199254740991:
                result["counters"][field] = int(number)
        except OSError:
            pass
    evidence("sysfs", bool(result["counters"]))
    socket_path = settings.get("pmcSocket")
    pmc = None
    if isinstance(socket_path, str) and len(socket_path) <= 256 and socket_path.startswith(("/run/", "/var/run/")) and ".." not in Path(socket_path).parts and not any(ord(c) < 32 for c in socket_path):
        domain = config.get("ptpDomain", 0)
        transport_specific = settings.get("transportSpecific", 0)
        if type(transport_specific) is not int or not 0 <= transport_specific <= 15:
            raise ValueError("tsn_transport_specific_not_commissioned")
        # pmc otherwise binds its client socket under /var/run. A private
        # temporary client path allows an unprivileged read-only collector.
        with tempfile.TemporaryDirectory(prefix="sf-tsn-") as client_directory:
            pmc = runner(["pmc", "-u", "-b", "0", "-d", str(domain), "-t", format(transport_specific, "x"), "-s", socket_path, "-i", str(Path(client_directory) / "pmc"), "GET TIME_STATUS_NP", "GET PORT_DATA_SET", "GET CURRENT_DATA_SET"])
        result["clock"]["domain"] = domain
    evidence("pmc", pmc is not None)
    if pmc is not None:
        clock = result["clock"]
        for pattern, field in ((r"master_offset\s+(-?\d+(?:\.\d+)?)", "offsetNs"), (r"meanPathDelay\s+(-?\d+(?:\.\d+)?)", "meanPathDelayNs")):
            match = re.search(pattern, pmc)
            if match:
                number = float(match[1])
                if math.isfinite(number) and abs(number) <= 9007199254740991:
                    clock[field] = number
        gm = re.search(r"gmIdentity\s+([A-Fa-f0-9.:]{1,64})", pmc)
        if gm:
            clock["grandmasterId"] = gm[1]
        states = re.findall(r"portState\s+([A-Z_]{1,32})", pmc)
        if len(set(states)) == 1:
            clock["portState"] = states[0]
            if states[0] in {"SLAVE", "TIME_RECEIVER"} and re.search(r"gmPresent\s+(?:true|1)\b", pmc) and "offsetNs" in clock:
                clock["state"] = "synchronized"
            elif states[0] in {"FAULTY", "DISABLED", "LISTENING", "UNCALIBRATED", "INITIALIZING"}:
                clock["state"] = "unsynchronized"
    return result
