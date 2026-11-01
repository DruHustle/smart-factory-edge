"""Typed observations and local commissioning boundaries for edge drivers."""
from __future__ import annotations

import ipaddress
import json
import math
import os
import re
import socket
import stat
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from connectivity_schema import DATA_TYPES, REGISTER_COUNTS

INTEGER_LIMITS = {
    "int16": (-32768, 32767), "uint16": (0, 65535),
    "int32": (-2147483648, 2147483647), "uint32": (0, 4294967295),
    "int64": (-9223372036854775808, 9223372036854775807),
    "uint64": (0, 18446744073709551615),
}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_timestamp(value):
    if value is None:
        return None
    if isinstance(value, str):
        if len(value) > 35 or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?(?:Z|\+00:00)", value):
            raise ValueError("source_timestamp")
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("source_timestamp")
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def normalize_value(tag, value):
    kind = tag["dataType"]
    if kind == "boolean":
        if type(value) is not bool:
            raise ValueError("tag_type_mismatch")
        return value
    if kind == "string":
        if not isinstance(value, str) or len(value) > 1024 or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError("tag_string_invalid")
        return value
    if kind in INTEGER_LIMITS:
        if kind in {"int64", "uint64"} and isinstance(value, str) and re.fullmatch(r"-?(?:0|[1-9][0-9]{0,19})", value):
            value = int(value)
        if type(value) is not int or not INTEGER_LIMITS[kind][0] <= value <= INTEGER_LIMITS[kind][1]:
            raise ValueError("tag_integer_range")
        if kind in {"int64", "uint64"}:
            return str(value)
    elif kind in {"float32", "float64"}:
        if type(value) not in {int, float} or not math.isfinite(value):
            raise ValueError("tag_float_invalid")
        if kind == "float32" and abs(value) > 3.4028234663852886e38:
            raise ValueError("tag_float_range")
    else:
        raise ValueError("tag_type_unsupported")
    result = value * tag.get("scale", 1) + tag.get("offset", 0)
    if not math.isfinite(result) or abs(result) > 1.7976931348623157e308:
        raise ValueError("tag_scaling_overflow")
    return result


def sample(tag, value=None, quality="unknown", native_status=None, source_timestamp=None, server_timestamp=None, basis="communication"):
    result = {"tagId": tag["tagId"], "dataType": tag["dataType"], "value": None, "quality": quality, "qualityBasis": basis}
    if "unit" in tag:
        result["unit"] = tag["unit"]
    if native_status:
        result["nativeStatus"] = str(native_status)[:128]
    if value is None and quality == "good":
        # Preserve the affected tag as a bad sample. A good/null pair violates
        # both cloud consumers' wire contract and would reject the whole batch.
        result.update(quality="bad", nativeStatus="source_value_missing")
    if quality != "bad" and value is not None:
        try:
            result["value"] = normalize_value(tag, value)
        except (ValueError, TypeError, OverflowError) as error:
            result.update(value=None, quality="bad", nativeStatus=str(error)[:128])
    for key, timestamp in (("sourceTimestamp", source_timestamp), ("serverTimestamp", server_timestamp)):
        if timestamp is not None:
            try:
                result[key] = utc_timestamp(timestamp)
            except (ValueError, OverflowError):
                result.update(value=None, quality="bad", nativeStatus="source_timestamp_invalid")
    return result


class LocalCommissioning:
    """Only the gateway operator can authorize destinations and local resources."""

    def __init__(self, environ=None):
        self.env = environ if environ is not None else os.environ
        self.production = self.env.get("EDGE_ENV", "development").strip().lower() == "production"
        self.networks = [ipaddress.ip_network(n.strip(), strict=False) for n in self.env.get("CONNECTIVITY_ALLOWED_NETWORKS", "").split(",") if n.strip()]
        self.hosts = {h.strip().lower() for h in self.env.get("CONNECTIVITY_ALLOWED_HOSTS", "").split(",") if h.strip()}
        self.serial_paths = {os.path.realpath(p.strip()) for p in self.env.get("CONNECTIVITY_ALLOWED_SERIAL_PORTS", "").split(",") if p.strip()}
        self.interfaces = {i.strip() for i in self.env.get("CONNECTIVITY_ALLOWED_INTERFACES", "").split(",") if i.strip()}
        self.secrets = {"credentials": {}, "collectors": {}}
        secret_file = self.env.get("CONNECTIVITY_SECRETS_FILE", "")
        if secret_file:
            path = Path(secret_file)
            info = path.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > 65536 or (self.production and info.st_mode & 0o077):
                raise ValueError("connectivity_secret_file_permissions")
            data = json.loads(path.read_text())
            if not isinstance(data, dict) or set(data) - {"credentials", "collectors"}:
                raise ValueError("connectivity_secret_file_schema")
            self.secrets.update(data)

    def credentials(self, connection):
        ref = connection.get("credentialRef")
        if not ref:
            return {}
        value = self.secrets.get("credentials", {}).get(ref)
        if not isinstance(value, dict):
            raise ValueError("credential_reference_unavailable")
        return dict(value)

    def collector(self, config):
        if config["interface"] not in self.interfaces and not (not self.production and config["interface"] == "lo"):
            raise ValueError("tsn_interface_not_commissioned")
        ref = config.get("collectorRef")
        if not ref:
            return {}
        value = self.secrets.get("collectors", {}).get(ref)
        if not isinstance(value, dict) or value.get("interface") != config["interface"]:
            raise ValueError("tsn_collector_not_commissioned")
        return dict(value)

    def endpoint(self, connection):
        parsed = urlparse(connection["endpoint"])
        if parsed.scheme == "serial":
            if os.path.realpath(parsed.path) not in self.serial_paths:
                raise ValueError("serial_port_not_commissioned")
            return parsed.path
        host = parsed.hostname
        try:
            addresses = {ipaddress.ip_address(host)}
        except ValueError:
            addresses = {ipaddress.ip_address(item[4][0]) for item in socket.getaddrinfo(host, parsed.port or 4840, type=socket.SOCK_STREAM)}
        if not addresses:
            raise ValueError("endpoint_resolution_failed")
        for address in addresses:
            if address.is_unspecified or str(address) in {"169.254.169.254", "fd00:ec2::254"}:
                raise ValueError("endpoint_not_commissioned")
            allowed = host.lower() in self.hosts or any(address in network for network in self.networks)
            if not self.production and address.is_loopback:
                allowed = True
            if address.is_multicast and parsed.scheme != "opc.udp":
                allowed = False
            if not allowed:
                raise ValueError("endpoint_not_commissioned")
        if self.production and parsed.scheme == "mqtt" and not all(a.is_loopback for a in addresses):
            raise ValueError("mqtt_tls_required")
        return str(sorted(addresses, key=str)[0])
