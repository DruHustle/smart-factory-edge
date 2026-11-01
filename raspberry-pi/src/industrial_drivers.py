"""Read-only drivers behind the industrial connectivity registry.

Each driver owns a connection and emits typed observations. Device writes and
arbitrary OPC UA methods are deliberately absent from this interface.
"""
from __future__ import annotations

import ipaddress
import json
import os
import socket
import struct
import threading
import time
import uuid
from urllib.parse import parse_qs, urlparse

from connectivity_schema import REGISTER_COUNTS, json_pointer, unique_fields
from connectivity_values import sample, utc_now, utc_timestamp
from fx_tsn import decode_uadp, fx_model_snapshot


DRIVER_CAPABILITIES = {
    "opcua": {"browse": True, "read": True, "subscribe": True, "write": False},
    "modbus_tcp": {"browse": False, "read": True, "subscribe": False, "write": False},
    "modbus_rtu": {"browse": False, "read": True, "subscribe": False, "write": False},
    "mqtt": {"browse": False, "read": False, "subscribe": True, "write": False},
    "opcua_fx": {"browse": True, "read": True, "subscribe": True, "write": False},
}


class Driver:
    def __init__(self, connection, commissioning, serial_lock=None):
        self.connection = connection
        self.commissioning = commissioning
        self.serial_lock = serial_lock or threading.RLock()

    def connect(self):
        raise NotImplementedError

    def collect(self):
        raise NotImplementedError

    def close(self):
        pass


def _state(tags):
    if not tags:
        return "waiting"
    return "degraded" if any(t["quality"] == "bad" for t in tags) else "connected"


class ModbusDriver(Driver):
    def connect(self):
        from pymodbus.client import ModbusTcpClient, ModbusSerialClient
        endpoint = urlparse(self.connection["endpoint"])
        authorized = self.commissioning.endpoint(self.connection)
        self.is_serial = self.connection["protocol"] == "modbus_rtu"
        self.io_lock = self.serial_lock if self.is_serial else threading.RLock()
        if self.is_serial:
            opts = parse_qs(endpoint.query)
            self.client = ModbusSerialClient(port=authorized, baudrate=int(opts.get("baudrate", ["9600"])[0]), parity=opts.get("parity", ["N"])[0], bytesize=int(opts.get("bytesize", ["8"])[0]), stopbits=int(opts.get("stopbits", ["1"])[0]), timeout=2, retries=0)
        else:
            self.client = ModbusTcpClient(authorized, port=endpoint.port or 502, timeout=2, retries=0)
        with self.io_lock:
            if not self.client.connect():
                raise ConnectionError("modbus_connection_unavailable")

    @staticmethod
    def decode(registers, tag):
        source = tag["source"]
        words = list(registers)
        if source.get("wordOrder", "big") == "little":
            words.reverse()
        data = b"".join(struct.pack(">H", word) for word in words)
        if source.get("byteOrder", "big") == "little":
            data = b"".join(data[i:i+2][::-1] for i in range(0, len(data), 2))
        fmt = {"int16": "h", "uint16": "H", "int32": "i", "uint32": "I", "int64": "q", "uint64": "Q", "float32": "f", "float64": "d"}[tag["dataType"]]
        return struct.unpack(">" + fmt, data)[0]

    def collect(self):
        # Batch only adjacent/overlapping commissioned addresses. Do not read
        # unrequested gaps or assume a device tolerates a full register range.
        plans = []
        tags = sorted(self.connection["tags"], key=lambda t: (t["source"]["unitId"], t["source"]["registerType"], t["source"]["address"]))
        for tag in tags:
            source = tag["source"]
            count = REGISTER_COUNTS.get(tag["dataType"], 1)
            key = (source["unitId"], source["registerType"])
            start, end = source["address"], source["address"] + count
            maximum = 2000 if key[1] in {"coil", "discrete_input"} else 125
            if plans and plans[-1]["key"] == key and start <= plans[-1]["end"] and end - plans[-1]["start"] <= maximum:
                plans[-1]["end"] = max(end, plans[-1]["end"])
                plans[-1]["tags"].append(tag)
            else:
                plans.append({"key": key, "start": start, "end": end, "tags": [tag]})
        result = []
        deadline = time.monotonic() + 8
        for plan in plans:
            if time.monotonic() >= deadline:
                result.extend(sample(t, quality="bad", native_status="acquisition_budget_exceeded") for t in plan["tags"])
                continue
            method = {"coil": "read_coils", "discrete_input": "read_discrete_inputs", "input_register": "read_input_registers", "holding_register": "read_holding_registers"}[plan["key"][1]]
            try:
                with self.io_lock:
                    response = getattr(self.client, method)(plan["start"], count=plan["end"] - plan["start"], device_id=plan["key"][0])
                if response.isError():
                    raise ValueError("modbus_read_exception")
                for tag in plan["tags"]:
                    offset = tag["source"]["address"] - plan["start"]
                    if tag["dataType"] == "boolean":
                        # Boolean tags can be commissioned on register tables
                        # as well as bit tables. A register uses zero/nonzero;
                        # only coils and discrete inputs have response.bits.
                        values = response.bits if plan["key"][1] in {"coil", "discrete_input"} else response.registers
                        value = bool(values[offset])
                    else:
                        value = self.decode(response.registers[offset:offset + REGISTER_COUNTS[tag["dataType"]]], tag)
                    result.append(sample(tag, value, native_status="read_succeeded_sensor_quality_unknown"))
            except Exception:
                result.extend(sample(t, quality="bad", native_status="modbus_read_failed") for t in plan["tags"])
        if not getattr(self.client, "connected", True):
            raise ConnectionError("modbus_connection_lost")
        return {"tags": result, "state": _state(result)}

    def close(self):
        if getattr(self, "client", None):
            with self.io_lock:
                self.client.close()


class OpcUaDriver(Driver):
    def connect(self):
        from asyncua import ua
        from asyncua.sync import Client
        self.commissioning.endpoint(self.connection)
        self.client = Client(self.connection["endpoint"], timeout=3)
        credentials = self.commissioning.credentials(self.connection)
        if credentials:
            policy = credentials.get("securityPolicy")
            mode = credentials.get("securityMode", "SignAndEncrypt")
            if not policy:
                if self.commissioning.production or credentials.get("allowInsecure") is not True:
                    raise ValueError("opcua_security_required")
            else:
                if mode not in {"Sign", "SignAndEncrypt"} or not all(credentials.get(k) for k in ("clientCertificate", "clientPrivateKey", "serverCertificate")):
                    raise ValueError("opcua_trust_configuration_required")
                if self.commissioning.production and mode != "SignAndEncrypt":
                    raise ValueError("opcua_encryption_required")
                self.client.set_security_string(",".join([policy, mode, credentials["clientCertificate"], credentials["clientPrivateKey"], credentials["serverCertificate"]]))
            if credentials.get("username"):
                self.client.set_user(credentials["username"])
                self.client.set_password(credentials.get("password", ""))
        else:
            from asset_adapters import configure_opcua_security
            configure_opcua_security(self.client)
            if os.getenv("OPCUA_USERNAME"):
                self.client.set_user(os.environ["OPCUA_USERNAME"])
                self.client.set_password(os.getenv("OPCUA_PASSWORD", ""))
        self.client.connect()
        self.namespaces = self.client.get_namespace_array()
        self.lock = threading.Lock()
        self.cache = {}
        self.by_node = {}
        self.subscription_failed = False
        self.subscription = self.client.create_subscription(self.connection["intervalMs"], self)
        nodes = {}
        for tag in self.connection["tags"]:
            try:
                source = tag["source"]
                node_id = source["nodeId"]
                if source.get("namespaceUri"):
                    node_id = f"ns={self.namespaces.index(source['namespaceUri'])};{node_id}"
                node = self.client.get_node(node_id)
                self.by_node.setdefault(node.nodeid.to_string(), []).append(tag)
                nodes[node.nodeid.to_string()] = node
            except Exception:
                self.cache[tag["tagId"]] = sample(tag, quality="bad", native_status="opcua_tag_unavailable", basis="protocol")
        if nodes:
            # One Read and one CreateMonitoredItems request keep 100 tags
            # bounded even when a server or address is unavailable.
            ordered = list(nodes.values())
            values = self.client.read_attributes(ordered, ua.AttributeIds.Value)
            for node, value in zip(ordered, values):
                for tag in self.by_node[node.nodeid.to_string()]:
                    self.cache[tag["tagId"]] = self._value(tag, value)
            handles = self.subscription.subscribe_data_change(ordered, queuesize=1, sampling_interval=self.connection["intervalMs"])
            for node, handle in zip(ordered, handles):
                if isinstance(handle, ua.StatusCode) and handle.is_bad():
                    for tag in self.by_node[node.nodeid.to_string()]:
                        self.cache[tag["tagId"]] = sample(tag, quality="bad", native_status="opcua_monitoring_unavailable", basis="protocol")
        self.discovery = self._discover()
        self.fx = fx_model_snapshot(self.client) if self.connection["protocol"] == "opcua_fx" else None

    @staticmethod
    def _value(tag, data):
        code = int(data.StatusCode.value)
        severity = code >> 30
        quality = "bad" if severity >= 2 else "uncertain" if severity == 1 else "good"
        expected = {"boolean": "Boolean", "int16": "Int16", "uint16": "UInt16", "int32": "Int32", "uint32": "UInt32", "int64": "Int64", "uint64": "UInt64", "float32": "Float", "float64": "Double", "string": "String"}[tag["dataType"]]
        if quality != "bad" and getattr(getattr(data.Value, "VariantType", None), "name", None) != expected:
            return sample(tag, quality="bad", native_status="opcua_source_type_mismatch", source_timestamp=data.SourceTimestamp, server_timestamp=data.ServerTimestamp, basis="protocol")
        return sample(tag, getattr(data.Value, "Value", None), quality, f"0x{code:08x}", data.SourceTimestamp, data.ServerTimestamp, "protocol")

    def datachange_notification(self, node, value, data):
        try:
            record = data.monitored_item.Value
            with self.lock:
                for tag in self.by_node.get(node.nodeid.to_string(), []):
                    self.cache[tag["tagId"]] = self._value(tag, record)
        except Exception:
            # Unknown callbacks cannot create new tags or qualify a source.
            return

    def status_change_notification(self, status):
        code = getattr(status, "Status", status)
        if hasattr(code, "is_bad") and code.is_bad():
            self.subscription_failed = True

    def _discover(self):
        from asyncua import ua
        discovered, seen, pending = [], set(), [(self.client.nodes.objects, 0)]
        deadline = time.monotonic() + 3
        try:
            while pending and len(seen) < 80 and len(discovered) < 20 and time.monotonic() < deadline:
                node, depth = pending.pop(0)
                identity = node.nodeid.to_string()
                if identity in seen or (node.nodeid.NamespaceIndex == 0 and node.nodeid.Identifier == ua.ObjectIds.Server):
                    continue
                seen.add(identity)
                kind = node.read_node_class()
                if kind == ua.NodeClass.Variable:
                    data_type = node.read_data_type_as_variant_type().name
                    index = node.nodeid.NamespaceIndex
                    discovered.append({"name": str(node.read_display_name().Text or identity)[:120], "nodeId": identity[:512], "namespaceUri": self.namespaces[index][:256] if index < len(self.namespaces) else "", "dataType": data_type[:32]})
                elif kind == ua.NodeClass.Object and depth < 6:
                    pending.extend((child, depth + 1) for child in node.get_children()[:80])
        except Exception:
            pass
        return discovered

    def collect(self):
        self.client.check_connection()
        if self.subscription_failed:
            raise ConnectionError("opcua_subscription_unavailable")
        with self.lock:
            tags = [dict(self.cache[tag["tagId"]]) for tag in self.connection["tags"]]
        result = {"tags": tags, "state": _state(tags), "discoveredTags": self.discovery}
        if self.fx is not None:
            result["fx"] = self.fx
            if not self.fx["modelVerified"]:
                result["state"] = "degraded"
                result["errorCode"] = "fx_information_model_not_observed"
            elif not tags:
                result["state"] = "connected"
        elif not tags:
            result["state"] = "connected"
        return result

    def close(self):
        try:
            if getattr(self, "subscription", None):
                self.subscription.delete()
        except Exception:
            pass
        try:
            if getattr(self, "client", None):
                self.client.disconnect()
        except Exception:
            pass


class MqttDriver(Driver):
    def connect(self):
        import paho.mqtt.client as mqtt
        from paho.mqtt.properties import Properties
        from paho.mqtt.packettypes import PacketTypes
        endpoint = urlparse(self.connection["endpoint"])
        host = self.commissioning.endpoint(self.connection)
        credentials = self.commissioning.credentials(self.connection)
        self.cache, self.timestamps = {}, {}
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.failed = False
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="sf-connectivity-" + uuid.uuid4().hex, protocol=mqtt.MQTTv5)
        if credentials.get("username"):
            self.client.username_pw_set(credentials["username"], credentials.get("password", ""))
        if endpoint.scheme == "mqtts":
            self.client.tls_set(ca_certs=credentials.get("caCertificate"), certfile=credentials.get("clientCertificate"), keyfile=credentials.get("clientPrivateKey"))
            host = endpoint.hostname  # Retain the commissioned TLS server name.
        topics = sorted({t["source"]["topic"] for t in self.connection["tags"]})
        self.topic_tags = {topic: [t for t in self.connection["tags"] if t["source"]["topic"] == topic] for topic in topics}
        self.pending_subscriptions = set()
        def connected(client, userdata, flags, code, properties):
            if getattr(code, "is_failure", False):
                self.failed = True
                self.ready.set()
                return
            for topic in topics:
                rc, mid = client.subscribe(topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=True))
                if rc:
                    self.failed = True
                self.pending_subscriptions.add(mid)
            if not topics:
                self.ready.set()
        def subscribed(client, userdata, mid, reasons, properties=None):
            if any(getattr(reason, "is_failure", False) for reason in reasons):
                self.failed = True
            self.pending_subscriptions.discard(mid)
            if not self.pending_subscriptions:
                self.ready.set()
        def disconnected(*args):
            self.failed = True
        self.client.on_connect, self.client.on_subscribe, self.client.on_disconnect = connected, subscribed, disconnected
        self.client.on_message = self._received
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = 0
        props.MaximumPacketSize = 17000  # bounded JSON body + literal topic/header
        self.client.connect(host, endpoint.port or (8883 if endpoint.scheme == "mqtts" else 1883), keepalive=30, properties=props)
        self.client.loop_start()
        if not self.ready.wait(5) or self.failed:
            raise ConnectionError("mqtt_subscription_unavailable")

    def _received(self, client, userdata, message):
        tags = self.topic_tags.get(message.topic, [])
        if not tags:
            return
        now = utc_now()
        try:
            if len(message.payload) > 16384:
                raise ValueError("mqtt_payload_too_large")
            payload = json.loads(message.payload.decode("utf-8", errors="strict"), object_pairs_hook=unique_fields, parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite_json")))
        except (ValueError, UnicodeDecodeError, RecursionError):
            with self.lock:
                for tag in tags:
                    self.cache[tag["tagId"]] = sample(tag, quality="bad", native_status="mqtt_payload_invalid", basis="payload")
                    self.timestamps[tag["tagId"]] = time.monotonic()
            return
        with self.lock:
            for tag in tags:
                try:
                    source = tag["source"]
                    value = json_pointer(payload, source["path"])
                    quality = json_pointer(payload, source["qualityPath"]) if "qualityPath" in source else "unknown"
                    if quality not in {"good", "bad", "uncertain", "unknown"}:
                        raise ValueError("mqtt_quality")
                    stamp = utc_timestamp(json_pointer(payload, source["timestampPath"])) if "timestampPath" in source else None
                    if message.retain:
                        quality = "uncertain" if quality != "bad" else "bad"
                    self.cache[tag["tagId"]] = sample(tag, value, quality, "mqtt_retained" if message.retain else "mqtt_message", stamp, now, "payload")
                except (ValueError, TypeError, KeyError, IndexError):
                    self.cache[tag["tagId"]] = sample(tag, quality="bad", native_status="mqtt_mapping_failed", basis="payload")
                self.timestamps[tag["tagId"]] = time.monotonic()

    def collect(self):
        if self.failed or not self.client.is_connected():
            raise ConnectionError("mqtt_connection_lost")
        now = time.monotonic()
        with self.lock:
            result = []
            for tag in self.connection["tags"]:
                cached = self.cache.get(tag["tagId"])
                if cached is None:
                    result.append(sample(tag, native_status="awaiting_message", basis="payload"))
                elif now - self.timestamps[tag["tagId"]] > max(30, 3 * self.connection["intervalMs"] / 1000):
                    result.append(sample(tag, quality="bad", native_status="mqtt_source_stale", basis="payload"))
                else:
                    result.append(dict(cached))
        return {"tags": result, "state": _state(result) if self.cache else "waiting"}

    def close(self):
        if getattr(self, "client", None):
            self.client.disconnect()
            self.client.loop_stop()


class UadpDriver(Driver):
    def connect(self):
        host = self.commissioning.endpoint(self.connection)
        endpoint = urlparse(self.connection["endpoint"])
        address = ipaddress.ip_address(host)
        if address.version != 4:
            raise ValueError("uadp_ipv4_required")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        self.socket.bind(("" if address.is_multicast else host, endpoint.port))
        if address.is_multicast:
            interface = self.connection["fx"].get("interfaceAddress", "0.0.0.0")
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(host) + socket.inet_aton(interface))
        self.socket.settimeout(0.25)
        self.lock, self.stop = threading.Lock(), threading.Event()
        self.last_packet = None
        self.last_received = 0
        self.sequence = None
        self.received = self.dropped = 0
        self.worker = threading.Thread(target=self._receive, daemon=True)
        self.worker.start()

    def _receive(self):
        window, packets = time.monotonic(), 0
        while not self.stop.is_set():
            try:
                data, address = self.socket.recvfrom(8193)
                now = time.monotonic()
                if now - window >= 1:
                    window, packets = now, 0
                packets += 1
                if packets > 500:
                    raise ValueError("uadp_rate_limit")
                packet = decode_uadp(data, self.connection)
                with self.lock:
                    sequence = packet["sequence"]
                    if self.sequence is not None and sequence is not None and now - self.last_received < 30:
                        distance = (sequence - self.sequence) & 0xFFFF
                        if distance == 0 or distance >= 32768:
                            self.dropped += 1
                            continue
                    self.sequence, self.last_packet, self.last_received = sequence, packet, now
                    self.received += 1
            except socket.timeout:
                continue
            except (OSError, ValueError, UnicodeDecodeError, struct.error):
                if self.stop.is_set():
                    break
                with self.lock:
                    self.dropped += 1

    def collect(self):
        with self.lock:
            packet = self.last_packet
            stale = packet is not None and time.monotonic() - self.last_received > max(5, 3 * self.connection["intervalMs"] / 1000)
            tags = [sample(t, quality="bad" if stale else "unknown", native_status="uadp_source_stale" if stale else "awaiting_uadp", basis="unauthenticated_udp") for t in self.connection["tags"]] if packet is None or stale else packet["tags"]
            fx = {"mode": "udp_uadp", "modelVerified": False, "components": [], "warnings": ["unauthenticated_uadp_observer", "hardware_qualification_required"], "determinism": "unverified", "publisherId": self.connection["fx"]["publisherId"], "writerGroupId": self.connection["fx"]["writerGroupId"], "dataSetWriterId": self.connection["fx"]["dataSetWriterId"], "receivedMessages": min(self.received, 9007199254740991), "droppedMessages": min(self.dropped, 9007199254740991)}
            if self.sequence is not None:
                fx["lastSequenceNumber"] = self.sequence
            if packet and not packet["metadataVerified"]:
                fx["warnings"].append("metadata_version_not_transmitted")
            return {"tags": tags, "state": "degraded" if stale else "waiting" if packet is None else _state(tags), "fx": fx}

    def close(self):
        if getattr(self, "stop", None):
            self.stop.set()
        if getattr(self, "socket", None):
            self.socket.close()
        if getattr(self, "worker", None):
            self.worker.join(timeout=1)


def create_driver(connection, commissioning, serial_lock=None):
    if connection["protocol"] == "opcua_fx" and connection["fx"]["mode"] == "udp_uadp":
        kind = UadpDriver
    else:
        kind = {"opcua": OpcUaDriver, "opcua_fx": OpcUaDriver, "modbus_tcp": ModbusDriver, "modbus_rtu": ModbusDriver, "mqtt": MqttDriver}[connection["protocol"]]
    return kind(connection, commissioning, serial_lock)
