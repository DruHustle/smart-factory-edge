import json
import math
import os
import signal
import sys
import time
import uuid
from itertools import count
from datetime import datetime, timezone
from threading import Event, Lock, Thread

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from dotenv import load_dotenv

from asset_adapters import (
    SerialJsonConnectionManager,
    load_asset_connections,
    read_asset,
    save_asset_profile,
    serial_asset_uses_port,
)
from ada031_control import Ada031SerialCommandManager, validate_command as validate_ada031_command
from config import load_settings
from durable_state import DurableState, DurablePublisher
from telemetry_policy import TelemetryPolicy
from provenance import normalize_provenance, IDENTIFIER
from measurement_units import valid_identity, valid_units, valid_mapping
from mqtt_topics import encode_level, decode_level, topic as encoded_topic
from security_collector import SecurityCollector

try:
    import serial  # type: ignore
except Exception:  # pragma: no cover
    serial = None


STOP = Event()
INGRESS_FAILED = Event()
MAX_EPOCH_MILLISECONDS = 9_223_372_036_854_775_807
WROVER_CONTROL_PINS = {18}
GATEWAY_SESSION = str(uuid.uuid4())
OBSERVATION_SEQUENCE = count()
GPIO_COMMAND_FIELDS = {"schemaVersion", "action", "commandId", "targetDeviceId", "pin", "value", "holdMs", "expiresAt"}


def gateway_observation(mode="live", quality="good"):
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    result = {"mode": mode, "sessionId": GATEWAY_SESSION, "sequence": next(OBSERVATION_SEQUENCE), "sourceQuality": quality, "sourceConnection": "online", "timestampOrigin": "gateway", "retained": False, "gatewayReceivedAt": now}
    if mode == "simulated": result["scenarioRunId"] = "gateway-synthetic:" + GATEWAY_SESSION
    return normalize_provenance(result)

def source_observation(value):
    return isinstance(value,dict) and set(value)=={"schemaVersion","mode","sourceQuality","sessionId","sequence"} and type(value.get("schemaVersion")) is int and value["schemaVersion"]==1 and value.get("mode")=="live" and value.get("sourceQuality") in {"good","bad"} and isinstance(value.get("sessionId"),str) and IDENTIFIER.fullmatch(value["sessionId"]) is not None and isinstance(value.get("sequence"),int) and not isinstance(value["sequence"],bool) and 0<=value["sequence"]<=9007199254740991

def _finite_number(value) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, TypeError, ValueError):
        return False


def telemetry_topic(site_id: str, line_id: str, device_id: str) -> str:
    return encoded_topic(site_id,line_id,device_id,"telemetry")


def heartbeat_topic(site_id: str, line_id: str, device_id: str) -> str:
    return encoded_topic(site_id,line_id,device_id,"heartbeat")


def command_topic(site_id: str, line_id: str, device_id: str) -> str:
    return encoded_topic(site_id,line_id,device_id,"commands")


def _valid_local_payload(topic: str, payload: bytes, settings) -> bool:
    """Accept only bounded telemetry from configured sensor identities and topics."""
    if len(payload) > 16 * 1024:
        return False
    parts = topic.split("/")
    if len(parts) != 5 or parts[0] != "factory":
        return False
    try:
        site_id,line_id,device_id=(decode_level(p) for p in parts[1:4])
    except ValueError:
        return False
    kind=parts[4]
    if (site_id, line_id) != (settings.site_id, settings.line_id):
        return False
    if not all(valid_identity(value) for value in (site_id,line_id,device_id)) or kind not in {"telemetry", "heartbeat"} or device_id not in settings.local_mqtt_device_ids:
        return False
    try:
        record = json.loads(payload.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return False
    if not isinstance(record, dict) or record.get("deviceId") != device_id:
        return False
    timestamp = record.get("timestamp")
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or not 0 < timestamp <= MAX_EPOCH_MILLISECONDS:
        return False

    if kind == "heartbeat":
        return set(record) == {"deviceId", "timestamp", "status"} and record.get("status") in {"online", "offline"}

    allowed = {"deviceId", "timestamp", "temperature", "humidity", "vibration", "power", "pressure", "rpm", "sensorType", "sensorStatus", "assetId", "assetSignals", "integration", "units", "observation", "mapping"}
    if any(key not in allowed for key in record):
        return False
    if "units" in record and not valid_units(record["units"]): return False
    if "observation" in record and not source_observation(record["observation"]): return False
    if "mapping" in record and not valid_mapping(record["mapping"],record.get("units",{}),record): return False
    try:
        normalize_provenance(record.get("integration"))
    except ValueError:
        return False
    for key in {"temperature", "humidity", "vibration", "power", "pressure", "rpm"} & record.keys():
        value = record[key]
        if value is not None and not _finite_number(value):
            return False
        # Relative humidity is a percentage for both the DHT11 and generic
        # industrial telemetry contract. Values outside this physical range
        # indicate a corrupt sample and must not reach storage or the UI.
        if key == "humidity" and value is not None and not 0 <= float(value) <= 100:
            return False
    if "sensorType" in record and (not isinstance(record["sensorType"], str) or not 1 <= len(record["sensorType"]) <= 64):
        return False
    if "sensorStatus" in record and record["sensorStatus"] not in {"ok", "read_error"}:
        return False
    if record.get("sensorType") == "DHT11":
        # The connected WROVER firmware always declares whether the physical
        # read succeeded. Never present a partial "ok" sample or retain stale
        # numbers alongside an explicit read failure.
        if record.get("sensorStatus") == "ok":
            if record.get("temperature") is None or record.get("humidity") is None:
                return False
        elif record.get("sensorStatus") == "read_error":
            if record.get("temperature") is not None or record.get("humidity") is not None:
                return False
        else:
            return False
    if "assetId" in record and (not isinstance(record["assetId"], str) or not record["assetId"] or len(record["assetId"]) > 128):
        return False
    signals = record.get("assetSignals")
    if signals is not None:
        if not isinstance(signals, dict) or len(signals) > 32:
            return False
        for name, value in signals.items():
            if not isinstance(name, str) or not name or len(name) > 64:
                return False
            if not _finite_number(value):
                return False
    return True


class LocalDeliveryError(RuntimeError):
    """A valid local message has not yet reached durable gateway storage."""


def forward_local_message(settings, cloud_client, msg, policy=None, transport_verified=False) -> bool:
    """Forward a validated WROVER message without exposing the cloud broker to it."""
    if not _valid_local_payload(msg.topic, msg.payload, settings):
        collector=getattr(settings,"_security_collector",None)
        if collector:
            try:collector.rejected_identity(settings,msg.topic)
            except Exception:print("[SECURITY] identity observation unavailable; telemetry validation preserved")
        print("[LOCAL MQTT] rejected message (identity, topic, size, or payload validation failed)")
        return False
    payload = json.loads(msg.payload.decode("utf-8"))
    payload["transportRetained"] = bool(getattr(msg,"retain",False))
    payload["transportRetainVerified"] = bool(transport_verified)
    gateway_id = getattr(settings, "device_id", "")
    if gateway_id:
        payload["gatewayId"] = gateway_id
    observation=payload.pop("observation",None)
    if observation is not None and "integration" not in payload:
        metadata=gateway_observation("live",observation["sourceQuality"])
        metadata.update({"timestampOrigin":"device","sessionId":observation["sessionId"],"sequence":observation["sequence"]})
        payload["integration"]=metadata
    if isinstance(payload.get("integration"), dict):
        payload["integration"] = normalize_provenance(payload["integration"])
        # Preserve acquisition classification and observation time. Broker retained
        # state cannot be cleared by a publisher's payload assertion.
        payload["integration"]["retained"] = bool(getattr(msg, "retain", False) or payload["integration"].get("retained", False))
    forwarded_payload = json.dumps(payload, separators=(",", ":"))
    payload_bytes = len(forwarded_payload.encode("utf-8"))
    if policy is not None and not policy.should_forward(msg.topic, payload):
        policy.accepted(msg.topic, payload, payload_bytes, forwarded=False)
        policy.report_if_due()
        return True
    result = cloud_client.publish(encoded_topic(settings.site_id,settings.line_id,payload["deviceId"],msg.topic.split("/")[-1]), forwarded_payload, qos=1, retain=False)
    if getattr(result, "rc", 0) != getattr(mqtt, "MQTT_ERR_SUCCESS", 0):
        print(f"[LOCAL MQTT] upstream queue rejected message on {msg.topic}")
        raise LocalDeliveryError("Durable queue rejected local telemetry")
    if policy is not None:
        policy.accepted(msg.topic, payload, payload_bytes, forwarded=True)
        policy.report_if_due()
    print(f"[LOCAL MQTT] telemetry queued -> {msg.topic}")
    return True


def validate_gpio_command(settings, command: dict) -> dict:
    """Validate a short-lived, allowlisted WROVER GPIO pulse from the cloud broker."""
    allowed_devices = set(getattr(settings, "local_mqtt_control_device_ids", ()))
    if not isinstance(command, dict) or set(command) != GPIO_COMMAND_FIELDS:
        raise ValueError("GPIO command fields are invalid")
    if len(json.dumps(command, separators=(",", ":")).encode("utf-8")) > 512:
        raise ValueError("GPIO command exceeds the supported size")
    if command.get("schemaVersion") != 1 or command.get("action") != "set_gpio":
        raise ValueError("GPIO command action or schema is unsupported")
    command_id = command.get("commandId")
    target = command.get("targetDeviceId")
    if not isinstance(command_id, str) or not 8 <= len(command_id) <= 64 or not all((c.isascii() and c.isalnum()) or c in "-_." for c in command_id):
        raise ValueError("GPIO command ID is invalid")
    if not isinstance(target, str) or target not in allowed_devices or target not in set(settings.local_mqtt_device_ids):
        raise ValueError("Target WROVER is not enabled for control")
    pin = command.get("pin")
    value = command.get("value")
    hold_ms = command.get("holdMs")
    expires_at = command.get("expiresAt")
    if isinstance(pin, bool) or not isinstance(pin, int) or pin not in WROVER_CONTROL_PINS:
        raise ValueError("GPIO pin is not in the WROVER control allowlist")
    if isinstance(value, bool) or not isinstance(value, int) or value not in {0, 1}:
        raise ValueError("GPIO output value must be 0 or 1")
    if isinstance(hold_ms, bool) or not isinstance(hold_ms, int) or not 1 <= hold_ms <= 2000:
        raise ValueError("GPIO pulse duration must be from 1 to 2000 ms")
    now_ms = int(time.time() * 1000)
    if isinstance(expires_at, bool) or not isinstance(expires_at, int) or not now_ms < expires_at <= now_ms + 30000:
        raise ValueError("GPIO command has expired or has an invalid expiry")
    return {
        "schemaVersion": 1,
        "action": "set_gpio",
        "commandId": command_id,
        "pin": pin,
        "value": value,
        "holdMs": hold_ms,
        "expiresAt": expires_at,
    }


def relay_gpio_command(settings, local_client, command: dict) -> dict:
    """Relay a validated control pulse to one WROVER over the Pi-local broker."""
    local_command = validate_gpio_command(settings, command)
    target = command["targetDeviceId"]
    topic = command_topic(settings.site_id, settings.line_id, target)
    result = local_client.publish(topic, json.dumps(local_command), qos=1, retain=False)
    if getattr(result, "rc", 0) != getattr(mqtt, "MQTT_ERR_SUCCESS", 0):
        raise RuntimeError("Pi-local MQTT broker rejected the GPIO command")
    return {"commandId": command["commandId"], "targetDeviceId": target, "status": "queued"}


def forward_local_control_ack(settings, cloud_client, msg) -> bool:
    """Forward a WROVER acknowledgement to the gateway's scoped cloud ack topic."""
    parts = msg.topic.split("/")
    if len(parts)!=5 or parts[0]!="factory" or parts[4]!="control-ack":return False
    try: site,line,device_id=(decode_level(p) for p in parts[1:4])
    except ValueError:return False
    if [site,line]!=[settings.site_id,settings.line_id]:return False
    if device_id not in set(getattr(settings, "local_mqtt_control_device_ids", ())):
        return False
    if len(msg.payload) > 1024:
        return False
    try:
        ack = json.loads(msg.payload.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return False
    if not isinstance(ack, dict) or ack.get("deviceId") != device_id:
        return False
    command_id = ack.get("commandId")
    timestamp = ack.get("timestamp")
    if not isinstance(command_id, str) or not 8 <= len(command_id) <= 64 or not all((c.isascii() and c.isalnum()) or c in "-_." for c in command_id):
        return False
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or not 0 < timestamp <= MAX_EPOCH_MILLISECONDS:
        return False
    if ack.get("status") not in {"applied", "rejected", "duplicate_ignored"}:
        return False
    pin = ack.get("pin")
    if pin is not None and (isinstance(pin, bool) or not isinstance(pin, int) or pin not in WROVER_CONTROL_PINS):
        return False
    result = cloud_client.publish(
        encoded_topic(settings.site_id,settings.line_id,settings.device_id,"commands/ack"),
        json.dumps({**ack, "gatewayDeviceId": settings.device_id}),
        qos=1,
        retain=False,
    )
    if getattr(result, "rc", 0) != getattr(mqtt, "MQTT_ERR_SUCCESS", 0):
        raise LocalDeliveryError("Durable queue rejected local control acknowledgement")
    return True


def make_local_mqtt_client(settings, cloud_client):
    """Create the local-only WROVER ingress client; this client never has cloud credentials."""
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=settings.local_mqtt_client_id, protocol=mqtt.MQTTv5, manual_ack=True)
    client._transport_verified=False
    subscriptions=set()
    policy = TelemetryPolicy(getattr(settings, "telemetry_coalescing_enabled", True),
                             getattr(settings, "telemetry_snapshot_seconds", 60))
    if settings.local_mqtt_username:
        client.username_pw_set(settings.local_mqtt_username, settings.local_mqtt_password)
    if settings.local_mqtt_use_tls:
        client.tls_set(ca_certs=settings.local_mqtt_ca_cert or None)

    def on_connect(_client, _userdata, _flags, rc, _properties=None):
        _client._transport_verified=False
        subscriptions.clear()
        if rc == 0:
            patterns=[]
            for site,line in sorted({(encode_level(settings.site_id),encode_level(settings.line_id)),(settings.site_id,settings.line_id)}):
                patterns.extend((f"factory/{site}/{line}/+/telemetry",f"factory/{site}/{line}/+/heartbeat"))
            patterns.extend(encoded_topic(settings.site_id,settings.line_id,d,"control-ack") for d in getattr(settings,"local_mqtt_control_device_ids",()))
            for pattern in patterns:
                result,mid=_client.subscribe(pattern,options=mqtt.SubscribeOptions(qos=1,retainAsPublished=True))
                if result != mqtt.MQTT_ERR_SUCCESS:
                    INGRESS_FAILED.set();STOP.set();return
                subscriptions.add(mid)
            print("[LOCAL MQTT] connected; verifying retained-delivery subscriptions")
        else:
            print(f"[LOCAL MQTT] broker connection failed rc={rc}")

    def on_subscribe(_client,_userdata,mid,reason_codes,_properties=None):
        if any(getattr(code,"is_failure",False) for code in reason_codes):
            _client._transport_verified=False
            INGRESS_FAILED.set();STOP.set();return
        subscriptions.discard(mid)
        _client._transport_verified=not subscriptions

    def on_disconnect(_client,*_args):
        _client._transport_verified=False
        subscriptions.clear()

    def on_message(_client, _userdata, msg):
        try:
            if msg.topic.endswith("/control-ack"):
                if not forward_local_control_ack(settings, cloud_client, msg):
                    print("[LOCAL MQTT] rejected invalid WROVER control acknowledgement")
            else:
                forward_local_message(settings, cloud_client, msg, policy,transport_verified=_client._transport_verified)
            # Invalid payloads are permanent rejects. Valid messages are ACKed
            # only after durable queuing or intentional policy suppression.
            _client.ack(msg.mid, msg.qos)
        except Exception as error:
            print(f"[LOCAL MQTT] delivery failed ({type(error).__name__}); restarting without acknowledging")
            INGRESS_FAILED.set()
            STOP.set()

    client.on_connect = on_connect
    client.on_subscribe=on_subscribe
    client.on_disconnect=on_disconnect
    client.on_message = on_message
    return client


def connect_local_mqtt(client,settings):
    properties=Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval=0xFFFFFFFF
    client.connect(settings.local_mqtt_host,settings.local_mqtt_port,keepalive=60,clean_start=False,properties=properties)


def make_mqtt_client(settings, apply_configuration=None, relay_control=None, relay_ada031_control=None, apply_connectivity=None, probe_connectivity=None):
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=settings.mqtt_client_id, clean_session=False)
    if settings.mqtt_username:
        client.username_pw_set(settings.mqtt_username, settings.mqtt_password)
    if settings.mqtt_use_tls:
        client.tls_set(ca_certs=settings.mqtt_ca_cert or None)

    expected_topic = command_topic(settings.site_id, settings.line_id, settings.device_id)
    connectivity_topic = encoded_topic(settings.site_id, settings.line_id, settings.device_id, "commands/connectivity")

    def unique_fields(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Command contains duplicate fields")
            result[key] = value
        return result

    def on_connect(_client, _userdata, _flags, rc, _properties=None):
        if rc == 0:
            print("[MQTT] connected")
            _client.subscribe(expected_topic, qos=1)
            _client.subscribe(connectivity_topic, qos=1)
        else:
            print(f"[MQTT] connect failed rc={rc}")

    def on_message(_client, _userdata, msg):
        ack = {"deviceId": settings.device_id, "status": "rejected", "timestamp": int(time.time() * 1000)}
        try:
            if msg.topic not in {expected_topic, connectivity_topic}:
                raise ValueError("Command topic does not match this gateway")
            if len(msg.payload) > 64 * 1024:
                raise ValueError("Command payload exceeds the supported size")
            command = json.loads(msg.payload.decode("utf-8", errors="strict"), object_pairs_hook=unique_fields)
            if not isinstance(command, dict):
                raise ValueError("Command must be a JSON object")
            # Separate retained desired-state documents must never overwrite
            # each other. Probes and physical commands use the original route.
            if (command.get("action") == "replace_connectivity_configuration") != (msg.topic == connectivity_topic):
                raise ValueError("Command action does not match the authorized route")
            if command.get("action") == "replace_asset_configuration":
                profile = command.get("configuration")
                if not isinstance(profile, dict) or apply_configuration is None:
                    raise ValueError("Gateway configuration handler is unavailable")
                ack["configurationHash"] = apply_configuration(profile)
                ack["status"] = "applied"
            elif command.get("action") == "replace_connectivity_configuration":
                if set(command) != {"action", "configuration"} or apply_connectivity is None or not isinstance(command.get("configuration"), dict):
                    raise ValueError("Connectivity configuration handler is unavailable")
                ack.update(apply_connectivity(command["configuration"]))
                ack.update(status="applied", action="replace_connectivity_configuration")
            elif command.get("action") == "probe_connectivity":
                if getattr(msg, "retain", False) or probe_connectivity is None:
                    raise ValueError("Connectivity probes require a live non-retained request")
                ack.update(probe_connectivity(command))
                ack["action"] = "probe_connectivity"
            elif command.get("action") == "set_gpio":
                if getattr(msg, "retain", False):
                    raise ValueError("GPIO control messages must not be retained")
                if relay_control is None:
                    raise ValueError("WROVER GPIO control relay is unavailable")
                ack.update(relay_control(command))
                ack["status"] = "queued"
            elif command.get("action") in {"ada031_control", "jog", "set_profile", "neutral", "stop_program"}:
                if getattr(msg, "retain", False):
                    raise ValueError("ADA031 motion messages must not be retained")
                if relay_ada031_control is None:
                    raise ValueError("ADA031 control handler is unavailable")
                ack.update(relay_ada031_control(command))
                ack["status"] = ack.get("status", "serial_write_accepted")
            else:
                raise ValueError("Command action is not supported")
        except Exception as exc:
            ack["reason"] = str(exc)[:160]
            print(f"[MQTT] rejected edge configuration: {exc}")
        ack_topic = expected_topic + "/ack"
        _client.publish(ack_topic, json.dumps(ack), qos=1, retain=False)

    client.on_connect = on_connect
    client.on_message = on_message
    return client


def parse_serial_line(line: str) -> dict:
    if len(line.encode("utf-8")) > 16*1024:
        raise ValueError("invalid_telemetry:bytes")
    record = json.loads(line)
    if not isinstance(record,dict):
        raise ValueError("invalid_telemetry:object")
    if "timestamp" not in record:
        record["timestamp"] = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        if isinstance(record.get("integration"), dict):
            record["integration"]["timestampOrigin"] = "gateway"
            record["integration"].setdefault("gatewayReceivedAt", datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"))
    if "integration" in record:
        record["integration"] = normalize_provenance(record["integration"])
    return record


def serial_reader(settings, publish_fn, asset_connections=None, configuration_lock=None, serial_io_lock=None):
    """Read a standalone serial sensor unless an AAS asset owns that same device."""
    if not settings.serial_enabled:
        return
    if serial is None:
        print("[SERIAL] pyserial unavailable; serial ingest disabled")
        return

    def current_assets():
        if asset_connections is None:
            return []
        if configuration_lock is None:
            return list(asset_connections)
        with configuration_lock:
            return list(asset_connections)

    while not STOP.is_set():
        if serial_asset_uses_port(current_assets(), settings.serial_port):
            STOP.wait(1)
            continue

        ser = None
        ownership_acquired = False
        try:
            if serial_io_lock is not None:
                serial_io_lock.acquire()
                ownership_acquired = True
                # The desired profile may have changed while this worker waited.
                if serial_asset_uses_port(current_assets(), settings.serial_port):
                    continue
            ser = serial.Serial(settings.serial_port, settings.serial_baud, timeout=1)
            print(f"[SERIAL] listening on {settings.serial_port} @ {settings.serial_baud}")
            while not STOP.is_set() and not serial_asset_uses_port(current_assets(), settings.serial_port):
                try:
                    raw = ser.readline().decode("utf-8", errors="strict").strip()
                    if raw:
                        publish_fn(parse_serial_line(raw))
                except Exception as exc:
                    print(f"[SERIAL] parse error: {exc}")
        except Exception as exc:
            print(f"[SERIAL] failed to open/read {settings.serial_port}: {exc}")
            # USB enumeration is asynchronous at boot and stable /dev/serial/by-id
            # paths can temporarily disappear after reconnects. Keep the worker
            # alive so systemd does not need to restart the whole gateway.
        finally:
            if ser is not None:
                ser.close()
            if ownership_acquired:
                serial_io_lock.release()

        # The profile may have released the serial device; retry after a short pause.
        STOP.wait(1)


def asset_poll_loop(
    settings,
    asset_connections,
    publish_fn,
    configuration_lock=None,
    serial_connection_manager=None,
    ada031_connection_manager=None,
    serial_io_lock=None,
):
    manager = serial_connection_manager or SerialJsonConnectionManager()
    try:
        while not STOP.is_set():
            if configuration_lock is None:
                current_assets = list(asset_connections)
            else:
                with configuration_lock:
                    current_assets = list(asset_connections)
            active_serial_ids = {
                asset["assetId"] for asset in current_assets
                if str(asset.get("protocol", "")).lower() == "serial"
            }
            manager.close_unused(active_serial_ids)
            allowed_ada031_ids = set(getattr(settings, "ada031_control_asset_ids", ()))
            active_ada031_ids = {
                asset["assetId"] for asset in current_assets
                if str(asset.get("protocol", "")).lower() == "ada031_v4_serial"
                and asset["assetId"] in allowed_ada031_ids
            }
            if ada031_connection_manager is not None:
                ada031_connection_manager.close_unused(active_ada031_ids)

            for asset in current_assets:
                try:
                    protocol = str(asset.get("protocol", "")).lower()
                    if protocol == "ada031_v4_serial":
                        # Opening an Uno serial port can reset the controller and
                        # move it to its startup pose. Only commissioned, explicitly
                        # allowlisted arms are opened for control and telemetry.
                        if asset["assetId"] not in allowed_ada031_ids or ada031_connection_manager is None:
                            continue
                        if serial_io_lock is None:
                            values = ada031_connection_manager.read_telemetry(asset)
                        else:
                            with serial_io_lock:
                                values = ada031_connection_manager.read_telemetry(asset)
                    else:
                        if serial_io_lock is None or protocol not in {"serial", "modbus_rtu"}:
                            values = read_asset(asset, manager)
                        else:
                            with serial_io_lock:
                                values = read_asset(asset, manager)
                    if not values:
                        continue
                    publish_fn({
                        "assetId": asset["assetId"],
                        **values,
                        "integration": gateway_observation(),
                        "timestamp": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                    })
                except Exception as exc:
                    print(f"[ASSET] {asset.get('assetId', '<unknown>')} poll failed: {exc}")
            STOP.wait(settings.asset_poll_interval_seconds)
    finally:
        manager.close()

def heartbeat_loop(settings, client):
    while not STOP.is_set():
        payload = {
            "deviceId": settings.device_id,
            "timestamp": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
            "status": "online",
        }
        # Current liveness is ephemeral: do not spool old heartbeats for replay.
        if client.is_connected():
            client.publish(heartbeat_topic(settings.site_id, settings.line_id, settings.device_id), json.dumps(payload, separators=(",", ":")), qos=1)
        STOP.wait(getattr(settings, "heartbeat_interval_seconds", 60))


def main():
    load_dotenv()
    settings = load_settings()
    try:
        asset_connections = load_asset_connections(settings.asset_config_file, settings.device_id)
    except Exception as exc:
        print(f"[ASSET] configuration error: {exc}")
        asset_connections = []

    configuration_lock = Lock()
    serial_io_lock = Lock()
    state_dir = getattr(settings, "state_dir", "")
    state = DurableState(str(__import__('pathlib').Path(state_dir) / "delivery.sqlite3"), getattr(settings, "max_queued_messages", 50000)) if state_dir else None
    security_collector=SecurityCollector.from_environment(state)
    settings._security_collector=security_collector
    ada031_commands = Ada031SerialCommandManager(command_state=state)
    connectivity_runtime = None

    def apply_configuration(profile):
        if connectivity_runtime:
            from asset_adapters import validate_asset_profile
            from connectivity_schema import endpoint_resource
            from urllib.parse import urlparse
            assets = validate_asset_profile(profile, settings.device_id)
            serial_paths = {os.path.realpath(urlparse(a["endpoint"]).path) for a in assets if a["protocol"] in {"serial", "modbus_rtu", "ada031_v4_serial"}}
            if serial_paths & connectivity_runtime.serial_paths():
                raise ValueError("Serial port is owned by industrial connectivity")
            with connectivity_runtime.lock:
                endpoints = {endpoint_resource(c["endpoint"]) for c in connectivity_runtime.configuration["connections"] if c["enabled"] and c["protocol"] in {"opcua", "opcua_fx", "modbus_tcp"}}
            if {endpoint_resource(a["endpoint"]) for a in assets if a["protocol"] in {"opcua", "modbus_tcp"}} & endpoints:
                raise ValueError("Endpoint is owned by industrial connectivity")
        digest = save_asset_profile(settings.asset_config_file, profile, settings.device_id)
        with configuration_lock:
            asset_connections[:] = load_asset_connections(settings.asset_config_file, settings.device_id)
            ada031_commands.close_unused({
                asset["assetId"] for asset in asset_connections
                if asset.get("protocol") == "ada031_v4_serial"
                and asset["assetId"] in set(settings.ada031_control_asset_ids)
            })
        if security_collector:
            try:security_collector.configuration_applied(digest)
            except Exception:print("[SECURITY] configuration observation unavailable; applied configuration preserved")
        return digest

    local_enabled = getattr(settings, "local_mqtt_enabled", False)
    if local_enabled:
        if not settings.local_mqtt_device_ids:
            raise RuntimeError("LOCAL_MQTT_ENABLED requires LOCAL_MQTT_DEVICE_IDS allowlist entries")
        if not settings.local_mqtt_username or not settings.local_mqtt_password:
            raise RuntimeError("LOCAL_MQTT_ENABLED requires a dedicated local broker username and password")

    local_client = None

    def relay_control(command):
        if local_client is None:
            raise RuntimeError("Pi-local MQTT broker is unavailable")
        return relay_gpio_command(settings, local_client, command)

    def relay_ada031_control(command):
        with configuration_lock:
            configured_assets = list(asset_connections)
            normalized, asset, command_byte = validate_ada031_command(settings, command, configured_assets)
        with serial_io_lock:
            return ada031_commands.execute(asset, normalized, command_byte)

    def apply_connectivity(configuration):
        if connectivity_runtime is None:
            raise ValueError("Connectivity runtime is unavailable")
        return connectivity_runtime.apply(configuration)

    def probe_connectivity(command):
        if connectivity_runtime is None:
            raise ValueError("Connectivity runtime is unavailable")
        return connectivity_runtime.queue_probe(command)

    client = make_mqtt_client(settings, apply_configuration, relay_control, relay_ada031_control, apply_connectivity, probe_connectivity)
    publisher = DurablePublisher(client, state) if state else client
    if security_collector:
        Thread(target=security_collector.run,args=(STOP,client),daemon=True).start()

    def publish_telemetry(data: dict):
        payload = {
            "deviceId": settings.device_id,
            "gatewayId": settings.device_id,
            "timestamp": data.get("timestamp", int(time.time() * 1000)),
            "temperature": data.get("temperature"),
            "humidity": data.get("humidity"),
            "vibration": data.get("vibration"),
            "power": data.get("power"),
            "pressure": data.get("pressure"),
            "rpm": data.get("rpm"),
        }
        if data.get("units") is not None:
            if not valid_units(data["units"]): raise ValueError("Invalid engineering units")
            payload["units"]=data["units"]
        if isinstance(data.get("mapping"),dict): payload["mapping"]=data["mapping"]
        if isinstance(data.get("integration"),dict): payload["integration"]=normalize_provenance(data["integration"])
        if data.get("assetId"):
            payload["assetId"] = data["assetId"]
        if isinstance(data.get("assetSignals"), dict):
            payload["assetSignals"] = data["assetSignals"]
        if isinstance(data.get("connectivity"), dict):
            payload["connectivity"] = data["connectivity"]
        topic = telemetry_topic(settings.site_id, settings.line_id, settings.device_id)
        result = publisher.publish(topic, json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False), qos=1)
        if getattr(result, "rc", 0) != 0:
            if "connectivity" in payload:
                raise RuntimeError("connectivity_delivery_unavailable")
            print("[DELIVERY] Telemetry could not be queued; investigate gateway storage/uplink")
        else:
            print(f"[MQTT] telemetry queued -> {topic}")

    def legacy_serial_paths():
        from urllib.parse import urlparse
        with configuration_lock:
            paths = {os.path.realpath(urlparse(a["endpoint"]).path) for a in asset_connections if a["protocol"] in {"serial", "modbus_rtu", "ada031_v4_serial"}}
        if settings.serial_enabled:
            paths.add(os.path.realpath(settings.serial_port))
        return paths

    def legacy_connections():
        with configuration_lock:
            return {a["endpoint"] for a in asset_connections if a["protocol"] in {"opcua", "modbus_tcp"}}

    try:
        from industrial_connectivity import ConnectivityRuntime
        from pathlib import Path
        connectivity_runtime = ConnectivityRuntime(
            os.getenv("CONNECTIVITY_CONFIG_FILE", str(Path(settings.asset_config_file).with_name("connectivity.json"))),
            settings.device_id, publish_telemetry, STOP, serial_io_lock,
            legacy_serial_paths, state, legacy_connections=legacy_connections,
        )
    except Exception:
        print("[CONNECTIVITY] local configuration unavailable; legacy acquisition preserved")

    def sig_handler(_sig, _frame):
        STOP.set()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    getattr(client, "connect_async", client.connect)(settings.mqtt_host, settings.mqtt_port, keepalive=60)
    client.loop_start()
    delivery_thread = None
    if state:
        delivery_thread = Thread(target=publisher.run, args=(STOP,), daemon=True)
        delivery_thread.start()

    if local_enabled:
        local_client = make_local_mqtt_client(settings, publisher)
        connect_local_mqtt(local_client,settings)
        local_client.loop_start()

    serial_thread = Thread(
        target=serial_reader,
        args=(settings, publish_telemetry, asset_connections, configuration_lock, serial_io_lock),
        daemon=True,
    )
    serial_thread.start()
    asset_serial_reader = SerialJsonConnectionManager()
    asset_thread = Thread(
        target=asset_poll_loop,
        args=(settings, asset_connections, publish_telemetry, configuration_lock, asset_serial_reader, ada031_commands, serial_io_lock),
        daemon=True,
    )
    asset_thread.start()

    connectivity_thread = None
    if connectivity_runtime is not None:
        connectivity_thread = Thread(target=connectivity_runtime.run, daemon=True)
        connectivity_thread.start()

    hb_thread = Thread(target=heartbeat_loop, args=(settings, client), daemon=True)
    hb_thread.start()

    print(f"[EDGE] gateway started; configured assets: {len(asset_connections)}; telemetry coalescing: {getattr(settings, 'telemetry_coalescing_enabled', True)}; snapshot/heartbeat seconds: {getattr(settings, 'telemetry_snapshot_seconds', 60)}/{getattr(settings, 'heartbeat_interval_seconds', 60)}")
    while not STOP.is_set():
        if settings.synthetic_telemetry_enabled and not settings.serial_enabled and not asset_connections:
            synthetic = {
                "temperature": 24.5,
                "units": {"temperature":"Cel","humidity":"%","vibration":"mm/s","power":"W","pressure":"bar","rpm":"1/min"},
                "integration": gateway_observation("simulated"),
                "humidity": 55.0,
                "vibration": 0.15,
                "power": 120.0,
                "pressure": 1.2,
                "rpm": 1450,
                "timestamp": int(time.time() * 1000),
            }
            publish_telemetry(synthetic)
        STOP.wait(settings.publish_interval_seconds)

    if local_client is not None:
        local_client.loop_stop()
        local_client.disconnect()
    # Stop callbacks before closing controller and database state. Producers may
    # still be finishing a bounded serial/protocol read when SIGTERM arrives.
    client.loop_stop()
    client.disconnect()
    workers = [serial_thread, asset_thread, hb_thread]
    if connectivity_thread is not None:
        workers.append(connectivity_thread)
    if delivery_thread is not None:
        workers.append(delivery_thread)
    for worker in workers:
        join = getattr(worker, "join", None)
        if join:
            join(timeout=10)
    with configuration_lock:
        ada031_commands.close()
    if state and not any(getattr(worker, "is_alive", lambda: False)() for worker in workers):
        state.close()
    print("[EDGE] gateway stopped")
    if INGRESS_FAILED.is_set():
        raise LocalDeliveryError("Local ingress stopped before durable acceptance; supervisor must retry")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
