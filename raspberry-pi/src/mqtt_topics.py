"""mqtt-topic-v1: reversible transport encoding; canonical payload IDs never change."""
from measurement_units import valid_identity


def encode_level(identity: str) -> str:
    if not valid_identity(identity):
        raise ValueError("Invalid canonical MQTT identity")
    return identity.replace(".", "%2E")


def decode_level(level: str) -> str:
    identity = level.replace("%2E", ".").replace("%2e", ".")
    if not valid_identity(identity):
        raise ValueError("Invalid MQTT topic identity")
    return identity


def topic(site: str, line: str, device: str, kind: str) -> str:
    if kind not in {"telemetry", "heartbeat", "commands", "commands/connectivity", "control-ack", "commands/ack"}:
        raise ValueError("Unsupported MQTT topic kind")
    value = "/".join(("factory", encode_level(site), encode_level(line), encode_level(device), kind))
    if len(value.encode("ascii")) > 255:
        raise ValueError("Encoded MQTT topic exceeds the broker's 255-byte route bound")
    return value
