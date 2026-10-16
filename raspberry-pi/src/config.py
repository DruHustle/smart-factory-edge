from dataclasses import dataclass
import os


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Settings:
    device_id: str
    site_id: str
    line_id: str

    mqtt_host: str
    mqtt_port: int
    mqtt_username: str
    mqtt_password: str
    mqtt_use_tls: bool
    mqtt_ca_cert: str
    mqtt_client_id: str

    serial_enabled: bool
    serial_port: str
    serial_baud: int

    asset_config_file: str
    synthetic_telemetry_enabled: bool
    asset_poll_interval_seconds: float

    publish_interval_seconds: int

    # Local broker ingress for WROVER-class sensors; cloud broker credentials
    # above are reserved for this gateway process only.
    local_mqtt_enabled: bool = False
    local_mqtt_host: str = "127.0.0.1"
    local_mqtt_port: int = 1883
    local_mqtt_username: str = ""
    local_mqtt_password: str = ""
    local_mqtt_use_tls: bool = False
    local_mqtt_ca_cert: str = ""
    local_mqtt_client_id: str = "pi-edge-ingress"
    local_mqtt_device_ids: tuple[str, ...] = ()
    # Only explicitly named WROVER devices may receive bounded GPIO pulses.
    local_mqtt_control_device_ids: tuple[str, ...] = ()
    # ADA031 is disabled unless its exact AAS asset ID is listed here.
    ada031_control_asset_ids: tuple[str, ...] = ()
    environment: str = "development"
    state_dir: str = ""
    max_queued_messages: int = 50000
    telemetry_coalescing_enabled: bool = True
    telemetry_snapshot_seconds: int = 60
    heartbeat_interval_seconds: int = 60


def load_settings() -> Settings:
    settings = Settings(
        device_id=os.getenv("DEVICE_ID", "pi-edge-01"),
        site_id=os.getenv("SITE_ID", "factory-a"),
        line_id=os.getenv("LINE_ID", "line-1"),
        mqtt_host=os.getenv("MQTT_HOST", "127.0.0.1"),
        mqtt_port=int(os.getenv("MQTT_PORT", "8883")),
        mqtt_username=os.getenv("MQTT_USERNAME", ""),
        mqtt_password=os.getenv("MQTT_PASSWORD", ""),
        mqtt_use_tls=_as_bool(os.getenv("MQTT_USE_TLS"), True),
        mqtt_ca_cert=os.getenv("MQTT_CA_CERT", ""),
        mqtt_client_id=os.getenv("MQTT_CLIENT_ID", "pi-edge-01"),
        local_mqtt_enabled=_as_bool(os.getenv("LOCAL_MQTT_ENABLED"), False),
        local_mqtt_host=os.getenv("LOCAL_MQTT_HOST", "127.0.0.1"),
        local_mqtt_port=int(os.getenv("LOCAL_MQTT_PORT", "1883")),
        local_mqtt_username=os.getenv("LOCAL_MQTT_USERNAME", ""),
        local_mqtt_password=os.getenv("LOCAL_MQTT_PASSWORD", ""),
        local_mqtt_use_tls=_as_bool(os.getenv("LOCAL_MQTT_USE_TLS"), False),
        local_mqtt_ca_cert=os.getenv("LOCAL_MQTT_CA_CERT", ""),
        local_mqtt_client_id=os.getenv("LOCAL_MQTT_CLIENT_ID", "pi-edge-ingress"),
        local_mqtt_device_ids=tuple(
            device_id.strip()
            for device_id in os.getenv("LOCAL_MQTT_DEVICE_IDS", "").split(",")
            if device_id.strip()
        ),
        local_mqtt_control_device_ids=tuple(
            device_id.strip()
            for device_id in os.getenv("LOCAL_MQTT_CONTROL_DEVICE_IDS", "").split(",")
            if device_id.strip()
        ),
        ada031_control_asset_ids=tuple(
            asset_id.strip()
            for asset_id in os.getenv("ADA031_CONTROL_ASSET_IDS", "").split(",")
            if asset_id.strip()
        ),
        environment=os.getenv("EDGE_ENV", "development").strip().lower(),
        state_dir=os.getenv("EDGE_STATE_DIR", "/var/lib/smart-factory-iot" if os.getenv("EDGE_ENV") == "production" else ""),
        max_queued_messages=int(os.getenv("MAX_QUEUED_MESSAGES", "50000")),
        telemetry_coalescing_enabled=_as_bool(os.getenv("TELEMETRY_COALESCING_ENABLED"), True),
        telemetry_snapshot_seconds=int(os.getenv("TELEMETRY_SNAPSHOT_SECONDS", "60")),
        heartbeat_interval_seconds=int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "60")),
        # Generic line-delimited serial ingest is opt-in. AAS serial profiles
        # have their own exclusive owner and must never race a default tty path.
        serial_enabled=_as_bool(os.getenv("SERIAL_ENABLED"), False),
        serial_port=os.getenv("SERIAL_PORT", "/dev/ttyUSB0"),
        serial_baud=int(os.getenv("SERIAL_BAUD", "115200")),
        asset_config_file=os.getenv("ASSET_CONFIG_FILE", "/var/lib/smart-factory-iot/assets.json"),
        synthetic_telemetry_enabled=_as_bool(os.getenv("SYNTHETIC_TELEMETRY_ENABLED"), False),
        asset_poll_interval_seconds=float(os.getenv("ASSET_POLL_INTERVAL_SECONDS", "0.5")),
        publish_interval_seconds=int(os.getenv("PUBLISH_INTERVAL_SECONDS", "5")),
    )
    import re
    if any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value) for value in
           (settings.device_id, settings.site_id, settings.line_id, *settings.local_mqtt_device_ids)):
        raise RuntimeError("MQTT identities must be safe topic segments")
    if settings.publish_interval_seconds < 1 or settings.asset_poll_interval_seconds <= 0 or settings.max_queued_messages < 1:
        raise RuntimeError("Polling intervals and delivery queue capacity must be positive")
    # The application considers devices offline after 120 seconds. Leave room
    # for network delay and a missed publish without changing that contract.
    if not 1 <= settings.telemetry_snapshot_seconds <= 60 or not 1 <= settings.heartbeat_interval_seconds <= 60:
        raise RuntimeError("Telemetry snapshots and heartbeats must be between 1 and 60 seconds")
    if settings.environment == "production":
        if not settings.state_dir or settings.synthetic_telemetry_enabled:
            raise RuntimeError("Production requires durable EDGE_STATE_DIR and disables synthetic telemetry")
        if settings.local_mqtt_enabled and not settings.local_mqtt_use_tls and settings.local_mqtt_host not in {"localhost", "127.0.0.1", "::1"}:
            raise RuntimeError("Plaintext local MQTT ingress must remain on gateway loopback")
        if not settings.mqtt_use_tls or not settings.mqtt_username or not settings.mqtt_password:
            raise RuntimeError("Production CloudAMQP uplink requires TLS and dedicated username/password credentials")
        if settings.local_mqtt_enabled and (not settings.local_mqtt_username or not settings.local_mqtt_password or not settings.local_mqtt_device_ids):
            raise RuntimeError("Production local MQTT ingress requires credentials and a non-empty device ID allowlist")
        if not set(settings.local_mqtt_control_device_ids).issubset(set(settings.local_mqtt_device_ids)):
            raise RuntimeError("GPIO control device IDs must also be in LOCAL_MQTT_DEVICE_IDS")
        if settings.local_mqtt_control_device_ids and not settings.local_mqtt_enabled:
            raise RuntimeError("WROVER GPIO control requires LOCAL_MQTT_ENABLED=true")
        if any(len(asset_id) > 128 or any(char in asset_id for char in "\r\n+#") for asset_id in settings.ada031_control_asset_ids):
            raise RuntimeError("ADA031_CONTROL_ASSET_IDS contains an invalid AAS asset identifier")
    return settings
