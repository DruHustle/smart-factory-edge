# Raspberry Pi Edge Gateway

The Pi is the plant edge gateway: it polls industrial machines over OPC UA, Modbus TCP/RTU, and serial JSON, exchanges validated control and controller telemetry with commissioned ADA031 V4 arms over 9600-baud USB serial, and hosts a local Mosquitto broker for ESP32 sensors. The Python process validates local sensor identity and payloads, then publishes telemetry to CloudAMQP over TLS. Eligible native MQTT devices may publish directly to CloudAMQP with their own scoped credentials and topic ACLs. The ADA031 V4 uses USB serial and must use the Pi gateway. Backend services use separate scoped identities. See the [ADA031 V4 guide](../docs/ada031-integration.md) for the command map and limits.

Gateway dependencies and their resolved transitive versions are pinned in `requirements.txt`; `requirements.in` records the direct dependencies. Edge CI and the coordinated release run `pip-audit==2.10.1` with `--strict` and fail on known vulnerabilities. Review dependency updates together, rerun the audit and gateway tests, and verify wheel availability on the commissioned Pi architecture. The application audit does not scan the installed Pi OS.

## Industrial device connectivity

The Industrial Devices workflow adds independent typed tag connections alongside
the existing AAS profile: actual OPC UA monitored subscriptions, Modbus TCP/RTU,
MQTT JSON, OPC UA FX information-model diagnostics, commissioned UADP keyframe
observation and read-only TSN evidence. It uses its own revisioned configuration
and retained `factory/{site}/{line}/{gateway}/commands/connectivity` route.
The original `/commands` route carries the retained legacy profile and non-retained probes.
Configure the local `CONNECTIVITY_*` allowlists and credential references before
enabling a production connection. See the [connectivity guide](../docs/industrial-connectivity.md)
for supported data types, source addresses, secure credentials, commissioning,
reporting limits, exact FX/TSN scope and reproducible real protocol tests.

## Relationship to the cloud deployment

The React UI runs on Vercel. One reviewed, non-root backend image runs as separate API and singleton worker containers on the Oracle VM alongside BaSyx. The API role runs the Node API plus Device, Identity and Analytics services; the worker role runs Telemetry and Notification services. Managed databases, Redis and CloudAMQP remain external. The Raspberry Pi stays in the factory and runs the systemd service below. ESP32 WROVER firmware is built and uploaded over USB with PlatformIO; the ADA031 V4 controller uses its USB/Arduino workflow. The Pi publishes upstream to CloudAMQP MQTT over TLS using its own gateway identity. See the [Oracle deployment guide](https://github.com/DruHustle/smart-factory-frontend/blob/main/deploy/oracle/README.md) for production and the [deployment index](../README.md) for edge setup. The [Vercel/Render guide](https://github.com/DruHustle/smart-factory-frontend/blob/main/RENDER_DEPLOYMENT.md) remains available for local development and historical rollback.

## Manual install

Use Raspberry Pi OS 64-bit with Python 3.11, the tested runtime. Revalidate dependencies and gateway tests before selecting a newer Python version. From this directory:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
# Configure broker identity, certificates, and equipment mappings.
chmod 600 .env
.venv/bin/python src/sensor_gateway.py
```

The Pi CloudAMQP uplink uses MQTT TLS on port 8883. Paho validates the broker certificate and hostname with the OS CA store; set `MQTT_CA_CERT` for a protected private CA. WROVER devices use the Pi local Mosquitto TLS listener on port 8883; its separate loopback listener on port 1883 is only for Python. Never turn off verification or use plaintext MQTT on a plant/public network.

## Telemetry efficiency and recovery

`TELEMETRY_COALESCING_ENABLED=true` coalesces identical live WROVER readings on the Pi. Every measurement, sensor-status, identity/metadata or button-signal change is forwarded immediately. The original full payload is sent again on the first sample at or after `TELEMETRY_SNAPSHOT_SECONDS` (default 60). Local five-second sampling and MQTT/TLS remain unchanged; these savings apply to the internet uplink. A restart sends a full snapshot. There is deliberately no numeric deadband: dashboard alarm thresholds are independently configurable, so even a small measurement change must reach the backend. If every sample timestamp is needed in cloud history, set coalescing to false.

An online sensor heartbeat is suppressed when recent forwarded telemetry or another heartbeat already covers its liveness interval. Offline status messages and historical telemetry are forwarded. Samples more than 15 seconds old, or more than five seconds in the future, bypass coalescing and never advance its live baseline. This preserves original timestamps during backlog replay. Baselines are separate per device/asset and advance only after the durable queue accepts a forwarded message. The cloud already ignores forwarded sensor heartbeats; current sensor telemetry establishes sensor liveness. Gateway heartbeats use `HEARTBEAT_INTERVAL_SECONDS` (default 60), are sent only while connected, and are never placed in the durable replay queue. Both configured intervals are limited to 60 seconds to leave headroom under the application's 120-second offline timeout.

Paho MQTT 2.1.0 uses the version 2 callback API. Local QoS 1 messages are manually acknowledged only after durable enqueue or intentional policy suppression; invalid messages are acknowledged and rejected to avoid poison-message loops. If valid data cannot be queued, ingress withholds acknowledgement and exits with failure after cleanup. The existing systemd `Restart=on-failure` policy reconnects the persistent subscription and permits broker redelivery. Control acknowledgements are never coalesced. The SQLite uplink queue still removes a message only after the cloud broker acknowledges it; backend ingestion remains responsible for persistence and duplicate handling. This is at-least-once delivery, not a guarantee against arbitrary disk failure, queue exhaustion, broker data loss, or sensor power loss before publishing.

Every minute, `[TELEMETRY POLICY]` logs input/forwarded/suppressed message counts and the corresponding MQTT **payload** bytes (after adding the gateway identity). These counters exclude MQTT/TCP/TLS headers, acknowledgements and Pi heartbeats; they are not wire-byte measurements. For one stable WROVER, the target is one sensor snapshot plus one gateway heartbeat per minute instead of 36 total publishes per minute. Actual savings depend on changes/events and reconnect traffic.

Run the unit suite with `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -v`. CI also runs `MQTT_TEST_PORT=18884 .venv/bin/python tests/mqtt_delivery_probe.py` against a disposable anonymous broker bound to workstation loopback. This probe verifies outage buffering, queue-full withholding of ACK, real broker redelivery after reconnect, queue reopening, original timestamps and PUBACK-based queue removal. Never use a production broker for the probe.

## Configure local Mosquitto ingress for WROVER

Set up the Pi broker before flashing ESP32 firmware. Install Mosquitto and chrony, assign the Pi a stable OT-network address, and create a DNS record such as `pi-gateway.ot.example` that resolves to that address. The TLS certificate must contain that exact DNS name in its Subject Alternative Name. Use certificates from the plant PKI or an approved private CA; do not ship the broker private key to a sensor.

```bash
sudo apt update
sudo apt install -y mosquitto mosquitto-clients chrony
sudo install -d -o root -g mosquitto -m 0750 /etc/mosquitto/smart-factory/certs
sudo install -d -o root -g mosquitto -m 0750 /etc/mosquitto/smart-factory
sudo install -o root -g root -m 0644 mosquitto/mosquitto.conf.example /etc/mosquitto/conf.d/smart-factory.conf
sudo install -o root -g root -m 0640 mosquitto/acl.example /etc/mosquitto/smart-factory/acl
```

Edit `smart-factory.conf` and replace `192.0.2.10` with the Pi's fixed OT address. Install the approved CA chain, Pi server certificate, and private key under the configured cert directory. The certificate needs a DNS SAN for the firmware's `MQTT_HOST`; keep its private key root-owned and readable only by Mosquitto. Create password entries interactively so secrets do not enter shell history:

```bash
sudo mosquitto_passwd -c /etc/mosquitto/smart-factory/passwd smartfactory-gateway
sudo mosquitto_passwd /etc/mosquitto/smart-factory/passwd esp32-wrover-01
sudo chown root:mosquitto /etc/mosquitto/smart-factory/passwd /etc/mosquitto/smart-factory/acl
sudo chmod 0640 /etc/mosquitto/smart-factory/passwd /etc/mosquitto/smart-factory/acl
sudo systemctl enable --now mosquitto
sudo systemctl status mosquitto
```

Use [mosquitto.conf.example](mosquitto/mosquitto.conf.example) and [acl.example](mosquitto/acl.example) as templates. Verify the ACL topic paths exactly match `SITE_ID`, `LINE_ID`, and each device ID. By default, the gateway account reads sensor telemetry/heartbeat topics and each sensor account writes only its own telemetry/heartbeat topics. GPIO command and acknowledgement ACL entries are commented out; enable the exact device topics only after that WROVER and its isolated driver wiring have been commissioned. Keep port 8883 reachable only from the OT VLAN, keep port 1883 bound to loopback, and firewall all sensor internet/cloud egress. The Python subscriber enforces `LOCAL_MQTT_DEVICE_IDS`, topic scope, payload size, timestamp, and metric types as defense in depth. The separate `LOCAL_MQTT_CONTROL_DEVICE_IDS` allowlist controls which telemetry devices may receive GPIO requests. The WROVER publishes QoS 1 and the Python subscriber uses a stable client ID with a persistent subscription; Mosquitto persistence can queue messages while the gateway process restarts. QoS 1 is at-least-once and consumers must tolerate duplicates.

Configure the Pi as the permitted NTP source for the OT subnet using chrony (`allow <OT_SUBNET>` in `/etc/chrony/chrony.conf`) and permit UDP/123 only from that subnet. Keep the Pi's upstream time synchronization aligned with site policy. Configure the WROVER to use this gateway hostname as its NTP server so it can validate the broker's TLS certificate without reaching public NTP servers.

Copy `.env.example` to `/etc/smart-factory-iot/edge.env` and keep `EDGE_ENV=production`; this makes the service reject a plaintext or unauthenticated cloud uplink. Set the CloudAMQP MQTT endpoint/credentials for `MQTT_*`, the local gateway account for `LOCAL_MQTT_*`, and the exact comma-separated `LOCAL_MQTT_DEVICE_IDS`. Keep `LOCAL_MQTT_CONTROL_DEVICE_IDS` empty until a WROVER output and its external driver have been commissioned; when enabled, this list must be a subset of `LOCAL_MQTT_DEVICE_IDS`. CloudAMQP values are taken from the instance console: use its MQTT TLS hostname, port 8883, and a dedicated gateway identity with publish access to its own telemetry/heartbeat/ack topics plus the allowlisted sensor telemetry/heartbeat topics that it forwards, and subscribe access only to that gateway's exact `/commands` and `/commands/connectivity` topics. Backend consumers and DeviceService need their own least-privilege identities. Never copy cloud values to a WROVER. See [CloudAMQP MQTT setup](https://www.cloudamqp.com/docs/mqtt.html). `LOCAL_MQTT_HOST=127.0.0.1` and port 1883 are intentional because the Python process uses the loopback-only listener.

## systemd install

Use a dedicated unprivileged `smartfactory` service account. Keep environment files and TLS credentials under `/etc/smart-factory-iot/`, owned by root and readable only by the service group. The separate `CONNECTIVITY_SECRETS_FILE` JSON requires mode `0600` and must be owned by the service account so it can read it; group-readable JSON is rejected. The unit uses the app's virtual environment, `dialout` for serial hardware, and systemd hardening.

```bash
sudo useradd --system --home-dir /opt/smart-factory-edge --shell /usr/sbin/nologin --groups dialout smartfactory
sudo install -d -o smartfactory -g smartfactory -m 0750 /opt/smart-factory-edge/raspberry-pi
sudo -u smartfactory python3 -m venv /opt/smart-factory-edge/raspberry-pi/.venv
sudo -u smartfactory /opt/smart-factory-edge/raspberry-pi/.venv/bin/pip install -r /opt/smart-factory-edge/raspberry-pi/requirements.txt
sudo install -d -o root -g smartfactory -m 0750 /etc/smart-factory-iot
sudo install -o root -g smartfactory -m 0640 .env /etc/smart-factory-iot/edge.env
sudo install -o root -g smartfactory -m 0640 systemd/smart-factory-edge.service /etc/systemd/system/smart-factory-edge.service
sudo systemctl daemon-reload
sudo systemctl enable --now smart-factory-edge
sudo systemctl status smart-factory-edge
```

Check logs with `journalctl -u smart-factory-edge -n 100 --no-pager`. If the account already exists, retain its current ownership/group configuration.

## Safe Pi code upload

First inspect the actual commissioned unit, including its overrides:

```bash
sudo systemctl cat smart-factory-edge
sudo systemctl show smart-factory-edge --property=ExecStart --property=WorkingDirectory
```

An existing `40-telemetry.conf` or `60-gateway-release.conf` can point `ExecStart` at a staged release, including a legacy installation path. Copying files to `/opt/smart-factory-edge/raspberry-pi/src` does not update that active release. Use the [staged release procedure](#staged-gateway-release) for this connectivity release and for any dependency change. Do not remove an existing override or overwrite its environment files to force the base path to run.

For an initial source upload or a reviewed source-only change whose effective `ExecStart` uses the base directory below, run these commands from the edge repository root on your development computer. Review the dry-run, then upload Pi code and its shared test fixtures into a staging directory. Never use `rsync --delete`:

```bash
ssh DEPLOY_USER@PI_IP 'mkdir -p /tmp/smart-factory-edge/raspberry-pi'
rsync -av --dry-run --exclude='.env' --exclude='*.env' --exclude='.venv/' --exclude='__pycache__/' --exclude='*.pyc' --exclude='.DS_Store' raspberry-pi/ DEPLOY_USER@PI_IP:/tmp/smart-factory-edge/raspberry-pi/
rsync -av --exclude='.env' --exclude='*.env' --exclude='.venv/' --exclude='__pycache__/' --exclude='*.pyc' --exclude='.DS_Store' raspberry-pi/ DEPLOY_USER@PI_IP:/tmp/smart-factory-edge/raspberry-pi/
rsync -av --dry-run fixtures/ DEPLOY_USER@PI_IP:/tmp/smart-factory-edge/fixtures/
rsync -av fixtures/ DEPLOY_USER@PI_IP:/tmp/smart-factory-edge/fixtures/
```

On the Pi, copy staged source over the installed source; this leaves its `.env` and `.venv` intact:

```bash
sudo cp -a /tmp/smart-factory-edge/raspberry-pi/. /opt/smart-factory-edge/raspberry-pi/
sudo install -d -o root -g smartfactory -m 0750 /opt/smart-factory-edge/fixtures
sudo cp -a /tmp/smart-factory-edge/fixtures/. /opt/smart-factory-edge/fixtures/
sudo chown -R smartfactory:smartfactory /opt/smart-factory-edge/raspberry-pi/src
sudo systemctl restart smart-factory-edge
sudo journalctl -u smart-factory-edge -n 100 --no-pager
```

Back up `/etc/smart-factory-iot/edge.env`, any `telemetry.env`, connectivity secrets, Mosquitto credentials/ACLs and certificates, chrony configuration, and both `assets.json` and `connectivity.json` separately. Take a consistent delivery-queue snapshot using SQLite's backup API or while the gateway is stopped. The upload transfers source and test fixtures; it does not configure the broker or provision sensor firmware.

## Staged gateway release

Gateway software is deployed separately from the Oracle API/worker image. `EDGE_SOURCE_REVISION` in that image records a source pin; it does not install software on a Pi. Qualify the compatible API and worker first, then install the reviewed edge commit on each commissioned gateway and verify its real observations.

Build the bundle from the reviewed committed tree on a build host with the commissioned Pi's Python version, CPU architecture and compatible OS wheel tags. The current dependency lock targets CPython 3.11. The wheel download below requires binary artifacts for that host; a development machine's wheelhouse is not automatically suitable for ARM64. Existing plant secrets and state must not be added to the bundle.

```bash
release_revision="$(git rev-parse HEAD)"
release_bundle="$(mktemp -d /tmp/smart-factory-gateway.XXXXXX)"
git archive "$release_revision" raspberry-pi/src raspberry-pi/tests raspberry-pi/examples raspberry-pi/scripts raspberry-pi/requirements.txt fixtures | tar -x -C "$release_bundle"
cp "$release_bundle/raspberry-pi/scripts/install-telemetry-release.py" "$release_bundle/install-telemetry-release.py"
printf '%s\n' "$release_revision" > "$release_bundle/SOURCE_REVISION"
python3.11 -m pip download --require-hashes --only-binary=:all: -r "$release_bundle/raspberry-pi/requirements.txt" --dest "$release_bundle/wheels"
python3 - "$release_bundle" <<'PY'
import hashlib
from pathlib import Path
import sys
bundle = Path(sys.argv[1])
files = sorted(path for path in bundle.rglob("*") if path.is_file() and path != bundle / "SHA256SUMS")
(bundle / "SHA256SUMS").write_text("".join(
    f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(bundle).as_posix()}\n"
    for path in files
))
PY
ssh DEPLOY_USER@PI_IP "mkdir -p /tmp/smart-factory-gateway-$release_revision"
rsync -av --dry-run "$release_bundle/" "DEPLOY_USER@PI_IP:/tmp/smart-factory-gateway-$release_revision/"
rsync -av "$release_bundle/" "DEPLOY_USER@PI_IP:/tmp/smart-factory-gateway-$release_revision/"
```

On the Pi, inspect `systemctl cat` and the effective `ExecStart` as above, compare `SOURCE_REVISION` with the approved full commit, and run the installer from that bundle, substituting the commit in the path:

```bash
sudo python3 /tmp/smart-factory-gateway-APPROVED_FULL_COMMIT/install-telemetry-release.py
sudo systemctl show smart-factory-edge --property=ExecStart --property=WorkingDirectory
sudo journalctl -u smart-factory-edge -n 100 --no-pager
```

The installer verifies all bundle checksums, including the shared connectivity and provenance fixtures. It creates `/opt/smart-factory-edge/raspberry-pi/releases/gateway-.../`, retaining `raspberry-pi/` beside `fixtures/`, and copies the virtual environment identified by the current effective `ExecStart`. That active environment may live under a legacy project path; the installer does not assume the base `.venv` is active. It invokes the staged Python directly, installs from the offline wheelhouse with `--require-hashes`, checks resolved versions and dependencies, and runs unit tests as `smartfactory`. The opt-in protocol bench remains a separate qualification and is not run against a commissioned gateway by the installer.

Only after qualification does it write the managed `60-gateway-release.conf`, which changes `ExecStart` alone. Existing unit hardening, `WorkingDirectory`, `40-telemetry.conf`, environment files, their ownership, credentials, asset/connectivity configuration, and the delivery queue remain in place. It rechecks the previous effective command before cutover and the new effective Python/script paths after `daemon-reload`; a later override that wins prevents any restart. Wrappers, additional interpreter arguments, escaped paths or an unmanaged file at the new override path require a reviewed manual cutover. New source and its environment are root-owned and service-group-readable.

The installer prints the staged path and its `rollback.sh`. A failed restart/health check restores the previous managed override, including its owner, group and mode, and restarts the previous command. A failure before the initial restart restores the override without restarting the gateway. Keep prior releases and the root-only rollback state. The rollback script refuses to overwrite a managed override changed by a later release or an operator. A running systemd unit is only a process check: confirm the gateway's applied revision, fresh expected tag values, correct quality/source timestamps and queue recovery through the compatible API before accepting the rollout. Local allowlists, service-readable credentials, exact broker ACLs and TSN collector access still require commissioning; the installer does not alter them.

### Coordinated rollback with connectivity data

Older cloud consumers can strip connectivity fields and acknowledge the remaining telemetry. For a rollback to those versions, stop affected gateways **without changing configuration revision**, keep the compatible API/worker running while the broker and .NET outbox drain, and preserve any remaining edge SQLite queue offline. Publishing an empty newer revision first can invalidate already-buffered observations from the preceding revision. Keep gateways stopped throughout the incompatible cloud rollback; an older gateway process can also replay raw queued envelopes. If the cloud backlog cannot drain, keep the old worker stopped as well. Restore a compatible API/worker before restarting a gateway and replaying retained connectivity observations. The local `rollback.sh` only restores gateway code selection; it does not coordinate this cross-service data barrier or delete queued data.

## Industrial protocols

Download the gateway JSON profile from an asset's AAS page and install it at `ASSET_CONFIG_FILE` (default `/var/lib/smart-factory-iot/assets.json`) as `smartfactory:smartfactory`, mode `0600` in the private state directory. It contains gateway endpoints and mappings, never passwords. Direct MQTT assets are omitted from the Pi profile and publish with device-scoped broker credentials.

Generic standalone serial ingest is opt-in (`SERIAL_ENABLED=false` by default). Leave it disabled when the USB device is represented by an AAS `serial`, `modbus_rtu`, or `ada031_v4_serial` profile. When it is required for a separate line-delimited JSON sensor, configure its own stable `/dev/serial/by-id/...` path. The gateway resolves stable symlinks and kernel aliases to one physical path, rejects duplicate serial owners in a profile, and synchronizes profile changes with port opens.

- **OPC UA:** Secure channel is required by default. Configure policy, `SignAndEncrypt`, trusted server certificate, client certificate/key, and optional username/password. `OPCUA_ALLOW_INSECURE=true` is restricted to isolated lab use.
- **Modbus TCP/RTU:** Current adapter has no encryption. Keep it inside an isolated OT VLAN or use an authenticated encrypted tunnel and firewall allowlists. Do not expose port 502 publicly.
- **Serial JSON:** Restrict device access to the service account and verify payload sources. Use a stable path under `/dev/serial/by-id/`; the Pi keeps telemetry ports open between polls.
- **ADA031 V4 USB serial:** Use protocol `ada031_v4_serial` at 9600 baud. The edge supports the vendor jog bytes and connected-firmware profile (`P`/`D`), neutral (`N`) and program-stop (`S`) bytes. The same synchronized serial owner reads validated controller JSON and forwards it as AAS `assetSignals`. These are commanded targets and counters, not measured joint feedback. The first USB open can reset the Uno and move all five axes to the firmware's 90° startup pose. Opening and control are disabled unless the exact AAS ID is listed in `ADA031_CONTROL_ASSET_IDS`; follow [the ADA031 guide](../docs/ada031-integration.md).

Validate NodeIds, namespace URIs, register offsets, data types, byte/word order, scale, and engineering units against vendor documents. MQTT topics are `factory/{site}/{line}/{deviceId}/telemetry`, `/heartbeat`, and `/commands`. Industrial protocols terminate at the Pi; WROVER sensor topics terminate at the local Pi broker. The Pi publishes validated gateway telemetry and subscribes only to its own exact `/commands` and `/commands/connectivity` topics. Direct MQTT devices publish to their own CloudAMQP topic using separate credentials and publish-only ACLs. Apply per-device ACLs at both brokers.

If an ADA031 USB disconnect invalidates the open descriptor, the gateway closes it and reopens the stable by-id path on the next telemetry poll. It never blindly repeats an ambiguous motion write. Repeated kernel USB over-current events require a hardware power/cable correction.

## Troubleshooting and tests

- MQTT TLS: confirm UTC clock, hostname, CA, port, broker account, and topic ACL.
- OPC UA: confirm server policy, certificate trust, key permissions, endpoint and credentials.
- Serial permission: check the `dialout` group and restart after group changes.
- Readings missing: inspect `journalctl`, profile mappings, units/scaling, and broker/backend logs.

### DHT11 telemetry does not reach the dashboard

Trace one real sample through each boundary; do not replace a missing sensor value with synthetic data.

1. Open the ESP-WROVER serial monitor at 115200 baud. A healthy startup reports `[DHT11] Sensor initialized`, Wi-Fi connected, the time-sync wait, and `[MQTT] Connected to Pi gateway`. Every five seconds it should report `[PUB] QoS 1 telemetry acknowledged by Pi broker` with `sensorStatus:"ok"`. A `read_error` record proves MQTT works but the DHT read failed; check the reviewed GPIO32/33 data pin, sensor power/ground and the required pull-up.
2. If startup remains at the time-sync message, verify the Pi chrony service, OT firewall UDP/123, `NTP_SERVER_1`, DNS/mDNS resolution and the Pi clock. TLS MQTT deliberately does not start before the WROVER has valid UTC.
3. If MQTT connection fails, verify that the WROVER uses the Pi's OT hostname/address and TLS listener 8883, that the certificate SAN matches that hostname, and that its device-specific Mosquitto username and ACL match the exact `SITE_ID`, `LINE_ID` and `DEVICE_ID`.
4. On the Pi, confirm both services and follow the ingress log:

   ```bash
   sudo systemctl status mosquitto smart-factory-edge --no-pager
   sudo journalctl -u mosquitto -u smart-factory-edge -f
   ```

   Each accepted sample produces `[LOCAL MQTT] telemetry queued -> factory/.../esp32-wrover-01/telemetry`. A rejection means the topic identity, configured `LOCAL_MQTT_DEVICE_IDS`, payload schema or value range does not match. No ingress line after a WROVER publish acknowledgement points to the local broker/session configuration.
5. If the Pi queues the local sample but the dashboard remains empty, verify the cloud MQTT connection/ACL and durable queue logs, then the backend MQTT consumer and dashboard telemetry-ingestion service. Keep `DEVICE_ID=pi-edge-01` for the gateway and `LOCAL_MQTT_DEVICE_IDS=esp32-wrover-01` for the sensor; they are intentionally different identities.

Relative humidity outside 0–100%, non-finite readings, unknown fields, mismatched IDs and oversized payloads are rejected at the gateway. With `sensorStatus:"read_error"`, temperature and humidity are intentionally absent while the heartbeat and button signals continue, which distinguishes a DHT wiring/read problem from a network problem.

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s raspberry-pi/tests -v
```


## Live AAS edge profile updates

The gateway cloud client subscribes to `factory/{SITE_ID}/{LINE_ID}/{DEVICE_ID}/commands`. DeviceService publishes a retained QoS 1 `replace_asset_configuration` desired-state command; the gateway validates `schemaVersion` and `gatewayDeviceId`, atomically persists `/var/lib/smart-factory-iot/assets.json` (mode 0600), and reloads the active polling loop. A status acknowledgement is published to the command topic with `/ack` appended. The broker must enforce a per-device topic ACL and TLS; the service response means the broker accepted the publish, not that the edge host applied it.

For commissioned WROVER outputs only, the gateway also accepts a non-retained `set_gpio` command containing `schemaVersion`, `commandId`, `targetDeviceId`, `pin`, `value`, `holdMs`, and `expiresAt`. It checks the separate control-device allowlist, GPIO18, a pulse of at most two seconds, and expiry within 30 seconds, then relays it to that WROVER's local `commands` topic. GPIO19 is reserved for the physical-button LED. The WROVER returns an acknowledgement on `control-ack`; the Pi forwards it to the gateway cloud `/commands/ack` topic. A gateway `queued` acknowledgement means the local broker accepted the pulse; only a WROVER `applied` acknowledgement means the GPIO changed. Neither acknowledgement proves the machine acted. The Smart Factory dashboard does not currently expose this GPIO command; use is disabled until a role-authorized application control API/UI is connected and the machine-specific output has been commissioned.

The AAS UI publishes ADA031 jog, profile, neutral and program-stop requests only to engineers and admins using a short-lived signed role token; DeviceService checks that role again. Commands identify the AAS asset, expire within 30 seconds, are never retained, and are limited by the Pi to one per 200 ms except the stop request. The exact asset ID must also appear in `ADA031_CONTROL_ASSET_IDS`. Only fixed actions are translated to one firmware byte. The gateway acknowledgement confirms a serial write only; connected-firmware telemetry reports controller state and commanded targets, not physical movement. Its profile is separate from generic Serial JSON polling.

The profile supports validated OPC UA, Modbus TCP/RTU, serial JSON, MQTT mappings, and ADA031 V4 control profiles. Do not put passwords, private keys, or user information in endpoints or mappings. Configure protocol secrets locally on the gateway. Offline desired-state is retained, so make sure broker ACLs restrict retained configuration publication and gateway subscription to the exact device topic. WROVER pulses and ADA031 motion commands are never retained.


## Complete local three-repository test

For a workstation integration run, start services in this order from separate terminals:

```bash
# Dashboard repository: PostgreSQL, Redis, Nginx, BaSyx repositories and registries
cd ../smart-factory-frontend
docker compose --profile aas up -d --build --wait

# Backend repository: backend PostgreSQL, RabbitMQ (AMQP/MQTT), and .NET services
cd ../smart-factory-backend
docker compose up -d --build --wait

# Edge repository: run the gateway simulator from raspberry-pi/
cd ../smart-factory-edge/raspberry-pi
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python src/sensor_gateway.py
```

Before starting the local Python simulator, replace the production-only values in its ignored `.env` with this workstation profile (keep broker credentials matched to the backend `.env`):

```dotenv
EDGE_ENV=development
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
MQTT_USE_TLS=false
MQTT_USERNAME=YOUR_LOCAL_BACKEND_BROKER_USER
MQTT_PASSWORD=YOUR_LOCAL_BACKEND_BROKER_PASSWORD
LOCAL_MQTT_ENABLED=false
SERIAL_ENABLED=false
SYNTHETIC_TELEMETRY_ENABLED=true
ASSET_CONFIG_FILE=./.state/assets.json
EDGE_STATE_DIR=./.state
```

The local workstation profile uses the backend repository MQTT listener at 127.0.0.1:1883 with TLS disabled, serial polling disabled, and synthetic telemetry enabled. Do not enable the Pi `LOCAL_MQTT_*` production path for this workstation test unless a local Mosquitto listener and test ACLs have been configured. The MQTT username/password must match the backend broker settings. This profile is only for an isolated local test; do not copy it to a Raspberry Pi or connect it to a plant network. For a fresh checkout, create each repository's ignored .env from its .env.example and set the shared JWT, AAS provisioning, Redis, telemetry-ingestion, and MQTT values consistently. Keep secrets out of source control.

## Durable delivery and writable gateway state

The systemd unit creates `/var/lib/smart-factory-iot` with mode 0700 using `StateDirectory`. Store `assets.json` there; `/etc/smart-factory-iot/edge.env` and certificates remain root-owned, read-only secrets. Existing installations must change `ASSET_CONFIG_FILE` in the protected environment file and migrate their profile before restarting. This allows atomic MQTT profile updates under `ProtectSystem=strict` without granting write access to credentials.

`EDGE_STATE_DIR` enables a SQLite queue for normalized telemetry. The gateway records samples before cloud publication, retries after reconnect/restart, and removes them only after MQTT QoS 1 acknowledgement. The queue defaults to 50,000 messages (`MAX_QUEUED_MESSAGES`) and accepts at most 16 KiB per message. Watch queue depth, free disk and uplink logs; a full queue reports an error and cannot buffer additional samples. Broker acknowledgement means the cloud broker accepted the message, not dashboard persistence. Backend and dashboard deduplicate retries. Pi-local QoS 1 sensor sessions buffer short gateway process outages; a gateway/storage failure or prolonged queue exhaustion still requires operational recovery.

The same private database reserves ADA031 command IDs before serial writes. Replayed IDs cannot reopen the controller or cause another jog after a process restart. Reservation happens before USB-open because opening the port can itself trigger the vendor startup pose. A crash between reservation and write is an ambiguous result: inspect the equipment instead of blindly retrying movement. Protocol control remains subject to the commissioned allowlist, expiry and rate limit.

The dashboard repository also provides `pnpm e2e:system`: a disposable real-service MQTT/telemetry test with simulated serial hardware. Run it from a Python environment containing the edge dependencies. Bench commissioning, power-cut recovery, cloud ACL/TLS testing and OTA acceptance remain separate physical/deployment checks.

## Optional security observation

The disabled-by-default [passive collector](../docs/cybersecurity-collector.md) uses the existing durable SQLite file and established connections. It adds no scan, subscriber or plant remediation. Follow the staging/manual commissioning and quarantine/retry procedure before enabling it on hardware.

## MQTT topic identity and acquisition context

Canonical payload and registered IDs are unchanged. Topic levels use `mqtt-topic-v1`: a literal dot is `%2E`, so `sensor.temp:01` publishes under `sensor%2Etemp:01`. Colons are preserved. Only the dot escape is decoded; wildcard, slash, nested and other escapes are rejected. Encoded routes are bounded to 255 bytes. The gateway normalizes older raw-dot Mosquitto topics and persisted spool routes on forwarding without rewriting acquisition JSON. Review exact publisher and device-specific ACL routes during commissioning; this release does not change a factory broker or ACL. See the [telemetry contract](https://github.com/DruHustle/smart-factory-frontend/blob/main/docs/telemetry-contract.md).

Pi-local ingress requires MQTT 5 with persistent sessions and Retain As Published subscriptions. Only actual subscription acknowledgements establish verified transport context; disconnects clear it. Local publisher JSON cannot assert that flag. Retained, old or unverified deliveries remain historical downstream, including when an MQTT 5 cloud subscriber receives them. The durable queue retains original acquisition identity across retries.

The disposable two-broker qualification passed with a Mosquitto local broker and either RabbitMQ 4.3.6 or Mosquitto 2.0.22 cloud broker. It exercised actual Pi subscription, spool, .NET/Node ingestion, punctuated IDs, mixed evidence and units, outage/restart, duplicates and database restore. The edge suite passed 78 tests and ESP32 compilation; no firmware was flashed and no physical control was issued. Hosted TLS/ACLs, Pi installation and hardware feedback remain separate commissioning acceptance.
