# Smart Factory IoT Edge

Raspberry Pi gateway and ESP32 WROVER firmware for industrial telemetry.

The frontend runs on Vercel. The API and singleton telemetry/notification worker run on the Oracle VM alongside BaSyx; see the [Oracle deployment guide](https://github.com/DruHustle/smart-factory-iot-frontend/blob/main/deploy/oracle/README.md). Oracle releases and Pi updates are currently manual. The historical Render release workflow is disabled. Edge quality CI validates pull requests targeting `master` and pushes to `master`; CI never flashes plant hardware.

## Data path

The Pi polls OPC UA, Modbus TCP/RTU, and serial JSON equipment, exchanges allowlisted control and controller telemetry with commissioned ADA031 V4 arms, and hosts a local MQTT listener for ESP32 WROVER sensors. The Pi validates local sensor identities and telemetry, then forwards it over TLS to CloudAMQP. Eligible native MQTT devices may publish directly to CloudAMQP with their own scoped credentials and topic ACLs. The ADA031 V4 uses USB serial and must use the Pi gateway. The .NET backend consumes both direct and gateway-originated MQTT telemetry and forwards it to the dashboard through a service-token bridge.

## Device guides

- [Raspberry Pi gateway](raspberry-pi/README.md): install, hardened systemd service, TLS, industrial protocol security, mapping, safe code upload, and troubleshooting.
- [ADA031 V4 integration guide](docs/ada031-integration.md): official stock command bytes, USB serial profile, allowlist, startup motion, temporary code clamps, and feedback limitations.
- [ADA031 V4 robotic-arm firmware](robotic-arm/README.md): Arduino Uno controller source, calibrated poses, build, and upload instructions.
- [ESP32 WROVER firmware](esp32-wrover/README.md): configure, build, flash, TLS trust, and broker ACLs.

Production Pi runs as unprivileged `smartfactory`, with secrets under `/etc/smart-factory-iot/`. The Pi hosts Mosquitto for local sensors and uses a dedicated identity for the TLS CloudAMQP telemetry uplink. Backend services use separate scoped CloudAMQP identities for consuming telemetry, publishing gateway commands, and event-bus traffic. The telemetry release installer stages a separate source tree and virtual environment, checks pinned dependencies and gateway tests under the service account, then switches systemd with a rollback script. Existing secrets, firmware and delivery queues remain in place. See the [gateway guide](raspberry-pi/README.md#telemetry-efficiency-and-recovery) for coalescing, acknowledgements and traffic counters. ESP32 firmware uses TLS with the Pi broker certificate and unique local credentials. It publishes QoS 1 telemetry to the Pi; the persistent local MQTT session queues messages during short gateway process restarts. Modbus TCP is plaintext in this adapter; keep it on an isolated OT network or authenticated encrypted tunnel.

## Tests and firmware upload

```bash
# From the repository root; install Python dependencies first as described
# in raspberry-pi/README.md.
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s raspberry-pi/tests -v
cd esp32-wrover
cp include/config.example.h include/smart_factory_config.h
# Edit the ignored smart_factory_config.h with the Pi broker TLS and device credentials.
pio run
pio device list
pio run -t upload --upload-port /dev/ttyUSB0  # use the port reported above
pio device monitor
```

On macOS the WROVER upload port is commonly `/dev/cu.usbserial-*`. See the [firmware guide](esp32-wrover/README.md) before wiring the isolated driver inputs or enabling GPIO requests.

Test with a staging broker and bench equipment before a plant rollout.

## Firmware update support

This edge release has no automated OTA/FOTA agent. The ESP-WROVER firmware has no network update receiver and must be built and flashed over USB using PlatformIO. Raspberry Pi software uses the staged, manual `rsync` deployment procedure in [the gateway guide](raspberry-pi/README.md); it is not updated by the dashboard. The ADA031 V4 Uno is programmed over USB with the project's connected firmware (the official stock sketch remains a deliberate legacy option). The dashboard marks firmware delivery unavailable and its legacy records must not be treated as device update evidence.

Do not enable remote firmware delivery until a signed release pipeline, hardware compatibility checks, authenticated device acknowledgements, rollback/recovery behavior, and staged rollout tests are in place.


### Classified equipment observations

The existing Pi-local relay optionally preserves an `integration` provenance object, including explicit live/simulated/replay mode, UTC receipt time and run identity. Simulated/replay messages require their run IDs. Broker retained state is preserved and cannot be cleared by a payload assertion. The relay does not manufacture a live classification for legacy sources. Original observation time and provenance survive the existing durable queue. No Onesa account, server, SDK or additional poller is needed; native equipment visualization is owned by the Smart Factory dashboard.

## Trusted telemetry candidate (27 October 2026)

The candidate aligns site/line/device IDs with the [coordinated observation contract](https://github.com/DruHustle/smart-factory-iot-frontend/blob/feat/product-experience-trust/docs/trusted-observations.md). Dots and colons remain valid application identities. Source units and reviewed raw scaling evidence survive forwarding and durable retries; absent legacy provenance/units are not relabelled as verified live evidence. New WROVER acquisition metadata identifies source quality, boot session and sequence. Broker retention cannot be cleared by a payload.

Gateway dependencies retain their existing versions and now include official artifact hashes. Regenerate with `python3 raspberry-pi/scripts/lock-gateway-dependencies.py`, review the lock changes, and install with `pip install --require-hashes -r raspberry-pi/requirements.txt`.

Local software evidence: 76 Python tests passed and the changed WROVER `compile-check` environment compiled successfully. No firmware was flashed and no physical command, collector commissioning or hosted release was performed by these checks.
