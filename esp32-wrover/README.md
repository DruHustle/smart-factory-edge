# ESP32 WROVER Firmware

The WROVER publishes physical DHT11 temperature, relative humidity, and button state to the Pi's local Mosquitto broker over MQTT/TLS. GPIO18 remains an allowlisted digital control output; GPIO19 is dedicated to the button-state LED. It does not connect to the cloud broker, application database, Redis, or the internet; the Pi validates and forwards telemetry and relays short-lived output requests.

## Configure, build, and upload

Install PlatformIO Core or its VS Code extension and the Espressif32 platform. Connect the board over USB. From this directory:

```bash
cp include/config.example.h include/smart_factory_config.h
```

Edit the ignored `include/smart_factory_config.h` with the OT Wi-Fi SSID/password, the Pi gateway mDNS name (`smart-factory-gateway.local`) and TLS listener port (`8883`), a unique Mosquitto user/password, the CA that signed the Pi broker certificate, and the gateway-provided NTP server. The firmware resolves `.local` names through mDNS for both NTP and MQTT; TLS still checks the original `MQTT_HOST` against a DNS Subject Alternative Name on the Pi broker certificate. The ESP32 must be on a LAN that forwards mDNS multicast and allows local client-to-client traffic. Keep WROVER credentials separate from the Pi's cloud broker credentials. GPIO18 defaults LOW as the startup and timeout-safe control level. Confirm the connected driver treats LOW as its safe state before enabling remote control; otherwise set `CONTROL_GPIO_SAFE_LEVEL` to the verified safe level in the ignored config.

```bash
pio run
pio device list
pio run -t upload --upload-port /dev/ttyUSB0   # use the port reported by PlatformIO
pio device monitor --port /dev/ttyUSB0 --baud 115200
```

On macOS the upload port is commonly `/dev/cu.usbserial-*`; on Linux it is commonly `/dev/ttyUSB0` or `/dev/ttyACM0`. Close other serial monitors before uploading. If the shell cannot find `pio`, open the PlatformIO Core CLI terminal in VS Code or use PlatformIO's IDE build/upload buttons. Verify the firmware build succeeded before connecting it to any machinery.

`include/smart_factory_config.h` is ignored by Git. Never commit it or print production secrets to shared build logs.

## DHT11 sensor wiring

The firmware reads a DHT11 on GPIO32 by default (`DHT11_DATA_PIN` in `include/smart_factory_config.h`). GPIO32 is exposed on the ESP-WROVER-KIT header and is separate from the GPIO18 control output and GPIO19 indicator. Check the exact WROVER-KIT revision and board header labels before wiring.

- Three-pin DHT11 module: connect `VCC` to `3V3`, `GND` to `GND`, and `DATA` to GPIO32. Confirm whether the module already includes a pull-up resistor.
- Bare four-pin DHT11: identify the pins from its own datasheet/marking, then add a 4.7–10 kΩ pull-up between `DATA` and `3V3` unless the sensor board already includes one. Do not infer pin order from the front view alone.
- Keep the data signal at 3.3 V logic. Do not apply 5 V to the ESP32 data pin.

Temperature is reported in °C and humidity in %RH every five seconds. If a read fails, the payload includes `sensorStatus: "read_error"` and omits the measurement values; it does not report synthetic values. A DHT11 is suitable for a basic bench demonstration, not calibrated process measurement, asset protection, or a safety function. The WROVER does not claim vibration, power, pressure, or RPM readings without their corresponding sensors.

## Bench LED and push-button wiring

This profile reserves GPIO19 for LED 2 and GPIO21 for a momentary user button. GPIO18 remains available for the separately controlled output. Disconnect the optional LCD, camera, and microSD interfaces because these pins are shared on ESP-WROVER-KIT V4.1.

- LED 2: GPIO19 → 330 Ω series resistor → LED anode (long leg); LED cathode (short leg/flat side) → GND. It stays on while the button is held and turns off on release.
- Button: GPIO21 → one button terminal; opposite terminal → GND. The firmware uses `INPUT_PULLUP`, so no external pull-up is required. A press is active LOW and is debounced in software.

Telemetry publishes immediately when the debounced button changes and includes numeric `assetSignals.buttonPressed` (`1` while held, otherwise `0`) and `assetSignals.buttonPressCount`. The counter is stored in ESP32 nonvolatile storage, survives restart and power loss, and resets only if the device's NVS/flash is erased. The dashboard displays the current Pressed/Released state and lifetime count.

## Network and broker security

Place the WROVER and Pi broker listener on a dedicated OT Wi-Fi/VLAN. Firewall the WROVER so its only application network destination is the Pi's MQTT/TLS listener and local time service. The Pi should be the sole device-network route to the cloud. Avoid a default route or internet egress from the WROVER where the network design permits.

The example uses MQTT/TLS on port 8883 with `WiFiClientSecure`, CA validation, and hostname verification. Firmware waits for UTC time from the Pi before TLS checks and does not fall back to plaintext. Use a unique Mosquitto account per WROVER and ACLs that permit publishing only its telemetry, heartbeat, and control acknowledgement; it may subscribe only to its own command topic. GPIO requests reach the WROVER through the Pi and must be non-retained, expire within 30 seconds, and pulse an allowlisted pin for at most 2 seconds. On MQTT disconnect the outputs return to their configured safe level. Port 1883 must be blocked from the OT Wi-Fi and bound to Pi loopback for the gateway process only.

## Topics and payload

- Publish telemetry: `factory/{site}/{line}/{deviceId}/telemetry`
- Publish heartbeat: `factory/{site}/{line}/{deviceId}/heartbeat`
- Subscribe control requests: `factory/{site}/{line}/{deviceId}/commands`
- Publish control acknowledgements: `factory/{site}/{line}/{deviceId}/control-ack`

The accepted control payload is a JSON object with `schemaVersion: 1`, `action: "set_gpio"`, a unique `commandId`, `pin: 18`, `value` (`0` or `1`), `holdMs` (1–2000), and UTC Unix-millisecond `expiresAt`. GPIO19 is reserved for LED 2 and is rejected as a remote-control target. The firmware rejects unknown pins, malformed fields, expired requests, replayed command IDs, and requests before time sync. QoS 1 delivery can duplicate messages; command IDs prevent repeated pulses within the recent-ID cache. An `applied` acknowledgement confirms the GPIO level changed; it does not confirm the external machine moved or reached a safe state.

GPIO18 and GPIO19 are shared with optional camera/LCD interfaces on the ESP-WROVER-KIT. Disconnect those peripherals and verify the exact board revision and header before wiring. GPIO18 is 3.3 V logic only: connect it to a compatible isolated driver input with an external bias that holds the machine input in its safe state during boot or loss of power. GPIO19 may drive only the documented low-current LED through its resistor. Never connect a motor, coil, mains, or industrial voltage directly to an ESP32 GPIO. Do not use these outputs for emergency stop, guarding, interlocks, or other protective functions; use independent safety-rated hardware.

The DHT11 is a basic, uncalibrated bench sensor and must not be used as a process instrument. Telemetry and command QoS default to 1. An engineer can request QoS 0, 1 or 2 for uplink and downlink from the Live WROVER controls; the WROVER persists an accepted configuration and confirms its subscription grant. Management requests are published at QoS 1, but a QoS 0 command subscription can lower their effective delivery to QoS 0; an unconfirmed request can be retried. The dashboard distinguishes a requested setting from a device acknowledgement, and the effective delivery level on each hop is limited by the publisher and subscriber. QoS 0 does not confirm delivery, QoS 1 can duplicate messages, and QoS 2 does not provide a power-loss-proof sensor-side spool. Downstream consumers must tolerate duplicates across restarts. GPIO18 has an independent software timer that returns the output to its configured safe level while the application waits on MQTT; this is not safety-rated protection. Do not present GPIO pulses as speed control or emergency stop.

## Raspberry Pi setup reference

Configure Mosquitto TLS, per-device ACLs, cloud broker uplink credentials, and the Python allowlist on the Pi first. Follow the [Raspberry Pi gateway guide](../raspberry-pi/README.md#configure-local-mosquitto-ingress-for-wrover) before flashing this firmware.
