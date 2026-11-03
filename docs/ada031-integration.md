# Adeept ADA031 V4 control and telemetry through the Raspberry Pi gateway

This guide documents both the one-byte protocol in the official **ADA031 V4.0** source package and the project's connected firmware. The arm is an AAS asset; the Pi is its edge gateway. The arm does not connect directly to MQTT or the cloud.

## Official V4 references

- [Adeept V4.0 tutorial and matching ZIP package](https://www.adeept.com/learn/detail-64.html)
- [Adeept ADA031 product information](https://www.adeept.com/adeept-arduino-compatible-diy-5-dof-robotic-arm-kit-for-arduino-uno-r3-steam-robot-arm-kit-with-arduino-and-processing-code_p0118_s0031.html)
- The matching archive supplied for this integration: `ADA031-Adeept_Robotic_Arm_Kit_for_Arduino-V4.0-20251205.zip`

The vendor identifies the V4 controller as ATmega328P based and Arduino Uno R3 compatible. The official Processing sketch drives five servos. Although the package includes six AD002 servos, the stock command sketch exposes five axes.

## Load the connected firmware onto the arm

The current dashboard profiles and telemetry cards require [`robotic-arm/AdeeptArmConnected.ino`](../robotic-arm/AdeeptArmConnected.ino) from this repository. It retains the vendor jog bytes and adds the `P`, `D`, `N`, and `S` commands plus bounded JSON telemetry. Do not install the Pi Python gateway or ESP32 firmware on the arm controller.

1. Before connecting USB or uploading, secure the arm and disconnect its separate servo power supply. The sketch writes 90° to every joint when it starts, and uploading restarts the board.
2. Install Arduino IDE 2.x and download/extract the official V4 archive listed above.
3. Open [`robotic-arm/AdeeptArmConnected.ino`](../robotic-arm/AdeeptArmConnected.ino), or build the folder with PlatformIO. Install the documented Servo, Wire, and SSD1306Ascii dependencies when using Arduino IDE. Use the official sketch only when deliberately commissioning legacy jog-only operation.
4. Connect the controller by USB. In Arduino IDE select **Arduino Uno** (ATmega328P) and the serial port for the arm, then compile and upload. The sketch uses Arduino's Servo library; if the IDE reports that library missing, install the official Arduino Servo library.
5. With the arm still secured and its servo supply disconnected, confirm upload completed. Reconnect servo power only after the work area is clear and the controller startup movement is expected.
6. For the Pi integration, close Arduino Serial Monitor and the vendor Processing application before starting the gateway. The gateway is the single owner of the 9600-baud port; do not run two serial clients at once.

The connected firmware reports commanded targets and program counters, not measured joint position, torque, temperature, current, or proof of motion. Its stop request is operational control, not an emergency stop.

## Stock controller protocol

The official Processing sketch opens USB serial at **9600 baud** and transmits one ASCII character per jog. It does not send JSON, Modbus, OPC UA, MQTT, a newline, or an acknowledgement.

| Joint | Sketch servo | Increase command | Decrease command | Step in sketch |
|---|---:|---|---|---:|
| Base | Servo 1, Arduino D9 | `o` | `p` | 1° |
| Shoulder | Servo 2, Arduino D6 | `u` | `i` | 1° |
| Elbow | Servo 3, Arduino D5 | `t` | `y` | 1° |
| Wrist rotation | Servo 4, Arduino D3 | `e` | `r` | 1° |
| Gripper | Servo 5, Arduino D11 | `q` | `w` | 10° |

The sketch accepts either letter case; the gateway sends the lowercase form. In its own state, the stock sketch clamps Servo 1–4 to **0–180°** and Servo 5 to **35–90°**. These are code clamps only. They are temporary software bounds, not validated mechanical travel limits, force limits, or a safety function.

The connected firmware additionally accepts:

| Operation | Serial byte |
|---|---|
| Repeat calibrated A → B → A profile | `P` |
| Demonstration profile | `D` |
| Move to the 90° neutral pose | `N` |
| Finish the current pose, then stop the active program and idle | `S` |

It emits newline-delimited JSON containing completed, failed and interrupted cycle counts, cycle duration, profile and sequence state, movement/button/calibration state, uptime, and five commanded servo targets. `failed_cycles` remains supported for older connected-firmware builds; operator stop/button-neutral interruptions use `interrupted_cycles` and are not physical-failure evidence. Potentiometer targets are included during calibration. The Pi validates and forwards these values as `assetSignals`; no tag mapping is required.

### Startup behavior and feedback

The sketch attaches the servos and writes **90° to all five axes in `setup()`**. Opening the Uno USB serial connection can pulse its auto-reset line, restart the sketch, and cause that startup movement. The edge driver keeps the serial port open after first use; restarting the Pi gateway or reconnecting USB can cause the first subsequent open to reset the arm again. Plan for that movement before enabling the gateway allowlist.

The stock sketch returns no feedback. The connected firmware returns its controller state and commanded targets, but still has no joint sensors or physical completion/fault feedback. A gateway acknowledgement means only that one byte was accepted by the USB serial write. Subsequent telemetry confirms the controller reported a state transition; it cannot confirm that a servo physically moved or reached the target.

## Platform command path

1. An engineer or administrator opens the asset's AAS page and chooses a jog, operation profile, neutral move, or program stop. A jog click requests one vendor step; the gripper step is 10°.
2. The dashboard checks the current database-backed role and signs a short-lived (one-minute) user-role token. DeviceService independently requires an `engineer` or `admin` bearer role.
3. DeviceService publishes the command as QoS 1, **non-retained** MQTT JSON to the owning Pi gateway's command topic. It expires 10 seconds after creation.
4. The Pi checks the exact action-specific schema, expiry, AAS asset ID allowlist, deployed profile protocol and serial endpoint. It rate-limits commands to one per 200 ms per arm (the stop request may bypass that delay) and durably suppresses duplicate command IDs.
5. The Pi maps the action to exactly one allowlisted ASCII byte and writes it at 9600 baud. The Pi publishes a gateway acknowledgement to its `/commands/ack` topic with `status: "serial_write_accepted"` and `feedback: "controller_telemetry_pending"`.

The browser confirms broker publication only. It does not wait for that gateway acknowledgement, and neither status confirms physical movement. MQTT ACLs must allow DeviceService to publish to the exact gateway command topic and the gateway to publish its acknowledgement. The arm itself has no MQTT credentials.

The accepted cloud command shape is:

```json
{
  "schemaVersion": 2,
  "action": "jog",
  "commandId": "c7d8a001-4b39-4e50-bcdf-409613ec3715",
  "targetAssetId": "urn:smart-factory:asset:...",
  "joint": "base",
  "direction": "increase",
  "expiresAt": 1791131234567
}
```

Allowed v2 actions are `jog`, `set_profile`, `neutral`, and `stop_program`. `stop_program` lets the current pose finish and then idles; it does not wait for the entire multi-pose sequence. Allowed profiles are `pick_and_place_repeat` and `demonstration_moves`. Joint identifiers are `base`, `shoulder`, `elbow`, `wrist_rotation`, and `gripper`; direction is `increase` or `decrease`. Exact field sets are enforced. The legacy v1 `ada031_control` jog is accepted during coordinated rollout. Arbitrary serial bytes and free-form commands are not accepted, and retained motion commands are rejected. `stop_program` is not an emergency stop.

## Create and connect the AAS asset

1. Register a gateway in **Gateway & Edge Device Connectivity**.
2. In **Assets → Create Asset**, use **Robotic arm** and select the registered Pi gateway.
3. Select **ADA031 V4 USB control** and enter the stable USB serial path reported by the Pi, for example:

   ```text
   serial:///dev/serial/by-id/usb-Arduino__www.arduino.cc__0043_.../if00?baudrate=9600
   ```

4. Provision the AAS and deploy its edge profile. The profile protocol is `ada031_v4_serial`; it reserves the serial device from the unrelated Serial JSON telemetry poller. Leave tag mappings empty: the edge adapter recognizes and validates the connected firmware's fixed telemetry schema.
5. Keep remote commands disabled initially. Once you have checked the arm, clearances, temporary clamps, USB reset movement, and gateway command path, add this exact AAS asset ID to the Pi environment variable `ADA031_CONTROL_ASSET_IDS` and restart `smart-factory-edge`.

The feature remains disabled on the Pi unless an exact asset ID appears in `ADA031_CONTROL_ASSET_IDS`. Do not use a broad wildcard. The profile alone does not enable motion. The browser's safety confirmation checkbox is a user reminder, not a substitute for physical protections.

## Raspberry Pi setup and deployment

Connect the controller with a USB data cable and identify the stable device path:

```bash
ls -l /dev/serial/by-id/
```

Ensure the `smartfactory` account can access that device (usually through `dialout`) and deploy the Pi service as described in [the Raspberry Pi gateway guide](../raspberry-pi/README.md). Store the allowlist only in `/etc/smart-factory-iot/edge.env`, owned by root and readable by the service group. Example, after commissioning:

```dotenv
ADA031_CONTROL_ASSET_IDS=urn:smart-factory:asset:replace-with-exact-id
```

Keep the cloud broker credential on the gateway service only. The Arduino controller receives a physical USB serial connection; it never receives Wi-Fi credentials, MQTT credentials, or the service token.

Check the gateway logs after enabling the asset and restarting the service:

```bash
sudo journalctl -u smart-factory-edge -n 100 --no-pager
```

The first connected-firmware telemetry poll opens the serial port. That open can reset the controller and execute its 90° startup writes before any UI command. Start with the arm secured, motion area clear, and independent power disconnect accessible. Never place hands in the work area while powered.

If USB power loss or re-enumeration invalidates the serial descriptor, the gateway closes and discards it, then reopens the stable `/dev/serial/by-id/...` path on the next telemetry poll. A failed or ambiguous motion write is never automatically repeated. Recurrent USB over-current messages in `dmesg` remain a hardware problem: correct the supply, cable, hub, or wiring before operation.

## Safety and scope

ADA031 is an educational hobby robotic arm, not a safety-rated industrial robot. The connected firmware reports controller targets and state, not physical servo feedback. Vendor code clamps, application role checks, MQTT delivery, and serial-write acknowledgements do not provide safety-rated stopping, guarding, speed monitoring, torque limitation, or position verification. Keep machine protective functions physically independent and complete an appropriate risk assessment before powered operation. Do not connect this setup to production machinery or people-facing motion without engineering validation and suitable independent safeguards.

The V4 stock sketch is not the ESP32 WROVER firmware. Do not flash the WROVER PlatformIO build to the arm controller.

## Tests and limitations

The Pi test suite checks command mapping, validation, expiration, allowlisting, duplicate suppression, non-retained policy, and the serial write contract without moving hardware:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s raspberry-pi/tests -v
```

Those tests do not verify the servos, wiring, physical clearances, mechanical limits, or actual movement. Commission those on the assembled arm with physical access to its power disconnect.
