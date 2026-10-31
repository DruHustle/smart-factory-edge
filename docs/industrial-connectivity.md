# Industrial device connectivity

The Pi gateway now runs independent, read-only protocol connections with arbitrary
typed tags. These tags are separate from the existing six scalar telemetry fields
and the AAS asset profile. Each tag can carry an AAS `assetId`, semantic `property`,
engineering `unit`, and scaling. A Kepware server can be an OPC UA source, but no
vendor preset or proprietary server is required.

## Implemented protocol behavior

| Protocol | Acquisition | Source evidence and limits |
| --- | --- | --- |
| OPC UA | Persistent secure client, bounded browse, bulk initial Read, actual monitored subscriptions | Namespace URI resolution, native StatusCode, source/server timestamps; scalar Boolean, Int16/UInt16, Int32/UInt32, Int64/UInt64, Float/Double, String |
| Modbus TCP / RTU | Persistent client, batched adjacent addresses, coils/discrete inputs/input/holding registers | Configured unit IDs, zero-based addresses, byte/word order, typed register widths; a successful read has quality `unknown` because transport success does not establish sensor quality |
| MQTT JSON | Independent MQTT 5 subscriber, exact topics, RFC 6901 JSON pointers | QoS 1, retained-delivery awareness, explicit payload quality/timestamps where mapped, stale-source timeout; source messages cannot select other tags or identities |
| OPC UA FX diagnostics | Existing server browse and monitored reads | Discovers actual type definitions in the Part 81 AC/CM namespace hierarchy; ordinary OPC UA alone never qualifies as an observed FX information model |
| OPC UA FX UADP observation | Real IPv4 UDP unicast/multicast subscriber for a commissioned scalar keyframe layout | Explicit publisher/group/writer IDs, field types/order, configured metadata version; unsigned transport is always marked `unauthenticated_udp` and never yields good/current trusted measurements |
| TSN evidence | Local, read-only `pmc`, `ethtool -T`, `tc -j qdisc show`, sysfs reads | Observed PTP/gPTP daemon state, offset/delay, timestamping capability, qdisc kinds and interface drop/error counters; missing tools/permissions produce explicit `unknown` / `unavailable` evidence |

There is no generic write method or OPC UA method invocation in these drivers.
FX connection establishment, PubSub publishing, TSN schedule programming, switch
configuration, clock changes, deterministic delivery guarantees and certification
are outside this implementation. Existing separately commissioned GPIO/robot
controls retain their existing contracts.

### OPC UA FX and UADP scope

Diagnostics recognize `AutomationComponentType`, `FunctionalEntityType`,
`ConnectionEndpointType` / `PubSubConnectionEndpointType`, and
`ConnectionManagerType`, including subtypes. Type identity uses the namespaces
`http://opcfoundation.org/UA/FX/AC/` and
`http://opcfoundation.org/UA/FX/CM/`, not display names alone. `modelVerified` means
that such a type was observed; it is not device conformance or certification.
Browse previews are bounded to 20 entries, with depth, node and time budgets.
Read specific component health/status variables by adding their NodeIds as tags.

UADP observation consumes an **already commissioned** publisher. The supported
profile is UADP version 1 with explicit PublisherId, WriterGroupId,
DataSetWriterId and payload header, scalar Variant / RawData / DataValue
keyframes, and optional sequence numbers. Field metadata must be supplied in
configuration with every field present in contiguous `fieldIndex` order.
Received configuration versions, when present, must match the configured major
and minor version. Absence is reported as `metadata_version_not_transmitted`.
Source and DataSet status severity is preserved; a field cannot clear a more
severe DataSet status. Unsigned Good is represented as `unknown`, with the native
status retained. Variant/DataValue field counts and types are checked. RawData
has no embedded type information and therefore depends on the commissioned
metadata. This observer does not infer a layout from unknown packets.

Signed/encrypted messages, security-key exchange, delta/keepalive/chunk messages,
promoted fields, arrays, structured values, fixed-offset profiles omitting
identity/payload headers, IPv6 and Ethernet Layer 2 UADP are unsupported and are
rejected. Datagram size is limited to 8 KiB and input processing to 500 packets
per second per connection. Duplicate/out-of-order sequence numbers are ignored;
a publisher can rebaseline after 30 seconds of silence. Missing data becomes bad
after the greater of 5 seconds or three configured intervals. Use one unicast
subscriber configuration per local address/port; the endpoint is the receiving
local address or multicast group, not the remote sender's address.

Every FX/TSN report has `determinism: "unverified"`. An observed taprio qdisc, a
PTP slave state, or a NIC's timestamp capability is useful diagnostic evidence;
none establishes end-to-end deadlines, IEEE 802.1AS conformance, or qualified
hardware behavior. No physical FX/TSN hardware qualification is asserted.

## Desired state and source addresses

`CONNECTIVITY_CONFIG_FILE` defaults to `connectivity.json` beside
`ASSET_CONFIG_FILE`. The schema version is `industrial-connectivity/v1`. The
document has `gatewayDeviceId`, a positive monotonically increasing `revision`,
and at most 16 connections with up to 100 tags each. Serialized configuration is
limited to **64,000 UTF-8 bytes** so the MQTT action wrapper fits in 64 KiB.
Unknown fields, embedded credentials, methods, duplicate IDs/JSON keys and
invalid protocol/address combinations are rejected.

Example configuration (the documentation address is intentionally disabled):

```json
{
  "schemaVersion": "industrial-connectivity/v1",
  "gatewayDeviceId": "pi-edge-01",
  "revision": 1,
  "connections": [{
    "connectionId": "press-controller",
    "name": "Press controller",
    "protocol": "modbus_tcp",
    "endpoint": "tcp://192.0.2.10:502",
    "enabled": false,
    "intervalMs": 1000,
    "tags": [{
      "tagId": "hydraulic-pressure",
      "name": "Hydraulic pressure",
      "dataType": "uint16",
      "unit": "bar",
      "assetId": "urn:factory:press:01",
      "property": "hydraulicPressure",
      "scale": 0.1,
      "offset": 0,
      "source": {"address": 12, "registerType": "input_register", "unitId": 1}
    }]
  }]
}
```

| Protocol | Endpoint and tag source |
| --- | --- |
| OPC UA | `opc.tcp://host:4840/path`; `{"namespaceUri":"urn:vendor:machine","nodeId":"s=Pressure"}`. An explicit indexed NodeId such as `ns=2;s=Pressure` can be used without `namespaceUri`. |
| Modbus TCP | `tcp://host:502`; `address`, `registerType`, `unitId`, optional matching `count`, `byteOrder`, `wordOrder`. Addresses are zero-based protocol offsets, not 40001-style reference numbers. |
| Modbus RTU | `serial:///dev/serial/by-id/commissioned-adapter?baudrate=19200&parity=E&stopbits=1&bytesize=8`; same source as TCP. |
| MQTT | `mqtts://broker:8883`; `{"topic":"machine/measurements","path":"/pressure/value","timestampPath":"/time","qualityPath":"/pressure/quality"}`. Wildcards are forbidden; quality/timestamp pointers are optional. |
| FX diagnostics | `opc.tcp://host:4840`; connection `fx:{"mode":"diagnostics"}`, ordinary OPC UA tag sources. |
| FX UDP | `opc.udp://239.192.0.1:4840`; tag source `{"fieldIndex":0}` plus explicit commissioning metadata below. |

For FX UDP, `fx` contains `mode:"udp_uadp"`, `publisherId` as a decimal string
or string publisher identity, `publisherIdType` (`Byte`, `UInt16`, `UInt32`,
`UInt64`, `String`), `writerGroupId`, `dataSetWriterId`,
`metadataMajorVersion`, `metadataMinorVersion`, and explicit
`allowUnsecured:true`. Optional `interfaceAddress` selects an IPv4 multicast
interface. This consent does not change the untrusted observation quality.

`dataType` describes source encoding. Integer source data must fit its declared
range; scaling may yield fractional engineering values for Int16/UInt16 and
Int32/UInt32. Int64/UInt64 travel as exact decimal **strings**, avoiding JSON
number precision loss. Boolean, String, Int64 and UInt64 require identity
scaling. Strings are limited to 1,024 characters; invalid types, non-finite
numbers and unsupported conversions become explicit bad/null observations.

## Local commissioning and authentication

The protected environment authorizes resources; a cloud configuration cannot
extend those permissions. In production all destination/resource allowlists are
empty by default:

```ini
CONNECTIVITY_ALLOWED_NETWORKS=192.0.2.0/24,239.192.0.1/32
CONNECTIVITY_ALLOWED_HOSTS=commissioned-plc.example
CONNECTIVITY_ALLOWED_SERIAL_PORTS=/dev/serial/by-id/commissioned-adapter
CONNECTIVITY_ALLOWED_INTERFACES=eth0
CONNECTIVITY_SECRETS_FILE=/etc/smart-factory-iot/connectivity-secrets.json
```

Replace these examples with actual reviewed resources. Hostname authorization
or membership in an allowed network is required. All DNS results are checked;
unspecified and cloud metadata addresses are denied. Plaintext MQTT to remote
hosts is rejected in production. In development, loopback network addresses and
the `lo` diagnostics interface are allowed; serial ports still require explicit
commissioning.

Credentials are referenced with a safe `credentialRef`; secret content remains
in an owner-only local JSON file, maximum 64 KiB. Its shape is
`{"credentials":{...},"collectors":{...}}`. A UA credential entry can contain
`securityPolicy`, `securityMode`, `clientCertificate`, `clientPrivateKey`,
`serverCertificate`, `username`, `password`. Production requires
`SignAndEncrypt` and an explicitly trusted server certificate. Without a
reference, the existing `OPCUA_*` settings apply. Insecure UA requires explicit
development opt-in. A MQTT credential entry can contain `username`, `password`,
`caCertificate`, `clientCertificate`, `clientPrivateKey`; system CA validation
applies when no custom CA is provided. No insecure TLS verification mode is
available.

A connection's optional `tsn` object contains `interface`, optional `ptpDomain`
(0–127) and `collectorRef`. For example a local collector entry can be
`{"interface":"eth0","pmcSocket":"/run/ptp4lro","transportSpecific":1}`.
The referenced interface must match, be allowlisted, and the socket must be an
explicit local `/run/` or `/var/run/` path. `transportSpecific` is local-only,
0–15; use the commissioned PTP/gPTP profile's value. The collector creates a
temporary local client socket and sends only `GET TIME_STATUS_NP`,
`GET PORT_DATA_SET`, `GET CURRENT_DATA_SET`. It never starts/configures ptp4l,
changes the clock, writes a qdisc or programs a switch. Existing daemon socket
permissions determine which observations are available. Diagnostic commands use
fixed argv without a shell, bounded output and two-second timeouts.

## Delivery, revision receipts and probes

DeviceService publishes `{"action":"replace_connectivity_configuration",
"configuration":{...}}` at QoS 1 with retention to exactly
`factory/{site}/{line}/{gateway}/commands/connectivity`. The legacy retained
asset profile stays on the original `/commands` topic. Both routes use existing
`mqtt-topic-v1` identity encoding. Give gateway and provisioning accounts only
the required exact topic permissions; [cloud-acl.example](../raspberry-pi/mosquitto/cloud-acl.example)
shows the additional route.

The gateway validates and atomically saves configuration (mode 0600, fsync), then
reconciles workers. Equal revision + equal content is idempotent; stale revisions
or changed content at the same revision are rejected. The existing `/commands/ack`
reports application, and an ordinary telemetry observation with reserved
`connectionId:"gateway-configuration"` carries the applied revision even for an
empty/disabled configuration. Saving, publishing and edge application remain
distinct states. The reserved receipt does not contain tag values or diagnostics.

`probe_connectivity` is a separate non-retained request on `/commands`, with a
UUID `commandId`, matching `gatewayDeviceId`, expiry within 60 seconds and one
validated connection. It executes the same read-only drivers, reuses fresh active
results where possible and returns a telemetry report with that command ID. The
queue holds four pending probes with replay protection. Probes cannot invoke
methods or provide arbitrary executable/socket paths.

Per-connection workers isolate failures and reconnect with bounded exponential
backoff. RTU ports have exclusive lifetime ownership, including configuration
replacement and probes. Legacy polling ownership conflicts are rejected;
disable/remove the old owner before migrating a source. Cloud reporting is
separate from acquisition: defaults are at most one changed snapshot per 10
seconds and a full snapshot every 30 seconds. Environment overrides are bounded
to 5–300 seconds. MQTT/UADP subscriptions keep the latest received value; this is
a sampled monitoring stream, not an archive of every packet or control cycle.

Typed observations are carried in top-level `connectivity` with schema
`industrial-observation/v1`, outside the existing strict `integration` metadata.
Reports are split into at most 20 tags each and fit the existing 16 KiB telemetry
limit, including Unicode values. Batches have durable UUID event IDs, acquisition
timestamps, connection state, native quality, source timestamps, unit and revision.
Simulations explicitly carry `mode:"simulated"` and `scenarioRunId`; source
payloads cannot claim live mode. Existing SQLite spooling keeps report identity
unchanged across MQTT retries/restarts and drops queued entries only after QoS 1
acknowledgement. Queue exhaustion reports failure and retries at the bounded
report cadence.

## Reproducing qualification

Install the existing hash-pinned requirements; no new unpinned Python dependency
is introduced. Ordinary unit/regression tests require no protocol servers:

```bash
python -m unittest discover -s raspberry-pi/tests -p 'test_*.py'
```

Run the real protocol suite in a **separate Python process**, with an isolated
MQTT 5 broker listening only on `127.0.0.1:18884`:

```bash
CONNECTIVITY_PROTOCOL_TESTS=1 CONNECTIVITY_TEST_MQTT_PORT=18884 \
  python -m unittest discover -s raspberry-pi/tests -p 'test_connectivity_protocols.py' -v
```

It starts real loopback OPC UA/Modbus servers and a UDP sender on ephemeral ports.
It verifies monitored updates and native bad status, exact UInt64, positive FX
type discovery and negative ordinary-UA detection, MQTT retained/invalid/stale
messages, actual Modbus RTU bytes/CRC over an explicitly commissioned local
pseudo-terminal, UADP RawData/Variant/DataValue acquisition, pinned metadata, duplicate
sequence rejection and wraparound. Core tests cover malformed/truncated packets,
configuration boundaries, revision persistence, serial ownership, reporting
limits, durable restart identity and TSN absent/observed evidence. Legacy smoke
tests install process-global MQTT mocks, hence the separate protocol process.

For an actual gateway → MQTT → .NET outbox → Node database run, keep this
simulation bench running and publish its generated configuration through the
normal Industrial Devices UI/API:

```bash
python raspberry-pi/tests/connectivity_protocol_bench.py \
  --configuration-out /tmp/connectivity-bench.json \
  --gateway-id connectivity-e2e --mqtt-port 18884 --seconds 1800
```

The bench uses OPC UA14840, Modbus TCP15020, UDP14841, and the selected MQTT port.
Start `sensor_gateway.py` with `EDGE_ENV=development`, the matching gateway/site/
line, loopback MQTT with TLS disabled, `SERIAL_ENABLED=false`,
`SYNTHETIC_TELEMETRY_ENABLED=false`, `OPCUA_ALLOW_INSECURE=true`, and a temporary
`EDGE_STATE_DIR`, `ASSET_CONFIG_FILE`, `CONNECTIVITY_CONFIG_FILE`. The bench's
configuration is simulated and its small FX type tree is explicitly a test
model, not a certified device. Its encoder fixture documents and corrects
asyncua2.0.1's reserved UInt64 PublisherId header bits; the production parser
rejects that malformed header. A separate specification-derived vector prevents
the test encoder and production parser from sharing the same mistake.

Physical Modbus RTU, secured device certificates, real FX controllers, managed
switches, hardware clock accuracy and deterministic TSN traffic require the
actual commissioned environment. Software tests and loopback observations do
not constitute those results.

## Reference specifications

- [OPC 10000-81: UAFX connecting devices and information model](https://reference.opcfoundation.org/specs/OPC-10000-81)
- [OPC 10000-14: UADP network messages](https://reference.opcfoundation.org/specs/OPC-10000-14/7.2.4.4)
- [OPC 10000-14: DataSet messages](https://reference.opcfoundation.org/specs/OPC-10000-14/7.2.4.5)
- [OPC 10000-6: DataValue binary encoding](https://reference.opcfoundation.org/specs/OPC-10000-6/5.2.2.17)
- [LinuxPTP management client](https://github.com/richardcochran/linuxptp/blob/master/pmc.8)
- [Linux kernel timestamping](https://www.kernel.org/doc/html/latest/networking/timestamping.html)
