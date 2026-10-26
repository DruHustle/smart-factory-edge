# Optional passive security collector

The collector is disabled by default. It observes this gateway's existing authorized paths, needs no peer platform and introduces no scan, probe, packet capture, MQTT subscriber or remote remediation. Do not install it on plant hardware as part of an API deployment. The ordinary telemetry/heartbeat/control authorization paths and quotas remain unchanged.

## Manual staging commissioning

1. Review the gateway's local registered device scope in Smart Factory Cybersecurity. An individual admin creates a source with namespace matching `SECURITY_PRODUCER_NAMESPACE`, the authorized device/application target, intended modes, allowed types and an expected interval of 300 seconds. Configure `mqtt-cloud` as a connection reference and, if applicable, `local-mqtt` with reviewed hashed allowed identities. A source event never chooses a local target/user/site.
2. Store the one-time source key in the existing owner-only environment file, not source control. TLS verification is mandatory in deployed configuration. The origin is an explicit HTTPS origin with no userinfo, path, query or fragment; the fixed destination is `/api/security/observations`. Redirects are not followed.
3. Set `SECURITY_COLLECTOR_ENABLED=true`, `SECURITY_API_ORIGIN`, `SECURITY_SOURCE_TOKEN`, `SECURITY_PRODUCER_NAMESPACE`; optional `SECURITY_INTERVAL_SECONDS=300`. Start with `SECURITY_MODE=simulated` and a bounded `SECURITY_RUN_ID` on a disposable/staging gateway. Explicitly review a move to `live`; do not relabel queued simulation as live.
4. Verify ordinary telemetry still delivers, security receipts appear under the correct source/mode, source outages become coverage findings, and a reconnect drains both independently bounded queues. Review declared versus runtime facts and certificate fingerprint/connection mapping. No label proves a protected network path, broker ACL or device attestation.
5. After staging acceptance and the hardware owner's approval, commission a physical gateway manually under its established service procedure. This release does not flash firmware or change broker ACLs. To roll back, disable the collector and restart the existing gateway service under that procedure; preserve the SQLite file and pending security rows for review. No API/worker rollback requires a plant rollout.

Supported observations: health (existing socket absent means degraded), TLS type of the already established Paho socket, public peer certificate dates/fingerprint from that socket, successful applied configuration revision/hash, and rejected identity on the configured local telemetry/heartbeat input. Configuration hashes are self-reported; values and private keys are not transmitted. Certificate/transport observations refresh at least every six hours while connected; the reviewed configuration hash persists across restart. Unknown local identity labels are hashed; a payload identity is not physical attestation.

## Queue, retries and diagnostics

The existing `delivery.sqlite3` contains a separate `security_evidence` table capped at 1,000 pending/quarantined rows. Telemetry's existing queue capacity is unchanged. Events have immutable UUIDs, explicit mode/run and original UTC occurrence time; IDs/content survive reconnects and reopen. Payloads are strict owned envelopes ≤8,192 UTF-8 bytes. One event is posted per request; only an explicit 202 accepted/duplicate receipt for that ID deletes it. HTTPS requests have a five-second timeout, bounded 65,536-byte receipt and no redirects. Permanent 400/401/403/409/413/415/422/redirect failures quarantine; transient errors back off 2–300 seconds, at most 12 attempts, then quarantine. Queue full is explicit and leaves telemetry intact. Unacknowledged evidence is never silently evicted. Invalid opt-in configuration logs a bounded unconfigured message and preserves telemetry.

For local owner inspection with the gateway stopped under its service procedure:

```sql
SELECT id,queued_at,attempts,next_attempt,quarantined_at,error_code
FROM security_evidence ORDER BY queued_at LIMIT 100;
```

Do not print payloads or environment tokens in shared logs. Correct a rejected source's credential/scope after reviewing its receiver error. To authorize retry, record the decision in the gateway's established commissioning log and reset only the selected queue row's attempts, next_attempt and quarantined_at using an owner-controlled SQLite session. Never change its ID/payload/mode. A permanent schema conflict needs evidence review, not endless retries. The API's native-outbox retry UI does not pretend to administer a disconnected Pi's local database.

## Disposable proof

`python -m unittest discover -s raspberry-pi/tests` covers validation, disabled/unconfigured behavior, durable reopen, independent queue limits, permanent quarantine, transient recovery and existing-socket evidence. The existing real-MQTT outage/restart probe remains mandatory and continues to verify normal telemetry.

`raspberry-pi/scripts/security-workspace-smoke.py` accepts `{origin,token,namespace}` on stdin for a separately running **local** disposable Node security workspace. It simulates an unreachable receiver, preserves an original event across SQLite reopen, delivers through the real HTTP ingestion/validator/rule path after recovery and checks ordinary telemetry remains queued. It also verifies the applied configuration hash survives reopen. It never connects to a broker or physical device. Its explicit loopback HTTP exception is test-only (`SECURITY_ALLOW_LOCAL_HTTP=true`); do not enable it in a deployed environment.

This checkout owns its collector/validator/tests; the Smart Factory Node checkout owns the receiver. No shared custom schema package, peer account or coordinated external release is required. Neutral events use `ot-security-evidence/v1`; the explicit local transport extension uses `smart-factory-ot-observation/v1`. External consumer acceptance and physical hardware acceptance remain separate.
