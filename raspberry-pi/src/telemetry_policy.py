"""Reduce repeated live sensor traffic without hiding measurement or event changes."""
from __future__ import annotations

import json
import time
from collections import OrderedDict


class TelemetryPolicy:
    def __init__(self, enabled=True, snapshot_seconds=60, clock=time.monotonic, wall_clock=time.time):
        self.enabled = enabled
        self.snapshot_seconds = snapshot_seconds
        self.clock = clock
        self.wall_clock = wall_clock
        self._samples = OrderedDict()
        self._activity = {}
        self._reported_at = clock()
        self._counts = dict(received=0, forwarded=0, suppressed=0, input_payload_bytes=0, forwarded_payload_bytes=0)

    def _live(self, record):
        # Replayed history must not be mistaken for current proof of life or
        # discarded as redundant. Preserve its original timestamp and payload.
        age = self.wall_clock() - record["timestamp"] / 1000
        return -5 <= age <= 15

    def should_forward(self, topic, record):
        if not self.enabled or not self._live(record):
            return True
        device = record["deviceId"]
        if topic.endswith("/heartbeat"):
            return record["status"] != "online" or self.clock() - self._activity.get(device, -float("inf")) >= self.snapshot_seconds
        key = (device, record.get("assetId"))
        previous = self._samples.get(key)
        if previous is None:
            return True
        timestamp, sent_at, values = previous
        if record["timestamp"] <= timestamp:
            return True
        # Exact comparison deliberately preserves *every* measurement change:
        # alarm thresholds can be changed in the dashboard independently.
        current = {k: v for k, v in record.items() if k != "timestamp"}
        return current != values or self.clock() - sent_at >= self.snapshot_seconds

    def accepted(self, topic, record, payload_bytes, forwarded):
        """Advance a baseline only after durable acceptance, never on failure."""
        self._counts["received"] += 1
        self._counts["input_payload_bytes"] += payload_bytes
        self._counts["forwarded" if forwarded else "suppressed"] += 1
        if not forwarded:
            return
        self._counts["forwarded_payload_bytes"] += payload_bytes
        if not self._live(record):
            return
        device = record["deviceId"]
        if topic.endswith("/telemetry"):
            key = (device, record.get("assetId"))
            previous = self._samples.get(key)
            if previous is not None and record["timestamp"] <= previous[0]:
                return
            self._samples[key] = (record["timestamp"], self.clock(), {k: v for k, v in record.items() if k != "timestamp"})
            self._samples.move_to_end(key)
            # Payloads may name assets; bound memory even if an authenticated
            # sensor sends a succession of previously unseen asset identities.
            if len(self._samples) > 256:
                self._samples.popitem(last=False)
        self._activity[device] = self.clock()

    def report_if_due(self):
        if self.clock() - self._reported_at < 60:
            return
        print("[TELEMETRY POLICY] " + json.dumps({"window_seconds": round(self.clock() - self._reported_at, 1), **self._counts}, separators=(",", ":")), flush=True)
        self._reported_at = self.clock()
        self._counts = dict.fromkeys(self._counts, 0)
