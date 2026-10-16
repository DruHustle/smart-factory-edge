import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from telemetry_policy import TelemetryPolicy


class TelemetryPolicyTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.policy = TelemetryPolicy(clock=lambda: self.now, wall_clock=lambda: 1_800_000_000 + self.now)
        self.topic = "factory/a/line/esp32-wrover-01/telemetry"

    def sample(self, **values):
        return {"deviceId": "esp32-wrover-01", "timestamp": int((1_800_000_000 + self.now) * 1000),
                "temperature": 19.5, "humidity": 51, "sensorStatus": "ok",
                "assetSignals": {"buttonPressed": 0, "buttonPressCount": 28}, **values}

    def send(self, sample):
        self.assertTrue(self.policy.should_forward(self.topic, sample))
        self.policy.accepted(self.topic, sample, 200, forwarded=True)

    def test_identical_samples_are_suppressed_until_full_snapshot(self):
        self.send(self.sample())
        for self.now in range(5, 60, 5):
            self.assertFalse(self.policy.should_forward(self.topic, self.sample()))
        self.now = 60
        self.assertTrue(self.policy.should_forward(self.topic, self.sample()))

    def test_small_measurement_changes_faults_and_button_edges_are_immediate(self):
        self.send(self.sample())
        for self.now, changes in enumerate([
            {"temperature": 19.6}, {"temperature": 19.5}, {"humidity": 52},
            {"sensorStatus": "read_error", "temperature": None, "humidity": None},
            {"assetSignals": {"buttonPressed": 1, "buttonPressCount": 29}},
            {"assetSignals": {"buttonPressed": 0, "buttonPressCount": 29}},
        ], 1):
            self.send(self.sample(**changes))

    def test_online_heartbeat_is_redundant_but_offline_event_is_not(self):
        self.send(self.sample())
        heartbeat = self.topic.replace("telemetry", "heartbeat")
        self.now = 5
        self.assertFalse(self.policy.should_forward(heartbeat, self.sample(status="online")))
        self.assertTrue(self.policy.should_forward(heartbeat, self.sample(status="offline")))
        self.now = 60
        self.assertTrue(self.policy.should_forward(heartbeat, self.sample(status="online")))

    def test_history_and_out_of_order_samples_are_preserved_without_resetting_liveness(self):
        self.send(self.sample())
        self.now = 60
        old = self.sample(timestamp=1_799_990_000_000)
        self.assertTrue(self.policy.should_forward(self.topic, old))
        self.policy.accepted(self.topic, old, 200, forwarded=True)
        self.assertTrue(self.policy.should_forward(self.topic, self.sample()))
        self.assertTrue(self.policy.should_forward(self.topic, self.sample(timestamp=1_800_000_000_000)))

    def test_queue_failure_does_not_advance_baseline(self):
        self.send(self.sample())
        self.now = 5
        changed = self.sample(temperature=20)
        self.assertTrue(self.policy.should_forward(self.topic, changed))
        # Queue failed, so accepted() must not be called; retry still forwards.
        self.assertTrue(self.policy.should_forward(self.topic, changed))

    def test_restart_sends_full_snapshot_and_assets_have_separate_baselines(self):
        self.send(self.sample(assetId="asset-a"))
        self.now = 5
        self.send(self.sample(assetId="asset-b"))
        self.assertTrue(TelemetryPolicy().should_forward(self.topic, self.sample()))

    def test_disabled_policy_forwards_every_sample(self):
        self.policy.enabled = False
        self.send(self.sample())
        self.now = 5
        self.send(self.sample())

    def test_asset_churn_is_bounded_and_evicted_asset_gets_full_snapshot(self):
        for index in range(257):
            self.send(self.sample(assetId="asset-" + str(index)))
        self.now = 5
        self.assertTrue(self.policy.should_forward(self.topic, self.sample(assetId="asset-0")))
        self.assertLessEqual(len(self.policy._samples), 256)

    def test_stable_minute_sends_one_of_twelve_samples_and_no_redundant_heartbeats(self):
        forwarded = []
        for self.now in range(0, 60, 5):
            for topic, record in [(self.topic, self.sample()),
                                  (self.topic.replace("telemetry", "heartbeat"),
                                   {"deviceId": "esp32-wrover-01", "timestamp": self.sample()["timestamp"], "status": "online"})]:
                send = self.policy.should_forward(topic, record)
                self.policy.accepted(topic, record, 200, forwarded=send)
                if send:
                    forwarded.append(topic)
        self.assertEqual(forwarded, [self.topic])
        self.assertEqual(self.policy._counts["received"], 24)
        self.assertEqual(self.policy._counts["suppressed"], 23)


if __name__ == "__main__":
    unittest.main()
