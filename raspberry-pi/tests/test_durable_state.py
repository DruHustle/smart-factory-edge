import sys
import tempfile
import unittest
import time
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from durable_state import DurableState, DurablePublisher

class DurableStateTests(unittest.TestCase):
    def test_gateway_qos_policy_is_durable_and_rejects_stale_revisions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "state.sqlite3")
            state = DurableState(path)
            self.assertIsNone(state.qos_policy())
            state.save_qos_policy(3, 2, 0)
            state.close()
            state = DurableState(path)
            self.assertEqual(state.qos_policy(), (3, 2, 0))
            state.save_qos_policy(3, 2, 0)
            for values in ((2, 1, 1), (3, 1, 1), (4, 3, 1), (4, True, 1)):
                with self.assertRaises(ValueError): state.save_qos_policy(*values)
            self.assertEqual(state.qos_policy(), (3, 2, 0))
            state.close()

    def test_qos_is_recorded_per_message_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "state.sqlite3")
            state = DurableState(path)
            for qos in (0, 1, 2):
                state.enqueue("test/telemetry", f"sample-{qos}", qos=qos)
            state.close()
            state = DurableState(path)
            sent = []
            client = SimpleNamespace(is_connected=lambda: True, publish=lambda topic, payload, **opts:
                sent.append((payload, opts["qos"])) or SimpleNamespace(rc=0, wait_for_publish=lambda **_:None, is_published=lambda:True))
            publisher = DurablePublisher(client, state)
            self.assertTrue(all(publisher.drain_one() for _ in range(3)))
            self.assertEqual(sent, [("sample-0", 0), ("sample-1", 1), ("sample-2", 2)])
            self.assertEqual(state._db.execute("SELECT count(*) FROM telemetry_qos").fetchone()[0], 0)
            state.close()

    def test_legacy_queue_rows_default_to_qos_one_and_reject_invalid_levels(self):
        with tempfile.TemporaryDirectory() as directory:
            state = DurableState(str(Path(directory) / "state.sqlite3"))
            state._db.execute("INSERT INTO telemetry VALUES (?, ?, ?, ?)", ("legacy", "test", "old", 0))
            state._db.commit()
            self.assertEqual(state.first_with_qos(), ("legacy", "test", "old", 1))
            for qos in (-1, 3, True, "1"):
                with self.assertRaises(ValueError):
                    state.enqueue("test", "value", qos=qos)
            state.close()

    def test_rollback_reinserting_same_payload_does_not_reuse_stale_qos(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "state.sqlite3")
            state = DurableState(path)
            identity = state.enqueue("test", "payload", qos=2)
            queued_at = state._db.execute("SELECT queued_at FROM telemetry WHERE id=?", (identity,)).fetchone()[0]
            # Simulate the previous release deleting and re-enqueuing without
            # touching the new metadata table.
            state._db.execute("DELETE FROM telemetry WHERE id=?", (identity,))
            state._db.execute("INSERT INTO telemetry VALUES (?, ?, ?, ?)", (identity, "test", "payload", queued_at + 1))
            state._db.commit()
            state.close()
            state = DurableState(path)
            self.assertEqual(state.first_with_qos(), (identity, "test", "payload", 1))
            self.assertEqual(state._db.execute("SELECT count(*) FROM telemetry_qos").fetchone()[0], 0)
            state.close()

    def test_old_dotted_topics_reopen_and_publish_without_renaming_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/"state.sqlite3")
            payload='{"deviceId":"sensor.temp:01","temperature":42}'
            state=DurableState(path);state.enqueue("factory/site/line.01/sensor.temp:01/telemetry",payload);state.close()
            state=DurableState(path);published=[]
            def send(topic,body,**_):
                published.append((topic,body));return SimpleNamespace(rc=0,wait_for_publish=lambda **_:None,is_published=lambda:True)
            client=SimpleNamespace(is_connected=lambda:True,publish=send)
            self.assertTrue(DurablePublisher(client,state).drain_one())
            self.assertEqual(published,[("factory/site/line%2E01/sensor%2Etemp:01/telemetry",payload)])
            self.assertIsNone(state.first());state.close()

    def test_queue_and_command_reservation_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "delivery.sqlite3")
            state = DurableState(path)
            identity = state.enqueue("test/telemetry", '{"temperature":42}')
            self.assertTrue(state.reserve_command("command-0001", 9999999999999))
            state.close()
            state = DurableState(path)
            self.assertEqual(state.first()[0], identity)
            self.assertFalse(state.reserve_command("command-0001", 9999999999999))
            state.delivered(identity)
            self.assertIsNone(state.first())
            self.assertEqual(Path(path).stat().st_mode & 0o777, 0o600)
            state.close()

    def test_bounded_queue_never_silently_removes_undelivered_data(self):
        with tempfile.TemporaryDirectory() as directory:
            state = DurableState(str(Path(directory) / "state.sqlite3"), max_messages=1)
            state.enqueue("test", "first")
            state.enqueue("test", "first")
            with self.assertRaisesRegex(RuntimeError, "queue is full"):
                state.enqueue("test", "second")
            self.assertEqual(state.first()[2], "first")
            state.close()

    def test_spool_removes_only_acknowledged_messages(self):
        with tempfile.TemporaryDirectory() as directory:
            state = DurableState(str(Path(directory) / "state.sqlite3"))
            acknowledged = [False]
            client = SimpleNamespace(is_connected=lambda: True, publish=lambda *a, **k:
                SimpleNamespace(rc=0, wait_for_publish=lambda **k: None, is_published=lambda: acknowledged[0]))
            publisher = DurablePublisher(client, state)
            self.assertEqual(publisher.publish("test/telemetry", "sample").rc, 0)
            self.assertFalse(publisher.drain_one())
            self.assertIsNotNone(state.first())
            acknowledged[0] = True
            self.assertTrue(publisher.drain_one())
            self.assertIsNone(state.first())
            state.close()

class ControllerReplayTests(unittest.TestCase):
    def test_replayed_command_after_restart_does_not_reopen_controller(self):
        from ada031_control import Ada031SerialCommandManager
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "state.sqlite3")
            asset = {"assetId": "urn:test:arm", "endpoint": "serial:///dev/test?baudrate=9600"}
            command = {"commandId": "review-replay-0001", "joint": "base", "direction": "increase", "expiresAt": int(time.time() * 1000) + 30000}
            opens = []
            writes = []
            port = SimpleNamespace(write=lambda value: writes.append(value) or 1, flush=lambda: None, close=lambda: None)
            state = DurableState(path)
            manager = Ada031SerialCommandManager(serial_factory=lambda *a, **k: opens.append(True) or port, sleeper=lambda _: None, command_state=state)
            self.assertEqual(manager.execute(asset, command, b'o')["status"], "serial_write_accepted")
            manager.close()
            state.close()
            state = DurableState(path)
            manager = Ada031SerialCommandManager(serial_factory=lambda *a, **k: opens.append(True) or port, sleeper=lambda _: None, command_state=state)
            self.assertEqual(manager.execute(asset, command, b'o')["status"], "duplicate_ignored")
            self.assertEqual(opens, [True])
            self.assertEqual(writes, [b'o'])
            manager.close()
            state.close()

if __name__ == "__main__": unittest.main()
