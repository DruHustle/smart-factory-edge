import sys
import tempfile
import unittest
import time
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from durable_state import DurableState, DurablePublisher

class DurableStateTests(unittest.TestCase):
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
