"""Bounded disk-backed telemetry queue and command replay protection."""
from __future__ import annotations
import hashlib
import os
import sqlite3
import time
from pathlib import Path
from threading import Lock
from types import SimpleNamespace

class DurableState:
    def __init__(self, path: str, max_messages: int = 50000):
        directory = Path(path).parent
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._db = sqlite3.connect(path, check_same_thread=False)
        os.chmod(path, 0o600)
        self._lock = Lock()
        self.max_messages = max_messages
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("CREATE TABLE IF NOT EXISTS telemetry (id TEXT PRIMARY KEY, topic TEXT NOT NULL, payload TEXT NOT NULL, queued_at REAL NOT NULL)")
        # Separate metadata preserves compatibility with the previous release's
        # four-column telemetry table and its rollback INSERT statements.
        self._db.execute("CREATE TABLE IF NOT EXISTS telemetry_qos (id TEXT PRIMARY KEY, queued_at REAL NOT NULL, qos INTEGER NOT NULL CHECK(qos BETWEEN 0 AND 2))")
        # A prior release can remove queued rows without knowing this table.
        self._db.execute("DELETE FROM telemetry_qos WHERE NOT EXISTS (SELECT 1 FROM telemetry WHERE telemetry.id=telemetry_qos.id AND telemetry.queued_at=telemetry_qos.queued_at)")
        self._db.execute("CREATE INDEX IF NOT EXISTS telemetry_queue_order ON telemetry (queued_at)")
        self._db.execute("CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, expires_at REAL NOT NULL)")
        self._db.commit()

    def enqueue(self, topic: str, payload: str, qos: int = 1) -> str:
        if type(qos) is not int or qos not in (0, 1, 2):
            raise ValueError("MQTT QoS must be 0, 1, or 2")
        if len(payload.encode('utf-8')) > 16384:
            raise ValueError("Telemetry exceeds the 16 KiB delivery limit")
        identity = hashlib.sha256((topic + "\n" + payload).encode('utf-8')).hexdigest()
        with self._lock, self._db:
            if self._db.execute("SELECT 1 FROM telemetry WHERE id=?", (identity,)).fetchone():
                return identity
            if self._db.execute("SELECT count(*) FROM telemetry").fetchone()[0] >= self.max_messages:
                raise RuntimeError("Durable telemetry queue is full; restore uplink connectivity")
            queued_at = time.time()
            self._db.execute("INSERT INTO telemetry VALUES (?, ?, ?, ?)", (identity, topic, payload, queued_at))
            self._db.execute("INSERT OR REPLACE INTO telemetry_qos VALUES (?, ?, ?)", (identity, queued_at, qos))
        return identity

    def first(self):
        with self._lock:
            return self._db.execute("SELECT id, topic, payload FROM telemetry ORDER BY queued_at, rowid LIMIT 1").fetchone()

    def first_with_qos(self):
        with self._lock:
            # A rollback can delete and reinsert an identical payload without
            # knowing about the metadata table. Match the original queue time
            # so that stale metadata can never change that legacy row's QoS.
            return self._db.execute("SELECT t.id, t.topic, t.payload, COALESCE(q.qos, 1) FROM telemetry t LEFT JOIN telemetry_qos q ON q.id=t.id AND q.queued_at=t.queued_at ORDER BY t.queued_at, t.rowid LIMIT 1").fetchone()

    def delivered(self, identity: str):
        with self._lock, self._db:
            self._db.execute("DELETE FROM telemetry WHERE id=?", (identity,))
            self._db.execute("DELETE FROM telemetry_qos WHERE id=?", (identity,))

    def reserve_command(self, identity: str, expires_at_ms: int) -> bool:
        # Reserve before the physical write. A crash cannot turn MQTT replay into
        # a second movement. A reserved write with no ack must be investigated.
        with self._lock, self._db:
            self._db.execute("DELETE FROM commands WHERE expires_at < ?", (time.time() - 60,))
            result = self._db.execute("INSERT OR IGNORE INTO commands VALUES (?, ?)", (identity, expires_at_ms / 1000))
            return result.rowcount == 1

    def close(self):
        with self._lock:
            self._db.close()

class DurablePublisher:
    def __init__(self, client, state: DurableState):
        self.client = client
        self.state = state

    def publish(self, topic, payload, qos=1, retain=False):
        if retain or type(qos) is not int or qos not in (0, 1, 2):
            raise ValueError("Delivery accepts non-retained MQTT QoS 0, 1, or 2")
        try:
            self.state.enqueue(topic, payload, qos)
            return SimpleNamespace(rc=0)
        except (RuntimeError, OSError, sqlite3.Error) as error:
            print(f"[DELIVERY] {error}")
            return SimpleNamespace(rc=1)

    def drain_one(self) -> bool:
        item = self.state.first_with_qos()
        if item is None or not self.client.is_connected():
            return False
        identity, topic, payload, qos = item
        if topic.startswith("factory/"):
            from mqtt_topics import decode_level,topic as encoded_topic
            parts=topic.split("/")
            if len(parts)==5:
                try:topic=encoded_topic(*(decode_level(p) for p in parts[1:4]),parts[4])
                except ValueError:
                    print("[DELIVERY] Queued topic requires identity/route review; evidence retained")
                    return False
        message = self.client.publish(topic, payload, qos=qos, retain=False)
        if message.rc != 0:
            return False
        try:
            # Paho confirms a socket write for QoS 0, PUBACK for QoS 1 and
            # PUBCOMP for QoS 2. QoS 0 therefore cannot prove broker receipt.
            message.wait_for_publish(timeout=5)
            if not message.is_published():
                return False
        except (RuntimeError, ValueError):
            return False
        self.state.delivered(identity)
        return True

    def run(self, stop):
        while not stop.is_set():
            try:
                if self.drain_one():
                    continue
            except (RuntimeError, OSError, sqlite3.Error) as error:
                print(f"[DELIVERY] Retry pending: {error}")
            stop.wait(1)
