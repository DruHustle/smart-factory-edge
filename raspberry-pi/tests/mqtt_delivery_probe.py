"""Isolated real-broker probe. Never point this at a production broker.

Run with MQTT_TEST_PORT set to a disposable, anonymous loopback broker port.
"""
import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from durable_state import DurablePublisher, DurableState
import sensor_gateway as gateway


def wait_for(condition, label, seconds=8):
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() >= deadline:
            raise AssertionError("Timed out: " + label)
        time.sleep(0.02)


def main():
    port = int(os.environ["MQTT_TEST_PORT"])
    if port == 1883:
        raise ValueError("Use a disposable nonstandard loopback broker port")
    identity = "delivery-probe-" + uuid.uuid4().hex[:10]
    clients = []

    def connect(client):
        clients.append(client)
        deadline = time.monotonic() + 8
        while True:
            try:
                if getattr(client,"_protocol",None)==mqtt.MQTTv5:
                    gateway.connect_local_mqtt(client,SimpleNamespace(local_mqtt_host="127.0.0.1",local_mqtt_port=port))
                else:client.connect("127.0.0.1", port)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        client.loop_start()
        wait_for(client.is_connected, "broker connection")
        return client

    def new_client(suffix):
        return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=identity + suffix)

    try:
        received = []
        subscribed = []
        receiver = new_client("-receiver")
        receiver.on_connect = lambda c, *_: c.subscribe("probe-cloud/#", qos=1)
        receiver.on_subscribe = lambda *_: subscribed.append(True)
        receiver.on_message = lambda c, u, m: received.append(json.loads(m.payload))
        connect(receiver)
        wait_for(lambda: subscribed, "cloud observer subscription")
        uplink = new_client("-uplink")
        # A prefix separates local and cloud traffic inside this test broker.
        cloud = SimpleNamespace(is_connected=uplink.is_connected,
            publish=lambda topic, *a, **kw: uplink.publish("probe-cloud/" + topic, *a, **kw))
        producer = connect(new_client("-sensor"))
        settings = SimpleNamespace(local_mqtt_client_id=identity + "-gateway", local_mqtt_username="",
            local_mqtt_use_tls=False, local_mqtt_device_ids=("probe-wrover",),
            site_id="probe", line_id="line", device_id="probe-pi", telemetry_coalescing_enabled=False)
        topic = "factory/probe/line/probe-wrover/telemetry"
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "queue.sqlite3")
            state = DurableState(path, max_messages=1)
            publisher = DurablePublisher(cloud, state)
            acked = []
            replayed = []

            def ingress():
                ready = []
                local = gateway.make_local_mqtt_client(settings, publisher)
                original_subscribe=local.on_subscribe
                def subscribed(*args):
                    original_subscribe(*args)
                    if local._transport_verified:ready.append(True)
                local.on_subscribe=subscribed
                original_ack = local.ack
                local.ack = lambda mid, qos: (acked.append(mid), original_ack(mid, qos))[1]
                original_message = local.on_message

                def message(c, u, m):
                    replayed.append(bool(m.dup))
                    original_message(c, u, m)

                local.on_message = message
                connect(local)
                wait_for(lambda: ready, "local persistent subscription")
                return local

            def publish(temperature, timestamp):
                record = {"deviceId": "probe-wrover", "timestamp": timestamp, "temperature": temperature}
                info = producer.publish(topic, json.dumps(record), qos=1)
                info.wait_for_publish(timeout=5)
                assert info.is_published()

            local = ingress()
            stamp = int(time.time() * 1000)
            publish(20, stamp)
            wait_for(lambda: len(acked) == 1, "ACK after durable acceptance during cloud outage")
            assert state.first() is not None
            assert not publisher.drain_one(), "Disconnected uplink must leave queue intact"
            publish(21, stamp + 1)
            wait_for(gateway.INGRESS_FAILED.is_set, "queue-full failure")
            assert len(acked) == 1, "Queue-full message must not be acknowledged"
            local.disconnect()
            local.loop_stop()

            # Real uplink reconnect and PUBACK release the first queue slot.
            connect(uplink)
            assert publisher.drain_one()
            wait_for(lambda: len(received) == 1, "first recovered cloud delivery")
            assert received[0]["timestamp"] == stamp
            gateway.INGRESS_FAILED.clear()
            gateway.STOP.clear()
            local = ingress()
            wait_for(lambda: len(acked) == 2, "unacknowledged message replay after gateway restart")
            assert any(replayed), "Broker must redeliver the unacknowledged QoS 1 message"
            assert json.loads(state.first()[2])["timestamp"] == stamp + 1

            # Persisted queue also survives a database/gateway reopen.
            local.disconnect()
            local.loop_stop()
            state.close()
            state = DurableState(path, max_messages=1)
            publisher = DurablePublisher(cloud, state)
            assert publisher.drain_one()
            wait_for(lambda: len(received) == 2, "delivery after durable queue reopen")
            assert [r["temperature"] for r in received] == [20, 21]
            assert [r["timestamp"] for r in received] == [stamp, stamp + 1]
            assert state.first() is None
            state.close()
            print("PASS: cloud outage buffering, queue-full withholding of ACK, real broker redelivery, queue reopen, original timestamps, PUBACK-based drain")
    finally:
        for client in reversed(clients):
            client.disconnect()
            client.loop_stop()


if __name__ == "__main__":
    main()
