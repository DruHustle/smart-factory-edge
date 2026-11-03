"""Verify QoS on both gateway hops against a disposable loopback broker.

Requires MQTT_TEST_PORT; never use a production listener.
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
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import sensor_gateway as gateway
from durable_state import DurablePublisher, DurableState

port = int(os.environ['MQTT_TEST_PORT'])
if port == 1883 or not 1024 < port < 65536:
    raise ValueError('Use a disposable, nonstandard loopback broker')
identity = 'qos-probe-' + uuid.uuid4().hex[:12]
clients = []

def wait_for(predicate, label):
    deadline = time.monotonic() + 8
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError('Timed out waiting for ' + label)
        time.sleep(.02)

def connect(client):
    clients.append(client)
    client.connect('127.0.0.1', port, 60)
    client.loop_start()
    wait_for(client.is_connected, 'connection')
    return client

def observer(client_id, topic):
    received, grants = [], []
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=identity + client_id)
    c.on_connect = lambda client, *_: client.subscribe(topic, qos=2)
    c.on_subscribe = lambda _c, _u, _mid, codes, _p: grants.extend(codes)
    c.on_message = lambda _c, _u, msg: received.append((msg.qos, json.loads(msg.payload)))
    connect(c)
    wait_for(lambda: grants, 'subscription grant')
    if grants[0].is_failure or grants[0].value != 2:
        raise AssertionError('Broker did not grant QoS 2')
    return received

try:
    received = observer('-cloud', 'probe-cloud/#')
    commands = observer('-device', 'factory/probe/line/wrover/commands')
    producer = connect(mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=identity + '-sender'))
    with tempfile.TemporaryDirectory() as directory:
        state = DurableState(str(Path(directory) / 'queue.sqlite3'))
        cloud = SimpleNamespace(is_connected=producer.is_connected,
            publish=lambda topic, *args, **kwargs: producer.publish('probe-cloud/' + topic, *args, **kwargs))
        publisher = DurablePublisher(cloud, state)
        settings = SimpleNamespace(site_id='probe', line_id='line', device_id='gateway',
            local_mqtt_device_ids=('wrover',), local_mqtt_control_device_ids=('wrover',))
        for level in (0, 1, 2):
            settings.mqtt_uplink_qos = level
            settings.mqtt_downlink_qos = level
            sample = {'deviceId': 'wrover', 'timestamp': int(time.time() * 1000), 'temperature': 20 + level}
            message = SimpleNamespace(topic='factory/probe/line/wrover/telemetry', payload=json.dumps(sample).encode(), retain=False)
            if not gateway.forward_local_message(settings, publisher, message):
                raise AssertionError('Telemetry was rejected')
            if not publisher.drain_one():
                raise AssertionError('Queued telemetry was not published')
            wait_for(lambda: len(received) > level, 'cloud delivery')
            if received[level][0] != level or received[level][1]['temperature'] != 20 + level:
                raise AssertionError('Cloud QoS or payload did not match')
            command = {'schemaVersion': 1, 'action': 'set_gpio', 'commandId': 'probe-command-' + str(level),
                'targetDeviceId': 'wrover', 'pin': 18, 'value': 1, 'holdMs': 50,
                'expiresAt': int(time.time() * 1000) + 5000}
            gateway.relay_gpio_command(settings, producer, command)
            wait_for(lambda: len(commands) > level, 'device command')
            if commands[level][0] != level or commands[level][1]['commandId'] != command['commandId']:
                raise AssertionError('Device QoS or command did not match')
        state.close()
    print('PASS: gateway uplink and downlink delivered QoS 0, 1 and 2 through the disposable broker')
finally:
    for client in clients:
        client.disconnect()
        client.loop_stop()
