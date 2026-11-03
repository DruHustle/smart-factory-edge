"""Exercise live gateway resubscription with a disposable loopback broker."""
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
if port == 1883 or not 1024 < port < 65536: raise ValueError('A disposable nonstandard loopback broker is required')
name = 'qos-reconfigure-' + uuid.uuid4().hex[:8]
clients = []
def wait(predicate, label):
    deadline = time.monotonic() + 10
    while not predicate():
        if time.monotonic() >= deadline: raise AssertionError('Timed out: ' + label)
        time.sleep(.02)
def start(client, local=False):
    clients.append(client)
    if local: gateway.connect_local_mqtt(client, SimpleNamespace(local_mqtt_host='127.0.0.1', local_mqtt_port=port))
    else: client.connect('127.0.0.1', port, 60)
    client.loop_start(); wait(client.is_connected, 'connection'); return client

settings = SimpleNamespace(site_id='probe', line_id='line', device_id='gateway', mqtt_client_id=name+'-cloud',
    mqtt_username='', mqtt_password='', mqtt_use_tls=False, local_mqtt_client_id=name+'-local', local_mqtt_username='',
    local_mqtt_password='', local_mqtt_use_tls=False, local_mqtt_device_ids=('wrover',), local_mqtt_control_device_ids=('wrover',),
    mqtt_uplink_qos=1, mqtt_downlink_qos=1, telemetry_coalescing_enabled=False)
try:
    gateway.STOP.clear(); gateway.INGRESS_FAILED.clear()
    sensor = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=name+'-sensor')
    commands, grants = [], []
    sensor.on_connect = lambda client, *_: client.subscribe('factory/probe/line/wrover/commands', qos=2)
    sensor.on_subscribe = lambda _c,_u,_mid,codes,_p: grants.extend(codes)
    sensor.on_message = lambda _c,_u,msg: commands.append((json.loads(msg.payload), msg.qos))
    start(sensor); wait(lambda: grants, 'sensor subscription')
    with tempfile.TemporaryDirectory() as directory:
        state = DurableState(str(Path(directory) / 'gateway.sqlite3'))
        cloud = None
        local = None
        cloud = start(gateway.make_mqtt_client(settings, relay_control=lambda command: gateway.relay_gpio_command(settings, local, command), relay_qos=lambda command: gateway.apply_gateway_qos(settings, state, cloud, local, command)))
        # Separate cloud topic namespace prevents the local fixture from consuming its own uplink.
        cloud_publisher = SimpleNamespace(is_connected=cloud.is_connected, publish=lambda topic,*a,**kw: cloud.publish('cloud/'+topic,*a,**kw))
        publisher = DurablePublisher(cloud_publisher, state)
        local = start(gateway.make_local_mqtt_client(settings, publisher), local=True)
        wait(lambda: local._transport_verified, 'initial local grants')
        for revision, up, down in ((1,0,2),(2,2,0),(3,1,1)):
            command = {'schemaVersion':1,'action':'configure_mqtt_qos','commandId':f'configure-{revision:04}',
                'targetDeviceId':'wrover','revision':revision,'uplinkQos':up,'downlinkQos':down,'expiresAt':int(time.time()*1000)+30000}
            sent_command = sensor.publish('factory/probe/line/gateway/commands', json.dumps(command), qos=1, retain=False)
            sent_command.wait_for_publish(5)
            wait(lambda: len(commands) >= revision * 2 - 1, 'device configuration request')
            wait(lambda: not any(thread.is_alive() for thread in cloud._qos_threads), 'QoS request completion')
            assert state.qos_policy() == (revision,up,down)
            assert (settings.mqtt_uplink_qos, settings.mqtt_downlink_qos) == (up,down)
            pulse = {'schemaVersion':1,'action':'set_gpio','commandId':f'gpio-probe-{revision:04}',
                'targetDeviceId':'wrover','pin':18,'value':1,'holdMs':50,'expiresAt':int(time.time()*1000)+5000}
            sensor.publish('factory/probe/line/gateway/commands',json.dumps(pulse),qos=down,retain=False).wait_for_publish(5)
            wait(lambda: len(commands) >= revision * 2, 'device command relay')
            assert commands[-1][0]['commandId'] == pulse['commandId'] and commands[-1][1] == down
            sample = {'deviceId':'wrover','timestamp':int(time.time()*1000),'temperature':20+revision}
            sent = sensor.publish('factory/probe/line/wrover/telemetry',json.dumps(sample),qos=2)
            sent.wait_for_publish(5)
            wait(lambda: state.first_with_qos() is not None, 'gateway telemetry queue')
            assert state.first_with_qos()[3] == up
            state.delivered(state.first()[0])
        state.close()
    print('PASS: live gateway broker grants, QoS reconfiguration, queued telemetry and durable policy')
finally:
    for client in reversed(clients): client.disconnect(); client.loop_stop()
    gateway.STOP.clear(); gateway.INGRESS_FAILED.clear()
