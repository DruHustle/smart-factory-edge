import json
import sys
import types
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from sensor_gateway import forward_local_message, parse_serial_line
class EquipmentProvenanceTests(unittest.TestCase):
 def settings(self):return types.SimpleNamespace(device_id='gateway',site_id='fixture',line_id='line',local_mqtt_device_ids=('sensor',))
 def message(self,mode='simulated',run=True):
  p={'deviceId':'sensor','timestamp':1792486800000,'temperature':45,'integration':{'mode':mode,'gatewayReceivedAt':'2026-10-20T09:00:00Z','retained':False}}
  if run:p['integration']['scenarioRunId' if mode=='simulated' else 'replayRunId']='stable-run'
  return types.SimpleNamespace(topic='factory/fixture/line/sensor/telemetry',payload=json.dumps(p).encode(),retain=True)
 def test_preserves_time_classification_and_retained_provenance(self):
  output=[];publisher=types.SimpleNamespace(publish=lambda topic,payload,**kw:output.append(json.loads(payload)) or types.SimpleNamespace(rc=0))
  self.assertTrue(forward_local_message(self.settings(),publisher,self.message()))
  self.assertEqual(output[0]['timestamp'],1792486800000);self.assertEqual(output[0]['integration']['mode'],'simulated');self.assertEqual(output[0]['integration']['scenarioRunId'],'stable-run');self.assertTrue(output[0]['integration']['retained'])
 def test_missing_scenario_or_replay_identity_is_rejected(self):
  publisher=types.SimpleNamespace(publish=lambda *a,**kw:self.fail('Invalid source provenance was forwarded'))
  self.assertFalse(forward_local_message(self.settings(),publisher,self.message(run=False)))
  self.assertFalse(forward_local_message(self.settings(),publisher,self.message(mode='replay',run=False)))

 def test_gateway_timestamp_origin_when_device_has_no_clock(self):
  value=parse_serial_line(json.dumps({'deviceId':'sensor','integration':{'mode':'simulated','scenarioRunId':'test'}}))
  self.assertEqual(value['integration']['timestampOrigin'],'gateway');self.assertTrue(value['integration']['gatewayReceivedAt'].endswith('Z'));self.assertGreater(value['timestamp'],0)
