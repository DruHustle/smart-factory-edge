import json,sys,types,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from sensor_gateway import forward_local_message,parse_serial_line
class ProvenancePolicyTests(unittest.TestCase):
 def test_locally_versioned_policy_cases_before_forward(self):
  fixture=json.loads((Path(__file__).resolve().parents[2]/'fixtures/telemetry/provenance-v1.json').read_text())
  settings=types.SimpleNamespace(device_id='gateway',site_id='fixture',line_id='line',local_mqtt_device_ids=('sensor',))
  for case in fixture['cases']:
   with self.subTest(case=case['name']):
    output=[];publisher=types.SimpleNamespace(publish=lambda topic,payload,**kw:output.append(json.loads(payload)) or types.SimpleNamespace(rc=0))
    p={'deviceId':'sensor','timestamp':1792486800000,'temperature':45,'integration':case['integration']}
    msg=types.SimpleNamespace(topic='factory/fixture/line/sensor/telemetry',payload=json.dumps(p).encode(),retain=False)
    valid=forward_local_message(settings,publisher,msg)
    self.assertEqual(case['valid'],valid)
    if valid:self.assertEqual(case['normalized'],output[0].get('integration'))
    else:self.assertEqual([],output)

 def test_serial_applies_the_same_policy_before_publish(self):
  fixture=json.loads((Path(__file__).resolve().parents[2]/'fixtures/telemetry/provenance-v1.json').read_text())
  for case in fixture['cases']:
   with self.subTest(case=case['name']):
    raw=json.dumps({'timestamp':1792486800000,'integration':case['integration']})
    if case['valid']:self.assertEqual(case['normalized'],parse_serial_line(raw)['integration'])
    else:
     with self.assertRaises(ValueError):parse_serial_line(raw)
 def test_extreme_json_sequence_is_rejected_without_numeric_overflow(self):
  from provenance import normalize_provenance
  with self.assertRaises(ValueError):normalize_provenance({'mode':'live','gatewayReceivedAt':'2026-10-20T09:00:00Z','sequence':10**1000})
