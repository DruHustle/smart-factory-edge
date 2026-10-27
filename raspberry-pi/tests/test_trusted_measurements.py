import json,sys,types,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from measurement_units import valid_identity,valid_units,mapped_value,valid_mapping
from mqtt_topics import encode_level,decode_level,topic
from sensor_gateway import telemetry_topic,heartbeat_topic,command_topic,forward_local_message

class TrustedMeasurementTests(unittest.TestCase):
 def test_all_topic_builders_share_the_wire_identity_contract(self):
  for identity in ('a','sensor.temp:01','legacy_01','sensor-01'):
   self.assertTrue(valid_identity(identity))
   for build in (telemetry_topic,heartbeat_topic,command_topic):
    encoded=build('site','line',identity);self.assertIn('/'+encode_level(identity)+'/',encoded);self.assertEqual(decode_level(encoded.split('/')[3]),identity)
  for identity in ('bad/topic','bad#id','+','a'*65,':starts'):
   self.assertFalse(valid_identity(identity))
   for build in (telemetry_topic,heartbeat_topic,command_topic):
    with self.assertRaises(ValueError):build('site','line',identity)
 def test_topic_encoding_cannot_change_identity_or_escape_a_level(self):
  for bad in ('sensor%2Fbad','sensor%252Ebad','sensor%23bad','sensor%00bad'):
   with self.assertRaises(ValueError):decode_level(bad)
  with self.assertRaises(ValueError):topic('a'+'.'*63,'b'+'.'*63,'c'+'.'*63,'telemetry')
  self.assertEqual(decode_level('sensor.temp:01'),'sensor.temp:01')
 def test_preserves_raw_scaling_and_declared_engineering_unit(self):
  output={};mapped_value(output,'power',15,{'unit':'kW','scale':.1,'offset':0},'register')
  self.assertEqual(output['power'],1.5);self.assertEqual(output['units'],{'power':'kW'})
  self.assertTrue(valid_mapping(output['mapping'],output['units'],output))
  output['power']=15;self.assertFalse(valid_mapping(output['mapping'],output['units'],output))
 def test_refuses_incompatible_units_and_unverified_mapping_values(self):
  self.assertFalse(valid_units({'vibration':'g'}));self.assertFalse(valid_units({'power':None}))
  with self.assertRaises(ValueError):mapped_value({},'vibration',2,{'unit':'g'})
  self.assertFalse(valid_mapping({'power':None},{'power':'W'},{'power':1}))
 def test_new_firmware_acquisition_retains_identity_quality_and_broker_retention(self):
  settings=types.SimpleNamespace(device_id='gateway:01',site_id='fixture',line_id='line',local_mqtt_device_ids=('sensor.temp:01',))
  record={'deviceId':'sensor.temp:01','timestamp':1793066400000,'temperature':23,'humidity':50,'units':{'temperature':'Cel','humidity':'%'},'observation':{'schemaVersion':1,'mode':'live','sourceQuality':'good','sessionId':'boot-123','sequence':9}}
  output=[];publisher=types.SimpleNamespace(publish=lambda topic,payload,**kwargs:output.append(json.loads(payload)) or types.SimpleNamespace(rc=0))
  msg=types.SimpleNamespace(topic=telemetry_topic('fixture','line','sensor.temp:01'),payload=json.dumps(record).encode(),retain=True)
  self.assertTrue(forward_local_message(settings,publisher,msg));received=output[0]
  self.assertEqual(received['timestamp'],record['timestamp']);self.assertEqual(received['integration']['mode'],'live');self.assertEqual(received['integration']['sessionId'],'boot-123');self.assertTrue(received['transportRetained']);self.assertTrue(received['integration']['retained']);self.assertEqual(received['units'],record['units'])
 def test_legacy_input_is_not_relabelled_as_verified_live(self):
  settings=types.SimpleNamespace(device_id='gateway',site_id='fixture',line_id='line',local_mqtt_device_ids=('sensor',))
  output=[];publisher=types.SimpleNamespace(publish=lambda topic,payload,**kwargs:output.append(json.loads(payload)) or types.SimpleNamespace(rc=0))
  msg=types.SimpleNamespace(topic=telemetry_topic('fixture','line','sensor'),payload=json.dumps({'deviceId':'sensor','timestamp':1793066400000,'temperature':23}).encode(),retain=False)
  self.assertTrue(forward_local_message(settings,publisher,msg));self.assertNotIn('integration',output[0]);self.assertNotIn('units',output[0])
 def test_firmware_frame_remains_within_its_512_byte_serialization_budget(self):
  record={'deviceId':'a'*64,'timestamp':4294967295,'sensorType':'DHT11','sensorStatus':'ok','temperature':-1000000000,'humidity':100,'units':{'temperature':'Cel','humidity':'%'},'observation':{'schemaVersion':1,'mode':'live','sourceQuality':'good','sessionId':'wrover-ffffffffffffffff','sequence':9007199254740991}}
  self.assertLess(len(json.dumps(record,separators=(',',':')).encode()),512)
