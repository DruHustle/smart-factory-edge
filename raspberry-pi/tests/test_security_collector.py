import io,json,os,sys,tempfile,unittest,urllib.error
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from durable_state import DurableState
from security_collector import SecurityCollector,validate_produced
TOKEN="sfsec.11111111-1111-4111-8111-111111111111."+"a"*43
class Reply:
    def __init__(self,identity,status="accepted"):self.status=202;self.body=json.dumps({"accepted":[{"sourceEventId":identity,"status":status}]}).encode()
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def read(self,n):return self.body[:n]
class SecurityCollectorTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.path=str(Path(self.directory.name)/"delivery.sqlite3");self.state=DurableState(self.path);self.collector=SecurityCollector(self.state,"http://127.0.0.1:3117",TOKEN,"fixture-pi",mode="simulated",run_id="fixture-run",allow_local=True)
    def tearDown(self):self.state.close();self.directory.cleanup()
    def test_disabled_collector_does_not_require_new_configuration_or_state(self):
        with patch.dict(os.environ,{},clear=True):self.assertIsNone(SecurityCollector.from_environment(None))
    def test_https_and_explicit_run_required(self):
        for kwargs in ({"origin":"http://external.invalid"},{"mode":"simulated","run_id":None},{"origin":"https://user:password@example.test"},{"origin":"https://example.test/?secret=value"}):
            args={"state":self.state,"origin":"https://example.test","token":TOKEN,"namespace":"fixture",**kwargs}
            with self.assertRaises(ValueError):SecurityCollector(**args)
    def test_durable_reopen_retains_identity_and_provenance(self):
        identity=self.collector.observe("collector.health",{"state":"online","expectedIntervalSeconds":300},"Fixture collector health.")
        self.state.close();self.state=DurableState(self.path)
        c=SecurityCollector(self.state,"http://127.0.0.1:3117",TOKEN,"fixture-pi",mode="simulated",run_id="fixture-run",allow_local=True)
        row=self.state._db.execute("SELECT id,payload FROM security_evidence").fetchone();self.assertEqual(row[0],identity);self.assertEqual(json.loads(row[1])["mode"],"simulated")
        c.opener=SimpleNamespace(open=lambda request,timeout:Reply(identity,"duplicate"));self.assertTrue(c.drain_one());self.assertEqual(self.state._db.execute("SELECT count(*) FROM security_evidence").fetchone()[0],0)
    def test_full_security_queue_preserves_unacknowledged_events_and_telemetry_capacity(self):
        self.collector.max_events=1;identity=self.collector.observe("collector.health",{"state":"online"},"First observation.")
        with self.assertRaises(RuntimeError):self.collector.observe("collector.health",{"state":"offline"},"Queue full observation.")
        self.state.enqueue("factory/fixture/line/device/telemetry",'{"deviceId":"device"}')
        self.assertEqual(self.state._db.execute("SELECT id FROM security_evidence").fetchone()[0],identity);self.assertIsNotNone(self.state.first())
    def test_permanent_rejection_is_quarantined_not_acknowledged(self):
        identity=self.collector.observe("collector.health",{"state":"online"},"Rejected observation.")
        def reject(*args,**kwargs):raise urllib.error.HTTPError("https://example.test",422,"Rejected",{},io.BytesIO())
        self.collector.opener=SimpleNamespace(open=reject);self.assertFalse(self.collector.drain_one())
        row=self.state._db.execute("SELECT id,attempts,quarantined_at,error_code FROM security_evidence").fetchone();self.assertEqual(row[0],identity);self.assertEqual(row[1],1);self.assertIsNotNone(row[2]);self.assertEqual(row[3],"receiver_422")
    def test_transient_outage_and_unconfirmed_timeout_retry_same_event(self):
        identity=self.collector.observe("collector.health",{"state":"online"},"Pending observation.")
        def timeout(*args,**kwargs):raise TimeoutError("never expose this body")
        self.collector.opener=SimpleNamespace(open=timeout);self.assertFalse(self.collector.drain_one())
        row=self.state._db.execute("SELECT id,quarantined_at,error_code FROM security_evidence").fetchone();self.assertEqual(row,(identity,None,"transport_unconfirmed"))
        self.state._db.execute("UPDATE security_evidence SET next_attempt=0");self.state._db.commit();self.collector.opener=SimpleNamespace(open=lambda *a,**k:Reply(identity));self.assertTrue(self.collector.drain_one())
    def test_unrecognized_identity_requires_existing_authorized_input_scope(self):
        settings=SimpleNamespace(site_id="site",line_id="line",local_mqtt_device_ids=("allowed",))
        for topic in ("factory/other/line/unknown/telemetry","factory/site/line/unknown/commands","factory/site/line/allowed/telemetry"):self.collector.rejected_identity(settings,topic)
        self.assertEqual(self.state._db.execute("SELECT count(*) FROM security_evidence").fetchone()[0],0)
        self.collector.rejected_identity(settings,"factory/site/line/unknown/telemetry");payload=json.loads(self.state._db.execute("SELECT payload FROM security_evidence").fetchone()[0]);self.assertEqual(payload["eventType"],"identity.unrecognized");self.assertTrue(payload["attributes"]["observedIdentityRef"].startswith("sha256:"));self.assertNotIn("unknown",payload["attributes"]["observedIdentityRef"])
    def test_only_actual_existing_socket_is_reported_as_runtime_transport(self):
        self.collector.sample(SimpleNamespace(_sock=None));self.assertEqual(self.state._db.execute("SELECT count(*) FROM security_evidence").fetchone()[0],1)
        self.collector.sample(SimpleNamespace(_sock=object()));payloads=[json.loads(r[0]) for r in self.state._db.execute("SELECT payload FROM security_evidence")];transport=[p for p in payloads if p["eventType"]=="transport.connection_observed"][0];self.assertFalse(transport["attributes"]["tlsActive"]);self.assertEqual(transport["attributes"]["evidenceBasis"],"runtime_socket")
    def test_produced_record_rejects_unsupported_attributes(self):
        self.collector.observe("collector.health",{"state":"online"},"Good observation.");p=json.loads(self.state._db.execute("SELECT payload FROM security_evidence").fetchone()[0]);p["attributes"]["password"]="never-transmit"
        with self.assertRaises(ValueError):validate_produced(p)
if __name__=="__main__":unittest.main()
