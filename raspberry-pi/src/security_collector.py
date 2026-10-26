"""Opt-in, read-only observations of this gateway's existing authorized connections.
No scans, packet capture, extra MQTT subscription, remote control, or private-key reads.
Uses the existing delivery.sqlite3 with a separate bounded queue; telemetry quota is unchanged.
"""
import hashlib
import json
import os
import re
import ssl
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

_ID = re.compile(r"[A-Za-z0-9_.:@/-]{1,128}", re.ASCII)
_TYPES = {
    "collector.health": ({"state"}, {"expectedIntervalSeconds"}),
    "config.changed": ({"configRevision", "changedKeys"}, {"baselineRevision"}),
    "identity.unrecognized": ({"inputRef", "observedIdentityRef"}, set()),
    "tls.certificate_observed": ({"endpointRef", "notBefore", "notAfter", "fingerprintSha256"}, set()),
    "integrity.change_observed": ({"previousSha256", "currentSha256", "evidenceBasis"}, set()),
    "transport.connection_observed": ({"connectionRef", "protocol", "tlsActive", "evidenceBasis"}, set()),
}

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def validate_produced(event):
    required={"schemaVersion","producerNamespace","sourceEventId","occurredAt","mode","eventType","assetRef","summary","quality","producerVersion","attributes"}
    if not isinstance(event,dict) or not required <= set(event) or set(event)-required-{"scenarioRunId","replayRunId"}:
        raise ValueError("invalid_security_fields")
    if event["eventType"] not in _TYPES or event["assetRef"] is not None or event["quality"] != "good":
        raise ValueError("unsupported_collector_evidence")
    expected="smart-factory-ot-observation/v1" if event["eventType"]=="transport.connection_observed" else "ot-security-evidence/v1"
    if event["schemaVersion"]!=expected: raise ValueError("unsupported_collector_version")
    for key,limit in (("producerNamespace",64),("sourceEventId",128),("producerVersion",64)):
        if not isinstance(event[key],str) or not 1<=len(event[key])<=limit or not _ID.fullmatch(event[key]): raise ValueError("invalid_collector_identity")
    from provenance import normalize_provenance
    normalize_provenance({"mode":event["mode"],"gatewayReceivedAt":event["occurredAt"],**{k:event[k] for k in ("scenarioRunId","replayRunId") if k in event}})
    attributes=event["attributes"];needed,optional=_TYPES[event["eventType"]]
    if not isinstance(attributes,dict) or not needed<=set(attributes) or set(attributes)-needed-optional: raise ValueError("invalid_collector_attributes")
    for key,value in attributes.items():
        if key=="changedKeys":
            if not isinstance(value,list) or not 1<=len(value)<=32 or any(not isinstance(k,str) or not _ID.fullmatch(k) for k in value): raise ValueError("invalid_changed_keys")
        elif key=="expectedIntervalSeconds":
            if type(value) is not int or not 1<=value<=86400: raise ValueError("invalid_collector_interval")
        elif key=="tlsActive":
            if type(value) is not bool: raise ValueError("invalid_tls_state")
        elif key in {"notBefore","notAfter"}:
            normalize_provenance({"mode":"live","gatewayReceivedAt":value})
        elif key.endswith("Sha256"):
            if not isinstance(value,str) or not re.fullmatch(r"[a-f0-9]{64}",value): raise ValueError("invalid_hash")
        elif not isinstance(value,str) or not _ID.fullmatch(value): raise ValueError("invalid_collector_attribute")
    if event["eventType"]=="collector.health" and attributes["state"] not in {"online","offline","degraded","paused"}:raise ValueError("invalid_collector_state")
    if event["eventType"]=="transport.connection_observed" and (attributes["protocol"]!="mqtt" or attributes["evidenceBasis"] not in {"runtime_socket","declared_config"}):raise ValueError("invalid_transport_basis")
    if event["eventType"]=="integrity.change_observed" and attributes["evidenceBasis"]!="self_reported":raise ValueError("unsupported_integrity_claim")
    if not isinstance(event["summary"],str) or not 1<=len(event["summary"])<=512 or any(ord(c)<32 for c in event["summary"]):raise ValueError("invalid_summary")
    if len(json.dumps(event,separators=(",",":"),ensure_ascii=False).encode("utf8"))>8192:raise ValueError("security_event_size")
    return event

class SecurityCollector:
    def __init__(self,state,origin,token,namespace,mode="live",run_id=None,interval=300,allow_local=False,max_events=1000,opener=None):
        url=urlparse(origin)
        if (url.scheme!="https" and not (allow_local and url.scheme=="http" and url.hostname in {"127.0.0.1","localhost"})) or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in {"","/"}:raise ValueError("security_origin_requires_configured_https")
        if not re.fullmatch(r"sfsec\.[a-f0-9-]{36}\.[A-Za-z0-9_-]{43}",token):raise ValueError("invalid_security_credential")
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}",namespace):raise ValueError("invalid_security_namespace")
        if mode not in {"live","simulated","replay"} or (mode!="live" and (not run_id or not _ID.fullmatch(run_id))):raise ValueError("security_mode_requires_run")
        if not 10<=interval<=86400 or not 1<=max_events<=1000:raise ValueError("security_collector_limits")
        self.state,self.origin,self.token,self.namespace=state,origin.rstrip("/"),token,namespace
        self.mode,self.run_id,self.interval,self.max_events=mode,run_id,interval,max_events
        self.opener=opener or urllib.request.build_opener(NoRedirect(),urllib.request.HTTPSHandler(context=ssl.create_default_context()))
        self.last_fingerprint=None;self.last_transport=None;self.last_certificate_sample=0;self.last_transport_sample=0;self.previous_config=None;self.failures=0
        with state._lock,state._db:
            state._db.execute("CREATE TABLE IF NOT EXISTS security_evidence(id TEXT PRIMARY KEY,payload TEXT NOT NULL,queued_at REAL NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,quarantined_at REAL,error_code TEXT)")
            state._db.execute("CREATE INDEX IF NOT EXISTS security_evidence_due ON security_evidence(next_attempt) WHERE quarantined_at IS NULL")
            state._db.execute("CREATE TABLE IF NOT EXISTS security_collector_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            previous=state._db.execute("SELECT value FROM security_collector_metadata WHERE key='configuration_sha256'").fetchone()
            self.previous_config=previous[0] if previous else None
    @classmethod
    def from_environment(cls,state):
        if os.getenv("SECURITY_COLLECTOR_ENABLED","").lower()!="true":return None
        try:
            if state is None:raise ValueError("durable_state_required")
            return cls(state,os.environ["SECURITY_API_ORIGIN"],os.environ["SECURITY_SOURCE_TOKEN"],os.environ["SECURITY_PRODUCER_NAMESPACE"],os.getenv("SECURITY_MODE","live"),os.getenv("SECURITY_RUN_ID"),int(os.getenv("SECURITY_INTERVAL_SECONDS","300")),os.getenv("SECURITY_ALLOW_LOCAL_HTTP")=="true")
        except (KeyError,ValueError):
            print("[SECURITY] collector unconfigured; telemetry operation preserved")
            return None
    def observe(self,event_type,attributes,summary):
        event={"schemaVersion":"smart-factory-ot-observation/v1" if event_type=="transport.connection_observed" else "ot-security-evidence/v1","producerNamespace":self.namespace,"sourceEventId":str(uuid.uuid4()),"occurredAt":utc_now(),"mode":self.mode,"eventType":event_type,"assetRef":None,"summary":summary,"quality":"good","producerVersion":"smart-factory-edge-security-1","attributes":attributes}
        if self.mode!="live":event["scenarioRunId" if self.mode=="simulated" else "replayRunId"]=self.run_id
        validate_produced(event)
        payload=json.dumps(event,separators=(",",":"),ensure_ascii=False)
        with self.state._lock,self.state._db:
            if self.state._db.execute("SELECT count(*) FROM security_evidence").fetchone()[0]>=self.max_events:raise RuntimeError("security_queue_full; telemetry_queue_unaffected")
            self.state._db.execute("INSERT INTO security_evidence(id,payload,queued_at) VALUES(?,?,?)",(event["sourceEventId"],payload,time.time()))
        return event["sourceEventId"]
    def configuration_applied(self,revision):
        self.observe("config.changed",{"configRevision":revision,"changedKeys":["asset_connections"]},"Gateway applied an authorized configuration revision.")
        if self.previous_config and revision!=self.previous_config:self.observe("integrity.change_observed",{"previousSha256":self.previous_config,"currentSha256":revision,"evidenceBasis":"self_reported"},"Gateway reports a changed applied configuration hash; signing is unverified.")
        with self.state._lock,self.state._db:
            self.state._db.execute("INSERT INTO security_collector_metadata(key,value) VALUES('configuration_sha256',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(revision,))
        self.previous_config=revision
    def rejected_identity(self,settings,topic):
        parts=topic.split("/")
        if len(parts)==5 and parts[0]=="factory" and parts[1:3]==[settings.site_id,settings.line_id] and parts[4] in {"telemetry","heartbeat"} and parts[3] not in settings.local_mqtt_device_ids:
            identity="sha256:"+hashlib.sha256(parts[3].encode()).hexdigest()
            self.observe("identity.unrecognized",{"inputRef":"local-mqtt","observedIdentityRef":identity},"An identity outside the configured local input allowlist was rejected.")
    def sample(self,client):
        sock=getattr(client,"_sock",None)
        self.observe("collector.health",{"state":"online" if sock else "degraded","expectedIntervalSeconds":self.interval},"Read-only gateway security collector sampled its existing connection.")
        if sock is None:return
        tls=isinstance(sock,ssl.SSLSocket)
        if self.last_transport!=tls or time.monotonic()-self.last_transport_sample>=21600:
            self.observe("transport.connection_observed",{"connectionRef":"mqtt-cloud","protocol":"mqtt","tlsActive":tls,"evidenceBasis":"runtime_socket"},"Gateway observed the TLS type of its existing MQTT socket; network path and broker ACLs are unverified.")
            self.last_transport=tls;self.last_transport_sample=time.monotonic()
        if tls:
            from cryptography import x509
            from cryptography.hazmat.primitives import hashes
            cert=x509.load_der_x509_certificate(sock.getpeercert(binary_form=True));fingerprint=cert.fingerprint(hashes.SHA256()).hex()
            if fingerprint!=self.last_fingerprint or time.monotonic()-self.last_certificate_sample>=21600:
                self.observe("tls.certificate_observed",{"endpointRef":"mqtt-cloud","notBefore":cert.not_valid_before_utc.isoformat(timespec="milliseconds").replace("+00:00","Z"),"notAfter":cert.not_valid_after_utc.isoformat(timespec="milliseconds").replace("+00:00","Z"),"fingerprintSha256":fingerprint},"Gateway observed the certificate of its configured MQTT connection.")
                self.last_fingerprint=fingerprint;self.last_certificate_sample=time.monotonic()
    def drain_one(self):
        with self.state._lock:
            row=self.state._db.execute("SELECT id,payload,attempts FROM security_evidence WHERE quarantined_at IS NULL AND next_attempt<=? ORDER BY queued_at,rowid LIMIT 1",(time.time(),)).fetchone()
        if row is None:return False
        identity,payload,attempts=row;permanent=False;code="transport_unconfirmed"
        try:
            request=urllib.request.Request(self.origin+"/api/security/observations",data=("{\"events\":["+payload+"]}").encode(),headers={"Authorization":"Bearer "+self.token,"Content-Type":"application/json"},method="POST")
            with self.opener.open(request,timeout=5) as response:
                body=response.read(65537)
                if response.status!=202 or len(body)>65536:raise ValueError("invalid_receipt")
                receipt=json.loads(body)
                if receipt!={"accepted":[{"sourceEventId":identity,"status":"accepted"}]} and receipt!={"accepted":[{"sourceEventId":identity,"status":"duplicate"}]}:raise ValueError("unconfirmed_receipt")
            with self.state._lock,self.state._db:self.state._db.execute("DELETE FROM security_evidence WHERE id=?",(identity,))
            return True
        except urllib.error.HTTPError as error:
            permanent=error.code in {400,401,403,409,413,415,422} or 300<=error.code<400
            code="receiver_"+str(error.code)
        except Exception:
            pass
        attempts+=1
        with self.state._lock,self.state._db:self.state._db.execute("UPDATE security_evidence SET attempts=?,next_attempt=?,quarantined_at=?,error_code=? WHERE id=?",(attempts,time.time()+min(300,2**min(attempts,8)),time.time() if permanent or attempts>=12 else None,code,identity))
        return False
    def run(self,stop,client):
        next_sample=0
        while not stop.is_set():
            try:
                if time.monotonic()>=next_sample:self.sample(client);next_sample=time.monotonic()+self.interval
                if not self.drain_one():stop.wait(1)
            except Exception:
                self.failures+=1
                if self.failures==1 or self.failures%60==0:print("[SECURITY] collection/delivery unavailable; inspect bounded local queue; telemetry operation preserved")
                stop.wait(1)
