#!/usr/bin/env python3
"""Disposable integration only. Token is supplied on stdin, never logged.
Uses this checkout's collector against a separately configured local Smart Factory Node API.
Does not connect to MQTT or touch physical hardware.
"""
import json,sys,tempfile,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from durable_state import DurableState
from security_collector import SecurityCollector
config=json.load(sys.stdin)
with tempfile.TemporaryDirectory(prefix="security-collector-smoke-") as directory:
    path=str(Path(directory)/"delivery.sqlite3")
    state=DurableState(path)
    state.enqueue("factory/fixture/line/device/telemetry",'{"deviceId":"device","temperature":24}')
    collector=SecurityCollector(state,"http://127.0.0.1:1",config["token"],config["namespace"],mode="simulated",run_id="actual-local-forwarding",allow_local=True)
    identity=collector.observe("collector.health",{"state":"degraded","expectedIntervalSeconds":300},"Isolated collector outage and recovery observation.")
    assert not collector.drain_one()
    assert state._db.execute("SELECT id,attempts FROM security_evidence").fetchone()==(identity,1)
    state.close()
    state=DurableState(path)
    collector=SecurityCollector(state,config["origin"],config["token"],config["namespace"],mode="simulated",run_id="actual-local-forwarding",allow_local=True)
    with state._lock,state._db:state._db.execute("UPDATE security_evidence SET next_attempt=0")
    assert collector.drain_one()
    assert state._db.execute("SELECT count(*) FROM security_evidence").fetchone()[0]==0
    assert state.first() is not None
    collector.configuration_applied("a"*64)
    state.close()
    state=DurableState(path)
    collector=SecurityCollector(state,config["origin"],config["token"],config["namespace"],mode="simulated",run_id="actual-local-forwarding",allow_local=True)
    assert collector.previous_config=="a"*64
    collector.configuration_applied("b"*64)
    while state._db.execute("SELECT count(*) FROM security_evidence").fetchone()[0]:assert collector.drain_one()
    assert state.first() is not None
    state.close()
print(json.dumps({"eventId":identity,"outageRetry":True,"restartPreservedIdentity":True,"telemetryQueuePreserved":True,"configurationHashPersisted":True}))
