"""Smart Factory ingress policy 1; independent edge implementation, no peer SDK."""
import json
import math
import re
from datetime import datetime, timezone

IDENTIFIER = re.compile(r"[A-Za-z0-9:_.-]{1,128}", re.ASCII)
UTC = re.compile(r"(?!0000)([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{1,9}))?(?:Z|\+00:00)", re.ASCII)
UUID = re.compile(r"(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)", re.IGNORECASE | re.ASCII)
FIELDS = {"mode", "scenarioRunId", "replayRunId", "sessionId", "sequence", "sourceQuality", "sourceConnection", "timestampOrigin", "retained", "gatewayReceivedAt", "sourceEvents"}

def normalize_provenance(value):
    """Return canonical metadata or raise a bounded, data-free validation code.
    A null/absent integration is legacy input. Optional child nulls are invalid.
    Receipt fractions up to nanoseconds truncate to milliseconds, never round.
    """
    if value is None:
        return None
    def fail(code):
        raise ValueError("invalid_provenance:" + code)
    if not isinstance(value, dict) or not set(value).issubset(FIELDS):
        fail("fields")
    def enum(name, allowed, default=None):
        v=value.get(name, default)
        if not isinstance(v, str) or v not in allowed: fail(name)
        return v
    mode=enum("mode", {"live","simulated","replay"})
    result={"mode":mode}
    for key in ("scenarioRunId","replayRunId","sessionId"):
        if key in value:
            if not isinstance(value[key],str) or not IDENTIFIER.fullmatch(value[key]): fail(key)
            result[key]=value[key]
    if mode=="simulated" and "scenarioRunId" not in result: fail("scenarioRunId")
    if mode=="replay" and "replayRunId" not in result: fail("replayRunId")
    if mode!="replay" and "replayRunId" in result: fail("replay_mode")
    if "sequence" in value:
        n=value["sequence"]
        if isinstance(n,bool) or not isinstance(n,(int,float)) or not 0<=n<=9007199254740991 or not math.isfinite(n) or int(n)!=n: fail("sequence")
        result["sequence"]=int(n)
    result["sourceQuality"]=enum("sourceQuality",{"good","bad","uncertain","unknown"},"unknown")
    result["sourceConnection"]=enum("sourceConnection",{"online","offline","unknown"},"unknown")
    result["timestampOrigin"]=enum("timestampOrigin",{"device","gateway"},"device")
    retained=value.get("retained",False)
    if type(retained) is not bool: fail("retained")
    result["retained"]=retained
    stamp=value.get("gatewayReceivedAt")
    m=UTC.fullmatch(stamp) if isinstance(stamp,str) and len(stamp)<=35 else None
    if not m: fail("gatewayReceivedAt")
    y,mo,d,h,mi,s=map(int,m.groups()[:6]);ms=int((m.group(7) or "").ljust(3,"0")[:3])
    try: datetime(y,mo,d,h,mi,s,ms*1000,tzinfo=timezone.utc)
    except ValueError: fail("gatewayReceivedAt")
    result["gatewayReceivedAt"]=f"{y:04d}-{mo:02d}-{d:02d}T{h:02d}:{mi:02d}:{s:02d}.{ms:03d}Z"
    events=value.get("sourceEvents",[])
    if not isinstance(events,list) or len(events)>20: fail("sourceEvents")
    normalized=[]
    for e in events:
        if not isinstance(e,dict) or set(e)!={"id","origin","code","state","componentId"}: fail("sourceEvents.fields")
        if any(not isinstance(e[k],str) or not IDENTIFIER.fullmatch(e[k]) for k in ("id","code")): fail("sourceEvents.identifier")
        if e["origin"]!="source" or not isinstance(e["state"],str) or e["state"] not in {"active","cleared"}: fail("sourceEvents.enum")
        if not isinstance(e["componentId"],str) or not UUID.fullmatch(e["componentId"]): fail("sourceEvents.componentId")
        normalized.append({**e,"componentId":e["componentId"].lower()})
    result["sourceEvents"]=normalized
    if len(json.dumps(result,separators=(",",":"),ensure_ascii=False).encode("utf-8"))>8192: fail("bytes")
    return result
