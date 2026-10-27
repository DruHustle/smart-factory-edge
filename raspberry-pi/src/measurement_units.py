"""Independent wire identity, units and reviewed scaling contract (version 1)."""
import math
import re

WIRE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}", re.ASCII)
SOURCE_UNITS = {
    "temperature": {"Cel", "°C", "C", "°F", "F", "K"}, "humidity": {"%", "fraction"},
    "vibration": {"mm/s", "m/s"}, "power": {"W", "kW", "MW"}, "pressure": {"Pa", "kPa", "MPa", "bar"}, "rpm": {"rpm", "r/min", "1/min", "Hz"},
}

def valid_identity(value):
    return isinstance(value, str) and WIRE_ID.fullmatch(value) is not None

def valid_units(units):
    return isinstance(units, dict) and len(units) <= 6 and all(key in SOURCE_UNITS and isinstance(unit, str) and unit in SOURCE_UNITS[key] for key, unit in units.items())

def mapped_value(output, metric, raw, mapping, raw_unit="native"):
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw):
        raise ValueError("Mapped source value must be finite")
    scale, offset = float(mapping.get("scale", 1)), float(mapping.get("offset", 0))
    value = float(raw) * scale + offset
    if not math.isfinite(value) or abs(value) > 1e9:
        raise ValueError("Mapped value is out of range")
    output[metric] = value
    unit = mapping.get("unit")
    if unit:
        if unit not in SOURCE_UNITS[metric]:
            raise ValueError("Unsupported engineering unit for metric")
        output.setdefault("units", {})[metric] = unit
        output.setdefault("mapping", {})[metric] = {"rawValue": float(raw), "rawUnit": raw_unit, "scale": scale, "offset": offset, "version": "sf-mapping-v1"}

def valid_mapping(mapping, units, values):
    if not isinstance(mapping, dict) or len(mapping) > 6 or not valid_units(units): return False
    for metric, item in mapping.items():
        if metric not in units or metric not in values or not isinstance(item, dict) or set(item) != {"rawValue", "rawUnit", "scale", "offset", "version"}: return False
        if item["rawUnit"] not in {"register", "native"} or item["version"] != "sf-mapping-v1": return False
        for key, bound in (("rawValue", 9007199254740991), ("scale", 1e9), ("offset", 1e9)):
            value=item[key]
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or abs(value)>bound:return False
        value=values[metric]
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or abs(item["rawValue"]*item["scale"]+item["offset"]-value)>max(1e-6,abs(value)*1e-9):return False
    return True
