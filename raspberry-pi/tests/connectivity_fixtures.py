"""Isolated simulation fixtures; they never address plant equipment."""
import copy


def tag(kind="float32", tag_id="pressure", source=None):
    return {"tagId": tag_id, "name": tag_id, "dataType": kind,
            "source": source or {"address": 0, "registerType": "holding_register", "unitId": 1}}


def connection(protocol="modbus_tcp", connection_id="bench", **overrides):
    endpoints = {"modbus_tcp": "tcp://127.0.0.1:15020", "modbus_rtu": "serial:///dev/ttyUSB9?baudrate=19200", "opcua": "opc.tcp://127.0.0.1:14840/bench/", "opcua_fx": "opc.udp://127.0.0.1:14841", "mqtt": "mqtt://127.0.0.1:18884"}
    sources = {"modbus_tcp": None, "modbus_rtu": None, "opcua": {"nodeId": "s=Pressure", "namespaceUri": "urn:smartfactory:bench"}, "opcua_fx": {"fieldIndex": 0}, "mqtt": {"topic": "bench/pressure", "path": "/value", "qualityPath": "/quality", "timestampPath": "/time"}}
    result = {"connectionId": connection_id, "name": "Isolated protocol bench", "protocol": protocol, "endpoint": endpoints[protocol], "enabled": True, "intervalMs": 100, "mode": "simulated", "scenarioRunId": "industrial-connectivity-tests", "tags": [tag(source=sources[protocol])]}
    if protocol == "opcua_fx":
        result["fx"] = {"mode": "udp_uadp", "publisherId": "18446744073709551615", "publisherIdType": "UInt64", "writerGroupId": 100, "dataSetWriterId": 62541, "metadataMajorVersion": 1, "metadataMinorVersion": 0, "allowUnsecured": True}
    result.update(overrides)
    return copy.deepcopy(result)


def configuration(*connections, revision=1):
    return {"schemaVersion": "industrial-connectivity/v1", "gatewayDeviceId": "pi-edge-01", "revision": revision, "connections": list(connections)}


def encoded_uadp(values=None, variant_types=None, encoding="variant", sequence=1, **header_overrides):
    """Encode with pinned asyncua, independently of the observer parser.

    asyncua 2.0.1 defines PUBLISHER_ID_UINT64 as 0x600, although OPC
    10000-14 7.2.4.4.2 specifies publisher type 3 (0x300). Correct only that
    fixture header; production rejects the reserved type6 from the library.
    The unit suite additionally checks an independently specified wire vector.
    """
    from asyncua import ua
    from asyncua.pubsub import uadp
    from asyncua.ua.ua_binary import pack_uatype
    values = values if values is not None else [12.5]
    kinds = variant_types or [ua.VariantType.Float] * len(values)
    header = uadp.UadpDataSetMessageHeader(SequenceNo=sequence, CfgMajorVersion=1, CfgMinorVersion=0, **header_overrides)
    if encoding == "raw":
        data = uadp.UadpDataSetRaw(header, b"".join(pack_uatype(k, v) for k, v in zip(kinds, values)))
    elif encoding == "datavalue":
        data = uadp.UadpDataSetDataValue(header, values)
    else:
        data = uadp.UadpDataSetVariant(header, [ua.Variant(v, k) for k, v in zip(kinds, values)])
    encoded = uadp.UadpNetworkMessage(Header=uadp.UadpHeader(PublisherId=ua.UInt64(18446744073709551615)), GroupHeader=uadp.UadpGroupHeader(WriterGroupId=100, SequenceNo=sequence), DataSetPayloadHeader=[62541], Payload=[data]).to_binary()
    if encoded[:2] == b"\xf1\x06":
        encoded = b"\xf1\x03" + encoded[2:]
    return encoded
