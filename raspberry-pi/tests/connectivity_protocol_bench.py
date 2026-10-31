"""Loopback simulation servers for reproducible real protocol integration.

This fixture creates an OPC UA server with a small simulated UAFX type tree,
a real Modbus TCP server, MQTT publications and standard UADP datagrams. It
does not implement or certify a UAFX device. All connections carry simulated
provenance. Run separately from a real sensor_gateway.py process.
"""
import argparse
import asyncio
import json
import logging
from pathlib import Path
import signal
import socket
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from connectivity_fixtures import connection, configuration, tag, encoded_uadp
from connectivity_schema import validate_configuration
from connectivity_values import utc_now
from fx_tsn import FX_AC, FX_CM


class ProtocolBench:
    def __init__(self, opcua_port=14840, modbus_port=15020, udp_port=14841, mqtt_port=18884):
        self.opcua_port, self.modbus_port, self.udp_port, self.mqtt_port = opcua_port, modbus_port, udp_port, mqtt_port
        self.stop_event = threading.Event()

    def start(self):
        from asyncua import ua
        from asyncua.sync import Server
        from pymodbus.simulator import DataType, SimData, SimDevice
        from pymodbus.server import ModbusTcpServer
        import paho.mqtt.client as mqtt
        self.ua_server = Server()
        self.ua_server.set_endpoint(f"opc.tcp://127.0.0.1:{self.opcua_port}/bench/")
        self.ua_server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.namespace = self.ua_server.register_namespace("urn:smartfactory:bench")
        fx_ac = self.ua_server.register_namespace(FX_AC)
        fx_cm = self.ua_server.register_namespace(FX_CM)
        # Types match Part 81 browse-name + namespace identities. The deliberately
        # small simulator is not a complete FX NodeSet or commissioning stack.
        ac_type = self.ua_server.nodes.base_object_type.add_object_type(ua.NodeId("AutomationComponentType", fx_ac), ua.QualifiedName("AutomationComponentType", fx_ac))
        fe_type = self.ua_server.nodes.base_object_type.add_object_type(ua.NodeId("FunctionalEntityType", fx_ac), ua.QualifiedName("FunctionalEntityType", fx_ac))
        cm_type = self.ua_server.nodes.base_object_type.add_object_type(ua.NodeId("ConnectionManagerType", fx_cm), ua.QualifiedName("ConnectionManagerType", fx_cm))
        component = self.ua_server.nodes.objects.add_object(ua.NodeId("BenchAC", self.namespace), ua.QualifiedName("Bench automation component", self.namespace), objecttype=ac_type.nodeid)
        entity = component.add_object(ua.NodeId("BenchFE", self.namespace), ua.QualifiedName("Bench functional entity", self.namespace), objecttype=fe_type.nodeid)
        self.ua_server.nodes.objects.add_object(ua.NodeId("BenchCM", self.namespace), ua.QualifiedName("Bench connection manager", self.namespace), objecttype=cm_type.nodeid)
        self.pressure = entity.add_variable(ua.NodeId("Pressure", self.namespace), "Pressure", 12.5, ua.VariantType.Float)
        self.running = entity.add_variable(ua.NodeId("Running", self.namespace), "Running", True, ua.VariantType.Boolean)
        self.counter = entity.add_variable(ua.NodeId("Counter", self.namespace), "Counter", 18446744073709551615, ua.VariantType.UInt64)
        self.label = entity.add_variable(ua.NodeId("Label", self.namespace), "Label", "simulation-running", ua.VariantType.String)
        self.ua_server.start()
        self.modbus_loop = asyncio.new_event_loop()
        self.modbus_ready = threading.Event()
        self.modbus_error = None
        async def serve():
            try:
                device = SimDevice(1, simdata=(
                    [SimData(0, values=[True, False, True], datatype=DataType.BITS)],
                    [SimData(0, values=[True, False], datatype=DataType.BITS)],
                    [SimData(0, values=[0x4148, 0, 65535, 65535, 65535, 65535, 123], datatype=DataType.REGISTERS)],
                    [SimData(0, values=[77], datatype=DataType.REGISTERS)]))
                self.modbus_server = ModbusTcpServer(device, address=("127.0.0.1", self.modbus_port))
                await self.modbus_server.serve_forever(background=True)
            except Exception as error:
                self.modbus_error = error
            finally:
                self.modbus_ready.set()
        def serve_thread():
            asyncio.set_event_loop(self.modbus_loop)
            self.modbus_loop.run_until_complete(serve())
            self.modbus_loop.run_forever()
        self.modbus_thread = threading.Thread(target=serve_thread, daemon=True)
        self.modbus_thread.start()
        if not self.modbus_ready.wait(10) or self.modbus_error:
            raise RuntimeError("loopback_modbus_fixture_failed") from self.modbus_error
        self.mqtt = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="sf-protocol-bench-" + str(self.opcua_port), protocol=mqtt.MQTTv5)
        self.mqtt.connect("127.0.0.1", self.mqtt_port)
        self.mqtt.loop_start()
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sender = threading.Thread(target=self._send, daemon=True)
        self.sender.start()
        return self

    def _send(self):
        sequence = 1
        while not self.stop_event.is_set():
            self.mqtt.publish("bench/pressure", json.dumps({"value": 12.5, "running": True, "quality": "good", "time": utc_now()}), qos=1, retain=False)
            self.udp.sendto(encoded_uadp(sequence=sequence), ("127.0.0.1", self.udp_port))
            sequence = (sequence + 1) & 65535
            self.stop_event.wait(0.5)

    def configuration(self, gateway_id="connectivity-e2e"):
        opcua_tags = [tag(kind, key.lower(), {"nodeId": "s=" + key, "namespaceUri": "urn:smartfactory:bench"}) for key, kind in (("Pressure", "float32"), ("Running", "boolean"), ("Counter", "uint64"), ("Label", "string"))]
        modbus_tags = [tag(), tag("uint64", "counter", {"address": 2, "registerType": "holding_register", "unitId": 1}), tag("boolean", "running", {"address": 0, "registerType": "coil", "unitId": 1})]
        fx = connection("opcua_fx", "fx-diagnostics", endpoint=f"opc.tcp://127.0.0.1:{self.opcua_port}/bench/", fx={"mode": "diagnostics"}, tags=[opcua_tags[0]], tsn={"interface": "lo"})
        config = configuration(connection("opcua", "opcua-bench", endpoint=f"opc.tcp://127.0.0.1:{self.opcua_port}/bench/", tags=opcua_tags), connection("modbus_tcp", "modbus-bench", endpoint=f"tcp://127.0.0.1:{self.modbus_port}", tags=modbus_tags), connection("mqtt", "mqtt-bench", endpoint=f"mqtt://127.0.0.1:{self.mqtt_port}"), fx, connection("opcua_fx", "uadp-bench", endpoint=f"opc.udp://127.0.0.1:{self.udp_port}"))
        config["gatewayDeviceId"] = gateway_id
        return validate_configuration(config, gateway_id)

    def close(self):
        self.stop_event.set()
        if hasattr(self, "sender"): self.sender.join(2)
        if hasattr(self, "mqtt"): self.mqtt.disconnect(); self.mqtt.loop_stop()
        if hasattr(self, "udp"): self.udp.close()
        if hasattr(self, "modbus_server"):
            asyncio.run_coroutine_threadsafe(self.modbus_server.shutdown(), self.modbus_loop).result(5)
        if hasattr(self, "modbus_loop"):
            self.modbus_loop.call_soon_threadsafe(self.modbus_loop.stop)
            self.modbus_thread.join(5)
            self.modbus_loop.close()
        if hasattr(self, "ua_server"): self.ua_server.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configuration-out", required=True)
    parser.add_argument("--gateway-id", default="connectivity-e2e")
    parser.add_argument("--mqtt-port", type=int, default=18884)
    parser.add_argument("--seconds", type=int, default=1800)
    args = parser.parse_args()
    logging.basicConfig(level=logging.ERROR)
    bench = ProtocolBench(mqtt_port=args.mqtt_port)
    try:
        bench.start()
        Path(args.configuration_out).write_text(json.dumps(bench.configuration(args.gateway_id), indent=2) + "\n")
        print("READY: OPC UA 14840, Modbus TCP 15020, UADP 14841, MQTT " + str(args.mqtt_port), flush=True)
        signal.signal(signal.SIGTERM, lambda *_: bench.stop_event.set())
        signal.signal(signal.SIGINT, lambda *_: bench.stop_event.set())
        bench.stop_event.wait(args.seconds)
    finally:
        bench.close()


if __name__ == "__main__":
    main()
