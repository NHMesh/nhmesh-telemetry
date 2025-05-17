import logging
import sys
import socket
import paho.mqtt.client as mqtt
import json
import meshtastic
import meshtastic.tcp_interface
from pubsub import pub
import argparse
from utils.envdefault import EnvDefault
import time
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

class MeshtasticMQTTHandler:
    """
    A class to handle Meshtastic MQTT communication.
    This class connects to a Meshtastic node and an MQTT broker,
    and publishes received packets to the MQTT broker.

    Args:
        broker (str): The MQTT broker address.
        port (int): The MQTT broker port.
        topic (str): The root MQTT topic.
        tls (bool): Whether to use TLS/SSL.
        username (str): The MQTT username.
        password (str): The MQTT password.
        node_ip (str): The IP address of the Meshtastic node.
    """
    
    def __init__(self, broker, port, topic, tls, username, password, node_ip):
        """
        Initializes the MeshtasticMQTTHandler.
        """
        self.broker = broker
        self.port = port
        self.topic = topic
        self.tls = tls
        self.username = username
        self.password = password
        self.node_ip = node_ip
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5  # seconds
        
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.username_pw_set(username=self.username, password=self.password)
        
        try:
            self.interface = meshtastic.tcp_interface.TCPInterface(hostname=self.node_ip)
            self.node_info = self.interface.getMyNodeInfo()
            self.connected_node_id = self.node_info["user"]["id"]
        except Exception as e:
            logging.error(f"Failed to setup Meshtastic interface: {e}")
            raise

        pub.subscribe(self.onReceive, "meshtastic.receive")

        # --- Traceroute Daemon Feature ---
        self._node_cache = {}  # node_id -> {"position": (lat, lon, alt), "long_name": str}
        self._tracerouted_nodes = set()
        self._last_traceroute_time = {}
        self._TRACEROUTE_INTERVAL = 3 * 60 * 60  # 3 hours
        self._traceroute_lock = threading.Lock()
        self._traceroute_active_lock = threading.Lock()
        threading.Thread(target=self._traceroute_daemon, daemon=True).start()

    def _format_position(self, pos):
        if pos is None:
            return "UNKNOWN"
        lat, lon, alt = pos
        s = f"({lat:.7f}, {lon:.7f})"
        if alt is not None:
            s += f" {alt}m"
        return s

    def _update_cache_from_packet(self, packet):
        node_id = packet.get("from")
        if node_id is None:
            return
        entry = self._node_cache.setdefault(node_id, {"position": None, "long_name": None})
        decoded = packet.get("decoded", {})
        if decoded.get("portnum") == "POSITION_APP":
            payload = decoded.get("payload")
            try:
                from meshtastic.protobuf import mesh_pb2
                pos = mesh_pb2.Position()
                pos.ParseFromString(payload)
                if pos.latitude_i != 0 and pos.longitude_i != 0:
                    lat = pos.latitude_i * 1e-7
                    lon = pos.longitude_i * 1e-7
                    alt = pos.altitude if pos.altitude != 0 else None
                    entry["position"] = (lat, lon, alt)
                else:
                    entry["position"] = None
            except Exception as e:
                logging.warning(f"Error parsing position: {e}")
        if decoded.get("portnum") == "USER_APP":
            payload = decoded.get("payload")
            try:
                from meshtastic.protobuf import mesh_pb2
                user = mesh_pb2.User()
                user.ParseFromString(payload)
                if user.long_name:
                    entry["long_name"] = user.long_name
            except Exception as e:
                logging.warning(f"Error parsing user: {e}")
        # Try to update from interface nodes DB if available
        if hasattr(self.interface, "nodes") and node_id in self.interface.nodes:
            user = self.interface.nodes[node_id].get("user", {})
            if user:
                entry["long_name"] = user.get("longName") or entry["long_name"]

    def _run_traceroute(self, node_id):
        entry = self._node_cache.get(node_id, {})
        long_name = entry.get("long_name")
        pos = entry.get("position")
        logging.info(f"[Traceroute] Node {node_id} | Long name: {long_name if long_name else 'UNKNOWN'} | Position: {self._format_position(pos)}")
        self.interface.sendTraceRoute(dest=node_id, hopLimit=10)
        with self._traceroute_lock:
            self._last_traceroute_time[node_id] = time.time()

    def _traceroute_worker(self, node_id):
        acquired = self._traceroute_active_lock.acquire(timeout=60)
        if not acquired:
            logging.warning(f"[Traceroute] Timeout waiting for traceroute lock for node {node_id}, skipping.")
            return
        try:
            traceroute_thread = threading.Thread(target=self._run_traceroute, args=(node_id,))
            traceroute_thread.start()
            traceroute_thread.join(timeout=60)
            if traceroute_thread.is_alive():
                logging.warning(f"[Traceroute] Traceroute to node {node_id} did not complete in 60 seconds, moving on.")
        finally:
            self._traceroute_active_lock.release()

    def _traceroute_daemon(self):
        while True:
            now = time.time()
            # Traceroute new nodes
            for node_id in list(self._node_cache.keys()):
                if node_id not in self._tracerouted_nodes:
                    self._tracerouted_nodes.add(node_id)
                    threading.Thread(target=self._traceroute_worker, args=(node_id,), daemon=True).start()
            # Periodic re-traceroute
            for node_id in list(self._node_cache.keys()):
                last_time = self._last_traceroute_time.get(node_id, 0)
                if now - last_time > self._TRACEROUTE_INTERVAL:
                    logging.info(f"[Periodic] Re-tracerouting node {node_id} (last at {time.ctime(last_time)})")
                    threading.Thread(target=self._traceroute_worker, args=(node_id,), daemon=True).start()
            time.sleep(300)  # Check every 5 minutes

    def connect(self):
        """
        Connects to the MQTT broker and starts the MQTT loop.
        Includes reconnection logic for both MQTT and Meshtastic.
        """
        reconnect_attempts = 0
        
        while reconnect_attempts < self.max_reconnect_attempts:
            try:
                self.mqtt_client.connect(self.broker, self.port, 60)
                self.mqtt_client.loop_forever()
            except (BrokenPipeError, ConnectionError) as e:
                logging.error(f"Connection error: {e}")
                reconnect_attempts += 1
                
                if reconnect_attempts < self.max_reconnect_attempts:
                    logging.info(f"Attempting to reconnect in {self.reconnect_delay} seconds... (Attempt {reconnect_attempts}/{self.max_reconnect_attempts})")
                    time.sleep(self.reconnect_delay)
                    try:
                        self.interface = meshtastic.tcp_interface.TCPInterface(hostname=self.node_ip)
                        self.node_info = self.interface.getMyNodeInfo()
                        self.connected_node_id = self.node_info["user"]["id"]
                    except Exception as e:
                        logging.error(f"Failed to reconnect to Meshtastic: {e}")
                        continue
                else:
                    logging.error("Max reconnection attempts reached. Exiting...")
                    raise
            except KeyboardInterrupt:
                self.interface.close()
                self.mqtt_client.disconnect()
                self.mqtt_client.loop_stop()
                print("Exiting...")
                sys.exit(0)
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                raise
        
    def onReceive(self, packet, interface): # called when a packet arrives
        """
        Handles incoming Meshtastic packets.
        Args:
            packet (dict): The received packet data.
        """
        self._update_cache_from_packet(packet)
        logging.info("Packet Received!")
        packet_dict = {}
        for field_descriptor, field_value in packet.items():
            packet_dict[field_descriptor] = field_value

        packet_dict["gatewayId"] = self.connected_node_id

        self.publish_dict_to_mqtt(packet_dict)
    
    def publish_dict_to_mqtt(self, payload):
        """
        Publishes a dictionary payload to an MQTT topic.

        Args:
            payload (dict): The dictionary payload to publish.
        """
        
        topic_node = f"{self.topic}/{payload['fromId']}"
        payload = json.dumps(payload, default=str)
        
        # Publish the JSON payload to the specified topic
        self.mqtt_client.publish(topic_node, payload)


if __name__ == "__main__":
    """Main entry point for the Meshtastic MQTT handler."""
    
    parser = argparse.ArgumentParser(description='Meshtastic MQTT Handler')
    parser.add_argument('--broker', default='mqtt.nhmesh.live', action=EnvDefault, envvar="MQTT_ENDPOINT", help='MQTT broker address')
    parser.add_argument('--port', default=1883, type=int, action=EnvDefault, envvar="MQTT_PORT", help='MQTT broker port')
    parser.add_argument('--topic', default='msh/US/NH/', action=EnvDefault, envvar="MQTT_TOPIC", help='Root topic')
    parser.add_argument('--tls', type=bool, default=False, help='Enable TLS/SSL')
    parser.add_argument('--username', action=EnvDefault, envvar="MQTT_USERNAME", help='MQTT username')
    parser.add_argument('--password', action=EnvDefault, envvar="MQTT_PASSWORD", help='MQTT password')
    parser.add_argument('--node-ip', action=EnvDefault, envvar="NODE_IP", help='Node IP address')
    args = parser.parse_args()

    try:
        client = MeshtasticMQTTHandler(args.broker, args.port, args.topic, args.tls, args.username, args.password, args.node_ip)
        client.connect()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)