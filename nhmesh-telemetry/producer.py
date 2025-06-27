import logging
from os import environ
import sys
import socket
import paho.mqtt.client as mqtt
import json
import meshtastic
import meshtastic.tcp_interface
from pubsub import pub
import argparse
from utils.envdefault import EnvDefault
from utils.number_utils import safe_float, safe_float_list, safe_process_position
import time
import threading
import queue


logging.basicConfig(
    level=environ.get('LOG_LEVEL', "INFO").upper(),
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
        traceroute_cooldown (int): Cooldown between traceroutes in seconds (default: 30).
    """
    
    def __init__(self, broker, port, topic, tls, username, password, node_ip, traceroute_cooldown=30):
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
        self._last_traceroute_time = {}
        self._TRACEROUTE_INTERVAL = 3 * 60 * 60  # 3 hours
        self._TRACEROUTE_COOLDOWN = traceroute_cooldown  # Configurable cooldown between any traceroutes
        self._last_global_traceroute_time = 0  # Global cooldown timestamp
        self._traceroute_queue = queue.Queue()
        self._traceroute_in_progress = threading.Lock()  # Ensure only one traceroute at a time
        
        self._traceroute_worker_thread = threading.Thread(target=self._traceroute_worker, daemon=True)
        self._traceroute_worker_thread.start()
        logging.info(f"Traceroute worker thread started with single-threaded processing and {traceroute_cooldown}s cooldown.")

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
        node_id = str(node_id)  # Ensure node_id is always a string
        is_new_node = node_id not in self._node_cache
        entry = self._node_cache.setdefault(node_id, {"position": None, "long_name": None})
        decoded = packet.get("decoded", {})
        # Helper to get bytes from payload
        def get_payload_bytes(payload):
            if isinstance(payload, bytes):
                return payload
            elif isinstance(payload, str):
                import base64
                try:
                    return base64.b64decode(payload)
                except Exception:
                    return None
            else:
                return None
        # POSITION_APP
        if decoded.get("portnum") == "POSITION_APP":
            payload = decoded.get("payload")
            if not isinstance(payload, dict):
                payload_bytes = get_payload_bytes(payload)
                if payload_bytes:
                    try:
                        from meshtastic.protobuf import mesh_pb2
                        pos = mesh_pb2.Position()
                        pos.ParseFromString(payload_bytes)
                        if pos.latitude_i != 0 and pos.longitude_i != 0:
                            lat, lon, alt = safe_process_position(pos.latitude_i, pos.longitude_i, pos.altitude)
                            entry["position"] = (lat, lon, alt)
                        else:
                            entry["position"] = None
                    except Exception as e:
                        logging.warning(f"Error parsing position: {e}")
        # USER_APP
        if decoded.get("portnum") == "USER_APP":
            payload = decoded.get("payload")
            if not isinstance(payload, dict):
                payload_bytes = get_payload_bytes(payload)
                if payload_bytes:
                    try:
                        from meshtastic.protobuf import mesh_pb2
                        user = mesh_pb2.User()
                        user.ParseFromString(payload_bytes)
                        if user.long_name:
                            entry["long_name"] = user.long_name
                    except Exception as e:
                        logging.warning(f"Error parsing user: {e}")
        # TRACEROUTE_APP
        if decoded.get("portnum") == "TRACEROUTE_APP":
            payload = decoded.get("payload")
            if not isinstance(payload, dict):
                payload_bytes = get_payload_bytes(payload)
                if payload_bytes:
                    try:
                        from meshtastic.protobuf import mesh_pb2
                        route = mesh_pb2.RouteDiscovery()
                        route.ParseFromString(payload_bytes)
                        # Add route information to the packet for MQTT publishing
                        packet["route"] = list(route.route)
                        packet["snr_towards"] = safe_float_list(route.snr_towards)
                        packet["route_back"] = list(route.route_back)
                        packet["snr_back"] = safe_float_list(route.snr_back)
                    except Exception as e:
                        logging.warning(f"Error parsing traceroute: {e}")
        # Try to update from interface nodes DB if available
        if hasattr(self.interface, "nodes") and node_id in self.interface.nodes:
            user = self.interface.nodes[node_id].get("user", {})
            if user:
                entry["long_name"] = user.get("longName") or entry["long_name"]
        # Enqueue traceroute for new nodes
        if is_new_node:
            logging.info(f"[Traceroute] New node discovered: {node_id}, enqueuing traceroute job.")
            self._traceroute_queue.put((node_id, 0))  # 0 retries so far
        # Periodic re-traceroute
        now = time.time()
        last_time = self._last_traceroute_time.get(node_id, 0)
        if now - last_time > self._TRACEROUTE_INTERVAL:
            logging.info(f"[Traceroute] Periodic traceroute needed for node {node_id}, enqueuing job.")
            self._traceroute_queue.put((node_id, 0))

    def _run_traceroute(self, node_id):
        node_id = str(node_id)  # Ensure node_id is always a string
        entry = self._node_cache.get(node_id, {})
        long_name = entry.get("long_name")
        pos = entry.get("position")
        logging.info(f"[Traceroute] Running traceroute for Node {node_id} | Long name: {long_name if long_name else 'UNKNOWN'} | Position: {self._format_position(pos)}")
        
        # Acquire lock to ensure only one traceroute at a time
        with self._traceroute_in_progress:
            try:
                # Log node info before traceroute
                try:
                    info = self.interface.getMyNodeInfo()
                    logging.info(f"[Traceroute] Node info before traceroute: {info}")
                except Exception as e:
                    logging.error(f"[Traceroute] Failed to get node info before traceroute: {e}")
                
                # Pre traceroute log
                logging.info(f"[Traceroute] About to send traceroute to {node_id}")
                
                # Update global traceroute time before attempting
                self._last_global_traceroute_time = time.time()
                
                try:
                    self.interface.sendTraceRoute(dest=node_id, hopLimit=10)
                    self._last_traceroute_time[node_id] = time.time()
                    logging.info(f"[Traceroute] Traceroute command sent for node {node_id}.")
                    return True
                    
                except Exception as e:
                    logging.error(f"[Traceroute] Error sending traceroute to node {node_id}: {e}")
                    return False
                    
            except Exception as e:
                logging.error(f"[Traceroute] Unexpected error sending traceroute to node {node_id}: {e}")
                return False

    def _traceroute_worker(self):
        while True:
            try:
                # Get the next job from the queue (blocks until available)
                node_id, retries = self._traceroute_queue.get()
                logging.info(f"[Traceroute] Worker picked up job for node {node_id}, attempt {retries+1}.")
                
                # Check global cooldown before processing
                now = time.time()
                time_since_last = now - self._last_global_traceroute_time
                
                if time_since_last < self._TRACEROUTE_COOLDOWN:
                    wait_time = self._TRACEROUTE_COOLDOWN - time_since_last
                    logging.info(f"[Traceroute] Global cooldown active, sleeping {wait_time:.1f} seconds before processing node {node_id}")
                    
                    # Sleep in small increments to remain responsive
                    while wait_time > 0:
                        sleep_duration = min(wait_time, 1.0)  # Sleep max 1 second at a time
                        time.sleep(sleep_duration)
                        wait_time -= sleep_duration
                        
                        # Re-check if we still need to wait (in case another traceroute completed)
                        current_time = time.time()
                        remaining_cooldown = self._TRACEROUTE_COOLDOWN - (current_time - self._last_global_traceroute_time)
                        if remaining_cooldown <= 0:
                            break
                        wait_time = min(wait_time, remaining_cooldown)
                
                success = self._run_traceroute(node_id)
                if not success:
                    logging.error(f"[Traceroute] Failed to traceroute node {node_id} after {retries+1} attempts.")
                else:
                    logging.info(f"[Traceroute] Traceroute for node {node_id} completed or sent.")
                    
            except Exception as e:
                logging.error(f"[Traceroute] Worker encountered error: {e}")

    def cleanup(self):
        """
        Properly cleanup all resources.
        """
        logging.info("[Cleanup] Starting cleanup process...")
        
        # Close interface
        if hasattr(self, 'interface'):
            try:
                self.interface.close()
                logging.info("[Cleanup] Meshtastic interface closed.")
            except Exception as e:
                logging.error(f"[Cleanup] Error closing interface: {e}")
        
        # Disconnect MQTT
        if hasattr(self, 'mqtt_client'):
            try:
                self.mqtt_client.disconnect()
                self.mqtt_client.loop_stop()
                logging.info("[Cleanup] MQTT client disconnected.")
            except Exception as e:
                logging.error(f"[Cleanup] Error disconnecting MQTT: {e}")

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
                logging.info("Received KeyboardInterrupt, cleaning up...")
                self.cleanup()
                print("Exiting...")
                sys.exit(0)
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                self.cleanup()
                raise
        
    def onReceive(self, packet, interface): # called when a packet arrives
        """
        Handles incoming Meshtastic packets.
        Args:
            packet (bytes|dict|str): The received packet data (could be bytes, JSON string, or dict).
        """
        import json
        from meshtastic.protobuf import mesh_pb2
        import base64
        # Try to decode packet as JSON first
        packet_dict = None
        if isinstance(packet, dict):
            packet_dict = packet
        elif isinstance(packet, bytes):
            try:
                packet_dict = json.loads(packet.decode('utf-8'))
            except Exception:
                # Not JSON, try protobuf
                try:
                    mesh_packet = mesh_pb2.MeshPacket()
                    mesh_packet.ParseFromString(packet)
                    # Convert protobuf to dict
                    from google.protobuf import json_format
                    packet_dict = json_format.MessageToDict(mesh_packet, preserving_proto_field_name=True)
                except Exception as e:
                    logging.error(f"Failed to decode packet as protobuf: {e}")
                    return
        elif isinstance(packet, str):
            try:
                packet_dict = json.loads(packet)
            except Exception:
                # Not JSON, try base64 decode then protobuf
                try:
                    packet_bytes = base64.b64decode(packet)
                    mesh_packet = mesh_pb2.MeshPacket()
                    mesh_packet.ParseFromString(packet_bytes)
                    from google.protobuf import json_format
                    packet_dict = json_format.MessageToDict(mesh_packet, preserving_proto_field_name=True)
                except Exception as e:
                    logging.error(f"Failed to decode packet string as protobuf: {e}")
                    return
        else:
            logging.error(f"Unknown packet type: {type(packet)}")
            return

        self._update_cache_from_packet(packet_dict)
        logging.info("Packet Received!")
        out_packet = {}
        for field_descriptor, field_value in packet_dict.items():
            out_packet[field_descriptor] = field_value

        out_packet["gatewayId"] = self.connected_node_id
        out_packet["source"] = "rf"

        self.publish_dict_to_mqtt(out_packet)
    
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
    parser.add_argument('--traceroute-cooldown', default=30, type=int, action=EnvDefault, envvar="TRACEROUTE_COOLDOWN", help='Cooldown between traceroutes in seconds (default: 30)')
    args = parser.parse_args()

    client = None
    try:
        client = MeshtasticMQTTHandler(
            args.broker, 
            args.port, 
            args.topic, 
            args.tls, 
            args.username, 
            args.password, 
            args.node_ip,
            args.traceroute_cooldown
        )
        client.connect()
    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt in main, cleaning up...")
        if client:
            client.cleanup()
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        if client:
            client.cleanup()
        sys.exit(1)