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