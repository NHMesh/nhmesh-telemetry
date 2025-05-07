import logging
import sys
import json
import base64
import argparse
from paho.mqtt import client as mqtt_client
from datetime import datetime, timezone
from utils.envdefault import EnvDefault
from meshtastic.protobuf import mqtt_pb2, mesh_pb2, telemetry_pb2, admin_pb2
from google.protobuf import json_format
from google.protobuf import message
from elasticsearch import Elasticsearch
import enum
from collections import deque

logging.basicConfig(
  level=logging.DEBUG,
  format="%(asctime)s - %(levelname)s - %(message)s",
  stream=sys.stdout,
)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('--broker', default='mqtt.nhmesh.live', action=EnvDefault, envvar="MQTT_ENDPOINT", help='MQTT broker address')
parser.add_argument('--port', default=1883, type=int, action=EnvDefault, envvar="MQTT_PORT", help='MQTT broker port')
parser.add_argument('--sub-topic', default='msh/US/#', action=EnvDefault, envvar="MQTT_SUB_TOPIC", help='MQTT topic to subscribe to')
parser.add_argument('--mqtt-username', action=EnvDefault, envvar="MQTT_USERNAME", help='MQTT username')
parser.add_argument('--mqtt-password', action=EnvDefault, envvar="MQTT_PASSWORD", help='MQTT password')
parser.add_argument('--es-endpoint', action=EnvDefault, envvar="ES_ENDPOINT", default='large4cats', help='Elasticsearch Endpoint')
args = parser.parse_args()

# Elasticsearch Configuration
es = Elasticsearch([args.es_endpoint])  # update as needed
index_name = 'mesh_packets'


class FIFOCache:
    def __init__(self, capacity=1000):
        self.capacity = capacity
        self.cache = deque(maxlen=capacity)

    def add(self, item_id):
        """Adds a packet ID to the cache. If the cache is full, the oldest item is removed."""
        self.cache.append(item_id)

    def contains(self, item_id):
        """Checks if a packet ID is currently in the cache."""
        return item_id in self.cache

    def get_all(self):
        """Returns a list of all packet IDs in the cache (in FIFO order)."""
        return list(self.cache)

    def __len__(self):
        """Returns the current number of items in the cache."""
        return len(self.cache)
    
class PacketType(enum.Enum):
  ADMIN_APP = "Admin"
  MAP_REPORT_APP = "Map Report"
  NEIGHBORINFO_APP = "Neighbor Info"
  NODEINFO = "Node Info"
  NODEINFO_APP = "Node Info"
  POSITION = "Position" 
  POSITION_APP = "Position"
  ROUTING_APP = "Routing"
  STORE_FORWARD_APP = "Store Forward"
  TELEMETRY = "Telemetry"
  TELEMETRY_APP = "Telemetry"
  TEXT = "Text Message"
  TEXT_MESSAGE_APP = "Text Message"
  TRACEROUTE = "Traceroute"
  TRACEROUTE_APP = "Traceroute"
  USER_INFO = "User Info"

PACKET_ID_CACHE = FIFOCache(capacity=1000)

def safe_decode(payload_bytes):
  try:
    return payload_bytes.decode("utf-8")
  except UnicodeDecodeError:
    return base64.b64encode(payload_bytes).decode("ascii")


def on_connect(client, userdata, flags, rc):
  if rc == 0:
    logging.info("Connected to MQTT Broker!")
    client.subscribe(args.sub_topic)
  else:
    logging.error("Failed to connect, return code %d\n", rc)


def handle_producer_mqtt(raw_packet):
  """
  Handle the producer MQTT packet.
  Args:
      raw_packet (dict): The raw packet data.
  Returns:
      dict: The parsed packet data.
  """
  try:
    parsed_data = {
      "from_id_num": raw_packet.get("from"),
      "to_id_num": raw_packet.get("to"),
      "portnum": raw_packet.get("decoded", {}).get("portnum"),
      "payload_raw": raw_packet.get("decoded", {}).get("payload"),
      "decoded": raw_packet.get("decoded", {}),
      "telemetry_time": raw_packet.get("decoded", {}).get("telemetry", {}).get("time"),
      "battery_level": raw_packet.get("decoded", {})
      .get("telemetry", {})
      .get("deviceMetrics", {})
      .get("batteryLevel"),
      "voltage": raw_packet.get("decoded", {})
      .get("telemetry", {})
      .get("deviceMetrics", {})
      .get("voltage"),
      "channel_utilization": raw_packet.get("decoded", {})
      .get("telemetry", {})
      .get("deviceMetrics", {})
      .get("channelUtilization"),
      "air_util_tx": raw_packet.get("decoded", {})
      .get("telemetry", {})
      .get("deviceMetrics", {})
      .get("airUtilTx"),
      "uptime_seconds": raw_packet.get("decoded", {})
      .get("telemetry", {})
      .get("deviceMetrics", {})
      .get("uptimeSeconds"),
      "packet_id": raw_packet.get("id"),
      "rx_time": raw_packet.get("rxTime"),
      "hop_limit": raw_packet.get("hopLimit"),
      "priority": raw_packet.get("priority"),
      "from_id_str": raw_packet.get("fromId"),
      "to_id_str": raw_packet.get("toId"),
      "channel": None,  # Not present in this type
      "hop_start": None,  # Not present in this type
      "hops_away": None,  # Not directly present (related to hop_limit)
      "rssi": raw_packet.get("rxRssi"),  # Not present in this type
      "sender_id_str": raw_packet.get("fromId"),  # Using fromId as sender
      "snr": raw_packet.get("rxSnr"),  # Not present in this type
      "timestamp": raw_packet.get("rxTime"),  # Using rxTime as timestamp
      "type": raw_packet.get("decoded", {}).get("portnum"),  # Using portnum as type
      "want_response": raw_packet.get("decoded", {}).get("wantResponse"),
      "delay": raw_packet.get("delay"),
      "next_hop": raw_packet.get("nextHop"),
      "relay_node": raw_packet.get("relayNode"),
      "tx_after": raw_packet.get("txAfter"),
      "pki_encrypted": raw_packet.get("pkiEncrypted"),
      "pdop": raw_packet.get("decoded", {}).get("position", {}).get("pdop"),
      "altitude": raw_packet.get("decoded", {}).get("position", {}).get("altitude"),
      "latitude": raw_packet.get("decoded", {}).get("position", {}).get("latitude"),
      "longitude": raw_packet.get("decoded", {}).get("position", {}).get("longitude"),
      "precision_bits": raw_packet.get("decoded", {}).get("position", {}).get("precisionBits"),
      "sats_in_view": raw_packet.get("decoded", {}).get("position", {}).get("satsInView"),
      "ground_speed": raw_packet.get("decoded", {}).get("position", {}).get("groundSpeed"),
      "ground_track": raw_packet.get("decoded", {}).get("position", {}).get("groundTrack"),
      "hardware": raw_packet.get("decoded", {}).get("user", {}).get("hardware"),
      "longname": raw_packet.get("decoded", {}).get("user", {}).get("longName"),
      "role": raw_packet.get("decoded", {}).get("user", {}).get("role"),
      "shortname": raw_packet.get("decoded", {}).get("user", {}).get("shortName"),
    }

    parsed_data["geo"] = f"{parsed_data["latitude"]},{parsed_data["longitude"]}"

    return parsed_data
  except json.JSONDecodeError as e:
    logger.exception(f"Error decoding JSON for type 1: {e}")
    return None
  except Exception as e:
    logger.exception(f"An error occurred while parsing type 1: {e}")
    return None


def handle_meshtastic_protobuf(raw_packet):
  binary_data = base64.b64decode(raw_packet)
  mesh_packet = mqtt_pb2.ServiceEnvelope()
  mesh_packet.ParseFromString(binary_data)

  return mesh_packet

def meshdash_wrapper(parsed_packet) -> dict:
  event_id = f"pkt_{parsed_packet['rx_time']}_{parsed_packet['packet_id']}"
  try:
    app_packet_type = PacketType[parsed_packet['portnum'].upper()].value
  except (KeyError, AttributeError):
    app_packet_type = "UNIMPLEMENTED"

  meshdash_packet = {}
  meshdash_packet["event_id"] = event_id
  meshdash_packet["app_packet_type"] = app_packet_type
  meshdash_packet["from"] = parsed_packet["from_id_num"]
  meshdash_packet["to"] = parsed_packet["to_id_num"]
  meshdash_packet["decoded"] = parsed_packet["decoded"]
  meshdash_packet["id"] = parsed_packet["packet_id"]
  meshdash_packet["rxTime"] = parsed_packet["rx_time"]
  meshdash_packet["rxSnr"] = parsed_packet["snr"]
  meshdash_packet["rxRssi"] = parsed_packet["rssi"]
  meshdash_packet["hopLimit"] = parsed_packet["hop_limit"]
  meshdash_packet["hopStart"] = parsed_packet["hop_start"]
  meshdash_packet["raw"] = parsed_packet["payload_raw"]
  meshdash_packet["fromId"] = parsed_packet["from_id_str"]
  meshdash_packet["toId"] = parsed_packet["to_id_str"]
  meshdash_packet["timestamp"] = parsed_packet["timestamp"]

  if app_packet_type == "Text Message": 
    try:
      meshdash_packet["decoded"]["text"] = base64.b64decode(meshdash_packet["raw"]).decode("utf-8")
      meshdash_packet["raw"] = base64.b64decode(meshdash_packet["raw"]).decode("utf-8")
    except: 
      pass

  return meshdash_packet

def on_message(client, userdata, msg):
  packet_type = ""
  try:
    payload = safe_decode(msg.payload)
    try:
      raw_packet = json.loads(payload)
    except json.JSONDecodeError:
      # either the node is of type nRF52 or the the user doesn't have JSON enabled
      try:
        protobuf_packet = handle_meshtastic_protobuf(payload)
        protobuf_packet = json_format.MessageToDict(
          protobuf_packet, always_print_fields_with_no_presence=True
        )
        raw_packet = protobuf_packet["packet"]
        raw_packet["gatewayId"] = protobuf_packet["gatewayId"]
        raw_packet["channelId"] = protobuf_packet["channelId"]

        # Decode the payload and parse the protobuf
        if payload := raw_packet.get("decoded", {}).get("payload"):
          payload_bytes = base64.b64decode(payload)
          
          mesh_packet = None 

          match raw_packet.get("decoded", {}).get("portnum", ""):

            case "ROUTING_APP":
              mesh_packet = mesh_pb2.Routing()
            case "TEXT_MESSAGE_APP":
              pass
            case "TELEMETRY_APP":
              mesh_packet = telemetry_pb2.Telemetry()
            case "ADMIN_APP":
              mesh_packet = admin_pb2.AdminMessage()
            case "POSITION_APP":
              mesh_packet = mesh_pb2.Position()
            case "NODEINFO_APP":
              mesh_packet = mesh_pb2.User()
            case "TRACEROUTE_APP":
              mesh_packet = mesh_pb2.RouteDiscovery()
            case "MAP_REPORT_APP":
              mesh_packet = mqtt_pb2.MapReport()

          if mesh_packet:
            mesh_packet.ParseFromString(payload_bytes)
            mesh_packet = json_format.MessageToDict(
              mesh_packet, always_print_fields_with_no_presence=True
            )
            port_num = raw_packet.get("decoded", {}).get("portnum", "")
            raw_packet["decoded"] = mesh_packet
            raw_packet["decoded"]["portnum"] = port_num

        source = "mqtt"
        packet_type = "nfr52"

      except message.DecodeError:
        logger.exception("Failed to decode payload as JSON or protobuf")
        return
    logging.debug("Received from %s", msg.topic)

    source = "mqtt" if "gatewayId" in raw_packet else "rf"
    gateway_id = raw_packet.get("gatewayId", "NOT IMPLEMENTED")

    if "type" in raw_packet:
      raw_packet["decoded"] = raw_packet.get("payload", {})
      raw_packet["decoded"]["portnum"] = raw_packet.get("type")

    parsed_packet = handle_producer_mqtt(raw_packet)

    # Prepare the document
    doc = {
      "topic": msg.topic,
      "raw": raw_packet,
      "parsed": parsed_packet,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "version": "1.3", # todo automatically get version from package data
    }
    
    # Index the document
    try:
      res = es.options(request_timeout=10).index(index=index_name, document=doc)
      logging.info(f"Document indexed: {res['_id']}")
    except:
      logging.exception("es failed")

    meshdash_packet = meshdash_wrapper(parsed_packet)
    meshdash_packet["gateway_id"] = gateway_id
    meshdash_packet["source"] = source

    if meshdash_packet["app_packet_type"] == "UNIMPLEMENTED":
      print (raw_packet)

    packet_status = "dropped"
    if not PACKET_ID_CACHE.contains(meshdash_packet["id"]):
      payload = json.dumps(meshdash_packet, default=str)
      topic = f"msh_parsed/{source}/{meshdash_packet['from']}"
      client.publish(topic, payload)
      PACKET_ID_CACHE.add(meshdash_packet["id"])
      packet_status = "sent"


    who_heard = {
      "app_packet_type": "WHO_HEARD",
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "heard_by": gateway_id,
      "packet_id": meshdash_packet["id"],
      "status": packet_status,
    }
    topic = f"msh_parsed/who_heard/{who_heard['heard_by']}"
    payload = json.dumps(who_heard, default=str)
    client.publish(topic, payload)

    try:
      res = es.options(request_timeout=10).index(index="mesh_packets", document=meshdash_packet)
    except:
      logging.exception("es failed")

    meshdash_packet["timestamp"] = datetime.now(timezone.utc).isoformat()

    try:
      res = es.options(request_timeout=10).index(index="mesh_packets", document=meshdash_packet)
    except:
      logging.exception("es failed")

  except Exception as e:
    logging.exception(f"Error processing message: {e}")


def run():
  client = mqtt_client.Client()
  client.username_pw_set(username=args.mqtt_username, password=args.mqtt_password)

  client.on_connect = on_connect
  client.on_message = on_message

  client.connect(args.broker, args.port)
  client.loop_forever()


if __name__ == "__main__":
  run()
