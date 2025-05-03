import logging
import sys
import json
import base64
import argparse
from paho.mqtt import client as mqtt_client
from datetime import datetime, timezone
from utils.envdefault import EnvDefault
from meshtastic.protobuf import mqtt_pb2
from google.protobuf import json_format
from google.protobuf import message
from elasticsearch import Elasticsearch

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
      "latitude": raw_packet.get("decoded", {}).get("position", {}).get("latitudeI"),
      "longitude": raw_packet.get("decoded", {}).get("position", {}).get("longitudeI"),
      "precision_bits": raw_packet.get("decoded", {}).get("position", {}).get("precisionBits"),
      "sats_in_view": raw_packet.get("decoded", {}).get("position", {}).get("satsInView"),
      "ground_speed": raw_packet.get("decoded", {}).get("position", {}).get("groundSpeed"),
      "ground_track": raw_packet.get("decoded", {}).get("position", {}).get("groundTrack"),
      "hardware": raw_packet.get("decoded", {}).get("user", {}).get("hardware"),
      "longname": raw_packet.get("decoded", {}).get("user", {}).get("longName"),
      "role": raw_packet.get("decoded", {}).get("user", {}).get("role"),
      "shortname": raw_packet.get("decoded", {}).get("user", {}).get("shortName"),
      "text": raw_packet.get("decoded", {}).get("text"),
    }
    return parsed_data
  except json.JSONDecodeError as e:
    logger.exception(f"Error decoding JSON for type 1: {e}")
    return None
  except Exception as e:
    logger.exception(f"An error occurred while parsing type 1: {e}")
    return None


def handle_meshtastic_mqtt(raw_packet):
  try:
    parsed_data = {
      "from_id_num": raw_packet.get("from"),
      "to_id_num": raw_packet.get("to"),
      "portnum": raw_packet.get("type"),  # Using 'type' as portnum equivalent
      "payload_raw": raw_packet.get(
        "payload", {}
      ),  # Raw payload not directly available in this type
      "telemetry_time": raw_packet.get("timestamp"),
      "battery_level": raw_packet.get("payload", {}).get("battery_level"),
      "voltage": raw_packet.get("payload", {}).get("voltage"),
      "channel_utilization": raw_packet.get("payload", {}).get("channel_utilization"),
      "air_util_tx": raw_packet.get("payload", {}).get("air_util_tx"),
      "uptime_seconds": raw_packet.get("payload", {}).get("uptime_seconds"),
      "packet_id": raw_packet.get("id"),
      "rx_time": raw_packet.get("timestamp"),  # Using timestamp as rx_time equivalent
      "hop_limit": None,  # Not directly present (related to hops_away)
      "priority": None,  # Not present in this type
      "from_id_str": raw_packet.get("sender"),
      "to_id_str": None,  # Not directly available, but 'to' is numerical
      "channel": raw_packet.get("channel"),
      "hop_start": raw_packet.get("hop_start"),
      "hops_away": raw_packet.get("hops_away"),
      "rssi": raw_packet.get("rssi"),
      "sender_id_str": raw_packet.get("sender"),
      "snr": raw_packet.get("snr"),
      "timestamp": raw_packet.get("timestamp"),
      "type": raw_packet.get("type"),
      "pdop": raw_packet.get("payload", {}).get("pdop"),
      "altitude": raw_packet.get("payload", {}).get("altitude"),
      "latitude": raw_packet.get("payload", {}).get("latitude_i"),
      "longitude": raw_packet.get("payload", {}).get("longitude_i"),
      "precision_bits": raw_packet.get("payload", {}).get("precision_bits"),
      "sats_in_view": raw_packet.get("payload", {}).get("sats_in_view"),
      "ground_speed": raw_packet.get("payload", {}).get("ground_speed"),
      "ground_track": raw_packet.get("payload", {}).get("ground_track"),
      "hardware": raw_packet.get("payload", {}).get("hardware"),
      "longname": raw_packet.get("payload", {}).get("longname"),
      "role": raw_packet.get("payload", {}).get("role"),
      "shortname": raw_packet.get("payload", {}).get("shortname"),
      "text": raw_packet.get("payload", {}).get("text"),
      "relay_node": raw_packet.get("relay_node"),
    }
    return parsed_data
  except json.JSONDecodeError as e:
    logger.exception(f"Error decoding JSON for type 2: {e}")
    return None
  except Exception as e:
    logger.exception(f"An error occurred while parsing type 2: {e}")
    return None


def handle_meshtastic_protobuf(raw_packet):
  binary_data = base64.b64decode(raw_packet)
  mesh_packet = mqtt_pb2.ServiceEnvelope()
  mesh_packet.ParseFromString(binary_data)

  return mesh_packet


def on_message(client, userdata, msg):
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
      except message.DecodeError:
        logger.exception("Failed to decode payload as JSON or protobuf")
        return
    logging.debug("Received from %s", msg.topic)

    if "type" in raw_packet:
      parsed_packet = handle_meshtastic_mqtt(raw_packet)
    else:
      parsed_packet = handle_producer_mqtt(raw_packet)

    # Prepare the document
    doc = {
      "topic": msg.topic,
      "raw": raw_packet,
      "parsed": parsed_packet,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "version": "1.0", # todo automatically get version from package data
    }
    
    # Index the document
    res = es.options(request_timeout=10).index(index=index_name, document=doc)
    
    logging.info(f"Document indexed: {res['_id']}")
    
    payload = json.dumps(doc, default=str)
    client.publish("msh_parsed", payload)

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
