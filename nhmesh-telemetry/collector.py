import logging
import sys
import json
import base64
import argparse

from paho.mqtt import client as mqtt_client
from elasticsearch import Elasticsearch
from datetime import datetime, timezone
from utils.envdefault import EnvDefault

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

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
        return payload_bytes.decode('utf-8')
    except UnicodeDecodeError:
        return base64.b64encode(payload_bytes).decode('ascii')


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT Broker!")
        client.subscribe(args.sub_topic)
    else:
        logging.error("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, msg):
    try:
        payload = safe_decode(msg.payload)
        data = json.loads(payload)

        logging.debug(f"Received from `{msg.topic}`")
        logging.debug(json.dumps(data, indent=2))
        # Prepare the document
        doc = {
            "topic": msg.topic,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Index the document
        res = es.options(request_timeout=10).index(index=index_name, document=doc)

        logging.info(f"Document indexed: {res['_id']}")

    except Exception as e:
        logging.error(f"Error processing message: {e}")

def run():
    client = mqtt_client.Client()
    client.username_pw_set(username=args.mqtt_username, password=args.mqtt_password)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(args.broker, args.port)
    client.loop_forever()

if __name__ == '__main__':
    run()
