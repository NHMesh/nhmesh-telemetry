# nhmesh-telemetry

## Docker Images

This project provides two Docker images:

### nhmesh/telemetry-producer

A Docker container that connects to a Meshtastic node and publishes packets to an MQTT broker.

#### Usage

```bash
docker run -d \
  --name telemetry-producer \
  -e NODE_IP=192.168.1.50 \
  -e MQTT_ENDPOINT=mqtt.nhmesh.live \
  -e MQTT_USERNAME=your_username \
  -e MQTT_PASSWORD=your_password \
  ghcr.io/nhmesh/telemetry-producer:latest
```

#### Environment Variables

| Variable        | Default            | Description                                     |
| --------------- | ------------------ | ----------------------------------------------- |
| `LOG_LEVEL`     | `INFO`             | Logging level (DEBUG, INFO, WARNING, ERROR)     |
| `MQTT_ENDPOINT` | `mqtt.nhmesh.live` | MQTT broker address                             |
| `MQTT_PORT`     | `1883`             | MQTT broker port                                |
| `MQTT_USERNAME` | -                  | MQTT username for authentication                |
| `MQTT_PASSWORD` | -                  | MQTT password for authentication                |
| `NODE_IP`       | -                  | IP address of the Meshtastic node to connect to |
| `MQTT_TOPIC`    | `msh/US/NH/`       | Root MQTT topic for publishing messages         |

#### Description

The Producer container connects to a Meshtastic node via its HTTP API and forwards packets from the mesh network to an MQTT broker. This allows for remote monitoring and processing of Meshtastic network traffic.

The producer requires:
1. A running Meshtastic node with HTTP API enabled
2. An MQTT broker that it can connect to with the provided credentials

### nhmesh/telemetry-collector

A Docker container that subscribes to MQTT topics with Meshtastic data and stores it into Elasticsearch.

#### Usage

```bash
docker run -d \
  --name telemetry-collector \
  -e MQTT_ENDPOINT=mqtt.nhmesh.live \
  -e MQTT_USERNAME=your_username \
  -e MQTT_PASSWORD=your_password \
  -e ES_ENDPOINT=your_elasticsearch_endpoint \
  -e ES_USERNAME=telemetry_writer \
  -e ES_PASSWORD=your_es_password \
  ghcr.io/nhmesh/telemetry-collector:latest
```

##### Environment Variables

| Variable                             | Default            | Description                                                 |
| ------------------------------------ | ------------------ | ----------------------------------------------------------- |
| `LOG_LEVEL`                          | `INFO`             | Logging level (DEBUG, INFO, WARNING, ERROR)                 |
| `MQTT_ENDPOINT`                      | `mqtt.nhmesh.live` | MQTT broker address                                         |
| `MQTT_PORT`                          | `1883`             | MQTT broker port                                            |
| `MQTT_USERNAME`                      | -                  | MQTT username for authentication                            |
| `MQTT_PASSWORD`                      | -                  | MQTT password for authentication                            |
| `MQTT_SUB_TOPIC`                     | `msh/US/#`         | MQTT topic pattern to subscribe to                          |
| `ES_ENDPOINT`                        | `large4cats`       | Elasticsearch endpoint URL                                  |
| `ES_USERNAME`                        | -                  | Elasticsearch username (typically `TELEMETRY_ES_USERNAME`)  |
| `ES_PASSWORD`                        | -                  | Elasticsearch password (typically `TELEMETRY_ES_PASSWORD`)  |
| `PACKET_PROCESSING_DELAY_SECONDS`    | `5.0`              | Seconds to delay packet processing to prioritize RF packets |
| `PACKET_PROCESSING_INTERVAL_SECONDS` | `0.5`              | Seconds interval for checking pending packets               |

##### Description

The Collector container subscribes to MQTT topics that contain Meshtastic packet data, processes these packets, and stores them into Elasticsearch for further analysis and visualization.

The collector requires:
1. An MQTT broker with Meshtastic data (usually published by the producer container)
2. An Elasticsearch instance for storing the processed data

## Development

 - Install Python
 - Install Poetry

```
poetry install

poetry run nhmesh-telemetry/producer.py
poetry run nhmesh-telemetry/collector.py
```

## Docker Setup

 1. In the docker folder... copy the sample.env to telemetry.env and tweak values
 2. `docker compose up -d`

 ```
 docker compose logs
 ```

Kibana dashboard - https://localhost:5601
