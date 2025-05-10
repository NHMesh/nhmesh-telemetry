#!/usr/bin/env python3
"""
MQTT to Elasticsearch Bridge for Meshtastic Packets.

This script subscribes to an MQTT topic where Meshtastic nodes publish data,
parses the incoming packets (JSON or Protobuf), transforms them into a
standardized format, indexes them into Elasticsearch, and optionally
re-publishes a processed version to a different MQTT topic.
It also handles deduplication of packets using a FIFO cache and prioritizes
RF-originated packets over MQTT-relayed packets by delaying processing.
"""

import logging
import sys
import json
import base64
import argparse
import time
import threading # Added for DelayedPacketProcessor
from paho.mqtt import client as mqtt_client
from datetime import datetime, timezone
from utils.envdefault import EnvDefault # Assuming this is a custom utility
from meshtastic.protobuf import mqtt_pb2, mesh_pb2, telemetry_pb2, admin_pb2
from google.protobuf import json_format, message as google_protobuf_message
from elasticsearch import Elasticsearch, exceptions as es_exceptions
import enum
from collections import deque
from prettytable import PrettyTable  # Install via `pip install prettytable`

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO, # Default to INFO, DEBUG can be noisy
    format="%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# --- Constants ---
DEFAULT_MQTT_BROKER = "mqtt.nhmesh.live"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_SUB_TOPIC = "msh/US/#"
DEFAULT_ES_ENDPOINT = "large4cats"
DEFAULT_GATEWAY_ID_FALLBACK = "NOT_IMPLEMENTED"
ES_INDEX_RAW_PACKETS = "mesh_packets_raw"
ES_INDEX_MESH_EVENTS = "mesh_events"
SCRIPT_VERSION = "1.4.0" # Updated version
DEFAULT_PACKET_PROCESSING_DELAY_SECONDS = 1.5 # Delay to wait for RF packet
DEFAULT_PACKET_PROCESSING_INTERVAL_SECONDS = 0.5 # How often to check pending


# --- Packet Statistics Tracker ---
class PacketStatistics:
    """Tracks and displays statistics about processed packets."""
    def __init__(self):
        self.lock = threading.Lock()
        self.packet_counts_by_type = {}
        self.missing_fields_count = 0
        self.total_packets = 0

    def record_packet(self, packet_type, has_missing_fields):
        """Records a packet's type and whether it has missing fields."""
        with self.lock:
            self.total_packets += 1
            if packet_type not in self.packet_counts_by_type:
                self.packet_counts_by_type[packet_type] = 0
            self.packet_counts_by_type[packet_type] += 1
            if has_missing_fields:
                self.missing_fields_count += 1

    def get_statistics(self):
        """Returns a snapshot of the current statistics."""
        with self.lock:
            return {
                "total_packets": self.total_packets,
                "packet_counts_by_type": dict(self.packet_counts_by_type),
                "missing_fields_count": self.missing_fields_count,
            }

    def reset(self):
        """Resets the statistics."""
        with self.lock:
            self.packet_counts_by_type.clear()
            self.missing_fields_count = 0
            self.total_packets = 0


# --- Global Packet Statistics Instance ---
PACKET_STATS = PacketStatistics()


# --- Statistics Display Thread ---
def display_statistics():
    """Periodically displays packet statistics in a pretty table."""
    while True:
        stats = PACKET_STATS.get_statistics()
        table = PrettyTable()
        table.field_names = ["Packet Type", "Count"]
        for packet_type, count in stats["packet_counts_by_type"].items():
            table.add_row([packet_type, count])
        table.add_row(["Total Packets", stats["total_packets"]])
        table.add_row(["Missing Fields", stats["missing_fields_count"]])
        print("\nPacket Statistics:\n", table)
        time.sleep(5)  # Display every 5 seconds

# Start the statistics display thread
stats_thread = threading.Thread(target=display_statistics, daemon=True)
stats_thread.start()

# --- Argument Parsing ---
def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Meshtastic MQTT to Elasticsearch Bridge")
    # ... (MQTT and ES arguments remain the same) ...
    parser.add_argument(
        "--broker",
        default=DEFAULT_MQTT_BROKER,
        action=EnvDefault,
        envvar="MQTT_ENDPOINT",
        help="MQTT broker address",
    )
    parser.add_argument(
        "--port",
        default=DEFAULT_MQTT_PORT,
        type=int,
        action=EnvDefault,
        envvar="MQTT_PORT",
        help="MQTT broker port",
    )
    parser.add_argument(
        "--sub-topic",
        default=DEFAULT_MQTT_SUB_TOPIC,
        action=EnvDefault,
        envvar="MQTT_SUB_TOPIC",
        help="MQTT topic to subscribe to",
    )
    parser.add_argument(
        "--mqtt-username",
        action=EnvDefault,
        envvar="MQTT_USERNAME",
        help="MQTT username",
    )
    parser.add_argument(
        "--mqtt-password",
        action=EnvDefault,
        envvar="MQTT_PASSWORD",
        help="MQTT password",
    )
    parser.add_argument(
        "--es-endpoint",
        action=EnvDefault,
        envvar="ES_ENDPOINT",
        default=DEFAULT_ES_ENDPOINT,
        help="Elasticsearch Endpoint (e.g., http://localhost:9200)",
    )
    parser.add_argument(
        "--processing-delay",
        type=float,
        default=DEFAULT_PACKET_PROCESSING_DELAY_SECONDS,
        action=EnvDefault,
        envvar="PACKET_PROCESSING_DELAY_SECONDS",
        help="Seconds to delay packet processing to prioritize RF packets.",
    )
    parser.add_argument(
        "--processing-interval",
        type=float,
        default=DEFAULT_PACKET_PROCESSING_INTERVAL_SECONDS,
        action=EnvDefault,
        envvar="PACKET_PROCESSING_INTERVAL_SECONDS",
        help="Seconds interval for checking pending packets.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG logging output."
    )
    return parser.parse_args()


ARGS = parse_arguments() # Parse arguments early for use in other parts if needed
if ARGS.debug:
    logging.getLogger().setLevel(logging.DEBUG)


# --- FIFO Cache for PUBLISHED Packet Deduplication ---
class FIFOCache:
    """A simple FIFO cache with a fixed capacity for published packet IDs."""
    # ... (Class implementation remains the same) ...
    def __init__(self, capacity=1000):
        if capacity <= 0:
            raise ValueError("Cache capacity must be a positive integer.")
        self.capacity = capacity
        self.cache = deque(maxlen=capacity)
        self._cache_set = set()

    def add(self, item_id):
        if self.cache.maxlen and len(self.cache) == self.cache.maxlen:
            oldest_item = self.cache[0]
            if oldest_item in self._cache_set:
                self._cache_set.remove(oldest_item)
        self.cache.append(item_id)
        self._cache_set.add(item_id)

    def contains(self, item_id):
        return item_id in self._cache_set

    def get_all(self):
        return list(self.cache)

    def __len__(self):
        return len(self.cache)

PUBLISHED_PACKET_ID_CACHE = FIFOCache(capacity=2000) # For deduplicating final MQTT publishes


# --- Packet Type Enumeration ---
class PacketAppType(enum.Enum):
    """Enumeration of known Meshtastic application packet types."""
    # ... (Enum remains the same) ...
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
    REPLY_APP = "Reply"
    IP_TUNNEL_APP = "IP Tunnel"
    SERIAL_APP = "Serial"
    STOREFORWARD_APP = "Store and Forward"
    RANGE_TEST_APP = "Range Test"
    UNKNOWN_APP = "Unknown"


# --- Delayed Packet Processor ---
class DelayedPacketProcessor:
    """
    Manages a cache of packets to delay processing, allowing prioritization
    of RF packets over MQTT packets for the same message.
    """
    def __init__(self, delay_seconds, interval_seconds):
        self.pending_packets = {}
        self.lock = threading.Lock()
        self.delay_seconds = delay_seconds
        self.interval_seconds = interval_seconds
        self.processing_thread = threading.Thread(target=self._process_loop, daemon=True, name="DelayedPacketProcessorThread")
        self.shutdown_event = threading.Event()
        logger.info(f"DelayedPacketProcessor initialized with {delay_seconds}s delay and {interval_seconds}s interval.")

    def start(self):
        """Starts the background processing thread."""
        if not self.processing_thread.is_alive():
            self.shutdown_event.clear()
            self.processing_thread.start()
            logger.info("DelayedPacketProcessor thread started.")

    def stop(self):
        """Signals the background processing thread to stop and waits for it."""
        logger.info("Stopping DelayedPacketProcessor thread...")
        self.shutdown_event.set()
        if self.processing_thread.is_alive():
            self.processing_thread.join(timeout=self.interval_seconds * 2) # Wait for thread to finish
        if self.processing_thread.is_alive():
            logger.warning("DelayedPacketProcessor thread did not stop in time.")
        else:
            logger.info("DelayedPacketProcessor thread stopped.")


    def add_packet(self, packet_id, meshdash_event, source_type, mqtt_client_inst, es_client_inst):
        """
        Adds a packet to the pending cache or updates an existing one.
        RF packets will overwrite/supplement MQTT packets.
        """
        if not packet_id:
            logger.warning("Attempted to add packet with no ID to delayed processor.")
            return

        with self.lock:
            current_time = time.monotonic()
            if packet_id not in self.pending_packets:
                self.pending_packets[packet_id] = {
                    "rf_event": None,
                    "mqtt_event": None,
                    "first_arrival_time": current_time,
                    "mqtt_client": mqtt_client_inst, # Store for later use by processing thread
                    "es_client": es_client_inst,
                }
                logger.debug(f"New packet {packet_id} ({source_type}) added to delay cache.")

            entry = self.pending_packets[packet_id]
            if source_type == "rf":
                entry["rf_event"] = meshdash_event
                logger.debug(f"RF version of packet {packet_id} stored.")
            elif source_type == "mqtt":
                # Only store MQTT if RF isn't already there, or if this MQTT is newer (though RF takes precedence)
                # For simplicity, just store it. Prioritization happens at processing.
                entry["mqtt_event"] = meshdash_event
                logger.debug(f"MQTT version of packet {packet_id} stored.")
            # Ensure client instances are updated if a newer packet provides them (though typically they are the same)
            entry["mqtt_client"] = mqtt_client_inst
            entry["es_client"] = es_client_inst


    def _process_loop(self):
        """Periodically calls _process_pending_packets until shutdown."""
        while not self.shutdown_event.is_set():
            try:
                self._process_pending_packets()
            except Exception as e:
                logger.exception(f"Error in DelayedPacketProcessor loop: {e}")
            # Wait for the interval or until shutdown is signaled
            self.shutdown_event.wait(self.interval_seconds)
        logger.info("DelayedPacketProcessor loop finished.")


    def _process_pending_packets(self):
        """Processes packets in the cache that have passed their delay."""
        current_time = time.monotonic()
        processed_ids = []

        # Iterate over a copy of keys if modifying the dict during iteration,
        # but here we build a list of IDs to process then modify outside loop or carefully.
        # For simplicity, lock during the check and potential processing decision phase.
        with self.lock:
            packet_ids_to_check = list(self.pending_packets.keys()) # Iterate over a snapshot of keys

            for packet_id in packet_ids_to_check:
                entry = self.pending_packets.get(packet_id) # Re-fetch in case it was removed
                if not entry:
                    continue

                if (current_time - entry["first_arrival_time"]) > self.delay_seconds:
                    chosen_event = None
                    chosen_source = None

                    if entry["rf_event"]:
                        chosen_event = entry["rf_event"]
                        chosen_source = "RF"
                        logger.debug(f"Prioritizing RF version for packet {packet_id}.")
                    elif entry["mqtt_event"]:
                        chosen_event = entry["mqtt_event"]
                        chosen_source = "MQTT"
                        logger.debug(f"No RF version for {packet_id} after delay, using MQTT version.")
                    else:
                        logger.warning(f"Packet {packet_id} lingered in delay cache with no event data. Removing.")
                        processed_ids.append(packet_id) # Mark for removal
                        continue # No event to process

                    if chosen_event:
                        logger.info(f"Processing packet {packet_id} from source {chosen_source} after delay.")
                        mqtt_c = entry["mqtt_client"]
                        es_c = entry["es_client"]

                        # --- Deduplication and MQTT Re-publishing (using PUBLISHED_PACKET_ID_CACHE) ---
                        packet_publish_status = "dropped_duplicate_in_final_cache"
                        unique_event_id = chosen_event.get("id") # This is the original packet_id string

                        if not unique_event_id:
                            logger.warning(f"Chosen event for {packet_id} has no 'id' field for final deduplication.")
                            packet_publish_status = "dropped_no_id_in_event"
                        # NodeInfo packets are often sent periodically; original script always re-published.
                        elif chosen_event.get("app_packet_type") == PacketAppType.NODEINFO.value or \
                             not PUBLISHED_PACKET_ID_CACHE.contains(unique_event_id):

                            meshdash_payload_json = json.dumps(chosen_event, default=str)
                            from_node_id_for_topic = chosen_event.get("from") # node ID string
                            source_type_for_topic = chosen_event.get("source_type", "unknown_source") # rf or mqtt

                            if from_node_id_for_topic:
                                republish_topic = f"msh_parsed/{source_type_for_topic}/{from_node_id_for_topic}"
                                try:
                                    if mqtt_c and hasattr(mqtt_c, 'publish'):
                                        mqtt_c.publish(republish_topic, meshdash_payload_json)
                                        logger.info(f"Re-published processed event {unique_event_id} to: {republish_topic}")
                                        packet_publish_status = "sent_to_mqtt"
                                    else:
                                        logger.error(f"MQTT client not available for re-publishing {unique_event_id}")
                                        packet_publish_status = "mqtt_client_unavailable"
                                except Exception as e:
                                    logger.error(f"Failed to re-publish {unique_event_id} to MQTT: {e}")
                                    packet_publish_status = "mqtt_publish_failed"
                            else:
                                logger.warning(f"Cannot re-publish {unique_event_id}: 'from' field missing.")
                                packet_publish_status = "dropped_no_from_id"

                            if unique_event_id and chosen_event.get("app_packet_type") != PacketAppType.NODEINFO.value:
                                PUBLISHED_PACKET_ID_CACHE.add(unique_event_id)
                        else:
                            logger.debug(f"Event ID {unique_event_id} (from packet {packet_id}) already in PUBLISHED_PACKET_ID_CACHE. Skipping re-publish.")

                        # --- Elasticsearch Indexing: MeshDash Event ---
                        if es_c:
                            index_document_to_es(es_c, ES_INDEX_MESH_EVENTS, chosen_event, chosen_event.get("event_id"))
                        else:
                            logger.error(f"Elasticsearch client not available for indexing event {chosen_event.get('event_id')}")

                        # Add to list of processed IDs for removal from pending_packets
                        processed_ids.append(packet_id)

            # Remove processed packets from the cache
            if processed_ids:
                logger.debug(f"Removing {len(processed_ids)} processed packets from delay cache: {processed_ids}")
                for pid in processed_ids:
                    if pid in self.pending_packets:
                        del self.pending_packets[pid]


# --- Utility, ES, Parsing, Transformation Functions (largely unchanged) ---
# safe_payload_decode, get_elasticsearch_client, index_document_to_es
# _decode_inner_protobuf, deserialize_mqtt_payload
# parse_standard_meshtastic_packet, parse_meshdash_nodeinfo_packet
# get_packet_app_type, create_meshdash_event
# (These functions remain the same as in the previous version, so they are omitted here for brevity
#  but should be included in the full script.)
# --- Placeholder for the unchanged functions from the previous response ---
def safe_payload_decode(payload_bytes):
    """Safely decodes payload bytes to a string."""
    try:
        return payload_bytes.decode("utf-8")
    except UnicodeDecodeError:
        logger.debug("Payload is not valid UTF-8, encoding as base64.")
        return base64.b64encode(payload_bytes).decode("ascii")
    except Exception as e:
        logger.error(f"Unexpected error during payload decoding: {e}")
        return ""

def get_elasticsearch_client(es_endpoint_url):
    """Creates and returns an Elasticsearch client instance."""
    try:
        if not es_endpoint_url.startswith(("http://", "https://")):
            logger.warning(f"ES endpoint '{es_endpoint_url}' no scheme, prepending 'http://'.")
            es_endpoint_url = f"http://{es_endpoint_url}"
        es_client = Elasticsearch([es_endpoint_url], timeout=10, max_retries=3, retry_on_timeout=True)
        if not es_client.ping():
            logger.error(f"Failed to connect to Elasticsearch at {es_endpoint_url}")
            return None
        logger.info(f"Successfully connected to Elasticsearch at {es_endpoint_url}")
        return es_client
    except es_exceptions.ConnectionError as e:
        logger.error(f"Elasticsearch connection error for {es_endpoint_url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error connecting to Elasticsearch: {e}")
    return None

def index_document_to_es(es_client, index_name, document, document_id=None):
    """Indexes a document into the specified Elasticsearch index."""
    if not es_client:
        logger.error("ES client unavailable. Cannot index document.")
        return False
    try:
        response = es_client.index(index=index_name, document=document, id=document_id)
        logger.debug(f"Doc indexed to '{index_name}' ID: {response.get('_id')}")
        return True
    except es_exceptions.ConnectionTimeout as e:
        logger.error(f"Timeout indexing to '{index_name}': {e}")
    except es_exceptions.RequestError as e:
        logger.error(f"Request error indexing to '{index_name}': {e}. Doc: {str(document)[:200]}")
    except es_exceptions.ElasticsearchException as e:
        logger.error(f"ES error indexing to '{index_name}': {e}")
    except Exception as e:
        logger.error(f"Unexpected error indexing to '{index_name}': {e}")
    return False

def _decode_inner_protobuf(portnum_str, payload_bytes):
    """Decodes the inner Protobuf message based on the portnum."""
    mesh_packet_proto = None
    try:
        app_type = PacketAppType[portnum_str.upper()]
    except KeyError:
        logger.warning(f"Unknown portnum for inner Protobuf decoding: {portnum_str}")
        return None

    type_to_proto_map = {
        PacketAppType.ROUTING_APP: mesh_pb2.Routing,
        PacketAppType.TELEMETRY_APP: telemetry_pb2.Telemetry,
        PacketAppType.ADMIN_APP: admin_pb2.AdminMessage,
        PacketAppType.POSITION_APP: mesh_pb2.Position,
        PacketAppType.NODEINFO_APP: mesh_pb2.User,
        PacketAppType.USER_INFO: mesh_pb2.User,
        PacketAppType.TRACEROUTE_APP: mesh_pb2.RouteDiscovery,
        PacketAppType.MAP_REPORT_APP: mqtt_pb2.MapReport,
    }
    if app_type in type_to_proto_map:
        mesh_packet_proto = type_to_proto_map[app_type]()
    elif app_type == PacketAppType.TEXT_MESSAGE_APP:
        return {}

    if mesh_packet_proto:
        try:
            mesh_packet_proto.ParseFromString(payload_bytes)
            return json_format.MessageToDict(mesh_packet_proto, always_print_fields_with_no_presence=True)
        except google_protobuf_message.DecodeError as e:
            logger.error(f"Failed to parse inner Protobuf for {portnum_str}: {e}")
    return None

def deserialize_mqtt_payload(payload_bytes):
    """Deserializes the raw MQTT payload (JSON or base64 Protobuf)."""
    decoded_payload_str = safe_payload_decode(payload_bytes)
    raw_packet_dict = None
    initial_format = None
    try:
        raw_packet_dict = json.loads(decoded_payload_str)
        initial_format = "json"
    except json.JSONDecodeError:
        try:
            service_envelope_bytes = base64.b64decode(decoded_payload_str)
            service_envelope = mqtt_pb2.ServiceEnvelope()
            service_envelope.ParseFromString(service_envelope_bytes)
            raw_packet_dict = json_format.MessageToDict(service_envelope.packet, always_print_fields_with_no_presence=True)
            raw_packet_dict["gatewayId"] = service_envelope.gateway_id
            raw_packet_dict["channelId"] = service_envelope.channel_id
            initial_format = "protobuf"
            inner_payload_b64 = raw_packet_dict.get("decoded", {}).get("payload")
            portnum_str = raw_packet_dict.get("decoded", {}).get("portnum")
            if inner_payload_b64 and isinstance(inner_payload_b64, str) and portnum_str:
                try:
                    inner_payload_bytes = base64.b64decode(inner_payload_b64)
                    decoded_inner_packet = _decode_inner_protobuf(portnum_str, inner_payload_bytes)
                    if decoded_inner_packet is not None:
                        raw_packet_dict["decoded"] = decoded_inner_packet
                        raw_packet_dict["decoded"]["portnum"] = portnum_str
                        if portnum_str == "TEXT_MESSAGE_APP":
                             raw_packet_dict["decoded"]["payload_bytes_for_text"] = inner_payload_bytes
                except Exception as e:
                    logger.warning(f"Could not base64 or protobuf decode inner payload: {e}")
        except Exception as e:
            logger.error(f"Failed to decode payload as JSON or Protobuf: {e}")
            return None, None
    return raw_packet_dict, initial_format

def parse_standard_meshtastic_packet(raw_packet_dict):
    """Parses a standard Meshtastic packet."""
    try:
        decoded_part = raw_packet_dict.get("decoded", {})
        telemetry_part = decoded_part.get("telemetry", {})
        device_metrics = telemetry_part.get("deviceMetrics", {})
        position_part = decoded_part.get("position", {})
        user_part = decoded_part.get("user", {})
        payload_data = decoded_part.get("payload")
        payload_raw_str = safe_payload_decode(payload_data) if isinstance(payload_data, bytes) else payload_data if isinstance(payload_data, str) else None

        rx_time_val = raw_packet_dict.get("rxTime")
        rx_timestamp_iso = datetime.fromtimestamp(rx_time_val, timezone.utc).isoformat() if rx_time_val is not None else None
        lat = position_part.get("latitudeI") / 1e7 if position_part.get("latitudeI") is not None else None
        lon = position_part.get("longitudeI") / 1e7 if position_part.get("longitudeI") is not None else None

        parsed_data = {
            "from_id_num": raw_packet_dict.get("from"), "to_id_num": raw_packet_dict.get("to"),
            "portnum": decoded_part.get("portnum"), "payload_raw": payload_raw_str,
            "decoded_content": decoded_part, "telemetry_time": telemetry_part.get("time"),
            "battery_level": device_metrics.get("batteryLevel"), "voltage": device_metrics.get("voltage"),
            "channel_utilization": device_metrics.get("channelUtilization"), "air_util_tx": device_metrics.get("airUtilTx"),
            "uptime_seconds": device_metrics.get("uptimeSeconds"), "packet_id": raw_packet_dict.get("id"),
            "rx_time": rx_time_val, 
            "rx_timestamp_iso": rx_timestamp_iso,
            "hop_limit": raw_packet_dict.get("hopLimit"), 
            "priority": raw_packet_dict.get("priority"),
            "from_id_str": raw_packet_dict.get("fromId"), "to_id_str": raw_packet_dict.get("toId"),
            "channel_id": raw_packet_dict.get("channelId"),
            "channel": raw_packet_dict.get("channel"), 
            "gateway_id": raw_packet_dict.get("gatewayId"),
            "hop_start": raw_packet_dict.get("hopStart"), "rssi": raw_packet_dict.get("rxRssi"),
            "snr": raw_packet_dict.get("rxSnr"), "want_response": decoded_part.get("wantAck") or decoded_part.get("wantResponse"),
            "pki_encrypted": raw_packet_dict.get("encrypted"),
            "pdop": position_part.get("pdop"), "altitude": position_part.get("altitude"),
            "latitude": lat, "longitude": lon,
            "precision_bits": position_part.get("precisionBits"), "sats_in_view": position_part.get("satsInView"),
            "position_time": position_part.get("time"), "ground_speed": position_part.get("groundSpeed"),
            "ground_track": position_part.get("groundTrack"),
            "user_hardware_model_id": user_part.get("hwModel"), "user_long_name": user_part.get("longName"),
            "user_role": user_part.get("role"), "user_short_name": user_part.get("shortName"),
            "user_is_licensed": user_part.get("isLicensed"),
            "geo_point": f"{lat},{lon}" if lat is not None and lon is not None else None
        }
        if "role" in user_part and isinstance(user_part["role"], int):
            try: parsed_data["user_role"] = mesh_pb2.Config.DeviceConfig.Role.Name(user_part["role"])
            except ValueError: parsed_data["user_role"] = f"UNKNOWN_ROLE_INT_{user_part['role']}"
        if decoded_part.get("portnum") == "TEXT_MESSAGE_APP" and "payload_bytes_for_text" in decoded_part:
            try:
                parsed_data["decoded_content"]["text"] = decoded_part["payload_bytes_for_text"].decode('utf-8')
                parsed_data["payload_raw"] = parsed_data["decoded_content"]["text"]
            except UnicodeDecodeError: logger.warning("UTF-8 decode failed for TEXT_MESSAGE_APP payload_bytes")
        return parsed_data
    except Exception as e:
        logger.exception(f"Error parsing standard packet: {e} - Pkt: {str(raw_packet_dict)[:200]}")
    return None

def parse_meshdash_nodeinfo_packet(raw_packet_dict):
    """Parses a Mesh Dash specific Node Info packet."""
    try:
        position_part = raw_packet_dict.get("position", {})
        user_part = raw_packet_dict.get("user", {})
        device_metrics = raw_packet_dict.get("deviceMetrics", {})
        from_id_num_val = raw_packet_dict.get("from", raw_packet_dict.get("node_id"))
        rx_time_val = raw_packet_dict.get("rxTime", raw_packet_dict.get("telemetry_time"))
        rx_timestamp_iso = datetime.fromtimestamp(rx_time_val, timezone.utc).isoformat() if rx_time_val is not None else None
        lat = position_part.get("latitude")
        lon = position_part.get("longitude")
        geo_point = None
        if lat is not None and lon is not None and (-90 <= lat <= 90) and (-180 <= lon <= 180):
            geo_point = f"{lat},{lon}"
        else:
            if lat is not None or lon is not None: # If any provided but invalid
                logger.warning(f"Invalid lat/lon for MeshDash nodeinfo: {lat},{lon}")
            lat, lon = None, None # Nullify invalid data

        parsed_data = {
            "from_id_num": from_id_num_val, "to_id_num": raw_packet_dict.get("to"),
            "portnum": PacketAppType.NODEINFO.name, "payload_raw": None,
            "decoded_content": user_part, "telemetry_time": raw_packet_dict.get("telemetry_time"),
            "battery_level": raw_packet_dict.get("batteryLevel", device_metrics.get("batteryLevel")),
            "voltage": raw_packet_dict.get("voltage", device_metrics.get("voltage")),
            "channel_utilization": raw_packet_dict.get("channelUtilization", device_metrics.get("channelUtilization")),
            "air_util_tx": raw_packet_dict.get("airUtilTx", device_metrics.get("airUtilTx")),
            "uptime_seconds": device_metrics.get("uptimeSeconds", device_metrics.get("upTimeSeconds")),
            "packet_id": str(rx_time_val) + "_" + str(from_id_num_val),
            "rx_time": rx_time_val, "rx_timestamp_iso": rx_timestamp_iso,
            "hop_limit": raw_packet_dict.get("hopLimit"), "priority": raw_packet_dict.get("priority"),
            "from_id_str": raw_packet_dict.get("fromId", str(from_id_num_val)),
            "to_id_str": raw_packet_dict.get("toId", "^all"),
            "channel_id": raw_packet_dict.get("channelId"),
            "channel": raw_packet_dict.get("channel"),
            "gateway_id": raw_packet_dict.get("gatewayId"),
            "rssi": raw_packet_dict.get("rssi"), "snr": raw_packet_dict.get("snr"),
            "altitude": position_part.get("altitude"), "latitude": lat, "longitude": lon,
            "sats_in_view": position_part.get("satsInView"), "position_time": position_part.get("time"),
            "user_hardware_model_id": user_part.get("hwModel", raw_packet_dict.get("hardware")),
            "user_long_name": user_part.get("longName"), "user_role": user_part.get("role"),
            "user_short_name": user_part.get("shortName"), "user_is_licensed": user_part.get("isLicensed"),
            "geo_point": geo_point
        }
        if "role" in user_part and isinstance(user_part["role"], int):
            try: parsed_data["user_role"] = mesh_pb2.Config.DeviceConfig.Role.Name(user_part["role"])
            except ValueError: parsed_data["user_role"] = f"UNKNOWN_ROLE_INT_{user_part['role']}"
        return parsed_data
    except Exception as e:
        logger.exception(f"Error parsing MeshDash NodeInfo: {e} - Pkt: {str(raw_packet_dict)[:200]}")
    return None

def get_packet_app_type(portnum_value):
    """Determines the PacketAppType from a portnum string or int."""
    if portnum_value is None: return PacketAppType.UNKNOWN_APP
    portnum_str = str(portnum_value).upper()
    try: return PacketAppType[portnum_str]
    except KeyError:
        if isinstance(portnum_value, int): # If int, try to map via protobuf's PortNum
            try: portnum_str = mesh_pb2.PortNum.Name(portnum_value)
            except ValueError: portnum_str = f"UNKNOWN_PORTNUM_INT_{portnum_value}"
            try: return PacketAppType[portnum_str] # Try matching again with named portnum
            except KeyError: pass # Fall through to string checks
        # Fallback for common variations
        if "TEXT" in portnum_str: return PacketAppType.TEXT_MESSAGE_APP
        if "POSITION" in portnum_str: return PacketAppType.POSITION_APP
        if "NODEINFO" in portnum_str: return PacketAppType.NODEINFO_APP
        if "TELEMETRY" in portnum_str: return PacketAppType.TELEMETRY_APP
        logger.warning(f"Could not map portnum '{portnum_value}' to PacketAppType.")
        return PacketAppType.UNKNOWN_APP

def create_meshdash_event(parsed_packet_data):
    """Transforms a parsed packet into the 'MeshDash' event format."""
    if not parsed_packet_data: return {}
    app_type_enum = get_packet_app_type(parsed_packet_data.get("portnum"))
    rx_time = parsed_packet_data.get("rx_time")
    event_timestamp = rx_time if rx_time is not None else datetime.now(timezone.utc).timestamp()

    meshdash_event = {
        "event_id": f"pkt_{parsed_packet_data.get('packet_id', str(event_timestamp))}",
        "app_packet_type": app_type_enum.value,
        "from": parsed_packet_data.get("from_id_num"), "to": parsed_packet_data.get("to_id_num"),
        "decoded": parsed_packet_data.get("decoded_content", {}),
        "id": parsed_packet_data.get("packet_id"), # This is the crucial one from the packet
        "rxTime": rx_time, "rxTimeISO": parsed_packet_data.get("rx_timestamp_iso"),
        "rxSnr": parsed_packet_data.get("snr"), "rxRssi": parsed_packet_data.get("rssi"),
        "hopLimit": parsed_packet_data.get("hop_limit"), "hopStart": parsed_packet_data.get("hop_start"),
        "raw_payload": parsed_packet_data.get("payload_raw"),
        "fromId": parsed_packet_data.get("from_id_str"), "toId": parsed_packet_data.get("to_id_str"),
        "timestamp": event_timestamp, # MeshDash expects epoch here
        "channelId": parsed_packet_data.get("channel_id"),
        "channel": parsed_packet_data.get("channel"),
        "altitude": parsed_packet_data.get("altitude"), "latitude": parsed_packet_data.get("latitude"),
        "longitude": parsed_packet_data.get("longitude"), "geo_point": parsed_packet_data.get("geo_point"),
        "batteryLevel": parsed_packet_data.get("battery_level"), "voltage": parsed_packet_data.get("voltage"),
        "channelUtilization": parsed_packet_data.get("channel_utilization"),
        "airUtilTx": parsed_packet_data.get("air_util_tx"),
        "uptimeSeconds": parsed_packet_data.get("uptime_seconds"),
        "user_long_name": parsed_packet_data.get("user_long_name"),
        "user_short_name": parsed_packet_data.get("user_short_name"),
        "user_hw_model": parsed_packet_data.get("user_hardware_model_id")
    }
    # If text message and text is already decoded in 'decoded_content.text' by parser
    if app_type_enum == PacketAppType.TEXT_MESSAGE_APP and "text" in meshdash_event["decoded"]:
        pass # Text already handled by parsers
    elif app_type_enum == PacketAppType.TEXT_MESSAGE_APP and meshdash_event["raw_payload"]:
        try: # Attempt to decode from raw_payload if not in decoded_content
            decoded_text = base64.b64decode(meshdash_event["raw_payload"]).decode('utf-8')
            meshdash_event["decoded"]["text"] = decoded_text
        except Exception:
            logger.debug(f"Could not decode raw_payload for text in meshdash_event: {meshdash_event['raw_payload'][:50]}")
    return meshdash_event
# --- End of placeholder ---


# --- MQTT Callbacks and Main Logic ---
def on_mqtt_connect(client, userdata, flags, rc):
    """Callback for when the client receives a CONNACK response from the server."""
    if rc == 0:
        logger.info("Successfully connected to MQTT Broker!")
        client.subscribe(ARGS.sub_topic) # ARGS is global
        logger.info(f"Subscribed to topic: {ARGS.sub_topic}")
    else:
        logger.error(f"Failed to connect to MQTT Broker, return code {rc}")


# --- Update process_mqtt_message to record statistics ---
def process_mqtt_message(client, msg, es_client_instance, delayed_processor):
    """
    Processes a single incoming MQTT message.
    Orchestrates deserialization, parsing, transformation, and hands off to delayed processor.
    """
    logger.debug(f"Received message on topic: {msg.topic} | Payload size: {len(msg.payload)} bytes")

    raw_packet_dict, initial_format = deserialize_mqtt_payload(msg.payload)
    if not raw_packet_dict:
        logger.error("Failed to deserialize MQTT payload, skipping message.")
        return

    is_relayed_via_mqtt = "gatewayId" in raw_packet_dict and raw_packet_dict["gatewayId"] is not None
    source_type = "mqtt" if is_relayed_via_mqtt else "rf"

    gateway_id = raw_packet_dict.get("gatewayId")
    if not gateway_id:
        if "peter" in msg.topic: gateway_id = "!a2ebb954" # Example specific rule
        else: 
            gateway_id = msg.topic.split("/")[-1]
    
    current_node_id = None # Potentially to identify this specific instance if it's an RF gateway

    # Heuristic: if source is RF, gatewayId should ideally be this instance's ID.
    # For now, using the determined gateway_id.
    # A --gateway-node-id argument could set current_node_id.
    if source_type == "rf" and current_node_id:
        gateway_id = current_node_id


    if "type" in raw_packet_dict and "decoded" not in raw_packet_dict:
        raw_packet_dict["decoded"] = raw_packet_dict.get("payload", {})
        raw_packet_dict["decoded"]["portnum"] = raw_packet_dict.get("type")

    if "user" in raw_packet_dict and ("node_id" in raw_packet_dict or "name" in raw_packet_dict.get("user",{})):
        parsed_packet_data = parse_meshdash_nodeinfo_packet(raw_packet_dict)
        packet_parser_type = "meshdash_nodeinfo"
    else:
        parsed_packet_data = parse_standard_meshtastic_packet(raw_packet_dict)
        packet_parser_type = "standard_meshtastic"

    if not parsed_packet_data:
        logger.error("Failed to parse packet data after deserialization.")
        return

    parsed_packet_data["_parser_type"] = packet_parser_type
    parsed_packet_data["_initial_format"] = initial_format
    unique_packet_identifier = parsed_packet_data.get("packet_id") # Crucial for all forms of deduplication

    if not unique_packet_identifier:
        logger.warning(f"Packet missing 'id' or 'packet_id' field, cannot reliably process or deduplicate. "
                       f"Topic: {msg.topic}, From: {parsed_packet_data.get('from_id_str', 'N/A')}")
        return

    # --- Record statistics ---
    packet_type = parsed_packet_data.get("portnum", "UNKNOWN")
    has_missing_fields = any(value is None for key, value in parsed_packet_data.items())
    PACKET_STATS.record_packet(packet_type, has_missing_fields)

    # --- Elasticsearch Indexing: Raw and Parsed (Happens Immediately) ---
    raw_doc_to_index = {
        "topic": msg.topic,
        "raw_deserialized_packet": raw_packet_dict,
        "parsed_intermediate_packet": parsed_packet_data,
        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        "script_version": SCRIPT_VERSION,
        "source_gateway_id": gateway_id,
        "reception_type": source_type, # 'rf' or 'mqtt' (of this specific message)
    }
    index_document_to_es(es_client_instance, ES_INDEX_RAW_PACKETS, raw_doc_to_index)

    # --- Transform to MeshDash Event format ---
    meshdash_event = create_meshdash_event(parsed_packet_data)
    if not meshdash_event:
        logger.error(f"Failed to create MeshDash event from parsed data for packet_id {unique_packet_identifier}.")
        return

    # Add determined gateway_id and source_type to the meshdash_event
    # This source_type is of THIS specific packet that created this event.
    # The delayed processor will choose based on RF presence.
    meshdash_event["gateway_id"] = gateway_id
    meshdash_event["source_type"] = source_type # 'rf' or 'mqtt' for this specific packet
    meshdash_event["processing_timestamp_iso"] = datetime.now(timezone.utc).isoformat()

    if meshdash_event.get("app_packet_type") == PacketAppType.UNKNOWN_APP.value:
        logger.warning(f"UNIMPLEMENTED app_packet_type for {unique_packet_identifier}. Portnum: {parsed_packet_data.get('portnum')}")

    # --- Hand off to Delayed Packet Processor ---
    # The unique_packet_identifier (original packet.id) is the key for delay processing.
    delayed_processor.add_packet(unique_packet_identifier, meshdash_event, source_type, client, es_client_instance)

def on_mqtt_message(client, userdata, msg):
    """Callback for when a PUBLISH message is received from the server."""
    try:
        es_client_instance = userdata.get("es_client")
        delayed_processor = userdata.get("delayed_processor")

        if not es_client_instance or not delayed_processor:
            logger.error("ES client or Delayed Processor not found in userdata. Cannot process message.")
            return
        process_mqtt_message(client, msg, es_client_instance, delayed_processor)
    except Exception as e:
        logger.exception(f"Critical error in on_message callback: {e}")


def run_bridge(args):
    """Sets up MQTT client, delayed processor, and starts the main loop."""
    es_client = get_elasticsearch_client(args.es_endpoint)
    if not es_client:
        logger.critical("Exiting due to Elasticsearch connection failure.")
        sys.exit(1)

    delayed_packet_processor = DelayedPacketProcessor(
        delay_seconds=args.processing_delay,
        interval_seconds=args.processing_interval
    )
    delayed_packet_processor.start()

    client_userdata = {
        "es_client": es_client,
        "args": args,
        "delayed_processor": delayed_packet_processor
    }

    mqtt_instance_id = f"meshtastic_es_bridge_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    mqtt_client_instance = mqtt_client.Client(client_id=mqtt_instance_id)
    if args.mqtt_username:
        mqtt_client_instance.username_pw_set(username=args.mqtt_username, password=args.mqtt_password)

    mqtt_client_instance.user_data_set(client_userdata)
    mqtt_client_instance.on_connect = on_mqtt_connect
    mqtt_client_instance.on_message = on_mqtt_message

    try:
        logger.info(f"Connecting to MQTT broker: {args.broker}:{args.port} with client ID {mqtt_instance_id}")
        mqtt_client_instance.connect(args.broker, args.port)
        mqtt_client_instance.loop_forever()
    except ConnectionRefusedError:
        logger.error(f"MQTT Connection refused by broker {args.broker}:{args.port}.")
    except OSError as e: # Catches socket.gaierror and other OS network errors
        logger.error(f"Network error connecting to MQTT broker {args.broker}: {e}")
    except KeyboardInterrupt:
        logger.info("User interrupt received.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in the main MQTT loop: {e}")
    finally:
        logger.info("Shutting down...")
        delayed_packet_processor.stop()
        if mqtt_client_instance.is_connected():
            logger.info("Disconnecting MQTT client.")
            mqtt_client_instance.disconnect()
            mqtt_client_instance.loop_stop() # Ensure loop is stopped
        if es_client:
            logger.info("Closing Elasticsearch client connection.")
            es_client.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    time.sleep(60) # wait for ES
    # ARGS is already parsed globally
    logger.info(f"Starting Meshtastic MQTT to Elasticsearch Bridge v{SCRIPT_VERSION}...")
    logger.info(f"Config - MQTT: {ARGS.broker}:{ARGS.port}, Topic: {ARGS.sub_topic}")
    logger.info(f"Config - ES Endpoint: {ARGS.es_endpoint}")
    logger.info(f"Config - Packet Processing: Delay {ARGS.processing_delay}s, Interval {ARGS.processing_interval}s")
    if ARGS.debug:
        logger.info("DEBUG logging enabled.")
    run_bridge(ARGS)