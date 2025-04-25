Markdown

# Meshtastic Packet Schema (Version 1)

This document describes the schema for Meshtastic packets parsed by the `parse_type_two` Python function. This structure represents a flattened dictionary containing various fields related to a Meshtastic communication.

## Schema

```json
{
  "from_id_num": INTEGER,
  "to_id_num": INTEGER,
  "portnum": STRING,
  "payload_raw": NULL,
  "telemetry_time": INTEGER,
  "battery_level": INTEGER,
  "voltage": FLOAT,
  "channel_utilization": INTEGER,
  "air_util_tx": FLOAT,
  "uptime_seconds": INTEGER,
  "packet_id": INTEGER,
  "rx_time": INTEGER,
  "hop_limit": NULL,
  "priority": NULL,
  "from_id_str": STRING,
  "to_id_str": NULL,
  "channel": INTEGER,
  "hop_start": INTEGER,
  "hops_away": INTEGER,
  "rssi": INTEGER,
  "sender_id_str": STRING,
  "snr": INTEGER,
  "timestamp": INTEGER,
  "type": STRING,
  "pdop": FLOAT,
  "altitude": INTEGER,
  "latitude": FLOAT,
  "longitude": FLOAT,
  "precision_bits": INTEGER,
  "sats_in_view": INTEGER,
  "ground_speed": FLOAT,
  "ground_track": FLOAT,
  "hardware": STRING,
  "longname": STRING,
  "role": STRING,
  "shortname": STRING,
  "text": STRING
}
```

## **Field Descriptions**

- **from_id_num**: (Integer) The numerical ID of the sending node.
- **to_id_num**: (Integer) The numerical ID of the intended recipient node (often the broadcast address 4294967295).
- **portnum**: (String) The application port number or identifier indicating the type of data contained in the packet (e.g., "TELEMETRY"). This is derived from the 'type' field in the raw packet.
- **payload_raw**: (Null) The raw, unprocessed payload of the packet. This format doesn't directly expose the raw payload as a separate field.
- **telemetry_time**: (Integer) The timestamp associated with the telemetry data reported in the packet (Unix epoch time).
- **battery_level**: (Integer) The battery level of the sending node, typically as a percentage.
- **voltage**: (Float) The battery voltage of the sending node.
- **channel_utilization**: (Integer) A measure of how busy the communication channel is, often represented as a percentage or an arbitrary scale.
- **air_util_tx**: (Float) Airtime utilization for transmission by the sending node, often a fractional value.
- **uptime_seconds**: (Integer) The amount of time the sending node has been running since its last boot-up, in seconds.
- **packet_id**: (Integer) A unique identifier for this specific packet.
- **rx_time**: (Integer) The timestamp when this packet was received by the local node (Unix epoch time). This is derived from the 'timestamp' field in the raw packet.
- **hop_limit**: (Null) The maximum number of hops this packet was allowed to travel. Not present in this packet type.
- **priority**: (Null) The priority level assigned to this packet. Not present in this packet type.
- **from_id_str**: (String) A human-readable identifier (e.g., !7c5ccbd0) of the sending node, often derived from its hardware address. This is taken from the 'sender' field in the raw packet.
- **to_id_str**: (Null) A human-readable identifier of the recipient node. Not directly available in this packet type.
- **channel**: (Integer) The communication channel number used for this packet.
- **hop_start**: (Integer) The hop count at which this packet originated.
- **hops_away**: (Integer) The number of hops this packet has traveled to reach the current node.
- **rssi**: (Integer) The Received Signal Strength Indication, a measure of the signal power of the received packet (typically in dBm, negative values indicate weaker signals).
- **sender_id_str**: (String) Same as from_id_str, the human-readable identifier of the sending node.
- **snr**: (Integer) The Signal-to-Noise Ratio, a measure of the signal strength relative to the background noise (typically in dB).
- **timestamp**: (Integer) A general timestamp associated with the packet, often representing the time of origin (Unix epoch time).
- **type**: (String) The type of data contained in the payload (e.g., "telemetry"). This corresponds to the portnum.
- **pdop**: (Float) Position Dilution of Precision, a measure of the accuracy of the GPS position fix. Lower values indicate better accuracy.
- **altitude**: (Integer) The altitude of the sending node, typically in meters.
- **latitude**: (Float) The latitude of the sending node, in degrees (often as an integer multiplied by a scaling factor in the raw data).
- **longitude**: (Float) The longitude of the sending node, in degrees (often as an integer multiplied by a scaling factor in the raw data).
- **precision_bits**: (Integer) The number of bits of precision in the reported GPS coordinates.
- **sats_in_view**: (Integer) The number of GPS satellites currently visible to the sending node.
- **ground_speed**: (Float) The speed of the sending node over the ground, typically in meters per second or kilometers per hour.
- **ground_track**: (Float) The direction of travel of the sending node, typically in degrees from North (0-359).
- **hardware**: (String) An identifier for the hardware of the sending node.
- **longname**: (String) A longer, more descriptive name of the sending node.
- **role**: (String) The role or type of the sending node in the Meshtastic network.
- **shortname**: (String) A shorter, abbreviated name of the sending node.
- **text**: (String) A text message contained in the payload.

This schema provides a consistent view of the data contained within this type of Meshtastic packet, making it easier to process and analyze.
