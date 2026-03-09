import json
import re
import math
import paho.mqtt.client as mqtt
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MQTT broker details
BROKER = "broker.mqtt.cool"
PORT = 1883
SUBSCRIBE_TOPIC = "SSE/Dev1"

def clean_nan(payload: str) -> str:
    """
    Replace JavaScript‑style 'nan' tokens with 'null' so the string becomes valid JSON.
    Matches 'nan' preceded by '[', ',', or whitespace and followed by ',', ']', or whitespace.
    """
    return re.sub(r'(?<=[\[,\s])nan(?=[,\s\]])', 'null', payload)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(SUBSCRIBE_TOPIC)
        logging.info(f"Subscribed to {SUBSCRIBE_TOPIC}")
    else:
        logging.error(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        # Decode and clean the payload
        raw_payload = msg.payload.decode('utf-8')
        logging.debug(f"Received raw: {raw_payload}")

        cleaned = clean_nan(raw_payload)
        data = json.loads(cleaned)

        device_id = data.get("deviceId")
        em_values = data.get("EM", [])

        if not device_id:
            logging.warning("Missing deviceId in payload")
            return

        if len(em_values) < 8:
            logging.warning(f"EM array has only {len(em_values)} elements, expected 8")
            return

        # Map the eight values to the dashboard keys
        new_payload = {
            "AV": em_values[0],
            "AA": em_values[1],
            "PF": None if isinstance(em_values[2], float) and math.isnan(em_values[2]) else em_values[2],
            "HZ": em_values[3],
            "KW": em_values[4],
            "KVA": em_values[5],
            "KWH": em_values[6],
            "KVAH": em_values[7]
        }

        # Publish to the dashboard topic
        new_topic = f"SSE-D/{device_id}"
        new_payload_json = json.dumps(new_payload)
        client.publish(new_topic, new_payload_json)
        logging.info(f"Published to {new_topic}: {new_payload_json}")

    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}\nPayload was: {raw_payload}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER, PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down")
        client.disconnect()
    except Exception as e:
        logging.error(f"Connection error: {e}")

if __name__ == "__main__":
    main()
