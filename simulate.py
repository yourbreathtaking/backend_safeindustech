# simulate.py
import time
import json
import random
import logging
import paho.mqtt.client as mqtt
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# MQTT Broker settings from config
MQTT_BROKER = settings.MQTT_BROKER  # e.g., "localhost"
MQTT_PORT = settings.MQTT_PORT      # e.g., 1883

# Define MQTT topics and datastream IDs for each zone sensor.
# These datastream IDs should correspond to those inserted in your database.
SENSORS = {
    "Production Area": {
        "topic": "iot_safeindustech/sensors/temperature/production",
        "datastream_id": "fe20a22d-df6f-4f8a-9636-c145cfdbed1c",  # Production Area datastream ID
        "base_temp": 70.0,  # Base temperature in °C; often above threshold
        "variation": 5.0    # Small fluctuations, but may spike occasionally
    },
    "Storage Area": {
        "topic": "iot_safeindustech/sensors/temperature/storage",
        "datastream_id": "70a3e6e7-5fa6-4cce-81a3-6edeb3ec5155",  # Replace with the actual Storage Area datastream ID
        "base_temp": 65.0,  # Base temperature in °C
        "variation": 2.0
    },
    "Office Area": {
        "topic": "iot_safeindustech/sensors/temperature/office",
        "datastream_id": "dbce6916-8ca9-4388-bd22-e9238df737da",  # Replace with the actual Office Area datastream ID
        "base_temp": 22.0,  # Base temperature in °C (typical office conditions)
        "variation": 1.0
    }
}


# Create and configure the MQTT client
client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

def simulate_temperature(zone_info):
    """
    Simulate a temperature reading for a given zone.
    
    Parameters:
      zone_info (dict): Dictionary containing:
          - datastream_id: The datastream ID for the zone.
          - base_temp: The base temperature reading.
          - variation: The maximum random variation.
          
    Returns:
      JSON string containing the datastream_id and the simulated temperature result.
    """
    base = zone_info["base_temp"]
    var = zone_info["variation"]

    # Occasionally produce an outlier reading (with 5% chance) to simulate a spike.
    if random.random() < 0.05:
        # Increase the temperature by a random amount between 5 and 10°C
        temperature = round(base + random.uniform(5, 10), 1)
    else:
        # Normal fluctuation: add/subtract a random value within the variation range
        temperature = round(base + random.uniform(-var, var), 1)

    data = {
        "datastream_id": zone_info["datastream_id"],
        "result": {
            "value": temperature,
            "unit": "°C"
        }
    }
    return json.dumps(data)

def publish_sensor_data():
    """
    Loop over each zone and publish its simulated temperature data to the respective MQTT topic.
    """
    try:
        while True:
            for zone_name, zone_info in SENSORS.items():
                temp_payload = simulate_temperature(zone_info)
                topic = zone_info["topic"]
                client.publish(topic, temp_payload)
                logging.info(f"Published to {topic}: {temp_payload}")
            # Wait 5 seconds between publishing cycles
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Simulation stopped by user.")
        client.disconnect()
    except Exception as e:
        logging.error(f"Error during simulation: {e}")

if __name__ == "__main__":
    publish_sensor_data()
