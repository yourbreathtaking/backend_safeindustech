# simulate_employee_position.py
import time
import json
import random
import paho.mqtt.client as mqtt
from config import settings

MQTT_BROKER = settings.MQTT_BROKER
MQTT_PORT = settings.MQTT_PORT

# Replace these placeholder values with actual datastream UUIDs from your database.
EMPLOYEE_POSITION_DATASTREAMS = {
    "Amine": "ec4d9377-94ae-4afa-a342-5c333b1b6fe3",  # example valid UUID for Amine
    "Yassin": "4c298b15-8bf6-4401-b9c0-59cb6da248fc",  # example valid UUID for Yassin
    "Mouhsin": "4e163b27-e39d-4ce5-816e-c1e0b587f208"   # example valid UUID for Mouhsin
}

client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

def simulate_employee_position(employee_name):
    ds_id = EMPLOYEE_POSITION_DATASTREAMS.get(employee_name)
    if not ds_id:
        return None
    # Base positions that match your SQL setup.
    base_positions = {
        "Amine": {"lat": 34.005, "lng": -6.847},
        "Yassin": {"lat": 34.006, "lng": -6.846},
        "Mouhsin": {"lat": 34.007, "lng": -6.845}
    }
    base = base_positions.get(employee_name)
    if not base:
        return None
    # Add a random small offset.
    offset = lambda: round(random.uniform(-0.0001, 0.0001), 6)
    simulated_position = {
        "lat": base["lat"] + offset(),
        "lng": base["lng"] + offset()
    }
    data = {
        "datastream_id": ds_id,
        "result": simulated_position
    }
    return json.dumps(data)

def publish_employee_positions():
    while True:
        for employee in EMPLOYEE_POSITION_DATASTREAMS.keys():
            payload = simulate_employee_position(employee)
            if payload:
                topic = "iot_safeindustech/sensors/position"
                client.publish(topic, payload)
                print(f"Published position for {employee}: {payload}")
        time.sleep(10)

if __name__ == "__main__":
    try:
        publish_employee_positions()
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
        client.disconnect()
