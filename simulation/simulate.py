import time
import json
import random
import requests
import paho.mqtt.client as mqtt

# MQTT configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Datastream IDs for each zone/sensor (Replace with real UUIDs)
ZONE_SENSORS =  {
    "Production": {
        "heat": "42a47f02-137f-43ff-ac6c-3392284ab107",
        "pression": "d8d71e0d-49c1-4bc0-bce9-a4d7e5cd3505",
        "spark": "b4e4c7b1-96b9-46db-b296-78b67ec40c2c",
        "smoke": "8dc9b73e-a49d-4133-af35-c54b07eb6a04"
    },
    "Stock": {
        "heat": "0316cf19-dda3-4a83-b51c-52301530becf",
        "pression": "a7bec186-e59e-401b-8552-aba9378678a6",
        "spark": "25874510-f400-4e08-8d53-cc2e74f2fa56",
        "smoke": "3dbcbd30-fc4d-4f0e-a1d3-5bc071c7ed51"
    },
    "Reception": {
        "heat": "016bf5ae-b890-4dcb-b03b-2cf15d3171d6",
        "pression": "48502376-89f8-41ff-809a-f910c77a885b",
        "spark": "1a860067-6819-4630-997b-06b77d1b0473",
        "smoke": "29d03b99-b7a3-47be-bc6a-9b82caca13bf"
    },
    "Security": {
        "heat": "38f1aa94-a570-413f-a9f2-37c9d0db0a95",
        "pression": "e19d2953-9ee8-4c1f-8214-45a0f03f08fb",
        "spark": "556bfb2f-ae06-44bf-958c-f3f801fa3323",
        "smoke": "a10ded9e-697c-4e86-b33a-9695b7ef931f"
    },
    "Administration": {
        "heat": "5d09051c-0573-4eb5-b995-e3956a313aee",
        "pression": "4692c5d9-e642-47fe-b5c6-5f15f8f073e5",
        "spark": "017b3dab-9f01-42df-96ba-37fb1f88e569",
        "smoke": "eb18cf9b-ebb5-4314-8230-4dfe47246bad"
    },
    "Monitoring": {
        "heat": "a232f422-bf45-44aa-b0c4-0a0a00b67e5f",
        "pression": "60161c4e-06f8-447c-a02f-a908f158841b",
        "spark": "ca18dfdb-7693-4367-b3c8-7d1b97ca46dd",
        "smoke": "cdb255c9-ee04-4d1e-98bd-f2386e3828c9"
    }
}

def fetch_settings():
    try:
        response = requests.get("http://localhost:8001/simulation-settings")
        return response.json()
    except:
        print("Error fetching settings")
        return {}

def simulate_value(sensor, params):
    if sensor == "spark":
        return params["spark"]
    return round(random.uniform(params[sensor] - 5, params[sensor] + 5), 2)

def simulate_sensor_data():
    while True:
        settings = fetch_settings()
        for zone, sensors in settings.items():
            for sensor, value in sensors.items():
                simulated_val = simulate_value(sensor, sensors)
                payload = {"datastream_id": ZONE_SENSORS[zone][sensor], "result": {"value": simulated_val}}
                client.publish(f"iot_safeindustech/sensors/{sensor}", json.dumps(payload))
                print(f"Published to {sensor} for {zone}: {simulated_val}")
        time.sleep(5)

if __name__ == "__main__":
    simulate_sensor_data()
