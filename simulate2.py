import time
import json
import random
import paho.mqtt.client as mqtt
from config import settings

# MQTT Configuration
MQTT_BROKER = settings.MQTT_BROKER
MQTT_PORT = settings.MQTT_PORT

# Define zones and their corresponding datastream UUIDs
ZONE_SENSORS = {
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

# Create MQTT client and connect
client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Helper function to send data
def send_mqtt_message(topic, payload):
    client.publish(topic, json.dumps(payload))
    print(f"Published to {topic}: {json.dumps(payload)}")

# Function to simulate sensor data
def simulate_sensor_data():
    while True:
        for zone, sensors in ZONE_SENSORS.items():
            # Generate random values for each sensor
            heat_value = round(random.uniform(50, 90), 1)  # °C
            pression_value = round(random.uniform(0, 2), 1)  # bar
            spark_value = random.choice([True, False])  # Binary
            smoke_value = round(random.uniform(0, 10) if spark_value else random.uniform(0, 2), 1)  # ppm

            # Send MQTT messages for each sensor
            send_mqtt_message("iot_safeindustech/sensors/heat", {
                "datastream_id": sensors["heat"],
                "sensor_type": "Heat",
                "result": {"value": heat_value, "unit": "°C"}
            })

            send_mqtt_message("iot_safeindustech/sensors/pression", {
                "datastream_id": sensors["pression"],
                "sensor_type": "Pression",
                "result": {"value": pression_value, "unit": "bar"}
            })

            send_mqtt_message("iot_safeindustech/sensors/spark", {
                "datastream_id": sensors["spark"],
                "sensor_type": "Spark",
                "result": {"value": spark_value}
            })

            send_mqtt_message("iot_safeindustech/sensors/smoke", {
                "datastream_id": sensors["smoke"],
                "sensor_type": "Smoke",
                "result": {"value": smoke_value, "unit": "ppm"}
            })

        time.sleep(5)  # Wait 5 seconds before next set of readings

# Start simulation
if __name__ == "__main__":
    try:
        simulate_sensor_data()
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
        client.disconnect()
