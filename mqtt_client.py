import asyncio
import json
import uuid
from aiomqtt import Client as MQTTClient
from config import settings
from db import database

# Define thresholds for alerting
HEAT_THRESHOLD = 70  # °C
PRESSION_THRESHOLD = 5.0  # bar
SPARK_DETECTED = True  # Sparks are a direct alarm trigger
SMOKE_THRESHOLD = 5.0  # ppm

async def process_message(message):
    try:
        payload_str = message.payload.decode("utf-8")
        data = json.loads(payload_str)

        datastream_id = data["datastream_id"]
        result = data["result"]  # e.g., {"value": 21.7, "unit": "°C"}
        sensor_type = data.get("sensor_type")  # Identify which sensor

        if not sensor_type:
            print(f"Warning: Sensor type not provided in {data}")
            return

        value = float(result.get("value", 0))

        # Insert observation into database
        insert_query = """
            INSERT INTO observation (id, datastream_id, phenomenon_time, result, created_at)
            VALUES (:obs_id, :ds_id, NOW(), CAST(:result_text AS jsonb), NOW())
        """
        insert_values = {
            "obs_id": uuid.uuid4(),
            "ds_id": uuid.UUID(datastream_id),
            "result_text": json.dumps(result)
        }
        await database.execute(query=insert_query, values=insert_values)
        print(f"Inserted {sensor_type} observation for datastream {datastream_id}")

        # Fetch corresponding zone based on Feature of Interest
        zone_query = """
            SELECT z.id, z.name, z.properties 
            FROM zone z
            JOIN datastream d ON d.feature_of_interest_id = z.feature_of_interest_id
            WHERE d.id = :ds_id
            LIMIT 1
        """
        zone = await database.fetch_one(query=zone_query, values={"ds_id": uuid.UUID(datastream_id)})

        if zone:
            zone_id = str(zone["id"])
            zone_name = zone["name"]
            properties = zone["properties"] or {}

            # Default: No alert
            alert = None

            # Update zone properties based on sensor type
            if sensor_type == "Heat":
                properties["current_heat"] = value
                if value > HEAT_THRESHOLD:
                    alert = "High Temperature Detected!"
            elif sensor_type == "Pression":
                properties["current_pression"] = value
                if value > PRESSION_THRESHOLD:
                    alert = "Pressure Exceeds Safe Levels!"
            elif sensor_type == "Spark":
                properties["current_spark"] = value
                if value == SPARK_DETECTED:
                    alert = "Spark Detected! Fire Risk!"
            elif sensor_type == "Smoke":
                properties["current_smoke"] = value
                if value > SMOKE_THRESHOLD:
                    alert = "High Smoke Concentration!"

            # Set alert if triggered, otherwise remove old alerts
            if alert:
                properties["alert"] = alert
            else:
                properties.pop("alert", None)

            # Update zone properties in DB
            update_query = """
                UPDATE zone
                SET properties = :properties
                WHERE id = :zone_id
            """
            await database.execute(query=update_query, values={"properties": json.dumps(properties), "zone_id": zone_id})
            print(f"Updated zone '{zone_name}' with {sensor_type} value {value}")

        else:
            print(f"Warning: No zone found for datastream {datastream_id}")

    except Exception as e:
        print(f"Error processing MQTT message: {e}")

async def mqtt_listener():
    async with MQTTClient(settings.MQTT_BROKER, settings.MQTT_PORT) as client:
        await client.subscribe("iot_safeindustech/sensors/#")
        async for message in client.messages:
            await process_message(message)

def start_mqtt_listener():
    loop = asyncio.get_event_loop()
    loop.create_task(mqtt_listener())
import asyncio
import json
import uuid
from aiomqtt import Client as MQTTClient
from config import settings
from db import database

# Thresholds for alerts (example values)
HEAT_THRESHOLD = 70       # °C
PRESSION_THRESHOLD = 5.0  # bar
SMOKE_THRESHOLD = 5.0     # ppm
# For Spark, we assume a True value triggers an alert

async def process_message(message):
    try:
        payload_str = message.payload.decode("utf-8")
        data = json.loads(payload_str)
        datastream_id = data["datastream_id"]
        result = data["result"]  # e.g., {"value": 85.0, "unit": "°C"}
        sensor_type = data.get("sensor_type")  # must be provided: "Heat", "Pression", "Spark", "Smoke"

        if not sensor_type:
            print("Warning: sensor_type missing in payload")
            return

        # Convert the result value as needed (we assume it's numeric or boolean)
        value = result.get("value")
        # For numeric sensors, ensure value is a float
        if sensor_type in ["Heat", "Pression", "Smoke"]:
            try:
                value = float(value)
            except Exception:
                print(f"Error: Value for {sensor_type} is not numeric: {value}")
                return

        # Insert observation record
        insert_query = """
            INSERT INTO observation (id, datastream_id, phenomenon_time, result, created_at)
            VALUES (:obs_id, :ds_id, NOW(), CAST(:result_text AS jsonb), NOW())
        """
        insert_values = {
            "obs_id": uuid.uuid4(),
            "ds_id": uuid.UUID(datastream_id),
            "result_text": json.dumps(result)
        }
        await database.execute(query=insert_query, values=insert_values)
        print(f"Inserted {sensor_type} observation for datastream {datastream_id}")

        # Now, find the zone that corresponds to this sensor message.
        # The join is based on the common FoI; both the datastream and the zone should share the same feature_of_interest_id.
        zone_query = """
            SELECT z.id, z.name, z.properties
            FROM zone z
            JOIN datastream d ON d.feature_of_interest_id = z.feature_of_interest_id
            WHERE d.id = :ds_id
            LIMIT 1
        """
        zone = await database.fetch_one(query=zone_query, values={"ds_id": uuid.UUID(datastream_id)})

        if zone:
            zone_id = str(zone["id"])
            zone_name = zone["name"]
            # Parse current properties; if null, use empty dict
            properties = json.loads(zone["properties"]) if zone["properties"] else {}

            alert = None

            # Update the zone's properties with the latest value for this sensor type.
            if sensor_type == "Heat":
                properties["current_heat"] = value
                if value > HEAT_THRESHOLD:
                    alert = "High Temperature Detected!"
            elif sensor_type == "Pression":
                properties["current_pression"] = value
                if value > PRESSION_THRESHOLD:
                    alert = "Pressure Exceeds Safe Levels!"
            elif sensor_type == "Spark":
                properties["current_spark"] = value
                if value is True:
                    alert = "Spark Detected! Fire Risk!"
            elif sensor_type == "Smoke":
                properties["current_smoke"] = value
                if value > SMOKE_THRESHOLD:
                    alert = "High Smoke Concentration!"

            if alert:
                properties["alert"] = alert
            else:
                properties.pop("alert", None)

            update_query = """
                UPDATE zone
                SET properties = :properties
                WHERE id = :zone_id
            """
            await database.execute(query=update_query, values={"properties": json.dumps(properties), "zone_id": zone_id})
            print(f"Updated zone '{zone_name}' with {sensor_type} value {value}")
        else:
            print(f"Warning: No zone found for datastream {datastream_id}")

    except Exception as e:
        print(f"Error processing MQTT message: {e}")

async def mqtt_listener():
    async with MQTTClient(settings.MQTT_BROKER, settings.MQTT_PORT) as client:
        await client.subscribe("iot_safeindustech/sensors/#")
        async for message in client.messages:
            # Convert the topic to a string (in case it's not)
            topic = str(message.topic)
            # Dispatch based on sensor type if you use separate topics,
            # or let the payload's sensor_type field handle it.
            await process_message(message)

def start_mqtt_listener():
    loop = asyncio.get_event_loop()
    loop.create_task(mqtt_listener())
