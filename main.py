# main.py
import asyncio
import json
import uuid
import uvicorn
from fastapi import FastAPI, WebSocket, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from db import database
from mqtt_client import start_mqtt_listener
from config import settings
from fastapi_utils.tasks import repeat_every
from sensors_router import sensors_router
from observed_properties_router import observed_properties_router
from observations_router import observations_router
from alerts_router import alerts_router
from fastapi import HTTPException


app = FastAPI()

# Allow CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to specific domains for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(sensors_router)
app.include_router(observed_properties_router)
app.include_router(observations_router)



# Startup and Shutdown events

@app.get("/usine")
async def get_usine():
    query = "SELECT * FROM usine LIMIT 1"
    usine = await database.fetch_one(query=query)
    if usine:
        return usine
    raise HTTPException(status_code=404, detail="Usine not found")

@app.on_event("startup")
async def startup_event():
    await database.connect()
    start_mqtt_listener()
    print("Database connected and MQTT listener started.")

@app.on_event("shutdown")
async def shutdown_event():
    await database.disconnect()
    print("Database disconnected.")

# REST endpoint: Get the latest observation for a given datastream ID.
@app.get("/observations/{datastream_id}")
async def get_latest_observation(datastream_id: str):
    query = """
        SELECT result, phenomenon_time
        FROM observation
        WHERE datastream_id = :datastream_id
        ORDER BY phenomenon_time DESC
        LIMIT 1
    """
    row = await database.fetch_one(query=query, values={"datastream_id": datastream_id})
    if row:
        return {
            "datastream_id": datastream_id,
            "result": json.loads(row["result"]),
            "timestamp": row["phenomenon_time"].isoformat()
        }
    return {"message": "No observation found."}

# REST endpoint: Get zone status (e.g., alerts) for all zones.

@app.get("/employees/positions")
async def get_employee_positions():
    # This query retrieves the latest observation for each employee's position datastream.
    query = """
        WITH latest_obs AS (
            SELECT d.id AS ds_id, MAX(o.phenomenon_time) AS max_time
            FROM observation o
            JOIN datastream d ON o.datastream_id = d.id
            GROUP BY d.id
        )
        SELECT 
            e.id AS employee_id, 
            e.name, 
            o.result AS position, 
            o.phenomenon_time AS timestamp
        FROM employee e
        -- We assume that the Thing name is exactly 'Employee Tracker - ' concatenated with the employee name.
        JOIN thing t ON t.name = 'Employee Tracker - ' || e.name
        JOIN datastream d ON d.thing_id = t.id
        JOIN latest_obs lo ON lo.ds_id = d.id
        JOIN observation o ON o.datastream_id = d.id AND o.phenomenon_time = lo.max_time
        ORDER BY e.name;
    """
    
    rows = await database.fetch_all(query=query)
    positions = []
    for row in rows:
        positions.append({
            "employee_id": row["employee_id"],
            "name": row["name"],
            "position": json.loads(row["position"]) if row["position"] else None,
            "timestamp": row["timestamp"]
        })
    
    return positions

@app.get("/zones/status")
async def get_zones_status():
    query = """
        SELECT 
            z.name, 
            z.risk_level, 
            z.properties,
            -- Latest Temperature
            (SELECT o.result->>'value'
             FROM observation o
             JOIN datastream d ON o.datastream_id = d.id
             WHERE d.feature_of_interest_id = z.feature_of_interest_id
             AND d.name ILIKE '%Heat%'
             ORDER BY o.phenomenon_time DESC
             LIMIT 1) AS current_temp,
            
            -- Latest Pressure
            (SELECT o.result->>'value'
             FROM observation o
             JOIN datastream d ON o.datastream_id = d.id
             WHERE d.feature_of_interest_id = z.feature_of_interest_id
             AND d.name ILIKE '%Pression%'
             ORDER BY o.phenomenon_time DESC
             LIMIT 1) AS current_pressure,

            -- Latest Smoke
            (SELECT o.result->>'value'
             FROM observation o
             JOIN datastream d ON o.datastream_id = d.id
             WHERE d.feature_of_interest_id = z.feature_of_interest_id
             AND d.name ILIKE '%Smoke%'
             ORDER BY o.phenomenon_time DESC
             LIMIT 1) AS current_smoke,

            -- Latest Spark Detection
            (SELECT (o.result->>'value')::BOOLEAN
             FROM observation o
             JOIN datastream d ON o.datastream_id = d.id
             WHERE d.feature_of_interest_id = z.feature_of_interest_id
             AND d.name ILIKE '%Spark%'
             ORDER BY o.phenomenon_time DESC
             LIMIT 1) AS spark_detected

        FROM zone z
        WHERE z.name IN ('Production', 'Stock', 'Reception', 'Security', 'Administration', 'Monitoring');  -- 🔥 Filter to 6 zones
    """

    rows = await database.fetch_all(query)
    
    zones_status = []
    
    TEMPERATURE_THRESHOLD = 70  # Alert threshold

    for row in rows:
        properties = {
            "current_temp": float(row["current_temp"]) if row["current_temp"] else None,
            "current_pressure": float(row["current_pressure"]) if row["current_pressure"] else None,
            "current_smoke": float(row["current_smoke"]) if row["current_smoke"] else None,
            "spark_detected": row["spark_detected"] if row["spark_detected"] is not None else False,
            "alert": "Temperature exceeds threshold" if row["current_temp"] and float(row["current_temp"]) > TEMPERATURE_THRESHOLD else None
        }
        
        zones_status.append({
            "name": row["name"],
            "risk_level": row["risk_level"],
            "properties": properties
        })
    
    return zones_status
@app.websocket("/ws/zones")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        query = """
            SELECT id, name, properties
            FROM zone
        """
        rows = await database.fetch_all(query=query)
        zones = []
        for row in rows:
            properties = json.loads(row["properties"]) if row["properties"] else {}
            zones.append({
                "id": row["id"],
                "name": row["name"],
                "heat": properties.get("current_heat", "N/A"),
                "pression": properties.get("current_pression", "N/A"),
                "spark": properties.get("current_spark", "N/A"),
                "smoke": properties.get("current_smoke", "N/A"),
                "alert": properties.get("alert", "None")
            })
        await websocket.send_json(zones)
        await asyncio.sleep(1)

# Endpoint: Return zones that are in alarm (i.e. have an alert in properties)
@app.get("/alarms")
async def get_alarms():
    query = """
        SELECT z.name AS zone_name, z.risk_level, z.properties
        FROM zone z
        WHERE z.properties->>'alert' IS NOT NULL
        ORDER BY z.name
    """
    rows = await database.fetch_all(query=query)
    alarms = []
    for row in rows:
        properties = json.loads(row["properties"]) if row["properties"] else {}
        alarms.append({
            "name": row["zone_name"],
            "risk_level": row["risk_level"],
            "properties": properties,
        })
    return alarms


# WebSocket endpoint: Stream the latest observation for a given datastream.

# Background task: Periodically check the latest temperature and trigger alerts if needed.
@app.on_event("startup")
@repeat_every(seconds=10)  # Check every 10 seconds.
async def check_temperature_threshold():
    query = """
        SELECT result, phenomenon_time
        FROM observation
        WHERE datastream_id = :datastream_id
        ORDER BY phenomenon_time DESC
        LIMIT 1
    """
    row = await database.fetch_one(query=query, values={"datastream_id": settings.TEMP_DATASTREAM_ID})
    if row:
        result = json.loads(row["result"])
        temperature_value = result.get("value")
        if temperature_value and float(temperature_value) > 70:
            print(f"Background Task ALERT: Temperature {temperature_value}°C exceeds threshold!")
            await mqtt_client.trigger_temperature_alert(settings.TEMP_DATASTREAM_ID, float(temperature_value))



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
