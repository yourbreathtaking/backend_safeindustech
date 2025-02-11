# sensors_router.py
from fastapi import APIRouter
from db import database

sensors_router = APIRouter(prefix="/Sensors", tags=["Sensors"])

@sensors_router.get("/")
async def get_sensors():
    query = "SELECT * FROM sensor"
    return await database.fetch_all(query=query)

@sensors_router.get("/{sensor_id}")
async def get_sensor(sensor_id: str):
    query = "SELECT * FROM sensor WHERE id = :sensor_id"
    return await database.fetch_one(query=query, values={"sensor_id": sensor_id})
