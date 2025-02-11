# observed_properties_router.py
from fastapi import APIRouter
from db import database

observed_properties_router = APIRouter(prefix="/ObservedProperties", tags=["ObservedProperties"])

@observed_properties_router.get("/")
async def get_observed_properties():
    query = "SELECT * FROM observed_property"
    return await database.fetch_all(query=query)

@observed_properties_router.get("/{observed_property_id}")
async def get_observed_property(observed_property_id: str):
    query = "SELECT * FROM observed_property WHERE id = :observed_property_id"
    return await database.fetch_one(query=query, values={"observed_property_id": observed_property_id})
