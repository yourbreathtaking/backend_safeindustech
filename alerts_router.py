# alerts_router.py
from fastapi import APIRouter
from db import database
import json

alerts_router = APIRouter(prefix="/Alerts", tags=["Alerts"])

@alerts_router.get("/")
async def get_active_alerts():
    # For example, we check zones for an 'alert' key in properties.
    query = "SELECT name, risk_level, properties FROM zone WHERE properties->>'alert' IS NOT NULL"
    rows = await database.fetch_all(query=query)
    alerts = []
    for row in rows:
        alerts.append({
            "zone": row["name"],
            "risk_level": row["risk_level"],
            "alert_details": json.loads(row["properties"]) if row["properties"] else {}
        })
    return alerts
