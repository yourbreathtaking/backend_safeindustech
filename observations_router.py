# observations_router.py
from fastapi import APIRouter, Query
from db import database
import json

observations_router = APIRouter(prefix="/Observations", tags=["Observations"])

@observations_router.get("/")
async def get_observations(datastream_id: str = Query(None), limit: int = 10):
    base_query = "SELECT * FROM observation"
    if datastream_id:
        base_query += " WHERE datastream_id = :datastream_id"
    base_query += " ORDER BY phenomenon_time DESC LIMIT :limit"
    values = {"limit": limit}
    if datastream_id:
        values["datastream_id"] = datastream_id

    rows = await database.fetch_all(query=base_query, values=values)
    observations = []
    for row in rows:
        observations.append({
            "id": row["id"],
            "datastream_id": row["datastream_id"],
            "result": json.loads(row["result"]),
            "phenomenon_time": row["phenomenon_time"].isoformat(),
            "created_at": row["created_at"].isoformat()
        })
    return observations
