from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

# Enable CORS (for frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Default simulation settings
simulation_settings = {
    "Production": {"heat": 85, "pression": 1.5, "smoke": 0.3, "spark": False},
    "Stock": {"heat": 80, "pression": 1.4, "smoke": 0.25, "spark": False},
    "Reception": {"heat": 75, "pression": 1.2, "smoke": 0.2, "spark": False},
    "Security": {"heat": 60, "pression": 1.0, "smoke": 0.1, "spark": False},
    "Administration": {"heat": 65, "pression": 1.1, "smoke": 0.15, "spark": False},
    "Monitoring": {"heat": 70, "pression": 1.3, "smoke": 0.2, "spark": False},
}

# API Model for updating values
class UpdateSettings(BaseModel):
    zone: str
    heat: float
    pression: float
    smoke: float
    spark: bool

@app.get("/simulation-settings")
async def get_settings():
    return simulation_settings

@app.post("/update-settings")
async def update_settings(settings: UpdateSettings):
    if settings.zone not in simulation_settings:
        raise HTTPException(status_code=400, detail="Invalid zone name")

    simulation_settings[settings.zone] = {
        "heat": settings.heat,
        "pression": settings.pression,
        "smoke": settings.smoke,
        "spark": settings.spark,
    }
    return {"message": "Settings updated successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
