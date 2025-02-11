# config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://postgres:amine@localhost:5433/iot_safeindustech"
    MQTT_BROKER: str = "localhost"
    MQTT_PORT: int = 1883
    TEMP_DATASTREAM_ID: str = "11111111-1111-1111-1111-111111111111"
    POS_DATASTREAM_ID: str = "22222222-2222-2222-2222-222222222222"

    class Config:
        env_file = ".env"  # Optional: load from a .env file if needed

settings = Settings()
