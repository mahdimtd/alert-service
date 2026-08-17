from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env',extra='ignore')
    DATABASE_URL : str

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS : str
    KAFKA_CONSUMER_GROUP : str
    METER_READINGS_TOPIC : str   # input: raw meter readings, set once the metering pipeline exists
    OUTBOX_TOPIC : str   # output: shared topic for every outbox event
    OUTBOX_POLL_INTERVAL_SECONDS : int = 5

settings = Settings()