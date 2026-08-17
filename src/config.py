from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env',extra='ignore')
    DATABASE_URL : str

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS : str
    KAFKA_CONSUMER_GROUP : str   # meter-reading consumer (Detection Engine)
    LIFECYCLE_KAFKA_CONSUMER_GROUP : str   # condition-events consumer (Lifecycle Manager)
    METER_READINGS_TOPIC : str   # input: raw meter readings, set once the metering pipeline exists
    CONDITION_EVENTS_TOPIC : str   # Detection Engine -> Lifecycle Manager handoff (ConditionDetected/ConditionCleared)
    OUTBOX_TOPIC : str   # output: shared topic for every alert-domain outbox event
    OUTBOX_POLL_INTERVAL_SECONDS : int = 5
    DETECTION_OUTBOX_POLL_INTERVAL_SECONDS : int = 5
    DETECTION_OUTBOX_MAX_ATTEMPTS : int = 10
    DETECTION_OUTBOX_BASE_BACKOFF_SECONDS : int = 5

settings = Settings()