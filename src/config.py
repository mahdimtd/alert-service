from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env',extra='ignore')
    DATABASE_URL : str

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS : str
    KAFKA_OFFSET: str
    KAFKA_CONSUMER_GROUP : str
    LIFECYCLE_KAFKA_CONSUMER_GROUP : str
    METER_READINGS_TOPIC : str
    CONDITION_EVENTS_TOPIC : str
    OUTBOX_TOPIC : str
    OUTBOX_POLL_INTERVAL_SECONDS : int = 5
    DETECTION_OUTBOX_POLL_INTERVAL_SECONDS : int = 5
    DETECTION_OUTBOX_MAX_ATTEMPTS : int = 10
    DETECTION_OUTBOX_BASE_BACKOFF_SECONDS : int = 5

    # Batching / polling tuning (override from .env anytime)
    CONSUMER_BATCH_SIZE : int = 500
    LIFECYCLE_BATCH_SIZE : int = 500
    RELAY_BATCH_SIZE : int = 500
    DETECTION_RELAY_BATCH_SIZE : int = 500
    KAFKA_POLL_TIMEOUT_MS : int = 1000
    HEARTBEAT_INTERVAL_SECONDS : int = 60
    SNOOZE_CHECK_INTERVAL_SECONDS : int = 600

    #endpoints
    DEFAULT_SNOOZE_MINUTES: int = 60

settings = Settings()