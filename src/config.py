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

    # Batching / polling tuning (override from .env anytime)
    CONSUMER_BATCH_SIZE : int = 500            # readings per detection batch
    LIFECYCLE_BATCH_SIZE : int = 500           # condition events per lifecycle batch
    RELAY_BATCH_SIZE : int = 500               # alert outbox rows per relay pass
    DETECTION_RELAY_BATCH_SIZE : int = 500     # detection outbox rows per relay pass
    KAFKA_POLL_TIMEOUT_MS : int = 1000         # getmany wait when topic is empty
    HEARTBEAT_INTERVAL_SECONDS : int = 60      # consumers' periodic liveness log

settings = Settings()