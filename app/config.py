from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str
    APP_ENV: str = "development"
    DATA_RETENTION_DAYS: int = 30
    SECRET_KEY: str = ""
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    COLLECTION_INTERVAL_SECONDS: int = 5
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    SLACK_WEBHOOK_URL: str = ""
    DEBUG: bool = False
    DOCS_ENABLED: bool = True

    REDIS_URL: str = "redis://localhost:6379/0"

    ALERT_ENGINE_STREAM_NAME: str = "metrics:ingested"
    ALERT_ENGINE_DLQ_NAME: str = "metrics:dead-letter"
    ALERT_ENGINE_CONSUMER_GROUP: str = "alert-engine"
    ALERT_ENGINE_CONSUMER_NAME: str = "worker-1"
    ALERT_ENGINE_MAXLEN: int = 100_000
    ALERT_ENGINE_MAX_RETRIES: int = 3
    ALERT_ENGINE_IDEMPOTENCY_TTL: int = 86_400
    ALERT_ENGINE_RENOTIFY_INTERVAL: int = 60
    ALERT_ENGINE_CACHE_REFRESH: int = 30
    ALERT_ENGINE_MAX_EVAL_AGE: int = 900
    ALERT_ENGINE_LOCK_TTL: int = 10

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
