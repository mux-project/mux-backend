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

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
