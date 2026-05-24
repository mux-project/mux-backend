from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str
    APP_ENV: str = "development"
    DATA_RETENTION_DAYS: int = 30

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
