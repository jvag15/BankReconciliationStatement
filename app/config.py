from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="allow")

    DATABASE_URL: str = "postgresql://brs_engine_app:__CHANGEME__@brs_postgres:5432/brs_db"
    api_auth_token: str = "dev-token"
    AZURE_OPENAI_API_KEY: str = ""
    AZURE_OPENAI_ENDPOINT: str = ""
    AZURE_OPENAI_MODEL: str = "Kimi-K2.6"
    AZURE_OPENAI_API_VERSION: str = ""

settings = Settings()
