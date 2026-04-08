from dataclasses import dataclass
import os


@dataclass
class Config:
    appEnv: str = os.getenv("APP_ENV", "development")
    appHost: str = os.getenv("APP_HOST", "0.0.0.0")
    appPort: int = int(os.getenv("APP_PORT", "8000"))
    databaseUrl: str = os.getenv("DATABASE_URL", "")
    redmineUrl: str = os.getenv("REDMINE_URL", "")
    apiKey: str = os.getenv("REDMINE_API_KEY", "")


def loadConfig() -> Config:
    return Config()
