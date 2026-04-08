from dataclasses import dataclass
import os


@dataclass
class Config:
    redmineUrl: str = os.getenv("REDMINE_URL", "")
    apiKey: str = os.getenv("REDMINE_API_KEY", "")


def loadConfig() -> Config:
    return Config()
