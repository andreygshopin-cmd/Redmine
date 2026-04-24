from dataclasses import dataclass
import os


@dataclass
class Config:
    appEnv: str = os.getenv("APP_ENV", "development")
    appHost: str = os.getenv("APP_HOST", "0.0.0.0")
    appPort: int = int(os.getenv("APP_PORT", "8000"))
    sessionSecret: str = os.getenv("SESSION_SECRET", "")
    databaseUrl: str = os.getenv("DATABASE_URL", "")
    redmineUrl: str = os.getenv("REDMINE_URL", "")
    apiKey: str = os.getenv("REDMINE_API_KEY", "")
    appBaseUrl: str = os.getenv("APP_BASE_URL", "")
    smtpHost: str = os.getenv("SMTP_HOST", "")
    smtpPort: int = int(os.getenv("SMTP_PORT", "587"))
    smtpUsername: str = os.getenv("SMTP_USERNAME", "")
    smtpPassword: str = os.getenv("SMTP_PASSWORD", "")
    smtpFromEmail: str = os.getenv("SMTP_FROM_EMAIL", "")
    smtpFromName: str = os.getenv("SMTP_FROM_NAME", "Redmine Snapshot Viewer")
    smtpUseTls: bool = os.getenv("SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "on"}
    smtpUseSsl: bool = os.getenv("SMTP_USE_SSL", "false").strip().lower() in {"1", "true", "yes", "on"}


def loadConfig() -> Config:
    return Config()
