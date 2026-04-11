from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.redmine.config import loadConfig


config = loadConfig()


def normalizeDatabaseUrl(databaseUrl: str) -> str:
    if databaseUrl.startswith("postgresql://") and "+psycopg" not in databaseUrl:
        return databaseUrl.replace("postgresql://", "postgresql+psycopg://", 1)
    return databaseUrl


normalizedDatabaseUrl = normalizeDatabaseUrl(config.databaseUrl)
engine = create_engine(normalizedDatabaseUrl, pool_pre_ping=True) if normalizedDatabaseUrl else None
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None

def checkDatabaseConnection() -> bool:
    if engine is None:
        return False

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))

    return True
