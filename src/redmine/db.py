from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.redmine.config import loadConfig


config = loadConfig()
engine = create_engine(config.databaseUrl, pool_pre_ping=True) if config.databaseUrl else None
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None


def checkDatabaseConnection() -> bool:
    if engine is None:
        return False

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))

    return True
