from fastapi import FastAPI

from src.redmine.config import loadConfig
from src.redmine.db import checkDatabaseConnection


config = loadConfig()
app = FastAPI(title="Redmine API", version="0.1.0")


@app.get("/")
def readRoot() -> dict[str, str]:
    return {
        "message": "Redmine API is running",
        "environment": config.appEnv,
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/db-health")
def dbHealth() -> dict[str, str]:
    if not config.databaseUrl:
        return {"status": "error", "details": "DATABASE_URL is not set"}

    try:
        checkDatabaseConnection()
        return {"status": "ok"}
    except Exception as error:
        return {"status": "error", "details": str(error)}
