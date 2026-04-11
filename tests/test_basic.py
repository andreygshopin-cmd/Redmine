from fastapi.responses import HTMLResponse

from src.redmine.app import getTime, readRoot
from src.redmine.config import loadConfig
from src.redmine.db import normalizeDatabaseUrl

def testLoadConfigReturnsObject() -> None:
    config = loadConfig()
    assert config is not None
    assert config.appHost != ""


def testReadRootReturnsHtmlPage() -> None:
    response = readRoot()

    assert isinstance(response, HTMLResponse)
    assert "Server time" in response.body.decode("utf-8")


def testGetTimeReturnsServerTimePayload() -> None:
    payload = getTime()

    assert "current_time" in payload
    assert "current_time_utc" in payload


def testNormalizeDatabaseUrlUsesPsycopgDriver() -> None:
    assert (
        normalizeDatabaseUrl("postgresql://user:pass@host/db")
        == "postgresql+psycopg://user:pass@host/db"
    )
    assert (
        normalizeDatabaseUrl("postgresql+psycopg://user:pass@host/db")
        == "postgresql+psycopg://user:pass@host/db"
    )

