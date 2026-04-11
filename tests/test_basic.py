from fastapi.responses import HTMLResponse

from src.redmine.app import getTime, readRoot
from src.redmine.config import loadConfig


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
