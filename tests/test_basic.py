from src.redmine.config import loadConfig


def testLoadConfigReturnsObject() -> None:
    config = loadConfig()
    assert config is not None
