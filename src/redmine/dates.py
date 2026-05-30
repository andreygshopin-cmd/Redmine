from datetime import UTC, date, datetime, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.redmine.config import loadConfig


DEFAULT_SNAPSHOT_TIMEZONE = "Europe/Samara"


def getSnapshotTimezone() -> tzinfo:
    timezoneName = str(loadConfig().snapshotTimezone or DEFAULT_SNAPSHOT_TIMEZONE).strip()
    if not timezoneName:
        timezoneName = DEFAULT_SNAPSHOT_TIMEZONE
    try:
        return ZoneInfo(timezoneName)
    except ZoneInfoNotFoundError:
        return UTC


def getSnapshotBusinessDate(now: datetime | None = None) -> date:
    currentTime = now or datetime.now(UTC)
    if currentTime.tzinfo is None:
        currentTime = currentTime.replace(tzinfo=UTC)
    return currentTime.astimezone(getSnapshotTimezone()).date()


def getSnapshotBusinessDateIso(now: datetime | None = None) -> str:
    return getSnapshotBusinessDate(now).isoformat()
