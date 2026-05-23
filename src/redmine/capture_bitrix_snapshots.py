import argparse

from src.redmine.app import (
    BitrixCapturePagePayload,
    captureBitrixSnapshotPage,
    startBitrixSnapshotCapture,
)


def _buildArgumentParser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture Bitrix CRM snapshots")
    parser.add_argument(
        "--entities",
        default=None,
        help="Comma-separated entity keys. Defaults to deal,lead,invoice.",
    )
    return parser


def runBitrixSnapshotCaptureJob(entities: str | None = None) -> dict[str, object]:
    startPayload = startBitrixSnapshotCapture(entities=entities)
    sessionId = str(startPayload["session_id"])
    selectedEntities = [
        entity
        for entity in startPayload.get("entities", [])
        if isinstance(entity, dict) and entity.get("key")
    ]
    snapshots: dict[str, object] = {}

    for entityPayload in selectedEntities:
        entityKey = str(entityPayload["key"])
        nextStart: int | None = 0
        while nextStart is not None:
            result = captureBitrixSnapshotPage(
                BitrixCapturePagePayload(
                    session_id=sessionId,
                    entity=entityKey,
                    start=nextStart,
                )
            )
            print(
                "Bitrix snapshot "
                f"{entityKey}: fetched {result.get('fetched')} of {result.get('total')}, "
                f"remaining {result.get('remaining')}, next {result.get('next')}.",
                flush=True,
            )
            if result.get("done"):
                snapshots[entityKey] = result.get("snapshot")
                break
            nextValue = result.get("next")
            nextStart = int(nextValue) if nextValue is not None else None

    return {
        "captured_for_date": startPayload.get("captured_for_date"),
        "session_id": sessionId,
        "snapshots": snapshots,
    }


def main() -> None:
    arguments = _buildArgumentParser().parse_args()
    result = runBitrixSnapshotCaptureJob(arguments.entities)
    snapshots = result.get("snapshots") if isinstance(result.get("snapshots"), dict) else {}
    print(
        "Captured Bitrix snapshots for "
        f"{result.get('captured_for_date')}: "
        f"{', '.join(sorted(str(key) for key in snapshots.keys()))}.",
        flush=True,
    )


if __name__ == "__main__":
    main()
