from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from threading import Lock

from requests import HTTPError

from src.redmine.config import loadConfig
from src.redmine.db import (
    createIssueSnapshotRun,
    ensureIssueSnapshotTables,
    ensureProjectsTable,
    listRecentIssueSnapshotRuns,
    listProjectsWithoutSnapshotForDate,
    listStoredProjects,
    storeMissingProjects,
)
from src.redmine.redmine_client import (
    applySpentHoursYearByIssue,
    fetchAllIssuesForProject,
    fetchAllProjectsFromRedmine,
    fetchSpentHoursByIssueForProjectYear,
)


CAPTURE_STATUS_DIR = Path(tempfile.gettempdir()) / "redmine_snapshot_capture"
CAPTURE_STATUS_PATH = CAPTURE_STATUS_DIR / "status.json"
CAPTURE_LOCK_PATH = CAPTURE_STATUS_DIR / "capture.lock"
CAPTURE_WORKER_MODULE = "src.redmine.capture_snapshots"
CAPTURE_WORKER_CWD = Path(__file__).resolve().parents[2]
CAPTURE_STALE_GRACE_SECONDS = 10


captureStatusLock = Lock()


def _buildDefaultCaptureStatus() -> dict[str, object]:
    return {
        "is_running": False,
        "captured_for_date": None,
        "total_projects": 0,
        "processed_projects": 0,
        "current_project_name": None,
        "last_completed_project_name": None,
        "created_runs": 0,
        "captured_issues": 0,
        "already_captured_projects": 0,
        "remaining_projects": 0,
        "current_project_issues_pages_loaded": 0,
        "current_project_issues_pages_total": 0,
        "current_project_time_pages_loaded": 0,
        "current_project_time_pages_total": 0,
        "error_message": None,
        "worker_pid": None,
        "started_at": None,
        "updated_at": None,
        "mode": None,
        "project_redmine_id": None,
    }


captureStatusState: dict[str, object] = _buildDefaultCaptureStatus()


def _nowIso() -> str:
    return datetime.now(UTC).isoformat()


def _parseIso(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _ensureCaptureStatusDir() -> None:
    CAPTURE_STATUS_DIR.mkdir(parents=True, exist_ok=True)


def _normalizeCaptureStatus(payload: dict[str, object] | None) -> dict[str, object]:
    status = _buildDefaultCaptureStatus()
    if payload:
        status.update(payload)
    return status


def _readCaptureStatusFromDisk() -> dict[str, object]:
    if not CAPTURE_STATUS_PATH.exists():
        return _buildDefaultCaptureStatus()

    try:
        payload = json.loads(CAPTURE_STATUS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _buildDefaultCaptureStatus()

    if not isinstance(payload, dict):
        return _buildDefaultCaptureStatus()

    return _normalizeCaptureStatus(payload)


def _writeCaptureStatusToDisk(status: dict[str, object]) -> None:
    _ensureCaptureStatusDir()
    normalized = _normalizeCaptureStatus(status)
    normalized["updated_at"] = _nowIso()
    temporaryPath = CAPTURE_STATUS_PATH.with_suffix(".tmp")
    temporaryPath.write_text(json.dumps(normalized, ensure_ascii=False), encoding="utf-8")
    temporaryPath.replace(CAPTURE_STATUS_PATH)
    captureStatusState.clear()
    captureStatusState.update(normalized)


def _pidIsAlive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False

    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {int(pid)}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        )
        output = (result.stdout or "").strip()
        if not output:
            return False
        if "No tasks are running" in output:
            return False
        return output.startswith('"')

    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _readCaptureLockMetadata() -> dict[str, object] | None:
    if not CAPTURE_LOCK_PATH.exists():
        return None

    try:
        payload = json.loads(CAPTURE_LOCK_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    return payload


def _writeCaptureLockMetadata(metadata: dict[str, object]) -> None:
    _ensureCaptureStatusDir()
    CAPTURE_LOCK_PATH.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")


def _removeCaptureLock() -> None:
    try:
        CAPTURE_LOCK_PATH.unlink()
    except FileNotFoundError:
        return


def _cleanupStaleCaptureArtifacts() -> None:
    lockMetadata = _readCaptureLockMetadata()
    lockStartedAt = _parseIso(lockMetadata.get("started_at")) if lockMetadata else None
    if (
        lockMetadata
        and not _pidIsAlive(int(lockMetadata.get("pid") or 0))
        and (
            lockStartedAt is None
            or (datetime.now(UTC) - lockStartedAt).total_seconds() >= CAPTURE_STALE_GRACE_SECONDS
        )
    ):
        _removeCaptureLock()

    status = _readCaptureStatusFromDisk()
    statusStartedAt = _parseIso(status.get("started_at"))
    if (
        bool(status.get("is_running"))
        and not _pidIsAlive(int(status.get("worker_pid") or 0))
        and (
            statusStartedAt is None
            or (datetime.now(UTC) - statusStartedAt).total_seconds() >= CAPTURE_STALE_GRACE_SECONDS
        )
    ):
        status.update(
            {
                "is_running": False,
                "current_project_name": None,
                "current_project_issues_pages_loaded": 0,
                "current_project_issues_pages_total": 0,
                "current_project_time_pages_loaded": 0,
                "current_project_time_pages_total": 0,
                "error_message": str(status.get("error_message") or "Фоновая загрузка завершилась раньше времени."),
            }
        )
        _writeCaptureStatusToDisk(status)


def _writeInitialCaptureStatus(
    *,
    mode: str,
    totalProjects: int,
    projectRedmineId: int | None,
) -> None:
    status = _buildDefaultCaptureStatus()
    status.update(
        {
            "is_running": True,
            "total_projects": totalProjects,
            "remaining_projects": totalProjects,
            "worker_pid": os.getpid(),
            "started_at": _nowIso(),
            "mode": mode,
            "project_redmine_id": projectRedmineId,
        }
    )
    _writeCaptureStatusToDisk(status)


def _ensureCaptureLockOwnership(mode: str, projectRedmineId: int | None) -> bool:
    with captureStatusLock:
        _cleanupStaleCaptureArtifacts()
        metadata = _readCaptureLockMetadata()
        currentPid = os.getpid()

        if metadata is not None:
            ownerPid = int(metadata.get("pid") or 0)
            if ownerPid == currentPid:
                return True
            if _pidIsAlive(ownerPid):
                return False

        _writeCaptureLockMetadata(
            {
                "pid": currentPid,
                "mode": mode,
                "project_redmine_id": projectRedmineId,
                "started_at": _nowIso(),
            }
        )
        return True


def _buildCaptureWorkerCommand(mode: str, projectRedmineId: int | None) -> list[str]:
    command = [sys.executable, "-m", CAPTURE_WORKER_MODULE, "--mode", mode]
    if projectRedmineId is not None:
        command.extend(["--project-redmine-id", str(projectRedmineId)])
    command.append("--adopt-lock")
    return command


def _startCaptureWorkerProcess(mode: str, projectRedmineId: int | None, totalProjects: int) -> bool:
    with captureStatusLock:
        _cleanupStaleCaptureArtifacts()
        currentStatus = _readCaptureStatusFromDisk()
        if bool(currentStatus.get("is_running")):
            return False

        try:
            _ensureCaptureStatusDir()
            lockFd = os.open(str(CAPTURE_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            _cleanupStaleCaptureArtifacts()
            if CAPTURE_LOCK_PATH.exists():
                return False
            lockFd = os.open(str(CAPTURE_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)

        with os.fdopen(lockFd, "w", encoding="utf-8") as lockHandle:
            json.dump(
                {
                    "pid": os.getpid(),
                    "mode": mode,
                    "project_redmine_id": projectRedmineId,
                    "started_at": _nowIso(),
                },
                lockHandle,
                ensure_ascii=False,
            )

        _writeInitialCaptureStatus(mode=mode, totalProjects=totalProjects, projectRedmineId=projectRedmineId)

        command = _buildCaptureWorkerCommand(mode, projectRedmineId)
        popenParameters: dict[str, object] = {
            "cwd": str(CAPTURE_WORKER_CWD),
            "close_fds": True,
        }
        if os.name == "nt":
            creationFlags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            creationFlags |= getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0)
            if creationFlags:
                popenParameters["creationflags"] = creationFlags
        else:
            popenParameters["start_new_session"] = True

        try:
            process = subprocess.Popen(command, **popenParameters)
        except Exception as error:
            _removeCaptureLock()
            failedStatus = _buildDefaultCaptureStatus()
            failedStatus["error_message"] = f"Не удалось запустить загрузку срезов: {error}"
            _writeCaptureStatusToDisk(failedStatus)
            raise

        _writeCaptureLockMetadata(
            {
                "pid": process.pid,
                "mode": mode,
                "project_redmine_id": projectRedmineId,
                "started_at": _nowIso(),
            }
        )
        runningStatus = _readCaptureStatusFromDisk()
        runningStatus.update({"worker_pid": process.pid, "mode": mode, "project_redmine_id": projectRedmineId})
        _writeCaptureStatusToDisk(runningStatus)
        return True


def _lowerWorkerPriority() -> None:
    if os.name != "posix":
        return
    try:
        os.nice(10)
    except OSError:
        return


def updateIssueSnapshotCaptureStatus(**values: object) -> None:
    with captureStatusLock:
        status = _readCaptureStatusFromDisk()
        status.update(values)
        if status.get("is_running") and not status.get("started_at"):
            status["started_at"] = _nowIso()
        _writeCaptureStatusToDisk(status)


def resetIssueSnapshotCaptureStatus() -> None:
    with captureStatusLock:
        _writeCaptureStatusToDisk(_buildDefaultCaptureStatus())


def getIssueSnapshotCaptureStatus() -> dict[str, object]:
    with captureStatusLock:
        _cleanupStaleCaptureArtifacts()
        return _readCaptureStatusFromDisk()


def isIssueSnapshotCaptureRunning() -> bool:
    return bool(getIssueSnapshotCaptureStatus()["is_running"])


def startIssueSnapshotCaptureInBackground() -> bool:
    return _startCaptureWorkerProcess(mode="all", projectRedmineId=None, totalProjects=0)


def startProjectIssueSnapshotCaptureInBackground(projectRedmineId: int) -> bool:
    return _startCaptureWorkerProcess(mode="project", projectRedmineId=projectRedmineId, totalProjects=1)


def runIssueSnapshotCaptureJob(
    mode: str = "all",
    projectRedmineId: int | None = None,
    adoptExistingLock: bool = False,
) -> dict[str, object]:
    if mode not in {"all", "project"}:
        raise RuntimeError(f"Unsupported capture mode: {mode}")

    if adoptExistingLock:
        _writeCaptureLockMetadata(
            {
                "pid": os.getpid(),
                "mode": mode,
                "project_redmine_id": projectRedmineId,
                "started_at": _nowIso(),
            }
        )
    elif not _ensureCaptureLockOwnership(mode, projectRedmineId):
        raise RuntimeError("Другая загрузка срезов уже выполняется.")

    _lowerWorkerPriority()
    runningStatus = _readCaptureStatusFromDisk()
    runningStatus.update(
        {
            "is_running": True,
            "worker_pid": os.getpid(),
            "mode": mode,
            "project_redmine_id": projectRedmineId,
        }
    )
    if not runningStatus.get("started_at"):
        runningStatus["started_at"] = _nowIso()
    _writeCaptureStatusToDisk(runningStatus)

    try:
        if mode == "project":
            if projectRedmineId is None:
                raise RuntimeError("Project Redmine ID is required for project capture mode")
            return captureIssueSnapshotForProject(projectRedmineId)
        return captureAllIssueSnapshots()
    except Exception as error:
        updateIssueSnapshotCaptureStatus(
            is_running=False,
            current_project_name=None,
            current_project_issues_pages_loaded=0,
            current_project_issues_pages_total=0,
            current_project_time_pages_loaded=0,
            current_project_time_pages_total=0,
            error_message=str(error),
        )
        raise
    finally:
        _removeCaptureLock()


def captureAllIssueSnapshots() -> dict[str, object]:
    config = loadConfig()
    if not config.databaseUrl:
        raise RuntimeError("DATABASE_URL is not set")
    if not config.redmineUrl:
        raise RuntimeError("REDMINE_URL is not set")
    if not config.apiKey:
        raise RuntimeError("REDMINE_API_KEY is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()

    redmineProjects = fetchAllProjectsFromRedmine(config.redmineUrl, config.apiKey)
    storeMissingProjects(redmineProjects)
    projects = listStoredProjects()
    if not projects:
        raise RuntimeError("No projects in the database. Refresh projects first.")

    capturedForDate = datetime.now(UTC).date().isoformat()
    captureYear = int(capturedForDate[:4])
    closedOnCutoff = f"{datetime.now(UTC).year - 1}-01-01"
    activeProjects = [project for project in projects if bool(project.get("is_enabled"))]
    pendingProjects = listProjectsWithoutSnapshotForDate(capturedForDate)
    createdRuns = 0
    capturedIssues = 0
    skippedProjects = []
    alreadyCapturedProjects = len(activeProjects) - len(pendingProjects)

    updateIssueSnapshotCaptureStatus(
        is_running=True,
        captured_for_date=capturedForDate,
        total_projects=len(pendingProjects),
        processed_projects=0,
        current_project_name=None,
        last_completed_project_name=None,
        created_runs=0,
        captured_issues=0,
        already_captured_projects=alreadyCapturedProjects,
        remaining_projects=len(pendingProjects),
        current_project_issues_pages_loaded=0,
        current_project_issues_pages_total=0,
        current_project_time_pages_loaded=0,
        current_project_time_pages_total=0,
        error_message=None,
    )

    try:
        for project in pendingProjects:
            projectName = str(project.get("name") or "")
            updateIssueSnapshotCaptureStatus(
                current_project_name=projectName,
                current_project_issues_pages_loaded=0,
                current_project_issues_pages_total=0,
                current_project_time_pages_loaded=0,
                current_project_time_pages_total=0,
            )

            identifier = project.get("identifier")
            if not identifier:
                skippedProjects.append(
                    {
                        "project_redmine_id": project["redmine_id"],
                        "project_name": project["name"],
                        "reason": "Project identifier is missing",
                    }
                )
                updateIssueSnapshotCaptureStatus(
                    processed_projects=createdRuns + len(skippedProjects),
                    last_completed_project_name=projectName,
                    current_project_name=None,
                )
                continue

            try:
                def updateIssuesProgress(loadedPages: int, totalPages: int, loadedItems: int, totalItems: int) -> None:
                    updateIssueSnapshotCaptureStatus(
                        current_project_name=projectName,
                        current_project_issues_pages_loaded=loadedPages,
                        current_project_issues_pages_total=totalPages,
                    )

                def updateTimeProgress(loadedPages: int, totalPages: int, loadedItems: int, totalItems: int) -> None:
                    updateIssueSnapshotCaptureStatus(
                        current_project_name=projectName,
                        current_project_time_pages_loaded=loadedPages,
                        current_project_time_pages_total=totalPages,
                    )

                issues = fetchAllIssuesForProject(
                    config.redmineUrl,
                    config.apiKey,
                    str(identifier),
                    int(project["redmine_id"]),
                    progressCallback=updateIssuesProgress,
                    partialLoad=bool(project.get("partial_load")),
                    closedOnOrAfter=closedOnCutoff,
                )
                spentHoursByIssue = fetchSpentHoursByIssueForProjectYear(
                    config.redmineUrl,
                    config.apiKey,
                    str(identifier),
                    captureYear,
                    progressCallback=updateTimeProgress,
                )
            except HTTPError as error:
                skippedProjects.append(
                    {
                        "project_redmine_id": project["redmine_id"],
                        "project_name": project["name"],
                        "reason": str(error),
                    }
                )
                updateIssueSnapshotCaptureStatus(
                    processed_projects=createdRuns + len(skippedProjects),
                    last_completed_project_name=projectName,
                    current_project_name=None,
                    current_project_issues_pages_loaded=0,
                    current_project_issues_pages_total=0,
                    current_project_time_pages_loaded=0,
                    current_project_time_pages_total=0,
                )
                continue

            applySpentHoursYearByIssue(issues, spentHoursByIssue)
            snapshotRunId = createIssueSnapshotRun(capturedForDate, project, issues)
            if snapshotRunId is None:
                updateIssueSnapshotCaptureStatus(
                    processed_projects=createdRuns + len(skippedProjects),
                    last_completed_project_name=projectName,
                    current_project_name=None,
                    current_project_issues_pages_loaded=0,
                    current_project_issues_pages_total=0,
                    current_project_time_pages_loaded=0,
                    current_project_time_pages_total=0,
                )
                continue

            createdRuns += 1
            capturedIssues += len(issues)
            updateIssueSnapshotCaptureStatus(
                processed_projects=createdRuns + len(skippedProjects),
                last_completed_project_name=projectName,
                current_project_name=None,
                created_runs=createdRuns,
                captured_issues=capturedIssues,
                current_project_issues_pages_loaded=0,
                current_project_issues_pages_total=0,
                current_project_time_pages_loaded=0,
                current_project_time_pages_total=0,
            )
    finally:
        updateIssueSnapshotCaptureStatus(
            is_running=False,
            current_project_name=None,
            created_runs=createdRuns,
            captured_issues=capturedIssues,
            already_captured_projects=alreadyCapturedProjects,
            remaining_projects=len(listProjectsWithoutSnapshotForDate(capturedForDate)),
            current_project_issues_pages_loaded=0,
            current_project_issues_pages_total=0,
            current_project_time_pages_loaded=0,
            current_project_time_pages_total=0,
        )

    return {
        "captured_for_date": capturedForDate,
        "created_runs": createdRuns,
        "captured_issues": capturedIssues,
        "already_captured_projects": alreadyCapturedProjects,
        "remaining_projects": len(listProjectsWithoutSnapshotForDate(capturedForDate)),
        "skipped_projects": skippedProjects,
        "snapshot_runs": listRecentIssueSnapshotRuns(),
    }


def captureIssueSnapshotForProject(projectRedmineId: int) -> dict[str, object]:
    config = loadConfig()
    if not config.databaseUrl:
        raise RuntimeError("DATABASE_URL is not set")
    if not config.redmineUrl:
        raise RuntimeError("REDMINE_URL is not set")
    if not config.apiKey:
        raise RuntimeError("REDMINE_API_KEY is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()

    project = next(
        (item for item in listStoredProjects() if int(item.get("redmine_id") or 0) == int(projectRedmineId)),
        None,
    )
    if project is None:
        raise RuntimeError(f"Project {projectRedmineId} not found")

    capturedForDate = datetime.now(UTC).date().isoformat()
    captureYear = int(capturedForDate[:4])
    closedOnCutoff = f"{datetime.now(UTC).year - 1}-01-01"
    projectName = str(project.get("name") or "")

    updateIssueSnapshotCaptureStatus(
        is_running=True,
        captured_for_date=capturedForDate,
        total_projects=1,
        processed_projects=0,
        current_project_name=projectName,
        last_completed_project_name=None,
        created_runs=0,
        captured_issues=0,
        already_captured_projects=0,
        remaining_projects=1,
        current_project_issues_pages_loaded=0,
        current_project_issues_pages_total=0,
        current_project_time_pages_loaded=0,
        current_project_time_pages_total=0,
        error_message=None,
    )

    snapshotRunId: int | None = None
    issues: list[dict[str, object]] = []
    try:
        identifier = project.get("identifier")
        if not identifier:
            raise RuntimeError("Project identifier is missing")

        def updateIssuesProgress(loadedPages: int, totalPages: int, loadedItems: int, totalItems: int) -> None:
            updateIssueSnapshotCaptureStatus(
                current_project_name=projectName,
                current_project_issues_pages_loaded=loadedPages,
                current_project_issues_pages_total=totalPages,
            )

        def updateTimeProgress(loadedPages: int, totalPages: int, loadedItems: int, totalItems: int) -> None:
            updateIssueSnapshotCaptureStatus(
                current_project_name=projectName,
                current_project_time_pages_loaded=loadedPages,
                current_project_time_pages_total=totalPages,
            )

        issues = fetchAllIssuesForProject(
            config.redmineUrl,
            config.apiKey,
            str(identifier),
            int(project["redmine_id"]),
            progressCallback=updateIssuesProgress,
            partialLoad=bool(project.get("partial_load")),
            closedOnOrAfter=closedOnCutoff,
        )
        updateIssueSnapshotCaptureStatus(
            current_project_name=projectName,
            current_project_issues_pages_loaded=getIssueSnapshotCaptureStatus().get("current_project_issues_pages_total", 0),
            current_project_time_pages_loaded=0,
            current_project_time_pages_total=0,
        )
        spentHoursByIssue = fetchSpentHoursByIssueForProjectYear(
            config.redmineUrl,
            config.apiKey,
            str(identifier),
            captureYear,
            progressCallback=updateTimeProgress,
        )
        applySpentHoursYearByIssue(issues, spentHoursByIssue)
        snapshotRunId = createIssueSnapshotRun(capturedForDate, project, issues)

        createdRuns = 1 if snapshotRunId is not None else 0
        alreadyCaptured = 0 if snapshotRunId is not None else 1
        capturedIssues = len(issues) if snapshotRunId is not None else 0
        updateIssueSnapshotCaptureStatus(
            processed_projects=1,
            last_completed_project_name=projectName,
            current_project_name=None,
            created_runs=createdRuns,
            captured_issues=capturedIssues,
            already_captured_projects=alreadyCaptured,
            remaining_projects=0,
            current_project_issues_pages_loaded=0,
            current_project_issues_pages_total=0,
            current_project_time_pages_loaded=0,
            current_project_time_pages_total=0,
        )
    finally:
        updateIssueSnapshotCaptureStatus(
            is_running=False,
            current_project_name=None,
            remaining_projects=0,
            current_project_issues_pages_loaded=0,
            current_project_issues_pages_total=0,
            current_project_time_pages_loaded=0,
            current_project_time_pages_total=0,
        )

    return {
        "captured_for_date": capturedForDate,
        "created_runs": 1 if snapshotRunId is not None else 0,
        "captured_issues": len(issues) if snapshotRunId is not None else 0,
        "already_captured_projects": 0 if snapshotRunId is not None else 1,
        "remaining_projects": 0,
        "snapshot_runs": listRecentIssueSnapshotRuns(),
    }
