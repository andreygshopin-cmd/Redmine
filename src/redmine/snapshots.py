from datetime import UTC, datetime
from threading import Lock, Thread

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


captureStatusLock = Lock()
captureStatusState: dict[str, object] = {
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
}


def updateIssueSnapshotCaptureStatus(**values: object) -> None:
    with captureStatusLock:
        captureStatusState.update(values)


def resetIssueSnapshotCaptureStatus() -> None:
    updateIssueSnapshotCaptureStatus(
        is_running=False,
        captured_for_date=None,
        total_projects=0,
        processed_projects=0,
        current_project_name=None,
        last_completed_project_name=None,
        created_runs=0,
        captured_issues=0,
        already_captured_projects=0,
        remaining_projects=0,
        current_project_issues_pages_loaded=0,
        current_project_issues_pages_total=0,
        current_project_time_pages_loaded=0,
        current_project_time_pages_total=0,
        error_message=None,
    )


def getIssueSnapshotCaptureStatus() -> dict[str, object]:
    with captureStatusLock:
        return dict(captureStatusState)


def isIssueSnapshotCaptureRunning() -> bool:
    with captureStatusLock:
        return bool(captureStatusState["is_running"])


def _runIssueSnapshotCaptureInBackground() -> None:
    try:
        captureAllIssueSnapshots()
    except Exception as error:  # pragma: no cover
        updateIssueSnapshotCaptureStatus(
            is_running=False,
            current_project_name=None,
            error_message=str(error),
        )


def startIssueSnapshotCaptureInBackground() -> bool:
    with captureStatusLock:
        if bool(captureStatusState["is_running"]):
            return False

        captureStatusState.update(
            {
                "is_running": True,
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
            }
        )

    Thread(target=_runIssueSnapshotCaptureInBackground, daemon=True).start()
    return True


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
    activeProjects = [project for project in projects if not bool(project.get("is_disabled"))]
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
