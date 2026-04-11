from datetime import UTC, datetime

from requests import HTTPError

from src.redmine.config import loadConfig
from src.redmine.db import (
    createIssueSnapshotBatch,
    createIssueSnapshotRun,
    ensureIssueSnapshotTables,
    ensureProjectsTable,
    finalizeIssueSnapshotBatch,
    listRecentIssueSnapshotBatches,
    listRecentIssueSnapshotRuns,
    listStoredProjects,
    storeMissingProjects,
)
from src.redmine.redmine_client import (
    fetchAllIssuesForProject,
    fetchAllProjectsFromRedmine,
)


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
    snapshotBatchId = createIssueSnapshotBatch(capturedForDate, len(projects))
    createdRuns = 0
    capturedIssues = 0
    totalEstimatedHours = 0.0
    totalSpentHours = 0.0
    skippedProjects = []

    for project in projects:
        identifier = project.get("identifier")
        if not identifier:
            skippedProjects.append(
                {
                    "project_redmine_id": project["redmine_id"],
                    "project_name": project["name"],
                    "reason": "Project identifier is missing",
                }
            )
            continue

        try:
            issues = fetchAllIssuesForProject(
                config.redmineUrl,
                config.apiKey,
                str(identifier),
                int(project["redmine_id"]),
            )
        except HTTPError as error:
            skippedProjects.append(
                {
                    "project_redmine_id": project["redmine_id"],
                    "project_name": project["name"],
                    "reason": str(error),
                }
            )
            continue

        createIssueSnapshotRun(snapshotBatchId, capturedForDate, project, issues)
        createdRuns += 1
        capturedIssues += len(issues)
        totalEstimatedHours += sum(float(issue.get("estimated_hours") or 0) for issue in issues)
        totalSpentHours += sum(float(issue.get("spent_hours") or 0) for issue in issues)

    finalizeIssueSnapshotBatch(
        snapshotBatchId,
        completedProjects=createdRuns,
        skippedProjects=len(skippedProjects),
        totalIssues=capturedIssues,
        totalEstimatedHours=totalEstimatedHours,
        totalSpentHours=totalSpentHours,
    )

    return {
        "snapshot_batch_id": snapshotBatchId,
        "captured_for_date": capturedForDate,
        "created_runs": createdRuns,
        "captured_issues": capturedIssues,
        "skipped_projects": skippedProjects,
        "snapshot_batches": listRecentIssueSnapshotBatches(),
        "snapshot_runs": listRecentIssueSnapshotRuns(),
    }
