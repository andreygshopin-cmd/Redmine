from fastapi.testclient import TestClient

from src.redmine import app as app_module
from src.redmine.app import app, getTime, readRoot
from src.redmine.config import loadConfig
from src.redmine.db import chunkSequence, normalizeDatabaseUrl
from src.redmine.redmine_client import (
    applySpentHoursYearByIssue,
    normalizeIssue,
    normalizeProject,
    parseRedmineDate,
)


client = TestClient(app)


def testLoadConfigReturnsObject() -> None:
    config = loadConfig()
    assert config is not None
    assert config.appHost != ""


def testReadRootReturnsHtmlPage() -> None:
    response = readRoot()

    assert response.media_type == "text/html"
    assert "Получение срезов задач" in response.body.decode("utf-8")
    assert "Удаление среза по дате" in response.body.decode("utf-8")


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


def testChunkSequenceSplitsIntoSmallerBatches() -> None:
    payload = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]

    chunks = chunkSequence(payload, 2)

    assert chunks == [
        [{"id": 1}, {"id": 2}],
        [{"id": 3}, {"id": 4}],
        [{"id": 5}],
    ]


def testNormalizeProjectMapsFields() -> None:
    project = normalizeProject(
        {
            "id": 42,
            "name": "Portal",
            "identifier": "portal",
            "status": 1,
            "homepage": "https://example.com",
            "parent": {"id": 7},
            "created_on": "2026-04-01T10:00:00Z",
            "updated_on": "2026-04-02T11:00:00Z",
        }
    )

    assert project["redmine_id"] == 42
    assert project["parent_redmine_id"] == 7
    assert project["created_on"] == "2026-04-01T10:00:00+00:00"
    assert project["updated_on"] == "2026-04-02T11:00:00+00:00"


def testNormalizeIssueMapsFields() -> None:
    issue = normalizeIssue(
        {
            "id": 501,
            "subject": "Add chart",
            "tracker": {"id": 2, "name": "Feature"},
            "status": {"id": 3, "name": "In Progress"},
            "priority": {"id": 4, "name": "Normal"},
            "author": {"id": 10, "name": "Ann"},
            "assigned_to": {"id": 20, "name": "Bob"},
            "parent": {"id": 400},
            "fixed_version": {"id": 8, "name": "Sprint 1"},
            "done_ratio": 50,
            "estimated_hours": 12.5,
            "spent_hours": 3.0,
            "start_date": "2026-04-01",
            "due_date": "2026-04-05",
            "created_on": "2026-04-01T10:00:00Z",
            "updated_on": "2026-04-02T11:00:00Z",
            "closed_on": None,
        },
        42,
    )

    assert issue["project_redmine_id"] == 42
    assert issue["issue_redmine_id"] == 501
    assert issue["tracker_name"] == "Feature"
    assert issue["fixed_version_name"] == "Sprint 1"
    assert issue["created_on"] == "2026-04-01T10:00:00+00:00"


def testParseRedmineDateAllowsEmptyValue() -> None:
    assert parseRedmineDate(None) is None


def testApplySpentHoursYearByIssueOverridesWithYearValue() -> None:
    issues = [
        {"issue_redmine_id": 10, "spent_hours_year": 99.0},
        {"issue_redmine_id": 11, "spent_hours_year": 88.0},
    ]

    result = applySpentHoursYearByIssue(issues, {10: 5.5})

    assert result[0]["spent_hours_year"] == 5.5
    assert result[1]["spent_hours_year"] == 0.0


def testGetProjectsEndpointReturnsStoredProjects(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listStoredProjects",
        lambda: [{"redmine_id": 10, "name": "Billing", "identifier": "billing"}],
    )

    response = client.get("/api/projects")

    assert response.status_code == 200
    assert response.json()["projects"][0]["redmine_id"] == 10


def testGetLatestSnapshotIssuesForProjectPageReturnsHtml(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        app_module,
        "getLatestSnapshotIssuesForProject",
        lambda projectRedmineId: {
            "snapshot_run": {
                "project_redmine_id": projectRedmineId,
                "project_name": "Billing",
                "captured_for_date": "2026-04-13",
            },
            "issues": [{"issue_redmine_id": 501, "subject": "Add chart"}],
        },
    )

    response = client.get("/projects/10/latest-snapshot-issues")

    assert response.status_code == 200
    body = response.text
    assert "Задачи последнего среза проекта" in body
    assert "Billing" in body
    assert "Add chart" in body


def testRefreshProjectsEndpointStoresOnlyMissingRows(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(
        app_module,
        "fetchAllProjectsFromRedmine",
        lambda redmineUrl, apiKey: [
            {"redmine_id": 1, "name": "Alpha", "identifier": "alpha"},
            {"redmine_id": 2, "name": "Beta", "identifier": "beta"},
        ],
    )
    monkeypatch.setattr(app_module, "storeMissingProjects", lambda projects: 2)
    monkeypatch.setattr(
        app_module,
        "listStoredProjects",
        lambda: [
            {"redmine_id": 1, "name": "Alpha", "identifier": "alpha"},
            {"redmine_id": 2, "name": "Beta", "identifier": "beta"},
        ],
    )

    response = client.post("/api/projects/refresh")

    assert response.status_code == 200
    assert response.json()["added_count"] == 2
    assert len(response.json()["projects"]) == 2


def testGetIssueSnapshotRunsEndpointReturnsRows(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listRecentIssueSnapshotRuns",
        lambda: [{"project_name": "Alpha", "total_issues": 5, "total_spent_hours_year": 2.0}],
    )

    response = client.get("/api/issues/snapshots/runs")

    assert response.status_code == 200
    assert response.json()["snapshot_runs"][0]["project_name"] == "Alpha"


def testDeleteIssueSnapshotsByDateEndpointDeletesRows(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        app_module,
        "deleteIssueSnapshotsForDate",
        lambda capturedForDate: {
            "captured_for_date": capturedForDate,
            "deleted_items": 12,
            "deleted_runs": 3,
        },
    )
    monkeypatch.setattr(
        app_module,
        "listRecentIssueSnapshotRuns",
        lambda: [{"id": 200, "project_name": "Beta", "total_issues": 10, "total_spent_hours_year": 4.0}],
    )

    response = client.delete("/api/issues/snapshots/by-date?captured_for_date=2026-04-11")

    assert response.status_code == 200
    assert response.json()["deleted_items"] == 12
    assert response.json()["deleted_runs"] == 3
    assert response.json()["snapshot_runs"][0]["id"] == 200


def testDeleteIssueSnapshotsByDateEndpointValidatesDate(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)

    response = client.delete("/api/issues/snapshots/by-date?captured_for_date=11-04-2026")

    assert response.status_code == 400
    assert "YYYY-MM-DD" in response.json()["detail"]


def testCaptureIssueSnapshotsEndpointCreatesRuns(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(
        app_module,
        "captureAllIssueSnapshots",
        lambda: {
            "captured_for_date": "2026-04-11",
            "created_runs": 1,
            "captured_issues": 1,
            "already_captured_projects": 4,
            "remaining_projects": 12,
            "skipped_projects": [],
            "snapshot_runs": [{"id": 100, "project_name": "Alpha", "total_issues": 1, "total_spent_hours_year": 5.5}],
        },
    )

    response = client.post("/api/issues/snapshots/capture")

    assert response.status_code == 200
    assert response.json()["captured_for_date"] == "2026-04-11"
    assert response.json()["created_runs"] == 1
    assert response.json()["captured_issues"] == 1
    assert response.json()["remaining_projects"] == 12


def testCaptureIssueSnapshotsUsesCurrentYearSpentHours(monkeypatch) -> None:
    from src.redmine import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module.loadConfig, lambda: app_module.config)
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(snapshots_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(snapshots_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        snapshots_module,
        "fetchAllProjectsFromRedmine",
        lambda redmineUrl, apiKey: [{"redmine_id": 1, "name": "Alpha", "identifier": "alpha"}],
    )
    monkeypatch.setattr(snapshots_module, "storeMissingProjects", lambda projects: 1)
    monkeypatch.setattr(
        snapshots_module,
        "listStoredProjects",
        lambda: [{"redmine_id": 1, "name": "Alpha", "identifier": "alpha"}],
    )
    monkeypatch.setattr(
        snapshots_module,
        "listProjectsWithoutSnapshotForDate",
        lambda capturedForDate: [{"redmine_id": 1, "name": "Alpha", "identifier": "alpha"}],
    )
    monkeypatch.setattr(
        snapshots_module,
        "fetchAllIssuesForProject",
        lambda redmineUrl, apiKey, projectIdentifier, projectRedmineId: [
            {"issue_redmine_id": 10, "spent_hours": 99.0, "spent_hours_year": 0.0, "estimated_hours": 1.0}
        ],
    )
    year_calls = []
    monkeypatch.setattr(
        snapshots_module,
        "fetchSpentHoursByIssueForProjectYear",
        lambda redmineUrl, apiKey, projectIdentifier, year: year_calls.append(year) or {10: 5.5},
    )
    created_payloads = []
    monkeypatch.setattr(
        snapshots_module,
        "createIssueSnapshotRun",
        lambda capturedForDate, project, issues: created_payloads.append(issues) or 101,
    )
    monkeypatch.setattr(snapshots_module, "listRecentIssueSnapshotRuns", lambda: [])

    result = snapshots_module.captureAllIssueSnapshots()

    assert result["created_runs"] == 1
    assert year_calls[0] == 2026
    assert created_payloads[0][0]["spent_hours"] == 99.0
    assert created_payloads[0][0]["spent_hours_year"] == 5.5


def testCaptureIssueSnapshotsEndpointSkipsForbiddenProject(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(
        app_module,
        "captureAllIssueSnapshots",
        lambda: {
            "captured_for_date": "2026-04-11",
            "created_runs": 0,
            "captured_issues": 0,
            "already_captured_projects": 10,
            "remaining_projects": 5,
            "skipped_projects": [{"project_name": "Alpha", "reason": "403 Client Error: Forbidden"}],
            "snapshot_runs": [],
        },
    )

    response = client.post("/api/issues/snapshots/capture")

    assert response.status_code == 200
    assert response.json()["captured_for_date"] == "2026-04-11"
    assert response.json()["created_runs"] == 0
    assert response.json()["captured_issues"] == 0
    assert response.json()["already_captured_projects"] == 10
    assert response.json()["skipped_projects"][0]["project_name"] == "Alpha"


def testCaptureIssueSnapshotsEndpointMapsRuntimeErrorToHttp(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(
        app_module,
        "captureAllIssueSnapshots",
        lambda: (_ for _ in ()).throw(RuntimeError("No projects in the database. Refresh projects first.")),
    )

    response = client.post("/api/issues/snapshots/capture")

    assert response.status_code == 400
    assert "Refresh projects first" in response.json()["detail"]
