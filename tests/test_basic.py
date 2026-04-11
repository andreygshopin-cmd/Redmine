from fastapi.testclient import TestClient

from src.redmine import app as app_module
from src.redmine.app import app, getTime, readRoot
from src.redmine.config import loadConfig
from src.redmine.db import normalizeDatabaseUrl
from src.redmine.redmine_client import normalizeIssue, normalizeProject, parseRedmineDate


client = TestClient(app)


def testLoadConfigReturnsObject() -> None:
    config = loadConfig()
    assert config is not None
    assert config.appHost != ""


def testReadRootReturnsHtmlPage() -> None:
    response = readRoot()

    assert response.media_type == "text/html"
    assert "Capture issue snapshot" in response.body.decode("utf-8")
    assert "Recent snapshot batches" in response.body.decode("utf-8")


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
        lambda: [{"project_name": "Alpha", "total_issues": 5}],
    )

    response = client.get("/api/issues/snapshots/runs")

    assert response.status_code == 200
    assert response.json()["snapshot_runs"][0]["project_name"] == "Alpha"


def testGetIssueSnapshotBatchesEndpointReturnsRows(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listRecentIssueSnapshotBatches",
        lambda: [{"id": 7, "captured_for_date": "2026-04-11", "total_projects": 3}],
    )

    response = client.get("/api/issues/snapshots/batches")

    assert response.status_code == 200
    assert response.json()["snapshot_batches"][0]["id"] == 7


def testCaptureIssueSnapshotsEndpointCreatesRuns(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(
        app_module,
        "captureAllIssueSnapshots",
        lambda: {
            "snapshot_batch_id": 55,
            "captured_for_date": "2026-04-11",
            "created_runs": 1,
            "captured_issues": 1,
            "skipped_projects": [],
            "snapshot_batches": [{"id": 55, "captured_for_date": "2026-04-11"}],
            "snapshot_runs": [{"id": 100, "project_name": "Alpha", "total_issues": 1}],
        },
    )

    response = client.post("/api/issues/snapshots/capture")

    assert response.status_code == 200
    assert response.json()["snapshot_batch_id"] == 55
    assert response.json()["created_runs"] == 1
    assert response.json()["captured_issues"] == 1
    assert response.json()["snapshot_batches"][0]["id"] == 55


def testCaptureIssueSnapshotsEndpointSkipsForbiddenProject(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(
        app_module,
        "captureAllIssueSnapshots",
        lambda: {
            "snapshot_batch_id": 77,
            "captured_for_date": "2026-04-11",
            "created_runs": 0,
            "captured_issues": 0,
            "skipped_projects": [{"project_name": "Alpha", "reason": "403 Client Error: Forbidden"}],
            "snapshot_batches": [{"id": 77}],
            "snapshot_runs": [],
        },
    )

    response = client.post("/api/issues/snapshots/capture")

    assert response.status_code == 200
    assert response.json()["snapshot_batch_id"] == 77
    assert response.json()["created_runs"] == 0
    assert response.json()["captured_issues"] == 0
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
