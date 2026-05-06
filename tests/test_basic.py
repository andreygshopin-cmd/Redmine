from fastapi.testclient import TestClient

from src.redmine import app as app_module
from src.redmine.app import app, getTime, readBitrixDealSnapshotComparePage, readBitrixPage, readRoot
from src.redmine.bitrix_client import fetchBitrixUserNames
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
    assert config.bitrixPortalUrl.startswith("https://")


def testReadRootReturnsHtmlPage() -> None:
    response = readRoot()

    assert response.media_type == "text/html"
    assert "Получение срезов задач" in response.body.decode("utf-8")
    assert "Удаление среза по дате" in response.body.decode("utf-8")


def testReadBitrixPageReturnsHtmlPage() -> None:
    response = readBitrixPage()
    body = response.body.decode("utf-8")

    assert response.media_type == "text/html"
    assert "Анализ сделок Bitrix" in body
    assert "Анализ изменений по сделкам Bitrix за интервалы времени. Формирование отчетности" in body
    assert "Получить срез по сделкам, лидам, счетам" in body
    assert "Удалить выбранный срез" in body
    assert "Выгрузить в Excel" in body
    assert "/api/bitrix/snapshots/capture/start" in body
    assert "/api/bitrix/snapshots/capture/page" in body
    assert "/api/bitrix/deal-snapshots?limit=500" in body
    assert "Скачиваю все сделки из Bitrix24" not in body
    assert 'href="/Bitrix/leads"' in body
    assert 'href="/Bitrix/invoices"' in body
    assert 'data-bitrix-filter="company_name"' in body
    assert "button-muted" in body
    assert 'data-bitrix-filter="currency_id"' not in body


def testReadBitrixPageMasksCredential(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "bitrixCredential", "123/secret-webhook-code")

    body = readBitrixPage().body.decode("utf-8")

    assert "123/secret-webhook-code" not in body


def testReadBitrixDealSnapshotComparePageReturnsHtmlPage() -> None:
    body = readBitrixDealSnapshotComparePage().body.decode("utf-8")

    assert "Сравнение срезов сделок" in body
    assert "/api/bitrix/deal-snapshots/compare" in body
    assert "<th>Валюта</th>" not in body


def testGetBitrixDealsEndpointReturnsItems(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "https://sms-it.bitrix24.ru")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "1/test-webhook")

    captured: dict[str, object] = {}

    def fakeFetchBitrixDeals(**kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "portal_url": "https://sms-it.bitrix24.ru",
            "auth_mode": "webhook_path",
            "items": [{"id": 501, "title": "Deal #501"}],
            "total": 1,
            "requested_limit": 5,
            "filter": {"stageId": "NEW"},
        }

    monkeypatch.setattr(app_module, "fetchBitrixDeals", fakeFetchBitrixDeals)

    response = client.get("/api/bitrix/deals?limit=5&stage_id=NEW")

    assert response.status_code == 200
    assert response.json()["items"][0]["id"] == 501
    assert captured["portalUrl"] == "https://sms-it.bitrix24.ru"
    assert captured["credential"] == "1/test-webhook"
    assert captured["limit"] == 5
    assert captured["stageId"] == "NEW"


def testGetBitrixDealsEndpointRequiresCredential(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "https://sms-it.bitrix24.ru")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "")

    response = client.get("/api/bitrix/deals")

    assert response.status_code == 400
    assert "Btrx is not set" in response.json()["detail"]


def testGetBitrixProfileEndpointReturnsProfile(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "https://sms-it.bitrix24.ru")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "1/test-webhook")

    captured: dict[str, object] = {}

    def fakeFetchBitrixProfile(**kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "portal_url": "https://sms-it.bitrix24.ru",
            "auth_mode": "webhook_path",
            "profile": {"ID": "1", "NAME": "Test"},
        }

    monkeypatch.setattr(app_module, "fetchBitrixProfile", fakeFetchBitrixProfile)

    response = client.get("/api/bitrix/profile")

    assert response.status_code == 200
    assert response.json()["profile"]["ID"] == "1"
    assert captured["portalUrl"] == "https://sms-it.bitrix24.ru"
    assert captured["credential"] == "1/test-webhook"


def testFetchBitrixUserNamesUsesBatchFilter(monkeypatch) -> None:
    capturedPayloads: list[dict[str, object]] = []

    def fakeCallBitrixRestMethod(portalUrl, credential, method, payload=None, timeout=45):
        capturedPayloads.append(dict(payload or {}))
        return {
            "result": [
                {"ID": "7", "LAST_NAME": "Иванов", "NAME": "Иван"},
                {"ID": "8", "LAST_NAME": "Петров", "NAME": "Петр"},
            ]
        }

    monkeypatch.setattr("src.redmine.bitrix_client.callBitrixRestMethod", fakeCallBitrixRestMethod)

    userNames = fetchBitrixUserNames("https://sms-it.bitrix24.ru", "1/test-webhook", [7, 8, 7])

    assert userNames == {7: "Иванов Иван", 8: "Петров Петр"}
    assert capturedPayloads == [{"filter": {"ID": [7, 8]}}]


def testCaptureBitrixDealSnapshotEndpointStoresDeals(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "https://sms-it.bitrix24.ru")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "1/test-webhook")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "must_change_password": False})
    monkeypatch.setattr(
        app_module,
        "fetchAllBitrixDeals",
        lambda **kwargs: {
            "auth_mode": "webhook_path",
            "total": 1,
            "items": [{"id": 501, "title": "Deal #501"}],
        },
    )
    monkeypatch.setattr(
        app_module,
        "fetchAllBitrixLeads",
        lambda **kwargs: {
            "auth_mode": "webhook_path",
            "total": 1,
            "items": [{"id": 601, "title": "Lead #601"}],
        },
    )
    monkeypatch.setattr(
        app_module,
        "fetchAllBitrixInvoices",
        lambda **kwargs: {
            "auth_mode": "webhook_path",
            "total": 1,
            "items": [{"id": 701, "title": "Invoice #701"}],
        },
    )
    monkeypatch.setattr(app_module, "fetchBitrixDealDictionaries", lambda **kwargs: {})
    monkeypatch.setattr(app_module, "fetchBitrixCrmItemDictionaries", lambda **kwargs: {})
    monkeypatch.setattr(app_module, "deleteBitrixDealSnapshotForDate", lambda capturedForDate: {})
    monkeypatch.setattr(app_module, "deleteBitrixCrmSnapshotForDate", lambda entityType, capturedForDate: {})
    monkeypatch.setattr(
        app_module,
        "createBitrixDealSnapshot",
        lambda deals, capturedForDate, dictionaries=None: {
            "snapshot_run_id": 10,
            "captured_for_date": capturedForDate,
            "total_deals": len(deals),
        },
    )
    monkeypatch.setattr(
        app_module,
        "createBitrixCrmSnapshot",
        lambda entityType, items, capturedForDate, dictionaries=None: {
            "snapshot_run_id": 20 if entityType == "lead" else 30,
            "captured_for_date": capturedForDate,
            "entity_type": entityType,
            "total_items": len(items),
        },
    )

    response = client.post("/api/bitrix/deal-snapshots/capture")

    assert response.status_code == 200
    assert response.json()["snapshot_run_id"] == 10
    assert response.json()["total_deals"] == 1
    assert response.json()["lead_snapshot"]["total_items"] == 1
    assert response.json()["invoice_snapshot"]["total_items"] == 1


def testCompareBitrixDealSnapshotsEndpointReturnsChanges(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "must_change_password": False})
    monkeypatch.setattr(
        app_module,
        "compareBitrixDealSnapshots",
        lambda leftDate, rightDate: {
            "left_run": {"captured_for_date": leftDate},
            "right_run": {"captured_for_date": rightDate},
            "changes": [{"deal_id": 501, "change_type": "changed"}],
            "available_dates": [rightDate, leftDate],
        },
    )

    response = client.get("/api/bitrix/deal-snapshots/compare?left_date=2026-05-01&right_date=2026-05-06")

    assert response.status_code == 200
    assert response.json()["changes"][0]["deal_id"] == 501


def testDeleteBitrixDealSnapshotByDateEndpointDeletesRows(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "must_change_password": False})
    monkeypatch.setattr(
        app_module,
        "deleteBitrixDealSnapshotForDate",
        lambda capturedForDate: {
            "captured_for_date": capturedForDate,
            "deleted_items": 100,
            "deleted_runs": 1,
        },
    )
    monkeypatch.setattr(app_module, "deleteBitrixCrmSnapshotForDate", lambda entityType, capturedForDate: {
        "entity_type": entityType,
        "captured_for_date": capturedForDate,
        "deleted_items": 0,
        "deleted_runs": 0,
    })
    monkeypatch.setattr(app_module, "listBitrixDealSnapshotRuns", lambda limit=50: [])

    response = client.delete("/api/bitrix/deal-snapshots/by-date?captured_for_date=2026-05-06")

    assert response.status_code == 200
    assert response.json()["deleted_items"] == 100
    assert response.json()["deleted_runs"] == 1


def testExportBitrixDealSnapshotEndpointReturnsAnsiCsv(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "must_change_password": False})
    monkeypatch.setattr(
        app_module,
        "getBitrixDealSnapshotItems",
        lambda capturedForDate, page=1, pageSize=5000, filters=None: {
            "snapshot_run": {"captured_for_date": capturedForDate or "2026-05-06"},
            "deals": [
                {
                    "deal_id": 501,
                    "title": "Сделка",
                    "stage_name": "Новая",
                    "assigned_by_name": "Иванов Иван",
                    "opportunity": 1234.6,
                    "category_name": "Продажи",
                    "created_time": "2026-05-01",
                    "updated_time": "2026-05-06",
                }
            ] if page == 1 else [],
            "total_count": 1,
        },
    )

    response = client.get("/api/bitrix/deal-snapshots/export?captured_for_date=2026-05-06")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=windows-1251"
    assert "bitrix-deals-2026-05-06.csv" in response.headers["content-disposition"]
    assert "Иванов Иван" in response.content.decode("cp1251")
    assert ";1235;" in response.content.decode("cp1251")


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
            "custom_fields": [{"id": 27, "name": "Базовая оценка", "value": "32"}],
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
    assert issue["baseline_estimate_hours"] == 32.0
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
        "getSnapshotIssuesForProjectByDate",
        lambda projectRedmineId, capturedForDate=None: {
            "snapshot_run": {
                "project_redmine_id": projectRedmineId,
                "project_name": "Billing",
                "captured_for_date": "2026-04-13",
            },
            "issues": [{"issue_redmine_id": 501, "subject": "Add chart"}],
            "available_dates": ["2026-04-13"],
        },
    )

    response = client.get("/projects/10/latest-snapshot-issues")

    assert response.status_code == 200
    body = response.text
    assert "Задачи последнего среза проекта" in body
    assert "Billing" in body
    assert "Add chart" in body


def testGetProjectBurndownPageReturnsChartPage(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listStoredProjects",
        lambda: [{"redmine_id": 10, "name": "Billing", "identifier": "billing"}],
    )
    monkeypatch.setattr(
        app_module,
        "getSnapshotRunsWithIssuesForProjectYear",
        lambda projectRedmineId, year: {
            "project": {
                "project_redmine_id": projectRedmineId,
                "project_name": "Billing",
                "project_identifier": "billing",
            },
            "snapshot_runs": [
                {
                    "id": 1,
                    "captured_for_date": f"{year}-04-13",
                    "total_baseline_estimate_hours": 12.0,
                    "issues": [
                        {
                            "issue_redmine_id": 501,
                            "tracker_name": "Feature",
                            "status_name": "В работе",
                            "baseline_estimate_hours": 12.0,
                            "estimated_hours": 0.0,
                            "spent_hours": 0.0,
                            "parent_issue_redmine_id": None,
                        },
                        {
                            "issue_redmine_id": 502,
                            "tracker_name": "Разработка",
                            "status_name": "В работе",
                            "baseline_estimate_hours": 0.0,
                            "estimated_hours": 10.0,
                            "spent_hours": 4.0,
                            "parent_issue_redmine_id": 501,
                        },
                    ],
                }
            ],
        },
    )

    response = client.get("/projects/10/burndown")

    assert response.status_code == 200
    assert "Диаграмма сгорания проекта" in response.text
    assert "Billing" in response.text
    assert "P1 = факт / база" in response.text
    assert "Объем.Прогноз" in response.text


def testSnapshotIssuesPageUsesCleanRussianText(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        app_module,
        "getSnapshotIssuesForProjectByDate",
        lambda projectRedmineId, capturedForDate=None: {
            "snapshot_run": {
                "project_redmine_id": projectRedmineId,
                "project_name": "Billing",
                "captured_for_date": "2026-04-14",
            },
            "issues": [{"issue_redmine_id": 501, "subject": "Add chart", "baseline_estimate_hours": 3.5}],
            "available_dates": ["2026-04-14"],
        },
    )

    response = client.get("/projects/10/latest-snapshot-issues")

    assert response.status_code == 200
    assert "Базовая оценка, ч" in response.text
    assert "Проект: Billing." in response.text


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
