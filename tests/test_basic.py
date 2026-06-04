from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.redmine import app as app_module
from src.redmine import bitrix_client as bitrix_client_module
from src.redmine import db as db_module
from src.redmine.app import app, getTime, readBitrixDealSnapshotComparePage, readBitrixInvoiceSummaryPage, readBitrixInvoicesPage, readBitrixLeadSnapshotComparePage, readBitrixLeadsPage, readBitrixPage, readRoot
from src.redmine.bitrix_client import fetchBitrixUserNames, fetchBitrixUsers
from src.redmine.config import loadConfig
from src.redmine.dates import getSnapshotBusinessDateIso
from src.redmine.db import chunkSequence, normalizeDatabaseUrl
from src.redmine.redmine_client import (
    applySpentHoursYearByIssue,
    normalizeIssue,
    normalizeProject,
    parseRedmineDate,
)


client = TestClient(app)
REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def bypassAuthMiddleware(monkeypatch):
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(
        app_module,
        "_getCurrentUser",
        lambda request: {
            "login": "tester",
            "roles": ["Admin"],
            "must_change_password": False,
        },
    )


def buildSnapshotIssuesPayload(
    projectRedmineId: int = 10,
    capturedForDate: str = "2026-04-13",
    issues: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    resolvedIssues = issues or [
        {
            "issue_redmine_id": 501,
            "subject": "Add chart",
            "tracker_name": "Разработка",
            "status_name": "В работе",
            "done_ratio": 0,
            "baseline_estimate_hours": 3.5,
            "estimated_hours": 4.0,
            "risk_estimate_hours": 5.0,
            "spent_hours": 1.0,
            "spent_hours_year": 1.0,
            "parent_issue_redmine_id": None,
        }
    ]
    return {
        "snapshot_run": {
            "project_redmine_id": projectRedmineId,
            "project_name": "Billing",
            "project_identifier": "billing",
            "captured_for_date": capturedForDate,
        },
        "issues": resolvedIssues,
        "available_dates": [capturedForDate],
        "filter_options": {"tracker_names": ["Разработка"], "status_names": ["В работе"]},
        "summary": {},
        "page": 1,
        "page_size": 1000,
        "total_pages": 1,
        "total_filtered_issues": len(resolvedIssues),
        "total_all_issues": len(resolvedIssues),
    }


def testLoadConfigReturnsObject() -> None:
    config = loadConfig()
    assert config is not None
    assert config.appHost != ""
    assert config.bitrixPortalUrl.startswith("https://")


def testSnapshotBusinessDateUsesSamaraTimezoneByDefault() -> None:
    assert getSnapshotBusinessDateIso(datetime(2026, 5, 29, 22, 0, tzinfo=UTC)) == "2026-05-30"
    assert getSnapshotBusinessDateIso(datetime(2026, 5, 29, 19, 0, tzinfo=UTC)) == "2026-05-29"


def testHealthShowsSnapshotDateSettings() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["snapshot_timezone"] == "Europe/Samara"
    assert payload["snapshot_business_date"]


def testRenderYamlSchedulesWeeklySnapshotCronJobs() -> None:
    body = (REPO_ROOT / "render.yaml").read_text(encoding="utf-8")

    assert "name: redmine-tdfp" in body
    assert "name: redmine-api" not in body
    assert "name: redmine-bitrix-snapshot-cron" in body
    assert 'schedule: "0 22 * * FRI"' in body
    assert "startCommand: python -m src.redmine.capture_bitrix_snapshots" in body
    assert "envVarKey: Btrx" in body
    assert "name: redmine-snapshot-cron" in body
    assert 'schedule: "0 23 * * FRI"' in body
    assert "startCommand: python -m src.redmine.capture_snapshots" in body
    assert "envVarKey: REDMINE_URL" in body
    assert "envVarKey: REDMINE_API_KEY" in body


def testDeployRenderWorkflowAvoidsRepositoryCheckoutDependency() -> None:
    body = (REPO_ROOT / ".github" / "workflows" / "deploy-render.yml").read_text(encoding="utf-8")

    assert "actions/checkout" not in body
    assert "actions/setup-python" not in body
    assert "python3 - <<'PY'" in body
    assert "EXPECTED_COMMIT_SHA" in body


def testRunBitrixSnapshotCaptureJobUsesPagedCapture(monkeypatch) -> None:
    from src.redmine import capture_bitrix_snapshots as bitrix_capture_module

    calls: list[tuple[str, str, int]] = []

    def fakeStartBitrixSnapshotCapture(entities=None):
        assert entities == "deal,lead,invoice"
        return {
            "session_id": "session-1",
            "captured_for_date": "2026-05-23",
            "entities": [{"key": "deal"}, {"key": "lead"}, {"key": "invoice"}],
        }

    def fakeCaptureBitrixSnapshotPage(payload):
        calls.append((payload.session_id, payload.entity, payload.start))
        if payload.entity == "deal" and payload.start == 0:
            return {
                "fetched": 500,
                "total": 600,
                "remaining": 100,
                "next": 500,
                "done": False,
            }
        return {
            "fetched": 600 if payload.entity == "deal" else 10,
            "total": 600 if payload.entity == "deal" else 10,
            "remaining": 0,
            "next": None,
            "done": True,
            "snapshot": {"entity": payload.entity},
        }

    monkeypatch.setattr(bitrix_capture_module, "startBitrixSnapshotCapture", fakeStartBitrixSnapshotCapture)
    monkeypatch.setattr(bitrix_capture_module, "captureBitrixSnapshotPage", fakeCaptureBitrixSnapshotPage)

    result = bitrix_capture_module.runBitrixSnapshotCaptureJob("deal,lead,invoice")

    assert result["captured_for_date"] == "2026-05-23"
    assert result["snapshots"] == {
        "deal": {"entity": "deal"},
        "lead": {"entity": "lead"},
        "invoice": {"entity": "invoice"},
    }
    assert calls == [
        ("session-1", "deal", 0),
        ("session-1", "deal", 500),
        ("session-1", "lead", 0),
        ("session-1", "invoice", 0),
    ]


def testReadRootReturnsHtmlPage() -> None:
    response = readRoot()

    assert response.media_type == "text/html"
    assert "Получение срезов задач" in response.body.decode("utf-8")
    assert "Удаление среза по дате" in response.body.decode("utf-8")


def testIndexQuickLinksUseCurrentUserRoles() -> None:
    adminBody = app_module.buildIndexPage({
        "login": "andrey.shopin@sms-a.ru",
        "roles": ["Admin"],
    })
    userBody = app_module.buildIndexPage({
        "login": "regular@example.com",
        "roles": ["User"],
    })

    assert 'href="/dashboards/andrey.shopin%40sms-a.ru"' in adminBody
    assert 'class="dashboard-nav-button"' in adminBody
    assert 'class="bitrix-nav-button"' in adminBody
    assert 'href="/Bitrix"' in adminBody
    assert 'href="/admin/users"' in adminBody
    assert "Проекты Redmine</a>" not in adminBody
    assert "Срезы задач</a>" not in adminBody
    assert 'href="/dashboards/' not in userBody
    assert 'href="/Bitrix"' not in userBody
    assert 'href="/admin/users"' not in userBody
    assert "Отчет по неделям" in adminBody
    assert "/weekly-closed-features" in adminBody


def testAndreyDashboardHasFourProjectStateWidgets() -> None:
    widgets = app_module.DASHBOARD_USER_CONFIGS["andrey.shopin@sms-a.ru"]["widgets"]

    assert [widget["id"] for widget in widgets] == [
        "project-state-1",
        "project-state-2",
        "project-state-3",
        "project-state-4",
    ]


def testProjectsSummaryPageUsesFullWidthScrollableTable(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "listPlanningDirections", lambda: ["КОТ"])

    body = app_module.buildProjectsSummaryPage()

    assert "projects-summary-table-panel" in body
    assert "width: calc(100vw - 40px)" in body
    assert "margin-left: calc(50% - 50vw + 20px)" in body
    assert "overflow-x: auto" in body
    assert "overflow-y: auto" in body
    assert "max-height: calc(100vh - 24px)" in body
    assert "width: max(100%, 250ch)" in body
    assert "min-width: 250ch" in body
    assert "table-layout: fixed" in body
    assert "Прогноз по разработке на год" in body
    assert "Прогноз остатка (=прогноз - факт)" in body
    assert "Остаток по заведенным задачам" in body
    assert "Небаланс" in body
    assert 'data-filter-key="development_forecast_year_hours"' in body
    assert 'data-filter-key="development_forecast_remaining_hours"' in body
    assert 'data-filter-key="development_remaining_hours"' in body
    assert 'data-filter-key="development_imbalance_hours"' in body


def testWeeklyClosedFeaturesReportPageRendersRows(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.sms-it.ru")
    monkeypatch.setattr(
        app_module,
        "listWeeklyClosedFeatureReport",
        lambda capturedForDate=None: {
            "selected_date": capturedForDate or "2026-04-18",
            "available_dates": ["2026-04-18", "2026-04-11"],
            "rows": [
                {
                    "project_name": "Billing",
                    "feature_redmine_id": 123,
                    "feature_subject": "Закрытая фича",
                    "previous_captured_for_date": "2026-04-11",
                    "previous_status_name": "В работе",
                    "status_name": "Готово(КОТ)",
                    "baseline_estimate_hours": 10,
                    "development_plan_hours": 20,
                    "development_risk_plan_hours": 25,
                    "development_fact_hours": 18,
                    "bug_plan_hours": 3,
                    "bug_risk_plan_hours": 4,
                    "bug_fact_hours": 2,
                }
            ],
        },
    )

    body = app_module.buildWeeklyClosedFeaturesReportPage("2026-04-18")

    assert "Отчет по закрытым фичам за неделю" in body
    assert "Billing" in body
    assert "https://redmine.sms-it.ru/issues/123" in body
    assert "Закрытая фича" in body
    assert "Готово(КОТ)" in body
    assert "25,0" in body
    assert "4,0" in body


def testWeeklyClosedFeaturesRouteUsesSelectedDate(monkeypatch) -> None:
    calls: list[str | None] = []

    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://example/db")
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listWeeklyClosedFeatureReport",
        lambda capturedForDate=None: calls.append(capturedForDate)
        or {"selected_date": "2026-04-18", "available_dates": ["2026-04-18"], "rows": []},
    )

    response = client.get("/weekly-closed-features?captured_for_date=2026-04-18")

    assert response.status_code == 200
    assert calls == ["2026-04-18"]
    assert "Отчет по закрытым фичам за неделю" in response.text


def testSnapshotDevelopmentTotalMetricsMatchSnapshotIssuesFormula() -> None:
    metrics = app_module.buildSnapshotDevelopmentTotalMetrics(
        {
            "development_spent_hours": 20.0,
            "development_process_spent_hours": 5.0,
            "bug_spent_hours": 3.0,
            "development_spent_hours_year": 7.0,
            "development_process_spent_hours_year": 2.0,
            "bug_spent_hours_year": 1.0,
        },
        {
            "development_total_forecast_hours": 100.0,
            "development_total_remaining_hours": 36.5,
        },
    )

    assert metrics["development_fact_year_hours"] == pytest.approx(10.0)
    assert metrics["development_fact_total_hours"] == pytest.approx(28.0)
    assert metrics["development_forecast_year_hours"] == pytest.approx(82.0)
    assert metrics["development_forecast_minus_fact_year_hours"] == pytest.approx(72.0)
    assert metrics["development_remaining_hours"] == pytest.approx(36.5)


def testApplyFeatureForecastsUsesSeparateBugReserveForFeatureAndVirtual() -> None:
    issues = [
        {
            "issue_redmine_id": 501,
            "tracker_name": "Feature",
            "status_name": "В работе",
            "feature_group_issue_redmine_id": 501,
            "feature_group_is_virtual": False,
            "is_feature_group_root": True,
        },
        {
            "issue_redmine_id": 502,
            "tracker_name": "Разработка",
            "baseline_estimate_hours": 10.0,
            "volume_hours": 25.0,
            "risk_volume_hours": 28.0,
            "feature_group_issue_redmine_id": 501,
            "feature_group_is_virtual": False,
        },
        {
            "issue_redmine_id": 503,
            "tracker_name": "Ошибка",
            "baseline_estimate_hours": 0.0,
            "volume_hours": 1.0,
            "risk_volume_hours": 2.0,
            "feature_group_issue_redmine_id": 501,
            "feature_group_is_virtual": False,
        },
        {
            "issue_redmine_id": 601,
            "tracker_name": "Разработка",
            "baseline_estimate_hours": 4.0,
            "volume_hours": 11.0,
            "risk_volume_hours": 11.0,
            "feature_group_is_virtual": True,
        },
    ]

    result = app_module.applyFeatureForecastsToSnapshotIssues(issues, 200.0, 150.0)

    featureRows = [issue for issue in result if issue.get("feature_group_issue_redmine_id") == 501]
    assert {issue["feature_forecast_hours"] for issue in featureRows} == {35.0}
    assert {issue["feature_risk_forecast_hours"] for issue in featureRows} == {38.0}
    assert result[3]["feature_forecast_hours"] == pytest.approx(15.0)
    assert result[3]["feature_risk_forecast_hours"] == pytest.approx(15.0)


def testBurndownFeatureGroupsDoNotCountParentIssueVolume() -> None:
    groups = app_module.buildBurndownFeatureGroups(
        [
            {
                "issue_redmine_id": 501,
                "tracker_name": "Feature",
                "status_name": "В работе",
                "parent_issue_redmine_id": None,
            },
            {
                "issue_redmine_id": 502,
                "tracker_name": "Разработка",
                "status_name": "В работе",
                "baseline_estimate_hours": 10.0,
                "estimated_hours": 20.0,
                "risk_estimate_hours": 30.0,
                "spent_hours": 5.0,
                "parent_issue_redmine_id": 501,
            },
            {
                "issue_redmine_id": 503,
                "tracker_name": "Разработка",
                "status_name": "В работе",
                "baseline_estimate_hours": 2.0,
                "estimated_hours": 4.0,
                "risk_estimate_hours": 6.0,
                "spent_hours": 1.0,
                "parent_issue_redmine_id": 502,
            },
        ]
    )

    group = groups[0]
    assert group["development_volume"] == pytest.approx(4.0)
    assert group["development_volume_risk"] == pytest.approx(6.0)
    assert group["development_remaining"] == pytest.approx(3.0)
    assert group["development_remaining_risk"] == pytest.approx(5.0)


def testSnapshotIssueMetricSqlZeroesVolumeAndRemainingForIssuesWithChildren() -> None:
    volumeSql, riskVolumeSql, remainingSql, riskRemainingSql = db_module._buildSnapshotIssueMetricsSql()

    assert "child.parent_issue_redmine_id = issue_snapshot_items.issue_redmine_id" in volumeSql
    assert "child.parent_issue_redmine_id = issue_snapshot_items.issue_redmine_id" in riskVolumeSql
    assert "child.parent_issue_redmine_id = issue_snapshot_items.issue_redmine_id" in remainingSql
    assert "child.parent_issue_redmine_id = issue_snapshot_items.issue_redmine_id" in riskRemainingSql


def testSnapshotWeeklyDeveloperLoadUsesDevelopmentTrackersOnly(monkeypatch) -> None:
    monkeypatch.setattr(
        app_module,
        "getSnapshotTimeEntriesForProjectByDateRange",
        lambda *args, **kwargs: {
            "time_entries": [
                {"spent_on": "2026-04-13", "hours": 40.0, "issue_tracker_name": "Разработка"},
                {"spent_on": "2026-04-13", "hours": 20.0, "issue_tracker_name": "Ошибка"},
                {"spent_on": "2026-04-13", "hours": 20.0, "issue_tracker_name": "Процессы разработки"},
                {"spent_on": "2026-04-13", "hours": 40.0, "issue_tracker_name": "Поддержка"},
                {"spent_on": "2026-04-13", "hours": 40.0, "issue_tracker_name": ""},
            ]
        },
    )

    rows = app_module.buildSnapshotWeeklyDeveloperLoad(10, "2026-04-13", weeks=1)

    assert rows[0]["hours"] == pytest.approx(80.0)
    assert rows[0]["developers"] == pytest.approx(2.0)


def testDashboardWeeklyDeveloperLoadUsesDevelopmentTrackersOnly(monkeypatch) -> None:
    def fakeTimeEntries(projectRedmineId, *args, **kwargs):
        return {
            "time_entries": [
                {"spent_on": "2026-04-13", "hours": 40.0, "issue_tracker_name": "Разработка"},
                {"spent_on": "2026-04-13", "hours": 20.0, "issue_tracker_name": "Ошибка"},
                {"spent_on": "2026-04-13", "hours": 20.0, "issue_tracker_name": "Процессы разработки"},
                {"spent_on": "2026-04-13", "hours": 40.0, "issue_tracker_name": "Поддержка"},
            ]
        }

    monkeypatch.setattr(app_module, "getSnapshotTimeEntriesForProjectByDateRange", fakeTimeEntries)

    rows = app_module.buildDashboardWeeklyDeveloperLoad([10, 20], "2026-04-13", weeks=1)

    assert rows[0]["hours"] == pytest.approx(160.0)
    assert rows[0]["developers"] == pytest.approx(4.0)
    assert rows[0]["project_redmine_ids"] == [10, 20]
    assert rows[0]["captured_for_date"] == "2026-04-13"


def testProjectsSummaryGroupsIncludeSnapshotForecastAndRemaining(monkeypatch) -> None:
    calls = []

    def fakeLoadMetrics(projectRedmineId, reportDate, projectIdentifier):
        calls.append((projectRedmineId, reportDate, projectIdentifier))
        return {
            "development_forecast_year_hours": 82.0,
            "development_forecast_minus_fact_year_hours": 72.0,
            "development_remaining_hours": 36.5,
            "snapshot_date": "2026-05-26",
            "use_risk_plan": True,
        }

    monkeypatch.setattr(app_module, "_loadProjectsSummarySnapshotDevelopmentMetrics", fakeLoadMetrics)

    groups = app_module._buildProjectsSummaryGroups(
        [
            {
                "id": 7,
                "redmine_identifier": "billing",
                "project_redmine_id": 10,
                "project_name": "Billing",
                "development_spent_hours_year": 10.0,
                "report_year_hours": 70.0,
            }
        ],
        date(2026, 5, 26),
    )

    assert calls == [(10, date(2026, 5, 26), "billing")]
    assert groups[0]["development_forecast_year_hours"] == pytest.approx(82.0)
    assert groups[0]["development_forecast_remaining_hours"] == pytest.approx(72.0)
    assert groups[0]["development_remaining_hours"] == pytest.approx(36.5)
    assert groups[0]["development_imbalance_hours"] == pytest.approx(12.0)
    assert groups[0]["snapshot_metrics_date"] == "2026-05-26"
    assert groups[0]["snapshot_metrics_use_risk_plan"] is True


def testReadBitrixPageReturnsHtmlPage() -> None:
    response = readBitrixPage()
    body = response.body.decode("utf-8")

    assert response.media_type == "text/html"
    assert "Анализ сделок Bitrix" in body
    assert "Анализ изменений по сделкам Bitrix за интервалы времени. Формирование отчетности" in body
    assert "Получить срез по сделкам, лидам, счетам" in body
    assert "Получить срез по сделкам" in body
    assert "Удалить выбранный срез" in body
    assert "Показать сделки" in body
    assert "Вернуться на главную" not in body
    assert "Открыть главную страницу" not in body
    assert body.index('href="/Bitrix/deal-snapshots/compare"') < body.index("<h2>Срезы сделок</h2>")
    assert body.index('href="/Bitrix/leads"') < body.index("<h2>Срезы сделок</h2>")
    assert body.index('href="/Bitrix/invoices"') < body.index("<h2>Срезы сделок</h2>")
    assert "до 500 строк" in body
    assert "до 1000 строк" not in body
    assert "Выгрузить в Excel" in body
    assert "/api/bitrix/snapshots/capture/start" in body
    assert 'startParams.set("entities", entityKeys.join(","))' in body
    assert "/api/bitrix/snapshots/capture/page" in body
    assert "is-capture-deal" in body
    assert "is-capture-lead" in body
    assert "is-capture-invoice" in body
    assert "/api/bitrix/deal-snapshots?limit=500" in body
    assert "/api/bitrix/responsibles?limit=1000" in body
    assert "Скачиваю все сделки из Bitrix24" not in body
    assert 'href="/Bitrix/leads"' in body
    assert 'href="/Bitrix/invoices"' in body
    assert 'data-bitrix-filter="company_name"' in body
    assert '<select data-bitrix-filter="stage_name">' in body
    assert '<select data-bitrix-filter="assigned_by_name">' in body
    assert '<input data-bitrix-filter="company_name">' in body
    assert '<select data-bitrix-filter="company_name">' not in body
    assert '<select data-bitrix-filter="category_name">' in body
    assert 'placeholder="Фильтр"' not in body
    assert '<option value="">Фильтр</option>' not in body
    assert "/api/bitrix/deal-snapshots/filter-options" in body
    assert "table-layout: fixed" in body
    assert "width: calc(100vw - 40px)" in body
    assert "overflow-x: auto" in body
    assert "overflow-y: visible" in body
    assert "position: relative" in body
    assert "viewport-sticky-table-header" in body
    assert 'setupViewportStickyTableHeader(".snapshot-table-wrap", ".snapshot-table")' in body
    assert "translateX(${-wrapper.scrollLeft}px)" in body
    assert "min-width: 190ch" in body
    assert "width: 50ch" in body
    assert "deal-col-fixed" in body
    assert "min-width: 1040px" not in body
    assert "button-muted" in body
    assert 'data-bitrix-filter="currency_id"' not in body
    assert 'font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif' in body
    assert "font-weight: 400" in body
    assert "width: calc(100vw - 40px)" in body
    assert "margin-left: calc(50% - 50vw + 20px)" in body
    assert "padding: 42px 24px 38px" in body


def testReadBitrixPageMasksCredential(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "bitrixCredential", "123/secret-webhook-code")

    body = readBitrixPage().body.decode("utf-8")

    assert "123/secret-webhook-code" not in body


def testBitrixPagesRequireFinanceOrAdmin(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(
        app_module,
        "_getCurrentUser",
        lambda request: {
            "login": "regular@example.com",
            "roles": ["User"],
            "must_change_password": False,
        },
    )

    pageResponse = client.get("/Bitrix")
    apiResponse = client.get("/api/bitrix/profile")

    assert pageResponse.status_code == 403
    assert "Admin или Finance" in pageResponse.text
    assert apiResponse.status_code == 403
    assert apiResponse.json()["detail"] == "Bitrix access requires Admin or Finance role"


def testBitrixPagesAllowFinance(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(
        app_module,
        "_getCurrentUser",
        lambda request: {
            "login": "finance@example.com",
            "roles": ["Finance"],
            "must_change_password": False,
        },
    )

    response = client.get("/Bitrix")

    assert response.status_code == 200
    assert "Bitrix" in response.text


def testReadBitrixDealSnapshotComparePageReturnsHtmlPage() -> None:
    body = readBitrixDealSnapshotComparePage().body.decode("utf-8")

    assert "Сравнение срезов сделок" in body
    assert "/api/bitrix/deal-snapshots/compare" in body
    assert 'class="bitrix-top-nav"' in body
    assert 'class="button bitrix-nav-button bitrix-nav-deals" href="/Bitrix">Сделки' in body
    assert 'class="button bitrix-nav-button bitrix-nav-leads" href="/Bitrix/leads">Лиды' in body
    assert 'class="button bitrix-nav-button bitrix-nav-invoices" href="/Bitrix/invoices">Счета' in body
    assert "К форме сделок" not in body
    assert "changed-cell" in body
    assert "buildCompareCell" in body
    assert "comparePageSizeInput" in body
    assert 'data-compare-filter="company"' in body
    assert 'data-compare-sort="company"' in body
    assert '<select class="compare-filter" data-compare-filter="change_type">' in body
    assert '<select class="compare-filter" data-compare-filter="stage">' in body
    assert '<select class="compare-filter" data-compare-filter="category">' in body
    assert 'data-compare-column-toggle="title" checked' in body
    assert 'data-compare-column-toggle="stage" checked' in body
    assert 'data-compare-column-toggle="opportunity" checked' in body
    assert "isCompareRowRelevant" in body
    assert "Поля для сравнения и отображения" in body
    assert 'placeholder="Фильтр"' not in body
    assert "thead { position: sticky; top: 0" in body
    assert "compare-col-id { width: 8ch" in body
    assert "compare-filter { width: 100%; min-width: 0" in body
    assert "viewport-sticky-table-header" in body
    assert 'setupViewportStickyTableHeader(".table-wrap", "table")' in body
    assert 'data-compare-sort="updated_time"' not in body
    assert 'data-compare-filter="updated_time"' not in body
    assert 'colspan="${2 + getSelectedCompareFields().length}"' in body
    assert 'colspan="8"' not in body
    assert '<a class="brand" href="/"' in body
    assert "smsit_template/images/logo.svg" in body
    assert 'font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif' in body
    assert "font-weight: 400" in body
    assert "compare-col-title" in body
    assert "width: 178ch; min-width: 178ch; max-width: 178ch" in body
    assert "table-layout: fixed" in body
    assert "th:first-child, td:first-child { padding-left: 6px" in body
    assert 'compare-filter[data-compare-filter="deal_id"]' in body
    assert "buildChangedCompareContent" in body
    assert "compare-old-value" in body
    assert ".compare-old-value { color: #aeb8c2; font-weight: 400" in body
    assert "color: #aeb8c2" in body
    assert "text-decoration: line-through" in body
    assert "&rarr;" in body
    assert "<th>Валюта</th>" not in body


def testReadBitrixLeadSnapshotComparePageReturnsHtmlPage() -> None:
    body = readBitrixLeadSnapshotComparePage().body.decode("utf-8")

    assert "Сравнение срезов лидов" in body
    assert "/api/bitrix/lead-snapshots/compare" in body
    assert "/crm/lead/details/" in body
    assert 'class="bitrix-top-nav"' in body
    assert 'class="button bitrix-nav-button bitrix-nav-deals" href="/Bitrix">Сделки' in body
    assert 'class="button bitrix-nav-button bitrix-nav-leads" href="/Bitrix/leads">Лиды' in body
    assert 'class="button bitrix-nav-button bitrix-nav-invoices" href="/Bitrix/invoices">Счета' in body
    assert "К форме лидов" not in body
    assert "changed-cell" in body
    assert "comparePageSizeInput" in body
    assert 'data-compare-column-toggle="title" checked' in body
    assert 'setupViewportStickyTableHeader(".table-wrap", "table")' in body


def testReadBitrixInvoicesPageReturnsInvoiceColumns() -> None:
    body = readBitrixInvoicesPage().body.decode("utf-8")

    assert "Счета Bitrix" in body
    assert "/api/bitrix/invoice-snapshots" in body
    assert 'class="bitrix-top-nav"' in body
    assert 'class="button bitrix-nav-button bitrix-nav-deals" href="/Bitrix">Сделки' in body
    assert 'class="button bitrix-nav-button bitrix-nav-leads" href="/Bitrix/leads">Лиды' in body
    assert 'class="button bitrix-nav-button bitrix-nav-invoices" href="/Bitrix/invoices">Счета' in body
    assert '<a class="brand" href="/"' in body
    assert "smsit_template/images/logo.svg" in body
    assert 'font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif' in body
    assert "font-weight: 400" in body
    assert "crm-table-invoice" in body
    assert ".crm-table-invoice { --crm-table-width: 418ch" in body
    assert "crm-col-id { width: 8ch" in body
    assert "crm-col-title { width: 50ch" in body
    assert "crm-col-deal { width: 50ch" in body
    assert "crm-col-responsible { width: 20ch" in body
    assert "crm-col-begin-date { width: 20ch" in body
    assert 'href="/Bitrix/invoices/summary"' in body
    assert "Воронка/стадия/счет" in body
    assert '<select data-filter="pipeline_stage_invoice">' in body
    assert 'data-filter="pipeline_stage_invoice"' in body
    assert "Сделка" in body
    assert 'data-filter="deal_id"' in body
    assert "item.deal_title" in body
    assert "Группа стадий" not in body
    assert "Стадия" in body
    assert '<select data-filter="invoice_stage">' in body
    assert "Дата выставления" in body
    assert 'data-filter="begin_date"' in body
    assert "Срок оплаты" in body
    assert 'data-filter="close_date"' in body
    assert "КОТ ПРОДУКТЫ" in body
    assert 'data-filter="kot_products"' in body
    assert "Продукты" in body
    assert 'data-filter="products"' in body
    assert "Продукты (энергетика)" in body
    assert 'data-filter="energy_products"' in body
    assert "Продукт (для отчета)" in body
    assert 'data-filter="product"' in body
    assert '<select data-filter="status_name">' in body
    assert '<select data-filter="assigned_by_name">' in body
    assert '<select data-filter="kot_products">' in body
    assert '<select data-filter="products">' in body
    assert '<select data-filter="energy_products">' in body
    assert '<select data-filter="product">' in body
    assert "Получить срез по счетам" in body
    assert "Экспорт в Excel" in body
    assert 'id="exportButton"' in body
    assert "/api/bitrix/invoice-snapshots/export" in body
    assert "buildExportParams" in body
    assert body.index('id="reloadButton"') < body.index('id="captureSnapshotButton"')
    assert "entities=${encodeURIComponent(entityKey)}" in body
    assert "overflow-x: auto; overflow-y: visible; position: relative" in body
    assert "th input, th select { width: 100%; min-width: 0" in body
    assert 'th input[data-filter="item_id"]' in body
    assert "viewport-sticky-table-header" in body
    assert 'setupViewportStickyTableHeader(".table-wrap", "table")' in body
    assert "buildPipelineStageInvoice" not in body
    assert 'placeholder="Фильтр"' not in body
    assert '<option value="">Фильтр</option>' not in body
    assert 'colspan="17"' in body


def testReadBitrixInvoiceSummaryPageReturnsHtmlPage() -> None:
    body = readBitrixInvoiceSummaryPage().body.decode("utf-8")

    assert "Сводный отчет по счетам" in body
    assert "/api/bitrix/invoice-snapshots/summary" in body
    assert 'class="bitrix-top-nav"' in body
    assert 'class="button bitrix-nav-button bitrix-nav-deals" href="/Bitrix">Сделки' in body
    assert 'class="button bitrix-nav-button bitrix-nav-leads" href="/Bitrix/leads">Лиды' in body
    assert 'class="button bitrix-nav-button bitrix-nav-invoices" href="/Bitrix/invoices">Счета' in body
    assert "Счета Bitrix</a>" not in body
    assert '<a class="brand" href="/"' in body
    assert "smsit_template/images/logo.svg" in body
    assert 'font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif' in body
    assert "font-weight: 400" in body
    assert "--ink: #16324a" in body
    assert "--muted: #64798d" in body
    assert "summaryYearInput" in body
    assert "summaryPipelineStageSelect" in body
    assert "summaryDateFieldSelect" in body
    assert "summaryReportSnapshotSelect" in body
    assert "summaryCompareSnapshotSelect" in body
    assert "Даты счетов" in body
    assert "Срез для отчета" in body
    assert "Срез для сравнения" in body
    assert "Дата выставления" in body
    assert "Срок оплаты" in body
    assert "Продукт (для отчета)" in body
    assert "Сумма за год" in body
    assert "pipeline_stage_invoice" in body
    assert "/api/bitrix/invoice-snapshots/summary/export.csv" in body
    assert "summaryExportButton" in body
    assert "groupRowsByProduct" in body
    assert "mergeSummaryRows" in body
    assert "renderComparedAmountCell" in body
    assert "toggle-button" in body
    assert "summary-cell-moved-from" in body
    assert "summary-cell-moved-to" in body
    assert "summary-cell-added" in body
    assert "summary-cell-changed" in body
    assert "summary-cell-removed" in body
    assert "previous-amount" in body
    assert ".summary-cell-added { background: #52cee6" in body
    assert ".summary-cell-changed { background: #d9fbff" in body
    assert ".summary-cell-moved-from { background: #fff8c9; }" in body
    assert "color: #aeb8c2" in body
    assert "display: block" in body
    assert "font-weight: 400" in body
    assert "text-decoration-thickness: 2px" in body
    assert "summary-cell-changed" in body
    assert "currentAmount > 0 && previousAmount > 0" in body
    assert "mergeSummaryCellClass" not in body
    assert "group.monthClasses" not in body
    assert "group.yearTotalClass" not in body
    assert "renderAmountCells(group.months, group.compareMonths, group.year_total, group.compareYearTotal)" in body
    assert "background: #e9edf1" not in body
    assert "filter-stack" in body
    assert "filter-card" in body
    assert "filter-row" in body
    assert "filter-actions" in body
    assert ".filter-stack { display: flex" in body
    assert "border: 0" in body
    assert "background: transparent" in body
    assert "hierarchy-col { width: 36ch" in body
    assert "col.month-col, col.total-col { width: 15ch" in body
    assert "min-width: 231ch" in body
    assert "th:first-child, td:first-child" in body
    assert "main { max-width: 1440px; min-height: 100vh" in body
    assert "display: flex; flex-direction: column" in body
    assert "overflow-x: auto; overflow-y: visible; position: relative" in body
    assert "thead { position: sticky; top: 0; z-index: 20; }" in body
    assert "tfoot { position: sticky; bottom: 0; z-index: 20; }" in body
    assert "tfoot td { background: #ffffff" in body
    assert "viewport-sticky-table-header" in body
    assert 'setupViewportStickyTableHeader(".table-wrap", "table")' in body
    assert "viewport-sticky-table-footer" in body
    assert 'setupViewportStickyTableFooter(".table-wrap", "table")' in body
    assert "const footerObserver = new MutationObserver" in body
    assert "background: #fff8d7" not in body
    assert "highlight-legend" in body
    assert "legend-swatch moved-from" in body
    assert "legend-swatch moved-to" in body
    assert "legend-swatch added" in body
    assert "legend-swatch removed" in body
    assert "function isComparisonActive()" in body
    assert "return Boolean(currentComparePayload)" in body
    assert ".filter-actions { align-self: flex-end" in body
    assert "/api/bitrix/invoice-snapshots/items" in body
    assert "compareSnapshotSelect.value === reportSnapshotSelect.value" in body
    assert "comparePayload = payload" in body
    assert "previousValue === null || previousValue === undefined ? 0" in body
    assert "previousTotal === null || previousTotal === undefined ? 0" in body
    assert "const totalMonthCells = renderAmountCells(totals.months, totals.months, totals.year_total, totals.year_total);" in body
    assert "renderComparedAmountCell(totals.year_total, totals.year_total, totals.year_total, totals.year_total)" in body
    assert "yearInput.addEventListener(\"change\", clearSummaryTable)" in body
    assert "dateFieldSelect.addEventListener(\"change\", clearSummaryTable)" in body
    assert "pipelineStageSelect.addEventListener(\"change\", clearSummaryTable)" in body
    assert "colspan=\"14\"" in body


def testReadBitrixLeadsPageReturnsDropdownFiltersWithoutPlaceholder() -> None:
    body = readBitrixLeadsPage().body.decode("utf-8")

    assert "Лиды Bitrix" in body
    assert "Получить срез по лидам" in body
    assert 'href="/Bitrix/leads/compare"' in body
    assert "Сравнить срезы лидов" in body
    assert 'class="bitrix-top-nav"' in body
    assert 'class="button bitrix-nav-button bitrix-nav-deals" href="/Bitrix">Сделки' in body
    assert 'class="button bitrix-nav-button bitrix-nav-leads" href="/Bitrix/leads">Лиды' in body
    assert 'class="button bitrix-nav-button bitrix-nav-invoices" href="/Bitrix/invoices">Счета' in body
    assert '<a class="brand" href="/"' in body
    assert "smsit_template/images/logo.svg" in body
    assert 'font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif' in body
    assert "font-weight: 400" in body
    assert body.index('id="reloadButton"') < body.index('id="captureSnapshotButton"')
    assert '<select data-filter="status_name">' in body
    assert '<select data-filter="assigned_by_name">' in body
    assert ".crm-table-standard { --crm-table-width: 170ch" in body
    assert "crm-col-id { width: 8ch" in body
    assert "overflow-x: auto; overflow-y: visible; position: relative" in body
    assert "viewport-sticky-table-header" in body
    assert 'setupViewportStickyTableHeader(".table-wrap", "table")' in body
    assert 'placeholder="Фильтр"' not in body
    assert '<option value="">Фильтр</option>' not in body


def testResolveBitrixInvoiceSelectFieldsAddsNamedFields(monkeypatch) -> None:
    bitrix_client_module._BITRIX_CRM_FIELD_CACHE.clear()

    def fakeCallBitrixRestMethod(*args, **kwargs) -> dict[str, object]:
        return {
            "result": {
                "fields": {
                    "ufCrmKotProducts": {"title": "КОТ ПРОДУКТЫ"},
                    "ufCrmProducts": {
                        "formLabel": "Продукты",
                        "items": [{"ID": "10", "VALUE": "Сервис"}],
                    },
                    "ufCrmEnergyProducts": {"listLabel": "Продукты (энергетика)"},
                    "ufCrmStageGroup": {
                        "title": "Группа стадий",
                        "items": [{"id": "20", "value": "КОТ"}],
                    },
                    "ufCrmPipelineStageInvoice": {"formLabel": "ВОРОНКА/СТАДИЯ/СЧЕТ"},
                }
            }
        }

    monkeypatch.setattr(bitrix_client_module, "callBitrixRestMethod", fakeCallBitrixRestMethod)

    selectFields, extraFieldInfo = bitrix_client_module.buildBitrixInvoiceSelectFields(
        "https://sms-it.bitrix24.ru",
        "1/test-webhook",
    )

    assert "ufCrmKotProducts" in selectFields
    assert "ufCrmProducts" in selectFields
    assert "ufCrmEnergyProducts" in selectFields
    assert "parentId2" in selectFields
    assert "ufCrmStageGroup" in selectFields
    assert "ufCrmPipelineStageInvoice" in selectFields
    assert extraFieldInfo["invoice_extra_field_names"]["kot_products"] == "ufCrmKotProducts"
    assert extraFieldInfo["invoice_extra_field_names"]["products"] == "ufCrmProducts"
    assert extraFieldInfo["invoice_extra_field_names"]["stage_group"] == "ufCrmStageGroup"
    assert extraFieldInfo["invoice_extra_field_names"]["pipeline_stage_invoice"] == "ufCrmPipelineStageInvoice"
    assert extraFieldInfo["invoice_extra_field_value_maps"]["products"]["10"] == "Сервис"
    assert extraFieldInfo["invoice_extra_field_value_maps"]["stage_group"]["20"] == "КОТ"


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


def testFetchBitrixUsersUsesGetRequest(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fakeCallBitrixRestMethodGet(portalUrl, credential, method, payload=None, timeout=45):
        captured.update(
            {
                "portalUrl": portalUrl,
                "credential": credential,
                "method": method,
                "payload": payload,
            }
        )
        return {
            "result": [
                {"ID": "7", "LAST_NAME": "Иванов", "NAME": "Иван", "ACTIVE": True},
            ]
        }

    monkeypatch.setattr("src.redmine.bitrix_client.callBitrixRestMethodGet", fakeCallBitrixRestMethodGet)

    payload = fetchBitrixUsers("https://sms-it.bitrix24.ru", "1/test-webhook")

    assert captured["method"] == "user.get"
    assert captured["payload"] == {}
    assert payload["users"][0]["name"] == "Иванов Иван"


def testCaptureBitrixDealSnapshotEndpointStoresDeals(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "https://sms-it.bitrix24.ru")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "1/test-webhook")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
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
    monkeypatch.setattr(app_module, "refreshBitrixUsersDictionary", lambda: {"upserted": 2})
    monkeypatch.setattr(app_module, "refreshBitrixCompaniesDictionary", lambda: {"upserted": 1})
    monkeypatch.setattr(app_module, "getBitrixUserNamesByIds", lambda userIds: {7: "Иванов Иван"})
    monkeypatch.setattr(app_module, "getBitrixCompanyNamesByIds", lambda companyIds: {100: "ООО Ромашка"})
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
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
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


def testCompareBitrixLeadSnapshotsEndpointReturnsChanges(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
    calls = []
    monkeypatch.setattr(
        app_module,
        "compareBitrixCrmSnapshots",
        lambda entityType, leftDate, rightDate: calls.append((entityType, leftDate, rightDate)) or {
            "left_run": {"captured_for_date": leftDate},
            "right_run": {"captured_for_date": rightDate},
            "changes": [{"deal_id": 701, "item_id": 701, "change_type": "changed"}],
            "available_dates": [rightDate, leftDate],
        },
    )

    response = client.get("/api/bitrix/lead-snapshots/compare?left_date=2026-05-01&right_date=2026-05-06")

    assert response.status_code == 200
    assert calls == [("lead", "2026-05-01", "2026-05-06")]
    assert response.json()["changes"][0]["item_id"] == 701


def testGetBitrixDealSnapshotFilterOptionsEndpointReturnsOptions(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
    monkeypatch.setattr(
        app_module,
        "getBitrixDealSnapshotFilterOptions",
        lambda capturedForDate: {
            "snapshot_run": {"captured_for_date": capturedForDate},
            "options": {"category_name": ["КОТ"], "stage_name": ["Новая"]},
        },
    )

    response = client.get("/api/bitrix/deal-snapshots/filter-options?captured_for_date=2026-05-06")

    assert response.status_code == 200
    assert response.json()["options"]["category_name"] == ["КОТ"]
    assert response.json()["snapshot_run"]["captured_for_date"] == "2026-05-06"


def testBitrixInvoiceSummaryEndpointPassesFilters(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
    captured: dict[str, object] = {}

    def fakeGetBitrixInvoiceSummary(year, *, dateField, capturedForDate, pipelineStages):
        captured["year"] = year
        captured["dateField"] = dateField
        captured["capturedForDate"] = capturedForDate
        captured["pipelineStages"] = pipelineStages
        return {
            "snapshot_run": {"captured_for_date": "2026-05-08"},
            "year": year,
            "date_field": dateField,
            "available_dates": ["2026-05-08", "2026-05-07"],
            "pipeline_stage_options": ["КОТ/Договор", "АСУРЭО/Оплата"],
            "selected_pipeline_stages": pipelineStages,
            "rows": [],
            "totals": {"months": {str(month): 0 for month in range(1, 13)}, "year_total": 0},
        }

    monkeypatch.setattr(app_module, "getBitrixInvoiceSummary", fakeGetBitrixInvoiceSummary)

    response = client.get(
        "/api/bitrix/invoice-snapshots/summary?year=2026&date_field=close_date"
        "&captured_for_date=2026-05-08"
        "&pipeline_stage_invoice=КОТ/Договор"
        "&pipeline_stage_invoice=АСУРЭО/Оплата"
    )

    assert response.status_code == 200
    assert captured["year"] == 2026
    assert captured["dateField"] == "close_date"
    assert captured["capturedForDate"] == "2026-05-08"
    assert captured["pipelineStages"] == ["КОТ/Договор", "АСУРЭО/Оплата"]


def testBitrixInvoiceSummaryExportEndpointReturnsAnsiCsv(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})

    monkeypatch.setattr(
        app_module,
        "getBitrixInvoiceSummary",
        lambda year, *, dateField, capturedForDate, pipelineStages: {
            "snapshot_run": {"captured_for_date": "2026-05-08"},
            "year": year,
            "date_field": dateField,
            "available_dates": ["2026-05-08"],
            "pipeline_stage_options": ["КОТ/Договор"],
            "selected_pipeline_stages": pipelineStages,
            "rows": [
                {
                    "product": "Сервис",
                    "deal_id": 501,
                    "deal_title": "Договор поддержки",
                    "months": {"1": 1000.4, "2": 2000.6},
                    "year_total": 3001.0,
                }
            ],
            "totals": {
                "months": {"1": 1000.4, "2": 2000.6},
                "year_total": 3001.0,
            },
        },
    )

    response = client.get("/api/bitrix/invoice-snapshots/summary/export.csv?year=2026")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=windows-1251"
    assert "bitrix-invoice-summary-2026.csv" in response.headers["content-disposition"]
    body = response.content.decode("cp1251")
    assert "Продукт (для отчета) / Сделка" in body
    assert "Сервис;1000;2001" in body
    assert "  Договор поддержки;1000;2001" in body


def testDeleteBitrixDealSnapshotByDateEndpointDeletesRows(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
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


def testGetBitrixResponsiblesEndpointReturnsUsers(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "https://sms-it.bitrix24.ru")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "1/test-webhook")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
    monkeypatch.setattr(app_module, "upsertBitrixUsers", lambda users: {"upserted": len(users)})
    monkeypatch.setattr(
        app_module,
        "fetchBitrixUsers",
        lambda **kwargs: {
            "auth_mode": "webhook_path",
            "users": [
                {"id": 7, "name": "Иванов Иван", "active": True},
                {"id": 8, "name": "Петров Петр", "active": True},
            ],
            "total": 2,
        },
    )

    response = client.get("/api/bitrix/responsibles?limit=1000")

    assert response.status_code == 200
    assert response.json()["total"] == 2
    assert response.json()["users"][0]["name"] == "Иванов Иван"


def testExportBitrixDealSnapshotEndpointReturnsAnsiCsv(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
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


def testExportBitrixInvoiceSnapshotEndpointReturnsAnsiCsv(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "bitrixPortalUrl", "")
    monkeypatch.setattr(app_module.config, "bitrixCredential", "")
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: {"login": "tester", "roles": ["Admin"], "must_change_password": False})
    captured: dict[str, object] = {}

    def fakeGetBitrixCrmSnapshotItemsApiPayload(entityType, capturedForDate, page, pageSize, filters):
        captured["entityType"] = entityType
        captured["capturedForDate"] = capturedForDate
        captured["pageSize"] = pageSize
        captured["filters"] = filters
        return {
            "snapshot_run": {"captured_for_date": capturedForDate or "2026-05-08"},
            "items": [
                {
                    "item_id": 6453,
                    "deal_id": 35421,
                    "deal_title": "Магнит",
                    "title": "Счет",
                    "opportunity": 991.6,
                    "status_name": "Новый",
                    "assigned_by_name": "Иванов Иван",
                    "company_name": "Компания",
                    "pipeline_stage_invoice": "КОТ/Договор в работе",
                    "invoice_stage": "Договор",
                    "begin_date": "2026-05-01",
                    "close_date": "2026-05-31",
                    "kot_products": "КОТ",
                    "products": "Продукт",
                    "energy_products": "Энергетика",
                    "product": "Продукт для отчета",
                    "created_time": "2026-05-01T10:00:00Z",
                    "updated_time": "2026-05-02T10:00:00Z",
                }
            ] if page == 1 else [],
            "total_count": 1,
        }

    monkeypatch.setattr(app_module, "getBitrixCrmSnapshotItemsApiPayload", fakeGetBitrixCrmSnapshotItemsApiPayload)

    response = client.get(
        "/api/bitrix/invoice-snapshots/export?captured_for_date=2026-05-08"
        "&status_name=Новый&assigned_by_name=Иванов"
    )

    decoded = response.content.decode("cp1251")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=windows-1251"
    assert "bitrix-invoices-2026-05-08.csv" in response.headers["content-disposition"]
    assert captured["entityType"] == "invoice"
    assert captured["capturedForDate"] == "2026-05-08"
    assert captured["pageSize"] == 5000
    assert captured["filters"]["status_name"] == "Новый"
    assert captured["filters"]["assigned_by_name"] == "Иванов"
    assert "Магнит" in decoded
    assert "КОТ/Договор в работе" in decoded
    assert ";992;" in decoded


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
    monkeypatch.setattr(app_module, "ensurePlanningProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "listPlanningProjectIdentifiers", lambda: ["billing"])
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
    monkeypatch.setattr(app_module, "listStoredProjects", lambda: [])
    monkeypatch.setattr(app_module, "listPlanningProjectsByRedmineIdentifier", lambda redmineIdentifier: [])
    monkeypatch.setattr(
        app_module,
        "getSnapshotTimeEntriesForProjectByDateRange",
        lambda *args, **kwargs: {
            "time_entries": [
                {
                    "spent_on": "2026-04-13",
                    "hours": 40.0,
                }
            ]
        },
    )

    def fakeGetFilteredSnapshotIssuesForProjectByDate(projectRedmineId, capturedForDate=None, filters=None, page=1, pageSize=1000):
        return buildSnapshotIssuesPayload(projectRedmineId, "2026-04-13")

    monkeypatch.setattr(app_module, "getFilteredSnapshotIssuesForProjectByDate", fakeGetFilteredSnapshotIssuesForProjectByDate)
    monkeypatch.setattr(app_module, "listFilteredSnapshotIssuesForProjectByDate", fakeGetFilteredSnapshotIssuesForProjectByDate)

    response = client.get("/projects/10/latest-snapshot-issues")

    assert response.status_code == 200
    body = response.text
    assert "Задачи среза проекта" in body
    assert "Billing" in body
    assert "Add chart" in body
    assert "Дата завершения (остаток по заведенным задачам)" in body
    assert "Остаток по заве- денным задачам" in body
    assert "Прогноз - факт" in body
    assert "summaryDevelopmentYearForecastMinusFact" in body
    assert 'class="snapshot-weekly-link"' in body
    assert 'target="_blank"' in body
    assert "/time-entries?project_redmine_id=10&amp;captured_for_date=2026-04-13&amp;date_from=2026-04-13&amp;date_to=2026-04-13" in body


def testGetProjectBurndownPageReturnsChartPage(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "ensurePlanningProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "listPlanningProjectsByRedmineIdentifier", lambda redmineIdentifier: [])
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
    monkeypatch.setattr(
        app_module,
        "getSnapshotRunsWithIssuesForProjectDateRange",
        lambda projectRedmineId, dateFrom, dateTo: {
            "project": {
                "project_redmine_id": projectRedmineId,
                "project_name": "Billing",
                "project_identifier": "billing",
            },
            "snapshot_runs": [
                {
                    "id": 1,
                    "captured_for_date": "2026-04-13",
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
    assert "Диаграмма сгорания" in response.text
    assert "Billing" in response.text
    assert "P1 = факт / база" in response.text
    assert "Объем.Прогноз" in response.text
    assert "P2/100 - 1" in response.text


def testSnapshotIssuesPageUsesCleanRussianText(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "listStoredProjects", lambda: [])
    monkeypatch.setattr(app_module, "listPlanningProjectsByRedmineIdentifier", lambda redmineIdentifier: [])
    monkeypatch.setattr(app_module, "getSnapshotTimeEntriesForProjectByDateRange", lambda *args, **kwargs: {"time_entries": []})

    def fakeGetFilteredSnapshotIssuesForProjectByDate(projectRedmineId, capturedForDate=None, filters=None, page=1, pageSize=1000):
        return buildSnapshotIssuesPayload(projectRedmineId, "2026-04-14")

    monkeypatch.setattr(app_module, "getFilteredSnapshotIssuesForProjectByDate", fakeGetFilteredSnapshotIssuesForProjectByDate)
    monkeypatch.setattr(app_module, "listFilteredSnapshotIssuesForProjectByDate", fakeGetFilteredSnapshotIssuesForProjectByDate)

    response = client.get("/projects/10/latest-snapshot-issues")

    assert response.status_code == 200
    assert "Базовая оценка, ч" in response.text
    assert "Проект в Redmine:" in response.text
    assert "Billing" in response.text


def testSnapshotTimeEntriesPageDefaultsToDevelopmentTrackersAndLinksIssueIds(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.sms-it.ru")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listStoredProjects",
        lambda: [{"redmine_id": 10, "name": "Billing", "identifier": "billing"}],
    )
    monkeypatch.setattr(
        app_module,
        "getSnapshotTimeEntriesForProjectByDateRange",
        lambda *args, **kwargs: {
            "snapshot_run": {
                "project_redmine_id": 10,
                "project_name": "Billing",
                "project_identifier": "billing",
                "captured_for_date": "2026-04-13",
            },
            "time_entries": [
                {
                    "id": 1,
                    "issue_redmine_id": 325869,
                    "issue_tracker_name": "Разработка",
                    "issue_status_name": "В работе",
                    "activity_name": "Development",
                    "hours": 40.0,
                    "spent_on": "2026-04-13",
                },
                {
                    "id": 2,
                    "issue_redmine_id": 325870,
                    "issue_tracker_name": "Ошибка",
                    "issue_status_name": "В работе",
                    "activity_name": "Development",
                    "hours": 20.0,
                    "spent_on": "2026-04-13",
                },
                {
                    "id": 3,
                    "issue_redmine_id": 325871,
                    "issue_tracker_name": "Процессы разработки",
                    "issue_status_name": "В работе",
                    "activity_name": "Development",
                    "hours": 20.0,
                    "spent_on": "2026-04-13",
                },
                {
                    "id": 4,
                    "issue_redmine_id": 325872,
                    "issue_tracker_name": "Поддержка",
                    "issue_status_name": "В работе",
                    "activity_name": "Support",
                    "hours": 40.0,
                    "spent_on": "2026-04-13",
                },
            ],
            "available_dates": ["2026-04-13"],
        },
    )

    redirectResponse = client.get(
        "/projects/10/time-entries?captured_for_date=2026-04-13&date_from=2026-04-13&date_to=2026-04-13",
        follow_redirects=False,
    )

    assert redirectResponse.status_code == 303
    assert redirectResponse.headers["location"] == "/time-entries?project_redmine_id=10&captured_for_date=2026-04-13&date_from=2026-04-13&date_to=2026-04-13"

    response = client.get("/time-entries?project_redmine_id=10&captured_for_date=2026-04-13&date_from=2026-04-13&date_to=2026-04-13")

    assert response.status_code == 200
    body = response.text
    assert 'value="Разработка" selected' in body
    assert 'value="Ошибка" selected' in body
    assert 'value="Процессы разработки" selected' in body
    assert 'value="Поддержка" selected' not in body
    assert 'id="timeEntriesVisibleCount">3</span>' in body
    assert 'id="timeEntriesHoursSummary">80,0</span>' in body
    assert 'const redmineIssueUrlBase = "https://redmine.sms-it.ru/issues/";' in body
    assert "time-entry-redmine-link" in body


def testGroupedSnapshotTimeEntriesPageShowsDevelopmentEntriesForDashboard(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.sms-it.ru")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listStoredProjects",
        lambda: [
            {"redmine_id": 10, "name": "Billing", "identifier": "billing"},
            {"redmine_id": 20, "name": "Support", "identifier": "support"},
        ],
    )

    def fakeTimeEntries(projectRedmineId, *args, **kwargs):
        return {
            "snapshot_run": {
                "project_redmine_id": projectRedmineId,
                "project_name": "Billing" if projectRedmineId == 10 else "Support",
                "project_identifier": "billing" if projectRedmineId == 10 else "support",
                "captured_for_date": "2026-04-13",
            },
            "time_entries": [
                {
                    "id": projectRedmineId,
                    "project_redmine_id": projectRedmineId,
                    "project_name": "Billing" if projectRedmineId == 10 else "Support",
                    "issue_redmine_id": 325000 + projectRedmineId,
                    "issue_tracker_name": "Разработка",
                    "issue_status_name": "В работе",
                    "activity_name": "Development",
                    "hours": 40.0,
                    "spent_on": "2026-04-13",
                },
                {
                    "id": projectRedmineId + 100,
                    "project_redmine_id": projectRedmineId,
                    "project_name": "Billing" if projectRedmineId == 10 else "Support",
                    "issue_redmine_id": 326000 + projectRedmineId,
                    "issue_tracker_name": "Поддержка",
                    "issue_status_name": "В работе",
                    "activity_name": "Support",
                    "hours": 40.0,
                    "spent_on": "2026-04-13",
                },
            ],
            "available_dates": ["2026-04-13"],
        }

    monkeypatch.setattr(app_module, "getSnapshotTimeEntriesForProjectByDateRange", fakeTimeEntries)

    response = client.get(
        "/time-entries?project_redmine_id=10&project_redmine_id=20&captured_for_date=2026-04-13&date_from=2026-04-13&date_to=2026-04-13"
    )

    assert response.status_code == 200
    body = response.text
    assert "Проекты: Billing, Support" in body
    assert 'id="timeEntriesVisibleCount">2</span>' in body
    assert 'id="timeEntriesHoursSummary">80,0</span>' in body
    assert "Выгрузить Excel" in body
    assert "Сбросить фильтр" in body
    assert 'value="Разработка" selected' in body
    assert "https://redmine.sms-it.ru/issues/325010" in body
    assert "https://redmine.sms-it.ru/issues/325020" in body
    assert "https://redmine.sms-it.ru/issues/326010" not in body
    assert "https://redmine.sms-it.ru/issues/326020" not in body


def testSnapshotTimeEntriesExcelExportAppliesFilters(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(
        app_module,
        "getSnapshotTimeEntriesForProjectByDateRange",
        lambda *args, **kwargs: {
            "snapshot_run": {
                "project_redmine_id": 10,
                "project_identifier": "billing",
                "captured_for_date": "2026-04-13",
            },
            "time_entries": [
                {
                    "id": 1,
                    "issue_redmine_id": 325869,
                    "issue_tracker_name": "Разработка",
                    "hours": 2.5,
                    "spent_on": "2026-04-13",
                },
                {
                    "id": 2,
                    "issue_redmine_id": 325870,
                    "issue_tracker_name": "Поддержка",
                    "hours": 7.5,
                    "spent_on": "2026-04-13",
                },
            ],
        },
    )

    response = client.get(
        "/projects/10/time-entries/export.xls?captured_for_date=2026-04-13&date_from=2026-04-13&date_to=2026-04-13&issue_tracker_name=Разработка"
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.ms-excel; charset=windows-1251"
    body = response.content.decode("cp1251")
    assert "325869" in body
    assert "325870" not in body
    assert "2,5" in body


def testGroupedSnapshotTimeEntriesExcelExportAppliesFilters(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listStoredProjects",
        lambda: [
            {"redmine_id": 10, "name": "Billing", "identifier": "billing"},
            {"redmine_id": 20, "name": "Support", "identifier": "support"},
        ],
    )

    def fakeTimeEntries(projectRedmineId, *args, **kwargs):
        return {
            "snapshot_run": {
                "project_redmine_id": projectRedmineId,
                "project_identifier": "billing" if projectRedmineId == 10 else "support",
                "captured_for_date": "2026-04-13",
            },
            "time_entries": [
                {
                    "id": projectRedmineId,
                    "project_redmine_id": projectRedmineId,
                    "issue_redmine_id": 325000 + projectRedmineId,
                    "issue_tracker_name": "Ошибка",
                    "hours": 4.0,
                    "spent_on": "2026-04-13",
                },
                {
                    "id": projectRedmineId + 100,
                    "project_redmine_id": projectRedmineId,
                    "issue_redmine_id": 326000 + projectRedmineId,
                    "issue_tracker_name": "Поддержка",
                    "hours": 8.0,
                    "spent_on": "2026-04-13",
                },
            ],
        }

    monkeypatch.setattr(app_module, "getSnapshotTimeEntriesForProjectByDateRange", fakeTimeEntries)

    response = client.get(
        "/time-entries/export.xls?project_redmine_id=10&project_redmine_id=20&captured_for_date=2026-04-13&date_from=2026-04-13&date_to=2026-04-13&issue_tracker_name=Ошибка"
    )

    assert response.status_code == 200
    body = response.content.decode("cp1251")
    assert "325010" in body
    assert "325020" in body
    assert "326010" not in body
    assert "326020" not in body
    assert "8,0" in body


def testAdminUsersPageShowsDashboardButtonForConfiguredUser(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(
        app_module,
        "listUsers",
        lambda: [
            {
                "id": 1,
                "login": "andrey.shopin@sms-a.ru",
                "roles": "Admin,User",
                "must_change_password": False,
            },
            {
                "id": 2,
                "login": "no-dashboard@example.com",
                "roles": "User",
                "must_change_password": False,
            },
        ],
    )

    response = client.get("/admin/users")

    assert response.status_code == 200
    assert "Dashboard" in response.text
    assert "/dashboards/andrey.shopin%40sms-a.ru" in response.text
    assert "/dashboards/no-dashboard%40example.com" not in response.text


def testDashboardProjectStateApiReturnsBurndownWidgetPayload(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "ensurePlanningProjectsTable", lambda: None)
    monkeypatch.setattr(
        app_module,
        "listStoredProjects",
        lambda: [
            {
                "redmine_id": 10,
                "name": "Billing",
                "identifier": "billing",
                "is_enabled": True,
                "is_disabled": False,
            }
        ],
    )
    monkeypatch.setattr(
        app_module,
        "listPlanningProjectsByRedmineIdentifier",
        lambda identifier: [
            {
                "id": 7,
                "customer": "ACME",
                "project_name": "Billing roadmap",
                "redmine_identifier": identifier,
                "start_date": "2026-04-01",
                "development_hours": 120.0,
                "baseline_estimate_hours": 80.0,
                "p1": 1.2,
                "p2": 1.3,
                "use_risk_plan": True,
            }
        ],
    )
    monkeypatch.setattr(
        app_module,
        "getSnapshotRunsWithIssuesForProjectDateRange",
        lambda *args, **kwargs: {
            "project": {
                "project_redmine_id": 10,
                "project_name": "Billing",
                "project_identifier": "billing",
            },
            "snapshot_runs": [
                {
                    "id": 1,
                    "project_redmine_id": 10,
                    "project_name": "Billing",
                    "project_identifier": "billing",
                    "captured_for_date": "2026-04-18",
                    "issues": [],
                }
            ],
        },
    )

    response = client.get(
        "/api/dashboards/andrey.shopin%40sms-a.ru/project-state?project_redmine_ids=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_project_ids"] == [10]
    assert payload["planning"]["p1_percent"] == 120.0
    assert payload["planning"]["p2_percent"] == 130.0
    assert payload["planning"]["use_risk_plan"] is True
    assert payload["snapshot_count"] == 1
    assert payload["planning_projects"][0]["name"] == "ACME - Billing roadmap"


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
    monkeypatch.setattr(app_module, "syncProjects", lambda projects: {"added_count": 2, "updated_count": 0})
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
    monkeypatch.setattr(app_module, "countIssueSnapshotRuns", lambda: 1)
    monkeypatch.setattr(
        app_module,
        "listRecentIssueSnapshotRuns",
        lambda limit=None: [{"project_name": "Alpha", "total_issues": 5, "total_spent_hours_year": 2.0}],
    )

    response = client.get("/api/issues/snapshots/runs")

    assert response.status_code == 200
    assert response.json()["snapshot_runs"][0]["project_name"] == "Alpha"


def testDeleteIssueSnapshotsByDateEndpointDeletesRows(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "countIssueSnapshotRuns", lambda: 1)
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
        lambda limit=None: [{"id": 200, "project_name": "Beta", "total_issues": 10, "total_spent_hours_year": 4.0}],
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
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "isIssueSnapshotCaptureRunning", lambda: False)
    monkeypatch.setattr(app_module, "startIssueSnapshotCaptureInBackground", lambda: True)
    monkeypatch.setattr(
        app_module,
        "getIssueSnapshotCaptureStatus",
        lambda: {
            "is_running": True,
            "captured_for_date": "2026-04-11",
            "created_runs": 0,
            "captured_issues": 0,
            "already_captured_projects": 0,
            "remaining_projects": 0,
        },
    )

    response = client.post("/api/issues/snapshots/capture")

    assert response.status_code == 200
    assert response.json()["started"] is True
    assert response.json()["captured_for_date"] == "2026-04-11"


def testCaptureIssueSnapshotsUsesCurrentYearSpentHours(monkeypatch) -> None:
    from src.redmine import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "loadConfig", lambda: app_module.config)
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
        lambda redmineUrl, apiKey, projectIdentifier, projectRedmineId, **kwargs: [
            {"issue_redmine_id": 10, "spent_hours": 99.0, "spent_hours_year": 0.0, "estimated_hours": 1.0}
        ],
    )
    monkeypatch.setattr(
        snapshots_module,
        "fetchAllTimeEntriesForProject",
        lambda redmineUrl, apiKey, projectIdentifier, projectRedmineId, **kwargs: [
            {"issue_redmine_id": 10, "hours": 5.5, "spent_on": "2026-05-10"}
        ],
    )
    created_payloads = []
    monkeypatch.setattr(
        snapshots_module,
        "createIssueSnapshotRun",
        lambda capturedForDate, project, issues, timeEntries=None: created_payloads.append(issues) or 101,
    )
    monkeypatch.setattr(snapshots_module, "listRecentIssueSnapshotRuns", lambda: [])

    result = snapshots_module.captureAllIssueSnapshots()

    assert result["created_runs"] == 1
    assert created_payloads[0][0]["spent_hours"] == 99.0
    assert created_payloads[0][0]["spent_hours_year"] == 5.5


def testCaptureIssueSnapshotsEndpointSkipsForbiddenProject(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "isIssueSnapshotCaptureRunning", lambda: True)
    monkeypatch.setattr(
        app_module,
        "getIssueSnapshotCaptureStatus",
        lambda: {
            "captured_for_date": "2026-04-11",
            "created_runs": 0,
            "captured_issues": 0,
            "already_captured_projects": 0,
            "remaining_projects": 1,
            "is_running": True,
        },
    )

    response = client.post("/api/issues/snapshots/capture")

    assert response.status_code == 200
    assert response.json()["started"] is False
    assert response.json()["captured_for_date"] == "2026-04-11"
    assert response.json()["created_runs"] == 0
    assert response.json()["captured_issues"] == 0
    assert response.json()["remaining_projects"] == 1


def testCaptureIssueSnapshotsEndpointMapsRuntimeErrorToHttp(monkeypatch) -> None:
    monkeypatch.setattr(app_module.config, "databaseUrl", "postgresql://demo")
    monkeypatch.setattr(app_module.config, "redmineUrl", "https://redmine.example.com")
    monkeypatch.setattr(app_module.config, "apiKey", "secret")
    monkeypatch.setattr(app_module, "ensureProjectsTable", lambda: None)
    monkeypatch.setattr(app_module, "ensureIssueSnapshotTables", lambda: None)
    monkeypatch.setattr(app_module, "isIssueSnapshotCaptureRunning", lambda: False)
    monkeypatch.setattr(app_module, "startIssueSnapshotCaptureInBackground", lambda: False)
    monkeypatch.setattr(
        app_module,
        "getIssueSnapshotCaptureStatus",
        lambda: {
            "is_running": False,
            "error_message": "No projects in the database. Refresh projects first.",
        },
    )

    response = client.post("/api/issues/snapshots/capture")

    assert response.status_code == 200
    assert response.json()["started"] is False
    assert "Refresh projects first" in response.json()["error_message"]
