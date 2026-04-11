from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from src.redmine.config import loadConfig
from src.redmine.db import (
    checkDatabaseConnection,
    deleteIssueSnapshotsForDate,
    ensureIssueSnapshotTables,
    ensureProjectsTable,
    listRecentIssueSnapshotRuns,
    listStoredProjects,
    storeMissingProjects,
)
from src.redmine.redmine_client import fetchAllProjectsFromRedmine
from src.redmine.snapshots import captureAllIssueSnapshots


config = loadConfig()
app = FastAPI(title="Redmine Snapshot Viewer")


PAGE_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Redmine: проекты и срезы</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f8fb;
      --panel: #ffffff;
      --panel-soft: #eef6f7;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --accent: #00a99d;
      --accent-2: #1777c8;
      --accent-deep: #0d4b8f;
      --danger: #c84a3e;
      --shadow: 0 18px 40px rgba(22, 50, 74, 0.08);
      --shadow-soft: 0 12px 24px rgba(22, 50, 74, 0.06);
    }

    * {
      box-sizing: border-box;
    }

    html {
      scroll-behavior: smooth;
    }

    body {
      margin: 0;
      font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(0, 169, 157, 0.12), transparent 24%),
        radial-gradient(circle at top right, rgba(23, 119, 200, 0.10), transparent 26%),
        linear-gradient(180deg, #fbfdff 0%, var(--bg) 100%);
      color: var(--text);
    }

    main {
      max-width: 1200px;
      margin: 0 auto;
      padding: 28px 20px 56px;
    }

    .hero {
      position: relative;
      overflow: hidden;
      margin-bottom: 24px;
      padding: 24px;
      border: 1px solid var(--line);
      border-radius: 28px;
      background:
        linear-gradient(140deg, rgba(255, 255, 255, 0.98) 0%, rgba(240, 248, 250, 0.96) 58%, rgba(232, 243, 250, 0.98) 100%);
      box-shadow: var(--shadow);
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: auto -60px -70px auto;
      width: 240px;
      height: 240px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(0, 169, 157, 0.18), rgba(0, 169, 157, 0));
      pointer-events: none;
    }

    .brand-bar {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 20px;
      margin-bottom: 22px;
    }

    .brand {
      display: inline-flex;
      align-items: center;
      gap: 16px;
      text-decoration: none;
      color: inherit;
    }

    .brand-logo-wrap {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 74px;
      height: 74px;
      padding: 14px;
      border-radius: 22px;
      background: #fff;
      border: 1px solid rgba(23, 119, 200, 0.12);
      box-shadow: var(--shadow-soft);
    }

    .brand-logo {
      display: block;
      width: 100%;
      height: auto;
    }

    .brand-copy {
      display: flex;
      flex-direction: column;
      gap: 4px;
      padding-top: 4px;
    }

    .brand-kicker {
      color: var(--accent-deep);
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .brand-title {
      font-size: 1.15rem;
      font-weight: 700;
      line-height: 1.2;
    }

    .brand-subtitle {
      color: var(--muted);
      font-size: 0.95rem;
      max-width: 420px;
      line-height: 1.45;
    }

    .brand-note {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 44px;
      padding: 10px 16px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid var(--line);
      color: var(--accent-deep);
      font-weight: 600;
      text-align: center;
      box-shadow: var(--shadow-soft);
    }

    h1 {
      margin: 0 0 10px;
      font-size: clamp(2rem, 5vw, 3.2rem);
      line-height: 1.03;
      letter-spacing: -0.03em;
    }

    .lead {
      margin: 0 0 28px;
      max-width: 760px;
      color: var(--muted);
      font-size: 1.06rem;
      line-height: 1.6;
    }

    .quick-links {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin: 0 0 24px;
    }

    .quick-links a {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 10px 16px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.94);
      color: var(--text);
      text-decoration: none;
      font-weight: 600;
      box-shadow: var(--shadow-soft);
      transition: transform 120ms ease, border-color 120ms ease;
    }

    .quick-links a:hover {
      transform: translateY(-1px);
      border-color: rgba(23, 119, 200, 0.35);
    }

    .grid {
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      margin-bottom: 22px;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 20px;
      box-shadow: var(--shadow-soft);
    }

    .panel h2 {
      margin: 0 0 8px;
      font-size: 1.15rem;
    }

    .panel p {
      margin: 0 0 16px;
      color: var(--muted);
      line-height: 1.5;
    }

    .row {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      align-items: center;
    }

    button {
      border: 0;
      border-radius: 999px;
      padding: 11px 18px;
      font: inherit;
      font-weight: 600;
      color: white;
      cursor: pointer;
      background: linear-gradient(135deg, var(--accent-2), var(--accent));
      transition: transform 120ms ease, opacity 120ms ease;
      box-shadow: 0 14px 24px rgba(23, 119, 200, 0.22);
    }

    button:hover {
      transform: translateY(-1px);
    }

    button:disabled {
      opacity: 0.55;
      cursor: not-allowed;
      transform: none;
    }

    button.danger {
      background: linear-gradient(135deg, #d65943, var(--danger));
    }

    input[type="date"] {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
      color: var(--text);
      background: #fff;
    }

    .status {
      min-height: 24px;
      margin-top: 14px;
      font-size: 0.96rem;
      color: var(--muted);
    }

    .status.error {
      color: var(--danger);
    }

    .status.success {
      color: var(--accent);
    }

    .meta {
      font-size: 0.98rem;
      color: var(--muted);
    }

    .table-panel {
      margin-top: 20px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border-radius: 18px;
      overflow: hidden;
    }

    th,
    td {
      text-align: left;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }

    th {
      background: var(--panel-soft);
      font-size: 0.88rem;
      letter-spacing: 0.02em;
      text-transform: uppercase;
      color: #426179;
    }

    tr:last-child td {
      border-bottom: 0;
    }

    .mono {
      font-family: Consolas, "Courier New", monospace;
      font-size: 0.95rem;
    }

    @media (max-width: 700px) {
      main {
        padding: 24px 14px 40px;
      }

      .hero {
        padding: 18px;
      }

      .brand-bar {
        flex-direction: column;
        align-items: flex-start;
      }

      .brand {
        align-items: flex-start;
      }

      .brand-logo-wrap {
        width: 62px;
        height: 62px;
      }

      th,
      td {
        padding: 10px 9px;
        font-size: 0.93rem;
      }
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="brand-bar">
        <a class="brand" href="https://sms-it.ru" target="_blank" rel="noreferrer">
          <span class="brand-logo-wrap">
            <img
              class="brand-logo"
              src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg"
              alt="СМС-ИТ"
            >
          </span>
          <span class="brand-copy">
            <span class="brand-kicker">СМС-ИТ</span>
            <span class="brand-title">Внутренний обзор проектов и ежедневных срезов</span>
            <span class="brand-subtitle">
              Панель для синхронизации проектов из Redmine, получения срезов задач и контроля оценок и трудозатрат.
            </span>
          </span>
        </a>
        <div class="brand-note">Продуктовая разработка и сопровождение</div>
      </div>

      <h1>Проекты Redmine в фирменной подаче</h1>
      <p class="lead">
        Страница визуально приведена ближе к стилю
        <a href="https://sms-it.ru" target="_blank" rel="noreferrer">sms-it.ru</a>:
        светлая корпоративная палитра, мягкие карточки, округлая навигация и логотип в левом верхнем углу для чистых скриншотов.
      </p>

      <nav class="quick-links" aria-label="Быстрый переход по разделам">
        <a href="#server-time">Время сервера</a>
        <a href="#project-actions">Проекты</a>
        <a href="#snapshot-actions">Срезы</a>
        <a href="#delete-snapshot">Удаление</a>
        <a href="#projects-table">Таблица проектов</a>
        <a href="#snapshot-runs-table">Таблица срезов</a>
      </nav>
    </section>

    <section class="grid">
      <article class="panel" id="server-time">
        <h2>Текущее время сервера</h2>
        <p>Быстрая проверка, что приложение отвечает и показывает актуальное время.</p>
        <div class="meta" id="timeValue">Загрузка...</div>
      </article>

      <article class="panel" id="project-actions">
        <h2>Проекты Redmine</h2>
        <p>Получает список проектов из Redmine и добавляет в базу только новые записи.</p>
        <div class="row">
          <button id="refreshProjectsButton" type="button">Обновить список проектов</button>
        </div>
        <div class="status" id="projectsStatus"></div>
      </article>

      <article class="panel" id="snapshot-actions">
        <h2>Получение срезов задач</h2>
        <p>
          Запрашивает срезы только для тех проектов, по которым на сегодняшнюю дату
          еще нет записи в базе данных.
        </p>
        <div class="row">
          <button id="captureSnapshotsButton" type="button">Получить срезы задач</button>
        </div>
        <div class="status" id="captureStatus"></div>
      </article>

      <article class="panel" id="delete-snapshot">
        <h2>Удаление среза по дате</h2>
        <p>Удаляет все срезы и все строки задач за выбранную календарную дату.</p>
        <div class="row">
          <input id="snapshotDateInput" type="date">
          <button id="deleteSnapshotsButton" class="danger" type="button">Очистить срез на дату</button>
        </div>
        <div class="status" id="deleteStatus"></div>
      </article>
    </section>

    <section class="panel table-panel" id="projects-table">
      <h2>Проекты в базе данных</h2>
      <p class="meta" id="projectsCount">Загрузка списка проектов...</p>
      <div style="overflow:auto;">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Название</th>
              <th>Идентификатор</th>
              <th>Статус</th>
              <th>Дата последнего среза</th>
              <th>Базовая оценка, ч</th>
              <th>Разработка: оценка, ч</th>
              <th>Разработка: факт за год, ч</th>
              <th>Ошибка: оценка, ч</th>
              <th>Ошибка: факт за год, ч</th>
              <th>Обновлен в Redmine</th>
              <th>Синхронизирован</th>
            </tr>
          </thead>
          <tbody id="projectsTableBody"></tbody>
        </table>
      </div>
    </section>

    <section class="panel table-panel" id="snapshot-runs-table">
      <h2>Последние срезы задач</h2>
      <p class="meta" id="snapshotRunsCount">Загрузка списка срезов...</p>
      <div style="overflow:auto;">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Дата среза</th>
              <th>Проект</th>
              <th>Идентификатор</th>
              <th>Задач</th>
              <th>План, ч</th>
              <th>Факт всего, ч</th>
              <th>Факт за год, ч</th>
              <th>Записан</th>
            </tr>
          </thead>
          <tbody id="snapshotRunsTableBody"></tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    const timeValue = document.getElementById("timeValue");
    const projectsStatus = document.getElementById("projectsStatus");
    const captureStatus = document.getElementById("captureStatus");
    const deleteStatus = document.getElementById("deleteStatus");
    const projectsCount = document.getElementById("projectsCount");
    const snapshotRunsCount = document.getElementById("snapshotRunsCount");
    const projectsTableBody = document.getElementById("projectsTableBody");
    const snapshotRunsTableBody = document.getElementById("snapshotRunsTableBody");
    const snapshotDateInput = document.getElementById("snapshotDateInput");
    const refreshProjectsButton = document.getElementById("refreshProjectsButton");
    const captureSnapshotsButton = document.getElementById("captureSnapshotsButton");
    const deleteSnapshotsButton = document.getElementById("deleteSnapshotsButton");

    function setStatus(element, message, kind = "") {
      element.textContent = message;
      element.className = "status" + (kind ? " " + kind : "");
    }

    function formatDate(value) {
      if (!value) {
        return "—";
      }

      return String(value).replace("T", " ").replace("+00:00", " UTC");
    }

    function renderProjects(projects) {
      projectsTableBody.innerHTML = "";
      projectsCount.textContent = `Проектов в базе: ${projects.length}`;

      if (!projects.length) {
        projectsTableBody.innerHTML = '<tr><td colspan="12">Проектов пока нет.</td></tr>';
        return;
      }

      for (const project of projects) {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td class="mono">${project.redmine_id ?? "—"}</td>
          <td>${project.name ?? "—"}</td>
          <td class="mono">${project.identifier ?? "—"}</td>
          <td>${project.status ?? "—"}</td>
          <td class="mono">${project.latest_snapshot_date ?? "—"}</td>
          <td>${project.baseline_estimate_hours ?? 0}</td>
          <td>${project.development_estimate_hours ?? 0}</td>
          <td>${project.development_spent_hours_year ?? 0}</td>
          <td>${project.bug_estimate_hours ?? 0}</td>
          <td>${project.bug_spent_hours_year ?? 0}</td>
          <td>${formatDate(project.updated_on)}</td>
          <td>${formatDate(project.synced_at)}</td>
        `;
        projectsTableBody.appendChild(row);
      }
    }

    function renderSnapshotRuns(snapshotRuns) {
      snapshotRunsTableBody.innerHTML = "";
      snapshotRunsCount.textContent = `Последних срезов в списке: ${snapshotRuns.length}`;

      if (!snapshotRuns.length) {
        snapshotRunsTableBody.innerHTML = '<tr><td colspan="9">Срезов пока нет.</td></tr>';
        return;
      }

      for (const run of snapshotRuns) {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td class="mono">${run.id ?? "—"}</td>
          <td class="mono">${run.captured_for_date ?? "—"}</td>
          <td>${run.project_name ?? "—"}</td>
          <td class="mono">${run.project_identifier ?? "—"}</td>
          <td>${run.total_issues ?? 0}</td>
          <td>${run.total_estimated_hours ?? 0}</td>
          <td>${run.total_spent_hours ?? 0}</td>
          <td>${run.total_spent_hours_year ?? 0}</td>
          <td>${formatDate(run.captured_at)}</td>
        `;
        snapshotRunsTableBody.appendChild(row);
      }
    }

    async function loadServerTime() {
      try {
        const response = await fetch("/api/time");
        const payload = await response.json();
        timeValue.textContent = `${payload.current_time} | UTC: ${payload.current_time_utc}`;
      } catch (error) {
        timeValue.textContent = "Не удалось получить время сервера.";
      }
    }

    async function loadProjects() {
      try {
        const response = await fetch("/api/projects");
        const payload = await response.json();
        renderProjects(payload.projects ?? []);
      } catch (error) {
        renderProjects([]);
        setStatus(projectsStatus, "Не удалось загрузить проекты из базы.", "error");
      }
    }

    async function loadSnapshotRuns() {
      try {
        const response = await fetch("/api/issues/snapshots/runs");
        const payload = await response.json();
        renderSnapshotRuns(payload.snapshot_runs ?? []);
      } catch (error) {
        renderSnapshotRuns([]);
        setStatus(captureStatus, "Не удалось загрузить список срезов.", "error");
      }
    }

    async function refreshProjects() {
      refreshProjectsButton.disabled = true;
      setStatus(projectsStatus, "Обновляем список проектов...");

      try {
        const response = await fetch("/api/projects/refresh", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Ошибка обновления проектов.");
        }

        renderProjects(payload.projects ?? []);
        setStatus(
          projectsStatus,
          `Готово: добавлено новых проектов ${payload.added_count ?? 0}.`,
          "success"
        );
      } catch (error) {
        setStatus(projectsStatus, error.message, "error");
      } finally {
        refreshProjectsButton.disabled = false;
      }
    }

    async function captureSnapshots() {
      captureSnapshotsButton.disabled = true;
      setStatus(captureStatus, "Получаем срезы задач...");

      try {
        const response = await fetch("/api/issues/snapshots/capture", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Ошибка получения срезов.");
        }

        if (payload.captured_for_date) {
          snapshotDateInput.value = payload.captured_for_date;
        }

        renderSnapshotRuns(payload.snapshot_runs ?? []);
        setStatus(
          captureStatus,
          `Готово: создано срезов ${payload.created_runs ?? 0}, задач ${payload.captured_issues ?? 0}, уже было срезов на сегодня ${payload.already_captured_projects ?? 0}.`,
          "success"
        );
      } catch (error) {
        setStatus(captureStatus, error.message, "error");
      } finally {
        captureSnapshotsButton.disabled = false;
      }
    }

    async function deleteSnapshotsForDate() {
      const capturedForDate = snapshotDateInput.value;
      if (!capturedForDate) {
        setStatus(deleteStatus, "Сначала выберите дату в календаре.", "error");
        return;
      }

      deleteSnapshotsButton.disabled = true;
      setStatus(deleteStatus, `Удаляем срезы за ${capturedForDate}...`);

      try {
        const response = await fetch(
          `/api/issues/snapshots/by-date?captured_for_date=${encodeURIComponent(capturedForDate)}`,
          { method: "DELETE" }
        );
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Ошибка удаления срезов.");
        }

        renderSnapshotRuns(payload.snapshot_runs ?? []);
        setStatus(
          deleteStatus,
          `Удалено срезов: ${payload.deleted_runs ?? 0}, строк задач: ${payload.deleted_items ?? 0}.`,
          "success"
        );
      } catch (error) {
        setStatus(deleteStatus, error.message, "error");
      } finally {
        deleteSnapshotsButton.disabled = false;
      }
    }

    refreshProjectsButton.addEventListener("click", refreshProjects);
    captureSnapshotsButton.addEventListener("click", captureSnapshots);
    deleteSnapshotsButton.addEventListener("click", deleteSnapshotsForDate);

    loadServerTime();
    loadProjects();
    loadSnapshotRuns();
  </script>
</body>
</html>
"""


def requireProjectSyncConfig() -> None:
    if not config.redmineUrl:
        raise HTTPException(status_code=400, detail="REDMINE_URL is not set")
    if not config.apiKey:
        raise HTTPException(status_code=400, detail="REDMINE_API_KEY is not set")


@app.get("/", response_class=HTMLResponse)
def readRoot() -> HTMLResponse:
    return HTMLResponse(PAGE_HTML)


@app.get("/api/time")
def getTime() -> dict[str, str]:
    nowUtc = datetime.now(UTC)
    return {
        "current_time": nowUtc.astimezone().isoformat(),
        "current_time_utc": nowUtc.isoformat(),
    }


@app.get("/api/projects")
def getProjects() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    return {"projects": listStoredProjects()}


@app.post("/api/projects/refresh")
def refreshProjects() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    requireProjectSyncConfig()
    ensureProjectsTable()

    projects = fetchAllProjectsFromRedmine(config.redmineUrl, config.apiKey)
    addedCount = storeMissingProjects(projects)

    return {
        "added_count": addedCount,
        "projects": listStoredProjects(),
    }


@app.get("/api/issues/snapshots/runs")
def getIssueSnapshotRuns() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    return {"snapshot_runs": listRecentIssueSnapshotRuns()}


@app.delete("/api/issues/snapshots/by-date")
def deleteIssueSnapshotsByDate(
    captured_for_date: str = Query(..., description="Дата в формате YYYY-MM-DD"),
) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()

    try:
        datetime.strptime(captured_for_date, "%Y-%m-%d")
    except ValueError as error:
        raise HTTPException(status_code=400, detail="captured_for_date must be YYYY-MM-DD") from error

    result = deleteIssueSnapshotsForDate(captured_for_date)
    result["snapshot_runs"] = listRecentIssueSnapshotRuns()
    return result


@app.post("/api/issues/snapshots/capture")
def captureIssueSnapshots() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    requireProjectSyncConfig()

    try:
        return captureAllIssueSnapshots()
    except RuntimeError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "environment": config.appEnv,
        "database_configured": bool(config.databaseUrl),
        "redmine_configured": bool(config.redmineUrl and config.apiKey),
    }


@app.get("/db-health")
def databaseHealth() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    try:
        connected = checkDatabaseConnection()
    except Exception as error:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(error)) from error

    return {"status": "ok" if connected else "down", "connected": connected}
