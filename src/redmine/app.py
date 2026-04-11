from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from src.redmine.config import loadConfig
from src.redmine.db import (
    checkDatabaseConnection,
    ensureIssueSnapshotTables,
    ensureProjectsTable,
    listRecentIssueSnapshotBatches,
    listRecentIssueSnapshotRuns,
    listStoredProjects,
    storeMissingProjects,
)
from src.redmine.redmine_client import fetchAllProjectsFromRedmine
from src.redmine.snapshots import captureAllIssueSnapshots


config = loadConfig()
app = FastAPI(title="Redmine API", version="0.1.0")


PAGE_HTML = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Redmine Projects</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f4efe6;
        --panel: #fffaf2;
        --panel-strong: #ffffff;
        --text: #1f2937;
        --muted: #6b7280;
        --accent: #14532d;
        --accent-2: #d97706;
        --line: rgba(31, 41, 55, 0.08);
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        font-family: Georgia, "Times New Roman", serif;
        background:
          radial-gradient(circle at top left, rgba(217, 119, 6, 0.18), transparent 32%),
          radial-gradient(circle at bottom right, rgba(20, 83, 45, 0.18), transparent 30%),
          var(--bg);
        color: var(--text);
      }

      .shell {
        width: min(1120px, calc(100vw - 32px));
        margin: 32px auto;
        display: grid;
        gap: 24px;
      }

      .card {
        padding: 28px;
        border: 1px solid var(--line);
        border-radius: 24px;
        background: var(--panel);
        box-shadow: 0 24px 60px rgba(31, 41, 55, 0.12);
      }

      .hero {
        display: grid;
        gap: 16px;
      }

      .eyebrow {
        margin: 0;
        color: var(--accent-2);
        font-size: 13px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }

      h1, h2 {
        margin: 0;
        line-height: 0.95;
      }

      h1 {
        font-size: clamp(34px, 7vw, 64px);
      }

      h2 {
        font-size: clamp(24px, 4vw, 34px);
      }

      p {
        margin: 0;
        color: var(--muted);
        font-size: 18px;
      }

      .actions {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
      }

      button {
        border: 0;
        border-radius: 999px;
        padding: 14px 22px;
        font: inherit;
        font-size: 16px;
        cursor: pointer;
        background: var(--accent);
        color: white;
        transition: transform 120ms ease, opacity 120ms ease;
      }

      button.secondary {
        background: var(--accent-2);
      }

      button:hover {
        transform: translateY(-1px);
      }

      button:disabled {
        opacity: 0.7;
        cursor: wait;
        transform: none;
      }

      .grid {
        display: grid;
        gap: 24px;
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      }

      .info-box {
        padding: 22px;
        border-radius: 18px;
        background: var(--panel-strong);
        border: 1px solid var(--line);
      }

      .label {
        display: block;
        font-size: 12px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.1em;
      }

      .value {
        margin-top: 10px;
        font-size: clamp(28px, 5vw, 42px);
        font-weight: 700;
      }

      .meta {
        margin-top: 10px;
        font-size: 14px;
        color: var(--muted);
      }

      .section-head {
        display: flex;
        gap: 16px;
        align-items: end;
        justify-content: space-between;
        flex-wrap: wrap;
        margin-bottom: 18px;
      }

      .status {
        min-height: 22px;
        color: var(--muted);
        font-size: 15px;
      }

      .status.error {
        color: #b91c1c;
      }

      .status.success {
        color: #166534;
      }

      .table-wrap {
        overflow: auto;
        border-radius: 18px;
        border: 1px solid var(--line);
        background: var(--panel-strong);
      }

      table {
        width: 100%;
        border-collapse: collapse;
        min-width: 720px;
      }

      th, td {
        padding: 14px 16px;
        text-align: left;
        border-bottom: 1px solid var(--line);
        vertical-align: top;
      }

      th {
        font-size: 13px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: var(--muted);
      }

      td {
        font-size: 15px;
      }

      tbody tr:last-child td {
        border-bottom: 0;
      }

      .empty {
        padding: 22px;
        color: var(--muted);
      }

      @media (max-width: 640px) {
        .shell {
          width: min(100vw - 20px, 1120px);
          margin: 10px auto 20px;
        }

        .card {
          padding: 20px;
          border-radius: 20px;
        }

        p {
          font-size: 16px;
        }
      }
    </style>
  </head>
  <body>
    <main class="shell">
      <section class="card hero">
        <p class="eyebrow">Redmine dashboard</p>
        <h1>Projects, issues, and server time</h1>
        <p>This page can synchronize projects from Redmine and capture issue snapshots for burn and growth charts.</p>
      </section>

      <section class="grid">
        <article class="card">
          <h2>Server time</h2>
          <p>Fetch the current time from the Python backend.</p>
          <div class="actions" style="margin-top: 20px;">
            <button id="load-time" type="button">Get current time</button>
          </div>
          <section class="info-box" style="margin-top: 22px;" aria-live="polite">
            <span class="label">Current server time</span>
            <div id="time-value" class="value">--:--:--</div>
            <div id="time-meta" class="meta">Click the button to load the current time.</div>
          </section>
        </article>

        <article class="card">
          <h2>Projects sync</h2>
          <p>Load projects from Redmine and store only the ones that are not yet in the database.</p>
          <div class="actions" style="margin-top: 20px;">
            <button id="refresh-projects" class="secondary" type="button">Refresh projects</button>
          </div>
          <div id="projects-status" class="status" style="margin-top: 22px;">Project list is loading from the database.</div>
        </article>

        <article class="card">
          <h2>Issue snapshots</h2>
          <p>Capture a dated issue slice for every stored project and save it to a reusable history batch.</p>
          <div class="actions" style="margin-top: 20px;">
            <button id="capture-snapshots" type="button">Capture issue snapshot</button>
          </div>
          <div id="snapshots-status" class="status" style="margin-top: 22px;">Recent snapshot runs are loading.</div>
        </article>
      </section>

      <section class="card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Periodic capture</p>
            <h2>Recent snapshot batches</h2>
          </div>
          <div id="batches-count" class="meta">0 batches</div>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Snapshot Date</th>
                <th>Started</th>
                <th>Completed</th>
                <th>Projects</th>
                <th>Skipped</th>
                <th>Issues</th>
              </tr>
            </thead>
            <tbody id="batches-table-body">
              <tr>
                <td colspan="6" class="empty">No snapshot batches yet.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Stored data</p>
            <h2>Projects in database</h2>
          </div>
          <div id="projects-count" class="meta">0 projects</div>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Identifier</th>
                <th>Parent</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody id="projects-table-body">
              <tr>
                <td colspan="5" class="empty">No projects loaded yet.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Historical data</p>
            <h2>Recent issue snapshot runs</h2>
          </div>
          <div id="snapshots-count" class="meta">0 runs</div>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Captured At</th>
                <th>Project</th>
                <th>Identifier</th>
                <th>Issues</th>
                <th>Estimated Hours</th>
                <th>Spent Hours</th>
              </tr>
            </thead>
            <tbody id="snapshots-table-body">
              <tr>
                <td colspan="6" class="empty">No snapshot runs yet.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </main>

    <script>
      const button = document.getElementById("load-time");
      const timeValue = document.getElementById("time-value");
      const timeMeta = document.getElementById("time-meta");
      const refreshProjectsButton = document.getElementById("refresh-projects");
      const captureSnapshotsButton = document.getElementById("capture-snapshots");
      const projectsStatus = document.getElementById("projects-status");
      const snapshotsStatus = document.getElementById("snapshots-status");
      const projectsCount = document.getElementById("projects-count");
      const batchesCount = document.getElementById("batches-count");
      const snapshotsCount = document.getElementById("snapshots-count");
      const projectsTableBody = document.getElementById("projects-table-body");
      const batchesTableBody = document.getElementById("batches-table-body");
      const snapshotsTableBody = document.getElementById("snapshots-table-body");

      function setStatus(target, message, tone = "") {
        target.textContent = message;
        target.className = tone ? `status ${tone}` : "status";
      }

      function renderProjects(projects) {
        projectsCount.textContent = `${projects.length} projects`;

        if (!projects.length) {
          projectsTableBody.innerHTML = '<tr><td colspan="5" class="empty">No projects in the database yet.</td></tr>';
          return;
        }

        const rows = projects.map((project) => `
          <tr>
            <td>${project.redmine_id}</td>
            <td>${project.name}</td>
            <td>${project.identifier || ""}</td>
            <td>${project.parent_redmine_id || ""}</td>
            <td>${project.updated_on || ""}</td>
          </tr>
        `);

        projectsTableBody.innerHTML = rows.join("");
      }

      function renderSnapshotBatches(batches) {
        batchesCount.textContent = `${batches.length} batches`;

        if (!batches.length) {
          batchesTableBody.innerHTML = '<tr><td colspan="6" class="empty">No snapshot batches yet.</td></tr>';
          return;
        }

        const rows = batches.map((batch) => `
          <tr>
            <td>${batch.captured_for_date}</td>
            <td>${batch.started_at}</td>
            <td>${batch.completed_at || ""}</td>
            <td>${batch.completed_projects} / ${batch.total_projects}</td>
            <td>${batch.skipped_projects}</td>
            <td>${batch.total_issues}</td>
          </tr>
        `);

        batchesTableBody.innerHTML = rows.join("");
      }

      function renderSnapshotRuns(runs) {
        snapshotsCount.textContent = `${runs.length} runs`;

        if (!runs.length) {
          snapshotsTableBody.innerHTML = '<tr><td colspan="6" class="empty">No snapshot runs yet.</td></tr>';
          return;
        }

        const rows = runs.map((run) => `
          <tr>
            <td>${run.captured_at}</td>
            <td>${run.project_name}</td>
            <td>${run.project_identifier}</td>
            <td>${run.total_issues}</td>
            <td>${run.total_estimated_hours}</td>
            <td>${run.total_spent_hours}</td>
          </tr>
        `);

        snapshotsTableBody.innerHTML = rows.join("");
      }

      async function loadServerTime() {
        button.disabled = true;
        timeMeta.textContent = "Requesting server time...";

        try {
          const response = await fetch("/api/time");
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }

          const payload = await response.json();
          timeValue.textContent = payload.current_time;
          timeMeta.textContent = `UTC timestamp: ${payload.current_time_utc}`;
        } catch (error) {
          timeValue.textContent = "Error";
          timeMeta.textContent = `Could not load time: ${error.message}`;
        } finally {
          button.disabled = false;
        }
      }

      async function loadProjects() {
        setStatus(projectsStatus, "Loading projects from the database...");

        try {
          const response = await fetch("/api/projects");
          const payload = await response.json();

          if (!response.ok) {
            throw new Error(payload.detail || `HTTP ${response.status}`);
          }

          renderProjects(payload.projects);
          setStatus(projectsStatus, `Loaded ${payload.projects.length} projects from the database.`, "success");
        } catch (error) {
          renderProjects([]);
          setStatus(projectsStatus, `Could not load projects: ${error.message}`, "error");
        }
      }

      async function loadSnapshotBatches() {
        try {
          const response = await fetch("/api/issues/snapshots/batches");
          const payload = await response.json();

          if (!response.ok) {
            throw new Error(payload.detail || `HTTP ${response.status}`);
          }

          renderSnapshotBatches(payload.snapshot_batches);
        } catch (error) {
          renderSnapshotBatches([]);
          setStatus(snapshotsStatus, `Could not load snapshot batches: ${error.message}`, "error");
        }
      }

      async function loadSnapshotRuns() {
        setStatus(snapshotsStatus, "Loading recent snapshot runs...");

        try {
          const response = await fetch("/api/issues/snapshots/runs");
          const payload = await response.json();

          if (!response.ok) {
            throw new Error(payload.detail || `HTTP ${response.status}`);
          }

          renderSnapshotRuns(payload.snapshot_runs);
          setStatus(snapshotsStatus, `Loaded ${payload.snapshot_runs.length} recent snapshot runs.`, "success");
        } catch (error) {
          renderSnapshotRuns([]);
          setStatus(snapshotsStatus, `Could not load snapshot runs: ${error.message}`, "error");
        }
      }

      async function refreshProjects() {
        refreshProjectsButton.disabled = true;
        setStatus(projectsStatus, "Requesting projects from Redmine and saving missing rows...");

        try {
          const response = await fetch("/api/projects/refresh", { method: "POST" });
          const payload = await response.json();

          if (!response.ok) {
            throw new Error(payload.detail || `HTTP ${response.status}`);
          }

          renderProjects(payload.projects);
          setStatus(
            projectsStatus,
            `Stored ${payload.projects.length} projects. Added ${payload.added_count} new rows from Redmine.`,
            "success"
          );
        } catch (error) {
          setStatus(projectsStatus, `Could not refresh projects: ${error.message}`, "error");
        } finally {
          refreshProjectsButton.disabled = false;
        }
      }

      async function captureSnapshots() {
        captureSnapshotsButton.disabled = true;
        setStatus(snapshotsStatus, "Requesting full issue slices from Redmine and saving snapshot history...");

        try {
          const response = await fetch("/api/issues/snapshots/capture", { method: "POST" });
          const payload = await response.json();

          if (!response.ok) {
            throw new Error(payload.detail || `HTTP ${response.status}`);
          }

          renderSnapshotRuns(payload.snapshot_runs);
          renderSnapshotBatches(payload.snapshot_batches);
          setStatus(
            snapshotsStatus,
            `Created batch ${payload.snapshot_batch_id}, stored ${payload.created_runs} project slices and ${payload.captured_issues} issues.`,
            "success"
          );
        } catch (error) {
          setStatus(snapshotsStatus, `Could not capture snapshots: ${error.message}`, "error");
        } finally {
          captureSnapshotsButton.disabled = false;
        }
      }

      button.addEventListener("click", loadServerTime);
      refreshProjectsButton.addEventListener("click", refreshProjects);
      captureSnapshotsButton.addEventListener("click", captureSnapshots);
      loadProjects();
      loadSnapshotBatches();
      loadSnapshotRuns();
    </script>
  </body>
</html>
"""


def requireProjectSyncConfig() -> None:
    if not config.databaseUrl:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")
    if not config.redmineUrl:
        raise HTTPException(status_code=500, detail="REDMINE_URL is not set")
    if not config.apiKey:
        raise HTTPException(status_code=500, detail="REDMINE_API_KEY is not set")


@app.get("/")
def readRoot() -> HTMLResponse:
    return HTMLResponse(PAGE_HTML)


@app.get("/api/time")
def getTime() -> dict[str, str]:
    now = datetime.now(UTC)
    return {
        "current_time": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "current_time_utc": now.isoformat(),
    }


@app.get("/api/projects")
def getProjects() -> dict[str, list[dict[str, object]]]:
    if not config.databaseUrl:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    return {"projects": listStoredProjects()}


@app.post("/api/projects/refresh")
def refreshProjects() -> dict[str, object]:
    requireProjectSyncConfig()

    ensureProjectsTable()
    projects = fetchAllProjectsFromRedmine(config.redmineUrl, config.apiKey)
    addedCount = storeMissingProjects(projects)
    storedProjects = listStoredProjects()

    return {
        "added_count": addedCount,
        "projects": storedProjects,
    }


@app.get("/api/issues/snapshots/runs")
def getIssueSnapshotRuns() -> dict[str, list[dict[str, object]]]:
    if not config.databaseUrl:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    return {"snapshot_runs": listRecentIssueSnapshotRuns()}


@app.get("/api/issues/snapshots/batches")
def getIssueSnapshotBatches() -> dict[str, list[dict[str, object]]]:
    if not config.databaseUrl:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    return {"snapshot_batches": listRecentIssueSnapshotBatches()}


@app.post("/api/issues/snapshots/capture")
def captureIssueSnapshots() -> dict[str, object]:
    requireProjectSyncConfig()
    try:
        return captureAllIssueSnapshots()
    except RuntimeError as error:
        detail = str(error)
        statusCode = 400 if "No projects in the database" in detail else 500
        raise HTTPException(status_code=statusCode, detail=detail) from error


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/db-health")
def dbHealth() -> dict[str, str]:
    if not config.databaseUrl:
        return {"status": "error", "details": "DATABASE_URL is not set"}

    try:
        checkDatabaseConnection()
        return {"status": "ok"}
    except Exception as error:
        return {"status": "error", "details": str(error)}
