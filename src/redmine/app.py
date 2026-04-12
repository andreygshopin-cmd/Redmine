from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from src.redmine.config import loadConfig
from src.redmine.db import (
    checkDatabaseConnection,
    countIssueSnapshotRuns,
    deleteIssueSnapshotsForDate,
    ensureIssueSnapshotTables,
    ensureProjectsTable,
    listRecentIssueSnapshotRuns,
    listStoredProjects,
    storeMissingProjects,
)
from src.redmine.redmine_client import fetchAllProjectsFromRedmine
from src.redmine.snapshots import (
    getIssueSnapshotCaptureStatus,
    isIssueSnapshotCaptureRunning,
    startIssueSnapshotCaptureInBackground,
)


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
      --bg: #ffffff;
      --panel: #ffffff;
      --panel-soft: #eef6f7;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --blue-302: #375d77;
      --yellow-109: #ffc600;
      --cyan-310: #52cee6;
      --orange-1585: #ff6c0e;
      --danger: #ff6c0e;
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
      background: var(--bg);
      color: var(--text);
    }

    .topbar {
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      z-index: 20;
      padding: 0 20px;
      background: #ffffff;
      border-bottom: 1px solid #eef2f6;
    }

    .topbar-spacer {
      height: 100px;
    }

    main {
      max-width: 1200px;
      margin: 0 auto;
      padding: 18px 20px 56px;
    }

    .hero {
      margin-bottom: 24px;
      padding: 0;
    }

    .brand-bar {
      max-width: 1200px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 24px;
      min-height: 100px;
    }

    .brand {
      display: inline-flex;
      align-items: center;
      gap: 0;
      text-decoration: none;
      color: inherit;
      flex: 0 0 auto;
    }

    .brand-logo-wrap {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 220px;
      height: 72px;
      padding: 0;
      border-radius: 0;
      background: transparent;
      border: 0;
      box-shadow: none;
    }

    .brand-logo {
      display: block;
      width: 100%;
      height: auto;
    }

    .hero-nav {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      flex: 1 1 auto;
    }

    h1 {
      margin: 0 0 10px;
      font-size: clamp(2rem, 5vw, 3.2rem);
      line-height: 1.03;
      letter-spacing: -0.03em;
    }

    .quick-links {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin: 0;
      justify-content: flex-end;
    }

    .quick-links a {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 10px 16px;
      border-radius: 6px;
      border: 1px solid transparent;
      color: #ffffff;
      text-decoration: none;
      font-weight: 600;
      box-shadow: var(--shadow-soft);
      transition: transform 120ms ease, border-color 120ms ease;
    }

    .quick-links a:nth-child(4n + 1) {
      background: var(--blue-302);
    }

    .quick-links a:nth-child(4n + 2) {
      background: var(--yellow-109);
      color: #16324a;
    }

    .quick-links a:nth-child(4n + 3) {
      background: var(--cyan-310);
      color: #16324a;
    }

    .quick-links a:nth-child(4n + 4) {
      background: var(--orange-1585);
    }

    .quick-links a:hover {
      transform: translateY(-1px);
      filter: brightness(1.03);
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
      border-radius: 8px;
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
      border-radius: 6px;
      padding: 11px 18px;
      font: inherit;
      font-weight: 600;
      color: white;
      cursor: pointer;
      background: var(--blue-302);
      transition: transform 120ms ease, opacity 120ms ease;
      box-shadow: 0 14px 24px rgba(55, 93, 119, 0.22);
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
      background: var(--orange-1585);
      box-shadow: 0 14px 24px rgba(255, 108, 14, 0.22);
    }

    #refreshProjectsButton {
      background: var(--yellow-109);
      color: #16324a;
      box-shadow: 0 14px 24px rgba(255, 198, 0, 0.24);
    }

    #captureSnapshotsButton {
      background: var(--cyan-310);
      color: #16324a;
      box-shadow: 0 14px 24px rgba(82, 206, 230, 0.24);
    }

    input[type="date"] {
      border: 1px solid var(--line);
      border-radius: 6px;
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
      color: var(--blue-302);
    }

    .meta {
      font-size: 0.98rem;
      color: var(--muted);
    }

    .table-panel {
      margin-top: 20px;
    }

    .table-toolbar {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      align-items: center;
      margin: 14px 0 16px;
    }

    .table-toolbar label {
      font-weight: 600;
      color: var(--text);
    }

    .filter-input {
      width: 160px;
    }

    .project-name-cell {
      white-space: nowrap;
    }

    .project-indent {
      color: var(--muted);
      font-weight: 600;
      letter-spacing: 0.02em;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border-radius: 8px;
      overflow: hidden;
    }

    .table-wrap {
      overflow: auto;
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

    .identifier-col {
      width: 13ch;
    }

    .project-link {
      color: var(--blue-302);
      text-decoration: none;
      white-space: nowrap;
      font-weight: 700;
      border-bottom: 1px dashed currentColor;
    }

    .project-link:hover {
      color: var(--orange-1585);
      border-bottom-style: solid;
    }

    @media (max-width: 700px) {
      .topbar {
        padding: 0 14px;
      }

      .topbar-spacer {
        height: 136px;
      }

      main {
        padding: 18px 14px 40px;
      }

      .brand-bar {
        flex-direction: column;
        align-items: flex-start;
        justify-content: center;
        gap: 12px;
        min-height: 136px;
      }

      .hero-nav,
      .quick-links {
        justify-content: flex-start;
      }

      .brand-logo-wrap {
        width: 180px;
        height: 58px;
      }

      th,
      td {
        padding: 10px 9px;
        font-size: 0.93rem;
      }
    }
  </style>
</head>
<body id="top">
  <div class="topbar">
    <div class="brand-bar">
      <a class="brand" href="#top">
        <span class="brand-logo-wrap">
          <img
            class="brand-logo"
            src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg"
            alt="СМС-ИТ"
          >
        </span>
      </a>
      <nav class="hero-nav" aria-label="Быстрый переход по разделам">
        <div class="quick-links">
          <a href="#project-actions">Проекты</a>
          <a href="#snapshot-actions">Срезы</a>
          <a href="#delete-snapshot">Удаление</a>
          <a href="#projects-table">Таблица проектов</a>
          <a href="#snapshot-runs-table">Таблица срезов</a>
        </div>
      </nav>
    </div>
  </div>
  <div class="topbar-spacer" aria-hidden="true"></div>

  <main>
    <section class="hero">
      <h1>Анализ проектов Redmine</h1>
    </section>

    <section class="grid">
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
      <div class="table-toolbar">
        <label for="projectsFactFilterInput">Мин. сумма факта за год по разработке и багфиксу</label>
        <input
          id="projectsFactFilterInput"
          class="filter-input"
          type="number"
          min="0"
          step="0.1"
          inputmode="decimal"
        >
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Название</th>
              <th class="identifier-col">Идентификатор</th>
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
      <div class="table-wrap">
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
    const projectsStatus = document.getElementById("projectsStatus");
    const captureStatus = document.getElementById("captureStatus");
    const deleteStatus = document.getElementById("deleteStatus");
    const projectsCount = document.getElementById("projectsCount");
    const snapshotRunsCount = document.getElementById("snapshotRunsCount");
    const projectsTableBody = document.getElementById("projectsTableBody");
    const snapshotRunsTableBody = document.getElementById("snapshotRunsTableBody");
    const snapshotDateInput = document.getElementById("snapshotDateInput");
    const projectsFactFilterInput = document.getElementById("projectsFactFilterInput");
    const refreshProjectsButton = document.getElementById("refreshProjectsButton");
    const captureSnapshotsButton = document.getElementById("captureSnapshotsButton");
    const deleteSnapshotsButton = document.getElementById("deleteSnapshotsButton");
    let captureStatusPollTimer = null;
    let allProjects = [];
    const projectsFactFilterStorageKey = "redmine.projects.factFilter.min";

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

    function formatHours(value) {
      const number = Number(value ?? 0);
      if (!Number.isFinite(number)) {
        return "0.0";
      }

      return number.toFixed(1);
    }

    function getProjectsFactFilterValue() {
      const rawValue = String(projectsFactFilterInput.value || "").trim().replace(",", ".");
      const parsed = Number(rawValue);
      if (!rawValue || !Number.isFinite(parsed) || parsed < 0) {
        return 0;
      }

      return parsed;
    }

    function saveProjectsFactFilterValue() {
      window.localStorage.setItem(projectsFactFilterStorageKey, String(projectsFactFilterInput.value || ""));
    }

    function restoreProjectsFactFilterValue() {
      const savedValue = window.localStorage.getItem(projectsFactFilterStorageKey);
      if (savedValue !== null) {
        projectsFactFilterInput.value = savedValue;
      }
    }

    function buildProjectHierarchy(projects) {
      const byParent = new Map();
      const visited = new Set();

      for (const project of projects) {
        const parentId = project.parent_redmine_id ?? null;
        if (!byParent.has(parentId)) {
          byParent.set(parentId, []);
        }
        byParent.get(parentId).push(project);
      }

      for (const children of byParent.values()) {
        children.sort((left, right) => String(left.name || "").localeCompare(String(right.name || ""), "ru"));
      }

      const ordered = [];

      function visit(parentId, level) {
        const children = byParent.get(parentId) || [];
        for (const child of children) {
          if (visited.has(child.redmine_id)) {
            continue;
          }
          visited.add(child.redmine_id);
          ordered.push({ ...child, hierarchy_level: level });
          visit(child.redmine_id, level + 1);
        }
      }

      visit(null, 0);

      for (const project of projects) {
        if (visited.has(project.redmine_id)) {
          continue;
        }
        ordered.push({ ...project, hierarchy_level: 0 });
        visit(project.redmine_id, 1);
      }

      return ordered;
    }

    function applyProjectsFilter(projects) {
      const minFactSum = getProjectsFactFilterValue();
      const byId = new Map(projects.map((project) => [project.redmine_id, project]));
      const includedIds = new Set();

      for (const project of projects) {
        const developmentFact = Number(project.development_spent_hours_year ?? 0);
        const bugFact = Number(project.bug_spent_hours_year ?? 0);
        if (developmentFact + bugFact <= minFactSum) {
          continue;
        }

        let currentProject = project;
        while (currentProject) {
          includedIds.add(currentProject.redmine_id);
          const parentId = currentProject.parent_redmine_id;
          currentProject = parentId ? byId.get(parentId) || null : null;
        }
      }

      return projects.filter((project) => includedIds.has(project.redmine_id));
    }

    function rerenderProjects() {
      renderProjects(allProjects);
    }

    async function loadCaptureProgress() {
      try {
        const response = await fetch("/api/issues/snapshots/capture-status");
        const payload = await response.json();

        if (!payload.is_running) {
          stopCaptureProgressPolling();
          await loadProjects();
          await loadSnapshotRuns();

          if (payload.error_message) {
            setStatus(captureStatus, payload.error_message, "error");
            captureSnapshotsButton.disabled = false;
            return;
          }

          if (payload.created_runs || payload.captured_issues || payload.already_captured_projects) {
            setStatus(
              captureStatus,
              `Готово: создано срезов ${payload.created_runs ?? 0}, задач ${payload.captured_issues ?? 0}, уже было срезов на сегодня ${payload.already_captured_projects ?? 0}.`,
              "success"
            );
          }

          captureSnapshotsButton.disabled = false;
          return;
        }

        const projectName = payload.current_project_name || payload.last_completed_project_name || "без названия";
        const processedProjects = Number(payload.processed_projects ?? 0);
        const totalProjects = Number(payload.total_projects ?? 0);
        setStatus(
          captureStatus,
          `Получаем срезы задач по проекту ${projectName}... ${processedProjects}/${totalProjects}`
        );
      } catch (error) {
        // Keep the last visible status if polling temporarily fails.
      }
    }

    function startCaptureProgressPolling() {
      stopCaptureProgressPolling();
      loadCaptureProgress();
      captureStatusPollTimer = window.setInterval(loadCaptureProgress, 1500);
    }

    function stopCaptureProgressPolling() {
      if (captureStatusPollTimer !== null) {
        window.clearInterval(captureStatusPollTimer);
        captureStatusPollTimer = null;
      }
    }

    function renderProjects(projects) {
      allProjects = Array.isArray(projects) ? [...projects] : [];
      const orderedProjects = buildProjectHierarchy(allProjects);
      const filteredProjects = applyProjectsFilter(orderedProjects);
      projectsTableBody.innerHTML = "";
      projectsCount.textContent = `Проектов в базе: ${allProjects.length}. После фильтра: ${filteredProjects.length}`;

      if (!filteredProjects.length) {
        projectsTableBody.innerHTML = '<tr><td colspan="12">Проектов пока нет.</td></tr>';
        return;
      }

      for (const project of filteredProjects) {
        const identifier = project.identifier ?? "";
        const identifierHtml = identifier
          ? `<a class="project-link mono" href="https://redmine.sms-it.ru/projects/${encodeURIComponent(identifier)}/issues" target="_blank" rel="noreferrer">${identifier}</a>`
          : "—";
        const level = Number(project.hierarchy_level ?? 0);
        const indent = level > 0 ? `${"--".repeat(level)} ` : "";
        const row = document.createElement("tr");
        row.innerHTML = `
          <td class="mono">${project.redmine_id ?? "—"}</td>
          <td class="project-name-cell"><span class="project-indent">${indent}</span>${project.name ?? "—"}</td>
          <td>${identifierHtml}</td>
          <td>${project.status ?? "—"}</td>
          <td class="mono">${project.latest_snapshot_date ?? "—"}</td>
          <td>${formatHours(project.baseline_estimate_hours)}</td>
          <td>${formatHours(project.development_estimate_hours)}</td>
          <td>${formatHours(project.development_spent_hours_year)}</td>
          <td>${formatHours(project.bug_estimate_hours)}</td>
          <td>${formatHours(project.bug_spent_hours_year)}</td>
          <td>${formatDate(project.updated_on)}</td>
          <td>${formatDate(project.synced_at)}</td>
        `;
        projectsTableBody.appendChild(row);
      }
    }

    function renderSnapshotRuns(snapshotRuns, totalCount = snapshotRuns.length) {
      snapshotRunsTableBody.innerHTML = "";
      snapshotRunsCount.textContent = `Всего срезов в базе: ${totalCount}`;

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
          <td>${formatHours(run.total_estimated_hours)}</td>
          <td>${formatHours(run.total_spent_hours)}</td>
          <td>${formatHours(run.total_spent_hours_year)}</td>
          <td>${formatDate(run.captured_at)}</td>
        `;
        snapshotRunsTableBody.appendChild(row);
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
        renderSnapshotRuns(payload.snapshot_runs ?? [], payload.total_count ?? 0);
      } catch (error) {
        renderSnapshotRuns([], 0);
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
      setStatus(captureStatus, "Запускаем получение срезов...");

      try {
        const response = await fetch("/api/issues/snapshots/capture", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Ошибка получения срезов.");
        }

        if (payload.captured_for_date) {
          snapshotDateInput.value = payload.captured_for_date;
        }

        setStatus(captureStatus, payload.detail || "Фоновая загрузка срезов запущена...");
        startCaptureProgressPolling();
      } catch (error) {
        stopCaptureProgressPolling();
        setStatus(captureStatus, error.message, "error");
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

        renderSnapshotRuns(payload.snapshot_runs ?? [], payload.total_count ?? 0);
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
    projectsFactFilterInput.addEventListener("input", () => {
      saveProjectsFactFilterValue();
      rerenderProjects();
    });

    restoreProjectsFactFilterValue();
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
    return {
        "snapshot_runs": listRecentIssueSnapshotRuns(limit=200),
        "total_count": countIssueSnapshotRuns(),
    }


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
    result["snapshot_runs"] = listRecentIssueSnapshotRuns(limit=200)
    result["total_count"] = countIssueSnapshotRuns()
    return result


@app.post("/api/issues/snapshots/capture")
def captureIssueSnapshots() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    requireProjectSyncConfig()
    ensureProjectsTable()
    ensureIssueSnapshotTables()

    if isIssueSnapshotCaptureRunning():
        return {
            "started": False,
            "detail": "Получение срезов уже выполняется.",
            **getIssueSnapshotCaptureStatus(),
        }

    started = startIssueSnapshotCaptureInBackground()
    return {
        "started": started,
        "detail": "Получение срезов запущено в фоновом режиме.",
        **getIssueSnapshotCaptureStatus(),
    }


@app.get("/api/issues/snapshots/capture-status")
def getIssueSnapshotCaptureProgress() -> dict[str, object]:
    return getIssueSnapshotCaptureStatus()


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
