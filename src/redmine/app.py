from datetime import UTC, datetime
from html import escape

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from src.redmine.config import loadConfig
from src.redmine.db import (
    checkDatabaseConnection,
    countIssueSnapshotRuns,
    deleteIssueSnapshotForProjectDate,
    deleteIssueSnapshotsForDate,
    ensureIssueSnapshotTables,
    ensureProjectsTable,
    getSnapshotIssuesForProjectByDate,
    listRecentIssueSnapshotRuns,
    listStoredProjects,
    pruneUnchangedIssueSnapshots,
    syncProjects,
    updateProjectLoadSettings,
)
from src.redmine.redmine_client import fetchAllProjectsFromRedmine
from src.redmine.snapshots import (
    getIssueSnapshotCaptureStatus,
    isIssueSnapshotCaptureRunning,
    startProjectIssueSnapshotCaptureInBackground,
    startIssueSnapshotCaptureInBackground,
)


config = loadConfig()
app = FastAPI(title="Redmine Snapshot Viewer")


class ProjectSettingsUpdate(BaseModel):
    enabled_project_ids: list[int] = []
    partial_project_ids: list[int] = []


PAGE_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Redmine: проекты и срезы</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    :root {
      color-scheme: light;
      --bg: #ffffff;
      --panel: #ffffff;
      --panel-soft: #eef6f7;
      --sticky-top: 100px;
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

    .panel[id],
    article[id],
    section[id] {
      scroll-margin-top: 118px;
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

    .filter-input-name {
      width: 240px;
    }

    .toolbar-spacer {
      flex: 1 1 auto;
    }

    .checkbox-cell {
      width: 64px;
      white-space: nowrap;
    }

    .project-name-cell {
      white-space: nowrap;
      position: relative;
    }

    .project-sticky-3.project-name-cell {
      min-width: 260px;
    }

    .project-tree {
      display: inline-flex;
      align-items: center;
      position: relative;
      padding-left: calc(var(--tree-level, 0) * 18px);
    }

    .project-tree::before {
      content: "";
      position: absolute;
      left: calc((var(--tree-level, 0) - 1) * 18px + 6px);
      top: 50%;
      width: 10px;
      border-top: 1px solid #bfd0db;
      opacity: 0;
    }

    .project-tree.has-parent::before {
      opacity: 0.8;
    }

    table {
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      background: var(--panel);
      border-radius: 8px;
    }

    .table-wrap {
      max-height: calc(100vh - 180px);
      overflow: auto;
      position: relative;
      border: 1px solid var(--line);
      border-radius: 8px;
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
      position: sticky;
      top: 0;
      z-index: 4;
    }

    tr:last-child td {
      border-bottom: 0;
    }

    .project-row-disabled {
      color: #93a1af;
    }

    .project-row-disabled a {
      color: #93a1af;
      border-bottom-color: currentColor;
    }

    .project-row-disabled .project-enabled-checkbox,
    .project-row-disabled .project-partial-checkbox,
    .project-row-disabled #enableVisibleProjectsCheckbox {
      accent-color: #b4bec8;
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

    .project-id-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 56px;
      padding: 2px 0;
      color: var(--blue-302);
      background: transparent;
      border: 0;
      border-bottom: 1px dashed currentColor;
      border-radius: 0;
      box-shadow: none;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      text-decoration: none;
    }

    .project-id-button:hover {
      color: var(--orange-1585);
      border-bottom-style: solid;
      transform: none;
    }

    .project-id-button:disabled {
      color: var(--muted);
      border-bottom-color: transparent;
      background: transparent;
      cursor: default;
      opacity: 1;
    }

    .project-id-actions {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      white-space: nowrap;
    }

    .project-capture-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 22px;
      height: 22px;
      padding: 0;
      font-size: 0.8rem;
      line-height: 1;
      border-radius: 4px;
      background: var(--cyan-310);
      color: #16324a;
      box-shadow: none;
    }

    .project-capture-button:hover {
      transform: translateY(-1px);
    }

    .snapshot-filter-input {
      width: 240px;
    }

    @media (max-width: 700px) {
      .topbar {
        padding: 0 14px;
      }

      :root {
        --sticky-top: 136px;
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
          <a href="#data-load-section">Загрузка данных</a>
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

    <section class="grid" id="data-load-section">
      <article class="panel" id="project-actions">
        <h2>Проекты Redmine</h2>
        <p>Получает список проектов из Redmine, добавляет новые записи и обновляет измененные.</p>
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
        <p>
          <a class="project-link" href="/snapshot-rules" target="_blank" rel="noreferrer">Правила получения срезов</a>
        </p>
        <div class="row">
          <button id="captureSnapshotsButton" type="button">Получить срезы задач</button>
          <button id="recaptureSnapshotsButton" type="button">Обновить последние срезы</button>
        </div>
        <div class="status" id="captureStatus"></div>
      </article>

      <article class="panel" id="delete-snapshot">
        <h2>Удаление среза по дате</h2>
        <p>Удаляет все срезы и все строки задач за выбранную календарную дату.</p>
        <div class="row">
          <input id="snapshotDateInput" type="date">
          <button id="deleteSnapshotsButton" class="danger" type="button">Очистить срез на дату</button>
          <button id="pruneSnapshotsButton" type="button">Проредить срезы</button>
        </div>
        <div class="status" id="deleteStatus"></div>
      </article>
    </section>

    <section class="panel table-panel" id="projects-table">
      <h2>Проекты в базе данных</h2>
      <p class="meta" id="projectsCount">Загрузка списка проектов...</p>
      <div class="table-toolbar">
        <label for="projectsNameFilterInput">Фильтр по названию</label>
        <input
          id="projectsNameFilterInput"
          class="filter-input filter-input-name"
          type="text"
          placeholder="Введите часть названия"
        >
        <label for="projectsFactFilterInput">Мин. сумма факта за год по разработке и багфиксу</label>
        <input
          id="projectsFactFilterInput"
          class="filter-input"
          type="number"
          min="0"
          step="0.1"
          inputmode="decimal"
        >
        <label id="showDisabledProjectsLabel">
          <input id="showDisabledProjectsCheckbox" type="checkbox">
          <span>Показывать выключенные</span>
        </label>
        <span class="toolbar-spacer"></span>
        <button id="applyProjectsSettingsButton" type="button">Применить настройки сохранения</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th class="checkbox-cell project-sticky-1">
                <label>
                  <input id="enableVisibleProjectsCheckbox" type="checkbox">
                  Вкл.
                </label>
              </th>
              <th class="checkbox-cell">Част.</th>
              <th class="project-sticky-2">ID</th>
              <th class="project-sticky-3">Название</th>
              <th class="identifier-col">Идентификатор</th>
              <th>Базовая оценка, ч</th>
              <th>Разработка: оценка, ч</th>
              <th>Разработка: факт за год, ч</th>
              <th>Процессы разработки: план, ч</th>
              <th>Процессы разработки: факт за год, ч</th>
              <th>Ошибка: оценка, ч</th>
              <th>Ошибка: факт за год, ч</th>
              <th>Статус проекта</th>
              <th>Дата последнего среза</th>
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
      <div class="table-toolbar">
        <label for="snapshotRunsProjectFilterInput">Фильтр по проекту</label>
        <input
          id="snapshotRunsProjectFilterInput"
          class="filter-input snapshot-filter-input"
          type="text"
          placeholder="Введите часть названия проекта"
        >
      </div>
      <div class="filter-reset-wrap">
        <button type="button" class="filter-reset-button" id="resetSnapshotFiltersButton">Сбросить фильтр</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Дата среза</th>
              <th>Проект</th>
              <th>Идентификатор</th>
              <th>Задач</th>
              <th>Базовая оценка, ч</th>
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
    const enableVisibleProjectsCheckbox = document.getElementById("enableVisibleProjectsCheckbox");
    const showDisabledProjectsCheckbox = document.getElementById("showDisabledProjectsCheckbox");
    const snapshotDateInput = document.getElementById("snapshotDateInput");
    const projectsNameFilterInput = document.getElementById("projectsNameFilterInput");
    const projectsFactFilterInput = document.getElementById("projectsFactFilterInput");
    const snapshotRunsProjectFilterInput = document.getElementById("snapshotRunsProjectFilterInput");
    const applyProjectsSettingsButton = document.getElementById("applyProjectsSettingsButton");
    const refreshProjectsButton = document.getElementById("refreshProjectsButton");
    const captureSnapshotsButton = document.getElementById("captureSnapshotsButton");
    const recaptureSnapshotsButton = document.getElementById("recaptureSnapshotsButton");
    const deleteSnapshotsButton = document.getElementById("deleteSnapshotsButton");
    const pruneSnapshotsButton = document.getElementById("pruneSnapshotsButton");
    let captureStatusPollTimer = null;
    let allProjects = [];
    let allSnapshotRuns = [];
    const projectsNameFilterStorageKey = "redmine.projects.nameFilter";
    const projectsFactFilterStorageKey = "redmine.projects.factFilter.min";
    const showDisabledProjectsStorageKey = "redmine.projects.showDisabled";

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
        return "0,0";
      }

      return number.toFixed(1).replace(".", ",");
    }

    function getProjectsFactFilterValue() {
      const rawValue = String(projectsFactFilterInput.value || "").trim().replace(",", ".");
      const parsed = Number(rawValue);
      if (!rawValue || !Number.isFinite(parsed) || parsed < 0) {
        return 0;
      }

      return parsed;
    }

    function getProjectsNameFilterValue() {
      return String(projectsNameFilterInput.value || "").trim().toLocaleLowerCase("ru");
    }

    function getShowDisabledProjectsValue() {
      return Boolean(showDisabledProjectsCheckbox.checked);
    }

    function saveProjectsNameFilterValue() {
      window.localStorage.setItem(projectsNameFilterStorageKey, String(projectsNameFilterInput.value || ""));
    }

    function saveProjectsFactFilterValue() {
      window.localStorage.setItem(projectsFactFilterStorageKey, String(projectsFactFilterInput.value || ""));
    }

    function saveShowDisabledProjectsValue() {
      window.localStorage.setItem(showDisabledProjectsStorageKey, showDisabledProjectsCheckbox.checked ? "1" : "0");
    }

    function restoreProjectsFactFilterValue() {
      const savedValue = window.localStorage.getItem(projectsFactFilterStorageKey);
      if (savedValue !== null) {
        projectsFactFilterInput.value = savedValue;
      }
    }

    function restoreProjectsNameFilterValue() {
      const savedValue = window.localStorage.getItem(projectsNameFilterStorageKey);
      if (savedValue !== null) {
        projectsNameFilterInput.value = savedValue;
      }
    }

    function restoreShowDisabledProjectsValue() {
      const savedValue = window.localStorage.getItem(showDisabledProjectsStorageKey);
      if (savedValue !== null) {
        showDisabledProjectsCheckbox.checked = savedValue === "1";
      }
    }

    function localizeUi() {
      document.title = "Redmine: проекты и срезы";
      const texts = [
        [".brand-logo", "alt", "СМС-ИТ"],
        [".hero-nav", "aria-label", "Быстрый переход по разделам"],
        [".quick-links a:nth-child(1)", "textContent", "Загрузка данных"],
        [".quick-links a:nth-child(2)", "textContent", "Таблица проектов"],
        [".quick-links a:nth-child(3)", "textContent", "Таблица срезов"],
        [".hero h1", "textContent", "Анализ проектов Redmine"],
        ["#project-actions h2", "textContent", "Проекты Redmine"],
        ["#project-actions p", "textContent", "Получает список проектов из Redmine, добавляет новые записи и обновляет измененные."],
        ["#refreshProjectsButton", "textContent", "Обновить список проектов"],
        ["#snapshot-actions h2", "textContent", "Получение срезов задач"],
        ["#snapshot-actions p", "textContent", "Запрашивает срезы только для тех проектов, по которым на сегодняшнюю дату еще нет записи в базе данных."],
        ["#captureSnapshotsButton", "textContent", "Получить срезы задач"],
        ["#recaptureSnapshotsButton", "textContent", "Обновить последние срезы"],
        ["#delete-snapshot h2", "textContent", "Удаление среза по дате"],
        ["#delete-snapshot p", "textContent", "Удаляет все срезы и все строки задач за выбранную календарную дату."],
        ["#deleteSnapshotsButton", "textContent", "Очистить срез на дату"],
        ["#pruneSnapshotsButton", "textContent", "Проредить срезы"],
        ["#projects-table h2", "textContent", "Проекты в базе данных"],
        ["label[for='projectsNameFilterInput']", "textContent", "Фильтр по названию"],
        ["#projectsNameFilterInput", "placeholder", "Введите часть названия"],
        ["label[for='projectsFactFilterInput']", "textContent", "Мин. сумма факта за год по разработке и багфиксу"],
        ["#showDisabledProjectsLabel span", "textContent", "Показывать выключенные"],
        ["#applyProjectsSettingsButton", "textContent", "Применить настройки сохранения"],
        ["#snapshot-runs-table h2", "textContent", "Последние срезы задач"],
        ["label[for='snapshotRunsProjectFilterInput']", "textContent", "Фильтр по проекту"],
        ["#snapshotRunsProjectFilterInput", "placeholder", "Введите часть названия проекта"],
        ["#projectsCount", "textContent", "Загрузка списка проектов..."],
        ["#snapshotRunsCount", "textContent", "Загрузка списка срезов..."],
      ];

      for (const [selector, mode, value] of texts) {
        const element = document.querySelector(selector);
        if (!element) continue;
        if (mode === "textContent") {
          element.textContent = value;
        } else {
          element.setAttribute(mode, value);
        }
      }

      const projectsHeaders = [
        "Вкл.",
        "Част.",
        "ID",
        "Название",
        "Идентификатор",
        "Базовая оценка, ч",
        "Разработка: оценка, ч",
        "Разработка: факт за год, ч",
        "Процессы разработки: план, ч",
        "Процессы разработки: факт за год, ч",
        "Ошибка: оценка, ч",
        "Ошибка: факт за год, ч",
        "Статус проекта",
        "Дата последнего среза",
        "Обновлен в Redmine",
        "Синхронизирован",
      ];
      document.querySelectorAll("#projects-table thead th").forEach((element, index) => {
        if (index === 0) {
          const label = element.querySelector("label");
          if (label) label.lastChild.textContent = " Вкл.";
          return;
        }
        if (projectsHeaders[index]) {
          element.textContent = projectsHeaders[index];
        }
      });

      const snapshotHeaders = [
        "ID",
        "Дата среза",
        "Проект",
        "Идентификатор",
        "Задач",
        "Базовая оценка, ч",
        "План, ч",
        "Факт всего, ч",
        "Факт за год, ч",
        "Записан",
      ];
      document.querySelectorAll("#snapshot-runs-table thead th").forEach((element, index) => {
        if (snapshotHeaders[index]) {
          element.textContent = snapshotHeaders[index];
        }
      });
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

    function getDirectlyMatchedProjects(projects) {
      const minFactSum = getProjectsFactFilterValue();
      const nameFilter = getProjectsNameFilterValue();
      const showDisabledProjects = getShowDisabledProjectsValue();
      const matchedProjects = [];

      for (const project of projects) {
        if (!showDisabledProjects && !project.is_enabled) {
          continue;
        }

        const developmentFact = Number(project.development_spent_hours_year ?? 0);
        const bugFact = Number(project.bug_spent_hours_year ?? 0);
        if (developmentFact + bugFact < minFactSum) {
          continue;
        }

        const projectName = String(project.name || "").toLocaleLowerCase("ru");
        const identifier = String(project.identifier || "").toLocaleLowerCase("ru");
        if (nameFilter && !projectName.includes(nameFilter) && !identifier.includes(nameFilter)) {
          continue;
        }

        matchedProjects.push(project);
      }

      return matchedProjects;
    }

    function applyProjectsFilter(projects) {
      const directlyMatchedProjects = getDirectlyMatchedProjects(projects);
      const byId = new Map(projects.map((project) => [project.redmine_id, project]));
      const includedIds = new Set();

      for (const project of directlyMatchedProjects) {
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

    function getSnapshotRunsProjectFilterValue() {
      return String(snapshotRunsProjectFilterInput.value || "").trim().toLocaleLowerCase("ru");
    }

    function rerenderSnapshotRuns() {
      renderSnapshotRuns(allSnapshotRuns);
    }

    function updateEnableVisibleProjectsCheckbox() {
      if (!enableVisibleProjectsCheckbox) {
        return;
      }

      const directlyMatchedProjects = getDirectlyMatchedProjects(buildProjectHierarchy(allProjects));

      if (!directlyMatchedProjects.length) {
        enableVisibleProjectsCheckbox.checked = false;
        enableVisibleProjectsCheckbox.indeterminate = false;
        enableVisibleProjectsCheckbox.disabled = true;
        return;
      }

      const enabledCount = directlyMatchedProjects.filter((project) => Boolean(project.is_enabled)).length;
      enableVisibleProjectsCheckbox.disabled = false;
      enableVisibleProjectsCheckbox.checked = enabledCount === directlyMatchedProjects.length;
      enableVisibleProjectsCheckbox.indeterminate = enabledCount > 0 && enabledCount < directlyMatchedProjects.length;
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
            recaptureSnapshotsButton.disabled = false;
            return;
          }

          if (payload.created_runs || payload.captured_issues || payload.already_captured_projects) {
            setStatus(
              captureStatus,
              `\u0413\u043e\u0442\u043e\u0432\u043e: \u0441\u043e\u0437\u0434\u0430\u043d\u043e \u0441\u0440\u0435\u0437\u043e\u0432 ${payload.created_runs ?? 0}, \u0437\u0430\u0434\u0430\u0447 ${payload.captured_issues ?? 0}, \u0443\u0436\u0435 \u0431\u044b\u043b\u043e \u0441\u0440\u0435\u0437\u043e\u0432 \u043d\u0430 \u0441\u0435\u0433\u043e\u0434\u043d\u044f ${payload.already_captured_projects ?? 0}.`,
              "success"
            );
          }

          captureSnapshotsButton.disabled = false;
          recaptureSnapshotsButton.disabled = false;
          return;
        }

        const projectName = payload.current_project_name || payload.last_completed_project_name || "\u0431\u0435\u0437 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u044f";
        const processedProjects = Number(payload.processed_projects ?? 0);
        const totalProjects = Number(payload.total_projects ?? 0);
        const issuesPagesLoaded = Number(payload.current_project_issues_pages_loaded ?? 0);
        const issuesPagesTotal = Number(payload.current_project_issues_pages_total ?? 0);
        const timePagesLoaded = Number(payload.current_project_time_pages_loaded ?? 0);
        const timePagesTotal = Number(payload.current_project_time_pages_total ?? 0);
        const pagesParts = [];

        if (issuesPagesTotal > 0) {
          pagesParts.push(`\u0437\u0430\u0434\u0430\u0447\u0438 ${issuesPagesLoaded}/${issuesPagesTotal} \u0441\u0442\u0440.`);
        }

        if (timePagesTotal > 0) {
          pagesParts.push(`\u0442\u0440\u0443\u0434\u043e\u0437\u0430\u0442\u0440\u0430\u0442\u044b ${timePagesLoaded}/${timePagesTotal} \u0441\u0442\u0440.`);
        }

        const pagesSuffix = pagesParts.length ? ` (${pagesParts.join(", ")})` : "";
        setStatus(
          captureStatus,
          `\u041f\u043e\u043b\u0443\u0447\u0430\u0435\u043c \u0441\u0440\u0435\u0437\u044b \u0437\u0430\u0434\u0430\u0447 \u043f\u043e \u043f\u0440\u043e\u0435\u043a\u0442\u0443 ${projectName}... ${processedProjects}/${totalProjects}${pagesSuffix}`
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
      let renderedCount = 0;
      projectsTableBody.innerHTML = "";
      updateEnableVisibleProjectsCheckbox();
      projectsCount.textContent = `Проектов в базе: ${allProjects.length}. После фильтра: ${filteredProjects.length}`;

      if (!filteredProjects.length) {
        projectsTableBody.innerHTML = '<tr><td colspan="16">Проектов пока нет.</td></tr>';
        return;
      }

      for (const project of filteredProjects) {
        try {
          const redmineId = Number(project?.redmine_id ?? 0) || project?.redmine_id || "—";
          const identifier = String(project?.identifier ?? "");
          const projectIssuesUrl = identifier
            ? `https://redmine.sms-it.ru/projects/${encodeURIComponent(identifier)}/issues?utf8=%E2%9C%93&set_filter=1&type=IssueQuery&f%5B%5D=status_id&op%5Bstatus_id%5D=*&query%5Bsort_criteria%5D%5B0%5D%5B%5D=id&query%5Bsort_criteria%5D%5B0%5D%5B%5D=desc&t%5B%5D=cf_27&t%5B%5D=spent_hours&t%5B%5D=estimated_hours&c%5B%5D=tracker&c%5B%5D=parent&c%5B%5D=status&c%5B%5D=priority&c%5B%5D=subject&c%5B%5D=assigned_to&c%5B%5D=estimated_hours&saved_query_id=0&current_project_id=${encodeURIComponent(identifier)}`
            : "";
          const identifierHtml = identifier
            ? `<a class="project-link mono" href="${projectIssuesUrl}" target="_blank" rel="noreferrer">${identifier}</a>`
            : "—";
          const level = Math.max(Number(project?.hierarchy_level ?? 0) || 0, 0);
          const projectTreeClass = level > 0 ? "project-tree has-parent" : "project-tree";
          const row = document.createElement("tr");
          row.className = project?.is_enabled ? "" : "project-row-disabled";
          row.innerHTML = `
            <td class="checkbox-cell project-sticky-1"><input class="project-enabled-checkbox" type="checkbox" data-project-id="${redmineId}" ${project?.is_enabled ? "checked" : ""}></td>
            <td class="checkbox-cell"><input class="project-partial-checkbox" type="checkbox" data-project-id="${redmineId}" ${project?.partial_load ? "checked" : ""} ${project?.is_enabled ? "" : "disabled"}></td>
            <td class="mono project-sticky-2">
              <span class="project-id-actions">
                <a class="project-id-button mono" href="/projects/${encodeURIComponent(redmineId)}/latest-snapshot-issues" target="_blank" rel="noreferrer">${redmineId}</a>
                <button class="project-capture-button" type="button" data-project-id="${redmineId}" title="Получить срез по проекту" ${project?.is_enabled ? "" : "disabled"}>↓</button>
              </span>
            </td>
            <td class="project-name-cell project-sticky-3"><span class="${projectTreeClass}" style="--tree-level:${level};"><a class="project-link" href="/projects/${encodeURIComponent(redmineId)}/burndown" target="_blank" rel="noreferrer">${project?.name ?? "\u2014"}</a></span></td>
            <td>${identifierHtml}</td>
            <td>${formatHours(project?.baseline_estimate_hours)}</td>
            <td>${formatHours(project?.development_estimate_hours)}</td>
            <td>${formatHours(project?.development_spent_hours_year)}</td>
            <td>${formatHours(project?.development_process_estimate_hours)}</td>
            <td>${formatHours(project?.development_process_spent_hours_year)}</td>
            <td>${formatHours(project?.bug_estimate_hours)}</td>
            <td>${formatHours(project?.bug_spent_hours_year)}</td>
            <td>${project?.status ?? "—"}</td>
            <td class="mono">${project?.latest_snapshot_date ?? "—"}</td>
            <td>${formatDate(project?.updated_on)}</td>
            <td>${formatDate(project?.synced_at)}</td>
          `;
          projectsTableBody.appendChild(row);
          renderedCount += 1;
        } catch (error) {
          console.error("Не удалось отрисовать проект", project, error);
        }
      }

      if (!renderedCount) {
        projectsTableBody.innerHTML = '<tr><td colspan="16">Не удалось отрисовать проекты.</td></tr>';
        throw new Error("Не удалось отрисовать проекты.");
      }
    }

    function renderSnapshotRuns(snapshotRuns, totalCount = snapshotRuns.length) {
      allSnapshotRuns = Array.isArray(snapshotRuns) ? [...snapshotRuns] : [];
      const filterValue = getSnapshotRunsProjectFilterValue();
      const groupedRuns = new Map();
      for (const run of allSnapshotRuns) {
        const projectId = Number(run.project_redmine_id ?? 0);
        if (!groupedRuns.has(projectId)) {
          groupedRuns.set(projectId, []);
        }
        groupedRuns.get(projectId).push(run);
      }

      const visibleRuns = [];
      for (const runs of groupedRuns.values()) {
        runs.sort((left, right) => {
          const dateCompare = String(right.captured_for_date ?? "").localeCompare(String(left.captured_for_date ?? ""));
          if (dateCompare !== 0) {
            return dateCompare;
          }
          const capturedCompare = String(right.captured_at ?? "").localeCompare(String(left.captured_at ?? ""));
          if (capturedCompare !== 0) {
            return capturedCompare;
          }
          return Number(right.id ?? 0) - Number(left.id ?? 0);
        });
        visibleRuns.push(...runs.slice(0, 3));
      }

      visibleRuns.sort((left, right) => {
        const projectCompare = String(left.project_name || "").localeCompare(String(right.project_name || ""), "ru");
        if (projectCompare !== 0) {
          return projectCompare;
        }
        const dateCompare = String(right.captured_for_date ?? "").localeCompare(String(left.captured_for_date ?? ""));
        if (dateCompare !== 0) {
          return dateCompare;
        }
        return Number(right.id ?? 0) - Number(left.id ?? 0);
      });

      const filteredRuns = visibleRuns.filter((run) => {
        if (!filterValue) {
          return true;
        }
        const projectName = String(run.project_name || "").toLocaleLowerCase("ru");
        const projectIdentifier = String(run.project_identifier || "").toLocaleLowerCase("ru");
        return projectName.includes(filterValue) || projectIdentifier.includes(filterValue);
      });

      snapshotRunsTableBody.innerHTML = "";
      snapshotRunsCount.textContent = `Всего срезов в базе: ${totalCount}. Показано: ${filteredRuns.length}`;

      if (!filteredRuns.length) {
        snapshotRunsTableBody.innerHTML = '<tr><td colspan="10">Срезов пока нет.</td></tr>';
        return;
      }

      for (const run of filteredRuns) {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td class="mono">${run.id ?? "—"}</td>
          <td class="mono">${run.captured_for_date ?? "—"}</td>
          <td>${run.project_name ?? "—"}</td>
          <td class="mono">${run.project_identifier ?? "—"}</td>
          <td>${run.total_issues ?? 0}</td>
          <td>${formatHours(run.total_baseline_estimate_hours)}</td>
          <td>${formatHours(run.total_estimated_hours)}</td>
          <td>${formatHours(run.total_spent_hours)}</td>
          <td>${formatHours(run.total_spent_hours_year)}</td>
          <td>${formatDate(run.captured_at)}</td>
        `;
        snapshotRunsTableBody.appendChild(row);
      }
    }

    async function captureSnapshotForProject(projectId) {
      captureSnapshotsButton.disabled = true;
      setStatus(captureStatus, `Запускаем получение среза по проекту ${projectId}...`);

      try {
        const response = await fetch(`/api/issues/snapshots/capture-project/${encodeURIComponent(projectId)}`, { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Ошибка получения среза по проекту.");
        }

        if (payload.captured_for_date) {
          snapshotDateInput.value = payload.captured_for_date;
        }

        setStatus(captureStatus, payload.detail || "Фоновая загрузка среза по проекту запущена...");
        startCaptureProgressPolling();
      } catch (error) {
        stopCaptureProgressPolling();
        setStatus(captureStatus, error.message, "error");
        captureSnapshotsButton.disabled = false;
      }
    }

    async function loadProjects() {
      try {
        const response = await fetch("/api/projects");
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "Не удалось загрузить проекты из базы.");
        }

        renderProjects(payload.projects ?? []);
        setStatus(projectsStatus, "");
      } catch (error) {
        console.error("Ошибка загрузки проектов", error);
        try {
          renderProjects([]);
        } catch (renderError) {
          console.error("Ошибка очистки таблицы проектов", renderError);
        }
        setStatus(projectsStatus, error?.message || "Не удалось загрузить проекты из базы.", "error");
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

    async function applyProjectsSettings() {
      applyProjectsSettingsButton.disabled = true;
      setStatus(projectsStatus, "Сохраняем настройки проектов...");

      try {
        const enabledProjectIds = allProjects
          .filter((project) => project.is_enabled)
          .map((project) => Number(project.redmine_id));
        const partialProjectIds = allProjects
          .filter((project) => project.is_enabled && project.partial_load)
          .map((project) => Number(project.redmine_id));

        const response = await fetch("/api/projects/settings", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            enabled_project_ids: enabledProjectIds,
            partial_project_ids: partialProjectIds,
          }),
        });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Ошибка сохранения настроек проектов.");
        }

        renderProjects(payload.projects ?? []);
        setStatus(
          projectsStatus,
          `Готово: включено проектов ${payload.enabled_count ?? 0}, частичная загрузка у ${payload.partial_count ?? 0}.`,
          "success"
        );
      } catch (error) {
        setStatus(projectsStatus, error.message, "error");
      } finally {
        applyProjectsSettingsButton.disabled = false;
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
          `Готово: добавлено новых проектов ${payload.added_count ?? 0}, обновлено ${payload.updated_count ?? 0}.`,
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
      recaptureSnapshotsButton.disabled = true;
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
        recaptureSnapshotsButton.disabled = false;
      }
    }

    async function recaptureSnapshots() {
      captureSnapshotsButton.disabled = true;
      recaptureSnapshotsButton.disabled = true;
      setStatus(captureStatus, "Запускаем обновление последних срезов...");

      try {
        const response = await fetch("/api/issues/snapshots/recapture", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Ошибка обновления последних срезов.");
        }

        if (payload.captured_for_date) {
          snapshotDateInput.value = payload.captured_for_date;
        }

        setStatus(captureStatus, payload.detail || "Фоновое обновление последних срезов запущено...");
        startCaptureProgressPolling();
      } catch (error) {
        stopCaptureProgressPolling();
        setStatus(captureStatus, error.message, "error");
        captureSnapshotsButton.disabled = false;
        recaptureSnapshotsButton.disabled = false;
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

    async function pruneSnapshots() {
      pruneSnapshotsButton.disabled = true;
      setStatus(deleteStatus, "Прореживаем неизменные срезы...");

      try {
        const response = await fetch("/api/issues/snapshots/prune", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "Ошибка прореживания срезов.");
        }

        renderSnapshotRuns(payload.snapshot_runs ?? [], payload.total_count ?? 0);
        setStatus(
          deleteStatus,
          `Прореживание завершено: удалено срезов ${payload.deleted_runs ?? 0}, строк задач ${payload.deleted_items ?? 0}.`,
          "success"
        );
      } catch (error) {
        setStatus(deleteStatus, error.message, "error");
      } finally {
        pruneSnapshotsButton.disabled = false;
      }
    }

    refreshProjectsButton.addEventListener("click", refreshProjects);
    applyProjectsSettingsButton.addEventListener("click", applyProjectsSettings);
    captureSnapshotsButton.addEventListener("click", captureSnapshots);
    recaptureSnapshotsButton.addEventListener("click", recaptureSnapshots);
    deleteSnapshotsButton.addEventListener("click", deleteSnapshotsForDate);
    pruneSnapshotsButton.addEventListener("click", pruneSnapshots);
    projectsNameFilterInput.addEventListener("input", () => {
      saveProjectsNameFilterValue();
      rerenderProjects();
    });
    projectsFactFilterInput.addEventListener("input", () => {
      saveProjectsFactFilterValue();
      rerenderProjects();
    });
    snapshotRunsProjectFilterInput.addEventListener("input", rerenderSnapshotRuns);
    showDisabledProjectsCheckbox.addEventListener("change", () => {
      saveShowDisabledProjectsValue();
      rerenderProjects();
    });
    projectsTableBody.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement) || !target.classList.contains("project-capture-button")) {
        return;
      }

      const projectId = Number(target.dataset.projectId || 0);
      if (!projectId) {
        return;
      }

      captureSnapshotForProject(projectId);
    });
    projectsTableBody.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement)) {
        return;
      }

      if (target.classList.contains("project-enabled-checkbox")) {
        const projectId = Number(target.dataset.projectId || 0);
        const project = allProjects.find((item) => Number(item.redmine_id) === projectId);
        if (!project) {
          return;
        }

        project.is_enabled = target.checked;
        if (!project.is_enabled) {
          project.partial_load = false;
        }
        rerenderProjects();
        return;
      }

      if (target.classList.contains("project-partial-checkbox")) {
        const projectId = Number(target.dataset.projectId || 0);
        const project = allProjects.find((item) => Number(item.redmine_id) === projectId);
        if (!project) {
          return;
        }

        if (!project.is_enabled) {
          project.partial_load = false;
          rerenderProjects();
          return;
        }

        project.partial_load = target.checked;
        rerenderProjects();
      }
    });
    enableVisibleProjectsCheckbox.addEventListener("change", () => {
      const shouldEnable = enableVisibleProjectsCheckbox.checked;
      const filteredProjectIds = new Set(
        getDirectlyMatchedProjects(buildProjectHierarchy(allProjects)).map((project) => Number(project.redmine_id))
      );

      allProjects.forEach((project) => {
        if (filteredProjectIds.has(Number(project.redmine_id))) {
          project.is_enabled = shouldEnable;
          if (!shouldEnable) {
            project.partial_load = false;
          }
        }
      });

      rerenderProjects();
    });

    localizeUi();
    restoreProjectsNameFilterValue();
    restoreProjectsFactFilterValue();
    restoreShowDisabledProjectsValue();
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


def formatPageHours(value: object) -> str:
    try:
        return f"{float(value or 0):.1f}".replace(".", ",")
    except (TypeError, ValueError):
        return "0,0"


def formatPageDateTime(value: object) -> str:
    if not value:
        return "—"
    return str(value).replace("T", " ").replace("+00:00", " UTC")


def formatSnapshotPageDateTime(value: object) -> str:
    if not value:
        return "—"
    return str(value).replace("T", " ").replace("+00:00", " UTC")


def buildBurndownPlaceholderPage(projectRedmineId: int) -> str:
    projects = listStoredProjects()
    project = next((item for item in projects if int(item.get("redmine_id") or 0) == projectRedmineId), None)
    projectName = escape(str(project.get("name") or "—")) if project else "—"
    projectIdentifier = escape(str(project.get("identifier") or "—")) if project else "—"

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Диаграмма сгорания</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    :root {{
      color-scheme: light;
      --bg: #ffffff;
      --panel: #ffffff;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --blue: #375d77;
      --orange: #ff6c0e;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: var(--bg); color: var(--text); }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 24px 20px 48px; }}
    .back-link {{ color: var(--blue); text-decoration: none; font-weight: 600; }}
    .back-link:hover {{ color: var(--orange); }}
    h1 {{ margin: 18px 0 12px; font-size: clamp(2rem, 4vw, 3rem); line-height: 1.05; }}
    .meta {{ color: var(--muted); margin: 0 0 24px; font-size: 1rem; }}
    .placeholder {{ border: 1px dashed var(--line); border-radius: 8px; padding: 32px; color: var(--muted); background: #f9fcfd; }}
  </style>
</head>
<body>
  <main>
    <a class="back-link" href="/">← К списку проектов</a>
    <h1>Диаграмма сгорания проекта</h1>
    <p class="meta">Проект: {projectName}. Идентификатор: {projectIdentifier}. ID: {projectRedmineId}.</p>
    <div class="placeholder">Страница подготовки диаграммы готова. Саму диаграмму добавим следующим шагом.</div>
  </main>
</body>
</html>"""


def buildSnapshotRulesPage() -> str:
    previousYearStart = f"{datetime.now(UTC).year - 1}-01-01"

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Правила получения срезов</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    :root {{
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
      --shadow-soft: 0 12px 24px rgba(22, 50, 74, 0.06);
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}

    main {{
      max-width: 960px;
      margin: 0 auto;
      padding: 24px 20px 48px;
    }}

    .topbar {{
      position: sticky;
      top: 0;
      z-index: 10;
      background: #ffffff;
      border-bottom: 1px solid #eef2f6;
      padding: 16px 20px;
    }}

    .topbar-inner {{
      max-width: 960px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }}

    .brand-logo-wrap {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 220px;
      height: 72px;
    }}

    .brand-logo {{
      width: 100%;
      height: auto;
      display: block;
    }}

    .back-link {{
      color: var(--blue-302);
      font-weight: 700;
      text-decoration: none;
      border-bottom: 1px dashed currentColor;
      white-space: nowrap;
    }}

    h1 {{
      margin: 0 0 12px;
      font-size: clamp(2rem, 5vw, 3rem);
      line-height: 1.05;
      letter-spacing: -0.03em;
    }}

    .lead {{
      color: var(--muted);
      line-height: 1.6;
      margin: 0 0 22px;
    }}

    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 20px;
      box-shadow: var(--shadow-soft);
      margin-bottom: 18px;
    }}

    .panel h2 {{
      margin: 0 0 12px;
      font-size: 1.15rem;
    }}

    ul {{
      margin: 0;
      padding-left: 22px;
      line-height: 1.7;
    }}

    li + li {{
      margin-top: 8px;
    }}

    .note {{
      background: var(--panel-soft);
      border-left: 4px solid var(--cyan-310);
      padding: 14px 16px;
      color: var(--text);
    }}

    code {{
      font-family: Consolas, "Courier New", monospace;
      font-size: 0.95em;
    }}
  </style>
</head>
<body>
  <header class="topbar">
    <div class="topbar-inner">
      <a href="/" aria-label="На главную">
        <span class="brand-logo-wrap">
          <img
            class="brand-logo"
            src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg"
            alt="СМС-ИТ"
          >
        </span>
      </a>
      <a class="back-link" href="/" target="_self" rel="noreferrer">Вернуться на главную</a>
    </div>
  </header>

  <main>
    <h1>Правила получения срезов</h1>
    <p class="lead">
      На этой странице собраны текущие правила, по которым приложение получает задачи из Redmine
      и записывает проектные срезы в базу данных.
    </p>

    <section class="panel">
      <h2>Какие проекты участвуют</h2>
      <ul>
        <li>В загрузку попадают только проекты, у которых в таблице <code>Проекты в базе данных</code> включен флажок <code>Вкл.</code>.</li>
        <li>Для автоматического общего запуска берутся только проекты, у которых на текущую календарную дату еще нет среза.</li>
        <li>При ручном запуске по одному проекту переснимается только выбранный проект.</li>
        <li>Если <code>Вкл.</code> выключен, срезы по проекту не загружаются.</li>
        <li>Если включены <code>Вкл.</code> и <code>Част.</code>, используется частичная загрузка задач.</li>
        <li>Если включен только <code>Вкл.</code>, загружаются все задачи проекта.</li>
      </ul>
    </section>

    <section class="panel">
      <h2>Какие задачи попадают в срез</h2>
      <ul>
        <li>Берутся только задачи самого проекта, без подпроектов: в запросах используется <code>subproject_id=!* </code>.</li>
        <li>При полной загрузке берутся все задачи проекта.</li>
        <li>При частичной загрузке всегда попадают все открытые задачи проекта.</li>
        <li>При частичной загрузке из закрытых задач попадают задачи, закрытые начиная с <code>{previousYearStart}</code>.</li>
        <li>При частичной загрузке также попадают закрытые задачи, которые были обновлены начиная с <code>{previousYearStart}</code>, даже если закрыты раньше.</li>
        <li>Если одна и та же задача подходит сразу под несколько правил, в срез она записывается один раз.</li>
      </ul>
    </section>

    <section class="panel">
      <h2>Какие данные по задачам сохраняются</h2>
      <ul>
        <li>Сохраняются основные поля задачи: трекер, статус, приоритет, исполнитель, версия, даты и проценты выполнения.</li>
        <li><code>Базовая оценка</code> читается из кастомного поля Redmine с названием <code>Базовая оценка</code>.</li>
        <li><code>План</code> берется из стандартного поля <code>estimated_hours</code>.</li>
        <li><code>Факт за год</code> считается по трудозатратам текущего года, а не за всю историю задачи.</li>
      </ul>
    </section>

    <section class="panel">
      <h2>Как срез записывается в базу</h2>
      <ul>
        <li>Сначала приложение полностью получает задачи и трудозатраты из Redmine.</li>
        <li>Только после полного получения данных по проекту создается запись среза и строки задач в базе.</li>
        <li>За одни сутки у проекта хранится только один срез.</li>
        <li>Дата среза хранится отдельно от времени, поэтому повторно автоматом тот же проект за те же сутки не переснимается.</li>
      </ul>
    </section>

    <section class="panel note">
      Если правила получения будут меняться, эта страница должна обновляться вместе с кодом, чтобы описание всегда совпадало с фактической логикой.
    </section>
  </main>
</body>
</html>"""


def buildLatestSnapshotIssuesPageClean(projectRedmineId: int, capturedForDate: str | None = None) -> str:
    snapshotPayload = getSnapshotIssuesForProjectByDate(projectRedmineId, capturedForDate)
    snapshotRun = snapshotPayload["snapshot_run"]
    issues = snapshotPayload["issues"]
    availableDates = [str(value) for value in snapshotPayload.get("available_dates") or []]

    if snapshotRun is None:
        optionsHtml = "".join(
            f'<option value="{escape(dateValue)}">{escape(dateValue)}</option>' for dateValue in availableDates
        )
        return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Задачи последнего среза</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    body {{ margin: 0; font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: #ffffff; color: #16324a; }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 24px 20px 48px; }}
    .back-link {{ color: #375d77; text-decoration: none; font-weight: 600; }}
    h1 {{ margin: 18px 0 12px; font-size: 2rem; }}
    .meta {{ color: #64798d; margin: 0 0 24px; }}
  </style>
</head>
<body>
    <main>
      <a class="back-link" href="/">← К списку проектов</a>
      <h1>Задачи последнего среза проекта</h1>
      <form method="get">
        <label for="capturedForDate">Дата среза</label>
        <select id="capturedForDate" name="captured_for_date" onchange="this.form.submit()">
          <option value="">Последний срез</option>
          {optionsHtml}
        </select>
      </form>
      <p class="meta">Для проекта с ID {projectRedmineId} срезы пока не найдены.</p>
    </main>
  </body>
</html>"""

    issueRowsHtml: list[str] = []
    for issue in issues:
        issueId = issue.get("issue_redmine_id", "—")
        subjectValue = escape(str(issue.get("subject") or "—"))
        trackerValue = escape(str(issue.get("tracker_name") or "—"))
        statusValue = escape(str(issue.get("status_name") or "—"))
        doneRatioValue = escape(str(issue.get("done_ratio") if issue.get("done_ratio") is not None else 0))
        baselineHoursValue = float(issue.get("baseline_estimate_hours") or 0)
        estimatedHoursValue = float(issue.get("estimated_hours") or 0)
        spentHoursValue = float(issue.get("spent_hours") or 0)
        spentHoursYearValue = float(issue.get("spent_hours_year") or 0)
        closedOnValue = escape(formatSnapshotPageDateTime(issue.get("closed_on")))
        assignedToValue = escape(str(issue.get("assigned_to_name") or "—"))
        fixedVersionValue = escape(str(issue.get("fixed_version_name") or "—"))
        issueRowsHtml.append(
            f"""
            <tr
              data-issue-id="{escape(str(issueId))}"
              data-subject="{subjectValue}"
              data-tracker="{trackerValue}"
              data-status="{statusValue}"
              data-done-ratio="{doneRatioValue}"
              data-baseline-estimate-hours="{baselineHoursValue}"
              data-estimated-hours="{estimatedHoursValue}"
              data-spent-hours="{spentHoursValue}"
              data-spent-hours-year="{spentHoursYearValue}"
              data-closed-on="{closedOnValue}"
              data-assigned-to="{assignedToValue}"
              data-fixed-version="{fixedVersionValue}"
            >
              <td class="mono"><a class="issue-link" href="https://redmine.sms-it.ru/issues/{issueId}" target="_blank" rel="noreferrer">{issueId}</a></td>
              <td class="subject-col">{subjectValue}</td>
              <td>{trackerValue}</td>
              <td>{statusValue}</td>
              <td>{doneRatioValue}</td>
              <td>{formatPageHours(baselineHoursValue)}</td>
              <td>{formatPageHours(estimatedHoursValue)}</td>
              <td>{formatPageHours(spentHoursValue)}</td>
              <td>{formatPageHours(spentHoursYearValue)}</td>
              <td>{closedOnValue}</td>
              <td>{assignedToValue}</td>
              <td class="version-col">{fixedVersionValue}</td>
            </tr>
            """
        )

    if not issueRowsHtml:
        issueRowsHtml.append('<tr><td colspan="12">В последнем срезе задач нет.</td></tr>')

    totalBaselineEstimateHours = sum(float(issue.get("baseline_estimate_hours") or 0) for issue in issues)
    totalEstimatedHours = sum(float(issue.get("estimated_hours") or 0) for issue in issues)
    totalSpentHours = sum(float(issue.get("spent_hours") or 0) for issue in issues)
    totalSpentHoursYear = sum(float(issue.get("spent_hours_year") or 0) for issue in issues)
    developmentEstimateHours = 0.0
    developmentSpentHours = 0.0
    developmentSpentHoursYear = 0.0
    developmentProcessEstimateHours = 0.0
    developmentProcessSpentHours = 0.0
    developmentProcessSpentHoursYear = 0.0
    bugEstimateHours = 0.0
    bugSpentHours = 0.0
    bugSpentHoursYear = 0.0

    for issue in issues:
        trackerName = str(issue.get("tracker_name") or "").strip().lower()
        if trackerName == "разработка":
            developmentEstimateHours += float(issue.get("estimated_hours") or 0)
            developmentSpentHours += float(issue.get("spent_hours") or 0)
            developmentSpentHoursYear += float(issue.get("spent_hours_year") or 0)
        elif trackerName == "процессы разработки":
            developmentProcessEstimateHours += float(issue.get("estimated_hours") or 0)
            developmentProcessSpentHours += float(issue.get("spent_hours") or 0)
            developmentProcessSpentHoursYear += float(issue.get("spent_hours_year") or 0)
        elif trackerName == "ошибка":
            bugEstimateHours += float(issue.get("estimated_hours") or 0)
            bugSpentHours += float(issue.get("spent_hours") or 0)
            bugSpentHoursYear += float(issue.get("spent_hours_year") or 0)

    projectName = escape(str(snapshotRun.get("project_name") or "—"))
    capturedForDate = escape(str(snapshotRun.get("captured_for_date") or "—"))
    selectedDate = str(snapshotRun.get("captured_for_date") or "")
    optionsHtml = ["<option value=\"\">Последний срез</option>"]
    for dateValue in availableDates:
        selectedAttr = " selected" if dateValue == selectedDate else ""
        optionsHtml.append(f'<option value="{escape(dateValue)}"{selectedAttr}>{escape(dateValue)}</option>')

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Задачи последнего среза</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
    <style>
      :root {{
        color-scheme: light;
        --bg: #ffffff;
        --panel: #ffffff;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --blue: #375d77;
      --orange: #ff6c0e;
    }}
      * {{ box-sizing: border-box; }}
      body {{ margin: 0; font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: var(--bg); color: var(--text); }}
      main {{ max-width: 1440px; margin: 0 auto; padding: 24px 20px 48px; }}
      .toolbar {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin: 0 0 16px; }}
      .back-link {{ color: var(--blue); text-decoration: none; font-weight: 600; }}
      .back-link:hover {{ color: var(--orange); }}
      h1 {{ margin: 18px 0 12px; font-size: clamp(2rem, 4vw, 3rem); line-height: 1.05; }}
      form {{ display: flex; gap: 10px; align-items: center; margin: 0; }}
      label {{ font-weight: 600; }}
      select {{ border: 1px solid var(--line); border-radius: 6px; padding: 8px 10px; font: inherit; }}
      button {{ border: 0; border-radius: 6px; padding: 10px 14px; font: inherit; font-weight: 600; cursor: pointer; background: #ff6c0e; color: #ffffff; }}
      .meta {{ color: var(--muted); margin: 0 0 24px; font-size: 1rem; }}
      .action-status {{ color: var(--muted); margin: 0 0 18px; min-height: 22px; }}
      .summary-block {{ margin: 0 0 20px; }}
      .summary-table {{ width: 100%; border-collapse: collapse; table-layout: fixed; border: 1px solid var(--line); }}
      .summary-table th,
      .summary-table td {{ border: 1px solid var(--line); padding: 12px 10px; vertical-align: middle; }}
      .summary-table thead th {{ position: static; background: #ffffff; color: var(--text); text-transform: none; font-size: 0.98rem; letter-spacing: 0; }}
      .summary-table tbody th {{ background: #ffffff; color: var(--text); text-transform: none; font-size: 1rem; font-weight: 500; }}
      .summary-table .summary-metric {{ text-align: right; font-size: 1.02rem; font-weight: 700; color: #173b5a; white-space: nowrap; }}
      .summary-table .summary-empty {{ background: #ffffff; }}
      .filter-input-table,
      .filter-select-table,
      .filter-number-value,
      .filter-number-op {{ width: 100%; border: 1px solid var(--line); border-radius: 6px; padding: 7px 8px; font: inherit; background: #ffffff; color: var(--text); }}
      .filter-select-table {{ min-height: 92px; }}
      .filter-number-op,
      .filter-number-value {{ width: 64px; }}
      .filter-number-wrap {{ display: flex; flex-direction: column; align-items: flex-start; gap: 4px; }}
      .filter-head th {{ top: var(--snapshot-filter-top, 44px); background: #f7fbfc; padding-top: 8px; padding-bottom: 8px; z-index: 3; text-transform: none; box-shadow: inset 0 1px 0 #d9e5eb; }}
      .filter-reset-wrap {{ display: flex; justify-content: flex-end; align-items: center; gap: 10px; margin: 0 0 10px; }}
      .filter-reset-button {{ background: #375d77; color: #ffffff; }}
      .filter-tip {{ color: var(--muted); font-size: 0.92rem; }}
      .table-wrap {{ max-height: calc(100vh - 220px); overflow: auto; border: 1px solid var(--line); border-radius: 8px; }}
      table {{ width: 100%; border-collapse: separate; border-spacing: 0; background: var(--panel); }}
      #snapshotIssuesTable {{ table-layout: fixed; }}
      th, td {{ text-align: left; padding: 12px 14px; border-bottom: 1px solid var(--line); vertical-align: top; }}
      th {{ position: sticky; top: 0; z-index: 2; background: #eef6f7; color: #426179; text-transform: uppercase; font-size: 0.88rem; }}
      tr:last-child td {{ border-bottom: 0; }}
      .mono {{ font-family: Consolas, "Courier New", monospace; font-size: 0.95rem; white-space: nowrap; }}
      .issue-link {{ color: var(--blue); text-decoration: none; border-bottom: 1px dashed currentColor; font-weight: 700; }}
      .issue-link:hover {{ color: var(--orange); border-bottom-style: solid; }}
      .subject-col {{ width: 32%; min-width: 32%; max-width: 32%; white-space: normal; word-break: break-word; }}
      .version-col {{ width: 18%; min-width: 18%; max-width: 18%; white-space: normal; word-break: break-word; }}
  </style>
</head>
  <body>
    <main>
      <a class="back-link" href="/">← К списку проектов</a>
      <h1>Задачи последнего среза проекта</h1>
      <div class="toolbar">
      <form method="get">
        <label for="capturedForDate">Дата среза</label>
        <select id="capturedForDate" name="captured_for_date" onchange="this.form.submit()">
          {''.join(optionsHtml)}
        </select>
      </form>
      <button type="button" id="recaptureSnapshotButton">Обновить последний срез</button>
      <button type="button" id="deleteSnapshotButton">Удалить выбранный срез</button>
      </div>
      <div class="action-status" id="snapshotActionStatus"></div>
      <p class="meta">Проект: {projectName}. Дата среза: {capturedForDate}. Задач: <span id="visibleIssuesCount">{len(issues)}</span> из {len(issues)}.</p>
      <div class="summary-block">
        <table class="summary-table">
          <thead>
            <tr>
              <th style="width: 33%"></th>
              <th>Базовая оценка</th>
              <th>План</th>
              <th>Факт за год</th>
              <th>Факт всего</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <th>Все задачи</th>
              <td class="summary-metric" id="summaryBaselineEstimate">{formatPageHours(totalBaselineEstimateHours)}</td>
              <td class="summary-metric" id="summaryEstimated">{formatPageHours(totalEstimatedHours)}</td>
              <td class="summary-metric" id="summarySpentYear">{formatPageHours(totalSpentHoursYear)}</td>
              <td class="summary-metric" id="summarySpent">{formatPageHours(totalSpentHours)}</td>
            </tr>
            <tr>
              <th>Разработка, ч</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentEstimated">{formatPageHours(developmentEstimateHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentSpentYear">{formatPageHours(developmentSpentHoursYear)}</td>
              <td class="summary-metric" id="summaryDevelopmentSpent">{formatPageHours(developmentSpentHours)}</td>
            </tr>
            <tr>
              <th>Процессы разработки, ч</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentProcessEstimated">{formatPageHours(developmentProcessEstimateHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentProcessSpentYear">{formatPageHours(developmentProcessSpentHoursYear)}</td>
              <td class="summary-metric" id="summaryDevelopmentProcessSpent">{formatPageHours(developmentProcessSpentHours)}</td>
            </tr>
            <tr>
              <th>Ошибка, ч</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryBugEstimated">{formatPageHours(bugEstimateHours)}</td>
              <td class="summary-metric" id="summaryBugSpentYear">{formatPageHours(bugSpentHoursYear)}</td>
              <td class="summary-metric" id="summaryBugSpent">{formatPageHours(bugSpentHours)}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="filter-reset-wrap">
        <span class="filter-tip">Фильтры применяются к таблице и суммам выше.</span>
        <button type="button" class="filter-reset-button" id="resetSnapshotFiltersButton">Сбросить фильтр</button>
      </div>
      <div class="table-wrap">
        <table id="snapshotIssuesTable">
        <thead>
          <tr>
            <th>ID</th>
            <th class="subject-col">Тема</th>
            <th>Трекер</th>
            <th>Статус</th>
            <th>Готово, %</th>
            <th>Базовая оценка, ч</th>
            <th>План, ч</th>
            <th>Факт всего, ч</th>
            <th>Факт за год, ч</th>
            <th>Закрыта</th>
            <th>Исполнитель</th>
            <th class="version-col">Версия</th>
          </tr>
          <tr class="filter-head">
            <th><input class="filter-input-table" type="text" data-filter-key="issueId" data-filter-role="text"></th>
            <th><input class="filter-input-table" type="text" data-filter-key="subject" data-filter-role="text"></th>
            <th><select class="filter-select-table" multiple data-filter-key="tracker" data-filter-role="multi"></select></th>
            <th><select class="filter-select-table" multiple data-filter-key="status" data-filter-role="multi"></select></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="doneRatio" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="1" data-filter-key="doneRatio" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="baseline" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="baseline" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="estimated" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="estimated" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="spent" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="spent" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="spentYear" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="spentYear" data-filter-role="value"></div></th>
            <th><input class="filter-input-table" type="text" data-filter-key="closedOn" data-filter-role="text"></th>
            <th><input class="filter-input-table" type="text" data-filter-key="assignedTo" data-filter-role="text"></th>
            <th><input class="filter-input-table" type="text" data-filter-key="fixedVersion" data-filter-role="text"></th>
          </tr>
        </thead>
        <tbody id="snapshotIssuesTableBody">
          {''.join(issueRowsHtml)}
        </tbody>
      </table>
    </div>
    <script>
      const snapshotActionStatus = document.getElementById("snapshotActionStatus");
      const visibleIssuesCount = document.getElementById("visibleIssuesCount");
      const snapshotIssuesTableBody = document.getElementById("snapshotIssuesTableBody");
      const snapshotIssuesTable = document.getElementById("snapshotIssuesTable");
      const snapshotIssueRows = Array.from(snapshotIssuesTableBody?.querySelectorAll("tr") || []);
      const textFilterInputs = Array.from(document.querySelectorAll("[data-filter-role='text']"));
      const multiSelectFilters = Array.from(document.querySelectorAll("[data-filter-role='multi']"));
      const numericFilterControls = Array.from(document.querySelectorAll("[data-filter-role='op'], [data-filter-role='value']"));
      const snapshotFilterInputs = [...textFilterInputs, ...multiSelectFilters, ...numericFilterControls];
      const resetSnapshotFiltersButton = document.getElementById("resetSnapshotFiltersButton");
      const summaryBaselineEstimate = document.getElementById("summaryBaselineEstimate");
      const summaryEstimated = document.getElementById("summaryEstimated");
      const summarySpent = document.getElementById("summarySpent");
      const summarySpentYear = document.getElementById("summarySpentYear");
      const summaryDevelopmentEstimated = document.getElementById("summaryDevelopmentEstimated");
      const summaryDevelopmentSpent = document.getElementById("summaryDevelopmentSpent");
      const summaryDevelopmentSpentYear = document.getElementById("summaryDevelopmentSpentYear");
      const summaryDevelopmentProcessEstimated = document.getElementById("summaryDevelopmentProcessEstimated");
      const summaryDevelopmentProcessSpent = document.getElementById("summaryDevelopmentProcessSpent");
      const summaryDevelopmentProcessSpentYear = document.getElementById("summaryDevelopmentProcessSpentYear");
      const summaryBugEstimated = document.getElementById("summaryBugEstimated");
      const summaryBugSpent = document.getElementById("summaryBugSpent");
      const summaryBugSpentYear = document.getElementById("summaryBugSpentYear");

      function setActionStatus(message) {{
        if (snapshotActionStatus) {{
          snapshotActionStatus.textContent = message;
        }}
      }}

      function formatFilterHours(value) {{
        const parsed = Number(value ?? 0);
        if (!Number.isFinite(parsed)) {{
          return "0,0";
        }}
        return parsed.toFixed(1).replace(".", ",");
      }}

      function updateSnapshotFilterHeaderOffset() {{
        const headRow = snapshotIssuesTable?.querySelector("thead tr:first-child");
        const tableElement = snapshotIssuesTable;
        if (!headRow || !tableElement) {{
          return;
        }}
        const height = Math.ceil(headRow.getBoundingClientRect().height || 44);
        tableElement.style.setProperty("--snapshot-filter-top", `${{height}}px`);
      }}

      function populateSnapshotMultiSelects() {{
        for (const select of multiSelectFilters) {{
          const filterKey = select.dataset.filterKey;
          if (!filterKey) {{
            continue;
          }}

          const selectedValues = new Set(Array.from(select.selectedOptions).map((option) => option.value));
          const values = Array.from(new Set(
            snapshotIssueRows
              .map((row) => String(row.dataset[filterKey] || "").trim())
              .filter(Boolean)
          )).sort((left, right) => left.localeCompare(right, "ru"));

          select.innerHTML = "";
          for (const value of values) {{
            const option = document.createElement("option");
            option.value = value;
            option.textContent = value;
            option.selected = selectedValues.has(value);
            select.appendChild(option);
          }}

        }}
      }}

      function matchesNumericFilter(rawValue, operator, filterValue) {{
        if (filterValue === "" || operator === "") {{
          return true;
        }}

        const left = Number(rawValue ?? 0);
        const right = Number(String(filterValue).replace(",", "."));
        if (!Number.isFinite(left) || !Number.isFinite(right)) {{
          return false;
        }}

        if (operator === ">") {{
          return left > right;
        }}
        if (operator === "<") {{
          return left < right;
        }}
        return left === right;
      }}

      function updateSnapshotSummaries(rows) {{
        let baselineEstimate = 0;
        let estimated = 0;
        let spent = 0;
        let spentYear = 0;
        let developmentEstimated = 0;
        let developmentSpent = 0;
        let developmentSpentYear = 0;
        let developmentProcessEstimated = 0;
        let developmentProcessSpent = 0;
        let developmentProcessSpentYear = 0;
        let bugEstimated = 0;
        let bugSpent = 0;
        let bugSpentYear = 0;

        for (const row of rows) {{
          const tracker = String(row.dataset.tracker || "").trim().toLowerCase();
          const rowBaselineEstimate = Number(row.dataset.baselineEstimateHours || 0);
          const rowEstimated = Number(row.dataset.estimatedHours || 0);
          const rowSpent = Number(row.dataset.spentHours || 0);
          const rowSpentYear = Number(row.dataset.spentHoursYear || 0);

          baselineEstimate += rowBaselineEstimate;
          estimated += rowEstimated;
          spent += rowSpent;
          spentYear += rowSpentYear;

          if (tracker === "разработка") {{
            developmentEstimated += rowEstimated;
            developmentSpent += rowSpent;
            developmentSpentYear += rowSpentYear;
          }} else if (tracker === "процессы разработки") {{
            developmentProcessEstimated += rowEstimated;
            developmentProcessSpent += rowSpent;
            developmentProcessSpentYear += rowSpentYear;
          }} else if (tracker === "ошибка") {{
            bugEstimated += rowEstimated;
            bugSpent += rowSpent;
            bugSpentYear += rowSpentYear;
          }}
        }}

        if (visibleIssuesCount) visibleIssuesCount.textContent = String(rows.length);
        if (summaryBaselineEstimate) summaryBaselineEstimate.textContent = formatFilterHours(baselineEstimate);
        if (summaryEstimated) summaryEstimated.textContent = formatFilterHours(estimated);
        if (summarySpent) summarySpent.textContent = formatFilterHours(spent);
        if (summarySpentYear) summarySpentYear.textContent = formatFilterHours(spentYear);
        if (summaryDevelopmentEstimated) summaryDevelopmentEstimated.textContent = formatFilterHours(developmentEstimated);
        if (summaryDevelopmentSpent) summaryDevelopmentSpent.textContent = formatFilterHours(developmentSpent);
        if (summaryDevelopmentSpentYear) summaryDevelopmentSpentYear.textContent = formatFilterHours(developmentSpentYear);
        if (summaryDevelopmentProcessEstimated) summaryDevelopmentProcessEstimated.textContent = formatFilterHours(developmentProcessEstimated);
        if (summaryDevelopmentProcessSpent) summaryDevelopmentProcessSpent.textContent = formatFilterHours(developmentProcessSpent);
        if (summaryDevelopmentProcessSpentYear) summaryDevelopmentProcessSpentYear.textContent = formatFilterHours(developmentProcessSpentYear);
        if (summaryBugEstimated) summaryBugEstimated.textContent = formatFilterHours(bugEstimated);
        if (summaryBugSpent) summaryBugSpent.textContent = formatFilterHours(bugSpent);
        if (summaryBugSpentYear) summaryBugSpentYear.textContent = formatFilterHours(bugSpentYear);
      }}

      function applySnapshotTableFilters() {{
        const textFilters = Object.fromEntries(textFilterInputs.map((input) => [
          input.dataset.filterKey,
          String(input.value || "").trim().toLocaleLowerCase("ru")
        ]));

        const multiFilters = Object.fromEntries(multiSelectFilters.map((select) => [
          select.dataset.filterKey,
          new Set(Array.from(select.selectedOptions).map((option) => option.value.toLocaleLowerCase("ru")))
        ]));

        const numericFilters = {{
          doneRatio: {{
            operator: String(document.querySelector('[data-filter-key="doneRatio"][data-filter-role="op"]')?.value || ""),
            value: String(document.querySelector('[data-filter-key="doneRatio"][data-filter-role="value"]')?.value || "").trim(),
          }},
          baseline: {{
            operator: String(document.querySelector('[data-filter-key="baseline"][data-filter-role="op"]')?.value || ""),
            value: String(document.querySelector('[data-filter-key="baseline"][data-filter-role="value"]')?.value || "").trim(),
          }},
          estimated: {{
            operator: String(document.querySelector('[data-filter-key="estimated"][data-filter-role="op"]')?.value || ""),
            value: String(document.querySelector('[data-filter-key="estimated"][data-filter-role="value"]')?.value || "").trim(),
          }},
          spent: {{
            operator: String(document.querySelector('[data-filter-key="spent"][data-filter-role="op"]')?.value || ""),
            value: String(document.querySelector('[data-filter-key="spent"][data-filter-role="value"]')?.value || "").trim(),
          }},
          spentYear: {{
            operator: String(document.querySelector('[data-filter-key="spentYear"][data-filter-role="op"]')?.value || ""),
            value: String(document.querySelector('[data-filter-key="spentYear"][data-filter-role="value"]')?.value || "").trim(),
          }},
        }};

        const visibleRows = [];
        for (const row of snapshotIssueRows) {{
          const trackerValue = String(row.dataset.tracker || "").toLocaleLowerCase("ru");
          const statusValue = String(row.dataset.status || "").toLocaleLowerCase("ru");
          const matches =
            String(row.dataset.issueId || "").toLocaleLowerCase("ru").includes(textFilters.issueId || "") &&
            String(row.dataset.subject || "").toLocaleLowerCase("ru").includes(textFilters.subject || "") &&
            (!multiFilters.tracker?.size || multiFilters.tracker.has(trackerValue)) &&
            (!multiFilters.status?.size || multiFilters.status.has(statusValue)) &&
            matchesNumericFilter(row.dataset.doneRatio, numericFilters.doneRatio.operator, numericFilters.doneRatio.value) &&
            matchesNumericFilter(row.dataset.baselineEstimateHours, numericFilters.baseline.operator, numericFilters.baseline.value) &&
            matchesNumericFilter(row.dataset.estimatedHours, numericFilters.estimated.operator, numericFilters.estimated.value) &&
            matchesNumericFilter(row.dataset.spentHours, numericFilters.spent.operator, numericFilters.spent.value) &&
            matchesNumericFilter(row.dataset.spentHoursYear, numericFilters.spentYear.operator, numericFilters.spentYear.value) &&
            String(row.dataset.closedOn || "").toLocaleLowerCase("ru").includes(textFilters.closedOn || "") &&
            String(row.dataset.assignedTo || "").toLocaleLowerCase("ru").includes(textFilters.assignedTo || "") &&
            String(row.dataset.fixedVersion || "").toLocaleLowerCase("ru").includes(textFilters.fixedVersion || "");

          row.style.display = matches ? "" : "none";
          if (matches) {{
            visibleRows.push(row);
          }}
        }}

        updateSnapshotSummaries(visibleRows);
      }}

      function resetSnapshotTableFilters() {{
        textFilterInputs.forEach((input) => {{
          input.value = "";
        }});
        multiSelectFilters.forEach((select) => {{
          Array.from(select.options).forEach((option) => {{
            option.selected = false;
          }});
        }});
        numericFilterControls.forEach((input) => {{
          input.value = "";
        }});
        applySnapshotTableFilters();
      }}

      async function pollRecaptureStatus(targetDate) {{
        try {{
          const response = await fetch("/api/issues/snapshots/capture-status");
          const payload = await response.json();

          if (payload.error_message) {{
            setActionStatus(payload.error_message);
            return;
          }}

          if (!payload.is_running) {{
            window.location.href = `/projects/{projectRedmineId}/latest-snapshot-issues?captured_for_date=${{encodeURIComponent(targetDate)}}`;
            return;
          }}

          const projectName = payload.current_project_name || payload.last_completed_project_name || "без названия";
          const issuesPagesLoaded = Number(payload.current_project_issues_pages_loaded ?? 0);
          const issuesPagesTotal = Number(payload.current_project_issues_pages_total ?? 0);
          const timePagesLoaded = Number(payload.current_project_time_pages_loaded ?? 0);
          const timePagesTotal = Number(payload.current_project_time_pages_total ?? 0);
          const progressParts = [];

          if (issuesPagesTotal > 0) {{
            progressParts.push(`задачи ${{
              issuesPagesLoaded
            }}/${{issuesPagesTotal}} стр.`);
          }}

          if (timePagesTotal > 0) {{
            progressParts.push(`трудозатраты ${{
              timePagesLoaded
            }}/${{timePagesTotal}} стр.`);
          }} else if (issuesPagesTotal > 0 && issuesPagesLoaded >= issuesPagesTotal) {{
            progressParts.push("готовим трудозатраты");
          }}

          const progressSuffix = progressParts.length ? ` (${{
            progressParts.join(", ")
          }})` : "";
          setActionStatus(`Получаем срез по проекту ${{projectName}}${{progressSuffix}}`);
          window.setTimeout(() => pollRecaptureStatus(targetDate), 1500);
        }} catch (error) {{
          setActionStatus("Не удалось получить статус повторного среза.");
        }}
      }}

      document.getElementById("recaptureSnapshotButton")?.addEventListener("click", async () => {{
        setActionStatus("Запускаем повторное получение среза...");
        const response = await fetch("/api/issues/snapshots/recapture-project/{projectRedmineId}", {{
          method: "POST"
        }});
        const payload = await response.json();

        if (!response.ok) {{
          window.alert(payload.detail || "Не удалось запустить повторное получение среза.");
          setActionStatus("");
          return;
        }}

        const targetDate = payload.captured_for_date || "{selectedDate}";
        setActionStatus(payload.detail || "Повторное получение среза запущено.");
        pollRecaptureStatus(targetDate);
      }});

      document.getElementById("deleteSnapshotButton")?.addEventListener("click", async () => {{
        if (!window.confirm("Удалить выбранный срез?")) {{
          return;
        }}

        const response = await fetch("/api/issues/snapshots/project/{projectRedmineId}/by-date?captured_for_date={capturedForDate}", {{
          method: "DELETE"
        }});
        const payload = await response.json();
        if (!response.ok) {{
          window.alert(payload.detail || "Не удалось удалить срез.");
          return;
        }}

        const nextDate = payload.available_dates?.[0];
        if (nextDate) {{
          window.location.href = `/projects/{projectRedmineId}/latest-snapshot-issues?captured_for_date=${{encodeURIComponent(nextDate)}}`;
          return;
        }}

        window.location.href = `/projects/{projectRedmineId}/latest-snapshot-issues`;
      }});

      textFilterInputs.forEach((input) => {{
        input.addEventListener("input", applySnapshotTableFilters);
      }});
      multiSelectFilters.forEach((select) => {{
        select.addEventListener("change", applySnapshotTableFilters);
      }});
      numericFilterControls.forEach((control) => {{
        control.addEventListener("input", applySnapshotTableFilters);
        control.addEventListener("change", applySnapshotTableFilters);
      }});

      resetSnapshotFiltersButton?.addEventListener("click", resetSnapshotTableFilters);
      populateSnapshotMultiSelects();
      updateSnapshotFilterHeaderOffset();
      window.addEventListener("resize", updateSnapshotFilterHeaderOffset);
      applySnapshotTableFilters();
    </script>
  </main>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def readRoot() -> HTMLResponse:
    return HTMLResponse(PAGE_HTML)


@app.get("/snapshot-rules", response_class=HTMLResponse)
def getSnapshotRulesPage() -> HTMLResponse:
    return HTMLResponse(buildSnapshotRulesPage())


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


@app.get("/projects/{project_redmine_id}/latest-snapshot-issues", response_class=HTMLResponse)
def getProjectLatestSnapshotIssuesPage(
    project_redmine_id: int,
    captured_for_date: str | None = Query(None, description="Дата среза в формате YYYY-MM-DD"),
) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    return HTMLResponse(buildLatestSnapshotIssuesPageClean(project_redmine_id, captured_for_date))


@app.get("/projects/{project_redmine_id}/burndown", response_class=HTMLResponse)
def getProjectBurndownPage(project_redmine_id: int) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    return HTMLResponse(buildBurndownPlaceholderPage(project_redmine_id))


@app.post("/api/projects/refresh")
def refreshProjects() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    requireProjectSyncConfig()
    ensureProjectsTable()

    projects = fetchAllProjectsFromRedmine(config.redmineUrl, config.apiKey)
    syncStats = syncProjects(projects)

    return {
        "added_count": syncStats["added_count"],
        "updated_count": syncStats["updated_count"],
        "projects": listStoredProjects(),
    }


@app.post("/api/projects/settings")
def updateProjectSettings(payload: ProjectSettingsUpdate) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    settingsStats = updateProjectLoadSettings(payload.enabled_project_ids, payload.partial_project_ids)
    return {
        "enabled_count": settingsStats["enabled_count"],
        "partial_count": settingsStats["partial_count"],
        "projects": listStoredProjects(),
    }


@app.get("/api/issues/snapshots/runs")
def getIssueSnapshotRuns() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    return {
        "snapshot_runs": listRecentIssueSnapshotRuns(limit=None),
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


@app.delete("/api/issues/snapshots/project/{project_redmine_id}/by-date")
def deleteIssueSnapshotByProjectDate(
    project_redmine_id: int,
    captured_for_date: str = Query(..., description="Дата в формате YYYY-MM-DD"),
) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()

    try:
        datetime.strptime(captured_for_date, "%Y-%m-%d")
    except ValueError as error:
        raise HTTPException(status_code=400, detail="captured_for_date must be YYYY-MM-DD") from error

    result = deleteIssueSnapshotForProjectDate(project_redmine_id, captured_for_date)
    result["available_dates"] = getSnapshotIssuesForProjectByDate(project_redmine_id, None).get("available_dates", [])
    return result


@app.post("/api/issues/snapshots/prune")
def pruneIssueSnapshots() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    result = pruneUnchangedIssueSnapshots()
    result["snapshot_runs"] = listRecentIssueSnapshotRuns(limit=None)
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


@app.post("/api/issues/snapshots/capture-project/{project_redmine_id}")
def captureIssueSnapshotByProject(project_redmine_id: int) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    requireProjectSyncConfig()
    ensureProjectsTable()
    ensureIssueSnapshotTables()

    if isIssueSnapshotCaptureRunning():
        return {
            "started": False,
            "detail": "Другое получение срезов уже выполняется.",
            **getIssueSnapshotCaptureStatus(),
        }

    project = next((item for item in listStoredProjects() if int(item.get("redmine_id") or 0) == project_redmine_id), None)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    if not bool(project.get("is_enabled")):
        raise HTTPException(status_code=400, detail="Проект выключен для загрузки")

    started = startProjectIssueSnapshotCaptureInBackground(project_redmine_id)
    return {
        "started": started,
        "detail": f"Получение среза по проекту «{project.get('name') or project_redmine_id}» запущено.",
        **getIssueSnapshotCaptureStatus(),
    }


@app.post("/api/issues/snapshots/recapture-project/{project_redmine_id}")
def recaptureIssueSnapshotByProject(project_redmine_id: int) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    requireProjectSyncConfig()
    ensureProjectsTable()
    ensureIssueSnapshotTables()

    if isIssueSnapshotCaptureRunning():
        return {
            "started": False,
            "detail": "Другое получение срезов уже выполняется.",
            **getIssueSnapshotCaptureStatus(),
        }

    project = next((item for item in listStoredProjects() if int(item.get("redmine_id") or 0) == project_redmine_id), None)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    if not bool(project.get("is_enabled")):
        raise HTTPException(status_code=400, detail="Проект выключен для загрузки")

    capturedForDate = datetime.now(UTC).date().isoformat()
    deleteIssueSnapshotForProjectDate(project_redmine_id, capturedForDate)
    started = startProjectIssueSnapshotCaptureInBackground(project_redmine_id)

    return {
        **getIssueSnapshotCaptureStatus(),
        "started": started,
        "captured_for_date": capturedForDate,
        "detail": f"Повторное получение среза по проекту «{project.get('name') or project_redmine_id}» запущено.",
    }


@app.post("/api/issues/snapshots/recapture")
def recaptureIssueSnapshots() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    requireProjectSyncConfig()
    ensureProjectsTable()
    ensureIssueSnapshotTables()

    if isIssueSnapshotCaptureRunning():
        return {
            "started": False,
            "detail": "Другое получение срезов уже выполняется.",
            **getIssueSnapshotCaptureStatus(),
        }

    capturedForDate = datetime.now(UTC).date().isoformat()
    enabledProjects = [project for project in listStoredProjects() if bool(project.get("is_enabled"))]

    for project in enabledProjects:
        projectId = int(project.get("redmine_id") or 0)
        if projectId:
            deleteIssueSnapshotForProjectDate(projectId, capturedForDate)

    started = startIssueSnapshotCaptureInBackground()
    return {
        **getIssueSnapshotCaptureStatus(),
        "started": started,
        "captured_for_date": capturedForDate,
        "detail": "Обновление последних срезов запущено.",
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



