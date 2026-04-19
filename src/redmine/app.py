from datetime import UTC, datetime
from html import escape
import csv
import io
import json
from datetime import date, timedelta
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel

from src.redmine.config import loadConfig
from src.redmine.db import (
    checkDatabaseConnection,
    countIssueSnapshotRuns,
    deleteIssueSnapshotForProjectDate,
    deleteIssueSnapshotsForDate,
    ensureIssueSnapshotTables,
    ensureProjectsTable,
    getFilteredSnapshotIssuesForProjectByDate,
    getSnapshotRunsWithIssuesForProjectYear,
    getSnapshotIssuesForProjectByDate,
    listFilteredSnapshotIssuesForProjectByDate,
    listRecentIssueSnapshotRuns,
    listSnapshotDatesForProject,
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
        <label for="snapshotRunsPerProjectInput">Срезов на проект</label>
        <input
          id="snapshotRunsPerProjectInput"
          class="filter-input"
          type="number"
          min="1"
          max="50"
          step="1"
          value="3"
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
    const snapshotRunsPerProjectInput = document.getElementById("snapshotRunsPerProjectInput");
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
    const snapshotRunsPerProjectStorageKey = "redmine.snapshotRuns.perProject";

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

    function getSnapshotRunsPerProjectValue() {
      const rawValue = Number(snapshotRunsPerProjectInput?.value || 3);
      if (!Number.isFinite(rawValue)) {
        return 3;
      }
      return Math.min(50, Math.max(1, Math.floor(rawValue)));
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

    function saveSnapshotRunsPerProjectValue() {
      window.localStorage.setItem(snapshotRunsPerProjectStorageKey, String(getSnapshotRunsPerProjectValue()));
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

    function restoreSnapshotRunsPerProjectValue() {
      const savedValue = window.localStorage.getItem(snapshotRunsPerProjectStorageKey);
      if (savedValue !== null && snapshotRunsPerProjectInput) {
        snapshotRunsPerProjectInput.value = savedValue;
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
        ["label[for='snapshotRunsPerProjectInput']", "textContent", "Срезов на проект"],
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
      const perProjectLimit = getSnapshotRunsPerProjectValue();
      if (snapshotRunsPerProjectInput) {
        snapshotRunsPerProjectInput.value = String(perProjectLimit);
      }
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
        visibleRuns.push(...runs.slice(0, perProjectLimit));
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
        const projectRuns = groupedRuns.get(Number(run.project_redmine_id ?? 0)) || [];
        const latestRunForProject = projectRuns[0] || run;
        const compareUrl = `/projects/${encodeURIComponent(run.project_redmine_id ?? "")}/compare-snapshots?left_date=${encodeURIComponent(run.captured_for_date ?? "")}&right_date=${encodeURIComponent(latestRunForProject.captured_for_date ?? run.captured_for_date ?? "")}`;
        const identifierValue = run.project_identifier ?? "—";
        const identifierHtml = run.project_identifier
          ? `<a class="project-link mono" href="${compareUrl}" target="_blank" rel="noreferrer">${identifierValue}</a>`
          : `<span class="mono">${identifierValue}</span>`;
        row.innerHTML = `
          <td class="mono">${run.id ?? "—"}</td>
          <td class="mono">${run.captured_for_date ?? "—"}</td>
          <td>${run.project_name ?? "—"}</td>
          <td>${identifierHtml}</td>
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
    snapshotRunsPerProjectInput.addEventListener("input", () => {
      saveSnapshotRunsPerProjectValue();
      rerenderSnapshotRuns();
    });
    snapshotRunsPerProjectInput.addEventListener("change", () => {
      saveSnapshotRunsPerProjectValue();
      rerenderSnapshotRuns();
    });
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
    restoreSnapshotRunsPerProjectValue();
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


def buildProjectRedmineIssuesUrl(projectIdentifier: object) -> str:
    projectIdentifierRaw = str(projectIdentifier or "").strip()
    if not projectIdentifierRaw:
        return ""

    return (
        "https://redmine.sms-it.ru/projects/"
        f"{quote(projectIdentifierRaw)}/issues?utf8=%E2%9C%93&set_filter=1&type=IssueQuery"
        "&f%5B%5D=status_id&op%5Bstatus_id%5D=*&query%5Bsort_criteria%5D%5B0%5D%5B%5D=id"
        "&query%5Bsort_criteria%5D%5B0%5D%5B%5D=desc&t%5B%5D=cf_27&t%5B%5D=spent_hours"
        "&t%5B%5D=estimated_hours&c%5B%5D=tracker&c%5B%5D=parent&c%5B%5D=status"
        "&c%5B%5D=priority&c%5B%5D=subject&c%5B%5D=assigned_to&c%5B%5D=estimated_hours"
        f"&saved_query_id=0&current_project_id={quote(projectIdentifierRaw)}"
    )


def buildProjectContextNavCss() -> str:
    return """
    .context-nav-shell {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 20px;
      margin: 0 0 18px;
    }
    .context-nav-brand {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      flex: 0 0 auto;
      text-decoration: none;
    }
    .context-nav-logo {
      display: block;
      width: 220px;
      max-width: 100%;
      height: auto;
    }
    .context-nav-panel {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      justify-content: flex-end;
      flex: 1 1 auto;
      margin: 0;
    }
    .context-nav-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
      padding: 10px 14px;
      border-radius: 6px;
      border: 1px solid transparent;
      text-decoration: none;
      font-weight: 700;
      transition: transform 120ms ease, filter 120ms ease, box-shadow 120ms ease;
      box-shadow: 0 8px 16px rgba(22, 50, 74, 0.08);
    }
    .context-nav-button:hover {
      transform: translateY(-1px);
      filter: brightness(1.03);
    }
    @media (max-width: 900px) {
      .context-nav-shell {
        flex-direction: column;
        align-items: flex-start;
      }
      .context-nav-panel {
        justify-content: flex-start;
        width: 100%;
      }
    }
    .context-nav-snapshots {
      background: #ffc600;
      color: #16324a;
    }
    .context-nav-compare {
      background: #52cee6;
      color: #16324a;
    }
    .context-nav-burndown {
      background: #ff6c0e;
      color: #ffffff;
    }
    .context-nav-redmine {
      background: #375d77;
      color: #ffffff;
    }
    """


def buildProjectContextNavPanel(
    projectRedmineId: int,
    projectIdentifier: object,
    *,
    currentPage: str,
    snapshotUrl: str | None = None,
    compareUrl: str | None = None,
    burndownUrl: str | None = None,
) -> str:
    resolvedSnapshotUrl = snapshotUrl or f"/projects/{projectRedmineId}/latest-snapshot-issues"
    resolvedCompareUrl = compareUrl or f"/projects/{projectRedmineId}/compare-snapshots"
    resolvedBurndownUrl = burndownUrl or f"/projects/{projectRedmineId}/burndown"
    redmineUrl = buildProjectRedmineIssuesUrl(projectIdentifier)

    buttons = [
        ("snapshots", resolvedSnapshotUrl, "Срезы проекта", "context-nav-snapshots", False),
        ("compare", resolvedCompareUrl, "Сравнение срезов", "context-nav-compare", False),
        ("burndown", resolvedBurndownUrl, "Диаграмма сгорания", "context-nav-burndown", False),
    ]
    if redmineUrl:
        buttons.append(("redmine", redmineUrl, "Открыть в Redmine", "context-nav-redmine", True))

    visibleButtons = [button for button in buttons if button[0] != currentPage]

    htmlParts: list[str] = [
        '<div class="context-nav-shell">',
        '<a class="context-nav-brand" href="/" aria-label="На главную">',
        '<img class="context-nav-logo" src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">',
        "</a>",
        '<nav class="context-nav-panel">',
    ]
    for _, href, label, cssClass, isExternal in visibleButtons:
        targetAttrs = ' target="_blank" rel="noreferrer"' if isExternal else ""
        htmlParts.append(
            f'<a class="context-nav-button {cssClass}" href="{escape(str(href))}"{targetAttrs}>{escape(label)}</a>'
        )
    htmlParts.append("</nav>")
    htmlParts.append("</div>")
    return "".join(htmlParts)


def buildSnapshotSummaryView(summary: dict[str, object] | None) -> dict[str, float]:
    source = dict(summary or {})
    baselineEstimateHours = float(source.get("baseline_estimate_hours") or 0)
    estimatedHours = float(source.get("estimated_hours") or 0)
    spentHours = float(source.get("spent_hours") or 0)
    spentHoursYear = float(source.get("spent_hours_year") or 0)
    developmentEstimatedHours = float(source.get("development_estimated_hours") or 0)
    developmentSpentHours = float(source.get("development_spent_hours") or 0)
    developmentSpentHoursYear = float(source.get("development_spent_hours_year") or 0)
    developmentProcessEstimatedHours = float(source.get("development_process_estimated_hours") or 0)
    developmentProcessSpentHours = float(source.get("development_process_spent_hours") or 0)
    developmentProcessSpentHoursYear = float(source.get("development_process_spent_hours_year") or 0)
    bugEstimatedHours = float(source.get("bug_estimated_hours") or 0)
    bugSpentHours = float(source.get("bug_spent_hours") or 0)
    bugSpentHoursYear = float(source.get("bug_spent_hours_year") or 0)

    developmentCombinedSpentHours = developmentSpentHours + developmentProcessSpentHours
    developmentCombinedSpentHoursYear = developmentSpentHoursYear + developmentProcessSpentHoursYear

    return {
        "baseline_estimate_hours": baselineEstimateHours,
        "estimated_hours": estimatedHours,
        "spent_hours": spentHours,
        "spent_hours_year": spentHoursYear,
        "development_estimated_hours": developmentEstimatedHours,
        "development_spent_hours": developmentSpentHours,
        "development_spent_hours_year": developmentSpentHoursYear,
        "development_process_estimated_hours": developmentProcessEstimatedHours,
        "development_process_spent_hours": developmentProcessSpentHours,
        "development_process_spent_hours_year": developmentProcessSpentHoursYear,
        "bug_estimated_hours": bugEstimatedHours,
        "bug_spent_hours": bugSpentHours,
        "bug_spent_hours_year": bugSpentHoursYear,
        "development_combined_spent_hours": developmentCombinedSpentHours,
        "development_combined_spent_hours_year": developmentCombinedSpentHoursYear,
        "development_total_estimated_hours": developmentEstimatedHours + developmentProcessEstimatedHours + bugEstimatedHours,
        "development_grand_spent_hours": developmentCombinedSpentHours + bugSpentHours,
        "development_grand_spent_hours_year": developmentCombinedSpentHoursYear + bugSpentHoursYear,
        "development_coverage_all_percent": (developmentCombinedSpentHours / baselineEstimateHours * 100) if baselineEstimateHours else 0,
        "bug_share_year_percent": (bugSpentHoursYear / developmentCombinedSpentHoursYear * 100) if developmentCombinedSpentHoursYear else 0,
        "bug_share_all_percent": (bugSpentHours / developmentCombinedSpentHours * 100) if developmentCombinedSpentHours else 0,
    }


def buildSnapshotIssueFiltersPayload(
    issueId: str | None = None,
    subject: str | None = None,
    trackerNames: list[str] | None = None,
    statusNames: list[str] | None = None,
    doneRatioOp: str | None = None,
    doneRatioValue: str | None = None,
    baselineOp: str | None = None,
    baselineValue: str | None = None,
    estimatedOp: str | None = None,
    estimatedValue: str | None = None,
    spentOp: str | None = None,
    spentValue: str | None = None,
    spentYearOp: str | None = None,
    spentYearValue: str | None = None,
    closedOn: str | None = None,
    assignedTo: str | None = None,
    fixedVersion: str | None = None,
) -> dict[str, object]:
    return {
        "issue_id": issueId or "",
        "subject": subject or "",
        "tracker_names": trackerNames or [],
        "status_names": statusNames or [],
        "done_ratio_op": doneRatioOp or "",
        "done_ratio_value": doneRatioValue or "",
        "baseline_op": baselineOp or "",
        "baseline_value": baselineValue or "",
        "estimated_op": estimatedOp or "",
        "estimated_value": estimatedValue or "",
        "spent_op": spentOp or "",
        "spent_value": spentValue or "",
        "spent_year_op": spentYearOp or "",
        "spent_year_value": spentYearValue or "",
        "closed_on": closedOn or "",
        "assigned_to": assignedTo or "",
        "fixed_version": fixedVersion or "",
    }


SNAPSHOT_COMPARE_FIELD_CONFIG: list[dict[str, object]] = [
    {
        "key": "baseline",
        "label": "Базовая оценка",
        "issue_key": "baseline_estimate_hours",
        "tracker_names": None,
    },
    {
        "key": "development_estimate",
        "label": "Разработка: оценка",
        "issue_key": "estimated_hours",
        "tracker_names": {"разработка", "процессы разработки"},
    },
    {
        "key": "development_spent_year",
        "label": "Разработка: факт за год",
        "issue_key": "spent_hours_year",
        "tracker_names": {"разработка", "процессы разработки"},
    },
]

SNAPSHOT_COMPARE_FIELD_BY_KEY = {
    str(item["key"]): item for item in SNAPSHOT_COMPARE_FIELD_CONFIG
}


def normalizeSnapshotCompareFields(selectedFields: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for fieldKey in selectedFields or []:
        key = str(fieldKey or "").strip()
        if key in SNAPSHOT_COMPARE_FIELD_BY_KEY and key not in normalized:
            normalized.append(key)
    return normalized or ["baseline"]


def resolveSnapshotCompareDates(
    availableDates: list[str],
    leftDate: str | None,
    rightDate: str | None,
) -> tuple[str | None, str | None]:
    if not availableDates:
        return None, None

    validLeft = leftDate if leftDate in availableDates else None
    validRight = rightDate if rightDate in availableDates else None

    if validLeft is None and validRight is None:
        validRight = availableDates[0]
        validLeft = availableDates[1] if len(availableDates) > 1 else availableDates[0]
        return validLeft, validRight

    if validRight is not None and validLeft is None:
        rightIndex = availableDates.index(validRight)
        validLeft = availableDates[rightIndex + 1] if rightIndex + 1 < len(availableDates) else (
            availableDates[0] if availableDates[0] != validRight else validRight
        )
        return validLeft, validRight

    if validLeft is not None and validRight is None:
        leftIndex = availableDates.index(validLeft)
        validRight = availableDates[leftIndex - 1] if leftIndex - 1 >= 0 else availableDates[0]
        if validRight == validLeft and len(availableDates) > 1:
            validRight = availableDates[1]
        return validLeft, validRight

    return validLeft, validRight


def getSnapshotCompareNumericValue(issue: dict[str, object] | None, compareFieldKey: str) -> float:
    if issue is None:
        return 0.0

    fieldConfig = SNAPSHOT_COMPARE_FIELD_BY_KEY[compareFieldKey]
    trackerNames = fieldConfig.get("tracker_names")
    if trackerNames:
        trackerName = normalizeBurndownText(issue.get("tracker_name"))
        if trackerName not in trackerNames:
            return 0.0

    rawValue = issue.get(str(fieldConfig["issue_key"]))
    try:
        return float(rawValue or 0)
    except (TypeError, ValueError):
        return 0.0


def buildSnapshotComparisonRows(
    leftIssues: list[dict[str, object]],
    rightIssues: list[dict[str, object]],
    selectedFields: list[str],
) -> tuple[list[dict[str, object]], dict[str, int]]:
    leftIssuesById: dict[int, dict[str, object]] = {}
    rightIssuesById: dict[int, dict[str, object]] = {}

    for issue in leftIssues:
        try:
            issueId = int(issue.get("issue_redmine_id") or 0)
        except (TypeError, ValueError):
            continue
        if issueId:
            leftIssuesById[issueId] = issue

    for issue in rightIssues:
        try:
            issueId = int(issue.get("issue_redmine_id") or 0)
        except (TypeError, ValueError):
            continue
        if issueId:
            rightIssuesById[issueId] = issue

    fieldChangeCounts = {fieldKey: 0 for fieldKey in selectedFields}
    changedRows: list[dict[str, object]] = []

    for issueId in sorted(set(leftIssuesById.keys()) | set(rightIssuesById.keys())):
        leftIssue = leftIssuesById.get(issueId)
        rightIssue = rightIssuesById.get(issueId)
        baseIssue = rightIssue or leftIssue or {}
        changedValues: dict[str, dict[str, object]] = {}
        existenceChanged = (leftIssue is None) != (rightIssue is None)
        rowHasChanges = existenceChanged
        changeKind = (
            "new"
            if leftIssue is None and rightIssue is not None
            else "deleted" if leftIssue is not None and rightIssue is None else "changed"
        )

        for fieldKey in selectedFields:
            leftValue = getSnapshotCompareNumericValue(leftIssue, fieldKey)
            rightValue = getSnapshotCompareNumericValue(rightIssue, fieldKey)
            isChanged = abs(leftValue - rightValue) > 1e-9
            if isChanged:
                fieldChangeCounts[fieldKey] += 1
                rowHasChanges = True

            changedValues[fieldKey] = {
                "left_value": leftValue,
                "right_value": rightValue,
                "is_changed": isChanged,
            }

        if not rowHasChanges:
            continue

        changedRows.append(
            {
                "issue_redmine_id": issueId,
                "subject": str(baseIssue.get("subject") or "—"),
                "tracker_name": str(baseIssue.get("tracker_name") or "—"),
                "left_status_name": str(leftIssue.get("status_name") or "—") if leftIssue else "—",
                "right_status_name": str(rightIssue.get("status_name") or "—") if rightIssue else "—",
                "change_kind": changeKind,
                "values": changedValues,
            }
        )

    return changedRows, fieldChangeCounts


def buildSnapshotComparisonPage(
    projectRedmineId: int,
    leftDate: str | None = None,
    rightDate: str | None = None,
    selectedFields: list[str] | None = None,
) -> str:
    availableDates = listSnapshotDatesForProject(projectRedmineId)
    normalizedFields = normalizeSnapshotCompareFields(selectedFields)
    resolvedLeftDate, resolvedRightDate = resolveSnapshotCompareDates(availableDates, leftDate, rightDate)
    storedProjects = listStoredProjects()
    storedProject = next(
        (item for item in storedProjects if int(item.get("redmine_id") or 0) == projectRedmineId),
        None,
    )
    projectIdentifierRaw = str((storedProject or {}).get("identifier") or "").strip()
    compareQueryParts = []
    if leftDate:
        compareQueryParts.append(f"left_date={quote(str(leftDate))}")
    if rightDate:
        compareQueryParts.append(f"right_date={quote(str(rightDate))}")
    for fieldKey in normalizedFields:
        compareQueryParts.append(f"field={quote(str(fieldKey))}")
    currentCompareUrl = f"/projects/{projectRedmineId}/compare-snapshots"
    if compareQueryParts:
        currentCompareUrl += f"?{'&'.join(compareQueryParts)}"
    navPanelHtml = buildProjectContextNavPanel(
        projectRedmineId,
        projectIdentifierRaw,
        currentPage="compare",
        compareUrl=currentCompareUrl,
    )

    if not availableDates or resolvedLeftDate is None or resolvedRightDate is None:
        projectName = escape(str((storedProject or {}).get("name") or "—"))
        compareFieldsHtml = "".join(
            (
                f'<label class="compare-field-option"><input type="checkbox" name="field" value="{escape(str(field["key"]))}"'
                f'{" checked" if str(field["key"]) in normalizedFields else ""}>'
                f'<span>{escape(str(field["label"]))}</span></label>'
            )
            for field in SNAPSHOT_COMPARE_FIELD_CONFIG
        )
        return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Сравнение срезов</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    body {{ margin: 0; font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: #ffffff; color: #16324a; }}
    main {{ max-width: 1280px; margin: 0 auto; padding: 24px 20px 48px; }}
    {buildProjectContextNavCss()}
    h1 {{ margin: 18px 0 12px; font-size: clamp(2rem, 5vw, 3.2rem); line-height: 1.05; }}
    .meta {{ color: #64798d; margin: 0 0 18px; line-height: 1.6; }}
    .controls-panel {{ border: 1px solid #d9e5eb; border-radius: 8px; padding: 18px 20px; background: #ffffff; }}
    .controls-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; }}
    .field {{ display: flex; flex-direction: column; gap: 6px; }}
    .field label {{ font-weight: 700; }}
    select {{ border: 1px solid #d9e5eb; border-radius: 6px; padding: 10px 12px; font: inherit; }}
    .compare-field-group {{ display: flex; flex-direction: column; gap: 8px; }}
    .compare-field-option {{ display: flex; align-items: center; gap: 8px; color: #16324a; }}
    button {{ border: 0; border-radius: 6px; padding: 10px 14px; font: inherit; font-weight: 700; cursor: pointer; background: #ff6c0e; color: #ffffff; }}
    .empty-state {{ margin-top: 18px; border: 1px dashed #d9e5eb; border-radius: 8px; padding: 24px; background: #f7fbfc; color: #64798d; }}
  </style>
</head>
<body>
  <main>
    {navPanelHtml}
    <h1>Сравнение срезов проекта</h1>
    <p class="meta">Проект: {projectName}. Для сравнения нужен хотя бы один сохраненный срез.</p>
    <section class="controls-panel">
      <form method="get">
        <div class="controls-grid">
          <div class="field">
            <label for="leftDate">Дата среза 1</label>
            <select id="leftDate" name="left_date"><option value="">Нет срезов</option></select>
          </div>
          <div class="field">
            <label for="rightDate">Дата среза 2</label>
            <select id="rightDate" name="right_date"><option value="">Нет срезов</option></select>
          </div>
          <div class="field">
            <label>Поля для сравнения</label>
            <div class="compare-field-group">{compareFieldsHtml}</div>
          </div>
        </div>
        <p><button type="submit">Сравнить</button></p>
      </form>
    </section>
    <div class="empty-state">Для этого проекта пока нет срезов, поэтому сравнивать еще нечего.</div>
  </main>
</body>
</html>"""

    leftPayload = getSnapshotIssuesForProjectByDate(projectRedmineId, resolvedLeftDate)
    rightPayload = getSnapshotIssuesForProjectByDate(projectRedmineId, resolvedRightDate)
    leftRun = leftPayload.get("snapshot_run") or {}
    rightRun = rightPayload.get("snapshot_run") or {}
    projectName = escape(
        str(
            rightRun.get("project_name")
            or leftRun.get("project_name")
            or (storedProject.get("name") if storedProject else "—")
        )
    )
    projectIdentifierRaw = str(
        rightRun.get("project_identifier")
        or leftRun.get("project_identifier")
        or (storedProject.get("identifier") if storedProject else "")
    ).strip()
    projectIdentifier = escape(projectIdentifierRaw or "—")
    comparisonRows, fieldChangeCounts = buildSnapshotComparisonRows(
        list(leftPayload.get("issues") or []),
        list(rightPayload.get("issues") or []),
        normalizedFields,
    )

    fieldOptionsHtml = "".join(
        (
            f'<label class="compare-field-option"><input type="checkbox" name="field" value="{escape(str(field["key"]))}"'
            f'{" checked" if str(field["key"]) in normalizedFields else ""}>'
            f'<span>{escape(str(field["label"]))}</span></label>'
        )
        for field in SNAPSHOT_COMPARE_FIELD_CONFIG
    )
    leftDateOptionsHtml = "".join(
        f'<option value="{escape(dateValue)}"{" selected" if dateValue == resolvedLeftDate else ""}>{escape(dateValue)}</option>'
        for dateValue in availableDates
    )
    rightDateOptionsHtml = "".join(
        f'<option value="{escape(dateValue)}"{" selected" if dateValue == resolvedRightDate else ""}>{escape(dateValue)}</option>'
        for dateValue in availableDates
    )

    selectedFieldLabels = [
        escape(str(SNAPSHOT_COMPARE_FIELD_BY_KEY[fieldKey]["label"])) for fieldKey in normalizedFields
    ]
    compareSummaryHtml = " · ".join(
        f"{escape(str(SNAPSHOT_COMPARE_FIELD_BY_KEY[fieldKey]['label']))}: {fieldChangeCounts[fieldKey]}"
        for fieldKey in normalizedFields
    )

    headerCells = []
    for fieldKey in normalizedFields:
        fieldLabel = escape(str(SNAPSHOT_COMPARE_FIELD_BY_KEY[fieldKey]["label"]))
        headerCells.append(
            f'<th>{fieldLabel}<br><span class="subhead">{escape(str(resolvedLeftDate))}</span></th>'
        )
        headerCells.append(
            f'<th>{fieldLabel}<br><span class="subhead">{escape(str(resolvedRightDate))}</span></th>'
        )

    bodyRows = []
    for row in comparisonRows:
        valueCells = []
        for fieldKey in normalizedFields:
            valueInfo = row["values"][fieldKey]
            changedClass = " changed-value" if valueInfo["is_changed"] else ""
            valueCells.append(
                f'<td class="mono compare-value{changedClass}">{formatPageHours(valueInfo["left_value"])}</td>'
            )
            valueCells.append(
                f'<td class="mono compare-value{changedClass}">{formatPageHours(valueInfo["right_value"])}</td>'
            )

        rowBadgeHtml = ""
        if row["change_kind"] == "new":
            rowBadgeHtml = '<span class="compare-badge compare-badge-new">Новая</span>'
        elif row["change_kind"] == "deleted":
            rowBadgeHtml = '<span class="compare-badge compare-badge-deleted">Удалена</span>'

        bodyRows.append(
            "<tr>"
            f'<td class="mono"><span class="compare-id-cell">{row["issue_redmine_id"]}{rowBadgeHtml}</span></td>'
            f'<td class="subject-col">{escape(str(row["subject"]))}</td>'
            f'<td>{escape(str(row["tracker_name"]))}</td>'
            f'<td>{escape(str(row["left_status_name"]))}</td>'
            f'<td>{escape(str(row["right_status_name"]))}</td>'
            + "".join(valueCells)
            + "</tr>"
        )

    latestSnapshotUrl = f"/projects/{projectRedmineId}/latest-snapshot-issues?captured_for_date={quote(str(resolvedRightDate))}"
    compareUrlCurrent = f"/projects/{projectRedmineId}/compare-snapshots?left_date={quote(str(resolvedLeftDate))}&right_date={quote(str(resolvedRightDate))}"
    for fieldKey in normalizedFields:
        compareUrlCurrent += f"&field={quote(str(fieldKey))}"
    navPanelHtml = buildProjectContextNavPanel(
        projectRedmineId,
        projectIdentifierRaw,
        currentPage="compare",
        snapshotUrl=latestSnapshotUrl,
        compareUrl=compareUrlCurrent,
    )

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Сравнение срезов</title>
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
      --highlight: rgba(255, 198, 0, 0.20);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: var(--bg); color: var(--text); }}
    main {{ max-width: 1480px; margin: 0 auto; padding: 24px 20px 48px; }}
    {buildProjectContextNavCss()}
    h1 {{ margin: 18px 0 12px; font-size: clamp(2rem, 5vw, 3.2rem); line-height: 1.05; }}
    .meta {{ color: var(--muted); margin: 0 0 14px; line-height: 1.6; }}
    .controls-panel {{ border: 1px solid var(--line); border-radius: 8px; padding: 18px 20px; background: var(--panel); margin: 0 0 18px; }}
    .controls-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; align-items: start; }}
    .field {{ display: flex; flex-direction: column; gap: 6px; }}
    .field label {{ font-weight: 700; }}
    select {{ border: 1px solid var(--line); border-radius: 6px; padding: 10px 12px; font: inherit; color: var(--text); background: #ffffff; }}
    .compare-field-group {{ display: flex; flex-direction: column; gap: 8px; padding-top: 2px; }}
    .compare-field-option {{ display: flex; align-items: center; gap: 8px; color: var(--text); }}
    button {{ border: 0; border-radius: 6px; padding: 10px 14px; font: inherit; font-weight: 700; cursor: pointer; background: var(--orange); color: #ffffff; }}
    .summary-note {{ color: var(--muted); margin: 0 0 14px; }}
    .table-wrap {{ overflow: auto; border: 1px solid var(--line); border-radius: 8px; }}
    table {{ width: 100%; border-collapse: separate; border-spacing: 0; background: #ffffff; min-width: 1080px; }}
    th, td {{ text-align: left; padding: 12px 14px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    th {{ position: sticky; top: 0; z-index: 2; background: #eef6f7; color: #426179; text-transform: uppercase; font-size: 0.88rem; }}
    tr:last-child td {{ border-bottom: 0; }}
    .mono {{ font-family: Consolas, "Courier New", monospace; font-size: 0.95rem; white-space: nowrap; }}
    .subject-col {{ min-width: 340px; max-width: 520px; white-space: normal; word-break: break-word; }}
    .subhead {{ display: inline-block; margin-top: 4px; color: var(--muted); text-transform: none; font-size: 0.82rem; font-weight: 500; }}
    .compare-value {{ text-align: right; }}
    .changed-value {{ background: var(--highlight); }}
    .compare-id-cell {{ display: inline-flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
    .compare-badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: none;
      letter-spacing: 0;
      white-space: nowrap;
    }}
    .compare-badge-new {{ background: rgba(56, 161, 105, 0.16); color: #2f855a; }}
    .compare-badge-deleted {{ background: rgba(229, 62, 62, 0.16); color: #c53030; }}
    .empty-state {{ border: 1px dashed var(--line); border-radius: 8px; padding: 24px; background: #f7fbfc; color: var(--muted); line-height: 1.6; }}
  </style>
</head>
<body>
  <main>
    {navPanelHtml}
    <h1>Сравнение срезов проекта</h1>
    <p class="meta">Проект: {projectName}. Идентификатор: {projectIdentifier}. По умолчанию сравниваются последний и предпоследний срезы.</p>
    <section class="controls-panel">
      <form method="get">
        <div class="controls-grid">
          <div class="field">
            <label for="leftDate">Дата среза 1</label>
            <select id="leftDate" name="left_date">{leftDateOptionsHtml}</select>
          </div>
          <div class="field">
            <label for="rightDate">Дата среза 2</label>
            <select id="rightDate" name="right_date">{rightDateOptionsHtml}</select>
          </div>
          <div class="field">
            <label>Поля для сравнения</label>
            <div class="compare-field-group">{fieldOptionsHtml}</div>
          </div>
        </div>
        <p><button type="submit">Сравнить</button></p>
      </form>
    </section>
    <p class="summary-note">Поля сравнения: {", ".join(selectedFieldLabels)}. Изменившихся задач: {len(comparisonRows)}. {compareSummaryHtml}</p>
    {
        f'''
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Тема</th>
            <th>Трекер</th>
            <th>Статус<br><span class="subhead">{escape(str(resolvedLeftDate))}</span></th>
            <th>Статус<br><span class="subhead">{escape(str(resolvedRightDate))}</span></th>
            {"".join(headerCells)}
          </tr>
        </thead>
        <tbody>
          {"".join(bodyRows)}
        </tbody>
      </table>
    </div>
        ''' if comparisonRows else '<div class="empty-state">По выбранным полям между этими двумя срезами изменений не найдено.</div>'
    }
  </main>
</body>
</html>"""


def normalizeBurndownText(value: object) -> str:
    return str(value or "").strip().lower()


def isBurndownClosedTaskStatus(statusName: object) -> bool:
    return normalizeBurndownText(statusName) in {"закрыта", "решена", "отказ"}


def isBurndownReadyFeatureStatus(statusName: object) -> bool:
    normalized = normalizeBurndownText(statusName)
    return normalized.startswith("готов") or normalized in {"закрыта", "решена"}


def buildBurndownFeatureGroups(issues: list[dict[str, object]]) -> list[dict[str, object]]:
    issuesById: dict[int, dict[str, object]] = {}
    for issue in issues:
        try:
            issueId = int(issue.get("issue_redmine_id") or 0)
        except (TypeError, ValueError):
            continue
        if issueId:
            issuesById[issueId] = issue

    resolvedFeatureIds: dict[int, int | None] = {}

    def resolveFeatureId(issueId: int, visited: set[int] | None = None) -> int | None:
        if issueId in resolvedFeatureIds:
            return resolvedFeatureIds[issueId]

        issue = issuesById.get(issueId)
        if issue is None:
            resolvedFeatureIds[issueId] = None
            return None

        trackerName = normalizeBurndownText(issue.get("tracker_name"))
        if trackerName == "feature":
            resolvedFeatureIds[issueId] = issueId
            return issueId

        if visited is None:
            visited = set()
        if issueId in visited:
            resolvedFeatureIds[issueId] = None
            return None
        visited.add(issueId)

        try:
            parentIssueId = int(issue.get("parent_issue_redmine_id") or 0)
        except (TypeError, ValueError):
            parentIssueId = 0

        if not parentIssueId:
            resolvedFeatureIds[issueId] = None
            return None

        featureId = resolveFeatureId(parentIssueId, visited)
        resolvedFeatureIds[issueId] = featureId
        return featureId

    groupsByKey: dict[str, dict[str, object]] = {}

    for issue in issues:
        try:
            issueId = int(issue.get("issue_redmine_id") or 0)
        except (TypeError, ValueError):
            continue

        featureId = resolveFeatureId(issueId) if issueId else None
        groupKey = str(featureId) if featureId is not None else "virtual"
        group = groupsByKey.setdefault(
            groupKey,
            {
                "group_key": groupKey,
                "is_virtual": featureId is None,
                "is_ready": False,
                "baseline_total": 0.0,
                "development_volume": 0.0,
                "development_remaining": 0.0,
                "bug_volume": 0.0,
                "bug_remaining": 0.0,
            },
        )

        baselineEstimateHours = float(issue.get("baseline_estimate_hours") or 0)
        group["baseline_total"] = float(group["baseline_total"]) + baselineEstimateHours

        trackerName = normalizeBurndownText(issue.get("tracker_name"))
        statusName = issue.get("status_name")
        planHours = float(issue.get("estimated_hours") or 0)
        factHours = float(issue.get("spent_hours") or 0)

        if featureId is not None and featureId == issueId and trackerName == "feature":
            group["is_ready"] = isBurndownReadyFeatureStatus(statusName)
            continue

        if trackerName == "разработка":
            if isBurndownClosedTaskStatus(statusName):
                volume = factHours
                remaining = 0.0
            else:
                volume = max(planHours, factHours)
                remaining = max(0.0, planHours - factHours)
            group["development_volume"] = float(group["development_volume"]) + volume
            group["development_remaining"] = float(group["development_remaining"]) + remaining
        elif trackerName == "процессы разработки":
            volume = max(planHours, factHours)
            group["development_volume"] = float(group["development_volume"]) + volume
        elif trackerName == "ошибка":
            if isBurndownClosedTaskStatus(statusName):
                volume = factHours
                remaining = 0.0
            else:
                volume = max(planHours, factHours)
                remaining = max(0.0, planHours - factHours)
            group["bug_volume"] = float(group["bug_volume"]) + volume
            group["bug_remaining"] = float(group["bug_remaining"]) + remaining

    return list(groupsByKey.values())


def buildBurndownChartSeeds(snapshotRuns: list[dict[str, object]]) -> list[dict[str, object]]:
    chartSeeds: list[dict[str, object]] = []

    for snapshotRun in snapshotRuns:
        chartSeeds.append(
            {
                "date": str(snapshotRun.get("captured_for_date") or ""),
                "budget_baseline_total": float(snapshotRun.get("total_baseline_estimate_hours") or 0),
                "groups": buildBurndownFeatureGroups(list(snapshotRun.get("issues") or [])),
            }
        )

    return chartSeeds


def buildBurndownDateLabels(year: int, month: int) -> list[str]:
    currentDate = date(year, month, 1)
    if month == 12:
        lastDate = date(year, 12, 31)
    else:
        lastDate = date(year, month + 1, 1) - timedelta(days=1)
    labels: list[str] = []

    while currentDate <= lastDate:
        labels.append(currentDate.isoformat())
        currentDate += timedelta(days=1)

    return labels


def buildBurndownPage(projectRedmineId: int) -> str:
    currentYear = datetime.now(UTC).year
    targetMonth = 4
    burndownPayload = getSnapshotRunsWithIssuesForProjectYear(projectRedmineId, currentYear)
    storedProjects = listStoredProjects()
    storedProject = next(
        (item for item in storedProjects if int(item.get("redmine_id") or 0) == projectRedmineId),
        None,
    )

    projectInfo = burndownPayload.get("project") or {}
    projectName = escape(
        str(projectInfo.get("project_name") or (storedProject.get("name") if storedProject else "—"))
    )
    projectIdentifierRaw = str(
        projectInfo.get("project_identifier") or (storedProject.get("identifier") if storedProject else "")
    ).strip()
    projectIdentifier = escape(projectIdentifierRaw or "—")
    snapshotIssuesUrl = f"/projects/{projectRedmineId}/latest-snapshot-issues"
    navPanelHtml = buildProjectContextNavPanel(
        projectRedmineId,
        projectIdentifierRaw,
        currentPage="burndown",
        snapshotUrl=snapshotIssuesUrl,
    )
    snapshotRuns = [
        snapshotRun
        for snapshotRun in list(burndownPayload.get("snapshot_runs") or [])
        if str(snapshotRun.get("captured_for_date") or "").startswith(f"{currentYear}-{targetMonth:02d}-")
    ]
    chartSeeds = buildBurndownChartSeeds(snapshotRuns)
    chartDatesJson = json.dumps(buildBurndownDateLabels(currentYear, targetMonth), ensure_ascii=False)
    chartSeedsJson = json.dumps(chartSeeds, ensure_ascii=False)

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Диаграмма сгорания</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      color-scheme: light;
      --bg: #ffffff;
      --panel: #ffffff;
      --panel-soft: #f5fafb;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --blue-302: #375d77;
      --yellow-109: #ffc600;
      --cyan-310: #52cee6;
      --orange-1585: #ff6c0e;
      --shadow-soft: 0 12px 24px rgba(22, 50, 74, 0.06);
    }}

    * {{ box-sizing: border-box; }}

    body {{
      margin: 0;
      font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}

    main {{
      max-width: 1440px;
      margin: 0 auto;
      padding: 24px 20px 48px;
    }}
    {buildProjectContextNavCss()}

    h1 {{
      margin: 18px 0 12px;
      font-size: clamp(2rem, 5vw, 3.2rem);
      line-height: 1.05;
      letter-spacing: -0.03em;
    }}

    .meta {{
      color: var(--muted);
      margin: 0 0 18px;
      font-size: 1rem;
      line-height: 1.6;
    }}

    .controls-panel,
    .chart-panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow-soft);
    }}

    .controls-panel {{
      display: flex;
      gap: 16px;
      align-items: end;
      flex-wrap: wrap;
      padding: 18px 20px;
      margin: 0 0 18px;
    }}

    .field {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 240px;
    }}

    .field label {{
      font-weight: 700;
    }}

    .field-note {{
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.4;
    }}

    .field input {{
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px 12px;
      font: inherit;
      color: var(--text);
      background: #ffffff;
    }}

    .chart-panel {{
      padding: 18px 20px 20px;
    }}

    .chart-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      flex-wrap: wrap;
      margin: 0 0 14px;
    }}

    .chart-title {{
      margin: 0;
      font-size: 1.2rem;
    }}

    .chart-subtitle {{
      margin: 4px 0 0;
      color: var(--muted);
    }}

    .chart-status {{
      min-height: 22px;
      color: var(--muted);
      font-size: 0.95rem;
    }}

    .chart-wrap {{
      position: relative;
      min-height: 560px;
    }}

    .empty-state {{
      border: 1px dashed var(--line);
      border-radius: 8px;
      padding: 24px;
      background: var(--panel-soft);
      color: var(--muted);
      line-height: 1.6;
    }}

    .legend-panel {{
      margin-top: 18px;
      padding: 18px 20px 20px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
    }}

    .legend-title {{
      margin: 0 0 14px;
      font-size: 1.05rem;
    }}

    .legend-grid {{
      display: grid;
      grid-template-columns: minmax(280px, 360px) minmax(320px, 1fr);
      gap: 18px 24px;
      align-items: start;
    }}

    .legend-list,
    .formula-list {{
      margin: 0;
      padding: 0;
      list-style: none;
    }}

    .legend-list li,
    .formula-list li {{
      display: flex;
      gap: 10px;
      align-items: flex-start;
      margin: 0 0 12px;
      line-height: 1.5;
      color: var(--text);
    }}

    .legend-swatch {{
      width: 28px;
      min-width: 28px;
      height: 14px;
      margin-top: 4px;
      border-radius: 999px;
      border: 2px solid transparent;
      background: transparent;
    }}

    .legend-swatch.budget-line {{
      border-color: #ff6c0e;
    }}

    .legend-swatch.forecast-line {{
      border-color: #375d77;
    }}

    .legend-swatch.current-line {{
      border-color: #0f9bb8;
    }}

    .legend-swatch.remaining-line {{
      border-color: #7b8c9d;
    }}

    .legend-swatch.dev-bar {{
      background: rgba(82, 206, 230, 0.38);
      border-color: rgba(82, 206, 230, 0.9);
      border-radius: 4px;
    }}

    .legend-swatch.bug-bar {{
      background: rgba(255, 108, 14, 0.30);
      border-color: rgba(255, 108, 14, 0.85);
      border-radius: 4px;
    }}

    .legend-swatch.dev-rem-bar {{
      background: #52cee6;
      border-color: #52cee6;
      border-radius: 4px;
    }}

    .legend-swatch.bug-rem-bar {{
      background: #ffc600;
      border-color: #ffc600;
      border-radius: 4px;
    }}

    .legend-name {{
      font-weight: 700;
      margin: 0 0 2px;
    }}

    .legend-text,
    .formula-text {{
      color: var(--muted);
    }}

    .legend-note {{
      margin: 12px 0 0;
      color: var(--muted);
      line-height: 1.55;
    }}

    @media (max-width: 900px) {{
      .chart-wrap {{
        min-height: 420px;
      }}

      .legend-grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main>
    {navPanelHtml}
    <h1>Диаграмма сгорания</h1>
    <p class="meta">Проект: {projectName}. Идентификатор: {projectIdentifier}. Период диаграммы: 01.04.{currentYear} — 30.04.{currentYear}. Срезов за апрель: {len(chartSeeds)}.</p>

    <section class="controls-panel">
      <div class="field">
        <label for="p1Input">P1 = факт / база</label>
        <input id="p1Input" type="text" inputmode="decimal" value="1,5">
        <div class="field-note">Используется в расчете бюджета и прогнозного объема.</div>
      </div>
      <div class="field">
        <label for="p2Input">P2 = факт с багами / факт</label>
        <input id="p2Input" type="text" inputmode="decimal" value="1,5">
        <div class="field-note">Изменения пересчитываются сразу после ввода без перезагрузки страницы.</div>
      </div>
    </section>

    <section class="chart-panel">
      <div class="chart-head">
        <div>
          <h2 class="chart-title">Бюджет, прогноз, текущий объем и остаток</h2>
          <p class="chart-subtitle">Линии показывают общие значения, а полупрозрачные столбики — состав по разработке и ошибкам.</p>
        </div>
        <div class="chart-status" id="burndownStatus"></div>
      </div>
      <div class="chart-wrap">
        <canvas id="burndownChart"></canvas>
      </div>
      <div id="burndownEmptyState" class="empty-state" style="display:none;">
        За апрель текущего года по проекту пока нет срезов, поэтому построить диаграмму еще не из чего.
      </div>
    </section>

    <section class="legend-panel">
      <h2 class="legend-title">Легенда и правила расчета</h2>
      <div class="legend-grid">
        <div>
          <ul class="legend-list">
            <li>
              <span class="legend-swatch budget-line"></span>
              <div>
                <div class="legend-name">Бюджет</div>
                <div class="legend-text">Оранжевая линия. Для каждого среза: сумма базовых оценок всех задач среза × P1 × P2.</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch forecast-line"></span>
              <div>
                <div class="legend-name">Объем.Прогноз</div>
                <div class="legend-text">Темно-синяя линия. Складывается по всем Feature и по виртуальной Feature для задач без Feature.</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch current-line"></span>
              <div>
                <div class="legend-name">Объем.Текущий</div>
                <div class="legend-text">Голубая линия. Равна сумме «Объема разработки» и «Объема ошибок», поэтому проходит по верхней точке текущего stacked-столбика.</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch remaining-line"></span>
              <div>
                <div class="legend-name">Объем.Остаток</div>
                <div class="legend-text">Серо-синяя линия. Равна сумме «Остатка разработки» и «Остатка ошибок».</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch dev-bar"></span>
              <div>
                <div class="legend-name">Объем разработки</div>
                <div class="legend-text">Полупрозрачный голубой столбик. В stack с ним выше идет «Объем ошибок».</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch bug-bar"></span>
              <div>
                <div class="legend-name">Объем ошибок</div>
                <div class="legend-text">Полупрозрачный оранжевый столбик поверх объема разработки.</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch dev-rem-bar"></span>
              <div>
                <div class="legend-name">Остаток разработки</div>
                <div class="legend-text">Непрозрачный голубой столбик. В stack с ним выше идет «Остаток ошибок».</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch bug-rem-bar"></span>
              <div>
                <div class="legend-name">Остаток ошибок</div>
                <div class="legend-text">Непрозрачный желтый столбик поверх остатка разработки.</div>
              </div>
            </li>
          </ul>
        </div>
        <div>
          <ul class="formula-list">
            <li>
              <div>
                <div class="legend-name">Разработка</div>
                <div class="formula-text">Если статус задачи «Закрыта», «Решена» или «Отказ», то объем = факт, остаток = 0. Для остальных статусов: объем = max(план, факт), остаток = max(0, план − факт).</div>
              </div>
            </li>
            <li>
              <div>
                <div class="legend-name">Процессы разработки</div>
                <div class="formula-text">Объем = max(план, факт), остаток всегда = 0.</div>
              </div>
            </li>
            <li>
              <div>
                <div class="legend-name">Ошибка</div>
                <div class="formula-text">Логика такая же, как у «Разработки»: закрытые/решенные/отказанные задачи дают объем = факт и остаток = 0, остальные — max(план, факт) и max(0, план − факт).</div>
              </div>
            </li>
            <li>
              <div>
                <div class="legend-name">Feature и виртуальная Feature</div>
                <div class="formula-text">Для каждой Feature отдельно собираются объем/остаток по разработке и по ошибкам. Если Feature в статусе «Готов*», «Закрыта» или «Решена», прогноз = разработка + ошибки. Иначе прогноз = max(текущий объем, сумма базовых оценок задач Feature и самой Feature × P1 × P2). Для задач без Feature считается отдельная виртуальная Feature по тем же правилам.</div>
              </div>
            </li>
          </ul>
          <p class="legend-note">Итоговые линии «Объем.Текущий», «Объем.Остаток» и «Объем.Прогноз» — это суммы по всем Feature и по виртуальной Feature в выбранном апрельском срезе.</p>
        </div>
      </div>
    </section>
  </main>

  <script>
    const burndownDateLabels = {chartDatesJson};
    const burndownSnapshots = {chartSeedsJson};

    const p1Input = document.getElementById("p1Input");
    const p2Input = document.getElementById("p2Input");
    const statusNode = document.getElementById("burndownStatus");
    const chartCanvas = document.getElementById("burndownChart");
    const emptyState = document.getElementById("burndownEmptyState");

    function parseFactor(rawValue, fallbackValue) {{
      const normalized = String(rawValue ?? "").trim().replace(",", ".");
      const parsed = Number.parseFloat(normalized);
      return Number.isFinite(parsed) ? parsed : fallbackValue;
    }}

    function formatHours(value) {{
      return Number(value || 0).toLocaleString("ru-RU", {{
        minimumFractionDigits: 1,
        maximumFractionDigits: 1,
      }});
    }}

    function formatDateLabel(value) {{
      const parts = String(value || "").split("-");
      if (parts.length !== 3) {{
        return String(value || "");
      }}
      return `${{parts[2]}}.${{parts[1]}}.${{parts[0]}}`;
    }}

    function computeSnapshotMetrics(snapshot, p1Value, p2Value) {{
      const groups = Array.isArray(snapshot?.groups) ? snapshot.groups : [];
      let forecast = 0;
      let currentDevelopment = 0;
      let currentBugs = 0;
      let remainingDevelopment = 0;
      let remainingBugs = 0;

      for (const group of groups) {{
        const baselineTotal = Number(group?.baseline_total || 0);
        const developmentVolume = Number(group?.development_volume || 0);
        const bugVolume = Number(group?.bug_volume || 0);
        const developmentRemaining = Number(group?.development_remaining || 0);
        const bugRemaining = Number(group?.bug_remaining || 0);
        const currentTotal = developmentVolume + bugVolume;
        const forecastFloor = baselineTotal * p1Value * p2Value;
        const groupForecast = group?.is_ready ? currentTotal : Math.max(currentTotal, forecastFloor);

        forecast += groupForecast;
        currentDevelopment += developmentVolume;
        currentBugs += bugVolume;
        remainingDevelopment += developmentRemaining;
        remainingBugs += bugRemaining;
      }}

      return {{
        budget: Number(snapshot?.budget_baseline_total || 0) * p1Value * p2Value,
        forecast,
        currentDevelopment,
        currentBugs,
        currentTotal: currentDevelopment + currentBugs,
        remainingDevelopment,
        remainingBugs,
        remainingTotal: remainingDevelopment + remainingBugs,
      }};
    }}

    let burndownChart = null;

    function buildBurndownDatasets(p1Value, p2Value) {{
      const metricsByDate = new Map();
      for (const snapshot of burndownSnapshots) {{
        metricsByDate.set(String(snapshot?.date || ""), computeSnapshotMetrics(snapshot, p1Value, p2Value));
      }}

      const budgetData = [];
      const forecastData = [];
      const currentTotalData = [];
      const currentDevelopmentData = [];
      const currentBugData = [];
      const remainingTotalData = [];
      const remainingDevelopmentData = [];
      const remainingBugData = [];

      for (const currentDate of burndownDateLabels) {{
        const metrics = metricsByDate.get(currentDate);
        if (!metrics) {{
          budgetData.push(null);
          forecastData.push(null);
          currentTotalData.push(null);
          currentDevelopmentData.push(null);
          currentBugData.push(null);
          remainingTotalData.push(null);
          remainingDevelopmentData.push(null);
          remainingBugData.push(null);
          continue;
        }}

        budgetData.push(metrics.budget);
        forecastData.push(metrics.forecast);
        currentTotalData.push(metrics.currentTotal);
        currentDevelopmentData.push(metrics.currentDevelopment);
        currentBugData.push(metrics.currentBugs);
        remainingTotalData.push(metrics.remainingTotal);
        remainingDevelopmentData.push(metrics.remainingDevelopment);
        remainingBugData.push(metrics.remainingBugs);
      }}

      return {{
        budgetData,
        forecastData,
        currentTotalData,
        currentDevelopmentData,
        currentBugData,
        remainingTotalData,
        remainingDevelopmentData,
        remainingBugData,
      }};
    }}

    function renderBurndownChart() {{
      const p1Value = parseFactor(p1Input.value, 1);
      const p2Value = parseFactor(p2Input.value, 1);

      if (!burndownSnapshots.length) {{
        emptyState.style.display = "block";
        chartCanvas.style.display = "none";
        statusNode.textContent = "За апрель текущего года пока нет срезов для расчета диаграммы.";
        return;
      }}

      emptyState.style.display = "none";
      chartCanvas.style.display = "block";

      if (typeof Chart === "undefined") {{
        statusNode.textContent = "Не удалось загрузить библиотеку графиков.";
        return;
      }}

      const datasets = buildBurndownDatasets(p1Value, p2Value);
      statusNode.textContent = `P1 = ${{formatHours(p1Value)}}, P2 = ${{formatHours(p2Value)}}. Срезов в расчете: ${{burndownSnapshots.length}}.`;
      const allChartValues = [
        ...datasets.budgetData,
        ...datasets.forecastData,
        ...datasets.currentTotalData,
        ...datasets.remainingTotalData,
        ...datasets.currentDevelopmentData,
        ...datasets.currentBugData,
        ...datasets.remainingDevelopmentData,
        ...datasets.remainingBugData,
      ].filter((value) => value !== null && value !== undefined);
      const maxChartValue = allChartValues.length
        ? Math.max(...allChartValues.map((value) => Number(value || 0)))
        : 0;
      const chartMax = maxChartValue > 0 ? maxChartValue * 1.08 : 10;

      const chartConfig = {{
        data: {{
          labels: burndownDateLabels,
          datasets: [
            {{
              type: "bar",
              label: "Объем разработки",
              data: datasets.currentDevelopmentData,
              stack: "current",
              backgroundColor: "rgba(82, 206, 230, 0.38)",
              borderColor: "rgba(82, 206, 230, 0.9)",
              borderWidth: 1,
              yAxisID: "yBars",
              order: 3,
            }},
            {{
              type: "bar",
              label: "Объем ошибок",
              data: datasets.currentBugData,
              stack: "current",
              backgroundColor: "rgba(255, 108, 14, 0.30)",
              borderColor: "rgba(255, 108, 14, 0.85)",
              borderWidth: 1,
              yAxisID: "yBars",
              order: 3,
            }},
            {{
              type: "bar",
              label: "Остаток разработки",
              data: datasets.remainingDevelopmentData,
              stack: "remaining",
              backgroundColor: "#52cee6",
              borderColor: "#52cee6",
              borderWidth: 1,
              yAxisID: "yBars",
              order: 3,
            }},
            {{
              type: "bar",
              label: "Остаток ошибок",
              data: datasets.remainingBugData,
              stack: "remaining",
              backgroundColor: "#ffc600",
              borderColor: "#ffc600",
              borderWidth: 1,
              yAxisID: "yBars",
              order: 3,
            }},
            {{
              type: "line",
              label: "Бюджет",
              data: datasets.budgetData,
              borderColor: "#ff6c0e",
              backgroundColor: "#ff6c0e",
              borderWidth: 3,
              pointRadius: 0,
              pointHoverRadius: 4,
              spanGaps: true,
              tension: 0.2,
              yAxisID: "yLines",
              order: 1,
            }},
            {{
              type: "line",
              label: "Объем.Прогноз",
              data: datasets.forecastData,
              borderColor: "#375d77",
              backgroundColor: "#375d77",
              borderWidth: 3,
              pointRadius: 0,
              pointHoverRadius: 4,
              spanGaps: true,
              tension: 0.2,
              yAxisID: "yLines",
              order: 1,
            }},
            {{
              type: "line",
              label: "Объем.Текущий",
              data: datasets.currentTotalData,
              borderColor: "#0f9bb8",
              backgroundColor: "#0f9bb8",
              borderWidth: 2,
              pointRadius: 0,
              pointHoverRadius: 4,
              spanGaps: true,
              tension: 0.15,
              yAxisID: "yLines",
              order: 1,
            }},
            {{
              type: "line",
              label: "Объем.Остаток",
              data: datasets.remainingTotalData,
              borderColor: "#7b8c9d",
              backgroundColor: "#7b8c9d",
              borderWidth: 2,
              pointRadius: 0,
              pointHoverRadius: 4,
              spanGaps: true,
              tension: 0.15,
              yAxisID: "yLines",
              order: 1,
            }},
          ],
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          interaction: {{
            mode: "index",
            intersect: false,
          }},
          plugins: {{
            legend: {{
              position: "bottom",
              labels: {{
                usePointStyle: true,
                boxWidth: 12,
              }},
            }},
            tooltip: {{
              callbacks: {{
                title(items) {{
                  return items.length ? formatDateLabel(items[0].label) : "";
                }},
                label(context) {{
                  return `${{context.dataset.label}}: ${{formatHours(context.parsed.y)}}`;
                }},
              }},
            }},
          }},
          scales: {{
            x: {{
              stacked: true,
              ticks: {{
                autoSkip: false,
                maxRotation: 0,
                minRotation: 0,
                callback(value) {{
                  const label = this.getLabelForValue(value);
                  return String(label || "").endsWith("-01") ? formatDateLabel(label).slice(0, 5) : "";
                }},
              }},
              grid: {{
                display: false,
              }},
            }},
            yBars: {{
              stacked: true,
              beginAtZero: true,
              max: chartMax,
              ticks: {{
                callback(value) {{
                  return formatHours(value);
                }},
              }},
            }},
            yLines: {{
              position: "left",
              beginAtZero: true,
              stacked: false,
              max: chartMax,
              display: false,
              grid: {{
                display: false,
              }},
            }},
          }},
        }},
      }};

      if (burndownChart) {{
        burndownChart.data = chartConfig.data;
        burndownChart.options = chartConfig.options;
        burndownChart.update();
        return;
      }}

      burndownChart = new Chart(chartCanvas, chartConfig);
    }}

    let rerenderTimer = null;

    function scheduleBurndownRender() {{
      if (rerenderTimer) {{
        window.clearTimeout(rerenderTimer);
      }}
      rerenderTimer = window.setTimeout(renderBurndownChart, 180);
    }}

    p1Input.addEventListener("input", scheduleBurndownRender);
    p2Input.addEventListener("input", scheduleBurndownRender);

    renderBurndownChart();
  </script>
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
        <li>При частичной загрузке из закрытых задач попадают только задачи, закрытые начиная с <code>{previousYearStart}</code>.</li>
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
    snapshotPayload = getFilteredSnapshotIssuesForProjectByDate(
        projectRedmineId,
        capturedForDate,
        page=1,
        pageSize=1000,
    )
    snapshotRun = snapshotPayload["snapshot_run"]
    issues = snapshotPayload["issues"]
    availableDates = [str(value) for value in snapshotPayload.get("available_dates") or []]
    storedProjects = listStoredProjects()
    storedProject = next(
        (item for item in storedProjects if int(item.get("redmine_id") or 0) == projectRedmineId),
        None,
    )
    storedProjectIdentifierRaw = str((storedProject or {}).get("identifier") or "").strip()

    if snapshotRun is None:
        optionsHtml = "".join(
            f'<option value="{escape(dateValue)}">{escape(dateValue)}</option>' for dateValue in availableDates
        )
        navPanelHtml = buildProjectContextNavPanel(
            projectRedmineId,
            storedProjectIdentifierRaw,
            currentPage="snapshots",
        )
        return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Задачи среза проекта</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    body {{ margin: 0; font-family: "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: #ffffff; color: #16324a; }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 24px 20px 48px; }}
    {buildProjectContextNavCss()}
    h1 {{ margin: 18px 0 12px; font-size: 2rem; }}
    .meta {{ color: #64798d; margin: 0 0 24px; }}
  </style>
</head>
<body>
    <main>
      {navPanelHtml}
      <h1>Задачи среза проекта</h1>
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

    issueRowsHtml = ['<tr><td colspan="12">Загружаем задачи...</td></tr>']

    summaryView = buildSnapshotSummaryView(snapshotPayload.get("summary"))
    totalBaselineEstimateHours = summaryView["baseline_estimate_hours"]
    totalEstimatedHours = summaryView["estimated_hours"]
    totalSpentHours = summaryView["spent_hours"]
    totalSpentHoursYear = summaryView["spent_hours_year"]
    developmentEstimateHours = summaryView["development_estimated_hours"]
    developmentSpentHours = summaryView["development_spent_hours"]
    developmentSpentHoursYear = summaryView["development_spent_hours_year"]
    developmentProcessEstimateHours = summaryView["development_process_estimated_hours"]
    developmentProcessSpentHours = summaryView["development_process_spent_hours"]
    developmentProcessSpentHoursYear = summaryView["development_process_spent_hours_year"]
    bugEstimateHours = summaryView["bug_estimated_hours"]
    bugSpentHours = summaryView["bug_spent_hours"]
    bugSpentHoursYear = summaryView["bug_spent_hours_year"]

    projectName = escape(str(snapshotRun.get("project_name") or "—"))
    capturedForDateRaw = str(snapshotRun.get("captured_for_date") or "")
    capturedForDate = escape(capturedForDateRaw or "—")
    selectedDate = capturedForDateRaw
    projectIdentifierRaw = str(
        snapshotRun.get("project_identifier")
        or (storedProject.get("identifier") if storedProject else "")
    ).strip()
    snapshotPageUrl = f"/projects/{projectRedmineId}/latest-snapshot-issues"
    if selectedDate:
        snapshotPageUrl += f"?captured_for_date={quote(selectedDate)}"
    comparePageUrl = f"/projects/{projectRedmineId}/compare-snapshots"
    if selectedDate:
        comparePageUrl += f"?right_date={quote(selectedDate)}"
    navPanelHtml = buildProjectContextNavPanel(
        projectRedmineId,
        projectIdentifierRaw,
        currentPage="snapshots",
        snapshotUrl=snapshotPageUrl,
        compareUrl=comparePageUrl,
    )
    initialIssuesJson = json.dumps(issues, ensure_ascii=False, default=str)
    initialSummaryJson = json.dumps(summaryView, ensure_ascii=False, default=str)
    initialFilterOptionsJson = json.dumps(snapshotPayload.get("filter_options") or {}, ensure_ascii=False, default=str)
    initialFilteredIssues = int(snapshotPayload.get("total_filtered_issues") or 0)
    initialTotalIssues = int(snapshotPayload.get("total_all_issues") or 0)
    initialPage = int(snapshotPayload.get("page") or 1)
    initialTotalPages = int(snapshotPayload.get("total_pages") or 1)
    initialPageSize = int(snapshotPayload.get("page_size") or 1000)
    optionsHtml = ["<option value=\"\">Последний срез</option>"]
    for dateValue in availableDates:
        selectedAttr = " selected" if dateValue == selectedDate else ""
        optionsHtml.append(f'<option value="{escape(dateValue)}"{selectedAttr}>{escape(dateValue)}</option>')

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Задачи среза проекта</title>
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
      {buildProjectContextNavCss()}
      .toolbar {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin: 0 0 16px; }}
      .toolbar button {{
        background: #eef2f5;
        color: var(--text);
        border: 1px solid var(--line);
        box-shadow: none;
      }}
      .toolbar button:hover {{
        background: #e4eaef;
      }}
      h1 {{ margin: 18px 0 12px; font-size: clamp(2rem, 4vw, 3rem); line-height: 1.05; }}
      form {{ display: flex; gap: 10px; align-items: center; margin: 0; flex-wrap: wrap; }}
      label {{ font-weight: 600; }}
      select,
      input[type="number"] {{ border: 1px solid var(--line); border-radius: 6px; padding: 8px 10px; font: inherit; }}
      button {{ border: 0; border-radius: 6px; padding: 10px 14px; font: inherit; font-weight: 600; cursor: pointer; background: #ff6c0e; color: #ffffff; }}
      .secondary-button {{ background: #375d77; color: #ffffff; }}
      .meta {{ color: var(--muted); margin: 0 0 24px; font-size: 1rem; }}
      .action-status {{ color: var(--muted); margin: 0 0 18px; min-height: 22px; }}
      .summary-block {{ margin: 0 0 20px; }}
      .summary-table {{ width: 100%; border-collapse: collapse; table-layout: fixed; border: 1px solid var(--line); }}
      .summary-table th,
      .summary-table td {{ border: 1px solid var(--line); padding: 12px 10px; vertical-align: middle; }}
      .summary-table thead th {{ position: static; background: #ffffff; color: var(--text); text-transform: none; font-size: 0.98rem; letter-spacing: 0; }}
      .summary-table tbody th {{ background: #ffffff; color: var(--text); text-transform: none; font-size: 1rem; font-weight: 500; }}
      .summary-table .summary-metric {{ text-align: right; font-size: 1.02rem; font-weight: 400; color: #173b5a; white-space: nowrap; }}
      .summary-table .summary-percent {{ font-weight: 700; }}
      .summary-table .summary-empty {{ background: #ffffff; }}
      .filter-input-table,
      .filter-select-table,
      .filter-number-value,
      .filter-number-op {{ width: 100%; border: 1px solid var(--line); border-radius: 6px; padding: 7px 8px; font: inherit; background: #ffffff; color: var(--text); }}
      .filter-select-table {{ min-height: 92px; }}
      .filter-number-op,
      .filter-number-value {{ width: 45px; }}
      .filter-number-value {{
        appearance: textfield;
        -moz-appearance: textfield;
      }}
      .filter-number-value::-webkit-outer-spin-button,
      .filter-number-value::-webkit-inner-spin-button {{
        -webkit-appearance: none;
        margin: 0;
      }}
      .filter-number-wrap {{ display: flex; flex-direction: column; align-items: flex-start; gap: 4px; }}
      .filter-head th {{ top: var(--snapshot-filter-top, 44px); background: #f7fbfc; padding-top: 8px; padding-bottom: 8px; z-index: 3; text-transform: none; box-shadow: inset 0 1px 0 #d9e5eb; }}
      .filter-reset-wrap {{ display: flex; justify-content: space-between; align-items: center; gap: 10px; margin: 0 0 10px; flex-wrap: wrap; }}
      .table-actions {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
      .filter-reset-button {{ background: #375d77; color: #ffffff; }}
      .csv-export-button {{ background: #375d77; color: #ffffff; }}
      .filter-tip {{ color: var(--muted); font-size: 0.92rem; }}
      .page-size-label {{ color: var(--muted); }}
      .page-size-input {{ width: 110px; }}
      .pagination-wrap {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; margin: 0 0 12px; flex-wrap: wrap; }}
      .pagination-buttons {{ display: flex; gap: 8px; align-items: center; }}
      .pagination-info {{ color: var(--muted); font-size: 0.94rem; }}
      .table-wrap {{ max-height: calc(100vh - 220px); overflow: auto; border: 1px solid var(--line); border-radius: 8px; }}
      table {{ width: 100%; border-collapse: separate; border-spacing: 0; background: var(--panel); }}
      #snapshotIssuesTable {{ min-width: 1800px; table-layout: auto; }}
      th, td {{ text-align: left; padding: 12px 14px; border-bottom: 1px solid var(--line); vertical-align: top; }}
      th {{ position: sticky; top: 0; z-index: 2; background: #eef6f7; color: #426179; text-transform: uppercase; font-size: 0.88rem; }}
      tr:last-child td {{ border-bottom: 0; }}
      .mono {{ font-family: Consolas, "Courier New", monospace; font-size: 0.95rem; white-space: nowrap; }}
      .issue-link {{ color: var(--blue); text-decoration: none; border-bottom: 1px dashed currentColor; font-weight: 700; }}
      .issue-link:hover {{ color: var(--orange); border-bottom-style: solid; }}
      .subject-col {{ width: 546px; min-width: 546px; max-width: 546px; white-space: normal; word-break: break-word; }}
      .tracker-col {{ width: 170px; min-width: 170px; max-width: 170px; white-space: normal; word-break: break-word; }}
      .status-col {{ width: 170px; min-width: 170px; max-width: 170px; white-space: normal; word-break: break-word; }}
      .closed-col {{ width: 190px; min-width: 190px; max-width: 190px; white-space: normal; word-break: break-word; }}
      .version-col {{ width: 360px; min-width: 360px; max-width: 360px; white-space: normal; word-break: break-word; }}
      .snapshot-group-row td {{
        background: #f3f7fa;
        color: #16324a;
        font-weight: 400;
        border-bottom: 1px solid var(--line);
      }}
      .snapshot-group-cell {{
        padding-top: 14px;
        padding-bottom: 14px;
      }}
      .snapshot-group-label {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
      }}
      .snapshot-group-kind {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 22px;
        padding: 2px 8px;
        border-radius: 999px;
        background: #dfe9ef;
        color: #375d77;
        font-size: 0.78rem;
        font-weight: 700;
      }}
      .snapshot-group-subject {{
        font-weight: 700;
      }}
      .snapshot-group-id {{
        display: inline-flex;
        flex-direction: column;
        align-items: flex-start;
        gap: 4px;
      }}
      .snapshot-group-id-label {{
        font-size: 0.78rem;
        font-weight: 700;
        color: #64798d;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }}
      .snapshot-group-id-empty {{
        color: #8ca0b2;
      }}
      .snapshot-group-metric {{
        font-weight: 400;
      }}
      .snapshot-child-subject {{
        display: inline-block;
        padding-left: 22px;
        position: relative;
      }}
      .snapshot-child-subject::before {{
        content: "";
        position: absolute;
        left: 8px;
        top: 0.8em;
        width: 8px;
        border-top: 1px solid #aabcca;
      }}
  </style>
    </head>
  <body>
    <main>
      {navPanelHtml}
      <h1>Задачи среза проекта</h1>
      <div class="toolbar">
      <form method="get">
        <label for="capturedForDate">Дата среза</label>
        <select id="capturedForDate" name="captured_for_date" onchange="this.form.submit()">
          {''.join(optionsHtml)}
        </select>
      </form>
      <label class="page-size-label" for="snapshotPageSizeInput">Задач на странице</label>
      <input class="page-size-input" id="snapshotPageSizeInput" type="number" min="10" max="10000" step="10" value="{initialPageSize}">
      <button type="button" class="secondary-button" id="applySnapshotPageSizeButton">Показать</button>
      <button type="button" class="secondary-button" id="exportSnapshotCsvButton">Выгрузить CSV</button>
      <button type="button" id="recaptureSnapshotButton">Обновить последний срез</button>
      <button type="button" id="deleteSnapshotButton">Удалить выбранный срез</button>
      </div>
      <div class="action-status" id="snapshotActionStatus"></div>
      <p class="meta">Проект: {projectName}. Дата среза: {capturedForDate}. По фильтру: <span id="filteredIssuesCount">{initialFilteredIssues}</span> из {initialTotalIssues}. На странице: <span id="pageIssuesCount">{len(issues)}</span>.</p>
      <div class="summary-block">
        <table class="summary-table">
          <thead>
            <tr>
              <th style="width: 33%"></th>
              <th>Базовая оценка</th>
              <th>План</th>
              <th colspan="2">Факт (год)</th>
              <th>% (год)</th>
              <th colspan="2">Факт (всего)</th>
              <th>% (всего)</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <th>Все задачи</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryEstimated">{formatPageHours(totalEstimatedHours)}</td>
              <td class="summary-metric" id="summarySpentYear" colspan="2">{formatPageHours(totalSpentHoursYear)}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summarySpent" colspan="2">{formatPageHours(totalSpentHours)}</td>
              <td class="summary-empty"></td>
            </tr>
            <tr>
              <th>Разработка, ч</th>
              <td class="summary-metric" id="summaryBaselineEstimate" rowspan="2">{formatPageHours(totalBaselineEstimateHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentEstimated">{formatPageHours(developmentEstimateHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentSpentYear">{formatPageHours(developmentSpentHoursYear)}</td>
              <td class="summary-metric" id="summaryDevelopmentCombinedSpentYear" rowspan="2">{formatPageHours(summaryView["development_combined_spent_hours_year"])}</td>
              <td class="summary-empty" rowspan="2"></td>
              <td class="summary-metric" id="summaryDevelopmentSpent">{formatPageHours(developmentSpentHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentCombinedSpent" rowspan="2">{formatPageHours(summaryView["development_combined_spent_hours"])}</td>
              <td class="summary-metric summary-percent" id="summaryDevelopmentCoverageAll" rowspan="2">{formatPageHours(summaryView["development_coverage_all_percent"])}%</td>
            </tr>
            <tr>
              <th>Процессы разработки, ч</th>
              <td class="summary-metric" id="summaryDevelopmentProcessEstimated">{formatPageHours(developmentProcessEstimateHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentProcessSpentYear">{formatPageHours(developmentProcessSpentHoursYear)}</td>
              <td class="summary-metric" id="summaryDevelopmentProcessSpent">{formatPageHours(developmentProcessSpentHours)}</td>
            </tr>
            <tr>
              <th>Ошибка, ч</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryBugEstimated">{formatPageHours(bugEstimateHours)}</td>
              <td class="summary-metric" id="summaryBugSpentYear" colspan="2">{formatPageHours(bugSpentHoursYear)}</td>
              <td class="summary-metric summary-percent" id="summaryBugShareYear">{formatPageHours(summaryView["bug_share_year_percent"])}%</td>
              <td class="summary-metric" id="summaryBugSpent" colspan="2">{formatPageHours(bugSpentHours)}</td>
              <td class="summary-metric summary-percent" id="summaryBugShareAll">{formatPageHours(summaryView["bug_share_all_percent"])}%</td>
            </tr>
            <tr>
              <th>Итого по разработке</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentTotalEstimated">{formatPageHours(summaryView["development_total_estimated_hours"])}</td>
              <td class="summary-metric" id="summaryDevelopmentGrandSpentYear" colspan="2">{formatPageHours(summaryView["development_grand_spent_hours_year"])}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentGrandSpent" colspan="2">{formatPageHours(summaryView["development_grand_spent_hours"])}</td>
              <td class="summary-empty"></td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="filter-reset-wrap">
        <span class="filter-tip">Фильтры применяются к таблице и суммам выше. Суммы считаются по всем задачам, удовлетворяющим фильтру, а не только по текущей странице.</span>
        <div class="table-actions">
          <button type="button" class="filter-reset-button" id="resetSnapshotFiltersButton">Сбросить фильтр</button>
        </div>
      </div>
      <div class="pagination-wrap">
        <div class="pagination-buttons">
          <button type="button" class="secondary-button" id="snapshotPrevPageButton">← Назад</button>
          <button type="button" class="secondary-button" id="snapshotNextPageButton">Вперед →</button>
        </div>
        <div class="pagination-info" id="snapshotPaginationInfo">Страница {initialPage} из {initialTotalPages}</div>
      </div>
      <div class="table-wrap">
        <table id="snapshotIssuesTable">
        <thead>
          <tr>
            <th>ID</th>
            <th class="subject-col">Тема</th>
            <th class="tracker-col">Трекер</th>
            <th class="status-col">Статус</th>
            <th>Готово, %</th>
            <th>Базовая оценка, ч</th>
            <th>План, ч</th>
            <th>Факт всего, ч</th>
            <th>Факт за год, ч</th>
            <th class="closed-col">Закрыта</th>
            <th>Исполнитель</th>
            <th class="version-col">Версия</th>
          </tr>
          <tr class="filter-head">
            <th><input class="filter-input-table" type="text" data-filter-key="issueId" data-filter-role="text"></th>
            <th><input class="filter-input-table" type="text" data-filter-key="subject" data-filter-role="text"></th>
            <th class="tracker-col"><select class="filter-select-table" multiple data-filter-key="tracker" data-filter-role="multi"></select></th>
            <th class="status-col"><select class="filter-select-table" multiple data-filter-key="status" data-filter-role="multi"></select></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="doneRatio" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="1" data-filter-key="doneRatio" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="baseline" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="baseline" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="estimated" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="estimated" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="spent" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="spent" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="spentYear" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="spentYear" data-filter-role="value"></div></th>
            <th class="closed-col"><input class="filter-input-table" type="text" data-filter-key="closedOn" data-filter-role="text"></th>
            <th><input class="filter-input-table" type="text" data-filter-key="assignedTo" data-filter-role="text"></th>
            <th class="version-col"><input class="filter-input-table" type="text" data-filter-key="fixedVersion" data-filter-role="text"></th>
          </tr>
        </thead>
        <tbody id="snapshotIssuesTableBody">
          {''.join(issueRowsHtml)}
        </tbody>
      </table>
    </div>
    <script>
      const snapshotActionStatus = document.getElementById("snapshotActionStatus");
      const filteredIssuesCount = document.getElementById("filteredIssuesCount");
      const pageIssuesCount = document.getElementById("pageIssuesCount");
      const snapshotIssuesTableBody = document.getElementById("snapshotIssuesTableBody");
      const snapshotIssuesTable = document.getElementById("snapshotIssuesTable");
      const snapshotPageSizeInput = document.getElementById("snapshotPageSizeInput");
      const applySnapshotPageSizeButton = document.getElementById("applySnapshotPageSizeButton");
      const exportSnapshotCsvButton = document.getElementById("exportSnapshotCsvButton");
      const snapshotPrevPageButton = document.getElementById("snapshotPrevPageButton");
      const snapshotNextPageButton = document.getElementById("snapshotNextPageButton");
      const snapshotPaginationInfo = document.getElementById("snapshotPaginationInfo");
      const textFilterInputs = Array.from(document.querySelectorAll("[data-filter-role='text']"));
      const multiSelectFilters = Array.from(document.querySelectorAll("[data-filter-role='multi']"));
      const numericFilterControls = Array.from(document.querySelectorAll("[data-filter-role='op'], [data-filter-role='value']"));
      const resetSnapshotFiltersButton = document.getElementById("resetSnapshotFiltersButton");
      const initialSnapshotIssues = {initialIssuesJson};
      const initialSnapshotSummary = {initialSummaryJson};
      const initialFilterOptions = {initialFilterOptionsJson};
      const selectedSnapshotDate = {json.dumps(selectedDate, ensure_ascii=False)};
      const snapshotPageSizeStorageKey = "latestSnapshotPageSize";
      let currentSnapshotPage = {initialPage};
      let currentSnapshotTotalPages = {initialTotalPages};
      let currentSnapshotFilteredIssues = {initialFilteredIssues};
      let currentSnapshotTotalIssues = {initialTotalIssues};
      let currentSnapshotPageSize = {initialPageSize};
      let snapshotReloadTimer = null;
      const summaryBaselineEstimate = document.getElementById("summaryBaselineEstimate");
      const summaryEstimated = document.getElementById("summaryEstimated");
      const summarySpent = document.getElementById("summarySpent");
      const summarySpentYear = document.getElementById("summarySpentYear");
      const summaryDevelopmentEstimated = document.getElementById("summaryDevelopmentEstimated");
      const summaryDevelopmentSpent = document.getElementById("summaryDevelopmentSpent");
      const summaryDevelopmentCombinedSpentYear = document.getElementById("summaryDevelopmentCombinedSpentYear");
      const summaryDevelopmentCombinedSpent = document.getElementById("summaryDevelopmentCombinedSpent");
      const summaryDevelopmentCoverageAll = document.getElementById("summaryDevelopmentCoverageAll");
      const summaryDevelopmentTotalEstimated = document.getElementById("summaryDevelopmentTotalEstimated");
      const summaryDevelopmentGrandSpentYear = document.getElementById("summaryDevelopmentGrandSpentYear");
      const summaryDevelopmentGrandSpent = document.getElementById("summaryDevelopmentGrandSpent");
      const summaryBugShareYear = document.getElementById("summaryBugShareYear");
      const summaryBugShareAll = document.getElementById("summaryBugShareAll");
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

      function escapeHtml(value) {{
        return String(value ?? "").replace(/[&<>"']/g, (char) => {{
          return {{
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
          }}[char] || char;
        }});
      }}

      function formatFilterHours(value) {{
        const parsed = Number(value ?? 0);
        if (!Number.isFinite(parsed)) {{
          return "0,0";
        }}
        return parsed.toFixed(1).replace(".", ",");
      }}

      function formatFilterPercent(value) {{
        return `${{formatFilterHours(value)}}%`;
      }}

      function formatSnapshotDateTime(value) {{
        if (!value) {{
          return "—";
        }}
        return String(value).replace("T", " ").replace("+00:00", " UTC");
      }}

      function buildSummaryView(summary) {{
        const baselineEstimateHours = Number(summary?.baseline_estimate_hours || 0);
        const estimatedHours = Number(summary?.estimated_hours || 0);
        const spentHours = Number(summary?.spent_hours || 0);
        const spentHoursYear = Number(summary?.spent_hours_year || 0);
        const developmentEstimatedHours = Number(summary?.development_estimated_hours || 0);
        const developmentSpentHours = Number(summary?.development_spent_hours || 0);
        const developmentSpentHoursYear = Number(summary?.development_spent_hours_year || 0);
        const developmentProcessEstimatedHours = Number(summary?.development_process_estimated_hours || 0);
        const developmentProcessSpentHours = Number(summary?.development_process_spent_hours || 0);
        const developmentProcessSpentHoursYear = Number(summary?.development_process_spent_hours_year || 0);
        const bugEstimatedHours = Number(summary?.bug_estimated_hours || 0);
        const bugSpentHours = Number(summary?.bug_spent_hours || 0);
        const bugSpentHoursYear = Number(summary?.bug_spent_hours_year || 0);
        const developmentCombinedSpentHours = developmentSpentHours + developmentProcessSpentHours;
        const developmentCombinedSpentHoursYear = developmentSpentHoursYear + developmentProcessSpentHoursYear;
        return {{
          baselineEstimateHours,
          estimatedHours,
          spentHours,
          spentHoursYear,
          developmentEstimatedHours,
          developmentSpentHours,
          developmentSpentHoursYear,
          developmentProcessEstimatedHours,
          developmentProcessSpentHours,
          developmentProcessSpentHoursYear,
          bugEstimatedHours,
          bugSpentHours,
          bugSpentHoursYear,
          developmentCombinedSpentHours,
          developmentCombinedSpentHoursYear,
          developmentCoverageAllPercent: baselineEstimateHours > 0 ? (developmentCombinedSpentHours / baselineEstimateHours) * 100 : 0,
          developmentTotalEstimatedHours: developmentEstimatedHours + developmentProcessEstimatedHours + bugEstimatedHours,
          developmentGrandSpentHoursYear: developmentCombinedSpentHoursYear + bugSpentHoursYear,
          developmentGrandSpentHours: developmentCombinedSpentHours + bugSpentHours,
          bugShareYearPercent: developmentCombinedSpentHoursYear > 0 ? (bugSpentHoursYear / developmentCombinedSpentHoursYear) * 100 : 0,
          bugShareAllPercent: developmentCombinedSpentHours > 0 ? (bugSpentHours / developmentCombinedSpentHours) * 100 : 0,
        }};
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

      function fillMultiSelect(select, values) {{
        if (!select) {{
          return;
        }}
        const selectedValues = new Set(Array.from(select.selectedOptions).map((option) => option.value));
        select.innerHTML = "";
        for (const value of values || []) {{
          const option = document.createElement("option");
          option.value = String(value);
          option.textContent = String(value);
          option.selected = selectedValues.has(String(value));
          select.appendChild(option);
        }}
      }}

      function populateSnapshotMultiSelects() {{
        fillMultiSelect(document.querySelector('[data-filter-key="tracker"][data-filter-role="multi"]'), initialFilterOptions.tracker_names || []);
        fillMultiSelect(document.querySelector('[data-filter-key="status"][data-filter-role="multi"]'), initialFilterOptions.status_names || []);
      }}

      function renderSnapshotSummary(summary) {{
        const view = buildSummaryView(summary);
        if (summaryBaselineEstimate) summaryBaselineEstimate.textContent = formatFilterHours(view.baselineEstimateHours);
        if (summaryEstimated) summaryEstimated.textContent = formatFilterHours(view.estimatedHours);
        if (summarySpent) summarySpent.textContent = formatFilterHours(view.spentHours);
        if (summarySpentYear) summarySpentYear.textContent = formatFilterHours(view.spentHoursYear);
        if (summaryDevelopmentEstimated) summaryDevelopmentEstimated.textContent = formatFilterHours(view.developmentEstimatedHours);
        if (summaryDevelopmentSpent) summaryDevelopmentSpent.textContent = formatFilterHours(view.developmentSpentHours);
        if (summaryDevelopmentSpentYear) summaryDevelopmentSpentYear.textContent = formatFilterHours(view.developmentSpentHoursYear);
        if (summaryDevelopmentProcessEstimated) summaryDevelopmentProcessEstimated.textContent = formatFilterHours(view.developmentProcessEstimatedHours);
        if (summaryDevelopmentProcessSpent) summaryDevelopmentProcessSpent.textContent = formatFilterHours(view.developmentProcessSpentHours);
        if (summaryDevelopmentProcessSpentYear) summaryDevelopmentProcessSpentYear.textContent = formatFilterHours(view.developmentProcessSpentHoursYear);
        if (summaryBugEstimated) summaryBugEstimated.textContent = formatFilterHours(view.bugEstimatedHours);
        if (summaryBugSpent) summaryBugSpent.textContent = formatFilterHours(view.bugSpentHours);
        if (summaryBugSpentYear) summaryBugSpentYear.textContent = formatFilterHours(view.bugSpentHoursYear);
        if (summaryDevelopmentCombinedSpentYear) summaryDevelopmentCombinedSpentYear.textContent = formatFilterHours(view.developmentCombinedSpentHoursYear);
        if (summaryDevelopmentCombinedSpent) summaryDevelopmentCombinedSpent.textContent = formatFilterHours(view.developmentCombinedSpentHours);
        if (summaryDevelopmentCoverageAll) summaryDevelopmentCoverageAll.textContent = formatFilterPercent(view.developmentCoverageAllPercent);
        if (summaryDevelopmentTotalEstimated) summaryDevelopmentTotalEstimated.textContent = formatFilterHours(view.developmentTotalEstimatedHours);
        if (summaryDevelopmentGrandSpentYear) summaryDevelopmentGrandSpentYear.textContent = formatFilterHours(view.developmentGrandSpentHoursYear);
        if (summaryDevelopmentGrandSpent) summaryDevelopmentGrandSpent.textContent = formatFilterHours(view.developmentGrandSpentHours);
        if (summaryBugShareYear) summaryBugShareYear.textContent = formatFilterPercent(view.bugShareYearPercent);
        if (summaryBugShareAll) summaryBugShareAll.textContent = formatFilterPercent(view.bugShareAllPercent);
      }}

      function renderSnapshotRows(issues) {{
        if (!snapshotIssuesTableBody) {{
          return;
        }}
        if (!Array.isArray(issues) || !issues.length) {{
          snapshotIssuesTableBody.innerHTML = '<tr><td colspan="12">По текущему фильтру задач нет.</td></tr>';
          return;
        }}
        let lastGroupKey = "";
        const rows = [];
        for (const issue of issues) {{
          const groupId = issue?.feature_group_issue_redmine_id;
          const groupKey = groupId ? `feature-${{groupId}}` : "virtual-feature";
          const groupSubject = String(issue?.feature_group_subject || "без Feature");
          const isVirtualGroup = Boolean(issue?.feature_group_is_virtual);
          if (groupKey !== lastGroupKey) {{
            const groupLink = !isVirtualGroup && groupId
              ? `<a class="issue-link" href="https://redmine.sms-it.ru/issues/${{encodeURIComponent(groupId)}}" target="_blank" rel="noreferrer">${{escapeHtml(groupId)}}</a>`
              : "";
            const groupTracker = isVirtualGroup ? "—" : escapeHtml(issue?.feature_group_tracker_name || "Feature");
            const groupStatus = isVirtualGroup ? "—" : escapeHtml(issue?.feature_group_status_name || "—");
            const groupDoneRatio = isVirtualGroup ? "—" : escapeHtml(issue?.feature_group_done_ratio ?? 0);
            const groupBaseline = isVirtualGroup ? "—" : formatFilterHours(issue?.feature_group_baseline_estimate_hours);
            const groupEstimated = isVirtualGroup ? "—" : formatFilterHours(issue?.feature_group_estimated_hours);
            const groupSpent = isVirtualGroup ? "—" : formatFilterHours(issue?.feature_group_spent_hours);
            const groupSpentYear = isVirtualGroup ? "—" : formatFilterHours(issue?.feature_group_spent_hours_year);
            const groupClosedOn = isVirtualGroup ? "—" : escapeHtml(formatSnapshotDateTime(issue?.feature_group_closed_on));
            const groupAssignedTo = isVirtualGroup ? "—" : escapeHtml(issue?.feature_group_assigned_to_name || "—");
            const groupVersion = isVirtualGroup ? "—" : escapeHtml(issue?.feature_group_fixed_version_name || "—");
            const groupIdCell = isVirtualGroup
              ? `<span class="snapshot-group-id"><span class="snapshot-group-id-empty">—</span></span>`
              : `<span class="snapshot-group-id">${{groupLink}}<span class="snapshot-group-id-label">Feature</span></span>`;
            rows.push(`
              <tr class="snapshot-group-row">
                <td class="snapshot-group-cell mono">${{groupIdCell}}</td>
                <td class="snapshot-group-cell subject-col"><span class="snapshot-group-subject">${{escapeHtml(groupSubject)}}</span></td>
                <td class="snapshot-group-cell tracker-col">${{groupTracker}}</td>
                <td class="snapshot-group-cell status-col">${{groupStatus}}</td>
                <td class="snapshot-group-cell snapshot-group-metric">${{groupDoneRatio}}</td>
                <td class="snapshot-group-cell snapshot-group-metric">${{groupBaseline}}</td>
                <td class="snapshot-group-cell snapshot-group-metric">${{groupEstimated}}</td>
                <td class="snapshot-group-cell snapshot-group-metric">${{groupSpent}}</td>
                <td class="snapshot-group-cell snapshot-group-metric">${{groupSpentYear}}</td>
                <td class="snapshot-group-cell closed-col">${{groupClosedOn}}</td>
                <td class="snapshot-group-cell">${{groupAssignedTo}}</td>
                <td class="snapshot-group-cell version-col">${{groupVersion}}</td>
              </tr>
            `);
            lastGroupKey = groupKey;
          }}

          if (issue?.is_feature_group_root) {{
            continue;
          }}

          const issueId = issue?.issue_redmine_id ?? "—";
          const issueLink = `https://redmine.sms-it.ru/issues/${{encodeURIComponent(issueId)}}`;
          rows.push(`
            <tr>
              <td class="mono"><a class="issue-link" href="${{issueLink}}" target="_blank" rel="noreferrer">${{escapeHtml(issueId)}}</a></td>
              <td class="subject-col"><span class="snapshot-child-subject">${{escapeHtml(issue?.subject || "—")}}</span></td>
              <td class="tracker-col">${{escapeHtml(issue?.tracker_name || "—")}}</td>
              <td class="status-col">${{escapeHtml(issue?.status_name || "—")}}</td>
              <td>${{escapeHtml(issue?.done_ratio ?? 0)}}</td>
              <td>${{formatFilterHours(issue?.baseline_estimate_hours)}}</td>
              <td>${{formatFilterHours(issue?.estimated_hours)}}</td>
              <td>${{formatFilterHours(issue?.spent_hours)}}</td>
              <td>${{formatFilterHours(issue?.spent_hours_year)}}</td>
              <td class="closed-col">${{escapeHtml(formatSnapshotDateTime(issue?.closed_on))}}</td>
              <td>${{escapeHtml(issue?.assigned_to_name || "—")}}</td>
              <td class="version-col">${{escapeHtml(issue?.fixed_version_name || "—")}}</td>
            </tr>
          `);
        }}
        snapshotIssuesTableBody.innerHTML = rows.join("");
      }}

      function normalizeNumericFilterValue(value) {{
        return String(value || "").trim().replace(",", ".");
      }}

      function readSnapshotPageSize() {{
        const rawValue = Number(snapshotPageSizeInput?.value || currentSnapshotPageSize || 1000);
        if (!Number.isFinite(rawValue)) {{
          return 1000;
        }}
        return Math.min(10000, Math.max(10, Math.floor(rawValue)));
      }}

      function updateSnapshotCounts(pageCount) {{
        if (filteredIssuesCount) filteredIssuesCount.textContent = String(currentSnapshotFilteredIssues);
        if (pageIssuesCount) pageIssuesCount.textContent = String(pageCount);
      }}

      function updateSnapshotPaginationInfo() {{
        if (snapshotPaginationInfo) {{
          snapshotPaginationInfo.textContent = `Страница ${{currentSnapshotPage}} из ${{currentSnapshotTotalPages}}`;
        }}
        if (snapshotPrevPageButton) snapshotPrevPageButton.disabled = currentSnapshotPage <= 1;
        if (snapshotNextPageButton) snapshotNextPageButton.disabled = currentSnapshotPage >= currentSnapshotTotalPages;
      }}

      function collectSnapshotFilters() {{
        return {{
          issue_id: String(document.querySelector('[data-filter-key="issueId"][data-filter-role="text"]')?.value || "").trim(),
          subject: String(document.querySelector('[data-filter-key="subject"][data-filter-role="text"]')?.value || "").trim(),
          tracker: Array.from(document.querySelector('[data-filter-key="tracker"][data-filter-role="multi"]')?.selectedOptions || []).map((option) => option.value),
          status: Array.from(document.querySelector('[data-filter-key="status"][data-filter-role="multi"]')?.selectedOptions || []).map((option) => option.value),
          done_ratio_op: String(document.querySelector('[data-filter-key="doneRatio"][data-filter-role="op"]')?.value || ""),
          done_ratio_value: normalizeNumericFilterValue(document.querySelector('[data-filter-key="doneRatio"][data-filter-role="value"]')?.value || ""),
          baseline_op: String(document.querySelector('[data-filter-key="baseline"][data-filter-role="op"]')?.value || ""),
          baseline_value: normalizeNumericFilterValue(document.querySelector('[data-filter-key="baseline"][data-filter-role="value"]')?.value || ""),
          estimated_op: String(document.querySelector('[data-filter-key="estimated"][data-filter-role="op"]')?.value || ""),
          estimated_value: normalizeNumericFilterValue(document.querySelector('[data-filter-key="estimated"][data-filter-role="value"]')?.value || ""),
          spent_op: String(document.querySelector('[data-filter-key="spent"][data-filter-role="op"]')?.value || ""),
          spent_value: normalizeNumericFilterValue(document.querySelector('[data-filter-key="spent"][data-filter-role="value"]')?.value || ""),
          spent_year_op: String(document.querySelector('[data-filter-key="spentYear"][data-filter-role="op"]')?.value || ""),
          spent_year_value: normalizeNumericFilterValue(document.querySelector('[data-filter-key="spentYear"][data-filter-role="value"]')?.value || ""),
          closed_on: String(document.querySelector('[data-filter-key="closedOn"][data-filter-role="text"]')?.value || "").trim(),
          assigned_to: String(document.querySelector('[data-filter-key="assignedTo"][data-filter-role="text"]')?.value || "").trim(),
          fixed_version: String(document.querySelector('[data-filter-key="fixedVersion"][data-filter-role="text"]')?.value || "").trim(),
        }};
      }}

      function buildSnapshotQueryParams(page, includePagination = true) {{
        const filters = collectSnapshotFilters();
        const params = new URLSearchParams();
        if (selectedSnapshotDate) params.set("captured_for_date", selectedSnapshotDate);
        if (includePagination) {{
          const pageSize = readSnapshotPageSize();
          params.set("page", String(page));
          params.set("page_size", String(pageSize));
        }}
        if (filters.issue_id) params.set("issue_id", filters.issue_id);
        if (filters.subject) params.set("subject", filters.subject);
        for (const value of filters.tracker) params.append("tracker", value);
        for (const value of filters.status) params.append("status", value);
        if (filters.done_ratio_op) params.set("done_ratio_op", filters.done_ratio_op);
        if (filters.done_ratio_value) params.set("done_ratio_value", filters.done_ratio_value);
        if (filters.baseline_op) params.set("baseline_op", filters.baseline_op);
        if (filters.baseline_value) params.set("baseline_value", filters.baseline_value);
        if (filters.estimated_op) params.set("estimated_op", filters.estimated_op);
        if (filters.estimated_value) params.set("estimated_value", filters.estimated_value);
        if (filters.spent_op) params.set("spent_op", filters.spent_op);
        if (filters.spent_value) params.set("spent_value", filters.spent_value);
        if (filters.spent_year_op) params.set("spent_year_op", filters.spent_year_op);
        if (filters.spent_year_value) params.set("spent_year_value", filters.spent_year_value);
        if (filters.closed_on) params.set("closed_on", filters.closed_on);
        if (filters.assigned_to) params.set("assigned_to", filters.assigned_to);
        if (filters.fixed_version) params.set("fixed_version", filters.fixed_version);
        return params;
      }}

      async function loadSnapshotIssues(page = 1) {{
        try {{
          const pageSize = readSnapshotPageSize();
          currentSnapshotPageSize = pageSize;
          if (snapshotPageSizeInput) snapshotPageSizeInput.value = String(pageSize);
          window.localStorage.setItem(snapshotPageSizeStorageKey, String(pageSize));
          if (snapshotPaginationInfo) snapshotPaginationInfo.textContent = "Обновляем таблицу...";
          const params = buildSnapshotQueryParams(page, true);
          const response = await fetch(`/api/projects/{projectRedmineId}/latest-snapshot-issues?${{params.toString()}}`);
          const payload = await response.json();
          if (!response.ok) {{
            window.alert(payload.detail || "Не удалось загрузить задачи среза.");
            updateSnapshotPaginationInfo();
            return;
          }}

          currentSnapshotPage = Number(payload.page || 1);
          currentSnapshotTotalPages = Number(payload.total_pages || 1);
          currentSnapshotFilteredIssues = Number(payload.total_filtered_issues || 0);
          currentSnapshotTotalIssues = Number(payload.total_all_issues || 0);
          renderSnapshotRows(payload.issues || []);
          renderSnapshotSummary(payload.summary || {{}});
          updateSnapshotCounts(Array.isArray(payload.issues) ? payload.issues.length : 0);
          updateSnapshotPaginationInfo();
          updateSnapshotFilterHeaderOffset();
        }} catch (error) {{
          window.alert("Не удалось загрузить задачи среза.");
          updateSnapshotPaginationInfo();
        }}
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
        loadSnapshotIssues(1);
      }}

      function scheduleSnapshotReload() {{
        if (snapshotReloadTimer) {{
          window.clearTimeout(snapshotReloadTimer);
        }}
        snapshotReloadTimer = window.setTimeout(() => loadSnapshotIssues(1), 250);
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

      applySnapshotPageSizeButton?.addEventListener("click", () => {{
        loadSnapshotIssues(1);
      }});

      exportSnapshotCsvButton?.addEventListener("click", () => {{
        const params = buildSnapshotQueryParams(currentSnapshotPage, false);
        window.location.href = `/projects/{projectRedmineId}/latest-snapshot-issues/export.csv?${{params.toString()}}`;
      }});

      snapshotPrevPageButton?.addEventListener("click", () => {{
        if (currentSnapshotPage > 1) {{
          loadSnapshotIssues(currentSnapshotPage - 1);
        }}
      }});

      snapshotNextPageButton?.addEventListener("click", () => {{
        if (currentSnapshotPage < currentSnapshotTotalPages) {{
          loadSnapshotIssues(currentSnapshotPage + 1);
        }}
      }});

      textFilterInputs.forEach((input) => {{
        input.addEventListener("input", scheduleSnapshotReload);
      }});
      multiSelectFilters.forEach((select) => {{
        select.addEventListener("change", () => loadSnapshotIssues(1));
      }});
      numericFilterControls.forEach((control) => {{
        control.addEventListener("input", scheduleSnapshotReload);
        control.addEventListener("change", scheduleSnapshotReload);
      }});

      resetSnapshotFiltersButton?.addEventListener("click", resetSnapshotTableFilters);
      populateSnapshotMultiSelects();
      renderSnapshotRows(initialSnapshotIssues);
      renderSnapshotSummary(initialSnapshotSummary);
      updateSnapshotCounts(initialSnapshotIssues.length);
      updateSnapshotFilterHeaderOffset();
      updateSnapshotPaginationInfo();
      window.addEventListener("resize", updateSnapshotFilterHeaderOffset);

      const storedSnapshotPageSize = Number(window.localStorage.getItem(snapshotPageSizeStorageKey) || 0);
      if (Number.isFinite(storedSnapshotPageSize) && storedSnapshotPageSize >= 10 && storedSnapshotPageSize <= 10000) {{
        snapshotPageSizeInput.value = String(Math.floor(storedSnapshotPageSize));
        if (storedSnapshotPageSize !== currentSnapshotPageSize) {{
          loadSnapshotIssues(1);
        }}
      }}
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


@app.get("/api/projects/{project_redmine_id}/latest-snapshot-issues")
def getProjectLatestSnapshotIssuesData(
    project_redmine_id: int,
    captured_for_date: str | None = Query(None, description="Дата среза в формате YYYY-MM-DD"),
    page: int = Query(1, ge=1),
    page_size: int = Query(1000, ge=10, le=10000),
    issue_id: str | None = Query(None),
    subject: str | None = Query(None),
    tracker: list[str] = Query([]),
    status: list[str] = Query([]),
    done_ratio_op: str | None = Query(None),
    done_ratio_value: str | None = Query(None),
    baseline_op: str | None = Query(None),
    baseline_value: str | None = Query(None),
    estimated_op: str | None = Query(None),
    estimated_value: str | None = Query(None),
    spent_op: str | None = Query(None),
    spent_value: str | None = Query(None),
    spent_year_op: str | None = Query(None),
    spent_year_value: str | None = Query(None),
    closed_on: str | None = Query(None),
    assigned_to: str | None = Query(None),
    fixed_version: str | None = Query(None),
) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    filters = buildSnapshotIssueFiltersPayload(
        issueId=issue_id,
        subject=subject,
        trackerNames=tracker,
        statusNames=status,
        doneRatioOp=done_ratio_op,
        doneRatioValue=done_ratio_value,
        baselineOp=baseline_op,
        baselineValue=baseline_value,
        estimatedOp=estimated_op,
        estimatedValue=estimated_value,
        spentOp=spent_op,
        spentValue=spent_value,
        spentYearOp=spent_year_op,
        spentYearValue=spent_year_value,
        closedOn=closed_on,
        assignedTo=assigned_to,
        fixedVersion=fixed_version,
    )
    return getFilteredSnapshotIssuesForProjectByDate(
        project_redmine_id,
        captured_for_date,
        filters=filters,
        page=page,
        pageSize=page_size,
    )


@app.get("/projects/{project_redmine_id}/latest-snapshot-issues/export.csv")
def exportProjectLatestSnapshotIssuesCsv(
    project_redmine_id: int,
    captured_for_date: str | None = Query(None, description="Дата среза в формате YYYY-MM-DD"),
    issue_id: str | None = Query(None),
    subject: str | None = Query(None),
    tracker: list[str] = Query([]),
    status: list[str] = Query([]),
    done_ratio_op: str | None = Query(None),
    done_ratio_value: str | None = Query(None),
    baseline_op: str | None = Query(None),
    baseline_value: str | None = Query(None),
    estimated_op: str | None = Query(None),
    estimated_value: str | None = Query(None),
    spent_op: str | None = Query(None),
    spent_value: str | None = Query(None),
    spent_year_op: str | None = Query(None),
    spent_year_value: str | None = Query(None),
    closed_on: str | None = Query(None),
    assigned_to: str | None = Query(None),
    fixed_version: str | None = Query(None),
) -> Response:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    filters = buildSnapshotIssueFiltersPayload(
        issueId=issue_id,
        subject=subject,
        trackerNames=tracker,
        statusNames=status,
        doneRatioOp=done_ratio_op,
        doneRatioValue=done_ratio_value,
        baselineOp=baseline_op,
        baselineValue=baseline_value,
        estimatedOp=estimated_op,
        estimatedValue=estimated_value,
        spentOp=spent_op,
        spentValue=spent_value,
        spentYearOp=spent_year_op,
        spentYearValue=spent_year_value,
        closedOn=closed_on,
        assignedTo=assigned_to,
        fixedVersion=fixed_version,
    )
    exportPayload = listFilteredSnapshotIssuesForProjectByDate(
        project_redmine_id,
        captured_for_date,
        filters=filters,
    )
    snapshotRun = exportPayload.get("snapshot_run")
    if snapshotRun is None:
        raise HTTPException(status_code=404, detail="Срез проекта не найден")

    output = io.StringIO(newline="")
    output.write("sep=;\n")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(
        [
            "ID",
            "Тема",
            "Трекер",
            "Статус",
            "Готово, %",
            "Базовая оценка, ч",
            "План, ч",
            "Факт всего, ч",
            "Факт за год, ч",
            "Закрыта",
            "Исполнитель",
            "Версия",
        ]
    )
    for issue in exportPayload.get("issues") or []:
        writer.writerow(
            [
                issue.get("issue_redmine_id") or "",
                str(issue.get("subject") or "—"),
                str(issue.get("tracker_name") or "—"),
                str(issue.get("status_name") or "—"),
                issue.get("done_ratio") if issue.get("done_ratio") is not None else 0,
                formatPageHours(issue.get("baseline_estimate_hours")),
                formatPageHours(issue.get("estimated_hours")),
                formatPageHours(issue.get("spent_hours")),
                formatPageHours(issue.get("spent_hours_year")),
                formatSnapshotPageDateTime(issue.get("closed_on")),
                str(issue.get("assigned_to_name") or "—"),
                str(issue.get("fixed_version_name") or "—"),
            ]
        )

    fileIdentifier = str(snapshotRun.get("project_identifier") or f"project_{project_redmine_id}")
    safeIdentifier = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in fileIdentifier)
    fileDate = str(snapshotRun.get("captured_for_date") or "latest")
    fileName = f"snapshot_{safeIdentifier}_{fileDate}.csv"
    csvText = output.getvalue()
    csvBytes = csvText.encode("cp1251", errors="replace")
    return Response(
        content=csvBytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": f'attachment; filename="{fileName}"'},
    )


@app.get("/projects/{project_redmine_id}/compare-snapshots", response_class=HTMLResponse)
def getProjectSnapshotComparePage(
    project_redmine_id: int,
    left_date: str | None = Query(None, description="Дата первого среза в формате YYYY-MM-DD"),
    right_date: str | None = Query(None, description="Дата второго среза в формате YYYY-MM-DD"),
    field: list[str] = Query([]),
) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    return HTMLResponse(buildSnapshotComparisonPage(project_redmine_id, left_date, right_date, field))


@app.get("/projects/{project_redmine_id}/burndown", response_class=HTMLResponse)
def getProjectBurndownPage(project_redmine_id: int) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    return HTMLResponse(buildBurndownPage(project_redmine_id))


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



