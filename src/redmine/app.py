from datetime import UTC, datetime
from html import escape
import csv
import hashlib
import hmac
import io
import json
from pathlib import Path
import secrets
import requests
from datetime import date, timedelta
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.redmine.config import loadConfig
from src.redmine.db import (
    checkDatabaseConnection,
    countIssueSnapshotRuns,
    createPlanningProject,
    createUser,
    deleteIssueSnapshotForProjectDate,
    deleteIssueSnapshotsForDate,
    deletePlanningProject,
    deleteUser,
    ensureIssueSnapshotTables,
    ensurePlanningProjectsTable,
    ensureProjectsTable,
    ensureUsersTable,
    getFilteredSnapshotIssuesForProjectByDate,
    getSnapshotRunsWithIssuesForProjectYear,
    getSnapshotIssuesForProjectByDate,
    getUserByLogin,
    listFilteredSnapshotIssuesForProjectByDate,
    listLatestSnapshotIssuesWithParents,
    listPlanningProjects,
    listRecentIssueSnapshotRuns,
    listSnapshotDatesForProject,
    listStoredProjects,
    listUsers,
    pruneUnchangedIssueSnapshots,
    seedInitialUsers,
    syncProjects,
    updateUser,
    updateUserPassword,
    updatePlanningProject,
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
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
SESSION_SECRET = config.sessionSecret or secrets.token_hex(32)
USER_ROLE = "User"
FINANCE_ROLE = "Finance"
ADMIN_ROLE = "Admin"
ALL_ROLES = (USER_ROLE, FINANCE_ROLE, ADMIN_ROLE)
DEFAULT_ADMIN_LOGINS = (
    "andrey.shopin@sms-a.ru",
    "stanislav.shidlovskiy@sms-a.ru",
    "sergey.laptev@sms-a.ru",
)

LOCAL_GOLOS_FONT_CSS = """
    @font-face {
      font-family: "Golos";
      font-style: normal;
      font-weight: 400;
      font-display: swap;
      src: url("/static/fonts/GolosText-400.ttf") format("truetype");
    }

    @font-face {
      font-family: "Golos";
      font-style: normal;
      font-weight: 500;
      font-display: swap;
      src: url("/static/fonts/GolosText-500.ttf") format("truetype");
    }

    @font-face {
      font-family: "Golos";
      font-style: normal;
      font-weight: 600;
      font-display: swap;
      src: url("/static/fonts/GolosText-600.ttf") format("truetype");
    }

    @font-face {
      font-family: "Golos";
      font-style: normal;
      font-weight: 700;
      font-display: swap;
      src: url("/static/fonts/GolosText-700.ttf") format("truetype");
    }

    @font-face {
      font-family: "Golos";
      font-style: normal;
      font-weight: 800;
      font-display: swap;
      src: url("/static/fonts/GolosText-800.ttf") format("truetype");
    }
""".strip()

GOOGLE_FONTS_SNIPPETS = (
    '  <link rel="preconnect" href="https://fonts.googleapis.com">\n',
    '  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n',
    '  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">\n',
)


def _renderHtmlPage(html: str) -> HTMLResponse:
    for snippet in GOOGLE_FONTS_SNIPPETS:
        html = html.replace(snippet, "")

    html = html.replace('"Golos Text"', '"Golos"')

    if LOCAL_GOLOS_FONT_CSS not in html:
        html = html.replace("<style>", f"<style>\n{LOCAL_GOLOS_FONT_CSS}\n", 1)

    return HTMLResponse(html)


def _hashPassword(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 260000)
    return f"pbkdf2_sha256$260000${salt.hex()}${derived.hex()}"


def _verifyPassword(password: str, passwordHash: str) -> bool:
    try:
        algorithm, iterationsRaw, saltHex, digestHex = passwordHash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterationsRaw)
        derived = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(saltHex),
            iterations,
        )
        return hmac.compare_digest(derived.hex(), digestHex)
    except Exception:
        return False


def _normalizeRoles(roles: list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    normalized = [role for role in (roles or []) if role in ALL_ROLES]
    if ADMIN_ROLE in normalized and USER_ROLE not in normalized:
        normalized.append(USER_ROLE)
    if FINANCE_ROLE in normalized and USER_ROLE not in normalized:
        normalized.append(USER_ROLE)
    order = {USER_ROLE: 0, FINANCE_ROLE: 1, ADMIN_ROLE: 2}
    return sorted(set(normalized), key=lambda role: order.get(role, 99))


def _serializeRoles(roles: list[str] | tuple[str, ...] | set[str] | None) -> str:
    return ",".join(_normalizeRoles(roles))


def _parseRoles(rawRoles: object) -> list[str]:
    if not rawRoles:
        return []
    if isinstance(rawRoles, str):
        return _normalizeRoles([role.strip() for role in rawRoles.split(",") if role.strip()])
    if isinstance(rawRoles, (list, tuple, set)):
        return _normalizeRoles([str(role) for role in rawRoles])
    return []


def _hasRole(user: dict[str, object] | None, role: str) -> bool:
    if not user:
        return False
    roles = _parseRoles(user.get("roles"))
    return role in roles


def _publicPath(path: str) -> bool:
    return path in {
        "/login",
        "/logout",
        "/change-password",
        "/health",
        "/db-health",
        "/api/auth/login",
        "/api/auth/change-password",
    } or path.startswith("/static")


def _seedDefaultAdminUsers() -> None:
    ensureUsersTable()
    seedInitialUsers(
        [
            {
                "login": login,
                "password_hash": _hashPassword("123"),
                "roles": _serializeRoles([ADMIN_ROLE]),
                "must_change_password": True,
            }
            for login in DEFAULT_ADMIN_LOGINS
        ]
    )


def _ensureAuthStorage() -> None:
    ensureUsersTable()
    _seedDefaultAdminUsers()


def _getCurrentUser(request: Request) -> dict[str, object] | None:
    try:
        userLogin = request.session.get("user_login")
    except (AssertionError, RuntimeError):
        return None

    if not userLogin:
        return None

    user = getUserByLogin(str(userLogin))
    if not user:
        request.session.clear()
        return None

    user["roles"] = _parseRoles(user.get("roles"))
    return user


def _requireAuthenticatedUser(request: Request) -> dict[str, object]:
    user = getattr(request.state, "current_user", None) or _getCurrentUser(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def _requireAdminUser(request: Request) -> dict[str, object]:
    user = _requireAuthenticatedUser(request)
    if not _hasRole(user, ADMIN_ROLE):
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


@app.middleware("http")
async def authMiddleware(request: Request, call_next):
    if not config.databaseUrl:
        return await call_next(request)

    path = request.url.path
    if _publicPath(path):
        if path.startswith("/api/auth/"):
            _ensureAuthStorage()
        return await call_next(request)

    _ensureAuthStorage()

    user = _getCurrentUser(request)
    if not user:
        if path.startswith("/api/"):
            return JSONResponse({"detail": "Authentication required"}, status_code=401)
        return RedirectResponse(url=f"/login?next={quote(str(request.url.path))}", status_code=303)

    request.state.current_user = user

    if bool(user.get("must_change_password")) and path != "/change-password":
        if path.startswith("/api/"):
            return JSONResponse({"detail": "Password change required", "must_change_password": True}, status_code=403)
        return RedirectResponse(url="/change-password", status_code=303)

    return await call_next(request)


app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax", https_only=False)


class ProjectSettingsUpdate(BaseModel):
    enabled_project_ids: list[int] = []
    partial_project_ids: list[int] = []


class PlanningProjectPayload(BaseModel):
    project_name: str
    redmine_identifier: str | None = None
    pm_name: str | None = None
    customer: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    baseline_estimate_hours: float | None = None
    p1: float | None = None
    p2: float | None = None
    estimate_doc_url: str | None = None
    bitrix_url: str | None = None
    comment_text: str | None = None


class LoginPayload(BaseModel):
    login: str
    password: str


class ChangePasswordPayload(BaseModel):
    new_password: str


class UserPayload(BaseModel):
    login: str
    password: str | None = None
    roles: list[str] = []
    must_change_password: bool = False


def _buildRedmineApiSession() -> requests.Session:
    session = requests.Session()
    session.headers.update({"X-Redmine-API-Key": config.apiKey})
    return session


def _parseRedmineIsoDate(value: str | None) -> datetime | None:
    if not value:
        return None

    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _isIssueIncludedByPartialRules(issuePayload: dict[str, object], cutoffDateIso: str) -> tuple[bool, str]:
    status = issuePayload.get("status") or {}
    statusName = str(status.get("name") or "")
    isClosed = bool(status.get("is_closed"))
    closedOnRaw = issuePayload.get("closed_on")
    closedOn = _parseRedmineIsoDate(str(closedOnRaw) if closedOnRaw else None)
    cutoffDate = date.fromisoformat(cutoffDateIso)

    if not isClosed:
        return True, f"Р—Р°РґР°С‡Р° РѕС‚РєСЂС‹С‚Р° РїРѕ СЃС‚Р°С‚СѓСЃСѓ В«{statusName}В», РїРѕСЌС‚РѕРјСѓ РїРѕРїР°РґР°РµС‚ РІ С‡Р°СЃС‚РёС‡РЅС‹Р№ СЃСЂРµР·."

    if closedOn is None:
        return False, (
            f"Р—Р°РґР°С‡Р° Р·Р°РєСЂС‹С‚Р° РїРѕ СЃС‚Р°С‚СѓСЃСѓ В«{statusName}В», РЅРѕ Сѓ РЅРµРµ РЅРµС‚ РґР°С‚С‹ closed_on, "
            "РїРѕСЌС‚РѕРјСѓ РїРѕ С‚РµРєСѓС‰РёРј РїСЂР°РІРёР»Р°Рј РІ С‡Р°СЃС‚РёС‡РЅС‹Р№ СЃСЂРµР· РЅРµ РїРѕРїР°РґР°РµС‚."
        )

    if closedOn.date() >= cutoffDate:
        return True, (
            f"Р—Р°РґР°С‡Р° Р·Р°РєСЂС‹С‚Р° {closedOn.date().isoformat()}, СЌС‚Рѕ РЅРµ СЂР°РЅСЊС€Рµ РїРѕСЂРѕРіР° {cutoffDateIso}, "
            "РїРѕСЌС‚РѕРјСѓ РІ С‡Р°СЃС‚РёС‡РЅС‹Р№ СЃСЂРµР· РїРѕРїР°РґР°РµС‚."
        )

    return False, (
        f"Р—Р°РґР°С‡Р° Р·Р°РєСЂС‹С‚Р° {closedOn.date().isoformat()}, СЌС‚Рѕ СЂР°РЅСЊС€Рµ РїРѕСЂРѕРіР° {cutoffDateIso}, "
        "РїРѕСЌС‚РѕРјСѓ РІ С‡Р°СЃС‚РёС‡РЅС‹Р№ СЃСЂРµР· РЅРµ РїРѕРїР°РґР°РµС‚."
    )


def _fetchRedmineIssueById(session: requests.Session, issueRedmineId: int) -> dict[str, object] | None:
    response = session.get(
        f"{config.redmineUrl.rstrip('/')}/issues/{issueRedmineId}.json",
        params={"include": "children"},
        timeout=60,
    )
    response.raise_for_status()
    return (response.json() or {}).get("issue") or None


def _normalizeSearchText(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _findMatchingCustomFields(
    issuePayload: dict[str, object],
    fieldNameQuery: str,
) -> list[dict[str, object]]:
    normalizedQuery = _normalizeSearchText(fieldNameQuery)
    matches: list[dict[str, object]] = []
    for field in issuePayload.get("custom_fields") or []:
        fieldName = str(field.get("name") or "")
        normalizedName = _normalizeSearchText(fieldName)
        if normalizedQuery and normalizedQuery in normalizedName:
            matches.append(
                {
                    "id": field.get("id"),
                    "name": fieldName,
                    "value": field.get("value"),
                }
            )

    return matches


def getLatestSnapshotIssuesWithExternalParents() -> dict[str, object]:
    ensureIssueSnapshotTables()

    candidates = listLatestSnapshotIssuesWithParents()
    if not candidates:
        return {"issues": [], "checked_count": 0, "error_count": 0}

    latestIssuesById: dict[int, dict[str, object]] = {}
    for issue in candidates:
        issueId = int(issue.get("issue_redmine_id") or 0)
        if issueId:
            latestIssuesById[issueId] = issue

    resultIssues: list[dict[str, object]] = []

    for issue in candidates:
        parentIssueId = int(issue.get("parent_issue_redmine_id") or 0)
        if not parentIssueId:
            continue

        parentIssue = latestIssuesById.get(parentIssueId)
        if not parentIssue:
            continue

        parentProjectId = int(parentIssue.get("project_redmine_id") or 0)
        childProjectId = int(issue.get("project_redmine_id") or 0)
        if not parentProjectId or parentProjectId == childProjectId:
            continue

        resultIssues.append(
            {
                **issue,
                "parent_project_redmine_id": parentProjectId,
                "parent_project_name": parentIssue.get("project_name"),
                "parent_project_identifier": parentIssue.get("project_identifier"),
                "parent_issue_subject": parentIssue.get("subject"),
                "parent_issue_tracker_name": parentIssue.get("tracker_name"),
                "parent_issue_status_name": parentIssue.get("status_name"),
            }
        )

    resultIssues.sort(
        key=lambda issue: (
            str(issue.get("project_name") or "").lower(),
            str(issue.get("captured_for_date") or ""),
            int(issue.get("issue_redmine_id") or 0),
        )
    )
    return {
        "issues": resultIssues,
        "checked_count": len(candidates),
        "error_count": 0,
    }


PAGE_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Redmine: РїСЂРѕРµРєС‚С‹ Рё СЃСЂРµР·С‹</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
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
      font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
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
      font-size: clamp(2.15rem, 4.9vw, 3.6rem);
      line-height: 0.98;
      letter-spacing: -0.04em;
      font-weight: 400;
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

    .quick-links a#adminPageButton {
      background: #eef2f5;
      color: var(--text);
      border-color: var(--line);
      box-shadow: none;
    }

    .quick-links a:nth-child(2) {
      background: var(--blue-302);
      color: #ffffff;
    }

    .quick-links a:nth-child(3) {
      background: var(--yellow-109);
      color: #16324a;
    }

    .quick-links a:nth-child(4) {
      background: var(--cyan-310);
      color: #16324a;
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

    #refreshProjectsButton,
    #planningProjectsPageButton,
    #recaptureSnapshotsButton,
    #strangeIssuesPageButton,
    #deleteSnapshotsButton,
    #pruneSnapshotsButton {
      background: #eef2f6;
      color: var(--blue-302);
      box-shadow: 0 8px 16px rgba(22, 50, 74, 0.05);
    }

    #refreshProjectsButton:hover,
    #planningProjectsPageButton:hover,
    #recaptureSnapshotsButton:hover,
    #strangeIssuesPageButton:hover,
    #deleteSnapshotsButton:hover,
    #pruneSnapshotsButton:hover {
      background: #e4eaef;
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

    #captureSnapshotsButton {
      background: var(--orange-1585);
      color: #ffffff;
      box-shadow: 0 14px 24px rgba(255, 108, 14, 0.22);
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

    .project-name-wrap {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }

    .project-planning-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      border: 1px solid #c9d7df;
      border-radius: 999px;
      background: #ffffff;
      color: #426179;
      text-decoration: none;
      font-size: 0.74rem;
      font-weight: 700;
      line-height: 1;
      box-shadow: none;
    }

    .project-planning-button:hover {
      border-color: #375d77;
      color: #375d77;
      background: #f5fafb;
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
      font-weight: 400;
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
            alt="РЎРњРЎ-РРў"
          >
        </span>
      </a>
      <nav class="hero-nav" aria-label="Р‘С‹СЃС‚СЂС‹Р№ РїРµСЂРµС…РѕРґ РїРѕ СЂР°Р·РґРµР»Р°Рј">
        <div class="quick-links">
          <a id="adminPageButton" href="/admin/users" style="display:none;">РђРґРјРёРЅРёСЃС‚СЂРёСЂРѕРІР°РЅРёРµ</a>
          <a href="#data-load-section">Р—Р°РіСЂСѓР·РєР° РґР°РЅРЅС‹С…</a>
          <a href="#projects-table">РўР°Р±Р»РёС†Р° РїСЂРѕРµРєС‚РѕРІ</a>
          <a href="#snapshot-runs-table">РўР°Р±Р»РёС†Р° СЃСЂРµР·РѕРІ</a>
        </div>
      </nav>
    </div>
  </div>
  <div class="topbar-spacer" aria-hidden="true"></div>

  <main>
    <section class="hero">
      <h1>РђРЅР°Р»РёР· РїСЂРѕРµРєС‚РѕРІ Redmine</h1>
    </section>

    <section class="grid" id="data-load-section">
      <article class="panel" id="project-actions">
        <h2>РџСЂРѕРµРєС‚С‹ Redmine</h2>
        <p>РџРѕР»СѓС‡Р°РµС‚ СЃРїРёСЃРѕРє РїСЂРѕРµРєС‚РѕРІ РёР· Redmine, РґРѕР±Р°РІР»СЏРµС‚ РЅРѕРІС‹Рµ Р·Р°РїРёСЃРё Рё РѕР±РЅРѕРІР»СЏРµС‚ РёР·РјРµРЅРµРЅРЅС‹Рµ.</p>
        <div class="row">
          <button id="refreshProjectsButton" type="button">РћР±РЅРѕРІРёС‚СЊ СЃРїРёСЃРѕРє РїСЂРѕРµРєС‚РѕРІ</button>
          <button id="planningProjectsPageButton" type="button">РџР»Р°РЅРёСЂРѕРІР°РЅРёРµ РїСЂРѕРµРєС‚РѕРІ</button>
        </div>
        <div class="status" id="projectsStatus"></div>
      </article>

      <article class="panel" id="snapshot-actions">
        <h2>РџРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·РѕРІ Р·Р°РґР°С‡</h2>
        <p>
          Р—Р°РїСЂР°С€РёРІР°РµС‚ СЃСЂРµР·С‹ С‚РѕР»СЊРєРѕ РґР»СЏ С‚РµС… РїСЂРѕРµРєС‚РѕРІ, РїРѕ РєРѕС‚РѕСЂС‹Рј РЅР° СЃРµРіРѕРґРЅСЏС€РЅСЋСЋ РґР°С‚Сѓ
          РµС‰Рµ РЅРµС‚ Р·Р°РїРёСЃРё РІ Р±Р°Р·Рµ РґР°РЅРЅС‹С….
        </p>
        <p>
          <a class="project-link" href="/snapshot-rules" target="_blank" rel="noreferrer">РџСЂР°РІРёР»Р° РїРѕР»СѓС‡РµРЅРёСЏ СЃСЂРµР·РѕРІ</a>
        </p>
        <div class="row">
          <button id="captureSnapshotsButton" type="button">РџРѕР»СѓС‡РёС‚СЊ СЃСЂРµР·С‹ Р·Р°РґР°С‡</button>
          <button id="recaptureSnapshotsButton" type="button">РћР±РЅРѕРІРёС‚СЊ РїРѕСЃР»РµРґРЅРёРµ СЃСЂРµР·С‹</button>
          <button id="strangeIssuesPageButton" type="button">Р’РѕРїСЂРѕСЃС‹ РїРѕ Р·Р°РґР°С‡Р°Рј</button>
        </div>
        <div class="status" id="captureStatus"></div>
      </article>

      <article class="panel" id="delete-snapshot">
        <h2>РЈРґР°Р»РµРЅРёРµ СЃСЂРµР·Р° РїРѕ РґР°С‚Рµ</h2>
        <p>РЈРґР°Р»СЏРµС‚ РІСЃРµ СЃСЂРµР·С‹ Рё РІСЃРµ СЃС‚СЂРѕРєРё Р·Р°РґР°С‡ Р·Р° РІС‹Р±СЂР°РЅРЅСѓСЋ РєР°Р»РµРЅРґР°СЂРЅСѓСЋ РґР°С‚Сѓ.</p>
        <div class="row">
          <input id="snapshotDateInput" type="date">
          <button id="deleteSnapshotsButton" class="danger" type="button">РћС‡РёСЃС‚РёС‚СЊ СЃСЂРµР· РЅР° РґР°С‚Сѓ</button>
          <button id="pruneSnapshotsButton" type="button">РџСЂРѕСЂРµРґРёС‚СЊ СЃСЂРµР·С‹</button>
        </div>
        <div class="status" id="deleteStatus"></div>
      </article>
    </section>

    <section class="panel table-panel" id="projects-table">
      <h2>РџСЂРѕРµРєС‚С‹ РІ Р±Р°Р·Рµ РґР°РЅРЅС‹С…</h2>
      <p class="meta" id="projectsCount">Р—Р°РіСЂСѓР·РєР° СЃРїРёСЃРєР° РїСЂРѕРµРєС‚РѕРІ...</p>
      <div class="table-toolbar">
        <label for="projectsNameFilterInput">Р¤РёР»СЊС‚СЂ РїРѕ РЅР°Р·РІР°РЅРёСЋ</label>
        <input
          id="projectsNameFilterInput"
          class="filter-input filter-input-name"
          type="text"
          placeholder="Р’РІРµРґРёС‚Рµ С‡Р°СЃС‚СЊ РЅР°Р·РІР°РЅРёСЏ"
        >
        <label for="projectsFactFilterInput">РњРёРЅ. СЃСѓРјРјР° С„Р°РєС‚Р° Р·Р° РіРѕРґ РїРѕ СЂР°Р·СЂР°Р±РѕС‚РєРµ Рё Р±Р°РіС„РёРєСЃСѓ</label>
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
          <span>РџРѕРєР°Р·С‹РІР°С‚СЊ РІС‹РєР»СЋС‡РµРЅРЅС‹Рµ</span>
        </label>
        <span class="toolbar-spacer"></span>
        <button id="applyProjectsSettingsButton" type="button">РџСЂРёРјРµРЅРёС‚СЊ РЅР°СЃС‚СЂРѕР№РєРё СЃРѕС…СЂР°РЅРµРЅРёСЏ</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th class="checkbox-cell project-sticky-1">
                <label>
                  <input id="enableVisibleProjectsCheckbox" type="checkbox">
                  Р’РєР».
                </label>
              </th>
              <th class="checkbox-cell">Р§Р°СЃС‚.</th>
              <th class="project-sticky-2">ID</th>
              <th class="project-sticky-3">РќР°Р·РІР°РЅРёРµ</th>
              <th class="identifier-col">РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ</th>
              <th>Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°, С‡</th>
              <th>Р Р°Р·СЂР°Р±РѕС‚РєР°: РѕС†РµРЅРєР°, С‡</th>
              <th>Р Р°Р·СЂР°Р±РѕС‚РєР°: С„Р°РєС‚ Р·Р° РіРѕРґ, С‡</th>
              <th>РџСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё: РїР»Р°РЅ, С‡</th>
              <th>РџСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё: С„Р°РєС‚ Р·Р° РіРѕРґ, С‡</th>
              <th>РћС€РёР±РєР°: РѕС†РµРЅРєР°, С‡</th>
              <th>РћС€РёР±РєР°: С„Р°РєС‚ Р·Р° РіРѕРґ, С‡</th>
              <th>РЎС‚Р°С‚СѓСЃ РїСЂРѕРµРєС‚Р°</th>
              <th>Р”Р°С‚Р° РїРѕСЃР»РµРґРЅРµРіРѕ СЃСЂРµР·Р°</th>
              <th>РћР±РЅРѕРІР»РµРЅ РІ Redmine</th>
              <th>РЎРёРЅС…СЂРѕРЅРёР·РёСЂРѕРІР°РЅ</th>
            </tr>
          </thead>
          <tbody id="projectsTableBody"></tbody>
        </table>
      </div>
    </section>

    <section class="panel table-panel" id="snapshot-runs-table">
      <h2>РџРѕСЃР»РµРґРЅРёРµ СЃСЂРµР·С‹ Р·Р°РґР°С‡</h2>
      <p class="meta" id="snapshotRunsCount">Р—Р°РіСЂСѓР·РєР° СЃРїРёСЃРєР° СЃСЂРµР·РѕРІ...</p>
      <div class="table-toolbar">
        <label for="snapshotRunsProjectFilterInput">Р¤РёР»СЊС‚СЂ РїРѕ РїСЂРѕРµРєС‚Сѓ</label>
        <input
          id="snapshotRunsProjectFilterInput"
          class="filter-input snapshot-filter-input"
          type="text"
          placeholder="Р’РІРµРґРёС‚Рµ С‡Р°СЃС‚СЊ РЅР°Р·РІР°РЅРёСЏ РїСЂРѕРµРєС‚Р°"
        >
        <label for="snapshotRunsPerProjectInput">РЎСЂРµР·РѕРІ РЅР° РїСЂРѕРµРєС‚</label>
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
        <button type="button" class="filter-reset-button is-inactive" id="resetSnapshotFiltersButton">РЎР±СЂРѕСЃРёС‚СЊ С„РёР»СЊС‚СЂ</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Р”Р°С‚Р° СЃСЂРµР·Р°</th>
              <th>РџСЂРѕРµРєС‚</th>
              <th>РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ</th>
              <th>Р—Р°РґР°С‡</th>
              <th>Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°, С‡</th>
              <th>РџР»Р°РЅ, С‡</th>
              <th>Р¤Р°РєС‚ РІСЃРµРіРѕ, С‡</th>
              <th>Р¤Р°РєС‚ Р·Р° РіРѕРґ, С‡</th>
              <th>Р—Р°РїРёСЃР°РЅ</th>
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
    const planningProjectsPageButton = document.getElementById("planningProjectsPageButton");
    const strangeIssuesPageButton = document.getElementById("strangeIssuesPageButton");
    const adminPageButton = document.getElementById("adminPageButton");
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
        return "вЂ”";
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

    async function loadCurrentUser() {
      try {
        const response = await fetch("/api/auth/me");
        if (!response.ok) {
          return;
        }

        const payload = await response.json();
        const roles = Array.isArray(payload?.user?.roles) ? payload.user.roles : [];
        if (roles.includes("Admin") && adminPageButton) {
          adminPageButton.style.display = "";
        }
      } catch (error) {
        console.error("Failed to load current user", error);
      }
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
      document.title = "Redmine: РїСЂРѕРµРєС‚С‹ Рё СЃСЂРµР·С‹";
      const texts = [
        [".brand-logo", "alt", "РЎРњРЎ-РРў"],
        [".hero-nav", "aria-label", "Р‘С‹СЃС‚СЂС‹Р№ РїРµСЂРµС…РѕРґ РїРѕ СЂР°Р·РґРµР»Р°Рј"],
        [".quick-links a:nth-child(1)", "textContent", "Р—Р°РіСЂСѓР·РєР° РґР°РЅРЅС‹С…"],
        [".quick-links a:nth-child(2)", "textContent", "РўР°Р±Р»РёС†Р° РїСЂРѕРµРєС‚РѕРІ"],
        [".quick-links a:nth-child(3)", "textContent", "РўР°Р±Р»РёС†Р° СЃСЂРµР·РѕРІ"],
        [".hero h1", "textContent", "РђРЅР°Р»РёР· РїСЂРѕРµРєС‚РѕРІ Redmine"],
        ["#project-actions h2", "textContent", "РџСЂРѕРµРєС‚С‹ Redmine"],
        ["#project-actions p", "textContent", "РџРѕР»СѓС‡Р°РµС‚ СЃРїРёСЃРѕРє РїСЂРѕРµРєС‚РѕРІ РёР· Redmine, РґРѕР±Р°РІР»СЏРµС‚ РЅРѕРІС‹Рµ Р·Р°РїРёСЃРё Рё РѕР±РЅРѕРІР»СЏРµС‚ РёР·РјРµРЅРµРЅРЅС‹Рµ."],
        ["#refreshProjectsButton", "textContent", "РћР±РЅРѕРІРёС‚СЊ СЃРїРёСЃРѕРє РїСЂРѕРµРєС‚РѕРІ"],
        ["#snapshot-actions h2", "textContent", "РџРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·РѕРІ Р·Р°РґР°С‡"],
        ["#snapshot-actions p", "textContent", "Р—Р°РїСЂР°С€РёРІР°РµС‚ СЃСЂРµР·С‹ С‚РѕР»СЊРєРѕ РґР»СЏ С‚РµС… РїСЂРѕРµРєС‚РѕРІ, РїРѕ РєРѕС‚РѕСЂС‹Рј РЅР° СЃРµРіРѕРґРЅСЏС€РЅСЋСЋ РґР°С‚Сѓ РµС‰Рµ РЅРµС‚ Р·Р°РїРёСЃРё РІ Р±Р°Р·Рµ РґР°РЅРЅС‹С…."],
        ["#captureSnapshotsButton", "textContent", "РџРѕР»СѓС‡РёС‚СЊ СЃСЂРµР·С‹ Р·Р°РґР°С‡"],
        ["#recaptureSnapshotsButton", "textContent", "РћР±РЅРѕРІРёС‚СЊ РїРѕСЃР»РµРґРЅРёРµ СЃСЂРµР·С‹"],
        ["#strangeIssuesPageButton", "textContent", "Р’РѕРїСЂРѕСЃС‹ РїРѕ Р·Р°РґР°С‡Р°Рј"],
        ["#delete-snapshot h2", "textContent", "РЈРґР°Р»РµРЅРёРµ СЃСЂРµР·Р° РїРѕ РґР°С‚Рµ"],
        ["#delete-snapshot p", "textContent", "РЈРґР°Р»СЏРµС‚ РІСЃРµ СЃСЂРµР·С‹ Рё РІСЃРµ СЃС‚СЂРѕРєРё Р·Р°РґР°С‡ Р·Р° РІС‹Р±СЂР°РЅРЅСѓСЋ РєР°Р»РµРЅРґР°СЂРЅСѓСЋ РґР°С‚Сѓ."],
        ["#deleteSnapshotsButton", "textContent", "РћС‡РёСЃС‚РёС‚СЊ СЃСЂРµР· РЅР° РґР°С‚Сѓ"],
        ["#pruneSnapshotsButton", "textContent", "РџСЂРѕСЂРµРґРёС‚СЊ СЃСЂРµР·С‹"],
        ["#projects-table h2", "textContent", "РџСЂРѕРµРєС‚С‹ РІ Р±Р°Р·Рµ РґР°РЅРЅС‹С…"],
        ["label[for='projectsNameFilterInput']", "textContent", "Р¤РёР»СЊС‚СЂ РїРѕ РЅР°Р·РІР°РЅРёСЋ"],
        ["#projectsNameFilterInput", "placeholder", "Р’РІРµРґРёС‚Рµ С‡Р°СЃС‚СЊ РЅР°Р·РІР°РЅРёСЏ"],
        ["label[for='projectsFactFilterInput']", "textContent", "РњРёРЅ. СЃСѓРјРјР° С„Р°РєС‚Р° Р·Р° РіРѕРґ РїРѕ СЂР°Р·СЂР°Р±РѕС‚РєРµ Рё Р±Р°РіС„РёРєСЃСѓ"],
        ["#showDisabledProjectsLabel span", "textContent", "РџРѕРєР°Р·С‹РІР°С‚СЊ РІС‹РєР»СЋС‡РµРЅРЅС‹Рµ"],
        ["#applyProjectsSettingsButton", "textContent", "РџСЂРёРјРµРЅРёС‚СЊ РЅР°СЃС‚СЂРѕР№РєРё СЃРѕС…СЂР°РЅРµРЅРёСЏ"],
        ["#snapshot-runs-table h2", "textContent", "РџРѕСЃР»РµРґРЅРёРµ СЃСЂРµР·С‹ Р·Р°РґР°С‡"],
        ["label[for='snapshotRunsProjectFilterInput']", "textContent", "Р¤РёР»СЊС‚СЂ РїРѕ РїСЂРѕРµРєС‚Сѓ"],
        ["#snapshotRunsProjectFilterInput", "placeholder", "Р’РІРµРґРёС‚Рµ С‡Р°СЃС‚СЊ РЅР°Р·РІР°РЅРёСЏ РїСЂРѕРµРєС‚Р°"],
        ["label[for='snapshotRunsPerProjectInput']", "textContent", "РЎСЂРµР·РѕРІ РЅР° РїСЂРѕРµРєС‚"],
        ["#projectsCount", "textContent", "Р—Р°РіСЂСѓР·РєР° СЃРїРёСЃРєР° РїСЂРѕРµРєС‚РѕРІ..."],
        ["#snapshotRunsCount", "textContent", "Р—Р°РіСЂСѓР·РєР° СЃРїРёСЃРєР° СЃСЂРµР·РѕРІ..."],
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
        "Р’РєР».",
        "Р§Р°СЃС‚.",
        "ID",
        "РќР°Р·РІР°РЅРёРµ",
        "РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ",
        "Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°, С‡",
        "Р Р°Р·СЂР°Р±РѕС‚РєР°: РѕС†РµРЅРєР°, С‡",
        "Р Р°Р·СЂР°Р±РѕС‚РєР°: С„Р°РєС‚ Р·Р° РіРѕРґ, С‡",
        "РџСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё: РїР»Р°РЅ, С‡",
        "РџСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё: С„Р°РєС‚ Р·Р° РіРѕРґ, С‡",
        "РћС€РёР±РєР°: РѕС†РµРЅРєР°, С‡",
        "РћС€РёР±РєР°: С„Р°РєС‚ Р·Р° РіРѕРґ, С‡",
        "РЎС‚Р°С‚СѓСЃ РїСЂРѕРµРєС‚Р°",
        "Р”Р°С‚Р° РїРѕСЃР»РµРґРЅРµРіРѕ СЃСЂРµР·Р°",
        "РћР±РЅРѕРІР»РµРЅ РІ Redmine",
        "РЎРёРЅС…СЂРѕРЅРёР·РёСЂРѕРІР°РЅ",
      ];
      document.querySelectorAll("#projects-table thead th").forEach((element, index) => {
        if (index === 0) {
          const label = element.querySelector("label");
          if (label) label.lastChild.textContent = " Р’РєР».";
          return;
        }
        if (projectsHeaders[index]) {
          element.textContent = projectsHeaders[index];
        }
      });

      const snapshotHeaders = [
        "ID",
        "Р”Р°С‚Р° СЃСЂРµР·Р°",
        "РџСЂРѕРµРєС‚",
        "РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ",
        "Р—Р°РґР°С‡",
        "Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°, С‡",
        "РџР»Р°РЅ, С‡",
        "Р¤Р°РєС‚ РІСЃРµРіРѕ, С‡",
        "Р¤Р°РєС‚ Р·Р° РіРѕРґ, С‡",
        "Р—Р°РїРёСЃР°РЅ",
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
      projectsCount.textContent = `РџСЂРѕРµРєС‚РѕРІ РІ Р±Р°Р·Рµ: ${allProjects.length}. РџРѕСЃР»Рµ С„РёР»СЊС‚СЂР°: ${filteredProjects.length}`;

      if (!filteredProjects.length) {
        projectsTableBody.innerHTML = '<tr><td colspan="16">РџСЂРѕРµРєС‚РѕРІ РїРѕРєР° РЅРµС‚.</td></tr>';
        return;
      }

      for (const project of filteredProjects) {
        try {
          const redmineId = Number(project?.redmine_id ?? 0) || project?.redmine_id || "вЂ”";
          const identifier = String(project?.identifier ?? "");
          const projectIssuesUrl = identifier
            ? `https://redmine.sms-it.ru/projects/${encodeURIComponent(identifier)}/issues?utf8=%E2%9C%93&set_filter=1&type=IssueQuery&f%5B%5D=status_id&op%5Bstatus_id%5D=*&query%5Bsort_criteria%5D%5B0%5D%5B%5D=id&query%5Bsort_criteria%5D%5B0%5D%5B%5D=desc&t%5B%5D=cf_27&t%5B%5D=spent_hours&t%5B%5D=estimated_hours&c%5B%5D=tracker&c%5B%5D=parent&c%5B%5D=status&c%5B%5D=priority&c%5B%5D=subject&c%5B%5D=assigned_to&c%5B%5D=estimated_hours&saved_query_id=0&current_project_id=${encodeURIComponent(identifier)}`
            : "";
          const identifierHtml = identifier
            ? `<a class="project-link mono" href="${projectIssuesUrl}" target="_blank" rel="noreferrer">${identifier}</a>`
            : "вЂ”";
          const planningProjectUrl = `/planning-projects?redmine_identifier=${encodeURIComponent(identifier)}&project_name=${encodeURIComponent(project?.name ?? "")}`;
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
                <button class="project-capture-button" type="button" data-project-id="${redmineId}" title="РџРѕР»СѓС‡РёС‚СЊ СЃСЂРµР· РїРѕ РїСЂРѕРµРєС‚Сѓ" ${project?.is_enabled ? "" : "disabled"}>в†“</button>
              </span>
            </td>
            <td class="project-name-cell project-sticky-3"><span class="${projectTreeClass}" style="--tree-level:${level};"><span class="project-name-wrap"><a class="project-link" href="/projects/${encodeURIComponent(redmineId)}/burndown" target="_blank" rel="noreferrer">${project?.name ?? "\u2014"}</a><a class="project-planning-button" href="${planningProjectUrl}" title="РћС‚РєСЂС‹С‚СЊ РїР»Р°РЅРёСЂРѕРІР°РЅРёРµ РїСЂРѕРµРєС‚Р°" aria-label="РћС‚РєСЂС‹С‚СЊ РїР»Р°РЅРёСЂРѕРІР°РЅРёРµ РїСЂРѕРµРєС‚Р°">i</a></span></span></td>
            <td>${identifierHtml}</td>
            <td>${formatHours(project?.baseline_estimate_hours)}</td>
            <td>${formatHours(project?.development_estimate_hours)}</td>
            <td>${formatHours(project?.development_spent_hours_year)}</td>
            <td>${formatHours(project?.development_process_estimate_hours)}</td>
            <td>${formatHours(project?.development_process_spent_hours_year)}</td>
            <td>${formatHours(project?.bug_estimate_hours)}</td>
            <td>${formatHours(project?.bug_spent_hours_year)}</td>
            <td>${project?.status ?? "вЂ”"}</td>
            <td class="mono">${project?.latest_snapshot_date ?? "вЂ”"}</td>
            <td>${formatDate(project?.updated_on)}</td>
            <td>${formatDate(project?.synced_at)}</td>
          `;
          projectsTableBody.appendChild(row);
          renderedCount += 1;
        } catch (error) {
          console.error("РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚СЂРёСЃРѕРІР°С‚СЊ РїСЂРѕРµРєС‚", project, error);
        }
      }

      if (!renderedCount) {
        projectsTableBody.innerHTML = '<tr><td colspan="16">РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚СЂРёСЃРѕРІР°С‚СЊ РїСЂРѕРµРєС‚С‹.</td></tr>';
        throw new Error("РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚СЂРёСЃРѕРІР°С‚СЊ РїСЂРѕРµРєС‚С‹.");
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
      snapshotRunsCount.textContent = `Р’СЃРµРіРѕ СЃСЂРµР·РѕРІ РІ Р±Р°Р·Рµ: ${totalCount}. РџРѕРєР°Р·Р°РЅРѕ: ${filteredRuns.length}`;

      if (!filteredRuns.length) {
        snapshotRunsTableBody.innerHTML = '<tr><td colspan="10">РЎСЂРµР·РѕРІ РїРѕРєР° РЅРµС‚.</td></tr>';
        return;
      }

      for (const run of filteredRuns) {
        const row = document.createElement("tr");
        const projectRuns = groupedRuns.get(Number(run.project_redmine_id ?? 0)) || [];
        const latestRunForProject = projectRuns[0] || run;
        const compareUrl = `/projects/${encodeURIComponent(run.project_redmine_id ?? "")}/compare-snapshots?left_date=${encodeURIComponent(run.captured_for_date ?? "")}&right_date=${encodeURIComponent(latestRunForProject.captured_for_date ?? run.captured_for_date ?? "")}`;
        const identifierValue = run.project_identifier ?? "вЂ”";
        const identifierHtml = run.project_identifier
          ? `<a class="project-link mono" href="${compareUrl}" target="_blank" rel="noreferrer">${identifierValue}</a>`
          : `<span class="mono">${identifierValue}</span>`;
        row.innerHTML = `
          <td class="mono">${run.id ?? "вЂ”"}</td>
          <td class="mono">${run.captured_for_date ?? "вЂ”"}</td>
          <td>${run.project_name ?? "вЂ”"}</td>
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
      setStatus(captureStatus, `Р—Р°РїСѓСЃРєР°РµРј РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·Р° РїРѕ РїСЂРѕРµРєС‚Сѓ ${projectId}...`);

      try {
        const response = await fetch(`/api/issues/snapshots/capture-project/${encodeURIComponent(projectId)}`, { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃСЂРµР·Р° РїРѕ РїСЂРѕРµРєС‚Сѓ.");
        }

        if (payload.captured_for_date) {
          snapshotDateInput.value = payload.captured_for_date;
        }

        setStatus(captureStatus, payload.detail || "Р¤РѕРЅРѕРІР°СЏ Р·Р°РіСЂСѓР·РєР° СЃСЂРµР·Р° РїРѕ РїСЂРѕРµРєС‚Сѓ Р·Р°РїСѓС‰РµРЅР°...");
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
          throw new Error(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РїСЂРѕРµРєС‚С‹ РёР· Р±Р°Р·С‹.");
        }

        renderProjects(payload.projects ?? []);
        setStatus(projectsStatus, "");
      } catch (error) {
        console.error("РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё РїСЂРѕРµРєС‚РѕРІ", error);
        try {
          renderProjects([]);
        } catch (renderError) {
          console.error("РћС€РёР±РєР° РѕС‡РёСЃС‚РєРё С‚Р°Р±Р»РёС†С‹ РїСЂРѕРµРєС‚РѕРІ", renderError);
        }
        setStatus(projectsStatus, error?.message || "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РїСЂРѕРµРєС‚С‹ РёР· Р±Р°Р·С‹.", "error");
      }
    }

    async function loadSnapshotRuns() {
      try {
        const response = await fetch("/api/issues/snapshots/runs");
        const payload = await response.json();
        renderSnapshotRuns(payload.snapshot_runs ?? [], payload.total_count ?? 0);
      } catch (error) {
        renderSnapshotRuns([], 0);
        setStatus(captureStatus, "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ СЃРїРёСЃРѕРє СЃСЂРµР·РѕРІ.", "error");
      }
    }

    async function applyProjectsSettings() {
      applyProjectsSettingsButton.disabled = true;
      setStatus(projectsStatus, "РЎРѕС…СЂР°РЅСЏРµРј РЅР°СЃС‚СЂРѕР№РєРё РїСЂРѕРµРєС‚РѕРІ...");

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
          throw new Error(payload.detail || "РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ РЅР°СЃС‚СЂРѕРµРє РїСЂРѕРµРєС‚РѕРІ.");
        }

        renderProjects(payload.projects ?? []);
        setStatus(
          projectsStatus,
          `Р“РѕС‚РѕРІРѕ: РІРєР»СЋС‡РµРЅРѕ РїСЂРѕРµРєС‚РѕРІ ${payload.enabled_count ?? 0}, С‡Р°СЃС‚РёС‡РЅР°СЏ Р·Р°РіСЂСѓР·РєР° Сѓ ${payload.partial_count ?? 0}.`,
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
      setStatus(projectsStatus, "РћР±РЅРѕРІР»СЏРµРј СЃРїРёСЃРѕРє РїСЂРѕРµРєС‚РѕРІ...");

      try {
        const response = await fetch("/api/projects/refresh", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ РїСЂРѕРµРєС‚РѕРІ.");
        }

        renderProjects(payload.projects ?? []);
        setStatus(
          projectsStatus,
          `Р“РѕС‚РѕРІРѕ: РґРѕР±Р°РІР»РµРЅРѕ РЅРѕРІС‹С… РїСЂРѕРµРєС‚РѕРІ ${payload.added_count ?? 0}, РѕР±РЅРѕРІР»РµРЅРѕ ${payload.updated_count ?? 0}.`,
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
      setStatus(captureStatus, "Р—Р°РїСѓСЃРєР°РµРј РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·РѕРІ...");

      try {
        const response = await fetch("/api/issues/snapshots/capture", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃСЂРµР·РѕРІ.");
        }

        if (payload.captured_for_date) {
          snapshotDateInput.value = payload.captured_for_date;
        }

        setStatus(captureStatus, payload.detail || "Р¤РѕРЅРѕРІР°СЏ Р·Р°РіСЂСѓР·РєР° СЃСЂРµР·РѕРІ Р·Р°РїСѓС‰РµРЅР°...");
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
      setStatus(captureStatus, "Р—Р°РїСѓСЃРєР°РµРј РѕР±РЅРѕРІР»РµРЅРёРµ РїРѕСЃР»РµРґРЅРёС… СЃСЂРµР·РѕРІ...");

      try {
        const response = await fetch("/api/issues/snapshots/recapture", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ РїРѕСЃР»РµРґРЅРёС… СЃСЂРµР·РѕРІ.");
        }

        if (payload.captured_for_date) {
          snapshotDateInput.value = payload.captured_for_date;
        }

        setStatus(captureStatus, payload.detail || "Р¤РѕРЅРѕРІРѕРµ РѕР±РЅРѕРІР»РµРЅРёРµ РїРѕСЃР»РµРґРЅРёС… СЃСЂРµР·РѕРІ Р·Р°РїСѓС‰РµРЅРѕ...");
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
        setStatus(deleteStatus, "РЎРЅР°С‡Р°Р»Р° РІС‹Р±РµСЂРёС‚Рµ РґР°С‚Сѓ РІ РєР°Р»РµРЅРґР°СЂРµ.", "error");
        return;
      }

      deleteSnapshotsButton.disabled = true;
      setStatus(deleteStatus, `РЈРґР°Р»СЏРµРј СЃСЂРµР·С‹ Р·Р° ${capturedForDate}...`);

      try {
        const response = await fetch(
          `/api/issues/snapshots/by-date?captured_for_date=${encodeURIComponent(capturedForDate)}`,
          { method: "DELETE" }
        );
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "РћС€РёР±РєР° СѓРґР°Р»РµРЅРёСЏ СЃСЂРµР·РѕРІ.");
        }

        renderSnapshotRuns(payload.snapshot_runs ?? [], payload.total_count ?? 0);
        setStatus(
          deleteStatus,
          `РЈРґР°Р»РµРЅРѕ СЃСЂРµР·РѕРІ: ${payload.deleted_runs ?? 0}, СЃС‚СЂРѕРє Р·Р°РґР°С‡: ${payload.deleted_items ?? 0}.`,
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
      setStatus(deleteStatus, "РџСЂРѕСЂРµР¶РёРІР°РµРј РЅРµРёР·РјРµРЅРЅС‹Рµ СЃСЂРµР·С‹...");

      try {
        const response = await fetch("/api/issues/snapshots/prune", { method: "POST" });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.detail || "РћС€РёР±РєР° РїСЂРѕСЂРµР¶РёРІР°РЅРёСЏ СЃСЂРµР·РѕРІ.");
        }

        renderSnapshotRuns(payload.snapshot_runs ?? [], payload.total_count ?? 0);
        setStatus(
          deleteStatus,
          `РџСЂРѕСЂРµР¶РёРІР°РЅРёРµ Р·Р°РІРµСЂС€РµРЅРѕ: СѓРґР°Р»РµРЅРѕ СЃСЂРµР·РѕРІ ${payload.deleted_runs ?? 0}, СЃС‚СЂРѕРє Р·Р°РґР°С‡ ${payload.deleted_items ?? 0}.`,
          "success"
        );
      } catch (error) {
        setStatus(deleteStatus, error.message, "error");
      } finally {
        pruneSnapshotsButton.disabled = false;
      }
    }

    refreshProjectsButton.addEventListener("click", refreshProjects);
    planningProjectsPageButton.addEventListener("click", () => {
      window.location.href = "/planning-projects";
    });
    strangeIssuesPageButton.addEventListener("click", () => {
      window.location.href = "/strange-snapshot-issues";
    });
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
    loadCurrentUser();
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
        return "вЂ”"
    return str(value).replace("T", " ").replace("+00:00", " UTC")


def formatSnapshotPageDateTime(value: object) -> str:
    if not value:
        return "вЂ”"
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
        ("snapshots", resolvedSnapshotUrl, "РЎСЂРµР·С‹ РїСЂРѕРµРєС‚Р°", "context-nav-snapshots", False),
        ("compare", resolvedCompareUrl, "РЎСЂР°РІРЅРµРЅРёРµ СЃСЂРµР·РѕРІ", "context-nav-compare", False),
        ("burndown", resolvedBurndownUrl, "Р”РёР°РіСЂР°РјРјР° СЃРіРѕСЂР°РЅРёСЏ", "context-nav-burndown", False),
    ]
    if redmineUrl:
        buttons.append(("redmine", redmineUrl, "РћС‚РєСЂС‹С‚СЊ РІ Redmine", "context-nav-redmine", True))

    visibleButtons = [button for button in buttons if button[0] != currentPage]

    htmlParts: list[str] = [
        '<div class="context-nav-shell">',
        '<a class="context-nav-brand" href="/" aria-label="РќР° РіР»Р°РІРЅСѓСЋ">',
        '<img class="context-nav-logo" src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="РЎРњРЎ-РРў">',
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
    riskEstimateHours = float(source.get("risk_estimate_hours") or 0)
    spentHours = float(source.get("spent_hours") or 0)
    spentHoursYear = float(source.get("spent_hours_year") or 0)
    featureBaselineEstimateHours = float(source.get("feature_baseline_estimate_hours") or 0)
    featureEstimatedHours = float(source.get("feature_estimated_hours") or 0)
    featureSpentHours = float(source.get("feature_spent_hours") or 0)
    featureSpentHoursYear = float(source.get("feature_spent_hours_year") or 0)
    developmentEstimatedHours = float(source.get("development_estimated_hours") or 0)
    developmentRiskEstimateHours = float(source.get("development_risk_estimate_hours") or 0)
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
        "risk_estimate_hours": riskEstimateHours,
        "spent_hours": spentHours,
        "spent_hours_year": spentHoursYear,
        "feature_baseline_estimate_hours": featureBaselineEstimateHours,
        "feature_estimated_hours": featureEstimatedHours,
        "feature_spent_hours": featureSpentHours,
        "feature_spent_hours_year": featureSpentHoursYear,
        "development_estimated_hours": developmentEstimatedHours,
        "development_risk_estimate_hours": developmentRiskEstimateHours,
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
    riskOp: str | None = None,
    riskValue: str | None = None,
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
        "risk_op": riskOp or "",
        "risk_value": riskValue or "",
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
        "label": "Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°",
        "issue_key": "baseline_estimate_hours",
        "tracker_names": None,
    },
    {
        "key": "development_estimate",
        "label": "Р Р°Р·СЂР°Р±РѕС‚РєР°: РѕС†РµРЅРєР°",
        "issue_key": "estimated_hours",
        "tracker_names": {"СЂР°Р·СЂР°Р±РѕС‚РєР°", "РїСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё"},
    },
    {
        "key": "development_spent_year",
        "label": "Р Р°Р·СЂР°Р±РѕС‚РєР°: С„Р°РєС‚ Р·Р° РіРѕРґ",
        "issue_key": "spent_hours_year",
        "tracker_names": {"СЂР°Р·СЂР°Р±РѕС‚РєР°", "РїСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё"},
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


def isSnapshotIssueEmptyForMissingCompare(
    issue: dict[str, object] | None,
    selectedFields: list[str],
) -> bool:
    if not issue:
        return True

    for fieldKey in selectedFields:
        metricValue = getSnapshotCompareNumericValue(issue, fieldKey)
        if abs(metricValue) > 1e-9:
            return False
    return True


def buildSnapshotComparisonRows(
    leftIssues: list[dict[str, object]],
    rightIssues: list[dict[str, object]],
    selectedFields: list[str],
    includeMissingIssues: bool = True,
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

        if (
            changeKind in {"new", "deleted"}
            and not includeMissingIssues
            and isSnapshotIssueEmptyForMissingCompare(leftIssue, selectedFields)
            and isSnapshotIssueEmptyForMissingCompare(rightIssue, selectedFields)
        ):
            continue

        changedRows.append(
            {
                "issue_redmine_id": issueId,
                "subject": str(baseIssue.get("subject") or "вЂ”"),
                "tracker_name": str(baseIssue.get("tracker_name") or "вЂ”"),
                "left_status_name": str(leftIssue.get("status_name") or "вЂ”") if leftIssue else "вЂ”",
                "right_status_name": str(rightIssue.get("status_name") or "вЂ”") if rightIssue else "вЂ”",
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
    includeMissingIssues: bool = False,
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
    if includeMissingIssues:
        compareQueryParts.append("include_missing=1")
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
        projectName = escape(str((storedProject or {}).get("name") or "вЂ”"))
        compareFieldsHtml = "".join(
            (
                f'<label class="compare-field-option"><input type="checkbox" name="field" value="{escape(str(field["key"]))}"'
                f'{" checked" if str(field["key"]) in normalizedFields else ""}>'
                f'<span>{escape(str(field["label"]))}</span></label>'
            )
            for field in SNAPSHOT_COMPARE_FIELD_CONFIG
        )
        includeMissingHtml = (
            '<label class="compare-field-option">'
            f'<input type="checkbox" name="include_missing" value="1"{" checked" if includeMissingIssues else ""}>'
            "<span>РџРѕРєР°Р·С‹РІР°С‚СЊ РЅРѕРІС‹Рµ/РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‰РёРµ Р·Р°РґР°С‡Рё СЃ РЅСѓР»РµРІС‹РјРё Р·РЅР°С‡РµРЅРёСЏРјРё</span></label>"
        )
        return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>РЎСЂР°РІРЅРµРЅРёРµ СЃСЂРµР·РѕРІ</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    body {{ margin: 0; font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: #ffffff; color: #16324a; }}
    main {{ max-width: 1280px; margin: 0 auto; padding: 24px 20px 48px; }}
    {buildProjectContextNavCss()}
    h1 {{ margin: 18px 0 12px; font-size: clamp(1.85rem, 4.2vw, 2.75rem); line-height: 1.02; letter-spacing: -0.04em; font-weight: 400; }}
    .meta {{ color: #64798d; margin: 0 0 18px; line-height: 1.6; }}
    .meta-strong {{ color: #33bdd8; font-weight: 400; }}
    .controls-panel {{ border: 1px solid #d9e5eb; border-radius: 8px; padding: 18px 20px; background: #ffffff; }}
    .controls-grid {{ display: grid; grid-template-columns: 1fr; gap: 14px; }}
    .field {{ display: flex; flex-direction: column; gap: 6px; }}
    .field label {{ font-weight: 700; }}
    .date-swap-field {{ justify-content: flex-end; }}
    .date-swap-label {{ opacity: 0; pointer-events: none; user-select: none; }}
    select {{ border: 1px solid #d9e5eb; border-radius: 6px; padding: 10px 12px; font: inherit; }}
    .compare-field-group {{ display: flex; flex-wrap: wrap; gap: 4px 12px; }}
    .compare-field-option {{ display: inline-flex; align-items: center; gap: 6px; color: #16324a; white-space: nowrap; }}
    .compare-option-caption {{ color: #64798d; font-size: 0.9rem; font-weight: 700; }}
    .compare-date-row {{ display: grid; grid-template-columns: auto auto auto minmax(320px, 1fr); gap: 10px 18px; align-items: start; }}
    .compare-date-field {{ min-width: 0; }}
    .compare-date-select {{ min-width: 146px; width: auto; }}
    .compare-swap-stack, .compare-extra-stack, .compare-compare-stack {{ display: flex; flex-direction: column; gap: 6px; }}
    .compare-swap-stack {{ align-items: flex-start; }}
    .compare-extra-stack {{ margin-top: 2px; }}
    .compare-compare-stack {{ min-width: 320px; }}
    .date-swap-button {{
      display: inline-flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 0;
      min-width: 6px;
      min-height: 28px;
      padding: 2px 1px;
      background: #eef2f5;
      color: #375d77;
      border: 1px solid #d9e5eb;
    }}
    .date-swap-button span {{ line-height: 1; font-size: 0.68rem; }}
    @media (max-width: 1100px) {{
      .compare-date-row {{ grid-template-columns: 1fr; }}
      .compare-compare-stack {{ min-width: 0; }}
    }}
    button {{ border: 0; border-radius: 6px; padding: 10px 14px; font: inherit; font-weight: 700; cursor: pointer; background: #ff6c0e; color: #ffffff; }}
    .empty-state {{ margin-top: 18px; border: 1px dashed #d9e5eb; border-radius: 8px; padding: 24px; background: #f7fbfc; color: #64798d; }}
    .compare-loading-overlay {{
      position: fixed;
      inset: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-direction: column;
      gap: 14px;
      background: rgba(255, 255, 255, 0.78);
      backdrop-filter: blur(1px);
      z-index: 9999;
      opacity: 0;
      pointer-events: none;
      transition: opacity 140ms ease;
    }}
    .compare-loading-overlay.is-visible {{ opacity: 1; pointer-events: auto; }}
    .compare-loading-spinner {{
      width: 40px;
      height: 40px;
      border-radius: 50%;
      border: 3px solid rgba(82, 206, 230, 0.24);
      border-top-color: #52cee6;
      animation: snapshot-compare-spin 0.8s linear infinite;
    }}
    .compare-loading-text {{ font-weight: 700; color: #375d77; }}
    @keyframes snapshot-compare-spin {{ to {{ transform: rotate(360deg); }} }}
  </style>
</head>
<body>
  <main>
    {navPanelHtml}
    <h1>РЎСЂР°РІРЅРµРЅРёРµ СЃСЂРµР·РѕРІ РїСЂРѕРµРєС‚Р°</h1>
    <p class="meta">РџСЂРѕРµРєС‚: <span class="meta-strong">{projectName}</span>. РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ: <span class="meta-strong">{escape(projectIdentifierRaw or "вЂ”")}</span>. Р”Р»СЏ СЃСЂР°РІРЅРµРЅРёСЏ РЅСѓР¶РµРЅ С…РѕС‚СЏ Р±С‹ РѕРґРёРЅ СЃРѕС…СЂР°РЅРµРЅРЅС‹Р№ СЃСЂРµР·.</p>
    <section class="controls-panel">
      <form method="get" id="compareSnapshotsForm">
        <div class="controls-grid">
          <div class="field">
            <div class="compare-date-row">
              <div class="field compare-date-field">
                <label for="leftDate">Р”Р°С‚Р° СЃСЂРµР·Р° 1</label>
                <select id="leftDate" name="left_date" class="compare-date-select"><option value="">РќРµС‚ СЃСЂРµР·РѕРІ</option></select>
              </div>
              <div class="compare-swap-stack">
                <div class="field date-swap-field">
                  <label class="date-swap-label" for="swapCompareDatesButton">РџРѕРјРµРЅСЏС‚СЊ РґР°С‚С‹ РјРµСЃС‚Р°РјРё</label>
                  <button type="button" class="date-swap-button" id="swapCompareDatesButton" aria-label="РџРѕРјРµРЅСЏС‚СЊ РґР°С‚С‹ РјРµСЃС‚Р°РјРё"><span>в†ђ</span><span>в†’</span></button>
                </div>
              </div>
              <div class="field compare-date-field">
                <label for="rightDate">Р”Р°С‚Р° СЃСЂРµР·Р° 2</label>
                <select id="rightDate" name="right_date" class="compare-date-select"><option value="">РќРµС‚ СЃСЂРµР·РѕРІ</option></select>
              </div>
              <div class="compare-compare-stack">
                <span class="compare-option-caption">РџРѕР»СЏ РґР»СЏ СЃСЂР°РІРЅРµРЅРёСЏ</span>
                <div class="compare-field-group">{compareFieldsHtml}</div>
                <div class="compare-extra-stack">
                  <span class="compare-option-caption">Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рµ РѕРїС†РёРё</span>
                  {includeMissingHtml}
                </div>
              </div>
            </div>
          </div>
        </div>
        <p><button type="submit">РЎСЂР°РІРЅРёС‚СЊ</button></p>
      </form>
    </section>
    <div class="empty-state">Р”Р»СЏ СЌС‚РѕРіРѕ РїСЂРѕРµРєС‚Р° РїРѕРєР° РЅРµС‚ СЃСЂРµР·РѕРІ, РїРѕСЌС‚РѕРјСѓ СЃСЂР°РІРЅРёРІР°С‚СЊ РµС‰Рµ РЅРµС‡РµРіРѕ.</div>
  </main>
  <div class="compare-loading-overlay" id="compareLoadingOverlay" aria-hidden="true">
    <span class="compare-loading-spinner" aria-hidden="true"></span>
    <span class="compare-loading-text">РЎСЂР°РІРЅРµРЅРёРµ СЃСЂРµР·РѕРІ РїСЂРѕРµРєС‚Р°...</span>
  </div>
  <script>
    const compareSnapshotsForm = document.getElementById("compareSnapshotsForm");
    const compareLoadingOverlay = document.getElementById("compareLoadingOverlay");
    const swapCompareDatesButton = document.getElementById("swapCompareDatesButton");
    const leftDateSelect = document.getElementById("leftDate");
    const rightDateSelect = document.getElementById("rightDate");
    compareSnapshotsForm?.addEventListener("submit", () => {{
      if (compareLoadingOverlay) {{
        compareLoadingOverlay.classList.add("is-visible");
        compareLoadingOverlay.setAttribute("aria-hidden", "false");
      }}
    }});
    swapCompareDatesButton?.addEventListener("click", () => {{
      if (!leftDateSelect || !rightDateSelect) {{
        return;
      }}
      const currentLeft = leftDateSelect.value;
      leftDateSelect.value = rightDateSelect.value;
      rightDateSelect.value = currentLeft;
      compareSnapshotsForm?.requestSubmit();
    }});
  </script>
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
            or (storedProject.get("name") if storedProject else "вЂ”")
        )
    )
    projectIdentifierRaw = str(
        rightRun.get("project_identifier")
        or leftRun.get("project_identifier")
        or (storedProject.get("identifier") if storedProject else "")
    ).strip()
    projectIdentifier = escape(projectIdentifierRaw or "вЂ”")
    comparisonRows, fieldChangeCounts = buildSnapshotComparisonRows(
        list(leftPayload.get("issues") or []),
        list(rightPayload.get("issues") or []),
        normalizedFields,
        includeMissingIssues=includeMissingIssues,
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
    includeMissingHtml = (
        '<label class="compare-field-option">'
        f'<input type="checkbox" name="include_missing" value="1"{" checked" if includeMissingIssues else ""}>'
        "<span>РџРѕРєР°Р·С‹РІР°С‚СЊ РЅРѕРІС‹Рµ/РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‰РёРµ Р·Р°РґР°С‡Рё СЃ РЅСѓР»РµРІС‹РјРё Р·РЅР°С‡РµРЅРёСЏРјРё</span></label>"
    )

    selectedFieldLabels = [
        escape(str(SNAPSHOT_COMPARE_FIELD_BY_KEY[fieldKey]["label"])) for fieldKey in normalizedFields
    ]
    compareSummaryHtml = " В· ".join(
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
            rowBadgeHtml = '<span class="compare-badge compare-badge-new">РќРѕРІР°СЏ</span>'
        elif row["change_kind"] == "deleted":
            rowBadgeHtml = '<span class="compare-badge compare-badge-deleted">РћС‚СЃСѓС‚СЃС‚РІСѓРµС‚</span>'

        issueIdValue = int(row["issue_redmine_id"])
        issueLinkHtml = (
            f'<a class="compare-issue-link" href="https://redmine.sms-it.ru/issues/{issueIdValue}" target="_blank" rel="noreferrer">{issueIdValue}</a>'
        )

        bodyRows.append(
            "<tr>"
            f'<td class="mono"><span class="compare-id-cell">{issueLinkHtml}{rowBadgeHtml}</span></td>'
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
    if includeMissingIssues:
        compareUrlCurrent += "&include_missing=1"
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
  <title>РЎСЂР°РІРЅРµРЅРёРµ СЃСЂРµР·РѕРІ</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
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
    body {{ margin: 0; font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: var(--bg); color: var(--text); }}
    main {{ max-width: 1480px; margin: 0 auto; padding: 24px 20px 48px; }}
    {buildProjectContextNavCss()}
    h1 {{ margin: 18px 0 12px; font-size: clamp(1.85rem, 4.2vw, 2.75rem); line-height: 1.02; letter-spacing: -0.04em; font-weight: 400; }}
    .meta {{ color: var(--muted); margin: 0 0 14px; line-height: 1.6; }}
    .meta-strong {{ color: #33bdd8; font-weight: 400; }}
    .controls-panel {{ border: 1px solid var(--line); border-radius: 8px; padding: 18px 20px; background: var(--panel); margin: 0 0 18px; }}
    .controls-grid {{ display: grid; grid-template-columns: 1fr; gap: 14px; align-items: start; }}
    .field {{ display: flex; flex-direction: column; gap: 6px; }}
    .field label {{ font-weight: 700; }}
    .date-swap-field {{ justify-content: flex-end; }}
    .date-swap-label {{ opacity: 0; pointer-events: none; user-select: none; }}
    select {{ border: 1px solid var(--line); border-radius: 6px; padding: 10px 12px; font: inherit; color: var(--text); background: #ffffff; }}
    .compare-field-group {{ display: flex; flex-wrap: wrap; gap: 4px 12px; padding-top: 0; }}
    .compare-field-option {{ display: inline-flex; align-items: center; gap: 6px; color: var(--text); white-space: nowrap; }}
    .compare-option-caption {{ color: var(--muted); font-size: 0.9rem; font-weight: 700; }}
    .compare-date-row {{ display: grid; grid-template-columns: auto auto auto minmax(320px, 1fr); gap: 10px 18px; align-items: start; }}
    .compare-date-field {{ min-width: 0; }}
    .compare-date-select {{ min-width: 146px; width: auto; }}
    .compare-swap-stack, .compare-extra-stack, .compare-compare-stack {{ display: flex; flex-direction: column; gap: 6px; }}
    .compare-swap-stack {{ align-items: flex-start; }}
    .compare-extra-stack {{ margin-top: 2px; }}
    .compare-compare-stack {{ min-width: 320px; }}
    .date-swap-button {{
      display: inline-flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 0;
      min-width: 6px;
      min-height: 28px;
      padding: 2px 1px;
      background: #eef2f5;
      color: #375d77;
      border: 1px solid var(--line);
    }}
    .date-swap-button span {{ line-height: 1; font-size: 0.68rem; }}
    @media (max-width: 1100px) {{
      .compare-date-row {{ grid-template-columns: 1fr; }}
      .compare-compare-stack {{ min-width: 0; }}
    }}
    button {{ border: 0; border-radius: 6px; padding: 10px 14px; font: inherit; font-weight: 700; cursor: pointer; background: var(--orange); color: #ffffff; }}
    .summary-note {{ color: var(--muted); margin: 0 0 14px; }}
    .table-wrap {{ position: relative; overflow: auto; border: 1px solid var(--line); border-radius: 8px; max-height: calc(100vh - 250px); }}
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
    .compare-issue-link {{ color: var(--blue); text-decoration: none; border-bottom: 1px dotted rgba(55, 93, 119, 0.45); }}
    .compare-issue-link:hover {{ border-bottom-color: rgba(55, 93, 119, 0.9); }}
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
    .compare-loading-overlay {{
      position: fixed;
      inset: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-direction: column;
      gap: 14px;
      background: rgba(255, 255, 255, 0.78);
      backdrop-filter: blur(1px);
      z-index: 9999;
      opacity: 0;
      pointer-events: none;
      transition: opacity 140ms ease;
    }}
    .compare-loading-overlay.is-visible {{ opacity: 1; pointer-events: auto; }}
    .compare-loading-spinner {{
      width: 40px;
      height: 40px;
      border-radius: 50%;
      border: 3px solid rgba(82, 206, 230, 0.24);
      border-top-color: #52cee6;
      animation: snapshot-compare-spin 0.8s linear infinite;
    }}
    .compare-loading-text {{ font-weight: 700; color: #375d77; }}
    @keyframes snapshot-compare-spin {{ to {{ transform: rotate(360deg); }} }}
  </style>
</head>
<body>
  <main>
    {navPanelHtml}
    <h1>РЎСЂР°РІРЅРµРЅРёРµ СЃСЂРµР·РѕРІ РїСЂРѕРµРєС‚Р°</h1>
    <p class="meta">РџСЂРѕРµРєС‚: <span class="meta-strong">{projectName}</span>. РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ: <span class="meta-strong">{projectIdentifier}</span>.</p>
    <section class="controls-panel">
      <form method="get" id="compareSnapshotsForm">
        <div class="controls-grid">
          <div class="field">
            <div class="compare-date-row">
              <div class="field compare-date-field">
                <label for="leftDate">Р”Р°С‚Р° СЃСЂРµР·Р° 1</label>
                <select id="leftDate" name="left_date" class="compare-date-select">{leftDateOptionsHtml}</select>
              </div>
              <div class="compare-swap-stack">
                <div class="field date-swap-field">
                  <label class="date-swap-label" for="swapCompareDatesButton">РџРѕРјРµРЅСЏС‚СЊ РґР°С‚С‹ РјРµСЃС‚Р°РјРё</label>
                  <button type="button" class="date-swap-button" id="swapCompareDatesButton" aria-label="РџРѕРјРµРЅСЏС‚СЊ РґР°С‚С‹ РјРµСЃС‚Р°РјРё"><span>в†ђ</span><span>в†’</span></button>
                </div>
              </div>
              <div class="field compare-date-field">
                <label for="rightDate">Р”Р°С‚Р° СЃСЂРµР·Р° 2</label>
                <select id="rightDate" name="right_date" class="compare-date-select">{rightDateOptionsHtml}</select>
              </div>
              <div class="compare-compare-stack">
                <span class="compare-option-caption">РџРѕР»СЏ РґР»СЏ СЃСЂР°РІРЅРµРЅРёСЏ</span>
                <div class="compare-field-group">{fieldOptionsHtml}</div>
                <div class="compare-extra-stack">
                  <span class="compare-option-caption">Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рµ РѕРїС†РёРё</span>
                  {includeMissingHtml}
                </div>
              </div>
            </div>
          </div>
        </div>
        <p><button type="submit">РЎСЂР°РІРЅРёС‚СЊ</button></p>
      </form>
    </section>
    <p class="summary-note">РџРѕР»СЏ СЃСЂР°РІРЅРµРЅРёСЏ: {", ".join(selectedFieldLabels)}. РР·РјРµРЅРёРІС€РёС…СЃСЏ Р·Р°РґР°С‡: {len(comparisonRows)}. {compareSummaryHtml}</p>
    {
        f'''
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>РўРµРјР°</th>
            <th>РўСЂРµРєРµСЂ</th>
            <th>РЎС‚Р°С‚СѓСЃ<br><span class="subhead">{escape(str(resolvedLeftDate))}</span></th>
            <th>РЎС‚Р°С‚СѓСЃ<br><span class="subhead">{escape(str(resolvedRightDate))}</span></th>
            {"".join(headerCells)}
          </tr>
        </thead>
        <tbody>
          {"".join(bodyRows)}
        </tbody>
      </table>
    </div>
        ''' if comparisonRows else '<div class="empty-state">РџРѕ РІС‹Р±СЂР°РЅРЅС‹Рј РїРѕР»СЏРј РјРµР¶РґСѓ СЌС‚РёРјРё РґРІСѓРјСЏ СЃСЂРµР·Р°РјРё РёР·РјРµРЅРµРЅРёР№ РЅРµ РЅР°Р№РґРµРЅРѕ.</div>'
    }
  </main>
  <div class="compare-loading-overlay" id="compareLoadingOverlay" aria-hidden="true">
    <span class="compare-loading-spinner" aria-hidden="true"></span>
    <span class="compare-loading-text">РЎСЂР°РІРЅРµРЅРёРµ СЃСЂРµР·РѕРІ РїСЂРѕРµРєС‚Р°...</span>
  </div>
  <script>
    const compareSnapshotsForm = document.getElementById("compareSnapshotsForm");
    const compareLoadingOverlay = document.getElementById("compareLoadingOverlay");
    const swapCompareDatesButton = document.getElementById("swapCompareDatesButton");
    const leftDateSelect = document.getElementById("leftDate");
    const rightDateSelect = document.getElementById("rightDate");
    compareSnapshotsForm?.addEventListener("submit", () => {{
      if (compareLoadingOverlay) {{
        compareLoadingOverlay.classList.add("is-visible");
        compareLoadingOverlay.setAttribute("aria-hidden", "false");
      }}
    }});
    swapCompareDatesButton?.addEventListener("click", () => {{
      if (!leftDateSelect || !rightDateSelect) {{
        return;
      }}
      const currentLeft = leftDateSelect.value;
      leftDateSelect.value = rightDateSelect.value;
      rightDateSelect.value = currentLeft;
      compareSnapshotsForm?.requestSubmit();
    }});
  </script>
</body>
</html>"""


def normalizeBurndownText(value: object) -> str:
    return str(value or "").strip().lower()


def isBurndownClosedTaskStatus(statusName: object) -> bool:
    return normalizeBurndownText(statusName) in {"Р·Р°РєСЂС‹С‚Р°", "СЂРµС€РµРЅР°", "РѕС‚РєР°Р·"}


def isBurndownReadyFeatureStatus(statusName: object) -> bool:
    normalized = normalizeBurndownText(statusName)
    return normalized.startswith("РіРѕС‚РѕРІ") or normalized in {"Р·Р°РєСЂС‹С‚Р°", "СЂРµС€РµРЅР°"}


def calculateBurndownBudgetBaselineTotal(issues: list[dict[str, object]]) -> float:
    budgetBaselineTotal = 0.0

    for issue in issues:
        trackerName = normalizeBurndownText(issue.get("tracker_name"))
        if trackerName == "feature":
            continue
        budgetBaselineTotal += float(issue.get("baseline_estimate_hours") or 0)

    return budgetBaselineTotal


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

        trackerName = normalizeBurndownText(issue.get("tracker_name"))
        statusName = issue.get("status_name")
        planHours = float(issue.get("estimated_hours") or 0)
        factHours = float(issue.get("spent_hours") or 0)

        if featureId is not None and featureId == issueId and trackerName == "feature":
            group["is_ready"] = isBurndownReadyFeatureStatus(statusName)
            continue

        baselineEstimateHours = float(issue.get("baseline_estimate_hours") or 0)
        group["baseline_total"] = float(group["baseline_total"]) + baselineEstimateHours

        if trackerName == "СЂР°Р·СЂР°Р±РѕС‚РєР°":
            if isBurndownClosedTaskStatus(statusName):
                volume = factHours
                remaining = 0.0
            else:
                volume = max(baselineEstimateHours, planHours, factHours)
                remaining = max(0.0, max(baselineEstimateHours, planHours) - factHours)
            group["development_volume"] = float(group["development_volume"]) + volume
            group["development_remaining"] = float(group["development_remaining"]) + remaining
        elif trackerName == "РїСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё":
            volume = max(planHours, factHours)
            group["development_volume"] = float(group["development_volume"]) + volume
        elif trackerName == "РѕС€РёР±РєР°":
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
        snapshotIssues = list(snapshotRun.get("issues") or [])
        chartSeeds.append(
            {
                "date": str(snapshotRun.get("captured_for_date") or ""),
                "budget_baseline_total": calculateBurndownBudgetBaselineTotal(snapshotIssues),
                "groups": buildBurndownFeatureGroups(snapshotIssues),
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
        str(projectInfo.get("project_name") or (storedProject.get("name") if storedProject else "вЂ”"))
    )
    projectIdentifierRaw = str(
        projectInfo.get("project_identifier") or (storedProject.get("identifier") if storedProject else "")
    ).strip()
    projectIdentifier = escape(projectIdentifierRaw or "вЂ”")
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
  <title>Р”РёР°РіСЂР°РјРјР° СЃРіРѕСЂР°РЅРёСЏ</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
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
      font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
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
      font-size: clamp(1.85rem, 4.2vw, 2.75rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 400;
    }}

    .meta {{
      color: var(--muted);
      margin: 0 0 18px;
      font-size: 1rem;
      line-height: 1.6;
    }}

    .meta-strong {{
      color: #33bdd8;
      font-weight: 400;
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
    <h1>Р”РёР°РіСЂР°РјРјР° СЃРіРѕСЂР°РЅРёСЏ</h1>
    <p class="meta">РџСЂРѕРµРєС‚: <span class="meta-strong">{projectName}</span>. РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ: <span class="meta-strong">{projectIdentifier}</span>. РџРµСЂРёРѕРґ РґРёР°РіСЂР°РјРјС‹: 01.04.{currentYear} вЂ” 30.04.{currentYear}. РЎСЂРµР·РѕРІ Р·Р° Р°РїСЂРµР»СЊ: {len(chartSeeds)}.</p>

    <section class="controls-panel">
      <div class="field">
        <label for="p1Input">P1 = С„Р°РєС‚ / Р±Р°Р·Р°</label>
        <input id="p1Input" type="text" inputmode="decimal" value="1,5">
        <div class="field-note">РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РІ СЂР°СЃС‡РµС‚Рµ Р±СЋРґР¶РµС‚Р° Рё РїСЂРѕРіРЅРѕР·РЅРѕРіРѕ РѕР±СЉРµРјР°.</div>
      </div>
      <div class="field">
        <label for="p2Input">P2 = С„Р°РєС‚ СЃ Р±Р°РіР°РјРё / С„Р°РєС‚</label>
        <input id="p2Input" type="text" inputmode="decimal" value="1,5">
        <div class="field-note">РР·РјРµРЅРµРЅРёСЏ РїРµСЂРµСЃС‡РёС‚С‹РІР°СЋС‚СЃСЏ СЃСЂР°Р·Сѓ РїРѕСЃР»Рµ РІРІРѕРґР° Р±РµР· РїРµСЂРµР·Р°РіСЂСѓР·РєРё СЃС‚СЂР°РЅРёС†С‹.</div>
      </div>
    </section>

    <section class="chart-panel">
      <div class="chart-head">
        <div>
          <h2 class="chart-title">Р‘СЋРґР¶РµС‚, РїСЂРѕРіРЅРѕР·, С‚РµРєСѓС‰РёР№ РѕР±СЉРµРј Рё РѕСЃС‚Р°С‚РѕРє</h2>
          <p class="chart-subtitle">Р›РёРЅРёРё РїРѕРєР°Р·С‹РІР°СЋС‚ РѕР±С‰РёРµ Р·РЅР°С‡РµРЅРёСЏ, Р° РїРѕР»СѓРїСЂРѕР·СЂР°С‡РЅС‹Рµ СЃС‚РѕР»Р±РёРєРё вЂ” СЃРѕСЃС‚Р°РІ РїРѕ СЂР°Р·СЂР°Р±РѕС‚РєРµ Рё РѕС€РёР±РєР°Рј.</p>
        </div>
        <div class="chart-status" id="burndownStatus"></div>
      </div>
      <div class="chart-wrap">
        <canvas id="burndownChart"></canvas>
      </div>
      <div id="burndownEmptyState" class="empty-state" style="display:none;">
        Р—Р° Р°РїСЂРµР»СЊ С‚РµРєСѓС‰РµРіРѕ РіРѕРґР° РїРѕ РїСЂРѕРµРєС‚Сѓ РїРѕРєР° РЅРµС‚ СЃСЂРµР·РѕРІ, РїРѕСЌС‚РѕРјСѓ РїРѕСЃС‚СЂРѕРёС‚СЊ РґРёР°РіСЂР°РјРјСѓ РµС‰Рµ РЅРµ РёР· С‡РµРіРѕ.
      </div>
    </section>

    <section class="legend-panel">
      <h2 class="legend-title">Р›РµРіРµРЅРґР° Рё РїСЂР°РІРёР»Р° СЂР°СЃС‡РµС‚Р°</h2>
      <div class="legend-grid">
        <div>
          <ul class="legend-list">
            <li>
              <span class="legend-swatch budget-line"></span>
              <div>
                <div class="legend-name">Р‘СЋРґР¶РµС‚</div>
                <div class="legend-text">РћСЂР°РЅР¶РµРІР°СЏ Р»РёРЅРёСЏ. Р”Р»СЏ РєР°Р¶РґРѕРіРѕ СЃСЂРµР·Р°: СЃСѓРјРјР° Р±Р°Р·РѕРІС‹С… РѕС†РµРЅРѕРє РІСЃРµС… Р·Р°РґР°С‡ СЃСЂРµР·Р° Р±РµР· Feature Г— P1 Г— P2.</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch forecast-line"></span>
              <div>
                <div class="legend-name">РћР±СЉРµРј.РџСЂРѕРіРЅРѕР·</div>
                <div class="legend-text">РўРµРјРЅРѕ-СЃРёРЅСЏСЏ Р»РёРЅРёСЏ. РЎРєР»Р°РґС‹РІР°РµС‚СЃСЏ РїРѕ РІСЃРµРј Feature Рё РїРѕ РІРёСЂС‚СѓР°Р»СЊРЅРѕР№ Feature РґР»СЏ Р·Р°РґР°С‡ Р±РµР· Feature.</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch current-line"></span>
              <div>
                <div class="legend-name">РћР±СЉРµРј.РўРµРєСѓС‰РёР№</div>
                <div class="legend-text">Р“РѕР»СѓР±Р°СЏ Р»РёРЅРёСЏ. Р Р°РІРЅР° СЃСѓРјРјРµ В«РћР±СЉРµРјР° СЂР°Р·СЂР°Р±РѕС‚РєРёВ» Рё В«РћР±СЉРµРјР° РѕС€РёР±РѕРєВ», РїРѕСЌС‚РѕРјСѓ РїСЂРѕС…РѕРґРёС‚ РїРѕ РІРµСЂС…РЅРµР№ С‚РѕС‡РєРµ С‚РµРєСѓС‰РµРіРѕ stacked-СЃС‚РѕР»Р±РёРєР°.</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch remaining-line"></span>
              <div>
                <div class="legend-name">РћР±СЉРµРј.РћСЃС‚Р°С‚РѕРє</div>
                <div class="legend-text">РЎРµСЂРѕ-СЃРёРЅСЏСЏ Р»РёРЅРёСЏ. Р Р°РІРЅР° СЃСѓРјРјРµ В«РћСЃС‚Р°С‚РєР° СЂР°Р·СЂР°Р±РѕС‚РєРёВ» Рё В«РћСЃС‚Р°С‚РєР° РѕС€РёР±РѕРєВ».</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch dev-bar"></span>
              <div>
                <div class="legend-name">РћР±СЉРµРј СЂР°Р·СЂР°Р±РѕС‚РєРё</div>
                <div class="legend-text">РџРѕР»СѓРїСЂРѕР·СЂР°С‡РЅС‹Р№ РіРѕР»СѓР±РѕР№ СЃС‚РѕР»Р±РёРє. Р’ stack СЃ РЅРёРј РІС‹С€Рµ РёРґРµС‚ В«РћР±СЉРµРј РѕС€РёР±РѕРєВ».</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch bug-bar"></span>
              <div>
                <div class="legend-name">РћР±СЉРµРј РѕС€РёР±РѕРє</div>
                <div class="legend-text">РџРѕР»СѓРїСЂРѕР·СЂР°С‡РЅС‹Р№ РѕСЂР°РЅР¶РµРІС‹Р№ СЃС‚РѕР»Р±РёРє РїРѕРІРµСЂС… РѕР±СЉРµРјР° СЂР°Р·СЂР°Р±РѕС‚РєРё.</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch dev-rem-bar"></span>
              <div>
                <div class="legend-name">РћСЃС‚Р°С‚РѕРє СЂР°Р·СЂР°Р±РѕС‚РєРё</div>
                <div class="legend-text">РќРµРїСЂРѕР·СЂР°С‡РЅС‹Р№ РіРѕР»СѓР±РѕР№ СЃС‚РѕР»Р±РёРє. Р’ stack СЃ РЅРёРј РІС‹С€Рµ РёРґРµС‚ В«РћСЃС‚Р°С‚РѕРє РѕС€РёР±РѕРєВ».</div>
              </div>
            </li>
            <li>
              <span class="legend-swatch bug-rem-bar"></span>
              <div>
                <div class="legend-name">РћСЃС‚Р°С‚РѕРє РѕС€РёР±РѕРє</div>
                <div class="legend-text">РќРµРїСЂРѕР·СЂР°С‡РЅС‹Р№ Р¶РµР»С‚С‹Р№ СЃС‚РѕР»Р±РёРє РїРѕРІРµСЂС… РѕСЃС‚Р°С‚РєР° СЂР°Р·СЂР°Р±РѕС‚РєРё.</div>
              </div>
            </li>
          </ul>
        </div>
        <div>
          <ul class="formula-list">
            <li>
              <div>
                <div class="legend-name">Р Р°Р·СЂР°Р±РѕС‚РєР°</div>
                <div class="formula-text">Р•СЃР»Рё СЃС‚Р°С‚СѓСЃ Р·Р°РґР°С‡Рё В«Р—Р°РєСЂС‹С‚Р°В», В«Р РµС€РµРЅР°В» РёР»Рё В«РћС‚РєР°Р·В», С‚Рѕ РѕР±СЉРµРј = С„Р°РєС‚, РѕСЃС‚Р°С‚РѕРє = 0. Р”Р»СЏ РѕСЃС‚Р°Р»СЊРЅС‹С… СЃС‚Р°С‚СѓСЃРѕРІ: РѕР±СЉРµРј = max(Р±Р°Р·Р°, РїР»Р°РЅ, С„Р°РєС‚), РѕСЃС‚Р°С‚РѕРє = max(0, max(Р±Р°Р·Р°, РїР»Р°РЅ) в€’ С„Р°РєС‚).</div>
              </div>
            </li>
            <li>
              <div>
                <div class="legend-name">РџСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё</div>
                <div class="formula-text">РћР±СЉРµРј = max(РїР»Р°РЅ, С„Р°РєС‚), РѕСЃС‚Р°С‚РѕРє РІСЃРµРіРґР° = 0.</div>
              </div>
            </li>
            <li>
              <div>
                <div class="legend-name">РћС€РёР±РєР°</div>
                <div class="formula-text">Р›РѕРіРёРєР° С‚Р°РєР°СЏ Р¶Рµ, РєР°Рє Сѓ В«Р Р°Р·СЂР°Р±РѕС‚РєРёВ»: Р·Р°РєСЂС‹С‚С‹Рµ/СЂРµС€РµРЅРЅС‹Рµ/РѕС‚РєР°Р·Р°РЅРЅС‹Рµ Р·Р°РґР°С‡Рё РґР°СЋС‚ РѕР±СЉРµРј = С„Р°РєС‚ Рё РѕСЃС‚Р°С‚РѕРє = 0, РѕСЃС‚Р°Р»СЊРЅС‹Рµ вЂ” max(РїР»Р°РЅ, С„Р°РєС‚) Рё max(0, РїР»Р°РЅ в€’ С„Р°РєС‚).</div>
              </div>
            </li>
            <li>
              <div>
                <div class="legend-name">Feature Рё РІРёСЂС‚СѓР°Р»СЊРЅР°СЏ Feature</div>
                <div class="formula-text">Р”Р»СЏ РєР°Р¶РґРѕР№ Feature РѕС‚РґРµР»СЊРЅРѕ СЃРѕР±РёСЂР°СЋС‚СЃСЏ РѕР±СЉРµРј/РѕСЃС‚Р°С‚РѕРє РїРѕ СЂР°Р·СЂР°Р±РѕС‚РєРµ Рё РїРѕ РѕС€РёР±РєР°Рј. Р•СЃР»Рё Feature РІ СЃС‚Р°С‚СѓСЃРµ В«Р“РѕС‚РѕРІ*В», В«Р—Р°РєСЂС‹С‚Р°В» РёР»Рё В«Р РµС€РµРЅР°В», РїСЂРѕРіРЅРѕР· = СЂР°Р·СЂР°Р±РѕС‚РєР° + РѕС€РёР±РєРё. РРЅР°С‡Рµ РїСЂРѕРіРЅРѕР· = max(С‚РµРєСѓС‰РёР№ РѕР±СЉРµРј, СЃСѓРјРјР° Р±Р°Р·РѕРІС‹С… РѕС†РµРЅРѕРє Р·Р°РґР°С‡ Feature Г— P1 Г— P2). Р”Р»СЏ Р·Р°РґР°С‡ Р±РµР· Feature СЃС‡РёС‚Р°РµС‚СЃСЏ РѕС‚РґРµР»СЊРЅР°СЏ РІРёСЂС‚СѓР°Р»СЊРЅР°СЏ Feature РїРѕ С‚РµРј Р¶Рµ РїСЂР°РІРёР»Р°Рј.</div>
              </div>
            </li>
          </ul>
          <p class="legend-note">РС‚РѕРіРѕРІС‹Рµ Р»РёРЅРёРё В«РћР±СЉРµРј.РўРµРєСѓС‰РёР№В», В«РћР±СЉРµРј.РћСЃС‚Р°С‚РѕРєВ» Рё В«РћР±СЉРµРј.РџСЂРѕРіРЅРѕР·В» вЂ” СЌС‚Рѕ СЃСѓРјРјС‹ РїРѕ РІСЃРµРј Feature Рё РїРѕ РІРёСЂС‚СѓР°Р»СЊРЅРѕР№ Feature РІ РІС‹Р±СЂР°РЅРЅРѕРј Р°РїСЂРµР»СЊСЃРєРѕРј СЃСЂРµР·Рµ.</p>
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
        statusNode.textContent = "Р—Р° Р°РїСЂРµР»СЊ С‚РµРєСѓС‰РµРіРѕ РіРѕРґР° РїРѕРєР° РЅРµС‚ СЃСЂРµР·РѕРІ РґР»СЏ СЂР°СЃС‡РµС‚Р° РґРёР°РіСЂР°РјРјС‹.";
        return;
      }}

      emptyState.style.display = "none";
      chartCanvas.style.display = "block";

      if (typeof Chart === "undefined") {{
        statusNode.textContent = "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ Р±РёР±Р»РёРѕС‚РµРєСѓ РіСЂР°С„РёРєРѕРІ.";
        return;
      }}

      const datasets = buildBurndownDatasets(p1Value, p2Value);
      statusNode.textContent = `P1 = ${{formatHours(p1Value)}}, P2 = ${{formatHours(p2Value)}}. РЎСЂРµР·РѕРІ РІ СЂР°СЃС‡РµС‚Рµ: ${{burndownSnapshots.length}}.`;
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
              label: "РћР±СЉРµРј СЂР°Р·СЂР°Р±РѕС‚РєРё",
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
              label: "РћР±СЉРµРј РѕС€РёР±РѕРє",
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
              label: "РћСЃС‚Р°С‚РѕРє СЂР°Р·СЂР°Р±РѕС‚РєРё",
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
              label: "РћСЃС‚Р°С‚РѕРє РѕС€РёР±РѕРє",
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
              label: "Р‘СЋРґР¶РµС‚",
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
              label: "РћР±СЉРµРј.РџСЂРѕРіРЅРѕР·",
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
              label: "РћР±СЉРµРј.РўРµРєСѓС‰РёР№",
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
              label: "РћР±СЉРµРј.РћСЃС‚Р°С‚РѕРє",
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
  <title>РџСЂР°РІРёР»Р° РїРѕР»СѓС‡РµРЅРёСЏ СЃСЂРµР·РѕРІ</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
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
      font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
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
      font-size: clamp(1.85rem, 4.2vw, 2.75rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 400;
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
      <a href="/" aria-label="РќР° РіР»Р°РІРЅСѓСЋ">
        <span class="brand-logo-wrap">
          <img
            class="brand-logo"
            src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg"
            alt="РЎРњРЎ-РРў"
          >
        </span>
      </a>
      <a class="back-link" href="/" target="_self" rel="noreferrer">Р’РµСЂРЅСѓС‚СЊСЃСЏ РЅР° РіР»Р°РІРЅСѓСЋ</a>
    </div>
  </header>

  <main>
    <h1>РџСЂР°РІРёР»Р° РїРѕР»СѓС‡РµРЅРёСЏ СЃСЂРµР·РѕРІ</h1>
    <p class="lead">
      РќР° СЌС‚РѕР№ СЃС‚СЂР°РЅРёС†Рµ СЃРѕР±СЂР°РЅС‹ С‚РµРєСѓС‰РёРµ РїСЂР°РІРёР»Р°, РїРѕ РєРѕС‚РѕСЂС‹Рј РїСЂРёР»РѕР¶РµРЅРёРµ РїРѕР»СѓС‡Р°РµС‚ Р·Р°РґР°С‡Рё РёР· Redmine
      Рё Р·Р°РїРёСЃС‹РІР°РµС‚ РїСЂРѕРµРєС‚РЅС‹Рµ СЃСЂРµР·С‹ РІ Р±Р°Р·Сѓ РґР°РЅРЅС‹С….
    </p>

    <section class="panel">
      <h2>РљР°РєРёРµ РїСЂРѕРµРєС‚С‹ СѓС‡Р°СЃС‚РІСѓСЋС‚</h2>
      <ul>
        <li>Р’ Р·Р°РіСЂСѓР·РєСѓ РїРѕРїР°РґР°СЋС‚ С‚РѕР»СЊРєРѕ РїСЂРѕРµРєС‚С‹, Сѓ РєРѕС‚РѕСЂС‹С… РІ С‚Р°Р±Р»РёС†Рµ <code>РџСЂРѕРµРєС‚С‹ РІ Р±Р°Р·Рµ РґР°РЅРЅС‹С…</code> РІРєР»СЋС‡РµРЅ С„Р»Р°Р¶РѕРє <code>Р’РєР».</code>.</li>
        <li>Р”Р»СЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРѕРіРѕ РѕР±С‰РµРіРѕ Р·Р°РїСѓСЃРєР° Р±РµСЂСѓС‚СЃСЏ С‚РѕР»СЊРєРѕ РїСЂРѕРµРєС‚С‹, Сѓ РєРѕС‚РѕСЂС‹С… РЅР° С‚РµРєСѓС‰СѓСЋ РєР°Р»РµРЅРґР°СЂРЅСѓСЋ РґР°С‚Сѓ РµС‰Рµ РЅРµС‚ СЃСЂРµР·Р°.</li>
        <li>РџСЂРё СЂСѓС‡РЅРѕРј Р·Р°РїСѓСЃРєРµ РїРѕ РѕРґРЅРѕРјСѓ РїСЂРѕРµРєС‚Сѓ РїРµСЂРµСЃРЅРёРјР°РµС‚СЃСЏ С‚РѕР»СЊРєРѕ РІС‹Р±СЂР°РЅРЅС‹Р№ РїСЂРѕРµРєС‚.</li>
        <li>Р•СЃР»Рё <code>Р’РєР».</code> РІС‹РєР»СЋС‡РµРЅ, СЃСЂРµР·С‹ РїРѕ РїСЂРѕРµРєС‚Сѓ РЅРµ Р·Р°РіСЂСѓР¶Р°СЋС‚СЃСЏ.</li>
        <li>Р•СЃР»Рё РІРєР»СЋС‡РµРЅС‹ <code>Р’РєР».</code> Рё <code>Р§Р°СЃС‚.</code>, РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ С‡Р°СЃС‚РёС‡РЅР°СЏ Р·Р°РіСЂСѓР·РєР° Р·Р°РґР°С‡.</li>
        <li>Р•СЃР»Рё РІРєР»СЋС‡РµРЅ С‚РѕР»СЊРєРѕ <code>Р’РєР».</code>, Р·Р°РіСЂСѓР¶Р°СЋС‚СЃСЏ РІСЃРµ Р·Р°РґР°С‡Рё РїСЂРѕРµРєС‚Р°.</li>
      </ul>
    </section>

    <section class="panel">
      <h2>РљР°РєРёРµ Р·Р°РґР°С‡Рё РїРѕРїР°РґР°СЋС‚ РІ СЃСЂРµР·</h2>
      <ul>
        <li>Р‘РµСЂСѓС‚СЃСЏ С‚РѕР»СЊРєРѕ Р·Р°РґР°С‡Рё СЃР°РјРѕРіРѕ РїСЂРѕРµРєС‚Р°, Р±РµР· РїРѕРґРїСЂРѕРµРєС‚РѕРІ: РІ Р·Р°РїСЂРѕСЃР°С… РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ <code>subproject_id=!* </code>.</li>
        <li>РџСЂРё РїРѕР»РЅРѕР№ Р·Р°РіСЂСѓР·РєРµ Р±РµСЂСѓС‚СЃСЏ РІСЃРµ Р·Р°РґР°С‡Рё РїСЂРѕРµРєС‚Р°.</li>
        <li>РџСЂРё С‡Р°СЃС‚РёС‡РЅРѕР№ Р·Р°РіСЂСѓР·РєРµ РІСЃРµРіРґР° РїРѕРїР°РґР°СЋС‚ РІСЃРµ РѕС‚РєСЂС‹С‚С‹Рµ Р·Р°РґР°С‡Рё РїСЂРѕРµРєС‚Р°.</li>
        <li>РџСЂРё С‡Р°СЃС‚РёС‡РЅРѕР№ Р·Р°РіСЂСѓР·РєРµ РёР· Р·Р°РєСЂС‹С‚С‹С… Р·Р°РґР°С‡ РїРѕРїР°РґР°СЋС‚ С‚РѕР»СЊРєРѕ Р·Р°РґР°С‡Рё, Р·Р°РєСЂС‹С‚С‹Рµ РЅР°С‡РёРЅР°СЏ СЃ <code>{previousYearStart}</code>.</li>
        <li>Р•СЃР»Рё РѕРґРЅР° Рё С‚Р° Р¶Рµ Р·Р°РґР°С‡Р° РїРѕРґС…РѕРґРёС‚ СЃСЂР°Р·Сѓ РїРѕРґ РЅРµСЃРєРѕР»СЊРєРѕ РїСЂР°РІРёР», РІ СЃСЂРµР· РѕРЅР° Р·Р°РїРёСЃС‹РІР°РµС‚СЃСЏ РѕРґРёРЅ СЂР°Р·.</li>
      </ul>
    </section>

    <section class="panel">
      <h2>РљР°РєРёРµ РґР°РЅРЅС‹Рµ РїРѕ Р·Р°РґР°С‡Р°Рј СЃРѕС…СЂР°РЅСЏСЋС‚СЃСЏ</h2>
      <ul>
        <li>РЎРѕС…СЂР°РЅСЏСЋС‚СЃСЏ РѕСЃРЅРѕРІРЅС‹Рµ РїРѕР»СЏ Р·Р°РґР°С‡Рё: С‚СЂРµРєРµСЂ, СЃС‚Р°С‚СѓСЃ, РїСЂРёРѕСЂРёС‚РµС‚, РёСЃРїРѕР»РЅРёС‚РµР»СЊ, РІРµСЂСЃРёСЏ, РґР°С‚С‹ Рё РїСЂРѕС†РµРЅС‚С‹ РІС‹РїРѕР»РЅРµРЅРёСЏ.</li>
        <li><code>Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°</code> С‡РёС‚Р°РµС‚СЃСЏ РёР· РєР°СЃС‚РѕРјРЅРѕРіРѕ РїРѕР»СЏ Redmine СЃ РЅР°Р·РІР°РЅРёРµРј <code>Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°</code>.</li>
        <li><code>РџР»Р°РЅ</code> Р±РµСЂРµС‚СЃСЏ РёР· СЃС‚Р°РЅРґР°СЂС‚РЅРѕРіРѕ РїРѕР»СЏ <code>estimated_hours</code>.</li>
        <li><code>Р¤Р°РєС‚ Р·Р° РіРѕРґ</code> СЃС‡РёС‚Р°РµС‚СЃСЏ РїРѕ С‚СЂСѓРґРѕР·Р°С‚СЂР°С‚Р°Рј С‚РµРєСѓС‰РµРіРѕ РіРѕРґР°, Р° РЅРµ Р·Р° РІСЃСЋ РёСЃС‚РѕСЂРёСЋ Р·Р°РґР°С‡Рё.</li>
      </ul>
    </section>

    <section class="panel">
      <h2>РљР°Рє СЃСЂРµР· Р·Р°РїРёСЃС‹РІР°РµС‚СЃСЏ РІ Р±Р°Р·Сѓ</h2>
      <ul>
        <li>РЎРЅР°С‡Р°Р»Р° РїСЂРёР»РѕР¶РµРЅРёРµ РїРѕР»РЅРѕСЃС‚СЊСЋ РїРѕР»СѓС‡Р°РµС‚ Р·Р°РґР°С‡Рё Рё С‚СЂСѓРґРѕР·Р°С‚СЂР°С‚С‹ РёР· Redmine.</li>
        <li>РўРѕР»СЊРєРѕ РїРѕСЃР»Рµ РїРѕР»РЅРѕРіРѕ РїРѕР»СѓС‡РµРЅРёСЏ РґР°РЅРЅС‹С… РїРѕ РїСЂРѕРµРєС‚Сѓ СЃРѕР·РґР°РµС‚СЃСЏ Р·Р°РїРёСЃСЊ СЃСЂРµР·Р° Рё СЃС‚СЂРѕРєРё Р·Р°РґР°С‡ РІ Р±Р°Р·Рµ.</li>
        <li>Р—Р° РѕРґРЅРё СЃСѓС‚РєРё Сѓ РїСЂРѕРµРєС‚Р° С…СЂР°РЅРёС‚СЃСЏ С‚РѕР»СЊРєРѕ РѕРґРёРЅ СЃСЂРµР·.</li>
        <li>Р”Р°С‚Р° СЃСЂРµР·Р° С…СЂР°РЅРёС‚СЃСЏ РѕС‚РґРµР»СЊРЅРѕ РѕС‚ РІСЂРµРјРµРЅРё, РїРѕСЌС‚РѕРјСѓ РїРѕРІС‚РѕСЂРЅРѕ Р°РІС‚РѕРјР°С‚РѕРј С‚РѕС‚ Р¶Рµ РїСЂРѕРµРєС‚ Р·Р° С‚Рµ Р¶Рµ СЃСѓС‚РєРё РЅРµ РїРµСЂРµСЃРЅРёРјР°РµС‚СЃСЏ.</li>
      </ul>
    </section>

    <section class="panel note">
      Р•СЃР»Рё РїСЂР°РІРёР»Р° РїРѕР»СѓС‡РµРЅРёСЏ Р±СѓРґСѓС‚ РјРµРЅСЏС‚СЊСЃСЏ, СЌС‚Р° СЃС‚СЂР°РЅРёС†Р° РґРѕР»Р¶РЅР° РѕР±РЅРѕРІР»СЏС‚СЊСЃСЏ РІРјРµСЃС‚Рµ СЃ РєРѕРґРѕРј, С‡С‚РѕР±С‹ РѕРїРёСЃР°РЅРёРµ РІСЃРµРіРґР° СЃРѕРІРїР°РґР°Р»Рѕ СЃ С„Р°РєС‚РёС‡РµСЃРєРѕР№ Р»РѕРіРёРєРѕР№.
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
  <title>Р—Р°РґР°С‡Рё СЃСЂРµР·Р° РїСЂРѕРµРєС‚Р°</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    body {{ margin: 0; font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: #ffffff; color: #16324a; }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 24px 20px 48px; }}
    {buildProjectContextNavCss()}
    h1 {{ margin: 18px 0 12px; font-size: clamp(1.85rem, 4vw, 2.5rem); line-height: 1.02; letter-spacing: -0.04em; font-weight: 400; }}
    .meta {{ color: #64798d; margin: 0 0 24px; }}
    .meta-strong {{ color: #52cee6; font-weight: 800; }}
  </style>
</head>
<body>
    <main>
      {navPanelHtml}
      <h1>Р—Р°РґР°С‡Рё СЃСЂРµР·Р° РїСЂРѕРµРєС‚Р°</h1>
      <form method="get">
        <label for="capturedForDate">Р”Р°С‚Р° СЃСЂРµР·Р°</label>
        <select id="capturedForDate" name="captured_for_date" onchange="this.form.submit()">
          <option value="">РџРѕСЃР»РµРґРЅРёР№ СЃСЂРµР·</option>
          {optionsHtml}
        </select>
      </form>
      <p class="meta">Р”Р»СЏ РїСЂРѕРµРєС‚Р° СЃ ID {projectRedmineId} СЃСЂРµР·С‹ РїРѕРєР° РЅРµ РЅР°Р№РґРµРЅС‹.</p>
    </main>
  </body>
</html>"""

    issueRowsHtml = ['<tr><td colspan="13">Р—Р°РіСЂСѓР¶Р°РµРј Р·Р°РґР°С‡Рё...</td></tr>']

    summaryView = buildSnapshotSummaryView(snapshotPayload.get("summary"))
    totalBaselineEstimateHours = summaryView["baseline_estimate_hours"]
    totalEstimatedHours = summaryView["estimated_hours"]
    totalRiskEstimateHours = summaryView["risk_estimate_hours"]
    totalSpentHours = summaryView["spent_hours"]
    totalSpentHoursYear = summaryView["spent_hours_year"]
    developmentEstimateHours = summaryView["development_estimated_hours"]
    developmentRiskEstimateHours = summaryView["development_risk_estimate_hours"]
    developmentSpentHours = summaryView["development_spent_hours"]
    developmentSpentHoursYear = summaryView["development_spent_hours_year"]
    developmentProcessEstimateHours = summaryView["development_process_estimated_hours"]
    developmentProcessSpentHours = summaryView["development_process_spent_hours"]
    developmentProcessSpentHoursYear = summaryView["development_process_spent_hours_year"]
    bugEstimateHours = summaryView["bug_estimated_hours"]
    bugSpentHours = summaryView["bug_spent_hours"]
    bugSpentHoursYear = summaryView["bug_spent_hours_year"]
    featureBaselineEstimateHours = summaryView["feature_baseline_estimate_hours"]
    featureEstimatedHours = summaryView["feature_estimated_hours"]
    featureSpentHours = summaryView["feature_spent_hours"]
    featureSpentHoursYear = summaryView["feature_spent_hours_year"]
    featureBaselineEstimateClass = "summary-feature-control-zero"
    featureEstimatedClass = "summary-feature-control-zero" if featureEstimatedHours == 0 else "summary-feature-control-alert"
    featureSpentYearClass = "summary-feature-control-zero" if featureSpentHoursYear == 0 else "summary-feature-control-alert"
    featureSpentClass = "summary-feature-control-zero" if featureSpentHours == 0 else "summary-feature-control-alert"

    projectName = escape(str(snapshotRun.get("project_name") or "вЂ”"))
    capturedForDateRaw = str(snapshotRun.get("captured_for_date") or "")
    capturedForDate = escape(capturedForDateRaw or "вЂ”")
    selectedDate = capturedForDateRaw
    projectIdentifierRaw = str(
        snapshotRun.get("project_identifier")
        or (storedProject.get("identifier") if storedProject else "")
    ).strip()
    projectIdentifier = escape(projectIdentifierRaw or "вЂ”")
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
    optionsHtml = ["<option value=\"\">РџРѕСЃР»РµРґРЅРёР№ СЃСЂРµР·</option>"]
    for dateValue in availableDates:
        selectedAttr = " selected" if dateValue == selectedDate else ""
        optionsHtml.append(f'<option value="{escape(dateValue)}"{selectedAttr}>{escape(dateValue)}</option>')

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Р—Р°РґР°С‡Рё СЃСЂРµР·Р° РїСЂРѕРµРєС‚Р°</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
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
      body {{ margin: 0; font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: var(--bg); color: var(--text); }}
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
      h1 {{ margin: 18px 0 12px; font-size: clamp(1.85rem, 4vw, 2.65rem); line-height: 1.02; letter-spacing: -0.04em; font-weight: 400; }}
      .meta-strong {{ color: #33bdd8; font-weight: 400; }}
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
      .summary-table .summary-feature-control-zero {{ color: #b8c3cf; }}
      .summary-table .summary-feature-control-alert {{ color: #d54343; font-weight: 700; }}
      .filter-input-table,
      .filter-select-table,
      .filter-number-value,
      .filter-number-op {{ width: 100%; border: 1px solid var(--line); border-radius: 6px; padding: 5px 7px; font-size: 0.82rem; line-height: 1.2; background: #ffffff; color: var(--text); }}
      .filter-select-table {{ min-height: 72px; }}
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
      .filter-head th {{ top: var(--snapshot-filter-top, 36px); background: #f7fbfc; padding-top: 5px; padding-bottom: 5px; z-index: 3; text-transform: none; box-shadow: inset 0 1px 0 #d9e5eb; }}
      .filter-reset-wrap {{ display: flex; justify-content: space-between; align-items: center; gap: 10px; margin: 0 0 10px; flex-wrap: wrap; }}
      .table-actions {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
      .filter-reset-button {{ background: #375d77; color: #ffffff; transition: background 120ms ease, color 120ms ease, border-color 120ms ease; }}
      .filter-reset-button.is-inactive {{
        background: #eef2f5;
        color: #8a98a8;
        border: 1px solid #d9e5eb;
      }}
      .filter-reset-button.is-inactive:hover {{
        background: #eef2f5;
      }}
      .csv-export-button {{ background: #375d77; color: #ffffff; }}
      .filter-tip {{ color: var(--muted); font-size: 0.92rem; }}
      .page-size-label {{ color: var(--muted); }}
      .page-size-input {{ width: 110px; }}
      .pagination-wrap {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; margin: 0 0 12px; flex-wrap: wrap; }}
      .pagination-buttons {{ display: flex; gap: 8px; align-items: center; }}
      .pagination-info {{ color: var(--muted); font-size: 0.94rem; }}
      .table-wrap {{ position: relative; min-height: 420px; overflow: auto; border: 1px solid var(--line); border-radius: 8px; }}
      .snapshot-loading-overlay {{
        position: absolute;
        inset: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 12px;
        background: rgba(255, 255, 255, 0.76);
        backdrop-filter: blur(1px);
        z-index: 6;
        opacity: 0;
        pointer-events: none;
        transition: opacity 140ms ease;
        border-radius: inherit;
      }}
      .snapshot-loading-overlay.is-visible {{
        opacity: 1;
        pointer-events: auto;
      }}
      .snapshot-loading-spinner {{
        width: 34px;
        height: 34px;
        border-radius: 50%;
        border: 3px solid rgba(82, 206, 230, 0.25);
        border-top-color: #52cee6;
        animation: snapshot-spin 0.8s linear infinite;
      }}
      .snapshot-loading-text {{
        font-weight: 700;
        color: #375d77;
      }}
      @keyframes snapshot-spin {{
        to {{ transform: rotate(360deg); }}
      }}
      table {{ width: 100%; border-collapse: separate; border-spacing: 0; background: var(--panel); }}
      #snapshotIssuesTable {{ min-width: 1800px; table-layout: auto; }}
      th, td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--line); vertical-align: top; }}
      th {{ position: sticky; top: 0; z-index: 4; background: #eef6f7; color: #426179; text-transform: uppercase; font-size: 0.74rem; line-height: 1.15; }}
      #snapshotIssuesTable thead th {{ top: 0; }}
      tr:last-child td {{ border-bottom: 0; }}
      .mono {{ font-family: Consolas, "Courier New", monospace; font-size: 0.95rem; white-space: nowrap; }}
      .issue-link {{ color: var(--blue); text-decoration: none; border-bottom: 1px dashed currentColor; font-weight: 700; }}
      .issue-link:hover {{ color: var(--orange); border-bottom-style: solid; }}
      .subject-col {{ width: 546px; min-width: 546px; max-width: 546px; white-space: normal; word-break: break-word; }}
      .tracker-col {{ width: 170px; min-width: 170px; max-width: 170px; white-space: normal; word-break: break-word; }}
      .status-col {{ width: 170px; min-width: 170px; max-width: 170px; white-space: normal; word-break: break-word; }}
      .baseline-col {{ width: 94px; min-width: 94px; max-width: 94px; white-space: normal; word-break: break-word; }}
      .spent-col {{ width: 94px; min-width: 94px; max-width: 94px; white-space: normal; word-break: break-word; }}
      .spent-year-col {{ width: 94px; min-width: 94px; max-width: 94px; white-space: normal; word-break: break-word; }}
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
        padding-left: calc(22px + (var(--snapshot-depth, 0) * 18px));
        position: relative;
      }}
      .snapshot-child-subject::before {{
        content: "";
        position: absolute;
        left: calc(8px + (var(--snapshot-depth, 0) * 18px));
        top: 0.8em;
        width: 8px;
        border-top: 1px solid #aabcca;
      }}
  </style>
    </head>
  <body>
    <main>
      {navPanelHtml}
      <h1>Р—Р°РґР°С‡Рё СЃСЂРµР·Р° РїСЂРѕРµРєС‚Р°</h1>
      <div class="toolbar">
      <form method="get">
        <label for="capturedForDate">Р”Р°С‚Р° СЃСЂРµР·Р°</label>
        <select id="capturedForDate" name="captured_for_date" onchange="this.form.submit()">
          {''.join(optionsHtml)}
        </select>
      </form>
      <label class="page-size-label" for="snapshotPageSizeInput">Р—Р°РґР°С‡ РЅР° СЃС‚СЂР°РЅРёС†Рµ</label>
      <input class="page-size-input" id="snapshotPageSizeInput" type="number" min="10" max="10000" step="10" value="{initialPageSize}">
      <button type="button" class="secondary-button" id="applySnapshotPageSizeButton">РџРѕРєР°Р·Р°С‚СЊ</button>
      <button type="button" class="secondary-button" id="exportSnapshotCsvButton">Р’С‹РіСЂСѓР·РёС‚СЊ CSV</button>
      <button type="button" id="recaptureSnapshotButton">Р—Р°РіСЂСѓР·РёС‚СЊ/РѕР±РЅРѕРІРёС‚СЊ РїРѕСЃР»РµРґРЅРёР№ СЃСЂРµР·</button>
      <button type="button" id="deleteSnapshotButton">РЈРґР°Р»РёС‚СЊ РІС‹Р±СЂР°РЅРЅС‹Р№ СЃСЂРµР·</button>
      </div>
      <div class="action-status" id="snapshotActionStatus"></div>
      <p class="meta">РџСЂРѕРµРєС‚: <span class="meta-strong">{projectName}</span>. РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ: <span class="meta-strong">{projectIdentifier}</span>. Р”Р°С‚Р° СЃСЂРµР·Р°: {capturedForDate}. РџРѕ С„РёР»СЊС‚СЂСѓ: <span id="filteredIssuesCount">{initialFilteredIssues}</span> РёР· {initialTotalIssues}. РќР° СЃС‚СЂР°РЅРёС†Рµ: <span id="pageIssuesCount">{len(issues)}</span>.</p>
      <div class="summary-block">
        <table class="summary-table">
          <thead>
            <tr>
              <th style="width: 33%"></th>
              <th>Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°</th>
              <th>РџР»Р°РЅ</th>
              <th>РџР»Р°РЅ СЃ СЂРёСЃРєР°РјРё</th>
              <th colspan="2">Р¤Р°РєС‚ (РіРѕРґ)</th>
              <th>% (РіРѕРґ)</th>
              <th colspan="2">Р¤Р°РєС‚ (РІСЃРµРіРѕ)</th>
              <th>% (РІСЃРµРіРѕ)</th>
            </tr>
          </thead>
          <tbody>
              <tr>
                <th>Р’СЃРµ Р·Р°РґР°С‡Рё Р±РµР· С„РёС‡</th>
                <td class="summary-empty"></td>
                <td class="summary-metric" id="summaryEstimated">{formatPageHours(totalEstimatedHours)}</td>
                <td class="summary-empty"></td>
                <td class="summary-metric" id="summarySpentYear" colspan="2">{formatPageHours(totalSpentHoursYear)}</td>
                <td class="summary-empty"></td>
                <td class="summary-metric" id="summarySpent" colspan="2">{formatPageHours(totalSpentHours)}</td>
                <td class="summary-empty"></td>
            </tr>
            <tr>
              <th>Р Р°Р·СЂР°Р±РѕС‚РєР°, С‡</th>
              <td class="summary-metric" id="summaryBaselineEstimate" rowspan="2">{formatPageHours(totalBaselineEstimateHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentEstimated">{formatPageHours(developmentEstimateHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentRiskEstimate">{formatPageHours(developmentRiskEstimateHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentSpentYear">{formatPageHours(developmentSpentHoursYear)}</td>
              <td class="summary-metric" id="summaryDevelopmentCombinedSpentYear" rowspan="2">{formatPageHours(summaryView["development_combined_spent_hours_year"])}</td>
              <td class="summary-empty" rowspan="2"></td>
              <td class="summary-metric" id="summaryDevelopmentSpent">{formatPageHours(developmentSpentHours)}</td>
              <td class="summary-metric" id="summaryDevelopmentCombinedSpent" rowspan="2">{formatPageHours(summaryView["development_combined_spent_hours"])}</td>
              <td class="summary-metric summary-percent" id="summaryDevelopmentCoverageAll" rowspan="2">{formatPageHours(summaryView["development_coverage_all_percent"])}%</td>
            </tr>
            <tr>
              <th>РџСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё, С‡</th>
              <td class="summary-metric" id="summaryDevelopmentProcessEstimated">{formatPageHours(developmentProcessEstimateHours)}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentProcessSpentYear">{formatPageHours(developmentProcessSpentHoursYear)}</td>
              <td class="summary-metric" id="summaryDevelopmentProcessSpent">{formatPageHours(developmentProcessSpentHours)}</td>
            </tr>
            <tr>
              <th>РћС€РёР±РєР°, С‡</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryBugEstimated">{formatPageHours(bugEstimateHours)}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryBugSpentYear" colspan="2">{formatPageHours(bugSpentHoursYear)}</td>
              <td class="summary-metric summary-percent" id="summaryBugShareYear">{formatPageHours(summaryView["bug_share_year_percent"])}%</td>
              <td class="summary-metric" id="summaryBugSpent" colspan="2">{formatPageHours(bugSpentHours)}</td>
              <td class="summary-metric summary-percent" id="summaryBugShareAll">{formatPageHours(summaryView["bug_share_all_percent"])}%</td>
            </tr>
            <tr>
              <th>РС‚РѕРіРѕ РїРѕ СЂР°Р·СЂР°Р±РѕС‚РєРµ</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentTotalEstimated">{formatPageHours(summaryView["development_total_estimated_hours"])}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentGrandSpentYear" colspan="2">{formatPageHours(summaryView["development_grand_spent_hours_year"])}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentGrandSpent" colspan="2">{formatPageHours(summaryView["development_grand_spent_hours"])}</td>
              <td class="summary-empty"></td>
            </tr>
            <tr>
              <th>РљРѕРЅС‚СЂРѕР»СЊ СЃРїРёСЃР°РЅРёСЏ РїРѕ С„РёС‡Р°Рј</th>
              <td class="summary-metric {featureBaselineEstimateClass}" id="summaryFeatureBaselineEstimate">{formatPageHours(featureBaselineEstimateHours)}</td>
              <td class="summary-metric {featureEstimatedClass}" id="summaryFeatureEstimated">{formatPageHours(featureEstimatedHours)}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric {featureSpentYearClass}" id="summaryFeatureSpentYear" colspan="2">{formatPageHours(featureSpentHoursYear)}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric {featureSpentClass}" id="summaryFeatureSpent" colspan="2">{formatPageHours(featureSpentHours)}</td>
              <td class="summary-empty"></td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="filter-reset-wrap">
        <span class="filter-tip">Р¤РёР»СЊС‚СЂС‹ РїСЂРёРјРµРЅСЏСЋС‚СЃСЏ Рє С‚Р°Р±Р»РёС†Рµ Рё СЃСѓРјРјР°Рј РІС‹С€Рµ. РЎСѓРјРјС‹ СЃС‡РёС‚Р°СЋС‚СЃСЏ РїРѕ РІСЃРµРј Р·Р°РґР°С‡Р°Рј, СѓРґРѕРІР»РµС‚РІРѕСЂСЏСЋС‰РёРј С„РёР»СЊС‚СЂСѓ, Р° РЅРµ С‚РѕР»СЊРєРѕ РїРѕ С‚РµРєСѓС‰РµР№ СЃС‚СЂР°РЅРёС†Рµ.</span>
        <div class="table-actions">
          <button type="button" class="filter-reset-button is-inactive" id="resetSnapshotFiltersButton">РЎР±СЂРѕСЃРёС‚СЊ С„РёР»СЊС‚СЂ</button>
        </div>
      </div>
      <div class="pagination-wrap">
        <div class="pagination-buttons">
          <button type="button" class="secondary-button" id="snapshotPrevPageButton">в†ђ РќР°Р·Р°Рґ</button>
          <button type="button" class="secondary-button" id="snapshotNextPageButton">Р’РїРµСЂРµРґ в†’</button>
        </div>
        <div class="pagination-info" id="snapshotPaginationInfo">РЎС‚СЂР°РЅРёС†Р° {initialPage} РёР· {initialTotalPages}</div>
      </div>
      <div class="table-wrap">
        <table id="snapshotIssuesTable">
        <thead>
          <tr>
            <th>ID</th>
            <th class="subject-col">РўРµРјР°</th>
            <th class="tracker-col">РўСЂРµРєРµСЂ</th>
            <th class="status-col">РЎС‚Р°С‚СѓСЃ</th>
            <th>Р“РѕС‚РѕРІРѕ, %</th>
            <th class="baseline-col">Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°, С‡</th>
            <th>РџР»Р°РЅ, С‡</th>
            <th>РџР»Р°РЅ СЃ СЂРёСЃРєР°РјРё, С‡</th>
            <th class="spent-col">Р¤Р°РєС‚ РІСЃРµРіРѕ, С‡</th>
            <th class="spent-year-col">Р¤Р°РєС‚ Р·Р° РіРѕРґ, С‡</th>
            <th class="closed-col">Р—Р°РєСЂС‹С‚Р°</th>
            <th>РСЃРїРѕР»РЅРёС‚РµР»СЊ</th>
            <th class="version-col">Р’РµСЂСЃРёСЏ</th>
          </tr>
          <tr class="filter-head">
            <th><input class="filter-input-table" type="text" data-filter-key="issueId" data-filter-role="text"></th>
            <th><input class="filter-input-table" type="text" data-filter-key="subject" data-filter-role="text"></th>
<th class="tracker-col"><select class="filter-select-table" multiple size="3" data-filter-key="tracker" data-filter-role="multi"></select></th>
<th class="status-col"><select class="filter-select-table" multiple size="3" data-filter-key="status" data-filter-role="multi"></select></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="doneRatio" data-filter-role="op"><option value="">вЂ”</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="1" data-filter-key="doneRatio" data-filter-role="value"></div></th>
            <th class="baseline-col"><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="baseline" data-filter-role="op"><option value="">вЂ”</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="baseline" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="estimated" data-filter-role="op"><option value="">вЂ”</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="estimated" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="risk" data-filter-role="op"><option value="">вЂ”</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="risk" data-filter-role="value"></div></th>
            <th class="spent-col"><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="spent" data-filter-role="op"><option value="">вЂ”</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="spent" data-filter-role="value"></div></th>
            <th class="spent-year-col"><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="spentYear" data-filter-role="op"><option value="">вЂ”</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="spentYear" data-filter-role="value"></div></th>
            <th class="closed-col"><input class="filter-input-table" type="text" data-filter-key="closedOn" data-filter-role="text"></th>
            <th><input class="filter-input-table" type="text" data-filter-key="assignedTo" data-filter-role="text"></th>
            <th class="version-col"><input class="filter-input-table" type="text" data-filter-key="fixedVersion" data-filter-role="text"></th>
          </tr>
        </thead>
        <tbody id="snapshotIssuesTableBody">
          {''.join(issueRowsHtml)}
        </tbody>
      </table>
      <div class="snapshot-loading-overlay" id="snapshotLoadingOverlay" aria-hidden="true">
        <span class="snapshot-loading-spinner" aria-hidden="true"></span>
        <span class="snapshot-loading-text">РћР±РЅРѕРІР»СЏРµРј С‚Р°Р±Р»РёС†Сѓ...</span>
      </div>
    </div>
    <script>
      const snapshotActionStatus = document.getElementById("snapshotActionStatus");
      const capturedForDateSelect = document.getElementById("capturedForDate");
      const filteredIssuesCount = document.getElementById("filteredIssuesCount");
      const pageIssuesCount = document.getElementById("pageIssuesCount");
      const snapshotIssuesTableBody = document.getElementById("snapshotIssuesTableBody");
      const snapshotIssuesTable = document.getElementById("snapshotIssuesTable");
      const snapshotLoadingOverlay = document.getElementById("snapshotLoadingOverlay");
      const snapshotTableWrap = document.querySelector(".table-wrap");
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
      const summaryRiskEstimate = document.getElementById("summaryRiskEstimate");
      const summaryFeatureBaselineEstimate = document.getElementById("summaryFeatureBaselineEstimate");
      const summaryFeatureEstimated = document.getElementById("summaryFeatureEstimated");
      const summaryFeatureSpent = document.getElementById("summaryFeatureSpent");
      const summaryFeatureSpentYear = document.getElementById("summaryFeatureSpentYear");
      const summaryEstimated = document.getElementById("summaryEstimated");
      const summarySpent = document.getElementById("summarySpent");
      const summarySpentYear = document.getElementById("summarySpentYear");
      const summaryDevelopmentEstimated = document.getElementById("summaryDevelopmentEstimated");
      const summaryDevelopmentRiskEstimate = document.getElementById("summaryDevelopmentRiskEstimate");
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
      let currentSnapshotFilterSignature = "";

      function setActionStatus(message) {{
        if (snapshotActionStatus) {{
          snapshotActionStatus.textContent = message;
        }}
      }}

      function setSnapshotLoading(isLoading) {{
        if (snapshotLoadingOverlay) {{
          snapshotLoadingOverlay.classList.toggle("is-visible", Boolean(isLoading));
          snapshotLoadingOverlay.setAttribute("aria-hidden", isLoading ? "false" : "true");
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
          return "вЂ”";
        }}
        return String(value).replace("T", " ").replace("+00:00", " UTC");
      }}

      function buildSummaryView(summary) {{
        const baselineEstimateHours = Number(summary?.baseline_estimate_hours || 0);
        const estimatedHours = Number(summary?.estimated_hours || 0);
        const riskEstimateHours = Number(summary?.risk_estimate_hours || 0);
        const spentHours = Number(summary?.spent_hours || 0);
        const spentHoursYear = Number(summary?.spent_hours_year || 0);
        const featureBaselineEstimateHours = Number(summary?.feature_baseline_estimate_hours || 0);
        const featureEstimatedHours = Number(summary?.feature_estimated_hours || 0);
        const featureSpentHours = Number(summary?.feature_spent_hours || 0);
        const featureSpentHoursYear = Number(summary?.feature_spent_hours_year || 0);
        const developmentEstimatedHours = Number(summary?.development_estimated_hours || 0);
        const developmentRiskEstimateHours = Number(summary?.development_risk_estimate_hours || 0);
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
          riskEstimateHours,
          spentHours,
          spentHoursYear,
          featureBaselineEstimateHours,
          featureEstimatedHours,
          featureSpentHours,
          featureSpentHoursYear,
          developmentEstimatedHours,
          developmentRiskEstimateHours,
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

      function updateSnapshotTableViewportHeight() {{
        if (!snapshotTableWrap) {{
          return;
        }}
        const rect = snapshotTableWrap.getBoundingClientRect();
        const availableHeight = Math.max(420, Math.floor(window.innerHeight - Math.max(rect.top, 0) - 12));
        snapshotTableWrap.style.height = `${{availableHeight}}px`;
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

      function getSnapshotIssueSortBucket(issue) {{
        const tracker = String(issue?.tracker_name || "").trim().toLowerCase();
        if (tracker === "СЂР°Р·СЂР°Р±РѕС‚РєР°") {{
          return 1;
        }}
        if (tracker === "РїСЂРѕС†РµСЃСЃС‹ СЂР°Р·СЂР°Р±РѕС‚РєРё") {{
          return 2;
        }}
        if (tracker === "РѕС€РёР±РєР°") {{
          return 3;
        }}
        return 4;
      }}

      function compareSnapshotIssuesForTree(left, right) {{
        const bucketDiff = getSnapshotIssueSortBucket(left) - getSnapshotIssueSortBucket(right);
        if (bucketDiff !== 0) {{
          return bucketDiff;
        }}
        return Number(left?.issue_redmine_id || 0) - Number(right?.issue_redmine_id || 0);
      }}

      function buildSnapshotTreeOrder(groupIssues) {{
        const issues = Array.isArray(groupIssues) ? groupIssues.slice() : [];
        const issueById = new Map();
        const childrenByParentId = new Map();
        const roots = [];

        for (const issue of issues) {{
          issueById.set(String(issue?.issue_redmine_id ?? ""), issue);
        }}

        for (const issue of issues) {{
          const parentId = issue?.parent_issue_redmine_id;
          const parentKey = parentId == null ? "" : String(parentId);
          if (parentKey && issueById.has(parentKey)) {{
            if (!childrenByParentId.has(parentKey)) {{
              childrenByParentId.set(parentKey, []);
            }}
            childrenByParentId.get(parentKey).push(issue);
          }} else {{
            roots.push(issue);
          }}
        }}

        roots.sort(compareSnapshotIssuesForTree);
        for (const children of childrenByParentId.values()) {{
          children.sort(compareSnapshotIssuesForTree);
        }}

        const ordered = [];
        const visited = new Set();

        function visit(issue, depth) {{
          const issueKey = String(issue?.issue_redmine_id ?? "");
          if (!issueKey || visited.has(issueKey)) {{
            return;
          }}
          visited.add(issueKey);
          ordered.push({{ ...issue, __treeDepth: depth }});
          for (const child of childrenByParentId.get(issueKey) || []) {{
            visit(child, depth + 1);
          }}
        }}

        for (const root of roots) {{
          visit(root, 0);
        }}

        for (const issue of issues.sort(compareSnapshotIssuesForTree)) {{
          visit(issue, 0);
        }}

        return ordered;
      }}

      function renderSnapshotSummary(summary) {{
        const view = buildSummaryView(summary);
        const updateFeatureControlMetric = (node, value, highlightNonZero = true) => {{
          if (!node) {{
            return;
          }}
          node.textContent = formatFilterHours(value);
          node.classList.toggle("summary-feature-control-zero", Number(value || 0) === 0);
          node.classList.toggle("summary-feature-control-alert", highlightNonZero && Number(value || 0) !== 0);
        }};
        if (summaryBaselineEstimate) summaryBaselineEstimate.textContent = formatFilterHours(view.baselineEstimateHours);
        if (summaryRiskEstimate) summaryRiskEstimate.textContent = formatFilterHours(view.riskEstimateHours);
        updateFeatureControlMetric(summaryFeatureBaselineEstimate, view.featureBaselineEstimateHours, false);
        updateFeatureControlMetric(summaryFeatureEstimated, view.featureEstimatedHours);
        updateFeatureControlMetric(summaryFeatureSpent, view.featureSpentHours);
        updateFeatureControlMetric(summaryFeatureSpentYear, view.featureSpentHoursYear);
        if (summaryEstimated) summaryEstimated.textContent = formatFilterHours(view.estimatedHours);
        if (summarySpent) summarySpent.textContent = formatFilterHours(view.spentHours);
        if (summarySpentYear) summarySpentYear.textContent = formatFilterHours(view.spentHoursYear);
        if (summaryDevelopmentEstimated) summaryDevelopmentEstimated.textContent = formatFilterHours(view.developmentEstimatedHours);
        if (summaryDevelopmentRiskEstimate) summaryDevelopmentRiskEstimate.textContent = formatFilterHours(view.developmentRiskEstimateHours);
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
          snapshotIssuesTableBody.innerHTML = '<tr><td colspan="13">РџРѕ С‚РµРєСѓС‰РµРјСѓ С„РёР»СЊС‚СЂСѓ Р·Р°РґР°С‡ РЅРµС‚.</td></tr>';
          return;
        }}
        const groupMap = new Map();
        const rows = [];
        for (const issue of issues) {{
          const groupId = issue?.feature_group_issue_redmine_id;
          const groupKey = groupId ? `feature-${{groupId}}` : "virtual-feature";
          if (!groupMap.has(groupKey)) {{
            groupMap.set(groupKey, {{
              groupIssue: issue,
              childIssues: [],
            }});
          }}
          if (!issue?.is_feature_group_root) {{
            groupMap.get(groupKey).childIssues.push(issue);
          }}
        }}

        for (const [groupKey, groupData] of groupMap.entries()) {{
          const issue = groupData.groupIssue;
          const groupId = issue?.feature_group_issue_redmine_id;
          const groupSubject = String(issue?.feature_group_subject || "Р±РµР· Feature");
          const isVirtualGroup = Boolean(issue?.feature_group_is_virtual);
          if (groupKey) {{
            const groupLink = !isVirtualGroup && groupId
              ? `<a class="issue-link" href="https://redmine.sms-it.ru/issues/${{encodeURIComponent(groupId)}}" target="_blank" rel="noreferrer">${{escapeHtml(groupId)}}</a>`
              : "";
            const groupTracker = isVirtualGroup ? "вЂ”" : escapeHtml(issue?.feature_group_tracker_name || "Feature");
            const groupStatus = isVirtualGroup ? "вЂ”" : escapeHtml(issue?.feature_group_status_name || "вЂ”");
            const groupDoneRatio = isVirtualGroup ? "вЂ”" : escapeHtml(issue?.feature_group_done_ratio ?? 0);
            const groupBaseline = isVirtualGroup ? "вЂ”" : formatFilterHours(issue?.feature_group_baseline_estimate_hours);
            const groupEstimated = isVirtualGroup ? "вЂ”" : formatFilterHours(issue?.feature_group_estimated_hours);
            const groupRisk = isVirtualGroup ? "вЂ”" : formatFilterHours(issue?.feature_group_risk_estimate_hours);
            const groupSpent = isVirtualGroup ? "вЂ”" : formatFilterHours(issue?.feature_group_spent_hours);
            const groupSpentYear = isVirtualGroup ? "вЂ”" : formatFilterHours(issue?.feature_group_spent_hours_year);
            const groupClosedOn = isVirtualGroup ? "вЂ”" : escapeHtml(formatSnapshotDateTime(issue?.feature_group_closed_on));
            const groupAssignedTo = isVirtualGroup ? "вЂ”" : escapeHtml(issue?.feature_group_assigned_to_name || "вЂ”");
            const groupVersion = isVirtualGroup ? "вЂ”" : escapeHtml(issue?.feature_group_fixed_version_name || "вЂ”");
            const groupIdCell = isVirtualGroup
              ? `<span class="snapshot-group-id"><span class="snapshot-group-id-empty">вЂ”</span></span>`
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
                <td class="snapshot-group-cell snapshot-group-metric">${{groupRisk}}</td>
                <td class="snapshot-group-cell snapshot-group-metric">${{groupSpent}}</td>
                <td class="snapshot-group-cell snapshot-group-metric">${{groupSpentYear}}</td>
                <td class="snapshot-group-cell closed-col">${{groupClosedOn}}</td>
                <td class="snapshot-group-cell">${{groupAssignedTo}}</td>
                <td class="snapshot-group-cell version-col">${{groupVersion}}</td>
              </tr>
            `);
          }}
          const orderedIssues = buildSnapshotTreeOrder(groupData.childIssues);
          for (const orderedIssue of orderedIssues) {{
            const issueId = orderedIssue?.issue_redmine_id ?? "вЂ”";
            const issueLink = `https://redmine.sms-it.ru/issues/${{encodeURIComponent(issueId)}}`;
            const treeDepth = Number(orderedIssue?.__treeDepth || 0);
            rows.push(`
              <tr>
                <td class="mono"><a class="issue-link" href="${{issueLink}}" target="_blank" rel="noreferrer">${{escapeHtml(issueId)}}</a></td>
                <td class="subject-col"><span class="snapshot-child-subject" style="--snapshot-depth: ${{treeDepth}};">${{escapeHtml(orderedIssue?.subject || "вЂ”")}}</span></td>
                <td class="tracker-col">${{escapeHtml(orderedIssue?.tracker_name || "вЂ”")}}</td>
                <td class="status-col">${{escapeHtml(orderedIssue?.status_name || "вЂ”")}}</td>
                <td>${{escapeHtml(orderedIssue?.done_ratio ?? 0)}}</td>
                <td class="baseline-col">${{formatFilterHours(orderedIssue?.baseline_estimate_hours)}}</td>
                <td>${{formatFilterHours(orderedIssue?.estimated_hours)}}</td>
                <td>${{formatFilterHours(orderedIssue?.risk_estimate_hours)}}</td>
                <td class="spent-col">${{formatFilterHours(orderedIssue?.spent_hours)}}</td>
                <td class="spent-year-col">${{formatFilterHours(orderedIssue?.spent_hours_year)}}</td>
                <td class="closed-col">${{escapeHtml(formatSnapshotDateTime(orderedIssue?.closed_on))}}</td>
                <td>${{escapeHtml(orderedIssue?.assigned_to_name || "вЂ”")}}</td>
                <td class="version-col">${{escapeHtml(orderedIssue?.fixed_version_name || "вЂ”")}}</td>
              </tr>
            `);
          }}
        }}
        snapshotIssuesTableBody.innerHTML = rows.join("");
        updateSnapshotFilterHeaderOffset();
        updateSnapshotTableViewportHeight();
      }}

      function normalizeNumericFilterValue(value) {{
        return String(value || "").trim().replace(",", ".");
      }}

      function normalizeSnapshotFilters(filters) {{
        const normalizedFilters = {{
          ...filters,
          done_ratio_op: "",
          done_ratio_value: "",
          baseline_op: "",
          baseline_value: "",
          estimated_op: "",
          estimated_value: "",
          risk_op: "",
          risk_value: "",
          spent_op: "",
          spent_value: "",
          spent_year_op: "",
          spent_year_value: "",
        }};

        const numericMappings = [
          ["done_ratio_op", "done_ratio_value"],
          ["baseline_op", "baseline_value"],
          ["estimated_op", "estimated_value"],
          ["risk_op", "risk_value"],
          ["spent_op", "spent_value"],
          ["spent_year_op", "spent_year_value"],
        ];

        for (const [opKey, valueKey] of numericMappings) {{
          if (filters[opKey] && filters[valueKey]) {{
            normalizedFilters[opKey] = filters[opKey];
            normalizedFilters[valueKey] = filters[valueKey];
          }}
        }}

        return normalizedFilters;
      }}

      function hasActiveSnapshotFilters() {{
        const filters = normalizeSnapshotFilters(collectSnapshotFilters());
        return Boolean(
          filters.issue_id ||
          filters.subject ||
          (Array.isArray(filters.tracker) && filters.tracker.length) ||
          (Array.isArray(filters.status) && filters.status.length) ||
          filters.done_ratio_op ||
          filters.done_ratio_value ||
          filters.baseline_op ||
          filters.baseline_value ||
          filters.estimated_op ||
          filters.estimated_value ||
          filters.risk_op ||
          filters.risk_value ||
          filters.spent_op ||
          filters.spent_value ||
          filters.spent_year_op ||
          filters.spent_year_value ||
          filters.closed_on ||
          filters.assigned_to ||
          filters.fixed_version
        );
      }}

      function updateResetSnapshotFiltersButtonState() {{
        if (!resetSnapshotFiltersButton) {{
          return;
        }}
        resetSnapshotFiltersButton.classList.toggle("is-inactive", !hasActiveSnapshotFilters());
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
          snapshotPaginationInfo.textContent = `РЎС‚СЂР°РЅРёС†Р° ${{currentSnapshotPage}} РёР· ${{currentSnapshotTotalPages}}`;
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
          risk_op: String(document.querySelector('[data-filter-key="risk"][data-filter-role="op"]')?.value || ""),
          risk_value: normalizeNumericFilterValue(document.querySelector('[data-filter-key="risk"][data-filter-role="value"]')?.value || ""),
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
        const filters = normalizeSnapshotFilters(collectSnapshotFilters());
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
        if (filters.risk_op) params.set("risk_op", filters.risk_op);
        if (filters.risk_value) params.set("risk_value", filters.risk_value);
        if (filters.spent_op) params.set("spent_op", filters.spent_op);
        if (filters.spent_value) params.set("spent_value", filters.spent_value);
        if (filters.spent_year_op) params.set("spent_year_op", filters.spent_year_op);
        if (filters.spent_year_value) params.set("spent_year_value", filters.spent_year_value);
        if (filters.closed_on) params.set("closed_on", filters.closed_on);
        if (filters.assigned_to) params.set("assigned_to", filters.assigned_to);
        if (filters.fixed_version) params.set("fixed_version", filters.fixed_version);
        return params;
      }}

      function buildSnapshotFilterSignature() {{
        return buildSnapshotQueryParams(1, false).toString();
      }}

      async function loadSnapshotIssues(page = 1) {{
        setSnapshotLoading(true);
        try {{
          const pageSize = readSnapshotPageSize();
          currentSnapshotPageSize = pageSize;
          if (snapshotPageSizeInput) snapshotPageSizeInput.value = String(pageSize);
          window.localStorage.setItem(snapshotPageSizeStorageKey, String(pageSize));
          if (snapshotPaginationInfo) snapshotPaginationInfo.textContent = "РћР±РЅРѕРІР»СЏРµРј С‚Р°Р±Р»РёС†Сѓ...";
          const params = buildSnapshotQueryParams(page, true);
          const response = await fetch(`/api/projects/{projectRedmineId}/latest-snapshot-issues?${{params.toString()}}`);
          const payload = await response.json();
          if (!response.ok) {{
            window.alert(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ Р·Р°РґР°С‡Рё СЃСЂРµР·Р°.");
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
          currentSnapshotFilterSignature = buildSnapshotFilterSignature();
          updateResetSnapshotFiltersButtonState();
        }} catch (error) {{
          window.alert("РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ Р·Р°РґР°С‡Рё СЃСЂРµР·Р°.");
          updateSnapshotPaginationInfo();
        }} finally {{
          setSnapshotLoading(false);
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
        updateResetSnapshotFiltersButtonState();
        loadSnapshotIssues(1);
      }}

      function scheduleSnapshotReload() {{
        const nextFilterSignature = buildSnapshotFilterSignature();
        if (nextFilterSignature === currentSnapshotFilterSignature) {{
          return;
        }}
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

          const projectName = payload.current_project_name || payload.last_completed_project_name || "Р±РµР· РЅР°Р·РІР°РЅРёСЏ";
          const issuesPagesLoaded = Number(payload.current_project_issues_pages_loaded ?? 0);
          const issuesPagesTotal = Number(payload.current_project_issues_pages_total ?? 0);
          const timePagesLoaded = Number(payload.current_project_time_pages_loaded ?? 0);
          const timePagesTotal = Number(payload.current_project_time_pages_total ?? 0);
          const progressParts = [];

          if (issuesPagesTotal > 0) {{
            progressParts.push(`Р·Р°РґР°С‡Рё ${{
              issuesPagesLoaded
            }}/${{issuesPagesTotal}} СЃС‚СЂ.`);
          }}

          if (timePagesTotal > 0) {{
            progressParts.push(`С‚СЂСѓРґРѕР·Р°С‚СЂР°С‚С‹ ${{
              timePagesLoaded
            }}/${{timePagesTotal}} СЃС‚СЂ.`);
          }} else if (issuesPagesTotal > 0 && issuesPagesLoaded >= issuesPagesTotal) {{
            progressParts.push("РіРѕС‚РѕРІРёРј С‚СЂСѓРґРѕР·Р°С‚СЂР°С‚С‹");
          }}

          const progressSuffix = progressParts.length ? ` (${{
            progressParts.join(", ")
          }})` : "";
          setActionStatus(`РџРѕР»СѓС‡Р°РµРј СЃСЂРµР· РїРѕ РїСЂРѕРµРєС‚Сѓ ${{projectName}}${{progressSuffix}}`);
          window.setTimeout(() => pollRecaptureStatus(targetDate), 1500);
        }} catch (error) {{
          setActionStatus("РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕР»СѓС‡РёС‚СЊ СЃС‚Р°С‚СѓСЃ РїРѕРІС‚РѕСЂРЅРѕРіРѕ СЃСЂРµР·Р°.");
        }}
      }}

      document.getElementById("recaptureSnapshotButton")?.addEventListener("click", async () => {{
        setActionStatus("Р—Р°РїСѓСЃРєР°РµРј РїРѕРІС‚РѕСЂРЅРѕРµ РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·Р°...");
        const response = await fetch("/api/issues/snapshots/recapture-project/{projectRedmineId}", {{
          method: "POST"
        }});
        const payload = await response.json();

        if (!response.ok) {{
          window.alert(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїСѓСЃС‚РёС‚СЊ РїРѕРІС‚РѕСЂРЅРѕРµ РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·Р°.");
          setActionStatus("");
          return;
        }}

        const targetDate = payload.captured_for_date || "{selectedDate}";
        setActionStatus(payload.detail || "РџРѕРІС‚РѕСЂРЅРѕРµ РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·Р° Р·Р°РїСѓС‰РµРЅРѕ.");
        pollRecaptureStatus(targetDate);
      }});

      document.getElementById("deleteSnapshotButton")?.addEventListener("click", async () => {{
        if (!window.confirm("РЈРґР°Р»РёС‚СЊ РІС‹Р±СЂР°РЅРЅС‹Р№ СЃСЂРµР·?")) {{
          return;
        }}

        const selectedDateForDelete = String(capturedForDateSelect?.value || "{capturedForDate}");
        const response = await fetch(`/api/issues/snapshots/project/{projectRedmineId}/by-date?captured_for_date=${{encodeURIComponent(selectedDateForDelete)}}`, {{
          method: "DELETE"
        }});
        const payload = await response.json();
        if (!response.ok) {{
          window.alert(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ СѓРґР°Р»РёС‚СЊ СЃСЂРµР·.");
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
        input.addEventListener("input", () => {{
          updateResetSnapshotFiltersButtonState();
          scheduleSnapshotReload();
        }});
      }});
      multiSelectFilters.forEach((select) => {{
        select.addEventListener("change", () => {{
          updateResetSnapshotFiltersButtonState();
          loadSnapshotIssues(1);
        }});
      }});
      numericFilterControls.forEach((control) => {{
        control.addEventListener("input", () => {{
          updateResetSnapshotFiltersButtonState();
          scheduleSnapshotReload();
        }});
        control.addEventListener("change", () => {{
          updateResetSnapshotFiltersButtonState();
          scheduleSnapshotReload();
        }});
      }});

      resetSnapshotFiltersButton?.addEventListener("click", resetSnapshotTableFilters);
      populateSnapshotMultiSelects();
      renderSnapshotRows(initialSnapshotIssues);
      renderSnapshotSummary(initialSnapshotSummary);
      updateSnapshotCounts(initialSnapshotIssues.length);
      updateSnapshotFilterHeaderOffset();
      updateSnapshotTableViewportHeight();
      updateSnapshotPaginationInfo();
      currentSnapshotFilterSignature = buildSnapshotFilterSignature();
      updateResetSnapshotFiltersButtonState();
      window.addEventListener("resize", updateSnapshotFilterHeaderOffset);
      window.addEventListener("resize", updateSnapshotTableViewportHeight);
      window.addEventListener("scroll", updateSnapshotTableViewportHeight, {{ passive: true }});

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


BITRIX_PAGE_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bitrix Test Page</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      color-scheme: light;
      --ink: #10293d;
      --muted: #5d7487;
      --paper: #f4f8fb;
      --card: rgba(255, 255, 255, 0.92);
      --line: rgba(16, 41, 61, 0.12);
      --blue-302: #375d77;
      --yellow-109: #ffc600;
      --cyan-310: #52cee6;
      --orange-1585: #ff6c0e;
      --shadow: 0 22px 50px rgba(16, 41, 61, 0.14);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(82, 206, 230, 0.35), transparent 34%),
        radial-gradient(circle at top right, rgba(255, 198, 0, 0.35), transparent 26%),
        linear-gradient(180deg, #ffffff 0%, var(--paper) 100%);
    }

    a {
      color: inherit;
    }

    .shell {
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }

    .hero {
      position: relative;
      overflow: hidden;
      padding: 28px;
      border-radius: 28px;
      border: 1px solid rgba(255, 255, 255, 0.7);
      background:
        linear-gradient(135deg, rgba(55, 93, 119, 0.96), rgba(16, 41, 61, 0.94)),
        linear-gradient(135deg, rgba(255, 198, 0, 0.18), rgba(82, 206, 230, 0.2));
      color: #ffffff;
      box-shadow: var(--shadow);
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: auto -90px -120px auto;
      width: 280px;
      height: 280px;
      border-radius: 50%;
      background: rgba(82, 206, 230, 0.16);
      filter: blur(2px);
    }

    .topline {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
      margin-bottom: 32px;
    }

    .brand {
      display: inline-flex;
      align-items: center;
      gap: 16px;
      text-decoration: none;
    }

    .brand img {
      width: 172px;
      height: auto;
      display: block;
    }

    .brand-copy {
      display: grid;
      gap: 2px;
    }

    .brand-copy strong {
      font-size: 1rem;
      letter-spacing: 0.02em;
    }

    .brand-copy span {
      color: rgba(255, 255, 255, 0.72);
      font-size: 0.95rem;
    }

    .back-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 44px;
      padding: 0 18px;
      border-radius: 999px;
      border: 1px solid rgba(255, 255, 255, 0.22);
      background: rgba(255, 255, 255, 0.1);
      text-decoration: none;
      font-weight: 600;
      backdrop-filter: blur(8px);
    }

    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      margin: 0 0 14px;
      color: rgba(255, 255, 255, 0.74);
      font-size: 0.96rem;
      text-transform: uppercase;
      letter-spacing: 0.14em;
    }

    .eyebrow::before {
      content: "";
      width: 42px;
      height: 3px;
      border-radius: 999px;
      background: var(--yellow-109);
    }

    h1 {
      max-width: 10ch;
      margin: 0;
      font-size: clamp(2.5rem, 6vw, 5rem);
      line-height: 0.96;
      letter-spacing: -0.05em;
    }

    .lead {
      max-width: 720px;
      margin: 18px 0 0;
      font-size: clamp(1rem, 2.3vw, 1.24rem);
      line-height: 1.65;
      color: rgba(255, 255, 255, 0.82);
    }

    .hero-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 28px;
    }

    .button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 48px;
      padding: 0 20px;
      border-radius: 14px;
      text-decoration: none;
      font-weight: 700;
      transition: transform 120ms ease, filter 120ms ease;
    }

    .button:hover {
      transform: translateY(-1px);
      filter: brightness(1.03);
    }

    .button-primary {
      background: var(--yellow-109);
      color: var(--ink);
    }

    .button-secondary {
      background: rgba(255, 255, 255, 0.12);
      color: #ffffff;
      border: 1px solid rgba(255, 255, 255, 0.2);
    }

    .grid {
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      margin-top: 22px;
    }

    .card {
      padding: 24px;
      border-radius: 24px;
      border: 1px solid var(--line);
      background: var(--card);
      box-shadow: 0 14px 32px rgba(16, 41, 61, 0.08);
      backdrop-filter: blur(16px);
    }

    .card h2 {
      margin: 0 0 12px;
      font-size: 1.08rem;
    }

    .card p,
    .card li {
      color: var(--muted);
      line-height: 1.65;
    }

    .card ul {
      margin: 0;
      padding-left: 18px;
    }

    .metrics {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      margin-top: 22px;
    }

    .metric {
      padding: 18px 20px;
      border-radius: 22px;
      background: rgba(255, 255, 255, 0.16);
      border: 1px solid rgba(255, 255, 255, 0.14);
      backdrop-filter: blur(10px);
    }

    .metric strong {
      display: block;
      margin-bottom: 6px;
      font-size: 2rem;
      letter-spacing: -0.04em;
    }

    .metric span {
      color: rgba(255, 255, 255, 0.78);
      line-height: 1.45;
    }

    .accent-card {
      background:
        linear-gradient(180deg, rgba(82, 206, 230, 0.16), rgba(255, 255, 255, 0.96)),
        #ffffff;
    }

    @media (max-width: 760px) {
      .shell {
        padding: 18px 14px 40px;
      }

      .hero {
        padding: 22px 18px;
        border-radius: 24px;
      }

      .brand img {
        width: 148px;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="topline">
        <a class="brand" href="/">
          <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="РЎРњРЎ-РРў">
          <span class="brand-copy">
            <strong>Redmine + Bitrix</strong>
            <span>РњР°СЂС€СЂСѓС‚ /Bitrix СѓР¶Рµ РґРѕСЃС‚СѓРїРµРЅ РЅР° СЃР°Р№С‚Рµ</span>
          </span>
        </a>
        <a class="back-link" href="/">Р’РµСЂРЅСѓС‚СЊСЃСЏ РЅР° РіР»Р°РІРЅСѓСЋ</a>
      </div>

      <p class="eyebrow">Bitrix / Test Route</p>
      <h1>Bitrix test page</h1>
      <p class="lead">
        Р­С‚Рѕ С‚РµСЃС‚РѕРІР°СЏ СЃС‚СЂР°РЅРёС†Р° РґР»СЏ РїСЂРѕРІРµСЂРєРё РѕС‚РґРµР»СЊРЅРѕРіРѕ РјР°СЂС€СЂСѓС‚Р° РІ С‚РµРєСѓС‰РµРј РїСЂРёР»РѕР¶РµРЅРёРё.
        РћРЅР° Р¶РёРІРµС‚ СЂСЏРґРѕРј СЃ РѕСЃРЅРѕРІРЅС‹Рј Redmine-РёРЅС‚РµСЂС„РµР№СЃРѕРј Рё РіРѕС‚РѕРІР° РґР»СЏ РґР°Р»СЊРЅРµР№С€РµР№
        РёРЅС‚РµРіСЂР°С†РёРё СЃ Bitrix-С„РѕСЂРјР°РјРё, iframe РёР»Рё РІРёРґР¶РµС‚Р°РјРё.
      </p>

      <div class="hero-actions">
        <a class="button button-primary" href="/">РћС‚РєСЂС‹С‚СЊ РіР»Р°РІРЅСѓСЋ СЃС‚СЂР°РЅРёС†Сѓ</a>
        <a class="button button-secondary" href="/health">РџСЂРѕРІРµСЂРёС‚СЊ health endpoint</a>
      </div>

      <div class="metrics">
        <div class="metric">
          <strong>/Bitrix</strong>
          <span>РњР°СЂС€СЂСѓС‚ РІС‹РЅРµСЃРµРЅ РІ FastAPI Рё РґРѕСЃС‚СѓРїРµРЅ РєР°Рє РѕС‚РґРµР»СЊРЅР°СЏ СЃС‚СЂР°РЅРёС†Р°.</span>
        </div>
        <div class="metric">
          <strong>HTML</strong>
          <span>РЎС‚СЂР°РЅРёС†Р° СЃС‚Р°С‚РёС‡РµСЃРєР°СЏ Рё Р±РµР·РѕРїР°СЃРЅРѕ РґРѕР±Р°РІР»РµРЅР° Р±РµР· РІР»РёСЏРЅРёСЏ РЅР° Р±Р°Р·Сѓ РґР°РЅРЅС‹С….</span>
        </div>
        <div class="metric">
          <strong>Ready</strong>
          <span>РњРѕР¶РЅРѕ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РєР°Рє РѕСЃРЅРѕРІСѓ РґР»СЏ РґР°Р»СЊРЅРµР№С€РµРіРѕ С‚РµСЃС‚РёСЂРѕРІР°РЅРёСЏ Bitrix.</span>
        </div>
      </div>
    </section>

    <section class="grid">
      <article class="card accent-card">
        <h2>Р§С‚Рѕ СѓР¶Рµ СЃРґРµР»Р°РЅРѕ</h2>
        <ul>
          <li>РџРѕРґРЅСЏС‚ РѕС‚РґРµР»СЊРЅС‹Р№ РјР°СЂС€СЂСѓС‚ РґР»СЏ СЃС‚СЂР°РЅРёС†С‹ Bitrix РІРЅСѓС‚СЂРё С‚РµРєСѓС‰РµРіРѕ РїСЂРёР»РѕР¶РµРЅРёСЏ.</li>
          <li>РЎС‚СЂР°РЅРёС†Р° РѕС„РѕСЂРјР»РµРЅР° РІ С†РІРµС‚Р°С… СЃСѓС‰РµСЃС‚РІСѓСЋС‰РµРіРѕ РёРЅС‚РµСЂС„РµР№СЃР°, С‡С‚РѕР±С‹ РѕРЅР° РІС‹РіР»СЏРґРµР»Р° С‡Р°СЃС‚СЊСЋ РїСЂРѕРґСѓРєС‚Р°.</li>
          <li>Р”РѕР±Р°РІР»РµРЅР° Р±Р°Р·РѕРІР°СЏ РЅР°РІРёРіР°С†РёСЏ РѕР±СЂР°С‚РЅРѕ РЅР° РіР»Р°РІРЅСѓСЋ Рё РЅР° health-РїСЂРѕРІРµСЂРєСѓ.</li>
        </ul>
      </article>

      <article class="card">
        <h2>Р”Р»СЏ С‡РµРіРѕ РїРѕРґС…РѕРґРёС‚</h2>
        <p>
          Р­С‚Сѓ СЃС‚СЂР°РЅРёС†Сѓ СѓРґРѕР±РЅРѕ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РєР°Рє С‚РµСЃС‚РѕРІСѓСЋ РїР»РѕС‰Р°РґРєСѓ РїРµСЂРµРґ РїРѕРґРєР»СЋС‡РµРЅРёРµРј
          Bitrix24-РІРёРґР¶РµС‚РѕРІ, HTML-РІСЃС‚Р°РІРѕРє, API-РґРёР°РіРЅРѕСЃС‚РёРєРё РёР»Рё РІРЅСѓС‚СЂРµРЅРЅРёС… СЃС†РµРЅР°СЂРёРµРІ
          РёРЅС‚РµРіСЂР°С†РёРё.
        </p>
      </article>

      <article class="card">
        <h2>РЎР»РµРґСѓСЋС‰РёР№ С€Р°Рі</h2>
        <p>
          Р•СЃР»Рё РїРѕРЅР°РґРѕР±РёС‚СЃСЏ, СЃСЋРґР° РјРѕР¶РЅРѕ Р±С‹СЃС‚СЂРѕ РґРѕР±Р°РІРёС‚СЊ С„РѕСЂРјСѓ Р°РІС‚РѕСЂРёР·Р°С†РёРё, webhooks,
          iframe c Bitrix РёР»Рё РґРёР°РіРЅРѕСЃС‚РёС‡РµСЃРєРёРµ Р±Р»РѕРєРё РґР»СЏ РѕР±РјРµРЅР° РґР°РЅРЅС‹РјРё РјРµР¶РґСѓ СЃРёСЃС‚РµРјР°РјРё.
        </p>
      </article>
    </section>
  </div>
</body>
</html>"""


def _normalizePlanningProjectText(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _normalizePlanningProjectDate(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        datetime.strptime(normalized, "%Y-%m-%d")
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Р”Р°С‚Р° РґРѕР»Р¶РЅР° Р±С‹С‚СЊ РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD") from error
    return normalized


def normalizePlanningProjectPayload(payload: PlanningProjectPayload) -> dict[str, object]:
    projectName = str(payload.project_name or "").strip()
    if not projectName:
        raise HTTPException(status_code=400, detail="РќР°Р·РІР°РЅРёРµ РїСЂРѕРµРєС‚Р° РѕР±СЏР·Р°С‚РµР»СЊРЅРѕ")

    return {
        "project_name": projectName,
        "redmine_identifier": _normalizePlanningProjectText(payload.redmine_identifier),
        "pm_name": _normalizePlanningProjectText(payload.pm_name),
        "customer": _normalizePlanningProjectText(payload.customer),
        "start_date": _normalizePlanningProjectDate(payload.start_date),
        "end_date": _normalizePlanningProjectDate(payload.end_date),
        "baseline_estimate_hours": payload.baseline_estimate_hours,
        "p1": payload.p1,
        "p2": payload.p2,
        "estimate_doc_url": _normalizePlanningProjectText(payload.estimate_doc_url),
        "bitrix_url": _normalizePlanningProjectText(payload.bitrix_url),
        "comment_text": _normalizePlanningProjectText(payload.comment_text),
    }


def buildStrangeSnapshotIssuesPage() -> str:
    diagnostics = getLatestSnapshotIssuesWithExternalParents()
    issues = diagnostics.get("issues") or []
    checkedCount = int(diagnostics.get("checked_count") or 0)
    errorCount = int(diagnostics.get("error_count") or 0)

    rowsHtml = ""
    if issues:
        for issue in issues:
            projectName = escape(str(issue.get("project_name") or "вЂ”"))
            projectIdentifier = str(issue.get("project_identifier") or "")
            projectIdentifierHtml = (
                f'<a class="project-link" href="{escape(buildProjectRedmineIssuesUrl(projectIdentifier))}" target="_blank" rel="noreferrer">{escape(projectIdentifier)}</a>'
                if projectIdentifier
                else "вЂ”"
            )
            issueId = int(issue.get("issue_redmine_id") or 0)
            issueUrl = f"{config.redmineUrl.rstrip('/')}/issues/{issueId}"
            parentIssueId = int(issue.get("parent_issue_redmine_id") or 0)
            parentIssueUrl = f"{config.redmineUrl.rstrip('/')}/issues/{parentIssueId}"
            parentProjectIdentifier = str(issue.get("parent_project_identifier") or "")
            parentProjectIdentifierHtml = (
                f'<a class="project-link" href="{escape(buildProjectRedmineIssuesUrl(parentProjectIdentifier))}" target="_blank" rel="noreferrer">{escape(parentProjectIdentifier)}</a>'
                if parentProjectIdentifier
                else "вЂ”"
            )
            rowsHtml += f"""
            <tr>
              <td>{escape(str(issue.get("captured_for_date") or "вЂ”"))}</td>
              <td>{projectName}</td>
              <td>{projectIdentifierHtml}</td>
              <td class="mono"><a class="issue-link" href="{escape(issueUrl)}" target="_blank" rel="noreferrer">{issueId}</a></td>
              <td>{escape(str(issue.get("subject") or "вЂ”"))}</td>
              <td>{escape(str(issue.get("tracker_name") or "вЂ”"))}</td>
              <td>{escape(str(issue.get("status_name") or "вЂ”"))}</td>
              <td class="mono"><a class="issue-link" href="{escape(parentIssueUrl)}" target="_blank" rel="noreferrer">{parentIssueId}</a></td>
              <td>{escape(str(issue.get("parent_issue_subject") or "вЂ”"))}</td>
              <td>{escape(str(issue.get("parent_project_name") or "вЂ”"))}</td>
              <td>{parentProjectIdentifierHtml}</td>
            </tr>
            """
    else:
        rowsHtml = """
            <tr>
              <td colspan="11" class="empty-cell">РџРѕ РїРѕСЃР»РµРґРЅРёРј СЃСЂРµР·Р°Рј С‚Р°РєРёРµ Р·Р°РґР°С‡Рё РЅРµ РЅР°Р№РґРµРЅС‹.</td>
            </tr>
        """

    warningHtml = ""
    if errorCount:
        warningHtml = f'<p class="warning">РќРµ СѓРґР°Р»РѕСЃСЊ РїСЂРѕРІРµСЂРёС‚СЊ {errorCount} СЂРѕРґРёС‚РµР»СЊСЃРєРёС… Р·Р°РґР°С‡ РІ Redmine, РїРѕСЌС‚РѕРјСѓ СЃРїРёСЃРѕРє РјРѕР¶РµС‚ Р±С‹С‚СЊ РЅРµРїРѕР»РЅС‹Рј.</p>'

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>РЎС‚СЂР°РЅРЅС‹Рµ Р·Р°РґР°С‡Рё</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {{
      color-scheme: light;
      --bg: #ffffff;
      --panel: #ffffff;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --blue: #375d77;
      --yellow: #ffc600;
      --cyan: #52cee6;
      --orange: #ff6c0e;
      --shadow-soft: 0 12px 24px rgba(22, 50, 74, 0.06);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    main {{
      max-width: 1600px;
      margin: 0 auto;
      padding: 24px 20px 48px;
    }}
    .topbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 24px;
      margin-bottom: 18px;
    }}
    .brand img {{
      width: 220px;
      height: auto;
      display: block;
    }}
    .actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
      padding: 10px 16px;
      border-radius: 6px;
      text-decoration: none;
      font-weight: 700;
      box-shadow: var(--shadow-soft);
    }}
    .button-home {{ background: var(--blue); color: #fff; }}
    .button-projects {{ background: var(--yellow); color: #16324a; }}
    h1 {{
      margin: 0 0 12px;
      font-size: clamp(1.85rem, 4vw, 2.65rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 400;
    }}
    .meta {{
      margin: 0 0 18px;
      color: var(--muted);
      line-height: 1.6;
    }}
    .warning {{
      margin: 0 0 18px;
      color: #b44d00;
      font-weight: 600;
    }}
    .table-wrap {{
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: auto;
      box-shadow: var(--shadow-soft);
    }}
    table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      background: var(--panel);
    }}
    th, td {{
      text-align: left;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: #eef6f7;
      color: #426179;
      text-transform: uppercase;
      font-size: 0.78rem;
      line-height: 1.15;
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .mono {{
      font-family: Consolas, "Courier New", monospace;
      white-space: nowrap;
    }}
    .issue-link, .project-link {{
      color: var(--blue);
      text-decoration: none;
      border-bottom: 1px dashed currentColor;
      font-weight: 700;
    }}
    .issue-link:hover, .project-link:hover {{
      color: var(--orange);
      border-bottom-style: solid;
    }}
    .empty-cell {{
      color: var(--muted);
      text-align: center;
      padding: 24px 16px;
    }}
  </style>
</head>
<body>
  <main>
    <div class="topbar">
      <a class="brand" href="/"><img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="РЎРњРЎ-РРў"></a>
      <div class="actions">
        <a class="button button-home" href="/">РќР° РіР»Р°РІРЅСѓСЋ</a>
        <a class="button button-projects" href="/#projects-table">РџСЂРѕРµРєС‚С‹</a>
      </div>
    </div>
    <h1>РЎС‚СЂР°РЅРЅС‹Рµ Р·Р°РґР°С‡Рё РїРѕ РїРѕСЃР»РµРґРЅРёРј СЃСЂРµР·Р°Рј</h1>
    <p class="meta">РџРѕРєР°Р·С‹РІР°СЋС‚СЃСЏ Р·Р°РґР°С‡Рё РёР· РїРѕСЃР»РµРґРЅРёС… СЃСЂРµР·РѕРІ РїСЂРѕРµРєС‚РѕРІ, Сѓ РєРѕС‚РѕСЂС‹С… РІ Redmine СЂРѕРґРёС‚РµР»СЊСЃРєР°СЏ Р·Р°РґР°С‡Р° РѕС‚РЅРѕСЃРёС‚СЃСЏ Рє РґСЂСѓРіРѕРјСѓ РїСЂРѕРµРєС‚Сѓ. РџСЂРѕРІРµСЂРµРЅРѕ РєР°РЅРґРёРґР°С‚РѕРІ: {checkedCount}. РќР°Р№РґРµРЅРѕ СЃС‚СЂР°РЅРЅС‹С… Р·Р°РґР°С‡: {len(issues)}.</p>
    {warningHtml}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Р”Р°С‚Р° СЃСЂРµР·Р°</th>
            <th>РџСЂРѕРµРєС‚</th>
            <th>РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ</th>
            <th>ID Р·Р°РґР°С‡Рё</th>
            <th>РўРµРјР° Р·Р°РґР°С‡Рё</th>
            <th>РўСЂРµРєРµСЂ</th>
            <th>РЎС‚Р°С‚СѓСЃ</th>
            <th>ID СЂРѕРґРёС‚РµР»СЏ</th>
            <th>РўРµРјР° СЂРѕРґРёС‚РµР»СЏ</th>
            <th>РџСЂРѕРµРєС‚ СЂРѕРґРёС‚РµР»СЏ</th>
            <th>РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ СЂРѕРґРёС‚РµР»СЏ</th>
          </tr>
        </thead>
        <tbody>
          {rowsHtml}
        </tbody>
      </table>
    </div>
  </main>
</body>
</html>"""


def buildPlanningProjectsPage() -> str:
    return """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>РџР»Р°РЅРёСЂРѕРІР°РЅРёРµ РїСЂРѕРµРєС‚РѕРІ</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      color-scheme: light;
      --bg: #ffffff;
      --panel: #ffffff;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --blue-302: #375d77;
      --yellow-109: #ffc600;
      --cyan-310: #52cee6;
      --orange-1585: #ff6c0e;
      --shadow-soft: 0 12px 24px rgba(22, 50, 74, 0.06);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Golos Text", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    main {
      max-width: 1440px;
      margin: 0 auto;
      padding: 24px 20px 56px;
    }
    .page-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      margin: 0 0 18px;
    }
    .brand {
      display: inline-flex;
      align-items: center;
      text-decoration: none;
    }
    .brand img {
      width: 220px;
      max-width: 100%;
      height: auto;
      display: block;
    }
    .head-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .head-actions a {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
      padding: 10px 14px;
      border-radius: 6px;
      text-decoration: none;
      font-weight: 700;
      box-shadow: var(--shadow-soft);
    }
    .head-actions a.home-link {
      background: var(--yellow-109);
      color: #16324a;
    }
    .head-actions a.redmine-link {
      background: var(--blue-302);
      color: #ffffff;
    }
    h1 {
      margin: 0 0 10px;
      font-size: clamp(2.5rem, 5.6vw, 4.25rem);
      line-height: 1;
      letter-spacing: -0.04em;
      font-weight: 400;
    }
    .lead {
      margin: 0 0 20px;
      color: var(--muted);
      line-height: 1.6;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 20px;
      box-shadow: var(--shadow-soft);
      margin: 0 0 18px;
    }
    .panel h2 {
      margin: 0 0 12px;
      font-size: 1.1rem;
    }
    .form-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(180px, 1fr));
      gap: 14px 16px;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .field-wide { grid-column: span 2; }
    .field label {
      font-weight: 700;
      font-size: 0.95rem;
    }
    .field input,
    .field textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px 12px;
      font: inherit;
      color: var(--text);
      background: #ffffff;
    }
    .field textarea {
      resize: vertical;
      min-height: 88px;
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 18px;
    }
    button {
      border: 0;
      border-radius: 6px;
      padding: 10px 16px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      box-shadow: var(--shadow-soft);
    }
    #savePlanningProjectButton {
      background: var(--blue-302);
      color: #ffffff;
    }
    #resetPlanningProjectFormButton {
      background: #eef2f5;
      color: var(--text);
      border: 1px solid var(--line);
      box-shadow: none;
    }
    .danger-button {
      background: var(--orange-1585);
      color: #ffffff;
    }
    .status {
      min-height: 22px;
      margin-top: 14px;
      color: var(--muted);
    }
    .table-meta {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      flex-wrap: wrap;
      margin: 0 0 12px;
      color: var(--muted);
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    table {
      width: 100%;
      min-width: 1480px;
      border-collapse: collapse;
      background: #ffffff;
    }
    th, td {
      padding: 11px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      background: #eef6f7;
      color: #426179;
      text-transform: uppercase;
      font-size: 0.78rem;
      line-height: 1.2;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    tr:last-child td { border-bottom: 0; }
    .mono { font-family: Consolas, "Courier New", monospace; }
    .link-cell a {
      color: var(--blue-302);
      text-decoration: none;
      border-bottom: 1px dashed currentColor;
    }
    .link-cell a:hover {
      color: var(--orange-1585);
      border-bottom-style: solid;
    }
    .row-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .row-actions button {
      padding: 7px 11px;
      font-size: 0.92rem;
    }
    .row-actions .edit-button {
      background: var(--cyan-310);
      color: #16324a;
    }
    .row-actions .delete-button {
      background: #eef2f5;
      color: #d54343;
      border: 1px solid #f0c8c8;
      box-shadow: none;
    }
    .empty-state {
      padding: 28px 20px;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 1100px) {
      .form-grid { grid-template-columns: repeat(2, minmax(180px, 1fr)); }
    }
    @media (max-width: 700px) {
      .page-head {
        flex-direction: column;
        align-items: flex-start;
      }
      .head-actions {
        justify-content: flex-start;
      }
      .form-grid { grid-template-columns: 1fr; }
      .field-wide { grid-column: span 1; }
    }
  </style>
</head>
<body>
  <main>
    <div class="page-head">
      <a class="brand" href="/" aria-label="РќР° РіР»Р°РІРЅСѓСЋ">
        <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="РЎРњРЎ-РРў">
      </a>
      <div class="head-actions">
        <a class="home-link" href="/">Р“Р»Р°РІРЅР°СЏ</a>
        <a class="redmine-link" href="https://redmine.sms-it.ru" target="_blank" rel="noreferrer">Redmine</a>
      </div>
    </div>

    <h1>РџР»Р°РЅРёСЂРѕРІР°РЅРёРµ РїСЂРѕРµРєС‚РѕРІ</h1>
    <p class="lead">Р—РґРµСЃСЊ РјРѕР¶РЅРѕ РІРµСЃС‚Рё СЂСѓС‡РЅРѕР№ РїР»Р°РЅ РїРѕ РїСЂРѕРµРєС‚Р°Рј: СЃСЂРѕРєРё, РєРѕСЌС„С„РёС†РёРµРЅС‚С‹, СЃСЃС‹Р»РєРё РЅР° РґРѕРєСѓРјРµРЅС‚С‹ Рё Bitrix, Р° С‚Р°РєР¶Рµ РѕС‚РІРµС‚СЃС‚РІРµРЅРЅРѕРіРѕ РџРњ.</p>

    <section class="panel">
      <div class="table-meta">
        <h2 style="margin:0;">РўР°Р±Р»РёС†Р° РїР»Р°РЅРёСЂРѕРІР°РЅРёСЏ</h2>
        <span id="planningProjectsCount">Р—Р°РіСЂСѓР·РєР°...</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>РќР°Р·РІР°РЅРёРµ РїСЂРѕРµРєС‚Р°</th>
              <th>РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ РІ Redmine</th>
              <th>РџРњ</th>
              <th>Р—Р°РєР°Р·С‡РёРє</th>
              <th>Р”Р°С‚Р° СЃС‚Р°СЂС‚Р°</th>
              <th>Р”Р°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ</th>
              <th>Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°</th>
              <th>P1 (С„Р°РєС‚ / Р±Р°Р·Р°), %</th>
              <th>P2 (С„Р°РєС‚ СЃ Р±Р°РіР°РјРё / С„Р°РєС‚), %</th>
              <th>Р”РѕРє СЃ РѕС†РµРЅРєРѕР№</th>
              <th>Bitrix</th>
              <th>РљРѕРјРјРµРЅС‚Р°СЂРёР№</th>
              <th>Р”РµР№СЃС‚РІРёСЏ</th>
            </tr>
          </thead>
          <tbody id="planningProjectsTableBody">
            <tr><td colspan="13" class="empty-state">Р—Р°РіСЂСѓР¶Р°РµРј Р·Р°РїРёСЃРё...</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2 id="planningFormTitle">РќРѕРІР°СЏ Р·Р°РїРёСЃСЊ</h2>
      <form id="planningProjectForm">
        <input type="hidden" id="planningProjectId">
        <div class="form-grid">
          <div class="field field-wide">
            <label for="planningProjectName">РќР°Р·РІР°РЅРёРµ РїСЂРѕРµРєС‚Р°</label>
            <input id="planningProjectName" type="text" required>
          </div>
          <div class="field">
            <label for="planningProjectIdentifier">РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ РІ Redmine</label>
            <input id="planningProjectIdentifier" type="text">
          </div>
          <div class="field">
            <label for="planningProjectPm">РџРњ</label>
            <input id="planningProjectPm" type="text">
          </div>
          <div class="field">
            <label for="planningProjectCustomer">Р—Р°РєР°Р·С‡РёРє</label>
            <input id="planningProjectCustomer" type="text">
          </div>
          <div class="field">
            <label for="planningProjectStartDate">Р”Р°С‚Р° СЃС‚Р°СЂС‚Р°</label>
            <input id="planningProjectStartDate" type="date">
          </div>
          <div class="field">
            <label for="planningProjectEndDate">Р”Р°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ</label>
            <input id="planningProjectEndDate" type="date">
          </div>
          <div class="field">
            <label for="planningProjectBaselineEstimate">Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°</label>
            <input id="planningProjectBaselineEstimate" type="number" step="0.1" inputmode="decimal">
          </div>
          <div class="field">
            <label for="planningProjectP1">P1 (С„Р°РєС‚ / Р±Р°Р·Р°), %</label>
            <input id="planningProjectP1" type="number" step="0.1" inputmode="decimal">
          </div>
          <div class="field">
            <label for="planningProjectP2">P2 (С„Р°РєС‚ СЃ Р±Р°РіР°РјРё / С„Р°РєС‚), %</label>
            <input id="planningProjectP2" type="number" step="0.1" inputmode="decimal">
          </div>
          <div class="field field-wide">
            <label for="planningProjectEstimateDoc">Р”РѕРє СЃ РѕС†РµРЅРєРѕР№</label>
            <input id="planningProjectEstimateDoc" type="url" placeholder="https://">
          </div>
          <div class="field field-wide">
            <label for="planningProjectBitrix">Bitrix</label>
            <input id="planningProjectBitrix" type="url" placeholder="https://">
          </div>
          <div class="field field-wide">
            <label for="planningProjectComment">РљРѕРјРјРµРЅС‚Р°СЂРёР№</label>
            <textarea id="planningProjectComment"></textarea>
          </div>
        </div>
        <div class="actions">
          <button type="submit" id="savePlanningProjectButton">РЎРѕС…СЂР°РЅРёС‚СЊ</button>
          <button type="button" id="resetPlanningProjectFormButton">РћС‡РёСЃС‚РёС‚СЊ С„РѕСЂРјСѓ</button>
        </div>
      </form>
      <div class="status" id="planningProjectsStatus"></div>
    </section>
  </main>

  <script>
    const planningProjectsTableBody = document.getElementById("planningProjectsTableBody");
    const planningProjectsCount = document.getElementById("planningProjectsCount");
    const planningProjectsStatus = document.getElementById("planningProjectsStatus");
    const planningProjectForm = document.getElementById("planningProjectForm");
    const planningFormTitle = document.getElementById("planningFormTitle");
    const planningProjectId = document.getElementById("planningProjectId");
    const planningProjectName = document.getElementById("planningProjectName");
    const planningProjectIdentifier = document.getElementById("planningProjectIdentifier");
    const planningProjectPm = document.getElementById("planningProjectPm");
    const planningProjectCustomer = document.getElementById("planningProjectCustomer");
    const planningProjectStartDate = document.getElementById("planningProjectStartDate");
    const planningProjectEndDate = document.getElementById("planningProjectEndDate");
    const planningProjectBaselineEstimate = document.getElementById("planningProjectBaselineEstimate");
    const planningProjectP1 = document.getElementById("planningProjectP1");
    const planningProjectP2 = document.getElementById("planningProjectP2");
    const planningProjectEstimateDoc = document.getElementById("planningProjectEstimateDoc");
    const planningProjectBitrix = document.getElementById("planningProjectBitrix");
    const planningProjectComment = document.getElementById("planningProjectComment");
    const resetPlanningProjectFormButton = document.getElementById("resetPlanningProjectFormButton");
    const planningProjectFormSection = planningProjectForm ? planningProjectForm.closest(".panel") : null;

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, (char) => {
        if (char === "&") return "&amp;";
        if (char === "<") return "&lt;";
        if (char === ">") return "&gt;";
        if (char === '"') return "&quot;";
        return "&#39;";
      });
    }

    function formatOptionalDate(value) {
      return value ? String(value) : "вЂ”";
    }

    function formatOptionalNumber(value) {
      if (value === null || value === undefined || value === "") {
        return "вЂ”";
      }
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) {
        return "вЂ”";
      }
      return parsed.toLocaleString("ru-RU", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
    }

    function buildOptionalLink(url) {
      if (!url) {
        return "вЂ”";
      }
      const safeUrl = escapeHtml(url);
      return `<a href="${safeUrl}" target="_blank" rel="noreferrer">${safeUrl}</a>`;
    }

    function setPlanningProjectsStatus(message) {
      if (planningProjectsStatus) {
        planningProjectsStatus.textContent = message || "";
      }
    }

    function getPlanningProjectsQueryState() {
      const params = new URLSearchParams(window.location.search);
      const redmineIdentifier = String(params.get("redmine_identifier") || "").trim();
      const projectName = String(params.get("project_name") || "").trim();
      return {
        redmineIdentifier,
        projectName,
      };
    }

    function resetPlanningProjectForm() {
      planningProjectId.value = "";
      planningProjectForm.reset();
      planningFormTitle.textContent = "РќРѕРІР°СЏ Р·Р°РїРёСЃСЊ";
      setPlanningProjectsStatus("");
    }

    function scrollPlanningProjectFormIntoView() {
      if (planningProjectFormSection instanceof HTMLElement) {
        planningProjectFormSection.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }

    function fillPlanningProjectForm(project) {
      planningProjectId.value = project.id ?? "";
      planningProjectName.value = project.project_name ?? "";
      planningProjectIdentifier.value = project.redmine_identifier ?? "";
      planningProjectPm.value = project.pm_name ?? "";
      planningProjectCustomer.value = project.customer ?? "";
      planningProjectStartDate.value = project.start_date ?? "";
      planningProjectEndDate.value = project.end_date ?? "";
      planningProjectBaselineEstimate.value = project.baseline_estimate_hours ?? "";
      planningProjectP1.value = project.p1 ?? "";
      planningProjectP2.value = project.p2 ?? "";
      planningProjectEstimateDoc.value = project.estimate_doc_url ?? "";
      planningProjectBitrix.value = project.bitrix_url ?? "";
      planningProjectComment.value = project.comment_text ?? "";
      planningFormTitle.textContent = "Р РµРґР°РєС‚РёСЂРѕРІР°РЅРёРµ Р·Р°РїРёСЃРё";
      setPlanningProjectsStatus("Р—Р°РїРёСЃСЊ Р·Р°РіСЂСѓР¶РµРЅР° РІ С„РѕСЂРјСѓ РґР»СЏ СЂРµРґР°РєС‚РёСЂРѕРІР°РЅРёСЏ.");
      scrollPlanningProjectFormIntoView();
    }

    function applyPlanningProjectPrefill(projects) {
      const queryState = getPlanningProjectsQueryState();
      if (!queryState.redmineIdentifier) {
        return;
      }

      const matchedProject = projects.find((project) => String(project?.redmine_identifier ?? "").trim() === queryState.redmineIdentifier);
      if (matchedProject) {
        fillPlanningProjectForm(matchedProject);
        setPlanningProjectsStatus(`РћС‚РєСЂС‹С‚Рѕ СЂРµРґР°РєС‚РёСЂРѕРІР°РЅРёРµ Р·Р°РїРёСЃРё РґР»СЏ РїСЂРѕРµРєС‚Р° СЃ РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂРѕРј ${queryState.redmineIdentifier}.`);
        return;
      }

      resetPlanningProjectForm();
      planningProjectIdentifier.value = queryState.redmineIdentifier;
      if (queryState.projectName) {
        planningProjectName.value = queryState.projectName;
      }
      planningFormTitle.textContent = "РќРѕРІР°СЏ Р·Р°РїРёСЃСЊ";
      setPlanningProjectsStatus(`Р—Р°РїРёСЃСЊ РЅРµ РЅР°Р№РґРµРЅР°. РџРѕРґРіРѕС‚РѕРІР»РµРЅР° РЅРѕРІР°СЏ С„РѕСЂРјР° РґР»СЏ РїСЂРѕРµРєС‚Р° СЃ РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂРѕРј ${queryState.redmineIdentifier}.`);
      scrollPlanningProjectFormIntoView();
    }

    function renderPlanningProjects(projects) {
      planningProjectsCount.textContent = `Р’СЃРµРіРѕ Р·Р°РїРёСЃРµР№: ${projects.length}`;
      if (!projects.length) {
        planningProjectsTableBody.innerHTML = '<tr><td colspan="13" class="empty-state">РџРѕРєР° РЅРµС‚ РЅРё РѕРґРЅРѕР№ Р·Р°РїРёСЃРё.</td></tr>';
        return;
      }

      planningProjectsTableBody.innerHTML = projects.map((project) => `
        <tr>
          <td>${escapeHtml(project.project_name ?? "вЂ”")}</td>
          <td class="mono">${escapeHtml(project.redmine_identifier ?? "вЂ”")}</td>
          <td>${escapeHtml(project.pm_name ?? "вЂ”")}</td>
          <td>${escapeHtml(project.customer ?? "вЂ”")}</td>
          <td>${formatOptionalDate(project.start_date)}</td>
          <td>${formatOptionalDate(project.end_date)}</td>
          <td>${formatOptionalNumber(project.baseline_estimate_hours)}</td>
          <td>${formatOptionalNumber(project.p1)}</td>
          <td>${formatOptionalNumber(project.p2)}</td>
          <td class="link-cell">${buildOptionalLink(project.estimate_doc_url)}</td>
          <td class="link-cell">${buildOptionalLink(project.bitrix_url)}</td>
          <td>${escapeHtml(project.comment_text ?? "вЂ”")}</td>
          <td>
            <div class="row-actions">
              <button type="button" class="edit-button" data-action="edit" data-id="${project.id}">Р РµРґР°РєС‚РёСЂРѕРІР°С‚СЊ</button>
              <button type="button" class="delete-button" data-action="delete" data-id="${project.id}">РЈРґР°Р»РёС‚СЊ</button>
            </div>
          </td>
        </tr>
      `).join("");
    }

    async function loadPlanningProjects() {
      planningProjectsTableBody.innerHTML = '<tr><td colspan="13" class="empty-state">Р—Р°РіСЂСѓР¶Р°РµРј Р·Р°РїРёСЃРё...</td></tr>';
      const response = await fetch("/api/planning-projects");
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РїР»Р°РЅРёСЂРѕРІР°РЅРёРµ РїСЂРѕРµРєС‚РѕРІ.");
      }
      renderPlanningProjects(payload.projects || []);
      applyPlanningProjectPrefill(payload.projects || []);
      return payload.projects || [];
    }

    function collectPlanningProjectPayload() {
      return {
        project_name: planningProjectName.value.trim(),
        redmine_identifier: planningProjectIdentifier.value.trim(),
        pm_name: planningProjectPm.value.trim(),
        customer: planningProjectCustomer.value.trim(),
        start_date: planningProjectStartDate.value || null,
        end_date: planningProjectEndDate.value || null,
        baseline_estimate_hours: planningProjectBaselineEstimate.value === "" ? null : Number(planningProjectBaselineEstimate.value),
        p1: planningProjectP1.value === "" ? null : Number(planningProjectP1.value),
        p2: planningProjectP2.value === "" ? null : Number(planningProjectP2.value),
        estimate_doc_url: planningProjectEstimateDoc.value.trim(),
        bitrix_url: planningProjectBitrix.value.trim(),
        comment_text: planningProjectComment.value.trim(),
      };
    }

    planningProjectForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const projectId = planningProjectId.value.trim();
      const method = projectId ? "PUT" : "POST";
      const url = projectId ? `/api/planning-projects/${encodeURIComponent(projectId)}` : "/api/planning-projects";
      setPlanningProjectsStatus(projectId ? "РЎРѕС…СЂР°РЅСЏРµРј РёР·РјРµРЅРµРЅРёСЏ..." : "РЎРѕР·РґР°РµРј Р·Р°РїРёСЃСЊ...");

      try {
        const response = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(collectPlanningProjectPayload()),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕС…СЂР°РЅРёС‚СЊ Р·Р°РїРёСЃСЊ.");
        }
        await loadPlanningProjects();
        resetPlanningProjectForm();
        setPlanningProjectsStatus(projectId ? "РР·РјРµРЅРµРЅРёСЏ СЃРѕС…СЂР°РЅРµРЅС‹." : "Р—Р°РїРёСЃСЊ СЃРѕР·РґР°РЅР°.");
      } catch (error) {
        setPlanningProjectsStatus(error instanceof Error ? error.message : "РћС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ.");
      }
    });

    resetPlanningProjectFormButton.addEventListener("click", () => {
      resetPlanningProjectForm();
    });

    planningProjectsTableBody.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const action = target.dataset.action;
      const projectId = target.dataset.id;
      if (!action || !projectId) {
        return;
      }

      try {
        const response = await fetch("/api/planning-projects");
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ Р·Р°РїРёСЃРё.");
        }
        const projects = payload.projects || [];
        const currentProject = projects.find((item) => String(item.id) === String(projectId));
        if (!currentProject) {
          throw new Error("Р—Р°РїРёСЃСЊ РЅРµ РЅР°Р№РґРµРЅР°.");
        }

        if (action === "edit") {
          fillPlanningProjectForm(currentProject);
          return;
        }

        if (action === "delete") {
          if (!window.confirm(`РЈРґР°Р»РёС‚СЊ Р·Р°РїРёСЃСЊ РїРѕ РїСЂРѕРµРєС‚Сѓ "${currentProject.project_name}"?`)) {
            return;
          }
          setPlanningProjectsStatus("РЈРґР°Р»СЏРµРј Р·Р°РїРёСЃСЊ...");
          const deleteResponse = await fetch(`/api/planning-projects/${encodeURIComponent(projectId)}`, { method: "DELETE" });
          const deletePayload = await deleteResponse.json();
          if (!deleteResponse.ok) {
            throw new Error(deletePayload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ СѓРґР°Р»РёС‚СЊ Р·Р°РїРёСЃСЊ.");
          }
          await loadPlanningProjects();
          if (planningProjectId.value === String(projectId)) {
            resetPlanningProjectForm();
          }
          setPlanningProjectsStatus("Р—Р°РїРёСЃСЊ СѓРґР°Р»РµРЅР°.");
        }
      } catch (error) {
        setPlanningProjectsStatus(error instanceof Error ? error.message : "РћС€РёР±РєР° РѕР±СЂР°Р±РѕС‚РєРё Р·Р°РїРёСЃРё.");
      }
    });

    loadPlanningProjects().catch((error) => {
      planningProjectsCount.textContent = "РћС€РёР±РєР°";
      planningProjectsTableBody.innerHTML = '<tr><td colspan="13" class="empty-state">РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ Р·Р°РїРёСЃРё.</td></tr>';
      setPlanningProjectsStatus(error instanceof Error ? error.message : "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РїР»Р°РЅРёСЂРѕРІР°РЅРёРµ РїСЂРѕРµРєС‚РѕРІ.");
    });
  </script>
</body>
</html>"""


def buildLoginPage(nextPath: str = "/") -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Р’С…РѕРґ РІ СЃРёСЃС‚РµРјСѓ</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    :root {{
      --bg: #f6f9fb;
      --panel: #ffffff;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --blue-302: #375d77;
      --orange-1585: #ff6c0e;
      --shadow: 0 18px 40px rgba(22, 50, 74, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
      font-family: "Golos", "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #f6f9fb 0%, #eef6f7 100%);
      color: var(--text);
    }}
    .card {{
      width: min(460px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: var(--shadow);
      padding: 28px 28px 24px;
    }}
    .brand {{
      display: inline-flex;
      align-items: center;
      margin-bottom: 18px;
    }}
    .brand img {{
      width: 180px;
      height: auto;
      display: block;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(1.8rem, 4vw, 2.4rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 400;
    }}
    .lead {{
      margin: 0 0 22px;
      color: var(--muted);
      line-height: 1.5;
    }}
    form {{
      display: grid;
      gap: 16px;
    }}
    label {{
      display: grid;
      gap: 7px;
      font-size: 0.96rem;
      font-weight: 600;
    }}
    input {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 12px 14px;
      font: inherit;
      color: var(--text);
      background: #ffffff;
    }}
    button {{
      border: 0;
      border-radius: 10px;
      padding: 13px 18px;
      background: var(--orange-1585);
      color: #ffffff;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }}
    button:disabled {{
      opacity: 0.7;
      cursor: wait;
    }}
    .status {{
      min-height: 22px;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .status.error {{ color: #d54343; }}
  </style>
</head>
<body>
  <section class="card">
    <a class="brand" href="/" aria-label="РЎРњРЎ-РРў">
      <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="РЎРњРЎ-РРў">
    </a>
    <h1>Р’С…РѕРґ РІ СЃРёСЃС‚РµРјСѓ</h1>
    <p class="lead">Р’РІРµРґРёС‚Рµ Р»РѕРіРёРЅ Рё РїР°СЂРѕР»СЊ, С‡С‚РѕР±С‹ РѕС‚РєСЂС‹С‚СЊ СЃРёСЃС‚РµРјСѓ Р°РЅР°Р»РёР·Р° РїСЂРѕРµРєС‚РѕРІ Redmine.</p>
    <form id="loginForm">
      <input id="nextPathInput" type="hidden" value="{safeNextPath}">
      <label for="loginInput">
        Р›РѕРіРёРЅ
        <input id="loginInput" type="text" autocomplete="username" required>
      </label>
      <label for="passwordInput">
        РџР°СЂРѕР»СЊ
        <input id="passwordInput" type="password" autocomplete="current-password" required>
      </label>
      <button id="loginButton" type="submit">Р’РѕР№С‚Рё</button>
    </form>
    <div class="status" id="loginStatus"></div>
  </section>

  <script>
    const loginForm = document.getElementById("loginForm");
    const loginButton = document.getElementById("loginButton");
    const loginStatus = document.getElementById("loginStatus");
    const nextPathInput = document.getElementById("nextPathInput");

    function setStatus(message, kind = "") {{
      loginStatus.textContent = message;
      loginStatus.className = "status" + (kind ? " " + kind : "");
    }}

    loginForm.addEventListener("submit", async (event) => {{
      event.preventDefault();
      loginButton.disabled = true;
      setStatus("РџСЂРѕРІРµСЂСЏРµРј Р»РѕРіРёРЅ Рё РїР°СЂРѕР»СЊ...");

      try {{
        const response = await fetch(`/api/auth/login?next=${{encodeURIComponent(nextPathInput.value || "/")}}`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            login: document.getElementById("loginInput").value,
            password: document.getElementById("passwordInput").value,
          }}),
        }});
        const payload = await response.json();
        if (!response.ok) {{
          throw new Error(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ РІРѕР№С‚Рё РІ СЃРёСЃС‚РµРјСѓ.");
        }}

        const nextPath = payload.next_path || nextPathInput.value || "/";
        window.location.href = payload.must_change_password ? "/change-password" : nextPath;
      }} catch (error) {{
        setStatus(error.message, "error");
        loginButton.disabled = false;
      }}
    }});
  </script>
</body>
</html>"""


def buildChangePasswordPage(user: dict[str, object]) -> str:
    login = escape(str(user.get("login") or ""))
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>РЎРјРµРЅР° РїР°СЂРѕР»СЏ</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    :root {{
      --bg: #f6f9fb;
      --panel: #ffffff;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --orange-1585: #ff6c0e;
      --shadow: 0 18px 40px rgba(22, 50, 74, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
      font-family: "Golos", "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #f6f9fb 0%, #eef6f7 100%);
      color: var(--text);
    }}
    .card {{
      width: min(500px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: var(--shadow);
      padding: 28px;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(1.8rem, 4vw, 2.3rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 400;
    }}
    .lead {{
      margin: 0 0 22px;
      color: var(--muted);
      line-height: 1.5;
    }}
    .login-note {{
      margin: 0 0 16px;
      color: var(--blue-302, #375d77);
      font-weight: 600;
    }}
    form {{
      display: grid;
      gap: 16px;
    }}
    label {{
      display: grid;
      gap: 7px;
      font-size: 0.96rem;
      font-weight: 600;
    }}
    input {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 12px 14px;
      font: inherit;
      color: var(--text);
      background: #ffffff;
    }}
    button {{
      border: 0;
      border-radius: 10px;
      padding: 13px 18px;
      background: var(--orange-1585);
      color: #ffffff;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }}
    button:disabled {{
      opacity: 0.7;
      cursor: wait;
    }}
    .status {{
      min-height: 22px;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .status.error {{ color: #d54343; }}
  </style>
</head>
<body>
  <section class="card">
    <h1>РЎРјРµРЅР° РїР°СЂРѕР»СЏ</h1>
    <p class="lead">Р”Р»СЏ СЌС‚РѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РІРєР»СЋС‡РµРЅР° РѕР±СЏР·Р°С‚РµР»СЊРЅР°СЏ СЃРјРµРЅР° РїР°СЂРѕР»СЏ. РЎРѕС…СЂР°РЅРёРј РЅРѕРІС‹Р№ РїР°СЂРѕР»СЊ Рё РїРѕСЃР»Рµ СЌС‚РѕРіРѕ РѕС‚РєСЂРѕРµРј СЃРёСЃС‚РµРјСѓ.</p>
    <p class="login-note">Р›РѕРіРёРЅ: {login}</p>
    <form id="changePasswordForm">
      <label for="newPasswordInput">
        РќРѕРІС‹Р№ РїР°СЂРѕР»СЊ
        <input id="newPasswordInput" type="password" autocomplete="new-password" minlength="3" required>
      </label>
      <label for="repeatPasswordInput">
        РџРѕРІС‚РѕСЂРёС‚Рµ РЅРѕРІС‹Р№ РїР°СЂРѕР»СЊ
        <input id="repeatPasswordInput" type="password" autocomplete="new-password" minlength="3" required>
      </label>
      <button id="changePasswordButton" type="submit">РЎРѕС…СЂР°РЅРёС‚СЊ РЅРѕРІС‹Р№ РїР°СЂРѕР»СЊ</button>
    </form>
    <div class="status" id="changePasswordStatus"></div>
  </section>

  <script>
    const changePasswordForm = document.getElementById("changePasswordForm");
    const changePasswordButton = document.getElementById("changePasswordButton");
    const changePasswordStatus = document.getElementById("changePasswordStatus");

    function setStatus(message, kind = "") {{
      changePasswordStatus.textContent = message;
      changePasswordStatus.className = "status" + (kind ? " " + kind : "");
    }}

    changePasswordForm.addEventListener("submit", async (event) => {{
      event.preventDefault();
      const newPassword = document.getElementById("newPasswordInput").value;
      const repeatPassword = document.getElementById("repeatPasswordInput").value;
      if (newPassword !== repeatPassword) {{
        setStatus("РќРѕРІС‹Р№ РїР°СЂРѕР»СЊ Рё РїРѕРІС‚РѕСЂ РґРѕР»Р¶РЅС‹ СЃРѕРІРїР°РґР°С‚СЊ.", "error");
        return;
      }}

      changePasswordButton.disabled = true;
      setStatus("РЎРѕС…СЂР°РЅСЏРµРј РЅРѕРІС‹Р№ РїР°СЂРѕР»СЊ...");

      try {{
        const response = await fetch("/api/auth/change-password", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ new_password: newPassword }}),
        }});
        const payload = await response.json();
        if (!response.ok) {{
          throw new Error(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕС…СЂР°РЅРёС‚СЊ РїР°СЂРѕР»СЊ.");
        }}

        window.location.href = payload.next_path || "/";
      }} catch (error) {{
        setStatus(error.message, "error");
        changePasswordButton.disabled = false;
      }}
    }});
  </script>
</body>
</html>"""


def buildAdminUsersPage() -> str:
    return """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>РђРґРјРёРЅРёСЃС‚СЂРёСЂРѕРІР°РЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    :root {
      --bg: #ffffff;
      --panel: #ffffff;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --blue-302: #375d77;
      --cyan-310: #52cee6;
      --orange-1585: #ff6c0e;
      --shadow: 0 18px 40px rgba(22, 50, 74, 0.08);
      --shadow-soft: 0 12px 24px rgba(22, 50, 74, 0.06);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Golos", "Segoe UI", sans-serif;
      background: #ffffff;
      color: var(--text);
    }
    main {
      max-width: 1240px;
      margin: 0 auto;
      padding: 24px 20px 56px;
    }
    .page-head {
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: center;
      margin-bottom: 24px;
    }
    .brand img {
      width: 200px;
      height: auto;
      display: block;
    }
    .head-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .head-actions a {
      padding: 10px 14px;
      border-radius: 999px;
      text-decoration: none;
      font-weight: 600;
      color: var(--text);
      background: #eef2f5;
      border: 1px solid var(--line);
    }
    h1 {
      margin: 0 0 10px;
      font-size: clamp(1.85rem, 4.2vw, 2.75rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 400;
    }
    .lead {
      margin: 0 0 24px;
      color: var(--muted);
      line-height: 1.5;
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(340px, 420px);
      gap: 22px;
      align-items: start;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: var(--shadow-soft);
      padding: 20px;
    }
    .panel h2 {
      margin: 0 0 14px;
      font-size: 1.25rem;
      line-height: 1.1;
    }
    .status {
      min-height: 22px;
      margin-top: 14px;
      color: var(--muted);
      font-size: 0.95rem;
    }
    .status.error { color: #d54343; }
    .status.success { color: #2f8a57; }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 12px;
    }
    table {
      width: 100%;
      min-width: 720px;
      border-collapse: collapse;
      background: #ffffff;
    }
    th, td {
      padding: 11px 12px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      text-align: left;
    }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: #eef6f7;
      color: #426179;
      text-transform: uppercase;
      font-size: 0.78rem;
      line-height: 1.2;
    }
    tr:last-child td { border-bottom: 0; }
    .row-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .row-actions button {
      border: 0;
      border-radius: 8px;
      padding: 8px 12px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }
    .edit-button {
      background: var(--cyan-310);
      color: #16324a;
    }
    .delete-button {
      background: #eef2f5;
      color: #d54343;
      border: 1px solid #f0c8c8;
    }
    form {
      display: grid;
      gap: 14px;
    }
    .field {
      display: grid;
      gap: 7px;
    }
    .field label {
      font-weight: 600;
    }
    .field input,
    .field select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 11px 12px;
      font: inherit;
      color: var(--text);
      background: #ffffff;
    }
    .field select[multiple] {
      min-height: 112px;
    }
    .checkbox-field {
      display: flex;
      align-items: center;
      gap: 10px;
      font-weight: 600;
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .actions button {
      border: 0;
      border-radius: 10px;
      padding: 11px 16px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      box-shadow: var(--shadow-soft);
    }
    #saveUserButton {
      background: var(--orange-1585);
      color: #ffffff;
    }
    #resetUserFormButton {
      background: #eef2f5;
      color: var(--text);
      border: 1px solid var(--line);
      box-shadow: none;
    }
    .tag-list {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
    }
    .tag {
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      background: #eef6f7;
      color: #375d77;
      font-size: 0.86rem;
      font-weight: 600;
    }
    .tag.must-change {
      background: #fff3ea;
      color: #ff6c0e;
    }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <div class="page-head">
      <a class="brand" href="/" aria-label="РЎРњРЎ-РРў">
        <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="РЎРњРЎ-РРў">
      </a>
      <div class="head-actions">
        <a href="/">Р“Р»Р°РІРЅР°СЏ</a>
        <a href="/logout">Р’С‹Р№С‚Рё</a>
      </div>
    </div>

    <h1>РђРґРјРёРЅРёСЃС‚СЂРёСЂРѕРІР°РЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№</h1>
    <p class="lead">Р—РґРµСЃСЊ РјРѕР¶РЅРѕ Р·Р°РІРѕРґРёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№, Р·Р°РґР°РІР°С‚СЊ РёРј РїСЂР°РІР° РґРѕСЃС‚СѓРїР° Рё РІРєР»СЋС‡Р°С‚СЊ РѕР±СЏР·Р°С‚РµР»СЊРЅСѓСЋ СЃРјРµРЅСѓ РїР°СЂРѕР»СЏ РїСЂРё СЃР»РµРґСѓСЋС‰РµРј РІС…РѕРґРµ.</p>

    <section class="grid">
      <article class="panel">
        <h2>РџРѕР»СЊР·РѕРІР°С‚РµР»Рё</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Р›РѕРіРёРЅ</th>
                <th>РџСЂР°РІР°</th>
                <th>РЎРјРµРЅР° РїР°СЂРѕР»СЏ</th>
                <th>РћР±РЅРѕРІР»РµРЅ</th>
                <th>Р”РµР№СЃС‚РІРёСЏ</th>
              </tr>
            </thead>
            <tbody id="usersTableBody">
              <tr><td colspan="5" style="text-align:center; color:#64798d;">Р—Р°РіСЂСѓР¶Р°РµРј РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№...</td></tr>
            </tbody>
          </table>
        </div>
        <div class="status" id="usersStatus"></div>
      </article>

      <article class="panel">
        <h2 id="userFormTitle">РќРѕРІС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ</h2>
        <form id="userForm">
          <input type="hidden" id="userIdInput">
          <div class="field">
            <label for="userLoginInput">Р›РѕРіРёРЅ</label>
            <input id="userLoginInput" type="text" autocomplete="username" required>
          </div>
          <div class="field">
            <label for="userPasswordInput">РџР°СЂРѕР»СЊ</label>
            <input id="userPasswordInput" type="password" autocomplete="new-password" placeholder="Р”Р»СЏ РЅРѕРІРѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РѕР±СЏР·Р°С‚РµР»РµРЅ">
          </div>
          <div class="field">
            <label for="userRolesSelect">РџСЂР°РІР°</label>
            <select id="userRolesSelect" multiple size="3">
              <option value="User">User</option>
              <option value="Finance">Finance</option>
              <option value="Admin">Admin</option>
            </select>
          </div>
          <label class="checkbox-field" for="userMustChangePasswordInput">
            <input id="userMustChangePasswordInput" type="checkbox">
            <span>РЎРјРµРЅРёС‚СЊ РїР°СЂРѕР»СЊ</span>
          </label>
          <div class="actions">
            <button id="saveUserButton" type="submit">РЎРѕС…СЂР°РЅРёС‚СЊ</button>
            <button id="resetUserFormButton" type="button">РћС‡РёСЃС‚РёС‚СЊ С„РѕСЂРјСѓ</button>
          </div>
        </form>
      </article>
    </section>
  </main>

  <script>
    const usersTableBody = document.getElementById("usersTableBody");
    const usersStatus = document.getElementById("usersStatus");
    const userForm = document.getElementById("userForm");
    const userFormTitle = document.getElementById("userFormTitle");
    const userIdInput = document.getElementById("userIdInput");
    const userLoginInput = document.getElementById("userLoginInput");
    const userPasswordInput = document.getElementById("userPasswordInput");
    const userRolesSelect = document.getElementById("userRolesSelect");
    const userMustChangePasswordInput = document.getElementById("userMustChangePasswordInput");
    const saveUserButton = document.getElementById("saveUserButton");
    const resetUserFormButton = document.getElementById("resetUserFormButton");
    let allUsers = [];

    function setStatus(message, kind = "") {
      usersStatus.textContent = message;
      usersStatus.className = "status" + (kind ? " " + kind : "");
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function formatDateTime(value) {
      if (!value) {
        return "вЂ”";
      }
      return String(value).replace("T", " ").replace("+00:00", " UTC");
    }

    function getSelectedRoles() {
      return Array.from(userRolesSelect.selectedOptions).map((option) => option.value);
    }

    function resetUserForm() {
      userIdInput.value = "";
      userFormTitle.textContent = "РќРѕРІС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ";
      userLoginInput.value = "";
      userPasswordInput.value = "";
      userMustChangePasswordInput.checked = false;
      Array.from(userRolesSelect.options).forEach((option) => {
        option.selected = option.value === "User";
      });
    }

    function editUser(userId) {
      const user = allUsers.find((item) => Number(item.id) === Number(userId));
      if (!user) {
        return;
      }

      userIdInput.value = String(user.id);
      userFormTitle.textContent = `Р РµРґР°РєС‚РёСЂРѕРІР°РЅРёРµ: ${user.login}`;
      userLoginInput.value = user.login || "";
      userPasswordInput.value = "";
      userMustChangePasswordInput.checked = Boolean(user.must_change_password);
      const roles = Array.isArray(user.roles) ? user.roles : [];
      Array.from(userRolesSelect.options).forEach((option) => {
        option.selected = roles.includes(option.value);
      });
      userForm.scrollIntoView({ behavior: "smooth", block: "start" });
    }

    function renderUsers(users) {
      allUsers = Array.isArray(users) ? users : [];
      if (!allUsers.length) {
        usersTableBody.innerHTML = '<tr><td colspan="5" style="text-align:center; color:#64798d;">РџРѕРєР° РЅРµС‚ РЅРё РѕРґРЅРѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.</td></tr>';
        return;
      }

      usersTableBody.innerHTML = "";
      for (const user of allUsers) {
        const roles = Array.isArray(user.roles) ? user.roles : [];
        const row = document.createElement("tr");
        row.innerHTML = `
          <td>${escapeHtml(user.login || "вЂ”")}</td>
          <td><div class="tag-list">${roles.map((role) => `<span class="tag">${escapeHtml(role)}</span>`).join("")}</div></td>
          <td>${user.must_change_password ? '<span class="tag must-change">Р”Р°</span>' : "РќРµС‚"}</td>
          <td>${escapeHtml(formatDateTime(user.updated_at))}</td>
          <td>
            <div class="row-actions">
              <button type="button" class="edit-button" data-user-id="${user.id}">Р РµРґР°РєС‚РёСЂРѕРІР°С‚СЊ</button>
              <button type="button" class="delete-button" data-user-id="${user.id}">РЈРґР°Р»РёС‚СЊ</button>
            </div>
          </td>
        `;
        usersTableBody.appendChild(row);
      }
    }

    async function loadUsers() {
      try {
        const response = await fetch("/api/admin/users");
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№.");
        }

        renderUsers(payload.users || []);
        setStatus("");
      } catch (error) {
        renderUsers([]);
        setStatus(error.message, "error");
      }
    }

    async function saveUser(event) {
      event.preventDefault();
      saveUserButton.disabled = true;
      setStatus("РЎРѕС…СЂР°РЅСЏРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ...");

      try {
        const userId = String(userIdInput.value || "").trim();
        const payload = {
          login: userLoginInput.value,
          password: userPasswordInput.value || null,
          roles: getSelectedRoles(),
          must_change_password: userMustChangePasswordInput.checked,
        };

        const response = await fetch(userId ? `/api/admin/users/${encodeURIComponent(userId)}` : "/api/admin/users", {
          method: userId ? "PUT" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const responsePayload = await response.json();
        if (!response.ok) {
          throw new Error(responsePayload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕС…СЂР°РЅРёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.");
        }

        renderUsers(responsePayload.users || []);
        resetUserForm();
        setStatus("РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ СЃРѕС…СЂР°РЅРµРЅ.", "success");
      } catch (error) {
        setStatus(error.message, "error");
      } finally {
        saveUserButton.disabled = false;
      }
    }

    async function deleteUser(userId) {
      if (!window.confirm("РЈРґР°Р»РёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ?")) {
        return;
      }

      setStatus("РЈРґР°Р»СЏРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ...");
      try {
        const response = await fetch(`/api/admin/users/${encodeURIComponent(userId)}`, { method: "DELETE" });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "РќРµ СѓРґР°Р»РѕСЃСЊ СѓРґР°Р»РёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.");
        }

        renderUsers(payload.users || []);
        if (String(userIdInput.value || "") === String(userId)) {
          resetUserForm();
        }
        setStatus("РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ СѓРґР°Р»РµРЅ.", "success");
      } catch (error) {
        setStatus(error.message, "error");
      }
    }

    userForm.addEventListener("submit", saveUser);
    resetUserFormButton.addEventListener("click", resetUserForm);
    usersTableBody.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const userId = target.dataset.userId;
      if (!userId) {
        return;
      }

      if (target.classList.contains("edit-button")) {
        editUser(userId);
      }
      if (target.classList.contains("delete-button")) {
        deleteUser(userId);
      }
    });

    resetUserForm();
    loadUsers();
  </script>
</body>
</html>"""


@app.get("/login", response_class=HTMLResponse)
def getLoginPage(request: Request, next: str | None = Query(None)) -> HTMLResponse:
    user = _getCurrentUser(request)
    if user:
        if bool(user.get("must_change_password")):
            return RedirectResponse(url="/change-password", status_code=303)
        return RedirectResponse(url=_getSafeNextPath(next), status_code=303)

    return _renderHtmlPage(buildLoginPage(next))


@app.get("/change-password", response_class=HTMLResponse)
def getChangePasswordPage(request: Request) -> HTMLResponse:
    user = _getCurrentUser(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    return _renderHtmlPage(buildChangePasswordPage(user))


@app.get("/logout")
def logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/admin/users", response_class=HTMLResponse)
def getAdminUsersPage(request: Request) -> HTMLResponse:
    _ensureAuthStorage()
    _requireAdminUser(request)
    return _renderHtmlPage(buildAdminUsersPage())


@app.post("/api/auth/login")
def login(request: Request, payload: LoginPayload, next: str | None = Query(None)) -> dict[str, object]:
    _ensureAuthStorage()
    loginValue = str(payload.login or "").strip().lower()
    passwordValue = str(payload.password or "")
    if not loginValue or not passwordValue:
        raise HTTPException(status_code=400, detail="Р’РІРµРґРёС‚Рµ Р»РѕРіРёРЅ Рё РїР°СЂРѕР»СЊ.")

    user = getUserByLogin(loginValue)
    if not user or not _verifyPassword(passwordValue, str(user.get("password_hash") or "")):
        raise HTTPException(status_code=401, detail="РќРµРІРµСЂРЅС‹Р№ Р»РѕРіРёРЅ РёР»Рё РїР°СЂРѕР»СЊ.")

    roles = _parseRoles(user.get("roles"))
    if USER_ROLE not in roles:
        raise HTTPException(status_code=403, detail="РЈ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµС‚ РїСЂР°РІР° РІС…РѕРґР° РІ СЃРёСЃС‚РµРјСѓ.")

    request.session["user_login"] = loginValue
    return {
        "ok": True,
        "must_change_password": bool(user.get("must_change_password")),
        "next_path": _getSafeNextPath(next),
        "user": {
            "id": user.get("id"),
            "login": user.get("login"),
            "roles": roles,
            "must_change_password": bool(user.get("must_change_password")),
        },
    }


@app.post("/api/auth/change-password")
def changePassword(request: Request, payload: ChangePasswordPayload) -> dict[str, object]:
    _ensureAuthStorage()
    user = _requireAuthenticatedUser(request)
    newPassword = str(payload.new_password or "")
    if len(newPassword) < 3:
        raise HTTPException(status_code=400, detail="РќРѕРІС‹Р№ РїР°СЂРѕР»СЊ РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ РЅРµ РєРѕСЂРѕС‡Рµ 3 СЃРёРјРІРѕР»РѕРІ.")

    updatedUser = updateUserPassword(int(user.get("id") or 0), _hashPassword(newPassword), False)
    if not updatedUser:
        raise HTTPException(status_code=404, detail="РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ.")

    request.session["user_login"] = str(updatedUser.get("login") or user.get("login") or "")
    return {"ok": True, "next_path": "/"}


@app.get("/api/auth/me")
def getCurrentUserInfo(request: Request) -> dict[str, object]:
    _ensureAuthStorage()
    user = _requireAuthenticatedUser(request)
    return {
        "user": {
            "id": user.get("id"),
            "login": user.get("login"),
            "roles": _parseRoles(user.get("roles")),
            "must_change_password": bool(user.get("must_change_password")),
        }
    }


@app.get("/api/admin/users")
def getAdminUsers(request: Request) -> dict[str, object]:
    _requireAdminUser(request)
    users = listUsers()
    for user in users:
        user["roles"] = _parseRoles(user.get("roles"))
    return {"users": users}


@app.post("/api/admin/users")
def createAdminUser(request: Request, payload: UserPayload) -> dict[str, object]:
    _requireAdminUser(request)
    loginValue = str(payload.login or "").strip().lower()
    if not loginValue:
        raise HTTPException(status_code=400, detail="Р›РѕРіРёРЅ РѕР±СЏР·Р°С‚РµР»РµРЅ.")
    if not payload.password:
        raise HTTPException(status_code=400, detail="Р”Р»СЏ РЅРѕРІРѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅСѓР¶РЅРѕ Р·Р°РґР°С‚СЊ РїР°СЂРѕР»СЊ.")

    roles = _normalizeRoles(payload.roles)
    if USER_ROLE not in roles:
        raise HTTPException(status_code=400, detail="РќСѓР¶РЅРѕ РІС‹Р±СЂР°С‚СЊ С…РѕС‚СЏ Р±С‹ РїСЂР°РІРѕ User.")

    try:
        createUser(
            {
                "login": loginValue,
                "password_hash": _hashPassword(str(payload.password)),
                "roles": _serializeRoles(roles),
                "must_change_password": bool(payload.must_change_password),
            }
        )
    except Exception as error:
        raise HTTPException(status_code=400, detail=f"РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР·РґР°С‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {error}") from error

    users = listUsers()
    for user in users:
        user["roles"] = _parseRoles(user.get("roles"))
    return {"users": users}


@app.put("/api/admin/users/{user_id}")
def updateAdminUser(request: Request, user_id: int, payload: UserPayload) -> dict[str, object]:
    currentUser = _requireAdminUser(request)
    loginValue = str(payload.login or "").strip().lower()
    if not loginValue:
        raise HTTPException(status_code=400, detail="Р›РѕРіРёРЅ РѕР±СЏР·Р°С‚РµР»РµРЅ.")

    roles = _normalizeRoles(payload.roles)
    if USER_ROLE not in roles:
        raise HTTPException(status_code=400, detail="РќСѓР¶РЅРѕ РІС‹Р±СЂР°С‚СЊ С…РѕС‚СЏ Р±С‹ РїСЂР°РІРѕ User.")

    try:
        updatedUser = updateUser(
            user_id,
            {
                "login": loginValue,
                "password_hash": _hashPassword(str(payload.password)) if payload.password else None,
                "roles": _serializeRoles(roles),
                "must_change_password": bool(payload.must_change_password),
            },
        )
    except Exception as error:
        raise HTTPException(status_code=400, detail=f"РќРµ СѓРґР°Р»РѕСЃСЊ РѕР±РЅРѕРІРёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {error}") from error

    if updatedUser is None:
        raise HTTPException(status_code=404, detail="РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ.")

    if int(currentUser.get("id") or 0) == user_id:
        request.session["user_login"] = loginValue

    users = listUsers()
    for user in users:
        user["roles"] = _parseRoles(user.get("roles"))
    return {"users": users}


@app.delete("/api/admin/users/{user_id}")
def deleteAdminUser(request: Request, user_id: int) -> dict[str, object]:
    currentUser = _requireAdminUser(request)
    if int(currentUser.get("id") or 0) == user_id:
        raise HTTPException(status_code=400, detail="РќРµР»СЊР·СЏ СѓРґР°Р»РёС‚СЊ С‚РµРєСѓС‰РµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")

    deleted = deleteUser(user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ.")

    users = listUsers()
    for user in users:
        user["roles"] = _parseRoles(user.get("roles"))
    return {"users": users}


@app.get("/", response_class=HTMLResponse)
def readRoot() -> HTMLResponse:
    return _renderHtmlPage(PAGE_HTML)


@app.get("/Bitrix", response_class=HTMLResponse)
@app.get("/bitrix", response_class=HTMLResponse, include_in_schema=False)
def readBitrixPage() -> HTMLResponse:
    return _renderHtmlPage(BITRIX_PAGE_HTML)


@app.get("/snapshot-rules", response_class=HTMLResponse)
def getSnapshotRulesPage() -> HTMLResponse:
    return _renderHtmlPage(buildSnapshotRulesPage())


@app.get("/planning-projects", response_class=HTMLResponse)
def getPlanningProjectsPage() -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    return _renderHtmlPage(buildPlanningProjectsPage())


@app.get("/strange-snapshot-issues", response_class=HTMLResponse)
def getStrangeSnapshotIssuesPage() -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    return _renderHtmlPage(buildStrangeSnapshotIssuesPage())


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


@app.get("/api/planning-projects")
def getPlanningProjects() -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    return {"projects": listPlanningProjects()}


@app.post("/api/planning-projects")
def createPlanningProjectApi(payload: PlanningProjectPayload) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    createdProject = createPlanningProject(normalizePlanningProjectPayload(payload))
    return {"project": createdProject, "projects": listPlanningProjects()}


@app.put("/api/planning-projects/{planning_project_id}")
def updatePlanningProjectApi(planning_project_id: int, payload: PlanningProjectPayload) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    updatedProject = updatePlanningProject(planning_project_id, normalizePlanningProjectPayload(payload))
    if updatedProject is None:
        raise HTTPException(status_code=404, detail="Р—Р°РїРёСЃСЊ РЅРµ РЅР°Р№РґРµРЅР°")
    return {"project": updatedProject, "projects": listPlanningProjects()}


@app.delete("/api/planning-projects/{planning_project_id}")
def deletePlanningProjectApi(planning_project_id: int) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    deleted = deletePlanningProject(planning_project_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Р—Р°РїРёСЃСЊ РЅРµ РЅР°Р№РґРµРЅР°")
    return {"deleted": True, "projects": listPlanningProjects()}


@app.get("/projects/{project_redmine_id}/latest-snapshot-issues", response_class=HTMLResponse)
def getProjectLatestSnapshotIssuesPage(
    project_redmine_id: int,
    captured_for_date: str | None = Query(None, description="Р”Р°С‚Р° СЃСЂРµР·Р° РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD"),
) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    return _renderHtmlPage(buildLatestSnapshotIssuesPageClean(project_redmine_id, captured_for_date))


@app.get("/api/projects/{project_redmine_id}/latest-snapshot-issues")
def getProjectLatestSnapshotIssuesData(
    project_redmine_id: int,
    captured_for_date: str | None = Query(None, description="Р”Р°С‚Р° СЃСЂРµР·Р° РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD"),
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
    risk_op: str | None = Query(None),
    risk_value: str | None = Query(None),
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
        riskOp=risk_op,
        riskValue=risk_value,
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
    captured_for_date: str | None = Query(None, description="Р”Р°С‚Р° СЃСЂРµР·Р° РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD"),
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
    risk_op: str | None = Query(None),
    risk_value: str | None = Query(None),
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
        riskOp=risk_op,
        riskValue=risk_value,
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
        raise HTTPException(status_code=404, detail="РЎСЂРµР· РїСЂРѕРµРєС‚Р° РЅРµ РЅР°Р№РґРµРЅ")

    output = io.StringIO(newline="")
    output.write("sep=;\n")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(
        [
            "ID",
            "РўРµРјР°",
            "РўСЂРµРєРµСЂ",
            "РЎС‚Р°С‚СѓСЃ",
            "Р“РѕС‚РѕРІРѕ, %",
            "Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°, С‡",
            "РџР»Р°РЅ, С‡",
            "РџР»Р°РЅ СЃ СЂРёСЃРєР°РјРё, С‡",
            "Р¤Р°РєС‚ РІСЃРµРіРѕ, С‡",
            "Р¤Р°РєС‚ Р·Р° РіРѕРґ, С‡",
            "Р—Р°РєСЂС‹С‚Р°",
            "РСЃРїРѕР»РЅРёС‚РµР»СЊ",
            "Р’РµСЂСЃРёСЏ",
        ]
    )
    for issue in exportPayload.get("issues") or []:
        writer.writerow(
            [
                issue.get("issue_redmine_id") or "",
                str(issue.get("subject") or "вЂ”"),
                str(issue.get("tracker_name") or "вЂ”"),
                str(issue.get("status_name") or "вЂ”"),
                issue.get("done_ratio") if issue.get("done_ratio") is not None else 0,
                formatPageHours(issue.get("baseline_estimate_hours")),
                formatPageHours(issue.get("estimated_hours")),
                formatPageHours(issue.get("risk_estimate_hours")),
                formatPageHours(issue.get("spent_hours")),
                formatPageHours(issue.get("spent_hours_year")),
                formatSnapshotPageDateTime(issue.get("closed_on")),
                str(issue.get("assigned_to_name") or "вЂ”"),
                str(issue.get("fixed_version_name") or "вЂ”"),
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
    left_date: str | None = Query(None, description="Р”Р°С‚Р° РїРµСЂРІРѕРіРѕ СЃСЂРµР·Р° РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD"),
    right_date: str | None = Query(None, description="Р”Р°С‚Р° РІС‚РѕСЂРѕРіРѕ СЃСЂРµР·Р° РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD"),
    field: list[str] = Query([]),
    include_missing: int = Query(0),
) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    return _renderHtmlPage(
        buildSnapshotComparisonPage(
            project_redmine_id,
            left_date,
            right_date,
            field,
            includeMissingIssues=bool(include_missing),
        )
    )


@app.get("/projects/{project_redmine_id}/burndown", response_class=HTMLResponse)
def getProjectBurndownPage(project_redmine_id: int) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    return _renderHtmlPage(buildBurndownPage(project_redmine_id))


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
    captured_for_date: str = Query(..., description="Р”Р°С‚Р° РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD"),
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
    captured_for_date: str = Query(..., description="Р”Р°С‚Р° РІ С„РѕСЂРјР°С‚Рµ YYYY-MM-DD"),
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
            "detail": "РџРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·РѕРІ СѓР¶Рµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ.",
            **getIssueSnapshotCaptureStatus(),
        }

    started = startIssueSnapshotCaptureInBackground()
    return {
        "started": started,
        "detail": "РџРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·РѕРІ Р·Р°РїСѓС‰РµРЅРѕ РІ С„РѕРЅРѕРІРѕРј СЂРµР¶РёРјРµ.",
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
            "detail": "Р”СЂСѓРіРѕРµ РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·РѕРІ СѓР¶Рµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ.",
            **getIssueSnapshotCaptureStatus(),
        }

    project = next((item for item in listStoredProjects() if int(item.get("redmine_id") or 0) == project_redmine_id), None)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    if not bool(project.get("is_enabled")):
        raise HTTPException(status_code=400, detail="РџСЂРѕРµРєС‚ РІС‹РєР»СЋС‡РµРЅ РґР»СЏ Р·Р°РіСЂСѓР·РєРё")

    started = startProjectIssueSnapshotCaptureInBackground(project_redmine_id)
    return {
        "started": started,
        "detail": f"РџРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·Р° РїРѕ РїСЂРѕРµРєС‚Сѓ В«{project.get('name') or project_redmine_id}В» Р·Р°РїСѓС‰РµРЅРѕ.",
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
            "detail": "Р”СЂСѓРіРѕРµ РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·РѕРІ СѓР¶Рµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ.",
            **getIssueSnapshotCaptureStatus(),
        }

    project = next((item for item in listStoredProjects() if int(item.get("redmine_id") or 0) == project_redmine_id), None)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    if not bool(project.get("is_enabled")):
        raise HTTPException(status_code=400, detail="РџСЂРѕРµРєС‚ РІС‹РєР»СЋС‡РµРЅ РґР»СЏ Р·Р°РіСЂСѓР·РєРё")

    capturedForDate = datetime.now(UTC).date().isoformat()
    deleteIssueSnapshotForProjectDate(project_redmine_id, capturedForDate)
    started = startProjectIssueSnapshotCaptureInBackground(project_redmine_id)

    return {
        **getIssueSnapshotCaptureStatus(),
        "started": started,
        "captured_for_date": capturedForDate,
        "detail": f"РџРѕРІС‚РѕСЂРЅРѕРµ РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·Р° РїРѕ РїСЂРѕРµРєС‚Сѓ В«{project.get('name') or project_redmine_id}В» Р·Р°РїСѓС‰РµРЅРѕ.",
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
            "detail": "Р”СЂСѓРіРѕРµ РїРѕР»СѓС‡РµРЅРёРµ СЃСЂРµР·РѕРІ СѓР¶Рµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ.",
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
        "detail": "РћР±РЅРѕРІР»РµРЅРёРµ РїРѕСЃР»РµРґРЅРёС… СЃСЂРµР·РѕРІ Р·Р°РїСѓС‰РµРЅРѕ.",
    }


@app.get("/api/issues/snapshots/capture-status")
def getIssueSnapshotCaptureProgress() -> dict[str, object]:
    return getIssueSnapshotCaptureStatus()


@app.get("/api/redmine/issues/{issue_redmine_id}/snapshot-diagnostics")
def getRedmineIssueSnapshotDiagnostics(
    issue_redmine_id: int,
    project_redmine_id: int = Query(..., description="Redmine ID РїСЂРѕРµРєС‚Р°, РґР»СЏ РєРѕС‚РѕСЂРѕРіРѕ РїСЂРѕРІРµСЂСЏРµРј РїРѕРїР°РґР°РЅРёРµ РІ СЃСЂРµР·"),
) -> dict[str, object]:
    requireProjectSyncConfig()
    ensureProjectsTable()

    project = next((item for item in listStoredProjects() if int(item.get("redmine_id") or 0) == int(project_redmine_id)), None)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    try:
        session = _buildRedmineApiSession()
        response = session.get(
            f"{config.redmineUrl.rstrip('/')}/issues/{issue_redmine_id}.json",
            params={"include": "children"},
            timeout=60,
        )
        response.raise_for_status()
        issuePayload = (response.json() or {}).get("issue") or {}
    except requests.HTTPError as error:
        statusCode = error.response.status_code if error.response is not None else 502
        raise HTTPException(status_code=statusCode, detail=f"Redmine issue request failed: {error}") from error
    except requests.RequestException as error:
        raise HTTPException(status_code=502, detail=f"Redmine request failed: {error}") from error

    issueProject = issuePayload.get("project") or {}
    issueProjectId = issueProject.get("id")
    issueProjectName = issueProject.get("name")
    issueProjectIdentifier = issueProject.get("identifier")
    partialLoad = bool(project.get("partial_load"))
    projectIdentifier = str(project.get("identifier") or "")
    cutoffDateIso = f"{datetime.now(UTC).year - 1}-01-01"

    exactProjectMatch = int(issueProjectId or 0) == int(project_redmine_id)
    if not exactProjectMatch:
        includedInSnapshot = False
        inclusionReason = (
            f"Р—Р°РґР°С‡Р° СЃРµР№С‡Р°СЃ РѕС‚РЅРѕСЃРёС‚СЃСЏ Рє РїСЂРѕРµРєС‚Сѓ В«{issueProjectName or 'Р±РµР· РЅР°Р·РІР°РЅРёСЏ'}В» "
            f"(id={issueProjectId}), Р° СЃСЂРµР· СЃРѕР±РёСЂР°РµС‚СЃСЏ РґР»СЏ РїСЂРѕРµРєС‚Р° В«{project.get('name') or project_redmine_id}В» "
            f"(id={project_redmine_id}) Р±РµР· РїРѕРґРїСЂРѕРµРєС‚РѕРІ."
        )
    elif not partialLoad:
        includedInSnapshot = True
        inclusionReason = (
            "Р”Р»СЏ РїСЂРѕРµРєС‚Р° РІС‹РєР»СЋС‡РµРЅР° С‡Р°СЃС‚РёС‡РЅР°СЏ Р·Р°РіСЂСѓР·РєР°, РїРѕСЌС‚РѕРјСѓ РІ СЃСЂРµР· РїРѕРїР°РґР°СЋС‚ РІСЃРµ Р·Р°РґР°С‡Рё СЃР°РјРѕРіРѕ РїСЂРѕРµРєС‚Р° "
            "Р±РµР· РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅРѕРіРѕ РѕС‚Р±РѕСЂР° РїРѕ СЃС‚Р°С‚СѓСЃСѓ РёР»Рё РґР°С‚Рµ Р·Р°РєСЂС‹С‚РёСЏ."
        )
    else:
        includedInSnapshot, inclusionReason = _isIssueIncludedByPartialRules(issuePayload, cutoffDateIso)

    return {
        "project": {
            "redmine_id": int(project_redmine_id),
            "name": project.get("name"),
            "identifier": projectIdentifier,
            "partial_load": partialLoad,
            "closed_on_cutoff": cutoffDateIso if partialLoad else None,
        },
        "issue": {
            "id": issue_redmine_id,
            "subject": issuePayload.get("subject"),
            "tracker": (issuePayload.get("tracker") or {}).get("name"),
            "status": (issuePayload.get("status") or {}).get("name"),
            "status_is_closed": bool((issuePayload.get("status") or {}).get("is_closed")),
            "project_id": issueProjectId,
            "project_name": issueProjectName,
            "project_identifier": issueProjectIdentifier,
            "parent_issue_id": (issuePayload.get("parent") or {}).get("id"),
            "baseline_estimate": next(
                (
                    field.get("value")
                    for field in (issuePayload.get("custom_fields") or [])
                    if str(field.get("name") or "") == "Р‘Р°Р·РѕРІР°СЏ РѕС†РµРЅРєР°"
                ),
                None,
            ),
            "estimated_hours": issuePayload.get("estimated_hours"),
            "spent_hours": issuePayload.get("spent_hours"),
            "closed_on": issuePayload.get("closed_on"),
            "updated_on": issuePayload.get("updated_on"),
        },
        "included_in_snapshot": includedInSnapshot,
        "reason": inclusionReason,
    }


@app.get("/api/redmine/projects/custom-field-diagnostics")
def getRedmineProjectCustomFieldDiagnostics(
    project_name: str = Query(..., description="РџРѕР»РЅРѕРµ РёР»Рё С‡Р°СЃС‚РёС‡РЅРѕРµ РёРјСЏ РїСЂРѕРµРєС‚Р° РІ Redmine"),
    field_name: str = Query(..., description="РќР°Р·РІР°РЅРёРµ РёР»Рё С‡Р°СЃС‚СЊ РЅР°Р·РІР°РЅРёСЏ РєР°СЃС‚РѕРјРЅРѕРіРѕ РїРѕР»СЏ"),
    sample_size: int = Query(10, ge=1, le=30, description="РЎРєРѕР»СЊРєРѕ Р·Р°РґР°С‡ РїСЂРѕРІРµСЂРёС‚СЊ РІ РїСЂРѕРµРєС‚Рµ"),
) -> dict[str, object]:
    requireProjectSyncConfig()

    normalizedProjectQuery = _normalizeSearchText(project_name)
    if not normalizedProjectQuery:
        raise HTTPException(status_code=400, detail="project_name is empty")

    try:
        redmineProjects = fetchAllProjectsFromRedmine(config.redmineUrl, config.apiKey)
    except requests.RequestException as error:
        raise HTTPException(status_code=502, detail=f"Redmine projects request failed: {error}") from error

    matchedProject = next(
        (
            project
            for project in redmineProjects
            if normalizedProjectQuery in _normalizeSearchText(project.get("name"))
        ),
        None,
    )
    if matchedProject is None:
        raise HTTPException(status_code=404, detail="Project not found in Redmine")

    projectIdentifier = str(matchedProject.get("identifier") or "").strip()
    if not projectIdentifier:
        raise HTTPException(status_code=400, detail="Matched project has no identifier")

    try:
        session = _buildRedmineApiSession()
        response = session.get(
            f"{config.redmineUrl.rstrip('/')}/issues.json",
            params={
                "project_id": projectIdentifier,
                "subproject_id": "!*",
                "status_id": "*",
                "sort": "id:desc",
                "limit": sample_size,
            },
            timeout=90,
        )
        response.raise_for_status()
        issuesPayload = (response.json() or {}).get("issues") or []
    except requests.HTTPError as error:
        statusCode = error.response.status_code if error.response is not None else 502
        raise HTTPException(status_code=statusCode, detail=f"Redmine issues request failed: {error}") from error
    except requests.RequestException as error:
        raise HTTPException(status_code=502, detail=f"Redmine issues request failed: {error}") from error

    sampledIssues: list[dict[str, object]] = []
    issuesWithField: list[dict[str, object]] = []
    allMatchedFieldNames: dict[str, dict[str, object]] = {}

    for issuePayload in issuesPayload:
        matchingFields = _findMatchingCustomFields(issuePayload, field_name)
        issueInfo = {
            "id": issuePayload.get("id"),
            "subject": issuePayload.get("subject"),
            "tracker": (issuePayload.get("tracker") or {}).get("name"),
            "status": (issuePayload.get("status") or {}).get("name"),
            "matching_fields": matchingFields,
        }
        sampledIssues.append(issueInfo)
        if matchingFields:
            issuesWithField.append(issueInfo)
            for field in matchingFields:
                allMatchedFieldNames[str(field.get("name") or "")] = field

    return {
        "project": {
            "redmine_id": matchedProject.get("redmine_id"),
            "name": matchedProject.get("name"),
            "identifier": projectIdentifier,
        },
        "field_query": field_name,
        "sample_size": sample_size,
        "issues_checked": len(sampledIssues),
        "issues_with_field": len(issuesWithField),
        "field_visible": bool(issuesWithField),
        "matched_field_names": sorted(allMatchedFieldNames.keys()),
        "sampled_issues": sampledIssues,
        "issues_with_field_samples": issuesWithField,
    }


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



