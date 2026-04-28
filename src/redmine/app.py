from datetime import UTC, datetime
from html import escape
import csv
import hashlib
import hmac
import io
import json
import os
from email.message import EmailMessage
from pathlib import Path
import secrets
import smtplib
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
    countPlanningProjects,
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
    getSnapshotRunsWithIssuesForProjectDateRange,
    getSnapshotIssuesForProjectByDate,
    getSnapshotTimeEntriesForProjectByDateRange,
    getUserByPasswordResetToken,
    getUserByLogin,
    listPlanningProjectIdentifiers,
    listPlanningProjectsByRedmineIdentifier,
    listFilteredSnapshotIssuesForProjectByDate,
    listLatestSnapshotIssuesWithParents,
    listIssueSnapshotCaptureErrors,
    listPlanningProjects,
    listRecentIssueSnapshotRuns,
    listSnapshotDatesForProject,
    listStoredProjects,
    listUsers,
    pruneUnchangedIssueSnapshots,
    seedInitialUsers,
    storeUserPasswordResetToken,
    syncProjects,
    clearUserPasswordResetToken,
    updateUser,
    updateUserPassword,
    updatePlanningProject,
    updateProjectLoadSettings,
    listPlanningDirections,
    listProjectPlanningSummary,
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
APP_BUILD_ID = os.getenv("APP_BUILD_ID", "2026-04-28-verify-1")
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
        "/forgot-password",
        "/reset-password",
        "/logout",
        "/change-password",
        "/health",
        "/db-health",
        "/api/auth/login",
        "/api/auth/request-password-reset",
        "/api/auth/reset-password",
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


def _getSafeNextPath(nextPath: str | None) -> str:
    candidate = str(nextPath or "").strip()
    if not candidate.startswith("/") or candidate.startswith("//"):
        return "/"
    return candidate or "/"


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
    direction: str | None = None
    project_name: str
    redmine_identifier: str | None = None
    pm_name: str | None = None
    customer: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    development_hours: float | None = None
    year_1: int | None = None
    hours_1: float | None = None
    year_2: int | None = None
    hours_2: float | None = None
    year_3: int | None = None
    hours_3: float | None = None
    baseline_estimate_hours: float | None = None
    p1: float | None = None
    p2: float | None = None
    estimate_doc_url: str | None = None
    bitrix_url: str | None = None
    comment_text: str | None = None
    question_flag: bool = False
    is_closed: bool = False


class LoginPayload(BaseModel):
    login: str
    password: str


class ChangePasswordPayload(BaseModel):
    new_password: str


class PasswordResetRequestPayload(BaseModel):
    email: str


class PasswordResetCompletePayload(BaseModel):
    token: str
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


def _hashResetToken(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _getBaseUrl(request: Request | None = None) -> str:
    configuredBaseUrl = str(config.appBaseUrl or "").strip().rstrip("/")
    if configuredBaseUrl:
        return configuredBaseUrl

    if request is not None:
        return str(request.base_url).rstrip("/")

    return f"http://{config.appHost}:{config.appPort}".rstrip("/")


def _buildPasswordResetLink(request: Request, token: str) -> str:
    return f"{_getBaseUrl(request)}/reset-password?token={quote(token)}"


def _sendPasswordResetEmail(loginValue: str, resetUrl: str) -> None:
    smtpHost = str(config.smtpHost or "").strip()
    smtpFromEmail = str(config.smtpFromEmail or "").strip()
    if not smtpHost or not smtpFromEmail:
        raise RuntimeError("SMTP is not configured")

    message = EmailMessage()
    message["Subject"] = "Сброс пароля"
    message["From"] = (
        f"{config.smtpFromName} <{smtpFromEmail}>"
        if str(config.smtpFromName or "").strip()
        else smtpFromEmail
    )
    message["To"] = loginValue
    message.set_content(
        "\n".join(
            [
                "Здравствуйте!",
                "",
                "Для сброса пароля перейдите по ссылке:",
                resetUrl,
                "",
                "Если вы не запрашивали сброс пароля, просто проигнорируйте это письмо.",
                "Ссылка действует 30 минут.",
            ]
        )
    )

    smtpPort = int(config.smtpPort or 0) or (465 if config.smtpUseSsl else 587)
    if config.smtpUseSsl:
        with smtplib.SMTP_SSL(smtpHost, smtpPort, timeout=30) as smtp:
            if config.smtpUsername:
                smtp.login(config.smtpUsername, config.smtpPassword)
            smtp.send_message(message)
        return

    with smtplib.SMTP(smtpHost, smtpPort, timeout=30) as smtp:
        smtp.ehlo()
        if config.smtpUseTls:
            smtp.starttls()
            smtp.ehlo()
        if config.smtpUsername:
            smtp.login(config.smtpUsername, config.smtpPassword)
        smtp.send_message(message)


AUTH_PAGE_STYLES = """
    body {
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
      font-family: "Golos", "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #f6f9fb 0%, #eef6f7 100%);
      color: #16324a;
    }
    .card {
      width: min(520px, 100%);
      background: #ffffff;
      border: 1px solid #d9e5eb;
      border-radius: 16px;
      box-shadow: 0 18px 40px rgba(22, 50, 74, 0.08);
      padding: 28px 28px 24px;
    }
    .brand {
      display: inline-flex;
      align-items: center;
      margin-bottom: 18px;
    }
    .brand img {
      width: 180px;
      height: auto;
      display: block;
    }
    h1 {
      margin: 0 0 10px;
      font-size: clamp(1.8rem, 4vw, 2.4rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 400;
    }
    .lead {
      margin: 0 0 22px;
      color: #64798d;
      line-height: 1.55;
    }
    form {
      display: grid;
      gap: 16px;
    }
    label {
      display: grid;
      gap: 7px;
      font-size: 0.96rem;
      font-weight: 600;
    }
    input {
      width: 100%;
      border: 1px solid #d9e5eb;
      border-radius: 10px;
      padding: 12px 14px;
      font: inherit;
      color: #16324a;
      background: #ffffff;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 13px 18px;
      background: #ff6c0e;
      color: #ffffff;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }
    button:disabled {
      opacity: 0.7;
      cursor: wait;
    }
    .secondary-link {
      display: inline;
      color: #375d77;
      text-decoration: underline;
      text-underline-offset: 2px;
      font-weight: 600;
    }
    .secondary-link:hover {
      color: #ff6c0e;
    }
    .status {
      min-height: 22px;
      color: #64798d;
      font-size: 0.95rem;
    }
    .status.error {
      color: #d54343;
    }
""".strip()


def buildLoginPage(nextPath: str = "/") -> str:
    safeNextPath = escape(_getSafeNextPath(nextPath))
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Вход в систему</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
{LOCAL_GOLOS_FONT_CSS}
{AUTH_PAGE_STYLES}
  </style>
</head>
<body>
  <section class="card">
    <a class="brand" href="/" aria-label="СМС-ИТ">
      <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">
    </a>
    <h1>Вход в систему</h1>
    <p class="lead">Введите логин и пароль, чтобы открыть систему анализа проектов Redmine.</p>
    <form id="loginForm">
      <input id="nextPathInput" type="hidden" value="{safeNextPath}">
      <label for="loginInput">
        Логин
        <input id="loginInput" type="text" autocomplete="username" required>
      </label>
      <label for="passwordInput">
        Пароль
        <input id="passwordInput" type="password" autocomplete="current-password" required>
      </label>
      <button id="loginButton" type="submit">Войти</button>
    </form>
    <a class="secondary-link" href="/forgot-password">Сбросить пароль</a>
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
      setStatus("Проверяем логин и пароль...");

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
          throw new Error(payload.detail || "Не удалось войти в систему.");
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


def buildForgotPasswordPage() -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Сброс пароля</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
{LOCAL_GOLOS_FONT_CSS}
{AUTH_PAGE_STYLES}
  </style>
</head>
<body>
  <section class="card">
    <a class="brand" href="/" aria-label="СМС-ИТ">
      <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">
    </a>
    <h1>Сброс пароля</h1>
    <p class="lead">Введите email-логин. Мы отправим письмо со ссылкой для задания нового пароля.</p>
    <form id="forgotPasswordForm">
      <label for="emailInput">
        Email
        <input id="emailInput" type="email" autocomplete="email" required>
      </label>
      <button id="forgotPasswordButton" type="submit">Отправить письмо</button>
    </form>
    <a class="secondary-link" href="/login">Вернуться ко входу</a>
    <div class="status" id="forgotPasswordStatus"></div>
  </section>

  <script>
    const forgotPasswordForm = document.getElementById("forgotPasswordForm");
    const forgotPasswordButton = document.getElementById("forgotPasswordButton");
    const forgotPasswordStatus = document.getElementById("forgotPasswordStatus");

    function setStatus(message, kind = "") {{
      forgotPasswordStatus.textContent = message;
      forgotPasswordStatus.className = "status" + (kind ? " " + kind : "");
    }}

    forgotPasswordForm.addEventListener("submit", async (event) => {{
      event.preventDefault();
      forgotPasswordButton.disabled = true;
      setStatus("Отправляем письмо...");
      try {{
        const response = await fetch("/api/auth/request-password-reset", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ email: document.getElementById("emailInput").value }}),
        }});
        const payload = await response.json();
        if (!response.ok) {{
          throw new Error(payload.detail || "Не удалось отправить письмо.");
        }}
        setStatus(payload.detail || "Если пользователь найден, письмо отправлено.");
      }} catch (error) {{
        setStatus(error.message, "error");
      }} finally {{
        forgotPasswordButton.disabled = false;
      }}
    }});
  </script>
</body>
</html>"""


def buildResetPasswordPage(token: str) -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Новый пароль</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
{LOCAL_GOLOS_FONT_CSS}
{AUTH_PAGE_STYLES}
  </style>
</head>
<body>
  <section class="card">
    <a class="brand" href="/" aria-label="СМС-ИТ">
      <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">
    </a>
    <h1>Задание нового пароля</h1>
    <p class="lead">Введите новый пароль. После сохранения можно будет войти в систему под этим логином.</p>
    <form id="resetPasswordForm">
      <input id="tokenInput" type="hidden" value="{escape(token)}">
      <label for="newPasswordInput">
        Новый пароль
        <input id="newPasswordInput" type="password" autocomplete="new-password" required>
      </label>
      <button id="resetPasswordButton" type="submit">Сохранить пароль</button>
    </form>
    <a class="secondary-link" href="/login">Вернуться ко входу</a>
    <div class="status" id="resetPasswordStatus"></div>
  </section>

  <script>
    const resetPasswordForm = document.getElementById("resetPasswordForm");
    const resetPasswordButton = document.getElementById("resetPasswordButton");
    const resetPasswordStatus = document.getElementById("resetPasswordStatus");

    function setStatus(message, kind = "") {{
      resetPasswordStatus.textContent = message;
      resetPasswordStatus.className = "status" + (kind ? " " + kind : "");
    }}

    resetPasswordForm.addEventListener("submit", async (event) => {{
      event.preventDefault();
      resetPasswordButton.disabled = true;
      setStatus("Сохраняем новый пароль...");
      try {{
        const response = await fetch("/api/auth/reset-password", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            token: document.getElementById("tokenInput").value,
            new_password: document.getElementById("newPasswordInput").value,
          }}),
        }});
        const payload = await response.json();
        if (!response.ok) {{
          throw new Error(payload.detail || "Не удалось сохранить новый пароль.");
        }}
        setStatus("Пароль обновлен. Перенаправляем на форму входа...");
        window.setTimeout(() => {{ window.location.href = "/login"; }}, 900);
      }} catch (error) {{
        setStatus(error.message, "error");
        resetPasswordButton.disabled = false;
      }}
    }});
  </script>
</body>
</html>"""


def buildChangePasswordPage() -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Смена пароля</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
{LOCAL_GOLOS_FONT_CSS}
{AUTH_PAGE_STYLES}
  </style>
</head>
<body>
  <section class="card">
    <a class="brand" href="/" aria-label="СМС-ИТ">
      <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">
    </a>
    <h1>Смена пароля</h1>
    <p class="lead">Для этого пользователя требуется обязательная смена пароля перед продолжением работы.</p>
    <form id="changePasswordForm">
      <label for="changePasswordInput">
        Новый пароль
        <input id="changePasswordInput" type="password" autocomplete="new-password" required>
      </label>
      <button id="changePasswordButton" type="submit">Сменить пароль</button>
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
      changePasswordButton.disabled = true;
      setStatus("Сохраняем пароль...");
      try {{
        const response = await fetch("/api/auth/change-password", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            new_password: document.getElementById("changePasswordInput").value,
          }}),
        }});
        const payload = await response.json();
        if (!response.ok) {{
          throw new Error(payload.detail || "Не удалось сменить пароль.");
        }}
        setStatus("Пароль обновлен. Перенаправляем...");
        window.setTimeout(() => {{
          window.location.href = payload.next_path || "/";
        }}, 900);
      }} catch (error) {{
        setStatus(error.message, "error");
        changePasswordButton.disabled = false;
      }}
    }});
  </script>
</body>
</html>"""


def buildAdminUsersPage(users: list[dict[str, object]]) -> str:
    usersJson = json.dumps(
        [
            {
                **user,
                "roles": _parseRoles(user.get("roles")),
            }
            for user in users
        ],
        ensure_ascii=False,
    )
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Администрирование пользователей</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
{LOCAL_GOLOS_FONT_CSS}
    body {{
      margin: 0;
      font-family: "Golos", "Segoe UI", sans-serif;
      background: #f6f9fb;
      color: #16324a;
    }}
    main {{
      max-width: 1320px;
      margin: 0 auto;
      padding: 24px 20px 48px;
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: clamp(1.85rem, 4.2vw, 2.75rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 400;
    }}
    .lead {{
      margin: 0 0 18px;
      color: #64798d;
      line-height: 1.55;
    }}
    .top-link {{
      display: inline-flex;
      margin-bottom: 18px;
      color: #375d77;
      text-decoration: underline;
      text-underline-offset: 2px;
      font-weight: 600;
    }}
    .layout {{
      display: grid;
      gap: 18px;
    }}
    .panel {{
      background: #ffffff;
      border: 1px solid #d9e5eb;
      border-radius: 8px;
      box-shadow: 0 12px 24px rgba(22, 50, 74, 0.06);
      padding: 18px 20px 20px;
    }}
    .table-wrap {{
      overflow: auto;
      border: 1px solid #d9e5eb;
      border-radius: 8px;
      background: #ffffff;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
    }}
    th, td {{
      border-bottom: 1px solid #d9e5eb;
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
      line-height: 1.45;
    }}
    th {{
      background: #f8fbfd;
      font-weight: 700;
    }}
    .row-actions {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .row-actions button, .form-actions button {{
      border: 0;
      border-radius: 8px;
      padding: 9px 14px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      color: #ffffff;
    }}
    .edit-button {{ background: #375d77; }}
    .delete-button {{ background: #d54343; }}
    .save-button {{ background: #ff6c0e; }}
    .reset-button {{ background: #375d77; }}
    .form-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px 16px;
    }}
    .field {{
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    .field label {{
      font-weight: 700;
    }}
    .field input {{
      width: 100%;
      border: 1px solid #d9e5eb;
      border-radius: 6px;
      padding: 10px 12px;
      font: inherit;
      color: #16324a;
      background: #ffffff;
    }}
    .roles-grid {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      align-items: center;
      min-height: 42px;
    }}
    .checkbox-field {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-weight: 600;
    }}
    .form-actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 16px;
    }}
    .status {{
      min-height: 22px;
      margin-top: 12px;
      color: #64798d;
      font-size: 0.95rem;
    }}
    .status.error {{ color: #d54343; }}
  </style>
</head>
<body>
  <main>
    <a class="top-link" href="/">Вернуться на главную</a>
    <h1>Администрирование пользователей</h1>
    <p class="lead">Здесь можно создавать пользователей, выдавать роли и включать обязательную смену пароля.</p>

    <section class="panel">
      <h2>Пользователи</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Логин</th>
              <th>Права</th>
              <th>Сменить пароль</th>
              <th>Действия</th>
            </tr>
          </thead>
          <tbody id="usersTableBody"></tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2 id="userFormTitle">Новый пользователь</h2>
      <form id="userForm">
        <input id="userId" type="hidden">
        <div class="form-grid">
          <div class="field">
            <label for="userLogin">Логин</label>
            <input id="userLogin" type="email" required>
          </div>
          <div class="field">
            <label for="userPassword">Пароль</label>
            <input id="userPassword" type="password" autocomplete="new-password">
          </div>
          <div class="field">
            <label>Права</label>
            <div class="roles-grid">
              <label class="checkbox-field"><input type="checkbox" value="User" class="role-checkbox"> <span>User</span></label>
              <label class="checkbox-field"><input type="checkbox" value="Finance" class="role-checkbox"> <span>Finance</span></label>
              <label class="checkbox-field"><input type="checkbox" value="Admin" class="role-checkbox"> <span>Admin</span></label>
            </div>
          </div>
          <div class="field">
            <label class="checkbox-field" for="userMustChangePassword">
              <input id="userMustChangePassword" type="checkbox">
              <span>Сменить пароль</span>
            </label>
          </div>
        </div>
        <div class="form-actions">
          <button class="save-button" type="submit">Сохранить</button>
          <button class="reset-button" id="resetUserFormButton" type="button">Очистить форму</button>
        </div>
      </form>
      <div class="status" id="userStatus"></div>
    </section>
  </main>

  <script>
    const initialUsers = {usersJson};
    const usersTableBody = document.getElementById("usersTableBody");
    const userForm = document.getElementById("userForm");
    const userFormTitle = document.getElementById("userFormTitle");
    const userId = document.getElementById("userId");
    const userLogin = document.getElementById("userLogin");
    const userPassword = document.getElementById("userPassword");
    const userMustChangePassword = document.getElementById("userMustChangePassword");
    const userStatus = document.getElementById("userStatus");
    const resetUserFormButton = document.getElementById("resetUserFormButton");

    let currentUsers = Array.isArray(initialUsers) ? initialUsers : [];

    function setStatus(message, kind = "") {{
      userStatus.textContent = message;
      userStatus.className = "status" + (kind ? " " + kind : "");
    }}

    function selectedRoles() {{
      return Array.from(document.querySelectorAll(".role-checkbox:checked")).map((checkbox) => checkbox.value);
    }}

    function resetUserForm() {{
      userForm.reset();
      userId.value = "";
      userFormTitle.textContent = "Новый пользователь";
      setStatus("");
    }}

    function fillUserForm(user) {{
      userId.value = user.id ?? "";
      userLogin.value = user.login ?? "";
      userPassword.value = "";
      userMustChangePassword.checked = Boolean(user.must_change_password);
      const roles = Array.isArray(user.roles) ? user.roles : [];
      document.querySelectorAll(".role-checkbox").forEach((checkbox) => {{
        checkbox.checked = roles.includes(checkbox.value);
      }});
      userFormTitle.textContent = "Редактирование пользователя";
      setStatus("Пользователь загружен в форму.");
      userForm.scrollIntoView({{ behavior: "smooth", block: "start" }});
    }}

    function renderUsers(users) {{
      if (!users.length) {{
        usersTableBody.innerHTML = '<tr><td colspan="4">Пока нет пользователей.</td></tr>';
        return;
      }}

      usersTableBody.innerHTML = users.map((user) => `
        <tr>
          <td>${{user.login || ""}}</td>
          <td>${{Array.isArray(user.roles) ? user.roles.join(", ") : ""}}</td>
          <td>${{user.must_change_password ? "Да" : "Нет"}}</td>
          <td>
            <div class="row-actions">
              <button type="button" class="edit-button" data-action="edit" data-id="${{user.id}}">Изм.</button>
              <button type="button" class="delete-button" data-action="delete" data-id="${{user.id}}">Удалить</button>
            </div>
          </td>
        </tr>
      `).join("");
    }}

    async function loadUsers() {{
      const response = await fetch("/api/admin/users");
      const payload = await response.json();
      if (!response.ok) {{
        throw new Error(payload.detail || "Не удалось загрузить пользователей.");
      }}
      currentUsers = Array.isArray(payload.users) ? payload.users : [];
      renderUsers(currentUsers);
    }}

    userForm.addEventListener("submit", async (event) => {{
      event.preventDefault();
      const id = userId.value;
      const method = id ? "PUT" : "POST";
      const url = id ? `/api/admin/users/${{encodeURIComponent(id)}}` : "/api/admin/users";
      setStatus(id ? "Сохраняем изменения..." : "Создаем пользователя...");
      try {{
        const response = await fetch(url, {{
          method,
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            login: userLogin.value,
            password: userPassword.value || null,
            roles: selectedRoles(),
            must_change_password: userMustChangePassword.checked,
          }}),
        }});
        const payload = await response.json();
        if (!response.ok) {{
          throw new Error(payload.detail || "Не удалось сохранить пользователя.");
        }}
        await loadUsers();
        resetUserForm();
        setStatus(id ? "Пользователь обновлен." : "Пользователь создан.");
      }} catch (error) {{
        setStatus(error.message, "error");
      }}
    }});

    resetUserFormButton.addEventListener("click", resetUserForm);

    usersTableBody.addEventListener("click", async (event) => {{
      const target = event.target;
      if (!(target instanceof HTMLElement)) {{
        return;
      }}
      const action = target.dataset.action;
      const id = target.dataset.id;
      if (!action || !id) {{
        return;
      }}
      const user = currentUsers.find((item) => String(item.id) === String(id));
      if (!user) {{
        setStatus("Пользователь не найден.", "error");
        return;
      }}
      if (action === "edit") {{
        fillUserForm(user);
        return;
      }}
      if (action === "delete") {{
        if (!window.confirm(`Удалить пользователя "${{user.login}}"?`)) {{
          return;
        }}
        try {{
          setStatus("Удаляем пользователя...");
          const response = await fetch(`/api/admin/users/${{encodeURIComponent(id)}}`, {{ method: "DELETE" }});
          const payload = await response.json();
          if (!response.ok) {{
            throw new Error(payload.detail || "Не удалось удалить пользователя.");
          }}
          await loadUsers();
          if (String(userId.value) === String(id)) {{
            resetUserForm();
          }}
          setStatus("Пользователь удален.");
        }} catch (error) {{
          setStatus(error.message, "error");
        }}
      }}
    }});

    renderUsers(currentUsers);
  </script>
</body>
</html>"""


def _isIssueIncludedByPartialRules(issuePayload: dict[str, object], cutoffDateIso: str) -> tuple[bool, str]:
    status = issuePayload.get("status") or {}
    statusName = str(status.get("name") or "")
    isClosed = bool(status.get("is_closed"))
    closedOnRaw = issuePayload.get("closed_on")
    closedOn = _parseRedmineIsoDate(str(closedOnRaw) if closedOnRaw else None)
    cutoffDate = date.fromisoformat(cutoffDateIso)

    if not isClosed:
        return True, f"Задача открыта по статусу «{statusName}», поэтому попадает в частичный срез."

    if closedOn is None:
        return False, (
            f"Задача закрыта по статусу «{statusName}», но у нее нет даты closed_on, "
            "поэтому по текущим правилам в частичный срез не попадает."
        )

    if closedOn.date() >= cutoffDate:
        return True, (
            f"Задача закрыта {closedOn.date().isoformat()}, это не раньше порога {cutoffDateIso}, "
            "поэтому в частичный срез попадает."
        )

    return False, (
        f"Задача закрыта {closedOn.date().isoformat()}, это раньше порога {cutoffDateIso}, "
        "поэтому в частичный срез не попадает."
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
  <title>Redmine: проекты и срезы</title>
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

    .quick-links a#projectsNavButton {
      background: var(--blue-302);
      color: #ffffff;
      border-color: transparent;
      box-shadow: 0 10px 18px rgba(55, 93, 119, 0.2);
    }

    .quick-links a#snapshotRunsNavButton {
      background: var(--yellow-109);
      color: var(--text);
      border-color: transparent;
      box-shadow: 0 10px 18px rgba(255, 198, 0, 0.24);
    }

    .quick-links a#adminPageButton {
      background: var(--cyan-310);
      color: var(--text);
      border-color: transparent;
      box-shadow: 0 10px 18px rgba(82, 206, 230, 0.24);
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
    #projectsSummaryPageButton,
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
    #projectsSummaryPageButton:hover,
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
      background: var(--cyan-310);
      color: var(--blue-302);
      box-shadow: 0 14px 24px rgba(82, 206, 230, 0.24);
    }

    #projectsSummaryPageButton {
      background: var(--yellow-109);
      color: var(--blue-302);
      box-shadow: 0 14px 24px rgba(255, 198, 0, 0.24);
    }

    #captureSnapshotsButton:hover {
      background: #44c8e0;
    }

    #projectsSummaryPageButton:hover {
      background: #f4bd00;
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

    .status-history-link {
      color: inherit;
      text-decoration: underline dotted rgba(55, 93, 119, 0.5);
      text-underline-offset: 3px;
      text-decoration-thickness: 1px;
      margin-left: 8px;
      cursor: pointer;
    }

    .status-history-link:hover {
      text-decoration-color: currentColor;
    }

    .status-history {
      margin-top: 10px;
      padding: 12px 14px;
      border: 1px solid rgba(55, 93, 119, 0.16);
      border-radius: 10px;
      background: rgba(247, 250, 252, 0.92);
      color: var(--text);
    }

    .status-history[hidden] {
      display: none;
    }

    .status-history-title {
      margin: 0 0 8px;
      font-size: 0.92rem;
      font-weight: 600;
      color: var(--blue-302);
    }

    .status-history-list {
      margin: 0;
      padding-left: 18px;
      display: grid;
      gap: 8px;
    }

    .status-history-list li {
      line-height: 1.45;
    }

    .status-history-empty {
      margin: 0;
      color: var(--muted);
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
      width: 7ch;
      min-width: 7ch;
      max-width: 7ch;
      white-space: nowrap;
    }

    .project-enabled-col {
      width: 11ch;
      min-width: 11ch;
      max-width: 11ch;
    }

    .project-name-cell {
      white-space: nowrap;
      position: relative;
      overflow: hidden;
    }

    .project-name-col {
      width: 25ch;
      min-width: 25ch;
      max-width: 25ch;
    }

    .project-sticky-3.project-name-cell {
      min-width: 25ch;
    }

    .projects-datetime-col {
      width: 28ch;
      min-width: 28ch;
      max-width: 28ch;
      white-space: nowrap;
    }

    .project-name-wrap {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
      max-width: 100%;
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

    .project-planning-button.has-planning {
      border-color: var(--yellow-109);
      background: var(--yellow-109);
      color: var(--blue-302);
      box-shadow: 0 0 0 1px rgba(255, 198, 0, 0.25);
    }

    .project-planning-button.has-planning:hover {
      border-color: #f4bd00;
      background: #f4bd00;
      color: var(--blue-302);
    }

    .project-tree {
      display: inline-flex;
      align-items: center;
      position: relative;
      padding-left: calc(var(--tree-level, 0) * 18px);
      min-width: 0;
      max-width: 100%;
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
      text-decoration-color: currentColor;
    }

    .project-row-disabled .project-enabled-checkbox,
    .project-row-disabled .project-partial-checkbox,
    .project-row-disabled #enableVisibleProjectsCheckbox {
      accent-color: #b4bec8;
    }

    .project-row-context-only {
      color: #bcc6cf;
      background: #fbfcfd;
    }

    .project-row-context-only a,
    .project-row-context-only .project-id-button,
    .project-row-context-only .project-capture-button {
      color: #bcc6cf;
      text-decoration-color: currentColor;
    }

    .project-row-context-only .project-planning-button {
      color: #a7b4bf;
      border-color: #d5dde5;
      background: #f8fafb;
    }

    .project-row-context-only .project-tree::before {
      border-top-color: #d8e0e7;
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
      text-decoration-line: underline;
      text-decoration-style: dashed;
      text-decoration-color: currentColor;
      text-decoration-thickness: 1px;
      text-underline-offset: 0.14em;
      white-space: nowrap;
      font-weight: 400;
      display: block;
      flex: 1 1 auto;
      min-width: 0;
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .project-link:hover {
      color: var(--orange-1585);
      text-decoration-style: solid;
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
          <a id="projectsNavButton" href="#projects-table">Проекты Redmine</a>
          <a id="snapshotRunsNavButton" href="#snapshot-runs-table">Срезы задач</a>
          <a id="adminPageButton" href="/admin/users">Администрирование</a>
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
        <h2>Управление проектами</h2>
        <p>Получает список проектов из Redmine, добавляет новые записи и обновляет измененные.</p>
        <div class="row">
<button id="refreshProjectsButton" type="button">Синхронизация с Redmine</button>
          <button id="projectsSummaryPageButton" type="button">Сводка по проектам</button>
          <button id="planningProjectsPageButton" type="button">Планирование проектов</button>
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
          <button id="strangeIssuesPageButton" type="button">Вопросы по задачам</button>
        </div>
        <div class="status" id="captureStatus"></div>
        <div class="status-history" id="captureStatusHistory" hidden></div>
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
        <h2>Проекты Redmine</h2>
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
              <th class="checkbox-cell project-enabled-col project-sticky-1">
                <label>
                  <input id="enableVisibleProjectsCheckbox" type="checkbox">
                  Вкл.
                </label>
              </th>
              <th class="checkbox-cell">Част.</th>
              <th class="project-sticky-2">ID</th>
              <th class="project-sticky-3 project-name-col">Название</th>
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
              <th class="projects-datetime-col">Обновлен в Redmine</th>
              <th class="projects-datetime-col">Синхронизирован</th>
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
        <button type="button" class="filter-reset-button is-inactive" id="resetSnapshotFiltersButton">Сбросить фильтр</button>
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
    const captureStatusHistory = document.getElementById("captureStatusHistory");
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
    const projectsSummaryPageButton = document.getElementById("projectsSummaryPageButton");
    const strangeIssuesPageButton = document.getElementById("strangeIssuesPageButton");
    const adminPageButton = document.getElementById("adminPageButton");
    const captureSnapshotsButton = document.getElementById("captureSnapshotsButton");
    const recaptureSnapshotsButton = document.getElementById("recaptureSnapshotsButton");
    const deleteSnapshotsButton = document.getElementById("deleteSnapshotsButton");
    const pruneSnapshotsButton = document.getElementById("pruneSnapshotsButton");
    let captureStatusPollTimer = null;
    let allProjects = [];
    let planningProjectIdentifiers = new Set();
    let allSnapshotRuns = [];
    let captureErrorsExpanded = false;
    const projectsNameFilterStorageKey = "redmine.projects.nameFilter";
    const projectsFactFilterStorageKey = "redmine.projects.factFilter.min";
    const showDisabledProjectsStorageKey = "redmine.projects.showDisabled";
    const snapshotRunsPerProjectStorageKey = "redmine.snapshotRuns.perProject";

    function setStatus(element, message, kind = "") {
      element.textContent = message;
      element.className = "status" + (kind ? " " + kind : "");
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function clearCaptureErrorHistory() {
      captureErrorsExpanded = false;
      captureStatusHistory.hidden = true;
      captureStatusHistory.innerHTML = "";
    }

    function renderCaptureErrorStatus(message) {
      captureStatus.className = "status error";
      captureStatus.innerHTML = `${escapeHtml(message)} <a href="#" class="status-history-link" id="captureStatusHistoryLink">Показать все ошибки</a>`;
      const historyLink = document.getElementById("captureStatusHistoryLink");
      if (historyLink) {
        historyLink.addEventListener("click", async (event) => {
          event.preventDefault();
          if (captureErrorsExpanded) {
            clearCaptureErrorHistory();
            captureStatus.className = "status error";
            captureStatus.innerHTML = `${escapeHtml(message)} <a href="#" class="status-history-link" id="captureStatusHistoryLink">Показать все ошибки</a>`;
            const resetLink = document.getElementById("captureStatusHistoryLink");
            if (resetLink) {
              resetLink.addEventListener("click", async (resetEvent) => {
                resetEvent.preventDefault();
                await loadCaptureErrorHistory(message);
              });
            }
            return;
          }
          await loadCaptureErrorHistory(message);
        });
      }
    }

    async function loadCaptureErrorHistory(currentMessage = "") {
      captureErrorsExpanded = true;
      captureStatusHistory.hidden = false;
      captureStatusHistory.innerHTML = '<p class="status-history-empty">Загружаем историю ошибок...</p>';

      try {
        const response = await fetch("/api/issues/snapshots/capture-errors");
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "Не удалось загрузить историю ошибок.");
        }

        const errors = Array.isArray(payload.errors) ? payload.errors : [];
        if (!errors.length) {
          captureStatusHistory.innerHTML = '<p class="status-history-empty">Сохраненных ошибок пока нет.</p>';
          return;
        }

        const itemsHtml = errors.map((item) => {
          const createdAt = escapeHtml(formatDate(item.created_at));
          const projectPart = item.project_name
            ? ` • ${escapeHtml(item.project_name)}`
            : item.project_redmine_id
              ? ` • проект ${escapeHtml(item.project_redmine_id)}`
              : "";
          const modePart = item.mode ? ` • режим ${escapeHtml(item.mode)}` : "";
          const runnerPart = item.runner_kind ? ` • ${escapeHtml(item.runner_kind)}` : "";
          const datePart = item.captured_for_date ? ` • дата ${escapeHtml(item.captured_for_date)}` : "";
          return `<li><span class="mono">${createdAt}</span>${projectPart}${modePart}${runnerPart}${datePart}<br>${escapeHtml(item.message || "")}</li>`;
        }).join("");

        captureStatusHistory.innerHTML = `
          <p class="status-history-title">История ошибок загрузки срезов</p>
          <ol class="status-history-list">${itemsHtml}</ol>
        `;
        renderCaptureErrorStatus(currentMessage || errors[0]?.message || "Ошибка загрузки срезов.");
        const activeLink = document.getElementById("captureStatusHistoryLink");
        if (activeLink) {
          activeLink.textContent = "Скрыть ошибки";
        }
      } catch (error) {
        captureStatusHistory.innerHTML = `<p class="status-history-empty">${escapeHtml(error.message || "Не удалось загрузить историю ошибок.")}</p>`;
      }
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
        ["#project-actions h2", "textContent", "Управление проектами"],
        ["#project-actions p", "textContent", "Получает список проектов из Redmine, добавляет новые записи и обновляет измененные."],
        ["#refreshProjectsButton", "textContent", "Синхронизация с Redmine"],
        ["#projectsSummaryPageButton", "textContent", "Сводка по проектам"],
        ["#snapshot-actions h2", "textContent", "Получение срезов задач"],
        ["#snapshot-actions p", "textContent", "Запрашивает срезы только для тех проектов, по которым на сегодняшнюю дату еще нет записи в базе данных."],
        ["#captureSnapshotsButton", "textContent", "Получить срезы задач"],
        ["#recaptureSnapshotsButton", "textContent", "Обновить последние срезы"],
        ["#strangeIssuesPageButton", "textContent", "Вопросы по задачам"],
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

      const projectsSectionHeading = document.querySelector("#projects-table h2");
      if (projectsSectionHeading) {
        projectsSectionHeading.textContent = "Проекты Redmine";
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
      const directlyMatchedIds = new Set(directlyMatchedProjects.map((project) => project.redmine_id));
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

      return projects
        .filter((project) => includedIds.has(project.redmine_id))
        .map((project) => ({
          ...project,
          _shown_as_ancestor_only: !directlyMatchedIds.has(project.redmine_id),
        }));
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
            renderCaptureErrorStatus(payload.error_message);
            captureSnapshotsButton.disabled = false;
            recaptureSnapshotsButton.disabled = false;
            return;
          }

          if (payload.created_runs || payload.captured_issues || payload.already_captured_projects) {
            clearCaptureErrorHistory();
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
        clearCaptureErrorHistory();
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
          const planningProjectUrl = `/planning-projects?redmine_identifier=${encodeURIComponent(identifier)}&project_name=${encodeURIComponent(project?.name ?? "")}&open_mode=auto`;
          const normalizedIdentifier = identifier.trim().toLocaleLowerCase("ru");
          const hasPlanningProject = Boolean(normalizedIdentifier) && planningProjectIdentifiers.has(normalizedIdentifier);
          const planningButtonClass = hasPlanningProject
            ? "project-planning-button has-planning"
            : "project-planning-button";
          const level = Math.max(Number(project?.hierarchy_level ?? 0) || 0, 0);
          const projectTreeClass = level > 0 ? "project-tree has-parent" : "project-tree";
          const row = document.createElement("tr");
          const rowClasses = [];
          if (!project?.is_enabled) {
            rowClasses.push("project-row-disabled");
          }
          if (project?._shown_as_ancestor_only) {
            rowClasses.push("project-row-context-only");
          }
          row.className = rowClasses.join(" ");
          row.innerHTML = `
            <td class="checkbox-cell project-enabled-col project-sticky-1"><input class="project-enabled-checkbox" type="checkbox" data-project-id="${redmineId}" ${project?.is_enabled ? "checked" : ""}></td>
            <td class="checkbox-cell"><input class="project-partial-checkbox" type="checkbox" data-project-id="${redmineId}" ${project?.partial_load ? "checked" : ""} ${project?.is_enabled ? "" : "disabled"}></td>
            <td class="mono project-sticky-2">
              <span class="project-id-actions">
                <a class="project-id-button mono" href="/projects/${encodeURIComponent(redmineId)}/latest-snapshot-issues" target="_blank" rel="noreferrer">${redmineId}</a>
                <button class="project-capture-button" type="button" data-project-id="${redmineId}" title="Получить срез по проекту" ${project?.is_enabled ? "" : "disabled"}>↓</button>
              </span>
            </td>
            <td class="project-name-cell project-name-col project-sticky-3"><span class="${projectTreeClass}" style="--tree-level:${level};"><span class="project-name-wrap"><a class="project-link" href="/projects/${encodeURIComponent(redmineId)}/burndown" target="_blank" rel="noreferrer">${project?.name ?? "\u2014"}</a><a class="project-planning-button" href="${planningProjectUrl}" target="_blank" rel="noreferrer" title="Открыть планирование проекта" aria-label="Открыть планирование проекта">i</a></span></span></td>
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
            <td class="projects-datetime-col">${formatDate(project?.updated_on)}</td>
            <td class="projects-datetime-col">${formatDate(project?.synced_at)}</td>
          `;
          row.innerHTML = row.innerHTML.replace('class="project-planning-button"', `class="${planningButtonClass}"`);
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
      clearCaptureErrorHistory();
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
        clearCaptureErrorHistory();
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

        planningProjectIdentifiers = new Set(
          Array.isArray(payload.planning_project_identifiers)
            ? payload.planning_project_identifiers
                .map((value) => String(value ?? "").trim().toLocaleLowerCase("ru"))
                .filter(Boolean)
            : []
        );
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
      clearCaptureErrorHistory();
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
        clearCaptureErrorHistory();
        setStatus(captureStatus, error.message, "error");
        captureSnapshotsButton.disabled = false;
        recaptureSnapshotsButton.disabled = false;
      }
    }

    async function recaptureSnapshots() {
      captureSnapshotsButton.disabled = true;
      recaptureSnapshotsButton.disabled = true;
      clearCaptureErrorHistory();
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
        clearCaptureErrorHistory();
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
    projectsSummaryPageButton.addEventListener("click", () => {
      window.location.href = "/projects-summary";
    });
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
        projectName = escape(str((storedProject or {}).get("name") or "—"))
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
            "<span>Показывать новые/отсутствующие задачи с нулевыми значениями</span></label>"
        )
        return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Сравнение срезов</title>
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
    <h1>Сравнение срезов проекта</h1>
    <p class="meta">Проект: <span class="meta-strong">{projectName}</span>. Идентификатор: <span class="meta-strong">{escape(projectIdentifierRaw or "—")}</span>. Для сравнения нужен хотя бы один сохраненный срез.</p>
    <section class="controls-panel">
      <form method="get" id="compareSnapshotsForm">
        <div class="controls-grid">
          <div class="field">
            <div class="compare-date-row">
              <div class="field compare-date-field">
                <label for="leftDate">Дата среза 1</label>
                <select id="leftDate" name="left_date" class="compare-date-select"><option value="">Нет срезов</option></select>
              </div>
              <div class="compare-swap-stack">
                <div class="field date-swap-field">
                  <label class="date-swap-label" for="swapCompareDatesButton">Поменять даты местами</label>
                  <button type="button" class="date-swap-button" id="swapCompareDatesButton" aria-label="Поменять даты местами"><span>←</span><span>→</span></button>
                </div>
              </div>
              <div class="field compare-date-field">
                <label for="rightDate">Дата среза 2</label>
                <select id="rightDate" name="right_date" class="compare-date-select"><option value="">Нет срезов</option></select>
              </div>
              <div class="compare-compare-stack">
                <span class="compare-option-caption">Поля для сравнения</span>
                <div class="compare-field-group">{compareFieldsHtml}</div>
                <div class="compare-extra-stack">
                  <span class="compare-option-caption">Дополнительные опции</span>
                  {includeMissingHtml}
                </div>
              </div>
            </div>
          </div>
        </div>
        <p><button type="submit">Сравнить</button></p>
      </form>
    </section>
    <div class="empty-state">Для этого проекта пока нет срезов, поэтому сравнивать еще нечего.</div>
  </main>
  <div class="compare-loading-overlay" id="compareLoadingOverlay" aria-hidden="true">
    <span class="compare-loading-spinner" aria-hidden="true"></span>
    <span class="compare-loading-text">Сравнение срезов проекта...</span>
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
        "<span>Показывать новые/отсутствующие задачи с нулевыми значениями</span></label>"
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
            rowBadgeHtml = '<span class="compare-badge compare-badge-new">Новая</span>'
        elif row["change_kind"] == "deleted":
            rowBadgeHtml = '<span class="compare-badge compare-badge-deleted">Отсутствует</span>'

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
  <title>Сравнение срезов</title>
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
    <h1>Сравнение срезов проекта</h1>
    <p class="meta">Проект: <span class="meta-strong">{projectName}</span>. Идентификатор: <span class="meta-strong">{projectIdentifier}</span>.</p>
    <section class="controls-panel">
      <form method="get" id="compareSnapshotsForm">
        <div class="controls-grid">
          <div class="field">
            <div class="compare-date-row">
              <div class="field compare-date-field">
                <label for="leftDate">Дата среза 1</label>
                <select id="leftDate" name="left_date" class="compare-date-select">{leftDateOptionsHtml}</select>
              </div>
              <div class="compare-swap-stack">
                <div class="field date-swap-field">
                  <label class="date-swap-label" for="swapCompareDatesButton">Поменять даты местами</label>
                  <button type="button" class="date-swap-button" id="swapCompareDatesButton" aria-label="Поменять даты местами"><span>←</span><span>→</span></button>
                </div>
              </div>
              <div class="field compare-date-field">
                <label for="rightDate">Дата среза 2</label>
                <select id="rightDate" name="right_date" class="compare-date-select">{rightDateOptionsHtml}</select>
              </div>
              <div class="compare-compare-stack">
                <span class="compare-option-caption">Поля для сравнения</span>
                <div class="compare-field-group">{fieldOptionsHtml}</div>
                <div class="compare-extra-stack">
                  <span class="compare-option-caption">Дополнительные опции</span>
                  {includeMissingHtml}
                </div>
              </div>
            </div>
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
  <div class="compare-loading-overlay" id="compareLoadingOverlay" aria-hidden="true">
    <span class="compare-loading-spinner" aria-hidden="true"></span>
    <span class="compare-loading-text">Сравнение срезов проекта...</span>
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
    return normalizeBurndownText(statusName) in {"закрыта", "решена", "отказ"}


def isBurndownReadyFeatureStatus(statusName: object) -> bool:
    normalized = normalizeBurndownText(statusName)
    return normalized.startswith("готов") or normalized in {"закрыта", "решена"}


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
                "development_volume_risk": 0.0,
                "development_remaining_risk": 0.0,
                "bug_volume": 0.0,
                "bug_remaining": 0.0,
                "bug_volume_risk": 0.0,
                "bug_remaining_risk": 0.0,
            },
        )

        trackerName = normalizeBurndownText(issue.get("tracker_name"))
        statusName = issue.get("status_name")
        planHours = float(issue.get("estimated_hours") or 0)
        riskPlanHours = float(issue.get("risk_estimate_hours") or 0)
        factHours = float(issue.get("spent_hours") or 0)

        if featureId is not None and featureId == issueId and trackerName == "feature":
            group["is_ready"] = isBurndownReadyFeatureStatus(statusName)
            continue

        baselineEstimateHours = float(issue.get("baseline_estimate_hours") or 0)
        group["baseline_total"] = float(group["baseline_total"]) + baselineEstimateHours

        if trackerName == "разработка":
            if isBurndownClosedTaskStatus(statusName):
                volume = factHours
                remaining = 0.0
                riskVolume = factHours
                riskRemaining = 0.0
            else:
                volume = max(baselineEstimateHours, planHours, factHours)
                remaining = max(0.0, max(baselineEstimateHours, planHours) - factHours)
                riskVolume = max(baselineEstimateHours, riskPlanHours, factHours)
                riskRemaining = max(0.0, max(baselineEstimateHours, riskPlanHours) - factHours)
            group["development_volume"] = float(group["development_volume"]) + volume
            group["development_remaining"] = float(group["development_remaining"]) + remaining
            group["development_volume_risk"] = float(group["development_volume_risk"]) + riskVolume
            group["development_remaining_risk"] = float(group["development_remaining_risk"]) + riskRemaining
        elif trackerName == "процессы разработки":
            volume = max(planHours, factHours)
            riskVolume = max(riskPlanHours, factHours)
            group["development_volume"] = float(group["development_volume"]) + volume
            group["development_volume_risk"] = float(group["development_volume_risk"]) + riskVolume
        elif trackerName == "ошибка":
            if isBurndownClosedTaskStatus(statusName):
                volume = factHours
                remaining = 0.0
                riskVolume = factHours
                riskRemaining = 0.0
            else:
                volume = max(planHours, factHours)
                remaining = max(0.0, planHours - factHours)
                riskVolume = max(riskPlanHours, factHours)
                riskRemaining = max(0.0, riskPlanHours - factHours)
            group["bug_volume"] = float(group["bug_volume"]) + volume
            group["bug_remaining"] = float(group["bug_remaining"]) + remaining
            group["bug_volume_risk"] = float(group["bug_volume_risk"]) + riskVolume
            group["bug_remaining_risk"] = float(group["bug_remaining_risk"]) + riskRemaining

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


def buildBurndownDateLabels(dateFrom: date, dateTo: date) -> list[str]:
    currentDate = dateFrom
    lastDate = dateTo
    labels: list[str] = []

    while currentDate <= lastDate:
        labels.append(currentDate.isoformat())
        currentDate += timedelta(days=1)

    return labels


def buildBurndownPage(projectRedmineId: int) -> str:
    ensurePlanningProjectsTable()
    storedProjects = listStoredProjects()
    storedProject = next(
        (item for item in storedProjects if int(item.get("redmine_id") or 0) == projectRedmineId),
        None,
    )
    projectNameRaw = str(storedProject.get("name") if storedProject else "—")
    projectIdentifierRaw = str(storedProject.get("identifier") if storedProject else "").strip()
    projectName = escape(projectNameRaw)
    projectIdentifier = escape(projectIdentifierRaw or "—")
    planningProjects = (
        listPlanningProjectsByRedmineIdentifier(projectIdentifierRaw) if projectIdentifierRaw else []
    )

    def addMonths(baseDate: date, monthsDelta: int) -> date:
        monthIndex = (baseDate.month - 1) + monthsDelta
        year = baseDate.year + monthIndex // 12
        month = monthIndex % 12 + 1
        if month == 12:
            monthLastDay = 31
        else:
            monthLastDay = (date(year, month + 1, 1) - timedelta(days=1)).day
        return date(year, month, min(baseDate.day, monthLastDay))

    planningStartDates = []
    for project in planningProjects:
        rawStartDate = str(project.get("start_date") or "").strip()
        if not rawStartDate:
            continue
        try:
            planningStartDates.append(date.fromisoformat(rawStartDate))
        except ValueError:
            continue

    todayLocal = datetime.now().date()
    chartStartDate = min(planningStartDates) if planningStartDates else addMonths(todayLocal, -1)
    chartEndDate = addMonths(todayLocal, 1)
    if chartStartDate > chartEndDate:
        chartStartDate = chartEndDate

    burndownPayload = getSnapshotRunsWithIssuesForProjectDateRange(
        projectRedmineId,
        chartStartDate.isoformat(),
        chartEndDate.isoformat(),
    )
    projectInfo = burndownPayload.get("project") or {}
    projectName = escape(str(projectInfo.get("project_name") or projectNameRaw or "—"))
    projectIdentifierRaw = str(projectInfo.get("project_identifier") or projectIdentifierRaw or "").strip()
    projectIdentifier = escape(projectIdentifierRaw or "—")

    def formatPlanningMetric(value: object) -> str:
        if value in (None, ""):
            return "—"
        try:
            numericValue = float(value)
        except (TypeError, ValueError):
            return str(value)
        return formatPageHours(numericValue)

    def normalizePlanningPercent(value: object, defaultPercent: float) -> float:
        if value in (None, ""):
            return defaultPercent
        try:
            numericValue = float(value)
        except (TypeError, ValueError):
            return defaultPercent
        return numericValue * 100 if abs(numericValue) <= 10 else numericValue

    def formatPlanningPercent(value: object) -> str:
        if value in (None, ""):
            return "—"
        try:
            numericValue = float(value)
        except (TypeError, ValueError):
            return str(value)
        normalizedValue = numericValue * 100 if abs(numericValue) <= 10 else numericValue
        return formatPageHours(normalizedValue)

    planningP1Values = [
        normalizePlanningPercent(project.get("p1"), 150.0)
        for project in planningProjects
        if project.get("p1") not in (None, "")
    ]
    planningP2Values = [
        normalizePlanningPercent(project.get("p2"), 150.0)
        for project in planningProjects
        if project.get("p2") not in (None, "")
    ]
    planningP1Unique = sorted({round(value, 6) for value in planningP1Values})
    planningP2Unique = sorted({round(value, 6) for value in planningP2Values})
    planningP1Mixed = len(planningP1Unique) > 1
    planningP2Mixed = len(planningP2Unique) > 1
    planningP1Percent = planningP1Unique[0] if len(planningP1Unique) == 1 else 150.0
    planningP2Percent = planningP2Unique[0] if len(planningP2Unique) == 1 else 150.0
    totalPlanningBaseline = sum(float(project.get("baseline_estimate_hours") or 0) for project in planningProjects)
    totalPlanningDevelopmentHours = sum(float(project.get("development_hours") or 0) for project in planningProjects)
    planningBaselineText = escape(formatPlanningMetric(totalPlanningBaseline))
    planningDevelopmentHoursText = escape(formatPlanningMetric(totalPlanningDevelopmentHours))
    planningP1Value = escape(formatPageHours(planningP1Percent))
    planningP2Value = escape(formatPageHours(planningP2Percent))
    planningP1InputClass = " planning-input-warning" if planningP1Mixed else ""
    planningP2InputClass = " planning-input-warning" if planningP2Mixed else ""
    planningProjectLinesHtml = "".join(
        (
            '<div class="planning-project-line">'
            f'{escape(str(project.get("customer") or "—"))} - {escape(str(project.get("project_name") or "Без названия"))}'
            f' <span class="planning-project-metrics">({escape(formatPlanningMetric(project.get("baseline_estimate_hours")))} / '
            f'{escape(formatPlanningMetric(project.get("development_hours")))} / '
            f'{escape(formatPageHours(project.get("p1")))} / '
            f'{escape(formatPageHours(project.get("p2")))})</span>'
            "</div>"
        )
        for project in planningProjects
    )
    planningProjectsTextHtml = (
        f'<div class="planning-project-lines">{planningProjectLinesHtml}</div>'
        if planningProjectLinesHtml
        else ""
    )
    snapshotIssuesUrl = f"/projects/{projectRedmineId}/latest-snapshot-issues"
    navPanelHtml = buildProjectContextNavPanel(
        projectRedmineId,
        projectIdentifierRaw,
        currentPage="burndown",
        snapshotUrl=snapshotIssuesUrl,
    )
    snapshotRuns = list(burndownPayload.get("snapshot_runs") or [])
    chartSeeds = buildBurndownChartSeeds(snapshotRuns)
    chartDatesJson = json.dumps(buildBurndownDateLabels(chartStartDate, chartEndDate), ensure_ascii=False)
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
      font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
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

    .planning-project-lines {{
      margin: -4px 0 18px;
      color: var(--muted);
      font-size: 0.98rem;
      line-height: 1.65;
    }}
    .planning-project-metrics {{
      color: #426179;
      white-space: nowrap;
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

    .field-checkbox {{
      min-width: 280px;
      justify-content: flex-end;
    }}

    .field-checkbox-label {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      font-weight: 700;
      min-height: 44px;
    }}

    .planning-input-warning {{
      border-color: #d9534f !important;
      color: #d9534f !important;
      font-weight: 700;
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

    .legend-callout {{
      margin: 0 0 16px;
      padding: 12px 14px;
      border-radius: 8px;
      background: #eef8fb;
      border: 1px solid #c9e8ef;
      color: var(--text);
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
    <p class="meta">Проект: <span class="meta-strong">{projectName}</span>. Идентификатор: <span class="meta-strong">{projectIdentifier}</span>. Период диаграммы: {chartStartDate.strftime("%d.%m.%Y")} — {chartEndDate.strftime("%d.%m.%Y")}. Срезов в диапазоне: {len(chartSeeds)}.</p>
    {planningProjectsTextHtml}

    <section class="controls-panel">
      <div class="field">
        <label for="p1Input">P1 = факт / база, %</label>
        <input id="p1Input" class="{planningP1InputClass.strip()}" type="text" inputmode="decimal" value="{planningP1Value}">
        <div class="field-note">Используется в расчете бюджета и прогнозного объема.</div>
      </div>
      <div class="field">
        <label for="p2Input">P2 = факт с багами / факт, %</label>
        <input id="p2Input" class="{planningP2InputClass.strip()}" type="text" inputmode="decimal" value="{planningP2Value}">
        <div class="field-note">Изменения пересчитываются сразу после ввода без перезагрузки страницы.</div>
      </div>
      <div class="field">
        <label>Базовая оценка</label>
        <input type="text" value="{planningBaselineText}" readonly>
        <div class="field-note">Значение подтянуто из формы «Планирование проектов».</div>
      </div>
      <div class="field">
        <label>Лимит разработки с багфиксом</label>
        <input type="text" value="{planningDevelopmentHoursText}" readonly>
        <div class="field-note">Значение подтянуто из формы «Планирование проектов».</div>
      </div>
      <div class="field field-checkbox">
        <label class="field-checkbox-label" for="useRiskPlanCheckbox">
          <input id="useRiskPlanCheckbox" type="checkbox">
          <span>Использовать План с рисками</span>
        </label>
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
      <div class="legend-callout">При выборе "Использовать План с рисками" во всех формулах вместо "План" используется "План с рисками".</div>
      <div class="legend-grid">
        <div>
          <ul class="legend-list">
            <li>
              <span class="legend-swatch budget-line"></span>
              <div>
                <div class="legend-name">Бюджет</div>
                <div class="legend-text">Оранжевая линия. Для каждого среза: сумма базовых оценок всех задач среза без Feature × P1/100 × P2/100.</div>
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
                <div class="formula-text">Если статус задачи «Закрыта», «Решена» или «Отказ», то объем = факт, остаток = 0. Для остальных статусов: объем = max(база, план, факт), остаток = max(0, max(база, план) − факт).</div>
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
                <div class="formula-text">Для каждой Feature отдельно собираются объем/остаток по разработке и по ошибкам. Если Feature в статусе «Готов*», «Закрыта» или «Решена», прогноз = разработка + ошибки. Иначе прогноз = max(текущий объем, сумма базовых оценок задач Feature × P1/100 × P2/100). Для задач без Feature считается отдельная виртуальная Feature по тем же правилам.</div>
              </div>
            </li>
          </ul>
          <p class="legend-note">Итоговые линии «Объем.Текущий», «Объем.Остаток» и «Объем.Прогноз» — это суммы по всем Feature и по виртуальной Feature в выбранном диапазоне срезов.</p>
        </div>
      </div>
    </section>
  </main>

  <script>
    const burndownDateLabels = {chartDatesJson};
    const burndownSnapshots = {chartSeedsJson};
    const planningDevelopmentHoursTotal = {json.dumps(totalPlanningDevelopmentHours, ensure_ascii=False)};

    const p1Input = document.getElementById("p1Input");
    const p2Input = document.getElementById("p2Input");
    const useRiskPlanCheckbox = document.getElementById("useRiskPlanCheckbox");
    const statusNode = document.getElementById("burndownStatus");
    const chartCanvas = document.getElementById("burndownChart");
    const emptyState = document.getElementById("burndownEmptyState");

    function parsePercentValue(rawValue, fallbackPercent) {{
      const normalized = String(rawValue ?? "").trim().replace(",", ".");
      const parsed = Number.parseFloat(normalized);
      if (!Number.isFinite(parsed)) {{
        return fallbackPercent;
      }}
      return Math.abs(parsed) <= 10 ? parsed * 100 : parsed;
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

    function computeSnapshotMetrics(snapshot, p1Factor, p2Factor, useRiskPlan) {{
      const groups = Array.isArray(snapshot?.groups) ? snapshot.groups : [];
      let forecast = 0;
      let currentDevelopment = 0;
      let currentBugs = 0;
      let remainingDevelopment = 0;
      let remainingBugs = 0;

      for (const group of groups) {{
        const baselineTotal = Number(group?.baseline_total || 0);
        const developmentVolume = Number(useRiskPlan ? (group?.development_volume_risk || 0) : (group?.development_volume || 0));
        const bugVolume = Number(useRiskPlan ? (group?.bug_volume_risk || 0) : (group?.bug_volume || 0));
        const developmentRemaining = Number(useRiskPlan ? (group?.development_remaining_risk || 0) : (group?.development_remaining || 0));
        const bugRemaining = Number(useRiskPlan ? (group?.bug_remaining_risk || 0) : (group?.bug_remaining || 0));
        const currentTotal = developmentVolume + bugVolume;
        const forecastFloor = baselineTotal * p1Factor * p2Factor;
        const groupForecast = group?.is_ready ? currentTotal : Math.max(currentTotal, forecastFloor);

        forecast += groupForecast;
        currentDevelopment += developmentVolume;
        currentBugs += bugVolume;
        remainingDevelopment += developmentRemaining;
        remainingBugs += bugRemaining;
      }}

      return {{
        budget: Number(snapshot?.budget_baseline_total || 0) * p1Factor * p2Factor,
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

    function buildBurndownDatasets(p1Factor, p2Factor, useRiskPlan) {{
      const metricsByDate = new Map();
      for (const snapshot of burndownSnapshots) {{
        metricsByDate.set(String(snapshot?.date || ""), computeSnapshotMetrics(snapshot, p1Factor, p2Factor, useRiskPlan));
      }}

      const budgetData = [];
      const forecastData = [];
      const currentTotalData = [];
      const currentDevelopmentData = [];
      const currentBugData = [];
      const remainingTotalData = [];
      const remainingDevelopmentData = [];
      const remainingBugData = [];
      const developmentHoursData = [];

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
          developmentHoursData.push(null);
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
        developmentHoursData.push(planningDevelopmentHoursTotal > 0 ? planningDevelopmentHoursTotal : null);
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
        developmentHoursData,
      }};
    }}

    function renderBurndownChart() {{
      const p1Percent = parsePercentValue(p1Input.value, {planningP1Percent});
      const p2Percent = parsePercentValue(p2Input.value, {planningP2Percent});
      const useRiskPlan = Boolean(useRiskPlanCheckbox?.checked);

      if (!burndownSnapshots.length) {{
        emptyState.style.display = "block";
        chartCanvas.style.display = "none";
        statusNode.textContent = "За апрель текущего года по проекту пока нет срезов для построения диаграммы.";
        return;
      }}

      emptyState.style.display = "none";
      chartCanvas.style.display = "block";

      if (typeof Chart === "undefined") {{
        statusNode.textContent = "Не удалось загрузить библиотеку диаграмм.";
        return;
      }}

      const p1Factor = p1Percent / 100;
      const p2Factor = p2Percent / 100;
      const datasets = buildBurndownDatasets(p1Factor, p2Factor, useRiskPlan);
      const planModeText = useRiskPlan ? "Используется План с рисками." : "Используется обычный План.";
      statusNode.textContent = `P1 = ${{formatHours(p1Percent)}}%, P2 = ${{formatHours(p2Percent)}}%. Срезов в расчете: ${{burndownSnapshots.length}}. ${{planModeText}}`;
      const allChartValues = [
        ...datasets.budgetData,
        ...datasets.forecastData,
        ...datasets.currentTotalData,
        ...datasets.remainingTotalData,
        ...datasets.currentDevelopmentData,
        ...datasets.currentBugData,
        ...datasets.remainingDevelopmentData,
        ...datasets.remainingBugData,
        ...datasets.developmentHoursData,
      ].filter((value) => value !== null && value !== undefined);
      const maxChartValue = allChartValues.length
        ? Math.max(...allChartValues.map((value) => Number(value || 0)))
        : 0;
      const chartMax = maxChartValue > 0 ? maxChartValue * 1.08 : 10;

      const chartDatasets = [
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
      ];

      if (planningDevelopmentHoursTotal > 0) {{
        chartDatasets.push({{
          type: "line",
          label: "Лимит разработки с багфиксом",
          data: datasets.developmentHoursData,
          borderColor: "#d9534f",
          backgroundColor: "#d9534f",
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          spanGaps: true,
          tension: 0,
          yAxisID: "yLines",
          order: 1,
        }});
      }}

      const chartConfig = {{
        data: {{
          labels: burndownDateLabels,
          datasets: chartDatasets,
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
    useRiskPlanCheckbox?.addEventListener("change", scheduleBurndownRender);

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
    requestedCapturedForDate = str(capturedForDate or "").strip()
    usedEarlierSnapshot = False
    fallbackCapturedForDate = ""

    if snapshotRun is None and requestedCapturedForDate and availableDates:
        try:
            requestedSnapshotDate = date.fromisoformat(requestedCapturedForDate)
        except ValueError:
            requestedSnapshotDate = None
        if requestedSnapshotDate is not None:
            earlierDates: list[str] = []
            for dateValue in availableDates:
                try:
                    candidateDate = date.fromisoformat(dateValue)
                except ValueError:
                    continue
                if candidateDate < requestedSnapshotDate:
                    earlierDates.append(dateValue)
            if earlierDates:
                earlierDates.sort(reverse=True)
                fallbackCapturedForDate = earlierDates[0]
                snapshotPayload = getFilteredSnapshotIssuesForProjectByDate(
                    projectRedmineId,
                    fallbackCapturedForDate,
                    page=1,
                    pageSize=1000,
                )
                snapshotRun = snapshotPayload["snapshot_run"]
                issues = snapshotPayload["issues"]
                availableDates = [str(value) for value in snapshotPayload.get("available_dates") or []]
                usedEarlierSnapshot = snapshotRun is not None

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

    issueRowsHtml = ['<tr><td colspan="13">Загружаем задачи...</td></tr>']

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

    projectName = escape(str(snapshotRun.get("project_name") or "—"))
    capturedForDateRaw = str(snapshotRun.get("captured_for_date") or "")
    capturedForDate = escape(capturedForDateRaw or "—")
    selectedDate = capturedForDateRaw
    projectIdentifierRaw = str(
        snapshotRun.get("project_identifier")
        or (storedProject.get("identifier") if storedProject else "")
    ).strip()
    projectIdentifier = escape(projectIdentifierRaw or "—")
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
      .toolbar {{ display: flex; flex-direction: column; gap: 12px; align-items: stretch; margin: 0 0 16px; }}
      .toolbar-row {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }}
      .toolbar-row.primary {{ align-items: flex-end; }}
      .toolbar-row.secondary {{ justify-content: flex-start; }}
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
      .meta-warning {{ color: #d54343; font-weight: 700; }}
      .toolbar-row.primary form {{ display: flex; flex-direction: column; align-items: flex-start; gap: 6px; }}
      .page-size-label {{
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        gap: 6px;
        font-weight: 600;
        color: var(--text);
      }}
      .snapshot-date-label-text {{
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
      }}
      .snapshot-date-warning {{ margin-top: 0; font-size: inherit; font-weight: inherit; }}
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
      <h1>Задачи среза проекта</h1>
      <div class="toolbar">
        <div class="toolbar-row primary">
          <form method="get">
            <label for="capturedForDate"><span class="snapshot-date-label-text">Дата среза{f'<span class="meta-warning snapshot-date-warning">(Более ранний, чем запрошен)</span>' if usedEarlierSnapshot and fallbackCapturedForDate else ''}</span></label>
            <select id="capturedForDate" name="captured_for_date" onchange="this.form.submit()">
              {''.join(optionsHtml)}
            </select>
          </form>
          <label class="page-size-label" for="snapshotPageSizeInput">Задач на странице
            <input class="page-size-input" id="snapshotPageSizeInput" type="number" min="10" max="10000" step="10" value="{initialPageSize}">
          </label>
          <button type="button" class="secondary-button" id="applySnapshotPageSizeButton">Показать</button>
          <button type="button" class="secondary-button" id="exportSnapshotCsvButton">Выгрузить CSV</button>
          <button type="button" class="secondary-button" id="viewSnapshotTimeEntriesButton">Списание времени</button>
        </div>
        <div class="toolbar-row secondary">
          <button type="button" id="deleteSnapshotButton">Удалить выбранный срез</button>
          <button type="button" id="recaptureSnapshotButton">Загрузить/обновить последний срез</button>
        </div>
      </div>
      <div class="action-status" id="snapshotActionStatus"></div>
      <p class="meta">Проект: <span class="meta-strong">{projectName}</span>. Идентификатор: <span class="meta-strong">{projectIdentifier}</span>. Дата среза: {capturedForDate}. По фильтру: <span id="filteredIssuesCount">{initialFilteredIssues}</span> из {initialTotalIssues}. На странице: <span id="pageIssuesCount">{len(issues)}</span>.</p>
      <div class="summary-block">
        <table class="summary-table">
          <thead>
            <tr>
              <th style="width: 33%"></th>
              <th>Базовая оценка</th>
              <th>План</th>
              <th>План с рисками</th>
              <th colspan="2">Факт (год)</th>
              <th>% (год)</th>
              <th colspan="2">Факт (всего)</th>
              <th>% (всего)</th>
            </tr>
          </thead>
          <tbody>
              <tr>
                <th>Все задачи без фич</th>
                <td class="summary-empty"></td>
                <td class="summary-metric" id="summaryEstimated">{formatPageHours(totalEstimatedHours)}</td>
                <td class="summary-empty"></td>
                <td class="summary-metric" id="summarySpentYear" colspan="2">{formatPageHours(totalSpentHoursYear)}</td>
                <td class="summary-empty"></td>
                <td class="summary-metric" id="summarySpent" colspan="2">{formatPageHours(totalSpentHours)}</td>
                <td class="summary-empty"></td>
            </tr>
            <tr>
              <th>Разработка, ч</th>
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
              <th>Процессы разработки, ч</th>
              <td class="summary-metric" id="summaryDevelopmentProcessEstimated">{formatPageHours(developmentProcessEstimateHours)}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentProcessSpentYear">{formatPageHours(developmentProcessSpentHoursYear)}</td>
              <td class="summary-metric" id="summaryDevelopmentProcessSpent">{formatPageHours(developmentProcessSpentHours)}</td>
            </tr>
            <tr>
              <th>Ошибка, ч</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryBugEstimated">{formatPageHours(bugEstimateHours)}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryBugSpentYear" colspan="2">{formatPageHours(bugSpentHoursYear)}</td>
              <td class="summary-metric summary-percent" id="summaryBugShareYear">{formatPageHours(summaryView["bug_share_year_percent"])}%</td>
              <td class="summary-metric" id="summaryBugSpent" colspan="2">{formatPageHours(bugSpentHours)}</td>
              <td class="summary-metric summary-percent" id="summaryBugShareAll">{formatPageHours(summaryView["bug_share_all_percent"])}%</td>
            </tr>
            <tr>
              <th>Итого по разработке</th>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentTotalEstimated">{formatPageHours(summaryView["development_total_estimated_hours"])}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentGrandSpentYear" colspan="2">{formatPageHours(summaryView["development_grand_spent_hours_year"])}</td>
              <td class="summary-empty"></td>
              <td class="summary-metric" id="summaryDevelopmentGrandSpent" colspan="2">{formatPageHours(summaryView["development_grand_spent_hours"])}</td>
              <td class="summary-empty"></td>
            </tr>
            <tr>
              <th>Контроль списания по фичам</th>
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
        <span class="filter-tip">Фильтры применяются к таблице и суммам выше. Суммы считаются по всем задачам, удовлетворяющим фильтру, а не только по текущей странице.</span>
        <div class="table-actions">
          <button type="button" class="filter-reset-button is-inactive" id="resetSnapshotFiltersButton">Сбросить фильтр</button>
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
            <th class="baseline-col">Базовая оценка, ч</th>
            <th>План, ч</th>
            <th>План с рисками, ч</th>
            <th class="spent-col">Факт всего, ч</th>
            <th class="spent-year-col">Факт за год, ч</th>
            <th class="closed-col">Закрыта</th>
            <th>Исполнитель</th>
            <th class="version-col">Версия</th>
          </tr>
          <tr class="filter-head">
            <th><input class="filter-input-table" type="text" data-filter-key="issueId" data-filter-role="text"></th>
            <th><input class="filter-input-table" type="text" data-filter-key="subject" data-filter-role="text"></th>
<th class="tracker-col"><select class="filter-select-table" multiple size="3" data-filter-key="tracker" data-filter-role="multi"></select></th>
<th class="status-col"><select class="filter-select-table" multiple size="3" data-filter-key="status" data-filter-role="multi"></select></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="doneRatio" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="1" data-filter-key="doneRatio" data-filter-role="value"></div></th>
            <th class="baseline-col"><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="baseline" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="baseline" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="estimated" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="estimated" data-filter-role="value"></div></th>
            <th><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="risk" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="risk" data-filter-role="value"></div></th>
            <th class="spent-col"><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="spent" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="spent" data-filter-role="value"></div></th>
            <th class="spent-year-col"><div class="filter-number-wrap"><select class="filter-number-op" data-filter-key="spentYear" data-filter-role="op"><option value="">—</option><option value=">">></option><option value="<"><</option><option value="=">=</option></select><input class="filter-number-value" type="number" step="0.1" data-filter-key="spentYear" data-filter-role="value"></div></th>
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
        <span class="snapshot-loading-text">Обновляем таблицу...</span>
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
      const viewSnapshotTimeEntriesButton = document.getElementById("viewSnapshotTimeEntriesButton");
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
          return "—";
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
        if (tracker === "разработка") {{
          return 1;
        }}
        if (tracker === "процессы разработки") {{
          return 2;
        }}
        if (tracker === "ошибка") {{
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
          snapshotIssuesTableBody.innerHTML = '<tr><td colspan="13">По текущему фильтру задач нет.</td></tr>';
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
          const groupSubject = String(issue?.feature_group_subject || "без Feature");
          const isVirtualGroup = Boolean(issue?.feature_group_is_virtual);
          if (groupKey) {{
            const groupLink = !isVirtualGroup && groupId
              ? `<a class="issue-link" href="https://redmine.sms-it.ru/issues/${{encodeURIComponent(groupId)}}" target="_blank" rel="noreferrer">${{escapeHtml(groupId)}}</a>`
              : "";
            const groupTracker = isVirtualGroup ? "—" : escapeHtml(issue?.feature_group_tracker_name || "Feature");
            const groupStatus = isVirtualGroup ? "—" : escapeHtml(issue?.feature_group_status_name || "—");
            const groupDoneRatio = isVirtualGroup ? "—" : escapeHtml(issue?.feature_group_done_ratio ?? 0);
            const groupBaseline = isVirtualGroup ? "—" : formatFilterHours(issue?.feature_group_baseline_estimate_hours);
            const groupEstimated = isVirtualGroup ? "—" : formatFilterHours(issue?.feature_group_estimated_hours);
            const groupRisk = isVirtualGroup ? "—" : formatFilterHours(issue?.feature_group_risk_estimate_hours);
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
            const issueId = orderedIssue?.issue_redmine_id ?? "—";
            const issueLink = `https://redmine.sms-it.ru/issues/${{encodeURIComponent(issueId)}}`;
            const treeDepth = Number(orderedIssue?.__treeDepth || 0);
            rows.push(`
              <tr>
                <td class="mono"><a class="issue-link" href="${{issueLink}}" target="_blank" rel="noreferrer">${{escapeHtml(issueId)}}</a></td>
                <td class="subject-col"><span class="snapshot-child-subject" style="--snapshot-depth: ${{treeDepth}};">${{escapeHtml(orderedIssue?.subject || "—")}}</span></td>
                <td class="tracker-col">${{escapeHtml(orderedIssue?.tracker_name || "—")}}</td>
                <td class="status-col">${{escapeHtml(orderedIssue?.status_name || "—")}}</td>
                <td>${{escapeHtml(orderedIssue?.done_ratio ?? 0)}}</td>
                <td class="baseline-col">${{formatFilterHours(orderedIssue?.baseline_estimate_hours)}}</td>
                <td>${{formatFilterHours(orderedIssue?.estimated_hours)}}</td>
                <td>${{formatFilterHours(orderedIssue?.risk_estimate_hours)}}</td>
                <td class="spent-col">${{formatFilterHours(orderedIssue?.spent_hours)}}</td>
                <td class="spent-year-col">${{formatFilterHours(orderedIssue?.spent_hours_year)}}</td>
                <td class="closed-col">${{escapeHtml(formatSnapshotDateTime(orderedIssue?.closed_on))}}</td>
                <td>${{escapeHtml(orderedIssue?.assigned_to_name || "—")}}</td>
                <td class="version-col">${{escapeHtml(orderedIssue?.fixed_version_name || "—")}}</td>
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
          currentSnapshotFilterSignature = buildSnapshotFilterSignature();
          updateResetSnapshotFiltersButtonState();
        }} catch (error) {{
          window.alert("Не удалось загрузить задачи среза.");
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

        const selectedDateForDelete = String(capturedForDateSelect?.value || "{capturedForDate}");
        const response = await fetch(`/api/issues/snapshots/project/{projectRedmineId}/by-date?captured_for_date=${{encodeURIComponent(selectedDateForDelete)}}`, {{
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

      viewSnapshotTimeEntriesButton?.addEventListener("click", () => {{
        const selectedTimeEntriesDate = String(capturedForDateSelect?.value || "{capturedForDate}" || "");
        const params = new URLSearchParams();
        if (selectedTimeEntriesDate) {{
          params.set("captured_for_date", selectedTimeEntriesDate);
        }}
        window.location.href = `/projects/{projectRedmineId}/time-entries?${{params.toString()}}`;
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


SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG = [
    {"key": "id", "label": "ID", "type": "number", "sum": False, "mono": True},
    {"key": "user_name", "label": "Пользователь", "type": "text", "sum": False, "mono": False},
    {"key": "activity_name", "label": "Активность", "type": "text", "sum": False, "mono": False},
    {"key": "hours", "label": "Часы", "type": "hours", "sum": True, "mono": False},
    {"key": "project_name", "label": "Проект", "type": "text", "sum": False, "mono": False},
    {"key": "issue_subject", "label": "Тема задачи", "type": "text", "sum": False, "mono": False},
    {"key": "comments", "label": "Комментарий", "type": "text", "sum": False, "mono": False},
    {"key": "issue_tracker_name", "label": "Трекер задачи", "type": "text", "sum": False, "mono": False},
    {"key": "issue_status_name", "label": "Статус задачи", "type": "text", "sum": False, "mono": False},
    {"key": "spent_on", "label": "Дата списания", "type": "date", "sum": False, "mono": False},
    {"key": "created_on", "label": "Создано", "type": "datetime", "sum": False, "mono": False},
    {"key": "updated_on", "label": "Обновлено", "type": "datetime", "sum": False, "mono": False},
    {"key": "snapshot_run_id", "label": "ID среза", "type": "number", "sum": False, "mono": True},
    {"key": "project_redmine_id", "label": "ID проекта Redmine", "type": "number", "sum": False, "mono": True},
    {"key": "time_entry_redmine_id", "label": "ID списания", "type": "number", "sum": False, "mono": True},
    {"key": "issue_redmine_id", "label": "ID задачи", "type": "number", "sum": False, "mono": True},
    {"key": "user_id", "label": "ID пользователя", "type": "number", "sum": False, "mono": True},
    {"key": "activity_id", "label": "ID активности", "type": "number", "sum": False, "mono": True},
]

SNAPSHOT_TIME_ENTRY_MULTISELECT_KEYS = {
    "issue_tracker_name",
    "issue_status_name",
    "activity_name",
}

SNAPSHOT_TIME_ENTRY_FIXED_WIDTHS = {
    "id": "8ch",
    "user_name": "16ch",
    "activity_name": "15ch",
    "hours": "10ch",
    "snapshot_run_id": "10ch",
    "project_redmine_id": "10ch",
    "project_name": "22ch",
    "time_entry_redmine_id": "10ch",
    "issue_redmine_id": "10ch",
    "issue_subject": "64ch",
    "comments": "32ch",
    "issue_tracker_name": "23ch",
    "issue_status_name": "23ch",
    "user_id": "10ch",
    "activity_id": "10ch",
    "spent_on": "18ch",
    "created_on": "23ch",
    "updated_on": "23ch",
}


def _normalizeSnapshotTimeEntriesDateValue(value: str | None, fallback: str) -> str:
    rawValue = str(value or "").strip()
    if not rawValue:
        return fallback
    try:
        return date.fromisoformat(rawValue).isoformat()
    except ValueError:
        return fallback


def _formatSnapshotTimeEntryCellValue(columnKey: str, value: object) -> str:
    if value in (None, ""):
        return "—"
    if columnKey == "hours":
        return formatPageHours(value)
    if columnKey in {"created_on", "updated_on"}:
        return formatSnapshotPageDateTime(value)
    return str(value)


def _applySnapshotTimeEntriesFilters(
    timeEntries: list[dict[str, object]],
    filters: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    normalizedTextFilters: dict[str, str] = {}
    normalizedMultiFilters: dict[str, list[str]] = {}

    for key, rawValue in (filters or {}).items():
        if key in SNAPSHOT_TIME_ENTRY_MULTISELECT_KEYS:
            values = rawValue if isinstance(rawValue, (list, tuple, set)) else [rawValue]
            normalizedValues: list[str] = []
            for value in values:
                normalizedValue = str(value or "").strip().lower()
                if normalizedValue and normalizedValue not in normalizedValues:
                    normalizedValues.append(normalizedValue)
            if normalizedValues:
                normalizedMultiFilters[key] = normalizedValues
            continue

        normalizedValue = str(rawValue or "").strip().lower()
        if normalizedValue:
            normalizedTextFilters[key] = normalizedValue

    if not normalizedTextFilters and not normalizedMultiFilters:
        return list(timeEntries)

    filteredEntries: list[dict[str, object]] = []
    for entry in timeEntries:
        matches = True
        for column in SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG:
            renderedValue = _formatSnapshotTimeEntryCellValue(column["key"], entry.get(column["key"])).lower()

            multiFilterValues = normalizedMultiFilters.get(column["key"])
            if multiFilterValues is not None:
                if renderedValue not in multiFilterValues:
                    matches = False
                    break
                continue

            filterValue = normalizedTextFilters.get(column["key"])
            if filterValue and filterValue not in renderedValue:
                matches = False
                break
        if matches:
            filteredEntries.append(entry)

    return filteredEntries


def buildSnapshotTimeEntriesPage(
    projectRedmineId: int,
    capturedForDate: str | None,
    dateFrom: str | None,
    dateTo: str | None,
) -> str:
    today = date.today()
    defaultCapturedForDate = today.isoformat()
    defaultDateFrom = date(today.year, 1, 1).isoformat()
    defaultDateTo = today.isoformat()
    defaultPageSize = 1000

    selectedCapturedForDate = _normalizeSnapshotTimeEntriesDateValue(capturedForDate, defaultCapturedForDate)
    selectedDateFrom = _normalizeSnapshotTimeEntriesDateValue(dateFrom, defaultDateFrom)
    selectedDateTo = _normalizeSnapshotTimeEntriesDateValue(dateTo, defaultDateTo)
    if selectedDateFrom > selectedDateTo:
        selectedDateFrom, selectedDateTo = selectedDateTo, selectedDateFrom

    snapshotPayload = getSnapshotTimeEntriesForProjectByDateRange(
        projectRedmineId,
        selectedCapturedForDate,
        selectedDateFrom,
        selectedDateTo,
    )
    snapshotRun = snapshotPayload["snapshot_run"]
    timeEntries = list(snapshotPayload.get("time_entries") or [])
    storedProjects = listStoredProjects()
    storedProject = next(
        (item for item in storedProjects if int(item.get("redmine_id") or 0) == projectRedmineId),
        None,
    )
    storedProjectIdentifierRaw = str((storedProject or {}).get("identifier") or "").strip()

    if snapshotRun is None:
        navPanelHtml = buildProjectContextNavPanel(
            projectRedmineId,
            storedProjectIdentifierRaw,
            currentPage="time_entries",
        )
        return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Списание времени</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    body {{ margin: 0; font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: #ffffff; color: #16324a; }}
    main {{ max-width: 1400px; margin: 0 auto; padding: 24px 20px 48px; }}
    {buildProjectContextNavCss()}
    h1 {{ margin: 18px 0 12px; font-size: clamp(1.85rem, 4vw, 2.65rem); line-height: 1.02; letter-spacing: -0.04em; font-weight: 400; }}
    .meta {{ color: #64798d; margin: 0 0 24px; }}
    .toolbar {{ display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap; margin: 0 0 18px; }}
    .toolbar label {{ display: flex; flex-direction: column; gap: 6px; font-weight: 600; color: #16324a; }}
    .toolbar input {{ border: 1px solid #d9e5eb; border-radius: 6px; padding: 8px 10px; font: inherit; }}
    .toolbar button {{ border: 0; border-radius: 6px; padding: 10px 14px; font: inherit; font-weight: 600; cursor: pointer; background: #375d77; color: #ffffff; }}
    .empty-state {{ padding: 18px 20px; border: 1px solid #d9e5eb; border-radius: 8px; background: #f8fbfd; color: #64798d; }}
  </style>
</head>
<body>
  <main>
    {navPanelHtml}
    <h1>Списание времени</h1>
    <p class="meta">Для проекта с ID {projectRedmineId} срезы пока не найдены.</p>
    <form class="toolbar" method="get">
      <label>Дата среза
        <input type="date" name="captured_for_date" value="{escape(selectedCapturedForDate)}">
      </label>
      <label>Дата от
        <input type="date" name="date_from" value="{escape(selectedDateFrom)}">
      </label>
      <label>Дата до
        <input type="date" name="date_to" value="{escape(selectedDateTo)}">
      </label>
      <button type="submit">Показать списания</button>
    </form>
    <div class="empty-state">Для даты среза {escape(selectedCapturedForDate)} списания времени не найдены. Если срез за эту дату еще не получен, сначала загрузите его.</div>
  </main>
</body>
</html>"""

    selectedSnapshotDateRaw = str(snapshotRun.get("captured_for_date") or "")
    projectName = escape(str(snapshotRun.get("project_name") or "—"))
    projectIdentifierRaw = str(snapshotRun.get("project_identifier") or storedProjectIdentifierRaw).strip()
    projectIdentifier = escape(projectIdentifierRaw or "—")
    snapshotPageUrl = f"/projects/{projectRedmineId}/latest-snapshot-issues"
    comparePageUrl = f"/projects/{projectRedmineId}/compare-snapshots"
    if selectedSnapshotDateRaw:
        snapshotPageUrl += f"?captured_for_date={quote(selectedSnapshotDateRaw)}"
        comparePageUrl += f"?right_date={quote(selectedSnapshotDateRaw)}"
    navPanelHtml = buildProjectContextNavPanel(
        projectRedmineId,
        projectIdentifierRaw,
        currentPage="time_entries",
        snapshotUrl=snapshotPageUrl,
        compareUrl=comparePageUrl,
    )

    filteredEntries = _applySnapshotTimeEntriesFilters(timeEntries)
    totalHours = sum(float(entry.get("hours") or 0) for entry in filteredEntries)
    timeEntriesJson = json.dumps(timeEntries, ensure_ascii=False, default=str)
    columnConfigJson = json.dumps(SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG, ensure_ascii=False)
    exportUrlBase = f"/projects/{projectRedmineId}/time-entries/export.csv"
    filterOptionsByKey: dict[str, list[str]] = {}
    for column in SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG:
        columnKey = str(column["key"])
        if columnKey not in SNAPSHOT_TIME_ENTRY_MULTISELECT_KEYS:
            continue
        optionValues = sorted(
            {
                _formatSnapshotTimeEntryCellValue(columnKey, entry.get(columnKey))
                for entry in timeEntries
            },
            key=lambda value: str(value).lower(),
        )
        filterOptionsByKey[columnKey] = optionValues
    filterOptionsByKeyJson = json.dumps(filterOptionsByKey, ensure_ascii=False)
    columnWidthCss = "\n".join(
        f'.col-{columnKey} {{ width: {columnWidth}; min-width: {columnWidth}; max-width: {columnWidth}; }}'
        for columnKey, columnWidth in SNAPSHOT_TIME_ENTRY_FIXED_WIDTHS.items()
    )

    headerCells = "".join(
        f'<th class="col-{escape(str(column["key"]))}" data-column-key="{escape(str(column["key"]))}">{escape(str(column["label"]))}</th>'
        for column in SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG
    )
    filterCellsList: list[str] = []
    for column in SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG:
        columnKey = str(column["key"])
        if columnKey in SNAPSHOT_TIME_ENTRY_MULTISELECT_KEYS:
            optionsHtml = "".join(
                f'<option value="{escape(optionValue)}">{escape(optionValue)}</option>'
                for optionValue in filterOptionsByKey.get(columnKey, [])
            )
            filterCellsList.append(
                f'<th class="col-{escape(columnKey)}"><select class="filter-input-table filter-select-table" data-filter-key="{escape(columnKey)}" multiple size="3">{optionsHtml}</select></th>'
            )
        else:
            filterCellsList.append(
                f'<th class="col-{escape(columnKey)}"><input class="filter-input-table" type="text" data-filter-key="{escape(columnKey)}"></th>'
            )
    filterCells = "".join(filterCellsList)
    footerCellsList: list[str] = []
    for index, column in enumerate(SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG):
        columnKey = str(column["key"])
        if index == 0:
            footerCellsList.append(
                f'<th class="summary-label">Итого: <span id="timeEntriesFilteredCount">{len(filteredEntries)}</span></th>'
            )
            continue
        if columnKey == "hours":
            footerCellsList.append(
                f'<th class="summary-value" data-summary-key="{escape(columnKey)}"></th>'
            )
        else:
            footerCellsList.append("<th></th>")
    footerCells = "".join(footerCellsList)

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Списание времени</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Golos+Text:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #ffffff;
      --line: #d9e5eb;
      --text: #16324a;
      --muted: #64798d;
      --panel: #ffffff;
      --header: #eef6f7;
      --header-2: #f7fbfc;
      --brand: #33bdd8;
      --time-filter-top: 44px;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif; background: var(--bg); color: var(--text); }}
    main {{ max-width: 1720px; margin: 0 auto; padding: 24px 20px 48px; }}
    {buildProjectContextNavCss()}
    h1 {{ margin: 18px 0 12px; font-size: clamp(1.85rem, 4vw, 2.65rem); line-height: 1.02; letter-spacing: -0.04em; font-weight: 400; }}
    .meta {{ color: var(--muted); margin: 0 0 18px; font-size: 1rem; }}
    .meta-strong {{ color: var(--brand); font-weight: 400; }}
    .toolbar {{ display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap; margin: 0 0 18px; }}
    .toolbar label {{ display: flex; flex-direction: column; gap: 6px; font-weight: 600; color: var(--text); }}
    .toolbar input {{ border: 1px solid var(--line); border-radius: 6px; padding: 8px 10px; font: inherit; }}
    .toolbar button {{ border: 0; border-radius: 6px; padding: 10px 14px; font: inherit; font-weight: 600; cursor: pointer; background: #375d77; color: #ffffff; }}
    .secondary {{ background: #eef2f5; color: #16324a; border: 1px solid var(--line); }}
    .table-actions {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-left: auto; }}
    .pagination-wrap {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; margin: 0 0 12px; flex-wrap: wrap; }}
    .pagination-buttons {{ display: flex; gap: 8px; align-items: center; }}
    .pagination-info, .summary-note {{ color: var(--muted); font-size: 0.94rem; }}
    .page-size-label {{ display: flex; flex-direction: column; gap: 6px; color: var(--text); font-weight: 600; }}
    .page-size-hint {{ color: var(--muted); font-weight: 600; }}
    .page-size-input {{ width: 110px; }}
    .table-wrap {{ position: relative; min-height: 420px; overflow: auto; border: 1px solid var(--line); border-radius: 8px; background: var(--panel); }}
    table {{ width: 100%; border-collapse: separate; border-spacing: 0; min-width: 2200px; table-layout: fixed; background: var(--panel); }}
    th, td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    thead .header-row th {{
      position: sticky;
      top: 0;
      z-index: 4;
      background: var(--header);
      color: #426179;
      text-transform: uppercase;
      font-size: 0.74rem;
      line-height: 1.15;
    }}
    thead .filter-row th {{
      position: sticky;
      top: var(--time-filter-top);
      z-index: 3;
      background: var(--header-2);
      padding-top: 6px;
      padding-bottom: 6px;
      box-shadow: inset 0 1px 0 var(--line);
    }}
    tfoot .footer-row th {{
      position: sticky;
      bottom: 0;
      z-index: 4;
      background: #ffffff;
      color: #173b5a;
      text-transform: none;
      font-size: 0.92rem;
      box-shadow: inset 0 1px 0 var(--line);
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .mono {{ font-family: Consolas, "Courier New", monospace; white-space: nowrap; }}
    .filter-input-table {{ width: 100%; border: 1px solid var(--line); border-radius: 6px; padding: 5px 7px; font-size: 0.82rem; line-height: 1.2; background: #ffffff; color: var(--text); }}
    .filter-select-table {{ min-height: 78px; padding-top: 4px; padding-bottom: 4px; }}
    {columnWidthCss}
    .summary-label {{ font-weight: 700; white-space: nowrap; }}
    .summary-value {{ font-weight: 700; white-space: nowrap; }}
    .empty-state {{ padding: 18px 20px; border: 1px solid var(--line); border-radius: 8px; background: #f8fbfd; color: var(--muted); }}
  </style>
</head>
<body>
  <main>
    {navPanelHtml}
    <h1>Списание времени</h1>
    <p class="meta">Проект: <span class="meta-strong">{projectName}</span>. Идентификатор: <span class="meta-strong">{projectIdentifier}</span>. Дата среза: {escape(selectedSnapshotDateRaw or "—")}.</p>
    <form class="toolbar" id="timeEntriesForm" method="get">
      <label>Дата среза
        <input type="date" name="captured_for_date" value="{escape(selectedSnapshotDateRaw or selectedCapturedForDate)}">
      </label>
      <label>Дата от
        <input type="date" name="date_from" value="{escape(selectedDateFrom)}">
      </label>
      <label>Дата до
        <input type="date" name="date_to" value="{escape(selectedDateTo)}">
      </label>
      <label class="page-size-label" for="timeEntriesPageSizeInput">Записей на странице</label>
      <input class="page-size-input" id="timeEntriesPageSizeInput" type="number" min="10" max="10000" step="10" value="{defaultPageSize}">
      <div class="table-actions">
        <button type="submit">Показать списания</button>
        <button type="button" class="secondary" id="exportTimeEntriesCsvButton">Выгрузить CSV</button>
        <button type="button" class="secondary" id="resetTimeEntryFiltersButton">Сбросить фильтр</button>
      </div>
    </form>
    <div class="pagination-wrap">
      <div class="pagination-buttons">
        <button type="button" class="secondary" id="timeEntriesPrevPageButton">← Назад</button>
        <button type="button" class="secondary" id="timeEntriesNextPageButton">Вперед →</button>
      </div>
      <div class="pagination-info" id="timeEntriesPaginationInfo">Страница 1 из 1</div>
    </div>
    <p class="summary-note">По текущему диапазону найдено записей: <span id="timeEntriesVisibleCount">{len(filteredEntries)}</span>. Сумма часов: <span id="timeEntriesHoursSummary">{formatPageHours(totalHours)}</span>.</p>
    <div class="table-wrap">
      <table id="snapshotTimeEntriesTable">
        <thead>
          <tr class="header-row">
            {headerCells}
          </tr>
          <tr class="filter-row">
            {filterCells}
          </tr>
        </thead>
        <tbody id="snapshotTimeEntriesTableBody">
          <tr><td colspan="{len(SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG)}">Загружаем списания...</td></tr>
        </tbody>
        <tfoot>
          <tr class="footer-row">
            {footerCells}
          </tr>
        </tfoot>
      </table>
    </div>
    <script>
      const timeEntryColumns = {columnConfigJson};
      const timeEntryFilterOptionsByKey = {filterOptionsByKeyJson};
      const allTimeEntries = {timeEntriesJson};
      const timeEntriesTable = document.getElementById("snapshotTimeEntriesTable");
      const timeEntriesTableWrap = document.querySelector(".table-wrap");
      const timeEntriesTableBody = document.getElementById("snapshotTimeEntriesTableBody");
      const timeEntriesVisibleCount = document.getElementById("timeEntriesVisibleCount");
      const timeEntriesHoursSummary = document.getElementById("timeEntriesHoursSummary");
      const timeEntriesPageSizeInput = document.getElementById("timeEntriesPageSizeInput");
      const timeEntriesPageSizeLabel = document.querySelector('label[for="timeEntriesPageSizeInput"]');
      const timeEntriesPrevPageButton = document.getElementById("timeEntriesPrevPageButton");
      const timeEntriesNextPageButton = document.getElementById("timeEntriesNextPageButton");
      const timeEntriesPaginationInfo = document.getElementById("timeEntriesPaginationInfo");
      const exportTimeEntriesCsvButton = document.getElementById("exportTimeEntriesCsvButton");
      const resetTimeEntryFiltersButton = document.getElementById("resetTimeEntryFiltersButton");
      const timeEntriesSummaryLabelCell = document.querySelector("tfoot .summary-label");
      const timeEntriesFilterInputs = Array.from(document.querySelectorAll("[data-filter-key]"));
      const timeEntriesPageSizeStorageKey = "snapshotTimeEntriesPageSize";
      let currentTimeEntriesPage = 1;
      let currentTimeEntriesTotalPages = 1;

      if (timeEntriesSummaryLabelCell) {{
        timeEntriesSummaryLabelCell.textContent = "Итого";
      }}

      if (timeEntriesPageSizeLabel && timeEntriesPageSizeInput) {{
        timeEntriesPageSizeLabel.textContent = "";
        const pageSizeHint = document.createElement("span");
        pageSizeHint.className = "page-size-hint";
        pageSizeHint.textContent = "Записей на странице";
        timeEntriesPageSizeLabel.appendChild(pageSizeHint);
        timeEntriesPageSizeLabel.appendChild(timeEntriesPageSizeInput);
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

      function formatHours(value) {{
        const parsed = Number(value ?? 0);
        if (!Number.isFinite(parsed)) {{
          return "0,0";
        }}
        return parsed.toFixed(1).replace(".", ",");
      }}

      function formatDateTime(value) {{
        if (!value) {{
          return "—";
        }}
        return String(value).replace("T", " ").replace("+00:00", " UTC");
      }}

      function renderTimeEntryCell(entry, column) {{
        const value = entry?.[column.key];
        if (value === null || value === undefined || value === "") {{
          return "—";
        }}
        if (column.key === "hours") {{
          return formatHours(value);
        }}
        if (column.key === "created_on" || column.key === "updated_on") {{
          return formatDateTime(value);
        }}
        return String(value);
      }}

      function getTimeEntryColumn(key) {{
        return timeEntryColumns.find((column) => column.key === key) || null;
      }}

      function getCurrentTimeEntryFilters() {{
        const filters = {{}};
        for (const input of timeEntriesFilterInputs) {{
          const filterKey = String(input.dataset.filterKey || "");
          if (input instanceof HTMLSelectElement && input.multiple) {{
            const selectedValues = Array.from(input.selectedOptions)
              .map((option) => String(option.value || "").trim().toLowerCase())
              .filter(Boolean);
            if (selectedValues.length) {{
              filters[filterKey] = selectedValues;
            }}
            continue;
          }}
          const filterValue = String(input.value || "").trim().toLowerCase();
          if (filterValue) {{
            filters[filterKey] = filterValue;
          }}
        }}
        return filters;
      }}

      function getFilteredTimeEntries() {{
        const filters = getCurrentTimeEntryFilters();
        const activeKeys = Object.keys(filters).filter((key) => {{
          const value = filters[key];
          return Array.isArray(value) ? value.length > 0 : Boolean(value);
        }});
        if (!activeKeys.length) {{
          return allTimeEntries.slice();
        }}
        return allTimeEntries.filter((entry) => {{
          return activeKeys.every((key) => {{
            const column = getTimeEntryColumn(key);
            if (!column) {{
              return true;
            }}
            const renderedValue = renderTimeEntryCell(entry, column).toLowerCase();
            const filterValue = filters[key];
            if (Array.isArray(filterValue)) {{
              return filterValue.includes(renderedValue);
            }}
            return renderedValue.includes(filterValue);
          }});
        }});
      }}

      function updateTimeEntriesStickyOffsets() {{
        const headerRow = timeEntriesTable?.querySelector("thead .header-row");
        const filterRow = timeEntriesTable?.querySelector("thead .filter-row");
        if (!headerRow || !filterRow || !timeEntriesTable) {{
          return;
        }}
        const headerHeight = Math.ceil(headerRow.getBoundingClientRect().height || 44);
        timeEntriesTable.style.setProperty("--time-filter-top", `${{headerHeight}}px`);
      }}

      function updateTimeEntriesTableViewportHeight() {{
        if (!timeEntriesTableWrap) {{
          return;
        }}
        const rect = timeEntriesTableWrap.getBoundingClientRect();
        const availableHeight = Math.max(420, Math.floor(window.innerHeight - Math.max(rect.top, 0) - 12));
        timeEntriesTableWrap.style.height = `${{availableHeight}}px`;
      }}

      function updateTimeEntrySummary(filteredEntries) {{
        timeEntriesVisibleCount.textContent = String(filteredEntries.length);
        const totalHours = filteredEntries.reduce((sum, entry) => sum + Number(entry?.hours || 0), 0);
        timeEntriesHoursSummary.textContent = formatHours(totalHours);
        const countCell = document.getElementById("timeEntriesFilteredCount");
        if (countCell) {{
          countCell.textContent = String(filteredEntries.length);
        }}
        const hoursSummaryCell = document.querySelector('tfoot [data-summary-key="hours"]');
        if (hoursSummaryCell) {{
          hoursSummaryCell.textContent = formatHours(totalHours);
        }}
      }}

      function updateTimeEntriesPagination(filteredEntries) {{
        const requestedPageSize = Number(timeEntriesPageSizeInput?.value || {defaultPageSize});
        const safePageSize = Number.isFinite(requestedPageSize) ? Math.min(10000, Math.max(10, Math.floor(requestedPageSize))) : {defaultPageSize};
        timeEntriesPageSizeInput.value = String(safePageSize);
        window.localStorage.setItem(timeEntriesPageSizeStorageKey, String(safePageSize));

        currentTimeEntriesTotalPages = Math.max(1, Math.ceil(filteredEntries.length / safePageSize));
        currentTimeEntriesPage = Math.min(currentTimeEntriesPage, currentTimeEntriesTotalPages);
        const startIndex = (currentTimeEntriesPage - 1) * safePageSize;
        const pageEntries = filteredEntries.slice(startIndex, startIndex + safePageSize);

        timeEntriesPaginationInfo.textContent = `Страница ${{currentTimeEntriesPage}} из ${{currentTimeEntriesTotalPages}}`;
        timeEntriesPrevPageButton.disabled = currentTimeEntriesPage <= 1;
        timeEntriesNextPageButton.disabled = currentTimeEntriesPage >= currentTimeEntriesTotalPages;
        return pageEntries;
      }}

      function renderTimeEntriesRows(entries) {{
        if (!entries.length) {{
          timeEntriesTableBody.innerHTML = `<tr><td colspan="${{timeEntryColumns.length}}">За выбранный период списания времени не найдены.</td></tr>`;
          return;
        }}

        timeEntriesTableBody.innerHTML = entries.map((entry) => {{
          const cells = timeEntryColumns.map((column) => {{
            const valueClasses = [`col-${{column.key}}`];
            if (column.mono) {{
              valueClasses.push("mono");
            }}
            return `<td class="${{valueClasses.join(" ")}}">${{escapeHtml(renderTimeEntryCell(entry, column))}}</td>`;
          }}).join("");
          return `<tr>${{cells}}</tr>`;
        }}).join("");
      }}

      function rerenderTimeEntries(resetPage = false) {{
        const filteredEntries = getFilteredTimeEntries();
        if (resetPage) {{
          currentTimeEntriesPage = 1;
        }}
        updateTimeEntrySummary(filteredEntries);
        const pageEntries = updateTimeEntriesPagination(filteredEntries);
        renderTimeEntriesRows(pageEntries);
        updateTimeEntriesStickyOffsets();
        updateTimeEntriesTableViewportHeight();
      }}

      function buildTimeEntriesExportParams() {{
        const params = new URLSearchParams();
        const capturedForDateInput = document.querySelector('[name="captured_for_date"]');
        const dateFromInput = document.querySelector('[name="date_from"]');
        const dateToInput = document.querySelector('[name="date_to"]');
        if (capturedForDateInput?.value) {{
          params.set("captured_for_date", capturedForDateInput.value);
        }}
        if (dateFromInput?.value) {{
          params.set("date_from", dateFromInput.value);
        }}
        if (dateToInput?.value) {{
          params.set("date_to", dateToInput.value);
        }}
        const filters = getCurrentTimeEntryFilters();
        for (const [key, value] of Object.entries(filters)) {{
          if (Array.isArray(value)) {{
            value.forEach((item) => params.append(key, item));
          }} else if (value) {{
            params.set(key, value);
          }}
        }}
        return params;
      }}

      timeEntriesFilterInputs.forEach((input) => {{
        const eventName = input instanceof HTMLSelectElement ? "change" : "input";
        input.addEventListener(eventName, () => rerenderTimeEntries(true));
      }});

      timeEntriesPrevPageButton?.addEventListener("click", () => {{
        if (currentTimeEntriesPage > 1) {{
          currentTimeEntriesPage -= 1;
          rerenderTimeEntries(false);
        }}
      }});

      timeEntriesNextPageButton?.addEventListener("click", () => {{
        if (currentTimeEntriesPage < currentTimeEntriesTotalPages) {{
          currentTimeEntriesPage += 1;
          rerenderTimeEntries(false);
        }}
      }});

      timeEntriesPageSizeInput?.addEventListener("change", () => rerenderTimeEntries(true));
      resetTimeEntryFiltersButton?.addEventListener("click", () => {{
        timeEntriesFilterInputs.forEach((input) => {{
          if (input instanceof HTMLSelectElement && input.multiple) {{
            Array.from(input.options).forEach((option) => {{
              option.selected = false;
            }});
            return;
          }}
          input.value = "";
        }});
        rerenderTimeEntries(true);
      }});

      exportTimeEntriesCsvButton?.addEventListener("click", () => {{
        const params = buildTimeEntriesExportParams();
        window.location.href = `{exportUrlBase}?${{params.toString()}}`;
      }});

      const storedTimeEntriesPageSize = Number(window.localStorage.getItem(timeEntriesPageSizeStorageKey) || 0);
      if (Number.isFinite(storedTimeEntriesPageSize) && storedTimeEntriesPageSize >= 10 && storedTimeEntriesPageSize <= 10000) {{
        timeEntriesPageSizeInput.value = String(Math.floor(storedTimeEntriesPageSize));
      }}

      rerenderTimeEntries(true);
      window.addEventListener("resize", updateTimeEntriesStickyOffsets);
      window.addEventListener("resize", updateTimeEntriesTableViewportHeight);
      window.addEventListener("scroll", updateTimeEntriesTableViewportHeight, {{ passive: true }});
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
          <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">
          <span class="brand-copy">
            <strong>Redmine + Bitrix</strong>
            <span>Маршрут /Bitrix уже доступен на сайте</span>
          </span>
        </a>
        <a class="back-link" href="/">Вернуться на главную</a>
      </div>

      <p class="eyebrow">Bitrix / Test Route</p>
      <h1>Bitrix test page</h1>
      <p class="lead">
        Это тестовая страница для проверки отдельного маршрута в текущем приложении.
        Она живет рядом с основным Redmine-интерфейсом и готова для дальнейшей
        интеграции с Bitrix-формами, iframe или виджетами.
      </p>

      <div class="hero-actions">
        <a class="button button-primary" href="/">Открыть главную страницу</a>
        <a class="button button-secondary" href="/health">Проверить health endpoint</a>
      </div>

      <div class="metrics">
        <div class="metric">
          <strong>/Bitrix</strong>
          <span>Маршрут вынесен в FastAPI и доступен как отдельная страница.</span>
        </div>
        <div class="metric">
          <strong>HTML</strong>
          <span>Страница статическая и безопасно добавлена без влияния на базу данных.</span>
        </div>
        <div class="metric">
          <strong>Ready</strong>
          <span>Можно использовать как основу для дальнейшего тестирования Bitrix.</span>
        </div>
      </div>
    </section>

    <section class="grid">
      <article class="card accent-card">
        <h2>Что уже сделано</h2>
        <ul>
          <li>Поднят отдельный маршрут для страницы Bitrix внутри текущего приложения.</li>
          <li>Страница оформлена в цветах существующего интерфейса, чтобы она выглядела частью продукта.</li>
          <li>Добавлена базовая навигация обратно на главную и на health-проверку.</li>
        </ul>
      </article>

      <article class="card">
        <h2>Для чего подходит</h2>
        <p>
          Эту страницу удобно использовать как тестовую площадку перед подключением
          Bitrix24-виджетов, HTML-вставок, API-диагностики или внутренних сценариев
          интеграции.
        </p>
      </article>

      <article class="card">
        <h2>Следующий шаг</h2>
        <p>
          Если понадобится, сюда можно быстро добавить форму авторизации, webhooks,
          iframe c Bitrix или диагностические блоки для обмена данными между системами.
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
        raise HTTPException(status_code=400, detail="Дата должна быть в формате YYYY-MM-DD") from error
    return normalized


def normalizePlanningProjectPayload(payload: PlanningProjectPayload) -> dict[str, object]:
    projectName = str(payload.project_name or "").strip()
    if not projectName:
        raise HTTPException(status_code=400, detail="Название проекта обязательно")

    return {
        "direction": _normalizePlanningProjectText(payload.direction),
        "project_name": projectName,
        "redmine_identifier": _normalizePlanningProjectText(payload.redmine_identifier),
        "pm_name": _normalizePlanningProjectText(payload.pm_name),
        "customer": _normalizePlanningProjectText(payload.customer),
        "start_date": _normalizePlanningProjectDate(payload.start_date),
        "end_date": _normalizePlanningProjectDate(payload.end_date),
        "development_hours": payload.development_hours,
        "year_1": payload.year_1,
        "hours_1": payload.hours_1,
        "year_2": payload.year_2,
        "hours_2": payload.hours_2,
        "year_3": payload.year_3,
        "hours_3": payload.hours_3,
        "baseline_estimate_hours": payload.baseline_estimate_hours,
        "p1": payload.p1,
        "p2": payload.p2,
        "estimate_doc_url": _normalizePlanningProjectText(payload.estimate_doc_url),
        "bitrix_url": _normalizePlanningProjectText(payload.bitrix_url),
        "comment_text": _normalizePlanningProjectText(payload.comment_text),
        "question_flag": bool(payload.question_flag),
        "is_closed": bool(payload.is_closed),
    }


def buildProjectsSummaryPage() -> str:
    todayIso = date.today().isoformat()
    directions = listPlanningDirections()
    if "КОТ" not in directions:
        directions = ["КОТ", *directions]
    directionOptionsHtml = "".join(
        f'<option value="{escape(direction)}"{" selected" if direction == "КОТ" else ""}>{escape(direction)}</option>'
        for direction in directions
    )
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Сводка по проектам</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    :root {{
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
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    main {{
      max-width: 1480px;
      margin: 0 auto;
      padding: 24px 20px 56px;
    }}
    .page-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      margin: 0 0 18px;
    }}
    .brand {{
      display: inline-flex;
      align-items: center;
      text-decoration: none;
    }}
    .brand img {{
      width: 220px;
      max-width: 100%;
      height: auto;
      display: block;
    }}
    .head-actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .head-actions a {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
      padding: 10px 14px;
      border-radius: 6px;
      text-decoration: none;
      font-weight: 700;
      box-shadow: var(--shadow-soft);
    }}
    .head-actions a.home-link {{
      background: var(--yellow-109);
      color: #16324a;
    }}
    .head-actions a.planning-link {{
      background: var(--blue-302);
      color: #ffffff;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(2.15rem, 4.9vw, 3.6rem);
      line-height: 0.98;
      letter-spacing: -0.04em;
      font-weight: 400;
    }}
    .lead {{
      margin: 0 0 20px;
      color: var(--muted);
      line-height: 1.6;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 20px;
      box-shadow: var(--shadow-soft);
      margin: 0 0 18px;
    }}
    .controls {{
      display: grid;
      grid-template-columns: repeat(4, minmax(180px, 1fr));
      gap: 14px 16px;
      align-items: end;
    }}
    .field {{
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    .field label {{
      font-weight: 700;
    }}
    .field input,
    .field select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px 12px;
      font: inherit;
      color: var(--text);
      background: #ffffff;
    }}
    .checkbox-field {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 42px;
      font-weight: 600;
      color: var(--text);
    }}
    .checkbox-field input {{
      width: 16px;
      height: 16px;
      margin: 0;
    }}
    button {{
      border: 0;
      border-radius: 6px;
      padding: 11px 18px;
      font: inherit;
      font-weight: 700;
      color: #ffffff;
      cursor: pointer;
      background: var(--orange-1585);
      box-shadow: var(--shadow-soft);
    }}
    .meta {{
      min-height: 22px;
      margin: 0 0 12px;
      color: var(--muted);
    }}
    .table-wrap {{
      overflow: auto;
      position: relative;
      height: calc(100vh - 260px);
      max-height: calc(100vh - 260px);
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
    }}
    table {{
      width: 100%;
      min-width: 1180px;
      border-collapse: collapse;
      background: #ffffff;
    }}
    th, td {{
      padding: 11px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: middle;
    }}
    th {{
      background: #eef6f7;
      color: #426179;
      text-transform: uppercase;
      font-size: 0.78rem;
      line-height: 1.2;
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .mono {{ font-family: Consolas, "Courier New", monospace; }}
    .empty-state {{
      padding: 28px 20px;
      color: var(--muted);
      text-align: center;
    }}
    @media (max-width: 980px) {{
      .controls {{
        grid-template-columns: repeat(2, minmax(180px, 1fr));
      }}
    }}
    @media (max-width: 700px) {{
      .page-head {{
        flex-direction: column;
        align-items: flex-start;
      }}
      .head-actions {{
        justify-content: flex-start;
      }}
      .controls {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <div class="page-head">
      <a class="brand" href="/" aria-label="На главную">
        <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">
      </a>
      <div class="head-actions">
        <a class="home-link" href="/">Главная</a>
        <a class="planning-link" href="/planning-projects">Планирование проектов</a>
      </div>
    </div>

    <h1>Сводка по проектам</h1>
    <p class="lead">Сводим плановые данные из планирования проектов и фактические часы разработки по последним срезам на выбранную дату.</p>

    <section class="panel">
      <div class="controls">
        <div class="field">
          <label for="projectsSummaryDateInput">Дата отчета</label>
          <input id="projectsSummaryDateInput" type="date" value="{todayIso}">
        </div>
        <div class="field">
          <label for="projectsSummaryDirectionSelect">Направление</label>
          <select id="projectsSummaryDirectionSelect">
            <option value="">Все направления</option>
            {directionOptionsHtml}
          </select>
        </div>
        <label class="checkbox-field" for="projectsSummaryClosedCheckbox">
          <input id="projectsSummaryClosedCheckbox" type="checkbox">
          <span>Закрытые проекты</span>
        </label>
        <button type="button" id="projectsSummaryRefreshButton">Показать сводку</button>
      </div>
    </section>

    <section class="panel">
      <p class="meta" id="projectsSummaryMeta">Загрузка...</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Направление</th>
              <th>Заказчик</th>
              <th>Название проекта</th>
              <th>Идентификатор в Redmine</th>
              <th>ПМ</th>
              <th>Часы разработки с багфиксом</th>
              <th>Часы за год отчета</th>
              <th>Разработка: факт за год, ч</th>
            </tr>
          </thead>
          <tbody id="projectsSummaryTableBody">
            <tr><td colspan="8" class="empty-state">Загружаем сводку...</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    const projectsSummaryDateInput = document.getElementById("projectsSummaryDateInput");
    const projectsSummaryDirectionSelect = document.getElementById("projectsSummaryDirectionSelect");
    const projectsSummaryClosedCheckbox = document.getElementById("projectsSummaryClosedCheckbox");
    const projectsSummaryRefreshButton = document.getElementById("projectsSummaryRefreshButton");
    const projectsSummaryMeta = document.getElementById("projectsSummaryMeta");
    const projectsSummaryTableBody = document.getElementById("projectsSummaryTableBody");

    function formatSummaryHours(value) {{
      if (value === null || value === undefined || value === "") {{
        return projectsSummaryStrings.dash;
      }}
      const number = Number(value);
      if (!Number.isFinite(number)) {{
        return projectsSummaryStrings.dash;
      }}
      return number.toLocaleString("ru-RU", {{ minimumFractionDigits: 1, maximumFractionDigits: 1 }});
    }}

    function formatSummaryText(value) {{
      const text = String(value ?? "").trim();
      return text || projectsSummaryStrings.dash;
    }}

    function buildProjectsSummaryMetaText(groupsCount, rowsCount) {{
      return `\u0414\u0430\u0442\u0430 \u043e\u0442\u0447\u0435\u0442\u0430: ${{currentProjectsSummaryReportDate}}. \u0413\u043e\u0434 \u043e\u0442\u0447\u0435\u0442\u0430: ${{currentProjectsSummaryReportYear || projectsSummaryStrings.dash}}. \u0413\u0440\u0443\u043f\u043f: ${{groupsCount}}. \u0421\u0442\u0440\u043e\u043a: ${{rowsCount}}.`;
    }}

    function renderProjectsSummaryRows(rows) {{
      if (!rows.length) {{
        projectsSummaryTableBody.innerHTML = '<tr><td colspan="8" class="empty-state">По выбранным условиям записей не найдено.</td></tr>';
        return;
      }}

      projectsSummaryTableBody.innerHTML = rows.map((row) => `
        <tr>
          <td>${{formatSummaryText(row.direction)}}</td>
          <td>${{formatSummaryText(row.customer)}}</td>
          <td>${{formatSummaryText(row.project_name)}}</td>
          <td class="mono">${{formatSummaryText(row.redmine_identifier)}}</td>
          <td>${{formatSummaryText(row.pm_name)}}</td>
          <td>${{formatSummaryHours(row.development_hours)}}</td>
          <td>${{formatSummaryHours(row.report_year_hours)}}</td>
          <td>${{formatSummaryHours(row.development_spent_hours_year)}}</td>
        </tr>
      `).join("");
    }}

    async function loadProjectsSummary() {{
      projectsSummaryMeta.textContent = "Загружаем сводку...";
      projectsSummaryTableBody.innerHTML = '<tr><td colspan="8" class="empty-state">Загружаем сводку...</td></tr>';
      const params = new URLSearchParams();
      params.set("report_date", String(projectsSummaryDateInput.value || "{todayIso}"));
      params.set("direction", String(projectsSummaryDirectionSelect.value || ""));
      params.set("is_closed", projectsSummaryClosedCheckbox.checked ? "true" : "false");

      try {{
        const response = await fetch(`/api/projects-summary-v2?${{params.toString()}}`);
        const payload = await response.json();
        if (!response.ok) {{
          throw new Error(payload.detail || "Не удалось загрузить сводку по проектам.");
        }}

        const rows = Array.isArray(payload.projects) ? payload.projects : [];
        projectsSummaryMeta.textContent = `Дата отчета: ${{payload.report_date}}. Год отчета: ${{payload.report_year}}. Записей: ${{rows.length}}.`;
        renderProjectsSummaryRows(rows);
      }} catch (error) {{
        projectsSummaryMeta.textContent = "Ошибка";
        projectsSummaryTableBody.innerHTML = '<tr><td colspan="8" class="empty-state">Не удалось загрузить сводку.</td></tr>';
      }}
    }}

    projectsSummaryRefreshButton?.addEventListener("click", loadProjectsSummary);
    projectsSummaryDateInput?.addEventListener("change", loadProjectsSummary);
    projectsSummaryDirectionSelect?.addEventListener("change", loadProjectsSummary);
    projectsSummaryClosedCheckbox?.addEventListener("change", loadProjectsSummary);

    projectsSummaryHeaderCells.forEach((cell, index) => {{
      const key = projectsSummaryColumnKeys[index] || "";
      if (!key) {{
        return;
      }}
      cell.dataset.sortKey = key;
      cell.addEventListener("click", () => {{
        if (projectsSummarySortKey === key) {{
          projectsSummarySortDirection = projectsSummarySortDirection === "asc" ? "desc" : "asc";
        }} else {{
          projectsSummarySortKey = key;
          projectsSummarySortDirection = "asc";
        }}
        refreshProjectsSummaryView();
      }});
    }});

    projectsSummaryFilterInputs.forEach((input) => {{
      const key = String(input.dataset.filterKey || "");
      const eventName = input.tagName === "SELECT" ? "change" : "input";
      input.addEventListener(eventName, () => {{
        projectsSummaryFilters[key] = String(input.value || "");
        refreshProjectsSummaryView();
      }});
    }});

    updateProjectsSummarySortIndicators();

    projectsSummaryHeaderCells.forEach((cell, index) => {{
      const key = projectsSummaryColumnKeys[index] || "";
      if (!key) {{
        return;
      }}
      cell.dataset.sortKey = key;
      cell.addEventListener("click", () => {{
        if (projectsSummarySortKey === key) {{
          projectsSummarySortDirection = projectsSummarySortDirection === "asc" ? "desc" : "asc";
        }} else {{
          projectsSummarySortKey = key;
          projectsSummarySortDirection = "asc";
        }}
        refreshProjectsSummaryView();
      }});
    }});

    projectsSummaryFilterInputs.forEach((input) => {{
      const key = String(input.dataset.filterKey || "");
      const eventName = input.tagName === "SELECT" ? "change" : "input";
      input.addEventListener(eventName, () => {{
        projectsSummaryFilters[key] = String(input.value || "");
        refreshProjectsSummaryView();
      }});
    }});

    updateProjectsSummarySortIndicators();

    projectsSummaryHeaderCells.forEach((cell, index) => {{
      const key = projectsSummaryColumnKeys[index] || "";
      if (!key) {{
        return;
      }}
      cell.dataset.sortKey = key;
      cell.addEventListener("click", () => {{
        if (projectsSummarySortKey === key) {{
          projectsSummarySortDirection = projectsSummarySortDirection === "asc" ? "desc" : "asc";
        }} else {{
          projectsSummarySortKey = key;
          projectsSummarySortDirection = "asc";
        }}
        refreshProjectsSummaryView();
      }});
    }});

    projectsSummaryFilterInputs.forEach((input) => {{
      const key = String(input.dataset.filterKey || "");
      input.addEventListener("change", () => {{
        projectsSummaryFilters[key] = String(input.value || "");
        refreshProjectsSummaryView();
      }});
    }});

    updateProjectsSummarySortIndicators();

    projectsSummaryHeaderCells.forEach((cell, index) => {{
      const key = projectsSummaryColumnKeys[index] || "";
      if (!key) {{
        return;
      }}
      cell.dataset.sortKey = key;
      cell.addEventListener("click", () => {{
        if (projectsSummarySortKey === key) {{
          projectsSummarySortDirection = projectsSummarySortDirection === "asc" ? "desc" : "asc";
        }} else {{
          projectsSummarySortKey = key;
          projectsSummarySortDirection = "asc";
        }}
        refreshProjectsSummaryView();
      }});
    }});

    projectsSummaryFilterInputs.forEach((input) => {{
      const key = String(input.dataset.filterKey || "");
      input.addEventListener("change", () => {{
        projectsSummaryFilters[key] = String(input.value || "");
        refreshProjectsSummaryView();
      }});
    }});

    updateProjectsSummarySortIndicators();

    loadProjectsSummary();
  </script>
</body>
</html>"""


def buildStrangeSnapshotIssuesPage() -> str:
    diagnostics = getLatestSnapshotIssuesWithExternalParents()
    issues = diagnostics.get("issues") or []
    checkedCount = int(diagnostics.get("checked_count") or 0)
    errorCount = int(diagnostics.get("error_count") or 0)

    rowsHtml = ""
    if issues:
        for issue in issues:
            projectName = escape(str(issue.get("project_name") or "—"))
            projectIdentifier = str(issue.get("project_identifier") or "")
            projectIdentifierHtml = (
                f'<a class="project-link" href="{escape(buildProjectRedmineIssuesUrl(projectIdentifier))}" target="_blank" rel="noreferrer">{escape(projectIdentifier)}</a>'
                if projectIdentifier
                else "—"
            )
            issueId = int(issue.get("issue_redmine_id") or 0)
            issueUrl = f"{config.redmineUrl.rstrip('/')}/issues/{issueId}"
            parentIssueId = int(issue.get("parent_issue_redmine_id") or 0)
            parentIssueUrl = f"{config.redmineUrl.rstrip('/')}/issues/{parentIssueId}"
            parentProjectIdentifier = str(issue.get("parent_project_identifier") or "")
            parentProjectIdentifierHtml = (
                f'<a class="project-link" href="{escape(buildProjectRedmineIssuesUrl(parentProjectIdentifier))}" target="_blank" rel="noreferrer">{escape(parentProjectIdentifier)}</a>'
                if parentProjectIdentifier
                else "—"
            )
            rowsHtml += f"""
            <tr>
              <td>{escape(str(issue.get("captured_for_date") or "—"))}</td>
              <td>{projectName}</td>
              <td>{projectIdentifierHtml}</td>
              <td class="mono"><a class="issue-link" href="{escape(issueUrl)}" target="_blank" rel="noreferrer">{issueId}</a></td>
              <td>{escape(str(issue.get("subject") or "—"))}</td>
              <td>{escape(str(issue.get("tracker_name") or "—"))}</td>
              <td>{escape(str(issue.get("status_name") or "—"))}</td>
              <td class="mono"><a class="issue-link" href="{escape(parentIssueUrl)}" target="_blank" rel="noreferrer">{parentIssueId}</a></td>
              <td>{escape(str(issue.get("parent_issue_subject") or "—"))}</td>
              <td>{escape(str(issue.get("parent_project_name") or "—"))}</td>
              <td>{parentProjectIdentifierHtml}</td>
            </tr>
            """
    else:
        rowsHtml = """
            <tr>
              <td colspan="11" class="empty-cell">По последним срезам такие задачи не найдены.</td>
            </tr>
        """

    warningHtml = ""
    if errorCount:
        warningHtml = f'<p class="warning">Не удалось проверить {errorCount} родительских задач в Redmine, поэтому список может быть неполным.</p>'

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Странные задачи</title>
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
      <a class="brand" href="/"><img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ"></a>
      <div class="actions">
        <a class="button button-home" href="/">На главную</a>
        <a class="button button-projects" href="/#projects-table">Проекты</a>
      </div>
    </div>
    <h1>Странные задачи по последним срезам</h1>
    <p class="meta">Показываются задачи из последних срезов проектов, у которых в Redmine родительская задача относится к другому проекту. Проверено кандидатов: {checkedCount}. Найдено странных задач: {len(issues)}.</p>
    {warningHtml}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Дата среза</th>
            <th>Проект</th>
            <th>Идентификатор</th>
            <th>ID задачи</th>
            <th>Тема задачи</th>
            <th>Трекер</th>
            <th>Статус</th>
            <th>ID родителя</th>
            <th>Тема родителя</th>
            <th>Проект родителя</th>
            <th>Идентификатор родителя</th>
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
    currentYear = datetime.now(UTC).year
    defaultYear1 = currentYear - 1
    defaultYear2 = currentYear
    defaultYear3 = currentYear + 1
    return """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Планирование проектов</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
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
      font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
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
      font-size: clamp(2.15rem, 4.9vw, 3.6rem);
      line-height: 0.98;
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
      gap: 16px 18px;
    }
    .form-row {
      display: grid;
      grid-template-columns: repeat(4, minmax(160px, 1fr));
      gap: 14px 16px;
      align-items: end;
    }
    .form-row.metrics-row {
      grid-template-columns: minmax(180px, 1.15fr) auto minmax(180px, 1fr) minmax(170px, 1fr) minmax(220px, 1fr);
    }
    .form-panels {
      display: grid;
      grid-template-columns: minmax(280px, 34%) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }
    .subpanel {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #f8fbfd;
      padding: 16px;
    }
    .subpanel-title {
      margin: 0 0 12px;
      font-size: 0.96rem;
      font-weight: 700;
      color: #426179;
    }
    .subpanel-note {
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.45;
    }
    .years-grid {
      display: grid;
      grid-template-columns: minmax(90px, 110px) minmax(0, 1fr);
      gap: 12px 14px;
      align-items: end;
    }
    .links-grid {
      display: grid;
      gap: 14px;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
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
    input[type="number"]::-webkit-outer-spin-button,
    input[type="number"]::-webkit-inner-spin-button {
      -webkit-appearance: none;
      margin: 0;
    }
    input[type="number"] {
      -moz-appearance: textfield;
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
    .checkbox-field {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 42px;
      font-weight: 600;
      color: var(--text);
    }
    .table-action-button {
      background: #eef2f5;
      color: var(--text);
      border: 1px solid var(--line);
      box-shadow: none;
    }
    .checkbox-field input {
      width: 16px;
      height: 16px;
      margin: 0;
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      max-height: min(68vh, 980px);
      background: #ffffff;
    }
    table {
      width: 100%;
      min-width: 2500px;
      border-collapse: collapse;
      background: #ffffff;
    }
    th, td {
      padding: 11px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    .planning-projects-table th {
      background: #eef6f7;
      color: #426179;
      text-transform: uppercase;
      font-size: 0.78rem;
      line-height: 1.2;
    }
    .planning-projects-table thead tr:first-child th {
      position: sticky;
      top: 0;
      z-index: 3;
      cursor: pointer;
    }
    .planning-projects-table thead tr:first-child th.no-sort {
      cursor: default;
    }
    .planning-projects-table thead tr.filter-row th {
      position: sticky;
      top: 42px;
      z-index: 2;
      padding: 8px 10px;
      background: #f8fbfd;
      text-transform: none;
      font-size: 0.78rem;
      font-weight: 600;
    }
    .planning-filter-input,
    .planning-filter-select {
      width: 100%;
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 6px 8px;
      font: inherit;
      font-size: 0.82rem;
      color: var(--text);
      background: #ffffff;
      box-sizing: border-box;
    }
    .planning-filter-input::placeholder {
      color: #96a7b7;
    }
    .sort-indicator {
      display: inline-block;
      margin-left: 6px;
      color: #7f93a6;
      font-size: 0.72rem;
      vertical-align: middle;
    }
    tr:last-child td { border-bottom: 0; }
    .mono { font-family: Consolas, "Courier New", monospace; }
    th.direction-col, td.direction-col { width: 12ch; min-width: 12ch; max-width: 12ch; white-space: nowrap; }
    th.closed-col, td.closed-col { width: 8ch; min-width: 8ch; max-width: 8ch; white-space: nowrap; text-align: center; }
    th.customer-col, td.customer-col { width: 180px; }
    th.project-name-col, td.project-name-col { width: 15ch; min-width: 15ch; max-width: 15ch; }
    th.identifier-col, td.identifier-col { width: 276px; min-width: 276px; max-width: 276px; }
    th.pm-col, td.pm-col { width: 150px; }
    th.start-date-col, td.start-date-col { width: 10ch; min-width: 10ch; max-width: 10ch; white-space: nowrap; }
    th.end-date-col, td.end-date-col { width: 16ch; min-width: 16ch; max-width: 16ch; white-space: nowrap; }
    th.development-col, td.development-col { width: 190px; min-width: 190px; }
    th.year-col, td.year-col { width: 10ch; min-width: 10ch; max-width: 10ch; white-space: nowrap; }
    th.year-hours-col, td.year-hours-col { width: 14ch; min-width: 14ch; max-width: 14ch; white-space: nowrap; }
    th.baseline-col, td.baseline-col { width: 140px; }
    th.p-col, td.p-col { width: 10ch; min-width: 10ch; max-width: 10ch; }
    th.p2-col, td.p2-col { width: 18ch; min-width: 18ch; max-width: 18ch; }
    th.doc-col, td.doc-col,
    th.bitrix-col, td.bitrix-col,
    th.comment-col, td.comment-col { width: 30ch; min-width: 30ch; max-width: 30ch; white-space: nowrap; }
    th.actions-col, td.actions-col { width: 160px; white-space: nowrap; }
    .link-cell a {
      color: var(--blue-302);
      text-decoration: none;
      border-bottom: 1px dashed currentColor;
      display: inline-block;
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .link-cell a:hover {
      color: var(--orange-1585);
      border-bottom-style: solid;
    }
    .row-actions {
      display: flex;
      gap: 6px;
      flex-wrap: nowrap;
    }
    .row-actions button {
      width: 32px;
      height: 32px;
      padding: 0;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-size: 0.92rem;
      border-radius: 8px;
    }
    .row-actions button svg {
      width: 16px;
      height: 16px;
      display: block;
    }
    .row-actions .edit-button {
      background: var(--cyan-310);
      color: #16324a;
    }
    .row-actions .copy-button {
      background: #eef2f5;
      color: var(--blue-302);
      border: 1px solid var(--line);
      box-shadow: none;
    }
    .row-actions .delete-button {
      background: #eef2f5;
      color: #d54343;
      border: 1px solid #f0c8c8;
      box-shadow: none;
    }
    .planning-projects-table tbody tr.question-flag-row td:not(.actions-col) {
      color: #c13b3b;
    }
    .planning-projects-table tbody tr.question-flag-row .link-cell a {
      color: inherit;
      border-bottom-color: currentColor;
    }
    .empty-state {
      padding: 28px 20px;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 1100px) {
      .form-row { grid-template-columns: repeat(2, minmax(180px, 1fr)); }
      .form-panels { grid-template-columns: 1fr; }
    }
    @media (max-width: 700px) {
      .page-head {
        flex-direction: column;
        align-items: flex-start;
      }
      .head-actions {
        justify-content: flex-start;
      }
      .form-row { grid-template-columns: 1fr; }
      .years-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <div class="page-head">
      <a class="brand" href="/" aria-label="На главную">
        <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">
      </a>
    </div>

    <h1>Планирование проектов</h1>
    <p class="lead">Инициализация проектов, критерии успешности, связь с контрактами</p>

    <section class="panel">
      <div class="table-meta">
        <h2 style="margin:0;">Таблица планирования</h2>
        <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap; justify-content:flex-end;">
          <span id="planningProjectsCount">Загрузка...</span>
          <button type="button" id="resetPlanningProjectsFiltersButton" class="table-action-button">Сбросить фильтры</button>
          <button type="button" id="resetPlanningProjectsSortingButton" class="table-action-button">Сбросить сортировку</button>
          <button type="button" id="exportPlanningProjectsButton">Выгрузить в Excel</button>
        </div>
      </div>
      <div class="table-wrap">
        <table class="planning-projects-table">
          <thead>
            <tr>
              <th class="actions-col no-sort">Действия</th>
              <th class="direction-col" data-sort-key="direction">Направление<span class="sort-indicator"></span></th>
              <th class="closed-col" data-sort-key="is_closed">&#1047;&#1072;&#1082;&#1088;&#1099;&#1090;<span class="sort-indicator"></span></th>
              <th class="customer-col" data-sort-key="customer">Заказчик<span class="sort-indicator"></span></th>
              <th class="project-name-col" data-sort-key="project_name">Название проекта<span class="sort-indicator"></span></th>
              <th class="identifier-col" data-sort-key="redmine_identifier">Идентификатор в Redmine<span class="sort-indicator"></span></th>
              <th class="pm-col" data-sort-key="pm_name">ПМ<span class="sort-indicator"></span></th>
              <th class="start-date-col" data-sort-key="start_date">Дата старта<span class="sort-indicator"></span></th>
              <th class="end-date-col" data-sort-key="end_date">Дата окончания<span class="sort-indicator"></span></th>
              <th class="development-col" data-sort-key="development_hours">Часы разработки с багфиксом<span class="sort-indicator"></span></th>
              <th class="year-col" data-sort-key="year_1">Год 1<span class="sort-indicator"></span></th>
              <th class="year-hours-col" data-sort-key="hours_1">Часы 1<span class="sort-indicator"></span></th>
              <th class="year-col" data-sort-key="year_2">Год 2<span class="sort-indicator"></span></th>
              <th class="year-hours-col" data-sort-key="hours_2">Часы 2<span class="sort-indicator"></span></th>
              <th class="year-col" data-sort-key="year_3">Год 3<span class="sort-indicator"></span></th>
              <th class="year-hours-col" data-sort-key="hours_3">Часы 3<span class="sort-indicator"></span></th>
              <th class="baseline-col" data-sort-key="baseline_estimate_hours">Базовая оценка<span class="sort-indicator"></span></th>
              <th class="p-col" data-sort-key="p1">P1 (факт / база), %<span class="sort-indicator"></span></th>
              <th class="p2-col" data-sort-key="p2">P2 (факт с багами / факт), %<span class="sort-indicator"></span></th>
              <th class="doc-col" data-sort-key="estimate_doc_url">Док с оценкой<span class="sort-indicator"></span></th>
              <th class="bitrix-col" data-sort-key="bitrix_url">Bitrix<span class="sort-indicator"></span></th>
              <th class="comment-col" data-sort-key="comment_text">Комментарий<span class="sort-indicator"></span></th>
            </tr>
            <tr class="filter-row">
              <th class="actions-col no-sort"></th>
              <th class="direction-col"><input class="planning-filter-input" data-filter-key="direction" type="text" placeholder="Фильтр"></th>
              <th class="closed-col">
                <select class="planning-filter-select" data-filter-key="is_closed">
                  <option value="">Все</option>
                  <option value="true">Да</option>
                  <option value="false">Нет</option>
                </select>
              </th>
              <th class="customer-col"><input class="planning-filter-input" data-filter-key="customer" type="text" placeholder="Фильтр"></th>
              <th class="project-name-col"><input class="planning-filter-input" data-filter-key="project_name" type="text" placeholder="Фильтр"></th>
              <th class="identifier-col"><input class="planning-filter-input" data-filter-key="redmine_identifier" type="text" placeholder="Фильтр"></th>
              <th class="pm-col"><input class="planning-filter-input" data-filter-key="pm_name" type="text" placeholder="Фильтр"></th>
              <th class="start-date-col"><input class="planning-filter-input" data-filter-key="start_date" type="text" placeholder="Фильтр"></th>
              <th class="end-date-col"><input class="planning-filter-input" data-filter-key="end_date" type="text" placeholder="Фильтр"></th>
              <th class="development-col"><input class="planning-filter-input" data-filter-key="development_hours" type="text" placeholder="Фильтр"></th>
              <th class="year-col"><input class="planning-filter-input" data-filter-key="year_1" type="text" placeholder="Фильтр"></th>
              <th class="year-hours-col"><input class="planning-filter-input" data-filter-key="hours_1" type="text" placeholder="Фильтр"></th>
              <th class="year-col"><input class="planning-filter-input" data-filter-key="year_2" type="text" placeholder="Фильтр"></th>
              <th class="year-hours-col"><input class="planning-filter-input" data-filter-key="hours_2" type="text" placeholder="Фильтр"></th>
              <th class="year-col"><input class="planning-filter-input" data-filter-key="year_3" type="text" placeholder="Фильтр"></th>
              <th class="year-hours-col"><input class="planning-filter-input" data-filter-key="hours_3" type="text" placeholder="Фильтр"></th>
              <th class="baseline-col"><input class="planning-filter-input" data-filter-key="baseline_estimate_hours" type="text" placeholder="Фильтр"></th>
              <th class="p-col"><input class="planning-filter-input" data-filter-key="p1" type="text" placeholder="Фильтр"></th>
              <th class="p2-col"><input class="planning-filter-input" data-filter-key="p2" type="text" placeholder="Фильтр"></th>
              <th class="doc-col"><input class="planning-filter-input" data-filter-key="estimate_doc_url" type="text" placeholder="Фильтр"></th>
              <th class="bitrix-col"><input class="planning-filter-input" data-filter-key="bitrix_url" type="text" placeholder="Фильтр"></th>
              <th class="comment-col"><input class="planning-filter-input" data-filter-key="comment_text" type="text" placeholder="Фильтр"></th>
            </tr>
          </thead>
          <tbody id="planningProjectsTableBody">
            <tr><td colspan="22" class="empty-state">Загружаем записи...</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2 id="planningFormTitle">Новая запись</h2>
      <form id="planningProjectForm">
        <input type="hidden" id="planningProjectId">
        <div class="form-grid">
          <div class="form-row">
            <div class="field">
              <label for="planningProjectDirection">Направление</label>
              <input id="planningProjectDirection" type="text">
            </div>
            <div class="field">
              <label for="planningProjectCustomer">Заказчик</label>
              <input id="planningProjectCustomer" type="text">
            </div>
            <div class="field">
              <label for="planningProjectName">Название проекта</label>
              <input id="planningProjectName" type="text" required>
            </div>
            <label class="checkbox-field" for="planningProjectClosed">
              <input id="planningProjectClosed" type="checkbox">
              <span>&#1047;&#1072;&#1082;&#1088;&#1099;&#1090;</span>
            </label>
          </div>
          <div class="form-row">
            <div class="field">
              <label for="planningProjectIdentifier">Идентификатор в Redmine</label>
              <input id="planningProjectIdentifier" type="text">
            </div>
            <div class="field">
              <label for="planningProjectPm">ПМ</label>
              <input id="planningProjectPm" type="text">
            </div>
            <div class="field">
              <label for="planningProjectStartDate">Дата старта</label>
              <input id="planningProjectStartDate" type="date">
            </div>
            <div class="field">
              <label for="planningProjectEndDate">Дата окончания</label>
              <input id="planningProjectEndDate" type="date">
            </div>
          </div>
          <div class="form-row metrics-row">
            <div class="field">
              <label for="planningProjectDevelopmentHours">Часы разработки с багфиксом</label>
              <input id="planningProjectDevelopmentHours" type="number" step="0.1" inputmode="decimal">
            </div>
            <label class="checkbox-field" for="planningProjectQuestionFlag">
              <input id="planningProjectQuestionFlag" type="checkbox">
              <span>?</span>
            </label>
            <div class="field">
              <label for="planningProjectBaselineEstimate">Базовая оценка</label>
              <input id="planningProjectBaselineEstimate" type="number" step="0.1" inputmode="decimal">
            </div>
            <div class="field">
              <label for="planningProjectP1">P1 (факт / база), %</label>
              <input id="planningProjectP1" type="number" step="0.1" inputmode="decimal">
            </div>
            <div class="field">
              <label for="planningProjectP2">P2 (факт с багами / факт), %</label>
              <input id="planningProjectP2" type="number" step="0.1" inputmode="decimal">
            </div>
          </div>
          <div class="form-panels">
            <section class="subpanel">
              <h3 class="subpanel-title">План по годам по разработке с багфиксом</h3>
              <p class="subpanel-note">Если План по годам не заполнен, то подразумевается, что все затраты ложатся в год = max (год старта, окончания договора, текущий год).</p>
              <div class="years-grid">
                <div class="field">
                  <label for="planningProjectYear1">Год 1</label>
                  <input id="planningProjectYear1" type="number" step="1" inputmode="numeric" value="__DEFAULT_YEAR_1__">
                </div>
                <div class="field">
                  <label for="planningProjectHours1">Часы 1</label>
                  <input id="planningProjectHours1" type="number" step="0.1" inputmode="decimal">
                </div>
                <div class="field">
                  <label for="planningProjectYear2">Год 2</label>
                  <input id="planningProjectYear2" type="number" step="1" inputmode="numeric" value="__DEFAULT_YEAR_2__">
                </div>
                <div class="field">
                  <label for="planningProjectHours2">Часы 2</label>
                  <input id="planningProjectHours2" type="number" step="0.1" inputmode="decimal">
                </div>
                <div class="field">
                  <label for="planningProjectYear3">Год 3</label>
                  <input id="planningProjectYear3" type="number" step="1" inputmode="numeric" value="__DEFAULT_YEAR_3__">
                </div>
                <div class="field">
                  <label for="planningProjectHours3">Часы 3</label>
                  <input id="planningProjectHours3" type="number" step="0.1" inputmode="decimal">
                </div>
              </div>
            </section>
            <section class="subpanel">
              <h3 class="subpanel-title">Ссылки и комментарии</h3>
              <div class="links-grid">
                <div class="field">
                  <label for="planningProjectEstimateDoc">Док с оценкой</label>
                  <input id="planningProjectEstimateDoc" type="url" placeholder="https://">
                </div>
                <div class="field">
                  <label for="planningProjectBitrix">Bitrix</label>
                  <input id="planningProjectBitrix" type="url" placeholder="https://">
                </div>
                <div class="field">
                  <label for="planningProjectComment">Комментарий</label>
                  <textarea id="planningProjectComment"></textarea>
                </div>
              </div>
            </section>
          </div>
        </div>
        <div class="actions">
          <button type="submit" id="savePlanningProjectButton">Сохранить</button>
          <button type="button" id="resetPlanningProjectFormButton">Очистить форму</button>
        </div>
      </form>
      <div class="status" id="planningProjectsStatus"></div>
    </section>
  </main>

  <script>
    const planningProjectsTableBody = document.getElementById("planningProjectsTableBody");
    const planningProjectsCount = document.getElementById("planningProjectsCount");
    const planningProjectsStatus = document.getElementById("planningProjectsStatus");
    const exportPlanningProjectsButton = document.getElementById("exportPlanningProjectsButton");
    const resetPlanningProjectsFiltersButton = document.getElementById("resetPlanningProjectsFiltersButton");
    const resetPlanningProjectsSortingButton = document.getElementById("resetPlanningProjectsSortingButton");
    const planningProjectForm = document.getElementById("planningProjectForm");
    const planningFormTitle = document.getElementById("planningFormTitle");
    const planningProjectId = document.getElementById("planningProjectId");
    const planningProjectDirection = document.getElementById("planningProjectDirection");
    const planningProjectClosed = document.getElementById("planningProjectClosed");
    const planningProjectName = document.getElementById("planningProjectName");
    const planningProjectIdentifier = document.getElementById("planningProjectIdentifier");
    const planningProjectPm = document.getElementById("planningProjectPm");
    const planningProjectCustomer = document.getElementById("planningProjectCustomer");
    const planningProjectStartDate = document.getElementById("planningProjectStartDate");
    const planningProjectEndDate = document.getElementById("planningProjectEndDate");
    const planningProjectDevelopmentHours = document.getElementById("planningProjectDevelopmentHours");
    const planningProjectQuestionFlag = document.getElementById("planningProjectQuestionFlag");
    const planningProjectBaselineEstimate = document.getElementById("planningProjectBaselineEstimate");
    const planningProjectP1 = document.getElementById("planningProjectP1");
    const planningProjectP2 = document.getElementById("planningProjectP2");
    const planningProjectYear1 = document.getElementById("planningProjectYear1");
    const planningProjectHours1 = document.getElementById("planningProjectHours1");
    const planningProjectYear2 = document.getElementById("planningProjectYear2");
    const planningProjectHours2 = document.getElementById("planningProjectHours2");
    const planningProjectYear3 = document.getElementById("planningProjectYear3");
    const planningProjectHours3 = document.getElementById("planningProjectHours3");
    const planningProjectEstimateDoc = document.getElementById("planningProjectEstimateDoc");
    const planningProjectBitrix = document.getElementById("planningProjectBitrix");
    const planningProjectComment = document.getElementById("planningProjectComment");
    const resetPlanningProjectFormButton = document.getElementById("resetPlanningProjectFormButton");
    const planningProjectFormSection = planningProjectForm ? planningProjectForm.closest(".panel") : null;
    const planningProjectsTable = document.querySelector(".planning-projects-table");
    const planningColumnFilterInputs = Array.from(document.querySelectorAll(".planning-filter-input, .planning-filter-select"));
    let currentPlanningProjects = [];
    let filteredPlanningProjects = [];
    let planningProjectsColumnFilterTimer = null;
    let planningProjectsSortState = { key: "", direction: "asc" };
    let planningProjectsColumnFilters = {};

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
      return value ? String(value) : "—";
    }

    function formatOptionalNumber(value) {
      if (value === null || value === undefined || value === "") {
        return "—";
      }
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) {
        return "—";
      }
      return parsed.toLocaleString("ru-RU", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
    }

    function formatOptionalInteger(value) {
      if (value === null || value === undefined || value === "") {
        return "—";
      }
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) {
        return "—";
      }
      return Math.round(parsed).toLocaleString("ru-RU", { maximumFractionDigits: 0 });
    }

    function formatPlanningYearCell(yearValue, hoursValue) {
      if (hoursValue === null || hoursValue === undefined || hoursValue === "") {
        return "—";
      }
      return formatOptionalInteger(yearValue);
    }

    function normalizePlanningFilterValue(value) {
      return String(value ?? "").trim().toLowerCase();
    }

    function getPlanningProjectFieldValue(project, key) {
      if (key === "is_closed") {
        return project?.is_closed ? "true" : "false";
      }
      if (["year_1", "year_2", "year_3"].includes(key)) {
        const hoursKey = `hours_${key.slice(-1)}`;
        return project?.[hoursKey] === null || project?.[hoursKey] === undefined || project?.[hoursKey] === ""
          ? ""
          : String(project?.[key] ?? "");
      }
      const value = project?.[key];
      return value === null || value === undefined ? "" : String(value);
    }

    function applyPlanningProjectsColumnFilters(projects) {
      return projects.filter((project) => {
        return Object.entries(planningProjectsColumnFilters).every(([key, rawValue]) => {
          const filterValue = normalizePlanningFilterValue(rawValue);
          if (!filterValue) {
            return true;
          }
          if (key === "is_closed") {
            return getPlanningProjectFieldValue(project, key) === filterValue;
          }
          return normalizePlanningFilterValue(getPlanningProjectFieldValue(project, key)).includes(filterValue);
        });
      });
    }

    function comparePlanningProjectValues(leftValue, rightValue) {
      const leftNumber = Number(leftValue);
      const rightNumber = Number(rightValue);
      const bothNumbers = leftValue !== "" && rightValue !== "" && Number.isFinite(leftNumber) && Number.isFinite(rightNumber);
      if (bothNumbers) {
        return leftNumber - rightNumber;
      }
      return String(leftValue).localeCompare(String(rightValue), "ru", { numeric: true, sensitivity: "base" });
    }

    function sortPlanningProjects(projects) {
      if (!planningProjectsSortState.key) {
        return [...projects];
      }
      const directionFactor = planningProjectsSortState.direction === "desc" ? -1 : 1;
      return [...projects].sort((leftProject, rightProject) => {
        const leftValue = getPlanningProjectFieldValue(leftProject, planningProjectsSortState.key);
        const rightValue = getPlanningProjectFieldValue(rightProject, planningProjectsSortState.key);
        const comparison = comparePlanningProjectValues(leftValue, rightValue);
        if (comparison !== 0) {
          return comparison * directionFactor;
        }
        return comparePlanningProjectValues(
          getPlanningProjectFieldValue(leftProject, "project_name"),
          getPlanningProjectFieldValue(rightProject, "project_name"),
        );
      });
    }

    function updatePlanningProjectsSortIndicators() {
      if (!planningProjectsTable) {
        return;
      }
      planningProjectsTable.querySelectorAll("thead tr:first-child th[data-sort-key]").forEach((headerCell) => {
        const indicator = headerCell.querySelector(".sort-indicator");
        if (!(indicator instanceof HTMLElement)) {
          return;
        }
        const sortKey = String(headerCell.dataset.sortKey || "");
        if (!sortKey || planningProjectsSortState.key !== sortKey) {
          indicator.textContent = "";
          return;
        }
        indicator.textContent = planningProjectsSortState.direction === "desc" ? "▼" : "▲";
      });
    }

    function refreshPlanningProjectsTable() {
      filteredPlanningProjects = sortPlanningProjects(applyPlanningProjectsColumnFilters(currentPlanningProjects));
      renderPlanningProjects(filteredPlanningProjects);
      updatePlanningProjectsSortIndicators();
    }

    function resetPlanningProjectsColumnFilters() {
      planningColumnFilterInputs.forEach((input) => {
        input.value = "";
      });
      planningProjectsColumnFilters = {};
      refreshPlanningProjectsTable();
    }

    function resetPlanningProjectsSorting() {
      planningProjectsSortState = { key: "", direction: "asc" };
      refreshPlanningProjectsTable();
    }

    function truncateDisplay(value, maxLength = 30) {
      const text = String(value ?? "").trim();
      if (!text) {
        return "—";
      }
      return text.length > maxLength ? `${escapeHtml(text.slice(0, maxLength))} ...` : escapeHtml(text);
    }

    function buildOptionalLink(url) {
      if (!url) {
        return "—";
      }
      const safeUrl = escapeHtml(url);
      return `<a href="${safeUrl}" target="_blank" rel="noreferrer" title="${safeUrl}">${truncateDisplay(url)}</a>`;
    }

    function setPlanningProjectsStatus(message) {
      if (planningProjectsStatus) {
        planningProjectsStatus.textContent = message || "";
      }
    }

    function getPlanningProjectsQueryState() {
      const params = new URLSearchParams(window.location.search);
      const planningProjectId = String(params.get("planning_project_id") || "").trim();
      const redmineIdentifier = String(params.get("redmine_identifier") || "").trim();
      const projectName = String(params.get("project_name") || "").trim();
      return {
        planningProjectId,
        redmineIdentifier,
        projectName,
      };
    }

    function clearPlanningProjectsQueryState() {
      const url = new URL(window.location.href);
      let changed = false;
      ["planning_project_id", "redmine_identifier", "project_name", "open_mode"].forEach((key) => {
        if (url.searchParams.has(key)) {
          url.searchParams.delete(key);
          changed = true;
        }
      });
      if (!changed) {
        return;
      }
      const nextUrl = `${url.pathname}${url.searchParams.toString() ? `?${url.searchParams.toString()}` : ""}${url.hash || ""}`;
      window.history.replaceState({}, "", nextUrl);
    }

    function getPlanningColumnFilterInput(key) {
      return planningColumnFilterInputs.find((input) => String(input.dataset.filterKey || "") === key) || null;
    }

    function setPlanningColumnFilter(key, value) {
      planningProjectsColumnFilters[key] = String(value ?? "");
      const filterInput = getPlanningColumnFilterInput(key);
      if (filterInput) {
        filterInput.value = String(value ?? "");
      }
    }

    function resetPlanningProjectForm() {
      planningProjectId.value = "";
      planningProjectForm.reset();
      planningProjectQuestionFlag.checked = false;
      planningFormTitle.textContent = "Новая запись";
      setPlanningProjectsStatus("");
    }

    function scrollPlanningProjectFormIntoView() {
      if (planningProjectFormSection instanceof HTMLElement) {
        planningProjectFormSection.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }

    function fillPlanningProjectForm(project, options = {}) {
      const preserveId = options.preserveId !== false;
      planningProjectId.value = preserveId ? (project.id ?? "") : "";
      planningProjectDirection.value = project.direction ?? "";
      planningProjectClosed.checked = Boolean(project.is_closed);
      planningProjectName.value = project.project_name ?? "";
      planningProjectIdentifier.value = project.redmine_identifier ?? "";
      planningProjectPm.value = project.pm_name ?? "";
      planningProjectCustomer.value = project.customer ?? "";
      planningProjectStartDate.value = project.start_date ?? "";
      planningProjectEndDate.value = project.end_date ?? "";
      planningProjectDevelopmentHours.value = project.development_hours ?? "";
      planningProjectQuestionFlag.checked = Boolean(project.question_flag);
      planningProjectYear1.value = project.year_1 ?? "";
      planningProjectHours1.value = project.hours_1 ?? "";
      planningProjectYear2.value = project.year_2 ?? "";
      planningProjectHours2.value = project.hours_2 ?? "";
      planningProjectYear3.value = project.year_3 ?? "";
      planningProjectHours3.value = project.hours_3 ?? "";
      planningProjectBaselineEstimate.value = project.baseline_estimate_hours ?? "";
      planningProjectP1.value = project.p1 ?? "";
      planningProjectP2.value = project.p2 ?? "";
      planningProjectEstimateDoc.value = project.estimate_doc_url ?? "";
      planningProjectBitrix.value = project.bitrix_url ?? "";
      planningProjectComment.value = project.comment_text ?? "";
      planningFormTitle.textContent = preserveId ? "Редактирование записи" : "Новая запись (копия)";
      setPlanningProjectsStatus(preserveId ? "Запись загружена в форму для редактирования." : "Поля заполнены из существующей записи. Можно сохранить как новую.");
      scrollPlanningProjectFormIntoView();
    }

    function applyPlanningProjectPrefill(projects) {
      const queryState = getPlanningProjectsQueryState();
      if (queryState.planningProjectId) {
        const matchedById = projects.find((project) => String(project?.id ?? "") === queryState.planningProjectId);
        if (matchedById) {
          fillPlanningProjectForm(matchedById);
          setPlanningProjectsStatus(`Открыто редактирование записи проекта "${matchedById.project_name}".`);
          clearPlanningProjectsQueryState();
          return;
        }
      }

      if (!queryState.redmineIdentifier) {
        return;
      }

      const matchedProjects = projects.filter((project) => String(project?.redmine_identifier ?? "").trim() === queryState.redmineIdentifier);
      if (matchedProjects.length === 1) {
        fillPlanningProjectForm(matchedProjects[0]);
        setPlanningProjectsStatus(`Открыто редактирование записи для проекта с идентификатором ${queryState.redmineIdentifier}.`);
        setPlanningColumnFilter("redmine_identifier", "");
        refreshPlanningProjectsTable();
        clearPlanningProjectsQueryState();
        return;
      }

      if (matchedProjects.length > 1) {
        setPlanningColumnFilter("redmine_identifier", queryState.redmineIdentifier);
        refreshPlanningProjectsTable();
        setPlanningProjectsStatus(`Найдено несколько записей по идентификатору ${queryState.redmineIdentifier}. Оставили фильтр на таблице.`);
        clearPlanningProjectsQueryState();
        return;
      }

      resetPlanningProjectForm();
      planningProjectIdentifier.value = queryState.redmineIdentifier;
      if (queryState.projectName) {
        planningProjectName.value = queryState.projectName;
      }
      planningFormTitle.textContent = "Новая запись";
      setPlanningProjectsStatus(`Запись не найдена. Подготовлена новая форма для проекта с идентификатором ${queryState.redmineIdentifier}.`);
      scrollPlanningProjectFormIntoView();
      clearPlanningProjectsQueryState();
    }

    function renderPlanningProjects(projects) {
      const totalProjects = Number(window.__planningProjectsTotal || currentPlanningProjects.length || 0);
      planningProjectsCount.textContent = `Показано: ${projects.length} из ${currentPlanningProjects.length || 0} (в выборке ${totalProjects}, лимит 100)`;
      if (!projects.length) {
        planningProjectsTableBody.innerHTML = '<tr><td colspan="22" class="empty-state">Пока нет ни одной записи.</td></tr>';
        return;
      }

      planningProjectsTableBody.innerHTML = projects.map((project) => `
        <tr class="${project.question_flag ? "question-flag-row" : ""}">
          <td class="actions-col">
            <div class="row-actions">
              <button type="button" class="edit-button" data-action="edit" data-id="${project.id}" title="Изменить" aria-label="Изменить">
                <svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg>
              </button>
              <button type="button" class="copy-button" data-action="copy" data-id="${project.id}" title="Копировать" aria-label="Копировать">
                <svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
              </button>
              <button type="button" class="delete-button" data-action="delete" data-id="${project.id}" title="Удалить" aria-label="Удалить">
                <svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>
              </button>
            </div>
          </td>
          <td class="direction-col">${escapeHtml(project.direction ?? "—")}</td>
          <td class="closed-col">${project.is_closed ? "Да" : ""}</td>
          <td class="customer-col">${escapeHtml(project.customer ?? "—")}</td>
          <td class="project-name-col">${escapeHtml(project.project_name ?? "—")}</td>
          <td class="identifier-col mono">${escapeHtml(project.redmine_identifier ?? "—")}</td>
          <td class="pm-col">${escapeHtml(project.pm_name ?? "—")}</td>
          <td class="start-date-col">${formatOptionalDate(project.start_date)}</td>
          <td class="end-date-col">${formatOptionalDate(project.end_date)}</td>
          <td class="development-col">${formatOptionalNumber(project.development_hours)}</td>
          <td class="year-col">${formatPlanningYearCell(project.year_1, project.hours_1)}</td>
          <td class="year-hours-col">${formatOptionalNumber(project.hours_1)}</td>
          <td class="year-col">${formatPlanningYearCell(project.year_2, project.hours_2)}</td>
          <td class="year-hours-col">${formatOptionalNumber(project.hours_2)}</td>
          <td class="year-col">${formatPlanningYearCell(project.year_3, project.hours_3)}</td>
          <td class="year-hours-col">${formatOptionalNumber(project.hours_3)}</td>
          <td class="baseline-col">${formatOptionalNumber(project.baseline_estimate_hours)}</td>
          <td class="p-col">${formatOptionalNumber(project.p1)}</td>
          <td class="p-col">${formatOptionalNumber(project.p2)}</td>
          <td class="doc-col link-cell">${buildOptionalLink(project.estimate_doc_url)}</td>
          <td class="bitrix-col link-cell">${buildOptionalLink(project.bitrix_url)}</td>
          <td class="comment-col" title="${escapeHtml(project.comment_text ?? "")}">${truncateDisplay(project.comment_text)}</td>
        </tr>
      `).join("");
    }

    async function loadPlanningProjects() {
      planningProjectsTableBody.innerHTML = '<tr><td colspan="22" class="empty-state">Загружаем записи...</td></tr>';
      const params = new URLSearchParams();
      params.set("include_closed", "true");
      params.set("limit", "100");
      const response = await fetch(`/api/planning-projects?${params.toString()}`);
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Не удалось загрузить планирование проектов.");
      }
      const projects = payload.projects || [];
      currentPlanningProjects = projects;
      window.__planningProjectsTotal = Number(payload.total || projects.length || 0);
      refreshPlanningProjectsTable();
      applyPlanningProjectPrefill(projects);
      return projects;
    }
    function collectPlanningProjectPayload() {
      return {
        direction: planningProjectDirection.value.trim(),
        project_name: planningProjectName.value.trim(),
        redmine_identifier: planningProjectIdentifier.value.trim(),
        pm_name: planningProjectPm.value.trim(),
        customer: planningProjectCustomer.value.trim(),
        start_date: planningProjectStartDate.value || null,
        end_date: planningProjectEndDate.value || null,
        development_hours: planningProjectDevelopmentHours.value === "" ? null : Number(planningProjectDevelopmentHours.value),
        year_1: planningProjectYear1.value === "" ? null : Number(planningProjectYear1.value),
        hours_1: planningProjectHours1.value === "" ? null : Number(planningProjectHours1.value),
        year_2: planningProjectYear2.value === "" ? null : Number(planningProjectYear2.value),
        hours_2: planningProjectHours2.value === "" ? null : Number(planningProjectHours2.value),
        year_3: planningProjectYear3.value === "" ? null : Number(planningProjectYear3.value),
        hours_3: planningProjectHours3.value === "" ? null : Number(planningProjectHours3.value),
        baseline_estimate_hours: planningProjectBaselineEstimate.value === "" ? null : Number(planningProjectBaselineEstimate.value),
        p1: planningProjectP1.value === "" ? null : Number(planningProjectP1.value),
        p2: planningProjectP2.value === "" ? null : Number(planningProjectP2.value),
        estimate_doc_url: planningProjectEstimateDoc.value.trim(),
        bitrix_url: planningProjectBitrix.value.trim(),
        comment_text: planningProjectComment.value.trim(),
        question_flag: Boolean(planningProjectQuestionFlag.checked),
        is_closed: Boolean(planningProjectClosed.checked),
      };
    }

    planningProjectForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const projectId = planningProjectId.value.trim();
      const method = projectId ? "PUT" : "POST";
      const url = projectId ? `/api/planning-projects/${encodeURIComponent(projectId)}` : "/api/planning-projects";
      setPlanningProjectsStatus(projectId ? "Сохраняем изменения..." : "Создаем запись...");

      try {
        const response = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(collectPlanningProjectPayload()),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "Не удалось сохранить запись.");
        }
        await loadPlanningProjects();
        resetPlanningProjectForm();
        setPlanningProjectsStatus(projectId ? "Изменения сохранены." : "Запись создана.");
      } catch (error) {
        setPlanningProjectsStatus(error instanceof Error ? error.message : "Ошибка сохранения.");
      }
    });

    resetPlanningProjectFormButton.addEventListener("click", () => {
      resetPlanningProjectForm();
    });

    planningProjectsTableBody.addEventListener("click", async (event) => {
      const triggerButton = event.target instanceof Element ? event.target.closest("button[data-action][data-id]") : null;
      if (!(triggerButton instanceof HTMLButtonElement)) {
        return;
      }
      const action = triggerButton.dataset.action;
      const projectId = triggerButton.dataset.id;
      if (!action || !projectId) {
        return;
      }

      try {
        const currentProject = currentPlanningProjects.find((item) => String(item.id) === String(projectId));
        if (!currentProject) {
          throw new Error("Запись не найдена.");
        }

        if (action === "edit") {
          fillPlanningProjectForm(currentProject);
          return;
        }

        if (action === "copy") {
          fillPlanningProjectForm(currentProject, { preserveId: false });
          return;
        }

        if (action === "delete") {
          if (!window.confirm(`Удалить запись по проекту "${currentProject.project_name}"?`)) {
            return;
          }
          setPlanningProjectsStatus("Удаляем запись...");
          const deleteResponse = await fetch(`/api/planning-projects/${encodeURIComponent(projectId)}`, { method: "DELETE" });
          const deletePayload = await deleteResponse.json();
          if (!deleteResponse.ok) {
            throw new Error(deletePayload.detail || "Не удалось удалить запись.");
          }
          await loadPlanningProjects();
          if (planningProjectId.value === String(projectId)) {
            resetPlanningProjectForm();
          }
          setPlanningProjectsStatus("Запись удалена.");
        }
      } catch (error) {
        setPlanningProjectsStatus(error instanceof Error ? error.message : "Ошибка обработки записи.");
      }
    });

    planningColumnFilterInputs.forEach((input) => {
      const key = String(input.dataset.filterKey || "");
      if (key) {
        planningProjectsColumnFilters[key] = String(input.value || "");
      }
      input.addEventListener("input", () => {
        const currentKey = String(input.dataset.filterKey || "");
        if (!currentKey) {
          return;
        }
        planningProjectsColumnFilters[currentKey] = String(input.value || "");
        if (planningProjectsColumnFilterTimer) {
          window.clearTimeout(planningProjectsColumnFilterTimer);
        }
        planningProjectsColumnFilterTimer = window.setTimeout(() => {
          refreshPlanningProjectsTable();
        }, 150);
      });
      input.addEventListener("change", () => {
        const currentKey = String(input.dataset.filterKey || "");
        if (!currentKey) {
          return;
        }
        planningProjectsColumnFilters[currentKey] = String(input.value || "");
        refreshPlanningProjectsTable();
      });
    });

    planningProjectsTable?.querySelector("thead tr:first-child")?.addEventListener("click", (event) => {
      const headerCell = event.target instanceof HTMLElement ? event.target.closest("th[data-sort-key]") : null;
      if (!(headerCell instanceof HTMLElement)) {
        return;
      }
      const sortKey = String(headerCell.dataset.sortKey || "");
      if (!sortKey) {
        return;
      }
      if (planningProjectsSortState.key === sortKey) {
        planningProjectsSortState.direction = planningProjectsSortState.direction === "asc" ? "desc" : "asc";
      } else {
        planningProjectsSortState = { key: sortKey, direction: "asc" };
      }
      refreshPlanningProjectsTable();
    });

    loadPlanningProjects().catch((error) => {
      planningProjectsCount.textContent = "Ошибка";
      planningProjectsTableBody.innerHTML = '<tr><td colspan="22" class="empty-state">Не удалось загрузить записи.</td></tr>';
      setPlanningProjectsStatus(error instanceof Error ? error.message : "Не удалось загрузить планирование проектов.");
    });

    exportPlanningProjectsButton?.addEventListener("click", () => {
      const params = new URLSearchParams();
      params.set("include_closed", "true");
      const query = params.toString();
      window.location.href = `/api/planning-projects/export.csv${query ? `?${query}` : ""}`;
    });

    resetPlanningProjectsFiltersButton?.addEventListener("click", () => {
      resetPlanningProjectsColumnFilters();
      setPlanningProjectsStatus("");
    });

    resetPlanningProjectsSortingButton?.addEventListener("click", () => {
      resetPlanningProjectsSorting();
      setPlanningProjectsStatus("");
    });
  </script>
</body>
</html>""".replace("__DEFAULT_YEAR_1__", str(defaultYear1)).replace("__DEFAULT_YEAR_2__", str(defaultYear2)).replace("__DEFAULT_YEAR_3__", str(defaultYear3))


@app.get("/", response_class=HTMLResponse)
def getIndexPage() -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    ensurePlanningProjectsTable()
    return _renderHtmlPage(PAGE_HTML)


@app.get("/login", response_class=HTMLResponse)
def getLoginPage(next: str | None = Query("/", alias="next")) -> HTMLResponse:
    _ensureAuthStorage()
    return _renderHtmlPage(buildLoginPage(next or "/"))


@app.get("/forgot-password", response_class=HTMLResponse)
def getForgotPasswordPage() -> HTMLResponse:
    _ensureAuthStorage()
    return _renderHtmlPage(buildForgotPasswordPage())


@app.get("/reset-password", response_class=HTMLResponse)
def getResetPasswordPage(token: str = Query(...)) -> HTMLResponse:
    _ensureAuthStorage()
    return _renderHtmlPage(buildResetPasswordPage(token))


@app.get("/change-password", response_class=HTMLResponse)
def getChangePasswordPage(request: Request) -> HTMLResponse:
    _ensureAuthStorage()
    user = _getCurrentUser(request)
    if not user:
      return RedirectResponse(url="/login?next=/change-password", status_code=303)
    return _renderHtmlPage(buildChangePasswordPage())


@app.get("/logout")
def logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.post("/api/auth/login")
def loginApi(
    request: Request,
    payload: LoginPayload,
    next: str | None = Query("/", alias="next"),
) -> dict[str, object]:
    _ensureAuthStorage()
    user = getUserByLogin(str(payload.login or "").strip())
    if not user or not _verifyPassword(str(payload.password or ""), str(user.get("password_hash") or "")):
        raise HTTPException(status_code=401, detail="Неверный логин или пароль.")

    request.session["user_login"] = str(user.get("login") or "")
    return {
        "ok": True,
        "must_change_password": bool(user.get("must_change_password")),
        "next_path": _getSafeNextPath(next),
    }


@app.post("/api/auth/change-password")
def changePasswordApi(request: Request, payload: ChangePasswordPayload) -> dict[str, object]:
    _ensureAuthStorage()
    user = _getCurrentUser(request)
    if not user:
        raise HTTPException(status_code=401, detail="Требуется вход в систему.")

    newPassword = str(payload.new_password or "")
    if len(newPassword) < 3:
        raise HTTPException(status_code=400, detail="Новый пароль должен содержать не меньше 3 символов.")

    updatedUser = updateUserPassword(int(user["id"]), _hashPassword(newPassword), False)
    if not updatedUser:
        raise HTTPException(status_code=404, detail="Пользователь не найден.")

    request.session["user_login"] = str(updatedUser.get("login") or user.get("login") or "")
    return {"ok": True, "next_path": "/"}


@app.post("/api/auth/request-password-reset")
def requestPasswordResetApi(request: Request, payload: PasswordResetRequestPayload) -> dict[str, object]:
    _ensureAuthStorage()
    loginValue = str(payload.email or "").strip()
    if not loginValue:
        raise HTTPException(status_code=400, detail="Укажите email.")

    smtpHost = str(config.smtpHost or "").strip()
    smtpFromEmail = str(config.smtpFromEmail or "").strip()
    if not smtpHost or not smtpFromEmail:
        raise HTTPException(status_code=503, detail="Отправка писем для сброса пароля пока не настроена.")

    user = getUserByLogin(loginValue)
    if user:
        rawToken = secrets.token_urlsafe(32)
        expiresAt = datetime.now(UTC) + timedelta(minutes=30)
        storeUserPasswordResetToken(int(user["id"]), _hashResetToken(rawToken), expiresAt)
        _sendPasswordResetEmail(loginValue, _buildPasswordResetLink(request, rawToken))

    return {"ok": True, "detail": "Если пользователь найден, письмо отправлено."}


@app.post("/api/auth/reset-password")
def resetPasswordApi(payload: PasswordResetCompletePayload) -> dict[str, object]:
    _ensureAuthStorage()
    newPassword = str(payload.new_password or "")
    if len(newPassword) < 3:
        raise HTTPException(status_code=400, detail="Новый пароль должен содержать не меньше 3 символов.")

    user = getUserByPasswordResetToken(_hashResetToken(str(payload.token or "")), datetime.now(UTC))
    if not user:
        raise HTTPException(status_code=400, detail="Ссылка для сброса пароля недействительна или устарела.")

    updatedUser = updateUserPassword(int(user["id"]), _hashPassword(newPassword), False)
    if not updatedUser:
        raise HTTPException(status_code=404, detail="Пользователь не найден.")

    clearUserPasswordResetToken(int(user["id"]))
    return {"ok": True}


@app.get("/admin/users", response_class=HTMLResponse)
def getAdminUsersPage(request: Request) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    _requireAdminUser(request)
    _ensureAuthStorage()
    return _renderHtmlPage(buildAdminUsersPage(listUsers()))


@app.get("/api/admin/users")
def getAdminUsersApi(request: Request) -> dict[str, object]:
    _requireAdminUser(request)
    _ensureAuthStorage()
    return {
        "users": [
            {**user, "roles": _parseRoles(user.get("roles"))}
            for user in listUsers()
        ]
    }


@app.post("/api/admin/users")
def createAdminUserApi(request: Request, payload: UserPayload) -> dict[str, object]:
    _requireAdminUser(request)
    _ensureAuthStorage()

    loginValue = str(payload.login or "").strip()
    if not loginValue:
        raise HTTPException(status_code=400, detail="Укажите логин.")
    if getUserByLogin(loginValue):
        raise HTTPException(status_code=400, detail="Пользователь с таким логином уже существует.")
    if not payload.password:
        raise HTTPException(status_code=400, detail="Укажите пароль.")

    created = createUser(
        {
            "login": loginValue,
            "password_hash": _hashPassword(payload.password),
            "roles": _serializeRoles(payload.roles),
            "must_change_password": bool(payload.must_change_password),
        }
    )
    created["roles"] = _parseRoles(created.get("roles"))
    return {"user": created}


@app.put("/api/admin/users/{user_id}")
def updateAdminUserApi(request: Request, user_id: int, payload: UserPayload) -> dict[str, object]:
    _requireAdminUser(request)
    _ensureAuthStorage()

    loginValue = str(payload.login or "").strip()
    if not loginValue:
        raise HTTPException(status_code=400, detail="Укажите логин.")

    existingByLogin = getUserByLogin(loginValue)
    if existingByLogin and int(existingByLogin["id"]) != user_id:
        raise HTTPException(status_code=400, detail="Пользователь с таким логином уже существует.")

    updated = updateUser(
        user_id,
        {
            "login": loginValue,
            "roles": _serializeRoles(payload.roles),
            "must_change_password": bool(payload.must_change_password),
            "password_hash": _hashPassword(payload.password) if payload.password else None,
        },
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Пользователь не найден.")
    updated["roles"] = _parseRoles(updated.get("roles"))
    return {"user": updated}


@app.delete("/api/admin/users/{user_id}")
def deleteAdminUserApi(request: Request, user_id: int) -> dict[str, object]:
    currentUser = _requireAdminUser(request)
    _ensureAuthStorage()

    if int(currentUser["id"]) == user_id:
        raise HTTPException(status_code=400, detail="Нельзя удалить текущего пользователя.")

    deleted = deleteUser(user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Пользователь не найден.")
    return {"deleted": True}


@app.get("/planning-projects", response_class=HTMLResponse)
def getPlanningProjectsPage() -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    return _renderHtmlPage(buildPlanningProjectsPage())


def _buildProjectsSummaryGroups(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    groupedRows: list[dict[str, object]] = []
    groupedByKey: dict[str, dict[str, object]] = {}

    for index, row in enumerate(rows):
        identifier = str(row.get("redmine_identifier") or "").strip()
        groupKey = identifier.lower() if identifier else f"__row_{index}_{row.get('id') or ''}"
        group = groupedByKey.get(groupKey)
        if group is None:
            group = {
                "redmine_identifier": identifier,
                "items": [],
                "_fact_values": [],
            }
            groupedByKey[groupKey] = group
            groupedRows.append(group)

        factYearValue = row.get("development_spent_hours_year")
        if factYearValue not in (None, ""):
            try:
                group["_fact_values"].append(float(factYearValue))
            except (TypeError, ValueError):
                pass

        group["items"].append(
            {
                "id": row.get("id"),
                "project_redmine_id": row.get("project_redmine_id"),
                "direction": row.get("direction"),
                "customer": row.get("customer"),
                "project_name": row.get("project_name"),
                "pm_name": row.get("pm_name"),
                "report_year_hours": row.get("report_year_hours"),
                "development_hours": row.get("development_hours"),
                "question_flag": bool(row.get("question_flag")),
                "link_project_name": row.get("link_project_name") or row.get("project_name"),
                "is_missing_planning_project": bool(row.get("is_missing_planning_project")),
            }
        )

    for group in groupedRows:
        factValues = list(group.pop("_fact_values", []))
        items = list(group.get("items") or [])
        group["project_redmine_id"] = next(
            (item.get("project_redmine_id") for item in items if item.get("project_redmine_id") not in (None, "")),
            None,
        )
        group["row_span"] = len(items)
        group["development_spent_hours_year_average"] = (
            sum(factValues) / len(factValues) if factValues else None
        )
        hasLimitValues = any(
            item.get("report_year_hours") not in (None, "") or item.get("development_hours") not in (None, "")
            for item in items
        )
        group["development_limit_hours"] = (
            sum(
                float(item.get("report_year_hours") or 0) + float(item.get("development_hours") or 0)
                for item in items
            )
            if hasLimitValues
            else None
        )

    return groupedRows


def _listProjectsSummaryRows(
    reportDate: date,
    direction: str | None = None,
    isClosed: bool | None = None,
    enabledOnly: bool = True,
) -> list[dict[str, object]]:
    storedProjects = listStoredProjects()
    storedProjectsByIdentifier = {
        str(project.get("identifier") or "").strip().lower(): project
        for project in storedProjects
        if str(project.get("identifier") or "").strip()
    }
    enabledIdentifiers = {
        str(project.get("identifier") or "").strip().lower()
        for project in storedProjects
        if project.get("is_enabled") and str(project.get("identifier") or "").strip()
    }

    if isClosed is None:
        planningRows = [
            *listProjectPlanningSummary(reportDate=reportDate.isoformat(), direction=direction, isClosed=False),
            *listProjectPlanningSummary(reportDate=reportDate.isoformat(), direction=direction, isClosed=True),
        ]
    else:
        planningRows = listProjectPlanningSummary(
            reportDate=reportDate.isoformat(),
            direction=direction,
            isClosed=isClosed,
        )

    planningIdentifiers = set(listPlanningProjectIdentifiers())
    summaryRows = []
    for row in planningRows:
        identifier = str(row.get("redmine_identifier") or "").strip().lower()
        if enabledOnly and identifier not in enabledIdentifiers:
            continue
        storedProject = storedProjectsByIdentifier.get(identifier)
        summaryRows.append(
            {
                **row,
                "project_redmine_id": (
                    row.get("project_redmine_id")
                    if row.get("project_redmine_id") not in (None, "")
                    else (storedProject.get("redmine_id") if storedProject else None)
                ),
            }
        )

    for project in storedProjects:
        identifier = str(project.get("identifier") or "").strip()
        if not identifier:
            continue
        if enabledOnly and not project.get("is_enabled"):
            continue
        if identifier.lower() in planningIdentifiers:
            continue

        summaryRows.append(
            {
                "id": None,
                "direction": None,
                "customer": None,
                "project_name": project.get("name"),
                "redmine_identifier": identifier,
                "project_redmine_id": project.get("redmine_id"),
                "pm_name": None,
                "development_hours": None,
                "report_year_hours": None,
                "development_spent_hours_year": (
                    float(project.get("development_spent_hours_year") or 0)
                    + float(project.get("development_process_spent_hours_year") or 0)
                    + float(project.get("bug_spent_hours_year") or 0)
                ),
                "question_flag": False,
                "is_closed": None,
                "link_project_name": project.get("name"),
                "is_missing_planning_project": True,
            }
        )

    return summaryRows


def buildProjectsSummaryPage() -> str:
    todayIso = date.today().isoformat()
    directions = listPlanningDirections()
    if "КОТ" not in directions:
        directions = ["КОТ", *directions]
    directionOptionsHtml = "".join(
        f'<option value="{escape(direction)}"{" selected" if direction == "КОТ" else ""}>{escape(direction)}</option>'
        for direction in directions
    )
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Сводка по проектам</title>
  <link rel="icon" href="https://sms-it.ru/favicon.ico" sizes="any">
  <style>
    :root {{
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
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Golos", "Segoe UI Variable", "Segoe UI", Tahoma, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    main {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 24px 20px 56px;
    }}
    .page-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      margin: 0 0 18px;
    }}
    .brand {{
      display: inline-flex;
      align-items: center;
      text-decoration: none;
    }}
    .brand img {{
      width: 220px;
      max-width: 100%;
      height: auto;
      display: block;
    }}
    .head-actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .head-actions a {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
      padding: 10px 14px;
      border-radius: 6px;
      text-decoration: none;
      font-weight: 700;
      box-shadow: var(--shadow-soft);
    }}
    .head-actions a.home-link {{
      background: var(--yellow-109);
      color: #16324a;
    }}
    .head-actions a.planning-link {{
      background: var(--blue-302);
      color: #ffffff;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(2.15rem, 4.9vw, 3.6rem);
      line-height: 0.98;
      letter-spacing: -0.04em;
      font-weight: 400;
    }}
    .lead {{
      margin: 0 0 20px;
      color: var(--muted);
      line-height: 1.6;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 20px;
      box-shadow: var(--shadow-soft);
      margin: 0 0 18px;
    }}
    .controls {{
      display: grid;
      grid-template-columns: minmax(180px, 220px) minmax(320px, 1fr) auto;
      gap: 14px 16px;
      align-items: end;
    }}
    .field {{
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    .field label {{
      font-weight: 700;
    }}
    .field input,
    .field select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px 12px;
      font: inherit;
      color: var(--text);
      background: #ffffff;
    }}
    .checkbox-field {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 42px;
      font-weight: 600;
      color: var(--text);
    }}
    .checkbox-field input {{
      width: 16px;
      height: 16px;
      margin: 0;
    }}
    .checkbox-hint {{
      margin: -2px 0 0;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.45;
    }}
    .control-actions {{
      display: inline-flex;
      gap: 10px;
      flex-wrap: nowrap;
      justify-content: flex-start;
      align-items: center;
    }}
    .control-actions button {{
      white-space: nowrap;
    }}
    button {{
      border: 0;
      border-radius: 6px;
      padding: 11px 18px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      box-shadow: var(--shadow-soft);
    }}
    #projectsSummaryRefreshButton {{
      background: var(--orange-1585);
      color: #ffffff;
    }}
    #exportProjectsSummaryButton {{
      background: #eceff3;
      color: var(--text);
    }}
    .meta {{
      min-height: 22px;
      margin: 0 0 12px;
      color: var(--muted);
    }}
    .table-wrap {{
      overflow: visible;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
    }}
    table {{
      width: 100%;
      min-width: 1180px;
      border-collapse: collapse;
      background: #ffffff;
    }}
    th, td {{
      padding: 11px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #eef6f7;
      color: #426179;
      text-transform: uppercase;
      font-size: 0.78rem;
      line-height: 1.2;
    }}
    thead {{
      position: static;
    }}
    thead tr:first-child th {{
      position: sticky;
      top: 0;
      z-index: 6;
    }}
    .summary-filter-row th {{
      background: #f8fbfc;
      padding-top: 8px;
      padding-bottom: 8px;
      position: sticky;
      top: var(--projects-summary-header-height, 44px);
      z-index: 5;
    }}
    .summary-filter-input {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 7px 9px;
      font: inherit;
      color: var(--text);
      background: #ffffff;
    }}
    .summary-filter-select {{
      min-width: 140px;
    }}
    th[data-sort-key] {{
      cursor: pointer;
      user-select: none;
    }}
    th[data-sort-key] .sort-indicator {{
      margin-left: 4px;
      color: #90a4b4;
      font-size: 0.72rem;
    }}
    td.group-cell {{
      background: #f7fbfc;
      font-weight: 600;
      vertical-align: middle;
    }}
    .summary-project-flagged {{
      color: #c13b3b;
    }}
    .summary-project-flagged a {{
      color: inherit !important;
      border-bottom-color: currentColor !important;
    }}
    tfoot td {{
      position: sticky;
      bottom: 0;
      z-index: 2;
      background: #f1f7f8;
      font-weight: 700;
      border-top: 2px solid #d7e6ea;
    }}
    .totals-label-cell {{
      color: var(--text);
    }}
    .totals-spacer-cell {{
      background: #f1f7f8;
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .mono {{ font-family: Consolas, "Courier New", monospace; }}
    .empty-state {{
      padding: 28px 20px;
      color: var(--muted);
      text-align: center;
    }}
    @media (max-width: 980px) {{
      .controls {{
        grid-template-columns: repeat(2, minmax(180px, 1fr));
      }}
      .control-actions {{
        grid-column: 1 / -1;
      }}
    }}
    @media (max-width: 700px) {{
      .page-head {{
        flex-direction: column;
        align-items: flex-start;
      }}
      .head-actions {{
        justify-content: flex-start;
      }}
      .controls {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <div class="page-head">
      <a class="brand" href="/" aria-label="На главную">
        <img src="https://sms-it.ru/wp-content/themes/smsit_template/images/logo.svg" alt="СМС-ИТ">
      </a>
      <div class="head-actions">
        <a class="home-link" href="/">Главная</a>
        <a class="planning-link" href="/planning-projects">Планирование проектов</a>
      </div>
    </div>

    <h1>Сводка по проектам</h1>
    <p class="lead">Сводим данные планирования проектов и фактические часы разработки по последним срезам на выбранную дату.</p>

    <section class="panel">
      <div class="controls">
        <div class="field">
          <label for="projectsSummaryDateInput">Дата отчета</label>
          <input id="projectsSummaryDateInput" type="date" value="{todayIso}">
        </div>
        <div class="field">
          <label class="checkbox-field" for="projectsSummaryEnabledOnlyCheckbox">
            <input id="projectsSummaryEnabledOnlyCheckbox" type="checkbox" checked>
            <span>Только по включенным проектам Redmine</span>
          </label>
          <p class="checkbox-hint">Проекты включаются в таблице "Проекты Redmine".</p>
        </div>
        <div class="control-actions">
          <button type="button" id="projectsSummaryRefreshButton">Показать сводку</button>
          <button type="button" id="exportProjectsSummaryButton">Выгрузить в Excel</button>
          <button type="button" id="resetProjectsSummaryFiltersButton">Сбросить фильтр</button>
        </div>
      </div>
    </section>

    <section class="panel">
      <p class="meta" id="projectsSummaryMeta">Загрузка...</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Идентификатор в Redmine</th>
              <th>Разработка: факт за год, ч</th>
              <th>Направление</th>
              <th>Заказчик</th>
              <th>Название проекта</th>
              <th>ПМ</th>
              <th>Лимит разработки с багфиксом</th>
              <th>Часы за год отчета</th>
              <th>Часы разработки с багфиксом</th>
            </tr>
            <tr class="summary-filter-row">
              <th><input class="summary-filter-input" data-filter-key="redmine_identifier" type="text"></th>
              <th><input class="summary-filter-input" data-filter-key="development_spent_hours_year_average" type="text"></th>
              <th><input class="summary-filter-input" data-filter-key="direction" type="text"></th>
              <th><input class="summary-filter-input" data-filter-key="customer" type="text"></th>
              <th><input class="summary-filter-input" data-filter-key="project_name" type="text"></th>
              <th><input class="summary-filter-input" data-filter-key="pm_name" type="text"></th>
              <th><input class="summary-filter-input" data-filter-key="development_limit_hours" type="text"></th>
              <th><input class="summary-filter-input" data-filter-key="report_year_hours" type="text"></th>
              <th><input class="summary-filter-input" data-filter-key="development_hours" type="text"></th>
            </tr>
          </thead>
          <tbody id="projectsSummaryTableBody">
            <tr><td colspan="9" class="empty-state">Загружаем сводку...</td></tr>
          </tbody>
          <tfoot id="projectsSummaryTableFoot"></tfoot>
        </table>
      </div>
    </section>
  </main>

  <script>
    const projectsSummaryDateInput = document.getElementById("projectsSummaryDateInput");
    const projectsSummaryEnabledOnlyCheckbox = document.getElementById("projectsSummaryEnabledOnlyCheckbox");
    const projectsSummaryRefreshButton = document.getElementById("projectsSummaryRefreshButton");
    const exportProjectsSummaryButton = document.getElementById("exportProjectsSummaryButton");
    const resetProjectsSummaryFiltersButton = document.getElementById("resetProjectsSummaryFiltersButton");
    const projectsSummaryMeta = document.getElementById("projectsSummaryMeta");
    const projectsSummaryTableBody = document.getElementById("projectsSummaryTableBody");
    const projectsSummaryTableFoot = document.getElementById("projectsSummaryTableFoot");
    const projectsSummaryTableWrap = document.querySelector(".table-wrap");
    const projectsSummaryStrings = {{
      all: "\\u0412\\u0441\\u0435",
      empty: "\\u041f\\u0443\\u0441\\u0442\\u043e",
      dash: "\\u2014",
      loading: "\\u0417\\u0430\\u0433\\u0440\\u0443\\u0436\\u0430\\u0435\\u043c \\u0441\\u0432\\u043e\\u0434\\u043a\\u0443...",
      error: "\\u041e\\u0448\\u0438\\u0431\\u043a\\u0430",
      loadFailed: "\\u041d\\u0435 \\u0443\\u0434\\u0430\\u043b\\u043e\\u0441\\u044c \\u0437\\u0430\\u0433\\u0440\\u0443\\u0437\\u0438\\u0442\\u044c \\u0441\\u0432\\u043e\\u0434\\u043a\\u0443.",
      noRows: "\\u041f\\u043e \\u0432\\u044b\\u0431\\u0440\\u0430\\u043d\\u043d\\u044b\\u043c \\u0443\\u0441\\u043b\\u043e\\u0432\\u0438\\u044f\\u043c \\u0437\\u0430\\u043f\\u0438\\u0441\\u0435\\u0439 \\u043d\\u0435 \\u043d\\u0430\\u0439\\u0434\\u0435\\u043d\\u043e.",
      factFiltered: "\\u0441\\u043e\\u0434\\u0435\\u0440\\u0436\\u0438\\u0442 \\u0437\\u0430\\u0442\\u0440\\u0430\\u0442\\u044b \\u043f\\u0440\\u043e\\u0435\\u043a\\u0442\\u043e\\u0432, \\u043d\\u0435 \\u043f\\u043e\\u043f\\u0430\\u0432\\u0448\\u0438\\u0445 \\u043f\\u043e\\u0434 \\u043a\\u0440\\u0438\\u0442\\u0435\\u0440\\u0438\\u0438 \\u0444\\u0438\\u043b\\u044c\\u0442\\u0440\\u0430",
    }};
    const projectsSummaryFactYearLabel = "\\u0420\\u0430\\u0437\\u0440\\u0430\\u0431\\u043e\\u0442\\u043a\\u0430 c \\u0431\\u0430\\u0433\\u0430\\u043c\\u0438 \\u0438 \\u043f\\u0440\\u043e\\u0446\\u0435\\u0441\\u0441\\u0430\\u043c\\u0438 \\u0437\\u0430 \\u0433\\u043e\\u0434, \\u0447";
    const projectsSummaryColumnKeys = [
      "redmine_identifier",
      "development_spent_hours_year_average",
      "direction",
      "customer",
      "project_name",
      "pm_name",
      "development_limit_hours",
      "report_year_hours",
      "development_hours",
    ];
    const projectsSummarySortableKeys = new Set(projectsSummaryColumnKeys);
    const projectsSummaryItemKeys = new Set([
      "direction",
      "customer",
      "project_name",
      "pm_name",
      "report_year_hours",
      "development_hours",
    ]);
    const projectsSummaryNumericKeys = new Set([
      "development_spent_hours_year_average",
      "development_limit_hours",
      "report_year_hours",
      "development_hours",
    ]);
    const projectsSummaryHeaderCells = Array.from(document.querySelectorAll("table thead tr:first-child th"));
    if (projectsSummaryHeaderCells[1]) {{
      projectsSummaryHeaderCells[1].textContent = projectsSummaryFactYearLabel;
    }}
    const projectsSummaryDirectionFilterCell = document.querySelector('.summary-filter-row [data-filter-key="direction"]')?.parentElement;
    if (projectsSummaryDirectionFilterCell) {{
      projectsSummaryDirectionFilterCell.innerHTML = `<select class="summary-filter-input summary-filter-select" data-filter-key="direction"><option value="">${{projectsSummaryStrings.all}}</option></select>`;
    }}
    let projectsSummaryFilterInputs = Array.from(document.querySelectorAll(".summary-filter-input"));
    let allProjectsSummaryGroups = [];
    let projectsSummaryFilters = {{}};
    let projectsSummarySortKey = "";
    let projectsSummarySortDirection = "asc";
    let currentProjectsSummaryReportDate = String(projectsSummaryDateInput?.value || "{todayIso}");
    let currentProjectsSummaryReportYear = String(currentProjectsSummaryReportDate).slice(0, 4);

    function formatSummaryHours(value) {{
      if (value === null || value === undefined || value === "") {{
        return projectsSummaryStrings.dash;
      }}
      const number = Number(value);
      if (!Number.isFinite(number)) {{
        return projectsSummaryStrings.dash;
      }}
      return number.toLocaleString("ru-RU", {{ minimumFractionDigits: 1, maximumFractionDigits: 1 }});
    }}

    function formatSummaryText(value) {{
      const text = String(value ?? "").trim();
      return text || projectsSummaryStrings.dash;
    }}

    function formatSummaryTotal(value) {{
      return hasSummaryValue(value) ? formatSummaryHours(value) : projectsSummaryStrings.dash;
    }}

    function buildProjectsSummaryMetaText(groupsCount, rowsCount) {{
      return `\u0414\u0430\u0442\u0430 \u043e\u0442\u0447\u0435\u0442\u0430: ${{currentProjectsSummaryReportDate}}. \u0413\u043e\u0434 \u043e\u0442\u0447\u0435\u0442\u0430: ${{currentProjectsSummaryReportYear || projectsSummaryStrings.dash}}. \u0413\u0440\u0443\u043f\u043f: ${{groupsCount}}. \u0421\u0442\u0440\u043e\u043a: ${{rowsCount}}.`;
    }}

    function syncProjectsSummaryStickyOffsets() {{
      if (!(projectsSummaryTableWrap instanceof HTMLElement)) {{
        return;
      }}
      const firstHeaderCell = projectsSummaryHeaderCells[0];
      if (!(firstHeaderCell instanceof HTMLElement)) {{
        return;
      }}
      const headerHeight = Math.ceil(firstHeaderCell.getBoundingClientRect().height || 44);
      projectsSummaryTableWrap.style.setProperty("--projects-summary-header-height", `${{headerHeight}}px`);
    }}

    function buildPlanningProjectLink(projectId, redmineIdentifier = "", projectName = "") {{
      const id = String(projectId ?? "").trim();
      if (id) {{
        return `/planning-projects?planning_project_id=${{encodeURIComponent(id)}}&open_mode=edit`;
      }}
      const identifier = String(redmineIdentifier ?? "").trim();
      const normalizedProjectName = String(projectName ?? "").trim();
      return identifier
        ? `/planning-projects?redmine_identifier=${{encodeURIComponent(identifier)}}&project_name=${{encodeURIComponent(normalizedProjectName)}}&open_mode=auto`
        : "";
    }}

    function buildSnapshotIssuesLink(projectRedmineId) {{
      const redmineId = String(projectRedmineId ?? "").trim();
      if (!redmineId) {{
        return "";
      }}
      const params = new URLSearchParams();
      if (currentProjectsSummaryReportDate) {{
        params.set("captured_for_date", currentProjectsSummaryReportDate);
      }}
      const query = params.toString();
      return `/projects/${{encodeURIComponent(redmineId)}}/latest-snapshot-issues${{query ? `?${{query}}` : ""}}`;
    }}

    function hasSummaryValue(value) {{
      return !(value === null || value === undefined || String(value).trim() === "");
    }}

    function wrapSummaryLink(content, projectId, redmineIdentifier = "", projectName = "") {{
      const href = buildPlanningProjectLink(projectId, redmineIdentifier, projectName);
      if (!href) {{
        return content;
      }}
      return `<a href="${{href}}" target="_blank" rel="noreferrer" style="color:inherit; text-decoration:none; border-bottom:1px dashed #b7c1cb;">${{content}}</a>`;
    }}

    function wrapCustomSummaryLink(content, href) {{
      if (!href) {{
        return content;
      }}
      return `<a href="${{href}}" target="_blank" rel="noreferrer" style="color:inherit; text-decoration:none; border-bottom:1px dashed #b7c1cb;">${{content}}</a>`;
    }}

    function normalizeSummaryFilterValue(value) {{
      return String(value ?? "").trim().toLowerCase();
    }}

    function updateProjectsSummarySortIndicators() {{
      projectsSummaryHeaderCells.forEach((cell, index) => {{
        const key = projectsSummaryColumnKeys[index] || "";
        if (!key) {{
          return;
        }}
        cell.dataset.sortKey = key;
        const label = String(cell.dataset.label || cell.textContent || "").replace(/[↑↓]$/, "").trim();
        cell.dataset.label = label;
        const indicator = key === projectsSummarySortKey
          ? (projectsSummarySortDirection === "desc" ? " ↓" : " ↑")
          : "";
        cell.textContent = `${{label}}${{indicator}}`;
      }});
    }}

    function compareProjectsSummaryValues(leftValue, rightValue, key) {{
      if (projectsSummaryNumericKeys.has(key)) {{
        const leftNumber = Number(leftValue);
        const rightNumber = Number(rightValue);
        const leftMissing = !Number.isFinite(leftNumber);
        const rightMissing = !Number.isFinite(rightNumber);
        if (leftMissing && rightMissing) {{
          return 0;
        }}
        if (leftMissing) {{
          return 1;
        }}
        if (rightMissing) {{
          return -1;
        }}
        return leftNumber - rightNumber;
      }}

      const leftText = String(leftValue ?? "").trim().toLocaleLowerCase("ru");
      const rightText = String(rightValue ?? "").trim().toLocaleLowerCase("ru");
      if (!leftText && !rightText) {{
        return 0;
      }}
      if (!leftText) {{
        return 1;
      }}
      if (!rightText) {{
        return -1;
      }}
      return leftText.localeCompare(rightText, "ru");
    }}

    function populateProjectsSummaryDirectionFilter(groups) {{
      const directionSelect = document.querySelector('.summary-filter-input[data-filter-key="direction"]');
      if (!(directionSelect instanceof HTMLSelectElement)) {{
        return;
      }}
      const currentValue = String(projectsSummaryFilters.direction ?? directionSelect.value ?? "");
      const directions = new Set();
      for (const group of groups) {{
        for (const item of Array.isArray(group.items) ? group.items : []) {{
          const direction = String(item.direction ?? "").trim();
          if (direction) {{
            directions.add(direction);
          }}
        }}
      }}
      directionSelect.innerHTML = "";
      const allOption = document.createElement("option");
      allOption.value = "";
      allOption.textContent = projectsSummaryStrings.all;
      directionSelect.appendChild(allOption);
      const emptyOption = document.createElement("option");
      emptyOption.value = "__empty__";
      emptyOption.textContent = projectsSummaryStrings.empty;
      directionSelect.appendChild(emptyOption);
      for (const direction of Array.from(directions).sort((left, right) => left.localeCompare(right, "ru"))) {{
        const option = document.createElement("option");
        option.value = direction;
        option.textContent = direction;
        directionSelect.appendChild(option);
      }}
      directionSelect.value = currentValue === "__empty__" || directions.has(currentValue) ? currentValue : "";
      projectsSummaryFilters.direction = String(directionSelect.value || "");
      projectsSummaryFilterInputs = Array.from(document.querySelectorAll(".summary-filter-input"));
    }}

    function applyProjectsSummaryFilters(groups) {{
      const normalizedFilters = Object.fromEntries(
        Object.entries(projectsSummaryFilters).map(([key, value]) => [key, normalizeSummaryFilterValue(value)])
      );
      const hasFilters = Object.values(normalizedFilters).some(Boolean);
      if (!hasFilters) {{
        return groups;
      }}

      return groups
        .map((group) => {{
          const groupIdentifier = String(group.redmine_identifier ?? "");
          const factValue = String(group.development_spent_hours_year_average ?? "");
          const limitValue = String(group.development_limit_hours ?? "");
          if (normalizedFilters.redmine_identifier && !normalizeSummaryFilterValue(groupIdentifier).includes(normalizedFilters.redmine_identifier)) {{
            return null;
          }}
          if (normalizedFilters.development_spent_hours_year_average && !normalizeSummaryFilterValue(factValue).includes(normalizedFilters.development_spent_hours_year_average)) {{
            return null;
          }}
          if (normalizedFilters.development_limit_hours && !normalizeSummaryFilterValue(limitValue).includes(normalizedFilters.development_limit_hours)) {{
            return null;
          }}

          const visibleItems = (Array.isArray(group.items) ? group.items : []).filter((item) => {{
            const fieldMap = {{
              direction: item.direction,
              customer: item.customer,
              project_name: item.project_name,
              pm_name: item.pm_name,
              report_year_hours: item.report_year_hours,
              development_hours: item.development_hours,
            }};
            return Object.entries(fieldMap).every(([key, rawValue]) => {{
              const filterValue = normalizedFilters[key];
              if (!filterValue) {{
                return true;
              }}
              if (key === "direction" && filterValue === "__empty__") {{
                return !String(rawValue ?? "").trim();
              }}
              return normalizeSummaryFilterValue(rawValue).includes(filterValue);
            }});
          }});

          if (!visibleItems.length) {{
            return null;
          }}

          return {{
            ...group,
            items: visibleItems,
            source_row_span: Number(group.row_span || (Array.isArray(group.items) ? group.items.length : 0)),
            row_span: visibleItems.length,
            development_limit_hours: visibleItems.some((item) => item.report_year_hours !== null && item.report_year_hours !== undefined && item.report_year_hours !== "" || item.development_hours !== null && item.development_hours !== undefined && item.development_hours !== "")
              ? visibleItems.reduce(
                  (sum, item) => sum + Number(item.report_year_hours || 0) + Number(item.development_hours || 0),
                  0,
                )
              : null,
          }};
        }})
        .filter(Boolean);
    }}

    function sortProjectsSummaryGroups(groups) {{
      const sortedGroups = groups.map((group) => {{
        const clonedItems = Array.isArray(group.items) ? [...group.items] : [];
        return {{
          ...group,
          items: clonedItems,
        }};
      }});

      if (!projectsSummarySortKey || !projectsSummarySortableKeys.has(projectsSummarySortKey)) {{
        return sortedGroups;
      }}

      const sortMultiplier = projectsSummarySortDirection === "desc" ? -1 : 1;

      if (projectsSummaryItemKeys.has(projectsSummarySortKey)) {{
        sortedGroups.forEach((group) => {{
          group.items.sort((left, right) => (
            compareProjectsSummaryValues(left?.[projectsSummarySortKey], right?.[projectsSummarySortKey], projectsSummarySortKey) * sortMultiplier
          ));
          group.row_span = group.items.length;
          const hasLimitValues = group.items.some((item) => item?.report_year_hours !== null && item?.report_year_hours !== undefined && item?.report_year_hours !== "" || item?.development_hours !== null && item?.development_hours !== undefined && item?.development_hours !== "");
          group.development_limit_hours = hasLimitValues
            ? group.items.reduce((sum, item) => sum + Number(item?.report_year_hours || 0) + Number(item?.development_hours || 0), 0)
            : null;
        }});
      }}

      sortedGroups.sort((left, right) => {{
        const leftValue = projectsSummaryItemKeys.has(projectsSummarySortKey)
          ? left?.items?.[0]?.[projectsSummarySortKey]
          : left?.[projectsSummarySortKey];
        const rightValue = projectsSummaryItemKeys.has(projectsSummarySortKey)
          ? right?.items?.[0]?.[projectsSummarySortKey]
          : right?.[projectsSummarySortKey];
        const primary = compareProjectsSummaryValues(leftValue, rightValue, projectsSummarySortKey);
        if (primary !== 0) {{
          return primary * sortMultiplier;
        }}
        return compareProjectsSummaryValues(left?.redmine_identifier, right?.redmine_identifier, "redmine_identifier");
      }});

      return sortedGroups;
    }}

    function refreshProjectsSummaryView() {{
      const filteredGroups = applyProjectsSummaryFilters(allProjectsSummaryGroups);
      const visibleGroups = sortProjectsSummaryGroups(filteredGroups);
      const rowsCount = visibleGroups.reduce((sum, group) => sum + Number(group.row_span || 0), 0);
      projectsSummaryMeta.textContent = `Р”Р°С‚Р° РѕС‚С‡РµС‚Р°: ${{currentProjectsSummaryReportDate}}. Р“РѕРґ РѕС‚С‡РµС‚Р°: ${{currentProjectsSummaryReportYear || "вЂ”"}}. Р“СЂСѓРїРї: ${{visibleGroups.length}}. РЎС‚СЂРѕРє: ${{rowsCount}}.`;
      projectsSummaryMeta.textContent = buildProjectsSummaryMetaText(visibleGroups.length, rowsCount);
      updateProjectsSummarySortIndicators();
      renderProjectsSummaryRows(visibleGroups);
      renderProjectsSummaryTotals(visibleGroups);
      syncProjectsSummaryStickyOffsets();
    }}

    function renderProjectsSummaryRows(groups) {{
      if (!groups.length) {{
        projectsSummaryTableBody.innerHTML = `<tr><td colspan="9" class="empty-state">${{projectsSummaryStrings.noRows}}</td></tr>`;
        if (projectsSummaryTableFoot) {{
          projectsSummaryTableFoot.innerHTML = "";
        }}
        return;
      }}

      projectsSummaryTableBody.innerHTML = groups.map((group) => {{
        const items = Array.isArray(group.items) ? group.items : [];
        const rowSpan = Number(group.row_span || items.length || 1);
        const sourceRowSpan = Number(group.source_row_span || rowSpan || 1);
        const factIsFiltered = sourceRowSpan > rowSpan;
        const groupIdentifier = String(group.redmine_identifier ?? "");
        const groupProjectRedmineId = String(group.project_redmine_id ?? "");
        const groupLinkProjectId = items.length === 1 ? items[0].id : "";
        const groupProjectName = String(items[0]?.link_project_name ?? items[0]?.project_name ?? "");
        const snapshotIssuesHref = buildSnapshotIssuesLink(groupProjectRedmineId);
        const identifierCell = `<td class="mono group-cell" rowspan="${{rowSpan}}">${{formatSummaryText(group.redmine_identifier)}}</td>`;
        const factContent = wrapCustomSummaryLink(
          formatSummaryHours(group.development_spent_hours_year_average),
          hasSummaryValue(group.development_spent_hours_year_average) ? snapshotIssuesHref : "",
        );
        const factLabel = factIsFiltered
          ? `<span style="color:#8a97a5;">${{factContent}} (${{projectsSummaryStrings.factFiltered}})</span>`
          : factContent;
        const factCell = `<td class="group-cell" rowspan="${{rowSpan}}">${{factLabel}}</td>`;
        const limitCell = `<td class="group-cell" rowspan="${{rowSpan}}">${{hasSummaryValue(group.development_limit_hours) ? wrapSummaryLink(formatSummaryHours(group.development_limit_hours), groupLinkProjectId, groupIdentifier, groupProjectName) : formatSummaryHours(group.development_limit_hours)}}</td>`;
        return items.map((item, index) => `
          <tr>
            ${{index === 0 ? identifierCell : ""}}
            ${{index === 0 ? factCell : ""}}
            <td class="${{item.question_flag ? "summary-project-flagged" : ""}}">${{formatSummaryText(item.direction)}}</td>
            <td class="${{item.question_flag ? "summary-project-flagged" : ""}}">${{formatSummaryText(item.customer)}}</td>
            <td class="${{item.question_flag ? "summary-project-flagged" : ""}}">${{wrapSummaryLink(formatSummaryText(item.project_name), item.id, groupIdentifier, item.link_project_name)}}</td>
            <td class="${{item.question_flag ? "summary-project-flagged" : ""}}">${{formatSummaryText(item.pm_name)}}</td>
            ${{index === 0 ? limitCell : ""}}
            <td class="${{item.question_flag ? "summary-project-flagged" : ""}}">${{hasSummaryValue(item.report_year_hours) ? wrapSummaryLink(formatSummaryHours(item.report_year_hours), item.id, groupIdentifier, item.link_project_name) : formatSummaryHours(item.report_year_hours)}}</td>
            <td class="${{item.question_flag ? "summary-project-flagged" : ""}}">${{hasSummaryValue(item.development_hours) ? wrapSummaryLink(formatSummaryHours(item.development_hours), item.id, groupIdentifier, item.link_project_name) : formatSummaryHours(item.development_hours)}}</td>
          </tr>
        `).join("");
      }}).join("");
    }}

    function renderProjectsSummaryTotals(groups) {{
      if (!(projectsSummaryTableFoot instanceof HTMLElement)) {{
        return;
      }}
      if (!groups.length) {{
        projectsSummaryTableFoot.innerHTML = "";
        return;
      }}

      let factTotal = 0;
      let factHasValues = false;
      let limitTotal = 0;
      let limitHasValues = false;
      let reportYearTotal = 0;
      let reportYearHasValues = false;
      let developmentTotal = 0;
      let developmentHasValues = false;

      for (const group of groups) {{
        const factValue = Number(group?.development_spent_hours_year_average);
        if (Number.isFinite(factValue)) {{
          factTotal += factValue;
          factHasValues = true;
        }}

        const limitValue = Number(group?.development_limit_hours);
        if (Number.isFinite(limitValue)) {{
          limitTotal += limitValue;
          limitHasValues = true;
        }}

        for (const item of Array.isArray(group?.items) ? group.items : []) {{
          const reportYearValue = Number(item?.report_year_hours);
          if (Number.isFinite(reportYearValue)) {{
            reportYearTotal += reportYearValue;
            reportYearHasValues = true;
          }}

          const developmentValue = Number(item?.development_hours);
          if (Number.isFinite(developmentValue)) {{
            developmentTotal += developmentValue;
            developmentHasValues = true;
          }}
        }}
      }}

      projectsSummaryTableFoot.innerHTML = `
        <tr>
          <td class="totals-label-cell mono">Итого</td>
          <td>${{formatSummaryTotal(factHasValues ? factTotal : null)}}</td>
          <td class="totals-spacer-cell" colspan="4"></td>
          <td>${{formatSummaryTotal(limitHasValues ? limitTotal : null)}}</td>
          <td>${{formatSummaryTotal(reportYearHasValues ? reportYearTotal : null)}}</td>
          <td>${{formatSummaryTotal(developmentHasValues ? developmentTotal : null)}}</td>
        </tr>
      `;
    }}

    function buildProjectsSummaryParams() {{
      const params = new URLSearchParams();
      params.set("report_date", String(projectsSummaryDateInput.value || "{todayIso}"));
      params.set("enabled_only", projectsSummaryEnabledOnlyCheckbox?.checked ? "true" : "false");
      return params;
    }}

    async function loadProjectsSummary() {{
      projectsSummaryMeta.textContent = projectsSummaryStrings.loading;
      projectsSummaryTableBody.innerHTML = `<tr><td colspan="9" class="empty-state">${{projectsSummaryStrings.loading}}</td></tr>`;
      if (projectsSummaryTableFoot) {{
        projectsSummaryTableFoot.innerHTML = "";
      }}
      const params = buildProjectsSummaryParams();

      try {{
        const response = await fetch(`/api/projects-summary-v2?${{params.toString()}}`);
        const payload = await response.json();
        if (!response.ok) {{
          throw new Error(payload.detail || "Не удалось загрузить сводку по проектам.");
        }}

        currentProjectsSummaryReportDate = String(payload.report_date || projectsSummaryDateInput.value || "{todayIso}");
        currentProjectsSummaryReportYear = String(payload.report_year || currentProjectsSummaryReportDate.slice(0, 4) || "");
        allProjectsSummaryGroups = Array.isArray(payload.groups) ? payload.groups : [];
        populateProjectsSummaryDirectionFilter(allProjectsSummaryGroups);
        refreshProjectsSummaryView();
      }} catch (error) {{
        projectsSummaryMeta.textContent = projectsSummaryStrings.error;
        projectsSummaryTableBody.innerHTML = `<tr><td colspan="9" class="empty-state">${{projectsSummaryStrings.loadFailed}}</td></tr>`;
        if (projectsSummaryTableFoot) {{
          projectsSummaryTableFoot.innerHTML = "";
        }}
      }}
    }}

    projectsSummaryRefreshButton?.addEventListener("click", loadProjectsSummary);
    projectsSummaryDateInput?.addEventListener("change", loadProjectsSummary);
    projectsSummaryEnabledOnlyCheckbox?.addEventListener("change", loadProjectsSummary);
    exportProjectsSummaryButton?.addEventListener("click", () => {{
      const params = buildProjectsSummaryParams();
      window.location.href = `/api/projects-summary/export.csv?${{params.toString()}}`;
    }});
    resetProjectsSummaryFiltersButton?.addEventListener("click", () => {{
      projectsSummaryFilters = {{}};
      projectsSummarySortKey = "";
      projectsSummarySortDirection = "asc";
      projectsSummaryEnabledOnlyCheckbox.checked = true;
      projectsSummaryFilterInputs.forEach((input) => {{
        input.value = "";
      }});
      loadProjectsSummary();
    }});
    projectsSummaryFilterInputs.forEach((input) => {{
      const key = String(input.dataset.filterKey || "");
      projectsSummaryFilters[key] = String(input.value || "");
      input.addEventListener("input", () => {{
        projectsSummaryFilters[key] = String(input.value || "");
        refreshProjectsSummaryView();
      }});
    }});

    projectsSummaryHeaderCells.forEach((cell, index) => {{
      const key = projectsSummaryColumnKeys[index] || "";
      if (!key) {{
        return;
      }}
      cell.dataset.sortKey = key;
      cell.addEventListener("click", () => {{
        if (projectsSummarySortKey === key) {{
          projectsSummarySortDirection = projectsSummarySortDirection === "asc" ? "desc" : "asc";
        }} else {{
          projectsSummarySortKey = key;
          projectsSummarySortDirection = "asc";
        }}
        refreshProjectsSummaryView();
      }});
    }});

    document.addEventListener("change", (event) => {{
      const target = event.target;
      if (!(target instanceof Element) || !target.classList.contains("summary-filter-input")) {{
        return;
      }}
      const key = String(target.getAttribute("data-filter-key") || "");
      projectsSummaryFilters[key] = String(target.value || "");
      refreshProjectsSummaryView();
    }});

    document.addEventListener("input", (event) => {{
      const target = event.target;
      if (!(target instanceof Element) || !target.classList.contains("summary-filter-input")) {{
        return;
      }}
      const key = String(target.getAttribute("data-filter-key") || "");
      projectsSummaryFilters[key] = String(target.value || "");
      refreshProjectsSummaryView();
    }});

    updateProjectsSummarySortIndicators();
    window.addEventListener("resize", syncProjectsSummaryStickyOffsets);

    loadProjectsSummary();
  </script>
</body>
</html>"""


@app.get("/projects-summary", response_class=HTMLResponse)
def getProjectsSummaryPage() -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    ensurePlanningProjectsTable()
    return _renderHtmlPage(buildProjectsSummaryPage())


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
    ensurePlanningProjectsTable()
    return {
        "projects": listStoredProjects(),
        "planning_project_identifiers": listPlanningProjectIdentifiers(),
    }


@app.get("/api/planning-projects")
def getPlanningProjects(
    q: str | None = Query(None),
    include_closed: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    return {
        "projects": listPlanningProjects(searchText=q, includeClosed=include_closed, limit=limit),
        "total": countPlanningProjects(searchText=q, includeClosed=include_closed),
        "limit": limit,
    }


@app.get("/api/projects-summary")
def getProjectsSummaryApi(
    report_date: str | None = Query(None),
    direction: str | None = Query("КОТ"),
    is_closed: bool = Query(False),
    enabled_only: bool = Query(True),
) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    ensurePlanningProjectsTable()

    try:
        reportDate = date.fromisoformat(str(report_date or date.today().isoformat()))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Некорректная дата отчета.") from exc

    projects = _listProjectsSummaryRows(
        reportDate=reportDate,
        direction=direction,
        isClosed=is_closed,
        enabledOnly=enabled_only,
    )
    return {
        "projects": projects,
        "report_date": reportDate.isoformat(),
        "report_year": reportDate.year,
        "direction": str(direction or "").strip(),
        "is_closed": bool(is_closed),
        "enabled_only": bool(enabled_only),
    }


@app.get("/api/projects-summary-v2")
def getProjectsSummaryApiV2(
    report_date: str | None = Query(None),
    direction: str | None = Query(None),
    is_closed: bool | None = Query(None),
    enabled_only: bool = Query(True),
) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    ensurePlanningProjectsTable()

    try:
        reportDate = date.fromisoformat(str(report_date or date.today().isoformat()))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Некорректная дата отчета.") from exc

    projects = _listProjectsSummaryRows(
        reportDate=reportDate,
        direction=direction,
        isClosed=is_closed,
        enabledOnly=enabled_only,
    )
    groups = _buildProjectsSummaryGroups(projects)
    return {
        "projects": projects,
        "groups": groups,
        "report_date": reportDate.isoformat(),
        "report_year": reportDate.year,
        "direction": str(direction or "").strip(),
        "is_closed": None if is_closed is None else bool(is_closed),
        "enabled_only": bool(enabled_only),
    }


@app.get("/api/projects-summary/export.csv")
def exportProjectsSummaryCsv(
    report_date: str | None = Query(None),
    direction: str | None = Query(None),
    is_closed: bool | None = Query(None),
    enabled_only: bool = Query(True),
) -> Response:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureProjectsTable()
    ensureIssueSnapshotTables()
    ensurePlanningProjectsTable()

    try:
        reportDate = date.fromisoformat(str(report_date or date.today().isoformat()))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Некорректная дата отчета.") from exc

    projects = _listProjectsSummaryRows(
        reportDate=reportDate,
        direction=direction,
        isClosed=is_closed,
        enabledOnly=enabled_only,
    )
    groups = _buildProjectsSummaryGroups(projects)

    output = io.StringIO(newline="")
    output.write("sep=;\n")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(
        [
            "Идентификатор в Redmine",
            "Разработка: факт за год, ч",
            "Направление",
            "Заказчик",
            "Название проекта",
            "ПМ",
            "Лимит разработки с багфиксом",
            "Часы за год отчета",
            "Часы разработки с багфиксом",
        ]
    )
    for group in groups:
        items = list(group.get("items") or [])
        identifier = str(group.get("redmine_identifier") or "")
        factAverage = group.get("development_spent_hours_year_average")
        developmentLimit = group.get("development_limit_hours")
        for itemIndex, item in enumerate(items):
            writer.writerow(
                [
                    identifier if itemIndex == 0 else "",
                    formatPageHours(factAverage) if itemIndex == 0 and factAverage not in (None, "") else "",
                    str(item.get("direction") or ""),
                    str(item.get("customer") or ""),
                    str(item.get("project_name") or ""),
                    str(item.get("pm_name") or ""),
                    formatPageHours(developmentLimit) if itemIndex == 0 and developmentLimit not in (None, "") else "",
                    formatPageHours(item.get("report_year_hours")) if item.get("report_year_hours") not in (None, "") else "",
                    formatPageHours(item.get("development_hours")) if item.get("development_hours") not in (None, "") else "",
                ]
            )

    csvBytes = output.getvalue().encode("cp1251", errors="replace")
    return Response(
        content=csvBytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": 'attachment; filename="projects_summary.csv"'},
    )


@app.get("/api/planning-projects/export.csv")
def exportPlanningProjectsCsv(
    q: str | None = Query(None),
    include_closed: bool = Query(False),
) -> Response:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    projects = listPlanningProjects(searchText=q, includeClosed=include_closed, limit=None)

    output = io.StringIO(newline="")
    output.write("sep=;\n")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(
        [
            "Направление",
            "Закрыт",
            "Заказчик",
            "Название проекта",
            "Идентификатор в Redmine",
            "ПМ",
            "Дата старта",
            "Дата окончания",
            "Часы разработки с багфиксом",
            "Год 1",
            "Часы 1",
            "Год 2",
            "Часы 2",
            "Год 3",
            "Часы 3",
            "Базовая оценка",
            "P1 (факт / база), %",
            "P2 (факт с багами / факт), %",
            "Док с оценкой",
            "Bitrix",
            "Комментарий",
        ]
    )
    for project in projects:
        writer.writerow(
            [
                str(project.get("direction") or ""),
                "Да" if project.get("is_closed") else "",
                str(project.get("customer") or ""),
                str(project.get("project_name") or ""),
                str(project.get("redmine_identifier") or ""),
                str(project.get("pm_name") or ""),
                str(project.get("start_date") or ""),
                str(project.get("end_date") or ""),
                formatPageHours(project.get("development_hours")) if project.get("development_hours") not in (None, "") else "",
                str(project.get("year_1") or ""),
                formatPageHours(project.get("hours_1")) if project.get("hours_1") not in (None, "") else "",
                str(project.get("year_2") or ""),
                formatPageHours(project.get("hours_2")) if project.get("hours_2") not in (None, "") else "",
                str(project.get("year_3") or ""),
                formatPageHours(project.get("hours_3")) if project.get("hours_3") not in (None, "") else "",
                formatPageHours(project.get("baseline_estimate_hours")) if project.get("baseline_estimate_hours") not in (None, "") else "",
                formatPageHours(project.get("p1")) if project.get("p1") not in (None, "") else "",
                formatPageHours(project.get("p2")) if project.get("p2") not in (None, "") else "",
                str(project.get("estimate_doc_url") or ""),
                str(project.get("bitrix_url") or ""),
                str(project.get("comment_text") or ""),
            ]
        )

    csvBytes = output.getvalue().encode("cp1251", errors="replace")
    return Response(
        content=csvBytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": 'attachment; filename="planning_projects.csv"'},
    )


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
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return {"project": updatedProject, "projects": listPlanningProjects()}


@app.delete("/api/planning-projects/{planning_project_id}")
def deletePlanningProjectApi(planning_project_id: int) -> dict[str, object]:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensurePlanningProjectsTable()
    deleted = deletePlanningProject(planning_project_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return {"deleted": True, "projects": listPlanningProjects()}


@app.get("/projects/{project_redmine_id}/latest-snapshot-issues", response_class=HTMLResponse)
def getProjectLatestSnapshotIssuesPage(
    project_redmine_id: int,
    captured_for_date: str | None = Query(None, description="Дата среза в формате YYYY-MM-DD"),
) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    return _renderHtmlPage(buildLatestSnapshotIssuesPageClean(project_redmine_id, captured_for_date))


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
            "План с рисками, ч",
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
                formatPageHours(issue.get("risk_estimate_hours")),
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


@app.get("/projects/{project_redmine_id}/time-entries", response_class=HTMLResponse)
def getProjectSnapshotTimeEntriesPage(
    project_redmine_id: int,
    captured_for_date: str | None = Query(None, description="Дата среза в формате YYYY-MM-DD"),
    date_from: str | None = Query(None, description="Дата начала периода в формате YYYY-MM-DD"),
    date_to: str | None = Query(None, description="Дата конца периода в формате YYYY-MM-DD"),
) -> HTMLResponse:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    ensureProjectsTable()
    return _renderHtmlPage(
        buildSnapshotTimeEntriesPage(
            project_redmine_id,
            captured_for_date,
            date_from,
            date_to,
        )
    )


@app.get("/projects/{project_redmine_id}/time-entries/export.csv")
def exportProjectSnapshotTimeEntriesCsv(
    request: Request,
    project_redmine_id: int,
    captured_for_date: str | None = Query(None, description="Дата среза в формате YYYY-MM-DD"),
    date_from: str | None = Query(None, description="Дата начала периода в формате YYYY-MM-DD"),
    date_to: str | None = Query(None, description="Дата конца периода в формате YYYY-MM-DD"),
    id: str | None = Query(None),
    snapshot_run_id: str | None = Query(None),
    project_name: str | None = Query(None),
    time_entry_redmine_id: str | None = Query(None),
    issue_redmine_id: str | None = Query(None),
    issue_subject: str | None = Query(None),
    issue_tracker_name: str | None = Query(None),
    issue_status_name: str | None = Query(None),
    user_id: str | None = Query(None),
    user_name: str | None = Query(None),
    activity_id: str | None = Query(None),
    activity_name: str | None = Query(None),
    hours: str | None = Query(None),
    comments: str | None = Query(None),
    spent_on: str | None = Query(None),
    created_on: str | None = Query(None),
    updated_on: str | None = Query(None),
) -> Response:
    if not config.databaseUrl:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    queryParams = request.query_params
    filters = {
        "id": id,
        "snapshot_run_id": snapshot_run_id,
        "project_redmine_id": queryParams.get("project_redmine_id"),
        "project_name": project_name,
        "time_entry_redmine_id": time_entry_redmine_id,
        "issue_redmine_id": issue_redmine_id,
        "issue_subject": issue_subject,
        "issue_tracker_name": queryParams.getlist("issue_tracker_name") or issue_tracker_name,
        "issue_status_name": queryParams.getlist("issue_status_name") or issue_status_name,
        "user_id": user_id,
        "user_name": user_name,
        "activity_id": activity_id,
        "activity_name": queryParams.getlist("activity_name") or activity_name,
        "hours": hours,
        "comments": comments,
        "spent_on": spent_on,
        "created_on": created_on,
        "updated_on": updated_on,
    }
    today = date.today()
    selectedCapturedForDate = _normalizeSnapshotTimeEntriesDateValue(captured_for_date, today.isoformat())
    selectedDateFrom = _normalizeSnapshotTimeEntriesDateValue(date_from, date(today.year, 1, 1).isoformat())
    selectedDateTo = _normalizeSnapshotTimeEntriesDateValue(date_to, today.isoformat())
    if selectedDateFrom > selectedDateTo:
        selectedDateFrom, selectedDateTo = selectedDateTo, selectedDateFrom

    payload = getSnapshotTimeEntriesForProjectByDateRange(
        project_redmine_id,
        selectedCapturedForDate,
        selectedDateFrom,
        selectedDateTo,
    )
    snapshotRun = payload.get("snapshot_run")
    if snapshotRun is None:
        raise HTTPException(status_code=404, detail="Срез проекта не найден")

    exportEntries = _applySnapshotTimeEntriesFilters(list(payload.get("time_entries") or []), filters)

    output = io.StringIO(newline="")
    output.write("sep=;\n")
    writer = csv.writer(output, delimiter=";")
    writer.writerow([str(column["label"]) for column in SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG])
    for entry in exportEntries:
        writer.writerow([
            _formatSnapshotTimeEntryCellValue(str(column["key"]), entry.get(column["key"]))
            for column in SNAPSHOT_TIME_ENTRY_COLUMN_CONFIG
        ])

    fileIdentifier = str(snapshotRun.get("project_identifier") or f"project_{project_redmine_id}")
    safeIdentifier = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in fileIdentifier)
    fileName = f"time_entries_{safeIdentifier}_{selectedCapturedForDate}_{selectedDateFrom}_{selectedDateTo}.csv"
    csvBytes = output.getvalue().encode("cp1251", errors="replace")
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


@app.get("/api/issues/snapshots/capture-errors")
def getIssueSnapshotCaptureErrors(limit: int = Query(50, ge=1, le=500)) -> dict[str, object]:
    ensureIssueSnapshotTables()
    return {"errors": listIssueSnapshotCaptureErrors(limit)}


@app.get("/api/redmine/issues/{issue_redmine_id}/snapshot-diagnostics")
def getRedmineIssueSnapshotDiagnostics(
    issue_redmine_id: int,
    project_redmine_id: int = Query(..., description="Redmine ID проекта, для которого проверяем попадание в срез"),
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
            f"Задача сейчас относится к проекту «{issueProjectName or 'без названия'}» "
            f"(id={issueProjectId}), а срез собирается для проекта «{project.get('name') or project_redmine_id}» "
            f"(id={project_redmine_id}) без подпроектов."
        )
    elif not partialLoad:
        includedInSnapshot = True
        inclusionReason = (
            "Для проекта выключена частичная загрузка, поэтому в срез попадают все задачи самого проекта "
            "без дополнительного отбора по статусу или дате закрытия."
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
                    if str(field.get("name") or "") == "Базовая оценка"
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
    project_name: str = Query(..., description="Полное или частичное имя проекта в Redmine"),
    field_name: str = Query(..., description="Название или часть названия кастомного поля"),
    sample_size: int = Query(10, ge=1, le=30, description="Сколько задач проверить в проекте"),
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
        "build_id": APP_BUILD_ID,
        "environment": config.appEnv,
        "database_configured": bool(config.databaseUrl),
        "redmine_configured": bool(config.redmineUrl and config.apiKey),
        "render_git_commit": os.getenv("RENDER_GIT_COMMIT", ""),
        "render_git_branch": os.getenv("RENDER_GIT_BRANCH", ""),
        "render_service_id": os.getenv("RENDER_SERVICE_ID", ""),
        "render_service_name": os.getenv("RENDER_SERVICE_NAME", ""),
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
