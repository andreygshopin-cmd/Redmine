from datetime import datetime, UTC

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from src.redmine.config import loadConfig
from src.redmine.db import checkDatabaseConnection


config = loadConfig()
app = FastAPI(title="Redmine API", version="0.1.0")


@app.get("/")
def readRoot() -> HTMLResponse:
    return HTMLResponse(
        """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Server Time</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f4efe6;
        --panel: #fffaf2;
        --text: #1f2937;
        --muted: #6b7280;
        --accent: #14532d;
        --accent-2: #d97706;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        font-family: Georgia, "Times New Roman", serif;
        background:
          radial-gradient(circle at top left, rgba(217, 119, 6, 0.18), transparent 32%),
          radial-gradient(circle at bottom right, rgba(20, 83, 45, 0.18), transparent 30%),
          var(--bg);
        color: var(--text);
      }

      .card {
        width: min(92vw, 680px);
        padding: 32px;
        border: 1px solid rgba(31, 41, 55, 0.08);
        border-radius: 24px;
        background: var(--panel);
        box-shadow: 0 24px 60px rgba(31, 41, 55, 0.12);
      }

      .eyebrow {
        margin: 0 0 12px;
        color: var(--accent-2);
        font-size: 13px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }

      h1 {
        margin: 0 0 12px;
        font-size: clamp(32px, 6vw, 56px);
        line-height: 0.95;
      }

      p {
        margin: 0;
        color: var(--muted);
        font-size: 18px;
      }

      .actions {
        display: flex;
        gap: 12px;
        margin-top: 28px;
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

      button:hover {
        transform: translateY(-1px);
      }

      button:disabled {
        opacity: 0.7;
        cursor: wait;
        transform: none;
      }

      .time-box {
        margin-top: 26px;
        padding: 22px;
        border-radius: 18px;
        background: white;
        border: 1px solid rgba(31, 41, 55, 0.08);
      }

      .label {
        display: block;
        font-size: 12px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.1em;
      }

      .time {
        margin-top: 10px;
        font-size: clamp(28px, 5vw, 42px);
        font-weight: 700;
      }

      .meta {
        margin-top: 10px;
        font-size: 14px;
        color: var(--muted);
      }
    </style>
  </head>
  <body>
    <main class="card">
      <p class="eyebrow">FastAPI client page</p>
      <h1>Server time</h1>
      <p>This page calls the Python backend and requests the current server time.</p>

      <div class="actions">
        <button id="load-time" type="button">Get current time</button>
      </div>

      <section class="time-box" aria-live="polite">
        <span class="label">Current server time</span>
        <div id="time-value" class="time">--:--:--</div>
        <div id="time-meta" class="meta">Click the button to load the current time.</div>
      </section>
    </main>

    <script>
      const button = document.getElementById("load-time");
      const timeValue = document.getElementById("time-value");
      const timeMeta = document.getElementById("time-meta");

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

      button.addEventListener("click", loadServerTime);
    </script>
  </body>
</html>
        """
    )


@app.get("/api/time")
def getTime() -> dict[str, str]:
    now = datetime.now(UTC)
    return {
        "current_time": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "current_time_utc": now.isoformat(),
    }


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
