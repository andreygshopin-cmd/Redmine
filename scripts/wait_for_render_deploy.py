import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _read_env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _fetch_health(health_url: str) -> dict[str, object]:
    request = Request(health_url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=20) as response:
        payload = response.read().decode("utf-8", errors="replace")
    return json.loads(payload)


def main() -> int:
    health_url = _read_env("HEALTH_URL", "https://redmine-tdfp.onrender.com/health")
    expected_commit = _read_env("EXPECTED_COMMIT_SHA").lower()
    timeout_seconds = int(_read_env("TIMEOUT_SECONDS", "1200"))
    poll_interval_seconds = int(_read_env("POLL_INTERVAL_SECONDS", "10"))

    if not expected_commit:
        print("EXPECTED_COMMIT_SHA is required", file=sys.stderr)
        return 2

    deadline = time.time() + timeout_seconds
    last_message = "health endpoint has not responded yet"
    while time.time() < deadline:
        try:
            payload = _fetch_health(health_url)
            render_commit = str(payload.get("render_git_commit") or "").strip().lower()
            status = str(payload.get("status") or "").strip().lower()
            if status == "ok" and render_commit == expected_commit:
                print(
                    json.dumps(
                        {
                            "ok": True,
                            "health_url": health_url,
                            "expected_commit": expected_commit,
                            "render_git_commit": render_commit,
                            "payload": payload,
                        },
                        ensure_ascii=False,
                    )
                )
                return 0
            last_message = json.dumps(payload, ensure_ascii=False)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_message = str(exc)
        time.sleep(max(1, poll_interval_seconds))

    print(
        json.dumps(
            {
                "ok": False,
                "health_url": health_url,
                "expected_commit": expected_commit,
                "last_observation": last_message,
            },
            ensure_ascii=False,
        ),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
