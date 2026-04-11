from datetime import datetime

import requests


def parseRedmineDate(value: str | None) -> str | None:
    if not value:
        return None

    return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()


def normalizeProject(project: dict[str, object]) -> dict[str, object]:
    parent = project.get("parent") or {}

    return {
        "redmine_id": project["id"],
        "name": project["name"],
        "identifier": project.get("identifier"),
        "status": project.get("status"),
        "homepage": project.get("homepage"),
        "parent_redmine_id": parent.get("id"),
        "created_on": parseRedmineDate(project.get("created_on")),
        "updated_on": parseRedmineDate(project.get("updated_on")),
    }


def fetchAllProjectsFromRedmine(redmineUrl: str, apiKey: str) -> list[dict[str, object]]:
    projects: list[dict[str, object]] = []
    offset = 0
    limit = 100
    session = requests.Session()
    session.headers.update({"X-Redmine-API-Key": apiKey})

    while True:
        response = session.get(
            f"{redmineUrl.rstrip('/')}/projects.json",
            params={"offset": offset, "limit": limit},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rawProjects = payload.get("projects", [])

        projects.extend(normalizeProject(project) for project in rawProjects)

        offset += len(rawProjects)
        totalCount = payload.get("total_count", offset)
        if offset >= totalCount or not rawProjects:
            break

    projects.sort(key=lambda project: str(project["name"]).lower())
    return projects

