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


def normalizeIssue(issue: dict[str, object], projectRedmineId: int) -> dict[str, object]:
    tracker = issue.get("tracker") or {}
    status = issue.get("status") or {}
    priority = issue.get("priority") or {}
    author = issue.get("author") or {}
    assignedTo = issue.get("assigned_to") or {}
    fixedVersion = issue.get("fixed_version") or {}
    parent = issue.get("parent") or {}

    return {
        "project_redmine_id": projectRedmineId,
        "issue_redmine_id": issue["id"],
        "subject": issue.get("subject"),
        "tracker_id": tracker.get("id"),
        "tracker_name": tracker.get("name"),
        "status_id": status.get("id"),
        "status_name": status.get("name"),
        "priority_id": priority.get("id"),
        "priority_name": priority.get("name"),
        "author_id": author.get("id"),
        "author_name": author.get("name"),
        "assigned_to_id": assignedTo.get("id"),
        "assigned_to_name": assignedTo.get("name"),
        "parent_issue_redmine_id": parent.get("id"),
        "fixed_version_id": fixedVersion.get("id"),
        "fixed_version_name": fixedVersion.get("name"),
        "done_ratio": issue.get("done_ratio"),
        "is_private": bool(issue.get("is_private", False)),
        "estimated_hours": issue.get("estimated_hours"),
        "spent_hours": issue.get("spent_hours"),
        "spent_hours_year": 0.0,
        "start_date": issue.get("start_date"),
        "due_date": issue.get("due_date"),
        "created_on": parseRedmineDate(issue.get("created_on")),
        "updated_on": parseRedmineDate(issue.get("updated_on")),
        "closed_on": parseRedmineDate(issue.get("closed_on")),
    }


def applySpentHoursYearByIssue(
    issues: list[dict[str, object]],
    spentHoursByIssue: dict[int, float],
) -> list[dict[str, object]]:
    for issue in issues:
        issue["spent_hours_year"] = spentHoursByIssue.get(int(issue["issue_redmine_id"]), 0.0)

    return issues


def buildSession(apiKey: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"X-Redmine-API-Key": apiKey})
    return session


def fetchAllProjectsFromRedmine(redmineUrl: str, apiKey: str) -> list[dict[str, object]]:
    projects: list[dict[str, object]] = []
    offset = 0
    limit = 100
    session = buildSession(apiKey)

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


def fetchAllIssuesForProject(
    redmineUrl: str,
    apiKey: str,
    projectIdentifier: str,
    projectRedmineId: int,
) -> list[dict[str, object]]:
    issues: list[dict[str, object]] = []
    offset = 0
    limit = 100
    session = buildSession(apiKey)

    while True:
        response = session.get(
            f"{redmineUrl.rstrip('/')}/issues.json",
            params={
                "project_id": projectIdentifier,
                "status_id": "*",
                "offset": offset,
                "limit": limit,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rawIssues = payload.get("issues", [])

        issues.extend(normalizeIssue(issue, projectRedmineId) for issue in rawIssues)

        offset += len(rawIssues)
        totalCount = payload.get("total_count", offset)
        if offset >= totalCount or not rawIssues:
            break

    issues.sort(key=lambda issue: int(issue["issue_redmine_id"]))
    return issues


def fetchSpentHoursByIssueForProjectYear(
    redmineUrl: str,
    apiKey: str,
    projectIdentifier: str,
    year: int,
) -> dict[int, float]:
    spentHoursByIssue: dict[int, float] = {}
    offset = 0
    limit = 100
    session = buildSession(apiKey)
    fromDate = f"{year}-01-01"
    toDate = f"{year}-12-31"

    while True:
        response = session.get(
            f"{redmineUrl.rstrip('/')}/time_entries.json",
            params={
                "project_id": projectIdentifier,
                "from": fromDate,
                "to": toDate,
                "offset": offset,
                "limit": limit,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rawEntries = payload.get("time_entries", [])

        for entry in rawEntries:
            issue = entry.get("issue") or {}
            issueId = issue.get("id")
            if issueId is None:
                continue

            spentHoursByIssue[int(issueId)] = spentHoursByIssue.get(int(issueId), 0.0) + float(
                entry.get("hours") or 0.0
            )

        offset += len(rawEntries)
        totalCount = payload.get("total_count", offset)
        if offset >= totalCount or not rawEntries:
            break

    return spentHoursByIssue
