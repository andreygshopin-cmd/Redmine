from collections.abc import Sequence

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.orm import sessionmaker

from src.redmine.config import loadConfig


config = loadConfig()
SNAPSHOT_INSERT_BATCH_SIZE = 200


def normalizeDatabaseUrl(databaseUrl: str) -> str:
    if databaseUrl.startswith("postgresql://") and "+psycopg" not in databaseUrl:
        return databaseUrl.replace("postgresql://", "postgresql+psycopg://", 1)
    return databaseUrl


def chunkSequence(items: Sequence[dict[str, object]], chunkSize: int) -> list[list[dict[str, object]]]:
    if chunkSize <= 0:
        raise ValueError("chunkSize must be greater than 0")

    return [list(items[index : index + chunkSize]) for index in range(0, len(items), chunkSize)]


normalizedDatabaseUrl = normalizeDatabaseUrl(config.databaseUrl)
engine = create_engine(normalizedDatabaseUrl, pool_pre_ping=True) if normalizedDatabaseUrl else None
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None


def checkDatabaseConnection() -> bool:
    if engine is None:
        return False

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))

    return True


def ensureProjectsTable() -> None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id SERIAL PRIMARY KEY,
                    redmine_id INTEGER NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    identifier TEXT,
                    status INTEGER,
                    homepage TEXT,
                    parent_redmine_id INTEGER,
                    created_on TIMESTAMPTZ NULL,
                    updated_on TIMESTAMPTZ NULL,
                    is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    partial_load BOOLEAN NOT NULL DEFAULT FALSE,
                    is_disabled BOOLEAN NOT NULL DEFAULT FALSE,
                    synced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE projects
                ADD COLUMN IF NOT EXISTS is_enabled BOOLEAN NOT NULL DEFAULT FALSE
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE projects
                ADD COLUMN IF NOT EXISTS partial_load BOOLEAN NOT NULL DEFAULT FALSE
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE projects
                ADD COLUMN IF NOT EXISTS is_disabled BOOLEAN NOT NULL DEFAULT FALSE
                """
            )
        )

        connection.execute(
            text(
                """
                UPDATE projects
                SET is_enabled = NOT COALESCE(is_disabled, FALSE)
                WHERE is_enabled IS DISTINCT FROM NOT COALESCE(is_disabled, FALSE)
                """
            )
        )


def ensureIssueSnapshotTables() -> None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS issue_snapshot_runs (
                    id BIGSERIAL PRIMARY KEY,
                    project_redmine_id INTEGER NOT NULL,
                    project_name TEXT NOT NULL,
                    project_identifier TEXT NOT NULL,
                    captured_for_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    total_issues INTEGER NOT NULL DEFAULT 0,
                    total_baseline_estimate_hours DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_estimated_hours DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_spent_hours DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_spent_hours_year DOUBLE PRECISION NOT NULL DEFAULT 0
                )
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS issue_snapshot_items (
                    id BIGSERIAL PRIMARY KEY,
                    snapshot_run_id BIGINT NOT NULL REFERENCES issue_snapshot_runs(id) ON DELETE CASCADE,
                    project_redmine_id INTEGER NOT NULL,
                    issue_redmine_id INTEGER NOT NULL,
                    subject TEXT,
                    tracker_id INTEGER,
                    tracker_name TEXT,
                    status_id INTEGER,
                    status_name TEXT,
                    priority_id INTEGER,
                    priority_name TEXT,
                    author_id INTEGER,
                    author_name TEXT,
                    assigned_to_id INTEGER,
                    assigned_to_name TEXT,
                    parent_issue_redmine_id INTEGER,
                    fixed_version_id INTEGER,
                    fixed_version_name TEXT,
                    done_ratio INTEGER,
                    is_private BOOLEAN NOT NULL DEFAULT FALSE,
                    baseline_estimate_hours DOUBLE PRECISION,
                    estimated_hours DOUBLE PRECISION,
                    spent_hours DOUBLE PRECISION,
                    spent_hours_year DOUBLE PRECISION,
                    start_date DATE NULL,
                    due_date DATE NULL,
                    created_on TIMESTAMPTZ NULL,
                    updated_on TIMESTAMPTZ NULL,
                    closed_on TIMESTAMPTZ NULL
                )
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE issue_snapshot_runs
                ADD COLUMN IF NOT EXISTS captured_for_date DATE NOT NULL DEFAULT CURRENT_DATE
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE issue_snapshot_runs
                ADD COLUMN IF NOT EXISTS total_baseline_estimate_hours DOUBLE PRECISION NOT NULL DEFAULT 0
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE issue_snapshot_items
                ADD COLUMN IF NOT EXISTS baseline_estimate_hours DOUBLE PRECISION
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE issue_snapshot_runs
                ADD COLUMN IF NOT EXISTS total_spent_hours_year DOUBLE PRECISION NOT NULL DEFAULT 0
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE issue_snapshot_items
                ADD COLUMN IF NOT EXISTS spent_hours_year DOUBLE PRECISION
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE issue_snapshot_runs
                DROP CONSTRAINT IF EXISTS issue_snapshot_runs_snapshot_batch_id_fkey
                """
            )
        )

        connection.execute(
            text(
                """
                ALTER TABLE issue_snapshot_runs
                DROP COLUMN IF EXISTS snapshot_batch_id
                """
            )
        )

        connection.execute(
            text(
                """
                DROP TABLE IF EXISTS issue_snapshot_batches
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_issue_snapshot_runs_project_captured
                ON issue_snapshot_runs(project_redmine_id, captured_at DESC)
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_issue_snapshot_runs_project_date_unique
                ON issue_snapshot_runs(project_redmine_id, captured_for_date)
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_issue_snapshot_items_run
                ON issue_snapshot_items(snapshot_run_id)
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_issue_snapshot_items_issue
                ON issue_snapshot_items(project_redmine_id, issue_redmine_id)
                """
            )
        )


def listStoredProjects() -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                WITH latest_snapshot_runs AS (
                    SELECT DISTINCT ON (r.project_redmine_id)
                        r.id,
                        r.project_redmine_id,
                        r.captured_for_date
                    FROM issue_snapshot_runs r
                    ORDER BY r.project_redmine_id, r.captured_for_date DESC, r.captured_at DESC, r.id DESC
                ),
                latest_snapshot_metrics AS (
                    SELECT
                        lr.project_redmine_id,
                        lr.captured_for_date AS latest_snapshot_date,
                        COALESCE(SUM(COALESCE(i.baseline_estimate_hours, 0)), 0) AS baseline_estimate_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(COALESCE(i.tracker_name, '')) = LOWER('Разработка')
                                    THEN COALESCE(i.estimated_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS development_estimate_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(COALESCE(i.tracker_name, '')) = LOWER('Разработка')
                                    THEN COALESCE(i.spent_hours_year, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS development_spent_hours_year,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(COALESCE(i.tracker_name, '')) = LOWER('Процессы разработки')
                                    THEN COALESCE(i.estimated_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS development_process_estimate_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(COALESCE(i.tracker_name, '')) = LOWER('Процессы разработки')
                                    THEN COALESCE(i.spent_hours_year, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS development_process_spent_hours_year,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(COALESCE(i.tracker_name, '')) = LOWER('Ошибка')
                                    THEN COALESCE(i.estimated_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS bug_estimate_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(COALESCE(i.tracker_name, '')) = LOWER('Ошибка')
                                    THEN COALESCE(i.spent_hours_year, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS bug_spent_hours_year
                    FROM latest_snapshot_runs lr
                    LEFT JOIN issue_snapshot_items i
                        ON i.snapshot_run_id = lr.id
                    GROUP BY lr.project_redmine_id, lr.captured_for_date
                )
                SELECT
                    redmine_id,
                    name,
                    identifier,
                    status,
                    homepage,
                    parent_redmine_id,
                    created_on,
                    updated_on,
                    is_enabled,
                    partial_load,
                    is_disabled,
                    synced_at,
                    m.latest_snapshot_date,
                    COALESCE(m.baseline_estimate_hours, 0) AS baseline_estimate_hours,
                    COALESCE(m.development_estimate_hours, 0) AS development_estimate_hours,
                    COALESCE(m.development_spent_hours_year, 0) AS development_spent_hours_year,
                    COALESCE(m.development_process_estimate_hours, 0) AS development_process_estimate_hours,
                    COALESCE(m.development_process_spent_hours_year, 0) AS development_process_spent_hours_year,
                    COALESCE(m.bug_estimate_hours, 0) AS bug_estimate_hours,
                    COALESCE(m.bug_spent_hours_year, 0) AS bug_spent_hours_year
                FROM projects
                LEFT JOIN latest_snapshot_metrics m
                    ON m.project_redmine_id = projects.redmine_id
                ORDER BY LOWER(name), redmine_id
                """
            )
        )

        return [dict(row._mapping) for row in rows]


def listRecentIssueSnapshotRuns(limit: int | None = 20) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        if limit is None:
            rows = connection.execute(
                text(
                    """
                    SELECT
                        r.id,
                        r.project_redmine_id,
                        COALESCE(p.name, r.project_name) AS project_name,
                        COALESCE(p.identifier, r.project_identifier) AS project_identifier,
                        r.captured_for_date,
                        r.captured_at,
                        r.total_issues,
                        r.total_baseline_estimate_hours,
                        r.total_estimated_hours,
                        r.total_spent_hours,
                        r.total_spent_hours_year
                    FROM issue_snapshot_runs r
                    LEFT JOIN projects p
                        ON p.redmine_id = r.project_redmine_id
                    ORDER BY LOWER(COALESCE(p.name, r.project_name)), r.captured_for_date DESC, r.captured_at DESC, r.id DESC
                    """
                )
            )
        else:
            rows = connection.execute(
                text(
                    """
                    SELECT
                        r.id,
                        r.project_redmine_id,
                        COALESCE(p.name, r.project_name) AS project_name,
                        COALESCE(p.identifier, r.project_identifier) AS project_identifier,
                        r.captured_for_date,
                        r.captured_at,
                        r.total_issues,
                        r.total_baseline_estimate_hours,
                        r.total_estimated_hours,
                        r.total_spent_hours,
                        r.total_spent_hours_year
                    FROM issue_snapshot_runs r
                    LEFT JOIN projects p
                        ON p.redmine_id = r.project_redmine_id
                    ORDER BY r.captured_at DESC, r.id DESC
                    LIMIT :limit_value
                    """
                ),
                {"limit_value": limit},
            )

        return [dict(row._mapping) for row in rows]


def countIssueSnapshotRuns() -> int:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        return int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM issue_snapshot_runs
                    """
                )
            ).scalar_one()
        )


def getLatestSnapshotIssuesForProject(projectRedmineId: int) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        latestRun = connection.execute(
            text(
                """
                SELECT
                    r.id,
                    r.project_redmine_id,
                    COALESCE(p.name, r.project_name) AS project_name,
                    COALESCE(p.identifier, r.project_identifier) AS project_identifier,
                    r.captured_for_date,
                    r.captured_at,
                    r.total_issues,
                    r.total_baseline_estimate_hours,
                    r.total_estimated_hours,
                    r.total_spent_hours,
                    r.total_spent_hours_year
                FROM issue_snapshot_runs r
                LEFT JOIN projects p
                    ON p.redmine_id = r.project_redmine_id
                WHERE r.project_redmine_id = :project_redmine_id
                ORDER BY r.captured_for_date DESC, r.captured_at DESC, r.id DESC
                LIMIT 1
                """
            ),
            {"project_redmine_id": projectRedmineId},
        ).mappings().first()

        if latestRun is None:
            return {"snapshot_run": None, "issues": []}

        issueRows = connection.execute(
            text(
                """
                SELECT
                    issue_redmine_id,
                    subject,
                    tracker_name,
                    status_name,
                    priority_name,
                    assigned_to_name,
                    fixed_version_name,
                    done_ratio,
                    baseline_estimate_hours,
                    estimated_hours,
                    spent_hours,
                    spent_hours_year,
                    start_date,
                    due_date,
                    created_on,
                    updated_on,
                    closed_on
                FROM issue_snapshot_items
                WHERE snapshot_run_id = :snapshot_run_id
                ORDER BY issue_redmine_id
                """
            ),
            {"snapshot_run_id": latestRun["id"]},
        )

        return {
            "snapshot_run": dict(latestRun),
            "issues": [dict(row._mapping) for row in issueRows],
        }


def listSnapshotDatesForProject(projectRedmineId: int) -> list[str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT DISTINCT captured_for_date
                FROM issue_snapshot_runs
                WHERE project_redmine_id = :project_redmine_id
                ORDER BY captured_for_date DESC
                """
            ),
            {"project_redmine_id": projectRedmineId},
        )

        return [str(row.captured_for_date) for row in rows]


def getSnapshotIssuesForProjectByDate(projectRedmineId: int, capturedForDate: str | None) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        params: dict[str, object] = {"project_redmine_id": projectRedmineId}
        if capturedForDate:
            params["captured_for_date"] = capturedForDate
            latestRun = connection.execute(
                text(
                    """
                    SELECT
                        r.id,
                        r.project_redmine_id,
                        COALESCE(p.name, r.project_name) AS project_name,
                        COALESCE(p.identifier, r.project_identifier) AS project_identifier,
                        r.captured_for_date,
                        r.captured_at,
                        r.total_issues,
                        r.total_baseline_estimate_hours,
                        r.total_estimated_hours,
                        r.total_spent_hours,
                        r.total_spent_hours_year
                    FROM issue_snapshot_runs r
                    LEFT JOIN projects p
                        ON p.redmine_id = r.project_redmine_id
                    WHERE r.project_redmine_id = :project_redmine_id
                      AND r.captured_for_date = :captured_for_date
                    ORDER BY r.captured_at DESC, r.id DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()
        else:
            latestRun = connection.execute(
                text(
                    """
                    SELECT
                        r.id,
                        r.project_redmine_id,
                        COALESCE(p.name, r.project_name) AS project_name,
                        COALESCE(p.identifier, r.project_identifier) AS project_identifier,
                        r.captured_for_date,
                        r.captured_at,
                        r.total_issues,
                        r.total_baseline_estimate_hours,
                        r.total_estimated_hours,
                        r.total_spent_hours,
                        r.total_spent_hours_year
                    FROM issue_snapshot_runs r
                    LEFT JOIN projects p
                        ON p.redmine_id = r.project_redmine_id
                    WHERE r.project_redmine_id = :project_redmine_id
                    ORDER BY r.captured_for_date DESC, r.captured_at DESC, r.id DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()

        availableDates = listSnapshotDatesForProject(projectRedmineId)
        if latestRun is None:
            return {"snapshot_run": None, "issues": [], "available_dates": availableDates}

        issueRows = connection.execute(
            text(
                """
                SELECT
                    issue_redmine_id,
                    subject,
                    tracker_name,
                    status_name,
                    priority_name,
                    assigned_to_name,
                    fixed_version_name,
                    done_ratio,
                    baseline_estimate_hours,
                    estimated_hours,
                    spent_hours,
                    spent_hours_year,
                    start_date,
                    due_date,
                    created_on,
                    updated_on,
                    closed_on
                FROM issue_snapshot_items
                WHERE snapshot_run_id = :snapshot_run_id
                ORDER BY issue_redmine_id
                """
            ),
            {"snapshot_run_id": latestRun["id"]},
        )

        return {
            "snapshot_run": dict(latestRun),
            "issues": [dict(row._mapping) for row in issueRows],
            "available_dates": availableDates,
        }


def getSnapshotRunsWithIssuesForProjectYear(projectRedmineId: int, year: int) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    yearStart = f"{year}-01-01"
    yearEnd = f"{year}-12-31"

    with engine.connect() as connection:
        runRows = connection.execute(
            text(
                """
                SELECT
                    r.id,
                    r.project_redmine_id,
                    COALESCE(p.name, r.project_name) AS project_name,
                    COALESCE(p.identifier, r.project_identifier) AS project_identifier,
                    r.captured_for_date,
                    r.captured_at,
                    r.total_issues,
                    r.total_baseline_estimate_hours,
                    r.total_estimated_hours,
                    r.total_spent_hours,
                    r.total_spent_hours_year
                FROM issue_snapshot_runs r
                LEFT JOIN projects p
                    ON p.redmine_id = r.project_redmine_id
                WHERE r.project_redmine_id = :project_redmine_id
                  AND r.captured_for_date BETWEEN :year_start AND :year_end
                ORDER BY r.captured_for_date ASC, r.captured_at ASC, r.id ASC
                """
            ),
            {
                "project_redmine_id": projectRedmineId,
                "year_start": yearStart,
                "year_end": yearEnd,
            },
        ).mappings().all()

        if not runRows:
            return {"project": None, "snapshot_runs": []}

        runIds = [int(row["id"]) for row in runRows]
        issueRows = connection.execute(
            text(
                """
                SELECT
                    snapshot_run_id,
                    issue_redmine_id,
                    subject,
                    tracker_name,
                    status_name,
                    parent_issue_redmine_id,
                    baseline_estimate_hours,
                    estimated_hours,
                    spent_hours,
                    spent_hours_year,
                    closed_on
                FROM issue_snapshot_items
                WHERE snapshot_run_id IN :run_ids
                ORDER BY snapshot_run_id ASC, issue_redmine_id ASC
                """
            ).bindparams(bindparam("run_ids", expanding=True)),
            {"run_ids": runIds},
        ).mappings().all()

    issuesByRunId: dict[int, list[dict[str, object]]] = {}
    for row in issueRows:
        runId = int(row["snapshot_run_id"])
        issuesByRunId.setdefault(runId, []).append(dict(row))

    firstRun = dict(runRows[0])
    return {
        "project": {
            "project_redmine_id": int(firstRun["project_redmine_id"]),
            "project_name": str(firstRun["project_name"] or "—"),
            "project_identifier": str(firstRun["project_identifier"] or "—"),
        },
        "snapshot_runs": [
            {
                **dict(row),
                "issues": issuesByRunId.get(int(row["id"]), []),
            }
            for row in runRows
        ],
    }


def listProjectsWithoutSnapshotForDate(capturedForDate: str) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    p.redmine_id,
                    p.name,
                    p.identifier,
                    p.status,
                    p.homepage,
                    p.parent_redmine_id,
                    p.created_on,
                    p.updated_on,
                    p.synced_at
                FROM projects p
                LEFT JOIN issue_snapshot_runs r
                    ON r.project_redmine_id = p.redmine_id
                   AND r.captured_for_date = :captured_for_date
                WHERE r.id IS NULL
                  AND COALESCE(p.is_enabled, FALSE) = TRUE
                ORDER BY LOWER(p.name), p.redmine_id
                """
            ),
            {"captured_for_date": capturedForDate},
        )

        return [dict(row._mapping) for row in rows]


def deleteIssueSnapshotsForDate(capturedForDate: str) -> dict[str, int | str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        runCount = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM issue_snapshot_runs
                WHERE captured_for_date = :captured_for_date
                """
            ),
            {"captured_for_date": capturedForDate},
        ).scalar_one()

        itemCount = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM issue_snapshot_items
                WHERE snapshot_run_id IN (
                    SELECT id
                    FROM issue_snapshot_runs
                    WHERE captured_for_date = :captured_for_date
                )
                """
            ),
            {"captured_for_date": capturedForDate},
        ).scalar_one()

        connection.execute(
            text(
                """
                DELETE FROM issue_snapshot_runs
                WHERE captured_for_date = :captured_for_date
                """
            ),
            {"captured_for_date": capturedForDate},
        )

    return {
        "captured_for_date": capturedForDate,
        "deleted_items": int(itemCount),
        "deleted_runs": int(runCount),
    }


def deleteIssueSnapshotForProjectDate(projectRedmineId: int, capturedForDate: str) -> dict[str, int | str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        runIds = [
            int(row.id)
            for row in connection.execute(
                text(
                    """
                    SELECT id
                    FROM issue_snapshot_runs
                    WHERE project_redmine_id = :project_redmine_id
                      AND captured_for_date = :captured_for_date
                    """
                ),
                {"project_redmine_id": projectRedmineId, "captured_for_date": capturedForDate},
            )
        ]

        if not runIds:
            return {
                "project_redmine_id": projectRedmineId,
                "captured_for_date": capturedForDate,
                "deleted_items": 0,
                "deleted_runs": 0,
            }

        itemCount = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM issue_snapshot_items
                    WHERE snapshot_run_id IN :run_ids
                    """
                ).bindparams(bindparam("run_ids", expanding=True)),
                {"run_ids": runIds},
            ).scalar_one()
        )

        connection.execute(
            text(
                """
                DELETE FROM issue_snapshot_runs
                WHERE id IN :run_ids
                """
            ).bindparams(bindparam("run_ids", expanding=True)),
            {"run_ids": runIds},
        )

    return {
        "project_redmine_id": projectRedmineId,
        "captured_for_date": capturedForDate,
        "deleted_items": itemCount,
        "deleted_runs": len(runIds),
    }


def pruneUnchangedIssueSnapshots() -> dict[str, int]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    id,
                    project_redmine_id,
                    captured_for_date,
                    total_issues,
                    total_baseline_estimate_hours,
                    total_estimated_hours,
                    total_spent_hours,
                    total_spent_hours_year
                FROM issue_snapshot_runs
                ORDER BY project_redmine_id, captured_for_date ASC, captured_at ASC, id ASC
                """
            )
        ).mappings().all()

        idsToDelete: list[int] = []
        currentProjectId: int | None = None
        currentGroup: list[dict[str, object]] = []
        currentMetrics: tuple[float, float, float, float, float] | None = None

        def flushGroup() -> None:
            if len(currentGroup) > 2:
                idsToDelete.extend(int(row["id"]) for row in currentGroup[1:-1])

        for row in rows:
            metrics = (
                float(row["total_issues"] or 0),
                float(row["total_baseline_estimate_hours"] or 0),
                float(row["total_estimated_hours"] or 0),
                float(row["total_spent_hours"] or 0),
                float(row["total_spent_hours_year"] or 0),
            )
            projectId = int(row["project_redmine_id"])

            if currentProjectId != projectId:
                flushGroup()
                currentProjectId = projectId
                currentGroup = [dict(row)]
                currentMetrics = metrics
                continue

            if metrics == currentMetrics:
                currentGroup.append(dict(row))
                continue

            flushGroup()
            currentGroup = [dict(row)]
            currentMetrics = metrics

        flushGroup()

        deletedRuns = len(idsToDelete)
        deletedItems = 0
        if idsToDelete:
            deletedItems = int(
                connection.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM issue_snapshot_items
                        WHERE snapshot_run_id IN :run_ids
                        """
                    ).bindparams(bindparam("run_ids", expanding=True)),
                    {"run_ids": idsToDelete},
                ).scalar_one()
            )
            connection.execute(
                text(
                    """
                    DELETE FROM issue_snapshot_runs
                    WHERE id IN :run_ids
                    """
                ).bindparams(bindparam("run_ids", expanding=True)),
                {"run_ids": idsToDelete},
            )

    return {
        "deleted_runs": deletedRuns,
        "deleted_items": deletedItems,
    }


def syncProjects(projects: Sequence[dict[str, object]]) -> dict[str, int]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")
    if not projects:
        return {"added_count": 0, "updated_count": 0}

    ids = [project["redmine_id"] for project in projects]
    addedCount = 0
    updatedCount = 0

    with engine.begin() as connection:
        existingRows = connection.execute(
            text(
                """
                SELECT
                    redmine_id,
                    name,
                    identifier,
                    status,
                    homepage,
                    parent_redmine_id,
                    created_on,
                    updated_on
                FROM projects
                WHERE redmine_id IN :ids
                """
            ).bindparams(
                bindparam("ids", expanding=True)
            ),
            {"ids": ids},
        )
        existingById = {int(row.redmine_id): dict(row._mapping) for row in existingRows}

        insertStatement = text(
            """
            INSERT INTO projects (
                redmine_id,
                name,
                identifier,
                status,
                homepage,
                parent_redmine_id,
                created_on,
                updated_on,
                is_enabled,
                partial_load,
                is_disabled
            ) VALUES (
                :redmine_id,
                :name,
                :identifier,
                :status,
                :homepage,
                :parent_redmine_id,
                :created_on,
                :updated_on,
                FALSE,
                FALSE,
                TRUE
            )
            """
        )

        updateStatement = text(
            """
            UPDATE projects
            SET
                name = :name,
                identifier = :identifier,
                status = :status,
                homepage = :homepage,
                parent_redmine_id = :parent_redmine_id,
                created_on = :created_on,
                updated_on = :updated_on,
                synced_at = CURRENT_TIMESTAMP
            WHERE redmine_id = :redmine_id
            """
        )

        for project in projects:
            existing = existingById.get(int(project["redmine_id"]))
            if existing is not None:
                if (
                    existing.get("name") != project.get("name")
                    or existing.get("identifier") != project.get("identifier")
                    or existing.get("status") != project.get("status")
                    or existing.get("homepage") != project.get("homepage")
                    or existing.get("parent_redmine_id") != project.get("parent_redmine_id")
                    or existing.get("created_on") != project.get("created_on")
                    or existing.get("updated_on") != project.get("updated_on")
                ):
                    updatedCount += 1
                connection.execute(updateStatement, project)
                continue

            connection.execute(insertStatement, project)
            addedCount += 1

    return {"added_count": addedCount, "updated_count": updatedCount}


def storeMissingProjects(projects: Sequence[dict[str, object]]) -> int:
    return syncProjects(projects)["added_count"]


def updateProjectLoadSettings(enabledProjectIds: Sequence[int], partialProjectIds: Sequence[int]) -> dict[str, int]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    enabledIds = sorted({int(projectId) for projectId in enabledProjectIds})
    partialIds = sorted({int(projectId) for projectId in partialProjectIds if int(projectId) in set(enabledIds)})

    with engine.begin() as connection:
        if enabledIds or partialIds:
            connection.execute(
                text(
                    """
                    UPDATE projects
                    SET
                        is_enabled = CASE
                            WHEN redmine_id IN :enabled_ids THEN TRUE
                            ELSE FALSE
                        END,
                        partial_load = CASE
                            WHEN redmine_id IN :partial_ids THEN TRUE
                            ELSE FALSE
                        END,
                        is_disabled = CASE
                            WHEN redmine_id IN :enabled_ids THEN FALSE
                            ELSE TRUE
                        END
                    """
                ).bindparams(
                    bindparam("enabled_ids", expanding=True),
                    bindparam("partial_ids", expanding=True),
                ),
                {
                    "enabled_ids": enabledIds or [-1],
                    "partial_ids": partialIds or [-1],
                },
            )
        else:
            connection.execute(
                text(
                    """
                    UPDATE projects
                    SET
                        is_enabled = FALSE,
                        partial_load = FALSE,
                        is_disabled = FALSE
                    """
                )
            )

    return {
        "enabled_count": len(enabledIds),
        "partial_count": len(partialIds),
    }


def createIssueSnapshotRun(
    capturedForDate: str,
    project: dict[str, object],
    issues: Sequence[dict[str, object]],
) -> int | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    totalBaselineEstimateHours = sum(float(issue.get("baseline_estimate_hours") or 0) for issue in issues)
    totalEstimatedHours = sum(float(issue.get("estimated_hours") or 0) for issue in issues)
    totalSpentHours = sum(float(issue.get("spent_hours") or 0) for issue in issues)
    totalSpentHoursYear = sum(float(issue.get("spent_hours_year") or 0) for issue in issues)

    with engine.begin() as connection:
        runRow = connection.execute(
            text(
                """
                INSERT INTO issue_snapshot_runs (
                    project_redmine_id,
                    project_name,
                    project_identifier,
                    captured_for_date,
                    total_issues,
                    total_baseline_estimate_hours,
                    total_estimated_hours,
                    total_spent_hours,
                    total_spent_hours_year
                ) VALUES (
                    :project_redmine_id,
                    :project_name,
                    :project_identifier,
                    :captured_for_date,
                    :total_issues,
                    :total_baseline_estimate_hours,
                    :total_estimated_hours,
                    :total_spent_hours,
                    :total_spent_hours_year
                )
                ON CONFLICT (project_redmine_id, captured_for_date) DO NOTHING
                RETURNING id
                """
            ),
            {
                "project_redmine_id": project["redmine_id"],
                "project_name": project["name"],
                "project_identifier": project["identifier"],
                "captured_for_date": capturedForDate,
                "total_issues": len(issues),
                "total_baseline_estimate_hours": totalBaselineEstimateHours,
                "total_estimated_hours": totalEstimatedHours,
                "total_spent_hours": totalSpentHours,
                "total_spent_hours_year": totalSpentHoursYear,
            },
        ).first()

        if runRow is None:
            return None

        snapshotRunId = int(runRow.id)

        if issues:
            insertStatement = text(
                """
                INSERT INTO issue_snapshot_items (
                    snapshot_run_id,
                    project_redmine_id,
                    issue_redmine_id,
                    subject,
                    tracker_id,
                    tracker_name,
                    status_id,
                    status_name,
                    priority_id,
                    priority_name,
                    author_id,
                    author_name,
                    assigned_to_id,
                    assigned_to_name,
                    parent_issue_redmine_id,
                    fixed_version_id,
                    fixed_version_name,
                    done_ratio,
                    is_private,
                    baseline_estimate_hours,
                    estimated_hours,
                    spent_hours,
                    spent_hours_year,
                    start_date,
                    due_date,
                    created_on,
                    updated_on,
                    closed_on
                ) VALUES (
                    :snapshot_run_id,
                    :project_redmine_id,
                    :issue_redmine_id,
                    :subject,
                    :tracker_id,
                    :tracker_name,
                    :status_id,
                    :status_name,
                    :priority_id,
                    :priority_name,
                    :author_id,
                    :author_name,
                    :assigned_to_id,
                    :assigned_to_name,
                    :parent_issue_redmine_id,
                    :fixed_version_id,
                    :fixed_version_name,
                    :done_ratio,
                    :is_private,
                    :baseline_estimate_hours,
                    :estimated_hours,
                    :spent_hours,
                    :spent_hours_year,
                    :start_date,
                    :due_date,
                    :created_on,
                    :updated_on,
                    :closed_on
                )
                """
            )

            payload = []
            for issue in issues:
                item = dict(issue)
                item["snapshot_run_id"] = snapshotRunId
                payload.append(item)

            for payloadChunk in chunkSequence(payload, SNAPSHOT_INSERT_BATCH_SIZE):
                connection.execute(insertStatement, payloadChunk)

    return snapshotRunId
