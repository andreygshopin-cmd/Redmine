from collections.abc import Sequence

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.orm import sessionmaker

from src.redmine.config import loadConfig


config = loadConfig()


def normalizeDatabaseUrl(databaseUrl: str) -> str:
    if databaseUrl.startswith("postgresql://") and "+psycopg" not in databaseUrl:
        return databaseUrl.replace("postgresql://", "postgresql+psycopg://", 1)
    return databaseUrl


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
                    synced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
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
                CREATE TABLE IF NOT EXISTS issue_snapshot_batches (
                    id BIGSERIAL PRIMARY KEY,
                    captured_for_date DATE NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMPTZ NULL,
                    total_projects INTEGER NOT NULL DEFAULT 0,
                    completed_projects INTEGER NOT NULL DEFAULT 0,
                    skipped_projects INTEGER NOT NULL DEFAULT 0,
                    total_issues INTEGER NOT NULL DEFAULT 0,
                    total_estimated_hours DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_spent_hours DOUBLE PRECISION NOT NULL DEFAULT 0
                )
                """
            )
        )

        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS issue_snapshot_runs (
                    id BIGSERIAL PRIMARY KEY,
                    snapshot_batch_id BIGINT NULL REFERENCES issue_snapshot_batches(id) ON DELETE SET NULL,
                    project_redmine_id INTEGER NOT NULL,
                    project_name TEXT NOT NULL,
                    project_identifier TEXT NOT NULL,
                    captured_for_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    total_issues INTEGER NOT NULL DEFAULT 0,
                    total_estimated_hours DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_spent_hours DOUBLE PRECISION NOT NULL DEFAULT 0
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
                    estimated_hours DOUBLE PRECISION,
                    spent_hours DOUBLE PRECISION,
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
                ADD COLUMN IF NOT EXISTS snapshot_batch_id BIGINT NULL
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
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'issue_snapshot_runs_snapshot_batch_id_fkey'
                    ) THEN
                        ALTER TABLE issue_snapshot_runs
                        ADD CONSTRAINT issue_snapshot_runs_snapshot_batch_id_fkey
                        FOREIGN KEY (snapshot_batch_id)
                        REFERENCES issue_snapshot_batches(id)
                        ON DELETE SET NULL;
                    END IF;
                END $$;
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
                CREATE INDEX IF NOT EXISTS idx_issue_snapshot_runs_batch
                ON issue_snapshot_runs(snapshot_batch_id)
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
                SELECT
                    redmine_id,
                    name,
                    identifier,
                    status,
                    homepage,
                    parent_redmine_id,
                    created_on,
                    updated_on,
                    synced_at
                FROM projects
                ORDER BY LOWER(name), redmine_id
                """
            )
        )

        return [dict(row._mapping) for row in rows]


def listRecentIssueSnapshotRuns(limit: int = 20) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    id,
                    snapshot_batch_id,
                    project_redmine_id,
                    project_name,
                    project_identifier,
                    captured_for_date,
                    captured_at,
                    total_issues,
                    total_estimated_hours,
                    total_spent_hours
                FROM issue_snapshot_runs
                ORDER BY captured_at DESC, id DESC
                LIMIT :limit_value
                """
            ),
            {"limit_value": limit},
        )

        return [dict(row._mapping) for row in rows]


def listRecentIssueSnapshotBatches(limit: int = 20) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    id,
                    captured_for_date,
                    started_at,
                    completed_at,
                    total_projects,
                    completed_projects,
                    skipped_projects,
                    total_issues,
                    total_estimated_hours,
                    total_spent_hours
                FROM issue_snapshot_batches
                ORDER BY started_at DESC, id DESC
                LIMIT :limit_value
                """
            ),
            {"limit_value": limit},
        )

        return [dict(row._mapping) for row in rows]


def storeMissingProjects(projects: Sequence[dict[str, object]]) -> int:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")
    if not projects:
        return 0

    ids = [project["redmine_id"] for project in projects]
    addedCount = 0

    with engine.begin() as connection:
        existingRows = connection.execute(
            text("SELECT redmine_id FROM projects WHERE redmine_id IN :ids").bindparams(
                bindparam("ids", expanding=True)
            ),
            {"ids": ids},
        )
        existingIds = {row.redmine_id for row in existingRows}

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
                updated_on
            ) VALUES (
                :redmine_id,
                :name,
                :identifier,
                :status,
                :homepage,
                :parent_redmine_id,
                :created_on,
                :updated_on
            )
            """
        )

        for project in projects:
            if project["redmine_id"] in existingIds:
                continue

            connection.execute(insertStatement, project)
            addedCount += 1

    return addedCount


def createIssueSnapshotBatch(
    capturedForDate: str,
    totalProjects: int,
) -> int:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        batchRow = connection.execute(
            text(
                """
                INSERT INTO issue_snapshot_batches (
                    captured_for_date,
                    total_projects
                ) VALUES (
                    :captured_for_date,
                    :total_projects
                )
                RETURNING id
                """
            ),
            {
                "captured_for_date": capturedForDate,
                "total_projects": totalProjects,
            },
        ).first()

    return int(batchRow.id)


def finalizeIssueSnapshotBatch(
    batchId: int,
    completedProjects: int,
    skippedProjects: int,
    totalIssues: int,
    totalEstimatedHours: float,
    totalSpentHours: float,
) -> None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE issue_snapshot_batches
                SET
                    completed_at = CURRENT_TIMESTAMP,
                    completed_projects = :completed_projects,
                    skipped_projects = :skipped_projects,
                    total_issues = :total_issues,
                    total_estimated_hours = :total_estimated_hours,
                    total_spent_hours = :total_spent_hours
                WHERE id = :batch_id
                """
            ),
            {
                "batch_id": batchId,
                "completed_projects": completedProjects,
                "skipped_projects": skippedProjects,
                "total_issues": totalIssues,
                "total_estimated_hours": totalEstimatedHours,
                "total_spent_hours": totalSpentHours,
            },
        )


def createIssueSnapshotRun(
    batchId: int | None,
    capturedForDate: str,
    project: dict[str, object],
    issues: Sequence[dict[str, object]],
) -> int:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    totalEstimatedHours = sum(float(issue.get("estimated_hours") or 0) for issue in issues)
    totalSpentHours = sum(float(issue.get("spent_hours") or 0) for issue in issues)

    with engine.begin() as connection:
        runRow = connection.execute(
            text(
                """
                INSERT INTO issue_snapshot_runs (
                    snapshot_batch_id,
                    project_redmine_id,
                    project_name,
                    project_identifier,
                    captured_for_date,
                    total_issues,
                    total_estimated_hours,
                    total_spent_hours
                ) VALUES (
                    :snapshot_batch_id,
                    :project_redmine_id,
                    :project_name,
                    :project_identifier,
                    :captured_for_date,
                    :total_issues,
                    :total_estimated_hours,
                    :total_spent_hours
                )
                RETURNING id
                """
            ),
            {
                "snapshot_batch_id": batchId,
                "project_redmine_id": project["redmine_id"],
                "project_name": project["name"],
                "project_identifier": project["identifier"],
                "captured_for_date": capturedForDate,
                "total_issues": len(issues),
                "total_estimated_hours": totalEstimatedHours,
                "total_spent_hours": totalSpentHours,
            },
        ).first()

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
                    estimated_hours,
                    spent_hours,
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
                    :estimated_hours,
                    :spent_hours,
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

            connection.execute(insertStatement, payload)

    return snapshotRunId
