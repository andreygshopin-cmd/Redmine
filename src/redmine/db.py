from collections.abc import Mapping, Sequence
import json
from threading import Lock

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection
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
_projectsTableEnsureLock = Lock()
_issueSnapshotTablesEnsureLock = Lock()
_planningProjectsTableEnsureLock = Lock()
_usersTableEnsureLock = Lock()
_projectsTableEnsured = False
_issueSnapshotTablesEnsured = False
_planningProjectsTableEnsured = False
_usersTableEnsured = False


def checkDatabaseConnection() -> bool:
    if engine is None:
        return False

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))

    return True


def ensureProjectsTable() -> None:
    global _projectsTableEnsured

    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    if _projectsTableEnsured:
        return

    with _projectsTableEnsureLock:
        if _projectsTableEnsured:
            return

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

        _projectsTableEnsured = True


def ensureIssueSnapshotTables() -> None:
    global _issueSnapshotTablesEnsured

    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    if _issueSnapshotTablesEnsured:
        return

    with _issueSnapshotTablesEnsureLock:
        if _issueSnapshotTablesEnsured:
            return

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
                        risk_estimate_hours DOUBLE PRECISION,
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
                    CREATE TABLE IF NOT EXISTS issue_snapshot_capture_status (
                        status_key TEXT PRIMARY KEY,
                        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS issue_snapshot_capture_errors (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        mode TEXT NULL,
                        project_redmine_id INTEGER NULL,
                        project_name TEXT NULL,
                        captured_for_date DATE NULL,
                        runner_kind TEXT NULL,
                        render_job_id TEXT NULL,
                        message TEXT NOT NULL
                    )
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS issue_snapshot_time_entries (
                        id BIGSERIAL PRIMARY KEY,
                        snapshot_run_id BIGINT NOT NULL REFERENCES issue_snapshot_runs(id) ON DELETE CASCADE,
                        project_redmine_id INTEGER NOT NULL,
                        project_name TEXT NULL,
                        time_entry_redmine_id INTEGER NOT NULL,
                        issue_redmine_id INTEGER NULL,
                        issue_subject TEXT NULL,
                        issue_tracker_name TEXT NULL,
                        issue_status_name TEXT NULL,
                        user_id INTEGER NULL,
                        user_name TEXT NULL,
                        activity_id INTEGER NULL,
                        activity_name TEXT NULL,
                        hours DOUBLE PRECISION NOT NULL DEFAULT 0,
                        comments TEXT NULL,
                        spent_on DATE NOT NULL,
                        created_on TIMESTAMPTZ NULL,
                        updated_on TIMESTAMPTZ NULL
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
                    ALTER TABLE issue_snapshot_items
                    ADD COLUMN IF NOT EXISTS risk_estimate_hours DOUBLE PRECISION
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
                    ALTER TABLE issue_snapshot_time_entries
                    ADD COLUMN IF NOT EXISTS project_name TEXT
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE issue_snapshot_time_entries
                    ADD COLUMN IF NOT EXISTS issue_subject TEXT
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE issue_snapshot_time_entries
                    ADD COLUMN IF NOT EXISTS issue_tracker_name TEXT
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE issue_snapshot_time_entries
                    ADD COLUMN IF NOT EXISTS issue_status_name TEXT
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

            connection.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_issue_snapshot_time_entries_run_entry_unique
                    ON issue_snapshot_time_entries(snapshot_run_id, time_entry_redmine_id)
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_issue_snapshot_time_entries_run
                    ON issue_snapshot_time_entries(snapshot_run_id)
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_issue_snapshot_time_entries_spent_on
                    ON issue_snapshot_time_entries(snapshot_run_id, spent_on)
                    """
                )
            )

        _issueSnapshotTablesEnsured = True


def readIssueSnapshotCaptureStatusRecord() -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT payload
                FROM issue_snapshot_capture_status
                WHERE status_key = 'global'
                """
            )
        ).scalar_one_or_none()

    if row is None:
        return None

    if isinstance(row, dict):
        return row

    if isinstance(row, str):
        try:
            payload = json.loads(row)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    return None


def writeIssueSnapshotCaptureStatusRecord(status: dict[str, object]) -> None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    payload = json.dumps(status, ensure_ascii=False)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO issue_snapshot_capture_status (status_key, payload, updated_at)
                VALUES ('global', CAST(:payload AS JSONB), CURRENT_TIMESTAMP)
                ON CONFLICT (status_key) DO UPDATE
                SET payload = CAST(:payload AS JSONB),
                    updated_at = CURRENT_TIMESTAMP
                """
            ),
            {"payload": payload},
        )


def appendIssueSnapshotCaptureErrorRecord(
    *,
    message: str,
    mode: str | None = None,
    projectRedmineId: int | None = None,
    projectName: str | None = None,
    capturedForDate: str | None = None,
    runnerKind: str | None = None,
    renderJobId: str | None = None,
) -> None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO issue_snapshot_capture_errors (
                    mode,
                    project_redmine_id,
                    project_name,
                    captured_for_date,
                    runner_kind,
                    render_job_id,
                    message
                ) VALUES (
                    :mode,
                    :project_redmine_id,
                    :project_name,
                    CAST(:captured_for_date AS DATE),
                    :runner_kind,
                    :render_job_id,
                    :message
                )
                """
            ),
            {
                "mode": mode,
                "project_redmine_id": projectRedmineId,
                "project_name": projectName,
                "captured_for_date": capturedForDate,
                "runner_kind": runnerKind,
                "render_job_id": renderJobId,
                "message": message,
            },
        )


def listIssueSnapshotCaptureErrors(limit: int = 100) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureIssueSnapshotTables()
    safeLimit = max(1, min(int(limit or 100), 500))
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    id,
                    created_at,
                    mode,
                    project_redmine_id,
                    project_name,
                    captured_for_date,
                    runner_kind,
                    render_job_id,
                    message
                FROM issue_snapshot_capture_errors
                ORDER BY created_at DESC, id DESC
                LIMIT :limit_value
                """
            ),
            {"limit_value": safeLimit},
        ).mappings().all()

    return [dict(row) for row in rows]


def ensurePlanningProjectsTable() -> None:
    global _planningProjectsTableEnsured

    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    if _planningProjectsTableEnsured:
        return

    with _planningProjectsTableEnsureLock:
        if _planningProjectsTableEnsured:
            return

        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS planning_projects (
                        id SERIAL PRIMARY KEY,
                        direction TEXT,
                        project_name TEXT NOT NULL,
                        redmine_identifier TEXT,
                        pm_name TEXT,
                        customer TEXT,
                        start_date DATE NULL,
                        end_date DATE NULL,
                        development_hours DOUBLE PRECISION NULL,
                        year_1 INTEGER NULL,
                        hours_1 DOUBLE PRECISION NULL,
                        year_2 INTEGER NULL,
                        hours_2 DOUBLE PRECISION NULL,
                        year_3 INTEGER NULL,
                        hours_3 DOUBLE PRECISION NULL,
                        baseline_estimate_hours DOUBLE PRECISION NULL,
                        p1 DOUBLE PRECISION NULL,
                        p2 DOUBLE PRECISION NULL,
                        estimate_doc_url TEXT,
                        bitrix_url TEXT,
                        comment_text TEXT,
                        question_flag BOOLEAN NOT NULL DEFAULT FALSE,
                        is_closed BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        deleted_at TIMESTAMPTZ NULL
                    )
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS planning_project_versions (
                        id SERIAL PRIMARY KEY,
                        planning_project_id INTEGER NOT NULL,
                        operation TEXT NOT NULL,
                        changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        direction TEXT,
                        project_name TEXT NOT NULL,
                        redmine_identifier TEXT,
                        pm_name TEXT,
                        customer TEXT,
                        start_date DATE NULL,
                        end_date DATE NULL,
                        development_hours DOUBLE PRECISION NULL,
                        year_1 INTEGER NULL,
                        hours_1 DOUBLE PRECISION NULL,
                        year_2 INTEGER NULL,
                        hours_2 DOUBLE PRECISION NULL,
                        year_3 INTEGER NULL,
                        hours_3 DOUBLE PRECISION NULL,
                        baseline_estimate_hours DOUBLE PRECISION NULL,
                        p1 DOUBLE PRECISION NULL,
                        p2 DOUBLE PRECISION NULL,
                        estimate_doc_url TEXT,
                        bitrix_url TEXT,
                        comment_text TEXT,
                        question_flag BOOLEAN NOT NULL DEFAULT FALSE,
                        is_closed BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMPTZ NULL,
                        updated_at TIMESTAMPTZ NULL,
                        deleted_at TIMESTAMPTZ NULL
                    )
                    """
                )
            )

            connection.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_name = 'planning_projects'
                              AND column_name = 'baseline_assessment'
                        ) AND NOT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_name = 'planning_projects'
                              AND column_name = 'customer'
                        ) THEN
                            ALTER TABLE planning_projects
                            RENAME COLUMN baseline_assessment TO customer;
                        END IF;
                    END
                    $$;
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS comment_text TEXT
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS development_hours DOUBLE PRECISION NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS year_1 INTEGER NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS hours_1 DOUBLE PRECISION NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS year_2 INTEGER NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS hours_2 DOUBLE PRECISION NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS year_3 INTEGER NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS hours_3 DOUBLE PRECISION NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS direction TEXT
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS question_flag BOOLEAN NOT NULL DEFAULT FALSE
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS is_closed BOOLEAN NOT NULL DEFAULT FALSE
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_projects
                    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_planning_projects_deleted_at
                    ON planning_projects(deleted_at)
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_planning_project_versions_project_changed
                    ON planning_project_versions(planning_project_id, changed_at DESC, id DESC)
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_planning_project_versions_identifier_changed
                    ON planning_project_versions(LOWER(COALESCE(redmine_identifier, '')), changed_at DESC, id DESC)
                    """
                )
            )

            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_planning_projects_name
                    ON planning_projects(LOWER(project_name))
                    """
                )
            )

            connection.execute(
                text(
                    """
                    INSERT INTO planning_project_versions (
                        planning_project_id,
                        operation,
                        changed_at,
                        direction,
                        project_name,
                        redmine_identifier,
                        pm_name,
                        customer,
                        start_date,
                        end_date,
                        development_hours,
                        year_1,
                        hours_1,
                        year_2,
                        hours_2,
                        year_3,
                        hours_3,
                        baseline_estimate_hours,
                        p1,
                        p2,
                        estimate_doc_url,
                        bitrix_url,
                        comment_text,
                        question_flag,
                        is_closed,
                        created_at,
                        updated_at,
                        deleted_at
                    )
                    SELECT
                        p.id,
                        CASE WHEN p.deleted_at IS NULL THEN 'create' ELSE 'delete' END,
                        COALESCE(p.deleted_at, p.updated_at, p.created_at, CURRENT_TIMESTAMP),
                        p.direction,
                        p.project_name,
                        p.redmine_identifier,
                        p.pm_name,
                        p.customer,
                        p.start_date,
                        p.end_date,
                        p.development_hours,
                        p.year_1,
                        p.hours_1,
                        p.year_2,
                        p.hours_2,
                        p.year_3,
                        p.hours_3,
                        p.baseline_estimate_hours,
                        p.p1,
                        p.p2,
                        p.estimate_doc_url,
                        p.bitrix_url,
                        p.comment_text,
                        p.question_flag,
                        p.is_closed,
                        p.created_at,
                        p.updated_at,
                        p.deleted_at
                    FROM planning_projects p
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM planning_project_versions v
                        WHERE v.planning_project_id = p.id
                    )
                    """
                )
            )

        _planningProjectsTableEnsured = True


def _buildPlanningProjectVersionPayload(
    projectRow: Mapping[str, object],
    operation: str,
    changedAt: object | None = None,
) -> dict[str, object]:
    return {
        "planning_project_id": projectRow.get("id"),
        "operation": str(operation),
        "changed_at": changedAt or projectRow.get("deleted_at") or projectRow.get("updated_at") or projectRow.get("created_at"),
        "direction": projectRow.get("direction"),
        "project_name": projectRow.get("project_name"),
        "redmine_identifier": projectRow.get("redmine_identifier"),
        "pm_name": projectRow.get("pm_name"),
        "customer": projectRow.get("customer"),
        "start_date": projectRow.get("start_date"),
        "end_date": projectRow.get("end_date"),
        "development_hours": projectRow.get("development_hours"),
        "year_1": projectRow.get("year_1"),
        "hours_1": projectRow.get("hours_1"),
        "year_2": projectRow.get("year_2"),
        "hours_2": projectRow.get("hours_2"),
        "year_3": projectRow.get("year_3"),
        "hours_3": projectRow.get("hours_3"),
        "baseline_estimate_hours": projectRow.get("baseline_estimate_hours"),
        "p1": projectRow.get("p1"),
        "p2": projectRow.get("p2"),
        "estimate_doc_url": projectRow.get("estimate_doc_url"),
        "bitrix_url": projectRow.get("bitrix_url"),
        "comment_text": projectRow.get("comment_text"),
        "question_flag": bool(projectRow.get("question_flag")),
        "is_closed": bool(projectRow.get("is_closed")),
        "created_at": projectRow.get("created_at"),
        "updated_at": projectRow.get("updated_at"),
        "deleted_at": projectRow.get("deleted_at"),
    }


def _insertPlanningProjectVersion(connection: Connection, projectRow: Mapping[str, object], operation: str) -> None:
    connection.execute(
        text(
            """
            INSERT INTO planning_project_versions (
                planning_project_id,
                operation,
                changed_at,
                direction,
                project_name,
                redmine_identifier,
                pm_name,
                customer,
                start_date,
                end_date,
                development_hours,
                year_1,
                hours_1,
                year_2,
                hours_2,
                year_3,
                hours_3,
                baseline_estimate_hours,
                p1,
                p2,
                estimate_doc_url,
                bitrix_url,
                comment_text,
                question_flag,
                is_closed,
                created_at,
                updated_at,
                deleted_at
            ) VALUES (
                :planning_project_id,
                :operation,
                COALESCE(:changed_at, CURRENT_TIMESTAMP),
                :direction,
                :project_name,
                :redmine_identifier,
                :pm_name,
                :customer,
                :start_date,
                :end_date,
                :development_hours,
                :year_1,
                :hours_1,
                :year_2,
                :hours_2,
                :year_3,
                :hours_3,
                :baseline_estimate_hours,
                :p1,
                :p2,
                :estimate_doc_url,
                :bitrix_url,
                :comment_text,
                :question_flag,
                :is_closed,
                :created_at,
                :updated_at,
                :deleted_at
            )
            """
        ),
        _buildPlanningProjectVersionPayload(projectRow, operation),
    )


def ensureUsersTable() -> None:
    global _usersTableEnsured

    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    if _usersTableEnsured:
        return

    with _usersTableEnsureLock:
        if _usersTableEnsured:
            return

        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS app_users (
                        id SERIAL PRIMARY KEY,
                        login TEXT NOT NULL UNIQUE,
                        password_hash TEXT NOT NULL,
                        roles TEXT NOT NULL DEFAULT 'User',
                        must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
                        reset_password_token_hash TEXT NULL,
                        reset_password_expires_at TIMESTAMPTZ NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE app_users
                    ADD COLUMN IF NOT EXISTS roles TEXT NOT NULL DEFAULT 'User'
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE app_users
                    ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT FALSE
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE app_users
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE app_users
                    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE app_users
                    ADD COLUMN IF NOT EXISTS reset_password_token_hash TEXT NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE app_users
                    ADD COLUMN IF NOT EXISTS reset_password_expires_at TIMESTAMPTZ NULL
                    """
                )
            )

        _usersTableEnsured = True


def seedInitialUsers(users: Sequence[dict[str, object]]) -> None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    if not users:
        return

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_users (
                    login,
                    password_hash,
                    roles,
                    must_change_password
                )
                VALUES (
                    :login,
                    :password_hash,
                    :roles,
                    :must_change_password
                )
                ON CONFLICT (login) DO NOTHING
                """
            ),
            list(users),
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
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(i.tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(i.baseline_estimate_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS baseline_estimate_hours,
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
                    COALESCE(m.total_baseline_estimate_hours, 0) AS total_baseline_estimate_hours,
                    COALESCE(m.total_estimated_hours, 0) AS total_estimated_hours,
                    COALESCE(m.total_spent_hours, 0) AS total_spent_hours,
                    COALESCE(m.total_spent_hours_year, 0) AS total_spent_hours_year
                FROM issue_snapshot_runs r
                LEFT JOIN projects p
                    ON p.redmine_id = r.project_redmine_id
                LEFT JOIN (
                    SELECT
                        snapshot_run_id,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(baseline_estimate_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS total_baseline_estimate_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(estimated_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS total_estimated_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(spent_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS total_spent_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(spent_hours_year, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS total_spent_hours_year
                    FROM issue_snapshot_items
                    GROUP BY snapshot_run_id
                ) m
                    ON m.snapshot_run_id = r.id
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
                    COALESCE(m.total_baseline_estimate_hours, 0) AS total_baseline_estimate_hours,
                    COALESCE(m.total_estimated_hours, 0) AS total_estimated_hours,
                    COALESCE(m.total_spent_hours, 0) AS total_spent_hours,
                    COALESCE(m.total_spent_hours_year, 0) AS total_spent_hours_year
                FROM issue_snapshot_runs r
                LEFT JOIN projects p
                    ON p.redmine_id = r.project_redmine_id
                LEFT JOIN (
                    SELECT
                        snapshot_run_id,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(baseline_estimate_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS total_baseline_estimate_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(estimated_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS total_estimated_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(spent_hours, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS total_spent_hours,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature'
                                    THEN COALESCE(spent_hours_year, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS total_spent_hours_year
                    FROM issue_snapshot_items
                    GROUP BY snapshot_run_id
                ) m
                    ON m.snapshot_run_id = r.id
                ORDER BY r.captured_at DESC, r.id DESC
                LIMIT :limit_value
                """
            ),
                {"limit_value": limit},
            )

        return [dict(row._mapping) for row in rows]


def countPlanningProjects(searchText: str | None = None, includeClosed: bool = False) -> int:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    normalizedSearch = str(searchText or "").strip()
    searchPattern = f"%{normalizedSearch.lower()}%" if normalizedSearch else ""

    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM planning_projects
                WHERE deleted_at IS NULL
                  AND (:include_closed = TRUE OR COALESCE(is_closed, FALSE) = FALSE)
                  AND (
                    :search_pattern = ''
                    OR LOWER(COALESCE(direction, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(project_name, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(redmine_identifier, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(pm_name, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(customer, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(estimate_doc_url, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(bitrix_url, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(comment_text, '')) LIKE :search_pattern
                  )
                """
            ),
            {"include_closed": includeClosed, "search_pattern": searchPattern},
        ).scalar_one()
    return int(row or 0)


def listPlanningProjects(
    searchText: str | None = None,
    includeClosed: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    normalizedSearch = str(searchText or "").strip()
    searchPattern = f"%{normalizedSearch.lower()}%" if normalizedSearch else ""
    normalizedLimit = max(1, int(limit)) if limit is not None else None
    normalizedOffset = max(0, int(offset or 0))

    queryText = """
                SELECT
                    id,
                    direction,
                    project_name,
                    redmine_identifier,
                    pm_name,
                    customer,
                    start_date,
                    end_date,
                    development_hours,
                    year_1,
                    hours_1,
                    year_2,
                    hours_2,
                    year_3,
                    hours_3,
                    baseline_estimate_hours,
                    p1,
                    p2,
                    estimate_doc_url,
                    bitrix_url,
                    comment_text,
                    question_flag,
                    is_closed,
                    created_at,
                    updated_at
                FROM planning_projects
                WHERE deleted_at IS NULL
                  AND (:include_closed = TRUE OR COALESCE(is_closed, FALSE) = FALSE)
                  AND (
                    :search_pattern = ''
                    OR LOWER(COALESCE(direction, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(project_name, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(redmine_identifier, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(pm_name, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(customer, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(estimate_doc_url, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(bitrix_url, '')) LIKE :search_pattern
                    OR LOWER(COALESCE(comment_text, '')) LIKE :search_pattern
                  )
                ORDER BY LOWER(project_name), id
    """
    if normalizedLimit is not None:
        queryText += "\n                LIMIT :limit OFFSET :offset"

    params: dict[str, object] = {
        "include_closed": includeClosed,
        "search_pattern": searchPattern,
    }
    if normalizedLimit is not None:
        params["limit"] = normalizedLimit
        params["offset"] = normalizedOffset

    with engine.connect() as connection:
        rows = connection.execute(text(queryText), params)
    return [dict(row._mapping) for row in rows]


def listPlanningDirections() -> list[str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT direction
                FROM (
                    SELECT DISTINCT TRIM(direction) AS direction
                    FROM planning_projects
                    WHERE deleted_at IS NULL
                      AND TRIM(COALESCE(direction, '')) <> ''
                ) directions
                ORDER BY LOWER(direction)
                """
            )
        ).fetchall()

    return [str(row.direction) for row in rows if str(row.direction or "").strip()]


def _listProjectPlanningSummaryInternal(
    reportDate: str,
    direction: str | None = None,
    isClosed: bool = False,
    versioned: bool = False,
) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    normalizedDirection = str(direction or "").strip()
    reportYear = int(str(reportDate)[:4])

    if versioned:
        planningProjectsCte = """
                effective_planning_projects AS (
                    SELECT
                        latest_versions.id,
                        latest_versions.direction,
                        latest_versions.customer,
                        latest_versions.project_name,
                        latest_versions.redmine_identifier,
                        latest_versions.pm_name,
                        latest_versions.development_hours,
                        latest_versions.year_1,
                        latest_versions.hours_1,
                        latest_versions.year_2,
                        latest_versions.hours_2,
                        latest_versions.year_3,
                        latest_versions.hours_3,
                        latest_versions.is_closed,
                        latest_versions.question_flag
                    FROM (
                        SELECT DISTINCT ON (v.planning_project_id)
                            v.planning_project_id AS id,
                            v.direction,
                            v.customer,
                            v.project_name,
                            v.redmine_identifier,
                            v.pm_name,
                            v.development_hours,
                            v.year_1,
                            v.hours_1,
                            v.year_2,
                            v.hours_2,
                            v.year_3,
                            v.hours_3,
                            v.is_closed,
                            v.question_flag,
                            v.operation
                        FROM planning_project_versions v
                        WHERE v.changed_at < (CAST(:report_date AS DATE) + INTERVAL '1 day')
                        ORDER BY v.planning_project_id, v.changed_at DESC, v.id DESC
                    ) latest_versions
                    WHERE latest_versions.operation <> 'delete'
                      AND COALESCE(latest_versions.is_closed, FALSE) = :is_closed
                      AND (
                        :direction = ''
                        OR LOWER(TRIM(COALESCE(latest_versions.direction, ''))) = LOWER(TRIM(:direction))
                      )
                ),
        """
    else:
        planningProjectsCte = """
                effective_planning_projects AS (
                    SELECT
                        id,
                        direction,
                        customer,
                        project_name,
                        redmine_identifier,
                        pm_name,
                        development_hours,
                        year_1,
                        hours_1,
                        year_2,
                        hours_2,
                        year_3,
                        hours_3,
                        is_closed,
                        question_flag
                    FROM planning_projects
                    WHERE deleted_at IS NULL
                      AND COALESCE(is_closed, FALSE) = :is_closed
                      AND (
                        :direction = ''
                        OR LOWER(TRIM(COALESCE(direction, ''))) = LOWER(TRIM(:direction))
                      )
                ),
        """

    queryText = f"""
                WITH
                {planningProjectsCte}
                project_identifier_map AS (
                    SELECT DISTINCT ON (LOWER(TRIM(COALESCE(identifier, ''))))
                        LOWER(TRIM(COALESCE(identifier, ''))) AS normalized_identifier,
                        redmine_id
                    FROM projects
                    WHERE TRIM(COALESCE(identifier, '')) <> ''
                    ORDER BY LOWER(TRIM(COALESCE(identifier, ''))), redmine_id
                ),
                latest_snapshot_runs AS (
                    SELECT DISTINCT ON (r.project_redmine_id)
                        r.id,
                        LOWER(TRIM(COALESCE(p.identifier, r.project_identifier, ''))) AS normalized_identifier
                    FROM issue_snapshot_runs r
                    LEFT JOIN projects p
                        ON p.redmine_id = r.project_redmine_id
                    WHERE r.captured_for_date <= :report_date
                    ORDER BY r.project_redmine_id, r.captured_for_date DESC, r.captured_at DESC, r.id DESC
                ),
                latest_snapshot_metrics AS (
                    SELECT
                        lr.normalized_identifier,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(i.tracker_name, ''))) IN (
                                        LOWER('Разработка'),
                                        LOWER('Процессы разработки'),
                                        LOWER('Ошибка')
                                    )
                                    THEN COALESCE(i.spent_hours_year, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS development_spent_hours_year
                    FROM latest_snapshot_runs lr
                    LEFT JOIN issue_snapshot_items i
                        ON i.snapshot_run_id = lr.id
                    GROUP BY lr.normalized_identifier
                )
                SELECT
                    fp.id,
                    fp.direction,
                    fp.customer,
                    fp.project_name,
                    fp.redmine_identifier,
                    pim.redmine_id AS project_redmine_id,
                    fp.pm_name,
                    fp.development_hours,
                    CASE
                        WHEN :report_year = fp.year_1 THEN fp.hours_1
                        WHEN :report_year = fp.year_2 THEN fp.hours_2
                        WHEN :report_year = fp.year_3 THEN fp.hours_3
                        ELSE NULL
                    END AS report_year_hours,
                    COALESCE(lsm.development_spent_hours_year, 0) AS development_spent_hours_year,
                    fp.question_flag,
                    fp.is_closed
                FROM effective_planning_projects fp
                LEFT JOIN latest_snapshot_metrics lsm
                    ON lsm.normalized_identifier = LOWER(TRIM(COALESCE(fp.redmine_identifier, '')))
                LEFT JOIN project_identifier_map pim
                    ON pim.normalized_identifier = LOWER(TRIM(COALESCE(fp.redmine_identifier, '')))
                ORDER BY
                    LOWER(COALESCE(fp.direction, '')),
                    LOWER(COALESCE(fp.customer, '')),
                    LOWER(COALESCE(fp.project_name, '')),
                    fp.id
    """

    with engine.connect() as connection:
        rows = connection.execute(
            text(queryText),
            {
                "report_date": str(reportDate),
                "report_year": reportYear,
                "direction": normalizedDirection,
                "is_closed": bool(isClosed),
            },
        ).mappings().all()

    return [dict(row) for row in rows]


def listProjectPlanningSummary(reportDate: str, direction: str | None = None, isClosed: bool = False) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    normalizedDirection = str(direction or "").strip()
    reportYear = int(str(reportDate)[:4])

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                WITH filtered_planning_projects AS (
                    SELECT
                        id,
                        direction,
                        customer,
                        project_name,
                        redmine_identifier,
                        pm_name,
                        development_hours,
                        year_1,
                        hours_1,
                        year_2,
                        hours_2,
                        year_3,
                        hours_3,
                        is_closed,
                        question_flag
                    FROM planning_projects
                    WHERE COALESCE(is_closed, FALSE) = :is_closed
                      AND (
                        :direction = ''
                        OR LOWER(TRIM(COALESCE(direction, ''))) = LOWER(TRIM(:direction))
                      )
                ),
                project_identifier_map AS (
                    SELECT DISTINCT ON (LOWER(TRIM(COALESCE(identifier, ''))))
                        LOWER(TRIM(COALESCE(identifier, ''))) AS normalized_identifier,
                        redmine_id
                    FROM projects
                    WHERE TRIM(COALESCE(identifier, '')) <> ''
                    ORDER BY LOWER(TRIM(COALESCE(identifier, ''))), redmine_id
                ),
                latest_snapshot_runs AS (
                    SELECT DISTINCT ON (r.project_redmine_id)
                        r.id,
                        LOWER(TRIM(COALESCE(p.identifier, r.project_identifier, ''))) AS normalized_identifier
                    FROM issue_snapshot_runs r
                    LEFT JOIN projects p
                        ON p.redmine_id = r.project_redmine_id
                    WHERE r.captured_for_date <= :report_date
                    ORDER BY r.project_redmine_id, r.captured_for_date DESC, r.captured_at DESC, r.id DESC
                ),
                latest_snapshot_metrics AS (
                    SELECT
                        lr.normalized_identifier,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(i.tracker_name, ''))) = LOWER('Разработка')
                                    THEN COALESCE(i.spent_hours_year, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS development_spent_hours_year
                    FROM latest_snapshot_runs lr
                    LEFT JOIN issue_snapshot_items i
                        ON i.snapshot_run_id = lr.id
                    GROUP BY lr.normalized_identifier
                )
                SELECT
                    fp.id,
                    fp.direction,
                    fp.customer,
                    fp.project_name,
                    fp.redmine_identifier,
                    pim.redmine_id AS project_redmine_id,
                    fp.pm_name,
                    fp.development_hours,
                    CASE
                        WHEN :report_year = fp.year_1 THEN fp.hours_1
                        WHEN :report_year = fp.year_2 THEN fp.hours_2
                        WHEN :report_year = fp.year_3 THEN fp.hours_3
                        ELSE NULL
                    END AS report_year_hours,
                    COALESCE(lsm.development_spent_hours_year, 0) AS development_spent_hours_year,
                    fp.question_flag,
                    fp.is_closed
                FROM filtered_planning_projects fp
                LEFT JOIN latest_snapshot_metrics lsm
                    ON lsm.normalized_identifier = LOWER(TRIM(COALESCE(fp.redmine_identifier, '')))
                LEFT JOIN project_identifier_map pim
                    ON pim.normalized_identifier = LOWER(TRIM(COALESCE(fp.redmine_identifier, '')))
                ORDER BY
                    LOWER(COALESCE(fp.direction, '')),
                    LOWER(COALESCE(fp.customer, '')),
                    LOWER(COALESCE(fp.project_name, '')),
                    fp.id
                """
            ),
            {
                "report_date": str(reportDate),
                "report_year": reportYear,
                "direction": normalizedDirection,
                "is_closed": bool(isClosed),
            },
        ).mappings().all()

    return [dict(row) for row in rows]


def listProjectPlanningSummary(reportDate: str, direction: str | None = None, isClosed: bool = False) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    normalizedDirection = str(direction or "").strip()
    reportYear = int(str(reportDate)[:4])

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                WITH filtered_planning_projects AS (
                    SELECT
                        id,
                        direction,
                        customer,
                        project_name,
                        redmine_identifier,
                        pm_name,
                        development_hours,
                        year_1,
                        hours_1,
                        year_2,
                        hours_2,
                        year_3,
                        hours_3,
                        is_closed,
                        question_flag
                    FROM planning_projects
                    WHERE COALESCE(is_closed, FALSE) = :is_closed
                      AND (
                        :direction = ''
                        OR LOWER(TRIM(COALESCE(direction, ''))) = LOWER(TRIM(:direction))
                      )
                ),
                project_identifier_map AS (
                    SELECT DISTINCT ON (LOWER(TRIM(COALESCE(identifier, ''))))
                        LOWER(TRIM(COALESCE(identifier, ''))) AS normalized_identifier,
                        redmine_id
                    FROM projects
                    WHERE TRIM(COALESCE(identifier, '')) <> ''
                    ORDER BY LOWER(TRIM(COALESCE(identifier, ''))), redmine_id
                ),
                latest_snapshot_runs AS (
                    SELECT DISTINCT ON (r.project_redmine_id)
                        r.id,
                        LOWER(TRIM(COALESCE(p.identifier, r.project_identifier, ''))) AS normalized_identifier
                    FROM issue_snapshot_runs r
                    LEFT JOIN projects p
                        ON p.redmine_id = r.project_redmine_id
                    WHERE r.captured_for_date <= :report_date
                    ORDER BY r.project_redmine_id, r.captured_for_date DESC, r.captured_at DESC, r.id DESC
                ),
                latest_snapshot_metrics AS (
                    SELECT
                        lr.normalized_identifier,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(i.tracker_name, ''))) IN (
                                        LOWER('Разработка'),
                                        LOWER('Процессы разработки'),
                                        LOWER('Ошибка')
                                    )
                                    THEN COALESCE(i.spent_hours_year, 0)
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS development_spent_hours_year
                    FROM latest_snapshot_runs lr
                    LEFT JOIN issue_snapshot_items i
                        ON i.snapshot_run_id = lr.id
                    GROUP BY lr.normalized_identifier
                )
                SELECT
                    fp.id,
                    fp.direction,
                    fp.customer,
                    fp.project_name,
                    fp.redmine_identifier,
                    pim.redmine_id AS project_redmine_id,
                    fp.pm_name,
                    fp.development_hours,
                    CASE
                        WHEN :report_year = fp.year_1 THEN fp.hours_1
                        WHEN :report_year = fp.year_2 THEN fp.hours_2
                        WHEN :report_year = fp.year_3 THEN fp.hours_3
                        ELSE NULL
                    END AS report_year_hours,
                    COALESCE(lsm.development_spent_hours_year, 0) AS development_spent_hours_year,
                    fp.question_flag,
                    fp.is_closed
                FROM filtered_planning_projects fp
                LEFT JOIN latest_snapshot_metrics lsm
                    ON lsm.normalized_identifier = LOWER(TRIM(COALESCE(fp.redmine_identifier, '')))
                LEFT JOIN project_identifier_map pim
                    ON pim.normalized_identifier = LOWER(TRIM(COALESCE(fp.redmine_identifier, '')))
                ORDER BY
                    LOWER(COALESCE(fp.direction, '')),
                    LOWER(COALESCE(fp.customer, '')),
                    LOWER(COALESCE(fp.project_name, '')),
                    fp.id
                """
            ),
            {
                "report_date": str(reportDate),
                "report_year": reportYear,
                "direction": normalizedDirection,
                "is_closed": bool(isClosed),
            },
        ).mappings().all()

    return [dict(row) for row in rows]


def listProjectPlanningSummary(
    reportDate: str,
    direction: str | None = None,
    isClosed: bool = False,
) -> list[dict[str, object]]:
    return _listProjectPlanningSummaryInternal(
        reportDate=reportDate,
        direction=direction,
        isClosed=isClosed,
        versioned=False,
    )


def listProjectPlanningSummaryVersioned(
    reportDate: str,
    direction: str | None = None,
    isClosed: bool = False,
) -> list[dict[str, object]]:
    return _listProjectPlanningSummaryInternal(
        reportDate=reportDate,
        direction=direction,
        isClosed=isClosed,
        versioned=True,
    )


def getPlanningProjectByRedmineIdentifier(redmineIdentifier: str) -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    normalizedIdentifier = str(redmineIdentifier or "").strip()
    if not normalizedIdentifier:
        return None

    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    id,
                    direction,
                    project_name,
                    redmine_identifier,
                    pm_name,
                    customer,
                    start_date,
                    end_date,
                    development_hours,
                    year_1,
                    hours_1,
                    year_2,
                    hours_2,
                    year_3,
                    hours_3,
                    baseline_estimate_hours,
                    p1,
                    p2,
                    estimate_doc_url,
                    bitrix_url,
                    comment_text,
                    question_flag,
                    is_closed,
                    created_at,
                    updated_at
                FROM planning_projects
                WHERE lower(trim(COALESCE(redmine_identifier, ''))) = lower(trim(:redmine_identifier))
                  AND deleted_at IS NULL
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"redmine_identifier": normalizedIdentifier},
        ).mappings().first()

    return dict(row) if row else None


def listPlanningProjectsByRedmineIdentifier(redmineIdentifier: str) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    normalizedIdentifier = str(redmineIdentifier or "").strip()
    if not normalizedIdentifier:
        return []

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    id,
                    direction,
                    project_name,
                    redmine_identifier,
                    pm_name,
                    customer,
                    start_date,
                    end_date,
                    development_hours,
                    year_1,
                    hours_1,
                    year_2,
                    hours_2,
                    year_3,
                    hours_3,
                    baseline_estimate_hours,
                    p1,
                    p2,
                    estimate_doc_url,
                    bitrix_url,
                    comment_text,
                    question_flag,
                    is_closed,
                    created_at,
                    updated_at
                FROM planning_projects
                WHERE lower(trim(COALESCE(redmine_identifier, ''))) = lower(trim(:redmine_identifier))
                  AND deleted_at IS NULL
                ORDER BY lower(project_name), id
                """
            ),
            {"redmine_identifier": normalizedIdentifier},
        ).mappings().all()

    return [dict(row) for row in rows]


def listPlanningProjectIdentifiers() -> list[str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT DISTINCT lower(trim(COALESCE(redmine_identifier, ''))) AS redmine_identifier
                FROM planning_projects
                WHERE deleted_at IS NULL
                  AND trim(COALESCE(redmine_identifier, '')) <> ''
                ORDER BY lower(trim(COALESCE(redmine_identifier, '')))
                """
            )
        ).fetchall()

    return [str(row.redmine_identifier) for row in rows if str(row.redmine_identifier or "").strip()]


def listUsers() -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    id,
                    login,
                    roles,
                    must_change_password,
                    created_at,
                    updated_at
                FROM app_users
                ORDER BY lower(login)
                """
            )
        ).fetchall()

    return [dict(row._mapping) for row in rows]


def getUserByLogin(login: str) -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    id,
                    login,
                    password_hash,
                    roles,
                    must_change_password,
                    reset_password_token_hash,
                    reset_password_expires_at,
                    created_at,
                    updated_at
                FROM app_users
                WHERE login = :login
                """
            ),
            {"login": login},
        ).mappings().first()

    return dict(row) if row else None


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
                    risk_estimate_hours,
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


def listLatestSnapshotIssuesWithParents() -> list[dict[str, object]]:
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
                        r.captured_for_date,
                        r.captured_at,
                        COALESCE(p.name, r.project_name) AS project_name,
                        COALESCE(p.identifier, r.project_identifier) AS project_identifier
                    FROM issue_snapshot_runs r
                    LEFT JOIN projects p
                        ON p.redmine_id = r.project_redmine_id
                    ORDER BY r.project_redmine_id, r.captured_for_date DESC, r.captured_at DESC, r.id DESC
                )
                SELECT
                    lr.id AS snapshot_run_id,
                    lr.project_redmine_id,
                    lr.project_name,
                    lr.project_identifier,
                    lr.captured_for_date,
                    lr.captured_at,
                    i.issue_redmine_id,
                    i.subject,
                    i.tracker_name,
                    i.status_name,
                    i.parent_issue_redmine_id
                FROM latest_snapshot_runs lr
                JOIN issue_snapshot_items i
                    ON i.snapshot_run_id = lr.id
                WHERE i.parent_issue_redmine_id IS NOT NULL
                ORDER BY LOWER(lr.project_name), lr.project_redmine_id, i.issue_redmine_id
                """
            )
        )

        return [dict(row._mapping) for row in rows]


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
                    risk_estimate_hours,
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


def _listSnapshotDatesForProjectWithConnection(connection, projectRedmineId: int) -> list[str]:
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
    return [str(row[0]) for row in rows]


def _getSnapshotRunWithConnection(connection, projectRedmineId: int, capturedForDate: str | None) -> dict[str, object] | None:
    params: dict[str, object] = {"project_redmine_id": projectRedmineId}
    if capturedForDate:
        params["captured_for_date"] = capturedForDate
        query = text(
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
        )
    else:
        query = text(
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
        )

    row = connection.execute(query, params).mappings().first()
    return dict(row) if row is not None else None


def _normalizeSnapshotIssueFilters(filters: dict[str, object] | None) -> dict[str, object]:
    payload = dict(filters or {})

    def normalizeText(value: object) -> str:
        return str(value or "").strip()

    def normalizeMulti(values: object) -> list[str]:
        if not isinstance(values, (list, tuple, set)):
            values = [values] if values not in (None, "") else []
        normalized: list[str] = []
        for value in values:
            textValue = normalizeText(value)
            if textValue and textValue not in normalized:
                normalized.append(textValue)
        return normalized

    def normalizeNumeric(value: object) -> float | None:
        rawValue = normalizeText(value).replace(",", ".")
        if not rawValue:
            return None
        try:
            return float(rawValue)
        except ValueError:
            return None

    normalized: dict[str, object] = {
        "issue_id": normalizeText(payload.get("issue_id")),
        "subject": normalizeText(payload.get("subject")),
        "tracker_names": normalizeMulti(payload.get("tracker_names")),
        "status_names": normalizeMulti(payload.get("status_names")),
        "closed_on": normalizeText(payload.get("closed_on")),
        "assigned_to": normalizeText(payload.get("assigned_to")),
        "fixed_version": normalizeText(payload.get("fixed_version")),
    }

    numericConfig = {
        "done_ratio": "done_ratio",
        "baseline": "baseline",
        "estimated": "estimated",
        "risk": "risk",
        "spent": "spent",
        "spent_year": "spent_year",
    }
    for filterKey, paramPrefix in numericConfig.items():
        operator = normalizeText(payload.get(f"{paramPrefix}_op"))
        normalized[f"{paramPrefix}_op"] = operator if operator in {">", "<", "="} else ""
        normalized[f"{paramPrefix}_value"] = normalizeNumeric(payload.get(f"{paramPrefix}_value"))

    return normalized


def _buildSnapshotIssueFilterParts(filters: dict[str, object] | None) -> tuple[list[str], dict[str, object], list[object]]:
    normalizedFilters = _normalizeSnapshotIssueFilters(filters)
    whereClauses: list[str] = []
    params: dict[str, object] = {}
    bindParams: list[object] = []

    textMappings = {
        "issue_id": "CAST(issue_redmine_id AS TEXT)",
        "subject": "COALESCE(subject, '')",
        "closed_on": "COALESCE(CAST(closed_on AS TEXT), '')",
        "assigned_to": "COALESCE(assigned_to_name, '')",
        "fixed_version": "COALESCE(fixed_version_name, '')",
    }
    for filterKey, column in textMappings.items():
        value = str(normalizedFilters.get(filterKey) or "")
        if value:
            paramName = f"{filterKey}_like"
            whereClauses.append(f"{column} ILIKE :{paramName}")
            params[paramName] = f"%{value}%"

    trackerNames = list(normalizedFilters.get("tracker_names") or [])
    if trackerNames:
        whereClauses.append("COALESCE(tracker_name, '—') IN :tracker_names")
        params["tracker_names"] = trackerNames
        bindParams.append(bindparam("tracker_names", expanding=True))

    statusNames = list(normalizedFilters.get("status_names") or [])
    if statusNames:
        whereClauses.append("COALESCE(status_name, '—') IN :status_names")
        params["status_names"] = statusNames
        bindParams.append(bindparam("status_names", expanding=True))

    numericMappings = {
        "done_ratio": "COALESCE(done_ratio, 0)",
        "baseline": "COALESCE(baseline_estimate_hours, 0)",
        "estimated": "COALESCE(estimated_hours, 0)",
        "risk": "COALESCE(risk_estimate_hours, 0)",
        "spent": "COALESCE(spent_hours, 0)",
        "spent_year": "COALESCE(spent_hours_year, 0)",
    }
    for filterKey, column in numericMappings.items():
        operator = str(normalizedFilters.get(f"{filterKey}_op") or "")
        value = normalizedFilters.get(f"{filterKey}_value")
        if operator and value is not None:
            paramName = f"{filterKey}_value"
            whereClauses.append(f"{column} {operator} :{paramName}")
            params[paramName] = value

    return whereClauses, params, bindParams


def _getSnapshotIssueFilterOptions(connection, snapshotRunId: int) -> dict[str, list[str]]:
    trackerRows = connection.execute(
        text(
            """
            SELECT DISTINCT COALESCE(tracker_name, '—') AS value
            FROM issue_snapshot_items
            WHERE snapshot_run_id = :snapshot_run_id
            ORDER BY value
            """
        ),
        {"snapshot_run_id": snapshotRunId},
    )
    statusRows = connection.execute(
        text(
            """
            SELECT DISTINCT COALESCE(status_name, '—') AS value
            FROM issue_snapshot_items
            WHERE snapshot_run_id = :snapshot_run_id
            ORDER BY value
            """
        ),
        {"snapshot_run_id": snapshotRunId},
    )
    return {
        "tracker_names": [str(row[0]) for row in trackerRows if row[0] is not None],
        "status_names": [str(row[0]) for row in statusRows if row[0] is not None],
    }


def _buildSnapshotIssueHierarchyQuery(baseWhereSql: str, paginated: bool) -> str:
    paginationSql = "\n            LIMIT :limit OFFSET :offset" if paginated else ""
    return f"""
            WITH RECURSIVE snapshot_items AS (
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
                    risk_estimate_hours,
                    spent_hours,
                    spent_hours_year,
                    start_date,
                    due_date,
                    created_on,
                    updated_on,
                    closed_on,
                    parent_issue_redmine_id
                FROM issue_snapshot_items
                WHERE snapshot_run_id = :snapshot_run_id
            ),
            filtered_items AS (
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
                    risk_estimate_hours,
                    spent_hours,
                    spent_hours_year,
                    start_date,
                    due_date,
                    created_on,
                    updated_on,
                    closed_on,
                    parent_issue_redmine_id
                FROM issue_snapshot_items
                WHERE {baseWhereSql}
            ),
            item_chain AS (
                SELECT
                    issue_redmine_id AS origin_issue_redmine_id,
                    issue_redmine_id AS current_issue_redmine_id,
                    parent_issue_redmine_id AS next_parent_issue_redmine_id,
                    LOWER(TRIM(COALESCE(tracker_name, ''))) AS current_tracker_name,
                    COALESCE(subject, '') AS current_subject,
                    0 AS depth
                FROM snapshot_items
                UNION ALL
                SELECT
                    item_chain.origin_issue_redmine_id,
                    parent.issue_redmine_id AS current_issue_redmine_id,
                    parent.parent_issue_redmine_id AS next_parent_issue_redmine_id,
                    LOWER(TRIM(COALESCE(parent.tracker_name, ''))) AS current_tracker_name,
                    COALESCE(parent.subject, '') AS current_subject,
                    item_chain.depth + 1 AS depth
                FROM item_chain
                JOIN snapshot_items parent
                    ON parent.issue_redmine_id = item_chain.next_parent_issue_redmine_id
                WHERE item_chain.depth < 50
            ),
            feature_groups AS (
                SELECT DISTINCT ON (origin_issue_redmine_id)
                    origin_issue_redmine_id,
                    current_issue_redmine_id AS feature_group_issue_redmine_id,
                    current_subject AS feature_group_subject
                FROM item_chain
                WHERE current_tracker_name = 'feature'
                ORDER BY origin_issue_redmine_id, depth ASC
            )
            SELECT
                filtered_items.issue_redmine_id,
                filtered_items.subject,
                filtered_items.tracker_name,
                filtered_items.status_name,
                filtered_items.priority_name,
                filtered_items.assigned_to_name,
                filtered_items.fixed_version_name,
                filtered_items.done_ratio,
                filtered_items.baseline_estimate_hours,
                filtered_items.estimated_hours,
                filtered_items.risk_estimate_hours,
                filtered_items.spent_hours,
                filtered_items.spent_hours_year,
                filtered_items.start_date,
                filtered_items.due_date,
                filtered_items.created_on,
                filtered_items.updated_on,
                filtered_items.closed_on,
                filtered_items.parent_issue_redmine_id,
                feature_groups.feature_group_issue_redmine_id,
                feature_root.tracker_name AS feature_group_tracker_name,
                feature_root.status_name AS feature_group_status_name,
                feature_root.assigned_to_name AS feature_group_assigned_to_name,
                feature_root.fixed_version_name AS feature_group_fixed_version_name,
                feature_root.done_ratio AS feature_group_done_ratio,
                feature_root.baseline_estimate_hours AS feature_group_baseline_estimate_hours,
                feature_root.estimated_hours AS feature_group_estimated_hours,
                feature_root.risk_estimate_hours AS feature_group_risk_estimate_hours,
                feature_root.spent_hours AS feature_group_spent_hours,
                feature_root.spent_hours_year AS feature_group_spent_hours_year,
                feature_root.closed_on AS feature_group_closed_on,
                COALESCE(NULLIF(feature_groups.feature_group_subject, ''), 'без Feature') AS feature_group_subject,
                CASE WHEN feature_groups.feature_group_issue_redmine_id IS NULL THEN TRUE ELSE FALSE END AS feature_group_is_virtual,
                CASE
                    WHEN feature_groups.feature_group_issue_redmine_id IS NOT NULL
                     AND filtered_items.issue_redmine_id = feature_groups.feature_group_issue_redmine_id
                     AND LOWER(TRIM(COALESCE(filtered_items.tracker_name, ''))) = 'feature'
                    THEN TRUE
                    ELSE FALSE
                END AS is_feature_group_root
            FROM filtered_items
            LEFT JOIN feature_groups
                ON feature_groups.origin_issue_redmine_id = filtered_items.issue_redmine_id
            LEFT JOIN issue_snapshot_items feature_root
                ON feature_root.snapshot_run_id = :snapshot_run_id
               AND feature_root.issue_redmine_id = feature_groups.feature_group_issue_redmine_id
            ORDER BY
                CASE WHEN feature_groups.feature_group_issue_redmine_id IS NULL THEN 1 ELSE 0 END,
                COALESCE(feature_groups.feature_group_issue_redmine_id, 2147483647),
                CASE
                    WHEN feature_groups.feature_group_issue_redmine_id IS NOT NULL
                     AND filtered_items.issue_redmine_id = feature_groups.feature_group_issue_redmine_id
                     AND LOWER(TRIM(COALESCE(filtered_items.tracker_name, ''))) = 'feature'
                    THEN 0
                    WHEN LOWER(TRIM(COALESCE(filtered_items.tracker_name, ''))) = 'разработка' THEN 1
                    WHEN LOWER(TRIM(COALESCE(filtered_items.tracker_name, ''))) = 'процессы разработки' THEN 2
                    WHEN LOWER(TRIM(COALESCE(filtered_items.tracker_name, ''))) = 'ошибка' THEN 3
                    ELSE 4
                END,
                filtered_items.issue_redmine_id{paginationSql}
            """


def getFilteredSnapshotIssuesForProjectByDate(
    projectRedmineId: int,
    capturedForDate: str | None,
    filters: dict[str, object] | None = None,
    page: int = 1,
    pageSize: int = 1000,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    safePageSize = max(1, int(pageSize or 1))
    requestedPage = max(1, int(page or 1))

    with engine.connect() as connection:
        latestRun = _getSnapshotRunWithConnection(connection, projectRedmineId, capturedForDate)
        availableDates = _listSnapshotDatesForProjectWithConnection(connection, projectRedmineId)
        if latestRun is None:
            return {
                "snapshot_run": None,
                "issues": [],
                "available_dates": availableDates,
                "filter_options": {"tracker_names": [], "status_names": []},
                "summary": {
                    "baseline_estimate_hours": 0.0,
                    "estimated_hours": 0.0,
                    "risk_estimate_hours": 0.0,
                    "spent_hours": 0.0,
                    "spent_hours_year": 0.0,
                    "development_estimated_hours": 0.0,
                    "development_risk_estimate_hours": 0.0,
                    "development_spent_hours": 0.0,
                    "development_spent_hours_year": 0.0,
                    "development_process_estimated_hours": 0.0,
                    "development_process_spent_hours": 0.0,
                    "development_process_spent_hours_year": 0.0,
                    "bug_estimated_hours": 0.0,
                    "bug_spent_hours": 0.0,
                    "bug_spent_hours_year": 0.0,
                },
                "page": 1,
                "page_size": safePageSize,
                "total_pages": 1,
                "total_filtered_issues": 0,
                "total_all_issues": 0,
            }

        whereClauses, filterParams, bindParams = _buildSnapshotIssueFilterParts(filters)
        baseWhereClauses = ["snapshot_run_id = :snapshot_run_id", *whereClauses]
        baseWhereSql = " AND ".join(baseWhereClauses)
        baseParams = {"snapshot_run_id": latestRun["id"], **filterParams}

        countStatement = text(
            f"""
            SELECT COUNT(*)
            FROM issue_snapshot_items
            WHERE {baseWhereSql}
            """
        ).bindparams(*bindParams)
        totalFilteredIssues = int(connection.execute(countStatement, baseParams).scalar_one() or 0)
        totalAllIssues = int(latestRun.get("total_issues") or 0)

        summaryStatement = text(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature' THEN COALESCE(baseline_estimate_hours, 0) ELSE 0 END), 0) AS baseline_estimate_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature' THEN COALESCE(estimated_hours, 0) ELSE 0 END), 0) AS estimated_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature' THEN COALESCE(risk_estimate_hours, 0) ELSE 0 END), 0) AS risk_estimate_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature' THEN COALESCE(spent_hours, 0) ELSE 0 END), 0) AS spent_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) <> 'feature' THEN COALESCE(spent_hours_year, 0) ELSE 0 END), 0) AS spent_hours_year,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'feature' THEN COALESCE(baseline_estimate_hours, 0) ELSE 0 END), 0) AS feature_baseline_estimate_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'feature' THEN COALESCE(estimated_hours, 0) ELSE 0 END), 0) AS feature_estimated_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'feature' THEN COALESCE(spent_hours, 0) ELSE 0 END), 0) AS feature_spent_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'feature' THEN COALESCE(spent_hours_year, 0) ELSE 0 END), 0) AS feature_spent_hours_year,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'разработка' THEN COALESCE(estimated_hours, 0) ELSE 0 END), 0) AS development_estimated_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'разработка' THEN COALESCE(risk_estimate_hours, 0) ELSE 0 END), 0) AS development_risk_estimate_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'разработка' THEN COALESCE(spent_hours, 0) ELSE 0 END), 0) AS development_spent_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'разработка' THEN COALESCE(spent_hours_year, 0) ELSE 0 END), 0) AS development_spent_hours_year,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'процессы разработки' THEN COALESCE(estimated_hours, 0) ELSE 0 END), 0) AS development_process_estimated_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'процессы разработки' THEN COALESCE(spent_hours, 0) ELSE 0 END), 0) AS development_process_spent_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'процессы разработки' THEN COALESCE(spent_hours_year, 0) ELSE 0 END), 0) AS development_process_spent_hours_year,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'ошибка' THEN COALESCE(estimated_hours, 0) ELSE 0 END), 0) AS bug_estimated_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'ошибка' THEN COALESCE(spent_hours, 0) ELSE 0 END), 0) AS bug_spent_hours,
                COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(tracker_name, ''))) = 'ошибка' THEN COALESCE(spent_hours_year, 0) ELSE 0 END), 0) AS bug_spent_hours_year
            FROM issue_snapshot_items
            WHERE {baseWhereSql}
            """
        ).bindparams(*bindParams)
        summaryRow = connection.execute(summaryStatement, baseParams).mappings().one()

        totalPages = max(1, (totalFilteredIssues + safePageSize - 1) // safePageSize) if totalFilteredIssues else 1
        currentPage = min(requestedPage, totalPages)
        pageParams = {
            **baseParams,
            "limit": safePageSize,
            "offset": (currentPage - 1) * safePageSize,
        }
        issuesStatement = text(_buildSnapshotIssueHierarchyQuery(baseWhereSql, paginated=True)).bindparams(*bindParams)
        issueRows = connection.execute(issuesStatement, pageParams).mappings().all()

        return {
            "snapshot_run": latestRun,
            "issues": [dict(row) for row in issueRows],
            "available_dates": availableDates,
            "filter_options": _getSnapshotIssueFilterOptions(connection, int(latestRun["id"])),
            "summary": dict(summaryRow),
            "page": currentPage,
            "page_size": safePageSize,
            "total_pages": totalPages,
            "total_filtered_issues": totalFilteredIssues,
            "total_all_issues": totalAllIssues,
        }


def listFilteredSnapshotIssuesForProjectByDate(
    projectRedmineId: int,
    capturedForDate: str | None,
    filters: dict[str, object] | None = None,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        latestRun = _getSnapshotRunWithConnection(connection, projectRedmineId, capturedForDate)
        availableDates = _listSnapshotDatesForProjectWithConnection(connection, projectRedmineId)
        if latestRun is None:
            return {"snapshot_run": None, "issues": [], "available_dates": availableDates}

        whereClauses, filterParams, bindParams = _buildSnapshotIssueFilterParts(filters)
        baseWhereSql = " AND ".join(["snapshot_run_id = :snapshot_run_id", *whereClauses])
        params = {"snapshot_run_id": latestRun["id"], **filterParams}
        issuesStatement = text(_buildSnapshotIssueHierarchyQuery(baseWhereSql, paginated=False)).bindparams(*bindParams)
        issueRows = connection.execute(issuesStatement, params).mappings().all()
        return {
            "snapshot_run": latestRun,
            "issues": [dict(row) for row in issueRows],
            "available_dates": availableDates,
        }


def getSnapshotTimeEntriesForProjectByDateRange(
    projectRedmineId: int,
    capturedForDate: str | None,
    dateFrom: str,
    dateTo: str,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        latestRun = _getSnapshotRunWithConnection(connection, projectRedmineId, capturedForDate)
        availableDates = _listSnapshotDatesForProjectWithConnection(connection, projectRedmineId)
        if latestRun is None:
            return {
                "snapshot_run": None,
                "time_entries": [],
                "available_dates": availableDates,
            }

        timeEntryRows = connection.execute(
            text(
                """
                SELECT
                    id,
                    snapshot_run_id,
                    project_redmine_id,
                    project_name,
                    time_entry_redmine_id,
                    issue_redmine_id,
                    issue_subject,
                    issue_tracker_name,
                    issue_status_name,
                    user_id,
                    user_name,
                    activity_id,
                    activity_name,
                    hours,
                    comments,
                    spent_on,
                    created_on,
                    updated_on
                FROM issue_snapshot_time_entries
                WHERE snapshot_run_id = :snapshot_run_id
                  AND spent_on BETWEEN :date_from AND :date_to
                ORDER BY spent_on DESC, time_entry_redmine_id DESC
                """
            ),
            {
                "snapshot_run_id": latestRun["id"],
                "date_from": dateFrom,
                "date_to": dateTo,
            },
        ).mappings().all()

        return {
            "snapshot_run": latestRun,
            "time_entries": [dict(row) for row in timeEntryRows],
            "available_dates": availableDates,
        }


def getSnapshotRunsWithIssuesForProjectYear(projectRedmineId: int, year: int) -> dict[str, object]:
    return getSnapshotRunsWithIssuesForProjectDateRange(
        projectRedmineId,
        f"{year}-01-01",
        f"{year}-12-31",
    )


def getSnapshotRunsWithIssuesForProjectDateRange(
    projectRedmineId: int,
    dateFrom: str,
    dateTo: str,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

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
                    AND r.captured_for_date BETWEEN :date_from AND :date_to
                  ORDER BY r.captured_for_date ASC, r.captured_at ASC, r.id ASC
                  """
              ),
              {
                  "project_redmine_id": projectRedmineId,
                  "date_from": dateFrom,
                  "date_to": dateTo,
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
                    p.is_enabled,
                    p.partial_load,
                    p.is_disabled,
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


def createPlanningProject(project: dict[str, object]) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                INSERT INTO planning_projects (
                    direction,
                    project_name,
                    redmine_identifier,
                    pm_name,
                    customer,
                    start_date,
                    end_date,
                    development_hours,
                    year_1,
                    hours_1,
                    year_2,
                    hours_2,
                    year_3,
                    hours_3,
                    baseline_estimate_hours,
                    p1,
                    p2,
                    estimate_doc_url,
                    bitrix_url,
                    comment_text,
                    question_flag,
                    is_closed,
                    updated_at
                ) VALUES (
                    :direction,
                    :project_name,
                    :redmine_identifier,
                    :pm_name,
                    :customer,
                    :start_date,
                    :end_date,
                    :development_hours,
                    :year_1,
                    :hours_1,
                    :year_2,
                    :hours_2,
                    :year_3,
                    :hours_3,
                    :baseline_estimate_hours,
                    :p1,
                    :p2,
                    :estimate_doc_url,
                    :bitrix_url,
                    :comment_text,
                    :question_flag,
                    :is_closed,
                    CURRENT_TIMESTAMP
                )
                RETURNING
                    id,
                    direction,
                    project_name,
                    redmine_identifier,
                    pm_name,
                    customer,
                    start_date,
                    end_date,
                    development_hours,
                    year_1,
                    hours_1,
                    year_2,
                    hours_2,
                    year_3,
                    hours_3,
                    baseline_estimate_hours,
                    p1,
                    p2,
                    estimate_doc_url,
                    bitrix_url,
                    comment_text,
                    question_flag,
                    is_closed,
                    created_at,
                    updated_at
                """
            ),
            project,
        ).mappings().one()
        rowDict = dict(row)
        _insertPlanningProjectVersion(connection, rowDict, "create")
        return rowDict


def updatePlanningProject(projectId: int, project: dict[str, object]) -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                UPDATE planning_projects
                SET
                    direction = :direction,
                    project_name = :project_name,
                    redmine_identifier = :redmine_identifier,
                    pm_name = :pm_name,
                    customer = :customer,
                    start_date = :start_date,
                    end_date = :end_date,
                    development_hours = :development_hours,
                    year_1 = :year_1,
                    hours_1 = :hours_1,
                    year_2 = :year_2,
                    hours_2 = :hours_2,
                    year_3 = :year_3,
                    hours_3 = :hours_3,
                    baseline_estimate_hours = :baseline_estimate_hours,
                    p1 = :p1,
                    p2 = :p2,
                    estimate_doc_url = :estimate_doc_url,
                    bitrix_url = :bitrix_url,
                    comment_text = :comment_text,
                    question_flag = :question_flag,
                    is_closed = :is_closed,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :project_id
                  AND deleted_at IS NULL
                RETURNING
                    id,
                    direction,
                    project_name,
                    redmine_identifier,
                    pm_name,
                    customer,
                    start_date,
                    end_date,
                    development_hours,
                    year_1,
                    hours_1,
                    year_2,
                    hours_2,
                    year_3,
                    hours_3,
                    baseline_estimate_hours,
                    p1,
                    p2,
                    estimate_doc_url,
                    bitrix_url,
                    comment_text,
                    question_flag,
                    is_closed,
                    created_at,
                    updated_at,
                    deleted_at
                """
            ),
            {
                **project,
                "project_id": projectId,
            },
        ).mappings().first()
        if not row:
            return None
        rowDict = dict(row)
        _insertPlanningProjectVersion(connection, rowDict, "update")
        return rowDict


def deletePlanningProject(projectId: int) -> bool:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                UPDATE planning_projects
                SET
                    deleted_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :project_id
                  AND deleted_at IS NULL
                RETURNING
                    id,
                    direction,
                    project_name,
                    redmine_identifier,
                    pm_name,
                    customer,
                    start_date,
                    end_date,
                    development_hours,
                    year_1,
                    hours_1,
                    year_2,
                    hours_2,
                    year_3,
                    hours_3,
                    baseline_estimate_hours,
                    p1,
                    p2,
                    estimate_doc_url,
                    bitrix_url,
                    comment_text,
                    question_flag,
                    is_closed,
                    created_at,
                    updated_at,
                    deleted_at
                """
            ),
            {"project_id": projectId},
        ).mappings().first()
        if not row:
            return False
        _insertPlanningProjectVersion(connection, dict(row), "delete")
        return True


def createUser(user: dict[str, object]) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                INSERT INTO app_users (
                    login,
                    password_hash,
                    roles,
                    must_change_password
                )
                VALUES (
                    :login,
                    :password_hash,
                    :roles,
                    :must_change_password
                )
                RETURNING
                    id,
                    login,
                    roles,
                    must_change_password,
                    created_at,
                    updated_at
                """
            ),
            user,
        ).mappings().one()

    return dict(row)


def updateUser(userId: int, user: dict[str, object]) -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    updateFields = [
        "login = :login",
        "roles = :roles",
        "must_change_password = :must_change_password",
        "updated_at = CURRENT_TIMESTAMP",
    ]
    parameters = {
        "user_id": userId,
        "login": user.get("login"),
        "roles": user.get("roles"),
        "must_change_password": user.get("must_change_password"),
    }

    if user.get("password_hash"):
        updateFields.insert(1, "password_hash = :password_hash")
        parameters["password_hash"] = user.get("password_hash")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                f"""
                UPDATE app_users
                SET
                    {", ".join(updateFields)}
                WHERE id = :user_id
                RETURNING
                    id,
                    login,
                    roles,
                    must_change_password,
                    created_at,
                    updated_at
                """
            ),
            parameters,
        ).mappings().first()

    return dict(row) if row else None


def updateUserPassword(userId: int, passwordHash: str, mustChangePassword: bool) -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                UPDATE app_users
                SET
                    password_hash = :password_hash,
                    must_change_password = :must_change_password,
                    reset_password_token_hash = NULL,
                    reset_password_expires_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :user_id
                RETURNING
                    id,
                    login,
                    roles,
                    must_change_password,
                    created_at,
                    updated_at
                """
            ),
            {
                "user_id": userId,
                "password_hash": passwordHash,
                "must_change_password": mustChangePassword,
            },
        ).mappings().first()

    return dict(row) if row else None


def storeUserPasswordResetToken(userId: int, tokenHash: str, expiresAt) -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                UPDATE app_users
                SET
                    reset_password_token_hash = :token_hash,
                    reset_password_expires_at = :expires_at,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :user_id
                RETURNING
                    id,
                    login,
                    roles,
                    must_change_password,
                    reset_password_token_hash,
                    reset_password_expires_at,
                    created_at,
                    updated_at
                """
            ),
            {
                "user_id": userId,
                "token_hash": tokenHash,
                "expires_at": expiresAt,
            },
        ).mappings().first()

    return dict(row) if row else None


def getUserByPasswordResetToken(tokenHash: str, nowAt) -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    id,
                    login,
                    password_hash,
                    roles,
                    must_change_password,
                    reset_password_token_hash,
                    reset_password_expires_at,
                    created_at,
                    updated_at
                FROM app_users
                WHERE reset_password_token_hash = :token_hash
                  AND reset_password_expires_at IS NOT NULL
                  AND reset_password_expires_at >= :now_at
                """
            ),
            {
                "token_hash": tokenHash,
                "now_at": nowAt,
            },
        ).mappings().first()

    return dict(row) if row else None


def clearUserPasswordResetToken(userId: int) -> dict[str, object] | None:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                UPDATE app_users
                SET
                    reset_password_token_hash = NULL,
                    reset_password_expires_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :user_id
                RETURNING
                    id,
                    login,
                    roles,
                    must_change_password,
                    reset_password_token_hash,
                    reset_password_expires_at,
                    created_at,
                    updated_at
                """
            ),
            {"user_id": userId},
        ).mappings().first()

    return dict(row) if row else None


def deleteUser(userId: int) -> bool:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    with engine.begin() as connection:
        result = connection.execute(
            text(
                """
                DELETE FROM app_users
                WHERE id = :user_id
                """
            ),
            {"user_id": userId},
        )

    return bool(result.rowcount)


def createIssueSnapshotRun(
    capturedForDate: str,
    project: dict[str, object],
    issues: Sequence[dict[str, object]],
    timeEntries: Sequence[dict[str, object]] | None = None,
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
                    risk_estimate_hours,
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
                    :risk_estimate_hours,
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

        if timeEntries:
            issueMetaById: dict[int, dict[str, object]] = {}
            for issue in issues:
                issueRedmineId = int(issue.get("issue_redmine_id") or 0)
                if issueRedmineId <= 0:
                    continue
                issueMetaById[issueRedmineId] = {
                    "issue_subject": issue.get("subject"),
                    "issue_tracker_name": issue.get("tracker_name"),
                    "issue_status_name": issue.get("status_name"),
                }

            timeEntryInsertStatement = text(
                """
                INSERT INTO issue_snapshot_time_entries (
                    snapshot_run_id,
                    project_redmine_id,
                    project_name,
                    time_entry_redmine_id,
                    issue_redmine_id,
                    issue_subject,
                    issue_tracker_name,
                    issue_status_name,
                    user_id,
                    user_name,
                    activity_id,
                    activity_name,
                    hours,
                    comments,
                    spent_on,
                    created_on,
                    updated_on
                ) VALUES (
                    :snapshot_run_id,
                    :project_redmine_id,
                    :project_name,
                    :time_entry_redmine_id,
                    :issue_redmine_id,
                    :issue_subject,
                    :issue_tracker_name,
                    :issue_status_name,
                    :user_id,
                    :user_name,
                    :activity_id,
                    :activity_name,
                    :hours,
                    :comments,
                    :spent_on,
                    :created_on,
                    :updated_on
                )
                """
            )

            timeEntryPayload: list[dict[str, object]] = []
            for timeEntry in timeEntries:
                item = dict(timeEntry)
                item["snapshot_run_id"] = snapshotRunId
                issueRedmineId = int(item.get("issue_redmine_id") or 0)
                issueMeta = issueMetaById.get(issueRedmineId, {})
                item["project_name"] = item.get("project_name") or project.get("name")
                item["issue_subject"] = issueMeta.get("issue_subject")
                item["issue_tracker_name"] = issueMeta.get("issue_tracker_name")
                item["issue_status_name"] = issueMeta.get("issue_status_name")
                timeEntryPayload.append(item)

            for payloadChunk in chunkSequence(timeEntryPayload, SNAPSHOT_INSERT_BATCH_SIZE):
                connection.execute(timeEntryInsertStatement, payloadChunk)

    return snapshotRunId
