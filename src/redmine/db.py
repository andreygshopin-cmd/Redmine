from collections.abc import Mapping, Sequence
import json
from threading import Lock

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import sessionmaker

from src.redmine.config import loadConfig


config = loadConfig()
SNAPSHOT_INSERT_BATCH_SIZE = 200
BITRIX_INVOICE_PRODUCT_SQL = """
CASE
    WHEN LENGTH(COALESCE(kot_products, '')) >= LENGTH(COALESCE(products, ''))
     AND LENGTH(COALESCE(kot_products, '')) >= LENGTH(COALESCE(energy_products, ''))
        THEN NULLIF(kot_products, '')
    WHEN LENGTH(COALESCE(products, '')) >= LENGTH(COALESCE(energy_products, ''))
        THEN NULLIF(products, '')
    ELSE NULLIF(energy_products, '')
END
"""
BITRIX_INVOICE_STAGE_SQL = """
COALESCE(
    NULLIF(TRIM(SPLIT_PART(COALESCE(pipeline_stage_invoice, ''), '/', 2)), ''),
    NULLIF(status_name, ''),
    NULLIF(status_id, '')
)
"""
BITRIX_INVOICE_DEAL_TITLE_SQL = """
(
    SELECT deal_items.title
    FROM bitrix_deal_snapshot_items deal_items
    WHERE deal_items.deal_id = bitrix_crm_snapshot_items.deal_id
    ORDER BY deal_items.snapshot_run_id DESC, deal_items.id DESC
    LIMIT 1
)
"""


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
_bitrixDealSnapshotTablesEnsureLock = Lock()
_planningProjectsTableEnsureLock = Lock()
_usersTableEnsureLock = Lock()
_projectsTableEnsured = False
_issueSnapshotTablesEnsured = False
_bitrixDealSnapshotTablesEnsured = False
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


def ensureBitrixDealSnapshotTables() -> None:
    global _bitrixDealSnapshotTablesEnsured

    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    if _bitrixDealSnapshotTablesEnsured:
        return

    with _bitrixDealSnapshotTablesEnsureLock:
        if _bitrixDealSnapshotTablesEnsured:
            return

        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS bitrix_deal_snapshot_runs (
                        id BIGSERIAL PRIMARY KEY,
                        captured_for_date DATE NOT NULL DEFAULT CURRENT_DATE,
                        captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        total_deals INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS bitrix_deal_snapshot_items (
                        id BIGSERIAL PRIMARY KEY,
                        snapshot_run_id BIGINT NOT NULL REFERENCES bitrix_deal_snapshot_runs(id) ON DELETE CASCADE,
                        deal_id BIGINT NOT NULL,
                        title TEXT,
                        stage_id TEXT,
                        stage_name TEXT,
                        assigned_by_id BIGINT,
                        assigned_by_name TEXT,
                        opportunity DOUBLE PRECISION,
                        currency_id TEXT,
                        company_id BIGINT,
                        company_name TEXT,
                        category_id BIGINT,
                        category_name TEXT,
                        created_time TIMESTAMPTZ NULL,
                        updated_time TIMESTAMPTZ NULL,
                        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb
                    )
                    """
                )
            )
            connection.execute(text("ALTER TABLE bitrix_deal_snapshot_items ADD COLUMN IF NOT EXISTS stage_name TEXT"))
            connection.execute(text("ALTER TABLE bitrix_deal_snapshot_items ADD COLUMN IF NOT EXISTS category_name TEXT"))
            connection.execute(text("ALTER TABLE bitrix_deal_snapshot_items ADD COLUMN IF NOT EXISTS assigned_by_name TEXT"))
            connection.execute(text("ALTER TABLE bitrix_deal_snapshot_items ADD COLUMN IF NOT EXISTS company_id BIGINT"))
            connection.execute(text("ALTER TABLE bitrix_deal_snapshot_items ADD COLUMN IF NOT EXISTS company_name TEXT"))
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS bitrix_crm_snapshot_runs (
                        id BIGSERIAL PRIMARY KEY,
                        entity_type TEXT NOT NULL,
                        captured_for_date DATE NOT NULL DEFAULT CURRENT_DATE,
                        captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        total_items INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS bitrix_crm_snapshot_items (
                        id BIGSERIAL PRIMARY KEY,
                        snapshot_run_id BIGINT NOT NULL REFERENCES bitrix_crm_snapshot_runs(id) ON DELETE CASCADE,
                        entity_type TEXT NOT NULL,
                        item_id BIGINT NOT NULL,
                        title TEXT,
                        status_id TEXT,
                        status_name TEXT,
                        assigned_by_id BIGINT,
                        assigned_by_name TEXT,
                        opportunity DOUBLE PRECISION,
                        currency_id TEXT,
                        deal_id BIGINT,
                        company_id BIGINT,
                        company_name TEXT,
                        category_id BIGINT,
                        category_name TEXT,
                        begin_date DATE NULL,
                        close_date DATE NULL,
                        kot_products TEXT,
                        products TEXT,
                        energy_products TEXT,
                        stage_group TEXT,
                        pipeline_stage_invoice TEXT,
                        created_time TIMESTAMPTZ NULL,
                        updated_time TIMESTAMPTZ NULL,
                        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb
                    )
                    """
                )
            )
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS category_id BIGINT"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS category_name TEXT"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS deal_id BIGINT"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS begin_date DATE NULL"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS close_date DATE NULL"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS kot_products TEXT"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS products TEXT"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS energy_products TEXT"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS stage_group TEXT"))
            connection.execute(text("ALTER TABLE bitrix_crm_snapshot_items ADD COLUMN IF NOT EXISTS pipeline_stage_invoice TEXT"))

            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS bitrix_users (
                        bitrix_user_id BIGINT PRIMARY KEY,
                        display_name TEXT NOT NULL,
                        last_name TEXT,
                        first_name TEXT,
                        second_name TEXT,
                        login TEXT,
                        email TEXT,
                        active TEXT,
                        work_position TEXT,
                        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        synced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS bitrix_companies (
                        bitrix_company_id BIGINT PRIMARY KEY,
                        title TEXT NOT NULL,
                        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        synced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_bitrix_deal_snapshot_runs_date_unique
                    ON bitrix_deal_snapshot_runs(captured_for_date)
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_bitrix_deal_snapshot_items_run_deal_unique
                    ON bitrix_deal_snapshot_items(snapshot_run_id, deal_id)
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_bitrix_crm_snapshot_runs_type_date_unique
                    ON bitrix_crm_snapshot_runs(entity_type, captured_for_date)
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_bitrix_crm_snapshot_items_run_item_unique
                    ON bitrix_crm_snapshot_items(snapshot_run_id, item_id)
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_bitrix_crm_snapshot_items_run
                    ON bitrix_crm_snapshot_items(snapshot_run_id)
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_bitrix_deal_snapshot_items_run
                    ON bitrix_deal_snapshot_items(snapshot_run_id)
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_bitrix_deal_snapshot_items_stage
                    ON bitrix_deal_snapshot_items(snapshot_run_id, stage_id)
                    """
                )
            )

        _bitrixDealSnapshotTablesEnsured = True


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
                        next_deadline DATE NULL,
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
                        use_risk_plan BOOLEAN NOT NULL DEFAULT FALSE,
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
                        next_deadline DATE NULL,
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
                        use_risk_plan BOOLEAN NOT NULL DEFAULT FALSE,
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
                    ADD COLUMN IF NOT EXISTS next_deadline DATE NULL
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_project_versions
                    ADD COLUMN IF NOT EXISTS next_deadline DATE NULL
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
                    ADD COLUMN IF NOT EXISTS use_risk_plan BOOLEAN NOT NULL DEFAULT FALSE
                    """
                )
            )

            connection.execute(
                text(
                    """
                    ALTER TABLE planning_project_versions
                    ADD COLUMN IF NOT EXISTS use_risk_plan BOOLEAN NOT NULL DEFAULT FALSE
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
                        next_deadline,
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
                        use_risk_plan,
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
                        p.next_deadline,
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
                        p.use_risk_plan,
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
        "next_deadline": projectRow.get("next_deadline"),
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
        "use_risk_plan": bool(projectRow.get("use_risk_plan")),
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
                next_deadline,
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
                use_risk_plan,
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
                :next_deadline,
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
                :use_risk_plan,
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
                    OR CAST(next_deadline AS TEXT) LIKE :search_pattern
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
                    next_deadline,
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
                    use_risk_plan,
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
                    OR CAST(next_deadline AS TEXT) LIKE :search_pattern
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
                    next_deadline,
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
                    use_risk_plan,
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
                    next_deadline,
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
                    use_risk_plan,
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
        "volume": "volume",
        "risk_volume": "risk_volume",
        "remaining": "remaining",
        "risk_remaining": "risk_remaining",
        "spent": "spent",
        "spent_year": "spent_year",
    }
    for filterKey, paramPrefix in numericConfig.items():
        operator = normalizeText(payload.get(f"{paramPrefix}_op"))
        normalized[f"{paramPrefix}_op"] = operator if operator in {">", "<", "="} else ""
        normalized[f"{paramPrefix}_value"] = normalizeNumeric(payload.get(f"{paramPrefix}_value"))

    return normalized


def _snapshotIssueFieldSql(alias: str, fieldName: str) -> str:
    return f"{alias}.{fieldName}" if alias else fieldName


def _buildSnapshotIssueMetricsSql(alias: str = "") -> tuple[str, str, str, str]:
    trackerSql = f"LOWER(TRIM(COALESCE({_snapshotIssueFieldSql(alias, 'tracker_name')}, '')))"
    statusSql = f"LOWER(TRIM(COALESCE({_snapshotIssueFieldSql(alias, 'status_name')}, '')))"
    baselineSql = f"COALESCE({_snapshotIssueFieldSql(alias, 'baseline_estimate_hours')}, 0)"
    estimatedSql = f"COALESCE({_snapshotIssueFieldSql(alias, 'estimated_hours')}, 0)"
    riskEstimatedSql = f"COALESCE({_snapshotIssueFieldSql(alias, 'risk_estimate_hours')}, {_snapshotIssueFieldSql(alias, 'estimated_hours')}, 0)"
    spentSql = f"COALESCE({_snapshotIssueFieldSql(alias, 'spent_hours')}, 0)"
    closedStatusesSql = "('закрыта', 'решена', 'отказ')"

    volumeSql = f"""
        CASE
            WHEN {trackerSql} = 'разработка' THEN
                CASE
                    WHEN {statusSql} IN {closedStatusesSql} THEN {spentSql}
                    ELSE GREATEST({baselineSql}, {estimatedSql}, {spentSql})
                END
            WHEN {trackerSql} = 'процессы разработки' THEN
                GREATEST({estimatedSql}, {spentSql})
            WHEN {trackerSql} = 'ошибка' THEN
                CASE
                    WHEN {statusSql} IN {closedStatusesSql} THEN {spentSql}
                    ELSE GREATEST({estimatedSql}, {spentSql})
                END
            ELSE NULL
        END
    """.strip()

    riskVolumeSql = f"""
        CASE
            WHEN {trackerSql} = 'разработка' THEN
                CASE
                    WHEN {statusSql} IN {closedStatusesSql} THEN {spentSql}
                    ELSE GREATEST({baselineSql}, {riskEstimatedSql}, {spentSql})
                END
            WHEN {trackerSql} = 'процессы разработки' THEN
                GREATEST({riskEstimatedSql}, {spentSql})
            WHEN {trackerSql} = 'ошибка' THEN
                CASE
                    WHEN {statusSql} IN {closedStatusesSql} THEN {spentSql}
                    ELSE GREATEST({riskEstimatedSql}, {spentSql})
                END
            ELSE NULL
        END
    """.strip()

    remainingSql = f"""
        CASE
            WHEN {trackerSql} = 'разработка' THEN
                CASE
                    WHEN {statusSql} IN {closedStatusesSql} THEN 0
                    ELSE GREATEST(0, GREATEST({baselineSql}, {estimatedSql}) - {spentSql})
                END
            WHEN {trackerSql} = 'процессы разработки' THEN 0
            WHEN {trackerSql} = 'ошибка' THEN
                CASE
                    WHEN {statusSql} IN {closedStatusesSql} THEN 0
                    ELSE GREATEST(0, {estimatedSql} - {spentSql})
                END
            ELSE NULL
        END
    """.strip()

    riskRemainingSql = f"""
        CASE
            WHEN {trackerSql} = 'разработка' THEN
                CASE
                    WHEN {statusSql} IN {closedStatusesSql} THEN 0
                    ELSE GREATEST(0, GREATEST({baselineSql}, {riskEstimatedSql}) - {spentSql})
                END
            WHEN {trackerSql} = 'процессы разработки' THEN 0
            WHEN {trackerSql} = 'ошибка' THEN
                CASE
                    WHEN {statusSql} IN {closedStatusesSql} THEN 0
                    ELSE GREATEST(0, {riskEstimatedSql} - {spentSql})
                END
            ELSE NULL
        END
    """.strip()

    return volumeSql, riskVolumeSql, remainingSql, riskRemainingSql


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

    volumeSql, riskVolumeSql, remainingSql, riskRemainingSql = _buildSnapshotIssueMetricsSql()
    numericMappings = {
        "done_ratio": "COALESCE(done_ratio, 0)",
        "baseline": "COALESCE(baseline_estimate_hours, 0)",
        "estimated": "COALESCE(estimated_hours, 0)",
        "risk": "COALESCE(risk_estimate_hours, 0)",
        "volume": f"COALESCE(({volumeSql}), 0)",
        "risk_volume": f"COALESCE(({riskVolumeSql}), 0)",
        "remaining": f"COALESCE(({remainingSql}), 0)",
        "risk_remaining": f"COALESCE(({riskRemainingSql}), 0)",
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
    volumeSql, riskVolumeSql, remainingSql, riskRemainingSql = _buildSnapshotIssueMetricsSql("filtered_items")
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
                {volumeSql} AS volume_hours,
                {riskVolumeSql} AS risk_volume_hours,
                {remainingSql} AS remaining_hours,
                {riskRemainingSql} AS risk_remaining_hours,
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
                    risk_estimate_hours,
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
                    next_deadline,
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
                    use_risk_plan,
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
                    :next_deadline,
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
                    :use_risk_plan,
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
                    next_deadline,
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
                    use_risk_plan,
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
                    next_deadline = :next_deadline,
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
                    use_risk_plan = :use_risk_plan,
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
                    next_deadline,
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
                    use_risk_plan,
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
                    next_deadline,
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
                    use_risk_plan,
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
                ON CONFLICT (snapshot_run_id, time_entry_redmine_id) DO UPDATE SET
                    project_redmine_id = EXCLUDED.project_redmine_id,
                    project_name = EXCLUDED.project_name,
                    issue_redmine_id = EXCLUDED.issue_redmine_id,
                    issue_subject = EXCLUDED.issue_subject,
                    issue_tracker_name = EXCLUDED.issue_tracker_name,
                    issue_status_name = EXCLUDED.issue_status_name,
                    user_id = EXCLUDED.user_id,
                    user_name = EXCLUDED.user_name,
                    activity_id = EXCLUDED.activity_id,
                    activity_name = EXCLUDED.activity_name,
                    hours = EXCLUDED.hours,
                    comments = EXCLUDED.comments,
                    spent_on = EXCLUDED.spent_on,
                    created_on = EXCLUDED.created_on,
                    updated_on = EXCLUDED.updated_on
                """
            )

            timeEntryPayloadById: dict[int, dict[str, object]] = {}
            for timeEntry in timeEntries:
                item = dict(timeEntry)
                timeEntryRedmineId = int(item.get("time_entry_redmine_id") or 0)
                if timeEntryRedmineId <= 0:
                    continue
                item["snapshot_run_id"] = snapshotRunId
                issueRedmineId = int(item.get("issue_redmine_id") or 0)
                issueMeta = issueMetaById.get(issueRedmineId, {})
                item["project_name"] = item.get("project_name") or project.get("name")
                item["issue_subject"] = issueMeta.get("issue_subject")
                item["issue_tracker_name"] = issueMeta.get("issue_tracker_name")
                item["issue_status_name"] = issueMeta.get("issue_status_name")
                timeEntryPayloadById[timeEntryRedmineId] = item

            timeEntryPayload = list(timeEntryPayloadById.values())

            for payloadChunk in chunkSequence(timeEntryPayload, SNAPSHOT_INSERT_BATCH_SIZE):
                connection.execute(timeEntryInsertStatement, payloadChunk)

    return snapshotRunId


def _toIntOrNone(value: object) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _toFloatOrNone(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _firstNonEmptyValue(*values: object) -> object | None:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def humanizeBitrixCrmStatusId(statusId: object) -> str | None:
    statusText = str(statusId or "").strip()
    if not statusText:
        return None

    if statusText.startswith("DT31_") and ":" in statusText:
        categoryPart, stageCode = statusText.split(":", 1)
        categoryId = categoryPart.replace("DT31_", "", 1)
        stageNames = {
            "N": "Новый",
            "P": "В работе",
            "S": "Успешно завершен",
            "F": "Провален",
        }
        readableStage = stageNames.get(stageCode, stageCode)
        if categoryId:
            return f"Воронка {categoryId}: {readableStage}"
        return readableStage

    return None


def _formatBitrixCustomFieldValue(value: object, valueMap: Mapping[object, str] | None = None) -> str | None:
    if value is None or value == "":
        return None

    resolvedValueMap = valueMap or {}
    if isinstance(value, (list, tuple, set)):
        parts = [
            formatted
            for formatted in (_formatBitrixCustomFieldValue(item, resolvedValueMap) for item in value)
            if formatted
        ]
        return ", ".join(parts) if parts else None

    if isinstance(value, dict):
        for key in ("VALUE", "value", "NAME", "name", "TITLE", "title", "TEXT", "text", "ID", "id"):
            if key in value and value.get(key) not in (None, ""):
                return _formatBitrixCustomFieldValue(value.get(key), resolvedValueMap)
        return json.dumps(value, ensure_ascii=False)

    for key in (value, str(value)):
        if key in resolvedValueMap:
            return resolvedValueMap[key]
    return str(value)


def _readBitrixInvoiceExtraField(
    item: Mapping[str, object],
    fieldKey: str,
    fieldNames: Mapping[object, object],
    valueMaps: Mapping[object, Mapping[object, str]],
) -> str | None:
    fieldName = fieldNames.get(fieldKey)
    if not isinstance(fieldName, str) or not fieldName:
        return None

    valueMap = valueMaps.get(fieldKey) or {}
    return _formatBitrixCustomFieldValue(item.get(fieldName), valueMap)


def createBitrixDealSnapshot(
    deals: Sequence[dict[str, object]],
    capturedForDate: str,
    dictionaries: Mapping[str, dict[object, str]] | None = None,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    normalizedDeals: list[dict[str, object]] = []
    dictionaryValues = dictionaries or {}
    stageNames = dictionaryValues.get("stage_names") or {}
    categoryNames = dictionaryValues.get("category_names") or {}
    assignedByNames = dictionaryValues.get("assigned_by_names") or {}
    companyNames = dictionaryValues.get("company_names") or {}
    for deal in deals:
        dealId = _toIntOrNone(deal.get("id"))
        if dealId is None:
            continue
        categoryId = _toIntOrNone(deal.get("categoryId"))
        stageId = str(deal.get("stageId") or "")
        assignedById = _toIntOrNone(deal.get("assignedById"))
        companyId = _toIntOrNone(deal.get("companyId"))
        normalizedDeals.append(
            {
                "deal_id": dealId,
                "title": deal.get("title"),
                "stage_id": stageId,
                "stage_name": stageNames.get(f"{categoryId or 0}:{stageId}") or stageNames.get(stageId),
                "assigned_by_id": assignedById,
                "assigned_by_name": assignedByNames.get(assignedById or 0),
                "opportunity": _toFloatOrNone(deal.get("opportunity")),
                "currency_id": deal.get("currencyId"),
                "company_id": companyId,
                "company_name": companyNames.get(companyId or 0),
                "category_id": categoryId,
                "category_name": categoryNames.get(categoryId or 0),
                "created_time": deal.get("createdTime"),
                "updated_time": deal.get("updatedTime"),
                "raw_payload": json.dumps(deal, ensure_ascii=False),
            }
        )

    with engine.begin() as connection:
        existingRunId = connection.execute(
            text(
                """
                SELECT id
                FROM bitrix_deal_snapshot_runs
                WHERE captured_for_date = CAST(:captured_for_date AS DATE)
                """
            ),
            {"captured_for_date": capturedForDate},
        ).scalar_one_or_none()

        if existingRunId is not None:
            snapshotRunId = int(existingRunId)
            connection.execute(
                text(
                    """
                    DELETE FROM bitrix_deal_snapshot_items
                    WHERE snapshot_run_id = :snapshot_run_id
                    """
                ),
                {"snapshot_run_id": snapshotRunId},
            )
            connection.execute(
                text(
                    """
                    UPDATE bitrix_deal_snapshot_runs
                    SET captured_at = CURRENT_TIMESTAMP,
                        total_deals = :total_deals
                    WHERE id = :snapshot_run_id
                    """
                ),
                {"snapshot_run_id": snapshotRunId, "total_deals": len(normalizedDeals)},
            )
        else:
            snapshotRunId = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO bitrix_deal_snapshot_runs (captured_for_date, total_deals)
                        VALUES (CAST(:captured_for_date AS DATE), :total_deals)
                        RETURNING id
                        """
                    ),
                    {"captured_for_date": capturedForDate, "total_deals": len(normalizedDeals)},
                ).scalar_one()
            )

        insertStatement = text(
            """
            INSERT INTO bitrix_deal_snapshot_items (
                snapshot_run_id, deal_id, title, stage_id, stage_name, assigned_by_id, assigned_by_name, opportunity,
                currency_id, company_id, company_name, category_id, category_name, created_time, updated_time, raw_payload
            ) VALUES (
                :snapshot_run_id, :deal_id, :title, :stage_id, :stage_name, :assigned_by_id, :assigned_by_name, :opportunity,
                :currency_id, :company_id, :company_name, :category_id, :category_name, :created_time, :updated_time, CAST(:raw_payload AS JSONB)
            )
            """
        )
        insertPayload = [{**item, "snapshot_run_id": snapshotRunId} for item in normalizedDeals]
        for payloadChunk in chunkSequence(insertPayload, SNAPSHOT_INSERT_BATCH_SIZE):
            connection.execute(insertStatement, payloadChunk)

    return {"snapshot_run_id": snapshotRunId, "captured_for_date": capturedForDate, "total_deals": len(normalizedDeals)}


def deleteBitrixDealSnapshotForDate(capturedForDate: str) -> dict[str, int | str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    with engine.begin() as connection:
        runIds = [
            int(row.id)
            for row in connection.execute(
                text(
                    """
                    SELECT id
                    FROM bitrix_deal_snapshot_runs
                    WHERE captured_for_date = CAST(:captured_for_date AS DATE)
                    """
                ),
                {"captured_for_date": capturedForDate},
            )
        ]

        if not runIds:
            return {
                "captured_for_date": capturedForDate,
                "deleted_items": 0,
                "deleted_runs": 0,
            }

        itemCount = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM bitrix_deal_snapshot_items
                    WHERE snapshot_run_id IN :run_ids
                    """
                ).bindparams(bindparam("run_ids", expanding=True)),
                {"run_ids": runIds},
            ).scalar_one()
        )

        connection.execute(
            text(
                """
                DELETE FROM bitrix_deal_snapshot_runs
                WHERE id IN :run_ids
                """
            ).bindparams(bindparam("run_ids", expanding=True)),
            {"run_ids": runIds},
        )

    return {
        "captured_for_date": capturedForDate,
        "deleted_items": itemCount,
        "deleted_runs": len(runIds),
    }


def listBitrixDealSnapshotRuns(limit: int = 50) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    safeLimit = max(1, min(int(limit or 50), 500))
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT id, captured_for_date, captured_at, total_deals
                FROM bitrix_deal_snapshot_runs
                ORDER BY captured_for_date DESC, captured_at DESC, id DESC
                LIMIT :limit_value
                """
            ),
            {"limit_value": safeLimit},
        )
        return [dict(row._mapping) for row in rows]


def listBitrixDealSnapshotDates() -> list[str]:
    return [str(row["captured_for_date"]) for row in listBitrixDealSnapshotRuns(500)]


def upsertBitrixUsers(users: Sequence[dict[str, object]]) -> dict[str, int]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    normalizedUsers: list[dict[str, object]] = []
    for user in users:
        userId = _toIntOrNone(user.get("id") or user.get("ID"))
        displayName = str(user.get("name") or user.get("display_name") or "").strip()
        if userId is None or not displayName:
            continue
        normalizedUsers.append(
            {
                "bitrix_user_id": userId,
                "display_name": displayName,
                "last_name": user.get("last_name") or user.get("LAST_NAME"),
                "first_name": user.get("first_name") or user.get("NAME"),
                "second_name": user.get("second_name") or user.get("SECOND_NAME"),
                "login": user.get("login") or user.get("LOGIN"),
                "email": user.get("email") or user.get("EMAIL"),
                "active": str(user.get("active") if user.get("active") is not None else user.get("ACTIVE") or ""),
                "work_position": user.get("work_position") or user.get("WORK_POSITION"),
                "raw_payload": json.dumps(user, ensure_ascii=False),
            }
        )

    if not normalizedUsers:
        return {"upserted": 0}

    statement = text(
        """
        INSERT INTO bitrix_users (
            bitrix_user_id, display_name, last_name, first_name, second_name,
            login, email, active, work_position, raw_payload
        ) VALUES (
            :bitrix_user_id, :display_name, :last_name, :first_name, :second_name,
            :login, :email, :active, :work_position, CAST(:raw_payload AS JSONB)
        )
        ON CONFLICT (bitrix_user_id) DO UPDATE
        SET display_name = EXCLUDED.display_name,
            last_name = EXCLUDED.last_name,
            first_name = EXCLUDED.first_name,
            second_name = EXCLUDED.second_name,
            login = EXCLUDED.login,
            email = EXCLUDED.email,
            active = EXCLUDED.active,
            work_position = EXCLUDED.work_position,
            raw_payload = EXCLUDED.raw_payload,
            synced_at = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as connection:
        for payloadChunk in chunkSequence(normalizedUsers, SNAPSHOT_INSERT_BATCH_SIZE):
            connection.execute(statement, payloadChunk)

    return {"upserted": len(normalizedUsers)}


def getBitrixUserNamesByIds(userIds: Sequence[int]) -> dict[object, str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    normalizedIdSet: set[int] = set()
    for value in userIds:
        try:
            normalizedId = int(value or 0)
        except (TypeError, ValueError):
            continue
        if normalizedId > 0:
            normalizedIdSet.add(normalizedId)
    normalizedIds = sorted(normalizedIdSet)
    if not normalizedIds:
        return {}

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT bitrix_user_id, display_name
                FROM bitrix_users
                WHERE bitrix_user_id IN :user_ids
                """
            ).bindparams(bindparam("user_ids", expanding=True)),
            {"user_ids": normalizedIds},
        )
        return {int(row.bitrix_user_id): str(row.display_name) for row in rows}


def upsertBitrixCompanies(companies: Sequence[dict[str, object]]) -> dict[str, int]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    normalizedCompanies: list[dict[str, object]] = []
    for company in companies:
        companyId = _toIntOrNone(company.get("id") or company.get("ID"))
        title = str(company.get("title") or company.get("TITLE") or "").strip()
        if companyId is None or not title:
            continue
        normalizedCompanies.append(
            {
                "bitrix_company_id": companyId,
                "title": title,
                "raw_payload": json.dumps(company, ensure_ascii=False),
            }
        )

    if not normalizedCompanies:
        return {"upserted": 0}

    statement = text(
        """
        INSERT INTO bitrix_companies (
            bitrix_company_id, title, raw_payload
        ) VALUES (
            :bitrix_company_id, :title, CAST(:raw_payload AS JSONB)
        )
        ON CONFLICT (bitrix_company_id) DO UPDATE
        SET title = EXCLUDED.title,
            raw_payload = EXCLUDED.raw_payload,
            synced_at = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as connection:
        for payloadChunk in chunkSequence(normalizedCompanies, SNAPSHOT_INSERT_BATCH_SIZE):
            connection.execute(statement, payloadChunk)

    return {"upserted": len(normalizedCompanies)}


def getBitrixCompanyNamesByIds(companyIds: Sequence[int]) -> dict[object, str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    normalizedIdSet: set[int] = set()
    for value in companyIds:
        try:
            normalizedId = int(value or 0)
        except (TypeError, ValueError):
            continue
        if normalizedId > 0:
            normalizedIdSet.add(normalizedId)
    normalizedIds = sorted(normalizedIdSet)
    if not normalizedIds:
        return {}

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT bitrix_company_id, title
                FROM bitrix_companies
                WHERE bitrix_company_id IN :company_ids
                """
            ).bindparams(bindparam("company_ids", expanding=True)),
            {"company_ids": normalizedIds},
        )
        return {int(row.bitrix_company_id): str(row.title) for row in rows}


def getBitrixDealSnapshotItems(
    capturedForDate: str | None = None,
    *,
    page: int = 1,
    pageSize: int = 1000,
    filters: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    safePage = max(1, int(page or 1))
    safePageSize = max(1, min(int(pageSize or 1000), 5000))
    params: dict[str, object] = {"limit_value": safePageSize, "offset_value": (safePage - 1) * safePageSize}
    whereClauses: list[str] = []
    filterValues = dict(filters or {})

    with engine.connect() as connection:
        if capturedForDate:
            params["captured_for_date"] = capturedForDate
            run = connection.execute(
                text(
                    """
                    SELECT id, captured_for_date, captured_at, total_deals
                    FROM bitrix_deal_snapshot_runs
                    WHERE captured_for_date = CAST(:captured_for_date AS DATE)
                    ORDER BY captured_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()
        else:
            run = connection.execute(
                text(
                    """
                    SELECT id, captured_for_date, captured_at, total_deals
                    FROM bitrix_deal_snapshot_runs
                    ORDER BY captured_for_date DESC, captured_at DESC, id DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()

        if run is None:
            return {
                "snapshot_run": None,
                "deals": [],
                "available_dates": listBitrixDealSnapshotDates(),
                "page": safePage,
                "page_size": safePageSize,
                "total_count": 0,
            }

        params["snapshot_run_id"] = int(run["id"])
        for fieldName, columnName in {
            "deal_id": "CAST(deal_id AS TEXT)",
            "title": "title",
            "stage_id": "stage_id",
            "stage_name": "COALESCE(stage_name, stage_id)",
            "assigned_by_id": "CAST(assigned_by_id AS TEXT)",
            "assigned_by_name": "COALESCE(assigned_by_name, CAST(assigned_by_id AS TEXT))",
            "opportunity": "CAST(opportunity AS TEXT)",
            "currency_id": "currency_id",
            "company_id": "CAST(company_id AS TEXT)",
            "company_name": "COALESCE(company_name, CAST(company_id AS TEXT))",
            "category_id": "CAST(category_id AS TEXT)",
            "category_name": "COALESCE(category_name, CAST(category_id AS TEXT))",
            "created_time": "CAST(created_time AS TEXT)",
            "updated_time": "CAST(updated_time AS TEXT)",
        }.items():
            value = str(filterValues.get(fieldName) or "").strip()
            if value:
                paramName = f"filter_{fieldName}"
                whereClauses.append(f"LOWER(COALESCE({columnName}, '')) LIKE :{paramName}")
                params[paramName] = f"%{value.lower()}%"

        whereSql = " AND ".join(["snapshot_run_id = :snapshot_run_id", *whereClauses])
        totalCount = int(
            connection.execute(
                text(f"SELECT COUNT(*) FROM bitrix_deal_snapshot_items WHERE {whereSql}"),
                params,
            ).scalar_one()
        )
        rows = connection.execute(
            text(
                f"""
                SELECT id, snapshot_run_id, deal_id, title, stage_id, stage_name, assigned_by_id, assigned_by_name,
                       opportunity, currency_id, company_id, company_name, category_id, category_name, created_time, updated_time
                FROM bitrix_deal_snapshot_items
                WHERE {whereSql}
                ORDER BY deal_id DESC
                LIMIT :limit_value OFFSET :offset_value
                """
            ),
            params,
        )
        return {
            "snapshot_run": dict(run),
            "deals": [dict(row._mapping) for row in rows],
            "available_dates": listBitrixDealSnapshotDates(),
            "page": safePage,
            "page_size": safePageSize,
            "total_count": totalCount,
        }


def getBitrixDealSnapshotFilterOptions(capturedForDate: str | None = None) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    with engine.connect() as connection:
        if capturedForDate:
            run = connection.execute(
                text(
                    """
                    SELECT id, captured_for_date, captured_at, total_deals
                    FROM bitrix_deal_snapshot_runs
                    WHERE captured_for_date = CAST(:captured_for_date AS DATE)
                    ORDER BY captured_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                {"captured_for_date": capturedForDate},
            ).mappings().first()
        else:
            run = connection.execute(
                text(
                    """
                    SELECT id, captured_for_date, captured_at, total_deals
                    FROM bitrix_deal_snapshot_runs
                    ORDER BY captured_for_date DESC, captured_at DESC, id DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()

        if run is None:
            return {"snapshot_run": None, "options": {}}

        fieldExpressions = {
            "stage_name": "COALESCE(stage_name, stage_id)",
            "assigned_by_name": "COALESCE(assigned_by_name, CAST(assigned_by_id AS TEXT))",
            "category_name": "COALESCE(category_name, CAST(category_id AS TEXT))",
        }
        options: dict[str, list[str]] = {}
        for fieldName, expression in fieldExpressions.items():
            rows = connection.execute(
                text(
                    f"""
                    SELECT DISTINCT value
                    FROM (
                        SELECT NULLIF(TRIM({expression}), '') AS value
                        FROM bitrix_deal_snapshot_items
                        WHERE snapshot_run_id = :snapshot_run_id
                    ) values_source
                    WHERE value IS NOT NULL
                    ORDER BY value
                    LIMIT 1000
                    """
                ),
                {"snapshot_run_id": int(run["id"])},
            )
            options[fieldName] = [str(row.value) for row in rows]

        return {"snapshot_run": dict(run), "options": options}


def _normalizeBitrixCrmSnapshotItems(
    entityType: str,
    items: Sequence[dict[str, object]],
    dictionaries: Mapping[str, dict[object, str]] | None = None,
) -> list[dict[str, object]]:
    dictionaryValues = dictionaries or {}
    statusNames = dictionaryValues.get("status_names") or {}
    categoryNames = dictionaryValues.get("category_names") or {}
    assignedByNames = dictionaryValues.get("assigned_by_names") or {}
    companyNames = dictionaryValues.get("company_names") or {}
    invoiceExtraFieldNames = dictionaryValues.get("invoice_extra_field_names") or {}
    invoiceExtraFieldValueMaps = dictionaryValues.get("invoice_extra_field_value_maps") or {}
    normalizedItems: list[dict[str, object]] = []
    for item in items:
        itemId = _toIntOrNone(item.get("id") or item.get("ID"))
        if itemId is None:
            continue
        statusId = str(item.get("statusId") or item.get("stageId") or item.get("STATUS_ID") or item.get("STAGE_ID") or "")
        assignedById = _toIntOrNone(item.get("assignedById") or item.get("ASSIGNED_BY_ID") or item.get("RESPONSIBLE_ID"))
        dealId = _toIntOrNone(item.get("parentId2") or item.get("PARENT_ID_2") or item.get("dealId") or item.get("DEAL_ID"))
        companyId = _toIntOrNone(item.get("companyId") or item.get("COMPANY_ID") or item.get("UF_COMPANY_ID"))
        categoryId = _toIntOrNone(item.get("categoryId") or item.get("CATEGORY_ID"))
        normalizedItems.append(
            {
                "entity_type": entityType,
                "item_id": itemId,
                "title": item.get("title") or item.get("TITLE") or item.get("ORDER_TOPIC") or item.get("ACCOUNT_NUMBER"),
                "status_id": statusId,
                "status_name": statusNames.get(statusId) or humanizeBitrixCrmStatusId(statusId),
                "assigned_by_id": assignedById,
                "assigned_by_name": assignedByNames.get(assignedById or 0),
                "opportunity": _toFloatOrNone(item.get("opportunity") or item.get("OPPORTUNITY") or item.get("PRICE")),
                "currency_id": item.get("currencyId") or item.get("CURRENCY_ID") or item.get("CURRENCY"),
                "deal_id": dealId,
                "company_id": companyId,
                "company_name": companyNames.get(companyId or 0),
                "category_id": categoryId,
                "category_name": categoryNames.get(categoryId or 0),
                "begin_date": _firstNonEmptyValue(item.get("begindate"), item.get("BEGINDATE"), item.get("dateBill"), item.get("DATE_BILL")),
                "close_date": _firstNonEmptyValue(item.get("closedate"), item.get("CLOSEDATE"), item.get("datePayBefore"), item.get("DATE_PAY_BEFORE")),
                "kot_products": _readBitrixInvoiceExtraField(item, "kot_products", invoiceExtraFieldNames, invoiceExtraFieldValueMaps),
                "products": _readBitrixInvoiceExtraField(item, "products", invoiceExtraFieldNames, invoiceExtraFieldValueMaps),
                "energy_products": _readBitrixInvoiceExtraField(item, "energy_products", invoiceExtraFieldNames, invoiceExtraFieldValueMaps),
                "stage_group": _readBitrixInvoiceExtraField(item, "stage_group", invoiceExtraFieldNames, invoiceExtraFieldValueMaps),
                "pipeline_stage_invoice": _readBitrixInvoiceExtraField(
                    item,
                    "pipeline_stage_invoice",
                    invoiceExtraFieldNames,
                    invoiceExtraFieldValueMaps,
                ),
                "created_time": item.get("createdTime") or item.get("DATE_CREATE") or item.get("DATE_INSERT"),
                "updated_time": item.get("updatedTime") or item.get("DATE_MODIFY") or item.get("DATE_UPDATE"),
                "raw_payload": json.dumps(item, ensure_ascii=False),
            }
        )
    return normalizedItems


def createBitrixCrmSnapshot(
    entityType: str,
    items: Sequence[dict[str, object]],
    capturedForDate: str,
    dictionaries: Mapping[str, dict[object, str]] | None = None,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    normalizedItems = _normalizeBitrixCrmSnapshotItems(entityType, items, dictionaries)
    with engine.begin() as connection:
        existingRunId = connection.execute(
            text(
                """
                SELECT id
                FROM bitrix_crm_snapshot_runs
                WHERE entity_type = :entity_type
                  AND captured_for_date = CAST(:captured_for_date AS DATE)
                """
            ),
            {"entity_type": entityType, "captured_for_date": capturedForDate},
        ).scalar_one_or_none()

        if existingRunId is not None:
            snapshotRunId = int(existingRunId)
            connection.execute(
                text("DELETE FROM bitrix_crm_snapshot_items WHERE snapshot_run_id = :snapshot_run_id"),
                {"snapshot_run_id": snapshotRunId},
            )
            connection.execute(
                text(
                    """
                    UPDATE bitrix_crm_snapshot_runs
                    SET captured_at = CURRENT_TIMESTAMP,
                        total_items = :total_items
                    WHERE id = :snapshot_run_id
                    """
                ),
                {"snapshot_run_id": snapshotRunId, "total_items": len(normalizedItems)},
            )
        else:
            snapshotRunId = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO bitrix_crm_snapshot_runs (entity_type, captured_for_date, total_items)
                        VALUES (:entity_type, CAST(:captured_for_date AS DATE), :total_items)
                        RETURNING id
                        """
                    ),
                    {"entity_type": entityType, "captured_for_date": capturedForDate, "total_items": len(normalizedItems)},
                ).scalar_one()
            )

        insertStatement = text(
            """
            INSERT INTO bitrix_crm_snapshot_items (
                snapshot_run_id, entity_type, item_id, title, status_id, status_name,
                assigned_by_id, assigned_by_name, opportunity, currency_id, deal_id, company_id,
                company_name, category_id, category_name, begin_date, close_date,
                kot_products, products, energy_products, stage_group, pipeline_stage_invoice,
                created_time, updated_time, raw_payload
            ) VALUES (
                :snapshot_run_id, :entity_type, :item_id, :title, :status_id, :status_name,
                :assigned_by_id, :assigned_by_name, :opportunity, :currency_id, :deal_id, :company_id,
                :company_name, :category_id, :category_name, :begin_date, :close_date,
                :kot_products, :products, :energy_products, :stage_group, :pipeline_stage_invoice,
                :created_time, :updated_time, CAST(:raw_payload AS JSONB)
            )
            """
        )
        insertPayload = [{**item, "snapshot_run_id": snapshotRunId} for item in normalizedItems]
        for payloadChunk in chunkSequence(insertPayload, SNAPSHOT_INSERT_BATCH_SIZE):
            connection.execute(insertStatement, payloadChunk)

    return {
        "snapshot_run_id": snapshotRunId,
        "captured_for_date": capturedForDate,
        "entity_type": entityType,
        "total_items": len(normalizedItems),
    }


def deleteBitrixCrmSnapshotForDate(entityType: str, capturedForDate: str) -> dict[str, int | str]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    with engine.begin() as connection:
        runIds = [
            int(row.id)
            for row in connection.execute(
                text(
                    """
                    SELECT id
                    FROM bitrix_crm_snapshot_runs
                    WHERE entity_type = :entity_type
                      AND captured_for_date = CAST(:captured_for_date AS DATE)
                    """
                ),
                {"entity_type": entityType, "captured_for_date": capturedForDate},
            )
        ]
        if not runIds:
            return {"entity_type": entityType, "captured_for_date": capturedForDate, "deleted_items": 0, "deleted_runs": 0}
        itemCount = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM bitrix_crm_snapshot_items
                    WHERE snapshot_run_id IN :run_ids
                    """
                ).bindparams(bindparam("run_ids", expanding=True)),
                {"run_ids": runIds},
            ).scalar_one()
        )
        connection.execute(
            text(
                """
                DELETE FROM bitrix_crm_snapshot_runs
                WHERE id IN :run_ids
                """
            ).bindparams(bindparam("run_ids", expanding=True)),
            {"run_ids": runIds},
        )

    return {"entity_type": entityType, "captured_for_date": capturedForDate, "deleted_items": itemCount, "deleted_runs": len(runIds)}


def listBitrixCrmSnapshotRuns(entityType: str, limit: int = 50) -> list[dict[str, object]]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    safeLimit = max(1, min(int(limit or 50), 500))
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT id, entity_type, captured_for_date, captured_at, total_items
                FROM bitrix_crm_snapshot_runs
                WHERE entity_type = :entity_type
                ORDER BY captured_for_date DESC, captured_at DESC, id DESC
                LIMIT :limit_value
                """
            ),
            {"entity_type": entityType, "limit_value": safeLimit},
        )
        return [dict(row._mapping) for row in rows]


def listBitrixCrmSnapshotDates(entityType: str) -> list[str]:
    return [str(row["captured_for_date"]) for row in listBitrixCrmSnapshotRuns(entityType, 500)]


def _getBitrixCrmSnapshotFilterOptions(
    connection: Connection,
    *,
    snapshotRunId: int,
    entityType: str,
) -> dict[str, list[str]]:
    optionExpressions = {
        "status_name": "COALESCE(status_name, status_id)",
        "assigned_by_name": "COALESCE(assigned_by_name, CAST(assigned_by_id AS TEXT))",
    }
    if entityType == "invoice":
        optionExpressions["pipeline_stage_invoice"] = "pipeline_stage_invoice"
        optionExpressions["invoice_stage"] = BITRIX_INVOICE_STAGE_SQL
        optionExpressions["kot_products"] = "kot_products"
        optionExpressions["products"] = "products"
        optionExpressions["energy_products"] = "energy_products"
        optionExpressions["product"] = BITRIX_INVOICE_PRODUCT_SQL

    options: dict[str, list[str]] = {}
    for fieldName, expression in optionExpressions.items():
        rows = connection.execute(
            text(
                f"""
                SELECT DISTINCT NULLIF(TRIM(COALESCE({expression}, '')), '') AS value
                FROM bitrix_crm_snapshot_items
                WHERE snapshot_run_id = :snapshot_run_id
                  AND entity_type = :entity_type
                ORDER BY value ASC
                LIMIT 5000
                """
            ),
            {"snapshot_run_id": snapshotRunId, "entity_type": entityType},
        )
        options[fieldName] = [
            str(row.value)
            for row in rows
            if row.value is not None and str(row.value).strip()
        ]
    return options


def getBitrixCrmSnapshotItems(
    entityType: str,
    capturedForDate: str | None = None,
    *,
    page: int = 1,
    pageSize: int = 1000,
    filters: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    safePage = max(1, int(page or 1))
    safePageSize = max(1, min(int(pageSize or 1000), 5000))
    params: dict[str, object] = {
        "entity_type": entityType,
        "limit_value": safePageSize,
        "offset_value": (safePage - 1) * safePageSize,
    }
    whereClauses: list[str] = []
    filterValues = dict(filters or {})

    with engine.connect() as connection:
        if capturedForDate:
            params["captured_for_date"] = capturedForDate
            run = connection.execute(
                text(
                    """
                    SELECT id, entity_type, captured_for_date, captured_at, total_items
                    FROM bitrix_crm_snapshot_runs
                    WHERE entity_type = :entity_type
                      AND captured_for_date = CAST(:captured_for_date AS DATE)
                    ORDER BY captured_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()
        else:
            run = connection.execute(
                text(
                    """
                    SELECT id, entity_type, captured_for_date, captured_at, total_items
                    FROM bitrix_crm_snapshot_runs
                    WHERE entity_type = :entity_type
                    ORDER BY captured_for_date DESC, captured_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()

        if run is None:
            return {
                "snapshot_run": None,
                "items": [],
                "available_dates": listBitrixCrmSnapshotDates(entityType),
                "filter_options": {},
                "page": safePage,
                "page_size": safePageSize,
                "total_count": 0,
            }

        params["snapshot_run_id"] = int(run["id"])
        filterOptions = _getBitrixCrmSnapshotFilterOptions(
            connection,
            snapshotRunId=int(run["id"]),
            entityType=entityType,
        )
        for fieldName, columnName in {
            "item_id": "CAST(item_id AS TEXT)",
            "title": "title",
            "status_id": "status_id",
            "status_name": "status_name",
            "assigned_by_id": "CAST(assigned_by_id AS TEXT)",
            "assigned_by_name": "assigned_by_name",
            "opportunity": "CAST(opportunity AS TEXT)",
            "company_id": "CAST(company_id AS TEXT)",
            "company_name": "company_name",
            "deal_id": f"CONCAT_WS(' ', CAST(deal_id AS TEXT), {BITRIX_INVOICE_DEAL_TITLE_SQL})",
            "category_id": "CAST(category_id AS TEXT)",
            "category_name": "category_name",
            "pipeline_stage_invoice": "pipeline_stage_invoice",
            "stage_group": "stage_group",
            "invoice_stage": BITRIX_INVOICE_STAGE_SQL,
            "begin_date": "CAST(begin_date AS TEXT)",
            "close_date": "CAST(close_date AS TEXT)",
            "kot_products": "kot_products",
            "products": "products",
            "energy_products": "energy_products",
            "product": BITRIX_INVOICE_PRODUCT_SQL,
            "created_time": "CAST(created_time AS TEXT)",
            "updated_time": "CAST(updated_time AS TEXT)",
        }.items():
            value = str(filterValues.get(fieldName) or "").strip()
            if value:
                paramName = f"filter_{fieldName}"
                whereClauses.append(f"LOWER(COALESCE({columnName}, '')) LIKE :{paramName}")
                params[paramName] = f"%{value.lower()}%"

        whereSql = " AND ".join(["snapshot_run_id = :snapshot_run_id", "entity_type = :entity_type", *whereClauses])
        totalCount = int(
            connection.execute(
                text(f"SELECT COUNT(*) FROM bitrix_crm_snapshot_items WHERE {whereSql}"),
                params,
            ).scalar_one()
        )
        rows = connection.execute(
            text(
                f"""
                SELECT id, snapshot_run_id, entity_type, item_id, title, status_id, status_name,
                       assigned_by_id, assigned_by_name, opportunity, currency_id, deal_id, company_id,
                       company_name, category_id, category_name, begin_date, close_date,
                       kot_products, products, energy_products, stage_group, pipeline_stage_invoice,
                       {BITRIX_INVOICE_STAGE_SQL} AS invoice_stage,
                       {BITRIX_INVOICE_DEAL_TITLE_SQL} AS deal_title,
                       {BITRIX_INVOICE_PRODUCT_SQL} AS product,
                       created_time, updated_time
                FROM bitrix_crm_snapshot_items
                WHERE {whereSql}
                ORDER BY item_id DESC
                LIMIT :limit_value OFFSET :offset_value
                """
            ),
            params,
        )

    return {
        "snapshot_run": dict(run),
        "items": [dict(row._mapping) for row in rows],
        "available_dates": listBitrixCrmSnapshotDates(entityType),
        "filter_options": filterOptions,
        "page": safePage,
        "page_size": safePageSize,
        "total_count": totalCount,
    }


def getBitrixInvoiceSummary(
    year: int,
    *,
    dateField: str = "begin_date",
    capturedForDate: str | None = None,
    pipelineStages: Sequence[str] | None = None,
) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    safeYear = int(year)
    if dateField not in {"begin_date", "close_date"}:
        raise ValueError("dateField must be begin_date or close_date")

    selectedPipelineStages = [
        str(value).strip()
        for value in pipelineStages or []
        if str(value).strip()
    ]
    params: dict[str, object] = {
        "entity_type": "invoice",
        "year_value": safeYear,
    }

    with engine.connect() as connection:
        if capturedForDate:
            params["captured_for_date"] = capturedForDate
            run = connection.execute(
                text(
                    """
                    SELECT id, entity_type, captured_for_date, captured_at, total_items
                    FROM bitrix_crm_snapshot_runs
                    WHERE entity_type = :entity_type
                      AND captured_for_date = CAST(:captured_for_date AS DATE)
                    ORDER BY captured_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()
        else:
            run = connection.execute(
                text(
                    """
                    SELECT id, entity_type, captured_for_date, captured_at, total_items
                    FROM bitrix_crm_snapshot_runs
                    WHERE entity_type = :entity_type
                    ORDER BY captured_for_date DESC, captured_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()

        if run is None:
            return {
                "snapshot_run": None,
                "year": safeYear,
                "date_field": dateField,
                "available_dates": listBitrixCrmSnapshotDates("invoice"),
                "pipeline_stage_options": [],
                "selected_pipeline_stages": selectedPipelineStages,
                "rows": [],
                "totals": {"months": {str(month): 0 for month in range(1, 13)}, "year_total": 0},
            }

        snapshotRunId = int(run["id"])
        filterOptions = _getBitrixCrmSnapshotFilterOptions(
            connection,
            snapshotRunId=snapshotRunId,
            entityType="invoice",
        )
        pipelineStageOptions = filterOptions.get("pipeline_stage_invoice") or []
        params["snapshot_run_id"] = snapshotRunId
        whereClauses = [
            "snapshot_run_id = :snapshot_run_id",
            "entity_type = :entity_type",
            f"{dateField} IS NOT NULL",
            f"EXTRACT(YEAR FROM {dateField}) = :year_value",
        ]
        bindParams = []
        if selectedPipelineStages:
            whereClauses.append("pipeline_stage_invoice IN :pipeline_stages")
            params["pipeline_stages"] = selectedPipelineStages
            bindParams.append(bindparam("pipeline_stages", expanding=True))

        whereSql = " AND ".join(whereClauses)
        monthExpression = f"CAST(EXTRACT(MONTH FROM {dateField}) AS INTEGER)"
        productExpression = f"COALESCE({BITRIX_INVOICE_PRODUCT_SQL}, '—')"
        dealTitleExpression = f"COALESCE({BITRIX_INVOICE_DEAL_TITLE_SQL}, CAST(deal_id AS TEXT), '—')"
        query = text(
            f"""
            SELECT
                {productExpression} AS product,
                deal_id,
                {dealTitleExpression} AS deal_title,
                {monthExpression} AS month_number,
                SUM(COALESCE(opportunity, 0)) AS amount
            FROM bitrix_crm_snapshot_items
            WHERE {whereSql}
            GROUP BY
                {productExpression},
                deal_id,
                {dealTitleExpression},
                {monthExpression}
            ORDER BY product ASC, deal_title ASC, deal_id ASC, month_number ASC
            """
        )
        if bindParams:
            query = query.bindparams(*bindParams)
        rows = [dict(row) for row in connection.execute(query, params).mappings()]

    summaryRowsByKey: dict[tuple[str, object, str], dict[str, object]] = {}
    totals = {"months": {str(month): 0.0 for month in range(1, 13)}, "year_total": 0.0}
    for row in rows:
        product = str(row["product"] or "—")
        dealId = row["deal_id"]
        dealTitle = str(row["deal_title"] or dealId or "—")
        key = (product, dealId, dealTitle)
        summaryRow = summaryRowsByKey.setdefault(
            key,
            {
                "product": product,
                "deal_id": dealId,
                "deal_title": dealTitle,
                "months": {str(month): 0.0 for month in range(1, 13)},
                "year_total": 0.0,
            },
        )
        monthNumber = int(row["month_number"] or 0)
        if monthNumber < 1 or monthNumber > 12:
            continue
        amount = float(row["amount"] or 0)
        monthKey = str(monthNumber)
        months = summaryRow["months"]
        if isinstance(months, dict):
            months[monthKey] = float(months.get(monthKey) or 0) + amount
        summaryRow["year_total"] = float(summaryRow["year_total"] or 0) + amount
        totals["months"][monthKey] = float(totals["months"].get(monthKey) or 0) + amount
        totals["year_total"] = float(totals["year_total"] or 0) + amount

    return {
        "snapshot_run": dict(run),
        "year": safeYear,
        "date_field": dateField,
        "available_dates": listBitrixCrmSnapshotDates("invoice"),
        "pipeline_stage_options": pipelineStageOptions,
        "selected_pipeline_stages": selectedPipelineStages,
        "rows": list(summaryRowsByKey.values()),
        "totals": totals,
    }


def compareBitrixDealSnapshots(leftDate: str | None = None, rightDate: str | None = None) -> dict[str, object]:
    if engine is None:
        raise RuntimeError("DATABASE_URL is not set")

    ensureBitrixDealSnapshotTables()
    runs = listBitrixDealSnapshotRuns(500)
    if not runs:
        return {"left_run": None, "right_run": None, "changes": [], "available_dates": []}

    def resolveRun(dateValue: str | None, fallbackIndex: int) -> dict[str, object] | None:
        if dateValue:
            for runItem in runs:
                if str(runItem.get("captured_for_date")) == str(dateValue):
                    return runItem
        if len(runs) > fallbackIndex:
            return runs[fallbackIndex]
        return None

    rightRun = resolveRun(rightDate, 0)
    leftRun = resolveRun(leftDate, 1 if len(runs) > 1 else 0)
    if leftRun is None or rightRun is None:
        return {
            "left_run": leftRun,
            "right_run": rightRun,
            "changes": [],
            "available_dates": [str(run["captured_for_date"]) for run in runs],
        }

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                WITH left_items AS (
                    SELECT * FROM bitrix_deal_snapshot_items WHERE snapshot_run_id = :left_run_id
                ),
                right_items AS (
                    SELECT * FROM bitrix_deal_snapshot_items WHERE snapshot_run_id = :right_run_id
                )
                SELECT
                    COALESCE(l.deal_id, r.deal_id) AS deal_id,
                    l.title AS left_title,
                    r.title AS right_title,
                    l.stage_id AS left_stage_id,
                    r.stage_id AS right_stage_id,
                    l.stage_name AS left_stage_name,
                    r.stage_name AS right_stage_name,
                    l.assigned_by_id AS left_assigned_by_id,
                    r.assigned_by_id AS right_assigned_by_id,
                    l.assigned_by_name AS left_assigned_by_name,
                    r.assigned_by_name AS right_assigned_by_name,
                    l.opportunity AS left_opportunity,
                    r.opportunity AS right_opportunity,
                    l.currency_id AS left_currency_id,
                    r.currency_id AS right_currency_id,
                    l.company_id AS left_company_id,
                    r.company_id AS right_company_id,
                    l.company_name AS left_company_name,
                    r.company_name AS right_company_name,
                    l.category_id AS left_category_id,
                    r.category_id AS right_category_id,
                    l.category_name AS left_category_name,
                    r.category_name AS right_category_name,
                    CASE
                        WHEN l.deal_id IS NULL THEN 'added'
                        WHEN r.deal_id IS NULL THEN 'removed'
                        ELSE 'changed'
                    END AS change_type
                FROM left_items l
                FULL OUTER JOIN right_items r ON r.deal_id = l.deal_id
                WHERE l.deal_id IS NULL
                   OR r.deal_id IS NULL
                   OR l.title IS DISTINCT FROM r.title
                   OR l.stage_id IS DISTINCT FROM r.stage_id
                   OR l.stage_name IS DISTINCT FROM r.stage_name
                   OR l.assigned_by_id IS DISTINCT FROM r.assigned_by_id
                   OR l.assigned_by_name IS DISTINCT FROM r.assigned_by_name
                   OR l.opportunity IS DISTINCT FROM r.opportunity
                   OR l.currency_id IS DISTINCT FROM r.currency_id
                   OR l.company_id IS DISTINCT FROM r.company_id
                   OR l.company_name IS DISTINCT FROM r.company_name
                   OR l.category_id IS DISTINCT FROM r.category_id
                   OR l.category_name IS DISTINCT FROM r.category_name
                ORDER BY COALESCE(l.deal_id, r.deal_id) DESC
                """
            ),
            {"left_run_id": int(leftRun["id"]), "right_run_id": int(rightRun["id"])},
        )

    return {
        "left_run": leftRun,
        "right_run": rightRun,
        "changes": [dict(row._mapping) for row in rows],
        "available_dates": [str(run["captured_for_date"]) for run in runs],
    }
