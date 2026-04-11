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

