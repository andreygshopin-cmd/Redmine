"""Append-only annual planning storage, shared by all application users."""

from datetime import UTC, date, datetime, time, timedelta
import hashlib
from threading import Lock
from uuid import uuid4

from sqlalchemy import (
    Column, DateTime, ForeignKey, Index, Integer, JSON, MetaData, String, Table,
    UniqueConstraint, func, insert, select, text, update,
)

from src.redmine import db
from src.redmine.dates import getSnapshotTimezone

metadata = MetaData()
entities = Table(
    "year_model_entities", metadata,
    Column("id", String(36), primary_key=True),
    Column("year", Integer, nullable=False, index=True),
    Column("kind", String(16), nullable=False),
    Column("seed_key", String(80), unique=True),
)
versions = Table(
    "year_model_versions", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("entity_id", String(36), ForeignKey("year_model_entities.id"), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("recorded_at", DateTime(timezone=True), nullable=False),
    Column("actor", String(320), nullable=False),
    Column("data", JSON, nullable=False),
    UniqueConstraint("entity_id", "revision", name="uq_year_model_entity_revision"),
)
Index("ix_year_model_versions_snapshot", versions.c.entity_id, versions.c.recorded_at, versions.c.id)
imports = Table(
    "year_model_initial_imports", metadata,
    Column("year", Integer, primary_key=True),
    Column("recorded_at", DateTime(timezone=True), nullable=False),
    Column("actor", String(320), nullable=False),
)
view_settings = Table(
    "year_model_view_settings", metadata,
    Column("user_login", String(320), primary_key=True),
    Column("selected_year", Integer, nullable=True),
    Column("collapsed_directions", JSON, nullable=False),
)
_schema_lock = Lock()
_ready_engine = None


def get_engine():
    if db.engine is None:
        raise RuntimeError("DATABASE_URL is not set")
    return db.engine


def ensure_storage():
    global _ready_engine
    engine = get_engine()
    if _ready_engine is engine:
        return
    with _schema_lock:
        if _ready_engine is engine:
            return
        with engine.begin() as conn:
            if engine.dialect.name == "postgresql":
                conn.execute(text("SELECT pg_advisory_xact_lock(7252027)"))
            metadata.create_all(conn)
        _ready_engine = engine


def lock_year(conn, year):
    # Serializes parent/child edits as well as first-time year creation across workers.
    if conn.dialect.name == "postgresql":
        conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 7260000 + year})


def read_view_settings(login):
    ensure_storage()
    with get_engine().connect() as conn:
        row = conn.execute(select(view_settings).where(view_settings.c.user_login == login)).mappings().first()
    return {"selected_year": row["selected_year"] if row else None,
            "collapsed_directions": row["collapsed_directions"] if row else []}


def save_view_settings(login, selected_year=None, direction_id=None, collapsed=None):
    ensure_storage()
    with get_engine().begin() as conn:
        if conn.dialect.name == "postgresql":
            # Also serializes the initial INSERT and independent patches from multiple tabs.
            key = int.from_bytes(hashlib.sha256(("year-model-view:" + login).encode()).digest()[:8], "big", signed=True)
            conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
        row = conn.execute(select(view_settings).where(view_settings.c.user_login == login).with_for_update()).mappings().first()
        saved = {"selected_year": row["selected_year"] if row else None,
                 "collapsed_directions": list(row["collapsed_directions"]) if row else []}
        if selected_year is not None:
            saved["selected_year"] = selected_year
        if direction_id is not None:
            if not conn.execute(select(entities.c.id).where(entities.c.id == direction_id, entities.c.kind == "direction")).first():
                raise LookupError("Направление не найдено")
            ids = set(saved["collapsed_directions"])
            ids.add(direction_id) if collapsed else ids.discard(direction_id)
            saved["collapsed_directions"] = sorted(ids)
        if row:
            conn.execute(update(view_settings).where(view_settings.c.user_login == login).values(**saved))
        else:
            conn.execute(insert(view_settings).values(user_login=login, **saved))
        return saved


def _create(conn, year, kind, data, actor, now, seed_key=None):
    entity_id = str(uuid4())
    conn.execute(insert(entities).values(id=entity_id, year=year, kind=kind, seed_key=seed_key))
    conn.execute(insert(versions).values(
        entity_id=entity_id, revision=1, recorded_at=now, actor=actor, data=data,
    ))
    return entity_id


def import_initial(year, records, actor, validate):
    """Import private initialization data atomically, once per year, without source-code seeds."""
    ensure_storage()
    with get_engine().begin() as conn:
        lock_year(conn, year)
        if conn.execute(select(imports.c.year).where(imports.c.year == year)).first():
            return {"already_imported": True}
        if snapshot(conn, year, include_deleted=True):
            raise FileExistsError("Первоначальный импорт доступен только для пустого года")
        now = datetime.now(UTC)
        company_data = {"name": "Компания", "target": "0", "company": True, "order": 9999}
        company_id = _create(conn, year, "direction", company_data, actor, now, f"company-{year}")
        ids = {"company": company_id}
        current = [{**company_data, "id": company_id, "kind": "direction"}]
        for record in sorted(records, key=lambda item: item["kind"] != "direction"):
            key, kind = record["key"], record["kind"]
            if not key or key in ids:
                raise ValueError("Ключи импортируемых записей должны быть уникальными; company зарезервирован")
            data = dict(record["data"])
            if kind != "direction":
                if record.get("direction_key") not in ids:
                    raise ValueError("Не найдено направление импортируемой записи")
                data["direction_id"] = ids[record["direction_key"]]
            clean = validate(kind, data, current)
            entity_id = _create(conn, year, kind, clean, actor, now)
            ids[key] = entity_id
            current.append({**clean, "id": entity_id, "kind": kind})
        conn.execute(insert(imports).values(year=year, recorded_at=now, actor=actor))
        return {"already_imported": False, "created": len(current)}


def cutoff_for(as_of: date):
    return datetime.combine(as_of + timedelta(days=1), time.min, getSnapshotTimezone()).astimezone(UTC)


def _serialize(row):
    stamp = row["recorded_at"]
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return {**row["data"], "id": row["entity_id"], "kind": row["kind"], "year": row["year"],
            "revision": row["revision"], "recorded_at": stamp.isoformat(), "actor": row["actor"]}


def snapshot(conn, year, cutoff=None, include_deleted=False):
    latest = select(versions.c.entity_id, func.max(versions.c.id).label("version_id")).join(
        entities, entities.c.id == versions.c.entity_id,
    ).where(entities.c.year == year)
    if cutoff is not None:
        latest = latest.where(versions.c.recorded_at < cutoff)
    latest = latest.group_by(versions.c.entity_id).subquery()
    rows = conn.execute(select(versions, entities.c.kind, entities.c.year).join(
        latest, versions.c.id == latest.c.version_id,
    ).join(entities, entities.c.id == versions.c.entity_id)).mappings()
    items = [_serialize(row) for row in rows]
    return [item for item in items if include_deleted or not item.get("deleted")]


def read_snapshot(year, as_of):
    ensure_storage()
    with get_engine().connect() as conn:
        items = snapshot(conn, year, cutoff_for(as_of))
        years = list(conn.execute(select(entities.c.year).distinct().order_by(entities.c.year)).scalars())
    return items, years


def read_history(entity_id, as_of):
    ensure_storage()
    with get_engine().connect() as conn:
        rows = conn.execute(select(versions, entities.c.kind, entities.c.year).join(
            entities, entities.c.id == versions.c.entity_id,
        ).where(versions.c.entity_id == entity_id, versions.c.recorded_at < cutoff_for(as_of)).order_by(
            versions.c.revision.desc(),
        )).mappings()
        return [_serialize(row) for row in rows]


def save(year, kind, data, actor, validate, entity_id=None, revision=None, delete=False):
    ensure_storage()
    with get_engine().begin() as conn:
        lock_year(conn, year)
        current = snapshot(conn, year)
        old = next((item for item in current if item["id"] == entity_id), None)
        if entity_id and (old is None or old["kind"] != kind):
            raise LookupError("Запись не найдена")
        if old and old["revision"] != revision:
            raise FileExistsError("Запись уже изменена другим пользователем. Обновите страницу и повторите изменения.")
        clean = validate(data, current, old, delete)
        now = datetime.now(UTC)
        if old:
            conn.execute(insert(versions).values(
                entity_id=entity_id, revision=revision + 1, recorded_at=now, actor=actor, data=clean,
            ))
        else:
            entity_id = _create(conn, year, kind, clean, actor, now)
        # Each populated year always has exactly one Company lane.
        if not any(item.get("company") for item in current) and not clean.get("company"):
            _create(conn, year, "direction", {
                "name": "Компания", "company": True, "order": 9999, "target": "0",
            }, actor, now, f"company-{year}")
        return entity_id
