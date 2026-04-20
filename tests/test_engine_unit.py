from __future__ import annotations

import pytest
from sqlalchemy import text

from mlp.db import DatabaseConfig, MLPDatabase, MLPIntegrityError, MLPQueryError, PoolConfig


def sqlite_config(tmp_path) -> DatabaseConfig:
    return DatabaseConfig(url=f"sqlite:///{tmp_path / 'unit.db'}")


def test_fetch_helpers_and_execute(tmp_path) -> None:
    db = MLPDatabase.from_config(sqlite_config(tmp_path))
    db.execute("CREATE TABLE demo (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    db.execute(text("INSERT INTO demo (name) VALUES (:name)"), {"name": "abc"})

    row = db.fetch_one("SELECT name FROM demo WHERE id = :id", {"id": 1})
    rows = db.fetch_all("SELECT name FROM demo ORDER BY id")

    assert row is not None
    assert row["name"] == "abc"
    assert [item["name"] for item in rows] == ["abc"]


def test_begin_commits_and_rolls_back(tmp_path) -> None:
    db = MLPDatabase.from_config(sqlite_config(tmp_path))
    db.execute("CREATE TABLE demo (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

    with db.begin() as conn:
        conn.execute(text("INSERT INTO demo (name) VALUES (:name)"), {"name": "committed"})

    with pytest.raises(RuntimeError), db.begin() as conn:
        conn.execute(text("INSERT INTO demo (name) VALUES (:name)"), {"name": "rolled_back"})
        raise RuntimeError("boom")

    rows = db.fetch_all("SELECT name FROM demo ORDER BY id")
    assert [row["name"] for row in rows] == ["committed"]


def test_begin_accepts_isolation_level(tmp_path) -> None:
    db = MLPDatabase.from_config(sqlite_config(tmp_path))

    with db.begin(isolation_level="SERIALIZABLE") as conn:
        assert conn.get_isolation_level() == "SERIALIZABLE"


def test_sql_error_is_translated(tmp_path) -> None:
    db = MLPDatabase.from_config(sqlite_config(tmp_path))

    with pytest.raises(MLPQueryError):
        db.fetch_one("SELECT * FROM missing_table")


def test_unique_violation_is_translated(tmp_path) -> None:
    db = MLPDatabase.from_config(sqlite_config(tmp_path))
    db.execute("CREATE TABLE demo (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE)")
    db.execute("INSERT INTO demo (code) VALUES (:code)", {"code": "A"})

    with pytest.raises(MLPIntegrityError):
        db.execute("INSERT INTO demo (code) VALUES (:code)", {"code": "A"})


def test_from_config_uses_pool_config(tmp_path) -> None:
    db = MLPDatabase.from_config(sqlite_config(tmp_path), pool_config=PoolConfig(pool_size=1, max_overflow=0, pool_timeout_seconds=1))

    assert db.engine.pool.size() == 1
