from __future__ import annotations

import os

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from conftest import MemoryLogger
from mlp.db import DatabaseConfig, LoggingConfig, MLPDatabase, MLPIntegrityError, MLPQueryError, PoolConfig

pytestmark = pytest.mark.skipif(not os.environ.get("MLP_DB_TEST_URL"), reason="MLP_DB_TEST_URL is not set")


def build_db(*, logger: MemoryLogger | None = None, logging_config: LoggingConfig | None = None) -> MLPDatabase:
    url = os.environ["MLP_DB_TEST_URL"]
    drivername = make_url(url).drivername
    if drivername != "mysql+mysqldb":
        pytest.fail(f"MLP_DB_TEST_URL must use mysqlclient: expected mysql+mysqldb, got {drivername}")
    return MLPDatabase.from_config(
        DatabaseConfig(url=url),
        pool_config=PoolConfig(pool_size=1, max_overflow=0, pool_timeout_seconds=1),
        logger=logger,
        logging_config=logging_config,
    )


def test_mysql_select_and_fetch_helpers() -> None:
    db = build_db()
    assert db.engine.dialect.name == "mysql"
    assert db.engine.dialect.driver == "mysqldb"

    row = db.fetch_one("SELECT 1 AS ok")
    assert row is not None
    assert row["ok"] == 1
    assert db.fetch_all("SELECT 1 AS ok UNION SELECT 2 AS ok") == [{"ok": 1}, {"ok": 2}]


def test_mysql_dml_transactions_and_errors() -> None:
    db = build_db()
    db.execute("DROP TABLE IF EXISTS mlp_db_test_demo")
    db.execute("CREATE TABLE mlp_db_test_demo (id INT AUTO_INCREMENT PRIMARY KEY, code VARCHAR(32) NOT NULL UNIQUE)")
    try:
        with db.begin(isolation_level="READ COMMITTED") as conn:
            conn.execute(text("INSERT INTO mlp_db_test_demo (code) VALUES (:code)"), {"code": "A"})

        with pytest.raises(RuntimeError), db.begin(isolation_level="READ COMMITTED") as conn:
            conn.execute(text("INSERT INTO mlp_db_test_demo (code) VALUES (:code)"), {"code": "B"})
            raise RuntimeError("rollback")

        assert [row["code"] for row in db.fetch_all("SELECT code FROM mlp_db_test_demo ORDER BY id")] == ["A"]

        with pytest.raises(MLPIntegrityError):
            db.execute("INSERT INTO mlp_db_test_demo (code) VALUES (:code)", {"code": "A"})
        with pytest.raises(MLPQueryError):
            db.fetch_one("SELECT * FROM mlp_db_missing_table")
    finally:
        db.execute("DROP TABLE IF EXISTS mlp_db_test_demo")


def test_mysql_logging_and_dedicated_connection() -> None:
    logger = MemoryLogger()
    db = build_db(
        logger=logger,
        logging_config=LoggingConfig(slow_query_threshold_ms=0.0001, log_pool_events=True, log_successful_queries=True, log_parameters=True),
    )

    normal = db.connect()
    try:
        dedicated = db.open_dedicated_connection()
        try:
            assert dedicated.execute(text("SELECT 1 AS ok")).mappings().one()["ok"] == 1
        finally:
            dedicated.close()
    finally:
        normal.close()

    db.fetch_one("SELECT 1 AS ok")
    db.fetch_one(text("SELECT :value AS value"), {"value": "abc"})

    assert any(record["event"] == "db_pool_checkout" for record in logger.records)
    assert any(record["event"] == "db_slow_query" for record in logger.records)
    assert any(record["event"] == "db_query" for record in logger.records)
    query_record = next(record for record in logger.records if record["event"] == "db_query" and "parameters" in record["context"])
    assert query_record["context"]["parameters"] in (["abc"], {"value": "abc"})


def test_mysql_successful_query_not_logged_by_default() -> None:
    logger = MemoryLogger()
    db = build_db(logger=logger, logging_config=LoggingConfig(slow_query_threshold_ms=None, log_successful_queries=False))

    db.fetch_one("SELECT 1 AS ok")

    assert not any(record["event"] == "db_query" for record in logger.records)


def test_mysql_pool_events_are_configurable() -> None:
    enabled_logger = MemoryLogger()
    enabled = build_db(logger=enabled_logger, logging_config=LoggingConfig(log_pool_events=True))
    enabled.fetch_one("SELECT 1")

    disabled_logger = MemoryLogger()
    disabled = build_db(logger=disabled_logger, logging_config=LoggingConfig(log_pool_events=False))
    disabled.fetch_one("SELECT 1")

    assert any(record["event"] == "db_pool_checkout" for record in enabled_logger.records)
    assert not any(record["event"] == "db_pool_checkout" for record in disabled_logger.records)
