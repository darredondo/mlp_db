from __future__ import annotations

from contextlib import suppress

from sqlalchemy import create_engine, text

from conftest import MemoryLogger
from mlp.db import LoggingConfig, MLPDatabase, MLPQueryError
from mlp.db.instrumentation import sanitize_parameters, truncate_statement


def test_truncates_statement() -> None:
    assert truncate_statement("abcdef", 3) == "abc...<truncated>"


def test_does_not_log_parameters_by_default() -> None:
    assert sanitize_parameters({"password": "secret"}, enabled=False) is None


def test_successful_query_is_not_logged_by_default() -> None:
    logger = MemoryLogger()
    db = MLPDatabase(create_engine("sqlite:///:memory:"), logger=logger)

    row = db.fetch_one("SELECT 1 AS ok")
    assert row is not None
    assert row["ok"] == 1

    assert not any(record["event"] == "db_query" for record in logger.records)


def test_slow_query_logs_when_threshold_is_low() -> None:
    logger = MemoryLogger()
    db = MLPDatabase(
        create_engine("sqlite:///:memory:"),
        logger=logger,
        logging_config=LoggingConfig(slow_query_threshold_ms=0.0001),
    )

    db.fetch_one("SELECT 1 AS ok")

    assert any(record["event"] == "db_slow_query" for record in logger.records)


def test_query_error_logs_and_public_method_translates() -> None:
    logger = MemoryLogger()
    db = MLPDatabase(create_engine("sqlite:///:memory:"), logger=logger)

    with suppress(MLPQueryError):
        db.fetch_one("SELECT * FROM missing_table")

    assert any(record["event"] == "db_query_error" for record in logger.records)


def test_pool_events_are_configurable() -> None:
    enabled_logger = MemoryLogger()
    enabled = MLPDatabase(
        create_engine("sqlite:///:memory:"),
        logger=enabled_logger,
        logging_config=LoggingConfig(log_pool_events=True),
    )
    enabled.fetch_one("SELECT 1")

    disabled_logger = MemoryLogger()
    disabled = MLPDatabase(
        create_engine("sqlite:///:memory:"),
        logger=disabled_logger,
        logging_config=LoggingConfig(log_pool_events=False),
    )
    disabled.fetch_one("SELECT 1")

    assert any(record["event"] == "db_pool_checkout" for record in enabled_logger.records)
    assert not any(record["event"] == "db_pool_checkout" for record in disabled_logger.records)


def test_log_parameters_when_enabled() -> None:
    logger = MemoryLogger()
    db = MLPDatabase(
        create_engine("sqlite:///:memory:"),
        logger=logger,
        logging_config=LoggingConfig(log_successful_queries=True, log_parameters=True),
    )

    db.fetch_one(text("SELECT :value AS value"), {"value": "abc"})

    query_record = next(record for record in logger.records if record["event"] == "db_query")
    assert query_record["context"]["parameters"] == ["abc"]
