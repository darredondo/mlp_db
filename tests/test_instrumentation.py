from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine

from mlp.db import LoggingConfig, MLPDatabase, instrumentation
from mlp.db.instrumentation import sanitize_parameters, truncate_statement
from mlp.logger import ComponentLoggerInterface, configure_trace


class MemoryLogger(ComponentLoggerInterface):
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def is_enabled(self, level: str) -> bool:
        return True

    def event(
        self,
        component: str,
        event: str,
        message: str,
        *,
        level: str = "INFO",
        context: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        self.records.append({"kind": "event", "component": component, "event": event, "level": level, "context": context or {}})

    def failure(
        self,
        component: str,
        event: str,
        message: str,
        *,
        exception: BaseException | None = None,
        context: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        self.records.append({"kind": "failure", "component": component, "event": event, "exception": exception, "context": context or {}})


class ExplodingLogger(ComponentLoggerInterface):
    def is_enabled(self, level: str) -> bool:
        return True

    def event(
        self,
        component: str,
        event: str,
        message: str,
        *,
        level: str = "INFO",
        context: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raise RuntimeError("logger event failed")

    def failure(
        self,
        component: str,
        event: str,
        message: str,
        *,
        exception: BaseException | None = None,
        context: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raise RuntimeError("logger failure failed")


class InfoOnlyExplodingEventLogger(ComponentLoggerInterface):
    def is_enabled(self, level: str) -> bool:
        return level != "DEBUG"

    def event(
        self,
        component: str,
        event: str,
        message: str,
        *,
        level: str = "INFO",
        context: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raise RuntimeError("event should not be called for filtered DEBUG query")

    def failure(
        self,
        component: str,
        event: str,
        message: str,
        *,
        exception: BaseException | None = None,
        context: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        raise RuntimeError("logger failure failed")


def test_truncates_statement() -> None:
    assert truncate_statement("abcdef", 3) == "abc...<truncated>"


def test_does_not_log_parameters_by_default() -> None:
    assert sanitize_parameters({"password": "secret"}, enabled=False) is None


def test_sanitizes_parameters_only_when_enabled() -> None:
    assert sanitize_parameters({"value": "abc"}, enabled=True) == {"value": "abc"}


def test_logger_failure_does_not_break_query_and_reports_to_stderr(capsys, monkeypatch) -> None:
    monkeypatch.setattr(instrumentation, "_LOGGER_FAILURE_LAST_REPORTED_AT", 0.0)
    db = MLPDatabase(
        create_engine("sqlite+pysqlite:///:memory:"),
        logger=ExplodingLogger(),
        logging_config=LoggingConfig(
            log_successful_queries=True,
            log_pool_events=True,
            log_transaction_events=True,
        ),
    )

    row = db.fetch_one("SELECT 1 AS value")

    assert row is not None
    assert row["value"] == 1
    captured = capsys.readouterr()
    assert "logger_internal_failure" in captured.err
    assert "db_query" in captured.err or "db_pool" in captured.err


def test_filtered_debug_query_does_not_build_or_emit_event(capsys) -> None:
    db = MLPDatabase(
        create_engine("sqlite+pysqlite:///:memory:"),
        logger=InfoOnlyExplodingEventLogger(),
        logging_config=LoggingConfig(log_successful_queries=True, slow_query_threshold_ms=None),
    )

    row = db.fetch_one("SELECT 1 AS value")

    assert row is not None
    assert row["value"] == 1
    assert capsys.readouterr().err == ""


def test_transaction_events_are_disabled_by_default() -> None:
    logger = MemoryLogger()
    db = MLPDatabase(create_engine("sqlite+pysqlite:///:memory:"), logger=logger)

    db.execute("SELECT 1")

    assert not any(record["event"].startswith("db_transaction_") for record in logger.records)


def test_transaction_events_can_be_enabled() -> None:
    logger = MemoryLogger()
    db = MLPDatabase(
        create_engine("sqlite+pysqlite:///:memory:"),
        logger=logger,
        logging_config=LoggingConfig(log_transaction_events=True),
    )

    db.execute("SELECT 1")

    events = [record["event"] for record in logger.records]
    assert "db_transaction_begin" in events
    assert "db_transaction_commit" in events


def test_trace_mode_emits_query_started_and_finished() -> None:
    memory_logger = MemoryLogger()
    logger = configure_trace(memory_logger, trace_enabled=True)
    db = MLPDatabase(
        create_engine("sqlite+pysqlite:///:memory:"),
        logger=logger,
        logging_config=LoggingConfig(log_successful_queries=False, slow_query_threshold_ms=None),
    )

    row = db.fetch_one("SELECT 1 AS value")

    assert row is not None
    events = [record["event"] for record in memory_logger.records]
    assert "db_query_started" in events
    assert "db_query_finished" in events
    finished = next(record for record in memory_logger.records if record["event"] == "db_query_finished")
    assert finished["context"]["statement"] == "SELECT 1 AS value"
    assert "duration_ms" in finished["context"]


def test_trace_mode_emits_query_failed() -> None:
    memory_logger = MemoryLogger()
    logger = configure_trace(memory_logger, trace_enabled=True)
    db = MLPDatabase(
        create_engine("sqlite+pysqlite:///:memory:"),
        logger=logger,
        logging_config=LoggingConfig(log_successful_queries=False, slow_query_threshold_ms=None),
    )

    try:
        db.fetch_one("SELECT * FROM missing_table")
    except Exception:
        pass

    failures = [record for record in memory_logger.records if record["kind"] == "failure"]
    assert any(record["event"] == "db_query_failed" for record in failures)
    failed = next(record for record in failures if record["event"] == "db_query_failed")
    assert "statement" in failed["context"]
    assert "duration_ms" in failed["context"]
