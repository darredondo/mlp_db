from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from time import monotonic, perf_counter
from typing import Any
from weakref import WeakKeyDictionary

from sqlalchemy import Engine, event
from sqlalchemy.engine import Connection

from mlp.logger import DEBUG, ComponentLoggerInterface, is_trace_enabled

from .config import LoggingConfig

COMPONENT = "mlp_db"
QUERY_TIMER_KEY = "_mlp_db_query_start_stack"
_INSTRUMENTED_ENGINES: WeakKeyDictionary[Engine, InstrumentationState] = WeakKeyDictionary()
_LOGGER_FAILURE_LAST_REPORTED_AT = 0.0
_LOGGER_FAILURE_MIN_INTERVAL_SECONDS = 30.0


@dataclass(slots=True)
class InstrumentationState:
    logger: ComponentLoggerInterface | None
    config: LoggingConfig


def instrument_engine(engine: Engine, *, logger: ComponentLoggerInterface | None, config: LoggingConfig | None = None) -> None:
    logging_config = config or LoggingConfig()
    state = _INSTRUMENTED_ENGINES.get(engine)
    if state is not None:
        state.logger = logger
        state.config = logging_config
        return
    state = InstrumentationState(logger=logger, config=logging_config)
    _INSTRUMENTED_ENGINES[engine] = state

    @event.listens_for(engine, "before_cursor_execute")
    def before_cursor_execute(
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        stack = conn.info.setdefault(QUERY_TIMER_KEY, [])
        stack.append(perf_counter())
        current_logger = state.logger
        if current_logger is None or not _safe_trace_enabled(current_logger, "db_query_started"):
            return
        if not _safe_is_enabled(current_logger, DEBUG, "db_query_started"):
            return
        _safe_event(
            current_logger,
            COMPONENT,
            "db_query_started",
            "Database query started.",
            level=DEBUG,
            context=_query_context(engine, statement, parameters, None, None, executemany, state.config),
        )

    @event.listens_for(engine, "after_cursor_execute")
    def after_cursor_execute(
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        duration_ms = _pop_duration_ms(conn)
        current_logger = state.logger
        if current_logger is None:
            return
        current_config = state.config
        trace_enabled = _safe_trace_enabled(current_logger, "db_query_finished")
        if trace_enabled and _safe_is_enabled(current_logger, DEBUG, "db_query_finished"):
            _safe_event(
                current_logger,
                COMPONENT,
                "db_query_finished",
                "Database query finished.",
                level=DEBUG,
                context=_query_context(engine, statement, parameters, duration_ms, cursor.rowcount, executemany, current_config),
            )
        should_log_query = current_config.log_successful_queries and not trace_enabled
        should_log_slow = current_config.slow_query_threshold_ms is not None and duration_ms >= current_config.slow_query_threshold_ms
        if not should_log_query and not should_log_slow:
            return
        event_name = "db_slow_query" if should_log_slow else "db_query"
        level = "WARNING" if should_log_slow else DEBUG
        if not _safe_is_enabled(current_logger, level, event_name):
            return
        _safe_event(
            current_logger,
            COMPONENT,
            event_name,
            "Database query executed.",
            level=level,
            context=_query_context(engine, statement, parameters, duration_ms, cursor.rowcount, executemany, current_config),
        )

    @event.listens_for(engine, "handle_error")
    def handle_error(exception_context: Any) -> None:
        current_logger = state.logger
        if current_logger is None:
            return
        trace_enabled = _safe_trace_enabled(current_logger, "db_query_failed")
        duration_ms = None
        if exception_context.connection is not None:
            duration_ms = _pop_duration_ms(exception_context.connection)
        original = exception_context.original_exception
        _safe_failure(
            current_logger,
            COMPONENT,
            "db_query_failed" if trace_enabled else "db_query_error",
            "Database query failed.",
            exception=original,
            context=_query_context(
                engine,
                exception_context.statement,
                exception_context.parameters,
                duration_ms,
                None,
                False,
                state.config,
                exception_type=original.__class__.__name__ if original is not None else None,
                connection_invalidated=getattr(exception_context, "is_disconnect", None),
            ),
        )

    @event.listens_for(engine, "begin")
    def begin(conn: Connection) -> None:
        if state.logger is not None and state.config.log_transaction_events:
            _safe_event(state.logger, COMPONENT, "db_transaction_begin", "Database transaction started.", level=DEBUG, context=_engine_context(engine))

    @event.listens_for(engine, "commit")
    def commit(conn: Connection) -> None:
        if state.logger is not None and state.config.log_transaction_events:
            _safe_event(state.logger, COMPONENT, "db_transaction_commit", "Database transaction committed.", level=DEBUG, context=_engine_context(engine))

    @event.listens_for(engine, "rollback")
    def rollback(conn: Connection) -> None:
        if state.logger is not None and state.config.log_transaction_events:
            _safe_event(state.logger, COMPONENT, "db_transaction_rollback", "Database transaction rolled back.", level=DEBUG, context=_engine_context(engine))

    _instrument_pool(engine, state)


def truncate_statement(statement: str | None, max_length: int) -> str | None:
    if statement is None:
        return None
    if len(statement) <= max_length:
        return statement
    return statement[:max_length] + "...<truncated>"


def sanitize_parameters(parameters: Any, *, enabled: bool) -> Any:
    if not enabled:
        return None
    return _sanitize_value(parameters)


def _instrument_pool(engine: Engine, state: InstrumentationState) -> None:
    @event.listens_for(engine.pool, "connect")
    def connect(dbapi_connection: Any, connection_record: Any) -> None:
        if state.logger is not None and state.config.log_pool_events:
            _safe_event(state.logger, COMPONENT, "db_pool_connect", "Database pool opened a connection.", level=DEBUG, context=_pool_context(engine, "connect"))

    @event.listens_for(engine.pool, "checkout")
    def checkout(dbapi_connection: Any, connection_record: Any, connection_proxy: Any) -> None:
        if state.logger is not None and state.config.log_pool_events:
            _safe_event(
                state.logger,
                COMPONENT,
                "db_pool_checkout",
                "Database pool checked out a connection.",
                level=DEBUG,
                context=_pool_context(engine, "checkout"),
            )

    @event.listens_for(engine.pool, "checkin")
    def checkin(dbapi_connection: Any, connection_record: Any) -> None:
        if state.logger is not None and state.config.log_pool_events:
            _safe_event(state.logger, COMPONENT, "db_pool_checkin", "Database pool checked in a connection.", level=DEBUG, context=_pool_context(engine, "checkin"))

    @event.listens_for(engine.pool, "invalidate")
    def invalidate(dbapi_connection: Any, connection_record: Any, exception: BaseException | None) -> None:
        if state.logger is not None and state.config.log_pool_events:
            _safe_failure(
                state.logger,
                COMPONENT,
                "db_pool_invalidate",
                "Database pool invalidated a connection.",
                exception=exception,
                context=_pool_context(engine, "invalidate", exception_type=exception.__class__.__name__ if exception else None),
            )


def _pop_duration_ms(conn: Connection) -> float:
    stack = conn.info.get(QUERY_TIMER_KEY)
    if stack:
        started_at = stack.pop()
        return round((perf_counter() - started_at) * 1000, 3)
    return 0.0


def _safe_event(
    logger: ComponentLoggerInterface,
    component: str,
    event_name: str,
    message: str,
    *,
    level: str,
    context: dict[str, Any],
) -> None:
    try:
        logger.event(component, event_name, message, level=level, context=context)
    except Exception as exc:
        _report_logger_failure_once(exc, event_name)
        return


def _safe_is_enabled(logger: ComponentLoggerInterface, level: str, event_name: str) -> bool:
    try:
        return logger.is_enabled(level)
    except Exception as exc:
        _report_logger_failure_once(exc, event_name)
        return False


def _safe_trace_enabled(logger: ComponentLoggerInterface, event_name: str) -> bool:
    try:
        return is_trace_enabled(logger)
    except Exception as exc:
        _report_logger_failure_once(exc, event_name)
        return False


def _safe_failure(
    logger: ComponentLoggerInterface,
    component: str,
    event_name: str,
    message: str,
    *,
    exception: BaseException | None = None,
    context: dict[str, Any],
) -> None:
    try:
        logger.failure(component, event_name, message, exception=exception, context=context)
    except Exception as exc:
        _report_logger_failure_once(exc, event_name)
        return


def _report_logger_failure_once(exc: Exception, event_name: str) -> None:
    global _LOGGER_FAILURE_LAST_REPORTED_AT
    now = monotonic()
    if now - _LOGGER_FAILURE_LAST_REPORTED_AT < _LOGGER_FAILURE_MIN_INTERVAL_SECONDS:
        return
    _LOGGER_FAILURE_LAST_REPORTED_AT = now
    stream = getattr(sys, "stderr", None)
    if stream is None or not hasattr(stream, "write"):
        return
    payload = {
        "component": COMPONENT,
        "event": "logger_internal_failure",
        "source_event": event_name,
        "exception_type": exc.__class__.__name__,
        "exception_message": str(exc),
    }
    try:
        stream.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        flush = getattr(stream, "flush", None)
        if callable(flush):
            flush()
    except Exception:
        return


def _query_context(
    engine: Engine,
    statement: str | None,
    parameters: Any,
    duration_ms: float | None,
    rowcount: int | None,
    executemany: bool,
    config: LoggingConfig,
    **extra: Any,
) -> dict[str, Any]:
    context = _engine_context(engine)
    context.update(
        {
            "statement": truncate_statement(statement, config.max_statement_length),
            "duration_ms": duration_ms,
            "rowcount": rowcount,
            "executemany": executemany,
        }
    )
    sanitized_parameters = sanitize_parameters(parameters, enabled=config.log_parameters)
    if sanitized_parameters is not None:
        context["parameters"] = sanitized_parameters
    context.update({key: value for key, value in extra.items() if value is not None})
    return context


def _engine_context(engine: Engine) -> dict[str, Any]:
    return {
        "dialect": engine.dialect.name,
        "driver": engine.dialect.driver,
        "database": engine.url.database,
    }


def _pool_context(engine: Engine, pool_event: str, **extra: Any) -> dict[str, Any]:
    context = _engine_context(engine)
    context["pool_event"] = pool_event
    context.update({key: value for key, value in extra.items() if value is not None})
    return context


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, str):
        return value if len(value) <= 500 else value[:500] + "...<truncated>"
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    if isinstance(value, Mapping):
        return {str(key): _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(item) for item in value[:20]]
    return value
