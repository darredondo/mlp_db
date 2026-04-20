from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import Any
from weakref import WeakKeyDictionary

from sqlalchemy import Engine, event
from sqlalchemy.engine import Connection

from mlp.logger import DEBUG, ComponentLoggerInterface

from .config import LoggingConfig

COMPONENT = "mlp_db"
QUERY_TIMER_KEY = "_mlp_db_query_start_stack"
_INSTRUMENTED_ENGINES: WeakKeyDictionary[Engine, InstrumentationState] = WeakKeyDictionary()


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
        should_log_query = current_config.log_successful_queries
        should_log_slow = current_config.slow_query_threshold_ms is not None and duration_ms >= current_config.slow_query_threshold_ms
        if not should_log_query and not should_log_slow:
            return
        event_name = "db_slow_query" if should_log_slow else "db_query"
        level = "WARNING" if should_log_slow else DEBUG
        current_logger.event(
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
        duration_ms = None
        if exception_context.connection is not None:
            duration_ms = _pop_duration_ms(exception_context.connection)
        original = exception_context.original_exception
        current_logger.failure(
            COMPONENT,
            "db_query_error",
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
        if state.logger is not None:
            state.logger.event(COMPONENT, "db_transaction_begin", "Database transaction started.", level=DEBUG, context=_engine_context(engine))

    @event.listens_for(engine, "commit")
    def commit(conn: Connection) -> None:
        if state.logger is not None:
            state.logger.event(COMPONENT, "db_transaction_commit", "Database transaction committed.", level=DEBUG, context=_engine_context(engine))

    @event.listens_for(engine, "rollback")
    def rollback(conn: Connection) -> None:
        if state.logger is not None:
            state.logger.event(COMPONENT, "db_transaction_rollback", "Database transaction rolled back.", level=DEBUG, context=_engine_context(engine))

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
            state.logger.event(COMPONENT, "db_pool_connect", "Database pool opened a connection.", level=DEBUG, context=_pool_context(engine, "connect"))

    @event.listens_for(engine.pool, "checkout")
    def checkout(dbapi_connection: Any, connection_record: Any, connection_proxy: Any) -> None:
        if state.logger is not None and state.config.log_pool_events:
            state.logger.event(
                COMPONENT,
                "db_pool_checkout",
                "Database pool checked out a connection.",
                level=DEBUG,
                context=_pool_context(engine, "checkout"),
            )

    @event.listens_for(engine.pool, "checkin")
    def checkin(dbapi_connection: Any, connection_record: Any) -> None:
        if state.logger is not None and state.config.log_pool_events:
            state.logger.event(COMPONENT, "db_pool_checkin", "Database pool checked in a connection.", level=DEBUG, context=_pool_context(engine, "checkin"))

    @event.listens_for(engine.pool, "invalidate")
    def invalidate(dbapi_connection: Any, connection_record: Any, exception: BaseException | None) -> None:
        if state.logger is not None and state.config.log_pool_events:
            state.logger.failure(
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
