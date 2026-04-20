from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

from sqlalchemy import Connection, Engine, Executable, create_engine, text
from sqlalchemy.engine import CursorResult, RowMapping
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from mlp.logger import ComponentLoggerInterface

from .config import DatabaseConfig, LoggingConfig, PoolConfig
from .exceptions import MLPTransactionError, translate_sqlalchemy_error
from .instrumentation import instrument_engine

Statement = str | Executable
Parameters = Mapping[str, Any] | Sequence[Any] | None


class MLPDatabase:
    def __init__(
        self,
        engine: Engine,
        *,
        logger: ComponentLoggerInterface | None = None,
        logging_config: LoggingConfig | None = None,
    ) -> None:
        if not isinstance(engine, Engine):
            raise TypeError("engine must be a SQLAlchemy Engine.")
        if logger is not None and not isinstance(logger, ComponentLoggerInterface):
            raise TypeError("logger must implement ComponentLoggerInterface.")
        if logging_config is not None and not isinstance(logging_config, LoggingConfig):
            raise TypeError("logging_config must be LoggingConfig or None.")
        self._engine = engine
        self._logger = logger
        self._logging_config = logging_config or LoggingConfig()
        self._dedicated_engine: Engine | None = None
        instrument_engine(self._engine, logger=logger, config=self._logging_config)

    @classmethod
    def from_config(
        cls,
        config: DatabaseConfig,
        *,
        pool_config: PoolConfig | None = None,
        logging_config: LoggingConfig | None = None,
        logger: ComponentLoggerInterface | None = None,
    ) -> MLPDatabase:
        if not isinstance(config, DatabaseConfig):
            raise TypeError("config must be DatabaseConfig.")
        if pool_config is not None and not isinstance(pool_config, PoolConfig):
            raise TypeError("pool_config must be PoolConfig or None.")
        pool = pool_config or PoolConfig()
        engine_kwargs: dict[str, Any] = {
            "echo": config.echo,
            "pool_pre_ping": config.pool_pre_ping,
            "pool_size": pool.pool_size,
            "max_overflow": pool.max_overflow,
            "pool_timeout": pool.pool_timeout_seconds,
        }
        if config.pool_recycle_seconds is not None:
            engine_kwargs["pool_recycle"] = config.pool_recycle_seconds
        try:
            engine = create_engine(config.url, **engine_kwargs)
        except SQLAlchemyError as exc:
            raise translate_sqlalchemy_error(exc) from exc
        database = cls(engine, logger=logger, logging_config=logging_config)
        database._database_config = config
        return database

    @property
    def engine(self) -> Engine:
        return self._engine

    def connect(self) -> Connection:
        try:
            return self._engine.connect()
        except SQLAlchemyError as exc:
            raise translate_sqlalchemy_error(exc) from exc

    @contextmanager
    def begin(self, *, isolation_level: str | None = None) -> Iterator[Connection]:
        conn: Connection | None = None
        transaction = None
        try:
            conn = self.connect()
            if isolation_level is not None:
                conn = conn.execution_options(isolation_level=isolation_level)
            try:
                transaction = conn.begin()
            except SQLAlchemyError as exc:
                raise MLPTransactionError(str(exc)) from exc
            try:
                yield conn
            except BaseException:
                if transaction.is_active:
                    try:
                        transaction.rollback()
                    except SQLAlchemyError as rollback_exc:
                        raise MLPTransactionError(str(rollback_exc)) from rollback_exc
                raise
            else:
                try:
                    transaction.commit()
                except SQLAlchemyError as exc:
                    raise MLPTransactionError(str(exc)) from exc
        except SQLAlchemyError as exc:
            raise translate_sqlalchemy_error(exc) from exc
        finally:
            if conn is not None:
                try:
                    conn.close()
                except SQLAlchemyError as exc:
                    raise translate_sqlalchemy_error(exc) from exc

    def execute(self, statement: Statement, parameters: Parameters = None) -> CursorResult[Any]:
        try:
            with self._engine.begin() as conn:
                return conn.execute(_coerce_statement(statement), parameters)
        except SQLAlchemyError as exc:
            raise translate_sqlalchemy_error(exc) from exc

    def fetch_one(self, statement: Statement, parameters: Parameters = None) -> RowMapping | None:
        try:
            with self._engine.connect() as conn:
                row = conn.execute(_coerce_statement(statement), parameters).mappings().first()
                return row
        except SQLAlchemyError as exc:
            raise translate_sqlalchemy_error(exc) from exc

    def fetch_all(self, statement: Statement, parameters: Parameters = None) -> list[RowMapping]:
        try:
            with self._engine.connect() as conn:
                return list(conn.execute(_coerce_statement(statement), parameters).mappings().all())
        except SQLAlchemyError as exc:
            raise translate_sqlalchemy_error(exc) from exc

    def open_dedicated_connection(self) -> Connection:
        try:
            return self._get_dedicated_engine().connect()
        except SQLAlchemyError as exc:
            raise translate_sqlalchemy_error(exc) from exc

    def dispose(self) -> None:
        self._engine.dispose()
        if self._dedicated_engine is not None:
            self._dedicated_engine.dispose()

    def _get_dedicated_engine(self) -> Engine:
        if self._dedicated_engine is None:
            dedicated = create_engine(
                self._engine.url,
                echo=self._engine.echo,
                pool_pre_ping=True,
                poolclass=NullPool,
            )
            instrument_engine(dedicated, logger=self._logger, config=self._logging_config)
            self._dedicated_engine = dedicated
        return self._dedicated_engine


def create_database(
    config: DatabaseConfig,
    *,
    pool_config: PoolConfig | None = None,
    logging_config: LoggingConfig | None = None,
    logger: ComponentLoggerInterface | None = None,
) -> MLPDatabase:
    return MLPDatabase.from_config(config, pool_config=pool_config, logging_config=logging_config, logger=logger)


def _coerce_statement(statement: Statement) -> Executable:
    if isinstance(statement, str):
        return text(statement)
    return statement
