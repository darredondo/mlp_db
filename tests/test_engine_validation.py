from __future__ import annotations

import pytest
from sqlalchemy import create_engine

from conftest import MemoryLogger
from mlp.db import DatabaseConfig, LoggingConfig, MLPDatabase
from mlp.db.instrumentation import _INSTRUMENTED_ENGINES


def test_database_validates_engine_type() -> None:
    with pytest.raises(TypeError, match="engine must be a SQLAlchemy Engine"):
        MLPDatabase(object())  # type: ignore[arg-type]


def test_database_validates_logger_type() -> None:
    engine = create_engine("mysql+mysqldb://u:p@localhost:3306/app?charset=utf8mb4")

    with pytest.raises(TypeError, match="logger must implement ComponentLoggerInterface"):
        MLPDatabase(engine, logger=object())  # type: ignore[arg-type]


def test_database_validates_logging_config_type() -> None:
    engine = create_engine("mysql+mysqldb://u:p@localhost:3306/app?charset=utf8mb4")

    with pytest.raises(TypeError, match="logging_config must be LoggingConfig or None"):
        MLPDatabase(engine, logging_config=object())  # type: ignore[arg-type]


def test_database_accepts_logging_config_instance() -> None:
    engine = create_engine("mysql+mysqldb://u:p@localhost:3306/app?charset=utf8mb4")

    db = MLPDatabase(engine, logging_config=LoggingConfig())

    assert db.engine is engine


def test_from_config_validates_config_type() -> None:
    with pytest.raises(TypeError, match="config must be DatabaseConfig"):
        MLPDatabase.from_config({"url": "mysql+mysqldb://u:p@localhost:3306/app"})  # type: ignore[arg-type]


def test_from_config_validates_pool_config_type() -> None:
    config = DatabaseConfig(url="mysql+mysqldb://u:p@localhost:3306/app?charset=utf8mb4")

    with pytest.raises(TypeError, match="pool_config must be PoolConfig or None"):
        MLPDatabase.from_config(config, pool_config={"pool_size": 1})  # type: ignore[arg-type]


def test_reusing_engine_updates_instrumentation_logger_and_config() -> None:
    engine = create_engine("mysql+mysqldb://u:p@localhost:3306/app?charset=utf8mb4")
    first_logger = MemoryLogger()
    second_logger = MemoryLogger()

    MLPDatabase(engine, logger=first_logger, logging_config=LoggingConfig(log_successful_queries=True))
    first_state = _INSTRUMENTED_ENGINES[engine]

    MLPDatabase(engine, logger=second_logger, logging_config=LoggingConfig(log_successful_queries=False))
    second_state = _INSTRUMENTED_ENGINES[engine]

    assert second_state is first_state
    assert second_state.logger is second_logger
    assert second_state.config.log_successful_queries is False
