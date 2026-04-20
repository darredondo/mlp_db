from __future__ import annotations

import pytest
from sqlalchemy import create_engine

from mlp.db import DatabaseConfig, LoggingConfig, MLPDatabase


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
