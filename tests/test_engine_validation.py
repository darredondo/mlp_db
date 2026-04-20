from __future__ import annotations

import pytest
from sqlalchemy import create_engine

from mlp.db import LoggingConfig, MLPDatabase


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
