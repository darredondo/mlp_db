from __future__ import annotations

import pytest

from mlp.db import DatabaseConfig, LoggingConfig, MLPConfigurationError, PoolConfig


def test_database_config_validates_url() -> None:
    with pytest.raises(MLPConfigurationError):
        DatabaseConfig(url="")

    with pytest.raises(MLPConfigurationError):
        DatabaseConfig(url="not a url")


def test_database_config_rejects_ambiguous_manual_bool() -> None:
    with pytest.raises(MLPConfigurationError):
        DatabaseConfig(url="mysql+mysqldb://u:p@localhost:3306/app?charset=utf8mb4", echo="false")  # type: ignore[arg-type]


def test_database_config_from_env_prefers_db_url() -> None:
    config = DatabaseConfig.from_env(
        environ={
            "DB_URL": "  mysql+mysqldb://u:p@localhost:3306/app?charset=utf8mb4  ",
            "DB_DIALECT": "postgresql",
            "DB_HOST": "ignored",
            "DB_NAME": "ignored",
        }
    )

    assert config.url == "mysql+mysqldb://u:p@localhost:3306/app?charset=utf8mb4"


def test_database_config_from_env_treats_blank_db_url_as_missing() -> None:
    with pytest.raises(MLPConfigurationError, match="DB_DIALECT"):
        DatabaseConfig.from_env(environ={"DB_URL": "   "})


def test_database_config_from_env_builds_sqlalchemy_url() -> None:
    config = DatabaseConfig.from_env(
        environ={
            "DB_DIALECT": "mysql",
            "DB_DRIVER": "mysqldb",
            "DB_HOST": "127.0.0.1",
            "DB_PORT": "3306",
            "DB_NAME": "app",
            "DB_USERNAME": "user",
            "DB_PASS": "p a s s",
            "DB_CHARSET": "utf8mb4",
        }
    )

    assert config.url == "mysql+mysqldb://user:p+a+s+s@127.0.0.1:3306/app?charset=utf8mb4"


def test_database_config_from_env_validates_port() -> None:
    with pytest.raises(MLPConfigurationError, match="invalid DB_PORT"):
        DatabaseConfig.from_env(
            environ={
                "DB_DIALECT": "mysql",
                "DB_DRIVER": "mysqldb",
                "DB_HOST": "127.0.0.1",
                "DB_PORT": "abc",
                "DB_NAME": "app",
            }
        )

    with pytest.raises(MLPConfigurationError, match="invalid DB_PORT"):
        DatabaseConfig.from_env(
            environ={
                "DB_DIALECT": "mysql",
                "DB_DRIVER": "mysqldb",
                "DB_HOST": "127.0.0.1",
                "DB_PORT": "0",
                "DB_NAME": "app",
            }
        )


def test_pool_and_logging_config_validation() -> None:
    with pytest.raises(MLPConfigurationError):
        PoolConfig(pool_size=-1)
    with pytest.raises(MLPConfigurationError):
        PoolConfig(pool_size=True)  # type: ignore[arg-type]
    with pytest.raises(MLPConfigurationError):
        PoolConfig(max_overflow=True)  # type: ignore[arg-type]
    with pytest.raises(MLPConfigurationError):
        PoolConfig(pool_timeout_seconds=0)
    with pytest.raises(MLPConfigurationError):
        LoggingConfig(slow_query_threshold_ms=0)
    with pytest.raises(MLPConfigurationError):
        LoggingConfig(log_parameters="yes")  # type: ignore[arg-type]
    with pytest.raises(MLPConfigurationError):
        LoggingConfig(max_statement_length=True)  # type: ignore[arg-type]
