from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote_plus, urlencode

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from .exceptions import MLPConfigurationError


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    url: str
    echo: bool = False
    pool_pre_ping: bool = True
    pool_recycle_seconds: int | None = None

    def __post_init__(self) -> None:
        _require_bool("echo", self.echo)
        _require_bool("pool_pre_ping", self.pool_pre_ping)
        if not isinstance(self.url, str) or not self.url.strip():
            raise MLPConfigurationError("database URL must be a non-empty string")
        if self.pool_recycle_seconds is not None:
            _require_positive_int("pool_recycle_seconds", self.pool_recycle_seconds)
        try:
            make_url(self.url)
        except ArgumentError as exc:
            raise MLPConfigurationError("database URL is not a valid SQLAlchemy URL") from exc

    @classmethod
    def from_env(cls, prefix: str = "DB_", environ: Mapping[str, str] | None = None) -> DatabaseConfig:
        env = os.environ if environ is None else environ
        raw_url = env.get(f"{prefix}URL")
        url = raw_url.strip() if raw_url is not None else None
        if url:
            return cls(
                url=url,
                echo=_parse_bool(env.get(f"{prefix}ECHO"), default=False),
                pool_pre_ping=_parse_bool(env.get(f"{prefix}POOL_PRE_PING"), default=True),
                pool_recycle_seconds=_parse_optional_int(env.get(f"{prefix}POOL_RECYCLE_SECONDS")),
            )

        dialect = _required_env(env, f"{prefix}DIALECT")
        driver = _optional_env(env, f"{prefix}DRIVER")
        host = _required_env(env, f"{prefix}HOST")
        database = _required_env(env, f"{prefix}NAME")
        username = _optional_env(env, f"{prefix}USERNAME")
        password = _optional_env(env, f"{prefix}PASS")
        port = _parse_optional_port(env.get(f"{prefix}PORT"), name=f"{prefix}PORT")
        charset = _optional_env(env, f"{prefix}CHARSET")

        driver_part = f"+{driver}" if driver else ""
        auth = ""
        if username is not None:
            auth = quote_plus(username)
            if password is not None:
                auth += f":{quote_plus(password)}"
            auth += "@"
        port_part = f":{port}" if port else ""
        query = f"?{urlencode({'charset': charset})}" if charset else ""
        built_url = f"{dialect}{driver_part}://{auth}{host}{port_part}/{quote_plus(database)}{query}"
        return cls(
            url=built_url,
            echo=_parse_bool(env.get(f"{prefix}ECHO"), default=False),
            pool_pre_ping=_parse_bool(env.get(f"{prefix}POOL_PRE_PING"), default=True),
            pool_recycle_seconds=_parse_optional_int(env.get(f"{prefix}POOL_RECYCLE_SECONDS")),
        )


@dataclass(frozen=True, slots=True)
class PoolConfig:
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        _require_non_negative_int("pool_size", self.pool_size)
        if not isinstance(self.max_overflow, int) or isinstance(self.max_overflow, bool):
            raise MLPConfigurationError("max_overflow must be an int")
        _require_positive_number("pool_timeout_seconds", self.pool_timeout_seconds)


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    log_successful_queries: bool = False
    slow_query_threshold_ms: float | None = 500.0
    log_pool_events: bool = True
    log_parameters: bool = False
    max_statement_length: int = 2000

    def __post_init__(self) -> None:
        _require_bool("log_successful_queries", self.log_successful_queries)
        _require_bool("log_pool_events", self.log_pool_events)
        _require_bool("log_parameters", self.log_parameters)
        _require_positive_int("max_statement_length", self.max_statement_length)
        if self.slow_query_threshold_ms is not None:
            _require_positive_number("slow_query_threshold_ms", self.slow_query_threshold_ms)


def _required_env(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if value is None:
        raise MLPConfigurationError(f"{name} is required when DB_URL is not set")
    stripped = value.strip()
    if stripped == "":
        raise MLPConfigurationError(f"{name} is required when DB_URL is not set")
    return stripped


def _optional_env(env: Mapping[str, str], name: str) -> str | None:
    value = env.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _parse_bool(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise MLPConfigurationError(f"invalid boolean environment value: {value!r}")


def _parse_optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise MLPConfigurationError(f"invalid integer environment value: {value!r}") from exc


def _parse_optional_port(value: str | None, *, name: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise MLPConfigurationError(f"invalid {name}: {value!r}") from exc
    if parsed <= 0:
        raise MLPConfigurationError(f"invalid {name}: must be a positive integer")
    return parsed


def _require_bool(name: str, value: bool) -> None:
    if not isinstance(value, bool):
        raise MLPConfigurationError(f"{name} must be a bool")


def _require_non_negative_int(name: str, value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise MLPConfigurationError(f"{name} must be a non-negative int")


def _require_positive_int(name: str, value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise MLPConfigurationError(f"{name} must be a positive int")


def _require_positive_number(name: str, value: float | int) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise MLPConfigurationError(f"{name} must be a positive number")
