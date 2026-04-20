from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlencode

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
LOGGER_SRC = ROOT.parent / "mlp_logger" / "src"
LOCAL_TEST_ENV = ROOT / ".env.test.local"

for path in (SRC, LOGGER_SRC):
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

from mlp.logger import ComponentLoggerInterface  # noqa: E402


def load_local_test_env(path: Path = LOCAL_TEST_ENV) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        os.environ.setdefault(key, _unquote_env_value(value.strip()))


def resolve_mysql_test_url() -> str | None:
    load_local_test_env()
    url = _resolve_profile_test_url("MLP_DB_MYSQL_TEST", legacy_prefix="MLP_DB_TEST")
    if url is not None:
        os.environ.setdefault("MLP_DB_MYSQL_TEST_URL", url)
        os.environ.setdefault("MLP_DB_TEST_URL", url)
    return url


def resolve_postgres_test_url() -> str | None:
    load_local_test_env()
    return _resolve_profile_test_url("MLP_DB_POSTGRES_TEST")


def _resolve_profile_test_url(prefix: str, *, legacy_prefix: str | None = None) -> str | None:
    explicit_url = _get_test_env(f"{prefix}_URL")
    if explicit_url:
        return explicit_url
    if legacy_prefix is not None:
        legacy_url = _get_test_env(f"{legacy_prefix}_URL")
        if legacy_url:
            return legacy_url

    dialect = _get_test_env(f"{prefix}_DIALECT")
    driver = _get_test_env(f"{prefix}_DRIVER")
    host = _get_test_env(f"{prefix}_HOST")
    database = _get_test_env(f"{prefix}_NAME")
    username = _get_test_env(f"{prefix}_USERNAME")
    password = _get_test_env(f"{prefix}_PASS")
    port = _get_test_env(f"{prefix}_PORT")
    charset = _get_test_env(f"{prefix}_CHARSET")

    if legacy_prefix is not None:
        dialect = dialect or _get_test_env(f"{legacy_prefix}_DIALECT")
        driver = driver or _get_test_env(f"{legacy_prefix}_DRIVER")
        host = host or _get_test_env(f"{legacy_prefix}_HOST")
        database = database or _get_test_env(f"{legacy_prefix}_NAME")
        username = username or _get_test_env(f"{legacy_prefix}_USERNAME")
        password = password or _get_test_env(f"{legacy_prefix}_PASS")
        port = port or _get_test_env(f"{legacy_prefix}_PORT")
        charset = charset or _get_test_env(f"{legacy_prefix}_CHARSET")

    if not dialect or not host or not database:
        return None
    return _build_test_url(
        dialect=dialect,
        driver=driver,
        host=host,
        database=database,
        username=username,
        password=password,
        port=port,
        charset=charset,
    )


def _build_test_url(
    *,
    dialect: str,
    driver: str | None,
    host: str,
    database: str,
    username: str | None,
    password: str | None,
    port: str | None,
    charset: str | None,
) -> str:
    driver_part = f"+{driver}" if driver else ""
    auth = ""
    if username:
        auth = quote_plus(username)
        if password:
            auth += f":{quote_plus(password)}"
        auth += "@"
    port_part = f":{port}" if port else ""
    query = f"?{urlencode({'charset': charset})}" if charset else ""
    return f"{dialect}{driver_part}://{auth}{host}{port_part}/{quote_plus(database)}{query}"


def _get_test_env(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _unquote_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


MYSQL_TEST_URL = resolve_mysql_test_url()
POSTGRES_TEST_URL = resolve_postgres_test_url()


class MemoryLogger(ComponentLoggerInterface):
    def __init__(self, debug_enabled: bool = True) -> None:
        self.debug_enabled = debug_enabled
        self.records: list[dict[str, Any]] = []

    def is_enabled(self, level: str) -> bool:
        return self.debug_enabled or level != "DEBUG"

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
        self.records.append(
            {"kind": "event", "component": component, "event": event, "message": message, "level": level, "context": context or {}}
        )

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
        self.records.append(
            {
                "kind": "failure",
                "component": component,
                "event": event,
                "message": message,
                "exception": exception,
                "context": context or {},
            }
        )
