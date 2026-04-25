# mlp-db

`mlp-db` is the shared database foundation for MLP Python components.

It is intentionally small: it builds and instruments SQLAlchemy Core engines, translates SQLAlchemy exceptions into `mlp.db` exceptions, provides transaction helpers, and exposes a dedicated connection path for long-lived locks. It does not include an ORM, business repositories, migrations, task queues, a process locker, temp object storage, or manual SQL escaping helpers.

## Why SQLAlchemy Core

MLP services should be able to use SQLAlchemy's tested connection pooling, dialect handling, transactions, SQL expression language, and driver support directly. `mlp-db` adds MLP conventions around configuration, logging, error translation, and dedicated lock connections without hiding the underlying `Engine` or `Connection`.

```python
from sqlalchemy import text

from mlp.db import DatabaseConfig, LoggingConfig, MLPDatabase
from mlp.logger import ConsoleLogger

logger = ConsoleLogger(minimum_level="DEBUG")

db = MLPDatabase.from_config(
    DatabaseConfig(url="mysql+mysqldb://user:pass@localhost:3306/app?charset=utf8mb4"),
    logger=logger,
    logging_config=LoggingConfig(
        log_successful_queries=False,
        slow_query_threshold_ms=250,
        log_pool_events=True,
    ),
)

row = db.fetch_one("SELECT 1 AS ok")

with db.begin(isolation_level="READ COMMITTED") as conn:
    conn.execute(text("INSERT INTO demo (name) VALUES (:name)"), {"name": "abc"})

dedicated = db.open_dedicated_connection()
try:
    dedicated.execute(text("SELECT 1"))
finally:
    dedicated.close()
```

The raw SQLAlchemy engine is available when a consumer needs the full Core API:

```python
engine = db.engine
```

## Installation

Core dependency:

```toml
dependencies = [
  "SQLAlchemy>=2.0,<3.0",
  "mlp-logging @ git+https://github.com/darredondo/mlp_logger.git@2910132",
]
```

The current MLP MySQL runtime profile is **MySQL with mysqlclient**:

```bash
pip install "mlp-db[mysqlclient]"
```

That profile uses SQLAlchemy URLs with the `mysql+mysqldb` driver name:

```text
mysql+mysqldb://user:pass@host:3306/db?charset=utf8mb4
```

PostgreSQL is also a supported integration profile using `psycopg`:

```bash
pip install "mlp-db[postgres]"
```

That profile uses:

```text
postgresql+psycopg://user:pass@host:5432/db
```

The package code itself stays SQLAlchemy Core based and does not hard-code a driver. The following extra is available for consumers that intentionally need the pure-Python MySQL driver, but it is not the MLP MySQL test profile:

```bash
pip install "mlp-db[pymysql]"
```

Examples of accepted SQLAlchemy DSNs:

```text
mysql+mysqldb://user:pass@host:3306/db?charset=utf8mb4
postgresql+psycopg://user:pass@host:5432/db
mysql+pymysql://user:pass@host:3306/db?charset=utf8mb4
```

## Configuration

Use frozen dataclasses:

```python
from mlp.db import DatabaseConfig, LoggingConfig, PoolConfig

config = DatabaseConfig(
    url="mysql+mysqldb://user:pass@localhost:3306/app?charset=utf8mb4",
    echo=False,
    pool_pre_ping=True,
    pool_recycle_seconds=3600,
)

pool = PoolConfig(
    pool_size=5,
    max_overflow=10,
    pool_timeout_seconds=30.0,
)

logging = LoggingConfig(
    log_successful_queries=False,
    slow_query_threshold_ms=500.0,
    log_pool_events=True,
    log_parameters=False,
)
```

`DatabaseConfig.from_env(prefix="DB_")` reads `DB_URL` first. If `DB_URL` is not set, it can build a URL from:

```text
DB_DIALECT
DB_DRIVER
DB_HOST
DB_PORT
DB_NAME
DB_USERNAME
DB_PASS
DB_CHARSET
```

`DB_URL` always wins.

## Query Helpers

The convenience methods keep common code short while still using SQLAlchemy Core. Use `execute()` for DDL/DML or statements where you do not need to consume rows after the call returns. For queries that return rows, prefer `fetch_one()` or `fetch_all()` because they consume the result while the connection is still open.

```python
db.execute("INSERT INTO demo (name) VALUES (:name)", {"name": "abc"})
row = db.fetch_one("SELECT 1 AS ok")
rows = db.fetch_all("SELECT id, name FROM demo ORDER BY id")
```

If a statement is a string, `mlp-db` converts it with `sqlalchemy.text()`. You can pass any SQLAlchemy Core executable directly.

## Transactions

`db.begin()` opens a connection, starts a transaction, commits on success, and rolls back on error:

```python
with db.begin(isolation_level="READ COMMITTED") as conn:
    conn.execute(text("UPDATE accounts SET status = :status"), {"status": "active"})
```

No global autocommit mode is enabled. `AUTOCOMMIT` can be passed explicitly as an isolation level only when the SQLAlchemy dialect supports it.

## Logging

Instrumentation uses SQLAlchemy events and `mlp_logger` with component `mlp_db`.

Emitted events include:

```text
db_query
db_slow_query
db_query_error
db_pool_connect
db_pool_checkout
db_pool_checkin
db_pool_invalidate
db_transaction_begin
db_transaction_commit
db_transaction_rollback
```

Successful queries are not logged by default. Slow queries are logged when `duration_ms >= slow_query_threshold_ms`. Query errors are always logged when a logger exists.

Pool events and transaction events are disabled by default. Enable them explicitly when you need a very verbose diagnostic run:

```python
from mlp.db import LoggingConfig

logging_config = LoggingConfig(
    log_pool_events=True,
    log_transaction_events=True,
)
```

Parameters are not logged by default. If `log_parameters=True`, values are sanitized and truncated before being added to the logging context. Statements are truncated using `max_statement_length`.

Instrumentation is scoped to the SQLAlchemy `Engine`. If multiple `MLPDatabase` wrappers share the same `Engine`, they share instrumentation state; constructing a later wrapper updates the logger and logging config used by that engine.

## Exceptions

Public `MLPDatabase` methods translate SQLAlchemy exceptions to `mlp.db` exceptions:

```python
from mlp.db import MLPDatabaseError, MLPIntegrityError, MLPQueryError

try:
    db.execute("INSERT INTO demo (code) VALUES (:code)", {"code": "A"})
except MLPIntegrityError:
    ...
except MLPQueryError:
    ...
except MLPDatabaseError:
    ...
```

Exception hierarchy:

```text
MLPDatabaseError
MLPConfigurationError
MLPConnectionError
MLPQueryError
MLPIntegrityError
MLPOperationalError
MLPTransactionError
MLPTimeoutError
```

The original SQLAlchemy exception is preserved as the cause with `raise translated from exc`.

## Dedicated Connections For Long Locks

`open_dedicated_connection()` opens a connection from an internal `NullPool` engine using the same database URL. This is meant for future long-lived process locks in `mlp_tasks`.

`mlp-db` only provides the dedicated connection. The lock primitive itself depends on the database engine.

MySQL example:

```python
lock_conn = db.open_dedicated_connection()
try:
    lock_conn.execute(text("SELECT GET_LOCK(:name, :timeout)"), {"name": "job", "timeout": 10})
finally:
    lock_conn.close()
```

PostgreSQL example:

```python
lock_conn = db.open_dedicated_connection()
try:
    lock_conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": 12345})
    # Or use pg_try_advisory_lock(:key) if the caller should decide what to do when the lock is busy.
finally:
    lock_conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": 12345})
    lock_conn.close()
```

This connection does not consume `pool_size` or `max_overflow` from the main application pool. It still counts against the database server's total connection limit, so callers must close it explicitly.

## Testing

Run unit tests:

```bash
python -m pytest
```

MySQL integration tests run only when `MLP_DB_MYSQL_TEST_URL` or compatible local settings are present. The test URL must use `mysqlclient`, which SQLAlchemy names `mysql+mysqldb`:

```bash
set MLP_DB_MYSQL_TEST_URL=mysql+mysqldb://user:pass@127.0.0.1:3306/test_db?charset=utf8mb4
python -m pytest tests/test_mysql_integration.py
```

For PostgreSQL, use `psycopg`, which SQLAlchemy names `postgresql+psycopg`:

```bash
set MLP_DB_POSTGRES_TEST_URL=postgresql+psycopg://user:pass@127.0.0.1:5432/test_db
python -m pytest tests/test_postgres_integration.py
```

For local agent runs, you can also create an unversioned `.env.test.local` file at the repo root. `tests/conftest.py` reads it and builds profile-specific test URLs automatically when the full URLs are not already set:

```text
MLP_DB_MYSQL_TEST_DIALECT=mysql
MLP_DB_MYSQL_TEST_DRIVER=mysqldb
MLP_DB_MYSQL_TEST_HOST=127.0.0.1
MLP_DB_MYSQL_TEST_PORT=3306
MLP_DB_MYSQL_TEST_NAME=mlp_db_test
MLP_DB_MYSQL_TEST_USERNAME=mlp_db_test
MLP_DB_MYSQL_TEST_PASS=secret
MLP_DB_MYSQL_TEST_CHARSET=utf8mb4

MLP_DB_POSTGRES_TEST_DIALECT=postgresql
MLP_DB_POSTGRES_TEST_DRIVER=psycopg
MLP_DB_POSTGRES_TEST_HOST=127.0.0.1
MLP_DB_POSTGRES_TEST_PORT=5432
MLP_DB_POSTGRES_TEST_NAME=mlp_db_test
MLP_DB_POSTGRES_TEST_USERNAME=mlp_db_test
MLP_DB_POSTGRES_TEST_PASS=secret
```

`.env.test.local` is ignored by git. A safe template is available in `.env.test.example`.

The MySQL and PostgreSQL integration tests cover `SELECT 1`, helpers, DML with parameters, commit, rollback, `READ COMMITTED`, SQL error translation, unique violation translation, pool logging, slow query logging, and dedicated connections.
