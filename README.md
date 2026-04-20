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
    DatabaseConfig(url="mysql+pymysql://user:pass@localhost:3306/app?charset=utf8mb4"),
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
  "mlp-logging @ git+https://github.com/darredondo/mlp_logger.git@v0.2.0",
]
```

Database drivers are optional extras so applications can choose their own:

```bash
pip install "mlp-db[pymysql]"
pip install "mlp-db[mysqlclient]"
pip install "mlp-db[postgres]"
```

Supported URLs are normal SQLAlchemy DSNs, for example:

```text
mysql+pymysql://user:pass@host:3306/db?charset=utf8mb4
mysql+mysqldb://user:pass@host:3306/db?charset=utf8mb4
postgresql+psycopg://user:pass@host:5432/db
```

`mlp-db` does not force one MySQL driver in code.

## Configuration

Use frozen dataclasses:

```python
from mlp.db import DatabaseConfig, LoggingConfig, PoolConfig

config = DatabaseConfig(
    url="mysql+pymysql://user:pass@localhost:3306/app?charset=utf8mb4",
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

The convenience methods keep common code short but still return SQLAlchemy Core objects:

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

Successful queries are not logged by default. Slow queries are logged when `duration_ms >= slow_query_threshold_ms`. Query errors are always logged when a logger exists. Pool events can be disabled with `LoggingConfig(log_pool_events=False)`.

Parameters are not logged by default. If `log_parameters=True`, values are sanitized and truncated before being added to the logging context. Statements are truncated using `max_statement_length`.

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

```python
lock_conn = db.open_dedicated_connection()
try:
    lock_conn.execute(text("SELECT GET_LOCK(:name, :timeout)"), {"name": "job", "timeout": 10})
finally:
    lock_conn.close()
```

This connection does not consume `pool_size` or `max_overflow` from the main application pool. It still counts against the database server's total connection limit, so callers must close it explicitly.

## Testing

Run unit tests:

```bash
python -m pytest
```

MySQL integration tests run only when `MLP_DB_TEST_URL` is set:

```bash
set MLP_DB_TEST_URL=mysql+pymysql://user:pass@127.0.0.1:3306/test_db?charset=utf8mb4
python -m pytest tests/test_mysql_integration.py
```

The integration tests cover `SELECT 1`, helpers, DML with parameters, commit, rollback, `READ COMMITTED`, SQL error translation, unique violation translation, pool logging, slow query logging, and dedicated connections.

