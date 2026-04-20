from __future__ import annotations

from sqlalchemy import exc as sa_exc


class MLPDatabaseError(Exception):
    """Base exception for mlp.db."""


class MLPConfigurationError(MLPDatabaseError):
    """Raised when database configuration is invalid."""


class MLPConnectionError(MLPDatabaseError):
    """Raised when opening or maintaining a database connection fails."""


class MLPQueryError(MLPDatabaseError):
    """Raised when a SQL statement fails."""


class MLPIntegrityError(MLPQueryError):
    """Raised for database integrity violations."""


class MLPOperationalError(MLPQueryError):
    """Raised for operational database errors."""


class MLPTransactionError(MLPDatabaseError):
    """Raised when transaction begin, commit or rollback fails."""


class MLPTimeoutError(MLPOperationalError):
    """Raised when the database pool or driver times out."""


def translate_sqlalchemy_error(exc: sa_exc.SQLAlchemyError) -> MLPDatabaseError:
    if isinstance(exc, sa_exc.TimeoutError):
        return MLPTimeoutError(str(exc))
    if isinstance(exc, sa_exc.IntegrityError):
        return MLPIntegrityError(str(exc))
    if isinstance(exc, sa_exc.DBAPIError) and exc.connection_invalidated:
        return MLPConnectionError(str(exc))
    if isinstance(exc, sa_exc.OperationalError):
        return MLPOperationalError(str(exc))
    return MLPQueryError(str(exc))
