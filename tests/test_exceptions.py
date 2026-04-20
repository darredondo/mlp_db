from __future__ import annotations

from sqlalchemy import exc as sa_exc

from mlp.db.exceptions import (
    MLPConnectionError,
    MLPIntegrityError,
    MLPOperationalError,
    MLPQueryError,
    MLPTimeoutError,
    translate_sqlalchemy_error,
)


def test_translate_sqlalchemy_errors() -> None:
    assert isinstance(translate_sqlalchemy_error(sa_exc.TimeoutError("timeout")), MLPTimeoutError)
    assert isinstance(translate_sqlalchemy_error(sa_exc.IntegrityError("stmt", {}, Exception("bad"))), MLPIntegrityError)
    assert isinstance(translate_sqlalchemy_error(sa_exc.OperationalError("stmt", {}, Exception("bad"))), MLPOperationalError)
    assert isinstance(translate_sqlalchemy_error(sa_exc.SQLAlchemyError("bad")), MLPQueryError)


def test_connection_invalidated_dbapi_error_maps_to_connection_error() -> None:
    exc = sa_exc.DBAPIError.instance(
        "stmt",
        {},
        Exception("disconnect"),
        Exception,
        connection_invalidated=True,
    )

    assert isinstance(translate_sqlalchemy_error(exc), MLPConnectionError)
