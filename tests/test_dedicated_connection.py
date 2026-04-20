from __future__ import annotations

from sqlalchemy import text

from mlp.db import DatabaseConfig, MLPDatabase, PoolConfig


def test_dedicated_connection_does_not_use_main_pool(tmp_path) -> None:
    db = MLPDatabase.from_config(
        DatabaseConfig(url=f"sqlite:///{tmp_path / 'dedicated.db'}"),
        pool_config=PoolConfig(pool_size=1, max_overflow=0, pool_timeout_seconds=0.1),
    )

    normal = db.connect()
    try:
        dedicated = db.open_dedicated_connection()
        try:
            assert dedicated.execute(text("SELECT 1 AS ok")).mappings().one()["ok"] == 1
        finally:
            dedicated.close()
    finally:
        normal.close()
