from __future__ import annotations

from sqlalchemy.engine import make_url

from mlp.db import DatabaseConfig


def test_accepts_common_sqlalchemy_dsns() -> None:
    urls = [
        "mysql+pymysql://user:pass@host:3306/db?charset=utf8mb4",
        "mysql+mysqldb://user:pass@host:3306/db?charset=utf8mb4",
        "postgresql+psycopg://user:pass@host:5432/db",
    ]

    for url in urls:
        config = DatabaseConfig(url=url)
        assert make_url(config.url).drivername in {"mysql+pymysql", "mysql+mysqldb", "postgresql+psycopg"}
