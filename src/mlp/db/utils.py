from __future__ import annotations

from sqlalchemy import Connection, Executable


def delete_in_chunks(conn: Connection, statement: Executable, *, chunk_size: int) -> int:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    deleted = 0
    while True:
        result = conn.execute(statement, {"chunk_size": chunk_size})
        rowcount = result.rowcount if result.rowcount and result.rowcount > 0 else 0
        deleted += rowcount
        if rowcount < chunk_size:
            return deleted
