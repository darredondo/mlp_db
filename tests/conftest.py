from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
LOGGER_SRC = ROOT.parent / "mlp_logger" / "src"

for path in (SRC, LOGGER_SRC):
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

from mlp.logger import ComponentLoggerInterface  # noqa: E402


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
