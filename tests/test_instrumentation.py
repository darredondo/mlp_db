from __future__ import annotations

from mlp.db.instrumentation import sanitize_parameters, truncate_statement


def test_truncates_statement() -> None:
    assert truncate_statement("abcdef", 3) == "abc...<truncated>"


def test_does_not_log_parameters_by_default() -> None:
    assert sanitize_parameters({"password": "secret"}, enabled=False) is None


def test_sanitizes_parameters_only_when_enabled() -> None:
    assert sanitize_parameters({"value": "abc"}, enabled=True) == {"value": "abc"}
