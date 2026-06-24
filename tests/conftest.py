# tests/conftest.py
"""
Test bootstrap:
- ensure_fiscrape_logger: guarantees fiscrape_logger.logger exists (stubbed if missing)
- patch_services_ticker: safe helper to patch yfinance.Ticker only inside fiscrape_core.services
"""

from __future__ import annotations

import logging
import sys
import types

import pytest


def _mk_stub_logger() -> logging.Logger:
    """Create a simple logger that mimics fiscrape_logger.logger shape."""
    logger = logging.getLogger("fiscrape_logger")
    # Avoid duplicate handlers across multiple test runs
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


@pytest.fixture(autouse=True)
def ensure_fiscrape_logger():
    """
    Autouse: before each test, ensure importing `fiscrape_logger` works and
    yields a `logger` attribute. If not present, inject a stub module.
    """
    try:
        import fiscrape_logger  # type: ignore

        if getattr(fiscrape_logger, "logger", None) is None:
            raise ImportError("fiscrape_logger.logger missing")
    except Exception:
        mod = types.ModuleType("fiscrape_logger")
        mod.logger = _mk_stub_logger()
        sys.modules["fiscrape_logger"] = mod


@pytest.fixture
def patch_services_ticker(monkeypatch):
    """
    Patch helper to replace yfinance.Ticker only inside fiscrape_core.services.

    Usage:
        def test_something(patch_services_ticker):
            class FakeT: ...
            patch_services_ticker(FakeT)
            # call services.get_history / services.get_live_quote and assert...
    """
    import fiscrape_core.services as services

    def _patch(fake_cls):
        monkeypatch.setattr(services.yf, "Ticker", lambda *a, **k: fake_cls())

    return _patch
