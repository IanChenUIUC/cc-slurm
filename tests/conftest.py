"""Shared fixtures for the cc-slurm engine tests.

Two tiers:
  * `mock_run` — black-box: drive the real `pipeline.py` against a mocked state
    (see mockpipe.py). Primary tier.
  * `engine`   — white-box: the loaded `pipeline` module, for asserting on
    internal structures (`Engine`, `_dep_tokens`, ...) that stdout can't reveal.
"""
import importlib.util
import pathlib
import sys

import pytest

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

from mockpipe import mock_run as _mock_run   # noqa: E402


@pytest.fixture
def mock_run():
    """The mock-harness entrypoint. Each call runs in its own fresh temp workdir,
    so cases never contaminate each other."""
    return _mock_run


@pytest.fixture(scope="session")
def engine():
    """The loaded `pipeline` module (for `engine.Engine(spec_dict)` white-box tests)."""
    spec = importlib.util.spec_from_file_location("pipeline", REPO / "pipeline.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
