import pathlib, shutil, sys
import pytest
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT.parent / "analytics"))


@pytest.fixture(scope="session")
def warehouse_path():
    """The warehouse built by `make ci-test` / the CI job (F1_DUCKDB_PATH), or skip when it does not exist."""
    import os
    p = os.environ.get("F1_DUCKDB_PATH")
    if not p or not pathlib.Path(p).exists():
        pytest.skip("set F1_DUCKDB_PATH to a warehouse built from the synthetic fixtures")
    return p


import pytest


@pytest.fixture(autouse=True)
def _restore_data_module():
    """Tests monkeypatch f1_dash.data inside Streamlit scripts (missing telemetry, missing laps). Restore the module so later tests see the real functions."""
    from f1_dash import data
    saved = dict(vars(data))
    yield
    vars(data).clear()
    vars(data).update(saved)
