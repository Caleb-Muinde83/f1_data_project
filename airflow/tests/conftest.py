"""Test harness: lets the extractor be tested without Airflow installed or network access.
The real OpenF1Hook is replaced by FakeHook, which serves scripted responses per (endpoint, params)."""
import sys, types, pathlib
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))


class FakeHook:
    script = {}      # {endpoint: callable(params) -> list | raises}
    calls = []

    def __init__(self, *a, **k):
        pass

    def get_endpoint(self, endpoint, params=None):
        FakeHook.calls.append((endpoint, dict(params or {})))
        handler = FakeHook.script.get(endpoint)
        if handler is None:
            raise RuntimeError(f"404 Client Error: no script for {endpoint}")
        return handler(params or {})


@pytest.fixture(autouse=True)
def fake_hook(monkeypatch, tmp_path):
    mod = types.ModuleType("hooks.openf1_hook")
    mod.OpenF1Hook = FakeHook
    pkg = types.ModuleType("hooks")
    monkeypatch.setitem(sys.modules, "hooks", pkg)
    monkeypatch.setitem(sys.modules, "hooks.openf1_hook", mod)
    sys.modules.pop("modules.universal_extractor", None)
    monkeypatch.setenv("OPENF1_OUTPUT_PATH", str(tmp_path / "raw"))
    monkeypatch.setattr("time.sleep", lambda *_: None)
    FakeHook.script, FakeHook.calls = {}, []
    yield FakeHook
