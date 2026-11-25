import types
import pytest

from modules.dbt_runner import DbtRunError, run_dbt_plan


class FakeRun:
    def __init__(self, codes):
        self.codes, self.calls = list(codes), []

    def __call__(self, argv, **kw):
        self.calls.append((argv, kw))
        return types.SimpleNamespace(returncode=self.codes.pop(0), stdout="ok", stderr="boom")


STEPS = [{"label": "a", "argv": ["dbt", "build", "--vars", '{"session_keys":[1]}']},
         {"label": "b", "argv": ["dbt", "build", "--select", "tag:telemetry_group"]}]


def test_runs_every_step_as_an_argument_list_without_a_shell():
    run = FakeRun([0, 0])
    out = run_dbt_plan(STEPS, cwd="/proj", profiles_dir="/p", scope={"mode": "scoped", "keys": [1]}, runner=run)
    assert [c[0] for c in run.calls] == [s["argv"] for s in STEPS]
    assert all(c[1]["shell"] is False and c[1]["cwd"] == "/proj" and c[1]["env"]["DBT_PROFILES_DIR"] == "/p" for c in run.calls)
    assert out["mode"] == "scoped" and out["session_keys"] == [1] and [s["returncode"] for s in out["steps"]] == [0, 0]


def test_a_failing_step_stops_the_run_and_names_the_command():
    run = FakeRun([0, 1])
    with pytest.raises(DbtRunError, match="step 2/2"):
        run_dbt_plan(STEPS, cwd="/proj", profiles_dir="/p", runner=run)
    run = FakeRun([2])
    with pytest.raises(DbtRunError):
        run_dbt_plan(STEPS, cwd="/proj", profiles_dir="/p", runner=run)
    assert len(run.calls) == 1                              # the second step never ran


def test_an_empty_plan_runs_nothing():
    run = FakeRun([])
    assert run_dbt_plan([], cwd="/proj", profiles_dir="/p", runner=run)["steps"] == [] and run.calls == []
