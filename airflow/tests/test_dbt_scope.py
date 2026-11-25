import json
import pytest

from modules.dbt_scope import (
    ScopeError, build_dbt_scope, chunk_keys, flatten_results, plan_dbt_commands,
    radio_transcripts_exist, validate_session_keys,
)


def r(endpoint, session, status="written", path="x"):
    return {"endpoint": endpoint, "session_key": session, "path": path, "status": status}


def vars_of(step):
    argv = step["argv"]
    return json.loads(argv[argv.index("--vars") + 1]) if "--vars" in argv else {}


def plan(scope, **kw):
    kw.setdefault("target", "container")
    kw.setdefault("profiles_dir", "/p")
    return plan_dbt_commands(scope, **kw)


# ---------- validation ----------
def test_session_keys_are_validated_as_integers():
    assert validate_session_keys([9159, "9158", 9159]) == [9158, 9159]
    for bad in (["9158; DROP TABLE x"], [9158.5], [True], [0], [-3], ["abc"], [None], "9158", None):
        with pytest.raises(ScopeError):
            validate_session_keys(bad)


def test_chunking_keeps_order_and_rejects_bad_sizes():
    assert chunk_keys([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunk_keys([], 10) == []
    for bad in (0, -1, 2.5, "3", True):
        with pytest.raises(ScopeError):
            chunk_keys([1], bad)


def test_flatten_results_drops_none_and_nested_lists():
    a, b = r("laps", 1), r("pit", 2)
    assert flatten_results([a, None], [[b], None], []) == [a, b]


# ---------- scope builder ----------
def test_latest_run_scopes_to_the_one_written_session():
    results = [r("meetings", "global"), r("laps", 9158), r("car_data", 9158), r("pit", 9158, "empty", None)]
    s = build_dbt_scope(results, [9158])
    assert s["mode"] == "scoped" and s["keys"] == [9158] and s["chunks"] == [[9158]]
    assert s["endpoints"] == ["car_data", "laps", "meetings"]


def test_only_sessions_with_written_files_are_scoped_skip_existing():
    results = [r("laps", 1, "skipped"), r("car_data", 1, "skipped"), r("laps", 2), r("laps", 3, "empty", None)]
    s = build_dbt_scope(results, [1, 2, 3])
    assert s["keys"] == [2]


def test_sessions_outside_the_selection_never_enter_the_scope():
    s = build_dbt_scope([r("laps", 1), r("laps", 2)], [2])
    assert s["keys"] == [2]


def test_nothing_written_means_dbt_is_skipped():
    s = build_dbt_scope([r("laps", 1, "skipped"), r("pit", 1, "empty", None), None], [1])
    assert s["mode"] == "none" and s["keys"] == []


def test_only_global_files_written_rebuilds_non_telemetry_only():
    s = build_dbt_scope([r("meetings", "global")], [])
    assert s["mode"] == "scoped" and s["keys"] == [] and s["chunks"] == []


def test_radio_transcripts_alone_still_trigger_the_cheap_models():
    assert build_dbt_scope([], [1], non_telemetry_changed=True)["mode"] == "scoped"
    assert build_dbt_scope([], [1])["mode"] == "none"


def test_chunks_follow_chunk_size():
    results = [r("laps", k) for k in range(1, 26)]
    s = build_dbt_scope(results, list(range(1, 26)), chunk_size=10)
    assert [len(c) for c in s["chunks"]] == [10, 10, 5]


def test_dbt_scope_full_ignores_results_and_has_no_keys():
    s = build_dbt_scope([], [], dbt_scope="full")
    assert s["mode"] == "full" and s["keys"] == []


def test_full_refresh_needs_the_backfill_guard():
    with pytest.raises(ScopeError):
        build_dbt_scope([r("laps", 1)], [1], dbt_full_refresh=True)
    assert build_dbt_scope([], [], dbt_full_refresh=True, allow_large_backfill=True)["mode"] == "full_refresh"


def test_bad_parameters_are_rejected():
    with pytest.raises(ScopeError):
        build_dbt_scope([], [], dbt_scope="everything")
    with pytest.raises(ScopeError):
        build_dbt_scope([r("laps", "9158; x")], None)


# ---------- command planning ----------
def test_single_chunk_is_one_whole_graph_build_with_json_vars():
    steps = plan({"mode": "scoped", "keys": [9159, 9158]})
    assert len(steps) == 1
    argv = steps[0]["argv"]
    assert argv[:2] == ["dbt", "build"] and "--select" not in argv and "--exclude" not in argv
    assert argv[argv.index("--target") + 1] == "container" and argv[argv.index("--profiles-dir") + 1] == "/p"
    assert vars_of(steps[0]) == {"session_keys": [9158, 9159]}
    assert all(isinstance(a, str) for a in argv)          # a list of strings, never one shell string


def test_many_chunks_build_the_rest_first_then_telemetry_per_chunk():
    steps = plan({"mode": "scoped", "keys": list(range(1, 26))}, chunk_size=10)
    assert len(steps) == 4
    assert steps[0]["argv"][-2:] == ["--exclude", "tag:telemetry_group"] and "--vars" not in steps[0]["argv"]
    for i, expected in enumerate([list(range(1, 11)), list(range(11, 21)), list(range(21, 26))], 1):
        a = steps[i]["argv"]
        assert a[a.index("--select") + 1] == "tag:telemetry_group"
        assert vars_of(steps[i])["session_keys"] == expected


def test_scoped_without_keys_excludes_the_telemetry_group():
    steps = plan({"mode": "scoped", "keys": []})
    assert steps[0]["argv"][-2:] == ["--exclude", "tag:telemetry_group"]


def test_none_plans_nothing_and_full_modes_have_no_scope_var():
    assert plan({"mode": "none"}) == []
    full = plan({"mode": "full"})[0]
    assert "--vars" not in full["argv"] and "--full-refresh" not in full["argv"]
    assert "--full-refresh" in plan({"mode": "full_refresh"})[0]["argv"]


def test_radio_flag_is_set_only_when_requested():
    assert vars_of(plan({"mode": "full"}, radio_transcripts=True)[0]) == {"enable_radio_transcripts": True}
    assert vars_of(plan({"mode": "scoped", "keys": [5]}, radio_transcripts=True)[0]) == {
        "enable_radio_transcripts": True, "session_keys": [5]}
    assert "--vars" not in plan({"mode": "full"})[0]["argv"]


def test_radio_transcripts_exist_checks_the_glob(tmp_path):
    assert radio_transcripts_exist(str(tmp_path)) is False
    (tmp_path / "radio_transcripts_9158_extracted.parquet").write_bytes(b"x")
    assert radio_transcripts_exist(str(tmp_path)) is True


def test_unsafe_keys_in_a_scope_are_rejected_at_planning_time():
    with pytest.raises(ScopeError):
        plan({"mode": "scoped", "keys": ["1; rm -rf /"]})
