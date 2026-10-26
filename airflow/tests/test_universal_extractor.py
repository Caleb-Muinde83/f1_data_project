import json
import pyarrow.parquet as pq
import pytest


def load():
    from modules import universal_extractor
    return universal_extractor


def test_sanitiser_stringifies_mixed_and_nested_columns():
    ue = load()
    rows = [{"gap": 1.2, "q": [1, 2]}, {"gap": "+1 LAP", "q": [3]}, {"gap": None, "q": None}]
    out = ue.sanitize_mixed_and_nested_columns(rows)
    assert out[0]["gap"] == "1.2" and out[1]["gap"] == "+1 LAP" and out[2]["gap"] is None
    assert json.loads(out[0]["q"]) == [1, 2]


def test_standard_extract_writes_parquet_with_metadata_and_no_tmp_left(fake_hook, tmp_path):
    ue = load()
    fake_hook.script["/laps"] = lambda p: [{"session_key": 9158, "lap_number": 1, "lap_duration": 90.1}]
    path = ue.extract_and_save_endpoint("laps", target_session=9158, run_id="run_x")
    t = pq.read_table(path)
    assert t.num_rows == 1
    assert {"_extracted_at_utc", "_source_endpoint", "_airflow_run_id"} <= set(t.column_names)
    assert t.to_pylist()[0]["_airflow_run_id"] == "run_x"
    assert not list((tmp_path / "raw").glob("*.tmp"))


def test_idempotent_same_file_name_overwritten(fake_hook, tmp_path):
    ue = load()
    fake_hook.script["/laps"] = lambda p: [{"session_key": 1, "lap_number": 1}]
    a = ue.extract_and_save_endpoint("laps", target_session=1)
    b = ue.extract_and_save_endpoint("laps", target_session=1)
    assert a == b and len(list((tmp_path / "raw").glob("*.parquet"))) == 1


def test_skip_existing_makes_no_api_call(fake_hook):
    ue = load()
    fake_hook.script["/laps"] = lambda p: [{"session_key": 1, "lap_number": 1}]
    ue.extract_and_save_endpoint("laps", target_session=1)
    fake_hook.calls.clear()
    path = ue.extract_and_save_endpoint("laps", target_session=1, skip_existing=True)
    assert path.endswith("laps_1_extracted.parquet") and fake_hook.calls == []


def test_404_is_skipped_gracefully(fake_hook):
    ue = load()
    def boom(p): raise RuntimeError("404 Client Error")
    fake_hook.script["/pit"] = boom
    assert ue.extract_and_save_endpoint("pit", target_session=5) is None


def test_422_on_heavy_endpoint_falls_back_to_time_windows(fake_hook):
    ue = load()
    fake_hook.script["/drivers"] = lambda p: [{"driver_number": 1}]
    fake_hook.script["/sessions"] = lambda p: [{"date_start": "2024-03-02T15:00:00+00:00", "date_end": "2024-03-02T15:40:00+00:00"}]
    def car(p):
        if "date>=" not in p:
            raise RuntimeError("422 Client Error: too large")
        return [{"session_key": 7, "driver_number": 1, "speed": 300, "date": p["date>="]}]
    fake_hook.script["/car_data"] = car
    path = ue.extract_and_save_endpoint("car_data", target_session=7)
    # 40 minutes / 15-minute windows = 3 windows
    assert pq.read_table(path).num_rows == 3


def test_429_retries_then_succeeds(fake_hook):
    ue = load()
    state = {"n": 0}
    def flaky(p):
        state["n"] += 1
        if state["n"] < 3: raise RuntimeError("429 Too Many Requests")
        return [{"session_key": 3, "x": 1}]
    fake_hook.script["/weather"] = flaky
    assert ue.extract_and_save_endpoint("weather", target_session=3) is not None and state["n"] == 3
