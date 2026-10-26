import json, os
import pyarrow as pa, pyarrow.parquet as pq


def _make(tmp_path, name, rows):
    p = tmp_path / name
    pq.write_table(pa.Table.from_pylist(rows), p)
    return str(p)


def test_manifest_counts_rows_and_files(tmp_path):
    from modules.pipeline_utils import build_manifest
    a = _make(tmp_path, "laps_1_extracted.parquet", [{"x": 1}, {"x": 2}])
    b = _make(tmp_path, "pit_1_extracted.parquet", [{"x": 1}])
    path = build_manifest([a, b], str(tmp_path), "manual__2026-01-01T00:00:00+00:00")
    m = json.load(open(path))
    assert m["file_count"] == 2 and m["total_rows"] == 3
    assert ":" not in os.path.basename(path)          # run ids contain ':' which Windows paths reject


def test_manifest_survives_corrupt_file(tmp_path):
    from modules.pipeline_utils import build_manifest
    bad = tmp_path / "laps_2_extracted.parquet"; bad.write_text("not parquet")
    m = json.load(open(build_manifest([str(bad)], str(tmp_path), "r1")))
    assert m["files"][0]["rows"] is None


def test_notify_failure_never_raises(monkeypatch):
    from modules.pipeline_utils import notify_failure
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://127.0.0.1:9/unreachable")
    notify_failure({"task_instance": None, "run_id": "r", "exception": Exception("boom")})


def test_gcs_loader_skips_unchanged_and_uploads_new(tmp_path):
    from modules.gcs_loader import upload_raw_files, object_name_for
    same = _make(tmp_path, "laps_1_extracted.parquet", [{"x": 1}])
    new = _make(tmp_path, "car_data_1_extracted.parquet", [{"x": 1}])

    class Fake:
        up = []
        def exists(self, bucket_name, object_name): return object_name.endswith("laps_1_extracted.parquet")
        def get_size(self, bucket_name, object_name): return os.path.getsize(same)
        def upload(self, bucket_name, object_name, filename): Fake.up.append(object_name)

    out = upload_raw_files([same, new], bucket="b", hook=Fake())
    assert out == ["gs://b/raw/car_data/car_data_1_extracted.parquet"] and Fake.up == ["raw/car_data/car_data_1_extracted.parquet"]
    assert object_name_for("/x/championship_drivers_9_extracted.parquet") == "raw/championship_drivers/championship_drivers_9_extracted.parquet"
