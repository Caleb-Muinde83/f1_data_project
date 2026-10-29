import importlib.util, pathlib
import pyarrow as pa

SPEC = importlib.util.spec_from_file_location("bq_loader", pathlib.Path(__file__).resolve().parents[2] / "scripts" / "load_raw_to_bigquery.py")
bq = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(bq)


def test_numbers_and_text_across_files_become_string_so_no_data_is_lost():
    a = pa.table({"driver_number": [1, 2], "gap": [0.5, 1.2]})
    b = pa.table({"driver_number": [1], "gap": ["+1 LAP"]})
    schema = bq.unified_schema([a, b])
    assert schema.field("gap").type == pa.string() and schema.field("driver_number").type == pa.int64()
    merged = pa.concat_tables([bq.conform(a, schema), bq.conform(b, schema)])
    assert merged["gap"].to_pylist() == ["0.5", "1.2", "+1 LAP"]


def test_int_and_float_widen_to_float_and_null_only_columns_adopt_other_type():
    a = pa.table({"x": [1, 2], "y": pa.array([None, None], type=pa.null())})
    b = pa.table({"x": [1.5], "y": ["a"]})
    s = bq.unified_schema([a, b])
    assert s.field("x").type == pa.float64() and s.field("y").type == pa.string()


def test_missing_columns_are_filled_with_nulls_and_all_null_defaults_to_string():
    a = pa.table({"x": [1]}); b = pa.table({"x": [2], "extra": ["z"]})
    s = bq.unified_schema([a, b]); merged = pa.concat_tables([bq.conform(a, s), bq.conform(b, s)])
    assert merged["extra"].to_pylist() == [None, "z"]
    assert bq.unify_types([pa.null()]) == pa.string()


def test_file_name_parsing_keeps_multiword_endpoints_and_ignores_manifests(tmp_path):
    for n in ["car_data_9158_extracted.parquet", "meetings_global_extracted.parquet", "session_result_9158_extracted.parquet", "radio_transcripts_9158_extracted.parquet"]:
        (tmp_path / n).write_bytes(b"x")
    (tmp_path / "_manifests").mkdir(); (tmp_path / "_manifests" / "run.json").write_text("{}")
    assert set(bq.group_files(str(tmp_path))) == {"car_data", "meetings", "session_result", "radio_transcripts"}
