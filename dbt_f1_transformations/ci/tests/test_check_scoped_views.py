import pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from check_scoped_views import check


def model(name, materialized, parents=(), code="select 1", tags=()):
    return f"model.p.{name}", {"resource_type": "model", "name": name, "raw_code": code, "tags": list(tags),
                                "config": {"materialized": materialized}, "depends_on": {"nodes": [f"model.p.{p}" for p in parents]}}


def manifest(*models):
    return {"nodes": dict(models)}


SCOPED = "select * from x where {{ in_scope('session_key') }}"


def good():
    return [model("stg_car_data", "view", code=SCOPED, tags=["telemetry_group"]),
            model("fct_telemetry", "incremental", ["stg_car_data"], tags=["telemetry_group"]),
            model("fct_telemetry_lap_summary", "incremental", ["fct_telemetry"], tags=["telemetry_group"]),
            model("fct_other", "table", ["stg_laps"])]


def test_the_intended_design_passes():
    assert check(manifest(*good())) == []


def test_a_table_reading_a_scoped_view_is_flagged():
    bad = good() + [model("fct_bad", "table", ["stg_car_data"], tags=["telemetry_group"])]
    assert any("fct_bad" in v and "scoped model stg_car_data" in v for v in check(manifest(*bad)))


def test_a_table_reading_through_an_intermediate_view_is_flagged():
    bad = good() + [model("v_mid", "view", ["stg_car_data"], tags=["telemetry_group"]),
                    model("fct_bad", "table", ["v_mid"], tags=["telemetry_group"])]
    assert any("fct_bad" in v for v in check(manifest(*bad)))


def test_a_table_reading_a_persisted_telemetry_table_is_fine():
    ok = good() + [model("fct_report", "table", ["fct_telemetry_lap_summary"], tags=["telemetry_group"])]
    assert check(manifest(*ok)) == []


def test_a_scoped_model_that_is_a_table_is_flagged():
    assert any("TABLE" in v for v in check(manifest(model("stg_x", "table", code=SCOPED, tags=["telemetry_group"]))))


def test_an_untagged_reader_of_the_group_is_flagged():
    bad = good() + [model("fct_new", "incremental", ["fct_telemetry"])]
    assert any("fct_new" in v and "not tagged" in v for v in check(manifest(*bad)))


def test_a_scoped_model_without_the_tag_is_flagged():
    assert any("not tagged" in v for v in check(manifest(model("stg_location", "view", code=SCOPED))))


def test_a_table_using_the_incremental_load_filter_is_flagged():
    bad = good() + [model("fct_bad", "table", ["fct_telemetry"], code="select * from x where {{ telemetry_load_filter('session_key') }}",
                          tags=["telemetry_group"])]
    assert any("fct_bad" in v and "TABLE" in v for v in check(manifest(*bad)))
