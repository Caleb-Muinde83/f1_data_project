"""Execute metrics through MetricFlow and compare each result with an independent, hand-written SQL query.
Run from the dbt project directory with the warehouse built and `mf` on PATH:
    F1_DUCKDB_PATH=... python ci/semantic_check.py
Needs `pip install dbt-metricflow`. All MetricFlow queries run first (they need DuckDB's single write lock), then the SQL checks."""
import csv, os, subprocess, sys
import duckdb

DB = os.environ["F1_DUCKDB_PATH"]
CHECKS = [
    ("avg representative lap time by driver", "avg_representative_lap_time_seconds", "driver__full_name",
     "select d.full_name, avg(l.lap_time_seconds) from fct_lap_performance l join dim_drivers d using(driver_number) where l.is_representative_lap group by 1"),
    ("points by driver", "championship_points", "driver__full_name",
     "select d.full_name, sum(r.total_points) from fct_race_results r join dim_drivers d using(driver_number) group by 1"),
    ("wins by driver", "race_wins", "driver__full_name",
     "select d.full_name, sum(case when r.is_winner then 1 else 0 end) from fct_race_results r join dim_drivers d using(driver_number) group by 1"),
    ("green-flag time lost by driver", "avg_green_flag_time_lost_seconds", "driver__full_name",
     "select d.full_name, avg(p.time_lost_seconds) from fct_pit_stops p join dim_drivers d using(driver_number) where not p.is_under_neutralisation group by 1"),
    ("points per start (ratio metric)", "points_per_start", "driver__full_name",
     "select d.full_name, sum(r.total_points)*1.0/count(*) from fct_race_results r join dim_drivers d using(driver_number) group by 1"),
    ("best lap by circuit (join through session)", "best_lap_time_seconds", "session__circuit_short_name",
     "select s.circuit_short_name, min(l.lap_time_seconds) from fct_lap_performance l join dim_sessions s using(session_key) group by 1"),
    ("on-track overtakes by overtaker", "on_track_overtakes", "driver__full_name",
     "select d.full_name, sum(case when not o.is_probable_pit_cycle_pass then 1 else 0 end) from fct_overtakes o join dim_drivers d on d.driver_number=o.overtaking_driver_number group by 1"),
]


def mf(metric, group):
    out = "/tmp/_mf_out.csv"
    if os.path.exists(out):
        os.remove(out)
    r = subprocess.run(["mf", "query", "--metrics", metric, "--group-by", group, "--csv", out], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"mf query failed for {metric}: {(r.stdout + r.stderr)[-500:]}")
    return list(csv.DictReader(open(out)))


results = [(t, m, g, s, mf(m, g)) for t, m, g, s in CHECKS]
con = duckdb.connect(DB, read_only=True)
ok = True
for title, metric, group, sql, rows in results:
    expected = {str(k): v for k, v in con.sql(sql).fetchall()}
    bad = [(r[group], r[metric], expected.get(r[group])) for r in rows
           if expected.get(r[group]) is None or abs(float(r[metric]) - float(expected[r[group]])) > 1e-6]
    missing = set(expected) - {r[group] for r in rows}
    good = not bad and not missing
    print(f"{'OK ' if good else 'BAD'} {title}: {len(rows)} groups {bad or ''} {('missing ' + str(missing)) if missing else ''}")
    ok &= good
print("ALL METRICS MATCH" if ok else "MISMATCHES")
sys.exit(0 if ok else 1)
