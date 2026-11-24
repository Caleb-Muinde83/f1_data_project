"""Run one SQL query against the warehouse (read-only) and print the result.
Close the dashboard and dbt first.   Usage (Git Bash, venv active):
    python run_query.py "SELECT COUNT(*) FROM fct_telemetry_distance_trace"
    python run_query.py queries.sql        (a file containing one or more queries separated by semicolons)
"""
import os
import sys
import duckdb

DB = os.environ.get("F1_DUCKDB_PATH", "F:/DaTech/f1_data_project/airflow/data/warehouse/f1_local_warehouse.duckdb")
arg = sys.argv[1] if len(sys.argv) > 1 else sys.exit(__doc__)
sql = open(arg, encoding="utf-8").read() if os.path.isfile(arg) else arg
con = duckdb.connect(DB, read_only=True)
for q in [s.strip() for s in sql.split(";") if s.strip()]:
    df = con.execute(q).df()
    print(df.to_string(index=False, max_rows=60) if not df.empty else "(no rows)")
    print()
