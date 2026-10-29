# Dashboards

    pip install -r dashboards/requirements.txt
    F1_DUCKDB_PATH=/path/to/f1_local_warehouse.duckdb streamlit run dashboards/app.py

* The warehouse is opened **read-only**. DuckDB allows one writer: if dbt is running you get a clear message, not a hang.
* The Strategy page's simulator needs `analytics/` on the path (`app.py` adds it) and at least two compounds with a fitted pace model.
* Tests: `F1_DUCKDB_PATH=<warehouse built from the synthetic fixtures> python -m pytest dashboards/tests` (part of `make ci-test`).
* The pages are tested headlessly; **their layout has not been reviewed in a browser**.
