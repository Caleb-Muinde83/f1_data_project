"""Compare every table built by dbt-duckdb with the same table built by dbt-bigquery (emulator), row for row.
  python compare_duckdb_bigquery.py <duckdb file> <project> <dataset> <emulator endpoint>
Floats are compared to 5 decimals, timestamps as UTC to the millisecond, rows as unordered multisets. Exit code 1 on any difference."""
import collections, datetime, decimal, sys
import duckdb
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

db, project, dataset, endpoint = sys.argv[1:5]
c = bigquery.Client(project=project, credentials=AnonymousCredentials(), client_options=ClientOptions(api_endpoint=endpoint))
d = duckdb.connect(db, read_only=True)


def norm(v):
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, (float, decimal.Decimal)):
        return round(float(v), 5)
    if isinstance(v, datetime.datetime):
        if v.tzinfo:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return v.isoformat(timespec="milliseconds")
    if isinstance(v, (list, tuple)):
        return str([norm(x) for x in v])
    return v


tables = [r[0] for r in d.sql("select table_name from information_schema.tables where table_schema='main' order by 1").fetchall()
          if not r[0].startswith("metricflow")]
bq_tables = {t.table_id for t in c.list_tables(f"{project}.{dataset}")}
same, bad, missing = 0, [], []
for t in tables:
    if t not in bq_tables:
        missing.append(t); continue
    cols = [x[0] for x in d.sql(f'describe "{t}"').fetchall()]
    drows = d.sql(f'select * from "{t}"').fetchall()
    brows = list(c.query(f"select * from `{project}.{dataset}.{t}`", retry=None, job_retry=None).result(retry=None))
    bcols = list(brows[0].keys()) if brows else cols
    if set(cols) != set(bcols):
        bad.append((t, f"column sets differ: duckdb-only={sorted(set(cols) - set(bcols))} bigquery-only={sorted(set(bcols) - set(cols))}")); continue
    order = sorted(cols); idx = [cols.index(k) for k in order]
    A = collections.Counter(tuple(norm(r[i]) for i in idx) for r in drows)
    B = collections.Counter(tuple(norm(r[k]) for k in order) for r in brows)
    if A == B:
        same += 1; continue
    differing = [k for i, k in enumerate(order)
                 if collections.Counter(norm(r[idx[i]]) for r in drows) != collections.Counter(norm(r[k]) for r in brows)]
    bad.append((t, f"rows duckdb={len(drows)} bigquery={len(brows)}; differing columns: {differing[:8]}"))
print(f"IDENTICAL: {same} tables | DIFFERENT: {len(bad)} | missing in BigQuery: {missing}")
for t, m in bad:
    print(f"  DIFF {t}: {m}")
sys.exit(1 if bad or missing else 0)
