#!/usr/bin/env python3
"""
Load the extractor's raw Parquet files into BigQuery, one table per endpoint (dataset f1_raw by default).

  python scripts/load_raw_to_bigquery.py --raw-dir airflow/data/raw --project my-project [--dataset f1_raw]
  python scripts/load_raw_to_bigquery.py ... --emulator http://localhost:9050     # local BigQuery emulator, no credentials

WHY THIS EXISTS: DuckDB reads many Parquet files with union_by_name and reconciles types. A BigQuery table has ONE type per
column, so cross-file drift (a number in one session's file, a '+1 LAP' string in another) must be reconciled BEFORE loading.
Rules: identical types stay; int + float -> FLOAT64; null-only columns adopt the other files' type; anything else -> STRING.
Each endpoint is fully re-loaded (WRITE_TRUNCATE), so re-running is idempotent. Files under _manifests/ are ignored.
"""
import argparse
import glob
import io
import os
import re
import sys
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

FILE_RE = re.compile(r"^(?P<endpoint>.+)_(?P<session>\d+|global)_extracted\.parquet$")


def unify_types(types):
    """Pick one Arrow type from the types a column has across files."""
    real = [t for t in types if not pa.types.is_null(t)]
    if not real:
        return pa.string()
    if all(t == real[0] for t in real):
        return real[0]
    if all(pa.types.is_integer(t) or pa.types.is_floating(t) for t in real):
        return pa.float64()
    return pa.string()


def unified_schema(tables):
    cols = defaultdict(list)
    order = []
    for t in tables:
        for f in t.schema:
            if f.name not in cols:
                order.append(f.name)
            cols[f.name].append(f.type)
    return pa.schema([(n, unify_types(cols[n])) for n in order])


def conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    arrays = []
    for f in schema:
        if f.name not in table.column_names:
            arrays.append(pa.nulls(table.num_rows, f.type))
            continue
        col = table[f.name]
        if col.type == f.type:
            arrays.append(col)
        elif pa.types.is_string(f.type):
            arrays.append(pa.array([None if v is None else str(v) for v in col.to_pylist()], type=pa.string()))
        else:
            arrays.append(col.cast(f.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def combine_endpoint(files):
    tables = [pq.read_table(f) for f in files]
    schema = unified_schema(tables)
    return pa.concat_tables([conform(t, schema) for t in tables])


def group_files(raw_dir):
    groups = defaultdict(list)
    for f in sorted(glob.glob(os.path.join(raw_dir, "*_extracted.parquet"))):
        m = FILE_RE.match(os.path.basename(f))
        if m:
            groups[m.group("endpoint")].append(f)
    return groups


def make_client(project, emulator=None):
    from google.cloud import bigquery
    if emulator:
        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials
        return bigquery.Client(project=project, credentials=AnonymousCredentials(), client_options=ClientOptions(api_endpoint=emulator))
    return bigquery.Client(project=project)


BQ_TYPES = {pa.types.is_integer: "INT64", pa.types.is_floating: "FLOAT64", pa.types.is_boolean: "BOOL"}


def bq_type(t):
    for pred, name in BQ_TYPES.items():
        if pred(t):
            return name
    return "STRING"


def load(client, dataset, endpoint, table: pa.Table, fmt="parquet"):
    from google.cloud import bigquery
    ref = f"{client.project}.{dataset}.{endpoint}"
    if fmt == "parquet":
        buf = io.BytesIO(); pq.write_table(table, buf, compression="snappy"); buf.seek(0)
        job = client.load_table_from_file(buf, ref, job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET, write_disposition="WRITE_TRUNCATE"))
    else:  # newline-delimited JSON with an explicit schema (for backends without Parquet load support)
        import json
        schema = [bigquery.SchemaField(f.name, bq_type(f.type)) for f in table.schema]
        buf = io.BytesIO("\n".join(json.dumps(r, default=str) for r in table.to_pylist()).encode())
        job = client.load_table_from_file(buf, ref, job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, schema=schema, write_disposition="WRITE_TRUNCATE"))
    job.result()
    return client.get_table(ref).num_rows


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", required=True); ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", default="f1_raw"); ap.add_argument("--emulator", help="e.g. http://localhost:9050")
    ap.add_argument("--format", choices=["parquet", "json"], default="parquet"); ap.add_argument("--location", default="US")
    a = ap.parse_args(argv)
    from google.cloud import bigquery
    client = make_client(a.project, a.emulator)
    from google.api_core.exceptions import NotFound
    try:
        client.get_dataset(f"{a.project}.{a.dataset}")
    except NotFound:
        client.create_dataset(bigquery.Dataset(f"{a.project}.{a.dataset}"))
    groups = group_files(a.raw_dir)
    if not groups:
        sys.exit(f"No *_extracted.parquet files under {a.raw_dir}")
    for endpoint, files in sorted(groups.items()):
        table = combine_endpoint(files)
        print(f"{endpoint:24s} {len(files):3d} file(s) -> {load(client, a.dataset, endpoint, table, a.format):>8,} rows")


if __name__ == "__main__":
    main()
