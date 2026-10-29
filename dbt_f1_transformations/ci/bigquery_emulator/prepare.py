"""Create the dbt target dataset and load the points seed into the BigQuery EMULATOR.
(The emulator cannot ingest CSV/Parquet loads, so the seed table is loaded through JSON here instead of `dbt seed`.)"""
import csv, io, json, sys
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

project, endpoint, dataset, seed_csv = sys.argv[1:5]
c = bigquery.Client(project=project, credentials=AnonymousCredentials(), client_options=ClientOptions(api_endpoint=endpoint))
try:
    c.get_dataset(f"{project}.{dataset}")
except Exception:
    c.create_dataset(bigquery.Dataset(f"{project}.{dataset}"))
rows = list(csv.DictReader(open(seed_csv)))
data = "\n".join(json.dumps({"session_type": r["session_type"], "position": int(r["position"]), "points": float(r["points"])}) for r in rows).encode()
schema = [bigquery.SchemaField("session_type", "STRING"), bigquery.SchemaField("position", "INT64"), bigquery.SchemaField("points", "FLOAT64")]
c.load_table_from_file(io.BytesIO(data), f"{project}.{dataset}.f1_points_system",
                       job_config=bigquery.LoadJobConfig(source_format="NEWLINE_DELIMITED_JSON", schema=schema, write_disposition="WRITE_TRUNCATE")).result()
print(f"seed loaded: {len(rows)} rows")
