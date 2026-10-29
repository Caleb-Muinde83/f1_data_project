"""
Run dbt against the local BigQuery EMULATOR (goccy/bigquery-emulator) with the real dbt-bigquery adapter.

  F1_BQ_API_ENDPOINT=http://localhost:9050 python ci/bq_emulator_runner.py build --target bigquery_emulator --profiles-dir .

The only change vs a normal dbt run is that Google authentication is replaced by anonymous credentials, because the emulator
has none. This is a TEST harness: the emulator implements a subset of GoogleSQL, so a pass here is strong evidence but not
proof of identical behaviour on real BigQuery.
"""
import sys
from google.auth.credentials import AnonymousCredentials
import dbt.adapters.bigquery.clients as clients
import dbt.adapters.bigquery.connections as connections

clients.create_google_credentials = lambda credentials: AnonymousCredentials()
if hasattr(connections, "create_google_credentials"):
    connections.create_google_credentials = lambda credentials: AnonymousCredentials()

# The emulator labels DDL statements as SELECT but gives them no destination table; dbt-bigquery then asks for that table's
# row count. Real BigQuery never does this, so answer "unknown" instead of crashing.
from google.cloud import bigquery  # noqa: E402
_real_get_table = bigquery.Client.get_table


class _NoRows:
    num_rows = None


def _safe_get_table(self, table, *args, **kwargs):
    return _NoRows() if table is None else _real_get_table(self, table, *args, **kwargs)


bigquery.Client.get_table = _safe_get_table

# The emulator does not implement the routines (UDF) endpoint; dbt-bigquery lists routines when it lists a dataset.
bigquery.Client.list_routines = lambda self, *args, **kwargs: iter(())

from dbt.cli.main import dbtRunner  # noqa: E402

result = dbtRunner().invoke(sys.argv[1:])
sys.exit(0 if result.success else 1)
