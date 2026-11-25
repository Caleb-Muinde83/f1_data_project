"""Equivalence check for session-scoped incremental builds (DuckDB).

Proves, on a copy of a warehouse that was built IN FULL, that rebuilding one session through the scoped path:
  * replaces that session exactly as a fresh full build computed it (no stale rows, no duplicates),
  * leaves every other session byte-for-byte untouched (including rows we deliberately tampered with as sentinels),
  * is idempotent (a second identical run changes nothing),
  * is a no-op for a session that has no data anywhere, and
  * CLEARS a session whose upstream rows vanished (the pre-delete), instead of leaving stale rows behind.

  cd dbt_f1_transformations
  F1_RAW_DATA_PATH=<fixtures dir> python ci/scoped_equivalence_check.py --full-db <built.duckdb> [--session 9002] [--target ci]

The input warehouse is only ever copied, never modified. Needs `dbt` (dbt-duckdb) on PATH and the `duckdb` Python package.
Exit code 1 on any failure.
"""
import argparse
import collections
import datetime
import decimal
import json
import os
import shutil
import subprocess
import sys
import tempfile

import duckdb

GROUP_TABLES = ["fct_telemetry", "fct_telemetry_distance_trace", "fct_telemetry_lap_summary",
                "fct_lap_time_delta_trace", "dim_session_corners", "fct_corner_performance"]
CI_VARS = {"enable_radio_transcripts": True, "min_stint_laps_for_degradation": 3}
FAILURES = []


def norm(v):
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, (float, decimal.Decimal)):
        return round(float(v), 5)
    if isinstance(v, datetime.datetime):
        return (v.astimezone(datetime.timezone.utc).replace(tzinfo=None) if v.tzinfo else v).isoformat(timespec="milliseconds")
    if isinstance(v, (list, tuple)):
        return str([norm(x) for x in v])
    return v


def snapshot(db):
    """{table: {session_key: Counter(rows)}} for every telemetry-group table. Rows are unordered multisets."""
    con = duckdb.connect(db, read_only=True)
    try:
        out = {}
        for t in GROUP_TABLES:
            cur = con.execute(f'SELECT * FROM "{t}"')
            cols = [d[0] for d in cur.description]
            k = cols.index("session_key")
            per = collections.defaultdict(collections.Counter)
            for row in cur.fetchall():
                per[row[k]][tuple(norm(x) for x in row)] += 1
            out[t] = per
        return out
    finally:
        con.close()


def execute(db, statements):
    con = duckdb.connect(db)
    try:
        for s in statements:
            con.execute(s)
    finally:
        con.close()


def check(ok, message):
    print(("  ok    " if ok else "  FAIL  ") + message)
    if not ok:
        FAILURES.append(message)


def same_rows(a, b):
    return a == b      # Counter equality: same rows, same multiplicities


def run_dbt(args, db, raw_dir, target, variables):
    env = {**os.environ, "F1_DUCKDB_PATH": db, "F1_RAW_DATA_PATH": raw_dir}
    cmd = ["dbt", "build", "--profiles-dir", ".", "--target", target] + args + ["--vars", json.dumps(variables)]
    print("  $", " ".join(cmd))
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        print(res.stdout[-4000:])
        print(res.stderr[-2000:])
    check(res.returncode == 0, f"dbt exited 0 ({' '.join(args) or 'whole graph'})")
    return res.returncode == 0


def compare(after, expected_for_k, before, k, what):
    """Session k must equal expected_for_k; every other session must equal `before`."""
    for t in GROUP_TABLES:
        check(same_rows(after[t].get(k, collections.Counter()), expected_for_k[t].get(k, collections.Counter())),
              f"{what}: {t} session {k} matches the full build")
        others_after = {s: c for s, c in after[t].items() if s != k}
        others_before = {s: c for s, c in before[t].items() if s != k}
        check(others_after == others_before, f"{what}: {t} other sessions untouched")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-db", required=True, help="warehouse built in full (copied, never modified)")
    ap.add_argument("--raw-dir", default=os.environ.get("F1_RAW_DATA_PATH"), help="raw Parquet dir the full build read")
    ap.add_argument("--session", type=int, default=9002, help="session to rebuild through the scoped path")
    ap.add_argument("--target", default="ci")
    a = ap.parse_args()
    if not a.raw_dir:
        sys.exit("--raw-dir (or F1_RAW_DATA_PATH) is required")
    k = a.session
    work_dir = tempfile.mkdtemp(prefix="f1_scoped_eq_")
    try:
        full = snapshot(a.full_db)
        have = {s for t in GROUP_TABLES for s in full[t]}
        if k not in have or len(have) < 2:
            sys.exit(f"need telemetry for session {k} and at least one other session in {a.full_db}; found {sorted(have)}")
        sentinel = sorted(have - {k})[0]
        print(f"session under test: {k}; sentinel (must stay untouched): {sentinel}")

        def fresh_copy(name):
            p = os.path.join(work_dir, name)
            shutil.copy(a.full_db, p)
            return p

        def tamper(db):
            """Damage session k in every way a stale/partial load could, and mark a sentinel row in another session."""
            execute(db, [
                f"UPDATE fct_telemetry SET speed = speed + 7 WHERE session_key = {k}",
                f"DELETE FROM fct_telemetry_distance_trace WHERE session_key = {k} AND distance_bin % 2 = 0",
                f"DELETE FROM fct_telemetry_lap_summary WHERE session_key = {k}",
                f"INSERT INTO fct_corner_performance SELECT * FROM fct_corner_performance WHERE session_key = {k} LIMIT 1",
                f"DELETE FROM dim_session_corners WHERE session_key = {k}",
                f"UPDATE fct_telemetry SET speed = speed + 1 WHERE session_key = {sentinel}",
                f"UPDATE fct_lap_time_delta_trace SET speed_delta_kph = 123.0 WHERE session_key = {sentinel}",
            ])

        print("\n[1] damaged session, whole-graph scoped build (what Airflow runs for one session)")
        db1 = fresh_copy("one.duckdb")
        tamper(db1)
        before = snapshot(db1)
        check(not same_rows(before["fct_telemetry"][k], full["fct_telemetry"][k]), "the damage is visible before the run (test is meaningful)")
        if run_dbt([], db1, a.raw_dir, a.target, {**CI_VARS, "session_keys": [k]}):
            after = snapshot(db1)
            compare(after, full, before, k, "scoped replace")
            print("\n[2] idempotency: the same scoped build again")
            if run_dbt([], db1, a.raw_dir, a.target, {**CI_VARS, "session_keys": [k]}):
                again = snapshot(db1)
                check(again == after, "second identical run changes nothing")

        print("\n[3] damaged session, telemetry-group-only build (what Airflow runs per chunk)")
        db2 = fresh_copy("chunk.duckdb")
        tamper(db2)
        before2 = snapshot(db2)
        if run_dbt(["--select", "tag:telemetry_group"], db2, a.raw_dir, a.target, {**CI_VARS, "session_keys": [k]}):
            compare(snapshot(db2), full, before2, k, "chunk replace")

        print("\n[4] zero rows: a scope with no data anywhere is a no-op")
        db3 = fresh_copy("empty.duckdb")
        before3 = snapshot(db3)
        if run_dbt(["--select", "tag:telemetry_group"], db3, a.raw_dir, a.target, {**CI_VARS, "session_keys": [987654321]}):
            check(snapshot(db3) == before3, "nothing changed for an unknown session")

        print(f"\n[5] upstream vanished: session {k} has no telemetry files any more, so its old rows must be cleared")
        db4 = fresh_copy("vanished.duckdb")
        raw_copy = os.path.join(work_dir, "raw_without_telemetry")
        shutil.copytree(a.raw_dir, raw_copy)
        for endpoint in ("car_data", "location"):
            path = os.path.join(raw_copy, f"{endpoint}_{k}_extracted.parquet")
            if os.path.exists(path):
                os.remove(path)
        before4 = snapshot(db4)
        if run_dbt(["--select", "tag:telemetry_group"], db4, raw_copy, a.target, {**CI_VARS, "session_keys": [k]}):
            after4 = snapshot(db4)
            for t in GROUP_TABLES:
                check(sum(after4[t].get(k, collections.Counter()).values()) == 0, f"vanished upstream: {t} holds no stale rows for {k}")
                check({s: c for s, c in after4[t].items() if s != k} == {s: c for s, c in before4[t].items() if s != k},
                      f"vanished upstream: {t} other sessions untouched")

        print("\n" + ("ALL CHECKS PASSED" if not FAILURES else f"{len(FAILURES)} CHECK(S) FAILED"))
        for f in FAILURES:
            print("  -", f)
        return 1 if FAILURES else 0
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
