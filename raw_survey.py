"""Read-only survey of the raw Parquet folder. Prints: per-endpoint file counts/sizes, column or type drift
between files, and glob-vs-single-file read timing. Writes nothing.
Run:  python raw_survey.py [raw_dir]      (default: $F1_RAW_DATA_PATH or /opt/airflow/data/raw)"""
import glob, os, re, sys, time, collections
import pyarrow.parquet as pq
import duckdb

raw = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("F1_RAW_DATA_PATH", "/opt/airflow/data/raw")
pat = re.compile(r"^(?P<ep>.+)_(?P<sess>[^_]+)_extracted\.parquet$")
by_ep = collections.defaultdict(list)
for f in glob.glob(os.path.join(raw, "*_extracted.parquet")):
    m = pat.match(os.path.basename(f))
    if m:
        by_ep[m["ep"]].append(f)

print(f"raw dir: {raw}\n{'endpoint':24s}{'files':>7s}{'MB':>10s}")
drift = []
for ep, files in sorted(by_ep.items()):
    mb = sum(os.path.getsize(f) for f in files) / 1e6
    print(f"{ep:24s}{len(files):7d}{mb:10.1f}")
    cols = collections.defaultdict(lambda: [0, set()])
    for f in files:
        for fld in pq.read_schema(f):
            cols[fld.name][0] += 1
            cols[fld.name][1].add(str(fld.type))
    for name, (n, types) in cols.items():
        if n < len(files) or len(types) > 1:
            drift.append((ep, name, f"{n}/{len(files)} files", ",".join(sorted(types))))

print("\nCOLUMN / TYPE DRIFT (column missing from some files, or more than one type):")
print("\n".join(f"  {ep}.{c}: present in {p}; types: {t}" for ep, c, p, t in drift) or "  none")

print("\nREAD TIMING (count(*)): all files via glob vs one file")
con = duckdb.connect()
for ep in ("laps", "car_data"):
    files = sorted(by_ep.get(ep, []))
    if not files:
        continue
    t = time.time(); n = con.execute(f"select count(*) from read_parquet('{raw}/{ep}_*_extracted.parquet', union_by_name=true)").fetchone()[0]; tg = time.time() - t
    t = time.time(); con.execute(f"select count(*) from read_parquet('{files[-1]}')").fetchone(); t1 = time.time() - t
    print(f"  {ep}: glob {tg:.2f}s ({len(files)} files, {n:,} rows) | single file {t1:.2f}s")
print("\nduckdb", duckdb.__version__)
