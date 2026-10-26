"""Small, Airflow-free helpers so they can be unit tested: run manifest + failure notification."""
import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

import pyarrow.parquet as pq


def build_manifest(files, raw_dir: str, run_id: str) -> str:
    """Write raw_dir/_manifests/<run_id>.json listing every file with its row count and size.
    dbt tests / reviewers can reconcile what was extracted in a run against what reached the warehouse."""
    entries = []
    for f in files:
        try:
            rows = pq.ParquetFile(f).metadata.num_rows
        except Exception as exc:  # a corrupt file must be visible in the manifest, not crash the run
            logging.error(f"Could not read Parquet metadata for {f}: {exc}")
            rows = None
        entries.append({"file": os.path.basename(f), "rows": rows, "bytes": os.path.getsize(f) if os.path.exists(f) else None})
    manifest = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "file_count": len(entries),
        "total_rows": sum(e["rows"] or 0 for e in entries),
        "files": sorted(entries, key=lambda e: e["file"]),
    }
    out_dir = os.path.join(raw_dir, "_manifests")
    os.makedirs(out_dir, exist_ok=True)
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(run_id))
    path = os.path.join(out_dir, f"{safe}.json")
    with open(path, "w") as fh:
        json.dump(manifest, fh, indent=2)
    return path


def notify_failure(context):
    """on_failure_callback: always logs one structured line; also POSTs to ALERT_WEBHOOK_URL if configured
    (Slack-compatible {'text': ...} payload). Never raises: alerting must not mask the original failure."""
    try:
        ti = context.get("task_instance")
        info = {
            "dag_id": getattr(ti, "dag_id", None), "task_id": getattr(ti, "task_id", None),
            "run_id": context.get("run_id"), "try_number": getattr(ti, "try_number", None),
            "exception": str(context.get("exception"))[:500],
        }
        logging.error("PIPELINE_FAILURE " + json.dumps(info))
        url = os.environ.get("ALERT_WEBHOOK_URL")
        if url:
            body = json.dumps({"text": f"OpenF1 pipeline failure: {info['dag_id']}.{info['task_id']} ({info['run_id']}): {info['exception']}"}).encode()
            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        logging.error(f"notify_failure itself failed: {exc}")
