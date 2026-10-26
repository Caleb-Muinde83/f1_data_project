"""Upload extracted raw Parquet files to Google Cloud Storage (data-lake landing zone).
Layout: gs://<bucket>/raw/<endpoint>/<file>.parquet. Existing objects with an identical size are skipped,
so re-running a DAG does not re-upload unchanged files.

Needs the Google provider (already in requirements.txt) and an Airflow connection `google_cloud_default`
(or workload identity on Composer). NOT exercised against real GCS in this repo's tests (see unit test with a fake hook).
"""
import logging
import os


def object_name_for(file_path: str, prefix: str = "raw") -> str:
    name = os.path.basename(file_path)
    endpoint = name.split("_")[0] if not name.startswith(("championship_", "session_", "race_", "team_", "car_", "starting_")) \
        else "_".join(name.split("_")[:2])
    return f"{prefix}/{endpoint}/{name}"


def upload_raw_files(file_paths, bucket: str, prefix: str = "raw", gcp_conn_id: str = "google_cloud_default", hook=None):
    if hook is None:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        hook = GCSHook(gcp_conn_id=gcp_conn_id)
    uploaded, skipped = [], 0
    for fp in file_paths:
        obj = object_name_for(fp, prefix)
        try:
            if hook.exists(bucket_name=bucket, object_name=obj) and hook.get_size(bucket_name=bucket, object_name=obj) == os.path.getsize(fp):
                skipped += 1
                continue
        except Exception as exc:  # fall through to a fresh upload if existence check fails
            logging.warning(f"Existence check failed for {obj}: {exc}")
        hook.upload(bucket_name=bucket, object_name=obj, filename=fp)
        uploaded.append(f"gs://{bucket}/{obj}")
    logging.info(f"GCS upload: {len(uploaded)} uploaded, {skipped} unchanged and skipped.")
    return uploaded
