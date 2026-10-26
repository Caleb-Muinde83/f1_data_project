resource "google_storage_bucket" "data_lake" {
  # Bucket names must be globally unique across all of Google Cloud
  # We append your project ID to ensure it doesn't clash with anyone else
  name          = "falcon-fleet-f1-data-lake-${var.gcp_project_id}"
  location      = var.gcp_region
  
  # This allows OpenTofu to delete the bucket later even if it has files inside it
  # (Great for dev environments, but you'd turn this off in production!)
  force_destroy = true 

  # Best practice for modern GCP buckets
  uniform_bucket_level_access = true
}

# Let's output the bucket name so OpenTofu tells us what it created
output "data_lake_bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "The name of the F1 data lake bucket"
}

# ---------------------------------------------------------------------------
# BigQuery datasets for a future cloud warehouse (raw landing, staging, marts).
# NOT yet applied, and the dbt models are not BigQuery-portable as-is (they use DuckDB-only SQL such as
# ASOF JOIN, read_parquet and json_extract_string). See README "Cloud roadmap".
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "layers" {
  for_each   = toset(["f1_raw", "f1_staging", "f1_marts"])
  dataset_id = each.key
  location   = var.bigquery_location

  # Safe default for a learning project; set to false before storing anything you cannot rebuild.
  delete_contents_on_destroy = true

  labels = {
    project = "f1-data-project"
    layer   = replace(each.key, "f1_", "")
  }
}

output "bigquery_datasets" {
  value       = [for d in google_bigquery_dataset.layers : d.dataset_id]
  description = "BigQuery datasets created for the cloud warehouse"
}
