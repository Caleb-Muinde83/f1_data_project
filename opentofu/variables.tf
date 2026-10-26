variable "gcp_project_id" {
  description = "The ID of your Google Cloud project"
  type        = string
  # We will leave this without a default so you can pass it in securely
}

variable "gcp_region" {
  description = "The GCP region to deploy resources to"
  type        = string
  default     = "us-central1" # Feel free to change this to a region closer to you
}

variable "gcp_zone" {
  description = "The specific zone within the region"
  type        = string
  default     = "us-central1-a"
}

variable "bigquery_location" {
  description = "Location for the BigQuery datasets (multi-region 'US'/'EU' or a region)"
  type        = string
  default     = "US"
}
