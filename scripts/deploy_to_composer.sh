#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

: "${GCP_PROJECT_ID:?Environment variable GCP_PROJECT_ID is required}"
: "${COMPOSER_REGION:?Environment variable COMPOSER_REGION is required}"
: "${COMPOSER_ENVIRONMENT:?Environment variable COMPOSER_ENVIRONMENT is required}"

if [[ -n "${GCP_REGION:-}" ]]; then
  REGION="$GCP_REGION"
else
  REGION="$COMPOSER_REGION"
fi

echo "Deploying Airflow code to Cloud Composer environment: $COMPOSER_ENVIRONMENT"
echo "Project: $GCP_PROJECT_ID"
echo "Composer region: $COMPOSER_REGION"

gcloud --version

# Configure gcloud project + region

gcloud config set project "$GCP_PROJECT_ID"
gcloud config set composer/location "$COMPOSER_REGION"
gcloud config set compute/region "$REGION"

# Validate Python syntax
if command -v python >/dev/null 2>&1; then
  echo "Validating Python files..."
  find airflow/dags -name '*.py' -print0 | xargs -0 python -m py_compile
  find airflow/plugins -name '*.py' -print0 | xargs -0 python -m py_compile
else
  echo "Warning: python not found in PATH; skipping Python validation"
fi

# Upload DAGs and plugins

echo "Uploading Airflow DAGs to Composer..."
gcloud composer environments storage dags import \
  --environment="$COMPOSER_ENVIRONMENT" \
  --location="$COMPOSER_REGION" \
  --source="airflow/dags"

echo "Uploading Airflow plugins to Composer..."
gcloud composer environments storage plugins import \
  --environment="$COMPOSER_ENVIRONMENT" \
  --location="$COMPOSER_REGION" \
  --source="airflow/plugins"

echo "Composer deployment completed: $COMPOSER_ENVIRONMENT"
