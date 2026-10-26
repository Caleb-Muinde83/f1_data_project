#!/usr/bin/env bash
# Copy DAGs and plugins to the Cloud Composer environment's bucket.
#   COMPOSER_BUCKET=us-central1-my-env-1234-bucket ./scripts/deploy_to_composer.sh [--dry-run]
# NOT exercised against a real Composer environment. Does not delete remote files (no rsync -d) on purpose.
set -euo pipefail
: "${COMPOSER_BUCKET:?Set COMPOSER_BUCKET to the Composer bucket name (no gs:// prefix)}"
DRY=""; [[ "${1:-}" == "--dry-run" ]] && DRY="-n"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXCLUDE='(__pycache__|\.pyc$|\.airflowignore$)'
echo "Syncing DAGs..."
gsutil -m rsync -r $DRY -x "$EXCLUDE" "$ROOT/airflow/dags" "gs://${COMPOSER_BUCKET}/dags"
echo "Syncing plugins..."
gsutil -m rsync -r $DRY -x "$EXCLUDE" "$ROOT/airflow/plugins" "gs://${COMPOSER_BUCKET}/plugins"
echo "Done${DRY:+ (dry run)}."
