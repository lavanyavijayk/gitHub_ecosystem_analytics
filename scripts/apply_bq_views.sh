#!/usr/bin/env bash
# ================================================================
# apply_bq_views.sh
#
# Applies BigQuery analysis views with the correct PROJECT_ID.
#
# Usage:
#   ./scripts/apply_bq_views.sh <PROJECT_ID> [DATASET]
#
# Example:
#   ./scripts/apply_bq_views.sh my-gcp-project github_oss_gold
# ================================================================

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <PROJECT_ID> [DATASET]"
    echo "  PROJECT_ID  Your GCP project ID"
    echo "  DATASET     BigQuery dataset (default: github_oss_gold)"
    exit 1
fi

PROJECT_ID="$1"
DATASET="${2:-github_oss_gold}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/create_bq_views.sql"

if [ ! -f "$SQL_FILE" ]; then
    echo "Error: $SQL_FILE not found"
    exit 1
fi

echo "Applying BigQuery views..."
echo "  Project: ${PROJECT_ID}"
echo "  Dataset: ${DATASET}"

# Replace placeholder and execute
sed "s/PROJECT_ID/${PROJECT_ID}/g" "$SQL_FILE" \
    | bq query --use_legacy_sql=false --project_id="$PROJECT_ID"

echo "Done. All views created in ${PROJECT_ID}.${DATASET}"
