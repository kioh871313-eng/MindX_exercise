#!/usr/bin/env bash
# =============================================================
# bootstrap_variables.sh
# Set Airflow Variables needed by the pipeline DAGs.
# Run this once after "docker compose up -d" and airflow-init completes.
#
# Usage (from project root):
#   docker exec airflow_scheduler bash /opt/airflow/dags/../scripts/bootstrap_variables.sh \
#     --bucket your-bucket-name \
#     --prefix computer_hardware_sales/
#
# Or run directly on host (requires Airflow CLI installed):
#   bash scripts/bootstrap_variables.sh --bucket your-bucket-name
# =============================================================

set -e

S3_BUCKET=""
S3_PREFIX="computer_hardware_sales/"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --bucket) S3_BUCKET="$2"; shift 2 ;;
    --prefix) S3_PREFIX="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "$S3_BUCKET" ]]; then
  echo "Error: --bucket is required."
  echo "Usage: $0 --bucket your-bucket-name [--prefix computer_hardware_sales/]"
  exit 1
fi

echo "Setting Airflow Variables..."

airflow variables set s3_bucket_name        "$S3_BUCKET"
airflow variables set s3_prefix             "$S3_PREFIX"
airflow variables set bronze_last_run_date  "2000-01-01"
airflow variables set silver_last_bronze_id "0"

echo ""
echo "Done. Variables set:"
airflow variables list
