#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID}"
: "${REGION:?Set REGION (e.g. europe-west1)}"
: "${BUCKET:?Set BUCKET}"

gcloud dataproc batches submit pyspark src/jobs/gold_transform.py   --project="${PROJECT_ID}"   --region="${REGION}"   --deps-bucket="gs://${BUCKET}"   --   --bronze-dir "gs://${BUCKET}/bronze/nyc_taxi/yellow"   --gold-dir "gs://${BUCKET}/gold"   --zone-path "gs://${BUCKET}/raw/taxi_zone_lookup.csv"
