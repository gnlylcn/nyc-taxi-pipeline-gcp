#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID}"
: "${REGION:?Set REGION (e.g. europe-west1)}"
: "${BUCKET:?Set BUCKET}"

MONTHS="2023-01,2023-02"

gcloud dataproc batches submit pyspark src/jobs/bronze_ingest.py   --project="${PROJECT_ID}"   --region="${REGION}"   --deps-bucket="gs://${BUCKET}"   --   --months "${MONTHS}"   --raw-dir "gs://${BUCKET}/raw"   --bronze-dir "gs://${BUCKET}/bronze/nyc_taxi/yellow"   --zone-path "gs://${BUCKET}/raw/taxi_zone_lookup.csv"
