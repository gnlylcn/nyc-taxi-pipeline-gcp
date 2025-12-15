#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID}"
: "${REGION:?Set REGION (e.g. europe-west1)}"
: "${BUCKET:?Set BUCKET}"

MONTHS=("2023-01" "2023-02")

TLC_BASE="https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

echo "[INFO] Project: $PROJECT_ID  Region: $REGION  Bucket: $BUCKET"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

echo "[INFO] Uploading zone lookup CSV..."
curl -L "$ZONE_URL" -o "$tmpdir/taxi_zone_lookup.csv"
gsutil -m cp "$tmpdir/taxi_zone_lookup.csv" "gs://$BUCKET/raw/taxi_zone_lookup.csv"

for m in "${MONTHS[@]}"; do
  fname="yellow_tripdata_${m}.parquet"
  url="$TLC_BASE/$fname"
  echo "[INFO] Downloading $url"
  curl -L "$url" -o "$tmpdir/$fname"
  echo "[INFO] Uploading to gs://$BUCKET/raw/yellow/$fname"
  gsutil -m cp "$tmpdir/$fname" "gs://$BUCKET/raw/yellow/$fname"
done

echo "[INFO] Done. Raw files are in gs://$BUCKET/raw/"
