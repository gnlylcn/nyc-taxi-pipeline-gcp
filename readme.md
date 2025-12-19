# NYC Taxi Mini Pipeline (GCP)

This repo is the GCP version of the NYC taxi mini pipeline.
Goal: run the same Bronze/Gold logic on GCS using Dataproc Serverless.

I kept it simple:
- Raw files are downloaded to a GCS bucket (scripts).
- Spark jobs read from `gs://.../raw` and write to `gs://.../bronze` and `gs://.../gold`.
- Terraform creates the bucket + service account permissions.

## What you need
- A GCP project with Billing enabled
- `gcloud` CLI
- `terraform`

## 0) Set project and login
```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
gcloud config get-value project
```

## 1) Enable required APIs
```bash
gcloud services enable storage.googleapis.com iam.googleapis.com dataproc.googleapis.com
```

## 2) Create bucket + service account (Terraform)
Copy the example tfvars and edit it:

```bash
cp terraform/envs/dev/terraform.tfvars.example terraform/envs/dev/terraform.tfvars
```

Then set:
- `project_id`
- `region`
- `bucket_name`  (must be globally unique)

Apply:
```bash
cd terraform/envs/dev
terraform init
terraform apply
```

## 3) Download raw files to GCS
This downloads the TLC parquet files and zone lookup CSV into the bucket.

```bash
export PROJECT_ID="<PROJECT_ID>"
export REGION="europe-west1"
export BUCKET="<YOUR_BUCKET_NAME>"

bash scripts/20_download_raw_to_gcs.sh
```

## 4) Run Bronze on Dataproc Serverless
```bash
export PROJECT_ID="<PROJECT_ID>"
export REGION="europe-west1"
export BUCKET="<YOUR_BUCKET_NAME>"

bash scripts/30_run_bronze_dataproc.sh
```

## 5) Run Gold on Dataproc Serverless
```bash
export PROJECT_ID="<PROJECT_ID>"
export REGION="europe-west1"
export BUCKET="<YOUR_BUCKET_NAME>"

bash scripts/40_run_gold_dataproc.sh
```

## Verify outputs in GCS
```bash
gsutil ls gs://$BUCKET/bronze/nyc_taxi/yellow/
gsutil ls gs://$BUCKET/gold/
```


## CI/CD
GitHub Actions runs on each push/PR:
- `ruff` for lint
- `pytest` for unit tests
- `terraform fmt` + `terraform validate`

## Reruns / historical backfill
Bronze is rerun-safe:
- We write partitioned by `source_month`
- We set `spark.sql.sources.partitionOverwriteMode=dynamic`
- For each month, we write with `mode("overwrite")`, which overwrites only that month partition


## Data model (Gold)
- `gold/dim_date`: one row per date_key with year/month/day
- `gold/dim_zone`: taxi zone lookup (location_id, borough, zone, service_zone)
- `gold/fact_trips`: trip grain with foreign keys to dates and pickup/dropoff zone ids

## The Gold layer follows a simple dimensional model.

- Fact Table: fact_trips

- Grain: one row per taxi trip

- Main columns:
- 
- trip_id (deterministic hash)
- 
- pickup_date_key
- 
- dropoff_date_key
- 
- pu_location_id
- 
- do_location_id
- 
- passenger_count
- 
- trip_distance
- 
- fare_amount
- 
- total_amount
- 
- source_month
- 
- Dimension Table: dim_date
- 
- One row per calendar date
- 
- Derived from pickup and dropoff timestamps
- 
- Columns: date_key, year, month, day
- 
- Dimension Table: dim_zone
- 
- One row per taxi zone
- 
- Based on the official zone lookup file
- 
- Columns: location_id, borough, zone, service_zone
- 
- The fact table references the dimensions using date keys and location IDs.