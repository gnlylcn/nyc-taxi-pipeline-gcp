terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id"  { type = string }
variable "region"      { type = string  default = "europe-west1" }
variable "bucket_name" { type = string }

resource "google_storage_bucket" "data" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = true
}

resource "google_service_account" "pipeline" {
  account_id   = "nyc-taxi-pipeline-sa"
  display_name = "NYC Taxi Pipeline SA"
}

resource "google_storage_bucket_iam_member" "bucket_object_admin" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

output "bucket" { value = google_storage_bucket.data.name }
output "service_account_email" { value = google_service_account.pipeline.email }
