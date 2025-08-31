# Terraform configuration for GCP infrastructure

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

# Provider configuration
provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "redis.googleapis.com",
    "sqladmin.googleapis.com",
    "vpcaccess.googleapis.com",
    "secretmanager.googleapis.com"
  ])
  
  service = each.value
  project = var.gcp_project_id
  
  disable_on_destroy = false
}

# Service Account for Feature Store
resource "google_service_account" "feature_store_sa" {
  account_id   = "feature-store-sa"
  display_name = "Feature Store Service Account"
  description  = "Service account for Feature Store application"
}

# IAM bindings for service account
resource "google_project_iam_member" "feature_store_bigquery" {
  project = var.gcp_project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.feature_store_sa.email}"
}