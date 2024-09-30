
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.4.0"

    }
  }
}
provider "google" {
  project     = "savvy-primacy-436914-v0"
  region      = "europe-west3"
  credentials = "./keys/terraform-runner-key.json"
}

resource "google_storage_bucket" "terraform-bucket" {
  name          = "savvy-primacy-436914-v0-bucket"
  location      = var.gcp_location
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location = var.gcp_location
  delete_contents_on_destroy = true
}