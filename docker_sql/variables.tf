variable "gcp_location" {
  description = "Location where gcp provisions resources"
  default = "EU"
}

variable "bq_dataset_name" {
    description = "Big query dataset name"
    default = "demo_dataset"  
}