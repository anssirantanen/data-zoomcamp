variable "gcp_location" {
  description = "Location where gcp provisions resources"
  default = "EU"
}

variable "bq_dataset_name" {
    description = "Taxi trip dataset"
    default = "trips_data_all"  
}