# GCP Project Configuration
variable "project_id" {
  description = "The GCP project ID where Cloud Composer 2 will be deployed"
  type        = string
  default     = "composer2-migration-project-dev"

  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id value cannot be empty."
  }
}

variable "region" {
  description = "The GCP region where Cloud Composer 2 will be deployed"
  type        = string
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1", "europe-west1", "asia-east1"], var.region)
    error_message = "The region must be one of the supported GCP regions for Cloud Composer 2."
  }
}

variable "zone" {
  description = "The GCP zone where Cloud Composer 2 will be deployed"
  type        = string
  default     = "us-central1-a"
}

# Environment Configuration
variable "environment_name" {
  description = "The name of the Cloud Composer 2 environment"
  type        = string
  default     = "composer2-dev"

  validation {
    condition     = length(var.environment_name) > 0
    error_message = "The environment_name value cannot be empty."
  }
}

variable "environment_size" {
  description = "The size of the Cloud Composer 2 environment (small, medium, large)"
  type        = string
  default     = "small"

  validation {
    condition     = contains(["small", "medium", "large"], var.environment_size)
    error_message = "The environment_size must be one of: small, medium, large."
  }
}

variable "composer_version" {
  description = "The version of Cloud Composer 2 to use"
  type        = string
  default     = "2.0.28"
}

variable "airflow_version" {
  description = "The version of Apache Airflow to use for the Cloud Composer 2 environment"
  type        = string
  default     = "2.2.5"
}

variable "node_count" {
  description = "The number of nodes to provision for the GKE cluster"
  type        = number
  default     = 3

  validation {
    condition     = var.node_count >= 3
    error_message = "At least 3 nodes are required for high availability."
  }
}

# Network Configuration
variable "network" {
  description = "The VPC network to use for the Cloud Composer 2 environment"
  type        = string
  default     = "composer2-migration-dev-network"
}

variable "subnetwork" {
  description = "The VPC subnetwork to use for the Cloud Composer 2 environment"
  type        = string
  default     = "composer2-migration-dev-subnet"
}

variable "ip_range_pods" {
  description = "The secondary IP range name for pods"
  type        = string
  default     = "composer-pods-dev"
}

variable "ip_range_services" {
  description = "The secondary IP range name for services"
  type        = string
  default     = "composer-services-dev"
}

variable "service_account_email" {
  description = "The service account email to use for the GKE cluster"
  type        = string
  default     = ""
}

variable "enable_private_endpoint" {
  description = "Whether to enable private endpoint for the GKE cluster"
  type        = bool
  default     = false
}

variable "master_ipv4_cidr_block" {
  description = "The CIDR block for the GKE master"
  type        = string
  default     = "172.16.0.0/28"
}

variable "web_server_access_control" {
  description = "IAP configuration for web server access control"
  type        = list(object({
    ip_range   = string
    description = string
  }))
  default     = [{
    ip_range   = "0.0.0.0/0"
    description = "Allow access from all IP addresses for development"
  }]
}

# Environment Variables
variable "environment_variables" {
  description = "Environment variables to set in the Cloud Composer 2 environment"
  type        = map(string)
  default     = {
    "AIRFLOW_VAR_ENV" = "dev"
    "AIRFLOW_VAR_GCP_PROJECT" = "composer2-migration-project-dev"
    "AIRFLOW_VAR_GCS_BUCKET" = "composer2-migration-dev-bucket"
  }
}

# Resource Allocation - Scheduler
variable "scheduler_cpu" {
  description = "CPU allocation for Airflow scheduler"
  type        = number
  default     = 2
}

variable "scheduler_memory_gb" {
  description = "Memory allocation for Airflow scheduler in GB"
  type        = number
  default     = 7.5
}

variable "scheduler_storage_gb" {
  description = "Storage allocation for Airflow scheduler in GB"
  type        = number
  default     = 10
}

variable "scheduler_count" {
  description = "Number of schedulers to run in the environment"
  type        = number
  default     = 1
}

# Resource Allocation - Web Server
variable "web_server_cpu" {
  description = "CPU allocation for Airflow web server"
  type        = number
  default     = 1
}

variable "web_server_memory_gb" {
  description = "Memory allocation for Airflow web server in GB"
  type        = number
  default     = 3.75
}

variable "web_server_storage_gb" {
  description = "Storage allocation for Airflow web server in GB"
  type        = number
  default     = 10
}

# Resource Allocation - Worker
variable "worker_cpu" {
  description = "CPU allocation for Airflow worker"
  type        = number
  default     = 2
}

variable "worker_memory_gb" {
  description = "Memory allocation for Airflow worker in GB"
  type        = number
  default     = 7.5
}

variable "worker_storage_gb" {
  description = "Storage allocation for Airflow worker in GB"
  type        = number
  default     = 10
}

variable "min_workers" {
  description = "Minimum number of workers in the environment"
  type        = number
  default     = 2
}

variable "max_workers" {
  description = "Maximum number of workers in the environment"
  type        = number
  default     = 6
}

# Database Configuration
variable "cloud_sql_machine_type" {
  description = "Machine type for the Cloud SQL instance"
  type        = string
  default     = "db-n1-standard-2"
}

# Maintenance Window
variable "maintenance_window_start_time" {
  description = "Start time of the maintenance window in RFC3339 format"
  type        = string
  default     = "2022-01-01T00:00:00Z"
}

variable "maintenance_window_end_time" {
  description = "End time of the maintenance window in RFC3339 format"
  type        = string
  default     = "2022-01-01T04:00:00Z"
}

variable "maintenance_recurrence" {
  description = "RFC5545 RRULE for maintenance window recurrence"
  type        = string
  default     = "FREQ=WEEKLY;BYDAY=SA,SU"
}

# Data Retention
variable "airflow_database_retention_days" {
  description = "Number of days to retain Airflow database data"
  type        = number
  default     = 30
}

# Dependencies
variable "pypi_packages" {
  description = "PyPI packages to install in the environment"
  type        = map(string)
  default     = {
    "apache-airflow-providers-google" = "latest"
    "apache-airflow-providers-http" = "latest"
    "apache-airflow-providers-postgres" = "latest"
    "pytest-airflow" = "latest"
  }
}

# DAG Processing
variable "dag_processor_count" {
  description = "Number of DAG processors to deploy for parallel DAG parsing"
  type        = number
  default     = 2
}

# Resilience Configuration
variable "enable_resilience_mode" {
  description = "Enable resilience mode for Cloud Composer 2 environment"
  type        = bool
  default     = true
}

# Logging Configuration
variable "log_level" {
  description = "Log level for the Cloud Composer 2 environment"
  type        = string
  default     = "INFO"
}