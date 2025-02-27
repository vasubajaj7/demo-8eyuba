variable "project_id" {
  description = "The GCP project ID where Cloud Composer 2 will be deployed"
  type        = string
  default     = "composer2-migration-project-qa"

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

variable "environment_name" {
  description = "The name of the Cloud Composer 2 environment"
  type        = string
  default     = "composer2-qa"

  validation {
    condition     = length(var.environment_name) > 0
    error_message = "The environment_name value cannot be empty."
  }
}

variable "environment_size" {
  description = "The size of the Cloud Composer 2 environment (small, medium, large)"
  type        = string
  default     = "medium"

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
  default     = 4

  validation {
    condition     = var.node_count >= 3
    error_message = "At least 3 nodes are required for high availability."
  }
}

variable "network" {
  description = "The VPC network to use for the Cloud Composer 2 environment"
  type        = string
  default     = "composer2-migration-qa-network"
}

variable "subnetwork" {
  description = "The VPC subnetwork to use for the Cloud Composer 2 environment"
  type        = string
  default     = "composer2-migration-qa-subnet"
}

variable "ip_range_pods" {
  description = "The secondary IP range name for pods"
  type        = string
  default     = "composer-pods-qa"
}

variable "ip_range_services" {
  description = "The secondary IP range name for services"
  type        = string
  default     = "composer-services-qa"
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
  default     = "172.17.0.0/28"
}

variable "web_server_access_control" {
  description = "IAP configuration for web server access control"
  type        = list(object({
    ip_range   = string
    description = string
  }))
  default     = [{
    ip_range   = "0.0.0.0/0"
    description = "Allow access from all IP addresses for QA testing"
  }]
}

variable "environment_variables" {
  description = "Environment variables to set in the Cloud Composer 2 environment"
  type        = map(string)
  default     = {
    "AIRFLOW_VAR_ENV" = "qa"
    "AIRFLOW_VAR_GCP_PROJECT" = "composer2-migration-project-qa"
    "AIRFLOW_VAR_GCS_BUCKET" = "composer2-migration-qa-bucket"
  }
}

variable "scheduler_cpu" {
  description = "CPU allocation for Airflow scheduler"
  type        = number
  default     = 4
}

variable "scheduler_memory_gb" {
  description = "Memory allocation for Airflow scheduler in GB"
  type        = number
  default     = 15.0
}

variable "scheduler_storage_gb" {
  description = "Storage allocation for Airflow scheduler in GB"
  type        = number
  default     = 20
}

variable "scheduler_count" {
  description = "Number of schedulers to run in the environment"
  type        = number
  default     = 2

  validation {
    condition     = var.scheduler_count >= 1
    error_message = "At least 1 scheduler is required."
  }
}

variable "web_server_cpu" {
  description = "CPU allocation for Airflow web server"
  type        = number
  default     = 2
}

variable "web_server_memory_gb" {
  description = "Memory allocation for Airflow web server in GB"
  type        = number
  default     = 7.5
}

variable "web_server_storage_gb" {
  description = "Storage allocation for Airflow web server in GB"
  type        = number
  default     = 20
}

variable "worker_cpu" {
  description = "CPU allocation for Airflow worker"
  type        = number
  default     = 4
}

variable "worker_memory_gb" {
  description = "Memory allocation for Airflow worker in GB"
  type        = number
  default     = 15.0
}

variable "worker_storage_gb" {
  description = "Storage allocation for Airflow worker in GB"
  type        = number
  default     = 50
}

variable "min_workers" {
  description = "Minimum number of workers in the environment"
  type        = number
  default     = 3

  validation {
    condition     = var.min_workers >= 2
    error_message = "At least 2 workers are required for high availability."
  }
}

variable "max_workers" {
  description = "Maximum number of workers in the environment"
  type        = number
  default     = 8

  validation {
    condition     = var.max_workers >= var.min_workers
    error_message = "Maximum workers must be greater than or equal to minimum workers."
  }
}

variable "cloud_sql_machine_type" {
  description = "Machine type for the Cloud SQL instance"
  type        = string
  default     = "db-n1-standard-4"
}

variable "cloud_sql_database_version" {
  description = "Database version for the Cloud SQL instance"
  type        = string
  default     = "POSTGRES_13"
}

variable "cloud_sql_disk_size_gb" {
  description = "Disk size in GB for the Cloud SQL instance"
  type        = number
  default     = 50
}

variable "cloud_sql_require_ssl" {
  description = "Require SSL connections to Cloud SQL instance"
  type        = bool
  default     = true
}

variable "enable_high_availability" {
  description = "Enable high availability configuration for Cloud SQL"
  type        = bool
  default     = true
}

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
  default     = "FREQ=WEEKLY;BYDAY=SU"
}

variable "airflow_database_retention_days" {
  description = "Number of days to retain Airflow database data"
  type        = number
  default     = 60
}

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

variable "dag_processor_count" {
  description = "Number of DAG processors to deploy for parallel DAG parsing"
  type        = number
  default     = 2

  validation {
    condition     = var.dag_processor_count >= 2
    error_message = "At least 2 DAG processors are required for QA environment."
  }
}

variable "enable_resilience_mode" {
  description = "Enable resilience mode for Cloud Composer 2 environment"
  type        = bool
  default     = true
}

variable "log_level" {
  description = "Log level for the Cloud Composer 2 environment"
  type        = string
  default     = "INFO"
}

variable "enable_web_server_https" {
  description = "Whether to enable HTTPS for the web server"
  type        = bool
  default     = true
}

variable "enable_load_balancer" {
  description = "Whether to enable load balancer for web server"
  type        = bool
  default     = true
}

variable "enable_cloud_data_lineage_integration" {
  description = "Enable Cloud Data Lineage integration"
  type        = bool
  default     = true
}

variable "enable_cmek" {
  description = "Enable Customer-Managed Encryption Keys (CMEK) for Cloud Composer 2"
  type        = bool
  default     = true
}