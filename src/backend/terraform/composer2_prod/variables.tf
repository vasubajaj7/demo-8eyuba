# Basic Configuration Variables
variable "project_id" {
  description = "The GCP project ID where Cloud Composer 2 will be deployed"
  type        = string
  default     = "composer2-migration-project-prod"

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
  default     = "composer2-prod"

  validation {
    condition     = length(var.environment_name) > 0
    error_message = "The environment_name value cannot be empty."
  }
}

variable "environment_size" {
  description = "The size of the Cloud Composer 2 environment (small, medium, large)"
  type        = string
  default     = "large"

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

# Cluster Configuration Variables
variable "node_count" {
  description = "The number of nodes to provision for the GKE cluster"
  type        = number
  default     = 6

  validation {
    condition     = var.node_count >= 6
    error_message = "At least 6 nodes are required for production high availability."
  }
}

variable "machine_type" {
  description = "The machine type to use for GKE nodes"
  type        = string
  default     = "n1-standard-4"
}

variable "disk_size_gb" {
  description = "The disk size in GB for GKE nodes"
  type        = number
  default     = 100

  validation {
    condition     = var.disk_size_gb >= 100
    error_message = "At least 100 GB of disk space is required for production nodes."
  }
}

# Network Configuration Variables
variable "network" {
  description = "The VPC network to use for the Cloud Composer 2 environment"
  type        = string
  default     = "composer2-migration-prod-network"
}

variable "subnetwork" {
  description = "The VPC subnetwork to use for the Cloud Composer 2 environment"
  type        = string
  default     = "composer2-migration-prod-subnet"
}

variable "ip_range_pods" {
  description = "The secondary IP range name for pods"
  type        = string
  default     = "composer-pods-prod"
}

variable "ip_range_services" {
  description = "The secondary IP range name for services"
  type        = string
  default     = "composer-services-prod"
}

variable "service_account_email" {
  description = "The service account email to use for the GKE cluster"
  type        = string
  default     = ""
}

# Security Configuration Variables
variable "enable_private_endpoint" {
  description = "Whether to enable private endpoint for the GKE cluster"
  type        = bool
  default     = true
}

variable "master_ipv4_cidr_block" {
  description = "The CIDR block for the GKE master"
  type        = string
  default     = "172.16.0.32/28"
}

variable "master_authorized_networks" {
  description = "List of CIDR blocks authorized to access the GKE master"
  type        = list(object({
    cidr_block   = string
    display_name = string
  }))
  default     = [
    {
      cidr_block   = "10.0.0.0/8"
      display_name = "Internal Corporate Network"
    }
  ]
}

variable "web_server_access_control" {
  description = "IAP configuration for web server access control"
  type        = list(object({
    ip_range   = string
    description = string
  }))
  default     = [
    {
      ip_range   = "10.0.0.0/8"
      description = "Internal Corporate Network"
    }
  ]
}

# Airflow Environment Variables
variable "environment_variables" {
  description = "Environment variables to set in the Cloud Composer 2 environment"
  type        = map(string)
  default     = {
    "AIRFLOW_VAR_ENV" = "prod"
    "AIRFLOW_VAR_GCP_PROJECT" = "composer2-migration-project-prod"
    "AIRFLOW_VAR_GCS_BUCKET" = "composer2-migration-prod-bucket"
  }
}

# Scheduler Configuration
variable "scheduler_cpu" {
  description = "CPU allocation for Airflow scheduler"
  type        = number
  default     = 8
}

variable "scheduler_memory_gb" {
  description = "Memory allocation for Airflow scheduler in GB"
  type        = number
  default     = 30.0
}

variable "scheduler_storage_gb" {
  description = "Storage allocation for Airflow scheduler in GB"
  type        = number
  default     = 50
}

variable "scheduler_count" {
  description = "Number of schedulers to run in the environment"
  type        = number
  default     = 3

  validation {
    condition     = var.scheduler_count >= 2
    error_message = "At least 2 schedulers are required for high availability in production."
  }
}

# Web Server Configuration
variable "web_server_cpu" {
  description = "CPU allocation for Airflow web server"
  type        = number
  default     = 4
}

variable "web_server_memory_gb" {
  description = "Memory allocation for Airflow web server in GB"
  type        = number
  default     = 15.0
}

variable "web_server_storage_gb" {
  description = "Storage allocation for Airflow web server in GB"
  type        = number
  default     = 30
}

# Worker Configuration
variable "worker_cpu" {
  description = "CPU allocation for Airflow worker"
  type        = number
  default     = 8
}

variable "worker_memory_gb" {
  description = "Memory allocation for Airflow worker in GB"
  type        = number
  default     = 30.0
}

variable "worker_storage_gb" {
  description = "Storage allocation for Airflow worker in GB"
  type        = number
  default     = 100
}

variable "min_workers" {
  description = "Minimum number of workers in the environment"
  type        = number
  default     = 4

  validation {
    condition     = var.min_workers >= 3
    error_message = "At least 3 workers are required for high availability in production."
  }
}

variable "max_workers" {
  description = "Maximum number of workers in the environment"
  type        = number
  default     = 12

  validation {
    condition     = var.max_workers >= var.min_workers
    error_message = "Maximum workers must be greater than or equal to minimum workers."
  }
}

# Cloud SQL Configuration
variable "cloud_sql_machine_type" {
  description = "Machine type for the Cloud SQL instance"
  type        = string
  default     = "db-n1-standard-8"
}

variable "cloud_sql_database_version" {
  description = "Database version for the Cloud SQL instance"
  type        = string
  default     = "POSTGRES_13"
}

variable "cloud_sql_disk_size_gb" {
  description = "Disk size in GB for the Cloud SQL instance"
  type        = number
  default     = 100

  validation {
    condition     = var.cloud_sql_disk_size_gb >= 100
    error_message = "At least 100 GB of disk space is required for production Cloud SQL."
  }
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

variable "backup_configuration" {
  description = "Backup configuration for Cloud SQL instance"
  type        = object({
    enabled                        = bool
    start_time                     = string
    location                       = string
    point_in_time_recovery_enabled = bool
    transaction_log_retention_days = number
    retained_backups               = number
    retention_unit                 = string
  })
  default     = {
    enabled                        = true
    start_time                     = "02:00"
    location                       = "us"
    point_in_time_recovery_enabled = true
    transaction_log_retention_days = 7
    retained_backups               = 30
    retention_unit                 = "COUNT"
  }
}

# Airflow Configuration
variable "airflow_config_overrides" {
  description = "Airflow configuration overrides for the environment"
  type        = map(string)
  default     = {
    "core.parallelism"                  = "64"
    "core.dag_concurrency"              = "48"
    "core.max_active_runs_per_dag"      = "16"
    "scheduler.parsing_processes"       = "8"
    "scheduler.min_file_process_interval" = "60"
    "scheduler.catchup_by_default"      = "False"
    "webserver.workers"                 = "8"
    "webserver.worker_refresh_batch_size" = "4"
    "webserver.web_server_worker_timeout" = "120"
    "celery.worker_concurrency"         = "16"
    "celery.worker_prefetch_multiplier" = "1"
    "email.email_backend"               = "airflow.utils.email.send_email_smtp"
    "smtp.smtp_host"                    = "smtp.example.com"
    "smtp.smtp_port"                    = "587"
    "smtp.smtp_starttls"                = "True"
    "smtp.smtp_ssl"                     = "False"
    "smtp.smtp_user"                    = "airflow@example.com"
    "smtp.smtp_mail_from"               = "airflow@example.com"
  }
}

# Maintenance Configuration
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
  default     = "FREQ=WEEKLY;BYDAY=SA"
}

variable "airflow_database_retention_days" {
  description = "Number of days to retain Airflow database data"
  type        = number
  default     = 90
}

# Python Package Configuration
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

# Advanced Configuration
variable "dag_processor_count" {
  description = "Number of DAG processors to deploy for parallel DAG parsing"
  type        = number
  default     = 4

  validation {
    condition     = var.dag_processor_count >= 2
    error_message = "At least 2 DAG processors are required for production environment."
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

# Security Configuration
variable "enable_cmek" {
  description = "Enable Customer-Managed Encryption Keys (CMEK) for Cloud Composer 2"
  type        = bool
  default     = true
}

variable "kms_key_ring" {
  description = "Name of the Cloud KMS key ring for CMEK encryption"
  type        = string
  default     = "composer2-prod-keyring"
}

variable "kms_key_name" {
  description = "Name of the Cloud KMS key for CMEK encryption"
  type        = string
  default     = "composer2-prod-key"
}

variable "enable_binary_authorization" {
  description = "Enable Binary Authorization for the GKE cluster"
  type        = bool
  default     = true
}

# Airflow Pools Configuration
variable "pools" {
  description = "Airflow pools to be created in the environment"
  type        = map(object({
    description = string
    slots = number
  }))
  default     = {
    "default_pool" = {
      description = "Default pool for tasks without explicit pool assignment"
      slots = 128
    },
    "etl_pool" = {
      description = "Pool for ETL tasks"
      slots = 64
    },
    "reporting_pool" = {
      description = "Pool for reporting tasks"
      slots = 32
    }
  }
}