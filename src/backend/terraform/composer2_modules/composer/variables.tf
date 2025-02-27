# ==============================================================================
# BASIC CONFIGURATION
# ==============================================================================

variable "project_id" {
  description = "The Google Cloud project ID where the Cloud Composer 2 environment will be created"
  type        = string

  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id value cannot be empty."
  }
}

variable "region" {
  description = "The Google Cloud region where the Cloud Composer 2 environment will be created"
  type        = string

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1", "europe-west1", "europe-west2", "europe-west3", "europe-west4", "asia-east1", "asia-northeast1", "asia-southeast1"], var.region)
    error_message = "The region must be one of the supported GCP regions for Cloud Composer 2."
  }
}

variable "zone" {
  description = "The Google Cloud zone where the Cloud Composer 2 environment will be created"
  type        = string
}

variable "environment_name" {
  description = "The name of the Cloud Composer 2 environment"
  type        = string

  validation {
    condition     = length(var.environment_name) > 0
    error_message = "The environment_name value cannot be empty."
  }
}

variable "environment_labels" {
  description = "Labels to apply to the Cloud Composer 2 environment"
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Network tags to apply to the instances in the Composer environment"
  type        = list(string)
  default     = []
}

# ==============================================================================
# VERSION CONFIGURATION
# ==============================================================================

variable "composer_version" {
  description = "The version of Cloud Composer 2 to use"
  type        = string
  default     = "2.0.28"
}

variable "airflow_version" {
  description = "The version of Apache Airflow to use in the Cloud Composer 2 environment"
  type        = string
  default     = "2.2.5"
}

variable "image_version" {
  description = "The full image version of Cloud Composer 2 (e.g., composer-2.0.28-airflow-2.2.5). If provided, this takes precedence over composer_version and airflow_version"
  type        = string
  default     = null
}

# ==============================================================================
# ENVIRONMENT SIZE AND SCALING
# ==============================================================================

variable "environment_size" {
  description = "The size of the Cloud Composer 2 environment (small, medium, large)"
  type        = string
  default     = "small"

  validation {
    condition     = contains(["small", "medium", "large"], var.environment_size)
    error_message = "The environment_size must be one of: small, medium, large."
  }
}

variable "environment_size_custom" {
  description = "Whether to use custom environment size configuration instead of predefined sizes"
  type        = bool
  default     = false
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

variable "min_workers" {
  description = "Minimum number of workers in the Cloud Composer 2 environment"
  type        = number
  default     = 2
}

variable "max_workers" {
  description = "Maximum number of workers in the Cloud Composer 2 environment"
  type        = number
  default     = 6
}

# ==============================================================================
# COMPUTE RESOURCE ALLOCATION
# ==============================================================================

variable "scheduler_cpu" {
  description = "CPU allocation for the Airflow scheduler"
  type        = number
  default     = null
}

variable "scheduler_memory_gb" {
  description = "Memory allocation for the Airflow scheduler in GB"
  type        = number
  default     = null
}

variable "scheduler_storage_gb" {
  description = "Storage allocation for the Airflow scheduler in GB"
  type        = number
  default     = null
}

variable "scheduler_count" {
  description = "Number of schedulers to run in the environment"
  type        = number
  default     = 1
}

variable "web_server_cpu" {
  description = "CPU allocation for the Airflow web server"
  type        = number
  default     = null
}

variable "web_server_memory_gb" {
  description = "Memory allocation for the Airflow web server in GB"
  type        = number
  default     = null
}

variable "web_server_storage_gb" {
  description = "Storage allocation for the Airflow web server in GB"
  type        = number
  default     = null
}

variable "worker_cpu" {
  description = "CPU allocation for each Airflow worker"
  type        = number
  default     = null
}

variable "worker_memory_gb" {
  description = "Memory allocation for each Airflow worker in GB"
  type        = number
  default     = null
}

variable "worker_storage_gb" {
  description = "Storage allocation for each Airflow worker in GB"
  type        = number
  default     = null
}

variable "cloud_sql_machine_type" {
  description = "Machine type for the Cloud SQL instance used by the Cloud Composer 2 environment"
  type        = string
  default     = "db-n1-standard-2"
}

variable "web_server_machine_type" {
  description = "Machine type for the Airflow web server in the Cloud Composer 2 environment"
  type        = string
  default     = "composer-n1-webserver-2"
}

variable "dag_processor_count" {
  description = "Number of Airflow DAG processors to deploy for parallel DAG parsing"
  type        = number
  default     = 2
}

# ==============================================================================
# NETWORKING CONFIGURATION
# ==============================================================================

variable "network_id" {
  description = "The VPC network to use for the Cloud Composer 2 environment resources"
  type        = string
  default     = null
}

variable "subnet_id" {
  description = "The subnetwork to use for the Cloud Composer 2 environment resources"
  type        = string
  default     = null
}

variable "pods_ip_range_name" {
  description = "The name of the secondary IP range for GKE pods"
  type        = string
  default     = null
}

variable "services_ip_range_name" {
  description = "The name of the secondary IP range for GKE services"
  type        = string
  default     = null
}

variable "enable_private_environment" {
  description = "Whether to create a private Cloud Composer 2 environment"
  type        = bool
  default     = true
}

variable "enable_private_endpoint" {
  description = "Whether to enable private endpoint for the Cloud Composer 2 environment"
  type        = bool
  default     = true
}

variable "master_ipv4_cidr" {
  description = "The CIDR block for the GKE master in a private Cloud Composer 2 environment"
  type        = string
  default     = "172.16.0.0/28"
}

variable "web_server_ipv4_cidr" {
  description = "The CIDR block for the Airflow web server in a private Cloud Composer 2 environment"
  type        = string
  default     = "172.16.0.16/28"
}

variable "cloud_sql_ipv4_cidr" {
  description = "The CIDR block for the Cloud SQL instance in a private Cloud Composer 2 environment"
  type        = string
  default     = "172.16.0.32/28"
}

variable "web_server_access_control" {
  description = "List of allowed IP ranges for accessing the Airflow web server UI"
  type        = list(object({
    ip_range   = string
    description = string
  }))
  default     = []
}

# ==============================================================================
# SECURITY CONFIGURATION
# ==============================================================================

variable "composer_service_account_id" {
  description = "The service account email to use for the GKE cluster running the Composer environment"
  type        = string
  default     = null
}

variable "enable_cmek" {
  description = "Whether to enable Customer-Managed Encryption Keys (CMEK) for the Cloud Composer 2 environment"
  type        = bool
  default     = false
}

variable "kms_key_name" {
  description = "The full resource name of the KMS key to use for CMEK encryption"
  type        = string
  default     = null
}

# ==============================================================================
# AIRFLOW CONFIGURATION
# ==============================================================================

variable "airflow_config_overrides" {
  description = "Airflow configuration overrides for the Cloud Composer 2 environment"
  type        = map(string)
  default     = {}
}

variable "env_variables" {
  description = "Environment variables to set in the Cloud Composer 2 environment"
  type        = map(string)
  default     = {}
}

variable "pypi_packages" {
  description = "PyPI packages to install in the Cloud Composer 2 environment"
  type        = map(string)
  default     = {}
}

variable "airflow_database_retention_days" {
  description = "Number of days to retain Airflow database data"
  type        = number
  default     = 30
}

# ==============================================================================
# MAINTENANCE CONFIGURATION
# ==============================================================================

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

# ==============================================================================
# RESILIENCE CONFIGURATION
# ==============================================================================

variable "enable_resilience_mode" {
  description = "Whether to enable high resilience mode for the Cloud Composer 2 environment"
  type        = bool
  default     = true
}