# Variables for the security module of Cloud Composer 2 deployment

variable "project_id" {
  description = "The GCP project ID where security resources will be deployed"
  type        = string
}

variable "region" {
  description = "The GCP region where security resources will be deployed"
  type        = string
  default     = "us-central1"
}

variable "env_name" {
  description = "Environment name (dev, qa, prod) for resource naming and configuration"
  type        = string
  default     = "dev"
}

# Service account configuration
variable "composer_service_account_name" {
  description = "Name of the service account to create for Cloud Composer 2"
  type        = string
  default     = "composer2-sa"
}

variable "composer_service_account_display_name" {
  description = "Display name for the Cloud Composer 2 service account"
  type        = string
  default     = "Cloud Composer 2 Service Account"
}

variable "composer_service_account_description" {
  description = "Description for the Cloud Composer 2 service account"
  type        = string
  default     = "Service account for Cloud Composer 2 with Airflow 2.X"
}

variable "use_existing_service_account" {
  description = "Whether to use an existing service account instead of creating a new one"
  type        = bool
  default     = false
}

variable "existing_service_account_id" {
  description = "Email address of existing service account if use_existing_service_account is true"
  type        = string
  default     = ""
}

variable "service_account_roles" {
  description = "List of IAM roles to assign to the Cloud Composer 2 service account"
  type        = list(string)
  default = [
    "roles/composer.worker",
    "roles/composer.serviceAgent",
    "roles/storage.objectAdmin",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter"
  ]
}

# Encryption configuration
variable "enable_cmek" {
  description = "Whether to enable Customer-Managed Encryption Keys (CMEK) for Cloud Composer 2"
  type        = bool
  default     = false
}

variable "create_kms_resources" {
  description = "Whether to create new KMS resources or use existing ones"
  type        = bool
  default     = true
}

variable "kms_project_id" {
  description = "Project ID where KMS resources exist or should be created (defaults to project_id if empty)"
  type        = string
  default     = ""
}

variable "kms_keyring_name" {
  description = "Name of the KMS keyring for CMEK encryption"
  type        = string
  default     = "composer2-keyring"
}

variable "kms_key_name" {
  description = "Name of the KMS key for CMEK encryption"
  type        = string
  default     = "composer2-key"
}

# Audit logging configuration
variable "enable_audit_logging" {
  description = "Whether to enable audit logging for Cloud Composer 2 environment"
  type        = bool
  default     = true
}

variable "audit_log_retention_days" {
  description = "Number of days to retain audit logs"
  type        = number
  default     = 30
}

# Identity-Aware Proxy (IAP) configuration
variable "enable_iap" {
  description = "Whether to enable Identity-Aware Proxy (IAP) for accessing Cloud Composer 2 web interface"
  type        = bool
  default     = true
}

variable "iap_members" {
  description = "List of members (users/groups) to grant IAP access (e.g., 'user:user@example.com', 'group:group@example.com')"
  type        = list(string)
  default     = []
}

variable "support_email" {
  description = "Support email for IAP brand verification (required when enable_iap=true)"
  type        = string
  default     = ""
}

# Secret Manager configuration
variable "create_secrets" {
  description = "Whether to create Secret Manager secrets for storing sensitive Cloud Composer 2 configurations"
  type        = bool
  default     = true
}

variable "secret_manager_prefix" {
  description = "Prefix for Secret Manager secret names"
  type        = string
  default     = "composer2"
}

variable "secret_rotation_period" {
  description = "Rotation period for secrets in Secret Manager (RFC3339 duration format)"
  type        = string
  default     = "2592000s" # 30 days
}

# Network security configuration
variable "enable_private_environment" {
  description = "Whether the Cloud Composer 2 environment should use private IP addresses"
  type        = bool
  default     = true
}

variable "enable_private_endpoint" {
  description = "Whether to create a private endpoint for the Cloud Composer 2 environment"
  type        = bool
  default     = false
}

variable "web_server_allow_ip_ranges" {
  description = "List of IP ranges allowed to access the Airflow web server"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "enable_cloud_sql_ssl" {
  description = "Whether to enable SSL for Cloud SQL connections"
  type        = bool
  default     = true
}

# Service controls and workload identity
variable "enable_vpc_service_controls" {
  description = "Whether to enable VPC Service Controls for the Cloud Composer 2 environment"
  type        = bool
  default     = false
}

variable "enable_workload_identity" {
  description = "Whether to enable Workload Identity for GKE clusters"
  type        = bool
  default     = true
}

# OAuth scopes for service account
variable "oauth_scopes" {
  description = "List of OAuth scopes to grant to the service account"
  type        = list(string)
  default     = ["https://www.googleapis.com/auth/cloud-platform"]
}