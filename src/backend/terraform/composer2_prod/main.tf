# Terraform configuration for deploying a Cloud Composer 2 production environment with Airflow 2.X.
# This file orchestrates the provisioning of all necessary infrastructure components by integrating the networking, security, and composer modules
# with production-grade settings for high availability, enhanced security, and optimal performance.

terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.34.0" # Google Cloud Platform provider version
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.34.0" # Google Cloud Platform Beta provider version
    }
  }
  backend "gcs" {
    bucket = "composer2-migration-prod-terraform-state"
    prefix = "composer2-prod"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

locals {
  # Environment labels to identify resources
  environment_labels = {
    environment = "prod"
    app         = "airflow"
    project     = "composer2-migration"
    managed_by  = "terraform"
    compliance  = "hipaa"
    cost_center = "cloud-operations"
  }
  
  # Airflow configuration overrides specific to production environment
  airflow_config_overrides = {
    core-dags_are_paused_at_creation = "True"
    core-load_examples = "False"
    core-dag_concurrency = "48"
    core-parallelism = "64"
    core-max_active_runs_per_dag = "16"
    scheduler-parsing_processes = "8"
    scheduler-min_file_process_interval = "60"
    scheduler-catchup_by_default = "False"
    webserver-workers = "8"
    webserver-worker_refresh_batch_size = "4"
    webserver-web_server_worker_timeout = "120"
    celery-worker_concurrency = "16"
    celery-worker_prefetch_multiplier = "1"
    email-email_backend = "airflow.utils.email.send_email_smtp"
    smtp-smtp_host = "smtp.example.com"
    smtp-smtp_port = "587"
    smtp-smtp_starttls = "True"
    smtp-smtp_ssl = "False"
    smtp-smtp_user = "airflow@example.com"
    smtp-smtp_mail_from = "airflow@example.com"
  }
  
  # Environment variables for the production environment
  env_variables = {
    AIRFLOW_VAR_ENV = "prod"
    AIRFLOW_VAR_GCP_PROJECT = var.project_id
    AIRFLOW_VAR_GCS_BUCKET = "${var.project_id}-composer-${var.environment_name}-bucket"
  }
  
  # PyPI packages to install in the production environment
  pypi_packages = {
    "apache-airflow-providers-google" = "latest"
    "apache-airflow-providers-http" = "latest"
    "apache-airflow-providers-postgres" = "latest"
    "pytest-airflow" = "latest"
  }
}

# module.networking: Configures network infrastructure for Cloud Composer 2 production environment with enhanced security controls
module "networking" {
  source = "../composer2_modules/networking"

  project_id                       = var.project_id
  region                            = var.region
  vpc_name                          = var.network
  subnet_name                       = var.subnetwork
  subnet_ip_range                   = "10.0.0.0/20"
  subnet_secondary_range_name_pods    = var.ip_range_pods
  subnet_secondary_range_name_services = var.ip_range_services
  subnet_secondary_ip_range_pods    = "10.4.0.0/14"
  subnet_secondary_ip_range_services = "10.0.32.0/20"
  enable_private_ip                 = true
  enable_private_endpoint           = var.enable_private_endpoint
  enable_cloud_nat                  = true
  create_firewall_rules            = true
  allowed_external_ip_ranges        = var.web_server_access_control[*].ip_range
  enable_iap                        = true
  network_topology                  = "hub-and-spoke"
  peering_network_ids               = []
}

# module.security: Configures security aspects for Cloud Composer 2 production environment including CMEK, IAM, and audit logging
module "security" {
  source = "../composer2_modules/security"

  project_id                       = var.project_id
  region                            = var.region
  env_name                          = var.environment_name
  composer_service_account_name     = "${var.environment_name}-sa"
  use_existing_service_account      = var.service_account_email != ""
  existing_service_account_id       = var.service_account_email
  service_account_roles             = [
    "roles/composer.worker",
    "roles/composer.serviceAgent",
    "roles/storage.objectAdmin",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/cloudkms.cryptoKeyEncrypterDecrypter",
    "roles/secretmanager.secretAccessor"
  ]
  enable_cmek                       = var.enable_cmek
  create_kms_resources              = var.enable_cmek
  kms_keyring_name                  = var.kms_key_ring
  kms_key_name                      = var.kms_key_name
  enable_audit_logging              = true
  audit_log_retention_days          = 90
  enable_iap                        = true
  iap_members                       = var.master_authorized_networks[*].display_name
  create_secrets                    = true
  secret_rotation_period            = "7776000s"
  enable_private_environment        = true
  enable_private_endpoint           = var.enable_private_endpoint
  web_server_allow_ip_ranges        = var.web_server_access_control[*].ip_range
  enable_cloud_sql_ssl              = var.cloud_sql_require_ssl
  enable_vpc_service_controls       = true
  enable_workload_identity          = true
  enable_binary_authorization       = var.enable_binary_authorization
}

# module.composer: Creates and configures the Cloud Composer 2 production environment with high availability and enterprise settings
module "composer" {
  source = "../composer2_modules/composer"

  project_id                  = var.project_id
  region                       = var.region
  environment_name            = var.environment_name
  composer_version            = var.composer_version
  airflow_version             = var.airflow_version
  environment_size            = var.environment_size
  node_count                  = var.node_count
  composer_service_account_id = module.security.service_account_email
  network_id                  = module.networking.network_id
  subnet_id                     = module.networking.subnet_id
  pods_ip_range_name          = var.ip_range_pods
  services_ip_range_name      = var.ip_range_services
  environment_labels          = local.environment_labels
  tags                        = ["composer2", "prod", "pci-compliant"]
  enable_private_environment  = true
  enable_private_endpoint     = var.enable_private_endpoint
  master_ipv4_cidr            = var.master_ipv4_cidr_block
  web_server_ipv4_cidr        = "172.16.0.16/28"
  cloud_sql_ipv4_cidr         = "172.16.0.32/28"
  web_server_access_control   = var.web_server_access_control
  scheduler_cpu               = var.scheduler_cpu
  scheduler_memory_gb         = var.scheduler_memory_gb
  scheduler_storage_gb        = var.scheduler_storage_gb
  scheduler_count             = var.scheduler_count
  web_server_cpu              = var.web_server_cpu
  web_server_memory_gb        = var.web_server_memory_gb
  web_server_storage_gb       = var.web_server_storage_gb
  worker_cpu                  = var.worker_cpu
  worker_memory_gb            = var.worker_memory_gb
  worker_storage_gb           = var.worker_storage_gb
  min_workers                 = var.min_workers
  max_workers                 = var.max_workers
  cloud_sql_machine_type      = var.cloud_sql_machine_type
  web_server_machine_type     = "composer-n1-webserver-4"
  airflow_config_overrides    = local.airflow_config_overrides
  env_variables               = local.env_variables
  pypi_packages               = local.pypi_packages
  maintenance_window_start_time = var.maintenance_window_start_time
  maintenance_window_end_time   = var.maintenance_window_end_time
  maintenance_recurrence        = var.maintenance_recurrence
  enable_cmek                   = var.enable_cmek
  kms_key_name                = var.enable_cmek ? module.security.kms_key_id : null
  enable_resilience_mode      = var.enable_resilience_mode
}

# Export the Cloud Composer 2 environment ID
output "composer_environment_id" {
  value       = module.composer.composer_environment_id
  description = "The full resource ID of the Cloud Composer 2 production environment"
}

# Export the Airflow URI
output "airflow_uri" {
  value       = module.composer.airflow_uri
  description = "The URI to access the Airflow 2.X web interface in the production environment"
}

# Export the GKE cluster name
output "gke_cluster" {
  value       = module.composer.gke_cluster
  description = "The GKE cluster name that hosts the Cloud Composer 2 production environment"
}

# Export the DAG GCS prefix
output "dag_gcs_prefix" {
  value       = module.composer.dag_gcs_prefix
  description = "The GCS bucket path where DAG files should be stored for the production environment"
}

# Export the network ID
output "network_id" {
  value       = module.networking.network_id
  description = "The ID of the VPC network used for the production environment"
}

# Export the subnet ID
output "subnet_id" {
  value       = module.networking.subnet_id
  description = "The ID of the subnet used for the production environment"
}

# Export the composer service account
output "composer_service_account" {
  value       = module.security.service_account_email
  description = "The service account email used by the Cloud Composer 2 production environment"
}

# Export the environment labels
output "environment_labels" {
  value       = local.environment_labels
  description = "The labels applied to the production environment resources"
}

# Export the security configuration
output "security_config" {
  value       = module.security.security_config
  description = "Security configuration details for the production environment"
}

# Export high availability configuration details
output "high_availability_config" {
  value = {
    scheduler_count = var.scheduler_count
    min_workers     = var.min_workers
    max_workers     = var.max_workers
  }
  description = "High availability configuration details for the production environment"
}

# Export resilience mode enabled status
output "resilience_mode_enabled" {
  value       = var.enable_resilience_mode
  description = "Whether resilience mode is enabled for the production environment"
}