terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.34.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.34.0"
    }
  }
  backend "gcs" {
    bucket = "composer2-migration-dev-terraform-state"
    prefix = "composer2-dev"
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
    environment = "dev"
    app         = "airflow"
    project     = "composer2-migration"
    managed_by  = "terraform"
  }
  
  # Airflow configuration overrides specific to development environment
  airflow_config_overrides = {
    core-dags_are_paused_at_creation = "False"
    core-load_examples = "False"
    core-dag_concurrency = "16"
    core-parallelism = "32"
    webserver-dag_orientation = "TB"
    webserver-navbar_color = "#4285f4"
    webserver-log-fetch-timeout-sec = "15"
    scheduler-parsing_processes = "4"
  }
  
  # Environment variables for the development environment
  env_variables = {
    AIRFLOW_VAR_ENV = "dev"
    AIRFLOW_VAR_GCP_PROJECT = var.project_id
    AIRFLOW_VAR_GCS_BUCKET = "${var.project_id}-composer-${var.environment_name}-bucket"
  }
  
  # PyPI packages to install in the development environment
  pypi_packages = {
    "apache-airflow-providers-google" = "latest"
    "apache-airflow-providers-http" = "latest"
    "apache-airflow-providers-postgres" = "latest"
    "pytest-airflow" = "latest"
    "black" = "23.3.0"
    "pylint" = "2.17.4"
  }
}

# Function to validate development environment configuration
resource "null_resource" "validate_dev_environment" {
  # Will throw an error if any validation check fails
  lifecycle {
    precondition {
      condition     = length(regexall("-dev$", var.project_id)) > 0
      error_message = "Project ID '${var.project_id}' does not follow the development naming convention (should end with -dev)."
    }
    
    precondition {
      condition     = length(regexall("^(dev|composer2-dev)", var.environment_name)) > 0
      error_message = "Environment name '${var.environment_name}' should start with 'dev' or be 'composer2-dev'."
    }
    
    precondition {
      condition     = var.environment_size == "small" || var.environment_size == "medium"
      error_message = "Development environments should use 'small' or 'medium' size, not '${var.environment_size}'."
    }
    
    precondition {
      condition     = length(var.web_server_access_control) > 0
      error_message = "web_server_access_control must include at least one entry for development environment."
    }
  }
}

# Networking module configuration
module "networking" {
  source = "../composer2_modules/networking"
  
  project_id = var.project_id
  region     = var.region
  vpc_name   = var.network
  subnet_name = var.subnetwork
  subnet_ip_range = "10.0.0.0/20"
  subnet_secondary_range_name_pods = var.ip_range_pods
  subnet_secondary_range_name_services = var.ip_range_services
  subnet_secondary_ip_range_pods = "10.4.0.0/14"
  subnet_secondary_ip_range_services = "10.0.32.0/20"
  enable_private_ip = true
  enable_private_endpoint = var.enable_private_endpoint
  enable_cloud_nat = true
  create_firewall_rules = true
  allowed_external_ip_ranges = ["0.0.0.0/0"]
  enable_iap = true
}

# Security module configuration
module "security" {
  source = "../composer2_modules/security"
  
  project_id = var.project_id
  region     = var.region
  env_name   = var.environment_name
  composer_service_account_name = "${var.environment_name}-sa"
  use_existing_service_account = var.service_account_email != ""
  existing_service_account_id = var.service_account_email
  service_account_roles = [
    "roles/composer.worker",
    "roles/composer.serviceAgent",
    "roles/storage.objectAdmin",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter"
  ]
  enable_cmek = false
  enable_audit_logging = true
  audit_log_retention_days = 30
  enable_iap = true
  iap_members = []
  create_secrets = true
  secret_rotation_period = "2592000s"
  enable_private_environment = true
  enable_private_endpoint = var.enable_private_endpoint
  web_server_allow_ip_ranges = ["0.0.0.0/0"]
  enable_cloud_sql_ssl = true
  enable_vpc_service_controls = false
  enable_workload_identity = true
}

# Composer module configuration
module "composer" {
  source = "../composer2_modules/composer"
  
  project_id = var.project_id
  region     = var.region
  environment_name = var.environment_name
  composer_version = "2.0.28"
  airflow_version = "2.2.5"
  environment_size = var.environment_size
  node_count = var.node_count
  composer_service_account_id = module.security.service_account_email
  network_id = module.networking.network_id
  subnet_id = module.networking.subnet_id
  pods_ip_range_name = var.ip_range_pods
  services_ip_range_name = var.ip_range_services
  environment_labels = local.environment_labels
  tags = ["composer2", "dev"]
  enable_private_environment = true
  enable_private_endpoint = var.enable_private_endpoint
  master_ipv4_cidr = var.master_ipv4_cidr_block
  web_server_ipv4_cidr = "172.16.0.16/28"
  cloud_sql_ipv4_cidr = "172.16.0.32/28"
  web_server_access_control = var.web_server_access_control
  scheduler_cpu = var.scheduler_cpu
  scheduler_memory_gb = var.scheduler_memory_gb
  scheduler_storage_gb = var.scheduler_storage_gb
  scheduler_count = var.scheduler_count
  web_server_cpu = var.web_server_cpu
  web_server_memory_gb = var.web_server_memory_gb
  web_server_storage_gb = var.web_server_storage_gb
  worker_cpu = var.worker_cpu
  worker_memory_gb = var.worker_memory_gb
  worker_storage_gb = var.worker_storage_gb
  min_workers = var.min_workers
  max_workers = var.max_workers
  cloud_sql_machine_type = var.cloud_sql_machine_type
  web_server_machine_type = "composer-n1-webserver-2"
  airflow_config_overrides = local.airflow_config_overrides
  env_variables = local.env_variables
  pypi_packages = local.pypi_packages
  maintenance_window_start_time = var.maintenance_window_start_time
  maintenance_window_end_time = var.maintenance_window_end_time
  maintenance_recurrence = var.maintenance_recurrence
  enable_cmek = false
  enable_resilience_mode = var.enable_resilience_mode
}

# Outputs
output "composer_environment_id" {
  value       = module.composer.composer_environment_id
  description = "The full resource ID of the Cloud Composer 2 development environment"
}

output "airflow_uri" {
  value       = module.composer.airflow_uri
  description = "The URI to access the Airflow 2.X web interface in the development environment"
}

output "gke_cluster" {
  value       = module.composer.gke_cluster
  description = "The GKE cluster name that hosts the Cloud Composer 2 development environment"
}

output "dag_gcs_prefix" {
  value       = module.composer.dag_gcs_prefix
  description = "The GCS bucket path where DAG files should be stored for the development environment"
}

output "network_id" {
  value       = module.networking.network_id
  description = "The ID of the VPC network used for the development environment"
}

output "subnet_id" {
  value       = module.networking.subnet_id
  description = "The ID of the subnet used for the development environment"
}

output "composer_service_account" {
  value       = module.security.service_account_email
  description = "The service account email used by the Cloud Composer 2 development environment"
}

output "environment_labels" {
  value       = local.environment_labels
  description = "The labels applied to the development environment resources"
}