# Terraform module for provisioning Cloud Composer 2 environments with Airflow 2.X
# This module creates and configures all required GCP resources for a production-ready
# Cloud Composer 2 environment, handling infrastructure, networking, scaling, and security.

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
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9.1"
    }
  }
}

provider "google" {
  # Configuration is inherited from parent module
}

provider "google-beta" {
  # Configuration is inherited from parent module
}

locals {
  # Compute the image version string if not explicitly provided
  image_version = var.image_version != null ? var.image_version : "composer-${var.composer_version}-airflow-${var.airflow_version}"
  
  # Default resource configurations for environment sizes
  environment_sizes = {
    small = {
      node_count        = 3
      scheduler_cpu     = 0.5
      scheduler_memory  = 1.875
      scheduler_storage = 1
      web_server_cpu    = 0.5
      web_server_memory = 1.875
      web_server_storage = 1
      worker_cpu        = 0.5
      worker_memory     = 1.875
      worker_storage    = 1
    },
    medium = {
      node_count        = 3
      scheduler_cpu     = 2.0
      scheduler_memory  = 7.5
      scheduler_storage = 5
      web_server_cpu    = 2.0
      web_server_memory = 7.5
      web_server_storage = 5
      worker_cpu        = 2.0
      worker_memory     = 7.5
      worker_storage    = 5
    },
    large = {
      node_count        = 6
      scheduler_cpu     = 4.0
      scheduler_memory  = 15.0
      scheduler_storage = 10
      web_server_cpu    = 2.0
      web_server_memory = 7.5
      web_server_storage = 5
      worker_cpu        = 4.0
      worker_memory     = 15.0
      worker_storage    = 10
    }
  }
  
  # Select size configuration or use custom values
  size_config = var.environment_size_custom ? {} : local.environment_sizes[var.environment_size]
  
  # Compute actual values to use (custom values take precedence over size defaults)
  node_count = var.node_count != null ? var.node_count : lookup(local.size_config, "node_count", 3)
  scheduler_cpu = var.scheduler_cpu != null ? var.scheduler_cpu : lookup(local.size_config, "scheduler_cpu", 0.5)
  scheduler_memory_gb = var.scheduler_memory_gb != null ? var.scheduler_memory_gb : lookup(local.size_config, "scheduler_memory", 1.875)
  scheduler_storage_gb = var.scheduler_storage_gb != null ? var.scheduler_storage_gb : lookup(local.size_config, "scheduler_storage", 1)
  web_server_cpu = var.web_server_cpu != null ? var.web_server_cpu : lookup(local.size_config, "web_server_cpu", 0.5)
  web_server_memory_gb = var.web_server_memory_gb != null ? var.web_server_memory_gb : lookup(local.size_config, "web_server_memory", 1.875)
  web_server_storage_gb = var.web_server_storage_gb != null ? var.web_server_storage_gb : lookup(local.size_config, "web_server_storage", 1)
  worker_cpu = var.worker_cpu != null ? var.worker_cpu : lookup(local.size_config, "worker_cpu", 0.5)
  worker_memory_gb = var.worker_memory_gb != null ? var.worker_memory_gb : lookup(local.size_config, "worker_memory", 1.875)
  worker_storage_gb = var.worker_storage_gb != null ? var.worker_storage_gb : lookup(local.size_config, "worker_storage", 1)
  
  # Validate required inputs
  validate_project_id = var.project_id != "" ? true : file("ERROR: project_id must be provided")
  
  # Validate environment name format (lowercase letters, numbers, and hyphens)
  validate_env_name = length(regexall("^[a-z]([-a-z0-9]*[a-z0-9])?$", var.environment_name)) > 0 ? true : file("ERROR: environment_name must consist of lowercase letters, numbers, and hyphens, and must start with a letter")
  
  # Validate network configuration
  validate_network = var.enable_private_endpoint && (var.network_id == "" || var.subnet_id == "") ? file("ERROR: network_id and subnet_id must be provided when enable_private_endpoint is true") : true
  
  # Validate service account configuration
  validate_service_account = var.composer_service_account_id == "" && var.create_composer_service_account == false ? file("ERROR: Either composer_service_account_id must be provided or create_composer_service_account must be true") : true
  
  # Validate CMEK configuration
  validate_cmek = var.enable_cmek && var.kms_key_name == "" ? file("ERROR: kms_key_name must be provided when enable_cmek is true") : true
}

# Create a dedicated service account for Composer if requested
resource "google_service_account" "composer_service_account" {
  count = var.create_composer_service_account ? 1 : 0
  
  account_id   = "${var.environment_name}-sa"
  display_name = "Service Account for Composer Environment ${var.environment_name}"
  project      = var.project_id
}

# Grant necessary roles to the service account
resource "google_project_iam_member" "composer_service_account_roles" {
  count = var.create_composer_service_account ? length(var.composer_service_account_roles) : 0
  
  project = var.project_id
  role    = var.composer_service_account_roles[count.index]
  member  = "serviceAccount:${google_service_account.composer_service_account[0].email}"
}

# IAM propagation delay to ensure permissions are properly set before creating Composer environment
resource "time_sleep" "wait_for_iam_propagation" {
  count = var.create_composer_service_account ? 1 : 0
  
  depends_on = [
    google_project_iam_member.composer_service_account_roles
  ]
  
  create_duration = "30s"
}

# Cloud Composer 2 environment
resource "google_composer_environment" "composer_env" {
  provider = google-beta
  
  name    = var.environment_name
  region  = var.region
  project = var.project_id
  labels  = var.environment_labels
  
  config {
    node_config {
      network    = var.network_id
      subnetwork = var.subnet_id
      
      service_account = var.create_composer_service_account ? google_service_account.composer_service_account[0].email : var.composer_service_account_id
      
      oauth_scopes = [
        "https://www.googleapis.com/auth/cloud-platform"
      ]
      
      dynamic "ip_allocation_policy" {
        for_each = var.network_id != "" ? [1] : []
        content {
          cluster_secondary_range_name = var.pods_ip_range_name
          services_secondary_range_name = var.services_ip_range_name
        }
      }
      
      tags = var.tags
    }
    
    software_config {
      image_version = local.image_version
      
      airflow_config_overrides = var.airflow_config_overrides
      env_variables           = var.env_variables
      pypi_packages           = var.pypi_packages
    }
    
    node_count = local.node_count
    
    dynamic "private_environment_config" {
      for_each = var.enable_private_endpoint ? [1] : []
      content {
        enable_private_endpoint        = true
        master_ipv4_cidr_block        = var.master_ipv4_cidr
        web_server_ipv4_cidr_block    = var.web_server_ipv4_cidr
        cloud_sql_ipv4_cidr_block     = var.cloud_sql_ipv4_cidr
        enable_privately_used_public_ips = true
      }
    }
    
    web_server_config {
      machine_type = var.web_server_machine_type
    }
    
    database_config {
      machine_type = var.cloud_sql_machine_type
    }
    
    dynamic "maintenance_window" {
      for_each = var.maintenance_window_start_time != "" ? [1] : []
      content {
        start_time  = var.maintenance_window_start_time
        end_time    = var.maintenance_window_end_time
        recurrence  = var.maintenance_recurrence
      }
    }
    
    workloads_config {
      scheduler {
        cpu         = local.scheduler_cpu
        memory_gb   = local.scheduler_memory_gb
        storage_gb  = local.scheduler_storage_gb
        count       = var.scheduler_count
      }
      
      web_server {
        cpu         = local.web_server_cpu
        memory_gb   = local.web_server_memory_gb
        storage_gb  = local.web_server_storage_gb
      }
      
      worker {
        cpu         = local.worker_cpu
        memory_gb   = local.worker_memory_gb
        storage_gb  = local.worker_storage_gb
        min_count   = var.min_workers
        max_count   = var.max_workers
      }
    }
    
    resilience_mode = var.enable_resilience_mode ? "HIGH_RESILIENCE" : "STANDARD_RESILIENCE"
  }
  
  dynamic "encryption_config" {
    for_each = var.enable_cmek ? [1] : []
    content {
      kms_key_name = var.kms_key_name
    }
  }
  
  depends_on = [
    time_sleep.wait_for_iam_propagation
  ]
}

# Extract GCS bucket names from the Composer environment for IAM permissions
locals {
  dag_gcs_prefix = google_composer_environment.composer_env.config[0].dag_gcs_prefix
  dag_bucket = length(regexall("gs://([^/]+)/.*", local.dag_gcs_prefix)) > 0 ? regex("gs://([^/]+)/.*", local.dag_gcs_prefix)[0] : ""
}

# Grant IAM permissions to access DAG bucket
resource "google_storage_bucket_iam_member" "dag_bucket_iam" {
  for_each = var.dag_bucket_access_members != null ? var.dag_bucket_access_members : {}
  
  bucket = local.dag_bucket
  role   = each.value
  member = each.key
  
  depends_on = [
    google_composer_environment.composer_env
  ]
}

# Grant IAM permissions to access Cloud SQL instance
resource "google_sql_database_instance_iam_member" "sql_instance_iam" {
  for_each = var.cloud_sql_access_members != null ? var.cloud_sql_access_members : {}
  
  # Extract Cloud SQL instance name from the connection string
  instance = split(":", split("/", google_composer_environment.composer_env.config[0].database_config[0].instance_name)[3])[0]
  project  = var.project_id
  role     = each.value
  member   = each.key
  
  depends_on = [
    google_composer_environment.composer_env
  ]
}

# Grant IAM permissions to access Composer environment
resource "google_composer_environment_iam_member" "composer_env_iam" {
  for_each = var.composer_environment_access_members != null ? var.composer_environment_access_members : {}
  
  project     = var.project_id
  region      = var.region
  environment = google_composer_environment.composer_env.name
  role        = each.value
  member      = each.key
  
  depends_on = [
    google_composer_environment.composer_env
  ]
}

# Outputs
output "composer_environment_id" {
  value       = google_composer_environment.composer_env.id
  description = "The fully qualified resource ID of the Cloud Composer 2 environment"
}

output "composer_environment_name" {
  value       = google_composer_environment.composer_env.name
  description = "The name of the Cloud Composer 2 environment"
}

output "airflow_uri" {
  value       = google_composer_environment.composer_env.config[0].airflow_uri
  description = "The URI to access the Airflow 2.X web interface"
}

output "gke_cluster" {
  value       = google_composer_environment.composer_env.config[0].gke_cluster
  description = "The GKE cluster name that hosts the Composer environment"
}

output "composer_service_account" {
  value       = var.create_composer_service_account ? google_service_account.composer_service_account[0].email : var.composer_service_account_id
  description = "The service account email used by the Composer environment"
}

output "dag_gcs_prefix" {
  value       = google_composer_environment.composer_env.config[0].dag_gcs_prefix
  description = "The GCS bucket path used for storing DAG files"
}

output "cloud_sql_instance_connection_name" {
  value       = google_composer_environment.composer_env.config[0].database_config[0].instance_name
  description = "The connection name of the Cloud SQL instance used by the Composer environment"
}

output "composer_network" {
  value       = var.network_id != "" ? var.network_id : google_composer_environment.composer_env.config[0].node_config[0].network
  description = "The network used for the Cloud Composer 2 environment"
}

output "composer_subnetwork" {
  value       = var.subnet_id != "" ? var.subnet_id : google_composer_environment.composer_env.config[0].node_config[0].subnetwork
  description = "The subnetwork used for the Cloud Composer 2 environment"
}

output "private_environment_config" {
  value       = var.enable_private_endpoint ? {
    enable_private_endpoint = true
    master_ipv4_cidr = var.master_ipv4_cidr
    web_server_ipv4_cidr = var.web_server_ipv4_cidr
    cloud_sql_ipv4_cidr = var.cloud_sql_ipv4_cidr
  } : null
  description = "The private environment configuration details"
}