# -----------------------------------------------------------------------------
# Cloud Composer 2 Production Environment - Terraform Outputs
# -----------------------------------------------------------------------------
# This file defines output variables for the Cloud Composer 2 production 
# environment that expose important resource information, connection details, 
# and configurations to be consumed by other modules or external systems 
# that need to interact with the production Airflow environment.
# -----------------------------------------------------------------------------

# Basic Environment Information
output "composer_environment_id" {
  description = "The fully qualified resource ID of the Cloud Composer 2 production environment"
  value       = module.composer.composer_environment_id
}

output "airflow_uri" {
  description = "The URI to access the Airflow 2.X web interface for the production environment"
  value       = module.composer.airflow_uri
}

output "gke_cluster" {
  description = "The GKE cluster name that hosts the Cloud Composer 2 production environment"
  value       = module.composer.gke_cluster
}

output "dag_gcs_prefix" {
  description = "The GCS bucket path prefix where DAG files should be uploaded for the production environment"
  value       = module.composer.dag_gcs_prefix
}

# Network Information
output "network_id" {
  description = "The ID of the VPC network used for the production environment"
  value       = module.networking.network_id
}

output "subnet_id" {
  description = "The ID of the subnetwork used for the production environment"
  value       = module.networking.subnet_id
}

# Identity and Access Management
output "composer_service_account" {
  description = "The service account email used by the Cloud Composer 2 production environment"
  value       = module.composer.composer_service_account
}

# Labeling and Metadata
output "environment_labels" {
  description = "The labels applied to the production environment resources for resource management and billing"
  value       = local.environment_labels
}

# Security Configuration
output "security_config" {
  description = "Security configuration details for the production environment"
  value       = module.security.security_config
}

# Network Configuration
output "private_environment_config" {
  description = "Private network configuration details including private IP settings and CIDR blocks"
  value = {
    enable_private_endpoint           = module.networking.private_ip_enabled
    master_ipv4_cidr_block            = var.master_ipv4_cidr_block
    web_server_ipv4_cidr_block        = "172.16.0.16/28"
    cloud_sql_ipv4_cidr_block         = "172.16.0.32/28"
    enable_privately_used_public_ips  = true
  }
}

# High Availability Configuration
output "high_availability_config" {
  description = "High availability configuration details for the production environment"
  value = {
    scheduler_count = var.scheduler_count
    min_workers     = var.min_workers
    max_workers     = var.max_workers
  }
}

# Encryption Configuration
output "encryption_config" {
  description = "CMEK encryption configuration for the production environment"
  value = {
    kms_key_id    = module.security.kms_key_id
    cmek_enabled  = var.enable_cmek
  }
}

# Resiliency Configuration
output "resilience_mode_enabled" {
  description = "Whether resilience mode is enabled for the production environment"
  value       = var.enable_resilience_mode
}

# Version Information
output "composer_version" {
  description = "The Cloud Composer 2 version used in the production environment"
  value       = var.composer_version
}

output "airflow_version" {
  description = "The Airflow 2.X version used in the production environment"
  value       = var.airflow_version
}

# Sizing Configuration
output "environment_size" {
  description = "The environment size configuration (small, medium, large) used for the production deployment"
  value       = var.environment_size
}

# Consolidated Production Deployment Information
output "prod_deployment_info" {
  description = "Consolidated production deployment information including environment ID, project, and creation timestamp"
  value = {
    environment_id      = module.composer.composer_environment_id
    project_id          = var.project_id
    environment_name    = var.environment_name
    region              = var.region
    deployment_datetime = timestamp()
  }
}