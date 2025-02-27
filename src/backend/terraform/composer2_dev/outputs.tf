# Terraform outputs file that defines and exposes essential information about 
# the Cloud Composer 2 development environment deployment for integration with 
# CI/CD pipelines, monitoring tools, and other systems.

# Environment identification
output "composer_environment_id" {
  description = "The full resource ID of the Cloud Composer 2 development environment"
  value       = module.composer.composer_environment_id
}

# Access endpoints
output "airflow_uri" {
  description = "The URI to access the Airflow 2.X web interface in the development environment"
  value       = module.composer.airflow_uri
}

# Storage information
output "dag_gcs_prefix" {
  description = "The GCS bucket path where DAG files should be stored for the development environment"
  value       = module.composer.dag_gcs_prefix
}

# Infrastructure details
output "gke_cluster" {
  description = "The GKE cluster name that hosts the Cloud Composer 2 development environment"
  value       = module.composer.gke_cluster
}

# Service account information
output "composer_service_account" {
  description = "The service account email used by the Cloud Composer 2 development environment"
  value       = module.composer.composer_service_account
}

# Network configuration
output "network_id" {
  description = "The ID of the VPC network used for the development environment"
  value       = module.networking.network_id
}

output "subnet_id" {
  description = "The ID of the subnet used for the development environment"
  value       = module.networking.subnet_id
}

# Database information
output "cloud_sql_instance" {
  description = "The connection name of the Cloud SQL instance used by the development environment"
  value       = module.composer.cloud_sql_instance_connection_name
}

# Environment configuration
output "environment_size" {
  description = "The environment size configured for the development environment"
  value       = var.environment_size
}

# Resource labeling
output "environment_labels" {
  description = "The labels applied to the development environment resources"
  value       = local.environment_labels
}

# Security configuration
output "security_config" {
  description = "Security configuration details for the development environment"
  value       = module.security.security_config
}

# Airflow configuration
output "airflow_config_overrides" {
  description = "The Airflow configuration overrides applied to the development environment"
  value       = local.airflow_config_overrides
}

# Private environment configuration
output "private_environment_config" {
  description = "The private environment configuration details for the development environment"
  value       = {
    enable_private_endpoint    = var.enable_private_endpoint
    enable_private_environment = true
    web_server_ipv4_cidr       = "172.16.0.16/28"
    cloud_sql_ipv4_cidr        = "172.16.0.32/28"
    master_ipv4_cidr           = var.master_ipv4_cidr_block
  }
}