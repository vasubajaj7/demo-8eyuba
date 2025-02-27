# Cloud Composer 2 QA Environment Outputs
# This file defines outputs from the QA environment deployment that are required for
# CI/CD pipelines, testing frameworks, and operational tools.

# Composer Environment Outputs
output "composer_environment_id" {
  value       = module.composer.composer_environment_id
  description = "The fully qualified resource ID of the Cloud Composer 2 QA environment"
}

output "composer_environment_name" {
  value       = module.composer.composer_environment_name
  description = "The name of the Cloud Composer 2 QA environment"
}

output "airflow_uri" {
  value       = module.composer.airflow_uri
  description = "The URI to access the Airflow 2.X web interface in the QA environment"
}

output "dag_gcs_prefix" {
  value       = module.composer.dag_gcs_prefix
  description = "The GCS bucket path where DAG files should be stored for the QA environment"
}

output "gke_cluster" {
  value       = module.composer.gke_cluster
  description = "The GKE cluster name that hosts the Cloud Composer 2 QA environment"
}

output "composer_service_account" {
  value       = module.composer.composer_service_account
  description = "The service account email used by the Cloud Composer 2 QA environment"
}

# Networking Outputs
output "network_id" {
  value       = module.networking.network_id
  description = "The ID of the VPC network used for the QA environment"
}

output "subnet_id" {
  value       = module.networking.subnet_id
  description = "The ID of the subnet used for the QA environment"
}

output "network_self_link" {
  value       = module.networking.network_self_link
  description = "The self-link URL of the VPC network used for the QA environment"
}

output "subnet_self_link" {
  value       = module.networking.subnet_self_link
  description = "The self-link URL of the subnet used for the QA environment"
}

# Database Outputs
output "cloud_sql_instance_connection_name" {
  value       = module.composer.cloud_sql_instance_connection_name
  description = "The connection name of the Cloud SQL instance used by the QA environment"
}

# Configuration Outputs
output "environment_size" {
  value       = "medium"
  description = "The environment size (medium) configured for the QA environment"
}

output "airflow_config_overrides" {
  value       = {
    "webserver-dag-orientation" = "TB"
    "webserver-dag-run-limit"   = "25"
    "core-dags-are-paused-at-creation" = "True"
    "core-parallelism"          = "50"
    "scheduler-parsing-processes" = "5"
  }
  description = "The Airflow configuration overrides applied to the QA environment"
}

output "security_config" {
  value       = module.security.security_config
  description = "Security configuration details for the QA environment"
}

output "environment_labels" {
  value       = {
    "environment" = "qa"
    "managed-by"  = "terraform"
    "application" = "airflow"
    "project"     = "composer2-migration"
  }
  description = "The labels applied to the QA environment resources"
}

output "private_environment_config" {
  value       = module.composer.private_environment_config
  description = "The private environment configuration details for the QA environment"
}

output "kms_key_id" {
  value       = module.security.kms_key_id
  description = "The KMS key ID used for CMEK encryption in the QA environment"
}

output "resilience_mode_enabled" {
  value       = true
  description = "Whether resilience mode is enabled for the QA environment"
}