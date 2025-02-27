# -----------------------------------------------------------------------------
# Cloud Composer 2 Module - Outputs
# -----------------------------------------------------------------------------
# This file defines output variables for the Cloud Composer 2 module that expose
# created resources, configurations, and connection details to parent modules.
# These outputs enable environment integration and provide necessary information
# for applications consuming the Airflow environment.
#
# Output variables defined here are crucial for:
# - Integration with CI/CD pipelines
# - Connectivity configuration for applications using Airflow
# - Environment monitoring and management
# - Security and compliance auditing
# -----------------------------------------------------------------------------

output "composer_environment_id" {
  description = "The fully qualified resource ID of the Cloud Composer 2 environment"
  value       = google_composer_environment.composer_env.id
}

output "composer_environment_name" {
  description = "The name of the Cloud Composer 2 environment"
  value       = google_composer_environment.composer_env.name
}

output "airflow_uri" {
  description = "The URI to access the Airflow 2.X web interface"
  value       = google_composer_environment.composer_env.config.0.airflow_uri
}

output "dag_gcs_prefix" {
  description = "The GCS bucket path prefix where DAG files should be uploaded"
  value       = google_composer_environment.composer_env.config.0.dag_gcs_prefix
}

output "gke_cluster" {
  description = "The GKE cluster name that hosts the Cloud Composer 2 environment"
  value       = google_composer_environment.composer_env.config.0.gke_cluster
}

output "composer_service_account" {
  description = "The service account email used by the Cloud Composer 2 environment"
  value       = google_composer_environment.composer_env.config.0.node_config.0.service_account
}

output "cloud_sql_instance_connection_name" {
  description = "The connection name of the Cloud SQL instance used by the Composer environment"
  value       = google_composer_environment.composer_env.config.0.sql_database_instance_connection_name
}

output "composer_network" {
  description = "The network ID used for the Cloud Composer 2 environment"
  value       = var.network_id
}

output "composer_subnetwork" {
  description = "The subnetwork ID used for the Cloud Composer 2 environment"
  value       = var.subnet_id
}

output "private_environment_config" {
  description = "Private network configuration details"
  value = {
    enable_private_endpoint           = var.enable_private_endpoint
    master_ipv4_cidr_block            = var.master_ipv4_cidr
    web_server_ipv4_cidr_block        = var.web_server_ipv4_cidr
    cloud_sql_ipv4_cidr_block         = var.cloud_sql_ipv4_cidr
    enable_privately_used_public_ips  = true
  }
}

output "environment_size" {
  description = "The environment size configuration (small, medium, large) or custom"
  value       = var.environment_size_custom ? "custom" : var.environment_size
}

output "composer_version" {
  description = "The Cloud Composer 2 version used in the environment"
  value       = var.composer_version
}

output "airflow_version" {
  description = "The Airflow 2.X version used in the environment"
  value       = var.airflow_version
}

output "environment_labels" {
  description = "Labels applied to the Cloud Composer 2 environment"
  value       = var.environment_labels
}

output "is_private_environment" {
  description = "Whether the environment is configured as a private Cloud Composer 2 environment"
  value       = var.enable_private_environment
}

output "is_resilient_mode_enabled" {
  description = "Whether resilience mode is enabled for the Cloud Composer 2 environment"
  value       = var.enable_resilience_mode
}