# Outputs for the Security Module used in Cloud Composer 2 deployments
# This file exposes created or configured security resources to be consumed by parent modules

output "service_account_email" {
  description = "Email address of the service account created or used for Cloud Composer 2"
  value       = local.service_account
}

output "service_account_id" {
  description = "The full resource ID of the service account"
  value       = var.use_existing_service_account ? "projects/${var.project_id}/serviceAccounts/${local.service_account}" : google_service_account.composer_sa[0].id
}

output "kms_key_id" {
  description = "The full resource ID of the KMS key used for CMEK encryption"
  value       = local.kms_key_id
}

output "kms_keyring_id" {
  description = "The full resource ID of the KMS keyring"
  value       = local.kms_keyring_id
}

output "secret_ids" {
  description = "Map of secret IDs created in Secret Manager"
  value       = var.create_secrets ? { for k, v in local.secrets_to_create : k => google_secret_manager_secret.composer_secrets[k].id } : {}
}

output "audit_logs_bucket" {
  description = "Name of the storage bucket created for audit logs"
  value       = var.enable_audit_logging ? google_storage_bucket.audit_logs[0].name : ""
}

output "iap_brand" {
  description = "IAP brand name if IAP is enabled"
  value       = var.enable_iap ? google_iap_brand.composer_iap_brand[0].name : ""
}

output "security_config" {
  description = "Combined security configuration object for simplified module referencing"
  value = {
    service_account            = local.service_account
    cmek_enabled               = var.enable_cmek
    kms_key_id                 = local.kms_key_id
    audit_logging_enabled      = var.enable_audit_logging
    audit_logs_bucket          = var.enable_audit_logging ? google_storage_bucket.audit_logs[0].name : ""
    iap_enabled                = var.enable_iap
    private_environment        = var.enable_private_environment
    private_endpoint           = var.enable_private_endpoint
    vpc_sc_enabled             = var.enable_vpc_service_controls
    cloud_sql_ssl_enabled      = var.enable_cloud_sql_ssl
    workload_identity_enabled  = var.enable_workload_identity
    web_server_allow_ip_ranges = var.web_server_allow_ip_ranges
  }
}

output "vpc_sc_enabled" {
  description = "Whether VPC Service Controls are enabled"
  value       = var.enable_vpc_service_controls
}

output "workload_identity_enabled" {
  description = "Whether Workload Identity is enabled for GKE clusters"
  value       = var.enable_workload_identity
}

output "private_environment_enabled" {
  description = "Whether private environment is enabled for Composer"
  value       = var.enable_private_environment
}

output "private_endpoint_enabled" {
  description = "Whether private endpoint is enabled for Composer access"
  value       = var.enable_private_endpoint
}

output "cloud_sql_ssl_enabled" {
  description = "Whether SSL is enabled for Cloud SQL connections"
  value       = var.enable_cloud_sql_ssl
}

output "oauth_scopes" {
  description = "List of OAuth scopes assigned to the service account"
  value       = var.oauth_scopes
}