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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.4.3"
    }
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
  service_account = var.use_existing_service_account ? var.existing_service_account_id : google_service_account.composer_sa[0].email
  kms_project_id = var.kms_project_id != "" ? var.kms_project_id : var.project_id
  kms_keyring_id = var.enable_cmek && var.create_kms_resources ? google_kms_keyring.composer_keyring[0].id : "projects/${local.kms_project_id}/locations/${var.region}/keyRings/${var.kms_keyring_name}"
  kms_key_id = var.enable_cmek ? "${local.kms_keyring_id}/cryptoKeys/${var.kms_key_name}" : ""
  support_email = var.support_email != "" ? var.support_email : "composer-support@${var.project_id}.iam.gserviceaccount.com"
  
  secrets_to_create = {
    "db-password" = "random-db-password"
    "api-key" = "random-api-key"
    "connection-string" = "jdbc:postgresql://localhost:5432/airflow"
  }
}

# Validation checks
resource "null_resource" "validation" {
  lifecycle {
    precondition {
      condition     = !var.use_existing_service_account || var.existing_service_account_id != ""
      error_message = "When use_existing_service_account is true, existing_service_account_id must be provided."
    }
    
    precondition {
      condition     = !var.enable_cmek || var.create_kms_resources || var.kms_project_id != ""
      error_message = "When enable_cmek is true and create_kms_resources is false, kms_project_id must be provided."
    }
  }
}

# Get project information
data "google_project" "project" {
  project_id = var.project_id
}

# Service Account for Cloud Composer 2
resource "google_service_account" "composer_sa" {
  count        = var.use_existing_service_account ? 0 : 1
  account_id   = var.composer_service_account_name
  display_name = var.composer_service_account_display_name
  description  = var.composer_service_account_description
  project      = var.project_id
}

# IAM role assignments for the service account
resource "google_project_iam_member" "composer_sa_roles" {
  for_each = toset(var.service_account_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${local.service_account}"
}

# KMS Keyring for CMEK encryption
resource "google_kms_keyring" "composer_keyring" {
  count    = var.enable_cmek && var.create_kms_resources ? 1 : 0
  name     = var.kms_keyring_name
  location = var.region
  project  = local.kms_project_id
}

# KMS CryptoKey for CMEK encryption
resource "google_kms_crypto_key" "composer_key" {
  count           = var.enable_cmek && var.create_kms_resources ? 1 : 0
  name            = var.kms_key_name
  key_ring        = google_kms_keyring.composer_keyring[0].id
  rotation_period = "7776000s" # 90 days
  
  version_template {
    algorithm        = "GOOGLE_SYMMETRIC_ENCRYPTION"
    protection_level = "SOFTWARE"
  }
  
  purpose = "ENCRYPT_DECRYPT"
}

# Grant the service account access to the KMS key
resource "google_kms_crypto_key_iam_member" "composer_sa_kms_access" {
  count         = var.enable_cmek ? 1 : 0
  crypto_key_id = local.kms_key_id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${local.service_account}"
}

# Grant the Composer service agent access to the KMS key
resource "google_kms_crypto_key_iam_member" "composer_service_agent_kms_access" {
  count         = var.enable_cmek ? 1 : 0
  crypto_key_id = local.kms_key_id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

# Audit logging bucket
resource "google_storage_bucket" "audit_logs" {
  count                       = var.enable_audit_logging ? 1 : 0
  name                        = "${var.project_id}-composer-${var.env_name}-audit-logs"
  location                    = var.region
  project                     = var.project_id
  force_destroy               = true
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = var.audit_log_retention_days
    }
    action {
      type = "Delete"
    }
  }
}

# Grant the service account access to write audit logs
resource "google_storage_bucket_iam_member" "audit_logs_writer" {
  count  = var.enable_audit_logging ? 1 : 0
  bucket = google_storage_bucket.audit_logs[0].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${local.service_account}"
}

# Enable IAP API
resource "google_project_service" "iap_api" {
  count              = var.enable_iap ? 1 : 0
  project            = var.project_id
  service            = "iap.googleapis.com"
  disable_on_destroy = false
}

# Create IAP brand
resource "google_iap_brand" "composer_iap_brand" {
  count             = var.enable_iap ? 1 : 0
  support_email     = local.support_email
  application_title = "Cloud Composer ${var.env_name}"
  project           = var.project_id

  depends_on = [google_project_service.iap_api]
}

# Grant IAP access to specified members
resource "google_iap_web_iam_member" "composer_iap_access" {
  for_each = var.enable_iap && length(var.iap_members) > 0 ? toset(var.iap_members) : toset([])
  project  = var.project_id
  role     = "roles/iap.httpsResourceAccessor"
  member   = each.value
  
  depends_on = [google_iap_brand.composer_iap_brand]
}

# Random ID for uniqueness
resource "random_id" "suffix" {
  byte_length = 4
  prefix      = ""
  
  keepers = {
    project_id = var.project_id
    env_name   = var.env_name
  }
}

# Create secrets in Secret Manager
resource "google_secret_manager_secret" "composer_secrets" {
  for_each  = var.create_secrets ? local.secrets_to_create : {}
  secret_id = "${var.secret_manager_prefix}-${each.key}"
  project   = var.project_id
  
  replication {
    automatic = true
  }
  
  rotation {
    next_rotation_time = timeadd(timestamp(), var.secret_rotation_period)
    rotation_period    = var.secret_rotation_period
  }
}

# Create initial secret versions
resource "google_secret_manager_secret_version" "composer_secret_versions" {
  for_each    = var.create_secrets ? local.secrets_to_create : {}
  secret      = google_secret_manager_secret.composer_secrets[each.key].id
  secret_data = each.value
}

# Grant the service account access to secrets
resource "google_secret_manager_secret_iam_member" "composer_sa_secret_access" {
  for_each  = var.create_secrets ? local.secrets_to_create : {}
  secret_id = google_secret_manager_secret.composer_secrets[each.key].secret_id
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.service_account}"
}

# Outputs
output "service_account_email" {
  description = "Email address of the service account created or used for Cloud Composer 2"
  value       = local.service_account
}

output "service_account_id" {
  description = "Full resource ID of the service account"
  value       = var.use_existing_service_account ? var.existing_service_account_id : google_service_account.composer_sa[0].id
}

output "kms_key_id" {
  description = "Full resource ID of the KMS key used for CMEK encryption"
  value       = local.kms_key_id
}

output "kms_keyring_id" {
  description = "Full resource ID of the KMS keyring"
  value       = local.kms_keyring_id
}

output "secret_ids" {
  description = "Map of secret IDs created in Secret Manager"
  value       = var.create_secrets ? {for k, _ in local.secrets_to_create : k => google_secret_manager_secret.composer_secrets[k].secret_id} : {}
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
    service_account_email = local.service_account
    cmek_enabled          = var.enable_cmek
    cmek_key_id           = local.kms_key_id
    iap_enabled           = var.enable_iap
    audit_logging_enabled = var.enable_audit_logging
    private_environment   = var.enable_private_environment
    private_endpoint      = var.enable_private_endpoint
    vpc_sc_enabled        = var.enable_vpc_service_controls
  }
}

output "vpc_sc_enabled" {
  description = "Whether VPC Service Controls are enabled"
  value       = var.enable_vpc_service_controls
}