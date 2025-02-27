terraform {
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
  required_version = ">= 1.0.0"
  
  backend "gcs" {
    bucket = "composer2-migration-terraform-state"
    prefix = "terraform/state/composer2/qa"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

locals {
  composer_env_name = var.environment_name
  network_project_id = var.project_id
}

# Get project information
data "google_project" "project" {
  project_id = var.project_id
}

# Create KMS Key for CMEK if enabled
resource "google_kms_key_ring" "composer_key_ring" {
  count    = var.enable_cmek ? 1 : 0
  name     = "${var.environment_name}-keyring"
  location = var.region
  project  = var.project_id
}

resource "google_kms_crypto_key" "composer_key" {
  count    = var.enable_cmek ? 1 : 0
  name     = "${var.environment_name}-key"
  key_ring = google_kms_key_ring.composer_key_ring[0].id
  
  purpose = "ENCRYPT_DECRYPT"
  
  version_template {
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }
  
  rotation_period = "7776000s" # 90 days
}

# Grant service account access to the KMS key
resource "google_kms_crypto_key_iam_member" "crypto_key_iam_member" {
  count         = var.enable_cmek ? 1 : 0
  crypto_key_id = google_kms_crypto_key.composer_key[0].id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

# Create a dedicated service account for Cloud Composer if not provided
resource "google_service_account" "composer_service_account" {
  count        = var.service_account_email == "" ? 1 : 0
  account_id   = "composer2-qa-sa"
  display_name = "Cloud Composer 2 QA Service Account"
  project      = var.project_id
}

# Grant necessary IAM roles to the service account
resource "google_project_iam_member" "composer_worker_role" {
  count   = var.service_account_email == "" ? 1 : 0
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_service_account[0].email}"
}

resource "google_project_iam_member" "storage_object_admin" {
  count   = var.service_account_email == "" ? 1 : 0
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.composer_service_account[0].email}"
}

resource "google_project_iam_member" "monitoring_editor" {
  count   = var.service_account_email == "" ? 1 : 0
  project = var.project_id
  role    = "roles/monitoring.editor"
  member  = "serviceAccount:${google_service_account.composer_service_account[0].email}"
}

# Create the Cloud Composer 2 environment
resource "google_composer_environment" "composer_env" {
  provider = google-beta
  
  name     = local.composer_env_name
  region   = var.region
  project  = var.project_id
  
  labels = {
    environment = "qa"
    managed_by  = "terraform"
    app         = "airflow"
    version     = var.airflow_version
  }
  
  config {
    software_config {
      image_version = "${var.composer_version}-airflow-${var.airflow_version}"
      
      airflow_config_overrides = {
        core-load_examples                        = "False"
        core-dags_are_paused_at_creation          = "True"
        core-default_task_retries                 = "3"
        webserver-dag_orientation                 = "TB"
        webserver-dag_default_view                = "graph"
        scheduler-parsing_processes               = tostring(var.dag_processor_count)
        scheduler-min_file_process_interval       = "60"
        webserver-refresh_interval                = "30"
        webserver-web_server_worker_timeout       = "120"
        logging-logging_level                     = var.log_level
        api-auth_backends                         = "airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth"
        metrics-statsd_on                         = "True"
        metrics-statsd_host                       = "localhost"
        metrics-statsd_port                       = "8125"
        metrics-statsd_prefix                     = "airflow"
        scheduler-scheduler_health_check_threshold = "30"
      }
      
      env_variables = var.environment_variables
      
      pypi_packages = var.pypi_packages
    }
    
    node_config {
      network    = var.network
      subnetwork = var.subnetwork
      
      service_account = var.service_account_email == "" ? google_service_account.composer_service_account[0].email : var.service_account_email
      
      ip_allocation_policy {
        cluster_secondary_range_name  = var.ip_range_pods
        services_secondary_range_name = var.ip_range_services
      }
    }
    
    node_count = var.node_count
    
    environment_size = var.environment_size
    
    private_environment_config {
      enable_private_endpoint = var.enable_private_endpoint
      master_ipv4_cidr_block  = var.master_ipv4_cidr_block
    }
    
    database_config {
      machine_type = var.cloud_sql_machine_type
    }
    
    web_server_config {
      machine_type = var.web_server_cpu <= 2 ? "composer-n1-webserver-2" : "composer-n1-webserver-4"
      
      dynamic "network_access_control" {
        for_each = length(var.web_server_access_control) > 0 ? [1] : []
        content {
          dynamic "allowed_ip_range" {
            for_each = var.web_server_access_control
            content {
              value       = allowed_ip_range.value.ip_range
              description = allowed_ip_range.value.description
            }
          }
        }
      }
    }
    
    maintenance_window {
      start_time = var.maintenance_window_start_time
      end_time   = var.maintenance_window_end_time
      recurrence = var.maintenance_recurrence
    }
    
    recovery_config {
      scheduled_snapshots_config {
        enabled = var.enable_resilience_mode
        time_zone = "UTC"
      }
    }
    
    encryption_config {
      kms_key_name = var.enable_cmek ? google_kms_crypto_key.composer_key[0].id : null
    }
    
    workloads_config {
      scheduler {
        cpu        = var.scheduler_cpu
        memory_gb  = var.scheduler_memory_gb
        storage_gb = var.scheduler_storage_gb
        count      = var.scheduler_count
      }
      
      web_server {
        cpu        = var.web_server_cpu
        memory_gb  = var.web_server_memory_gb
        storage_gb = var.web_server_storage_gb
      }
      
      worker {
        cpu        = var.worker_cpu
        memory_gb  = var.worker_memory_gb
        storage_gb = var.worker_storage_gb
        min_count  = var.min_workers
        max_count  = var.max_workers
      }
    }
  }
  
  depends_on = [
    google_kms_crypto_key_iam_member.crypto_key_iam_member,
    google_project_iam_member.composer_worker_role,
    google_project_iam_member.storage_object_admin,
    google_project_iam_member.monitoring_editor
  ]
  
  timeouts {
    create = "90m"
    update = "90m"
    delete = "60m"
  }
}

# Set up monitoring alerts for the Cloud Composer 2 environment
resource "google_monitoring_alert_policy" "composer_environment_health" {
  display_name = "Cloud Composer 2 QA Environment Health"
  combiner     = "OR"
  project      = var.project_id
  
  conditions {
    display_name = "Airflow Scheduler Heartbeat"
    
    condition_threshold {
      filter = "resource.type = \"cloud_composer_environment\" AND resource.labels.environment_name = \"${local.composer_env_name}\" AND metric.type = \"composer.googleapis.com/environment/healthy\""
      
      duration = "300s"
      comparison = "COMPARISON_LT"
      threshold_value = 1
      
      trigger {
        count = 1
      }
      
      aggregations {
        alignment_period = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  documentation {
    content = "The Cloud Composer 2 QA environment is not healthy. Check the environment status in the GCP Console."
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "composer_dag_processing_time" {
  display_name = "Cloud Composer 2 QA DAG Processing Time"
  combiner     = "OR"
  project      = var.project_id
  
  conditions {
    display_name = "DAG Processing Time"
    
    condition_threshold {
      filter = "resource.type = \"cloud_composer_environment\" AND resource.labels.environment_name = \"${local.composer_env_name}\" AND metric.type = \"composer.googleapis.com/environment/dag_processing/total_parse_time\""
      
      duration = "300s"
      comparison = "COMPARISON_GT"
      threshold_value = 30 # 30 seconds threshold as per technical specification
      
      trigger {
        count = 1
      }
      
      aggregations {
        alignment_period = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  documentation {
    content = "DAG processing time is exceeding the 30-second threshold. Review your DAGs for optimization opportunities."
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "1800s"
  }
}

# Create a Cloud Storage bucket for logs and backups
resource "google_storage_bucket" "composer_logs_bucket" {
  name          = "${var.project_id}-composer-logs"
  location      = var.region
  project       = var.project_id
  storage_class = "STANDARD"
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = var.airflow_database_retention_days
    }
    action {
      type = "Delete"
    }
  }
  
  uniform_bucket_level_access = true
  
  dynamic "encryption" {
    for_each = var.enable_cmek ? [1] : []
    content {
      default_kms_key_name = google_kms_crypto_key.composer_key[0].id
    }
  }
  
  labels = {
    environment = "qa"
    managed_by  = "terraform"
    app         = "airflow"
    version     = var.airflow_version
  }
}

# Outputs for CI/CD pipeline integration
output "composer_environment" {
  description = "Cloud Composer 2 QA environment details"
  value = {
    id             = google_composer_environment.composer_env.id
    name           = google_composer_environment.composer_env.name
    airflow_uri    = google_composer_environment.composer_env.config[0].airflow_uri
    dag_gcs_prefix = google_composer_environment.composer_env.config[0].dag_gcs_prefix
  }
}