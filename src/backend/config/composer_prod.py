"""
Production environment configuration for Cloud Composer 2 with Airflow 2.X.

This file defines production-specific settings and overrides for Airflow, including
robust resource allocations, high-availability configurations, strict security controls,
and other configuration parameters required for a reliable, secure production environment.
"""

import os  # v3.8+ - For accessing environment variables and path operations
import json  # For JSON parsing and serialization of configuration values

# Environment identifiers
ENV_NAME = 'prod'
GCP_PROJECT_ID = 'composer2-migration-project-prod'
GCP_REGION = 'us-central1'
GCP_ZONE = 'us-central1-a'
COMPOSER_ENVIRONMENT_NAME = 'composer2-prod'

# Environment configuration
environment_config = {
    'name': 'prod',
    'display_name': 'Production',
    'is_production': True,
    'description': 'Production environment for running critical business workflows'
}

# GCP configuration
gcp_config = {
    'project_id': GCP_PROJECT_ID,
    'region': GCP_REGION,
    'zone': GCP_ZONE,
    'labels': {
        'environment': 'prod',
        'app': 'airflow',
        'project': 'composer2-migration',
        'criticality': 'high'
    }
}

# Airflow configuration
airflow_config = {
    'version': '2.2.5',
    'executor': 'CeleryExecutor',
    'parallelism': 96,
    'dag_concurrency': 48,
    'max_active_runs_per_dag': 32,
    'worker_log_retention_days': 90,
    'load_examples': False,
    'default_timezone': 'UTC',
    'default_ui_timezone': 'UTC',
    'catchup_by_default': False,
    'dagbag_import_timeout': 90,
    'default_pool_slots': 256,
    'default_queue': 'default',
    'email_notifications': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'default_email_recipient': 'airflow-alerts-prod@example.com'
}

# Cloud Composer configuration
composer_config = {
    'environment_name': COMPOSER_ENVIRONMENT_NAME,
    'environment_size': 'large',
    'node_count': 6,
    'scheduler': {
        'cpu': 8,
        'memory_gb': 30.0,
        'storage_gb': 50,
        'count': 2
    },
    'web_server': {
        'cpu': 4,
        'memory_gb': 15.0,
        'storage_gb': 50
    },
    'worker': {
        'cpu': 8,
        'memory_gb': 30.0,
        'storage_gb': 100,
        'min_count': 5,
        'max_count': 15
    },
    'environment_variables': {
        'AIRFLOW_VAR_ENV': 'prod',
        'AIRFLOW_VAR_GCP_PROJECT': GCP_PROJECT_ID,
        'AIRFLOW_VAR_GCS_BUCKET': 'composer2-migration-prod-bucket'
    },
    'private_ip': True,
    'cloud_sql_machine_type': 'db-n1-standard-8',
    'web_server_machine_type': 'composer-n1-standard-8',
    'airflow_database_retention_days': 90,
    'auto_scaling_enabled': True,
    'log_level': 'INFO',
    'enable_high_availability': True,
    'enable_resilience_mode': True,
    'dag_processor_count': 4,
    'maintenance_window_start_time': '2023-01-01T02:00:00Z',
    'maintenance_window_end_time': '2023-01-01T06:00:00Z',
    'maintenance_recurrence': 'FREQ=WEEKLY;BYDAY=SU',
    'enable_cloud_data_lineage_integration': True,
    'enable_web_server_https': True,
    'enable_load_balancer': True,
    'enable_binary_authorization': True
}

# Storage configuration
storage_config = {
    'dag_bucket': 'composer2-migration-prod-dags',
    'data_bucket': 'composer2-migration-prod-data',
    'logs_bucket': 'composer2-migration-prod-logs',
    'backup_bucket': 'composer2-migration-prod-backup',
    'bucket_location': 'US',
    'bucket_class': 'STANDARD',
    'bucket_versioning': True,
    'object_lifecycle_days': 90
}

# Database configuration
database_config = {
    'instance_name': 'composer2-migration-prod-db',
    'machine_type': 'db-n1-standard-8',
    'database_version': 'POSTGRES_13',
    'disk_size_gb': 100,
    'disk_type': 'PD_SSD',
    'backup_enabled': True,
    'backup_start_time': '01:00',
    'high_availability': True,
    'connection_name': f'{GCP_PROJECT_ID}:{GCP_REGION}:composer2-migration-prod-db',
    'backup_retention_days': 30,
    'point_in_time_recovery_enabled': True
}

# Network configuration
network_config = {
    'network_name': 'composer2-migration-prod-network',
    'subnetwork_name': 'composer2-migration-prod-subnet',
    'ip_range': '10.2.0.0/24',
    'use_public_ips': False,
    'enable_private_endpoint': True,
    'web_server_ipv4_cidr': '172.18.0.0/28',
    'cloud_sql_ipv4_cidr': '172.18.0.16/28',
    'gke_cluster_ipv4_cidr': '172.18.1.0/24',
    'services_ipv4_cidr': '172.18.2.0/24',
    'master_authorized_networks': [
        '10.0.0.0/24',
        '10.1.0.0/24',
        '10.2.0.0/24',
        '192.168.0.0/24'
    ],
    'enable_vpc_service_controls': True
}

# Security configuration
security_config = {
    'service_account': f'composer2-migration-prod-sa@{GCP_PROJECT_ID}.iam.gserviceaccount.com',
    'oauth_scopes': [
        'https://www.googleapis.com/auth/cloud-platform'
    ],
    'kms_key': f'projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/keyRings/composer-keyring/cryptoKeys/composer-key',
    'use_ip_masq_agent': True,
    'web_server_allow_ip_ranges': [
        '10.0.0.0/8',
        '172.16.0.0/12',
        '192.168.0.0/16'
    ],
    'enable_private_environment': True,
    'cloud_sql_require_ssl': True,
    'enable_vpc_connectivity': True,
    'enable_iap': True,
    'enable_cmek': True,
    'secret_manager_prefix': 'composer2-prod',
    'secret_rotation_period': '8760h',
    'audit_log_retention_days': 365
}

# Airflow pool configuration
pools = [
    {
        'name': 'default_pool',
        'slots': 128,
        'description': "Default pool for tasks that don't specify a pool"
    },
    {
        'name': 'etl_pool',
        'slots': 32,
        'description': 'Dedicated pool for ETL workflow tasks'
    },
    {
        'name': 'data_sync_pool',
        'slots': 24,
        'description': 'Pool for data synchronization tasks'
    },
    {
        'name': 'reporting_pool',
        'slots': 16,
        'description': 'Pool for report generation tasks'
    },
    {
        'name': 'high_priority_pool',
        'slots': 8,
        'description': 'Reserved pool for critical tasks'
    },
    {
        'name': 'production_only_pool',
        'slots': 16,
        'description': 'Pool for production-specific workflow tasks'
    }
]

def get_environment_config():
    """
    Returns the complete configuration dictionary for the production environment.
    
    Returns:
        dict: Complete configuration dictionary for production environment
    """
    return {
        'environment': environment_config,
        'gcp': gcp_config,
        'airflow': airflow_config,
        'composer': composer_config,
        'storage': storage_config,
        'database': database_config,
        'network': network_config,
        'security': security_config,
        'pools': pools
    }

# Export the complete configuration
config = get_environment_config()