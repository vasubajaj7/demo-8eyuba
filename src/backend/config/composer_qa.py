"""
QA Environment Configuration for Cloud Composer 2 with Airflow 2.X

This module defines configuration settings specific to the QA environment for
Cloud Composer 2 running Airflow 2.X. It includes environment variables,
resource allocations, network settings, security configurations, and other
parameters required for a reliable QA testing environment.

The configuration is organized into logical sections:
- environment: Basic environment identification
- gcp: Google Cloud Platform settings
- airflow: Apache Airflow configuration
- composer: Cloud Composer 2 specific settings
- storage: GCS bucket configurations
- database: Cloud SQL settings
- network: Network and subnet configurations
- security: Security-related settings
- pools: Airflow worker pool configurations

This configuration supports the Airflow 2.X migration project and is used in the
Peer Review + QA Review stage of the approval workflow.
"""

import os  # For accessing environment variables and path operations
import json  # For JSON parsing and serialization of configuration values

# Environment constants
ENV_NAME = 'qa'
GCP_PROJECT_ID = 'composer2-migration-project-qa'
GCP_REGION = 'us-central1'
GCP_ZONE = 'us-central1-a'
COMPOSER_ENVIRONMENT_NAME = 'composer2-qa'

# Environment configuration
environment_config = {
    'name': ENV_NAME,
    'display_name': 'Quality Assurance',
    'is_production': False,
    'description': 'Quality Assurance environment for testing and validation workflows'
}

# Google Cloud Platform configuration
gcp_config = {
    'project_id': GCP_PROJECT_ID,
    'region': GCP_REGION,
    'zone': GCP_ZONE,
    'labels': {
        'environment': ENV_NAME,
        'app': 'airflow',
        'project': 'composer2-migration'
    }
}

# Airflow configuration
airflow_config = {
    'version': '2.2.5',
    'executor': 'CeleryExecutor',
    'parallelism': 48,
    'dag_concurrency': 24,
    'max_active_runs_per_dag': 24,
    'worker_log_retention_days': 60,
    'load_examples': False,
    'default_timezone': 'UTC',
    'default_ui_timezone': 'UTC',
    'catchup_by_default': False,
    'dagbag_import_timeout': 75,
    'default_pool_slots': 192,
    'default_queue': 'default',
    'email_notifications': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'default_email_recipient': 'airflow-alerts-qa@example.com'
}

# Cloud Composer 2 configuration
composer_config = {
    'environment_name': COMPOSER_ENVIRONMENT_NAME,
    'environment_size': 'medium',
    'node_count': 4,
    'scheduler': {
        'cpu': 4,
        'memory_gb': 15.0,
        'storage_gb': 30,
        'count': 1
    },
    'web_server': {
        'cpu': 2,
        'memory_gb': 7.5,
        'storage_gb': 30
    },
    'worker': {
        'cpu': 4,
        'memory_gb': 15.0,
        'storage_gb': 75,
        'min_count': 3,
        'max_count': 8
    },
    'environment_variables': {
        'AIRFLOW_VAR_ENV': ENV_NAME,
        'AIRFLOW_VAR_GCP_PROJECT': GCP_PROJECT_ID,
        'AIRFLOW_VAR_GCS_BUCKET': f'composer2-migration-{ENV_NAME}-bucket'
    },
    'private_ip': True,
    'cloud_sql_machine_type': 'db-n1-standard-4',
    'web_server_machine_type': 'composer-n1-standard-4',
    'airflow_database_retention_days': 60,
    'auto_scaling_enabled': True,
    'log_level': 'INFO'
}

# Storage configuration
storage_config = {
    'dag_bucket': f'composer2-migration-{ENV_NAME}-dags',
    'data_bucket': f'composer2-migration-{ENV_NAME}-data',
    'logs_bucket': f'composer2-migration-{ENV_NAME}-logs',
    'backup_bucket': f'composer2-migration-{ENV_NAME}-backup',
    'bucket_location': 'US',
    'bucket_class': 'STANDARD',
    'bucket_versioning': True,
    'object_lifecycle_days': 60
}

# Database configuration
database_config = {
    'instance_name': f'composer2-migration-{ENV_NAME}-db',
    'machine_type': 'db-n1-standard-4',
    'database_version': 'POSTGRES_13',
    'disk_size_gb': 75,
    'disk_type': 'PD_SSD',
    'backup_enabled': True,
    'backup_start_time': '02:00',
    'high_availability': True,
    'connection_name': f'{GCP_PROJECT_ID}:{GCP_REGION}:composer2-migration-{ENV_NAME}-db'
}

# Network configuration
network_config = {
    'network_name': f'composer2-migration-{ENV_NAME}-network',
    'subnetwork_name': f'composer2-migration-{ENV_NAME}-subnet',
    'ip_range': '10.1.0.0/24',
    'use_public_ips': False,
    'enable_private_endpoint': True,
    'web_server_ipv4_cidr': '172.17.0.0/28',
    'cloud_sql_ipv4_cidr': '172.17.0.16/28',
    'gke_cluster_ipv4_cidr': '172.17.1.0/24',
    'services_ipv4_cidr': '172.17.2.0/24',
    'master_authorized_networks': [
        '10.0.0.0/24',
        '10.1.0.0/24',
        '10.2.0.0/24'
    ]
}

# Security configuration
security_config = {
    'service_account': f'composer2-migration-{ENV_NAME}-sa@{GCP_PROJECT_ID}.iam.gserviceaccount.com',
    'oauth_scopes': ['https://www.googleapis.com/auth/cloud-platform'],
    'kms_key': f'projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/keyRings/composer-keyring/cryptoKeys/composer-key',
    'use_ip_masq_agent': True,
    'web_server_allow_ip_ranges': ['10.0.0.0/8', '172.16.0.0/12'],
    'enable_private_environment': True,
    'cloud_sql_require_ssl': True,
    'enable_vpc_connectivity': True,
    'enable_iap': True
}

# Airflow pool configuration
pools_config = [
    {
        'name': 'default_pool',
        'slots': 96,
        'description': "Default pool for tasks that don't specify a pool"
    },
    {
        'name': 'etl_pool',
        'slots': 24,
        'description': 'Dedicated pool for ETL workflow tasks'
    },
    {
        'name': 'data_sync_pool',
        'slots': 16,
        'description': 'Pool for data synchronization tasks'
    },
    {
        'name': 'reporting_pool',
        'slots': 12,
        'description': 'Pool for report generation tasks'
    },
    {
        'name': 'high_priority_pool',
        'slots': 6,
        'description': 'Reserved pool for critical tasks'
    },
    {
        'name': 'qa_testing_pool',
        'slots': 16,
        'description': 'Dedicated pool for QA automated test workflows'
    }
]

def get_environment_config():
    """
    Returns the complete configuration dictionary for the QA environment
    
    Returns:
        dict: Complete configuration dictionary for QA environment
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
        'pools': pools_config
    }

# Export the complete configuration
config = get_environment_config()