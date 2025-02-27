#!/usr/bin/env python3
"""
Development environment configuration for Cloud Composer 2 with Airflow 2.X.

This file defines development-specific settings and overrides for Airflow, 
including resource allocations, environment variables, and other configuration 
parameters required for the development environment.
"""

import os
import json  # version: standard library

# Global environment variables
ENV_NAME = 'dev'
GCP_PROJECT_ID = 'composer2-migration-project-dev'
GCP_REGION = 'us-central1'
GCP_ZONE = 'us-central1-a'
COMPOSER_ENVIRONMENT_NAME = 'composer2-dev'

# Environment configuration
environment_config = {
    'name': 'dev',
    'display_name': 'Development',
    'is_production': False,
    'description': 'Development environment for testing and development workflows'
}

# GCP configuration
gcp_config = {
    'project_id': GCP_PROJECT_ID,
    'region': GCP_REGION,
    'zone': GCP_ZONE,
    'labels': {
        'environment': 'dev',
        'app': 'airflow',
        'project': 'composer2-migration'
    }
}

# Airflow configuration
airflow_config = {
    'version': '2.2.5',
    'executor': 'CeleryExecutor',
    'parallelism': 32,
    'dag_concurrency': 16,
    'max_active_runs_per_dag': 16,
    'worker_log_retention_days': 30,
    'load_examples': False,
    'default_timezone': 'UTC',
    'default_ui_timezone': 'UTC',
    'catchup_by_default': False,
    'dagbag_import_timeout': 60,
    'default_pool_slots': 128,
    'default_queue': 'default',
    'email_notifications': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'default_email_recipient': 'airflow-alerts-dev@example.com'
}

# Cloud Composer configuration
composer_config = {
    'environment_name': COMPOSER_ENVIRONMENT_NAME,
    'environment_size': 'small',
    'node_count': 3,
    'scheduler': {
        'cpu': 4,
        'memory_gb': 15.0,
        'storage_gb': 20,
        'count': 1
    },
    'web_server': {
        'cpu': 2,
        'memory_gb': 7.5,
        'storage_gb': 20
    },
    'worker': {
        'cpu': 4,
        'memory_gb': 15.0,
        'storage_gb': 50,
        'min_count': 2,
        'max_count': 6
    },
    'environment_variables': {
        'AIRFLOW_VAR_ENV': 'dev',
        'AIRFLOW_VAR_GCP_PROJECT': GCP_PROJECT_ID,
        'AIRFLOW_VAR_GCS_BUCKET': 'composer2-migration-dev-bucket'
    },
    'private_ip': True,
    'cloud_sql_machine_type': 'db-n1-standard-2',
    'web_server_machine_type': 'composer-n1-standard-2',
    'airflow_database_retention_days': 30,
    'auto_scaling_enabled': True,
    'log_level': 'INFO'
}

# Storage configuration
storage_config = {
    'dag_bucket': 'composer2-migration-dev-dags',
    'data_bucket': 'composer2-migration-dev-data',
    'logs_bucket': 'composer2-migration-dev-logs',
    'backup_bucket': 'composer2-migration-dev-backup',
    'bucket_location': 'US',
    'bucket_class': 'STANDARD',
    'bucket_versioning': True,
    'object_lifecycle_days': 30
}

# Database configuration
database_config = {
    'instance_name': 'composer2-migration-dev-db',
    'machine_type': 'db-n1-standard-2',
    'database_version': 'POSTGRES_13',
    'disk_size_gb': 50,
    'disk_type': 'PD_SSD',
    'backup_enabled': True,
    'backup_start_time': '03:00',
    'high_availability': False,
    'connection_name': f'{GCP_PROJECT_ID}:{GCP_REGION}:composer2-migration-dev-db'
}

# Network configuration
network_config = {
    'network_name': 'composer2-migration-dev-network',
    'subnetwork_name': 'composer2-migration-dev-subnet',
    'ip_range': '10.0.0.0/24',
    'use_public_ips': False,
    'enable_private_endpoint': False,
    'web_server_ipv4_cidr': '172.16.0.0/28',
    'cloud_sql_ipv4_cidr': '172.16.0.16/28',
    'gke_cluster_ipv4_cidr': '172.16.1.0/24',
    'services_ipv4_cidr': '172.16.2.0/24',
    'master_authorized_networks': ['0.0.0.0/0']
}

# Security configuration
security_config = {
    'service_account': f'composer2-migration-dev-sa@{GCP_PROJECT_ID}.iam.gserviceaccount.com',
    'oauth_scopes': ['https://www.googleapis.com/auth/cloud-platform'],
    'kms_key': '',
    'use_ip_masq_agent': True,
    'web_server_allow_ip_ranges': ['0.0.0.0/0'],
    'enable_private_environment': True,
    'cloud_sql_require_ssl': True,
    'enable_vpc_connectivity': True,
    'enable_iap': True
}

# Airflow pools configuration
pools_config = [
    {
        'name': 'default_pool',
        'slots': 64,
        'description': "Default pool for tasks that don't specify a pool"
    },
    {
        'name': 'etl_pool',
        'slots': 16,
        'description': 'Dedicated pool for ETL workflow tasks'
    },
    {
        'name': 'data_sync_pool',
        'slots': 12,
        'description': 'Pool for data synchronization tasks'
    },
    {
        'name': 'reporting_pool',
        'slots': 8,
        'description': 'Pool for report generation tasks'
    },
    {
        'name': 'high_priority_pool',
        'slots': 4,
        'description': 'Reserved pool for critical tasks'
    }
]

def get_environment_config():
    """
    Returns the complete configuration dictionary for the development environment.
    
    Returns:
        dict: Complete configuration dictionary for development environment
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