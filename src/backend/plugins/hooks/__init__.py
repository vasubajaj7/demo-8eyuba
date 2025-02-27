"""
Initialization module for custom hooks package in Apache Airflow 2.X.

This module imports and exposes all custom hook classes and their default connection IDs
for use in Airflow 2.X DAGs and operators. It's designed to support the migration
from Cloud Composer 1 with Airflow 1.10.15 to Cloud Composer 2 with Airflow 2.X.

The hooks provide enhanced functionality for working with Google Cloud Platform services,
HTTP APIs, and PostgreSQL databases, with improved error handling, retry logic, and
compatibility features.
"""

import logging

# Import custom hooks and their default connection IDs
from .custom_gcp_hook import CustomGCPHook, DEFAULT_GCP_CONN_ID
from .custom_http_hook import CustomHTTPHook, DEFAULT_HTTP_CONN_ID
from .custom_postgres_hook import CustomPostgresHook, DEFAULT_POSTGRES_CONN_ID

# Set up logging
logger = logging.getLogger(__name__)

# Define exports
__all__ = [
    'CustomGCPHook',
    'CustomHTTPHook', 
    'CustomPostgresHook',
    'DEFAULT_GCP_CONN_ID',
    'DEFAULT_HTTP_CONN_ID',
    'DEFAULT_POSTGRES_CONN_ID'
]