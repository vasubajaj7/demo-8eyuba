"""
Custom operators package for Apache Airflow 2.X.

This package provides a collection of custom operators that extend the standard
Airflow operators with enhanced functionality for GCS operations, HTTP interactions,
and PostgreSQL database operations. These operators are designed to support the
migration from Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2 environments
while maintaining backward compatibility.

The operators in this package provide additional error handling, retry capabilities,
improved logging, and other enhancements to simplify DAG development and improve
operational reliability.
"""

import logging

# Import custom GCS operators
from .custom_gcp_operator import (
    CustomGCSOperator,
    GCSFileExistsOperator,
    GCSUploadOperator,
    GCSDownloadOperator,
    GCSListFilesOperator,
    GCSDeleteFileOperator,
    DEFAULT_CHUNK_SIZE
)

# Import custom HTTP operators
from .custom_http_operator import (
    CustomHttpOperator,
    CustomHttpSensorOperator,
    DEFAULT_HTTP_CONN_ID
)

# Import custom PostgreSQL operators
from .custom_postgres_operator import (
    CustomPostgresOperator,
    MAX_RETRIES,
    DEFAULT_RETRY_DELAY
)

# Set up logging
logger = logging.getLogger(__name__)

# List of all public objects exported by this package
__all__ = [
    'CustomGCSOperator',
    'GCSFileExistsOperator',
    'GCSUploadOperator',
    'GCSDownloadOperator',
    'GCSListFilesOperator',
    'GCSDeleteFileOperator',
    'DEFAULT_CHUNK_SIZE',
    'CustomHttpOperator',
    'CustomHttpSensorOperator',
    'DEFAULT_HTTP_CONN_ID',
    'CustomPostgresOperator',
    'MAX_RETRIES',
    'DEFAULT_RETRY_DELAY'
]