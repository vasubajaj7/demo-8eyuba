"""
Custom sensors package for Apache Airflow 2.X.

This module exports all custom sensor classes that have been migrated from
Airflow 1.10.15 to be compatible with Airflow 2.X. These sensors provide
enhanced monitoring capabilities for GCP services, HTTP endpoints, and
PostgreSQL databases.

This initialization file makes the custom sensors available for import elsewhere
in the application and registers them with the Airflow plugin system as part of
the migration to Cloud Composer 2.
"""

import logging

# Import GCP sensors
from .custom_gcp_sensor import (
    CustomGCSFileSensor,
    CustomBigQueryTableSensor,
    CustomBigQueryJobSensor,
    CustomGCSObjectsWithPrefixExistenceSensor,
    DEFAULT_GCP_CONN_ID,
)

# Import HTTP sensors
from .custom_http_sensor import (
    CustomHttpSensor,
    CustomHttpJsonSensor,
    CustomHttpStatusSensor,
    DEFAULT_HTTP_CONN_ID,
)

# Import PostgreSQL sensors
from .custom_postgres_sensor import (
    CustomPostgresSensor,
    CustomPostgresTableExistenceSensor,
    CustomPostgresRowCountSensor,
    CustomPostgresValueCheckSensor,
    DEFAULT_POSTGRES_CONN_ID,
    DEFAULT_SCHEMA,
)

# Setup logging
logger = logging.getLogger(__name__)

# Package version
__version__ = '1.0.0'

# Define all exported components
__all__ = [
    # GCP Sensors
    'CustomGCSFileSensor',
    'CustomBigQueryTableSensor',
    'CustomBigQueryJobSensor',
    'CustomGCSObjectsWithPrefixExistenceSensor',
    'DEFAULT_GCP_CONN_ID',
    
    # HTTP Sensors
    'CustomHttpSensor',
    'CustomHttpJsonSensor',
    'CustomHttpStatusSensor',
    'DEFAULT_HTTP_CONN_ID',
    
    # PostgreSQL Sensors
    'CustomPostgresSensor',
    'CustomPostgresTableExistenceSensor',
    'CustomPostgresRowCountSensor',
    'CustomPostgresValueCheckSensor',
    'DEFAULT_POSTGRES_CONN_ID',
    'DEFAULT_SCHEMA',
]

logger.info("Custom sensors package for Airflow 2.X initialized")