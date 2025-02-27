"""
Initialization module for sensor tests in the Airflow migration project, exposing test classes and utility functions for testing custom sensors during migration from Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2.
"""

__version__ = "0.1.0"

from .test_custom_gcp_sensor import TestCustomGCSFileSensor  # Import the test class for GCS file sensor
from .test_custom_gcp_sensor import TestCustomBigQueryTableSensor  # Import the test class for BigQuery table sensor
from .test_custom_gcp_sensor import TestCustomBigQueryJobSensor  # Import the test class for BigQuery job sensor
from .test_custom_gcp_sensor import TestCustomGCSObjectsWithPrefixExistenceSensor  # Import the test class for GCS objects with prefix sensor
from .test_custom_http_sensor import TestCustomHttpSensor  # Import the test class for HTTP sensor
from .test_custom_http_sensor import TestCustomHttpJsonSensor  # Import the test class for HTTP JSON sensor
from .test_custom_http_sensor import TestCustomHttpStatusSensor  # Import the test class for HTTP status sensor
from .test_custom_postgres_sensor import TestCustomPostgresSensor  # Import the test class for PostgreSQL sensor
from .test_custom_postgres_sensor import TestCustomPostgresTableExistenceSensor  # Import the test class for PostgreSQL table existence sensor
from .test_custom_postgres_sensor import TestCustomPostgresRowCountSensor  # Import the test class for PostgreSQL row count sensor
from .test_custom_postgres_sensor import TestCustomPostgresValueCheckSensor  # Import the test class for PostgreSQL value check sensor
from ..utils.airflow2_compatibility_utils import is_airflow2  # Import the utility function to check Airflow version

__all__ = [
    "TestCustomGCSFileSensor",
    "TestCustomBigQueryTableSensor",
    "TestCustomBigQueryJobSensor",
    "TestCustomGCSObjectsWithPrefixExistenceSensor",
    "TestCustomHttpSensor",
    "TestCustomHttpJsonSensor",
    "TestCustomHttpStatusSensor",
    "TestCustomPostgresSensor",
    "TestCustomPostgresTableExistenceSensor",
    "TestCustomPostgresRowCountSensor",
    "TestCustomPostgresValueCheckSensor"
]