"""
Package initialization file for Google Cloud Platform integration tests. Provides access to test
classes for GCP services including Cloud Storage, Cloud SQL, Secret Manager, Container Registry,
Cloud Build, and Cloud Monitoring, which are essential for validating Airflow migration from
version 1.10.15 to 2.X in Cloud Composer.
"""

import pytest  # pytest-6.0+

# Import test classes for Cloud Storage functionality testing
from src.test.gcp_tests.test_cloud_storage import (
    TestGCSUtilities,
    TestCustomGCPHook,
    TestGCSHookCompatibility,
    TestGCSEncryption,
)  # src/test/gcp_tests/test_cloud_storage.py

# Import test classes for Cloud SQL functionality testing
from src.test.gcp_tests.test_cloud_sql import (
    TestCloudSQLConnection,
    TestCloudSQLQueries,
    TestCloudSQLOperators,
    setup_mock_cloudsql_environment,
)  # src/test/gcp_tests/test_cloud_sql.py

# Import test classes for Secret Manager functionality testing
from src.test.gcp_tests.test_secret_manager import (
    TestSecretManagerIntegration,
    TestSecretManagerWithAirflow2,
    TestSecretManagerPerformance,
    setup_mock_secret_manager,
)  # src/test/gcp_tests/test_secret_manager.py

# Import compatibility check for Airflow version detection
from src.test.utils.airflow2_compatibility_utils import (
    is_airflow2,
)  # src/test/utils/airflow2_compatibility_utils.py

# Define global test variables
GCP_TEST_MODULES = [
    "test_cloud_storage",
    "test_cloud_sql",
    "test_secret_manager",
    "test_container_registry",
    "test_cloud_build",
    "test_cloud_monitoring",
]
USING_AIRFLOW2 = is_airflow2()


def set_up_test_markers():
    """Sets up pytest markers for GCP test modules"""
    # Register 'gcp' marker with pytest for all GCP tests
    pytest.mark.gcp

    # Register specific markers for each GCP service (cloud_storage, cloud_sql, etc.)
    pytest.mark.cloud_storage
    pytest.mark.cloud_sql
    pytest.mark.secret_manager

    # Register 'airflow2' marker for tests specific to Airflow 2.X
    pytest.mark.airflow2

    # Register 'migration' marker for migration verification tests
    pytest.mark.migration


# Export test classes for GCS utility functions
__all__ = [
    "TestGCSUtilities",
    "TestCustomGCPHook",
    "TestGCSHookCompatibility",
    "TestGCSEncryption",
    "TestCloudSQLConnection",
    "TestCloudSQLQueries",
    "TestCloudSQLOperators",
    "TestSecretManagerIntegration",
    "TestSecretManagerWithAirflow2",
    "TestSecretManagerPerformance",
    "setup_mock_cloudsql_environment",
    "setup_mock_secret_manager",
    "GCP_TEST_MODULES",
    "USING_AIRFLOW2",
]