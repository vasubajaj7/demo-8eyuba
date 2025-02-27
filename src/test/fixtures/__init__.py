"""
Initialization module for the test fixtures package, exposing all essential mock objects,
data generators, and testing utilities for validating the migration from Airflow 1.10.15 to
Airflow 2.X in Cloud Composer 2. This module aggregates and initializes test fixtures from
various submodules to provide a consistent testing framework across different test categories.
"""

import os  # Python standard library for environment variables
import datetime  # Python standard library for date and time objects
from datetime import datetime  # Import datetime class directly
from typing import Any, Dict, List, Optional, Union  # Python standard library for type hints

import pytest  # pytest v6.0.0+ - Fixtures and testing utilities
from pytest import fixture  # Import fixture decorator explicitly

from ..utils.airflow2_compatibility_utils import is_airflow2  # Utility to check Airflow version
from .mock_data import MockDataGenerator  # Class for generating mock data
from .mock_data import MockAirflowModels  # Class for creating mock Airflow models
from .mock_data import DEFAULT_DATE  # Standard test date
from .mock_data import DEFAULT_DATE_AIRFLOW1  # Airflow 1.X compatible test date
from .mock_data import DEFAULT_DATE_AIRFLOW2  # Airflow 2.X compatible test date
from .mock_hooks import MockHookManager  # Context manager for hook mocking
from .mock_hooks import MockCustomGCPHook  # Mock GCP hook class
from .mock_hooks import MockCustomHTTPHook  # Mock HTTP hook class
from .mock_hooks import MockCustomPostgresHook  # Mock Postgres hook class
from .mock_hooks import create_mock_custom_gcp_hook  # Factory function for GCP hooks
from .mock_hooks import create_mock_custom_http_hook  # Factory function for HTTP hooks
from .mock_hooks import create_mock_custom_postgres_hook  # Factory function for Postgres hooks
from .mock_operators import MockOperatorManager  # Context manager for operator mocking
from .mock_operators import create_mock_gcs_operator  # Factory function for GCS operators
from .mock_operators import create_mock_http_operator  # Factory function for HTTP operators
from .mock_operators import create_mock_postgres_operator  # Factory function for Postgres operators
from .dag_fixtures import DAGTestContext  # Context manager for DAG testing
from .dag_fixtures import create_test_dag  # Function to create test DAGs
from .dag_fixtures import create_simple_dag  # Function to create simple DAGs
from .dag_fixtures import create_taskflow_test_dag  # Function to create TaskFlow API DAGs (Airflow 2.X)
from .dag_fixtures import validate_dag_structure  # Function to validate DAG structure

# Define package version
VERSION = "0.1.0"

# Determine fixture mode from environment variable
FIXTURE_MODE = os.environ.get('AIRFLOW_FIXTURE_MODE', 'auto')

# Determine Airflow 2.X testing mode from environment variable
AIRFLOW2_TESTING = os.environ.get('AIRFLOW2_TESTING', 'true').lower() in ('true', '1', 't')


def get_mock_data_generator(airflow2_compatible: Optional[bool] = None) -> MockDataGenerator:
    """
    Factory function to create a MockDataGenerator instance with appropriate Airflow version compatibility

    Args:
        airflow2_compatible (Optional[bool]): If True, creates a MockDataGenerator for Airflow 2.X;
                                             if False, creates one for Airflow 1.X; if None, determines
                                             compatibility automatically using is_airflow2().

    Returns:
        MockDataGenerator: Configured instance of MockDataGenerator
    """
    if airflow2_compatible is None:
        airflow2_compatible = is_airflow2()
    return MockDataGenerator(airflow2_compatible=airflow2_compatible)


def get_version_appropriate_date(airflow2_compatible: Optional[bool] = None) -> datetime:
    """
    Returns the appropriate date object based on Airflow version compatibility

    Args:
        airflow2_compatible (Optional[bool]): If True, returns a datetime object for Airflow 2.X;
                                             if False, returns one for Airflow 1.X; if None, determines
                                             compatibility automatically using is_airflow2().

    Returns:
        datetime: Date object compatible with specified Airflow version
    """
    if airflow2_compatible is None:
        airflow2_compatible = is_airflow2()
    if airflow2_compatible:
        return DEFAULT_DATE_AIRFLOW2
    else:
        return DEFAULT_DATE_AIRFLOW1


def setup_test_environment(airflow2_compatible: Optional[bool] = None) -> Dict[str, Any]:
    """
    Sets up the testing environment with appropriate fixtures and mocks

    Args:
        airflow2_compatible (Optional[bool]): If True, sets up the environment for Airflow 2.X;
                                             if False, sets up for Airflow 1.X; if None, determines
                                             compatibility automatically using is_airflow2().

    Returns:
        Dict[str, Any]: Dictionary containing configured test environment objects and fixtures
    """
    if airflow2_compatible is None:
        airflow2_compatible = is_airflow2()

    # Create mock data generator
    mock_data_generator = get_mock_data_generator(airflow2_compatible)

    # Create mock Airflow models
    mock_airflow_models = MockAirflowModels(airflow2_compatible)

    # Set up environment with correct date constants
    if airflow2_compatible:
        test_date = DEFAULT_DATE_AIRFLOW2
    else:
        test_date = DEFAULT_DATE_AIRFLOW1

    # Return dictionary with all configured environment objects
    return {
        "mock_data_generator": mock_data_generator,
        "mock_airflow_models": mock_airflow_models,
        "test_date": test_date,
    }

# Export all items for easy access
__all__ = [
    "VERSION",
    "DEFAULT_DATE",
    "DEFAULT_DATE_AIRFLOW1",
    "DEFAULT_DATE_AIRFLOW2",
    "MockDataGenerator",
    "MockAirflowModels",
    "MockHookManager",
    "MockOperatorManager",
    "DAGTestContext",
    "MockCustomGCPHook",
    "MockCustomHTTPHook",
    "MockCustomPostgresHook",
    "create_mock_custom_gcp_hook",
    "create_mock_custom_http_hook",
    "create_mock_custom_postgres_hook",
    "create_mock_gcs_operator",
    "create_mock_http_operator",
    "create_mock_postgres_operator",
    "create_test_dag",
    "create_simple_dag",
    "create_taskflow_test_dag",
    "validate_dag_structure",
    "get_mock_data_generator",
    "get_version_appropriate_date",
    "setup_test_environment"
]