#!/usr/bin/env python3
"""
Package initialization file for Airflow version tests that provides access to utilities,
test fixtures, and common imports needed for testing migration from Apache Airflow 1.10.15
to Airflow 2.X. This module exposes key compatibility utilities and testing functions
that are shared across the various test modules in the airflow_version_tests package.
"""

import unittest  # Python standard library
import pytest  # 6.0+
import datetime  # Python standard library
import logging  # Python standard library

# Internal imports
from ..utils.airflow2_compatibility_utils import is_airflow2  # Function to check if running with Airflow 2.X
from ..utils.airflow2_compatibility_utils import is_taskflow_available  # Function to check if TaskFlow API is available
from ..utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin  # Mixin class for test cases that need to run with different Airflow versions
from ..utils.airflow2_compatibility_utils import CompatibleDAGTestCase  # Base test case class for cross-version DAG testing
from ..utils.airflow2_compatibility_utils import convert_to_taskflow  # Function to convert PythonOperator-based code to TaskFlow API
from ..utils.airflow2_compatibility_utils import mock_airflow1_imports  # Context manager for mocking Airflow 1.X imports
from ..utils.airflow2_compatibility_utils import mock_airflow2_imports  # Context manager for mocking Airflow 2.X imports
from ..fixtures.dag_fixtures import DAGTestContext  # Context manager for testing DAGs in a controlled environment
from ..fixtures.dag_fixtures import create_test_dag  # Function to create DAGs for testing
from ..fixtures.dag_fixtures import create_taskflow_test_dag  # Function to create TaskFlow API DAGs for testing

# Global variables
VERSION_TEST_LOGGER = logging.getLogger('airflow.test.version')  # Logger for version compatibility test reporting
AIRFLOW_VERSION_IMPORT_ERROR_MSG = "Cannot import Airflow - version compatibility tests will be limited"  # Message for Airflow import failure


def get_airflow_version() -> str:
    """
    Safely determines the current Airflow version being used for testing

    Returns:
        str: Current Airflow version or a default fallback version
    """
    try:
        # Try to import airflow and get __version__
        import airflow
        version = airflow.__version__
        return version
    except ImportError:
        # If import fails, log warning with AIRFLOW_VERSION_IMPORT_ERROR_MSG
        VERSION_TEST_LOGGER.warning(AIRFLOW_VERSION_IMPORT_ERROR_MSG)
        # Return detected version or default to '1.10.15' on import failure
        return '1.10.15'


__all__ = [
    'is_airflow2',
    'is_taskflow_available',
    'Airflow2CompatibilityTestMixin',
    'CompatibleDAGTestCase',
    'convert_to_taskflow',
    'mock_airflow1_imports',
    'mock_airflow2_imports',
    'DAGTestContext',
    'create_test_dag',
    'create_taskflow_test_dag',
    'get_airflow_version',
    'VERSION_TEST_LOGGER'
]