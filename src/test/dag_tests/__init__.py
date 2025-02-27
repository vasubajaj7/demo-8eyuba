#!/usr/bin/env python3
"""
Package initialization file for the DAG testing module. This module provides centralized
imports, utilities, and configuration for testing Airflow DAGs during migration from
Airflow 1.10.15 to Airflow 2.X. It exposes test classes and functions for validating
DAG integrity, execution, and compatibility with Cloud Composer 2.
"""

import os  # Python standard library
import logging  # Python standard library

import pytest  # pytest-6.0+

# Internal imports
from test.utils.dag_validation_utils import (  # src/test/utils/dag_validation_utils.py
    validate_dag_integrity,
    validate_dag_loading,
    validate_airflow2_compatibility,
    DAGValidator,
)
from test.fixtures.dag_fixtures import (  # src/test/fixtures/dag_fixtures.py
    create_test_dag,
    create_simple_dag,
    get_example_dags,
    DAGTestContext,
)
from test.utils.test_helpers import setup_test_environment  # src/test/utils/test_helpers.py
from test.utils.airflow2_compatibility_utils import is_airflow2  # src/test/utils/airflow2_compatibility_utils.py

# Configure logging for the test package
logger = logging.getLogger('airflow.test.dag_tests')

# Define the directory for DAG tests
DAG_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))


def setup_dag_tests():
    """
    Initialize and configure the DAG tests module and environment
    """
    # Set up logging configuration for DAG tests
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Initialize environment variables specific to DAG testing
    os.environ['AIRFLOW_DAGS'] = DAG_TESTS_DIR

    # Register DAG test paths with pytest for discovery
    pytest.register_assert_rewrite("src.test.dag_tests")


# Expose TestDAGIntegrity class from test_dag_integrity module
class TestDAGIntegrity:
    """Expose TestDAGIntegrity class from test_dag_integrity module"""
    pass

# Expose validate_dag_integrity function for direct import
"""Expose validate_dag_integrity function for direct import"""
validate_dag_integrity = validate_dag_integrity
# Expose validate_dag_loading function for direct import
"""Expose validate_dag_loading function for direct import"""
validate_dag_loading = validate_dag_loading
# Expose validate_airflow2_compatibility function for direct import
"""Expose validate_airflow2_compatibility function for direct import"""
validate_airflow2_compatibility = validate_airflow2_compatibility
# Expose DAGValidator class for direct import
"""Expose DAGValidator class for direct import"""
DAGValidator = DAGValidator
# Expose create_test_dag function for direct import
"""Expose create_test_dag function for direct import"""
create_test_dag = create_test_dag
# Expose create_simple_dag function for direct import
"""Expose create_simple_dag function for direct import"""
create_simple_dag = create_simple_dag
# Expose DAGTestContext class for direct import
"""Expose DAGTestContext class for direct import"""
DAGTestContext = DAGTestContext
# Provide function to initialize the DAG test environment
"""Provide function to initialize the DAG test environment"""
setup_dag_tests = setup_dag_tests