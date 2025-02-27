#!/usr/bin/env python3
"""
Initialization file for the test package that marks the directory as a Python package,
defines package metadata, and provides centralized access to critical testing utilities
for Airflow migration testing.
"""

import os  # standard library
import sys  # standard library

# Internal imports
from .utils import airflow2_compatibility_utils  # src/test/utils/airflow2_compatibility_utils.py
from .utils import assertion_utils  # src/test/utils/assertion_utils.py
from .utils import dag_validation_utils  # src/test/utils/dag_validation_utils.py
from .utils import test_helpers  # src/test/utils/test_helpers.py

__version__ = "1.0.0"
__author__ = "Migration Team"
__airflow_version__ = airflow2_compatibility_utils.AIRFLOW_VERSION
TEST_ENVIRONMENT = os.environ.get('TEST_ENVIRONMENT', 'local')


def get_test_environment() -> str:
    """
    Returns the current test environment context

    Returns:
        str: The current test environment (local, dev, qa, prod)
    """
    return TEST_ENVIRONMENT


def is_airflow2_environment() -> bool:
    """
    Wrapper function to check if current environment is running Airflow 2.X

    Returns:
        bool: True if running in Airflow 2.X environment, False otherwise
    """
    return airflow2_compatibility_utils.is_airflow2()


__all__ = [
    '__version__',
    '__author__',
    '__airflow_version__',
    'get_test_environment',
    'is_airflow2_environment',
    'airflow2_compatibility_utils',
    'assertion_utils',
    'dag_validation_utils',
    'test_helpers'
]