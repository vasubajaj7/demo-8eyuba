#!/usr/bin/env python3
"""
Initialization file for the script_tests package, which contains tests for scripts used in the Apache Airflow 2.X migration process.
It exposes test utilities, fixtures, and constants specific to script testing while also importing common test framework components from parent packages.
"""

import os  # standard library
import pytest  # pytest-6.0+
import logging  # standard library
import unittest.mock  # standard library

# Internal module imports
from ..utils.test_helpers import create_test_execution_context, version_compatible_test, TestAirflowContext  # src/test/utils/test_helpers.py
from ..utils.airflow2_compatibility_utils import is_airflow2  # src/test/utils/airflow2_compatibility_utils.py
from ..utils.assertion_utils import assert_script_execution_success  # src/test/utils/assertion_utils.py

# Define global variables
SCRIPTS_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'backend', 'scripts')
logger = logging.getLogger('airflow.test.script_tests')


def create_script_test_context(airflow_version: str = None, custom_context: dict = None) -> TestAirflowContext:
    """
    Creates a test execution context for script testing with appropriate environment variables and paths

    Args:
        airflow_version (str): Airflow version to simulate (optional)
        custom_context (dict): Custom context to add to the test execution (optional)

    Returns:
        TestAirflowContext: Context manager for script testing environment
    """
    # Initialize airflow_version to default value if not provided
    if airflow_version is None:
        airflow_version = "2.0.0" if is_airflow2() else "1.10.15"

    # Initialize empty custom_context if not provided
    if custom_context is None:
        custom_context = {}

    # Create a TestAirflowContext with specified Airflow version
    context = TestAirflowContext(airflow_version)

    # Enhance context with script-specific environment variables
    context.env['SCRIPTS_DIR'] = SCRIPTS_DIR
    context.env['SCRIPTS_TEST_DIR'] = SCRIPTS_TEST_DIR

    # Add script paths to Python path in the context
    context.add_to_python_path(SCRIPTS_DIR)
    context.add_to_python_path(SCRIPTS_TEST_DIR)

    # Update context with custom context
    context.update(custom_context)

    # Return the configured context manager
    return context


def get_script_path(script_name: str) -> str:
    """
    Gets the absolute path to a script file in the scripts directory

    Args:
        script_name (str): Name of the script file

    Returns:
        str: Absolute path to the specified script
    """
    # Validate script_name input is not empty
    if not script_name:
        raise ValueError("script_name cannot be empty")

    # Join SCRIPTS_DIR with the script name to form full path
    full_path = os.path.join(SCRIPTS_DIR, script_name)

    # Verify file exists, raise error if not
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Script file not found: {full_path}")

    # Return absolute path to the script
    return os.path.abspath(full_path)


__all__ = [
    'create_script_test_context',
    'get_script_path',
    'SCRIPTS_TEST_DIR',
    'SCRIPTS_DIR',
    'version_compatible_test',
    'TestAirflowContext'
]