#!/usr/bin/env python3
"""
Package initialization module for the Airflow variable test suite,
providing shared functionality for testing variable handling during
migration from Airflow 1.10.15 to Airflow 2.X. Exposes test classes
and utility functions for environment variables, Airflow variables,
and variables.json configuration testing.
"""

# Standard library imports
import os  # v3.0+ - Access operating system functionality for file path operations
import sys  # v3.0+ - System-specific parameters and functions
import logging  # v3.2+ - Configure logging for the test package

# Internal imports
from .test_environment_variables import TestEnvironmentVariables  # src/test/variable_tests/test_environment_variables.py
from .test_environment_variables import TestEnvironmentSpecificVars  # src/test/variable_tests/test_environment_variables.py
from .test_environment_variables import TestAirflowVersionEnvVars  # src/test/variable_tests/test_environment_variables.py
from .test_environment_variables import TestComposerEnvironmentVars  # src/test/variable_tests/test_environment_variables.py
from .test_environment_variables import TestEnvironmentVariableOverride  # src/test/variable_tests/test_environment_variables.py
from .test_airflow_variables import TestAirflowVariablesBase  # src/test/variable_tests/test_airflow_variables.py
from .test_airflow_variables import TestVariableFileLoading  # src/test/variable_tests/test_airflow_variables.py
from .test_airflow_variables import TestVariableImport  # src/test/variable_tests/test_airflow_variables.py
from .test_airflow_variables import TestAirflowVersionCompatibility  # src/test/variable_tests/test_airflow_variables.py
from .test_airflow_variables import TestVariablesInDAGs  # src/test/variable_tests/test_airflow_variables.py
from .test_airflow_variables import create_test_variable_file  # src/test/variable_tests/test_airflow_variables.py
from .test_airflow_variables import load_test_variables  # src/test/variable_tests/test_airflow_variables.py

# Initialize logger
logger = logging.getLogger('airflow.test.variable_tests')

# Define global test variables
TEST_VARS_DIR = os.path.dirname(os.path.abspath(__file__))
__version__ = "1.0.0"


def get_test_variables_path() -> str:
    """
    Returns the path to the test variables configuration

    Returns:
        str: Path to the test variables configuration file
    """
    # Construct the path to the variables.json file in the backend config directory
    test_variables_path = os.path.join(TEST_VARS_DIR, 'variables.json')
    # Return the constructed path
    return test_variables_path


# Export test classes and utility functions for test discovery
__all__ = [
    'TestEnvironmentVariables',
    'TestEnvironmentSpecificVars',
    'TestAirflowVersionEnvVars',
    'TestComposerEnvironmentVars',
    'TestEnvironmentVariableOverride',
    'TestAirflowVariablesBase',
    'TestVariableFileLoading',
    'TestVariableImport',
    'TestAirflowVersionCompatibility',
    'TestVariablesInDAGs',
    'create_test_variable_file',
    'load_test_variables',
    'get_test_variables_path',
    '__version__'
]