"""
Package initialization file for the macro_tests module, which contains test cases for custom Apache Airflow macros.
It enables test discovery and provides version compatibility utilities for testing macros across Airflow 1.10.15 and Airflow 2.X during the Cloud Composer migration.
"""

import logging  # Python standard library
import os  # Python standard library

from src.test.macro_tests.test_custom_macros import TestCustomMacros, TestAirflow2MacroCompatibility  # src/test/macro_tests/test_custom_macros.py
from src.test.utils.airflow2_compatibility_utils import is_airflow2  # src/test/utils/airflow2_compatibility_utils.py

# Configure logging for the macro_tests package
logger = logging.getLogger('airflow.test.macro_tests')

# Define a list of all test classes to be made available when the module is imported
__all__ = ['TestCustomMacros', 'TestAirflow2MacroCompatibility']

# Determine if the tests are running in an Airflow 2.X environment
RUNNING_AIRFLOW2 = is_airflow2()


def setup_module():
    """
    Initialize the macro_tests module upon first import
    """
    # Configure logging for the macro_tests module
    logger.setLevel(logging.INFO)
    # Log the detected Airflow version
    logger.info(f"Detected Airflow 2: {RUNNING_AIRFLOW2}")
    # Prepare module environment for test discovery
    # This can include setting up environment variables, creating directories, etc.
    pass