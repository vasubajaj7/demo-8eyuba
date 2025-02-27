#!/usr/bin/env python3
"""
Package initialization file for the 'composer_tests' module, which contains test suites
for validating Cloud Composer 2 functionality and the migration process from
Cloud Composer 1 (Airflow 1.10.15) to Cloud Composer 2 (Airflow 2.X).

This module organizes and exposes the test classes and utilities specific to
Cloud Composer environments.
"""

import os
import sys
import logging

# Import testing utilities from airflow2_compatibility_utils
from ..utils.airflow2_compatibility_utils import (
    Airflow2CompatibilityTestMixin,
    CompatibleDAGTestCase,
    is_airflow2,
    is_taskflow_available
)

# Initialize logger
logger = logging.getLogger('airflow.test.composer')

# Define package globals
COMPOSER_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
COMPOSER2_TESTING = bool(os.environ.get('COMPOSER2_TESTING', True))
__version__ = '1.0.0'

def setup_composer_tests():
    """
    Initialize composer testing module and configure environment.
    
    Sets up logging configuration for composer tests and initializes
    environment variables specific to Composer testing.
    
    Returns:
        None
    """
    # Configure logging for composer tests
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format
    )
    
    # Ensure the composer_tests directory is in the Python path
    if COMPOSER_TESTS_DIR not in sys.path:
        sys.path.append(COMPOSER_TESTS_DIR)
    
    # Set environment variables for testing if needed
    if 'COMPOSER2_TESTING' not in os.environ:
        os.environ['COMPOSER2_TESTING'] = 'True'
    
    logger.info("Composer tests module initialized")

def get_composer_tests_version():
    """
    Returns the current version of the composer tests package.
    
    Returns:
        str: Version string
    """
    return __version__

# Export classes and functions to make them available when importing the package
__all__ = [
    '__version__',
    'Airflow2CompatibilityTestMixin',
    'CompatibleDAGTestCase',
    'is_airflow2',
    'is_taskflow_available',
    'setup_composer_tests',
    'get_composer_tests_version'
]