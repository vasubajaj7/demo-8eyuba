"""
Package initialization file for the hook_tests module that contains test suites for Airflow hook
implementations during migration from Airflow 1.10.15 to Airflow 2.X. This module makes test
cases discoverable by the testing framework and provides shared utilities and fixtures specific
to hook testing.
"""
# Python standard library
import os
import logging

# Third-party libraries
import pytest  # pytest v6.0+

# Internal module imports
from test.hook_tests import test_custom_gcp_hook  # src/test/hook_tests/test_custom_gcp_hook.py
from test.hook_tests import test_custom_http_hook  # src/test/hook_tests/test_custom_http_hook.py
from test.hook_tests import test_custom_postgres_hook  # src/test/hook_tests/test_custom_postgres_hook.py
from test import utils  # src/test/utils/__init__.py

# Initialize logger
logger = logging.getLogger('airflow.test.hooks')

# Define global variables
HOOK_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))


def configure_hook_tests():
    """
    Configures the hook testing environment and registers hook-specific test markers
    """
    # Set up logging configuration specific for hook tests
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Register custom pytest markers for hook tests if pytest is available
    try:
        pytest.mark.hook_test  # type:ignore[attr-defined]
        pytest.mark.gcp_hook  # type:ignore[attr-defined]
        pytest.mark.http_hook  # type:ignore[attr-defined]
        pytest.mark.postgres_hook  # type:ignore[attr-defined]
    except AttributeError:
        logger.warning("pytest markers not available. Ensure pytest is installed.")

    # Configure environment variables specific to hook testing
    os.environ['HOOK_TESTS_ENABLED'] = 'true'
    logger.info("Hook tests environment configured.")


# Expose test classes for test discovery
TestCustomGCPHook = test_custom_gcp_hook.TestCustomGCPHook
TestCustomHTTPHook = test_custom_http_hook.TestCustomHTTPHook
TestCustomHTTPHookAirflow2Compatibility = test_custom_http_hook.TestCustomHTTPHookAirflow2Compatibility
TestCustomHTTPHookIntegration = test_custom_http_hook.TestCustomHTTPHookIntegration
TestCustomPostgresHook = test_custom_postgres_hook.TestCustomPostgresHook

# Expose configuration function
configure_hook_tests = configure_hook_tests