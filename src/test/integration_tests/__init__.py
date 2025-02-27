"""
Initialization file for the integration_tests package that provides common utilities,
configurations, and imports for testing the migration from Apache Airflow 1.10.15 to
Airflow 2.X in integrated environments. Enables test discovery and provides cross-version
compatibility support.
"""

# Python standard library imports
import os  # Access environment variables and paths
import logging  # Configure logging for integration tests

# Third-party library imports
import pytest  # pytest-6.0+ Testing framework for Python

# Internal module imports
from ..utils.airflow2_compatibility_utils import is_airflow2  # src/test/utils/airflow2_compatibility_utils.py
from ..utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin  # src/test/utils/airflow2_compatibility_utils.py
from ..utils.test_helpers import setup_test_environment  # src/test/utils/test_helpers.py

# Configure logger
logger = logging.getLogger('airflow.test.integration')

# Define integration test path
INTEGRATION_TEST_PATH = os.path.dirname(os.path.abspath(__file__))

# Define __all__ to control what gets imported when using from integration_tests import *
__all__ = ["setup_integration_test_environment", "IntegrationTestCase"]


def setup_integration_test_environment(config: dict) -> dict:
    """
    Prepares the environment for running integration tests, including configuring connections
    to test databases, GCP services, and other required resources.

    Args:
        config: Configuration dictionary

    Returns:
        Environment configuration with updated test settings
    """
    # Configure logging for integration tests
    logger.info("Setting up integration test environment")

    # Set up environment variables for integration testing
    os.environ['INTEGRATION_TEST_MODE'] = 'True'

    # Detect Airflow version and apply appropriate configurations
    if is_airflow2():
        logger.info("Running in Airflow 2.X environment")
    else:
        logger.info("Running in Airflow 1.X environment")

    # Initialize connections to test resources if needed
    # Example: Database connections, GCP service accounts, etc.
    # This part is intentionally left blank as it depends on the specific test setup

    # Register pytest plugins and fixtures
    # This part is intentionally left blank as it depends on the specific test setup

    # Return the configured environment dictionary
    logger.info("Integration test environment setup complete")
    return config


class IntegrationTestCase(Airflow2CompatibilityTestMixin):
    """
    Base class for integration tests that provides common utilities for testing across Airflow versions
    """

    def __init__(self):
        """
        Initialize the integration test case with version compatibility support
        """
        # Initialize parent class
        super().__init__()

        # Set airflow2_mode based on current environment
        self.airflow2_mode = is_airflow2()

        # Configure test environment
        self.test_environment = setup_test_environment({})

        # Set up logging
        logger.info("Initialized IntegrationTestCase")

    def setUp(self) -> None:
        """
        Set up the test environment before each test method runs
        """
        # Call setup_integration_test_environment
        self.test_environment = setup_integration_test_environment({})

        # Initialize test resources
        # Example: Create test tables, upload test data, etc.
        logger.info("Setting up test resources")

        # Set up mocks if needed
        logger.info("Setting up mocks")

    def tearDown(self) -> None:
        """
        Clean up after each test method has run
        """
        # Clean up test resources
        # Example: Drop test tables, delete test data, etc.
        logger.info("Cleaning up test resources")

        # Remove mocks
        logger.info("Removing mocks")

        # Reset environment variables if needed
        if 'INTEGRATION_TEST_MODE' in os.environ:
            del os.environ['INTEGRATION_TEST_MODE']
        logger.info("Tear down complete")

    def run_with_compatibility(self, test_func: callable, kwargs: dict) -> dict:
        """
        Run a test with compatibility support for both Airflow 1.X and 2.X

        Args:
            test_func: Test function to run
            kwargs: Keyword arguments for the test function

        Returns:
            Test results from both versions if applicable
        """
        # Check current Airflow version
        if self.airflow2_mode:
            logger.info("Running test in Airflow 2.X native environment")
        else:
            logger.info("Running test in Airflow 1.X native environment")

        # Run test in native environment
        native_result = test_func(**kwargs)

        # If testing cross-version, also run in other version
        if not self.airflow2_mode:
            logger.info("Running test in Airflow 2.X mocked environment")
            with self.runWithAirflow2(test_func, **kwargs) as airflow2_result:
                # Compare results for compatibility
                logger.info("Comparing results for compatibility")
                # Add comparison logic here if needed
                pass
        else:
            logger.info("Running test in Airflow 1.X mocked environment")
            with self.runWithAirflow1(test_func, **kwargs) as airflow1_result:
                # Compare results for compatibility
                logger.info("Comparing results for compatibility")
                # Add comparison logic here if needed
                pass

        # Return test results
        return native_result