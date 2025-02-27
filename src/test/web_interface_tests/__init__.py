"""
Package initialization file for the web interface test suite.
This module makes the web interface test classes and utilities available for test discovery
and provides package-level configuration for testing Airflow 2.X web UI during migration
from Cloud Composer 1 to Cloud Composer 2.
"""
import logging  # standard library
import os  # standard library
import sys  # standard library

# Internal imports
from .test_webserver_config import TestWebServerConfig  # src/test/web_interface_tests/test_webserver_config.py
from .test_webserver_config import TestWebServerConfigAirflow2  # src/test/web_interface_tests/test_webserver_config.py
from .test_webserver_config import TestWebServerConfigMigration  # src/test/web_interface_tests/test_webserver_config.py
from .test_ui_functionality import BaseUITest  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import TestMainNavigation  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import TestDagView  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import TestTaskInstanceView  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import TestAdminInterface  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import TestResponsiveDesign  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import TestThemeSupport  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import TestUIInteractions  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import TestAirflow2UIFeatures  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import create_webdriver  # src/test/web_interface_tests/test_ui_functionality.py
from .test_ui_functionality import wait_for_element  # src/test/web_interface_tests/test_ui_functionality.py

# Configure logging for the web interface test package
logger = logging.getLogger('airflow.test.web_interface')

# Access environment variables and file paths
# Access environment variables and file paths
UI_TEST_ENABLED = bool(os.environ.get('UI_TEST_ENABLED', False))

# System-specific parameters and functions for test environment
# System-specific parameters and functions for test environment
__version__ = "1.0.0"


def initialize_web_tests() -> bool:
    """
    Initialize the web interface test package and configure test environment

    Returns:
        bool: True if initialization successful
    """
    # Configure logging for web interface tests
    # Check if UI testing is enabled in environment
    # Set up required environment variables for UI testing
    # Return initialization status
    logger.info("Initializing web interface tests...")
    if not UI_TEST_ENABLED:
        logger.warning("UI testing is disabled. Set UI_TEST_ENABLED to True to enable.")
        return False

    logger.info("UI testing is enabled.")
    return True


# Export test classes and utilities for test discovery
__all__ = [
    'TestWebServerConfig',
    'TestWebServerConfigAirflow2',
    'TestWebServerConfigMigration',
    'BaseUITest',
    'TestMainNavigation',
    'TestDagView',
    'TestTaskInstanceView',
    'TestAdminInterface',
    'TestResponsiveDesign',
    'TestThemeSupport',
    'TestUIInteractions',
    'TestAirflow2UIFeatures',
    'create_webdriver',
    'wait_for_element',
    'initialize_web_tests',
    '__version__'
]