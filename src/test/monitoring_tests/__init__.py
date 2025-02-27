"""
Initialization module for the monitoring_tests package, which contains tests for Cloud Monitoring alerts, dashboards, metrics collection, and logging in the Cloud Composer 2 migration project. This module makes test classes and utility functions available at the package level for monitoring-related test cases.
"""

import os  # Python standard library - Access environment variables and file system operations
import logging  # Python standard library - Set up logging for the monitoring tests package
import pytest  # pytest-6.0+ - Testing framework used for monitoring tests

# Internal imports
from .test_alerts import TestAlertConfigurations  # src/test/monitoring_tests/test_alerts.py - Import test class for alert configuration validation
from .test_alerts import TestDagFailureAlerts  # src/test/monitoring_tests/test_alerts.py - Import test class for DAG failure alerts
from .test_alerts import TestTaskDurationAlerts  # src/test/monitoring_tests/test_alerts.py - Import test class for task duration alerts
from .test_alerts import TestComposerHealthAlerts  # src/test/monitoring_tests/test_alerts.py - Import test class for Composer health alerts
from .test_alerts import TestAlertIntegration  # src/test/monitoring_tests/test_alerts.py - Import test class for alert integration across environments
from .test_alerts import load_all_alert_configs  # src/test/monitoring_tests/test_alerts.py - Import utility function to load alert configurations
from .test_alerts import create_mock_time_series  # src/test/monitoring_tests/test_alerts.py - Import utility function to create mock time series
from .test_alerts import evaluate_alert_condition  # src/test/monitoring_tests/test_alerts.py - Import utility function to evaluate alert conditions
from ..utils.airflow2_compatibility_utils import airflow2_compatibility_utils  # src/test/utils/airflow2_compatibility_utils.py
from ..utils.airflow2_compatibility_utils.airflow2_compatibility_utils import is_airflow2  # src/test/utils/airflow2_compatibility_utils.py

# Set up logging for the monitoring tests package
logger = logging.getLogger('airflow.test.monitoring')

# Define global variables
MONITORING_DIR = os.path.dirname(os.path.abspath(__file__))
MONITORING_CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(MONITORING_DIR))), 'backend/monitoring')
VERSION = "0.1.0"

def setup_package():
    """
    Initialize the monitoring tests package
    """
    # Configure logging for monitoring tests
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Set up environment variables for monitoring tests
    os.environ['MONITORING_TEST'] = 'true'
    os.environ['MONITORING_CONFIG_DIR'] = MONITORING_CONFIG_DIR

    # Register monitoring test paths with pytest for discovery
    pytest.register_assert_rewrite(MONITORING_DIR)
    logger.info("Monitoring tests package initialized")

def get_monitoring_config_dir() -> str:
    """
    Returns the path to the monitoring configuration directory

    Returns:
        str: Path to monitoring configuration directory
    """
    # Return the path stored in MONITORING_CONFIG_DIR global variable
    return MONITORING_CONFIG_DIR