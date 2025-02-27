#!/usr/bin/env python3
"""
Test module for validating the Celery executor and worker functionality during migration from
Apache Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2. Contains comprehensive
test suites to ensure that task scheduling, distribution, and execution work correctly
with the Celery executor across both Airflow versions.
"""

import logging  # Python standard library - Configure logging for the Celery test package
import os  # Python standard library - Access environment variables for Celery test configuration
import sys  # Python standard library - Access system-specific parameters and functions

# Internal imports
from .test_celery_executor import TestCeleryExecutor  # src/test/celery_tests/test_celery_executor.py - Import the main Celery executor test class for package-level exposure
from .test_celery_executor import TestCeleryExecutorIntegration  # src/test/celery_tests/test_celery_executor.py - Import the Celery executor integration test class for package-level exposure
from .test_celery_executor import TestCeleryExecutorAirflow2Migration  # src/test/celery_tests/test_celery_executor.py - Import the Celery executor migration test class for package-level exposure
from .test_celery_executor import create_test_celery_task  # src/test/celery_tests/test_celery_executor.py - Import utility function for creating Celery test tasks for package-level exposure
from .test_celery_executor import wait_for_task_completion  # src/test/celery_tests/test_celery_executor.py - Import utility function for waiting on task completion for package-level exposure
from .test_workers import TestCeleryWorker  # src/test/celery_tests/test_workers.py - Import the Celery worker test class for package-level exposure
from .test_workers import TestWorkerPool  # src/test/celery_tests/test_workers.py - Import the worker pool test class for package-level exposure
from .test_workers import TestWorkerMigrationCompatibility  # src/test/celery_tests/test_workers.py - Import the worker migration compatibility test class for package-level exposure
from .test_workers import TestCeleryKubernetesIntegration  # src/test/celery_tests/test_workers.py - Import the Celery-Kubernetes integration test class for package-level exposure
from .test_workers import create_test_worker  # src/test/celery_tests/test_workers.py - Import utility function for creating test workers for package-level exposure
from .test_workers import get_worker_stats  # src/test/celery_tests/test_workers.py - Import utility function for retrieving worker statistics for package-level exposure

# Configure logging for the Celery test package
logger = logging.getLogger('airflow.test.celery')

# Define default Celery configuration for testing
CELERY_CONFIG = {
    "broker_url": "redis://localhost:6379/0",
    "result_backend": "redis://localhost:6379/0",
    "worker_concurrency": 16,
    "task_default_queue": "default",
    "task_default_exchange": "airflow",
    "task_default_exchange_type": "direct"
}

__all__ = [
    "TestCeleryExecutor",
    "TestCeleryExecutorIntegration",
    "TestCeleryExecutorAirflow2Migration",
    "TestCeleryWorker",
    "TestWorkerPool",
    "TestWorkerMigrationCompatibility",
    "TestCeleryKubernetesIntegration",
    "create_test_celery_task",
    "wait_for_task_completion",
    "create_test_worker",
    "get_worker_stats",
    "configure_celery_testing",
    "is_celery_executor_available",
    "CELERY_CONFIG"
]


def configure_celery_testing(custom_config: dict = None) -> dict:
    """
    Configures the Celery testing environment with appropriate settings for both Airflow 1.X and 2.X compatibility testing

    Args:
        custom_config (dict): A dictionary containing custom Celery configuration overrides

    Returns:
        dict: Complete Celery configuration dictionary
    """
    # Start with base configuration from CELERY_CONFIG
    celery_config = CELERY_CONFIG.copy()

    # Add custom configuration if provided
    if custom_config:
        celery_config.update(custom_config)

    # Set environment-specific overrides
    if os.environ.get('CI'):
        celery_config['broker_url'] = os.environ.get('CELERY_BROKER_URL', celery_config['broker_url'])
        celery_config['result_backend'] = os.environ.get('CELERY_RESULT_BACKEND', celery_config['result_backend'])

    # Configure testing-specific Celery settings
    celery_config['task_always_eager'] = True  # Execute tasks locally instead of sending to Celery workers
    celery_config['task_store_errors_even_if_ignored'] = True  # Store errors for testing purposes

    # Apply Airflow version-specific settings
    # Add any Airflow version-specific settings here if needed

    # Return the complete configuration dictionary
    return celery_config


def is_celery_executor_available() -> bool:
    """
    Checks if the Celery executor is available and configured correctly in the current environment

    Returns:
        bool: True if Celery executor is available, False otherwise
    """
    # Check for required Celery packages
    try:
        import celery  # noqa: F401 # celery-5.2+
    except ImportError:
        logger.warning("Celery package is not installed")
        return False

    # Check for broker availability (Redis)
    try:
        import redis  # noqa: F401
    except ImportError:
        logger.warning("Redis package is not installed")
        return False

    # Verify Airflow configuration for Celery support
    if os.environ.get('AIRFLOW__CORE__EXECUTOR') != 'CeleryExecutor':
        logger.warning("Airflow is not configured to use CeleryExecutor")
        return False

    # Return availability status
    return True