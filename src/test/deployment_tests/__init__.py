#!/usr/bin/env python3
"""
Initialization module for deployment tests focused on validating the deployment process
during migration from Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2.
Provides common utility functions, path helpers, and fixtures for testing deployment
scripts, environment configurations, and multi-environment deployment workflows.
"""

# Python standard library imports
import os  # Python standard library
import sys  # Python standard library
import pathlib  # Python standard library
import logging  # Python standard library

# Third-party library imports
import pytest  # pytest-6.0+

# Internal module imports
from ..utils import assertion_utils  # src/test/utils/assertion_utils.py
from ..utils import test_helpers  # src/test/utils/test_helpers.py
from ..fixtures import mock_gcp_services  # src/test/fixtures/mock_gcp_services.py

# Initialize logger
LOGGER = logging.getLogger(__name__)

# Define global variables
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(TEST_DIR, '../../..'))
BACKEND_DIR = os.path.join(PROJECT_ROOT, 'src', 'backend')
SCRIPT_DIR = os.path.join(BACKEND_DIR, 'scripts')
CONFIG_DIR = os.path.join(BACKEND_DIR, 'config')
CI_CD_DIR = os.path.join(BACKEND_DIR, 'ci-cd')
DAGS_DIR = os.path.join(BACKEND_DIR, 'dags')
TEST_ENVIRONMENTS = ['dev', 'qa', 'prod']


def get_test_file_path(relative_path: str) -> str:
    """
    Utility function to get absolute path to a test file

    Args:
        relative_path: Relative path to the test file

    Returns:
        Absolute path to the test file
    """
    # Combine TEST_DIR with the relative path
    absolute_path = os.path.join(TEST_DIR, relative_path)
    # Return the absolute file path
    return absolute_path


def get_deployment_script_path(script_name: str) -> str:
    """
    Utility function to get absolute path to a deployment script

    Args:
        script_name: Name of the deployment script

    Returns:
        Absolute path to the script
    """
    # Combine SCRIPT_DIR with the script name
    absolute_path = os.path.join(SCRIPT_DIR, script_name)
    # Return the absolute script path
    return absolute_path


def get_config_path(config_name: str) -> str:
    """
    Utility function to get absolute path to a configuration file

    Args:
        config_name: Name of the configuration file

    Returns:
        Absolute path to the configuration file
    """
    # Combine CONFIG_DIR with the config name
    absolute_path = os.path.join(CONFIG_DIR, config_name)
    # Return the absolute config path
    return absolute_path


def get_ci_cd_script_path(script_name: str) -> str:
    """
    Utility function to get absolute path to a CI/CD script

    Args:
        script_name: Name of the CI/CD script

    Returns:
        Absolute path to the CI/CD script
    """
    # Combine CI_CD_DIR with the script name
    absolute_path = os.path.join(CI_CD_DIR, script_name)
    # Return the absolute CI/CD script path
    return absolute_path


def create_test_environment(environment: str, env_vars: dict, with_gcp_mocks: bool) -> dict:
    """
    Creates a test environment with specified configuration for deployment testing

    Args:
        environment: Name of the environment (dev, qa, prod)
        env_vars: Dictionary of environment variables to set
        with_gcp_mocks: Boolean indicating whether to set up GCP mocks

    Returns:
        Test environment configuration with paths and settings
    """
    # Validate environment is one of TEST_ENVIRONMENTS
    if environment not in TEST_ENVIRONMENTS:
        raise ValueError(f"Invalid environment: {environment}. Must be one of {TEST_ENVIRONMENTS}")

    # Set up basic environment configuration with temp directories
    test_env = {
        'environment': environment,
        'temp_dir': os.path.join(TEST_DIR, f"temp_{environment}"),
        'log_dir': os.path.join(TEST_DIR, f"temp_{environment}", "logs")
    }

    # Add environment variables for the specified environment
    test_env['env_vars'] = env_vars
    for key, value in env_vars.items():
        os.environ[key] = value

    # Set up GCP mocks if with_gcp_mocks is True
    if with_gcp_mocks:
        test_env['gcp_mocks'] = {
            'mock_gcs_bucket': mock_gcp_services.mock_gcs_bucket,
            'mock_composer_environment': mock_gcp_services.mock_composer_environment
        }

    # Return the configured test environment dictionary
    return test_env


def cleanup_test_environment(test_env: dict) -> None:
    """
    Cleans up resources created for a test environment

    Args:
        test_env: Dictionary containing test environment configuration

    Returns:
        No return value
    """
    # Remove temporary directories created for the test
    if os.path.exists(test_env['temp_dir']):
        import shutil
        shutil.rmtree(test_env['temp_dir'])

    # Clear any mocked GCP resources
    if 'gcp_mocks' in test_env:
        test_env['gcp_mocks']['mock_gcs_bucket'].stop()
        test_env['gcp_mocks']['mock_composer_environment'].stop()

    # Reset environment variables to original values
    if 'env_vars' in test_env:
        for key in test_env['env_vars']:
            if key in os.environ:
                del os.environ[key]


def get_environment_config(environment: str) -> dict:
    """
    Retrieves environment-specific configuration for testing

    Args:
        environment: Name of the environment (dev, qa, prod)

    Returns:
        Environment configuration for testing
    """
    # Validate environment is one of TEST_ENVIRONMENTS
    if environment not in TEST_ENVIRONMENTS:
        raise ValueError(f"Invalid environment: {environment}. Must be one of {TEST_ENVIRONMENTS}")

    # Create standard test configuration for the environment
    env_config = {
        'environment': environment,
        'project_id': f"test-project-{environment}",
        'location': "us-central1"
    }

    # Add environment-specific settings (project ID, location, etc.)
    if environment == 'dev':
        env_config['project_id'] = "test-project-dev"
    elif environment == 'qa':
        env_config['project_id'] = "test-project-qa"
    elif environment == 'prod':
        env_config['project_id'] = "test-project-prod"

    # Return the environment configuration dictionary
    return env_config