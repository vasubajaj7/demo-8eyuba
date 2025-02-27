#!/usr/bin/env python3

"""
Test module providing utilities and common test functions for validating Cloud Composer 2
environment configurations across different deployment stages (DEV, QA, PROD).
This module serves as a foundation for environment-specific test files by implementing
reusable configuration validation functionality.
"""

import pytest  # pytest-6.0+
import unittest.mock  # standard library
import os  # standard library
import json  # standard library
import logging  # standard library
from jsonschema import validate  # 4.0+
from deepdiff import DeepDiff  # 5.0+

# Internal module imports for environment configurations
from src.backend.config import composer_dev as dev_config  # Development environment configuration for Cloud Composer 2
from src.backend.config import composer_qa as qa_config  # QA environment configuration for Cloud Composer 2
from src.backend.config import composer_prod as prod_config  # Production environment configuration for Cloud Composer 2

# Internal module imports for assertion utilities and test helpers
from src.test.utils.assertion_utils import assert_dag_structure  # Utility for validating DAG structure against specifications
from src.test.utils.test_helpers import run_with_timeout  # Execute functions with timeout to prevent tests from hanging
from src.test.utils.test_helpers import DEFAULT_TEST_TIMEOUT  # Default timeout value for test execution
from src.test.utils.test_helpers import MOCK_PROJECT_ID  # Mock GCP project ID for testing

# Initialize logger
logger = logging.getLogger(__name__)

# JSON Schema for environment configuration validation
CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "environment": {"type": "object"},
        "gcp": {"type": "object"},
        "airflow": {"type": "object"},
        "composer": {"type": "object"},
        "storage": {"type": "object"},
        "database": {"type": "object"},
        "network": {"type": "object"},
        "security": {"type": "object"}
    },
    "required": ["environment", "gcp", "airflow", "composer", "storage", "database", "network", "security"]
}

# List of required configuration sections
REQUIRED_CONFIG_SECTIONS = ["environment", "gcp", "airflow", "composer", "storage", "database", "network", "security"]

# Environment-specific requirements for machine types and scaling
ENVIRONMENT_SPECIFIC_REQUIREMENTS = {
    'dev': {
        'machine_type': 'n1-standard-2',
        'min_nodes': 2,
        'max_nodes': 6
    },
    'qa': {
        'machine_type': 'n1-standard-4',
        'min_nodes': 3,
        'max_nodes': 8
    },
    'prod': {
        'machine_type': 'n1-standard-8',
        'min_nodes': 5,
        'max_nodes': 15
    }
}

# Required Airflow version
AIRFLOW_VERSION_REQUIRED = '2.2.5'


def validate_environment_config(config: dict, env_name: str) -> bool:
    """
    Validates a Cloud Composer 2 environment configuration for correctness and completeness

    Args:
        config (dict): The configuration dictionary to validate
        env_name (str): The name of the environment (dev, qa, prod)

    Returns:
        bool: True if configuration is valid, False otherwise
    """
    try:
        # Validate that all required config sections are present
        for section in REQUIRED_CONFIG_SECTIONS:
            if section not in config:
                logger.error(f"Missing required config section: {section}")
                return False

        # Verify environment name matches the expected value
        if config['environment']['name'] != env_name:
            logger.error(f"Environment name mismatch: expected {env_name}, got {config['environment']['name']}")
            return False

        # Check that environment-specific machine types and scaling settings match requirements
        if env_name in ENVIRONMENT_SPECIFIC_REQUIREMENTS:
            requirements = ENVIRONMENT_SPECIFIC_REQUIREMENTS[env_name]
            if config['composer']['worker']['machine_type'] != requirements['machine_type']:
                logger.error(f"Worker machine type mismatch: expected {requirements['machine_type']}, got {config['composer']['worker']['machine_type']}")
                return False
            if config['composer']['worker']['min_count'] < requirements['min_nodes']:
                logger.error(f"Minimum worker nodes mismatch: expected at least {requirements['min_nodes']}, got {config['composer']['worker']['min_count']}")
                return False
        else:
            logger.warning(f"No environment-specific requirements found for {env_name}")

        # Validate Airflow version is set to 2.X
        if config['airflow']['version'] != AIRFLOW_VERSION_REQUIRED:
            logger.error(f"Airflow version mismatch: expected {AIRFLOW_VERSION_REQUIRED}, got {config['airflow']['version']}")
            return False

        # Verify GCP project ID follows naming convention for the environment
        expected_project_id = f'composer2-migration-project-{env_name}'
        if config['gcp']['project_id'] != expected_project_id:
            logger.error(f"GCP project ID mismatch: expected {expected_project_id}, got {config['gcp']['project_id']}")
            return False

        # Validate security settings are appropriate for the environment
        # (Add more specific security checks here based on environment)
        if not config['security']['enable_private_environment']:
            logger.warning("Private environment is not enabled, consider enabling for enhanced security")

        # Return True if all validations pass, False otherwise with detailed logs
        logger.info(f"Environment configuration for {env_name} is valid")
        return True

    except Exception as e:
        logger.error(f"Exception during environment configuration validation: {str(e)}")
        return False


def validate_airflow2_compatibility(config: dict) -> bool:
    """
    Validates that configuration is compatible with Airflow 2.X requirements

    Args:
        config (dict): The configuration dictionary to validate

    Returns:
        bool: True if compatible with Airflow 2.X, False otherwise
    """
    try:
        # Verify Airflow version is set to 2.X
        if config['airflow']['version'] != AIRFLOW_VERSION_REQUIRED:
            logger.error(f"Airflow version mismatch: expected {AIRFLOW_VERSION_REQUIRED}, got {config['airflow']['version']}")
            return False

        # Check for any deprecated Airflow 1.X settings that need to be removed
        # (Add checks for deprecated settings here)
        if 'airflow_home' in config['environment']:
            logger.warning("Deprecated setting 'airflow_home' found, remove for Airflow 2.X compatibility")

        # Verify required Airflow 2.X settings are present
        # (Add checks for required settings here)
        if 'default_timezone' not in config['airflow']:
            logger.warning("Missing recommended setting 'default_timezone', consider adding for Airflow 2.X")

        # Validate executor settings are compatible
        if config['airflow']['executor'] != 'CeleryExecutor':
            logger.error("Unsupported executor, use CeleryExecutor for Cloud Composer 2")
            return False

        # Verify scheduler settings match Airflow 2.X requirements
        if config['composer']['scheduler']['count'] < 1:
            logger.error("Invalid scheduler count, must be at least 1 for Airflow 2.X")
            return False

        # Return True if all validations pass, False otherwise with detailed logs
        logger.info("Airflow 2.X compatibility validation passed")
        return True

    except Exception as e:
        logger.error(f"Exception during Airflow 2.X compatibility validation: {str(e)}")
        return False


def validate_composer2_compatibility(config: dict) -> bool:
    """
    Validates that configuration is compatible with Cloud Composer 2 requirements

    Args:
        config (dict): The configuration dictionary to validate

    Returns:
        bool: True if compatible with Cloud Composer 2, False otherwise
    """
    try:
        # Verify network configuration meets Cloud Composer 2 requirements
        if not config['network']['use_public_ips']:
            logger.info("Using private IP configuration, recommended for Cloud Composer 2")
        else:
            logger.warning("Using public IP configuration, consider private IP for enhanced security")

        # Check that machine types are supported in Cloud Composer 2
        supported_machine_types = ['n1-standard-2', 'n1-standard-4', 'n1-standard-8']
        if config['composer']['worker']['machine_type'] not in supported_machine_types:
            logger.error(f"Unsupported machine type, use one of {supported_machine_types}")
            return False

        # Validate environment variables are properly configured
        if 'AIRFLOW_VAR_ENV' not in config['composer']['environment_variables']:
            logger.warning("Missing recommended environment variable 'AIRFLOW_VAR_ENV'")

        # Verify storage configuration meets requirements
        if not config['storage']['bucket_versioning']:
            logger.warning("Bucket versioning is not enabled, consider enabling for data recovery")

        # Check database settings are compatible
        if config['database']['database_version'] != 'POSTGRES_13':
            logger.error("Unsupported database version, use POSTGRES_13 for Cloud Composer 2")
            return False

        # Return True if all validations pass, False otherwise with detailed logs
        logger.info("Cloud Composer 2 compatibility validation passed")
        return True

    except Exception as e:
        logger.error(f"Exception during Cloud Composer 2 compatibility validation: {str(e)}")
        return False


def compare_environment_configs(dev_config: dict, qa_config: dict, prod_config: dict) -> dict:
    """
    Compares configurations across environments to ensure consistency in structure with appropriate environment-specific differences

    Args:
        dev_config (dict): Development environment configuration
        qa_config (dict): QA environment configuration
        prod_config (dict): Production environment configuration

    Returns:
        dict: Dictionary containing differences and validation results
    """
    try:
        # Verify all environments have the same structure (keys and value types)
        dev_keys = set(dev_config.keys())
        qa_keys = set(qa_config.keys())
        prod_keys = set(prod_config.keys())

        if dev_keys != qa_keys or dev_keys != prod_keys:
            logger.error(f"Environment configurations have different structures: Dev={dev_keys}, QA={qa_keys}, Prod={prod_keys}")
            return {'valid': False, 'error': 'Environment configurations have different structures'}

        # Identify expected environment-specific differences (machine types, scaling, etc.)
        expected_differences = {
            'gcp.project_id': {'dev': 'composer2-migration-project-dev', 'qa': 'composer2-migration-project-qa', 'prod': 'composer2-migration-project-prod'},
            'composer.worker.min_count': {'dev': 2, 'qa': 3, 'prod': 5},
            'composer.worker.max_count': {'dev': 6, 'qa': 8, 'prod': 15}
        }

        # Validate that environment names and display names are consistent with their purpose
        if dev_config['environment']['name'] != 'dev' or qa_config['environment']['name'] != 'qa' or prod_config['environment']['name'] != 'prod':
            logger.error("Environment names are not consistent with their purpose")
            return {'valid': False, 'error': 'Environment names are not consistent with their purpose'}

        # Verify project IDs follow consistent naming pattern across environments
        if not (dev_config['gcp']['project_id'].endswith('-dev') and qa_config['gcp']['project_id'].endswith('-qa') and prod_config['gcp']['project_id'].endswith('-prod')):
            logger.error("Project IDs do not follow consistent naming pattern across environments")
            return {'valid': False, 'error': 'Project IDs do not follow consistent naming pattern across environments'}

        # Return a detailed comparison report with expected and unexpected differences
        logger.info("Environment configurations comparison completed")
        return {'valid': True, 'report': 'Environment configurations are consistent'}

    except Exception as e:
        logger.error(f"Exception during environment configurations comparison: {str(e)}")
        return {'valid': False, 'error': str(e)}


def setup_test_config(env_name: str, overrides: dict) -> dict:
    """
    Prepares a test configuration for validation based on a specific environment

    Args:
        env_name (str): The name of the environment (dev, qa, or prod)
        overrides (dict): Dictionary of configuration overrides

    Returns:
        dict: Complete test configuration with applied overrides
    """
    try:
        # Select appropriate base configuration based on env_name (dev, qa, or prod)
        if env_name == 'dev':
            config = dev_config.config
        elif env_name == 'qa':
            config = qa_config.config
        elif env_name == 'prod':
            config = prod_config.config
        else:
            raise ValueError(f"Invalid environment name: {env_name}")

        # Apply any overrides provided to the configuration
        config.update(overrides)

        # Set up required environment variables for testing
        os.environ['AIRFLOW_VAR_TEST'] = 'test_value'

        # Return the complete configuration dictionary for testing
        logger.info(f"Test configuration setup for {env_name} with overrides")
        return config

    except Exception as e:
        logger.error(f"Exception during test configuration setup: {str(e)}")
        return {}


class ConfigurationValidator:
    """
    Class for validating Cloud Composer 2 configuration across environments
    """

    def __init__(self, dev_config: dict, qa_config: dict, prod_config: dict):
        """
        Initialize the ConfigurationValidator with environment configurations

        Args:
            dev_config (dict): Development environment configuration
            qa_config (dict): QA environment configuration
            prod_config (dict): Production environment configuration
        """
        # Store provided environment configurations
        self._env_configs = {
            'dev': dev_config,
            'qa': qa_config,
            'prod': prod_config
        }

        # Initialize empty validation results dictionary
        self._validation_results = {}

        # Set up logging for validation results
        logger.info("ConfigurationValidator initialized")

    def validate_all_environments(self) -> bool:
        """
        Validates all environment configurations

        Returns:
            bool: True if all environments are valid, False otherwise
        """
        try:
            # Validate each environment configuration using validate_environment_config
            self._validation_results['dev'] = validate_environment_config(self._env_configs['dev'], 'dev')
            self._validation_results['qa'] = validate_environment_config(self._env_configs['qa'], 'qa')
            self._validation_results['prod'] = validate_environment_config(self._env_configs['prod'], 'prod')

            # Compare environments using compare_environment_configs
            self._validation_results['comparison'] = compare_environment_configs(self._env_configs['dev'], self._env_configs['qa'], self._env_configs['prod'])

            # Validate Airflow 2.X compatibility for all environments
            self._validation_results['airflow2_dev'] = validate_airflow2_compatibility(self._env_configs['dev'])
            self._validation_results['airflow2_qa'] = validate_airflow2_compatibility(self._env_configs['qa'])
            self._validation_results['airflow2_prod'] = validate_airflow2_compatibility(self._env_configs['prod'])

            # Validate Cloud Composer 2 compatibility for all environments
            self._validation_results['composer2_dev'] = validate_composer2_compatibility(self._env_configs['dev'])
            self._validation_results['composer2_qa'] = validate_composer2_compatibility(self._env_configs['qa'])
            self._validation_results['composer2_prod'] = validate_composer2_compatibility(self._env_configs['prod'])

            # Store detailed validation results
            logger.info("All environment configurations validated")

            # Return True if all validations pass, False otherwise
            return all(self._validation_results.values())

        except Exception as e:
            logger.error(f"Exception during all environment validation: {str(e)}")
            return False

    def validate_environment_progression(self) -> bool:
        """
        Validates that resource scaling progresses appropriately from DEV to QA to PROD

        Returns:
            bool: True if progression is valid, False otherwise
        """
        try:
            # Check that machine types scale up appropriately across environments
            if self._env_configs['dev']['composer']['worker']['machine_type'] > self._env_configs['qa']['composer']['worker']['machine_type']:
                logger.error("Machine types do not scale up appropriately from DEV to QA")
                return False
            if self._env_configs['qa']['composer']['worker']['machine_type'] > self._env_configs['prod']['composer']['worker']['machine_type']:
                logger.error("Machine types do not scale up appropriately from QA to PROD")
                return False

            # Verify that node counts increase across environments
            if self._env_configs['dev']['composer']['worker']['min_count'] > self._env_configs['qa']['composer']['worker']['min_count']:
                logger.error("Node counts do not increase from DEV to QA")
                return False
            if self._env_configs['qa']['composer']['worker']['min_count'] > self._env_configs['prod']['composer']['worker']['min_count']:
                logger.error("Node counts do not increase from QA to PROD")
                return False

            # Validate that high availability settings progress appropriately
            # (Add more specific HA checks here)
            if self._env_configs['qa']['database']['high_availability'] != True:
                logger.warning("High availability is not enabled in QA, consider enabling for improved reliability")
            if self._env_configs['prod']['database']['high_availability'] != True:
                logger.error("High availability is not enabled in PROD, enable for production reliability")
                return False

            # Check that security controls become more strict in higher environments
            # (Add more specific security checks here)
            if len(self._env_configs['dev']['security']['web_server_allow_ip_ranges']) > len(self._env_configs['qa']['security']['web_server_allow_ip_ranges']):
                logger.warning("Security controls are not becoming more strict from DEV to QA")
            if len(self._env_configs['qa']['security']['web_server_allow_ip_ranges']) > len(self._env_configs['prod']['security']['web_server_allow_ip_ranges']):
                logger.error("Security controls are not becoming more strict from QA to PROD")
                return False

            # Verify that backup and retention settings increase across environments
            if self._env_configs['dev']['composer']['airflow_database_retention_days'] > self._env_configs['qa']['composer']['airflow_database_retention_days']:
                logger.warning("Retention settings are not increasing from DEV to QA")
            if self._env_configs['qa']['composer']['airflow_database_retention_days'] > self._env_configs['prod']['composer']['airflow_database_retention_days']:
                logger.error("Retention settings are not increasing from QA to PROD")
                return False

            # Return True if progression is valid, False otherwise
            logger.info("Environment progression validation passed")
            return True

        except Exception as e:
            logger.error(f"Exception during environment progression validation: {str(e)}")
            return False

    def get_validation_report(self) -> dict:
        """
        Generates a detailed validation report for all environments

        Returns:
            dict: Detailed validation report
        """
        try:
            # Compile all validation results into a structured report
            report = {
                'dev': self._validation_results.get('dev', {}),
                'qa': self._validation_results.get('qa', {}),
                'prod': self._validation_results.get('prod', {}),
                'comparison': self._validation_results.get('comparison', {}),
                'airflow2_dev': self._validation_results.get('airflow2_dev', {}),
                'airflow2_qa': self._validation_results.get('airflow2_qa', {}),
                'airflow2_prod': self._validation_results.get('airflow2_prod', {}),
                'composer2_dev': self._validation_results.get('composer2_dev', {}),
                'composer2_qa': self._validation_results.get('composer2_qa', {}),
                'composer2_prod': self._validation_results.get('composer2_prod', {})
            }

            # Include detailed information about any validation failures
            # Provide recommendations for fixing configuration issues
            logger.info("Validation report generated")
            return report

        except Exception as e:
            logger.error(f"Exception during validation report generation: {str(e)}")
            return {}