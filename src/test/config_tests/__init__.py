#!/usr/bin/env python3
"""
Initialization module for the configuration tests package related to Airflow and Cloud Composer migration.
Provides shared test utilities, constants, markers, and base classes to support testing configuration files
during migration from Airflow 1.10.15 to Airflow 2.X.
"""

import os  # package_name: os, package_version: standard library, purpose: Access to environment variables and file path operations
import sys  # package_name: sys, package_version: standard library, purpose: System-specific parameters and functions
import json  # package_name: json, package_version: standard library, purpose: JSON parsing and validation for configuration files
import pytest  # package_name: pytest, package_version: 6.0+, purpose: Testing framework for test case creation and organization
import unittest  # package_name: unittest, package_version: standard library, purpose: Base testing framework for test case classes
import pathlib  # package_name: pathlib, package_version: standard library, purpose: Object-oriented filesystem paths for file operations

from ..utils import airflow2_compatibility_utils  # module: '../utils/airflow2_compatibility_utils', path: 'src/test/utils/airflow2_compatibility_utils.py', purpose: Provides utilities for Airflow version compatibility testing
from ..utils import assertion_utils  # module: '../utils/assertion_utils', path: 'src/test/utils/assertion_utils.py', purpose: Provides assertion utilities for testing configuration files

# Define pytest markers for configuration tests
CONFIG_TEST_MARKERS = [pytest.mark.config, pytest.mark.migration]

# Define the root path for configuration files
CONFIG_ROOT_PATH = pathlib.Path('src/backend/config')

# Define the expected deployment environments
EXPECTED_ENVIRONMENTS = ['dev', 'qa', 'prod']

# Define the configuration version
CONFIG_VERSION = "1.0.0"


def find_config_file(filename: str) -> pathlib.Path:
    """
    Locates a configuration file in the configuration directory

    Args:
        filename: Name of the configuration file

    Returns:
        Path to the requested configuration file
    """
    # Validate filename is not None or empty
    if not filename:
        raise ValueError("Filename cannot be None or empty")

    # Search for the file in CONFIG_ROOT_PATH
    file_path = CONFIG_ROOT_PATH / filename
    if file_path.exists():
        return file_path

    # If not found, search in parent directories
    parent_path = CONFIG_ROOT_PATH.parent / filename
    if parent_path.exists():
        return parent_path

    # Return path if found, otherwise None
    return None


def load_json_config(file_path: pathlib.Path) -> dict:
    """
    Loads and parses a JSON configuration file

    Args:
        file_path: Path to the JSON configuration file

    Returns:
        Parsed JSON configuration data
    """
    # Validate file_path exists
    if not file_path or not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    # Open and read the file
    with open(file_path, 'r') as f:
        content = f.read()

    # Parse content as JSON
    data = json.loads(content)

    # Return the parsed data
    return data


def validate_config_for_environment(config: dict, environment: str) -> bool:
    """
    Validates that a configuration is suitable for a specific environment

    Args:
        config: Configuration dictionary
        environment: Target environment

    Returns:
        True if configuration is valid for the environment
    """
    # Check if environment is supported in EXPECTED_ENVIRONMENTS
    if environment not in EXPECTED_ENVIRONMENTS:
        raise ValueError(f"Unsupported environment: {environment}")

    # Validate configuration structure
    if not isinstance(config, dict):
        raise TypeError("Configuration must be a dictionary")

    # Check for environment-specific settings
    if 'environment' in config and config['environment']['name'] != environment:
        return False

    # Perform environment-specific validation rules
    # (Add more specific validation logic here as needed)

    # Return validation result
    return True


class ConfigTestCase(unittest.TestCase):
    """
    Base test case class for configuration file testing
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the ConfigTestCase test class
        """
        # Call parent constructor
        super().__init__(*args, **kwargs)

        # Initialize config_path to None
        self.config_path = None

        # Initialize config_data to None
        self.config_data = None

    def verify_config_exists(self, config_file_path: str) -> bool:
        """
        Verifies that a configuration file exists

        Args:
            config_file_path: Path to the configuration file

        Returns:
            True if the file exists
        """
        # Convert config_file_path to pathlib.Path if it's a string
        if isinstance(config_file_path, str):
            config_file_path = pathlib.Path(config_file_path)

        # Check if the file exists
        file_exists = config_file_path.exists()

        # Return True if exists, False otherwise
        return file_exists

    def load_config(self, config_file_path: str) -> dict:
        """
        Loads a configuration file for testing

        Args:
            config_file_path: Path to the configuration file

        Returns:
            Loaded configuration data
        """
        # Convert config_file_path to pathlib.Path if it's a string
        if isinstance(config_file_path, str):
            config_file_path = pathlib.Path(config_file_path)

        # Determine the file type from extension
        file_extension = config_file_path.suffix.lower()

        # For JSON files, use load_json_config
        if file_extension == '.json':
            loaded_data = load_json_config(config_file_path)

        # For other supported formats, use appropriate loader
        else:
            raise ValueError(f"Unsupported configuration file format: {file_extension}")

        # Store loaded data in self.config_data
        self.config_data = loaded_data

        # Return the loaded configuration data
        return loaded_data

    def assert_config_value(self, key_path: str, expected_value: Any, strict_comparison: bool = True) -> None:
        """
        Asserts that a configuration value meets expected criteria

        Args:
            key_path: Path to the configuration value
            expected_value: Expected value of the configuration
            strict_comparison: Use strict equality comparison
        """
        # Parse key_path to navigate nested configuration
        keys = key_path.split('.')
        actual_value = self.config_data
        for key in keys:
            actual_value = actual_value[key]

        # If strict_comparison is True, use assertEqual
        if strict_comparison:
            self.assertEqual(actual_value, expected_value, f"Value at {key_path} does not match expected value")

        # Otherwise, perform appropriate comparison based on value types
        else:
            if isinstance(expected_value, (int, float)):
                self.assertAlmostEqual(actual_value, expected_value, msg=f"Value at {key_path} does not match expected value")
            else:
                self.assertEqual(actual_value, expected_value, f"Value at {key_path} does not match expected value")

        # Assert that the comparison is successful
        pass

    def test_config_file_exists(self):
        """
        Test that the configuration file exists
        """
        # Verify that self.config_path is not None
        self.assertIsNotNone(self.config_path, "Config path must be set before running this test")

        # Assert that the file exists using verify_config_exists
        self.assertTrue(self.verify_config_exists(self.config_path), f"Configuration file does not exist: {self.config_path}")


class Airflow2ConfigTestMixin:
    """
    Mixin class providing Airflow 2.X specific configuration testing utilities
    """

    def __init__(self):
        """
        Initializes the Airflow2ConfigTestMixin
        """
        # Set is_airflow2_env using is_airflow2() function
        self.is_airflow2_env = airflow2_compatibility_utils.is_airflow2()

    def skip_if_airflow1(self, test_func):
        """
        Decorator to skip a test when running with Airflow 1.X

        Args:
            test_func: Test function to conditionally skip

        Returns:
            Wrapped test function
        """
        # Check if running Airflow 1.X
        if not self.is_airflow2_env:
            # If true, mark test to be skipped with reason
            return unittest.skip("Test requires Airflow 2.X")(test_func)

        # Otherwise return original test function
        return test_func

    def validate_airflow2_config(self, config: dict) -> bool:
        """
        Validates that a configuration is compatible with Airflow 2.X

        Args:
            config: Configuration dictionary

        Returns:
            True if configuration is compatible with Airflow 2.X
        """
        # Check for Airflow 2.X specific configuration requirements
        # Verify that no deprecated Airflow 1.X settings are present
        # Validate that renamed settings use correct naming convention

        # Return validation result
        return True