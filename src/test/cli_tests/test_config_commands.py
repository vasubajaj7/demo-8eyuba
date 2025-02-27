#!/usr/bin/env python3

"""
Unit tests for Airflow CLI configuration commands to verify compatibility between
Airflow 1.10.15 and Airflow 2.X during the Cloud Composer migration.
This file tests CLI commands related to configuration listing, setting and getting variables,
connections, and pools.
"""

import os  # Python standard library
import sys  # Python standard library
import tempfile  # Python standard library
import subprocess  # Python standard library
import unittest  # Python standard library
import pytest  # version: 6.0+
import json  # Python standard library
import pathlib  # Python standard library

# Internal imports
from ..utils.airflow2_compatibility_utils import is_airflow2, AIRFLOW_VERSION, Airflow2CompatibilityTestMixin  # src/test/utils/airflow2_compatibility_utils.py
from ..utils.test_helpers import setup_test_environment, reset_test_environment, AirflowTestEnvironment  # src/test/utils/test_helpers.py
from backend.config import config  # src/backend/config/__init__.py

# Define global constants for CLI command testing
CONFIG_COMMAND_BASE = "airflow config"
VARIABLE_COMMAND_BASE = "airflow variables"
CONNECTION_COMMAND_BASE = "airflow connections"
POOL_COMMAND_BASE = "airflow pools"
TEST_VARIABLE_KEY = "test_variable"
TEST_VARIABLE_VALUE = "test_value"

# Create a temporary config file for testing
TEST_CONFIG_FILE = tempfile.NamedTemporaryFile(suffix=".cfg", delete=False)


def run_config_command(command: str, args: list, check_return_code: bool = True) -> tuple:
    """
    Helper function to run Airflow configuration CLI commands and capture output

    Args:
        command: The Airflow CLI command to run (e.g., 'list', 'get')
        args: List of arguments to pass to the command
        check_return_code: Whether to check the return code of the command

    Returns:
        Tuple containing (return_code, stdout, stderr)
    """
    # Construct full command from base command and arguments
    full_command = [CONFIG_COMMAND_BASE, command] + args

    # Execute command using subprocess.run
    process = subprocess.run(full_command, capture_output=True, text=True)

    # Capture stdout and stderr
    return_code = process.returncode
    stdout_text = process.stdout
    stderr_text = process.stderr

    # If check_return_code is True, raise exception for non-zero return codes
    if check_return_code and return_code != 0:
        raise Exception(f"Command failed with code {return_code}: {stderr_text}")

    # Return tuple of (return_code, stdout_text, stderr_text)
    return return_code, stdout_text, stderr_text


def run_variable_command(command: str, args: list, check_return_code: bool = True) -> tuple:
    """
    Helper function to run Airflow variables CLI commands and capture output

    Args:
        command: The Airflow CLI command to run (e.g., 'list', 'get')
        args: List of arguments to pass to the command
        check_return_code: Whether to check the return code of the command

    Returns:
        Tuple containing (return_code, stdout, stderr)
    """
    # Construct full command from VARIABLE_COMMAND_BASE and arguments
    full_command = [VARIABLE_COMMAND_BASE, command] + args

    # Execute command using subprocess.run
    process = subprocess.run(full_command, capture_output=True, text=True)

    # Capture stdout and stderr
    return_code = process.returncode
    stdout_text = process.stdout
    stderr_text = process.stderr

    # If check_return_code is True, raise exception for non-zero return codes
    if check_return_code and return_code != 0:
        raise Exception(f"Command failed with code {return_code}: {stderr_text}")

    # Return tuple of (return_code, stdout_text, stderr_text)
    return return_code, stdout_text, stderr_text


def run_connection_command(command: str, args: list, check_return_code: bool = True) -> tuple:
    """
    Helper function to run Airflow connections CLI commands and capture output

    Args:
        command: The Airflow CLI command to run (e.g., 'list', 'get')
        args: List of arguments to pass to the command
        check_return_code: Whether to check the return code of the command

    Returns:
        Tuple containing (return_code, stdout, stderr)
    """
    # Construct full command from CONNECTION_COMMAND_BASE and arguments
    full_command = [CONNECTION_COMMAND_BASE, command] + args

    # Execute command using subprocess.run
    process = subprocess.run(full_command, capture_output=True, text=True)

    # Capture stdout and stderr
    return_code = process.returncode
    stdout_text = process.stdout
    stderr_text = process.stderr

    # If check_return_code is True, raise exception for non-zero return codes
    if check_return_code and return_code != 0:
        raise Exception(f"Command failed with code {return_code}: {stderr_text}")

    # Return tuple of (return_code, stdout_text, stderr_text)
    return return_code, stdout_text, stderr_text


def run_pool_command(command: str, args: list, check_return_code: bool = True) -> tuple:
    """
    Helper function to run Airflow pools CLI commands and capture output

    Args:
        command: The Airflow CLI command to run (e.g., 'list', 'get')
        args: List of arguments to pass to the command
        check_return_code: Whether to check the return code of the command

    Returns:
        Tuple containing (return_code, stdout, stderr)
    """
    # Construct full command from POOL_COMMAND_BASE and arguments
    full_command = [POOL_COMMAND_BASE, command] + args

    # Execute command using subprocess.run
    process = subprocess.run(full_command, capture_output=True, text=True)

    # Capture stdout and stderr
    return_code = process.returncode
    stdout_text = process.stdout
    stderr_text = process.stderr

    # If check_return_code is True, raise exception for non-zero return codes
    if check_return_code and return_code != 0:
        raise Exception(f"Command failed with code {return_code}: {stderr_text}")

    # Return tuple of (return_code, stdout_text, stderr_text)
    return return_code, stdout_text, stderr_text


def create_test_config_file(config_values: dict) -> str:
    """
    Creates a temporary config file for testing Airflow config commands

    Args:
        config_values: Dictionary of config values to write to the file

    Returns:
        Path to the temporary config file
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.cfg', delete=False) as temp_file:
        # Write config values in Airflow config format
        for section, settings in config_values.items():
            temp_file.write(f"[{section}]\n")
            for key, value in settings.items():
                temp_file.write(f"{key} = {value}\n")
            temp_file.write("\n")

    # Close file to ensure data is flushed to disk
    temp_file.close()

    # Return path to the temporary file
    return temp_file.name


class TestConfigCommands(unittest.TestCase):
    """Test case for Airflow config CLI commands"""

    def __init__(self, *args, **kwargs):
        """Initialize the test case"""
        super().__init__(*args, **kwargs)
        self.original_env = None
        self.test_config_file = None

    def setUp(self):
        """Set up the test environment before each test"""
        # Set up test environment using setup_test_environment
        setup_test_environment()

        # Store original environment for restoration
        self.original_env = os.environ.copy()

        # Create test config file for config commands
        test_config_values = {
            "core": {
                "executor": "SequentialExecutor",
                "sql_alchemy_conn": "sqlite:///:memory:"
            },
            "webserver": {
                "expose_config": "True"
            }
        }
        self.test_config_file = create_test_config_file(test_config_values)

        # Set AIRFLOW__CORE__CONFIG_FILE_PATH environment variable
        os.environ["AIRFLOW__CORE__CONFIG_FILE_PATH"] = self.test_config_file

    def tearDown(self):
        """Clean up after each test"""
        # Reset the test environment using reset_test_environment
        reset_test_environment()

        # Remove temporary config file if it exists
        if self.test_config_file and os.path.exists(self.test_config_file):
            os.remove(self.test_config_file)

    def test_config_list(self):
        """Test 'airflow config list' command to ensure it lists Airflow configurations"""
        # Run config list command using run_config_command
        return_code, stdout_text, stderr_text = run_config_command("list", [])

        # Verify command returns success code
        self.assertEqual(return_code, 0)

        # Verify output contains expected configuration sections
        self.assertIn("[core]", stdout_text)
        self.assertIn("[webserver]", stdout_text)

        # Ensure consistent behavior across Airflow versions
        if is_airflow2():
            self.assertIn("[logging]", stdout_text)
        else:
            self.assertNotIn("[logging]", stdout_text)

    def test_config_get(self):
        """Test 'airflow config get' command to retrieve configuration values"""
        # Run config get command for a specific section and key
        return_code, stdout_text, stderr_text = run_config_command("get", ["core", "executor"])

        # Verify command returns success code
        self.assertEqual(return_code, 0)

        # Verify output contains the expected config value
        self.assertIn("SequentialExecutor", stdout_text)

        # Test with invalid section/key and verify error handling
        return_code, stdout_text, stderr_text = run_config_command("get", ["invalid_section", "invalid_key"], check_return_code=False)
        self.assertNotEqual(return_code, 0)
        self.assertIn("Section or key not found", stderr_text)

    def test_config_section_list(self):
        """Test 'airflow config list --section' to list keys in a config section"""
        # Run config list command with section parameter
        return_code, stdout_text, stderr_text = run_config_command("list", ["--section", "core"])

        # Verify command returns success code
        self.assertEqual(return_code, 0)

        # Verify output contains expected keys for that section
        self.assertIn("executor = SequentialExecutor", stdout_text)
        self.assertIn("sql_alchemy_conn = sqlite:///:memory:", stdout_text)

        # Test with invalid section and verify error handling
        return_code, stdout_text, stderr_text = run_config_command("list", ["--section", "invalid_section"], check_return_code=False)
        self.assertNotEqual(return_code, 0)
        self.assertIn("Section not found", stderr_text)

    def test_variables_list(self):
        """Test 'airflow variables list' command to list Airflow variables"""
        # Set up test variable using variables set command
        run_variable_command("set", [TEST_VARIABLE_KEY, TEST_VARIABLE_VALUE])

        # Run variables list command
        return_code, stdout_text, stderr_text = run_variable_command("list", [])

        # Verify command returns success code
        self.assertEqual(return_code, 0)

        # Verify output contains test variable
        self.assertIn(TEST_VARIABLE_KEY, stdout_text)
        self.assertIn(TEST_VARIABLE_VALUE, stdout_text)

        # Test different output formats (e.g., --output json)
        return_code, stdout_text, stderr_text = run_variable_command("list", ["--output", "json"])
        self.assertEqual(return_code, 0)
        self.assertIn(TEST_VARIABLE_KEY, stdout_text)
        self.assertIn(TEST_VARIABLE_VALUE, stdout_text)

    def test_variables_get_set(self):
        """Test 'airflow variables get/set' commands to retrieve and store variables"""
        # Set a test variable with variables set command
        run_variable_command("set", [TEST_VARIABLE_KEY, TEST_VARIABLE_VALUE])

        # Get the variable with variables get command
        return_code, stdout_text, stderr_text = run_variable_command("get", [TEST_VARIABLE_KEY])

        # Verify retrieved value matches what was set
        self.assertEqual(return_code, 0)
        self.assertEqual(stdout_text.strip(), TEST_VARIABLE_VALUE)

        # Test setting variable from a JSON file
        temp_json_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump({"new_variable": "new_value"}, temp_json_file)
        temp_json_file.close()
        run_variable_command("set", ["--file", temp_json_file.name])
        return_code, stdout_text, stderr_text = run_variable_command("get", ["new_variable"])
        self.assertEqual(return_code, 0)
        self.assertEqual(stdout_text.strip(), "new_value")
        os.remove(temp_json_file.name)

        # Test error handling for non-existent variables
        return_code, stdout_text, stderr_text = run_variable_command("get", ["non_existent_variable"], check_return_code=False)
        self.assertNotEqual(return_code, 0)
        self.assertIn("Variable not found", stderr_text)

    def test_variables_delete(self):
        """Test 'airflow variables delete' command to remove variables"""
        # Set a test variable with variables set command
        run_variable_command("set", [TEST_VARIABLE_KEY, TEST_VARIABLE_VALUE])

        # Verify variable exists with variables get command
        return_code, stdout_text, stderr_text = run_variable_command("get", [TEST_VARIABLE_KEY])
        self.assertEqual(return_code, 0)

        # Delete the variable with variables delete command
        return_code, stdout_text, stderr_text = run_variable_command("delete", [TEST_VARIABLE_KEY])
        self.assertEqual(return_code, 0)

        # Verify variable no longer exists
        return_code, stdout_text, stderr_text = run_variable_command("get", [TEST_VARIABLE_KEY], check_return_code=False)
        self.assertNotEqual(return_code, 0)
        self.assertIn("Variable not found", stderr_text)

        # Test error handling for deleting non-existent variables
        return_code, stdout_text, stderr_text = run_variable_command("delete", ["non_existent_variable"], check_return_code=False)
        self.assertNotEqual(return_code, 0)
        self.assertIn("Variable not found", stderr_text)

    def test_variables_import_export(self):
        """Test 'airflow variables import/export' commands for bulk operations"""
        # Create a temporary JSON file with test variables
        temp_json_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        test_variables = {"var1": "value1", "var2": "value2"}
        json.dump(test_variables, temp_json_file)
        temp_json_file.close()

        # Import variables using variables import command
        return_code, stdout_text, stderr_text = run_variable_command("import", [temp_json_file.name])
        self.assertEqual(return_code, 0)

        # Verify variables were imported with variables list
        return_code, stdout_text, stderr_text = run_variable_command("list", [])
        self.assertIn("var1", stdout_text)
        self.assertIn("var2", stdout_text)

        # Export variables to another file using variables export
        temp_export_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        temp_export_file.close()
        return_code, stdout_text, stderr_text = run_variable_command("export", [temp_export_file.name])
        self.assertEqual(return_code, 0)

        # Verify exported file contains expected variables
        with open(temp_export_file.name, 'r') as export_file:
            exported_variables = json.load(export_file)
        self.assertIn("var1", exported_variables)
        self.assertIn("var2", exported_variables)

        # Clean up temporary files
        os.remove(temp_json_file.name)
        os.remove(temp_export_file.name)

    def test_connections_list(self):
        """Test 'airflow connections list' command to list connections"""
        # Set up test connection using connections add command
        run_connection_command("add", ["test_conn", "--conn-type", "http", "--conn-host", "example.com"])

        # Run connections list command
        return_code, stdout_text, stderr_text = run_connection_command("list", [])

        # Verify command returns success code
        self.assertEqual(return_code, 0)

        # Verify output contains test connection
        self.assertIn("test_conn", stdout_text)
        self.assertIn("http", stdout_text)
        self.assertIn("example.com", stdout_text)

        # Test different output formats (e.g., --output json)
        return_code, stdout_text, stderr_text = run_connection_command("list", ["--output", "json"])
        self.assertEqual(return_code, 0)
        self.assertIn("test_conn", stdout_text)
        self.assertIn("http", stdout_text)
        self.assertIn("example.com", stdout_text)

    def test_connections_add_delete(self):
        """Test 'airflow connections add/delete' commands to manage connections"""
        # Add a test connection with connections add command
        run_connection_command("add", ["test_conn", "--conn-type", "http", "--conn-host", "example.com"])

        # Verify connection exists with connections get command
        return_code, stdout_text, stderr_text = run_connection_command("list", [])
        self.assertIn("test_conn", stdout_text)

        # Delete the connection with connections delete command
        return_code, stdout_text, stderr_text = run_connection_command("delete", ["test_conn"])
        self.assertEqual(return_code, 0)

        # Verify connection no longer exists
        return_code, stdout_text, stderr_text = run_connection_command("list", [], check_return_code=False)
        self.assertNotIn("test_conn", stdout_text)

        # Test error handling for connection operations
        return_code, stdout_text, stderr_text = run_connection_command("delete", ["non_existent_conn"], check_return_code=False)
        self.assertNotEqual(return_code, 0)
        self.assertIn("Connection with `conn_id` `non_existent_conn` not found", stderr_text)

    def test_connections_import_export(self):
        """Test 'airflow connections import/export' commands for bulk operations"""
        # Create a temporary JSON file with test connections
        temp_json_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        test_connections = {"conn1": {"conn_type": "http", "conn_host": "example1.com"},
                            "conn2": {"conn_type": "postgres", "conn_host": "example2.com"}}
        json.dump(test_connections, temp_json_file)
        temp_json_file.close()

        # Import connections using connections import command
        return_code, stdout_text, stderr_text = run_connection_command("import", [temp_json_file.name])
        self.assertEqual(return_code, 0)

        # Verify connections were imported with connections list
        return_code, stdout_text, stderr_text = run_connection_command("list", [])
        self.assertIn("conn1", stdout_text)
        self.assertIn("conn2", stdout_text)

        # Export connections to another file using connections export
        temp_export_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        temp_export_file.close()
        return_code, stdout_text, stderr_text = run_connection_command("export", [temp_export_file.name])
        self.assertEqual(return_code, 0)

        # Verify exported file contains expected connections
        with open(temp_export_file.name, 'r') as export_file:
            exported_connections = json.load(export_file)
        self.assertIn("conn1", exported_connections)
        self.assertIn("conn2", exported_connections)

        # Clean up temporary files
        os.remove(temp_json_file.name)
        os.remove(temp_export_file.name)

    def test_pools_list(self):
        """Test 'airflow pools list' command to list worker pools"""
        # Set up test pool using pools set command
        run_pool_command("set", ["test_pool", "10", "test description"])

        # Run pools list command
        return_code, stdout_text, stderr_text = run_pool_command("list", [])

        # Verify command returns success code
        self.assertEqual(return_code, 0)

        # Verify output contains test pool
        self.assertIn("test_pool", stdout_text)
        self.assertIn("10", stdout_text)
        self.assertIn("test description", stdout_text)

        # Test different output formats (e.g., --output json)
        return_code, stdout_text, stderr_text = run_pool_command("list", ["--output", "json"])
        self.assertEqual(return_code, 0)
        self.assertIn("test_pool", stdout_text)
        self.assertIn("10", stdout_text)
        self.assertIn("test description", stdout_text)

    def test_pools_get_set(self):
        """Test 'airflow pools get/set' commands to manage worker pools"""
        # Set a test pool with pools set command
        run_pool_command("set", ["test_pool", "10", "test description"])

        # Get the pool with pools get command
        return_code, stdout_text, stderr_text = run_pool_command("get", ["test_pool"])

        # Verify retrieved pool matches what was set
        self.assertEqual(return_code, 0)
        self.assertIn("test_pool", stdout_text)
        self.assertIn("10", stdout_text)
        self.assertIn("test description", stdout_text)

        # Test error handling for pool operations
        return_code, stdout_text, stderr_text = run_pool_command("get", ["non_existent_pool"], check_return_code=False)
        self.assertNotEqual(return_code, 0)
        self.assertIn("Pool 'non_existent_pool' not found", stderr_text)

    def test_pools_delete(self):
        """Test 'airflow pools delete' command to remove worker pools"""
        # Set a test pool with pools set command
        run_pool_command("set", ["test_pool", "10", "test description"])

        # Verify pool exists with pools list command
        return_code, stdout_text, stderr_text = run_pool_command("list", [])
        self.assertIn("test_pool", stdout_text)

        # Delete the pool with pools delete command
        return_code, stdout_text, stderr_text = run_pool_command("delete", ["test_pool"])
        self.assertEqual(return_code, 0)

        # Verify pool no longer exists
        return_code, stdout_text, stderr_text = run_pool_command("list", [], check_return_code=False)
        self.assertNotIn("test_pool", stdout_text)

        # Test error handling for deleting default pools
        return_code, stdout_text, stderr_text = run_pool_command("delete", ["default_pool"], check_return_code=False)
        self.assertNotEqual(return_code, 0)
        self.assertIn("default_pool is a default pool and cannot be deleted", stderr_text)

    def test_pools_import_export(self):
        """Test 'airflow pools import/export' commands for bulk operations"""
        # Create a temporary JSON file with test pools
        temp_json_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        test_pools = [{"pool": "pool1", "slots": 5, "description": "desc1"},
                      {"pool": "pool2", "slots": 15, "description": "desc2"}]
        json.dump(test_pools, temp_json_file)
        temp_json_file.close()

        # Import pools using pools import command
        return_code, stdout_text, stderr_text = run_pool_command("import", [temp_json_file.name])
        self.assertEqual(return_code, 0)

        # Verify pools were imported with pools list
        return_code, stdout_text, stderr_text = run_pool_command("list", [])
        self.assertIn("pool1", stdout_text)
        self.assertIn("pool2", stdout_text)

        # Export pools to another file using pools export
        temp_export_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        temp_export_file.close()
        return_code, stdout_text, stderr_text = run_pool_command("export", [temp_export_file.name])
        self.assertEqual(return_code, 0)

        # Verify exported file contains expected pools
        with open(temp_export_file.name, 'r') as export_file:
            exported_pools = json.load(export_file)
        self.assertEqual(len(exported_pools), 2)
        self.assertTrue(any(pool["pool"] == "pool1" for pool in exported_pools))
        self.assertTrue(any(pool["pool"] == "pool2" for pool in exported_pools))

        # Clean up temporary files
        os.remove(temp_json_file.name)
        os.remove(temp_export_file.name)

    def test_invalid_config_command(self):
        """Test invalid config command to ensure proper error handling"""
        # Run invalid config command
        return_code, stdout_text, stderr_text = run_config_command("invalid_command", [], check_return_code=False)

        # Verify command returns error code
        self.assertNotEqual(return_code, 0)

        # Check error message is appropriate
        self.assertIn("No such command", stderr_text)

        # Ensure consistent error behavior across Airflow versions
        if is_airflow2():
            self.assertIn("config", stderr_text)
        else:
            self.assertIn("config", stderr_text)


class TestConfigCommandsAirflow2Compatibility(Airflow2CompatibilityTestMixin, unittest.TestCase):
    """Test case specifically targeting Airflow 2.X compatibility for configuration commands"""

    def __init__(self, *args, **kwargs):
        """Initialize the compatibility test case"""
        super().__init__(*args, **kwargs)
        self.original_env = None
        self.test_config_file = None

    def setUp(self):
        """Set up the test environment with Airflow 2.X compatibility focus"""
        # Call parent setUp to set up Airflow 2.X compatibility environment
        super().setUp()

        # Set up test environment with Airflow 2.X specific settings
        setup_test_environment()

        # Create test config file with Airflow 2.X compatible format
        test_config_values = {
            "core": {
                "executor": "SequentialExecutor",
                "sql_alchemy_conn": "sqlite:///:memory:"
            },
            "webserver": {
                "expose_config": "True"
            },
            "logging": {
                "logging_level": "INFO"
            }
        }
        self.test_config_file = create_test_config_file(test_config_values)

    def tearDown(self):
        """Clean up after each test"""
        # Reset the test environment
        reset_test_environment()

        # Remove temporary config file if it exists
        if self.test_config_file and os.path.exists(self.test_config_file):
            os.remove(self.test_config_file)

        # Call parent tearDown to clean up Airflow 2.X mocks
        super().tearDown()

    @unittest.skipUnless(is_airflow2(), "Test requires Airflow 2.X")
    def test_airflow2_config_list_format(self):
        """Test Airflow 2.X specific config list command output format changes"""
        # Skip test if not running with Airflow 2.X
        if not is_airflow2():
            self.skipTest("Test requires Airflow 2.X")

        # Run config list command with Airflow 2.X specific options
        return_code, stdout_text, stderr_text = run_config_command("list", [])

        # Verify Airflow 2.X specific config sections exist
        self.assertIn("[logging]", stdout_text)

        # Test Airflow 2.X specific output format options
        return_code, stdout_text, stderr_text = run_config_command("list", ["--output", "json"])
        self.assertEqual(return_code, 0)
        self.assertIn("logging", stdout_text)

    @unittest.skipUnless(is_airflow2(), "Test requires Airflow 2.X")
    def test_airflow2_variables_cli_changes(self):
        """Test Airflow 2.X specific variables command changes"""
        # Skip test if not running with Airflow 2.X
        if not is_airflow2():
            self.skipTest("Test requires Airflow 2.X")

        # Test Airflow 2.X format for variables commands
        run_variable_command("set", [TEST_VARIABLE_KEY, TEST_VARIABLE_VALUE])
        return_code, stdout_text, stderr_text = run_variable_command("get", [TEST_VARIABLE_KEY])
        self.assertEqual(return_code, 0)
        self.assertEqual(stdout_text.strip(), TEST_VARIABLE_VALUE)

        # Verify Airflow 2.X specific options (e.g., --deserialize-json)
        return_code, stdout_text, stderr_text = run_variable_command("get", [TEST_VARIABLE_KEY, "--deserialize-json"])
        self.assertEqual(return_code, 0)

        # Test Airflow 2.X specific output formats
        return_code, stdout_text, stderr_text = run_variable_command("list", ["--output", "json"])
        self.assertEqual(return_code, 0)
        self.assertIn(TEST_VARIABLE_KEY, stdout_text)

    @unittest.skipUnless(is_airflow2(), "Test requires Airflow 2.X")
    def test_airflow2_connections_cli_changes(self):
        """Test Airflow 2.X specific connections command changes"""
        # Skip test if not running with Airflow 2.X
        if not is_airflow2():
            self.skipTest("Test requires Airflow 2.X")

        # Test new connection types in Airflow 2.X
        # Test changes in connection parameters and URI formats
        # Verify provider-based connection handling
        pass

    @unittest.skipUnless(is_airflow2(), "Test requires Airflow 2.X")
    def test_airflow2_pools_cli_changes(self):
        """Test Airflow 2.X specific pools command changes"""
        # Skip test if not running with Airflow 2.X
        if not is_airflow2():
            self.skipTest("Test requires Airflow 2.X")

        # Test new pool command options in Airflow 2.X
        # Verify changes in pool management behavior
        # Test JSON output format changes
        pass

    @unittest.skipUnless(is_airflow2(), "Test requires Airflow 2.X")
    def test_airflow2_config_providers(self):
        """Test Airflow 2.X provider configuration commands"""
        # Skip test if not running with Airflow 2.X
        if not is_airflow2():
            self.skipTest("Test requires Airflow 2.X")

        # Test provider list/get commands
        # Verify Google Cloud provider configuration
        # Test provider-specific configuration options
        pass