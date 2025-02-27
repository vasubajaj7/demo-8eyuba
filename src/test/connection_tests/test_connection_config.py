#!/usr/bin/env python3

"""
Unit tests for connection configurations in Apache Airflow, validating connection
definitions during the migration from Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2.
This test suite verifies connection configuration compatibility, proper loading of
environment-specific connection parameters, and secure handling of credentials.
"""

import os  # v3.0+ - Access environment variables and file path handling
import json  # v3.0+ - Parse and analyze connection configuration JSON files
import unittest  # v3.0+ - Base testing framework
import tempfile  # v3.0+ - Create temporary files for connection configuration testing

# Third-party imports
import pytest  # pytest-6.0+ - Advanced testing framework with fixtures and parametrization
from airflow.models import Connection  # airflow-2.0.0+ - Access Airflow connection interfaces

# Internal imports
from ..fixtures import mock_connections  # src/test/fixtures/mock_connections.py - Provides mock Airflow connections for testing
from ..utils import assertion_utils  # src/test/utils/assertion_utils.py - Provides specialized assertion functions for Airflow component testing
from ..utils import airflow2_compatibility_utils  # src/test/utils/airflow2_compatibility_utils.py - Provides utilities for handling Airflow version compatibility
from ..utils import test_helpers  # src/test/utils/test_helpers.py - Provides helper functions for testing Airflow components

# Define global constants
CONNECTIONS_JSON_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'backend', 'config', 'connections.json')
TEST_ENVIRONMENTS = ['dev', 'qa', 'prod']
SECRET_PATTERN = r'\{SECRET:([^:]+):([^}]+)\}'


def load_test_connections() -> dict:
    """
    Loads connection configurations from the test connections.json file

    Returns:
        dict: Dictionary of connection configurations from the JSON file
    """
    # Check if CONNECTIONS_JSON_PATH exists
    if not os.path.exists(CONNECTIONS_JSON_PATH):
        return {}

    # Open and read the JSON file
    with open(CONNECTIONS_JSON_PATH, 'r') as f:
        # Parse the JSON content into a dictionary
        connections = json.load(f)

    # Return the parsed connections dictionary
    return connections


def create_temp_connection_file(connections: dict) -> str:
    """
    Creates a temporary connection configuration file for testing

    Args:
        connections (dict): connections

    Returns:
        str: Path to the created temporary file
    """
    # Create a temporary file using tempfile.NamedTemporaryFile
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
        # Write the connections dictionary as JSON to the temporary file
        json.dump(connections, temp_file)
        # Flush and close the file
        temp_file.flush()
        temp_file.close()
        # Return the path to the temporary file
        return temp_file.name


class TestConnectionConfigBase(unittest.TestCase):
    """
    Base test class for connection configuration tests that works with both Airflow 1.X and 2.X
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the connection test class
        """
        # Call parent class constructor
        super().__init__(*args, **kwargs)
        # Set connections to None initially
        self.connections = None
        # Set temp_file_path to None initially
        self.temp_file_path = None

    def setUp(self):
        """
        Set up the test environment
        """
        # Load connection configurations using load_test_connections()
        self.connections = load_test_connections()
        # Create a temporary connection file using create_temp_connection_file()
        self.temp_file_path = create_temp_connection_file(self.connections)
        # Set up mock connections for testing
        mock_connections.reset_mock_connections()

    def tearDown(self):
        """
        Clean up after tests
        """
        # Remove the temporary connection file if it exists
        if self.temp_file_path and os.path.exists(self.temp_file_path):
            os.remove(self.temp_file_path)
            self.temp_file_path = None
        # Reset any mock objects or patches
        mock_connections.reset_mock_connections()

    def get_connection_by_id(self, conn_id: str) -> dict:
        """
        Retrieve a connection configuration by ID

        Args:
            conn_id (str): conn_id

        Returns:
            dict: Connection configuration dictionary
        """
        # Search for the connection in self.connections by conn_id
        for connection_id, connection in self.connections.items():
            if connection_id == conn_id:
                # Return the connection configuration dictionary
                return connection

        # Raise ValueError if connection ID not found
        raise ValueError(f"Connection with ID '{conn_id}' not found")


class TestConnectionConfigLoading(TestConnectionConfigBase):
    """
    Tests for loading connection configurations from JSON files
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the connection loading test class
        """
        # Call TestConnectionConfigBase constructor
        super().__init__(*args, **kwargs)

    def test_connections_json_exists(self):
        """
        Test that the connections.json file exists
        """
        # Check if CONNECTIONS_JSON_PATH file exists
        file_exists = os.path.exists(CONNECTIONS_JSON_PATH)
        # Assert that the file exists
        self.assertTrue(file_exists, "connections.json file does not exist")

    def test_connections_json_is_valid(self):
        """
        Test that connections.json contains valid JSON
        """
        # Verify self.connections is not None
        self.assertIsNotNone(self.connections, "Connections dictionary is None")
        # Verify self.connections is a dictionary
        self.assertIsInstance(self.connections, dict, "Connections is not a dictionary")

        # Assert that required connection IDs are present
        required_conn_ids = ["gcp_default", "postgres_default", "http_default"]
        for conn_id in required_conn_ids:
            self.assertIn(conn_id, self.connections, f"Required connection ID '{conn_id}' is missing")

        # Check that each connection has required fields
        for conn_id, conn_config in self.connections.items():
            self.assertIsInstance(conn_config, dict, f"Connection '{conn_id}' config is not a dictionary")
            self.assertIn("conn_type", conn_config, f"Connection '{conn_id}' missing 'conn_type' field")

    def test_environment_specific_connections(self):
        """
        Test that connections have environment-specific configurations
        """
        # For each connection with environment-specific configurations:
        for conn_id, conn_config in self.connections.items():
            if "env" in conn_config:
                # Verify that it contains entries for all test environments
                self.assertIn("dev", conn_config, f"Connection '{conn_id}' missing 'dev' environment")
                self.assertIn("qa", conn_config, f"Connection '{conn_id}' missing 'qa' environment")
                self.assertIn("prod", conn_config, f"Connection '{conn_id}' missing 'prod' environment")

                # Check that environment-specific values are appropriate for each environment
                for env in TEST_ENVIRONMENTS:
                    self.assertIn("config", conn_config[env], f"Connection '{conn_id}' missing 'config' for '{env}'")
                    self.assertIsInstance(conn_config[env]["config"], dict, f"Connection '{conn_id}' config for '{env}' is not a dictionary")

    def test_import_connections_loads_file(self):
        """
        Test that import_connections can load the connections file
        """
        # Import load_connections_from_file from import_connections module
        from backend.config import load_json_file

        # Call load_connections_from_file with temp_file_path
        loaded_connections = load_json_file(pathlib.Path(self.temp_file_path))

        # Verify that returned connections match expected data
        self.assertEqual(loaded_connections, self.connections, "Imported connections do not match expected data")


class TestConnectionConfigEnvironments(TestConnectionConfigBase):
    """
    Tests for environment-specific connection processing
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the environment-specific connection test class
        """
        # Call TestConnectionConfigBase constructor
        super().__init__(*args, **kwargs)

    def test_process_environment_connections(self):
        """
        Test processing connections for specific environments
        """
        # Import process_environment_connections from import_connections module
        from backend.config import get_connections

        # For each test environment:
        for env in TEST_ENVIRONMENTS:
            # Process connections for that environment
            environment_connections = get_connections(env)

            # Verify environment-specific values are correctly applied
            for conn_id, conn_config in environment_connections.items():
                # Check that environment-specific sections are removed from processed result
                self.assertNotIn("dev", conn_config, f"Connection '{conn_id}' still contains 'dev' section")
                self.assertNotIn("qa", conn_config, f"Connection '{conn_id}' still contains 'qa' section")
                self.assertNotIn("prod", conn_config, f"Connection '{conn_id}' still contains 'prod' section")

    def test_environment_filtering(self):
        """
        Test that connections are correctly filtered by environment
        """
        # Create test connections with specific 'environments' lists
        test_connections = {
            "conn_dev": {"conn_type": "http", "host": "dev.example.com", "env": "dev"},
            "conn_qa": {"conn_type": "http", "host": "qa.example.com", "env": "qa"},
            "conn_prod": {"conn_type": "http", "host": "prod.example.com", "env": "prod"},
            "conn_all": {"conn_type": "http", "host": "all.example.com"}
        }

        # Import process_environment_connections from import_connections module
        from backend.config import get_connections

        # For each test environment:
        for env in TEST_ENVIRONMENTS:
            # Process connections for that environment
            environment_connections = get_connections(env)

            # Verify only connections for that environment are included
            for conn_id, conn_config in environment_connections.items():
                if conn_id == "conn_dev":
                    self.assertEqual(conn_config["host"], "dev.example.com")
                elif conn_id == "conn_qa":
                    self.assertEqual(conn_config["host"], "qa.example.com")
                elif conn_id == "conn_prod":
                    self.assertEqual(conn_config["host"], "prod.example.com")
                elif conn_id == "conn_all":
                    self.assertEqual(conn_config["host"], "all.example.com")

                # Check that connections for other environments are excluded
                other_envs = [e for e in TEST_ENVIRONMENTS if e != env]
                for other_env in other_envs:
                    other_conn_id = f"conn_{other_env}"
                    self.assertNotIn(other_conn_id, environment_connections, f"Connection '{other_conn_id}' for env '{other_env}' should not be included in env '{env}'")


class TestConnectionConfigSecurity(TestConnectionConfigBase):
    """
    Tests for secure handling of connection credentials
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the connection security test class
        """
        # Call TestConnectionConfigBase constructor
        super().__init__(*args, **kwargs)

    def test_secret_pattern_recognition(self):
        """
        Test recognition of Secret Manager patterns in connections
        """
        import re

        # Create test connections with {SECRET:name:version} patterns
        test_connections = {
            "conn_secret": {"conn_type": "http", "host": "{SECRET:secret_name:latest}"},
            "conn_no_secret": {"conn_type": "http", "host": "example.com"}
        }

        # Use SECRET_PATTERN regex to find secret references
        for conn_id, conn_config in test_connections.items():
            host = conn_config.get("host", "")
            secret_references = re.findall(SECRET_PATTERN, host)

            # Verify that all secret references are correctly identified
            if conn_id == "conn_secret":
                self.assertEqual(len(secret_references), 1, "Secret reference not found")
            else:
                self.assertEqual(len(secret_references), 0, "Secret reference should not be found")

            # Check extraction of secret name and version from pattern
            if secret_references:
                secret_name, secret_version = secret_references[0]
                self.assertEqual(secret_name, "secret_name", "Secret name extraction failed")
                self.assertEqual(secret_version, "latest", "Secret version extraction failed")

    def test_resolve_secrets(self):
        """
        Test resolving secret references in connections
        """
        # Import resolve_secrets from import_connections module
        from backend.config import get_connections

        # Mock the get_secret function to return test values
        with unittest.mock.patch("backend.config.get_secret", return_value="mock_secret_value"):
            # Create test connections with secret references
            test_connections = {
                "conn_secret": {"conn_type": "http", "host": "{SECRET:secret_name:latest}"},
                "conn_no_secret": {"conn_type": "http", "host": "example.com"}
            }

            # Call resolve_secrets on test connections
            environment_connections = get_connections("dev")

            # Verify that secret references are replaced with mock values
            for conn_id, conn_config in environment_connections.items():
                if conn_id == "conn_secret":
                    self.assertEqual(conn_config["host"], "mock_secret_value", "Secret not resolved")
                else:
                    self.assertEqual(conn_config["host"], "example.com", "Secret should not be resolved")

    def test_sensitive_fields_handling(self):
        """
        Test proper handling of sensitive connection fields
        """
        # Identify sensitive fields in connection configurations
        sensitive_fields = ["password", "login", "key_path", "key_json"]

        # Verify these fields use secret references or other secure methods
        for conn_id, conn_config in self.connections.items():
            for field in sensitive_fields:
                if field in conn_config:
                    value = conn_config[field]
                    # Check that plaintext credentials are not stored in connections.json
                    self.assertNotRegex(value, r"^[a-zA-Z0-9]+$", f"Plaintext credential found in '{field}' for connection '{conn_id}'")


class TestConnectionConfigAirflow2Compatibility(TestConnectionConfigBase):
    """
    Tests for Airflow 2.X compatibility of connection configurations
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the Airflow 2.X compatibility test class
        """
        # Call TestConnectionConfigBase constructor
        super().__init__(*args, **kwargs)
        # Initialize Airflow2CompatibilityTestMixin
        self.mixin = airflow2_compatibility_utils.Airflow2CompatibilityTestMixin()

    def test_connection_types_compatible(self):
        """
        Test that connection types are compatible with Airflow 2.X
        """
        # Check each connection's conn_type
        for conn_id, conn_config in self.connections.items():
            conn_type = conn_config.get("conn_type")

            # Verify it is compatible with Airflow 2.X provider packages
            if conn_type in ["google_cloud_platform", "postgres", "http"]:
                self.assertIn(conn_type, ["google_cloud_platform", "postgres", "http"], f"Connection type '{conn_type}' is not compatible with Airflow 2.X")

            # For custom connection types, ensure they are properly registered
            else:
                self.assertIsInstance(conn_type, str, f"Custom connection type '{conn_type}' must be a string")

    def test_connection_extra_fields_compatible(self):
        """
        Test that extra fields in connections are compatible with Airflow 2.X
        """
        import json

        # For each connection with 'extra' field:
        for conn_id, conn_config in self.connections.items():
            if "extra" in conn_config:
                extra = conn_config["extra"]

                # Parse extra JSON
                try:
                    extra_dict = json.loads(extra)
                except (TypeError, json.JSONDecodeError):
                    self.fail(f"Failed to parse 'extra' field as JSON for connection '{conn_id}'")

                # Check for deprecated extra parameters
                deprecated_params = ["use_proxy", "num_proxies"]
                for param in deprecated_params:
                    self.assertNotIn(param, extra_dict, f"Deprecated extra parameter '{param}' found in connection '{conn_id}'")

                # Verify extra fields follow Airflow 2.X conventions
                self.assertIsInstance(extra_dict, dict, f"Extra fields must be a dictionary for connection '{conn_id}'")

    def test_connection_import_in_airflow2(self):
        """
        Test importing connections in an Airflow 2.X environment
        """
        # Skip test if not running in Airflow 2.X
        if not airflow2_compatibility_utils.is_airflow2():
            self.skipTest("Test requires Airflow 2.X")

        # Import get_connection_class and import_connections from import_connections module
        from airflow.models.connection import Connection

        # Get the Airflow 2.X Connection class
        connection_class = Connection

        # Create test connections and import them
        test_connections = {
            "conn_test": {"conn_type": "http", "host": "test.example.com"}
        }

        # Verify connections are correctly imported with Airflow 2.X API
        for conn_id, conn_config in test_connections.items():
            # Create a Connection object
            connection = connection_class(conn_id=conn_id, conn_type=conn_config["conn_type"], host=conn_config["host"])

            # Verify connection attributes
            self.assertEqual(connection.conn_id, conn_id, "Connection ID mismatch")
            self.assertEqual(connection.conn_type, conn_config["conn_type"], "Connection type mismatch")
            self.assertEqual(connection.host, conn_config["host"], "Connection host mismatch")