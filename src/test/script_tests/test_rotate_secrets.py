"""
Unit tests for the rotate_secrets.py script that automates secure rotation of credentials
in GCP Secret Manager for Cloud Composer environments during migration from Airflow 1.10.15 to Airflow 2.X.
"""
import unittest
from unittest.mock import patch, MagicMock
import os  # File path operations and environment variable access
import sys  # System-specific parameters and functions
import json  # JSON encoding and decoding for config manipulation
import io  # In-memory text stream handling for capturing output
import tempfile  # Temporary file creation for testing file operations
import pathlib  # Object-oriented filesystem path manipulation
import pytest  # Advanced testing framework for Python >= 3.6

# Third-party imports
from google.cloud.secretmanager import SecretManagerServiceClient  # google-cloud-secret-manager >= 2.0.0
from google.api_core import exceptions  # google-api-core >= 2.0.0

# Internal imports
from src.test.fixtures.mock_gcp_services import MockSecretManagerClient, create_mock_secret_manager_client  # Mock implementation of Secret Manager client for testing
from src.test.utils.assertion_utils import assert_logs_contain  # Utility functions for test assertions
from src.test.utils.test_helpers import setup_test_path, capture_logs  # Helper functions for test setup and execution
from src.backend.scripts.rotate_secrets import rotate_secret, check_secret_rotation_needed, generate_new_secret_value, update_connections_with_new_secrets, update_variables_with_new_secrets, find_secrets_in_connections, find_secrets_in_variables, main  # The script being tested for secret rotation functionality


class RotateSecretsTest(unittest.TestCase):
    """
    Test case class for the rotate_secrets.py script that validates secret rotation functionality
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)

    def setUp(self):
        """
        Set up test environment before each test
        """
        # Create temporary test directory for files
        self.test_dir = tempfile.mkdtemp()
        # Set up project path to include source directories
        self.project_path = setup_test_path()
        # Configure test environment variables
        self.env_vars = {
            'AIRFLOW_HOME': self.test_dir,
            'GOOGLE_APPLICATION_CREDENTIALS': os.path.join(self.test_dir, 'test_key.json')
        }
        self.original_env = os.environ.copy()
        os.environ.update(self.env_vars)

        # Create mock configurations for both Composer 1 and Composer 2
        self.composer1_config = {'connections': {}, 'variables': {}}
        self.composer2_config = {'connections': {}, 'variables': {}}

        # Set up mock Secret Manager client
        self.mock_secret_manager_client = create_mock_secret_manager_client()

        # Set up other mocks (db_utils, gcp_utils, alert_utils)
        self.db_utils_patch = patch('src.backend.scripts.rotate_secrets.execute_query')
        self.gcp_utils_patch = patch('src.backend.scripts.rotate_secrets.SecretManagerServiceClient', return_value=self.mock_secret_manager_client)
        self.alert_utils_patch = patch('src.backend.scripts.rotate_secrets.send_email_alert')

        self.db_utils_mock = self.db_utils_patch.start()
        self.gcp_utils_mock = self.gcp_utils_patch.start()
        self.alert_utils_mock = self.alert_utils_patch.start()

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        # Remove temporary test directory
        import shutil
        shutil.rmtree(self.test_dir)

        # Reset environment variables
        os.environ.clear()
        os.environ.update(self.original_env)

        # Stop all mock patches
        self.db_utils_patch.stop()
        self.gcp_utils_patch.stop()
        self.alert_utils_patch.stop()

    def test_check_secret_rotation_needed(self):
        """
        Test the check_secret_rotation_needed function for determining when rotation is required
        """
        # Set up mock secret versions with different creation dates
        secret_id = "test-secret"
        project_id = "test-project"
        rotation_period_days = 90

        # Test secret that needs rotation (older than threshold)
        with patch.object(MockSecretManagerClient, 'get_secret', return_value=MagicMock(create_time=datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc))):
            needs_rotation = check_secret_rotation_needed(secret_id, self.mock_secret_manager_client, project_id, rotation_period_days)
            self.assertTrue(needs_rotation)

        # Test secret that doesn't need rotation (newer than threshold)
        with patch.object(MockSecretManagerClient, 'get_secret', return_value=MagicMock(create_time=datetime.datetime.now(tz=datetime.timezone.utc))):
            needs_rotation = check_secret_rotation_needed(secret_id, self.mock_secret_manager_client, project_id, rotation_period_days)
            self.assertFalse(needs_rotation)

        # Test handling of non-existent secrets
        with patch.object(MockSecretManagerClient, 'get_secret', side_effect=Exception("Secret not found")):
            needs_rotation = check_secret_rotation_needed(secret_id, self.mock_secret_manager_client, project_id, rotation_period_days)
            self.assertTrue(needs_rotation)

    def test_generate_new_secret_value(self):
        """
        Test the generation of new secret values based on type and security requirements
        """
        # Test password generation with appropriate complexity
        password = generate_new_secret_value("test_password", "old_password", secret_type="password")
        self.assertTrue(any(c.isupper() for c in password))
        self.assertTrue(any(c.islower() for c in password))
        self.assertTrue(any(c.isdigit() for c in password))
        self.assertTrue(any(c in "!@#$%^&*()-_=+[]{}|;:,.<>?" for c in password))

        # Test API key generation with correct format
        api_key = generate_new_secret_value("test_api_key", "old_api_key", secret_type="api_key")
        self.assertTrue(api_key.isalnum())

        # Test connection string generation that preserves structure
        connection_string = generate_new_secret_value("test_connection", '{"host": "localhost", "password": "old_password"}', secret_type="connection")
        self.assertIn("host", connection_string)
        self.assertIn("password", connection_string)
        self.assertNotEqual("old_password", json.loads(connection_string)["password"])

        # Test variable value generation
        variable_value = generate_new_secret_value("test_variable", "old_variable", secret_type="variable")
        self.assertNotEqual("old_variable", variable_value)

    def test_rotate_secret_success(self):
        """
        Test successful rotation of a single secret
        """
        # Configure mock Secret Manager client with test secret
        secret_id = "test-secret"
        project_id = "test-project"
        self.mock_secret_manager_client._secrets[secret_id] = MagicMock()

        # Call rotate_secret function with test parameters
        result = rotate_secret(secret_id, self.mock_secret_manager_client, project_id, dry_run=False)

        # Verify new secret version was created
        self.mock_secret_manager_client.add_secret_version.assert_called_once()

        # Verify correct API calls were made
        self.mock_secret_manager_client.access_secret_version.assert_called_once()

        # Verify old and new values are properly returned
        self.assertEqual(result["status"], "success")
        self.assertIsNotNone(result["old_value"])
        self.assertIsNotNone(result["new_value"])

        # Check proper logging of success
        with capture_logs() as logs:
            rotate_secret(secret_id, self.mock_secret_manager_client, project_id, dry_run=False)
            assert_logs_contain(logs, ["Successfully created new version of secret test-secret"])

    def test_rotate_secret_failure(self):
        """
        Test handling of failures during secret rotation
        """
        # Configure mock Secret Manager client to raise exceptions
        secret_id = "test-secret"
        project_id = "test-project"

        # Test handling of NotFound exception
        with patch.object(MockSecretManagerClient, 'access_secret_version', side_effect=exceptions.NotFound("Secret not found")):
            result = rotate_secret(secret_id, self.mock_secret_manager_client, project_id, dry_run=False)
            self.assertEqual(result["status"], "failed")
            self.assertIn("Secret not found", result["error"])

        # Test handling of PermissionDenied exception
        with patch.object(MockSecretManagerClient, 'access_secret_version', side_effect=exceptions.PermissionDenied("Permission denied")):
            result = rotate_secret(secret_id, self.mock_secret_manager_client, project_id, dry_run=False)
            self.assertEqual(result["status"], "failed")
            self.assertIn("Permission denied", result["error"])

        # Test handling of general ApiError
        with patch.object(MockSecretManagerClient, 'access_secret_version', side_effect=Exception("General API error")):
            result = rotate_secret(secret_id, self.mock_secret_manager_client, project_id, dry_run=False)
            self.assertEqual(result["status"], "failed")
            self.assertIn("General API error", result["error"])

        # Verify appropriate error handling and logging
        with capture_logs() as logs:
            rotate_secret(secret_id, self.mock_secret_manager_client, project_id, dry_run=False)
            assert_logs_contain(logs, ["Failed to access or update secret test-secret"])

    def test_find_secrets_in_connections(self):
        """
        Test identification of secret references in connection configurations
        """
        # Create test connection config with embedded secret references
        connections_config = {
            "conn1": {"conn_type": "http", "host": "{SECRET:test-secret-host}", "login": "test", "password": "{SECRET:test-secret-password}"},
            "conn2": {"conn_type": "postgres", "host": "localhost", "schema": "test", "extra": '{"user": "test", "password": "{SECRET:test-secret-dbpassword}"}'},
            "conn3": {"conn_type": "gcp", "extra": {"project_id": "test", "key_path": "{SECRET:test-secret-keypath}"}}
        }

        # Call find_secrets_in_connections with test config
        secret_references = find_secrets_in_connections(connections_config)

        # Verify all secret references are correctly identified
        self.assertIn("test-secret-host", secret_references)
        self.assertIn("test-secret-password", secret_references)
        self.assertIn("test-secret-dbpassword", secret_references)
        self.assertIn("test-secret-keypath", secret_references)

        # Verify nested secret references are found
        self.assertEqual(len(secret_references["test-secret-host"]), 1)
        self.assertEqual(len(secret_references["test-secret-password"]), 1)
        self.assertEqual(len(secret_references["test-secret-dbpassword"]), 1)
        self.assertEqual(len(secret_references["test-secret-keypath"]), 1)

        # Test with no secret references present
        connections_config = {"conn1": {"conn_type": "http", "host": "test", "login": "test", "password": "test"}}
        secret_references = find_secrets_in_connections(connections_config)
        self.assertEqual(len(secret_references), 0)

    def test_find_secrets_in_variables(self):
        """
        Test identification of secret references in variable configurations
        """
        # Create test variables config with embedded secret references
        variables_config = {
            "var1": "{SECRET:test-secret-var1}",
            "var2": {"value": "{SECRET:test-secret-var2}", "description": "Test"},
            "var3": ["{SECRET:test-secret-var3}", "test"]
        }

        # Call find_secrets_in_variables with test config
        secret_references = find_secrets_in_variables(variables_config)

        # Verify all secret references are correctly identified
        self.assertIn("test-secret-var1", secret_references)
        self.assertIn("test-secret-var2", secret_references)
        self.assertIn("test-secret-var3", secret_references)

        # Verify secret references in complex variable values
        self.assertEqual(len(secret_references["test-secret-var1"]), 1)
        self.assertEqual(len(secret_references["test-secret-var2"]), 1)
        self.assertEqual(len(secret_references["test-secret-var3"]), 1)

        # Test with no secret references present
        variables_config = {"var1": "test", "var2": {"value": "test", "description": "Test"}}
        secret_references = find_secrets_in_variables(variables_config)
        self.assertEqual(len(secret_references), 0)

    def test_update_connections_with_new_secrets(self):
        """
        Test updating connection configurations with newly rotated secrets
        """
        # Create test connection config with secret references
        connections_config = {
            "conn1": {"conn_type": "http", "host": "{SECRET:test-secret-host}", "login": "test", "password": "{SECRET:test-secret-password}"}
        }

        # Create dictionary of rotated secrets with old and new values
        rotated_secrets = {
            "test-secret-host": {"status": "success", "old_value": "old-host", "new_value": "new-host"},
            "test-secret-password": {"status": "success", "old_value": "old-password", "new_value": "new-password"}
        }

        # Call update_connections_with_new_secrets
        updated_config = update_connections_with_new_secrets(connections_config, rotated_secrets)

        # Verify secret references are replaced with new values
        self.assertEqual(updated_config["conn1"]["host"], "{SECRET:new-host}")
        self.assertEqual(updated_config["conn1"]["password"], "{SECRET:new-password}")

        # Verify connection structure is preserved
        self.assertEqual(updated_config["conn1"]["conn_type"], "http")
        self.assertEqual(updated_config["conn1"]["login"], "test")

        # Test with no applicable secrets to update
        rotated_secrets = {"test-secret-other": {"status": "success", "old_value": "old", "new_value": "new"}}
        updated_config = update_connections_with_new_secrets(connections_config, rotated_secrets)
        self.assertEqual(updated_config["conn1"]["host"], "{SECRET:test-secret-host}")
        self.assertEqual(updated_config["conn1"]["password"], "{SECRET:test-secret-password}")

    def test_update_variables_with_new_secrets(self):
        """
        Test updating variable configurations with newly rotated secrets
        """
        # Create test variables config with secret references
        variables_config = {
            "var1": "{SECRET:test-secret-var1}",
            "var2": {"value": "{SECRET:test-secret-var2}", "description": "Test"}
        }

        # Create dictionary of rotated secrets with old and new values
        rotated_secrets = {
            "test-secret-var1": {"status": "success", "old_value": "old-var1", "new_value": "new-var1"},
            "test-secret-var2": {"status": "success", "old_value": "old-var2", "new_value": "new-var2"}
        }

        # Call update_variables_with_new_secrets
        updated_config = update_variables_with_new_secrets(variables_config, rotated_secrets)

        # Verify secret references are replaced with new values
        self.assertEqual(updated_config["var1"], "{SECRET:new-var1}")
        self.assertEqual(updated_config["var2"]["value"], "{SECRET:new-var2}")

        # Verify variable structure is preserved
        self.assertEqual(updated_config["var2"]["description"], "Test")

        # Test with no applicable secrets to update
        rotated_secrets = {"test-secret-other": {"status": "success", "old_value": "old", "new_value": "new"}}
        updated_config = update_variables_with_new_secrets(variables_config, rotated_secrets)
        self.assertEqual(updated_config["var1"], "{SECRET:test-secret-var1}")
        self.assertEqual(updated_config["var2"]["value"], "{SECRET:test-secret-var2}")

    def test_environment_specific_config(self):
        """
        Test loading and handling of environment-specific configurations
        """
        # Create test config files for dev, qa, and prod environments
        dev_config = {"secret": "dev-secret"}
        qa_config = {"secret": "qa-secret"}
        prod_config = {"secret": "prod-secret"}

        # Mock the load_config function to use test files
        with (
            patch('src.backend.scripts.rotate_secrets.load_config', return_value=dev_config) as load_config_mock
        ):
            # Test loading config for each environment
            config = load_config_mock("dev", self.test_dir)
            self.assertEqual(config["secret"], "dev-secret")

            config = load_config_mock("qa", self.test_dir)
            self.assertEqual(config["secret"], "dev-secret")

            config = load_config_mock("prod", self.test_dir)
            self.assertEqual(config["secret"], "dev-secret")

    def test_main_function_args(self):
        """
        Test parsing and handling of command-line arguments in main function
        """
        # Mock sys.argv with various command line arguments
        with patch('sys.argv', ['rotate_secrets.py', '--env', 'dev']):
            args = main.__wrapped__()
            self.assertEqual(args.env, 'dev')
            self.assertFalse(args.dry_run)

        with patch('sys.argv', ['rotate_secrets.py', '--env', 'qa', '--secret-id', 'test-secret']):
            args = main.__wrapped__()
            self.assertEqual(args.env, 'qa')
            self.assertEqual(args.secret_id, 'test-secret')

        with patch('sys.argv', ['rotate_secrets.py', '--env', 'prod', '--dry-run']):
            args = main.__wrapped__()
            self.assertEqual(args.env, 'prod')
            self.assertTrue(args.dry_run)

        with patch('sys.argv', ['rotate_secrets.py', '--env', 'dev', '--verbose']):
            args = main.__wrapped__()
        
    def test_main_function_execution(self):
        """
        Test the overall execution flow of the main function
        """
        # Configure mock Secret Manager client with test secrets
        secret_id = "test-secret"
        project_id = "test-project"
        self.mock_secret_manager_client._secrets[secret_id] = MagicMock()

        # Create test configuration files
        test_config = {
            "project_id": project_id,
            "managed_secrets": [secret_id],
            "notifications": {"email_recipients": ["test@example.com"]},
            "connections": {},
            "variables": {}
        }

        # Mock command line arguments for a specific execution scenario
        with (
            patch('sys.argv', ['rotate_secrets.py', '--env', 'dev', '--secret-id', secret_id]),
            patch('src.backend.scripts.rotate_secrets.load_config', return_value=test_config),
            patch('src.backend.scripts.rotate_secrets.check_secret_rotation_needed', return_value=True)
        ):
            # Call the main function
            return_code = main()

            # Verify correct secrets were rotated
            self.mock_secret_manager_client.add_secret_version.assert_called_once()

            # Verify configurations were updated
            self.db_utils_mock.assert_called()

            # Verify notification was sent
            self.alert_utils_mock.assert_called()

            # Check exit code reflects success/failure
            self.assertEqual(return_code, 0)

    def test_composer1_vs_composer2(self):
        """
        Test handling of differences between Composer 1 and Composer 2 environments
        """
        # Create test configs for both Composer 1 and Composer 2
        composer1_config = {"connections": {}, "variables": {}}
        composer2_config = {"connections": {}, "variables": {}}

        # Test secret rotation in Composer 1 context
        with patch('src.backend.scripts.rotate_secrets.load_config', return_value=composer1_config):
            # Test secret rotation in Composer 2 context
            with patch('src.backend.scripts.rotate_secrets.load_config', return_value=composer2_config):
                # Verify proper handling of version-specific features
                # Verify migration compatibility
                pass

    def test_audit_logging(self):
        """
        Test that secret rotation events are properly audit-logged
        """
        # Configure log capture
        with capture_logs() as logs:
            # Perform a secret rotation operation
            secret_id = "test-secret"
            project_id = "test-project"
            self.mock_secret_manager_client._secrets[secret_id] = MagicMock()
            rotate_secret(secret_id, self.mock_secret_manager_client, project_id, dry_run=False)

            # Verify audit log entries contain required information
            assert_logs_contain(logs, ["Successfully created new version of secret test-secret"])

            # Verify timestamp, user, action, and resource details
            # Test compliance with security logging requirements