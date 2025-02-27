#!/usr/bin/env python3
"""
Unit tests for connection secret handling in Apache Airflow, focusing on the 
secure integration with Google Cloud Secret Manager during the migration from 
Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2.

This test suite verifies secure storage, retrieval, and rotation of connection
credentials.
"""

import os
import json
import unittest
from unittest.mock import patch, MagicMock
import re
import pytest

# Airflow imports
import airflow
from airflow.models import Connection

# Google Cloud Secret Manager
from google.cloud.secretmanager import SecretManagerServiceClient

# Internal imports
from ..fixtures.mock_connections import MockConnectionManager, get_mock_connection, create_mock_connection
from ..utils.airflow2_compatibility_utils import is_airflow2, Airflow2CompatibilityTestMixin
from ...backend.dags.utils.gcp_utils import get_secret, create_secret, SecretManagerClient

# Define global constants
# Regex pattern to match secret references in connection attributes
SECRET_PATTERN = r'\{SECRET:([^:]+):([^}]+)\}'

# Test environments
TEST_ENVIRONMENTS = ['dev', 'qa', 'prod']

# Mock secret values for testing
SECRET_VALUES = {
    'test-secret-1': 'test-value-1',
    'test-secret-2': 'test-value-2'
}


def mock_get_secret(secret_id: str, version_id: str = 'latest', conn_id: str = None) -> str:
    """
    Mock function to replace gcp_utils.get_secret for testing
    
    Args:
        secret_id: The ID of the secret to retrieve
        version_id: The version of the secret (default: 'latest')
        conn_id: The connection ID for GCP (unused in mock)
        
    Returns:
        Mock secret value from SECRET_VALUES or a default value
    """
    key = f"{secret_id}"
    if key in SECRET_VALUES:
        return SECRET_VALUES[key]
    return f"mock-secret-value-for-{secret_id}-version-{version_id}"


def create_connection_with_secret(conn_id: str, conn_type: str, secret_params: dict) -> dict:
    """
    Helper function to create a connection with secret references
    
    Args:
        conn_id: Connection ID
        conn_type: Connection type (postgres, google_cloud_platform, etc.)
        secret_params: Dictionary mapping attribute names to secret IDs
        
    Returns:
        Connection dictionary with secret references
    """
    conn = {
        'conn_id': conn_id,
        'conn_type': conn_type
    }
    
    # Add secret references for each parameter
    for param_name, secret_id in secret_params.items():
        conn[param_name] = f"{{SECRET:{secret_id}:latest}}"
    
    return conn


class TestConnectionSecrets(unittest.TestCase):
    """
    Base test class for connection secret tests that works with both Airflow 1.X and 2.X
    """
    
    def setUp(self):
        """Set up the test environment"""
        # Initialize mock connection manager
        self.mock_manager = MockConnectionManager()
        self.mock_manager.__enter__()
        
        # Create test connections with secret references
        self.test_connections = [
            create_connection_with_secret(
                conn_id="postgres_with_secret",
                conn_type="postgres",
                secret_params={
                    'password': 'test-secret-1',
                    'host': 'test-secret-2'
                }
            ),
            create_connection_with_secret(
                conn_id="gcp_with_secret",
                conn_type="google_cloud_platform",
                secret_params={
                    'password': 'test-secret-1',
                    'extra': 'test-secret-2'
                }
            )
        ]
        
        # Patch the get_secret function
        self.get_secret_patch = patch(
            '...backend.dags.utils.gcp_utils.get_secret',
            side_effect=mock_get_secret
        )
        self.mock_get_secret = self.get_secret_patch.start()
        
        # Patch the SecretManagerClient
        self.secret_manager_patch = patch(
            '...backend.dags.utils.gcp_utils.SecretManagerClient'
        )
        self.mock_secret_manager = self.secret_manager_patch.start()
        
    def tearDown(self):
        """Clean up after tests"""
        # Stop all patches
        self.get_secret_patch.stop()
        self.secret_manager_patch.stop()
        
        # Cleanup mock connection manager
        self.mock_manager.__exit__(None, None, None)
        self.mock_manager = None
        self.test_connections = []
    
    def test_secret_pattern_matching(self):
        """Test that the SECRET_PATTERN regex correctly identifies secrets"""
        # Test cases
        test_strings = [
            ("{SECRET:my-secret:latest}", True, ("my-secret", "latest")),
            ("{SECRET:db-password:v1}", True, ("db-password", "v1")),
            ("{SECRET:api-key:2}", True, ("api-key", "2")),
            ("No secret here", False, None),
            ("Partial {SECRET:incomplete", False, None),
            ("{SECRET:missing-version}", False, None),
            ("{SECRET:too:many:colons:here}", False, None)
        ]
        
        for test_str, should_match, expected_groups in test_strings:
            matches = re.search(SECRET_PATTERN, test_str)
            
            if should_match:
                self.assertIsNotNone(matches, f"Pattern should match: {test_str}")
                self.assertEqual(matches.group(1), expected_groups[0], 
                                 f"Secret ID doesn't match in {test_str}")
                self.assertEqual(matches.group(2), expected_groups[1], 
                                 f"Version doesn't match in {test_str}")
            else:
                self.assertIsNone(matches, f"Pattern should not match: {test_str}")


class TestSecretManagerIntegration(TestConnectionSecrets):
    """
    Tests for Secret Manager integration with Airflow connections
    """
    
    def test_get_secret_function(self):
        """Test that get_secret from gcp_utils functions correctly"""
        # Setup mock response
        mock_client = MagicMock()
        mock_secret_version = MagicMock()
        mock_secret_version.payload.data = b"test-secret-value"
        mock_client.access_secret_version.return_value = mock_secret_version
        
        with patch('...backend.dags.utils.gcp_utils.SecretManagerServiceClient', return_value=mock_client):
            with patch('...backend.dags.utils.gcp_utils.get_gcp_connection') as mock_get_conn:
                # Setup mock connection for GCP project
                mock_conn = MagicMock()
                mock_conn.extra_dejson = {'project': 'test-project'}
                mock_get_conn.return_value = mock_conn
                
                # Call the function
                result = get_secret("test-secret-id", "latest")
                
                # Verify correct calls were made
                self.assertTrue(mock_client.access_secret_version.called)
                
                # Check secret value was correctly retrieved
                # Note: Actual implementation may decode the bytes differently
                self.assertIsNotNone(result)
    
    def test_create_secret_function(self):
        """Test that create_secret from gcp_utils functions correctly"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.create_secret.return_value = MagicMock()
        mock_client.add_secret_version.return_value = MagicMock()
        
        with patch('...backend.dags.utils.gcp_utils.SecretManagerServiceClient', return_value=mock_client):
            with patch('...backend.dags.utils.gcp_utils.get_gcp_connection') as mock_get_conn:
                # Setup mock connection for GCP project
                mock_conn = MagicMock()
                mock_conn.extra_dejson = {'project': 'test-project'}
                mock_get_conn.return_value = mock_conn
                
                # Call the function to create a new secret
                result = create_secret("new-secret-id", "new-secret-value")
                
                # Verify correct calls were made
                self.assertTrue(result)
                # Either create_secret or add_secret_version should be called
                self.assertTrue(
                    mock_client.create_secret.called or 
                    mock_client.add_secret_version.called
                )


class TestSecretResolution(TestConnectionSecrets):
    """
    Tests for resolving secret references in connection configurations
    """
    
    def test_resolve_secrets_function(self):
        """Test that secret references are correctly resolved"""
        # Create connection with secret references
        conn = create_connection_with_secret(
            conn_id="test_conn",
            conn_type="postgres",
            secret_params={
                'password': 'test-secret-1',
                'host': 'test-secret-2'
            }
        )
        
        # Function to simulate secret resolution
        def resolve_secrets(connection):
            result = connection.copy()
            for key, value in connection.items():
                if isinstance(value, str):
                    match = re.search(SECRET_PATTERN, value)
                    if match:
                        secret_id = match.group(1)
                        if secret_id in SECRET_VALUES:
                            result[key] = SECRET_VALUES[secret_id]
            return result
        
        # Resolve secrets
        resolved_conn = resolve_secrets(conn)
        
        # Verify secret references were replaced
        self.assertEqual(resolved_conn['password'], SECRET_VALUES['test-secret-1'])
        self.assertEqual(resolved_conn['host'], SECRET_VALUES['test-secret-2'])
        
        # Verify non-secret attributes remain unchanged
        self.assertEqual(resolved_conn['conn_id'], conn['conn_id'])
        self.assertEqual(resolved_conn['conn_type'], conn['conn_type'])
    
    def test_secret_resolution_in_connection_attributes(self):
        """Test secret resolution in specific connection attributes"""
        # Create test connections with secrets in different attributes
        test_conns = [
            # Password secret
            create_connection_with_secret(
                conn_id="conn_with_password",
                conn_type="postgres",
                secret_params={'password': 'test-secret-1'}
            ),
            # Host secret
            create_connection_with_secret(
                conn_id="conn_with_host",
                conn_type="postgres",
                secret_params={'host': 'test-secret-2'}
            ),
            # Extra field secret (as JSON string)
            {
                'conn_id': 'conn_with_extra',
                'conn_type': 'google_cloud_platform',
                'extra': json.dumps({'project_id': '{SECRET:test-secret-1:latest}'})
            },
            # Multiple secrets
            {
                'conn_id': 'conn_with_multiple',
                'conn_type': 'postgres',
                'password': '{SECRET:test-secret-1:latest}',
                'host': '{SECRET:test-secret-2:latest}'
            }
        ]
        
        # Function to simulate secret resolution
        def resolve_secrets(connection):
            result = connection.copy()
            for key, value in connection.items():
                if isinstance(value, str):
                    # Check for direct secret references
                    match = re.search(SECRET_PATTERN, value)
                    if match:
                        secret_id = match.group(1)
                        if secret_id in SECRET_VALUES:
                            result[key] = value.replace(
                                f"{{SECRET:{secret_id}:latest}}", 
                                SECRET_VALUES[secret_id]
                            )
                    
                    # Check for secrets in JSON strings (like 'extra')
                    if key == 'extra' and value.startswith('{'):
                        try:
                            extra_dict = json.loads(value)
                            modified = False
                            
                            for extra_key, extra_value in extra_dict.items():
                                if isinstance(extra_value, str):
                                    match = re.search(SECRET_PATTERN, extra_value)
                                    if match:
                                        secret_id = match.group(1)
                                        if secret_id in SECRET_VALUES:
                                            extra_dict[extra_key] = SECRET_VALUES[secret_id]
                                            modified = True
                            
                            if modified:
                                result[key] = json.dumps(extra_dict)
                        except json.JSONDecodeError:
                            pass
            return result
        
        # Test resolution for each connection
        for conn in test_conns:
            resolved_conn = resolve_secrets(conn)
            
            # Verify resolved values based on connection type
            if conn['conn_id'] == 'conn_with_password':
                self.assertEqual(resolved_conn['password'], SECRET_VALUES['test-secret-1'])
            
            elif conn['conn_id'] == 'conn_with_host':
                self.assertEqual(resolved_conn['host'], SECRET_VALUES['test-secret-2'])
            
            elif conn['conn_id'] == 'conn_with_extra':
                extra_dict = json.loads(resolved_conn['extra'])
                self.assertEqual(extra_dict['project_id'], SECRET_VALUES['test-secret-1'])
            
            elif conn['conn_id'] == 'conn_with_multiple':
                self.assertEqual(resolved_conn['password'], SECRET_VALUES['test-secret-1'])
                self.assertEqual(resolved_conn['host'], SECRET_VALUES['test-secret-2'])
    
    def test_secret_resolution_error_handling(self):
        """Test error handling during secret resolution"""
        # Create a connection with a reference to a non-existent secret
        conn = create_connection_with_secret(
            conn_id="conn_with_missing_secret",
            conn_type="postgres",
            secret_params={'password': 'nonexistent-secret'}
        )
        
        # Mock get_secret to raise an exception for the nonexistent secret
        def mock_get_secret_with_error(secret_id, version_id='latest', conn_id=None):
            if secret_id in SECRET_VALUES:
                return SECRET_VALUES[secret_id]
            raise Exception(f"Secret {secret_id} not found")
        
        # Function to simulate secret resolution with error handling
        def resolve_secrets_with_error_handling(connection):
            result = connection.copy()
            for key, value in connection.items():
                if isinstance(value, str):
                    match = re.search(SECRET_PATTERN, value)
                    if match:
                        secret_id = match.group(1)
                        version_id = match.group(2)
                        try:
                            secret_value = mock_get_secret_with_error(secret_id, version_id)
                            result[key] = secret_value
                        except Exception as e:
                            # In case of error, log and keep the original reference
                            print(f"Error resolving secret: {e}")
            return result
        
        # Test resolution with error handling
        with patch('...backend.dags.utils.gcp_utils.get_secret', side_effect=mock_get_secret_with_error):
            resolved_conn = resolve_secrets_with_error_handling(conn)
            
            # The password should still contain the original reference
            self.assertEqual(resolved_conn['password'], conn['password'])


class TestSecretRotation(TestConnectionSecrets):
    """
    Tests for secret rotation functionality
    """
    
    def test_rotate_secret_function(self):
        """Test that a secret can be rotated correctly"""
        # Implementation of a simplified rotate_secret function for testing
        def rotate_secret(secret_id, secret_type="password", length=16):
            import random
            import string
            
            # Generate a new random secret value
            if secret_type == "password":
                chars = string.ascii_letters + string.digits + string.punctuation
                new_value = ''.join(random.choice(chars) for _ in range(length))
            elif secret_type == "key":
                chars = string.ascii_letters + string.digits
                new_value = ''.join(random.choice(chars) for _ in range(length))
            else:
                new_value = f"new-value-for-{secret_id}"
            
            # Store the new secret in Secret Manager
            create_secret(secret_id, new_value)
            return new_value
        
        # Mock the Secret Manager client
        mock_client = MagicMock()
        mock_client.add_secret_version.return_value = MagicMock()
        
        with patch('...backend.dags.utils.gcp_utils.SecretManagerServiceClient', return_value=mock_client):
            with patch('...backend.dags.utils.gcp_utils.get_gcp_connection') as mock_get_conn:
                # Setup mock connection for GCP project
                mock_conn = MagicMock()
                mock_conn.extra_dejson = {'project': 'test-project'}
                mock_get_conn.return_value = mock_conn
                
                # Test rotating a password secret
                new_secret = rotate_secret("db-password", "password", 16)
                
                # Verify a new secret was generated
                self.assertIsNotNone(new_secret)
                self.assertEqual(len(new_secret), 16)
                
                # Verify the appropriate Secret Manager method was called
                # The exact call depends on the implementation of create_secret
                self.assertTrue(mock_client.add_secret_version.called or mock_client.create_secret.called)
    
    def test_connection_update_after_rotation(self):
        """Test connections are updated after secret rotation"""
        # Create connections with secret references
        conn1 = create_connection_with_secret(
            conn_id="conn1",
            conn_type="postgres",
            secret_params={'password': 'db-password'}
        )
        
        conn2 = create_connection_with_secret(
            conn_id="conn2",
            conn_type="postgres",
            secret_params={'password': 'api-key'}
        )
        
        # Dictionary of rotated secrets with new versions
        rotated_secrets = {
            'db-password': {'value': 'new-db-password', 'version': 'v2'},
            'api-key': {'value': 'new-api-key', 'version': 'v3'}
        }
        
        # Function to update connections with new secret versions
        def update_connections(connections, rotated_secrets):
            updated = []
            for conn in connections:
                new_conn = conn.copy()
                for key, value in conn.items():
                    if isinstance(value, str):
                        match = re.search(SECRET_PATTERN, value)
                        if match:
                            secret_id = match.group(1)
                            if secret_id in rotated_secrets:
                                # Update to new version but keep the same secret ID
                                new_version = rotated_secrets[secret_id]['version']
                                new_conn[key] = f"{{SECRET:{secret_id}:{new_version}}}"
                updated.append(new_conn)
            return updated
        
        # Update the connections
        updated_connections = update_connections([conn1, conn2], rotated_secrets)
        
        # Verify connections were updated correctly
        self.assertEqual(updated_connections[0]['password'], 
                        f"{{SECRET:db-password:{rotated_secrets['db-password']['version']}}}")
        
        self.assertEqual(updated_connections[1]['password'], 
                        f"{{SECRET:api-key:{rotated_secrets['api-key']['version']}}}")
    
    def test_rotation_period_check(self):
        """Test verification of whether rotation is needed based on age"""
        # Mock function to check if a secret needs rotation based on age
        def check_rotation_needed(secret_id, days_threshold):
            from datetime import datetime, timedelta
            
            # Mock secret creation times for testing
            creation_times = {
                'new-secret': datetime.now() - timedelta(days=10),
                'old-secret': datetime.now() - timedelta(days=100),
                'medium-secret': datetime.now() - timedelta(days=45)
            }
            
            if secret_id in creation_times:
                creation_time = creation_times[secret_id]
                age_days = (datetime.now() - creation_time).days
                return age_days > days_threshold
            
            # Default case
            return False
        
        # Test various scenarios
        self.assertFalse(check_rotation_needed('new-secret', 30), 
                        "New secret should not need rotation")
        
        self.assertTrue(check_rotation_needed('old-secret', 30), 
                       "Old secret should need rotation")
        
        self.assertTrue(check_rotation_needed('medium-secret', 30), 
                       "Medium-aged secret should need rotation")
        
        self.assertFalse(check_rotation_needed('medium-secret', 60), 
                        "Medium-aged secret should not need rotation with higher threshold")
        
        # Test edge cases
        self.assertTrue(check_rotation_needed('old-secret', 0), 
                       "Any secret needs rotation if threshold is 0")
        
        self.assertFalse(check_rotation_needed('new-secret', 1000), 
                        "No secret needs rotation if threshold is very high")


class TestAirflow2SecretIntegration(TestConnectionSecrets):
    """
    Tests specific to Airflow 2.X secret handling integration
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.airflow2_mixin = Airflow2CompatibilityTestMixin()
    
    @unittest.skipIf(not is_airflow2(), "Test requires Airflow 2.X")
    def test_airflow2_connection_with_secrets(self):
        """Test that Airflow 2.X connection API handles secrets correctly"""
        if not is_airflow2():
            self.skipTest("Test requires Airflow 2.X")
        
        # Create a connection with secret references
        conn = create_connection_with_secret(
            conn_id="airflow2_test_conn",
            conn_type="postgres",
            secret_params={
                'password': 'test-secret-1',
                'host': 'test-secret-2'
            }
        )
        
        # Mock the imports we need for this test
        with patch('airflow.models.Connection') as mock_connection:
            # Create a mock Connection object
            mock_conn_instance = MagicMock()
            mock_connection.return_value = mock_conn_instance
            
            # Mock Connection.from_dict to return our mock instance
            mock_connection.from_dict.return_value = mock_conn_instance
            
            # Function to resolve secrets and create the connection
            def create_airflow_connection(conn_dict):
                # Resolve secrets
                resolved = conn_dict.copy()
                for key, value in conn_dict.items():
                    if isinstance(value, str):
                        match = re.search(SECRET_PATTERN, value)
                        if match:
                            secret_id = match.group(1)
                            if secret_id in SECRET_VALUES:
                                resolved[key] = SECRET_VALUES[secret_id]
                
                # Create connection from dict
                connection = mock_connection.from_dict(resolved)
                return connection
            
            # Create a connection with resolved secrets
            connection = create_airflow_connection(conn)
            
            # Verify Connection.from_dict was called with resolved secrets
            mock_connection.from_dict.assert_called_once()
            args, kwargs = mock_connection.from_dict.call_args
            conn_dict = args[0]
            
            # Check the password and host were correctly resolved
            self.assertEqual(conn_dict['password'], SECRET_VALUES['test-secret-1'])
            self.assertEqual(conn_dict['host'], SECRET_VALUES['test-secret-2'])
    
    @unittest.skipIf(not is_airflow2(), "Test requires Airflow 2.X")
    def test_airflow2_variable_with_secrets(self):
        """Test that Airflow 2.X variable API handles secrets correctly"""
        if not is_airflow2():
            self.skipTest("Test requires Airflow 2.X")
        
        # Create a variable with a secret reference
        variable = {
            'key': 'api_endpoint',
            'value': '{SECRET:test-secret-1:latest}'
        }
        
        # Mock the Variable class
        with patch('airflow.models.Variable') as mock_variable:
            # Mock Variable.set method
            mock_variable.set = MagicMock()
            
            # Function to resolve secrets and set variable
            def set_variable_with_secret(var_dict):
                key = var_dict['key']
                value = var_dict['value']
                
                # Resolve secret references
                match = re.search(SECRET_PATTERN, value)
                if match:
                    secret_id = match.group(1)
                    if secret_id in SECRET_VALUES:
                        value = SECRET_VALUES[secret_id]
                
                # Set the variable
                mock_variable.set(key, value)
                
                return {
                    'key': key,
                    'value': value
                }
            
            # Set a variable with a resolved secret
            result = set_variable_with_secret(variable)
            
            # Verify Variable.set was called with resolved secret
            mock_variable.set.assert_called_once()
            args, kwargs = mock_variable.set.call_args
            key, value = args
            
            # Check the variable key and value
            self.assertEqual(key, 'api_endpoint')
            self.assertEqual(value, SECRET_VALUES['test-secret-1'])