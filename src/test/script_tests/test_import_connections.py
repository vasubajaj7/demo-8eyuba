#!/usr/bin/env python3
"""
Unit tests for the import_connections.py script which imports connection configurations
during migration from Airflow 1.10.15 to Airflow 2.X.

These tests validate all script functionalities including file loading, environment-specific 
processing, secret resolution, and connection imports across Airflow versions.
"""

import os
import sys
import json
import unittest
import tempfile
from unittest import mock
from pathlib import Path

import pytest

# Internal imports
from src.backend.scripts.import_connections import (
    load_connections_from_file,
    load_connections_from_gcs,
    process_environment_connections,
    resolve_secrets,
    import_connections,
    get_connection_class,
    detect_airflow_version
)

from src.backend.dags.utils.gcp_utils import (
    gcs_file_exists,
    gcs_download_file,
    get_secret
)

from src.test.fixtures.mock_connections import (
    MockConnectionManager,
    create_mock_connection,
    get_mock_connection,
    reset_mock_connections
)

from src.test.utils.airflow2_compatibility_utils import (
    is_airflow2,
    mock_airflow2_imports,
    mock_airflow1_imports,
    Airflow2CompatibilityTestMixin
)

# Global test data
TEST_CONNECTIONS_FILE = os.path.join(os.path.dirname(__file__), '../fixtures/test_connections.json')
TEST_GCS_BUCKET = 'test-bucket'
TEST_GCS_PATH = f'gs://{TEST_GCS_BUCKET}/config/connections.json'
TEST_CONNECTIONS = [
    {
        'conn_id': 'test_pg_conn',
        'conn_type': 'postgres',
        'host': 'localhost',
        'schema': 'airflow',
        'login': 'airflow',
        'password': 'airflow',
        'port': 5432,
        'description': 'Test PostgreSQL connection',
        'environments': ['dev', 'qa']
    },
    {
        'conn_id': 'test_gcp_conn',
        'conn_type': 'google_cloud_platform',
        'description': 'Test GCP connection',
        'extra': {'project': 'test-project', 'keyfile_json': '{}'},
        'environments': ['dev', 'qa', 'prod']
    },
    {
        'conn_id': 'test_http_conn',
        'conn_type': 'http',
        'host': 'api.example.com',
        'schema': 'https',
        'description': 'Test HTTP connection',
        'environments': ['prod']
    },
    {
        'conn_id': 'test_secret_conn',
        'conn_type': 'postgres',
        'host': 'db.example.com',
        'schema': 'airflow',
        'login': 'airflow',
        'password': '{SECRET:test-secret:latest}',
        'port': 5432,
        'description': 'Connection with secret',
        'environments': ['dev', 'qa', 'prod']
    }
]


def create_test_connections_file(connections=None):
    """
    Creates a temporary file with test connection configurations
    
    Args:
        connections: List of connection configurations to write to file
    
    Returns:
        str: Path to the created temporary file
    """
    connections = connections or TEST_CONNECTIONS
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
    with open(temp_file.name, 'w') as f:
        json.dump(connections, f)
    return temp_file.name


def setup_module():
    """
    Set up function that runs once at the beginning of the test module
    """
    # Create test_connections.json file for tests
    with open(TEST_CONNECTIONS_FILE, 'w') as f:
        json.dump(TEST_CONNECTIONS, f)
    
    # Set up environment variables
    os.environ['AIRFLOW_ENV'] = 'dev'
    
    # Initialize mock connections
    reset_mock_connections()


def teardown_module():
    """
    Clean up function that runs once at the end of the test module
    """
    # Remove test connections file
    if os.path.exists(TEST_CONNECTIONS_FILE):
        os.remove(TEST_CONNECTIONS_FILE)
    
    # Reset mock connections
    reset_mock_connections()
    
    # Restore original environment
    if 'AIRFLOW_ENV' in os.environ:
        del os.environ['AIRFLOW_ENV']


@mock.patch('src.backend.dags.utils.gcp_utils.gcs_file_exists')
@mock.patch('src.backend.dags.utils.gcp_utils.gcs_download_file')
@mock.patch('src.backend.dags.utils.gcp_utils.get_secret')
class TestImportConnections(unittest.TestCase):
    """
    Test class for testing the import_connections.py script core functionality
    """
    
    def setUp(self):
        """
        Set up test environment before each test method
        """
        # Create a temporary connections file
        self.temp_file_path = create_test_connections_file()
        
        # Set up mock connection manager
        self.conn_manager = MockConnectionManager()
        
        # Common mock setup
        self.mock_gcs_exists = mock.MagicMock(return_value=True)
        self.mock_gcs_download = mock.MagicMock()
        self.mock_get_secret = mock.MagicMock(return_value='test-password')
    
    def tearDown(self):
        """
        Clean up test environment after each test method
        """
        # Remove temporary files
        if os.path.exists(self.temp_file_path):
            os.remove(self.temp_file_path)
        
        # Reset mock connections
        reset_mock_connections()
    
    def test_load_connections_from_file(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test loading connections from a local JSON file
        """
        # Load connections from file
        connections = load_connections_from_file(self.temp_file_path)
        
        # Verify connections were loaded correctly
        self.assertEqual(len(connections), 4)
        self.assertEqual(connections[0]['conn_id'], 'test_pg_conn')
        self.assertEqual(connections[1]['conn_type'], 'google_cloud_platform')
        
        # Test with non-existent file
        non_existent_file = '/path/to/nonexistent/file.json'
        with self.assertRaises(FileNotFoundError):
            load_connections_from_file(non_existent_file)
        
        # Test with invalid JSON
        invalid_json_file = create_test_connections_file()
        with open(invalid_json_file, 'w') as f:
            f.write('invalid json data')
        
        with self.assertRaises(json.JSONDecodeError):
            load_connections_from_file(invalid_json_file)
        
        os.remove(invalid_json_file)
    
    def test_load_connections_from_gcs(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test loading connections from a GCS file
        """
        # Set up mocks
        mock_gcs_exists.return_value = True
        
        # Set up download behavior
        def download_side_effect(bucket, object_name, local_path, conn_id=None):
            with open(local_path, 'w') as f:
                json.dump(TEST_CONNECTIONS, f)
        
        mock_gcs_download.side_effect = download_side_effect
        
        # Load connections from GCS
        connections = load_connections_from_gcs(TEST_GCS_PATH)
        
        # Verify mocks were called correctly
        mock_gcs_exists.assert_called_with(TEST_GCS_BUCKET, 'config/connections.json', None)
        mock_gcs_download.assert_called_once()
        
        # Verify connections were loaded correctly
        self.assertEqual(len(connections), 4)
        self.assertEqual(connections[0]['conn_id'], 'test_pg_conn')
        
        # Test with non-existent GCS path
        mock_gcs_exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            load_connections_from_gcs(TEST_GCS_PATH)
        
        # Test with invalid GCS path
        with self.assertRaises(ValueError):
            load_connections_from_gcs('invalid-gcs-path')
    
    def test_process_environment_connections(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test processing connections for specific environments
        """
        # Process connections for 'dev' environment
        dev_connections = process_environment_connections(TEST_CONNECTIONS, 'dev')
        
        # Verify only dev-applicable connections are included
        self.assertEqual(len(dev_connections), 3)
        conn_ids = [conn['conn_id'] for conn in dev_connections]
        self.assertIn('test_pg_conn', conn_ids)
        self.assertIn('test_gcp_conn', conn_ids)
        self.assertIn('test_secret_conn', conn_ids)
        self.assertNotIn('test_http_conn', conn_ids)
        
        # Process connections for 'prod' environment
        prod_connections = process_environment_connections(TEST_CONNECTIONS, 'prod')
        
        # Verify only prod-applicable connections are included
        self.assertEqual(len(prod_connections), 3)
        conn_ids = [conn['conn_id'] for conn in prod_connections]
        self.assertIn('test_gcp_conn', conn_ids)
        self.assertIn('test_http_conn', conn_ids)
        self.assertIn('test_secret_conn', conn_ids)
        self.assertNotIn('test_pg_conn', conn_ids)
        
        # Verify environments field is removed
        for conn in prod_connections:
            self.assertNotIn('environments', conn)
        
        # Test with invalid environment
        with self.assertRaises(ValueError):
            process_environment_connections(TEST_CONNECTIONS, 'invalid')
    
    def test_resolve_secrets(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test resolving secret references in connection configurations
        """
        # Set up mock secret
        mock_get_secret.return_value = 'secret-password'
        
        # Create test connections with secret references
        test_conn = {
            'conn_id': 'secret_conn',
            'conn_type': 'postgres',
            'host': 'localhost',
            'password': '{SECRET:test-secret:latest}',
            'extra': '{SECRET:test-extra:v1}'
        }
        
        # Resolve secrets
        resolved_conns = resolve_secrets([test_conn])
        
        # Verify mock was called correctly
        mock_get_secret.assert_any_call('test-secret', 'latest', None)
        mock_get_secret.assert_any_call('test-extra', 'v1', None)
        
        # Verify secrets were resolved
        self.assertEqual(resolved_conns[0]['password'], 'secret-password')
        self.assertEqual(resolved_conns[0]['extra'], 'secret-password')
        
        # Test with secret retrieval error
        mock_get_secret.side_effect = Exception("Secret not found")
        
        # This should not raise an exception, just log the error
        resolved_conns = resolve_secrets([test_conn])
        self.assertEqual(resolved_conns[0]['password'], '{SECRET:test-secret:latest}')
    
    def test_import_connections(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test importing connections into Airflow
        """
        # Set up test connections
        test_connections = [
            {
                'conn_id': 'test_import_conn',
                'conn_type': 'postgres',
                'host': 'localhost',
                'schema': 'airflow',
                'login': 'airflow',
                'password': 'airflow',
                'port': 5432,
                'description': 'Test import connection'
            }
        ]
        
        # Test importing connections
        with self.conn_manager:
            success_count, skipped_count, failed_count = import_connections(
                test_connections,
                dry_run=False,
                skip_existing=False,
                force=False,
                validate=True
            )
            
            # Verify counts
            self.assertEqual(success_count, 1)
            self.assertEqual(skipped_count, 0)
            self.assertEqual(failed_count, 0)
            
            # Verify connection was added
            conn = get_mock_connection('test_import_conn')
            self.assertEqual(conn.conn_id, 'test_import_conn')
            self.assertEqual(conn.conn_type, 'postgres')
            self.assertEqual(conn.host, 'localhost')
        
        # Test with dry_run=True
        with self.conn_manager:
            success_count, skipped_count, failed_count = import_connections(
                test_connections,
                dry_run=True,
                skip_existing=False,
                force=False
            )
            
            # Verify counts
            self.assertEqual(success_count, 1)
            self.assertEqual(skipped_count, 0)
            self.assertEqual(failed_count, 0)
            
            # Verify connection was not added (since it's a dry run)
            with self.assertRaises(Exception):
                get_mock_connection('test_import_conn')
        
        # Test with skip_existing=True
        with self.conn_manager:
            # Add the connection first
            create_mock_connection(
                conn_id='test_import_conn',
                conn_type='postgres',
                host='localhost'
            )
            
            success_count, skipped_count, failed_count = import_connections(
                test_connections,
                skip_existing=True
            )
            
            # Verify counts
            self.assertEqual(success_count, 0)
            self.assertEqual(skipped_count, 1)
            self.assertEqual(failed_count, 0)
        
        # Test with force=True
        with self.conn_manager:
            # Add the connection first with different values
            create_mock_connection(
                conn_id='test_import_conn',
                conn_type='postgres',
                host='original-host'
            )
            
            success_count, skipped_count, failed_count = import_connections(
                test_connections,
                force=True
            )
            
            # Verify counts
            self.assertEqual(success_count, 1)
            self.assertEqual(skipped_count, 0)
            self.assertEqual(failed_count, 0)
            
            # Verify connection was updated
            conn = get_mock_connection('test_import_conn')
            self.assertEqual(conn.host, 'localhost')
        
        # Test with filter_conn_id
        with self.conn_manager:
            multi_connections = [
                {
                    'conn_id': 'test_conn1',
                    'conn_type': 'postgres',
                    'host': 'host1'
                },
                {
                    'conn_id': 'test_conn2',
                    'conn_type': 'postgres',
                    'host': 'host2'
                }
            ]
            
            success_count, skipped_count, failed_count = import_connections(
                multi_connections,
                filter_conn_id='test_conn1'
            )
            
            # Verify only test_conn1 was imported
            self.assertEqual(success_count, 1)
            
            # Verify test_conn1 exists and test_conn2 doesn't
            conn = get_mock_connection('test_conn1')
            self.assertEqual(conn.host, 'host1')
            
            with self.assertRaises(Exception):
                get_mock_connection('test_conn2')
    
    def test_detect_airflow_version(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test detecting the installed Airflow version
        """
        # Get actual installed version
        major, minor, patch = detect_airflow_version()
        
        # Verify result is a tuple of 3 integers
        self.assertIsInstance(major, int)
        self.assertIsInstance(minor, int)
        self.assertIsInstance(patch, int)
        
        # Mock airflow.__version__ to test specific versions
        with mock.patch('airflow.__version__', '2.3.4'):
            major, minor, patch = detect_airflow_version()
            self.assertEqual(major, 2)
            self.assertEqual(minor, 3)
            self.assertEqual(patch, 4)
        
        with mock.patch('airflow.__version__', '1.10.15'):
            major, minor, patch = detect_airflow_version()
            self.assertEqual(major, 1)
            self.assertEqual(minor, 10)
            self.assertEqual(patch, 15)
        
        # Test with invalid version format
        with mock.patch('airflow.__version__', 'invalid'):
            major, minor, patch = detect_airflow_version()
            self.assertEqual(major, 1)
            self.assertEqual(minor, 0)
            self.assertEqual(patch, 0)
        
        # Test with ImportError
        with mock.patch('airflow.__version__', side_effect=ImportError):
            major, minor, patch = detect_airflow_version()
            self.assertEqual(major, 0)
            self.assertEqual(minor, 0)
            self.assertEqual(patch, 0)
    
    def test_get_connection_class(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test retrieving the appropriate Connection class based on Airflow version
        """
        # Get the actual Connection class
        Connection = get_connection_class()
        
        # Verify it's a class
        self.assertTrue(isinstance(Connection, type))
        
        # Test in Airflow 1.X environment
        if is_airflow2():
            # If running in Airflow 2, we need to mock Airflow 1
            with mock_airflow1_imports():
                Connection = get_connection_class()
                # In mocked Airflow 1 environment, should import from airflow.models
                self.assertIn('models.Connection', str(Connection))
        else:
            # Already in Airflow 1
            Connection = get_connection_class()
            self.assertIn('models.Connection', str(Connection))
        
        # Test in Airflow 2.X environment
        if not is_airflow2():
            # If running in Airflow 1, we need to mock Airflow 2
            with mock_airflow2_imports():
                Connection = get_connection_class()
                # In mocked Airflow 2 environment, should import from airflow.models.connection
                self.assertIn('connection.Connection', str(Connection))
        else:
            # Already in Airflow 2
            Connection = get_connection_class()
            self.assertIn('connection.Connection', str(Connection))


@mock.patch('src.backend.dags.utils.gcp_utils.gcs_file_exists')
@mock.patch('src.backend.dags.utils.gcp_utils.gcs_download_file')
@mock.patch('src.backend.dags.utils.gcp_utils.get_secret')
class TestCrossVersionCompatibility(unittest.TestCase, Airflow2CompatibilityTestMixin):
    """
    Test class for verifying import_connections.py works across Airflow versions
    """
    
    def setUp(self):
        """
        Set up test environment for cross-version testing
        """
        # Initialize the compatibility mixin
        Airflow2CompatibilityTestMixin.__init__(self)
        
        # Create a temporary connections file
        self.temp_file_path = create_test_connections_file()
        
        # Set up mock connection manager
        self.conn_manager = MockConnectionManager()
        
        # Common mock setup
        self.mock_gcs_exists = mock.MagicMock(return_value=True)
        self.mock_gcs_download = mock.MagicMock()
        self.mock_get_secret = mock.MagicMock(return_value='test-password')
    
    def tearDown(self):
        """
        Clean up test environment after each test method
        """
        # Remove temporary files
        if os.path.exists(self.temp_file_path):
            os.remove(self.temp_file_path)
        
        # Reset mock connections
        reset_mock_connections()
    
    def test_airflow1_compatibility(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test that import_connections works with Airflow 1.10.15
        """
        # Set up test connections
        test_connections = [{
            'conn_id': 'test_airflow1_conn',
            'conn_type': 'postgres',
            'host': 'localhost',
            'schema': 'airflow',
            'login': 'airflow',
            'password': 'airflow'
        }]
        
        # Test importing in Airflow 1.X environment
        with mock_airflow1_imports(), self.conn_manager:
            success_count, _, _ = import_connections(test_connections)
            
            # Verify connection was added
            self.assertEqual(success_count, 1)
            conn = get_mock_connection('test_airflow1_conn')
            self.assertEqual(conn.conn_id, 'test_airflow1_conn')
    
    def test_airflow2_compatibility(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test that import_connections works with Airflow 2.X
        """
        # Set up test connections
        test_connections = [{
            'conn_id': 'test_airflow2_conn',
            'conn_type': 'postgres',
            'host': 'localhost',
            'schema': 'airflow',
            'login': 'airflow',
            'password': 'airflow'
        }]
        
        # Test importing in Airflow 2.X environment
        with mock_airflow2_imports(), self.conn_manager:
            success_count, _, _ = import_connections(test_connections)
            
            # Verify connection was added
            self.assertEqual(success_count, 1)
            conn = get_mock_connection('test_airflow2_conn')
            self.assertEqual(conn.conn_id, 'test_airflow2_conn')
    
    def test_version_specific_connection_handling(self, mock_get_secret, mock_gcs_download, mock_gcs_exists):
        """
        Test that connections are handled appropriately for each Airflow version
        """
        # Connection with version-specific attributes
        test_connections = [{
            'conn_id': 'test_version_conn',
            'conn_type': 'http',
            'host': 'api.example.com',
            # The 'extra' field structure changed between Airflow 1.X and 2.X
            'extra': json.dumps({
                'auth_type': 'basic',  # Airflow 1.X style
                'connection_type': 'http'  # Airflow 2.X style
            })
        }]
        
        # Test in Airflow 1.X environment
        with mock_airflow1_imports(), self.conn_manager:
            success_count, _, _ = import_connections(test_connections)
            self.assertEqual(success_count, 1)
            
            # In Airflow 1.X, the connection should be created without issues
            conn = get_mock_connection('test_version_conn')
            self.assertEqual(conn.conn_id, 'test_version_conn')
        
        # Reset for next test
        reset_mock_connections()
        
        # Test in Airflow 2.X environment
        with mock_airflow2_imports(), self.conn_manager:
            success_count, _, _ = import_connections(test_connections)
            self.assertEqual(success_count, 1)
            
            # In Airflow 2.X, the connection should be created without issues
            # and handle the extra field correctly
            conn = get_mock_connection('test_version_conn')
            self.assertEqual(conn.conn_id, 'test_version_conn')


class TestEnvironmentSpecificConnections(unittest.TestCase):
    """
    Test class for testing environment-specific connection processing
    """
    
    def setUp(self):
        """
        Set up test environment for environment-specific testing
        """
        # Create connections with environment-specific sections
        self.env_connections = [
            {
                'conn_id': 'env_specific_conn',
                'conn_type': 'postgres',
                'host': 'default-host',
                'login': 'default-user',
                'password': 'default-password',
                'schema': 'default-db',
                'port': 5432,
                'description': 'Environment-specific connection',
                'environments': ['dev', 'qa', 'prod'],
                'dev_config': {
                    'host': 'dev-host',
                    'schema': 'dev-db'
                },
                'qa_config': {
                    'host': 'qa-host',
                    'schema': 'qa-db'
                },
                'prod_config': {
                    'host': 'prod-host',
                    'schema': 'prod-db',
                    'port': 5433
                }
            }
        ]
        
        # Create a temporary connections file
        self.temp_file_path = create_test_connections_file(self.env_connections)
    
    def tearDown(self):
        """
        Clean up test environment after each test method
        """
        # Remove temporary files
        if os.path.exists(self.temp_file_path):
            os.remove(self.temp_file_path)
    
    def test_env_specific_sections(self):
        """
        Test processing of environment-specific sections in connections
        """
        # Process for 'dev' environment
        dev_connections = process_environment_connections(self.env_connections, 'dev')
        
        # Verify dev-specific values were applied
        self.assertEqual(len(dev_connections), 1)
        dev_conn = dev_connections[0]
        self.assertEqual(dev_conn['host'], 'dev-host')
        self.assertEqual(dev_conn['schema'], 'dev-db')
        self.assertEqual(dev_conn['port'], 5432)  # Unchanged
        
        # Verify dev_config section was removed
        self.assertNotIn('dev_config', dev_conn)
        self.assertNotIn('qa_config', dev_conn)
        self.assertNotIn('prod_config', dev_conn)
        
        # Process for 'prod' environment
        prod_connections = process_environment_connections(self.env_connections, 'prod')
        
        # Verify prod-specific values were applied
        self.assertEqual(len(prod_connections), 1)
        prod_conn = prod_connections[0]
        self.assertEqual(prod_conn['host'], 'prod-host')
        self.assertEqual(prod_conn['schema'], 'prod-db')
        self.assertEqual(prod_conn['port'], 5433)  # Changed in prod_config
    
    def test_env_specific_parameters(self):
        """
        Test processing of environment-specific parameters in connections
        """
        # Create connections with parameters that vary by environment
        env_param_connections = [
            {
                'conn_id': 'env_param_conn',
                'conn_type': 'postgres',
                'host': 'db-{ENV}.example.com',
                'schema': '{ENV}_database',
                'extra': json.dumps({'env_name': '{ENV}'}),
                'environments': ['dev', 'qa', 'prod']
            }
        ]
        
        # Process for 'dev' environment
        dev_connections = process_environment_connections(env_param_connections, 'dev')
        
        # Verify placeholders were replaced
        self.assertEqual(dev_connections[0]['host'], 'db-dev.example.com')
        self.assertEqual(dev_connections[0]['schema'], 'dev_database')
        self.assertEqual(json.loads(dev_connections[0]['extra'])['env_name'], 'dev')
        
        # Process for 'prod' environment
        prod_connections = process_environment_connections(env_param_connections, 'prod')
        
        # Verify placeholders were replaced
        self.assertEqual(prod_connections[0]['host'], 'db-prod.example.com')
        self.assertEqual(prod_connections[0]['schema'], 'prod_database')
        self.assertEqual(json.loads(prod_connections[0]['extra'])['env_name'], 'prod')
    
    def test_env_placeholder_replacement(self):
        """
        Test replacement of environment placeholders in connections
        """
        # Create connections with complex placeholder patterns
        complex_connections = [
            {
                'conn_id': 'complex_conn',
                'conn_type': 'postgres',
                'host': 'db-{ENV}-cluster.example.com',
                'schema': '{ENV}_app',
                'login': 'user-{ENV}',
                'extra': json.dumps({
                    'project': 'my-{ENV}-project',
                    'region': 'us-central1',
                    'metadata': {
                        'environment': '{ENV}',
                        'tier': '{ENV}-tier'
                    }
                }),
                'environments': ['dev', 'qa', 'prod']
            }
        ]
        
        # Process for 'qa' environment
        qa_connections = process_environment_connections(complex_connections, 'qa')
        
        # Verify placeholders were replaced at all levels
        qa_conn = qa_connections[0]
        self.assertEqual(qa_conn['host'], 'db-qa-cluster.example.com')
        self.assertEqual(qa_conn['schema'], 'qa_app')
        self.assertEqual(qa_conn['login'], 'user-qa')
        
        # Check nested JSON objects
        extra_dict = json.loads(qa_conn['extra'])
        self.assertEqual(extra_dict['project'], 'my-qa-project')
        self.assertEqual(extra_dict['metadata']['environment'], 'qa')
        self.assertEqual(extra_dict['metadata']['tier'], 'qa-tier')


@mock.patch('src.backend.scripts.import_connections.import_connections')
@mock.patch('src.backend.scripts.import_connections.load_connections_from_file')
@mock.patch('src.backend.scripts.import_connections.load_connections_from_gcs')
class TestCommandLineInterface(unittest.TestCase):
    """
    Test class for testing the command-line interface of import_connections.py
    """
    
    def setUp(self):
        """
        Set up test environment for CLI testing
        """
        # Save original arguments
        self.original_argv = sys.argv.copy()
        
        # Set up common mocks
        self.mock_load_file = mock.MagicMock(return_value=TEST_CONNECTIONS)
        self.mock_load_gcs = mock.MagicMock(return_value=TEST_CONNECTIONS)
        self.mock_import = mock.MagicMock(return_value=(2, 1, 0))
    
    def tearDown(self):
        """
        Clean up test environment after each test method
        """
        # Restore original arguments
        sys.argv = self.original_argv
    
    def test_cli_file_path(self, mock_load_gcs, mock_load_file, mock_import):
        """
        Test CLI with local file path argument
        """
        # Set up mocks
        mock_load_file.return_value = TEST_CONNECTIONS
        
        # Set up arguments
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE
        ]
        
        # Import the module and call main
        from src.backend.scripts.import_connections import main
        exit_code = main()
        
        # Verify functions were called correctly
        mock_load_file.assert_called_with(TEST_CONNECTIONS_FILE)
        mock_import.assert_called()
        
        # Verify exit code
        self.assertEqual(exit_code, 0)
    
    def test_cli_gcs_path(self, mock_load_gcs, mock_load_file, mock_import):
        """
        Test CLI with GCS path argument
        """
        # Set up mocks
        mock_load_gcs.return_value = TEST_CONNECTIONS
        
        # Set up arguments
        sys.argv = [
            'import_connections.py',
            '--gcs-path', TEST_GCS_PATH
        ]
        
        # Import the module and call main
        from src.backend.scripts.import_connections import main
        exit_code = main()
        
        # Verify functions were called correctly
        mock_load_gcs.assert_called_with(TEST_GCS_PATH, 'google_cloud_default')
        mock_import.assert_called()
        
        # Verify exit code
        self.assertEqual(exit_code, 0)
    
    def test_cli_environment(self, mock_load_gcs, mock_load_file, mock_import):
        """
        Test CLI with environment argument
        """
        # Set up mocks
        mock_load_file.return_value = TEST_CONNECTIONS
        
        # Set up arguments for testing with dev environment
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--environment', 'dev'
        ]
        
        # Import the module and call main
        from src.backend.scripts.import_connections import main
        exit_code = main()
        
        # Verify mock was called correctly
        mock_load_file.assert_called_with(TEST_CONNECTIONS_FILE)
        
        # Run again with qa environment
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--environment', 'qa'
        ]
        
        exit_code = main()
        self.assertEqual(exit_code, 0)
        
        # Run again with prod environment
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--environment', 'prod'
        ]
        
        exit_code = main()
        self.assertEqual(exit_code, 0)
        
        # Test with invalid environment
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--environment', 'invalid'
        ]
        
        # Should raise a SystemExit from argparse
        with self.assertRaises(SystemExit):
            main()
    
    def test_cli_options(self, mock_load_gcs, mock_load_file, mock_import):
        """
        Test CLI with various options
        """
        # Set up mocks
        mock_load_file.return_value = TEST_CONNECTIONS
        
        # Test with dry_run
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--dry-run'
        ]
        
        from src.backend.scripts.import_connections import main
        main()
        
        # Verify mock was called with dry_run=True
        mock_import.assert_called_with(
            TEST_CONNECTIONS,
            dry_run=True,
            skip_existing=False,
            force=False,
            validate=False,
            filter_conn_id=None
        )
        
        # Test with skip_existing
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--skip-existing'
        ]
        
        main()
        
        # Verify mock was called with skip_existing=True
        mock_import.assert_called_with(
            TEST_CONNECTIONS,
            dry_run=False,
            skip_existing=True,
            force=False,
            validate=False,
            filter_conn_id=None
        )
        
        # Test with force
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--force'
        ]
        
        main()
        
        # Verify mock was called with force=True
        mock_import.assert_called_with(
            TEST_CONNECTIONS,
            dry_run=False,
            skip_existing=False,
            force=True,
            validate=False,
            filter_conn_id=None
        )
        
        # Test with validate
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--validate'
        ]
        
        main()
        
        # Verify mock was called with validate=True
        mock_import.assert_called_with(
            TEST_CONNECTIONS,
            dry_run=False,
            skip_existing=False,
            force=False,
            validate=True,
            filter_conn_id=None
        )
        
        # Test with conn_id filter
        sys.argv = [
            'import_connections.py',
            '--file-path', TEST_CONNECTIONS_FILE,
            '--conn-id', 'test_pg_conn'
        ]
        
        main()
        
        # Verify mock was called with conn_id filter
        mock_import.assert_called_with(
            TEST_CONNECTIONS,
            dry_run=False,
            skip_existing=False,
            force=False,
            validate=False,
            filter_conn_id='test_pg_conn'
        )


if __name__ == '__main__':
    unittest.main()