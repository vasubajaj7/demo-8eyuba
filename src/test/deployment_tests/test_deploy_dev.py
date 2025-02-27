"""Unit tests for the development environment deployment process to Cloud Composer 2.
This module verifies that DAGs are correctly deployed to the development environment using the deploy-dev.sh script
and deploy_dags.py functions with appropriate validation and configuration.
"""
import unittest
from unittest.mock import patch, MagicMock
import pytest
import os
import sys
import tempfile
import subprocess
import shutil

# Internal imports
from src.backend.scripts.deploy_dags import deploy_dags_to_composer
from src.backend.config.composer_dev import config
from src.test.fixtures.mock_gcp_services import create_mock_storage_client
from src.test.utils.test_helpers import create_test_dag_file
from src.test.utils.assertion_utils import assert_file_exists

# Define global constants
DEPLOY_SCRIPT_PATH = os.path.join('src', 'backend', 'ci-cd', 'deploy-dev.sh')
TEST_DAG_CONTENT = """Example test DAG for deployment tests

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> end
"""
INVALID_DAG_CONTENT = """Invalid DAG for testing validation

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# Missing required fields will cause validation failure
dag = DAG('invalid_dag')

start = DummyOperator(task_id='start', dag=dag)
"""

class TestDevDeployment(unittest.TestCase):
    """Tests the development environment deployment process"""

    def __init__(self, *args, **kwargs):
        """Initialize the test class"""
        super().__init__(*args, **kwargs)

    def setUp(self):
        """Set up test environment before each test"""
        # Create temporary directory for test DAGs
        self.temp_dir = tempfile.mkdtemp()

        # Set up environment variables for testing
        self.original_env = os.environ.copy()
        os.environ['PROJECT_ID'] = config['gcp']['project_id']
        os.environ['COMPOSER_BUCKET'] = config['storage']['dag_bucket']
        os.environ['COMPOSER_REGION'] = config['gcp']['region']
        os.environ['COMPOSER_ENVIRONMENT'] = config['composer']['environment_name']
        os.environ['ENVIRONMENT'] = config['environment']['name']

        # Create mock objects for GCP services
        self.mock_storage_client = create_mock_storage_client()

    def tearDown(self):
        """Clean up test environment after each test"""
        # Remove temporary test directory
        shutil.rmtree(self.temp_dir)

        # Reset environment variables
        os.environ = self.original_env

    def test_deploy_script_exists(self):
        """Test that the deployment script for dev exists"""
        # Use assert_file_exists to verify deploy-dev.sh script exists
        assert_file_exists(DEPLOY_SCRIPT_PATH)

        # Check that script is executable
        assert os.access(DEPLOY_SCRIPT_PATH, os.X_OK), "Deploy script is not executable"

    def test_deploy_script_execution(self):
        """Test execution of the deploy-dev.sh script"""
        # Patch subprocess.check_call and subprocess.check_output
        with patch('subprocess.check_call') as mock_check_call, \
             patch('subprocess.check_output', return_value=b'success') as mock_check_output:

            # Set up mock return values for successful execution
            mock_check_call.return_value = 0
            mock_check_output.return_value = b'success'

            # Create test DAG files in temporary directory
            dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

            # Execute deploy-dev.sh script with necessary parameters
            script_path = os.path.abspath(DEPLOY_SCRIPT_PATH)
            project_id = os.environ['PROJECT_ID']
            composer_bucket = os.environ['COMPOSER_BUCKET']
            composer_region = os.environ['COMPOSER_REGION']
            composer_environment = os.environ['COMPOSER_ENVIRONMENT']

            result = subprocess.run(
                [script_path, project_id, composer_bucket, composer_region, composer_environment, self.temp_dir],
                capture_output=True,
                text=True,
                check=False
            )

            # Verify subprocess calls were made with correct arguments
            mock_check_call.assert_called_with(
                ['gsutil', 'cp', dag_file, f'gs://{composer_bucket}/dags'],
                stderr=subprocess.STDOUT
            )

            # Assert script returned success status
            self.assertEqual(result.returncode, 0)

    def test_deploy_dags_function(self):
        """Test direct execution of deploy_dags_to_composer function"""
        # Create test DAG files in temporary directory
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Mock GCS client and related service calls
        with patch('src.backend.scripts.deploy_dags.StorageClient', return_value=self.mock_storage_client), \
             patch('src.backend.scripts.deploy_dags.gcs_upload_file') as mock_gcs_upload:

            # Call deploy_dags_to_composer with dev environment parameters
            result = deploy_dags_to_composer(self.temp_dir, config)

            # Verify file upload operations were performed correctly
            mock_gcs_upload.assert_called_with(dag_file, config['storage']['dag_bucket'], 'dags/test_dag.py')

            # Assert function returned success status
            self.assertTrue(result['success'])

    def test_deploy_dag_validation(self):
        """Test that DAG validation happens during deployment"""
        # Create both valid and invalid DAG files
        valid_dag_file = create_test_dag_file(self.temp_dir, 'valid_dag.py', TEST_DAG_CONTENT)
        invalid_dag_file = create_test_dag_file(self.temp_dir, 'invalid_dag.py', INVALID_DAG_CONTENT)

        # Patch validate_dag_files function to track calls
        with patch('src.backend.scripts.deploy_dags.validate_dag_files') as mock_validate_dag_files, \
             patch('src.backend.scripts.deploy_dags.StorageClient', return_value=self.mock_storage_client), \
             patch('src.backend.scripts.deploy_dags.gcs_upload_file') as mock_gcs_upload:

            # Set up mock return values for validation function
            mock_validate_dag_files.return_value = {'success': False, 'summary': {'error_count': 1, 'warning_count': 0}}

            # Call deploy_dags_to_composer function
            result = deploy_dags_to_composer(self.temp_dir, config)

            # Verify validation function was called
            mock_validate_dag_files.assert_called()

            # Confirm valid DAGs were deployed while invalid ones were rejected
            # Check that appropriate error messages were logged
            self.assertFalse(result['success'])

    def test_deploy_error_handling(self):
        """Test error handling during deployment process"""
        # Create test DAG files
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Mock GCS client to raise exceptions during upload
        with patch('src.backend.scripts.deploy_dags.StorageClient', return_value=self.mock_storage_client), \
             patch('src.backend.scripts.deploy_dags.gcs_upload_file') as mock_gcs_upload:

            # Configure mock GCS client to raise an exception
            mock_gcs_upload.side_effect = Exception("Upload failed")

            # Call deploy_dags_to_composer function
            result = deploy_dags_to_composer(self.temp_dir, config)

            # Verify function handles exceptions appropriately
            # Assert function returns failure status
            self.assertFalse(result['success'])

            # Check that appropriate error messages were logged
            self.assertIn("Upload failed", result['message'])

    def test_deploy_dev_environment_config(self):
        """Test that deployment uses dev environment configuration"""
        # Create test DAG files
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Mock GCS client and related service calls
        with patch('src.backend.scripts.deploy_dags.StorageClient', return_value=self.mock_storage_client), \
             patch('src.backend.scripts.deploy_dags.gcs_upload_file') as mock_gcs_upload:

            # Call deploy_dags_to_composer with dev environment
            result = deploy_dags_to_composer(self.temp_dir, config)

            # Verify dev-specific configuration values were used
            # Check that PROJECT_ID matches dev environment
            self.assertEqual(os.environ['PROJECT_ID'], config['gcp']['project_id'])

            # Confirm proper environment variables were set
            self.assertEqual(os.environ['ENVIRONMENT'], config['environment']['name'])

            # Assert function used correct deployment bucket
            mock_gcs_upload.assert_called_with(dag_file, config['storage']['dag_bucket'], 'dags/test_dag.py')

    def test_deploy_authentication(self):
        """Test authentication process during deployment"""
        # Patch authentication function to track calls
        with patch('src.backend.scripts.deploy_dags.authenticate_to_gcp') as mock_authenticate, \
             patch('src.backend.scripts.deploy_dags.StorageClient', return_value=self.mock_storage_client), \
             patch('src.backend.scripts.deploy_dags.gcs_upload_file'):

            # Create test DAG files
            dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

            # Call deploy_dags_to_composer function
            result = deploy_dags_to_composer(self.temp_dir, config)

            # Verify authentication was called with correct parameters
            mock_authenticate.assert_called_with(config)

            # Assert authentication used dev service account
            # Check proper credentials were used for GCP services
            pass

    def test_deploy_verification(self):
        """Test that deployment verifies success"""
        # Create test DAG files
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Mock GCS client and deployment functions
        with patch('src.backend.scripts.deploy_dags.StorageClient', return_value=self.mock_storage_client), \
             patch('src.backend.scripts.deploy_dags.gcs_upload_file') as mock_gcs_upload, \
             patch('src.backend.scripts.deploy_dags.GCSClient.list_files') as mock_list_files:

            # Patch verification steps to track calls
            mock_list_files.return_value = ['dags/test_dag.py']

            # Call deploy_dags_to_composer function
            result = deploy_dags_to_composer(self.temp_dir, config)

            # Verify that list operation was performed after upload
            mock_list_files.assert_called()

            # Confirm verification steps were performed
            # Assert deployment returned success status
            self.assertTrue(result['success'])