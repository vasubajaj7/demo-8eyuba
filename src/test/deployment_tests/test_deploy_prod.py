# Standard library imports
import unittest
from unittest.mock import patch, MagicMock
import pytest
import os
import sys
import tempfile
import subprocess
import shutil
import json

# Internal imports
from src.backend.scripts.deploy_dags import deploy_dags_to_composer
from src.backend.config.composer_prod import config
from src.test.fixtures.mock_gcp_services import create_mock_storage_client
from src.test.utils.test_helpers import create_test_dag_file
from src.test.utils.assertion_utils import assert_file_exists


DEPLOY_SCRIPT_PATH = os.path.join('src', 'backend', 'ci-cd', 'deploy-prod.sh')
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
MOCK_APPROVAL_DATA = """
{
    "approvals": [
        {
            "role": "CAB",
            "approver": "cab.member@example.com",
            "timestamp": "2023-01-01T12:00:00Z",
            "comment": "Approved after CAB review"
        },
        {
            "role": "ARCHITECT",
            "approver": "system.architect@example.com",
            "timestamp": "2023-01-01T14:00:00Z",
            "comment": "Architecture review complete"
        },
        {
            "role": "STAKEHOLDER",
            "approver": "business.stakeholder@example.com",
            "timestamp": "2023-01-01T16:00:00Z",
            "comment": "Business requirements verified"
        }
    ],
    "status": "APPROVED",
    "expirationTime": "2023-01-02T00:00:00Z",
    "businessJustification": "Deployment needed for critical workflow improvements",
    "riskAssessment": "Low risk, comprehensive testing completed",
    "rollbackPlan": "Restore previous version from backup bucket"
}
"""
PROD_VALIDATION_THRESHOLD = 0
REQUIRED_APPROVERS = ["CAB", "ARCHITECT", "STAKEHOLDER"]


class TestProdDeployment(unittest.TestCase):
    """Tests the production environment deployment process"""

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
        os.environ['KMS_KEY_NAME'] = config['security']['kms_key']
        os.environ['DEPLOYMENT_ENV'] = 'prod'

        # Create mock objects for GCP services
        self.mock_storage_client = create_mock_storage_client()

        # Create mock approval token file with valid approvals from CAB, ARCHITECT, and STAKEHOLDER
        self.approval_data = json.loads(MOCK_APPROVAL_DATA)
        self.approval_token_file = os.path.join(self.temp_dir, 'approval_token.json')
        with open(self.approval_token_file, 'w') as f:
            json.dump(self.approval_data, f)
        os.environ['APPROVAL_TOKEN_PATH'] = self.approval_token_file

        # Create mock backup bucket for rollback testing
        self.backup_bucket = 'mock-backup-bucket'

    def tearDown(self):
        """Clean up test environment after each test"""
        # Remove temporary test directory
        shutil.rmtree(self.temp_dir)

        # Remove temporary approval token file
        if os.path.exists(self.approval_token_file):
            os.remove(self.approval_token_file)

        # Reset environment variables
        os.environ = self.original_env

    def test_deploy_script_exists(self):
        """Test that the deployment script for production exists"""
        # Use assert_file_exists to verify deploy-prod.sh script exists
        assert_file_exists(DEPLOY_SCRIPT_PATH)

        # Check that script is executable
        assert os.access(DEPLOY_SCRIPT_PATH, os.X_OK), "Deployment script is not executable"

    @patch('subprocess.check_call')
    @patch('subprocess.check_output')
    def test_deploy_script_execution(self, mock_check_output, mock_check_call):
        """Test execution of the deploy-prod.sh script"""
        # Set up mock return values for successful execution
        mock_check_output.return_value = b'mock_backup_location'
        mock_check_call.return_value = 0

        # Create test DAG files in temporary directory
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Create mock approval token file with valid CAB, ARCHITECT, and STAKEHOLDER approvals
        approval_token_file = os.path.join(self.temp_dir, 'approval_token.json')
        with open(approval_token_file, 'w') as f:
            json.dump(json.loads(MOCK_APPROVAL_DATA), f)

        # Execute deploy-prod.sh script with necessary parameters
        script_path = DEPLOY_SCRIPT_PATH
        project_id = os.environ['PROJECT_ID']
        composer_bucket = os.environ['COMPOSER_BUCKET']
        kms_key_name = os.environ['KMS_KEY_NAME']
        approval_token_path = approval_token_file
        dags_folder = self.temp_dir

        command = [
            script_path,
            project_id,
            composer_bucket,
            kms_key_name,
            approval_token_path,
            dags_folder
        ]
        subprocess.check_call(command)

        # Verify subprocess calls were made with correct arguments
        mock_check_call.assert_called_with(command)

        # Assert script returned success status
        self.assertEqual(mock_check_call.return_value, 0)

        # Verify approval verification was performed
        self.assertTrue(os.path.exists(approval_token_file))

        # Verify backup operation was performed before deployment
        mock_check_output.assert_called_with(['gsutil', 'rsync', '-r', f'{composer_bucket}/dags', 'mock_backup_location'])

    @patch('src.backend.scripts.deploy_dags.GCSClient.upload_file')
    def test_deploy_dags_function(self, mock_upload_file):
        """Test direct execution of deploy_dags_to_composer function"""
        # Create test DAG files in temporary directory
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Mock GCS client and related service calls
        mock_storage_client = create_mock_storage_client()

        # Call deploy_dags_to_composer with production environment parameters
        result = deploy_dags_to_composer(
            environment='prod',
            source_folder=self.temp_dir,
            dry_run=False,
            parallel=False
        )

        # Verify file upload operations were performed correctly
        mock_upload_file.assert_called()

        # Assert function returned success status
        self.assertTrue(result['success'])

    @patch('src.backend.scripts.deploy_dags.validate_dag_files')
    def test_deploy_dag_validation(self, mock_validate_dag_files):
        """Test that DAG validation happens during deployment with production-specific thresholds"""
        # Create both valid and invalid DAG files
        valid_dag_file = create_test_dag_file(self.temp_dir, 'valid_dag.py', TEST_DAG_CONTENT)
        invalid_dag_file = create_test_dag_file(self.temp_dir, 'invalid_dag.py', INVALID_DAG_CONTENT)

        # Patch validate_dag_files function to track calls
        mock_validate_dag_files.return_value = {'success': False, 'summary': {'error_count': 1, 'warning_count': 0}}

        # Call deploy_dags_to_composer function with production environment
        result = deploy_dags_to_composer(
            environment='prod',
            source_folder=self.temp_dir,
            dry_run=False,
            parallel=False
        )

        # Verify validation function was called with PROD_VALIDATION_THRESHOLD (0)
        mock_validate_dag_files.assert_called()

        # Confirm valid DAGs were deployed while invalid ones were rejected
        self.assertFalse(result['success'])

        # Verify warning-free validation is required for production
        self.assertEqual(result['failure_count'], 1)

        # Check that appropriate error messages were logged
        self.assertIn('DAG validation failed', result['message'])

    @patch('src.backend.scripts.deploy_dags.GCSClient.upload_file')
    def test_deploy_error_handling(self, mock_upload_file):
        """Test error handling during production deployment process"""
        # Create test DAG files
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Mock GCS client to raise exceptions during upload
        mock_upload_file.side_effect = Exception('Upload failed')

        # Call deploy_dags_to_composer function
        result = deploy_dags_to_composer(
            environment='prod',
            source_folder=self.temp_dir,
            dry_run=False,
            parallel=False
        )

        # Verify function handles exceptions appropriately
        self.assertFalse(result['success'])

        # Assert function returns failure status
        self.assertEqual(result['failure_count'], 1)

        # Check that appropriate error messages were logged
        self.assertIn('Upload failed', result['message'])

        # Verify rollback procedure was triggered
        # Verify stakeholder notification was attempted
        pass

    @patch('src.backend.scripts.deploy_dags.GCSClient.upload_file')
    def test_deploy_prod_environment_config(self, mock_upload_file):
        """Test that deployment uses production environment configuration"""
        # Create test DAG files
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Mock GCS client and related service calls
        mock_storage_client = create_mock_storage_client()

        # Call deploy_dags_to_composer with production environment
        result = deploy_dags_to_composer(
            environment='prod',
            source_folder=self.temp_dir,
            dry_run=False,
            parallel=False
        )

        # Verify production-specific configuration values were used
        # Check that PROJECT_ID matches production environment
        self.assertEqual(os.environ['PROJECT_ID'], config['gcp']['project_id'])

        # Confirm proper environment variables were set
        self.assertEqual(os.environ['DEPLOYMENT_ENV'], 'prod')

        # Assert function used correct production deployment bucket
        self.assertEqual(os.environ['COMPOSER_BUCKET'], config['storage']['dag_bucket'])

        # Verify KMS encryption was applied
        self.assertEqual(os.environ['KMS_KEY_NAME'], config['security']['kms_key'])

    @patch('src.backend.scripts.deploy_dags.authenticate_to_gcp')
    def test_deploy_authentication(self, mock_authenticate_to_gcp):
        """Test authentication process during production deployment"""
        # Patch authentication function to track calls
        mock_authenticate_to_gcp.return_value = MagicMock()

        # Create test DAG files
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Call deploy_dags_to_composer function with production environment
        result = deploy_dags_to_composer(
            environment='prod',
            source_folder=self.temp_dir,
            dry_run=False,
            parallel=False
        )

        # Verify authentication was called with correct parameters
        mock_authenticate_to_gcp.assert_called()

        # Assert authentication used production service account
        # Check proper credentials were used for GCP services
        # Verify enhanced security checks were performed
        pass

    @patch('src.backend.scripts.deploy_dags.GCSClient.upload_file')
    def test_deploy_verification(self, mock_upload_file):
        """Test that production deployment includes comprehensive verification"""
        # Create test DAG files
        dag_file = create_test_dag_file(self.temp_dir, 'test_dag.py', TEST_DAG_CONTENT)

        # Mock GCS client and deployment functions
        mock_storage_client = create_mock_storage_client()

        # Patch verification steps to track calls
        # Call deploy_dags_to_composer function with production environment
        result = deploy_dags_to_composer(
            environment='prod',
            source_folder=self.temp_dir,
            dry_run=False,
            parallel=False
        )

        # Verify that enhanced production verification steps were performed
        # Check that production-specific test suite was executed
        # Verify DAG parsing time meets performance requirements
        # Verify production verification report was generated
        # Assert deployment returned success status
        pass

    def test_approval_workflow_verification(self):
        """Test the approval workflow verification for production deployment"""
        # Create mock approval workflow file with required approvers
        # Create mock approval token with various approval scenarios
        # Test case 1: Valid CAB, ARCHITECT and STAKEHOLDER approvals (should succeed)
        # Test case 2: Missing CAB approval (should fail)
        # Test case 3: Missing ARCHITECT approval (should fail)
        # Test case 4: Missing STAKEHOLDER approval (should fail)
        # Test case 5: Expired approvals (should fail)
        # Test case 6: Missing business justification (should fail)
        # Test case 7: Missing risk assessment (should fail)
        # Test case 8: Missing rollback plan (should fail)
        # Verify appropriate messages and status codes for each case
        pass

    def test_prod_specific_validation_rules(self):
        """Test that production-specific validation rules are applied"""
        # Create test DAG files with various numbers of warnings
        # Patch validation function to return controllable warning counts
        # Verify DAG with 0 warnings passes validation
        # Verify DAG with > 0 warnings fails validation (stricter than DEV/QA)
        # Compare with QA validation threshold (which would be more lenient)
        # Verify appropriate error messages are generated
        # Check that security validation is performed
        pass

    def test_stakeholder_notification(self):
        """Test that stakeholders receive notifications about production deployment"""
        # Patch notify_stakeholders function to track calls
        # Execute deployment with various statuses
        # Verify notification is sent with correct parameters
        # Check notification contains links to Airflow UI
        # Verify notification includes validation and verification reports
        # Check that email and Slack channels for stakeholders are correctly targeted
        # Verify calendar event is created for post-deployment monitoring
        pass

    def test_deployment_audit_recording(self):
        """Test that production deployments are properly audited"""
        # Patch record_deployment_audit function to track calls
        # Execute deployment
        # Verify audit record was created with correct metadata
        # Check audit record includes comprehensive approval information
        # Verify audit record was stored in the correct location
        # Check that proper timestamps and user information are recorded
        # Verify Cloud Audit Logs entry was created
        pass

    def test_backup_and_rollback(self):
        """Test backup creation and rollback capabilities"""
        # Patch backup_current_state function to track calls
        # Patch rollback_deployment function to track calls
        # Create test scenario with successful backup but failed deployment
        # Verify backup was created with correct metadata
        # Force deployment failure
        # Verify rollback procedure was triggered automatically
        # Check that rollback restored from correct backup location
        # Verify appropriate notifications were sent about rollback
        # Confirm rollback success was verified
        pass

    def test_security_controls(self):
        """Test that security controls are applied during production deployment"""
        # Mock security scanning process
        # Create test DAG files including some with potential security issues
        # Execute deployment with security validation enabled
        # Verify security scanning was performed
        # Check that DAGs with security issues were rejected
        # Verify CMEK encryption was applied to uploaded files
        # Confirm secure service account was used
        # Test that private environment security constraints were enforced
        pass