import pytest
import os
import json
import subprocess
import tempfile
import shutil
from unittest import TestCase
from unittest.mock import MagicMock, patch, call

# Global constants for test directories
TEST_DEPLOY_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), '../../ci/deploy')
TEST_CONFIGS_DIR = os.path.join(os.path.dirname(__file__), '../resources/configs')

def setup_module():
    """Setup function that runs once before any tests in the module execute, preparing the test environment."""
    # Create directories if they don't exist
    os.makedirs(TEST_CONFIGS_DIR, exist_ok=True)
    
    # Set any environment variables needed for testing
    os.environ['CI_TEST_MODE'] = 'true'
    os.environ['AIRFLOW_VERSION'] = '2.4.3'
    os.environ['COMPOSER_VERSION'] = '2.0.0'

def teardown_module():
    """Teardown function that runs once after all tests in the module have completed, cleaning up the test environment."""
    # Clean up created directories that were just for testing if they were created in this test
    if os.path.basename(TEST_CONFIGS_DIR) == 'configs' and os.path.exists(TEST_CONFIGS_DIR):
        shutil.rmtree(TEST_CONFIGS_DIR)
    
    # Reset environment variables
    if 'CI_TEST_MODE' in os.environ:
        del os.environ['CI_TEST_MODE']
    if 'AIRFLOW_VERSION' in os.environ:
        del os.environ['AIRFLOW_VERSION']
    if 'COMPOSER_VERSION' in os.environ:
        del os.environ['COMPOSER_VERSION']

def mock_deployment_script(script_name, success=True, return_data=None):
    """
    Helper function to create a mock for deployment scripts to simulate their behavior without actually running them.
    
    Args:
        script_name (str): Name of the script to mock
        success (bool): Whether the script should simulate success or failure
        return_data (dict): Data to return from the mock
        
    Returns:
        MagicMock: A configured mock object that simulates a deployment script
    """
    mock = MagicMock()
    if success:
        mock.return_value = 0 if return_data is None else return_data
    else:
        mock.side_effect = subprocess.CalledProcessError(1, script_name, "Deployment failed")
    
    return mock

def mock_approval_process(environment, approved=True):
    """
    Helper function to create a mock for the approval process to simulate different approval scenarios.
    
    Args:
        environment (str): The environment for which to mock approval
        approved (bool): Whether the approval should be granted or rejected
        
    Returns:
        MagicMock: A configured mock object that simulates an approval process
    """
    mock = MagicMock()
    if approved:
        mock.return_value = {
            "environment": environment,
            "status": "approved",
            "approvers": ["user1", "user2"],
            "timestamp": "2023-01-01T12:00:00Z",
            "comments": "Approved for deployment"
        }
    else:
        mock.return_value = {
            "environment": environment,
            "status": "rejected",
            "approvers": ["user1"],
            "timestamp": "2023-01-01T12:00:00Z",
            "comments": "Rejected due to pending changes"
        }
    
    return mock

def create_test_config_file(filename, config_data):
    """
    Creates a test configuration file with the specified content for testing environment-specific configurations.
    
    Args:
        filename (str): Name of the configuration file
        config_data (dict): Configuration data to write to the file
        
    Returns:
        str: Path to the created test configuration file
    """
    file_path = os.path.join(TEST_CONFIGS_DIR, filename)
    with open(file_path, 'w') as f:
        json.dump(config_data, f, indent=2)
    
    return file_path

def simulate_validation_tests(environment, should_pass=True):
    """
    Simulates running validation tests on a deployed environment and returns the result.
    
    Args:
        environment (str): The environment on which to simulate running tests
        should_pass (bool): Whether the tests should pass or fail
        
    Returns:
        dict: Simulated test results with success/failure status and details
    """
    results = {
        "environment": environment,
        "success": should_pass,
        "timestamp": "2023-01-01T12:30:00Z",
        "tests_executed": 15,
        "tests_passed": 15 if should_pass else 12,
        "tests_failed": 0 if should_pass else 3,
        "details": []
    }
    
    if should_pass:
        results["details"] = [
            {"name": "test_dag_loading", "status": "passed", "duration": "1.2s"},
            {"name": "test_dag_parsing", "status": "passed", "duration": "0.8s"},
            {"name": "test_connections", "status": "passed", "duration": "1.5s"}
        ]
    else:
        results["details"] = [
            {"name": "test_dag_loading", "status": "passed", "duration": "1.2s"},
            {"name": "test_dag_parsing", "status": "failed", "duration": "0.8s", "error": "Invalid DAG structure"},
            {"name": "test_connections", "status": "failed", "duration": "1.5s", "error": "Connection failed"}
        ]
    
    return results

def create_mock_approval_workflow(environment, approvers, approvals):
    """
    Creates a mock approval workflow JSON object for testing the approval process.
    
    Args:
        environment (str): The environment for the approval workflow
        approvers (list): List of approvers required
        approvals (dict): Dictionary of approver to approval status
        
    Returns:
        str: JSON string representing the approval workflow
    """
    workflow = {
        "environment": environment,
        "required_approvers": approvers,
        "min_approvals": len(approvers),
        "approvals": [{"user": user, "status": status, "timestamp": "2023-01-01T12:00:00Z"}
                      for user, status in approvals.items()],
        "workflow_id": f"approval-{environment}-123",
        "created_at": "2023-01-01T10:00:00Z",
        "expires_at": "2023-01-03T10:00:00Z"
    }
    
    return json.dumps(workflow)

class TestEnvironmentPromotion(TestCase):
    """
    Test class containing test cases for the environment promotion workflow in the CI/CD pipeline
    for Cloud Composer 2 migration.
    """
    
    def setUp(self):
        """Setup method that runs before each test method executes, preparing the test state."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Set up paths to mock deployment scripts
        self.dev_script_path = os.path.join(TEST_DEPLOY_SCRIPTS_DIR, 'deploy_to_dev.sh')
        self.qa_script_path = os.path.join(TEST_DEPLOY_SCRIPTS_DIR, 'deploy_to_qa.sh')
        self.prod_script_path = os.path.join(TEST_DEPLOY_SCRIPTS_DIR, 'deploy_to_prod.sh')
        
        # Create basic test configuration files
        create_test_config_file('dev_config.json', {
            "environment": "DEV",
            "airflow_version": "2.4.3",
            "composer_version": "2.0.0",
            "dag_folder": "dags",
            "log_level": "INFO"
        })
        
        create_test_config_file('qa_config.json', {
            "environment": "QA",
            "airflow_version": "2.4.3",
            "composer_version": "2.0.0",
            "dag_folder": "dags",
            "log_level": "INFO"
        })
        
        create_test_config_file('prod_config.json', {
            "environment": "PROD",
            "airflow_version": "2.4.3",
            "composer_version": "2.0.0",
            "dag_folder": "dags",
            "log_level": "WARNING"
        })
        
        # Set up Airflow 2.X specific configuration
        create_test_config_file('airflow2_config.json', {
            "core": {
                "dags_folder": "/home/airflow/gcs/dags",
                "executor": "CeleryExecutor",
                "load_examples": "False"
            },
            "scheduler": {
                "dag_dir_list_interval": "30",
                "parsing_processes": "2"
            },
            "webserver": {
                "web_server_name": "airflow-composer2",
                "web_server_port": "8080"
            }
        })
        
        # Set up Cloud Composer 2 specific configuration
        create_test_config_file('composer2_config.json', {
            "airflow": {
                "config_overrides": {
                    "webserver-web_server_worker_timeout": "120",
                    "celery-worker_concurrency": "8"
                }
            },
            "environment_size": "MEDIUM",
            "scheduler": {
                "cpu": "2",
                "memory_gb": "7.5",
                "storage_gb": "5",
                "count": "1"
            },
            "web_server": {
                "cpu": "0.5",
                "memory_gb": "2",
                "storage_gb": "1"
            },
            "worker": {
                "cpu": "1",
                "memory_gb": "4",
                "storage_gb": "5",
                "min_count": "2",
                "max_count": "6"
            }
        })
    
    def tearDown(self):
        """Teardown method that runs after each test method completes, cleaning up the test state."""
        # Remove the temporary directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    def test_dev_to_qa_promotion_approved(self, mock_approval, mock_run):
        """Tests the promotion process from DEV to QA environment when approval is granted."""
        # Mock DEV deployment as successful
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("QA", approved=True).return_value
        
        # Import the promotion module (imported here to allow for proper patching)
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        result = promote_environment("DEV", "QA")
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert QA deployment script was called with correct parameters
        mock_run.assert_called_with([self.qa_script_path, '--source', 'DEV', '--target', 'QA'],
                                   check=True, capture_output=True, text=True)
        
        # Verify that approval was checked
        mock_approval.assert_called_with("QA")
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    def test_dev_to_qa_promotion_rejected(self, mock_approval, mock_run):
        """Tests the promotion process from DEV to QA environment when approval is rejected."""
        # Mock DEV deployment as successful
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Mock approval process as rejected
        mock_approval.return_value = mock_approval_process("QA", approved=False).return_value
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        result = promote_environment("DEV", "QA")
        
        # Assert that the promotion was unsuccessful
        self.assertFalse(result)
        
        # Assert that deployment wasn't attempted since approval was rejected
        mock_run.assert_not_called()
        
        # Verify that approval was checked
        mock_approval.assert_called_with("QA")
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    @patch('ci.approval.get_stakeholder_approval')
    def test_qa_to_prod_promotion_approved(self, mock_stakeholder_approval, mock_cab_approval, mock_run):
        """Tests the promotion process from QA to PROD environment when CAB approval is granted."""
        # Mock QA deployment as successful
        qa_result = MagicMock()
        qa_result.returncode = 0
        mock_run.return_value = qa_result
        
        # Mock CAB approval process as successful
        mock_cab_approval.return_value = mock_approval_process("PROD", approved=True).return_value
        
        # Mock stakeholder approval as successful
        mock_stakeholder_approval.return_value = {
            "environment": "PROD",
            "status": "approved",
            "approvers": ["architect", "product_owner"],
            "timestamp": "2023-01-01T12:00:00Z",
            "comments": "Approved for production deployment"
        }
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        result = promote_environment("QA", "PROD")
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert PROD deployment script was called with correct parameters
        mock_run.assert_called_with([self.prod_script_path, '--source', 'QA', '--target', 'PROD'],
                                   check=True, capture_output=True, text=True)
        
        # Verify that approvals were checked
        mock_cab_approval.assert_called_with("PROD")
        mock_stakeholder_approval.assert_called_with("PROD")
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    @patch('ci.approval.get_stakeholder_approval')
    def test_qa_to_prod_promotion_rejected(self, mock_stakeholder_approval, mock_cab_approval, mock_run):
        """Tests the promotion process from QA to PROD environment when CAB approval is rejected."""
        # Mock QA deployment as successful
        qa_result = MagicMock()
        qa_result.returncode = 0
        mock_run.return_value = qa_result
        
        # Mock CAB approval process as rejected
        mock_cab_approval.return_value = mock_approval_process("PROD", approved=False).return_value
        
        # Mock stakeholder approval (should not be called if CAB rejects)
        mock_stakeholder_approval.return_value = None
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        result = promote_environment("QA", "PROD")
        
        # Assert that the promotion was unsuccessful
        self.assertFalse(result)
        
        # Assert that deployment wasn't attempted since approval was rejected
        mock_run.assert_not_called()
        
        # Verify that CAB approval was checked but stakeholder approval wasn't
        mock_cab_approval.assert_called_with("PROD")
        mock_stakeholder_approval.assert_not_called()
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    @patch('ci.approval.get_stakeholder_approval')
    def test_stakeholder_approval_required_for_prod(self, mock_stakeholder_approval, mock_cab_approval, mock_run):
        """Tests that both CAB and stakeholder approvals are required for production deployment."""
        # Mock QA deployment as successful
        qa_result = MagicMock()
        qa_result.returncode = 0
        mock_run.return_value = qa_result
        
        # Mock CAB approval process as successful
        mock_cab_approval.return_value = mock_approval_process("PROD", approved=True).return_value
        
        # Mock stakeholder approval as rejected
        mock_stakeholder_approval.return_value = {
            "environment": "PROD",
            "status": "rejected",
            "approvers": ["architect"],
            "timestamp": "2023-01-01T12:00:00Z",
            "comments": "Architecture review pending"
        }
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        result = promote_environment("QA", "PROD")
        
        # Assert that the promotion was unsuccessful
        self.assertFalse(result)
        
        # Assert that deployment wasn't attempted since stakeholder approval was rejected
        mock_run.assert_not_called()
        
        # Verify that both approvals were checked
        mock_cab_approval.assert_called_with("PROD")
        mock_stakeholder_approval.assert_called_with("PROD")
    
    @patch('subprocess.run')
    @patch('ci.promotion.promote_environment')
    def test_direct_dev_to_prod_promotion_blocked(self, mock_promote, mock_run):
        """Tests that direct promotion from DEV to PROD is not allowed, enforcing the required staging through QA."""
        # Set up mocks for the deployment scripts
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Import the promotion controller
        from ci.promotion_controller import execute_promotion
        
        # Execute direct promotion from DEV to PROD
        with self.assertRaises(ValueError) as context:
            execute_promotion("DEV", "PROD")
        
        # Verify that an error was raised
        self.assertTrue("Direct promotion from DEV to PROD is not allowed" in str(context.exception))
        
        # Verify that the promote_environment function was not called
        mock_promote.assert_not_called()
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    def test_failed_deployment_stops_promotion(self, mock_approval, mock_run):
        """Tests that a failed deployment in one environment prevents promotion to the next environment."""
        # Mock DEV deployment as failed
        mock_run.side_effect = subprocess.CalledProcessError(1, "deploy_to_dev.sh", "Deployment failed")
        
        # Mock approval process (should not be called if deployment fails)
        mock_approval.return_value = None
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        with self.assertRaises(subprocess.CalledProcessError):
            promote_environment("DEV", "QA")
        
        # Verify that approval was not checked since deployment failed
        mock_approval.assert_not_called()
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    def test_environment_specific_config_applied(self, mock_approval, mock_run):
        """Tests that environment-specific configurations are correctly applied during promotion."""
        # Mock DEV deployment as successful
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("QA", approved=True).return_value
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        result = promote_environment("DEV", "QA")
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert that deployment was called with environment-specific configs
        mock_run.assert_called_with([self.qa_script_path, '--source', 'DEV', '--target', 'QA'],
                                   check=True, capture_output=True, text=True)
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    @patch('ci.compatibility.check_airflow_compatibility')
    def test_airflow_version_compatibility_checked(self, mock_compatibility_check, mock_approval, mock_run):
        """Tests that Airflow version compatibility is verified during the promotion process."""
        # Mock DEV deployment as successful
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("QA", approved=True).return_value
        
        # Mock compatibility check as successful
        mock_compatibility_check.return_value = True
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        result = promote_environment("DEV", "QA", check_compatibility=True)
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert compatibility check was called
        mock_compatibility_check.assert_called_once()
        
        # Assert that deployment proceeded after successful compatibility check
        mock_run.assert_called_with([self.qa_script_path, '--source', 'DEV', '--target', 'QA'],
                                   check=True, capture_output=True, text=True)
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    @patch('ci.validation.run_validation_tests')
    def test_validation_tests_executed_after_promotion(self, mock_validation, mock_approval, mock_run):
        """Tests that validation tests are executed after a successful promotion to verify the deployment."""
        # Mock QA deployment as successful
        qa_result = MagicMock()
        qa_result.returncode = 0
        mock_run.return_value = qa_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("PROD", approved=True).return_value
        
        # Mock validation tests as successful
        mock_validation.return_value = simulate_validation_tests("PROD", should_pass=True)
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow with validation
        result = promote_environment("QA", "PROD", run_validation=True)
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert validation tests were executed
        mock_validation.assert_called_with("PROD")
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    @patch('ci.validation.run_validation_tests')
    @patch('ci.rollback.rollback_deployment')
    def test_rollback_on_validation_failure(self, mock_rollback, mock_validation, mock_approval, mock_run):
        """Tests that a rollback is performed if validation tests fail after promotion."""
        # Mock QA deployment as successful
        qa_result = MagicMock()
        qa_result.returncode = 0
        mock_run.return_value = qa_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("PROD", approved=True).return_value
        
        # Mock validation tests as failed
        mock_validation.return_value = simulate_validation_tests("PROD", should_pass=False)
        
        # Mock rollback as successful
        mock_rollback.return_value = True
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow with validation
        result = promote_environment("QA", "PROD", run_validation=True, auto_rollback=True)
        
        # Assert that the promotion ultimately failed due to validation
        self.assertFalse(result)
        
        # Assert validation tests were executed
        mock_validation.assert_called_with("PROD")
        
        # Assert rollback was called
        mock_rollback.assert_called_with("PROD", "QA")
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    @patch('ci.notification.send_notification')
    def test_notification_sent_on_promotion_events(self, mock_notification, mock_approval, mock_run):
        """Tests that notifications are sent on important promotion events (success, failure, approval, rejection)."""
        # Mock DEV deployment as successful
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("QA", approved=True).return_value
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow with notifications
        result = promote_environment("DEV", "QA", send_notifications=True)
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert notifications were sent (at least 2 notifications should be sent: on approval and on successful deployment)
        self.assertGreaterEqual(mock_notification.call_count, 2)
        
        # Check for approval notification
        mock_notification.assert_any_call(
            "QA Deployment Approved",
            f"Deployment to QA environment has been approved. Proceeding with deployment.",
            "approval"
        )
        
        # Check for success notification
        mock_notification.assert_any_call(
            "QA Deployment Successful",
            f"Deployment to QA environment completed successfully.",
            "success"
        )
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    @patch('ci.history.log_promotion')
    def test_promotion_history_logged(self, mock_history_log, mock_approval, mock_run):
        """Tests that promotion history is properly logged for audit and tracking."""
        # Mock DEV deployment as successful
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("QA", approved=True).return_value
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow
        result = promote_environment("DEV", "QA")
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert history was logged
        mock_history_log.assert_called_once()
        
        # Check that the correct environments were included in the log
        args, kwargs = mock_history_log.call_args
        self.assertEqual(args[0], "DEV")
        self.assertEqual(args[1], "QA")
        self.assertTrue(args[2])  # success status
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    def test_airflow2_specific_configurations(self, mock_approval, mock_run):
        """Tests that Airflow 2.X specific configurations are properly applied during environment promotion."""
        # Mock DEV deployment as successful
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("QA", approved=True).return_value
        
        # Create custom config file path for testing
        config_path = os.path.join(self.temp_dir, 'airflow2_test_config.json')
        
        # Create test configuration file with Airflow 2.X specific settings
        with open(config_path, 'w') as f:
            json.dump({
                "environment": "QA",
                "airflow_version": "2.4.3",
                "core": {
                    "dags_folder": "/home/airflow/gcs/dags",
                    "executor": "CeleryExecutor",
                    "load_examples": "False",
                    "dag_discovery_safe_mode": "True"  # Airflow 2.X specific
                },
                "scheduler": {
                    "standalone_dag_processor": "True",  # Airflow 2.X specific
                    "parsing_processes": "2"
                },
                "task_runner": {
                    "log_retention_count": "5"  # Airflow 2.X specific
                }
            }, f)
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow with custom config
        result = promote_environment("DEV", "QA", config_path=config_path)
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert that the deployment script was called with the custom config path
        mock_run.assert_called_with([self.qa_script_path, '--source', 'DEV', '--target', 'QA', '--config', config_path],
                                   check=True, capture_output=True, text=True)
    
    @patch('subprocess.run')
    @patch('ci.approval.get_approval_status')
    def test_composer2_specific_configurations(self, mock_approval, mock_run):
        """Tests that Cloud Composer 2 specific configurations are properly applied during environment promotion."""
        # Mock DEV deployment as successful
        dev_result = MagicMock()
        dev_result.returncode = 0
        mock_run.return_value = dev_result
        
        # Mock approval process as successful
        mock_approval.return_value = mock_approval_process("QA", approved=True).return_value
        
        # Create custom config file path for testing
        config_path = os.path.join(self.temp_dir, 'composer2_test_config.json')
        
        # Create test configuration file with Cloud Composer 2 specific settings
        with open(config_path, 'w') as f:
            json.dump({
                "environment": "QA",
                "composer_version": "2.0.0",
                "environment_size": "MEDIUM",
                "node_config": {
                    "service_account": "composer-service-account@project.iam.gserviceaccount.com",
                    "enable_ip_masq_agent": True
                },
                "workloads_config": {  # Composer 2 specific
                    "scheduler": {
                        "cpu": "2",
                        "memory_gb": "7.5",
                        "storage_gb": "5",
                        "count": "1"
                    },
                    "web_server": {
                        "cpu": "0.5",
                        "memory_gb": "2",
                        "storage_gb": "1"
                    },
                    "worker": {
                        "cpu": "1",
                        "memory_gb": "4",
                        "storage_gb": "5",
                        "min_count": "2",
                        "max_count": "6"
                    }
                }
            }, f)
        
        # Import the promotion module
        from ci.promotion import promote_environment
        
        # Execute the promotion workflow with custom config
        result = promote_environment("DEV", "QA", config_path=config_path)
        
        # Assert that the promotion was successful
        self.assertTrue(result)
        
        # Assert that the deployment script was called with the custom config path
        mock_run.assert_called_with([self.qa_script_path, '--source', 'DEV', '--target', 'QA', '--config', config_path],
                                   check=True, capture_output=True, text=True)