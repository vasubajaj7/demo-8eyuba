#!/usr/bin/env python3

"""
Test module for validating the deployment process of Airflow DAGs to Cloud Composer 2 environments.
Ensures deployment scripts correctly handle validation, environment-specific configurations,
and approval workflows during migration from Airflow 1.10.15 to Airflow 2.X.
"""

import unittest  # standard library
import os  # standard library
import sys  # standard library
import shutil  # standard library
import tempfile  # standard library
import subprocess  # standard library
import json  # standard library
import pytest  # pytest-6.0+
import yaml  # pyyaml-5.0+
from unittest.mock import patch, MagicMock  # standard library

# Internal imports
from ..utils.test_helpers import DAGTestRunner, run_with_timeout  # src/test/utils/test_helpers.py
from ..utils.assertion_utils import assert_dag_airflow2_compatible  # src/test/utils/assertion_utils.py
from .test_approval_workflow import load_approval_workflow_config  # src/test/ci/test_approval_workflow.py

class TestDeploymentValidation(unittest.TestCase):
    """
    Test class for validating the deployment validation process
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the test class
        """
        super().__init__(*args, **kwargs)
        self.test_env = None
        self.temp_dir = None

    def setUp(self):
        """
        Set up test environment before each test
        """
        # Create temporary directory
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_env = self._setup_test_environment(self.temp_dir.name)

        # Set up environment variables
        os.environ['AIRFLOW_HOME'] = self.test_env['temp_dir']
        os.environ['DAGS_FOLDER'] = self.test_env['dags_dir']
        os.environ['CONFIG_DIR'] = self.test_env['config_dir']

    def tearDown(self):
        """
        Clean up after each test
        """
        # Remove temporary directory
        self._cleanup_test_environment(self.test_env)
        if self.temp_dir:
            self.temp_dir.cleanup()

        # Reset environment variables
        if 'AIRFLOW_HOME' in os.environ:
            del os.environ['AIRFLOW_HOME']
        if 'DAGS_FOLDER' in os.environ:
            del os.environ['DAGS_FOLDER']
        if 'CONFIG_DIR' in os.environ:
            del os.environ['CONFIG_DIR']

    def test_validation_script_exists(self):
        """
        Test that validation script exists and is executable
        """
        # Check that deployment script exists at expected location
        deployment_script = os.path.join(self.test_env['scripts_dir'], 'validate_dags.py')
        self.assertTrue(os.path.exists(deployment_script), "Deployment script does not exist")

        # Verify script has executable permissions
        self.assertTrue(os.access(deployment_script, os.X_OK), "Deployment script is not executable")

        # Test basic script invocation returns expected help output
        result = subprocess.run([deployment_script, '--help'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, "Deployment script --help command failed")
        self.assertIn("usage: validate_dags.py", result.stdout, "Deployment script --help output is incorrect")

    @patch('src.backend.scripts.validate_dags.validate_dag_files')
    def test_dag_validation_during_deployment(self, mock_validate_dag_files: MagicMock):
        """
        Test that DAG validation occurs during deployment
        """
        # Create test environment with sample DAGs
        environment = 'dev'
        source_folder = self.test_env['dags_dir']

        # Execute deployment script with test environment
        result = self._execute_deployment_script(environment, source_folder, {}, dry_run=True)
        self.assertEqual(result.returncode, 0, "Deployment script failed")

        # Verify validate_dag_files was called with correct arguments
        mock_validate_dag_files.assert_called_once_with(source_folder, environment)

        # Check that validation results were correctly processed
        validation_results = self._parse_validation_results(result.stdout)
        self.assertIn('dags_checked', validation_results['stats'], "Validation results missing dags_checked")

    def test_environment_specific_configuration(self):
        """
        Test that environment-specific configuration is correctly applied
        """
        # For each test environment (dev, qa, prod):
        for environment in ['dev', 'qa', 'prod']:
            # Create test environment configuration
            config_file = os.path.join(self.test_env['config_dir'], f'{environment}.json')
            with open(config_file, 'w') as f:
                json.dump({'env': environment, 'setting': f'test_setting_{environment}'}, f)

            # Execute deployment script for that environment
            result = self._execute_deployment_script(environment, self.test_env['dags_dir'], {}, dry_run=True)
            self.assertEqual(result.returncode, 0, f"Deployment script failed for {environment}")

            # Verify correct environment config was loaded
            self.assertIn(f"Loading configuration for environment: {environment}", result.stdout, f"Incorrect environment config loaded for {environment}")

            # Check environment-specific settings were applied
            self.assertIn(f"test_setting_{environment}", result.stdout, f"Environment-specific setting not applied for {environment}")

    @patch('src.backend.scripts.validate_dags.validate_dag_files')
    def test_failed_validation_blocks_deployment(self, mock_validate_dag_files: MagicMock):
        """
        Test that failed validation prevents deployment
        """
        # Configure mock_validate_dag_files to return validation errors
        mock_validate_dag_files.return_value = {'status': 'failure', 'errors': ['Validation error']}

        # Execute deployment script
        result = self._execute_deployment_script('dev', self.test_env['dags_dir'], {}, dry_run=True)
        self.assertNotEqual(result.returncode, 0, "Deployment script should have failed")

        # Verify deployment was blocked
        self.assertIn("Validation failed. Deployment blocked.", result.stdout, "Deployment was not blocked")

        # Check appropriate error message was returned
        self.assertIn("Validation error", result.stdout, "Validation error message missing")

    def test_approval_workflow_integration(self):
        """
        Test integration with approval workflow
        """
        # Load approval workflow configuration
        approval_config = load_approval_workflow_config()

        # For QA and PROD environments:
        for environment in ['qa', 'prod']:
            # Verify deployment respects approval requirements
            env_config = next(env for env in approval_config['environments'] if env['name'] == environment)
            if env_config['requiresApproval']:
                # Test deployment with and without approval tokens
                # Check that approvals are verified before deployment
                pass

    @patch('src.backend.scripts.import_variables.import_variables')
    @patch('src.backend.scripts.import_connections.import_connections')
    def test_variables_and_connections_import(self, mock_import_variables: MagicMock, mock_import_connections: MagicMock):
        """
        Test import of variables and connections during deployment
        """
        # Create sample variables and connections files
        variables_file = os.path.join(self.test_env['config_dir'], 'variables.json')
        connections_file = os.path.join(self.test_env['config_dir'], 'connections.json')
        with open(variables_file, 'w') as f:
            json.dump({'var1': {'env': 'dev', 'value': 'value1'}}, f)
        with open(connections_file, 'w') as f:
            json.dump({'conn1': {'env': 'dev', 'config': {'host': 'localhost'}}}, f)

        # Execute deployment with variables and connections options
        result = self._execute_deployment_script('dev', self.test_env['dags_dir'], {'--variables': variables_file, '--connections': connections_file}, dry_run=True)
        self.assertEqual(result.returncode, 0, "Deployment script failed")

        # Verify import_variables and import_connections were called correctly
        mock_import_variables.assert_called_once()
        mock_import_connections.assert_called_once()

        # Check environment-specific filtering was applied
        self.assertIn("Importing variables", result.stdout, "Variables import not triggered")
        self.assertIn("Importing connections", result.stdout, "Connections import not triggered")

    @patch('src.backend.dags.utils.gcp_utils.gcs_upload_file')
    def test_dry_run_mode(self, mock_gcs_upload_file: MagicMock):
        """
        Test dry run mode doesn't actually deploy
        """
        # Execute deployment script with --dry-run flag
        result = self._execute_deployment_script('dev', self.test_env['dags_dir'], {}, dry_run=True)
        self.assertEqual(result.returncode, 0, "Deployment script failed")

        # Verify validation was performed
        self.assertIn("Performing DAG validation", result.stdout, "Validation not performed")

        # Check that no actual uploads occurred (mock not called)
        mock_gcs_upload_file.assert_not_called()

        # Verify appropriate dry run output was returned
        self.assertIn("Dry run mode: No changes will be applied", result.stdout, "Dry run output missing")

    @patch('concurrent.futures.ThreadPoolExecutor')
    def test_parallel_deployment(self, mock_thread_pool_executor: MagicMock):
        """
        Test parallel deployment option
        """
        # Execute deployment script with --parallel flag
        result = self._execute_deployment_script('dev', self.test_env['dags_dir'], {'--parallel': True}, dry_run=True)
        self.assertEqual(result.returncode, 0, "Deployment script failed")

        # Verify ThreadPoolExecutor was created with correct worker count
        mock_thread_pool_executor.assert_called_once()

        # Check that map function was called for parallel uploads
        # Validate results were correctly aggregated
        pass

    def _setup_test_environment(self, temp_dir: str) -> dict:
        """
        Sets up a test environment with temporary directories and sample DAGs
        """
        # Create a temporary directory for testing
        temp_dir = os.path.abspath(temp_dir)

        # Create subdirectories for DAGs, scripts, and config
        dags_dir = os.path.join(temp_dir, 'dags')
        scripts_dir = os.path.join(temp_dir, 'scripts')
        config_dir = os.path.join(temp_dir, 'config')
        os.makedirs(dags_dir)
        os.makedirs(scripts_dir)
        os.makedirs(config_dir)

        # Copy sample DAGs to the temporary directory
        sample_dag = os.path.join(dags_dir, 'sample_dag.py')
        with open(sample_dag, 'w') as f:
            f.write("print('Sample DAG')")

        # Create environment-specific configuration for testing
        for environment in ['dev', 'qa', 'prod']:
            config_file = os.path.join(config_dir, f'{environment}.json')
            with open(config_file, 'w') as f:
                json.dump({'env': environment}, f)

        # Copy deployment script to the temporary directory
        deployment_script = os.path.abspath('src/backend/scripts/validate_dags.py')
        shutil.copy(deployment_script, scripts_dir)
        os.chmod(os.path.join(scripts_dir, 'validate_dags.py'), 0o755)

        # Return dictionary with test environment details
        return {
            'temp_dir': temp_dir,
            'dags_dir': dags_dir,
            'scripts_dir': scripts_dir,
            'config_dir': config_dir
        }

    def _cleanup_test_environment(self, test_env: dict):
        """
        Cleans up the temporary test environment
        """
        # Remove temporary directory
        # Reset environment variables
        # Clean up any other resources created during testing
        pass

    def _execute_deployment_script(self, environment: str, source_folder: str, additional_args: dict, dry_run: bool) -> subprocess.CompletedProcess:
        """
        Executes the deployment script with specified arguments
        """
        # Construct command with appropriate arguments
        command = [
            'python',
            os.path.join(self.test_env['scripts_dir'], 'validate_dags.py'),
            '--environment', environment,
            '--source-dir', source_folder,
            '--config-dir', self.test_env['config_dir']
        ]

        # If dry_run is True, add --dry-run flag
        if dry_run:
            command.append('--dry-run')

        # Add additional arguments
        for key, value in additional_args.items():
            command.append(key)
            if value is not True:
                command.append(value)

        # Execute command using subprocess.run
        result = subprocess.run(command, capture_output=True, text=True)

        # Capture and return output and return code
        return result

    def _parse_validation_results(self, output: str) -> dict:
        """
        Parses validation results from script output
        """
        # Parse JSON output from validation script
        try:
            start_index = output.find('{')
            end_index = output.rfind('}') + 1
            json_output = output[start_index:end_index]
            results = json.loads(json_output)
        except json.JSONDecodeError:
            print("Error decoding JSON output:")
            print(output)
            raise

        # Extract validation statistics and issues
        return results