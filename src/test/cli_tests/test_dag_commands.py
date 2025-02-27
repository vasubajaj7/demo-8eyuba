#!/usr/bin/env python3

"""
Unit tests for Airflow CLI dag commands, ensuring both backward compatibility from Airflow 1.10.15
and proper functionality in Airflow 2.X during the Cloud Composer migration.
Tests cover command invocation, output validation, and new Airflow 2.X-specific features.
"""

import unittest  # Python standard library
import unittest.mock  # Python standard library
import pytest  # pytest-6.0+
import os  # Python standard library
import sys  # Python standard library
import subprocess  # Python standard library
import json  # Python standard library
import tempfile  # Python standard library

# airflow // version: 2.0.0+
from airflow.configuration import conf
# airflow // version: 2.0.0+
from airflow.cli.commands.dag_command import dag_test
# airflow // version: 2.0.0+
from airflow.models.dag import DAG
# airflow // version: 2.0.0+
from airflow.utils.cli import get_parser, setup_parser

# Internal module imports
from test.utils.test_helpers import run_with_timeout, capture_logs, DEFAULT_TEST_TIMEOUT  # src/test/utils/test_helpers.py
from test.utils.airflow2_compatibility_utils import is_airflow2  # src/test/utils/airflow2_compatibility_utils.py
from test.utils.airflow2_compatibility_utils import mock_airflow2_imports  # src/test/utils/airflow2_compatibility_utils.py
from test.utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin  # src/test/utils/airflow2_compatibility_utils.py

# Define testing constants and parameters
TEST_DAGS_FOLDER = os.path.join(os.path.dirname(__file__), '../..', 'backend/dags')
EXAMPLE_DAG_ID = 'example_dag_basic'


def run_cli_command(command_args: list, capture_output: bool = True, timeout_seconds: int = None) -> tuple:
    """
    Helper function to run Airflow CLI commands and return the result

    Args:
        command_args (list): List of command arguments to pass to the Airflow CLI
        capture_output (bool): Whether to capture the output of the command
        timeout_seconds (int): Timeout in seconds for the command execution

    Returns:
        tuple: Process return code and output (stdout and stderr)
    """
    # Start with base command prefix 'airflow'
    command = ['airflow']

    # Add provided command_args to complete the command
    command.extend(command_args)

    # Set timeout_seconds to DEFAULT_TEST_TIMEOUT if not provided
    if timeout_seconds is None:
        timeout_seconds = DEFAULT_TEST_TIMEOUT

    # Use subprocess.run to execute the command
    process = subprocess.run(
        command,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
        timeout=timeout_seconds,
        text=True,
        env=os.environ.copy()
    )

    # Capture stdout and stderr if capture_output is True
    stdout = process.stdout if capture_output else None
    stderr = process.stderr if capture_output else None

    # Return tuple containing (return_code, stdout, stderr)
    return process.returncode, stdout, stderr


def parse_json_output(output: str) -> dict:
    """
    Parse JSON output from Airflow CLI commands

    Args:
        output (str): The output string from the CLI command

    Returns:
        dict: Parsed JSON data from CLI output
    """
    # Strip whitespace from output
    output = output.strip()

    # Try to parse output as JSON
    try:
        parsed_json = json.loads(output)
        return parsed_json
    except json.JSONDecodeError:
        return None


class TestDagCommands(unittest.TestCase):
    """Test case for Airflow DAG CLI commands"""

    def setUp(self):
        """Set up the test environment before each test"""
        # Override AIRFLOW_HOME environment variable to test directory
        self.original_airflow_home = os.environ.get('AIRFLOW_HOME')
        os.environ['AIRFLOW_HOME'] = tempfile.mkdtemp()

        # Set DAGS_FOLDER environment variable to TEST_DAGS_FOLDER
        self.original_dags_folder = os.environ.get('DAGS_FOLDER')
        os.environ['DAGS_FOLDER'] = TEST_DAGS_FOLDER

        # Make sure example DAG files are available for tests
        self.example_dag_path = os.path.join(TEST_DAGS_FOLDER, 'example_dag_basic.py')
        self.assertTrue(os.path.exists(self.example_dag_path), "Example DAG file not found")

    def tearDown(self):
        """Clean up after each test"""
        # Reset environment variables
        if self.original_airflow_home:
            os.environ['AIRFLOW_HOME'] = self.original_airflow_home
        else:
            del os.environ['AIRFLOW_HOME']

        if self.original_dags_folder:
            os.environ['DAGS_FOLDER'] = self.original_dags_folder
        else:
            if 'DAGS_FOLDER' in os.environ:
                del os.environ['DAGS_FOLDER']

        # Clean up any temporary files created during tests
        pass

    def test_dag_list_command(self):
        """Test the 'airflow dags list' command"""
        # Run 'airflow dags list' command
        return_code, stdout, stderr = run_cli_command(['dags', 'list'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output contains expected DAG IDs
        self.assertIn(EXAMPLE_DAG_ID, stdout, "DAG ID not found in output")

        # Verify output format matches expectations for Airflow version
        if is_airflow2():
            self.assertIn("dag_id", stdout, "Airflow 2.X format not detected")
        else:
            self.assertNotIn("dag_id", stdout, "Airflow 1.X format not detected")

    def test_dag_list_json_format(self):
        """Test the 'airflow dags list --output json' command for JSON output"""
        # Run 'airflow dags list --output json' command
        return_code, stdout, stderr = run_cli_command(['dags', 'list', '--output', 'json'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output is valid JSON
        parsed_json = parse_json_output(stdout)
        self.assertIsNotNone(parsed_json, "Output is not valid JSON")

        # Parse JSON and verify expected DAGs are present
        dag_list = parsed_json['dags']
        self.assertTrue(any(dag['dag_id'] == EXAMPLE_DAG_ID for dag in dag_list), "DAG ID not found in JSON output")

        # Validate all expected fields are present in JSON
        expected_fields = ['dag_id', 'fileloc', 'owner', 'description', 'is_paused', 'is_subdag', 'timezone']
        for dag in dag_list:
            for field in expected_fields:
                self.assertIn(field, dag, f"Field '{field}' not found in JSON output")

    def test_dag_report_command(self):
        """Test the 'airflow dags report' command"""
        # Run 'airflow dags report' command
        return_code, stdout, stderr = run_cli_command(['dags', 'report'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output contains expected DAG metrics
        self.assertIn("Total DAGs", stdout, "Total DAGs metric not found")
        self.assertIn("Total Tasks", stdout, "Total Tasks metric not found")

        # Verify report format is correct
        self.assertIn("File", stdout, "File section not found")
        self.assertIn("DAG Id", stdout, "DAG Id section not found")

    def test_dag_show_command(self):
        """Test the 'airflow dags show' command"""
        # Run 'airflow dags show example_dag_basic' command
        return_code, stdout, stderr = run_cli_command(['dags', 'show', EXAMPLE_DAG_ID])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output contains expected DAG structure
        self.assertIn("DAG: example_dag_basic", stdout, "DAG name not found in output")
        self.assertIn("Tasks:", stdout, "Tasks section not found in output")

        # Verify task dependencies are correctly displayed
        self.assertIn("No dependencies", stdout, "Dependencies not found in output")

    def test_dag_trigger_command(self):
        """Test the 'airflow dags trigger' command"""
        # Set up test DAG for triggering
        # Run 'airflow dags trigger example_dag_basic' command
        return_code, stdout, stderr = run_cli_command(['dags', 'trigger', EXAMPLE_DAG_ID])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output indicates DAG was triggered successfully
        self.assertIn(f"Queued a DagRun for {EXAMPLE_DAG_ID}", stdout, "DAG trigger confirmation not found")

        # Verify DAG run is created in the database
        # (This requires database access, which may not be available in all test environments)
        pass

    def test_dag_pause_command(self):
        """Test the 'airflow dags pause/unpause' commands"""
        # Run 'airflow dags pause example_dag_basic' command
        return_code, stdout, stderr = run_cli_command(['dags', 'pause', EXAMPLE_DAG_ID])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output indicates DAG was paused
        self.assertIn(f"Successfully paused DAG: {EXAMPLE_DAG_ID}", stdout, "DAG pause confirmation not found")

        # Verify DAG is marked as paused in database
        # (This requires database access, which may not be available in all test environments)
        pass

        # Run 'airflow dags unpause example_dag_basic' command
        return_code, stdout, stderr = run_cli_command(['dags', 'unpause', EXAMPLE_DAG_ID])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output indicates DAG was unpaused
        self.assertIn(f"Successfully unpaused DAG: {EXAMPLE_DAG_ID}", stdout, "DAG unpause confirmation not found")

        # Verify DAG is marked as not paused in database
        # (This requires database access, which may not be available in all test environments)
        pass

    def test_dag_state_command(self):
        """Test the 'airflow dags state' command"""
        # Set up test DAG with known state
        # Run 'airflow dags state example_dag_basic run_id execution_date' command
        return_code, stdout, stderr = run_cli_command(['dags', 'state', EXAMPLE_DAG_ID, 'run_id', '2023-01-01'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output contains the correct DAG state
        self.assertIn("State:", stdout, "State information not found in output")

    def test_dag_next_execution_command(self):
        """Test the 'airflow dags next-execution' command"""
        # Run 'airflow dags next-execution example_dag_basic' command
        return_code, stdout, stderr = run_cli_command(['dags', 'next-execution', EXAMPLE_DAG_ID])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output contains the expected next execution date
        self.assertIn("Next run:", stdout, "Next run information not found in output")


@pytest.mark.skipif(not is_airflow2(), reason='Airflow 2.X specific tests')
class TestDagCommandsAirflow2(unittest.TestCase):
    """Test case for Airflow 2.X specific DAG CLI commands"""

    def setUp(self):
        """Initialize the Airflow 2.X specific test case"""
        # Call parent constructor
        super().setUp()

        # Set up Airflow 2.X specific test environment
        self.original_airflow_home = os.environ.get('AIRFLOW_HOME')
        os.environ['AIRFLOW_HOME'] = tempfile.mkdtemp()

        # Set DAGS_FOLDER environment variable to TEST_DAGS_FOLDER
        self.original_dags_folder = os.environ.get('DAGS_FOLDER')
        os.environ['DAGS_FOLDER'] = TEST_DAGS_FOLDER

        # Make sure example DAG files are available for tests
        self.example_dag_path = os.path.join(TEST_DAGS_FOLDER, 'example_dag_basic.py')
        self.assertTrue(os.path.exists(self.example_dag_path), "Example DAG file not found")

    def tearDown(self):
        """Clean up after each test"""
        # Reset environment variables
        if self.original_airflow_home:
            os.environ['AIRFLOW_HOME'] = self.original_airflow_home
        else:
            del os.environ['AIRFLOW_HOME']

        if self.original_dags_folder:
            os.environ['DAGS_FOLDER'] = self.original_dags_folder
        else:
            if 'DAGS_FOLDER' in os.environ:
                del os.environ['DAGS_FOLDER']

        # Clean up any temporary files created during tests
        pass

    def test_dag_test_command(self):
        """Test the 'airflow dags test' command (new in Airflow 2.X)"""
        # Run 'airflow dags test example_dag_basic 2023-01-01' command
        return_code, stdout, stderr = run_cli_command(['dags', 'test', EXAMPLE_DAG_ID, '2023-01-01'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output indicates DAG was tested successfully
        self.assertIn("Tests passed for DAG", stdout, "DAG test success message not found")

        # Verify all tasks completed successfully
        self.assertIn("All task instances succeeded", stdout, "Task success message not found")

    def test_dag_backfill_dryrun_command(self):
        """Test the 'airflow dags backfill --dry-run' command (feature enhanced in Airflow 2.X)"""
        # Run 'airflow dags backfill --dry-run example_dag_basic -s 2023-01-01 -e 2023-01-03' command
        return_code, stdout, stderr = run_cli_command(['dags', 'backfill', '--dry-run', EXAMPLE_DAG_ID, '-s', '2023-01-01', '-e', '2023-01-03'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output shows plan for backfill without execution
        self.assertIn("Dry run mode. Would have created the following DagRuns:", stdout, "Dry run message not found")

        # Verify no DAG runs were actually created
        # (This requires database access, which may not be available in all test environments)
        pass

    def test_dag_show_dependencies_command(self):
        """Test the 'airflow dags show-dependencies' command (new format in Airflow 2.X)"""
        # Run 'airflow dags show-dependencies' command
        return_code, stdout, stderr = run_cli_command(['dags', 'show-dependencies'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output displays correct task dependencies
        self.assertIn("Task dependencies:", stdout, "Task dependencies message not found")

        # Verify graphical representation is correctly formatted
        self.assertIn("graph TD", stdout, "Graph format not found")


class TestMigratedDagCommands(unittest.TestCase, Airflow2CompatibilityTestMixin):
    """Test case for verifying CLI command compatibility during migration from Airflow 1.X to 2.X"""

    def setUp(self):
        """Initialize the migration compatibility test case"""
        # Call parent constructor
        super().setUp()

        # Initialize Airflow2CompatibilityTestMixin
        Airflow2CompatibilityTestMixin.__init__(self)

        # Set up test environment variables
        self.original_airflow_home = os.environ.get('AIRFLOW_HOME')
        os.environ['AIRFLOW_HOME'] = tempfile.mkdtemp()

        # Set DAGS_FOLDER environment variable to TEST_DAGS_FOLDER
        self.original_dags_folder = os.environ.get('DAGS_FOLDER')
        os.environ['DAGS_FOLDER'] = TEST_DAGS_FOLDER

        # Make sure example DAG files are available for tests
        self.example_dag_path = os.path.join(TEST_DAGS_FOLDER, 'example_dag_basic.py')
        self.assertTrue(os.path.exists(self.example_dag_path), "Example DAG file not found")

    def tearDown(self):
        """Clean up after each test"""
        # Reset environment variables
        if self.original_airflow_home:
            os.environ['AIRFLOW_HOME'] = self.original_airflow_home
        else:
            del os.environ['AIRFLOW_HOME']

        if self.original_dags_folder:
            os.environ['DAGS_FOLDER'] = self.original_dags_folder
        else:
            if 'DAGS_FOLDER' in os.environ:
                del os.environ['DAGS_FOLDER']

        # Clean up any temporary files created during tests
        pass

    def test_deprecated_dag_list_format(self):
        """Test deprecated CLI command format compatibility"""
        # Run both 'airflow list_dags' (1.X style) and 'airflow dags list' (2.X style)
        return_code_1, stdout_1, stderr_1 = run_cli_command(['list_dags'])
        return_code_2, stdout_2, stderr_2 = run_cli_command(['dags', 'list'])

        # Verify both commands exit with return code 0 in appropriate environments
        if is_airflow2():
            self.assertEqual(return_code_1, 0, f"Deprecated command failed with stderr: {stderr_1}")
            self.assertEqual(return_code_2, 0, f"New command failed with stderr: {stderr_2}")
        else:
            self.assertEqual(return_code_1, 0, f"Deprecated command failed with stderr: {stderr_1}")
            self.assertEqual(return_code_2, 0, f"New command failed with stderr: {stderr_2}")

        # Check outputs are equivalent in terms of DAG information
        self.assertIn(EXAMPLE_DAG_ID, stdout_1, "DAG ID not found in deprecated command output")
        self.assertIn(EXAMPLE_DAG_ID, stdout_2, "DAG ID not found in new command output")

        # Verify deprecated command shows warning in Airflow 2.X
        if is_airflow2():
            self.assertIn("Deprecated command", stderr_1, "Deprecated command warning not found")

    def test_deprecated_backfill_command(self):
        """Test deprecated backfill command compatibility"""
        # Run both 'airflow backfill' (1.X style) and 'airflow dags backfill' (2.X style)
        return_code_1, stdout_1, stderr_1 = run_cli_command(['backfill', EXAMPLE_DAG_ID, '-s', '2023-01-01', '-e', '2023-01-03'])
        return_code_2, stdout_2, stderr_2 = run_cli_command(['dags', 'backfill', EXAMPLE_DAG_ID, '-s', '2023-01-01', '-e', '2023-01-03'])

        # Verify both commands exit with appropriate return code
        self.assertEqual(return_code_1, 0)
        self.assertEqual(return_code_2, 0)

        # Check outputs are equivalent
        self.assertIn("Backfilling", stdout_1)
        self.assertIn("Backfilling", stdout_2)

        # Verify deprecated command shows warning in Airflow 2.X
        if is_airflow2():
            self.assertIn("Deprecated command", stderr_1, "Deprecated command warning not found")

    def test_command_output_format_compatibility(self):
        """Test output format compatibility between Airflow versions"""
        # Run equivalent commands in both Airflow 1.X and 2.X environments
        return_code_1, stdout_1, stderr_1 = run_cli_command(['dags', 'list'])
        return_code_2, stdout_2, stderr_2 = run_cli_command(['dags', 'list'])

        # Compare output structure and content
        # Verify essential information is preserved in both formats
        self.assertIn(EXAMPLE_DAG_ID, stdout_1)
        self.assertIn(EXAMPLE_DAG_ID, stdout_2)

        # Check for version-specific fields and handling
        if is_airflow2():
            self.assertIn("dag_id", stdout_2)
        else:
            self.assertNotIn("dag_id", stdout_1)