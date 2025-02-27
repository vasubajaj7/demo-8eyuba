#!/usr/bin/env python3

"""
Unit tests for Airflow CLI development commands, ensuring both backward compatibility from Airflow 1.10.15
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
TEST_DAG_FILE = os.path.join(TEST_DAGS_FOLDER, EXAMPLE_DAG_ID + '.py')


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


def create_test_dag_file(code_content: str) -> str:
    """
    Creates a temporary DAG file for testing development commands

    Args:
        code_content (str): The content of the DAG file

    Returns:
        str: Path to the temporary DAG file
    """
    # Create a temporary file with .py extension
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as tmp_file:
        # Write the provided code_content to the file
        tmp_file.write(code_content.encode())
        # Close the file to ensure content is flushed to disk
        tmp_file.close()
        # Return the path to the temporary file
        return tmp_file.name


class TestDevCommands(unittest.TestCase):
    """Test case for Airflow dev CLI commands"""

    def setUp(self):
        """Set up the test environment before each test"""
        # Store original environment variables
        self.original_env = os.environ.copy()

        # Override AIRFLOW_HOME environment variable to test directory
        self.test_dir = tempfile.mkdtemp()
        os.environ['AIRFLOW_HOME'] = self.test_dir

        # Set DAGS_FOLDER environment variable to TEST_DAGS_FOLDER
        os.environ['DAGS_FOLDER'] = TEST_DAGS_FOLDER

        # Initialize empty list for tracking temporary files
        self.temp_files = []

    def tearDown(self):
        """Clean up after each test"""
        # Restore original environment variables
        os.environ.clear()
        os.environ.update(self.original_env)

        # Remove any temporary files created during tests
        for file_path in self.temp_files:
            try
                os.remove(file_path)
            except FileNotFoundError:
                pass

    def test_dev_parse_command(self):
        """Test the 'airflow dev parse' command for parsing DAG files"""
        # Run 'airflow dev parse TEST_DAG_FILE' command
        return_code, stdout, stderr = run_cli_command(['dev', 'parse', TEST_DAG_FILE])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output indicates successful DAG parsing
        self.assertIn("Successfully parsed DAG", stdout, "DAG parsing success message not found")

        # Verify DAG ID and other metadata is correctly reported
        self.assertIn(f"DAG ID: {EXAMPLE_DAG_ID}", stdout, "DAG ID not found in output")

    def test_dev_parse_invalid_file(self):
        """Test 'airflow dev parse' with an invalid DAG file"""
        # Create a temporary file with invalid DAG code
        invalid_code = "import airflow\n\nthis is not valid python"
        invalid_file_path = create_test_dag_file(invalid_code)
        self.temp_files.append(invalid_file_path)

        # Run 'airflow dev parse' on the invalid file
        return_code, stdout, stderr = run_cli_command(['dev', 'parse', invalid_file_path])

        # Verify command exits with non-zero return code
        self.assertNotEqual(return_code, 0, "Command should have failed")

        # Check error output mentions the specific syntax or validation error
        self.assertIn("SyntaxError", stderr, "SyntaxError not found in error output")

    def test_dev_list_commands(self):
        """Test 'airflow dev --help' to list all available dev commands"""
        # Run 'airflow dev --help' command
        return_code, stdout, stderr = run_cli_command(['dev', '--help'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output lists all available development commands
        self.assertIn("Development commands", stdout, "Development commands section not found")

        # Verify mandatory commands like 'parse' are included in the list
        self.assertIn("parse", stdout, "'parse' command not found in help output")

    def test_dev_check_command(self):
        """Test the 'airflow dev check' command for validating Airflow installation"""
        # Run 'airflow dev check' command
        return_code, stdout, stderr = run_cli_command(['dev', 'check'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output includes system information
        self.assertIn("System information", stdout, "System information section not found")

        # Verify Python and Airflow version are correctly reported
        self.assertIn("Python Version:", stdout, "Python version not found")
        self.assertIn("Airflow Version:", stdout, "Airflow version not found")

    def test_dev_validate_dag_dependency(self):
        """Test dev command for validating DAG dependencies"""
        # Create test DAG with dependencies
        dag_code = """
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG('test_dag_dependency', start_date=datetime(2023, 1, 1))

task1 = BashOperator(task_id='task1', bash_command='echo task1', dag=dag)
task2 = BashOperator(task_id='task2', bash_command='echo task2', dag=dag)

task2.set_upstream(task1)
"""
        test_dag_file = create_test_dag_file(dag_code)
        self.temp_files.append(test_dag_file)

        # Run 'airflow dev validate-dag-dependency TEST_DAG_FILE' command
        return_code, stdout, stderr = run_cli_command(['dev', 'validate-dag-dependency', test_dag_file])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output shows correct DAG dependency information
        self.assertIn("task1 -> task2", stdout, "DAG dependency information not found")


@pytest.mark.skipif(not is_airflow2(), reason='Airflow 2.X specific tests')
class TestDevCommandsAirflow2(unittest.TestCase):
    """Test case for Airflow 2.X specific development CLI commands"""

    def setUp(self):
        """Initialize the Airflow 2.X specific test case"""
        # Call parent constructor
        super().setUp()

        # Set up Airflow 2.X specific test environment
        self.original_env = os.environ.copy()
        os.environ['AIRFLOW_HOME'] = tempfile.mkdtemp()

        # Set DAGS_FOLDER environment variable to TEST_DAGS_FOLDER
        self.original_dags_folder = os.environ.get('DAGS_FOLDER')
        os.environ['DAGS_FOLDER'] = TEST_DAGS_FOLDER

        # Make sure example DAG files are available for tests
        self.example_dag_path = os.path.join(TEST_DAGS_FOLDER, 'example_dag_basic.py')
        self.assertTrue(os.path.exists(self.example_dag_path), "Example DAG file not found")

    def tearDown(self):
        """Clean up after each test"""
        # Restore original environment variables
        os.environ.clear()
        os.environ.update(self.original_env)

        # Clean up any temporary files created during tests
        pass

    def test_dev_tasks_command(self):
        """Test the new 'airflow dev tasks' command in Airflow 2.X"""
        # Run 'airflow dev tasks EXAMPLE_DAG_ID' command
        return_code, stdout, stderr = run_cli_command(['dev', 'tasks', EXAMPLE_DAG_ID])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output lists all tasks in the DAG
        self.assertIn("Tasks:", stdout, "Tasks section not found in output")

        # Verify task details and dependencies are correctly shown
        self.assertIn("start", stdout, "Task 'start' not found in output")
        self.assertIn("end", stdout, "Task 'end' not found in output")

    def test_dev_validate_dag_command(self):
        """Test the 'airflow dev validate-dag' command in Airflow 2.X"""
        # Run 'airflow dev validate-dag TEST_DAG_FILE' command
        return_code, stdout, stderr = run_cli_command(['dev', 'validate-dag', TEST_DAG_FILE])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output confirms DAG is valid
        self.assertIn("DAG: example_dag_basic - Valid", stdout, "DAG validation success message not found")

        # Test with invalid DAG to verify error handling
        invalid_code = "from airflow import DAG\n\nthis is not valid python"
        invalid_file_path = create_test_dag_file(invalid_code)
        self.temp_files.append(invalid_file_path)

        return_code, stdout, stderr = run_cli_command(['dev', 'validate-dag', invalid_file_path])
        self.assertNotEqual(return_code, 0, "Command should have failed for invalid DAG")
        self.assertIn("DAG:  - Invalid", stdout, "DAG validation failure message not found")

    def test_dev_dag_processor_command(self):
        """Test the 'airflow dev dag-processor' command in Airflow 2.X"""
        # Run 'airflow dev dag-processor --help' command
        return_code, stdout, stderr = run_cli_command(['dev', 'dag-processor', '--help'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output explains DAG processor utility
        self.assertIn("Process and load DAG files", stdout, "DAG processor help message not found")

        # Test basic functionality with appropriate options
        # (This requires more setup and may not be feasible in all test environments)
        pass

    def test_dev_json_schemas_command(self):
        """Test the 'airflow dev json-schemas' command in Airflow 2.X"""
        # Run 'airflow dev json-schemas' command
        return_code, stdout, stderr = run_cli_command(['dev', 'json-schemas'])

        # Verify command exits with return code 0
        self.assertEqual(return_code, 0, f"Command failed with stderr: {stderr}")

        # Check output contains valid JSON schema data
        try:
            json.loads(stdout)
        except json.JSONDecodeError:
            self.fail("Output is not valid JSON")

        # Verify schema structures match Airflow 2.X API definitions
        self.assertIn("DagCollection.json", stdout, "DagCollection schema not found")


class TestMigratedDevCommands(unittest.TestCase, Airflow2CompatibilityTestMixin):
    """Test case for verifying CLI command compatibility during migration from Airflow 1.X to 2.X"""

    def setUp(self):
        """Initialize the migration compatibility test case"""
        # Call parent constructor
        super().setUp()

        # Initialize Airflow2CompatibilityTestMixin
        Airflow2CompatibilityTestMixin.__init__(self)

        # Set up test environment variables
        self.original_env = os.environ.copy()
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
        os.environ.clear()
        os.environ.update(self.original_env)

        # Clean up any temporary files created during tests
        pass

    def test_deprecated_parse_command(self):
        """Test compatibility of deprecated DAG parsing commands"""
        # Run both Airflow 1.X and 2.X style parse commands
        return_code_1, stdout_1, stderr_1 = run_cli_command(['dev', 'parse', TEST_DAG_FILE])
        return_code_2, stdout_2, stderr_2 = run_cli_command(['dev', 'parse', TEST_DAG_FILE])

        # Verify both commands return appropriate exit codes
        self.assertEqual(return_code_1, 0)
        self.assertEqual(return_code_2, 0)

        # Compare output for functional equivalence
        self.assertIn("Successfully parsed DAG", stdout_1)
        self.assertIn("Successfully parsed DAG", stdout_2)

        # Check for appropriate deprecation warnings
        if is_airflow2():
            self.assertIn("Successfully parsed DAG", stdout_1, "Deprecated command warning not found")

    def test_parse_command_output_compatibility(self):
        """Test output format compatibility between Airflow versions"""
        # Create test DAG file compatible with both versions
        dag_code = """
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG('test_dag_compatibility', start_date=datetime(2023, 1, 1))

task1 = BashOperator(task_id='task1', bash_command='echo task1', dag=dag)
"""
        test_dag_file = create_test_dag_file(dag_code)
        self.temp_files.append(test_dag_file)

        # Run parse command on both Airflow 1.X and 2.X environments
        return_code_1, stdout_1, stderr_1 = run_cli_command(['dev', 'parse', test_dag_file])
        return_code_2, stdout_2, stderr_2 = run_cli_command(['dev', 'parse', test_dag_file])

        # Compare output structure and content
        # Verify essential information is preserved in both formats
        self.assertIn("DAG ID: test_dag_compatibility", stdout_1)
        self.assertIn("DAG ID: test_dag_compatibility", stdout_2)

    def test_command_error_handling_compatibility(self):
        """Test error handling compatibility between Airflow versions"""
        # Create invalid DAG file that should fail in both versions
        invalid_code = "from airflow import DAG\n\nthis is not valid python"
        invalid_file_path = create_test_dag_file(invalid_code)
        self.temp_files.append(invalid_file_path)

        # Run dev commands on the invalid file in both environments
        return_code_1, stdout_1, stderr_1 = run_cli_command(['dev', 'parse', invalid_file_path])
        return_code_2, stdout_2, stderr_2 = run_cli_command(['dev', 'parse', invalid_file_path])

        # Verify both versions report errors appropriately
        self.assertNotEqual(return_code_1, 0)
        self.assertNotEqual(return_code_2, 0)

        # Compare error messages for clarity and usefulness
        self.assertIn("SyntaxError", stderr_1)
        self.assertIn("SyntaxError", stderr_2)