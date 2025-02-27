"""
Package initialization file for CLI command testing modules used in validating the compatibility
of Airflow CLI commands during migration from Airflow 1.10.15 to Airflow 2.X.
This module provides common utilities and fixtures specific to CLI testing,
making them available to all test modules in the cli_tests package.
"""

import logging  # Python standard library - Configure logging for the CLI test package
import os  # Python standard library - Access environment variables and file paths for CLI testing
import subprocess  # Python standard library - Execute CLI commands in subprocesses for testing
import tempfile  # Python standard library - Create temporary files for CLI command testing
from typing import Tuple  # Typing hint for function return values

# Internal imports
from ..utils.airflow2_compatibility_utils import is_airflow2, AIRFLOW_VERSION, Airflow2CompatibilityTestMixin  # Utilities for handling Airflow version compatibility in CLI command tests
from ..utils.test_helpers import setup_test_environment, AirflowTestEnvironment  # Testing utilities for setting up controlled CLI test environments

# Configure logger
logger = logging.getLogger('airflow.test.cli')

# Define CLI test root directory
CLI_TEST_ROOT = os.path.dirname(os.path.abspath(__file__))

# Define package version
__version__ = "1.0.0"


def run_cli_command(command: str, args: list, check_return_code: bool = True) -> Tuple[int, str, str]:
    """
    Helper function to run Airflow CLI commands and capture output

    Args:
        command: The base Airflow CLI command (e.g., 'airflow')
        args: List of arguments to pass to the command
        check_return_code: If True, raise exception for non-zero return codes

    Returns:
        Tuple containing (return_code, stdout, stderr)
    """
    # Construct full command from base command and arguments
    full_command = [command] + args

    # Execute command using subprocess.run
    process = subprocess.run(
        full_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False  # Do not raise exceptions automatically
    )

    # Capture stdout and stderr
    stdout_text = process.stdout
    stderr_text = process.stderr

    # If check_return_code is True, raise exception for non-zero return codes
    if check_return_code and process.returncode != 0:
        raise Exception(f"Command '{' '.join(full_command)}' failed with return code {process.returncode}:\n{stderr_text}")

    # Return tuple of (return_code, stdout_text, stderr_text)
    return process.returncode, stdout_text, stderr_text


def create_temp_config_file(config_values: dict) -> str:
    """
    Creates a temporary config file for testing Airflow CLI commands

    Args:
        config_values: Dictionary of config values

    Returns:
        Path to the temporary config file
    """
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.cfg')

    # Write config values in Airflow config format
    for section, options in config_values.items():
        temp_file.write(f"[{section}]\n")
        for key, value in options.items():
            temp_file.write(f"{key} = {value}\n")

    # Close file to ensure data is flushed to disk
    temp_file.close()

    # Return path to the temporary file
    return temp_file.name


class CLITestCaseBase:
    """
    Base test case class for Airflow CLI command testing
    """

    def __init__(self):
        """
        Initialize the CLI test case
        """
        # Call parent constructor
        super().__init__()

        # Initialize properties with None
        self.original_env = None
        self.test_config_file = None

    def setUp(self):
        """
        Set up the test environment before each CLI test
        """
        # Set up test environment using setup_test_environment
        setup_test_environment()

        # Store original environment for restoration
        self.original_env = os.environ.copy()

        # Create test config file for CLI commands
        self.test_config_file = create_temp_config_file({
            'cli_test': {
                'test_value': 'test_config_value'
            }
        })

        # Set AIRFLOW__CORE__CONFIG_FILE_PATH environment variable
        os.environ['AIRFLOW__CORE__CONFIG_FILE_PATH'] = self.test_config_file

    def tearDown(self):
        """
        Clean up after each test
        """
        # Restore original environment variables
        if self.original_env:
            os.environ.clear()
            os.environ.update(self.original_env)

        # Remove temporary config file if it exists
        if self.test_config_file and os.path.exists(self.test_config_file):
            os.remove(self.test_config_file)

    def run_command(self, command: str, args: list, check_return_code: bool = True) -> Tuple[int, str, str]:
        """
        Run an Airflow CLI command with proper environment setup

        Args:
            command: The base Airflow CLI command (e.g., 'airflow')
            args: List of arguments to pass to the command
            check_return_code: If True, raise exception for non-zero return codes

        Returns:
            Tuple containing (return_code, stdout, stderr)
        """
        # Use run_cli_command with appropriate environment setup
        try:
            return_code, stdout_text, stderr_text = run_cli_command(command, args, check_return_code)
        except Exception as e:
            logger.error(f"Command execution failed: {str(e)}")
            raise

        # Log command execution for debugging
        logger.info(f"Executed command: {command} {' '.join(args)}")
        logger.debug(f"Stdout: {stdout_text}")
        logger.debug(f"Stderr: {stderr_text}")

        # Return command execution results
        return return_code, stdout_text, stderr_text


__all__ = [
    'run_cli_command',
    'create_temp_config_file',
    'CLITestCaseBase',
    'logger',
    'CLI_TEST_ROOT'
]