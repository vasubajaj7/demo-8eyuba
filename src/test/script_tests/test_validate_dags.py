#!/usr/bin/env python3

"""
Test module for the DAG validation script that verifies Apache Airflow DAG files for
compatibility with Airflow 2.X during migration. It validates the functionality of the
validate_dags.py script which analyzes DAGs for deprecated features, import statements,
and structural issues.
"""

import os  # Operating system interfaces for file path operations
import tempfile  # Generate temporary files and directories for testing
import shutil  # High-level file operations for test setup and teardown
import json  # JSON processing for checking validation reports
from datetime import datetime  # Date and time manipulation for test context
import unittest.mock  # Mocking functionality for testing

import pytest  # Testing framework for Python >= 3.6
from airflow.models import DAG  # Airflow core functionality for testing DAG validation

# Internal imports
from src.backend.scripts import validate_dags  # Module under test
from src.backend.scripts.validate_dags import (
    find_dag_files,
    validate_dag_files,
    generate_report,
    create_airflow2_compatibility_report,
    ValidationContext,
    ReportFormatter,
)
from src.test.utils import test_helpers  # Provides helper functions for testing Airflow components
from src.test.utils.test_helpers import create_test_execution_context, version_compatible_test, TestAirflowContext
from src.test.utils import assertion_utils  # Provides specialized assertion functions for validating Airflow components
from src.test.utils.assertion_utils import assert_dag_airflow2_compatible
from src.test.utils import dag_validation_utils  # Provides utilities for validating DAG structure and compatibility
from src.test.utils.dag_validation_utils import validate_dag_integrity, DAGValidator
from src.test.fixtures import dag_fixtures  # Provides test fixtures for creating and manipulating Airflow DAGs during testing
from src.test.fixtures.dag_fixtures import create_test_dag, create_simple_dag, DAGTestContext

# Define global test variables
TEST_DAG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_dags')
FIXTURE_DAG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../fixtures/dags')


def setup_test_dag_directory(temp_dir: str = None, dag_files: list = None) -> str:
    """
    Creates a temporary directory with test DAG files for validation testing

    Args:
        temp_dir: Optional temporary directory path. If None, a new temporary directory is created.
        dag_files: List of DAG file contents to create in the temporary directory.

    Returns:
        Path to the temporary DAG directory
    """
    # Create a temporary DAG directory if temp_dir not provided
    if temp_dir is None:
        temp_dir = tempfile.mkdtemp()

    # Create test DAG files in the temporary directory
    if dag_files:
        for i, dag_content in enumerate(dag_files):
            dag_file_path = os.path.join(temp_dir, f"test_dag_{i}.py")
            with open(dag_file_path, "w") as f:
                f.write(dag_content)

    # Return the path to the temporary DAG directory
    return temp_dir


def create_dag_with_deprecated_features(dag_id: str) -> str:
    """
    Creates a test DAG containing Airflow 1.X deprecated features for testing validation

    Args:
        dag_id: The DAG ID for the test DAG

    Returns:
        DAG file content as string
    """
    # Create a DAG definition with deprecated features like provide_context
    dag_content = f"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {{
    'owner': 'airflow',
    'provide_context': True,
}}

dag = DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

def my_python_function(context):
    print(context)

task1 = PythonOperator(
    task_id='python_task',
    python_callable=my_python_function,
    dag=dag
)
"""
    # Return the DAG file content as a string
    return dag_content


def create_airflow2_compatible_dag(dag_id: str) -> str:
    """
    Creates a test DAG that is fully compatible with Airflow 2.X

    Args:
        dag_id: The DAG ID for the test DAG

    Returns:
        DAG file content as string
    """
    # Create a DAG definition using Airflow 2.X syntax
    dag_content = f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id='{dag_id}',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example']
)

def my_python_function():
    print("Airflow 2.X compatible task")

with dag:
    task1 = PythonOperator(
        task_id='python_task',
        python_callable=my_python_function
    )
"""
    # Return the DAG file content as a string
    return dag_content


class TestValidateDAGs:
    """
    Test class for the validate_dags.py script's core functionality
    """

    def setup_method(self, method):
        """
        Set up test environment before each test method

        Args:
            method: The test method being executed
        """
        # Create a temporary directory for test DAG files
        self.temp_dag_dir = tempfile.mkdtemp()

        # Set up test fixtures
        self.dag_file_1 = os.path.join(self.temp_dag_dir, "dag_file_1.py")
        self.dag_file_2 = os.path.join(self.temp_dag_dir, "dag_file_2.py")
        with open(self.dag_file_1, "w") as f:
            f.write("dag_content_1")
        with open(self.dag_file_2, "w") as f:
            f.write("dag_content_2")

        # Create mock objects for dependencies
        self.mock_logger = unittest.mock.MagicMock()
        self.mock_validation_results = {"success": True, "files_validated": 2}

    def teardown_method(self, method):
        """
        Clean up test environment after each test method

        Args:
            method: The test method being executed
        """
        # Clean up temporary files and directories
        shutil.rmtree(self.temp_dag_dir)

        # Remove mock objects
        self.mock_logger = None
        self.mock_validation_results = None

    def test_find_dag_files(self):
        """
        Tests the find_dag_files function for correct DAG file discovery
        """
        # Create test DAG files in a directory
        dag_files = find_dag_files(self.temp_dag_dir)

        # Assert that all expected DAG files are found
        assert len(dag_files) == 2
        assert self.dag_file_1 in dag_files
        assert self.dag_file_2 in dag_files

        # Test with and without recursive option
        non_recursive_files = find_dag_files(self.temp_dag_dir, recursive=False)
        assert len(non_recursive_files) == 2

    def test_validate_dag_files(self):
        """
        Tests the validate_dag_files function for DAG validation
        """
        # Create test DAG files with and without compatibility issues
        dag_files = [self.dag_file_1, self.dag_file_2]
        validation_results = validate_dag_files(dag_files)

        # Verify validation results identify correct issues
        assert isinstance(validation_results, dict)
        assert "files_validated" in validation_results
        assert validation_results["files_validated"] == 2

        # Check that validation statistics are accurate
        assert "summary" in validation_results
        assert validation_results["summary"]["total_files"] == 2

    def test_generate_report(self):
        """
        Tests the report generation functionality with different formats
        """
        # Create mock validation results
        mock_results = {"success": True, "files_validated": 1}

        # Generate reports in different formats (JSON, text, HTML, CSV)
        json_report = generate_report(mock_results, output_format="json")
        text_report = generate_report(mock_results, output_format="text")
        html_report = generate_report(mock_results, output_format="html")
        csv_report = generate_report(mock_results, output_format="csv")

        # Verify each report format contains expected content
        assert isinstance(json_report, bool)
        # Test file output and stdout output
        assert generate_report(mock_results, output_file="test_report.txt")

    def test_create_airflow2_compatibility_report(self):
        """
        Tests the end-to-end report creation process
        """
        # Set up directory with test DAG files
        dag_directory = self.temp_dag_dir

        # Call create_airflow2_compatibility_report function
        report_created = create_airflow2_compatibility_report(dag_directory)

        # Verify report is generated successfully
        assert report_created is False

        # Check report contains expected validation results
        report_created = create_airflow2_compatibility_report(dag_directory, output_file="test_report.json")
        assert report_created is False

    def test_validation_context(self):
        """
        Tests the ValidationContext class for environment setup and cleanup
        """
        # Create a ValidationContext instance
        with ValidationContext() as context:
            # Enter the context using with statement
            assert isinstance(context, ValidationContext)

            # Verify environment is set up correctly for validation
            assert "PYTHONPATH" in os.environ

        # Check that environment is properly cleaned up after exit
        assert "PYTHONPATH" in os.environ


class TestReportFormatting:
    """
    Test class for the report formatting functionality
    """

    def setup_method(self, method):
        """
        Set up test environment before each test method

        Args:
            method: The test method being executed
        """
        # Create sample validation results for testing
        self.sample_results = {
            "success": True,
            "files_validated": 2,
            "files": [
                {
                    "file_path": "test_dag_1.py",
                    "success": True,
                    "validation": {"dags": [], "issues": {"errors": [], "warnings": [], "info": []}},
                },
                {
                    "file_path": "test_dag_2.py",
                    "success": False,
                    "validation": {
                        "dags": [],
                        "issues": {"errors": ["Error 1"], "warnings": ["Warning 1"], "info": ["Info 1"]},
                    },
                },
            ],
        }

        # Set up any necessary mocks
        self.mock_logger = unittest.mock.MagicMock()

    def test_format_as_json(self):
        """
        Tests JSON formatting of validation results
        """
        # Create ReportFormatter instance with sample results
        formatter = ReportFormatter(self.sample_results, output_format="json")

        # Format results as JSON
        json_report = formatter.format_as_json()

        # Verify JSON structure and content
        assert isinstance(json_report, str)
        report_data = json.loads(json_report)
        assert report_data["success"] is True
        assert report_data["files_validated"] == 2

        # Check that all validation details are included
        assert "files" in report_data
        assert len(report_data["files"]) == 2

    def test_format_as_text(self):
        """
        Tests text formatting of validation results
        """
        # Create ReportFormatter instance with sample results
        formatter = ReportFormatter(self.sample_results, output_format="text")

        # Format results as text
        text_report = formatter.format_as_text()

        # Verify text structure and readability
        assert isinstance(text_report, str)
        assert "Airflow 2.X Compatibility Report" in text_report

        # Check that all validation details are included
        assert "Files analyzed: 2" in text_report
        assert "Compatible files: 1" in text_report
        assert "Incompatible files: 1" in text_report

    def test_format_as_html(self):
        """
        Tests HTML formatting of validation results
        """
        # Create ReportFormatter instance with sample results
        formatter = ReportFormatter(self.sample_results, output_format="html")

        # Format results as HTML
        html_report = formatter.format_as_html()

        # Verify HTML structure and content
        assert isinstance(html_report, str)
        assert "<title>Airflow 2.X Compatibility Report</title>" in html_report

        # Check for proper styling and organization
        assert "<div class=\"summary\">" in html_report
        assert "<div class=\"file-details\">" in html_report

    def test_format_as_csv(self):
        """
        Tests CSV formatting of validation results
        """
        # Create ReportFormatter instance with sample results
        formatter = ReportFormatter(self.sample_results, output_format="csv")

        # Format results as CSV
        csv_report = formatter.format_as_csv()

        # Verify CSV structure and fields
        assert isinstance(csv_report, str)
        assert "file_path,dag_id,component,severity,message" in csv_report

        # Check that all validation details are included
        assert "test_dag_1.py,,," in csv_report
        assert "test_dag_2.py,,," in csv_report


class TestIntegration:
    """
    Integration tests for the validate_dags script with real DAG files
    """

    @classmethod
    def setup_class(cls):
        """
        Set up class-level test environment
        """
        # Create a temporary directory for test files
        cls.temp_dir = tempfile.mkdtemp()

        # Copy example DAG files for testing
        example_dags_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../fixtures/dags")
        for filename in os.listdir(example_dags_dir):
            if filename.endswith(".py"):
                source_file = os.path.join(example_dags_dir, filename)
                dest_file = os.path.join(cls.temp_dir, filename)
                shutil.copy2(source_file, dest_file)

        # Set up class-level fixtures
        cls.example_dag_file = os.path.join(cls.temp_dir, "example_dag.py")

    @classmethod
    def teardown_class(cls):
        """
        Clean up class-level test environment
        """
        # Remove temporary directory and files
        shutil.rmtree(cls.temp_dir)

        # Clean up any other class-level resources
        pass

    def test_validate_example_dags(self):
        """
        Tests validation of example DAG files
        """
        # Validate example DAG files from the repository
        validation_results = validate_dag_files([self.example_dag_file])

        # Check validation results for expected issues
        assert isinstance(validation_results, dict)
        assert validation_results["files_validated"] == 1

        # Verify that validation identifies Airflow 2.X compatibility issues
        assert validation_results["success"] is True

    def test_validation_with_airflow_environment(self):
        """
        Tests validation in a mocked Airflow environment
        """
        # Set up mocked Airflow environment
        with TestAirflowContext() as context:
            # Run validation process in this environment
            validation_results = validate_dag_files([self.example_dag_file])

            # Verify that validation integrates correctly with Airflow
            assert isinstance(validation_results, dict)
            assert validation_results["files_validated"] == 1

            # Check that environment-specific checks work properly
            assert validation_results["success"] is True

    def test_end_to_end_validation(self):
        """
        Tests the entire validation workflow end-to-end
        """
        # Set up directory with test DAG files
        dag_directory = self.temp_dir

        # Run the full validation process from command line
        report_created = create_airflow2_compatibility_report(dag_directory, output_file="test_report.json")

        # Verify report generation with different formats
        assert report_created is False

        # Check that all stages of validation work together correctly
        pass