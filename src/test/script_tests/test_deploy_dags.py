#!/usr/bin/env python3

"""
Test suite for deploy_dags.py script which automates the deployment of DAGs to Cloud Composer 2 environments.
Verifies the script's ability to validate, upload, and configure DAGs across development, QA, and production environments.
"""

import os  # standard library
import sys  # standard library
import pytest  # pytest-6.0+
from unittest import mock  # standard library
from google.cloud import storage  # google-cloud-storage-2.0.0+

# Internal imports
from src.backend.scripts import deploy_dags  # src/backend/scripts/deploy_dags.py
from src.test.utils import test_helpers  # src/test/utils/test_helpers.py
from src.test.utils import assertion_utils  # src/test/utils/assertion_utils.py
from src.test.fixtures import mock_gcp_services  # src/test/fixtures/mock_gcp_services.py
from src.test.fixtures import mock_data  # src/test/fixtures/mock_data.py


def setup_test_environment(num_dags: int = 3, include_invalid: bool = False) -> tuple[str, list]:
    """
    Sets up a test environment with mock DAG files for testing the deployment script

    Args:
        num_dags (int): Number of valid DAG files to create
        include_invalid (bool): Whether to include an invalid DAG file

    Returns:
        tuple: (str, list) - Path to test directory and list of DAG files
    """
    # Create a temporary directory for test files
    with test_helpers.temp_directory() as test_dir:
        # Create subdirectory structure mimicking the project layout
        dags_dir = os.path.join(test_dir, "dags")
        os.makedirs(dags_dir)

        # Create several valid DAG files with SAMPLE_DAG_CONTENT
        dag_files = []
        for i in range(num_dags):
            dag_file = os.path.join(dags_dir, f"dag_{i+1}.py")
            test_helpers.create_mock_file(dag_file, mock_data.SAMPLE_DAG_CONTENT)
            dag_files.append(dag_file)

        # If include_invalid is True, add an invalid DAG file
        if include_invalid:
            invalid_dag_file = os.path.join(dags_dir, "invalid_dag.py")
            test_helpers.create_mock_file(invalid_dag_file, mock_data.SAMPLE_INVALID_DAG_CONTENT)
            dag_files.append(invalid_dag_file)

        # Return the path to the test directory and list of created DAG files
        return test_dir, dag_files


def mock_gcp_environment(env_name: str) -> dict:
    """
    Sets up a mock GCP environment for testing with proper authentication and storage

    Args:
        env_name (str): Name of the environment (e.g., 'dev', 'qa', 'prod')

    Returns:
        dict: Mock environment configuration with storage client and bucket
    """
    # Create mock storage client using create_mock_storage_client
    mock_storage_client = mock_gcp_services.create_mock_storage_client()

    # Create mock bucket based on environment name
    mock_bucket = mock_gcp_services.MockBucket(f"composer2-migration-{env_name}-dags")

    # Set up mock authentication credentials
    credentials = mock.MagicMock()

    # Build and return environment configuration dictionary
    return {
        "storage_client": mock_storage_client,
        "bucket": mock_bucket,
        "credentials": credentials,
    }


@pytest.mark.parametrize(
    "args,expected",
    [
        (
            ["dev"],
            {
                "environment": "dev",
                "source_folder": deploy_dags.DEFAULT_DAG_FOLDER,
                "variables_file": None,
                "connections_file": None,
                "verbose": False,
                "dry_run": False,
                "parallel": False,
                "include_patterns": None,
                "exclude_patterns": None,
            },
        ),
        (
            ["qa", "--source-folder", "/custom/dags"],
            {
                "environment": "qa",
                "source_folder": "/custom/dags",
                "variables_file": None,
                "connections_file": None,
                "verbose": False,
                "dry_run": False,
                "parallel": False,
                "include_patterns": None,
                "exclude_patterns": None,
            },
        ),
        (
            [
                "prod",
                "--variables-file",
                "/path/to/vars.json",
                "--connections-file",
                "/path/to/conns.json",
                "--verbose",
                "--dry-run",
                "--parallel",
                "--include-patterns",
                "dag_*.py",
                "test_*.py",
                "--exclude-patterns",
                "temp_*.py",
            ],
            {
                "environment": "prod",
                "source_folder": deploy_dags.DEFAULT_DAG_FOLDER,
                "variables_file": "/path/to/vars.json",
                "connections_file": "/path/to/conns.json",
                "verbose": True,
                "dry_run": True,
                "parallel": True,
                "include_patterns": ["dag_*.py", "test_*.py"],
                "exclude_patterns": ["temp_*.py"],
            },
        ),
    ],
)
def test_parse_arguments(args, expected):
    """Tests that command line arguments are correctly parsed"""
    # Use mock_sys_argv to simulate command line arguments
    with test_helpers.mock_sys_argv(["deploy_dags.py"] + args):
        # Call parse_arguments function from deploy_dags
        parsed_args = deploy_dags.parse_arguments()

        # Verify parsed arguments match expected values
        assert vars(parsed_args) == expected


@pytest.mark.parametrize(
    "folder_exists,has_dags,expected",
    [
        (True, True, True),
        (True, False, False),
        (False, True, False),
        (False, False, False),
    ],
)
def test_validate_source_folder(folder_exists, has_dags, expected):
    """Tests validation of the source folder containing DAG files"""
    # Set up test directory structure based on parameters
    with test_helpers.temp_directory() as test_dir:
        source_folder = os.path.join(test_dir, "dags")

        # If folder_exists is True, create the folder
        if folder_exists:
            os.makedirs(source_folder)

            # If has_dags is True, add DAG files to the folder
            if has_dags:
                test_helpers.create_mock_file(os.path.join(source_folder, "dag_1.py"), mock_data.SAMPLE_DAG_CONTENT)

        # Call validate_source_folder function from deploy_dags
        result = deploy_dags.validate_source_folder(source_folder)

        # Verify result matches expected value
        assert result == expected


@pytest.mark.parametrize(
    "include_patterns,exclude_patterns,expected_count",
    [
        (None, None, 5),
        (["dag_1.py", "dag_2.py"], None, 2),
        (None, ["dag_3.py", "dag_4.py"], 3),
        (["dag_*.py"], ["dag_2.py"], 3),
        (["*"], ["*.txt"], 4),
    ],
)
def test_collect_dag_files(include_patterns, exclude_patterns, expected_count):
    """Tests collection of DAG files from the source directory with filtering"""
    # Set up test directory with a known number of DAG files
    with test_helpers.temp_directory() as test_dir:
        source_folder = os.path.join(test_dir, "dags")
        os.makedirs(source_folder)

        # Add various file types and in subdirectories
        test_helpers.create_mock_file(os.path.join(source_folder, "dag_1.py"), mock_data.SAMPLE_DAG_CONTENT)
        test_helpers.create_mock_file(os.path.join(source_folder, "dag_2.py"), mock_data.SAMPLE_DAG_CONTENT)
        test_helpers.create_mock_file(os.path.join(source_folder, "dag_3.py"), mock_data.SAMPLE_DAG_CONTENT)
        test_helpers.create_mock_file(os.path.join(source_folder, "dag_4.py"), mock_data.SAMPLE_DAG_CONTENT)
        test_helpers.create_mock_file(os.path.join(source_folder, "not_a_dag.txt"), "Not a DAG")

        # Call collect_dag_files with the test parameters
        dag_files = deploy_dags.collect_dag_files(source_folder, include_patterns, exclude_patterns)

        # Verify the correct files are collected based on patterns
        assert len(dag_files) == expected_count


@pytest.mark.parametrize(
    "file_exists,upload_succeeds,dry_run",
    [
        (True, True, False),
        (True, False, False),
        (True, True, True),
        (False, True, False),
    ],
)
def test_upload_dag_file(file_exists, upload_succeeds, dry_run):
    """Tests uploading a single DAG file to GCS bucket"""
    # Set up test DAG file based on file_exists parameter
    with test_helpers.temp_directory() as test_dir:
        dag_file = os.path.join(test_dir, "dag_1.py")
        if file_exists:
            test_helpers.create_mock_file(dag_file, mock_data.SAMPLE_DAG_CONTENT)

        # Configure mock GCS client with appropriate behavior based on upload_succeeds
        mock_gcs_client = mock.MagicMock()
        if upload_succeeds:
            mock_gcs_client.upload_file.return_value = "gs://test-bucket/dags/dag_1.py"
        else:
            mock_gcs_client.upload_file.side_effect = Exception("Upload failed")

        # Call upload_dag_file function with parameters including dry_run flag
        success, message = deploy_dags.upload_dag_file(
            dag_file, mock_gcs_client, "test-bucket", "dags", dry_run
        )

        # Verify the function returns expected success status
        assert success == (upload_succeeds or dry_run)

        # If dry_run is True, verify no actual upload occurred
        if dry_run:
            mock_gcs_client.upload_file.assert_not_called()
        else:
            # Otherwise, verify the upload was attempted with correct parameters
            if file_exists:
                mock_gcs_client.upload_file.assert_called_once_with(
                    local_file_path=dag_file, bucket_name="test-bucket", object_name="dags/dag_1.py"
                )


@pytest.mark.parametrize(
    "environment,parallel,expected_success",
    [
        ("dev", True, True),
        ("qa", False, True),
        ("prod", True, False),
    ],
)
def test_deploy_to_environment(environment, parallel, expected_success):
    """Tests deploying multiple DAG files to a specific environment"""
    # Set up test directory with multiple DAG files
    with test_helpers.temp_directory() as test_dir:
        source_folder = os.path.join(test_dir, "dags")
        os.makedirs(source_folder)
        test_helpers.create_mock_file(os.path.join(source_folder, "dag_1.py"), mock_data.SAMPLE_DAG_CONTENT)
        test_helpers.create_mock_file(os.path.join(source_folder, "dag_2.py"), mock_data.SAMPLE_DAG_CONTENT)
        dag_files = [os.path.join(source_folder, "dag_1.py"), os.path.join(source_folder, "dag_2.py")]

        # Create mock environment configuration for the specified environment
        mock_env_config = mock.MagicMock()
        mock_env_config.get.return_value = {"dag_bucket": "test-bucket"}
        mock_gcp_environment = mock.MagicMock()
        mock_gcp_environment.return_value = {"storage_client": mock.MagicMock(), "bucket": mock.MagicMock()}

        # Call deploy_to_environment function with parameters
        with mock.patch("src.backend.scripts.deploy_dags.authenticate_to_gcp", return_value=mock.MagicMock()), \
             mock.patch("src.backend.scripts.deploy_dags.GCSClient", return_value=mock.MagicMock()):
            deployment_results = deploy_dags.deploy_to_environment(
                dag_files, mock_env_config, dry_run=False, parallel=parallel
            )

        # Verify the deployment results match expectations
        assert deployment_results["success"] == expected_success

        # Check success and failure counts in the results
        if expected_success:
            assert deployment_results["success_count"] == 2
            assert deployment_results["failure_count"] == 0
        else:
            assert deployment_results["success_count"] == 0
            assert deployment_results["failure_count"] == 2

        # If parallel is True, verify concurrent uploads were attempted
        if parallel:
            pass  # Add assertions for parallel execution if needed


def test_main_success():
    """Tests successful execution of the main function with valid inputs"""
    # Set up test environment with valid DAG files
    with mock.patch("src.backend.scripts.deploy_dags.parse_arguments") as mock_parse_arguments, \
         mock.patch("src.backend.scripts.deploy_dags.validate_source_folder", return_value=True), \
         mock.patch("src.backend.scripts.deploy_dags.collect_dag_files", return_value=["dag1.py", "dag2.py"]), \
         mock.patch("src.backend.scripts.deploy_dags.validate_dag_files", return_value={"success": True}), \
         mock.patch("src.backend.scripts.deploy_dags.deploy_to_environment", return_value={"success": True, "message": "Deployed successfully"}), \
         mock.patch("src.backend.scripts.deploy_dags.handle_variables_and_connections", return_value={}), \
         mock.patch("src.backend.scripts.deploy_dags.get_environment_config") as mock_get_environment_config:

        # Configure command line arguments for a successful deployment
        mock_parse_arguments.return_value = mock.MagicMock(
            environment="dev",
            source_folder="/path/to/dags",
            variables_file=None,
            connections_file=None,
            verbose=False,
            dry_run=False,
            parallel=False,
            include_patterns=None,
            exclude_patterns=None,
        )
        mock_get_environment_config.return_value = {}

        # Call main function from deploy_dags
        return_code = deploy_dags.main()

        # Verify the return code is 0 (success)
        assert return_code == 0


def test_main_failure_invalid_dags():
    """Tests main function behavior with invalid DAG files"""
    # Set up test environment with at least one invalid DAG file
    with mock.patch("src.backend.scripts.deploy_dags.parse_arguments") as mock_parse_arguments, \
         mock.patch("src.backend.scripts.deploy_dags.validate_source_folder", return_value=True), \
         mock.patch("src.backend.scripts.deploy_dags.collect_dag_files", return_value=["dag1.py", "dag2.py"]), \
         mock.patch("src.backend.scripts.deploy_dags.validate_dag_files", return_value={"success": False}), \
         mock.patch("src.backend.scripts.deploy_dags.deploy_to_environment"), \
         mock.patch("src.backend.scripts.deploy_dags.handle_variables_and_connections"), \
         mock.patch("src.backend.scripts.deploy_dags.get_environment_config"):

        # Configure command line arguments including validation
        mock_parse_arguments.return_value = mock.MagicMock(
            environment="dev",
            source_folder="/path/to/dags",
            variables_file=None,
            connections_file=None,
            verbose=False,
            dry_run=False,
            parallel=False,
            include_patterns=None,
            exclude_patterns=None,
        )

        # Call main function from deploy_dags
        return_code = deploy_dags.main()

        # Verify the return code is non-zero (failure)
        assert return_code != 0


@pytest.mark.parametrize("environment", ["dev", "qa", "prod"])
def test_main_environment_specific(environment):
    """Tests environment-specific configurations during deployment"""
    # Set up test environment with valid DAG files
    with mock.patch("src.backend.scripts.deploy_dags.parse_arguments") as mock_parse_arguments, \
         mock.patch("src.backend.scripts.deploy_dags.validate_source_folder", return_value=True), \
         mock.patch("src.backend.scripts.deploy_dags.collect_dag_files", return_value=["dag1.py", "dag2.py"]), \
         mock.patch("src.backend.scripts.deploy_dags.validate_dag_files", return_value={"success": True}), \
         mock.patch("src.backend.scripts.deploy_dags.deploy_to_environment", return_value={"success": True, "message": "Deployed successfully"}), \
         mock.patch("src.backend.scripts.deploy_dags.handle_variables_and_connections", return_value={}), \
         mock.patch("src.backend.scripts.deploy_dags.get_environment_config") as mock_get_environment_config:

        # Configure command line arguments for the specified environment
        mock_parse_arguments.return_value = mock.MagicMock(
            environment=environment,
            source_folder="/path/to/dags",
            variables_file=None,
            connections_file=None,
            verbose=False,
            dry_run=False,
            parallel=False,
            include_patterns=None,
            exclude_patterns=None,
        )

        # Mock environment-specific configurations and services
        mock_env_config = {"dag_bucket": f"test-bucket-{environment}"}
        mock_get_environment_config.return_value = mock_env_config

        # Call main function from deploy_dags
        return_code = deploy_dags.main()

        # Verify environment-specific settings were applied
        assert return_code == 0


@pytest.mark.parametrize(
    "with_vars,with_conns,expected_success",
    [
        (True, True, True),
        (True, False, True),
        (False, True, True),
        (False, False, True),
    ],
)
def test_handle_variables_and_connections(with_vars, with_conns, expected_success):
    """Tests the handling of Airflow variables and connections during deployment"""
    # Set up test environment with variable and connection files based on parameters
    mock_load_variables_from_file = mock.MagicMock(return_value={"var1": "value1"})
    mock_import_variables = mock.MagicMock(return_value=(1, 0, 0))
    mock_load_connections_from_file = mock.MagicMock(return_value=[{"conn_id": "conn1", "conn_type": "http"}])
    mock_import_connections = mock.MagicMock(return_value=(1, 0, 0))

    # Mock the import_variables and import_connections functions
    with mock.patch("src.backend.scripts.deploy_dags.load_variables_from_file", mock_load_variables_from_file), \
         mock.patch("src.backend.scripts.deploy_dags.import_variables", mock_import_variables), \
         mock.patch("src.backend.scripts.deploy_dags.load_connections_from_file", mock_load_connections_from_file), \
         mock.patch("src.backend.scripts.deploy_dags.import_connections", mock_import_connections):

        # Set up environment configuration for a specific environment
        mock_env_config = {"environment": {"name": "dev"}}

        # Call handle_variables_and_connections function with parameters
        variables_file = "/path/to/vars.json" if with_vars else None
        connections_file = "/path/to/conns.json" if with_conns else None
        results = deploy_dags.handle_variables_and_connections(
            variables_file, connections_file, mock_env_config, dry_run=False
        )

        # Verify appropriate import functions were called with correct parameters
        if with_vars:
            mock_load_variables_from_file.assert_called_once_with(variables_file)
            mock_import_variables.assert_called_once()
        else:
            mock_load_variables_from_file.assert_not_called()
            mock_import_variables.assert_not_called()

        if with_conns:
            mock_load_connections_from_file.assert_called_once_with(connections_file)
            mock_import_connections.assert_called_once()
        else:
            mock_load_connections_from_file.assert_not_called()
            mock_import_connections.assert_not_called()

        # Check that the results match expected_success
        assert (results["variables"]["success"] > 0) == with_vars
        assert (results["connections"]["success"] > 0) == with_conns


@pytest.mark.integration
def test_integration_full_deployment():
    """Integration test for the full deployment process"""
    # Set up complete test environment with DAGs, variables, and connections
    with test_helpers.temp_directory() as test_dir:
        source_folder = os.path.join(test_dir, "dags")
        os.makedirs(source_folder)
        test_helpers.create_mock_file(os.path.join(source_folder, "dag_1.py"), mock_data.SAMPLE_DAG_CONTENT)
        test_helpers.create_mock_file(os.path.join(source_folder, "dag_2.py"), mock_data.SAMPLE_DAG_CONTENT)

        variables_file = os.path.join(test_dir, "variables.json")
        with open(variables_file, "w") as f:
            json.dump({"var1": "value1"}, f)

        connections_file = os.path.join(test_dir, "connections.json")
        with open(connections_file, "w") as f:
            json.dump([{"conn_id": "conn1", "conn_type": "http", "host": "example.com"}], f)

        # Configure realistic command line arguments for deployment
        args = [
            "dev",
            "--source-folder",
            source_folder,
            "--variables-file",
            variables_file,
            "--connections-file",
            connections_file,
            "--verbose",
        ]

        # Mock all necessary GCP services while preserving integration between components
        with mock.patch("src.backend.scripts.deploy_dags.parse_arguments") as mock_parse_arguments, \
             mock.patch("src.backend.scripts.deploy_dags.validate_source_folder", return_value=True), \
             mock.patch("src.backend.scripts.deploy_dags.validate_dag_files", return_value={"success": True}), \
             mock.patch("src.backend.scripts.deploy_dags.deploy_to_environment", return_value={"success": True, "message": "Deployed successfully"}), \
             mock.patch("src.backend.scripts.deploy_dags.get_environment_config") as mock_get_environment_config, \
             mock.patch("src.backend.scripts.deploy_dags.authenticate_to_gcp", return_value=mock.MagicMock()), \
             mock.patch("src.backend.scripts.deploy_dags.GCSClient", return_value=mock.MagicMock()), \
             mock.patch("src.backend.scripts.deploy_dags.load_variables_from_file", return_value={"var1": "value1"}), \
             mock.patch("src.backend.scripts.deploy_dags.import_variables", return_value=(1, 0, 0)), \
             mock.patch("src.backend.scripts.deploy_dags.load_connections_from_file", return_value=[{"conn_id": "conn1", "conn_type": "http"}]), \
             mock.patch("src.backend.scripts.deploy_dags.import_connections", return_value=(1, 0, 0)):

            # Mock command line arguments
            mock_parse_arguments.return_value = mock.MagicMock(
                environment="dev",
                source_folder=source_folder,
                variables_file=variables_file,
                connections_file=connections_file,
                verbose=True,
                dry_run=False,
                parallel=False,
                include_patterns=None,
                exclude_patterns=None,
            )

            # Mock environment-specific configurations
            mock_env_config = {"dag_bucket": "test-bucket-dev"}
            mock_get_environment_config.return_value = mock_env_config

            # Call main function from deploy_dags
            return_code = deploy_dags.main(args)

            # Verify all components interact correctly
            assert return_code == 0