# Third-party imports
import pytest  # pytest-6.0+ Testing framework for test case definitions
import os  # standard library Operating system interfaces for file path operations
import json  # standard library JSON serialization and deserialization for variable testing
import tempfile  # standard library Create temporary files for testing
from unittest.mock import MagicMock  # standard library Mocking framework for test doubles

# Internal imports
from backend.scripts import import_variables  # Import the script module being tested
from test.utils import airflow2_compatibility_utils  # Utilities for testing with different Airflow versions
from test.fixtures import mock_data  # Predefined mock variables for testing
from test.utils.test_helpers import TestAirflowContext  # Context manager for Airflow testing environment

# Define global test variables
TEST_VARIABLES_FILE = os.path.join(os.path.dirname(__file__), 'test_variables.json')
SAMPLE_VARIABLES = {
    'project_id': 'test-project',
    'data_bucket': 'test-data-bucket',
    'retry_count': '3',
    'email_on_failure': 'airflow@example.com',
    'dev': {'api_endpoint': 'https://dev-api.example.com', 'threshold': '100'},
    'qa': {'api_endpoint': 'https://qa-api.example.com', 'threshold': '250'},
    'prod': {'api_endpoint': 'https://api.example.com', 'threshold': '500'}
}


def setup_module():
    """Setup function that runs once before all tests in the module"""
    # Create a test variables JSON file for testing
    # Write the SAMPLE_VARIABLES to the test file
    with open(TEST_VARIABLES_FILE, 'w') as f:
        json.dump(SAMPLE_VARIABLES, f)


def teardown_module():
    """Teardown function that runs once after all tests in the module"""
    # Remove the test variables JSON file if it exists
    if os.path.exists(TEST_VARIABLES_FILE):
        os.remove(TEST_VARIABLES_FILE)


def create_temp_variables_file(variables: dict, filename: str) -> str:
    """Creates a temporary variables JSON file with specified content"""
    # Create a temporary directory if needed
    temp_dir = tempfile.mkdtemp()
    # Serialize the variables dictionary to JSON
    json_data = json.dumps(variables)
    # Write the JSON data to the specified file
    file_path = os.path.join(temp_dir, filename)
    with open(file_path, 'w') as f:
        f.write(json_data)
    # Return the path to the created file
    return file_path


def mock_gcs_file_exists(exists: bool) -> MagicMock:
    """Creates a mock for the gcs_file_exists function"""
    # Create a MagicMock object
    mock = MagicMock()
    # Configure the mock to return the specified exists value
    mock.return_value = exists
    # Return the configured mock
    return mock


def mock_gcs_download_file(content: str) -> MagicMock:
    """Creates a mock for the gcs_download_file function"""
    # Create a MagicMock object
    mock = MagicMock()

    # Configure side_effect to write the specified content to the destination file
    def side_effect(bucket_name, object_name, dest_path, conn_id):
        with open(dest_path, 'w') as f:
            f.write(content)

    mock.side_effect = side_effect
    # Return the configured mock
    return mock


class TestImportVariables:
    """Test case class for testing the import_variables.py script"""

    def setup_method(self):
        """Setup method that runs before each test"""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        # Initialize empty list for tracking mock modules
        self.mock_modules = []

    def teardown_method(self):
        """Teardown method that runs after each test"""
        # Remove temporary directory and files
        import shutil
        shutil.rmtree(self.temp_dir)
        # Remove any module patches created during tests
        for mock_module in self.mock_modules:
            mock_module.stop()

    def test_load_variables_from_file(self):
        """Test loading variables from a local JSON file"""
        # Create a temporary variables file with test data
        variables = {'test_key': 'test_value'}
        file_path = create_temp_variables_file(variables, 'test_variables.json')
        # Call load_variables_from_file with the file path
        loaded_variables = import_variables.load_variables_from_file(file_path)
        # Assert that returned variables match expected values
        assert loaded_variables == variables

        # Test with non-existent file to verify error handling
        with pytest.raises(SystemExit) as exc_info:
            import_variables.load_variables_from_file('non_existent_file.json')
        assert exc_info.type == SystemExit

    def test_load_variables_from_gcs(self):
        """Test loading variables from a GCS JSON file"""
        # Mock gcp_utils.gcs_file_exists to return True
        mock_exists = mock_gcs_file_exists(True)
        patcher_exists = patch('backend.scripts.import_variables.gcs_file_exists', mock_exists)
        self.mock_modules.append(patcher_exists)
        patcher_exists.start()

        # Mock gcp_utils.gcs_download_file to write test data
        variables = {'test_key': 'test_value'}
        json_data = json.dumps(variables)
        mock_download = mock_gcs_download_file(json_data)
        patcher_download = patch('backend.scripts.import_variables.gcs_download_file', mock_download)
        self.mock_modules.append(patcher_download)
        patcher_download.start()

        # Call load_variables_from_gcs with a GCS path
        loaded_variables = import_variables.load_variables_from_gcs('gs://test_bucket/test_file.json')
        # Assert that returned variables match expected values
        assert loaded_variables == variables

        # Test with non-existent GCS file to verify error handling
        mock_exists = mock_gcs_file_exists(False)
        patcher_exists = patch('backend.scripts.import_variables.gcs_file_exists', mock_exists)
        self.mock_modules.append(patcher_exists)
        patcher_exists.start()
        with pytest.raises(SystemExit) as exc_info:
            import_variables.load_variables_from_gcs('gs://test_bucket/non_existent_file.json')
        assert exc_info.type == SystemExit

    def test_process_environment_variables(self):
        """Test processing variables based on target environment"""
        # Create a variables dictionary with environment-specific patterns
        variables = {
            'api_endpoint': '${ENV}',
            'threshold': {'dev': 100, 'qa': 200, 'prod': 300},
            'common': {'common_key': 'common_value'},
            'dev_only': {'dev_key': 'dev_value'}
        }
        # Call process_environment_variables with different environments
        dev_variables = import_variables.process_environment_variables(variables, 'dev')
        qa_variables = import_variables.process_environment_variables(variables, 'qa')
        prod_variables = import_variables.process_environment_variables(variables, 'prod')
        # Assert that environment-specific processing works correctly
        assert dev_variables['api_endpoint'] == 'dev'
        assert qa_variables['api_endpoint'] == 'qa'
        assert prod_variables['api_endpoint'] == 'prod'
        # Verify that environment-specific variable attributes are correctly merged
        assert 'dev_key' in dev_variables
        assert 'dev_key' not in qa_variables
        assert 'dev_key' not in prod_variables
        # Test with all three environments: dev, qa, and prod
        assert dev_variables['environment'] == 'dev'
        assert qa_variables['environment'] == 'qa'
        assert prod_variables['environment'] == 'prod'

    def test_import_variables(self):
        """Test importing variables into Airflow"""
        # Mock the Variable class based on Airflow version
        mock_variable_class = MagicMock()
        patcher_variable = patch('backend.scripts.import_variables.get_variable_class', return_value=mock_variable_class)
        self.mock_modules.append(patcher_variable)
        patcher_variable.start()

        # Create test variables dictionary
        variables = {'test_key': 'test_value', 'test_key2': 'test_value2'}
        # Call import_variables with different parameter combinations
        success, skipped, failed = import_variables.import_variables(variables)
        # Verify dry run mode only logs actions without making changes
        assert success == 2
        assert skipped == 0
        assert failed == 0
        # Verify skip_existing and force parameters work correctly
        mock_variable_class.get.side_effect = [True, False]
        success, skipped, failed = import_variables.import_variables(variables, skip_existing=True)
        assert success == 1
        assert skipped == 1
        assert failed == 0
        # Assert correct success, skipped, and failed counts are returned
        success, skipped, failed = import_variables.import_variables(variables, force=True)
        assert success == 2
        assert skipped == 0
        assert failed == 0

    def test_detect_airflow_version(self):
        """Test detection of Airflow version"""
        # Mock airflow.__version__ with different version strings
        mock_airflow = MagicMock()
        mock_airflow.__version__ = '2.2.5'
        patcher_airflow = patch('backend.scripts.import_variables.airflow', mock_airflow)
        self.mock_modules.append(patcher_airflow)
        patcher_airflow.start()
        # Call detect_airflow_version
        major, minor, patch = import_variables.detect_airflow_version()
        # Assert that version components are correctly extracted
        assert major == 2
        assert minor == 2
        assert patch == 5
        # Test error handling with invalid version formats
        mock_airflow.__version__ = 'invalid_version'
        major, minor, patch = import_variables.detect_airflow_version()
        assert major == 0
        assert minor == 0
        assert patch == 0

    def test_get_variable_class(self):
        """Test getting the appropriate Variable class based on Airflow version"""
        # Mock detect_airflow_version to return different versions
        mock_detect_version = MagicMock()
        mock_detect_version.return_value = (1, 10, 15)
        patcher_detect = patch('backend.scripts.import_variables.detect_airflow_version', mock_detect_version)
        self.mock_modules.append(patcher_detect)
        patcher_detect.start()
        # Call get_variable_class
        variable_class = import_variables.get_variable_class()
        # Verify correct Variable class is returned for Airflow 1.X
        assert variable_class.__module__ == 'airflow.models'
        mock_detect_version.return_value = (2, 2, 5)
        variable_class = import_variables.get_variable_class()
        # Verify correct Variable class is returned for Airflow 2.X
        assert variable_class.__module__ == 'airflow.models.variable'
        # Test error handling for import failures
        mock_detect_version.return_value = (3, 0, 0)
        with pytest.raises(SystemExit) as exc_info:
            import_variables.get_variable_class()
        assert exc_info.type == SystemExit

    def test_main_function(self):
        """Test the main function with various arguments"""
        # Create temporary test files and mock command line arguments
        variables = {'test_key': 'test_value'}
        file_path = create_temp_variables_file(variables, 'test_variables.json')
        args = ['--file_path', file_path]
        # Mock necessary dependencies (Variable class, GCS functions)
        mock_variable_class = MagicMock()
        patcher_variable = patch('backend.scripts.import_variables.get_variable_class', return_value=mock_variable_class)
        self.mock_modules.append(patcher_variable)
        patcher_variable.start()
        # Call main function with different parameter combinations
        exit_code = import_variables.main(args)
        # Verify correct exit codes based on success/failure
        assert exit_code == 0
        # Test with file path argument, GCS path argument, and environment-specific settings
        args = ['--gcs_path', 'gs://test_bucket/test_file.json', '--environment', 'dev']
        mock_exists = mock_gcs_file_exists(True)
        patcher_exists = patch('backend.scripts.import_variables.gcs_file_exists', mock_exists)
        self.mock_modules.append(patcher_exists)
        patcher_exists.start()
        mock_download = mock_gcs_download_file(json.dumps(variables))
        patcher_download = patch('backend.scripts.import_variables.gcs_download_file', mock_download)
        self.mock_modules.append(patcher_download)
        patcher_download.start()
        exit_code = import_variables.main(args)
        assert exit_code == 0

    def test_compatibility_with_airflow2(self):
        """Test compatibility with Airflow 2.X"""
        # Use TestAirflowContext context to set up testing environment
        with TestAirflowContext():
            # Mock different Airflow version scenarios
            with patch('backend.scripts.import_variables.detect_airflow_version', return_value=(2, 0, 0)):
                # Call import_variables script functions
                variables = {'test_key': 'test_value'}
                success, skipped, failed = import_variables.import_variables(variables)
                # Verify correct behavior with Airflow 2.X specific features
                assert success == 1
                assert skipped == 0
                assert failed == 0
            # Ensure backward compatibility with Airflow 1.10.15
            with patch('backend.scripts.import_variables.detect_airflow_version', return_value=(1, 10, 15)):
                success, skipped, failed = import_variables.import_variables(variables)
                assert success == 1
                assert skipped == 0
                assert failed == 0

    def test_environment_specific_variables(self):
        """Test handling of environment-specific variables"""
        # Create variables with dev/qa/prod environment-specific sections
        variables = {
            'common_variable': 'common_value',
            'dev_only': {'dev_key': 'dev_value'},
            'qa_only': {'qa_key': 'qa_value'},
            'prod_only': {'prod_key': 'prod_value'}
        }
        # Test process_environment_variables with each environment
        dev_variables = import_variables.process_environment_variables(variables, 'dev')
        qa_variables = import_variables.process_environment_variables(variables, 'qa')
        prod_variables = import_variables.process_environment_variables(variables, 'prod')
        # Verify correct variables are included or excluded based on environment
        assert 'dev_key' in dev_variables
        assert 'qa_key' not in dev_variables
        assert 'prod_key' not in dev_variables
        assert 'dev_key' not in qa_variables
        assert 'qa_key' in qa_variables
        assert 'prod_key' not in qa_variables
        assert 'dev_key' not in prod_variables
        assert 'qa_key' not in prod_variables
        assert 'prod_key' in prod_variables
        # Test handling of environment-specific substitutions
        variables = {'api_endpoint': '${ENV}'}
        dev_variables = import_variables.process_environment_variables(variables, 'dev')
        assert dev_variables['api_endpoint'] == 'dev'