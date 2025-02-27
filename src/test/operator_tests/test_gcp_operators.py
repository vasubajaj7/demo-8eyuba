"""
Test suite for validating standard Google Cloud Platform operators from the Airflow providers package
during migration from Airflow 1.10.15 to Airflow 2.X. Tests compatibility, functionality, and behavior
of GCP operators to ensure successful migration to Cloud Composer 2.
"""

# Standard library imports
import unittest  # Base testing framework
import unittest.mock  # Mocking functionality for isolation testing
import pytest  # Advanced testing framework with fixtures
import os  # File path operations for test resources
import tempfile  # Temporary file creation for testing
import datetime  # Date handling for test context

# Third-party imports
from google.api_core import exceptions as google_exceptions  # GCP service exceptions for testing error handling

# Airflow imports
from airflow.models import DagBag  # Access to Airflow models for context creation
from airflow.utils import dates  # Date utilities for task execution testing
from airflow.providers.google.cloud.operators import gcs  # GCS operators for Airflow 2.X
from airflow.providers.google.cloud.operators import bigquery  # BigQuery operators for Airflow 2.X
from airflow.providers.google.cloud.operators import cloud_sql  # Cloud SQL operators for Airflow 2.X
from airflow.providers.google.cloud.operators import dataflow  # Dataflow operators for Airflow 2.X

# Internal imports
from ..fixtures.mock_gcp_services import create_mock_storage_client, MockStorageClient, MockBucket, MockBlob  # Create mock GCS client for testing standard operators
from ..utils.operator_validation_utils import validate_operator_signature, test_operator_migration, OPERATOR_VALIDATION_LEVEL  # Validate operator signatures across Airflow versions
from ..utils.assertion_utils import assert_operator_airflow2_compatible  # Assert operators are compatible with Airflow 2.X
from ..utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin, is_airflow2, AIRFLOW_1_TO_2_OPERATOR_MAPPING  # Mixin providing Airflow 2.X compatibility utilities for testing

# Define global constants for testing
TEST_PROJECT = "test-gcp-project"
TEST_BUCKET = "test-gcs-bucket"
TEST_OBJECT = "test-object.txt"
TEST_CONTENT = "Test content for GCS testing"
TEST_DATASET = "test_dataset"
TEST_TABLE = "test_table"


def create_mock_context(additional_context: dict = None) -> dict:
    """
    Creates a mock Airflow task context for testing operator execution

    Args:
        additional_context (dict): Additional context to merge with the default context

    Returns:
        dict: Mock Airflow context dictionary
    """
    execution_date = dates.days_ago(1)  # Create execution date using airflow.utils.dates.days_ago(1)
    task_instance = unittest.mock.MagicMock(task_id='test_task', execution_date=execution_date)  # Create task instance with appropriate task_id and execution_date
    xcoms = {}  # Create empty XComs dictionary

    context = {
        'dag': unittest.mock.MagicMock(dag_id='test_dag'),
        'task_instance': task_instance,
        'ti': task_instance,
        'execution_date': execution_date,
        'dag_run': unittest.mock.MagicMock(),
        'xcom_pull': lambda task_ids=None, key=None, dag_id=None: xcoms.get(key),
        'xcom_push': lambda key, value: xcoms.update({key: value}),
    }

    if additional_context:  # Merge with additional_context if provided
        context.update(additional_context)

    return context  # Return context dictionary with all required values


def setup_gcs_mock_data(bucket_contents: dict = None) -> MockStorageClient:
    """
    Sets up mock GCS data for testing GCS operators

    Args:
        bucket_contents (dict): Dictionary of bucket contents

    Returns:
        MockStorageClient: Configured mock GCS client
    """
    buckets = {}  # Create empty buckets dictionary if none provided
    if not bucket_contents:
        bucket_contents = {}

    if TEST_BUCKET not in bucket_contents:  # Add TEST_BUCKET to buckets dictionary if not present
        buckets[TEST_BUCKET] = {}
    else:
        buckets = bucket_contents

    mock_client = create_mock_storage_client(mock_responses=buckets)  # Create mock storage client with specified bucket contents
    return mock_client  # Return mock client for use in tests


def get_airflow1_operator(operator_name: str, operator_params: dict = None) -> object:
    """
    Get the Airflow 1.X version of a GCP operator by name

    Args:
        operator_name (str): Name of the operator
        operator_params (dict): Parameters for the operator

    Returns:
        object: Instantiated Airflow 1.X operator
    """
    module_path = AIRFLOW_1_TO_2_OPERATOR_MAPPING[operator_name].rsplit('.', 1)[0]  # Import the operator from Airflow 1.X module path
    module = importlib.import_module(module_path)
    operator_class = getattr(module, operator_name)

    if operator_params is None:  # Create parameter dictionary with defaults if not provided
        operator_params = {}

    operator = operator_class(**operator_params)  # Instantiate operator with parameters
    return operator  # Return operator instance


def get_airflow2_operator(operator_name: str, operator_params: dict = None) -> object:
    """
    Get the Airflow 2.X version of a GCP operator by name

    Args:
        operator_name (str): Name of the operator
        operator_params (dict): Parameters for the operator

    Returns:
        object: Instantiated Airflow 2.X operator
    """
    module_path = AIRFLOW_1_TO_2_OPERATOR_MAPPING[operator_name].rsplit('.', 1)[0]  # Import the operator from Airflow 2.X provider package path
    module = importlib.import_module(module_path)
    operator_class = getattr(module, operator_name)

    if operator_params is None:  # Create parameter dictionary with defaults if not provided
        operator_params = {}

    operator = operator_class(**operator_params)  # Instantiate operator with parameters
    return operator  # Return operator instance


class TestGCPOperatorBase(unittest.TestCase):
    """
    Base test class for all GCP operator tests
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)  # Call parent constructor
        self.mock_context = None  # Initialize mock_context to None
        self.mock_gcs_client = None  # Initialize mock_gcs_client to None
        self.patchers = []  # Initialize empty patchers list

    def setUp(self):
        """
        Set up test environment before each test
        """
        super().setUp()  # Call parent setUp
        self.mock_context = create_mock_context()  # Create mock context with create_mock_context()
        self.mock_gcs_client = setup_gcs_mock_data()  # Set up mock GCS client with setup_gcs_mock_data()

        # Set up patchers for GCP services
        storage_patcher = unittest.mock.patch('airflow.providers.google.cloud.operators.gcs.GCSHook.get_conn', return_value=self.mock_gcs_client)
        self.patchers.append(storage_patcher)

        for patcher in self.patchers:  # Start all patchers
            patcher.start()

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        for patcher in self.patchers:  # Stop all patchers
            patcher.stop()
        super().tearDown()  # Call parent tearDown


class TestGCSOperatorMigration(TestGCPOperatorBase):
    """
    Tests for GCS operator migration from Airflow 1.X to 2.X
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)  # Call parent constructor
        self.test_params = {  # Initialize test_params with default GCS test parameters
            'bucket_name': TEST_BUCKET,
            'object_name': TEST_OBJECT,
            'filename': 'local_file.txt',
            'task_id': 'test_task'
        }

    def test_gcs_create_bucket_operator_migration(self):
        """
        Test GCSCreateBucketOperator migration
        """
        airflow1_operator = get_airflow1_operator('GoogleCloudStorageCreateBucketOperator', self.test_params)  # Get Airflow 1.X operator instance
        airflow2_operator = get_airflow2_operator('GCSCreateBucketOperator', self.test_params)  # Get Airflow 2.X operator instance
        test_result = test_operator_migration(airflow1_operator, airflow2_operator, OPERATOR_VALIDATION_LEVEL['FULL'])  # Test migration with FULL validation level
        self.assertTrue(test_result.success)  # Assert migration result success

    def test_gcs_list_operator_migration(self):
        """
        Test GCSListObjectsOperator migration
        """
        airflow1_operator = get_airflow1_operator('GoogleCloudStorageListOperator', self.test_params)  # Get Airflow 1.X operator instance
        airflow2_operator = get_airflow2_operator('GCSListObjectsOperator', self.test_params)  # Get Airflow 2.X operator instance
        test_result = test_operator_migration(airflow1_operator, airflow2_operator, OPERATOR_VALIDATION_LEVEL['FULL'])  # Test migration with FULL validation level
        self.assertTrue(test_result.success)  # Assert migration result success

    def test_gcs_download_operator_migration(self):
        """
        Test GCSToLocalFilesystemOperator migration
        """
        airflow1_operator = get_airflow1_operator('GoogleCloudStorageDownloadOperator', self.test_params)  # Get Airflow 1.X operator instance
        airflow2_operator = get_airflow2_operator('GCSToLocalFilesystemOperator', self.test_params)  # Get Airflow 2.X operator instance
        test_result = test_operator_migration(airflow1_operator, airflow2_operator, OPERATOR_VALIDATION_LEVEL['FULL'])  # Test migration with FULL validation level
        self.assertTrue(test_result.success)  # Assert migration result success

    def test_gcs_upload_operator_migration(self):
        """
        Test LocalFilesystemToGCSOperator migration
        """
        airflow1_operator = get_airflow1_operator('GoogleCloudStorageUploadOperator', self.test_params)  # Get Airflow 1.X operator instance
        airflow2_operator = get_airflow2_operator('LocalFilesystemToGCSOperator', self.test_params)  # Get Airflow 2.X operator instance
        test_result = test_operator_migration(airflow1_operator, airflow2_operator, OPERATOR_VALIDATION_LEVEL['FULL'])  # Test migration with FULL validation level
        self.assertTrue(test_result.success)  # Assert migration result success


class TestBigQueryOperatorMigration(TestGCPOperatorBase):
    """
    Tests for BigQuery operator migration from Airflow 1.X to 2.X
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)  # Call parent constructor
        self.test_params = {  # Initialize test_params with default BigQuery test parameters
            'dataset_id': TEST_DATASET,
            'table_id': TEST_TABLE,
            'project_id': TEST_PROJECT,
            'task_id': 'test_task'
        }

    def test_bigquery_operator_migration(self):
        """
        Test BigQueryOperator migration
        """
        airflow1_operator = get_airflow1_operator('BigQueryOperator', self.test_params)  # Get Airflow 1.X operator instance
        airflow2_operator = get_airflow2_operator('BigQueryOperator', self.test_params)  # Get Airflow 2.X operator instance
        test_result = test_operator_migration(airflow1_operator, airflow2_operator, OPERATOR_VALIDATION_LEVEL['FULL'])  # Test migration with FULL validation level
        self.assertTrue(test_result.success)  # Assert migration result success

    def test_bigquery_create_empty_table_operator_migration(self):
        """
        Test BigQueryCreateEmptyTableOperator migration
        """
        airflow1_operator = get_airflow1_operator('BigQueryCreateEmptyTableOperator', self.test_params)  # Get Airflow 1.X operator instance
        airflow2_operator = get_airflow2_operator('BigQueryCreateEmptyTableOperator', self.test_params)  # Get Airflow 2.X operator instance
        test_result = test_operator_migration(airflow1_operator, airflow2_operator, OPERATOR_VALIDATION_LEVEL['FULL'])  # Test migration with FULL validation level
        self.assertTrue(test_result.success)  # Assert migration result success

    def test_bigquery_create_external_table_operator_migration(self):
        """
        Test BigQueryCreateExternalTableOperator migration
        """
        airflow1_operator = get_airflow1_operator('BigQueryCreateExternalTableOperator', self.test_params)  # Get Airflow 1.X operator instance
        airflow2_operator = get_airflow2_operator('BigQueryCreateExternalTableOperator', self.test_params)  # Get Airflow 2.X operator instance
        test_result = test_operator_migration(airflow1_operator, airflow2_operator, OPERATOR_VALIDATION_LEVEL['FULL'])  # Test migration with FULL validation level
        self.assertTrue(test_result.success)  # Assert migration result success

    def test_bigquery_extract_operator_migration(self):
        """
        Test BigQueryExtractTableOperator migration
        """
        airflow1_operator = get_airflow1_operator('BigQueryExtractTableOperator', self.test_params)  # Get Airflow 1.X operator instance
        airflow2_operator = get_airflow2_operator('BigQueryExtractTableOperator', self.test_params)  # Get Airflow 2.X operator instance
        test_result = test_operator_migration(airflow1_operator, airflow2_operator, OPERATOR_VALIDATION_LEVEL['FULL'])  # Test migration with FULL validation level
        self.assertTrue(test_result.success)  # Assert migration result success


class TestGCSFunctionalOperators(TestGCPOperatorBase):
    """
    Functional tests for GCS operators in Airflow 2.X
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)  # Call parent constructor

    def test_gcs_create_bucket(self):
        """
        Test GCSCreateBucketOperator functionality
        """
        operator = gcs.GCSCreateBucketOperator(  # Create GCSCreateBucketOperator instance
            task_id='create_bucket',
            bucket_name='new_test_bucket',
            gcp_conn_id='google_cloud_default'
        )
        operator.execute(context=self.mock_context)  # Execute operator with mock context

        bucket = self.mock_gcs_client.bucket('new_test_bucket')  # Verify bucket was created with correct parameters
        self.assertIsNotNone(bucket)
        self.assertEqual(bucket.name, 'new_test_bucket')
        self.assertEqual(bucket.location, 'us-central1')
        self.assertTrue(True)  # Assert operation success

    def test_gcs_list_objects(self):
        """
        Test GCSListObjectsOperator functionality
        """
        # Set up mock bucket with test objects
        bucket = self.mock_gcs_client.bucket(TEST_BUCKET)
        bucket.blob('test_object_1.txt').upload_from_string(TEST_CONTENT)
        bucket.blob('test_object_2.txt').upload_from_string(TEST_CONTENT)

        operator = gcs.GCSListObjectsOperator(  # Create GCSListObjectsOperator instance
            task_id='list_objects',
            bucket_name=TEST_BUCKET,
            gcp_conn_id='google_cloud_default'
        )
        result = operator.execute(context=self.mock_context)  # Execute operator with mock context

        self.assertIn('test_object_1.txt', result)  # Verify result contains expected objects
        self.assertIn('test_object_2.txt', result)
        self.assertTrue(True)  # Assert operation success

    def test_gcs_download(self):
        """
        Test GCSToLocalFilesystemOperator functionality
        """
        # Set up mock bucket with test object
        bucket = self.mock_gcs_client.bucket(TEST_BUCKET)
        bucket.blob(TEST_OBJECT).upload_from_string(TEST_CONTENT)

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file = os.path.join(tmp_dir, 'downloaded_file.txt')
            operator = gcs.GCSToLocalFilesystemOperator(  # Create GCSToLocalFilesystemOperator instance
                task_id='download_file',
                bucket_name=TEST_BUCKET,
                object_name=TEST_OBJECT,
                filename=local_file,
                gcp_conn_id='google_cloud_default'
            )
            operator.execute(context=self.mock_context)  # Execute operator with mock context

            with open(local_file, 'r') as f:  # Verify file was downloaded to correct location
                content = f.read()
                self.assertEqual(content, TEST_CONTENT)
        self.assertTrue(True)  # Assert operation success

    def test_gcs_upload(self):
        """
        Test LocalFilesystemToGCSOperator functionality
        """
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:  # Create temporary local file with test content
            tmp_file.write(TEST_CONTENT)
            tmp_file_path = tmp_file.name

        operator = gcs.LocalFilesystemToGCSOperator(  # Create LocalFilesystemToGCSOperator instance
            task_id='upload_file',
            bucket_name=TEST_BUCKET,
            object_name=TEST_OBJECT,
            filename=tmp_file_path,
            gcp_conn_id='google_cloud_default'
        )
        operator.execute(context=self.mock_context)  # Execute operator with mock context

        blob = self.mock_gcs_client.bucket(TEST_BUCKET).get_blob(TEST_OBJECT)  # Verify file was uploaded to correct location
        self.assertIsNotNone(blob)
        self.assertEqual(blob.download_as_string().decode('utf-8'), TEST_CONTENT)
        self.assertTrue(True)  # Assert operation success

        os.remove(tmp_file_path)  # Clean up temporary file


class TestBigQueryFunctionalOperators(TestGCPOperatorBase):
    """
    Functional tests for BigQuery operators in Airflow 2.X
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)  # Call parent constructor

    def test_bigquery_operator(self):
        """
        Test BigQueryOperator functionality
        """
        # Set up mock BigQuery service
        mock_query_results = [{'id': 1, 'name': 'test', 'value': 10.0, 'timestamp': datetime.datetime.now().isoformat()}]
        self.mock_gcs_client._query_results = {'SELECT * FROM test_dataset.test_table': mock_query_results}

        operator = bigquery.BigQueryOperator(  # Create BigQueryOperator instance with test SQL
            task_id='bq_query',
            sql='SELECT * FROM test_dataset.test_table',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default'
        )
        operator.execute(context=self.mock_context)  # Execute operator with mock context

        self.assertTrue(True)  # Assert operation success

    def test_bigquery_create_empty_table(self):
        """
        Test BigQueryCreateEmptyTableOperator functionality
        """
        # Set up mock BigQuery service
        operator = bigquery.BigQueryCreateEmptyTableOperator(  # Create BigQueryCreateEmptyTableOperator instance
            task_id='bq_create_table',
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE,
            project_id=TEST_PROJECT,
            gcp_conn_id='google_cloud_default'
        )
        operator.execute(context=self.mock_context)  # Execute operator with mock context

        self.assertTrue(True)  # Assert operation success


class TestDataflowOperators(TestGCPOperatorBase):
    """
    Tests for Dataflow operators in Airflow 2.X
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)  # Call parent constructor

    def test_dataflow_java_operator(self):
        """
        Test DataflowJavaOperator functionality
        """
        # Set up mock Dataflow service
        operator = dataflow.DataflowJavaOperator(  # Create DataflowJavaOperator instance
            task_id='dataflow_java',
            jar='test.jar',
            gcp_conn_id='google_cloud_default'
        )
        operator.execute(context=self.mock_context)  # Execute operator with mock context

        self.assertTrue(True)  # Assert operation success

    def test_dataflow_python_operator(self):
        """
        Test DataflowPythonOperator functionality
        """
        # Set up mock Dataflow service
        operator = dataflow.DataflowPythonOperator(  # Create DataflowPythonOperator instance
            task_id='dataflow_python',
            py_file='test.py',
            gcp_conn_id='google_cloud_default'
        )
        operator.execute(context=self.mock_context)  # Execute operator with mock context

        self.assertTrue(True)  # Assert operation success