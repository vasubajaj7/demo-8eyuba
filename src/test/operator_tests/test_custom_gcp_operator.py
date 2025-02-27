"""
Unit tests for custom Google Cloud Platform operators to verify compatibility and
functionality after migration from Airflow 1.10.15 to Airflow 2.X. Tests cover
GCS operations including file existence checking, uploading, downloading, listing,
and deletion with mock GCP services.
"""

import pytest  # pytest 6.0+
import unittest  # Python standard library
import unittest.mock  # Python standard library
from unittest.mock import patch, MagicMock  # Python standard library
import os  # Python standard library
import tempfile  # Python standard library
from datetime import datetime  # Python standard library

# Internal imports
from backend.plugins.operators.custom_gcp_operator import CustomGCSOperator, GCSFileExistsOperator, GCSUploadOperator, GCSDownloadOperator, GCSListFilesOperator, GCSDeleteFileOperator  # src/backend/plugins/operators/custom_gcp_operator.py
from test.fixtures.mock_operators import MockGCSFileExistsOperator, MockGCSUploadOperator, MockGCSDownloadOperator, MockGCSListFilesOperator, MockGCSDeleteFileOperator, create_mock_context, patch_operators  # src/test/fixtures/mock_operators.py
from test.fixtures.mock_hooks import MockCustomGCPHook, patch_custom_hooks  # src/test/fixtures/mock_hooks.py
from test.fixtures.mock_gcp_services import create_mock_storage_client, DEFAULT_BUCKET_NAME, patch_gcp_services  # src/test/fixtures/mock_gcp_services.py
from test.utils.assertion_utils import assert_operator_airflow2_compatible  # src/test/utils/assertion_utils.py
from test.utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin, is_airflow2  # src/test/utils/airflow2_compatibility_utils.py

# Global test variables
TEST_BUCKET = "test-bucket"
TEST_OBJECT = "test-object.txt"
TEST_LOCAL_FILE = "/tmp/test-file.txt"
TEST_GCP_CONN_ID = "test-gcp-conn"

# Mock GCP responses
MOCK_GCP_RESPONSES = {
    "gcs_file_exists": True,
    "gcs_upload_file": "gs://test-bucket/test-object.txt",
    "gcs_download_file": "/tmp/test-file.txt",
    "gcs_list_files": ["file1.txt", "file2.txt", "file3.txt"],
    "gcs_delete_file": True
}


def get_operator_test_args(override_args: dict = None) -> dict:
    """
    Helper function to get standardized test arguments for GCP operators

    Args:
        override_args (dict): Dictionary of arguments to override the defaults

    Returns:
        dict: Dictionary of test arguments
    """
    base_args = {
        'bucket_name': TEST_BUCKET,
        'object_name': TEST_OBJECT,
        'local_file_path': TEST_LOCAL_FILE,
        'gcp_conn_id': TEST_GCP_CONN_ID,
        'delegate_to': None
    }

    if override_args:
        base_args.update(override_args)

    return base_args


class TestCustomGCSOperator(unittest.TestCase):
    """
    Test case for the base CustomGCSOperator class
    """
    mock_responses = {}

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)
        self.mock_responses = MOCK_GCP_RESPONSES

    def setUp(self):
        """
        Set up test environment before each test
        """
        self.patchers = patch_custom_hooks(self.mock_responses)
        for patcher in self.patchers.values():
            patcher.start()

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        for patcher in self.patchers.values():
            patcher.stop()

    def test_get_hook(self):
        """
        Test that get_hook method returns the correct hook instance
        """
        operator = CustomGCSOperator(task_id="test_operator", gcp_conn_id=TEST_GCP_CONN_ID, delegate_to=None)
        hook = operator.get_hook()
        self.assertIsInstance(hook, MockCustomGCPHook)
        self.assertEqual(hook.gcp_conn_id, TEST_GCP_CONN_ID)
        self.assertIsNone(hook.delegate_to)

    def test_custom_gcs_operator_airflow2_compatible(self):
        """
        Test that the operator is compatible with Airflow 2.X
        """
        operator = CustomGCSOperator(task_id="test_operator", gcp_conn_id=TEST_GCP_CONN_ID, delegate_to=None)
        assert_operator_airflow2_compatible(operator)


class TestGCSFileExistsOperator(unittest.TestCase):
    """
    Test case for the GCSFileExistsOperator
    """
    test_args = {}
    mock_responses = {}
    patchers = []

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)
        self.test_args = get_operator_test_args()
        self.patchers = []

    def setUp(self):
        """
        Set up test environment before each test
        """
        self.mock_responses = {"gcs_file_exists": True}
        self.patchers = patch_custom_hooks(self.mock_responses)
        for patcher in self.patchers.values():
            patcher.start()

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        for patcher in self.patchers.values():
            patcher.stop()

    def test_init(self):
        """
        Test initialization of GCSFileExistsOperator
        """
        operator = GCSFileExistsOperator(task_id="test_operator", **self.test_args)
        self.assertEqual(operator.bucket_name, TEST_BUCKET)
        self.assertEqual(operator.object_name, TEST_OBJECT)
        self.assertEqual(operator.gcp_conn_id, TEST_GCP_CONN_ID)
        self.assertIsNone(operator.delegate_to)

    def test_execute_file_exists(self):
        """
        Test execute method when file exists
        """
        self.mock_responses["gcs_file_exists"] = True
        context = create_mock_context()
        operator = GCSFileExistsOperator(task_id="test_operator", **self.test_args)
        result = operator.execute(context)
        self.assertTrue(result)

    def test_execute_file_does_not_exist(self):
        """
        Test execute method when file does not exist
        """
        self.mock_responses["gcs_file_exists"] = False
        context = create_mock_context()
        operator = GCSFileExistsOperator(task_id="test_operator", **self.test_args)
        result = operator.execute(context)
        self.assertFalse(result)

    def test_execute_with_error(self):
        """
        Test execute method with error during execution
        """
        with self.assertRaises(Exception):
            context = create_mock_context()
            test_args_with_error = get_operator_test_args({'bucket_name': None})
            operator = GCSFileExistsOperator(task_id="test_operator", **test_args_with_error)
            operator.execute(context)

    def test_airflow2_compatibility(self):
        """
        Test compatibility with Airflow 2.X
        """
        operator = GCSFileExistsOperator(task_id="test_operator", **self.test_args)
        assert_operator_airflow2_compatible(operator)


class TestGCSUploadOperator(unittest.TestCase):
    """
    Test case for the GCSUploadOperator
    """
    test_args = {}
    mock_responses = {}
    patchers = []

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)
        self.test_args = get_operator_test_args()
        self.patchers = []

    def setUp(self):
        """
        Set up test environment before each test
        """
        self.mock_responses = {"gcs_upload_file": "gs://test-bucket/test-object.txt"}
        self.patchers = patch_custom_hooks(self.mock_responses)
        for patcher in self.patchers.values():
            patcher.start()
        # Create a dummy file for testing
        with open(TEST_LOCAL_FILE, "w") as f:
            f.write("test data")

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        for patcher in self.patchers.values():
            patcher.stop()
        # Remove the dummy file
        if os.path.exists(TEST_LOCAL_FILE):
            os.remove(TEST_LOCAL_FILE)

    def test_init(self):
        """
        Test initialization of GCSUploadOperator
        """
        operator = GCSUploadOperator(task_id="test_operator", local_file_path=TEST_LOCAL_FILE, **self.test_args)
        self.assertEqual(operator.local_file_path, TEST_LOCAL_FILE)
        self.assertEqual(operator.bucket_name, TEST_BUCKET)
        self.assertEqual(operator.object_name, TEST_OBJECT)
        self.assertEqual(operator.gcp_conn_id, TEST_GCP_CONN_ID)
        self.assertIsNone(operator.delegate_to)

    def test_execute_upload(self):
        """
        Test execute method for file upload
        """
        self.mock_responses["gcs_upload_file"] = "gs://test-bucket/test-object.txt"
        context = create_mock_context()
        operator = GCSUploadOperator(task_id="test_operator", local_file_path=TEST_LOCAL_FILE, **self.test_args)
        result = operator.execute(context)
        self.assertEqual(result, "gs://test-bucket/test-object.txt")

    def test_execute_with_error(self):
        """
        Test execute method with error during execution
        """
        with self.assertRaises(Exception):
            context = create_mock_context()
            test_args_with_error = get_operator_test_args({'bucket_name': None})
            operator = GCSUploadOperator(task_id="test_operator", local_file_path=TEST_LOCAL_FILE, **test_args_with_error)
            operator.execute(context)

    def test_file_path_validation(self):
        """
        Test validation of local file path
        """
        with self.assertRaises(ValueError):
            GCSUploadOperator(task_id="test_operator", local_file_path="/invalid/file/path.txt", **self.test_args)

    def test_airflow2_compatibility(self):
        """
        Test compatibility with Airflow 2.X
        """
        operator = GCSUploadOperator(task_id="test_operator", local_file_path=TEST_LOCAL_FILE, **self.test_args)
        assert_operator_airflow2_compatible(operator)


class TestGCSDownloadOperator(unittest.TestCase):
    """
    Test case for the GCSDownloadOperator
    """
    test_args = {}
    mock_responses = {}
    patchers = []

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)
        self.test_args = get_operator_test_args()
        self.patchers = []

    def setUp(self):
        """
        Set up test environment before each test
        """
        self.mock_responses = {"gcs_download_file": "/tmp/test-file.txt"}
        self.patchers = patch_custom_hooks(self.mock_responses)
        for patcher in self.patchers.values():
            patcher.start()
        # Create a dummy directory for downloads
        os.makedirs(os.path.dirname(TEST_LOCAL_FILE), exist_ok=True)

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        for patcher in self.patchers.values():
            patcher.stop()
        # Remove the dummy directory
        if os.path.exists(os.path.dirname(TEST_LOCAL_FILE)):
            shutil.rmtree(os.path.dirname(TEST_LOCAL_FILE))

    def test_init(self):
        """
        Test initialization of GCSDownloadOperator
        """
        operator = GCSDownloadOperator(task_id="test_operator", local_file_path=TEST_LOCAL_FILE, **self.test_args)
        self.assertEqual(operator.local_file_path, TEST_LOCAL_FILE)
        self.assertEqual(operator.bucket_name, TEST_BUCKET)
        self.assertEqual(operator.object_name, TEST_OBJECT)
        self.assertEqual(operator.gcp_conn_id, TEST_GCP_CONN_ID)
        self.assertIsNone(operator.delegate_to)

    def test_execute_download(self):
        """
        Test execute method for file download
        """
        self.mock_responses["gcs_download_file"] = "/tmp/test-file.txt"
        context = create_mock_context()
        operator = GCSDownloadOperator(task_id="test_operator", local_file_path=TEST_LOCAL_FILE, **self.test_args)
        result = operator.execute(context)
        self.assertEqual(result, "/tmp/test-file.txt")

    def test_execute_with_error(self):
        """
        Test execute method with error during execution
        """
        with self.assertRaises(Exception):
            context = create_mock_context()
            test_args_with_error = get_operator_test_args({'bucket_name': None})
            operator = GCSDownloadOperator(task_id="test_operator", local_file_path=TEST_LOCAL_FILE, **test_args_with_error)
            operator.execute(context)

    def test_airflow2_compatibility(self):
        """
        Test compatibility with Airflow 2.X
        """
        operator = GCSDownloadOperator(task_id="test_operator", local_file_path=TEST_LOCAL_FILE, **self.test_args)
        assert_operator_airflow2_compatible(operator)


class TestGCSListFilesOperator(unittest.TestCase):
    """
    Test case for the GCSListFilesOperator
    """
    test_args = {}
    mock_responses = {}
    patchers = []

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)
        self.test_args = get_operator_test_args()
        self.patchers = []

    def setUp(self):
        """
        Set up test environment before each test
        """
        self.mock_responses = {"gcs_list_files": ["file1.txt", "file2.txt", "file3.txt"]}
        self.patchers = patch_custom_hooks(self.mock_responses)
        for patcher in self.patchers.values():
            patcher.start()

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        for patcher in self.patchers.values():
            patcher.stop()

    def test_init(self):
        """
        Test initialization of GCSListFilesOperator
        """
        operator = GCSListFilesOperator(task_id="test_operator", bucket_name=TEST_BUCKET, **self.test_args)
        self.assertEqual(operator.bucket_name, TEST_BUCKET)
        self.assertEqual(operator.prefix, '')
        self.assertEqual(operator.gcp_conn_id, TEST_GCP_CONN_ID)
        self.assertIsNone(operator.delegate_to)

    def test_execute_list_files(self):
        """
        Test execute method for file listing
        """
        self.mock_responses["gcs_list_files"] = ["file1.txt", "file2.txt", "file3.txt"]
        context = create_mock_context()
        operator = GCSListFilesOperator(task_id="test_operator", bucket_name=TEST_BUCKET, **self.test_args)
        result = operator.execute(context)
        self.assertEqual(result, ["file1.txt", "file2.txt", "file3.txt"])

    def test_execute_with_prefix(self):
        """
        Test execute method with prefix filtering
        """
        self.mock_responses["gcs_list_files"] = ["file1.txt"]
        context = create_mock_context()
        operator = GCSListFilesOperator(task_id="test_operator", bucket_name=TEST_BUCKET, prefix="file", **self.test_args)
        result = operator.execute(context)
        self.assertEqual(result, ["file1.txt"])

    def test_execute_with_error(self):
        """
        Test execute method with error during execution
        """
        with self.assertRaises(Exception):
            context = create_mock_context()
            test_args_with_error = get_operator_test_args({'bucket_name': None})
            operator = GCSListFilesOperator(task_id="test_operator", bucket_name=TEST_BUCKET, **test_args_with_error)
            operator.execute(context)

    def test_airflow2_compatibility(self):
        """
        Test compatibility with Airflow 2.X
        """
        operator = GCSListFilesOperator(task_id="test_operator", bucket_name=TEST_BUCKET, **self.test_args)
        assert_operator_airflow2_compatible(operator)


class TestGCSDeleteFileOperator(unittest.TestCase):
    """
    Test case for the GCSDeleteFileOperator
    """
    test_args = {}
    mock_responses = {}
    patchers = []

    def __init__(self, *args, **kwargs):
        """
        Initialize the test case
        """
        super().__init__(*args, **kwargs)
        self.test_args = get_operator_test_args()
        self.patchers = []

    def setUp(self):
        """
        Set up test environment before each test
        """
        self.mock_responses = {"gcs_delete_file": True}
        self.patchers = patch_custom_hooks(self.mock_responses)
        for patcher in self.patchers.values():
            patcher.start()

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        for patcher in self.patchers.values():
            patcher.stop()

    def test_init(self):
        """
        Test initialization of GCSDeleteFileOperator
        """
        operator = GCSDeleteFileOperator(task_id="test_operator", **self.test_args)
        self.assertEqual(operator.bucket_name, TEST_BUCKET)
        self.assertEqual(operator.object_name, TEST_OBJECT)
        self.assertEqual(operator.gcp_conn_id, TEST_GCP_CONN_ID)
        self.assertIsNone(operator.delegate_to)

    def test_execute_delete_file(self):
        """
        Test execute method for file deletion
        """
        self.mock_responses["gcs_delete_file"] = True
        context = create_mock_context()
        operator = GCSDeleteFileOperator(task_id="test_operator", **self.test_args)
        result = operator.execute(context)
        self.assertTrue(result)

    def test_execute_delete_nonexistent_file(self):
        """
        Test execute method for deleting non-existent file
        """
        self.mock_responses["gcs_delete_file"] = False
        context = create_mock_context()
        operator = GCSDeleteFileOperator(task_id="test_operator", **self.test_args)
        result = operator.execute(context)
        self.assertFalse(result)

    def test_execute_with_error(self):
        """
        Test execute method with error during execution
        """
        with self.assertRaises(Exception):
            context = create_mock_context()
            test_args_with_error = get_operator_test_args({'bucket_name': None})
            operator = GCSDeleteFileOperator(task_id="test_operator", **test_args_with_error)
            operator.execute(context)

    def test_airflow2_compatibility(self):
        """
        Test compatibility with Airflow 2.X
        """
        operator = GCSDeleteFileOperator(task_id="test_operator", **self.test_args)
        assert_operator_airflow2_compatible(operator)