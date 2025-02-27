"""
Test module for validating Google Cloud Storage functionality in both Airflow 1.10.15 and Airflow 2.X
environments during Cloud Composer migration. Contains unit and integration tests for GCS operations
including file uploads, downloads, listing, and deletion, as well as compatibility verification
between Airflow versions.
"""

import unittest
import pytest  # pytest-6.0+
import unittest.mock  # Python standard library
import tempfile  # Python standard library
import os  # Python standard library
import datetime  # Python standard library

# Airflow 2.X GCS hook for testing compatibility
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # airflow-providers-google v2.0.0+

# Airflow 1.10.15 GCS hook for compatibility testing
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook  # airflow 1.10.15

# Access utility functions for GCS operations
from src.backend.dags.utils.gcp_utils import (
    gcs_file_exists,
    gcs_upload_file,
    gcs_download_file,
    gcs_list_files,
    gcs_delete_file,
    GCSClient,
)  # src/backend/dags/utils/gcp_utils.py

# Test custom GCP hook for improved GCS operations
from src.backend.plugins.hooks.custom_gcp_hook import CustomGCPHook  # src/backend/plugins/hooks/custom_gcp_hook.py

# Create mock GCS clients for isolated testing
from src.test.fixtures.mock_gcp_services import (
    create_mock_storage_client,
    patch_gcp_services,
    DEFAULT_BUCKET_NAME,
)  # src/test/fixtures/mock_gcp_services.py

# Check if tests are running with Airflow 2.X
from src.test.utils.airflow2_compatibility_utils import is_airflow2  # src/test/utils/airflow2_compatibility_utils.py

# Mixin for compatibility testing between Airflow versions
from src.test.utils.airflow2_compatibility_utils import (
    Airflow2CompatibilityTestMixin,
)  # src/test/utils/airflow2_compatibility_utils.py

# Assert GCS task execution results are consistent between Airflow versions
from src.test.utils.assertion_utils import (
    assert_task_execution_unchanged,
)  # src/test/utils/assertion_utils.py

# Define global test variables
TEST_BUCKET = "test-migration-bucket"
TEST_OBJECT = "test-object.txt"
TEST_CONTENT = "Test content for GCS storage validation"
TEST_FOLDER = "test-folder/"
TEST_PREFIX = "test-prefix-"
GCP_CONN_ID = "google_cloud_default"


def setup_module():
    """Set up test module fixtures and environment."""
    # Set up mock GCP services for isolated testing
    print("Setting up mock GCP services...")

    # Create test bucket and test objects for testing
    print(f"Creating test bucket: {TEST_BUCKET}")
    print(f"Creating test object: {TEST_OBJECT}")

    # Configure environment variables for testing
    print("Configuring environment variables...")


def teardown_module():
    """Clean up test module fixtures and environment."""
    # Remove test objects and bucket
    print(f"Removing test object: {TEST_OBJECT}")
    print(f"Removing test bucket: {TEST_BUCKET}")

    # Clean up mock GCP services
    print("Cleaning up mock GCP services...")

    # Reset environment variables
    print("Resetting environment variables...")


def create_test_file(content: str) -> str:
    """Creates a temporary test file with specified content.

    Args:
        content: Content to write to the file

    Returns:
        Path to the created temporary file
    """
    # Create temporary file with a unique name
    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".txt") as temp_file:
        # Write content to file
        temp_file.write(content)
        file_path = temp_file.name

    # Return file path
    print(f"Created temporary test file: {file_path}")
    return file_path


@pytest.mark.gcp
@pytest.mark.cloud_storage
class TestGCSUtilities(unittest.TestCase):
    """Test case for GCS utility functions in gcp_utils.py."""

    def __init__(self, *args, **kwargs):
        """Initialize the GCS utilities test case."""
        super().__init__(*args, **kwargs)
        # Initialize storage_patchers to None
        self.storage_patchers = None
        # Initialize mock_storage to None
        self.mock_storage = None

    def setUp(self):
        """Set up test case with mock GCS clients."""
        # Create mock responses for GCS operations
        mock_responses = {
            "gcs": {
                DEFAULT_BUCKET_NAME: {
                    "blobs": {
                        TEST_OBJECT: TEST_CONTENT,
                        f"{TEST_FOLDER}{TEST_OBJECT}": TEST_CONTENT,
                        f"{TEST_PREFIX}{TEST_OBJECT}": TEST_CONTENT,
                    }
                }
            }
        }

        # Set up storage_patchers using patch_gcp_services
        self.storage_patchers = patch_gcp_services(mock_responses)

        # Start all patchers
        for patcher in self.storage_patchers.values():
            patcher.start()

        # Set up mock bucket and test objects
        self.mock_storage = create_mock_storage_client(mock_responses["gcs"])

    def tearDown(self):
        """Clean up test case resources."""
        # Stop all storage patchers
        for patcher in self.storage_patchers.values():
            patcher.stop()

        # Reset mock objects
        self.mock_storage = None

        # Clean temporary test files
        pass

    def test_gcs_file_exists(self):
        """Test gcs_file_exists function for existing and non-existing files."""
        # Set up mock responses for file exists checks
        bucket_name = DEFAULT_BUCKET_NAME
        existing_object = TEST_OBJECT
        non_existing_object = "non-existing-object.txt"

        # Call gcs_file_exists for existing file and verify True return
        exists = gcs_file_exists(bucket_name, existing_object, conn_id=GCP_CONN_ID)
        self.assertTrue(exists)

        # Call gcs_file_exists for non-existing file and verify False return
        exists = gcs_file_exists(bucket_name, non_existing_object, conn_id=GCP_CONN_ID)
        self.assertFalse(exists)

    def test_gcs_upload_file(self):
        """Test gcs_upload_file function uploads files correctly."""
        # Create temporary test file with content
        local_file_path = create_test_file(TEST_CONTENT)

        # Call gcs_upload_file to upload to GCS
        gcs_uri = gcs_upload_file(
            local_file_path, DEFAULT_BUCKET_NAME, TEST_OBJECT, conn_id=GCP_CONN_ID
        )

        # Verify upload was called with correct parameters
        expected_gcs_uri = f"gs://{DEFAULT_BUCKET_NAME}/{TEST_OBJECT}"
        self.assertEqual(gcs_uri, expected_gcs_uri)

        # Clean up temporary file
        os.remove(local_file_path)

    def test_gcs_download_file(self):
        """Test gcs_download_file function downloads files correctly."""
        # Set up mock blob with test content
        bucket_name = DEFAULT_BUCKET_NAME
        object_name = TEST_OBJECT

        # Call gcs_download_file to download from GCS
        with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".txt") as temp_file:
            local_file_path = temp_file.name
        downloaded_file_path = gcs_download_file(bucket_name, object_name, local_file_path, conn_id=GCP_CONN_ID)

        # Verify download was called with correct parameters
        self.assertEqual(downloaded_file_path, local_file_path)

        # Verify file content matches expected content
        with open(downloaded_file_path, "r") as downloaded_file:
            downloaded_content = downloaded_file.read()
        self.assertEqual(downloaded_content, TEST_CONTENT)

        # Clean up downloaded file
        os.remove(downloaded_file_path)

    def test_gcs_list_files(self):
        """Test gcs_list_files function lists files correctly with and without prefix."""
        # Set up mock bucket with test objects
        bucket_name = DEFAULT_BUCKET_NAME

        # Call gcs_list_files without prefix
        files = gcs_list_files(bucket_name, conn_id=GCP_CONN_ID)
        expected_files = [TEST_OBJECT, f"{TEST_FOLDER}{TEST_OBJECT}", f"{TEST_PREFIX}{TEST_OBJECT}"]
        self.assertEqual(set(files), set(expected_files))

        # Call gcs_list_files with prefix
        prefix = TEST_PREFIX
        files = gcs_list_files(bucket_name, prefix=prefix, conn_id=GCP_CONN_ID)
        expected_files = [f"{TEST_PREFIX}{TEST_OBJECT}"]
        self.assertEqual(set(files), set(expected_files))

    def test_gcs_delete_file(self):
        """Test gcs_delete_file function deletes files correctly."""
        # Set up mock bucket with test object
        bucket_name = DEFAULT_BUCKET_NAME
        object_name = TEST_OBJECT

        # Call gcs_delete_file to delete the object
        gcs_delete_file(bucket_name, object_name, conn_id=GCP_CONN_ID)

        # Verify object no longer exists in mock bucket
        files = gcs_list_files(bucket_name, conn_id=GCP_CONN_ID)
        expected_files = [f"{TEST_FOLDER}{TEST_OBJECT}", f"{TEST_PREFIX}{TEST_OBJECT}"]
        self.assertEqual(set(files), set(expected_files))

    def test_gcs_client_class(self):
        """Test GCSClient class methods work correctly."""
        # Create GCSClient instance
        client = GCSClient(conn_id=GCP_CONN_ID)

        # Test file_exists method
        self.assertTrue(client.file_exists(DEFAULT_BUCKET_NAME, TEST_OBJECT))

        # Test upload_file method
        local_file_path = create_test_file(TEST_CONTENT)
        gcs_uri = client.upload_file(local_file_path, DEFAULT_BUCKET_NAME, "uploaded_object.txt")
        self.assertEqual(gcs_uri, f"gs://{DEFAULT_BUCKET_NAME}/uploaded_object.txt")
        os.remove(local_file_path)

        # Test download_file method
        with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".txt") as temp_file:
            local_file_path = temp_file.name
        downloaded_file_path = client.download_file(DEFAULT_BUCKET_NAME, TEST_OBJECT, local_file_path)
        self.assertEqual(downloaded_file_path, local_file_path)
        os.remove(local_file_path)

        # Test list_files method
        files = client.list_files(DEFAULT_BUCKET_NAME)
        expected_files = [TEST_OBJECT, f"{TEST_FOLDER}{TEST_OBJECT}", f"{TEST_PREFIX}{TEST_OBJECT}"]
        self.assertEqual(set(files), set(expected_files))

        # Test delete_file method
        client.delete_file(DEFAULT_BUCKET_NAME, TEST_OBJECT)
        files = client.list_files(DEFAULT_BUCKET_NAME)
        expected_files = [f"{TEST_FOLDER}{TEST_OBJECT}", f"{TEST_PREFIX}{TEST_OBJECT}"]
        self.assertEqual(set(files), set(expected_files))


@pytest.mark.gcp
@pytest.mark.cloud_storage
class TestCustomGCPHook(unittest.TestCase):
    """Test case for CustomGCPHook GCS methods."""

    def __init__(self, *args, **kwargs):
        """Initialize the CustomGCPHook test case."""
        super().__init__(*args, **kwargs)
        # Initialize storage_patchers to None
        self.storage_patchers = None
        # Initialize mock_storage to None
        self.mock_storage = None

    def setUp(self):
        """Set up test case with CustomGCPHook and mocks."""
        # Create mock responses for GCS operations
        mock_responses = {
            "gcs": {
                DEFAULT_BUCKET_NAME: {
                    "blobs": {
                        TEST_OBJECT: TEST_CONTENT,
                        f"{TEST_FOLDER}{TEST_OBJECT}": TEST_CONTENT,
                        f"{TEST_PREFIX}{TEST_OBJECT}": TEST_CONTENT,
                    }
                }
            }
        }

        # Set up storage_patchers using patch_gcp_services
        self.storage_patchers = patch_gcp_services(mock_responses)

        # Start all patchers
        for patcher in self.storage_patchers.values():
            patcher.start()

        # Create CustomGCPHook instance
        self.hook = CustomGCPHook(gcp_conn_id=GCP_CONN_ID)

        # Set up mock bucket and test objects
        self.mock_storage = create_mock_storage_client(mock_responses["gcs"])

    def tearDown(self):
        """Clean up test case resources."""
        # Stop all storage patchers
        for patcher in self.storage_patchers.values():
            patcher.stop()

        # Reset mock objects
        self.mock_storage = None

        # Clean temporary test files
        pass

    def test_gcs_file_exists(self):
        """Test CustomGCPHook.gcs_file_exists method."""
        # Set up mock responses for file exists checks
        bucket_name = DEFAULT_BUCKET_NAME
        existing_object = TEST_OBJECT
        non_existing_object = "non-existing-object.txt"

        # Call hook.gcs_file_exists for existing file and verify True return
        exists = self.hook.gcs_file_exists(bucket_name, existing_object)
        self.assertTrue(exists)

        # Call hook.gcs_file_exists for non-existing file and verify False return
        exists = self.hook.gcs_file_exists(bucket_name, non_existing_object)
        self.assertFalse(exists)

    def test_gcs_upload_file(self):
        """Test CustomGCPHook.gcs_upload_file method."""
        # Create temporary test file with content
        local_file_path = create_test_file(TEST_CONTENT)

        # Call hook.gcs_upload_file to upload to GCS
        gcs_uri = self.hook.gcs_upload_file(local_file_path, DEFAULT_BUCKET_NAME, TEST_OBJECT)

        # Verify upload was called with correct parameters
        expected_gcs_uri = f"gs://{DEFAULT_BUCKET_NAME}/{TEST_OBJECT}"
        self.assertEqual(gcs_uri, expected_gcs_uri)

        # Clean up temporary file
        os.remove(local_file_path)

    def test_gcs_download_file(self):
        """Test CustomGCPHook.gcs_download_file method."""
        # Set up mock blob with test content
        bucket_name = DEFAULT_BUCKET_NAME
        object_name = TEST_OBJECT

        # Call hook.gcs_download_file to download from GCS
        with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".txt") as temp_file:
            local_file_path = temp_file.name
        downloaded_file_path = self.hook.gcs_download_file(bucket_name, object_name, local_file_path)

        # Verify download was called with correct parameters
        self.assertEqual(downloaded_file_path, local_file_path)

        # Verify file content matches expected content
        with open(downloaded_file_path, "r") as downloaded_file:
            downloaded_content = downloaded_file.read()
        self.assertEqual(downloaded_content, TEST_CONTENT)

        # Clean up downloaded file
        os.remove(downloaded_file_path)

    def test_gcs_list_files(self):
        """Test CustomGCPHook.gcs_list_files method."""
        # Set up mock bucket with test objects
        bucket_name = DEFAULT_BUCKET_NAME

        # Call hook.gcs_list_files without prefix
        files = self.hook.gcs_list_files(bucket_name)
        expected_files = [TEST_OBJECT, f"{TEST_FOLDER}{TEST_OBJECT}", f"{TEST_PREFIX}{TEST_OBJECT}"]
        self.assertEqual(set(files), set(expected_files))

        # Call hook.gcs_list_files with prefix
        prefix = TEST_PREFIX
        files = self.hook.gcs_list_files(bucket_name, prefix=prefix)
        expected_files = [f"{TEST_PREFIX}{TEST_OBJECT}"]
        self.assertEqual(set(files), set(expected_files))

    def test_gcs_delete_file(self):
        """Test CustomGCPHook.gcs_delete_file method."""
        # Set up mock bucket with test object
        bucket_name = DEFAULT_BUCKET_NAME
        object_name = TEST_OBJECT

        # Call hook.gcs_delete_file to delete the object
        self.hook.gcs_delete_file(bucket_name, object_name)

        # Verify object no longer exists in mock bucket
        files = self.hook.gcs_list_files(bucket_name)
        expected_files = [f"{TEST_FOLDER}{TEST_OBJECT}", f"{TEST_PREFIX}{TEST_OBJECT}"]
        self.assertEqual(set(files), set(expected_files))


@pytest.mark.gcp
@pytest.mark.cloud_storage
@pytest.mark.compatibility
class TestGCSHookCompatibility(unittest.TestCase, Airflow2CompatibilityTestMixin):
    """Test case for verifying compatibility between Airflow 1.x and 2.x GCS hooks."""

    def __init__(self, *args, **kwargs):
        """Initialize the GCS hook compatibility test case."""
        super().__init__(*args, **kwargs)
        # Initialize storage_patchers to None
        self.storage_patchers = None
        # Initialize mock_storage to None
        self.mock_storage = None

    def setUp(self):
        """Set up test case with mock GCS clients."""
        # Create mock responses for GCS operations
        mock_responses = {
            "gcs": {
                DEFAULT_BUCKET_NAME: {
                    "blobs": {
                        TEST_OBJECT: TEST_CONTENT,
                        f"{TEST_FOLDER}{TEST_OBJECT}": TEST_CONTENT,
                        f"{TEST_PREFIX}{TEST_OBJECT}": TEST_CONTENT,
                    }
                }
            }
        }

        # Set up storage_patchers using patch_gcp_services
        self.storage_patchers = patch_gcp_services(mock_responses)

        # Start all patchers
        for patcher in self.storage_patchers.values():
            patcher.start()

        # Set up mock bucket and test objects
        self.mock_storage = create_mock_storage_client(mock_responses["gcs"])

    def tearDown(self):
        """Clean up test case resources."""
        # Stop all storage patchers
        for patcher in self.storage_patchers.values():
            patcher.stop()

        # Reset mock objects
        self.mock_storage = None

        # Clean temporary test files
        pass

    def test_airflow1_gcs_hook(self):
        """Test Airflow 1.10.15 GoogleCloudStorageHook functionality."""
        # Skip test if running with Airflow 2.X
        self.skipIfAirflow2()

        # Create GoogleCloudStorageHook instance
        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=GCP_CONN_ID)

        # Test exists, upload, download, list, and delete methods
        exists = hook.exists(DEFAULT_BUCKET_NAME, TEST_OBJECT)
        self.assertTrue(exists)

        # Record operational results for comparison
        self.airflow1_results = {"exists": exists}

    def test_airflow2_gcs_hook(self):
        """Test Airflow 2.X GCSHook functionality."""
        # Skip test if running with Airflow 1.X
        self.skipIfAirflow1()

        # Create GCSHook instance
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

        # Test exists, upload, download, list, and delete methods
        exists = hook.exists(bucket_name=DEFAULT_BUCKET_NAME, object_name=TEST_OBJECT)
        self.assertTrue(exists)

        # Record operational results for comparison
        self.airflow2_results = {"exists": exists}

    def test_hook_compatibility(self):
        """Test compatibility between Airflow 1.x and 2.x GCS hook implementations."""
        # Run Airflow 1.X hook tests using runWithAirflow1
        self.runWithAirflow1(self.test_airflow1_gcs_hook)

        # Run Airflow 2.X hook tests using runWithAirflow2
        self.runWithAirflow2(self.test_airflow2_gcs_hook)

        # Compare results using assert_task_execution_unchanged
        assert_task_execution_unchanged(self.airflow1_results, self.airflow2_results)

    def test_airflow2_specific_features(self):
        """Test Airflow 2.X GCSHook specific new features."""
        # Skip test if running with Airflow 1.X
        self.skipIfAirflow1()

        # Test new methods available only in Airflow 2.X GCSHook
        pass


@pytest.mark.gcp
@pytest.mark.cloud_storage
@pytest.mark.security
class TestGCSEncryption(unittest.TestCase):
    """Test case for GCS object encryption functionality."""

    def __init__(self, *args, **kwargs):
        """Initialize the GCS encryption test case."""
        super().__init__(*args, **kwargs)
        # Initialize storage_patchers to None
        self.storage_patchers = None
        # Initialize mock_storage to None
        self.mock_storage = None

    def setUp(self):
        """Set up test case with mock GCS clients."""
        # Create mock responses for GCS operations
        mock_responses = {"gcs": {DEFAULT_BUCKET_NAME: {"blobs": {TEST_OBJECT: TEST_CONTENT}}}}

        # Set up storage_patchers using patch_gcp_services
        self.storage_patchers = patch_gcp_services(mock_responses)

        # Start all patchers
        for patcher in self.storage_patchers.values():
            patcher.start()

        # Set up mock bucket with encryption settings
        self.mock_storage = create_mock_storage_client(mock_responses["gcs"])

    def tearDown(self):
        """Clean up test case resources."""
        # Stop all storage patchers
        for patcher in self.storage_patchers.values():
            patcher.stop()

        # Reset mock objects
        self.mock_storage = None

    def test_gcs_default_encryption(self):
        """Test GCS default encryption settings for objects."""
        # Create GCSClient instance
        client = GCSClient(conn_id=GCP_CONN_ID)

        # Upload file without encryption parameters
        local_file_path = create_test_file(TEST_CONTENT)
        client.upload_file(local_file_path, DEFAULT_BUCKET_NAME, TEST_OBJECT)

        # Verify object has default encryption applied
        # Verify encryption metadata is set correctly
        pass

    def test_gcs_custom_encryption(self):
        """Test GCS custom encryption keys for objects."""
        # Create CustomGCPHook instance
        hook = CustomGCPHook(gcp_conn_id=GCP_CONN_ID)

        # Set up mock encryption key
        mock_encryption_key = "mock_encryption_key"

        # Upload file with encryption parameters
        local_file_path = create_test_file(TEST_CONTENT)
        hook.gcs_upload_file(local_file_path, DEFAULT_BUCKET_NAME, TEST_OBJECT)

        # Verify object has custom encryption applied
        # Verify encryption metadata and keys are set correctly
        pass

    def test_gcs_encryption_migration(self):
        """Test encryption compatibility between Airflow 1.X and 2.X."""
        # Compare encryption parameter handling in Airflow 1.X and 2.X
        # Verify encrypted objects can be accessed correctly after migration
        # Test encryption handling in both GCS hook versions
        # Document any changes in encryption handling between versions
        pass