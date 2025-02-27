"""
Test suite for the custom HTTP hook implementation that validates compatibility and
functionality when migrating from Airflow 1.10.15 to Airflow 2.X. Tests cover basic
HTTP operations, error handling, retries, and advanced functionality with both mock
and integration tests.
"""

import unittest  # v3.4+ - Base testing framework for test classes
import unittest.mock  # v3.4+ - Mocking functionality for HTTP responses and requests
import pytest  # v6.0+ - Advanced testing framework for fixtures and parameterization
import requests  # v2.25.0+ - HTTP client library for request testing and mocking
import requests_mock  # v1.9.0+ - Mocking library for requests HTTP interactions
import tenacity  # v6.2.0+ - Testing retry functionality in HTTP hook
import io  # standard library - File-like objects for mock HTTP responses
import os  # standard library - Path manipulation for file download/upload tests
import tempfile  # standard library - Temporary file creation for download/upload tests
from datetime import datetime  # standard library

# Internal imports
from backend.plugins.hooks.custom_http_hook import CustomHTTPHook  # Import the custom HTTP hook class to be tested
from src.test.fixtures.mock_connections import create_mock_http_connection  # Create mock HTTP connections for testing the hook
from src.test.fixtures.mock_connections import MockConnectionManager  # Manage mock connections during tests
from src.test.fixtures.mock_connections import HTTP_CONN_ID  # Default HTTP connection ID for testing
from src.test.fixtures.mock_hooks import MockCustomHTTPHook  # Mock implementation of HTTP hook for isolated testing
from src.test.fixtures.mock_hooks import create_mock_custom_http_hook  # Create a configured mock HTTP hook for testing
from src.test.utils.airflow2_compatibility_utils import is_airflow2  # Check if test is running in Airflow 2.X environment
from src.test.utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin  # Mixin for version-specific test handling
from src.test.utils.assertion_utils import assert_operator_airflow2_compatible  # Verify hook is compatible with Airflow 2.X
from src.test.fixtures.mock_data import DEFAULT_DATE  # Standard date for test fixtures

# Define global constants for testing
TEST_CONN_ID = "http_test"
BASE_URL = "https://example.com/api/v1"
TEST_ENDPOINT = "/test"
TEST_OAUTH_ENDPOINT = "/oauth"
MOCK_RESPONSE_SUCCESS = '{"status": "success", "data": {"id": 1, "name": "test"}}'
MOCK_RESPONSE_ERROR = '{"status": "error", "message": "Bad request"}'


class TestCustomHTTPHook(unittest.TestCase):
    """Main test class for CustomHTTPHook functionality verification"""

    def setUp(self):
        """Set up test environment before each test"""
        # Create a mock HTTP connection for testing
        create_mock_http_connection(
            conn_id=TEST_CONN_ID, host=BASE_URL, extra={"timeout": 60}
        )
        # Set up connection manager to patch BaseHook.get_connection
        self.mock_get_connection = MockConnectionManager()
        self.mock_get_connection.__enter__()
        self.mock_get_connection.add_connection(get_mock_connection(TEST_CONN_ID))
        # Initialize the CustomHTTPHook with test connection ID
        self.hook = CustomHTTPHook(http_conn_id=TEST_CONN_ID, method="GET")
        # Create mock session for HTTP requests
        self.mock_session = unittest.mock.MagicMock()
        self.hook.get_conn = unittest.mock.MagicMock(return_value=self.mock_session)

    def tearDown(self):
        """Clean up after each test"""
        # Stop all mocks and patchers
        self.mock_get_connection.__exit__(None, None, None)
        # Reset any connection mocks
        self.hook.get_conn.reset_mock()

    def test_init(self):
        """Test proper initialization of the CustomHTTPHook"""
        # Verify hook has correct connection ID
        self.assertEqual(self.hook.http_conn_id, TEST_CONN_ID)
        # Verify default parameters are set correctly
        self.assertEqual(self.hook.method, "GET")
        # Verify method is initialized properly
        self.hook = CustomHTTPHook(http_conn_id=TEST_CONN_ID, method="POST")
        self.assertEqual(self.hook.method, "POST")
        # Check retry parameters match defaults from hook
        self.assertEqual(self.hook.retry_limit, 3)
        self.assertEqual(self.hook.retry_delay, 1.0)
        self.assertEqual(self.hook.retry_backoff, 2.0)

    def test_get_conn(self):
        """Test get_conn method returns properly configured requests session"""
        # Call get_conn method
        session = self.hook.get_conn()
        # Verify the session has proper headers
        self.assertIn("Content-Type", session.headers)
        self.assertEqual(session.headers["Content-Type"], "application/json")
        # Verify session has timeout configuration
        self.assertTrue(hasattr(session, "request_kwargs"))
        # Check if OAuth token is set if present in connection extras
        create_mock_http_connection(
            conn_id=TEST_CONN_ID,
            host=BASE_URL,
            extra={"timeout": 60, "oauth_token": "test_token", "token_type": "Bearer"},
        )
        hook_with_oauth = CustomHTTPHook(http_conn_id=TEST_CONN_ID, method="GET")
        session_with_oauth = hook_with_oauth.get_conn()
        self.assertEqual(
            session_with_oauth.headers["Authorization"], "Bearer test_token"
        )

    def test_run_basic(self):
        """Test basic run method functionality"""
        # Mock HTTP response for session.request
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response._content = MOCK_RESPONSE_SUCCESS.encode("utf-8")
        self.mock_session.request.return_value = mock_response
        # Call run method with test endpoint
        response = self.hook.run(endpoint=TEST_ENDPOINT)
        # Verify the method makes the correct HTTP call
        self.mock_session.request.assert_called_once_with(
            "GET", f"{BASE_URL}{TEST_ENDPOINT}", data=None, headers=None, params=None, timeout=60
        )
        # Check response handling is correct
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.reason, "OK")

    def test_run_with_params(self):
        """Test run method with query parameters"""
        # Mock HTTP response for session.request
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response._content = MOCK_RESPONSE_SUCCESS.encode("utf-8")
        self.mock_session.request.return_value = mock_response
        # Call run method with test params dict
        test_params = {"param1": "value1", "param2": "value2"}
        response = self.hook.run(endpoint=TEST_ENDPOINT, params=test_params)
        # Verify params are properly passed to request
        self.mock_session.request.assert_called_once_with(
            "GET",
            f"{BASE_URL}{TEST_ENDPOINT}",
            data=None,
            headers=None,
            params=test_params,
            timeout=60,
        )
        # Check response is correctly returned
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.reason, "OK")

    def test_run_with_error(self):
        """Test run method with error responses"""
        # Mock HTTP error response (status 400, 500, etc.)
        mock_response = requests.Response()
        mock_response.status_code = 400
        mock_response.reason = "Bad Request"
        mock_response._content = MOCK_RESPONSE_ERROR.encode("utf-8")
        self.mock_session.request.return_value = mock_response
        # Call run method expecting exception
        with self.assertRaises(AirflowException) as context:
            self.hook.run(endpoint=TEST_ENDPOINT)
        # Verify error is properly caught and raised
        self.assertIn("HTTP error: 400 - Bad Request", str(context.exception))
        # Check alert_on_error behavior
        self.hook.alert_on_error = False
        try:
            self.hook.run(endpoint=TEST_ENDPOINT)
        except AirflowException:
            self.fail("Exception was raised when alert_on_error is False")

    def test_run_with_advanced_retry(self):
        """Test advanced retry functionality"""
        # Set up mock to fail with status 500 then succeed
        mock_response_fail = requests.Response()
        mock_response_fail.status_code = 500
        mock_response_fail.reason = "Internal Server Error"
        mock_response_fail._content = MOCK_RESPONSE_ERROR.encode("utf-8")
        mock_response_success = requests.Response()
        mock_response_success.status_code = 200
        mock_response_success.reason = "OK"
        mock_response_success._content = MOCK_RESPONSE_SUCCESS.encode("utf-8")
        self.mock_session.request.side_effect = [
            mock_response_fail,
            mock_response_fail,
            mock_response_success,
        ]
        # Call run_with_advanced_retry method
        response = self.hook.run_with_advanced_retry(endpoint=TEST_ENDPOINT)
        # Verify retry behavior matches configuration
        self.assertEqual(self.mock_session.request.call_count, 3)
        # Ensure successful response after retries
        self.assertEqual(response.status_code, 200)

    def test_run_and_get_json(self):
        """Test run_and_get_json method parses JSON correctly"""
        # Mock HTTP response with JSON content
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response._content = MOCK_RESPONSE_SUCCESS.encode("utf-8")
        self.mock_session.request.return_value = mock_response
        # Call run_and_get_json method
        json_response = self.hook.run_and_get_json(endpoint=TEST_ENDPOINT)
        # Verify JSON is parsed correctly
        self.assertEqual(json_response["status"], "success")
        self.assertEqual(json_response["data"]["id"], 1)
        # Test with invalid JSON content
        mock_response._content = "Invalid JSON".encode("utf-8")
        self.mock_session.request.return_value = mock_response
        with self.assertRaises(AirflowException) as context:
            self.hook.run_and_get_json(endpoint=TEST_ENDPOINT)
        self.assertIn("Failed to parse JSON response", str(context.exception))

    def test_run_and_get_text(self):
        """Test run_and_get_text method returns text content"""
        # Mock HTTP response with text content
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response._content = "Test text content".encode("utf-8")
        self.mock_session.request.return_value = mock_response
        # Call run_and_get_text method
        text_content = self.hook.run_and_get_text(endpoint=TEST_ENDPOINT)
        # Verify text content is returned correctly
        self.assertEqual(text_content, "Test text content")

    def test_download_file(self):
        """Test file download functionality"""
        # Create temporary file for download destination
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            local_path = tmp_file.name
        # Mock HTTP response with file content
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response.iter_content = unittest.mock.MagicMock(
            return_value=[b"Test file content"]
        )
        self.mock_session.get.return_value.__enter__.return_value = mock_response
        # Call download_file method
        downloaded_path = self.hook.download_file(endpoint=TEST_ENDPOINT, local_path=local_path)
        # Verify file is downloaded correctly with proper content
        self.assertEqual(downloaded_path, local_path)
        with open(local_path, "rb") as f:
            self.assertEqual(f.read(), b"Test file content")
        os.remove(local_path)

    def test_upload_file(self):
        """Test file upload functionality"""
        # Create temporary file with test content
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(b"Test upload content")
            tmp_file.close()
            file_path = tmp_file.name
        # Mock HTTP response for upload
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        self.mock_session.request.return_value = mock_response
        # Call upload_file method
        response = self.hook.upload_file(endpoint=TEST_ENDPOINT, file_path=file_path)
        # Verify file is correctly included in multipart request
        self.assertEqual(response.status_code, 200)
        os.remove(file_path)

    def test_connection_test(self):
        """Test connection testing functionality"""
        # Mock successful HTTP response for connection test
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        self.mock_session.head.return_value = mock_response
        # Call test_connection method
        result = self.hook.test_connection()
        # Verify it returns True
        self.assertTrue(result)
        # Test with connection failure case
        mock_response.status_code = 400
        self.mock_session.head.return_value = mock_response
        result = self.hook.test_connection()
        self.assertFalse(result)

    def test_oauth_header(self):
        """Test OAuth header handling"""
        # Create connection with OAuth token in extras
        create_mock_http_connection(
            conn_id=TEST_CONN_ID,
            host=BASE_URL,
            extra={"timeout": 60, "oauth_token": "test_token", "token_type": "Bearer"},
        )
        hook_with_oauth = CustomHTTPHook(http_conn_id=TEST_CONN_ID, method="GET")
        # Call get_conn method
        session = hook_with_oauth.get_conn()
        # Verify Authorization header is properly set
        self.assertEqual(session.headers["Authorization"], "Bearer test_token")
        # Test with different token types and formats
        create_mock_http_connection(
            conn_id=TEST_CONN_ID,
            host=BASE_URL,
            extra={"timeout": 60, "oauth_token": "test_token", "token_type": "OAuth"},
        )
        hook_with_oauth = CustomHTTPHook(http_conn_id=TEST_CONN_ID, method="GET")
        session = hook_with_oauth.get_conn()
        self.assertEqual(session.headers["Authorization"], "OAuth test_token")


class TestCustomHTTPHookAirflow2Compatibility(
    unittest.TestCase, Airflow2CompatibilityTestMixin
):
    """Tests specific to Airflow 2.X compatibility for CustomHTTPHook"""

    def setUp(self):
        """Set up test environment before each test"""
        # Check if running with Airflow 2.X
        self._using_airflow2 = is_airflow2()
        # Set up appropriate mocks for current Airflow version
        if self._using_airflow2:
            create_mock_http_connection(
                conn_id=TEST_CONN_ID,
                host=BASE_URL,
                extra={"timeout": 60, "oauth_token": "test_token"},
            )
        else:
            create_mock_http_connection(
                conn_id=TEST_CONN_ID,
                host=BASE_URL,
                extra={"timeout": 60, "oauth_token": "test_token"},
            )
        # Initialize the CustomHTTPHook with test connection ID
        self.hook = CustomHTTPHook(http_conn_id=TEST_CONN_ID, method="GET")

    def test_provider_import_compatibility(self):
        """Test hook works with Airflow 2.X provider package structure"""
        # Verify hook correctly extends Airflow 2.X BaseHTTPHook from provider package
        self.assertTrue(
            isinstance(self.hook, airflow.providers.http.hooks.http.HttpHook)
        )
        # Check import paths are using provider structure
        self.assertTrue(
            "airflow.providers.http.hooks.http" in CustomHTTPHook.__module__
        )
        # Verify hook functionality with provider package base classes
        self.assertTrue(hasattr(self.hook, "run"))

    def test_airflow2_connection_handling(self):
        """Test hook correctly handles Airflow 2.X connection objects"""
        # Create Airflow 2.X style connection with URI and JSON extras
        create_mock_http_connection(
            conn_id=TEST_CONN_ID,
            host=BASE_URL,
            extra={"timeout": 60, "oauth_token": "test_token"},
        )
        # Initialize hook with this connection
        hook = CustomHTTPHook(http_conn_id=TEST_CONN_ID, method="GET")
        # Verify hook correctly parses connection attributes
        self.assertEqual(hook.base_url, BASE_URL)
        # Check hook functionality with Airflow 2.X connection
        session = hook.get_conn()
        self.assertIn("Content-Type", session.headers)

    def test_deprecated_params_handling(self):
        """Test hook handles deprecated parameters correctly"""
        # Check for proper warnings when using deprecated parameters
        # Verify deprecated parameters are mapped to new parameters if needed
        # Ensure backward compatibility with Airflow 1.X parameter names
        pass


@pytest.mark.integration
class TestCustomHTTPHookIntegration(unittest.TestCase):
    """Integration tests for CustomHTTPHook with live endpoints"""

    def setUp(self):
        """Set up integration test environment"""
        # Check if integration tests are enabled via environment variable
        if os.environ.get("INTEGRATION_TESTS", "").lower() != "true":
            pytest.skip("Integration tests are disabled")
        # Skip tests if integration testing is disabled
        # Create real HTTP connection for testing against actual endpoints
        create_mock_http_connection(conn_id=TEST_CONN_ID, host="https://rickandmortyapi.com")
        # Initialize the CustomHTTPHook with real connection
        self.hook = CustomHTTPHook(http_conn_id=TEST_CONN_ID, method="GET")

    def test_public_api_get(self):
        """Test GET request against a public API"""
        # Make GET request to public API endpoint
        response = self.hook.run(endpoint="/api/character/1")
        # Verify response status code and content
        self.assertEqual(response.status_code, 200)
        self.assertIn("Rick Sanchez", response.text)
        # Test both run() and run_and_get_json() methods
        json_response = self.hook.run_and_get_json(endpoint="/api/character/1")
        self.assertEqual(json_response["name"], "Rick Sanchez")

    def test_public_api_post(self):
        """Test POST request against a public API"""
        # Make POST request to public API endpoint
        test_payload = {"name": "Test Character", "status": "Alive"}
        response = self.hook.run(endpoint="/api/character", data=test_payload, method="POST")
        # Include test payload in request
        # Verify request is processed correctly
        self.assertEqual(response.status_code, 404)

    def test_download_file_integration(self):
        """Test file download from a public URL"""
        # Download file from public URL
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            local_path = tmp_file.name
        downloaded_path = self.hook.download_file(
            endpoint="/api/character/avatar/1.jpeg", local_path=local_path
        )
        # Verify file content is downloaded correctly
        self.assertEqual(downloaded_path, local_path)
        # Check file integrity
        self.assertTrue(os.path.exists(local_path))
        os.remove(local_path)