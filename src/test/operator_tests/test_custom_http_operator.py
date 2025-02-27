"""
Test suite for the CustomHttpOperator and CustomHttpSensorOperator classes, validating their
functionality for both Airflow 1.10.15 and Airflow 2.X as part of the Cloud Composer migration project.
The tests ensure proper execution, error handling, response processing, and compatibility with the
new Airflow 2.X architecture.
"""
import unittest
from unittest import mock
import pytest
import requests
import datetime
import json
import tempfile
import os

from airflow.exceptions import AirflowException  # apache-airflow 2.0.0+

# Internal imports
from backend.plugins.operators.custom_http_operator import CustomHttpOperator, CustomHttpSensorOperator  # src/backend/plugins/operators/custom_http_operator.py
from backend.plugins.hooks.custom_http_hook import CustomHTTPHook  # src/backend/plugins/hooks/custom_http_hook.py
from test.utils.airflow2_compatibility_utils import is_airflow2, Airflow2CompatibilityTestMixin  # src/test/utils/airflow2_compatibility_utils.py
from test.fixtures.mock_hooks import MockCustomHTTPHook  # src/test/fixtures/mock_hooks.py
from test.fixtures.mock_connections import create_mock_http_connection, patch_airflow_connections  # src/test/fixtures/mock_connections.py
from test.fixtures.mock_operators import create_mock_context  # src/test/fixtures/mock_operators.py

# Global constants
TEST_HTTP_CONN_ID = "http_test"
DEFAULT_ENDPOINT = "/api/test"
TEST_MOCK_RESPONSES = json.dumps({
    "success": {"data": {"status": "success", "value": 42}},
    "error": {"status_code": 500, "error": True},
    "filtered": {"data": {"nested": {"value": 100}}}
})


def setup_module():
    """Module setup function to configure test environment"""
    create_mock_http_connection(conn_id=TEST_HTTP_CONN_ID, host="https://example.com")
    global mock_responses
    mock_responses = json.loads(TEST_MOCK_RESPONSES)
    global connection_patch
    connection_patch = patch_airflow_connections()
    connection_patch.start()


def teardown_module():
    """Module cleanup function"""
    connection_patch.stop()


def response_check_function(response: dict) -> bool:
    """Test function for checking HTTP responses"""
    if "status" in response and response["status"] == "success":
        return True
    return False


class TestCustomHttpOperator(unittest.TestCase, Airflow2CompatibilityTestMixin):
    """Test case for CustomHttpOperator functionality"""

    def __init__(self, *args, **kwargs):
        """Initialize test case"""
        super().__init__(*args, **kwargs)
        self.mock_hook = None
        self.test_context = None
        self.success_response = None
        self.error_response = None
        self.nested_response = None
        self.patcher = None

    def setUp(self):
        """Set up test fixtures"""
        self.mock_hook = MockCustomHTTPHook(
            conn_id=TEST_HTTP_CONN_ID, method="GET", mock_responses={}
        )
        self.patcher = mock.patch(
            "backend.plugins.operators.custom_http_operator.CustomHTTPHook",
            return_value=self.mock_hook,
        )
        self.patcher.start()

        self.success_response = {"status": "success", "value": 42}
        self.error_response = {"status_code": 500, "error": True}
        self.nested_response = {"nested": {"value": 100}}
        self.test_context = create_mock_context()

    def tearDown(self):
        """Clean up test fixtures"""
        self.patcher.stop()
        self.mock_hook = None
        self.test_context = None
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)

    def test_init(self):
        """Test operator initialization with different parameters"""
        # Test with minimal parameters
        op1 = CustomHttpOperator(task_id="test_http_min", endpoint=DEFAULT_ENDPOINT)
        self.assertEqual(op1.endpoint, DEFAULT_ENDPOINT)
        self.assertEqual(op1.http_conn_id, TEST_HTTP_CONN_ID)

        # Test with all parameters
        op2 = CustomHttpOperator(
            task_id="test_http_all",
            endpoint=DEFAULT_ENDPOINT,
            method="POST",
            http_conn_id=TEST_HTTP_CONN_ID,
            data={"key": "value"},
            headers={"Content-Type": "application/json"},
            params={"param1": "value1"},
            response_filter="$.data.value",
            response_check=response_check_function,
            use_advanced_retry=False,
            retry_limit=5,
            retry_delay=2.0,
            retry_backoff=1.5,
            retry_status_codes=[404, 500],
            extra_options={"timeout": 30},
            alert_on_error=True,
        )
        self.assertEqual(op2.method, "POST")
        self.assertEqual(op2.data, {"key": "value"})
        self.assertEqual(op2.headers, {"Content-Type": "application/json"})
        self.assertEqual(op2.params, {"param1": "value1"})
        self.assertEqual(op2.response_filter, "$.data.value")
        self.assertEqual(op2.response_check, response_check_function)
        self.assertEqual(op2.use_advanced_retry, False)
        self.assertEqual(op2.retry_limit, 5)
        self.assertEqual(op2.retry_delay, 2.0)
        self.assertEqual(op2.retry_backoff, 1.5)
        self.assertEqual(op2.retry_status_codes, [404, 500])
        self.assertEqual(op2.extra_options, {"timeout": 30})
        self.assertEqual(op2.alert_on_error, True)

        # Test that default values are used when parameters not provided
        self.assertEqual(op1.method, "GET")
        self.assertEqual(op1.data, {})
        self.assertEqual(op1.headers, {})
        self.assertEqual(op1.params, {})
        self.assertEqual(op1.response_filter, None)
        self.assertEqual(op1.response_check, None)
        self.assertEqual(op1.use_advanced_retry, True)
        self.assertEqual(op1.retry_limit, 3)
        self.assertEqual(op1.retry_delay, 1.0)
        self.assertEqual(op1.retry_backoff, 2.0)
        self.assertEqual(op1.retry_status_codes, [500, 502, 503, 504])
        self.assertEqual(op1.extra_options, {})
        self.assertEqual(op1.alert_on_error, True)

    def test_execute_success(self):
        """Test successful HTTP operator execution"""
        self.mock_hook.run.return_value = requests.Response()
        self.mock_hook.run.return_value.status_code = 200
        self.mock_hook.run.return_value.json.return_value = self.success_response
        op = CustomHttpOperator(task_id="test_http_success", endpoint=DEFAULT_ENDPOINT)
        result = op.execute(context=self.test_context)
        self.assertEqual(result, self.success_response)
        self.mock_hook.run.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT, data={}, headers={}, params={}, extra_options={}
        )

    def test_execute_error(self):
        """Test HTTP operator execution with error response"""
        self.mock_hook.run.return_value = requests.Response()
        self.mock_hook.run.return_value.status_code = 500
        self.mock_hook.run.return_value.json.return_value = self.error_response
        op = CustomHttpOperator(task_id="test_http_error", endpoint=DEFAULT_ENDPOINT)
        with self.assertRaises(AirflowException) as context:
            op.execute(self.test_context)
        self.assertTrue("HTTP error" in str(context.exception))
        self.mock_hook.run.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT, data={}, headers={}, params={}, extra_options={}
        )

    def test_response_filter(self):
        """Test filtering of HTTP responses"""
        self.mock_hook.run.return_value = requests.Response()
        self.mock_hook.run.return_value.status_code = 200
        self.mock_hook.run.return_value.json.return_value = {"data": self.nested_response}
        op = CustomHttpOperator(
            task_id="test_http_filter", endpoint=DEFAULT_ENDPOINT, response_filter="$.data.nested.value"
        )
        result = op.execute(self.test_context)
        self.assertEqual(result, 100)

    def test_response_check(self):
        """Test response validation with check function"""
        self.mock_hook.run.return_value = requests.Response()
        self.mock_hook.run.return_value.status_code = 200
        self.mock_hook.run.return_value.json.return_value = self.success_response
        op = CustomHttpOperator(
            task_id="test_http_check", endpoint=DEFAULT_ENDPOINT, response_check=response_check_function
        )
        result = op.execute(self.test_context)
        self.assertEqual(result, self.success_response)

        # Test with failing response check
        self.mock_hook.run.return_value.json.return_value = {"status": "failure", "value": 0}
        op = CustomHttpOperator(
            task_id="test_http_check_fail", endpoint=DEFAULT_ENDPOINT, response_check=response_check_function
        )
        with self.assertRaises(AirflowException) as context:
            op.execute(self.test_context)
        self.assertTrue("Response check failed" in str(context.exception))

    def test_download_file(self):
        """Test file download functionality"""
        self.temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(self.temp_dir, "downloaded_file.txt")
        self.mock_hook.download_file.return_value = local_path
        op = CustomHttpOperator(task_id="test_http_download", endpoint=DEFAULT_ENDPOINT)
        downloaded_path = op.download_file(local_path=local_path, context=self.test_context)
        self.assertEqual(downloaded_path, local_path)
        self.mock_hook.download_file.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT, local_path=local_path, params={}, headers={}, create_dirs=True
        )

        # Test download with directory creation
        local_path_new_dir = os.path.join(self.temp_dir, "new_dir", "downloaded_file.txt")
        downloaded_path = op.download_file(local_path=local_path_new_dir, create_dirs=True, context=self.test_context)
        self.assertEqual(downloaded_path, local_path_new_dir)
        self.mock_hook.download_file.assert_called_with(
            endpoint=DEFAULT_ENDPOINT, local_path=local_path_new_dir, params={}, headers={}, create_dirs=True
        )

    def test_upload_file(self):
        """Test file upload functionality"""
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.mock_hook.upload_file.return_value = requests.Response()
        op = CustomHttpOperator(task_id="test_http_upload", endpoint=DEFAULT_ENDPOINT)
        op.upload_file(file_path=temp_file.name, context=self.test_context)
        self.mock_hook.upload_file.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT, file_path=temp_file.name, field_name="file", data={}, headers={}, params={}
        )
        os.remove(temp_file.name)

    def test_advanced_retry(self):
        """Test advanced retry functionality"""
        self.mock_hook.run_with_advanced_retry.return_value = requests.Response()
        self.mock_hook.run_with_advanced_retry.return_value.status_code = 200
        self.mock_hook.run_with_advanced_retry.return_value.json.return_value = self.success_response
        op = CustomHttpOperator(
            task_id="test_http_retry", endpoint=DEFAULT_ENDPOINT, retry_limit=3, retry_delay=1.0
        )
        op.execute(self.test_context)
        self.mock_hook.run_with_advanced_retry.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT, data={}, headers={}, params={}, extra_options={},
            retry_limit=3, retry_delay=1.0, retry_backoff=2.0, retry_status_codes=[500, 502, 503, 504]
        )

    @pytest.mark.skipif(not is_airflow2(), reason="Test requires Airflow 2.X")
    def test_airflow2_compatibility(self):
        """Test compatibility with Airflow 2.X"""
        # Configure test for Airflow 2.X specific features
        # Create operator with Airflow 2.X parameters
        # Execute operator with test context
        # Verify operator works correctly with Airflow 2.X provider packages
        # Test Airflow 2.X specific error handling
        pass


class TestCustomHttpSensorOperator(unittest.TestCase, Airflow2CompatibilityTestMixin):
    """Test case for CustomHttpSensorOperator functionality"""

    def __init__(self, *args, **kwargs):
        """Initialize test case"""
        super().__init__(*args, **kwargs)
        self.mock_hook = None
        self.test_context = None
        self.success_response = None
        self.error_response = None
        self.patcher = None

    def setUp(self):
        """Set up test fixtures"""
        self.mock_hook = MockCustomHTTPHook(
            conn_id=TEST_HTTP_CONN_ID, method="GET", mock_responses={}
        )
        self.patcher = mock.patch(
            "backend.plugins.operators.custom_http_operator.CustomHTTPHook",
            return_value=self.mock_hook,
        )
        self.patcher.start()

        self.success_response = {"status": "success", "value": 42}
        self.error_response = {"status_code": 500, "error": True}
        self.test_context = create_mock_context()

    def tearDown(self):
        """Clean up test fixtures"""
        self.patcher.stop()
        self.mock_hook = None
        self.test_context = None

    def test_init(self):
        """Test sensor initialization with different parameters"""
        # Test with minimal parameters
        op1 = CustomHttpSensorOperator(task_id="test_http_min", endpoint=DEFAULT_ENDPOINT)
        self.assertEqual(op1.endpoint, DEFAULT_ENDPOINT)
        self.assertEqual(op1.http_conn_id, TEST_HTTP_CONN_ID)

        # Test with all parameters
        op2 = CustomHttpSensorOperator(
            task_id="test_http_all",
            endpoint=DEFAULT_ENDPOINT,
            http_conn_id=TEST_HTTP_CONN_ID,
            method="POST",
            data={"key": "value"},
            headers={"Content-Type": "application/json"},
            params={"param1": "value1"},
            response_filter="$.data.value",
            response_check=response_check_function,
            retry_limit=5,
            extra_options={"timeout": 30},
            alert_on_error=True,
        )
        self.assertEqual(op2.method, "POST")
        self.assertEqual(op2.data, {"key": "value"})
        self.assertEqual(op2.headers, {"Content-Type": "application/json"})
        self.assertEqual(op2.params, {"param1": "value1"})
        self.assertEqual(op2.response_filter, "$.data.value")
        self.assertEqual(op2.response_check, response_check_function)
        self.assertEqual(op2.retry_limit, 5)
        self.assertEqual(op2.extra_options, {"timeout": 30})
        self.assertEqual(op2.alert_on_error, True)

        # Test that default values are used when parameters not provided
        self.assertEqual(op1.method, "GET")
        self.assertEqual(op1.data, {})
        self.assertEqual(op1.headers, {})
        self.assertEqual(op1.params, {})
        self.assertEqual(op1.response_filter, None)
        self.assertEqual(op1.response_check, None)
        self.assertEqual(op1.retry_limit, 3)
        self.assertEqual(op1.extra_options, {})
        self.assertEqual(op1.alert_on_error, True)

    def test_poke_success(self):
        """Test sensor poke method with success response"""
        self.mock_hook.run.return_value = requests.Response()
        self.mock_hook.run.return_value.status_code = 200
        self.mock_hook.run.return_value.json.return_value = self.success_response
        op = CustomHttpSensorOperator(
            task_id="test_http_poke_success",
            endpoint=DEFAULT_ENDPOINT,
            response_check=response_check_function,
        )
        result = op.poke(context=self.test_context)
        self.assertTrue(result)
        self.mock_hook.run.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT, data={}, headers={}, params={}, extra_options={}
        )

    def test_poke_failure(self):
        """Test sensor poke method with failing check"""
        self.mock_hook.run.return_value = requests.Response()
        self.mock_hook.run.return_value.status_code = 200
        self.mock_hook.run.return_value.json.return_value = {"status": "failure", "value": 0}
        op = CustomHttpSensorOperator(
            task_id="test_http_poke_failure",
            endpoint=DEFAULT_ENDPOINT,
            response_check=response_check_function,
        )
        result = op.poke(context=self.test_context)
        self.assertFalse(result)
        self.mock_hook.run.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT, data={}, headers={}, params={}, extra_options={}
        )

    def test_poke_error(self):
        """Test sensor poke method with HTTP error"""
        self.mock_hook.run.return_value = requests.Response()
        self.mock_hook.run.return_value.status_code = 500
        self.mock_hook.run.return_value.json.return_value = self.error_response
        op = CustomHttpSensorOperator(task_id="test_http_poke_error", endpoint=DEFAULT_ENDPOINT)
        with self.assertRaises(AirflowException) as context:
            op.poke(self.test_context)
        self.assertTrue("HTTP error" in str(context.exception))

        # Test with alert_on_error=False and verify no exception
        op = CustomHttpSensorOperator(
            task_id="test_http_poke_error_no_alert", endpoint=DEFAULT_ENDPOINT, alert_on_error=False
        )
        result = op.poke(self.test_context)
        self.assertFalse(result)

    @pytest.mark.skipif(not is_airflow2(), reason="Test requires Airflow 2.X")
    def test_airflow2_compatibility(self):
        """Test sensor compatibility with Airflow 2.X"""
        # Configure test for Airflow 2.X specific features
        # Create sensor with Airflow 2.X parameters
        # Execute poke method with test context
        # Verify sensor works correctly with Airflow 2.X provider packages
        # Test Airflow 2.X specific timeout handling
        pass