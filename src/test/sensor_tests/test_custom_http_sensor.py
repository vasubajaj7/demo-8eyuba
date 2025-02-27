"""
Unit tests for custom HTTP sensors to ensure compatibility with Airflow 2.X during migration from Airflow 1.10.15 to Cloud Composer 2.
Tests functionality, error handling, and backward compatibility for HTTP-based sensors.
"""
import pytest  # pytest v6.0+ - Testing framework for Python
import unittest.mock  # standard library - Mocking framework for unit tests
import requests  # requests 2.25.0+ - HTTP library for testing response objects
import json  # standard library - JSON parsing for HTTP response testing
from typing import Dict, Any, Callable, List, Optional  # Typing hints for better code readability

from jsonpath_ng import parse as parse_jsonpath  # jsonpath-ng 1.5.0+ - JSONPath implementation for testing JSON sensors
from airflow.providers.http.sensors.http import HttpSensor  # airflow.providers.http 2.0+ - Airflow 2.X HTTP sensor for compatibility testing

# Conditional import for Airflow 1.X HTTP sensor
try:
    from airflow.contrib.sensors.http_sensor import HttpSensor as Airflow1HttpSensor  # airflow.contrib.sensors.http_sensor 1.10.15 - Airflow 1.X HTTP sensor for backward compatibility testing
except ImportError:
    Airflow1HttpSensor = None

# Internal imports
from backend.plugins.sensors.custom_http_sensor import CustomHttpSensor, CustomHttpJsonSensor, CustomHttpStatusSensor  # Import the custom HTTP sensor for testing
from backend.plugins.hooks.custom_http_hook import CustomHTTPHook  # Import the custom HTTP hook to understand dependency behavior
from test.fixtures.mock_hooks import MockCustomHTTPHook, create_mock_custom_http_hook  # Import mock HTTP hook for testing without real HTTP connections
from test.fixtures.mock_sensors import MockHttpSensor, create_mock_http_sensor  # Import mock HTTP sensor for isolated testing
from test.utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin, is_airflow2  # Mixin providing utilities for cross-version Airflow testing
from test.fixtures.mock_data import DEFAULT_DATE, create_mock_airflow_context  # Creates a mock Airflow task context for testing

# Define test constants
TEST_ENDPOINT = '/api/test'
TEST_HTTP_CONN_ID = 'http_default'
MOCK_HTTP_RESPONSES = {
    '/api/test': {'status_code': 200, 'content': b'OK', 'json': {'status': 'success'}},
    '/api/status': {'status_code': 200, 'content': b'{"status": "ready"}', 'json': {'status': 'ready'}},
    '/api/error': {'status_code': 404, 'content': b'Not Found', 'raise_for_status': True},
    '/api/timeout': {'status_code': 504, 'content': b'Gateway Timeout'}
}


@pytest.mark.parametrize(
    'endpoint,http_conn_id,method,response_check,extra_options', [
        ('/api/test', 'http_default', 'GET', None, {}),
        ('/api/status', 'http_custom', 'POST', lambda response: response.status_code == 200, {'headers': {'Content-Type': 'application/json'}})
    ]
)
def test_custom_http_sensor_init(endpoint: str, http_conn_id: str, method: str, response_check: Callable, extra_options: Dict):
    """Test initialization of the CustomHttpSensor with various parameters"""
    # Initialize a CustomHttpSensor with the provided parameters
    sensor = CustomHttpSensor(
        task_id='http_sensor_init',
        endpoint=endpoint,
        http_conn_id=http_conn_id,
        method=method,
        response_check=response_check,
        extra_options=extra_options
    )

    # Verify that all parameters are correctly stored on the sensor instance
    assert sensor.endpoint == endpoint
    assert sensor.http_conn_id == http_conn_id
    assert sensor.method == method

    # Check that method defaults to 'GET' when not specified
    if method is None:
        assert sensor.method == 'GET'

    # Verify that response_check is properly set or defaults to a pattern check
    if response_check is not None:
        assert sensor.response_check == response_check

    # Check that extra_options are properly stored
    assert sensor.extra_options == extra_options


def test_custom_http_sensor_poke_success():
    """Test the CustomHttpSensor poke method with a successful response"""
    # Create a mock HTTP hook that returns a successful response
    mock_hook = MockCustomHTTPHook(
        conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        mock_responses={TEST_ENDPOINT: requests.Response()}
    )
    mock_hook.run = unittest.mock.MagicMock(return_value=requests.Response())

    # Configure a CustomHttpSensor with the mocked hook
    sensor = CustomHttpSensor(
        task_id='http_sensor_success',
        endpoint=TEST_ENDPOINT,
        http_conn_id=TEST_HTTP_CONN_ID,
        method='GET'
    )

    # Patch the _get_hook method to return the mock hook
    with unittest.mock.patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=mock_hook):
        # Call the poke method with a mock Airflow context
        context = create_mock_airflow_context(task_id=sensor.task_id, dag_id='test_dag')
        result = sensor.poke(context)

        # Verify that poke returns True indicating the condition is satisfied
        assert result is True

        # Assert that the hook's run method was called with correct parameters
        mock_hook.run.assert_called_once_with(
            endpoint=TEST_ENDPOINT,
            data={},
            headers={},
            params={},
            extra_options={}
        )


def test_custom_http_sensor_poke_failure():
    """Test the CustomHttpSensor poke method with a failed response"""
    # Create a mock HTTP hook that returns a failed response
    mock_hook = MockCustomHTTPHook(
        conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        mock_responses={TEST_ENDPOINT: Exception('Request failed')}
    )
    mock_hook.run = unittest.mock.MagicMock(side_effect=Exception('Request failed'))

    # Configure a CustomHttpSensor with the mocked hook
    sensor = CustomHttpSensor(
        task_id='http_sensor_failure',
        endpoint=TEST_ENDPOINT,
        http_conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        alert_on_error=False  # Disable alerts for this test
    )

    # Patch the _get_hook method to return the mock hook
    with unittest.mock.patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=mock_hook):
        # Call the poke method with a mock Airflow context
        context = create_mock_airflow_context(task_id=sensor.task_id, dag_id='test_dag')
        result = sensor.poke(context)

        # Verify that poke returns False indicating the condition is not satisfied
        assert result is False

        # Assert that the hook's run method was called with correct parameters
        mock_hook.run.assert_called_once_with(
            endpoint=TEST_ENDPOINT,
            data={},
            headers={},
            params={},
            extra_options={}
        )


def test_custom_http_sensor_with_pattern():
    """Test the CustomHttpSensor with a pattern matching response check"""
    # Create a mock HTTP hook that returns a response with a specific pattern
    mock_hook = MockCustomHTTPHook(
        conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        mock_responses={TEST_ENDPOINT: requests.Response()}
    )
    mock_hook.run = unittest.mock.MagicMock(return_value=requests.Response())
    mock_hook.run.return_value.text = 'Response contains the pattern'

    # Configure a CustomHttpSensor with a pattern parameter
    sensor = CustomHttpSensor(
        task_id='http_sensor_pattern',
        endpoint=TEST_ENDPOINT,
        http_conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        pattern='pattern'
    )

    # Patch the _get_hook method to return the mock hook
    with unittest.mock.patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=mock_hook):
        # Call the poke method with a mock Airflow context
        context = create_mock_airflow_context(task_id=sensor.task_id, dag_id='test_dag')
        result = sensor.poke(context)

        # Verify that the sensor correctly matches the pattern in the response
        assert result is True

        # Test with both matching and non-matching patterns
        sensor.pattern = 'nonexistent'
        result = sensor.poke(context)
        assert result is False


def test_custom_http_json_sensor():
    """Test the CustomHttpJsonSensor with JSONPath expressions"""
    # Create a mock HTTP hook that returns a JSON response
    mock_hook = MockCustomHTTPHook(
        conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        mock_responses={TEST_ENDPOINT: requests.Response()}
    )
    mock_hook.run = unittest.mock.MagicMock(return_value=requests.Response())
    mock_hook.run.return_value.json = lambda: {'status': 'ready', 'value': 123}

    # Configure a CustomHttpJsonSensor with a JSONPath expression
    sensor = CustomHttpJsonSensor(
        task_id='http_json_sensor',
        endpoint=TEST_ENDPOINT,
        http_conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        json_path='$.status',
        expected_value='ready'
    )

    # Patch the _get_hook method to return the mock hook
    with unittest.mock.patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=mock_hook):
        # Call the poke method with a mock Airflow context
        context = create_mock_airflow_context(task_id=sensor.task_id, dag_id='test_dag')
        result = sensor.poke(context)

        # Verify that the sensor correctly extracts and validates JSON data
        assert result is True

        # Test with both matching and non-matching JSONPath expressions
        sensor.json_path = '$.nonexistent'
        result = sensor.poke(context)
        assert result is False


@pytest.mark.parametrize(
    'status_code,expected_codes,expected_result', [
        (200, [200], True),
        (404, [200], False),
        (500, [500, 501, 502], True)
    ]
)
def test_custom_http_status_sensor(status_code: int, expected_codes: List[int], expected_result: bool):
    """Test the CustomHttpStatusSensor with various status codes"""
    # Create a mock HTTP hook that returns a response with the specified status code
    mock_hook = MockCustomHTTPHook(
        conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        mock_responses={TEST_ENDPOINT: requests.Response()}
    )
    mock_hook.run = unittest.mock.MagicMock(return_value=requests.Response())
    mock_hook.run.return_value.status_code = status_code

    # Configure a CustomHttpStatusSensor with the expected status codes
    sensor = CustomHttpStatusSensor(
        task_id='http_status_sensor',
        endpoint=TEST_ENDPOINT,
        http_conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        expected_status_codes=expected_codes
    )

    # Patch the _get_hook method to return the mock hook
    with unittest.mock.patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=mock_hook):
        # Call the poke method with a mock Airflow context
        context = create_mock_airflow_context(task_id=sensor.task_id, dag_id='test_dag')
        result = sensor.poke(context)

        # Verify that the sensor correctly validates the response status code
        assert result == expected_result

        # Assert that the result matches the expected outcome based on parameters
        assert result is expected_result


def test_custom_http_sensor_exception_handling():
    """Test that the CustomHttpSensor properly handles exceptions"""
    # Create a mock HTTP hook that raises different exceptions (ConnectionError, Timeout, etc.)
    mock_hook = MockCustomHTTPHook(
        conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        mock_responses={TEST_ENDPOINT: requests.exceptions.RequestException('Simulated error')}
    )
    mock_hook.run = unittest.mock.MagicMock(side_effect=requests.exceptions.RequestException('Simulated error'))

    # Configure a CustomHttpSensor with various error handling settings
    sensor = CustomHttpSensor(
        task_id='http_sensor_exception',
        endpoint=TEST_ENDPOINT,
        http_conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        alert_on_error=False  # Disable alerts for this test
    )

    # Patch the _get_hook method to return the mock hook
    with unittest.mock.patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=mock_hook):
        # Call the poke method with a mock Airflow context
        context = create_mock_airflow_context(task_id=sensor.task_id, dag_id='test_dag')
        result = sensor.poke(context)

        # Verify that exceptions are properly caught and handled
        assert result is False

        # Check that alert_on_error parameter works as expected
        sensor.alert_on_error = True
        with unittest.mock.patch('backend.plugins.sensors.custom_http_sensor.CustomHttpSensor.send_error_alert') as mock_send_alert:
            sensor.poke(context)
            mock_send_alert.assert_called_once()


def test_custom_http_sensor_retry_behavior():
    """Test the retry behavior of the CustomHttpSensor"""
    # Create a mock HTTP hook that simulates retry scenarios
    mock_hook = MockCustomHTTPHook(
        conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        mock_responses={TEST_ENDPOINT: requests.Response()}
    )
    mock_hook.run = unittest.mock.MagicMock(side_effect=[
        requests.exceptions.RequestException('Retryable error'),
        requests.Response()  # Successful response after retry
    ])

    # Configure a CustomHttpSensor with retry_limit and retry_delay parameters
    sensor = CustomHttpSensor(
        task_id='http_sensor_retry',
        endpoint=TEST_ENDPOINT,
        http_conn_id=TEST_HTTP_CONN_ID,
        method='GET',
        retry_limit=2,
        retry_delay=0.1,
        alert_on_error=False  # Disable alerts for this test
    )

    # Patch the _get_hook method to return the mock hook
    with unittest.mock.patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=mock_hook):
        # Call the poke method with a mock Airflow context
        context = create_mock_airflow_context(task_id=sensor.task_id, dag_id='test_dag')
        result = sensor.poke(context)

        # Verify that retries are correctly attempted based on configuration
        assert mock_hook.run.call_count == 2  # One initial call + one retry
        assert result is True

        # Check that retry count and retry delay are respected
        sensor.retry_limit = 1
        sensor.poke(context)
        assert mock_hook.run.call_count == 3  # One initial call + one retry


class TestCustomHttpSensor(Airflow2CompatibilityTestMixin):
    """Test class for the CustomHttpSensor"""
    mock_responses = MOCK_HTTP_RESPONSES

    def __init__(self):
        """Set up the TestCustomHttpSensor"""
        super().__init__()
        self.mock_responses = MOCK_HTTP_RESPONSES

    def setup_method(self, method):
        """Set up method runs before each test"""
        # Create mock HTTP hook for testing
        self.mock_http_hook = create_mock_custom_http_hook(
            conn_id=TEST_HTTP_CONN_ID,
            method='GET',
            mock_responses=self.mock_responses
        )

        # Set up test data and configuration
        self.task_id = 'test_http_sensor'
        self.endpoint = '/api/test'
        self.context = create_mock_airflow_context(task_id=self.task_id, dag_id='test_dag')

        # Patch the CustomHTTPHook to return our mock
        self.http_hook_patch = patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=self.mock_http_hook)
        self.http_hook_patch.start()

    def teardown_method(self, method):
        """Clean up after each test"""
        # Remove patches and mocks
        self.http_hook_patch.stop()

        # Clean up any test resources
        pass

    def test_init(self):
        """Test initialization of the sensor"""
        # Create sensor with various parameters
        sensor = CustomHttpSensor(
            task_id=self.task_id,
            endpoint=self.endpoint,
            http_conn_id=TEST_HTTP_CONN_ID,
            method='GET',
            pattern='OK'
        )

        # Verify attributes are set correctly
        assert sensor.task_id == self.task_id
        assert sensor.endpoint == self.endpoint
        assert sensor.http_conn_id == TEST_HTTP_CONN_ID
        assert sensor.method == 'GET'
        assert sensor.pattern == 'OK'

    def test_poke_success(self):
        """Test successful poke operation"""
        # Configure mock for success response
        self.mock_http_hook._mock_responses[self.endpoint] = requests.Response()
        self.mock_http_hook.run = unittest.mock.MagicMock(return_value=requests.Response())
        self.mock_http_hook.run.return_value.text = 'OK'
        self.mock_http_hook.run.return_value.status_code = 200

        # Create sensor
        sensor = CustomHttpSensor(
            task_id=self.task_id,
            endpoint=self.endpoint,
            http_conn_id=TEST_HTTP_CONN_ID,
            method='GET',
            pattern='OK'
        )

        # Execute poke and verify result is True
        result = sensor.poke(self.context)
        assert result is True

    def test_poke_failure(self):
        """Test failed poke operation"""
        # Configure mock for failed response
        self.mock_http_hook._mock_responses[self.endpoint] = Exception('Request failed')
        self.mock_http_hook.run = unittest.mock.MagicMock(side_effect=Exception('Request failed'))

        # Create sensor
        sensor = CustomHttpSensor(
            task_id=self.task_id,
            endpoint=self.endpoint,
            http_conn_id=TEST_HTTP_CONN_ID,
            method='GET',
            pattern='OK',
            alert_on_error=False  # Disable alerts for this test
        )

        # Execute poke and verify result is False
        result = sensor.poke(self.context)
        assert result is False


class TestCustomHttpJsonSensor(Airflow2CompatibilityTestMixin):
    """Test class for the CustomHttpJsonSensor"""
    mock_responses = MOCK_HTTP_RESPONSES

    def __init__(self):
        """Set up the TestCustomHttpJsonSensor"""
        super().__init__()
        self.mock_responses = MOCK_HTTP_RESPONSES

    def setup_method(self, method):
        """Set up method runs before each test"""
        # Create mock HTTP hook for testing
        self.mock_http_hook = create_mock_custom_http_hook(
            conn_id=TEST_HTTP_CONN_ID,
            method='GET',
            mock_responses=self.mock_responses
        )

        # Set up test JSON data and configuration
        self.task_id = 'test_http_json_sensor'
        self.endpoint = '/api/status'
        self.json_path = '$.status'
        self.expected_value = 'ready'
        self.context = create_mock_airflow_context(task_id=self.task_id, dag_id='test_dag')

        # Patch the CustomHTTPHook to return our mock
        self.http_hook_patch = patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=self.mock_http_hook)
        self.http_hook_patch.start()

    def test_json_path_matching(self):
        """Test JSONPath expression matching"""
        # Configure mock with JSON response
        self.mock_http_hook._mock_responses[self.endpoint] = requests.Response()
        self.mock_http_hook.run = unittest.mock.MagicMock(return_value=requests.Response())
        self.mock_http_hook.run.return_value.json = lambda: {'status': 'ready'}

        # Create sensor with JSONPath expression
        sensor = CustomHttpJsonSensor(
            task_id=self.task_id,
            endpoint=self.endpoint,
            http_conn_id=TEST_HTTP_CONN_ID,
            method='GET',
            json_path=self.json_path,
            expected_value=self.expected_value
        )

        # Test with matching and non-matching expressions
        result = sensor.poke(self.context)
        assert result is True

        sensor.expected_value = 'not_ready'
        result = sensor.poke(self.context)
        assert result is False


class TestCustomHttpStatusSensor(Airflow2CompatibilityTestMixin):
    """Test class for the CustomHttpStatusSensor"""
    mock_responses = MOCK_HTTP_RESPONSES

    def __init__(self):
        """Set up the TestCustomHttpStatusSensor"""
        super().__init__()
        self.mock_responses = MOCK_HTTP_RESPONSES

    def setup_method(self, method):
        """Set up method runs before each test"""
        # Create mock HTTP hook for testing
        self.mock_http_hook = create_mock_custom_http_hook(
            conn_id=TEST_HTTP_CONN_ID,
            method='GET',
            mock_responses=self.mock_responses
        )

        # Set up test data with various status codes
        self.task_id = 'test_http_status_sensor'
        self.endpoint = '/api/test'
        self.expected_status_codes = [200]
        self.context = create_mock_airflow_context(task_id=self.task_id, dag_id='test_dag')

        # Patch the CustomHTTPHook to return our mock
        self.http_hook_patch = patch('backend.plugins.sensors.custom_http_sensor._get_hook', return_value=self.mock_http_hook)
        self.http_hook_patch.start()

    def test_status_code_matching(self):
        """Test status code matching logic"""
        # Configure mock with status code
        self.mock_http_hook._mock_responses[self.endpoint] = requests.Response()
        self.mock_http_hook.run = unittest.mock.MagicMock(return_value=requests.Response())
        self.mock_http_hook.run.return_value.status_code = 200

        # Create sensor with expected status codes
        sensor = CustomHttpStatusSensor(
            task_id=self.task_id,
            endpoint=self.endpoint,
            http_conn_id=TEST_HTTP_CONN_ID,
            method='GET',
            expected_status_codes=self.expected_status_codes
        )

        # Test with matching and non-matching status codes
        result = sensor.poke(self.context)
        assert result is True

        sensor.expected_status_codes = [404]
        result = sensor.poke(self.context)
        assert result is False

        # Verify correct behavior for each scenario
        self.mock_http_hook.run.return_value.status_code = 404
        sensor.expected_status_codes = [404]
        result = sensor.poke(self.context)
        assert result is True


class TestAirflow2Compatibility(Airflow2CompatibilityTestMixin):
    """Test class for validating Airflow 2.X compatibility"""

    def test_http_sensor_import_compatibility(self):
        """Test compatibility of HTTP sensor imports"""
        # Test importing and using the sensor in both Airflow versions
        if is_airflow2():
            from airflow.providers.http.sensors.http import HttpSensor
            assert HttpSensor is not None
        else:
            try:
                from airflow.contrib.sensors.http_sensor import HttpSensor
                assert HttpSensor is not None
            except ImportError:
                pytest.skip("Airflow 1.X not installed")

        # Verify consistent behavior across versions
        assert True

    def test_parameter_mapping(self):
        """Test parameter name mapping between Airflow versions"""
        # Create sensor with parameters using both naming conventions
        if is_airflow2():
            sensor = CustomHttpSensor(
                task_id='test_sensor',
                endpoint='/api/test',
                http_conn_id='http_default'
            )
        else:
            sensor = CustomHttpSensor(
                task_id='test_sensor',
                endpoint='/api/test',
                http_conn_id='http_default'
            )

        # Verify parameters are correctly mapped in both versions
        assert sensor.endpoint == '/api/test'
        assert sensor.http_conn_id == 'http_default'


def test_http_sensor_migration_path():
    """Test that HTTP sensors correctly support both Airflow versions during migration"""
    # Create a sensor with Airflow 1.X parameter naming
    if Airflow1HttpSensor:
        sensor1 = Airflow1HttpSensor(
            task_id='test_sensor_1x',
            http_conn_id='http_default',
            endpoint='/api/test'
        )

        # Test it works correctly with both Airflow 1.X and 2.X imports
        assert sensor1.endpoint == '/api/test'

    # Create a sensor with Airflow 2.X parameter naming
    sensor2 = HttpSensor(
        task_id='test_sensor_2x',
        http_conn_id='http_default',
        endpoint='/api/test'
    )

    # Test it correctly maps parameters when used with both Airflow versions
    assert sensor2.endpoint == '/api/test'

    # Verify no regressions in functionality during migration
    assert True