"""
Custom HTTP sensors for Apache Airflow 2.X.

This module provides enhanced HTTP sensors that extend the standard Airflow HTTP
sensor capabilities with additional functionality for API monitoring, including:
- JSON response validation with JSONPath
- Pattern matching in responses
- Status code validation
- Robust retry capabilities
- Comprehensive error handling with alerting
- Full Airflow 2.X compatibility

These sensors are designed to work with the CustomHTTPHook and integrate with
the alert and validation utilities.
"""

import logging
import json
import re
from typing import Dict, Any, Optional, Callable, List, Pattern, Union

# For JSONPath support
from jsonpath_ng import parse as parse_jsonpath
from jsonpath_ng.exceptions import JsonPathParserError

# Airflow imports
from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

# Internal imports
from ..hooks.custom_http_hook import CustomHTTPHook
from ...dags.utils.alert_utils import send_alert, AlertLevel
from ...dags.utils.validation_utils import validate_sensor_args

# Default connection ID
DEFAULT_HTTP_CONN_ID = 'http_default'

# Set up logging
logger = logging.getLogger('airflow.sensors.custom_http_sensor')


def _get_hook(http_conn_id: str, method: str, retry_limit: int, retry_delay: float) -> CustomHTTPHook:
    """
    Helper function to instantiate a CustomHTTPHook with the appropriate parameters.
    
    Args:
        http_conn_id: Airflow connection ID to use
        method: HTTP method to use (GET, POST, etc.)
        retry_limit: Maximum number of retries
        retry_delay: Delay between retries in seconds
        
    Returns:
        CustomHTTPHook: Configured HTTP hook instance
    """
    hook = CustomHTTPHook(
        http_conn_id=http_conn_id,
        method=method,
        retry_limit=retry_limit,
        retry_delay=retry_delay
    )
    
    logger.debug(f"Created HTTP hook with connection ID: {http_conn_id}, method: {method}")
    return hook


class CustomHttpSensor(BaseSensorOperator):
    """
    Sensor that periodically polls an HTTP endpoint and executes a provided 
    response check function.
    
    This sensor enhances the standard HttpSensor with improved error handling,
    configurable retry behavior, and alerting capabilities.
    
    Attributes:
        endpoint: The relative URL to hit
        http_conn_id: Airflow connection ID for the HTTP connection
        method: HTTP method to use
        headers: HTTP headers to send with the request
        params: Query parameters to include in the request
        data: Request payload for POST/PUT methods
        extra_options: Additional options for the HTTP request
        response_check: Function that returns True if condition is met
        pattern: Regex pattern to search for in the response
        retry_limit: Maximum number of retry attempts for the HTTP request
        retry_delay: Delay between retries in seconds
        alert_on_error: Whether to send an alert when an HTTP error occurs
    """
    
    template_fields = ('endpoint', 'headers', 'params', 'data')

    def __init__(
        self,
        *,
        endpoint: str,
        http_conn_id: str = DEFAULT_HTTP_CONN_ID,
        method: str = 'GET',
        headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        extra_options: Optional[Dict] = None,
        response_check: Optional[Callable] = None,
        pattern: Optional[str] = None,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        alert_on_error: bool = False,
        **kwargs
    ) -> None:
        """
        Initialize the CustomHttpSensor.
        
        Args:
            endpoint: The relative URL to hit
            http_conn_id: Airflow connection ID for the HTTP connection
            method: HTTP method to use
            headers: HTTP headers to send with the request
            params: Query parameters to include in the request
            data: Request payload for POST/PUT methods
            extra_options: Additional options for the HTTP request
            response_check: Function that returns True if condition is met
            pattern: Regex pattern to search for in the response
            retry_limit: Maximum number of retry attempts for the HTTP request
            retry_delay: Delay between retries in seconds
            alert_on_error: Whether to send an alert when an HTTP error occurs
            **kwargs: Additional arguments to pass to the BaseSensorOperator
        """
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.method = method
        self.headers = headers or {}
        self.params = params or {}
        self.data = data or {}
        self.extra_options = extra_options or {}
        
        # Set response_check function
        if response_check:
            self.response_check = response_check
        elif pattern:
            self.response_check = self._pattern_response_check
            self.pattern = pattern
        else:
            self.response_check = lambda response: True
            
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.alert_on_error = alert_on_error
        
        # Validate the sensor arguments for Airflow 2.X compatibility
        validate_sensor_args(self)

    def _pattern_response_check(self, response) -> bool:
        """
        Check if the response contains the specified pattern.
        
        Args:
            response: The HTTP response object
            
        Returns:
            bool: True if the pattern is found in the response text, False otherwise
        """
        if response is None:
            return False
            
        response_text = response.text
        match = re.search(self.pattern, response_text)
        found = match is not None
        
        if found:
            logger.info(f"Pattern '{self.pattern}' found in response")
        else:
            logger.info(f"Pattern '{self.pattern}' not found in response")
            
        return found

    @apply_defaults
    def poke(self, context: Dict) -> bool:
        """
        Execute the sensor's logic.
        
        Args:
            context: Airflow context dictionary
            
        Returns:
            bool: True if the criteria are met, False otherwise
        """
        hook = _get_hook(
            http_conn_id=self.http_conn_id,
            method=self.method,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay
        )
        
        try:
            logger.info(f"Poking: {self.endpoint} with method {self.method}")
            response = hook.run(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                params=self.params,
                extra_options=self.extra_options
            )
            
            logger.info(f"HTTP response status code: {response.status_code}")
            return self.response_check(response)
            
        except Exception as e:
            logger.error(f"HTTP request failed: {str(e)}")
            
            if self.alert_on_error:
                self.send_error_alert(e, context)
                
            return False

    def send_error_alert(self, exception: Exception, context: Dict) -> None:
        """
        Send an alert when the sensor encounters an error.
        
        Args:
            exception: The exception that occurred
            context: Airflow context dictionary
        """
        error_message = (
            f"HTTP Sensor Error: {self.task_id} failed to connect to {self.endpoint}. "
            f"Error: {str(exception)}"
        )
        
        dag_id = context.get('dag').dag_id if context.get('dag') else 'unknown'
        task_id = context.get('task').task_id if context.get('task') else self.task_id
        
        logger.error(f"Sending alert for sensor error: {error_message}")
        
        send_alert(
            alert_level=AlertLevel.ERROR,
            context={
                'dag_id': dag_id,
                'task_id': task_id,
                'endpoint': self.endpoint,
                'method': self.method,
                'status': 'ERROR',
                'message': error_message
            },
            exception=exception
        )


class CustomHttpJsonSensor(CustomHttpSensor):
    """
    Sensor that polls an HTTP endpoint and validates the JSON response using JSONPath.
    
    This sensor extends CustomHttpSensor to specifically handle JSON responses and
    validate them using JSONPath expressions.
    
    Attributes:
        json_path: JSONPath expression to evaluate against the response
        expected_value: Expected value from the JSONPath expression
    """
    
    template_fields = CustomHttpSensor.template_fields + ('json_path',)

    def __init__(
        self,
        *,
        endpoint: str,
        json_path: str,
        expected_value: Any = None,
        http_conn_id: str = DEFAULT_HTTP_CONN_ID,
        method: str = 'GET',
        headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        alert_on_error: bool = False,
        **kwargs
    ) -> None:
        """
        Initialize the CustomHttpJsonSensor.
        
        Args:
            endpoint: The relative URL to hit
            json_path: JSONPath expression to evaluate against the response
            expected_value: Expected value from the JSONPath expression
            http_conn_id: Airflow connection ID for the HTTP connection
            method: HTTP method to use
            headers: HTTP headers to send with the request
            params: Query parameters to include in the request
            data: Request payload for POST/PUT methods
            retry_limit: Maximum number of retry attempts for the HTTP request
            retry_delay: Delay between retries in seconds
            alert_on_error: Whether to send an alert when an HTTP error occurs
            **kwargs: Additional arguments to pass to the CustomHttpSensor
        """
        self.json_path = json_path
        self.expected_value = expected_value
        
        # Parse JSONPath expression
        try:
            self.jsonpath_expression = parse_jsonpath(json_path)
        except JsonPathParserError as e:
            raise AirflowException(f"Invalid JSONPath expression: {json_path}. Error: {str(e)}")
        
        # Call parent constructor
        super().__init__(
            endpoint=endpoint,
            http_conn_id=http_conn_id,
            method=method,
            headers=headers,
            params=params,
            data=data,
            response_check=self._json_path_response_check,
            retry_limit=retry_limit,
            retry_delay=retry_delay,
            alert_on_error=alert_on_error,
            **kwargs
        )

    def _json_path_response_check(self, response) -> bool:
        """
        Check if the JSONPath expression finds the expected value in the response.
        
        Args:
            response: The HTTP response object
            
        Returns:
            bool: True if the expected value is found, False otherwise
        """
        if response is None:
            return False
            
        try:
            # Parse response as JSON
            json_data = response.json()
            
            # Apply JSONPath expression
            matches = [match.value for match in self.jsonpath_expression.find(json_data)]
            
            if self.expected_value is not None:
                # Check if any of the found values match the expected value
                found = self.expected_value in matches
                logger.info(
                    f"JSONPath '{self.json_path}' found {len(matches)} match(es). "
                    f"Expected value {self.expected_value} was {'' if found else 'not '}found."
                )
            else:
                # If no expected value specified, check if there are any matches
                found = len(matches) > 0
                logger.info(
                    f"JSONPath '{self.json_path}' found {len(matches)} match(es). "
                    f"Looking for any match: {found}"
                )
                
            return found
            
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to parse response as JSON: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error checking JSON response with JSONPath: {str(e)}")
            return False


class CustomHttpStatusSensor(CustomHttpSensor):
    """
    Sensor that polls an HTTP endpoint and checks for specific status codes.
    
    This sensor extends CustomHttpSensor to specifically validate HTTP status codes
    in the response.
    
    Attributes:
        expected_status_codes: List of acceptable HTTP status codes
    """
    
    def __init__(
        self,
        *,
        endpoint: str,
        expected_status_codes: Optional[List[int]] = None,
        http_conn_id: str = DEFAULT_HTTP_CONN_ID,
        method: str = 'GET',
        headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        alert_on_error: bool = False,
        **kwargs
    ) -> None:
        """
        Initialize the CustomHttpStatusSensor.
        
        Args:
            endpoint: The relative URL to hit
            expected_status_codes: List of acceptable HTTP status codes
            http_conn_id: Airflow connection ID for the HTTP connection
            method: HTTP method to use
            headers: HTTP headers to send with the request
            params: Query parameters to include in the request
            data: Request payload for POST/PUT methods
            retry_limit: Maximum number of retry attempts for the HTTP request
            retry_delay: Delay between retries in seconds
            alert_on_error: Whether to send an alert when an HTTP error occurs
            **kwargs: Additional arguments to pass to the CustomHttpSensor
        """
        self.expected_status_codes = expected_status_codes or [200]
        
        # Call parent constructor
        super().__init__(
            endpoint=endpoint,
            http_conn_id=http_conn_id,
            method=method,
            headers=headers,
            params=params,
            data=data,
            response_check=self._status_code_response_check,
            retry_limit=retry_limit,
            retry_delay=retry_delay,
            alert_on_error=alert_on_error,
            **kwargs
        )

    def _status_code_response_check(self, response) -> bool:
        """
        Check if the response status code matches any of the expected status codes.
        
        Args:
            response: The HTTP response object
            
        Returns:
            bool: True if the status code matches, False otherwise
        """
        if response is None:
            return False
            
        status_code = response.status_code
        matches = status_code in self.expected_status_codes
        
        if matches:
            logger.info(f"Status code {status_code} matches one of the expected codes: {self.expected_status_codes}")
        else:
            logger.info(f"Status code {status_code} does not match any expected code: {self.expected_status_codes}")
            
        return matches