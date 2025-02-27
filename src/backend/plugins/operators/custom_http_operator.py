"""
Enhanced HTTP operator for Apache Airflow 2.X.

This module provides enhanced HTTP operators that extend the standard SimpleHttpOperator 
with advanced features for API integrations, robust error handling, advanced retry 
capabilities, and response processing. It is designed to work with Airflow 2.X and is 
part of the migration from Airflow 1.10.15 to Cloud Composer 2.
"""

import os
import logging
import json
import tempfile
from typing import Any, Dict, List, Optional, Union, Callable

import jmespath
import tenacity

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.operators.http import SimpleHttpOperator

# Internal imports
from ..hooks.custom_http_hook import CustomHTTPHook
from ...dags.utils.validation_utils import validate_connection
from ...dags.utils.alert_utils import send_alert, AlertLevel

# Set up logging
logger = logging.getLogger('airflow.operators.custom_http_operator')

# Default constants
DEFAULT_HTTP_CONN_ID = 'http_default'
DEFAULT_RETRY_LIMIT = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_RETRY_BACKOFF = 2.0
DEFAULT_RETRY_STATUS_CODES = [500, 502, 503, 504]


def _process_response(response: object, response_filter: str = None, response_check: callable = None) -> object:
    """
    Process HTTP response with optional filtering and validation.
    
    Args:
        response: The HTTP response object
        response_filter: JMESPath expression to filter the response data
        response_check: Function to validate the response
        
    Returns:
        Processed response data (filtered if filter applied)
        
    Raises:
        AirflowException: If response_check returns False
    """
    # Check if response has JSON content
    content_type = response.headers.get('content-type', '')
    is_json = 'application/json' in content_type.lower()
    
    # Parse response based on content type
    if is_json:
        try:
            response_data = response.json()
        except ValueError:
            # Failed to parse JSON, fall back to text content
            logger.warning("Failed to parse JSON response despite content-type, using text content")
            response_data = response.text
    else:
        response_data = response.text
    
    # Apply jmespath filter if provided
    if response_filter and is_json:
        try:
            response_data = jmespath.search(response_filter, response_data)
            logger.debug(f"Applied JMESPath filter: {response_filter}")
        except jmespath.exceptions.JMESPathError as e:
            logger.warning(f"JMESPath filter error: {str(e)}, returning unfiltered response")
    
    # Apply response check if provided
    if response_check:
        if not response_check(response_data):
            raise AirflowException("Response check failed: response_check returned False")
    
    return response_data


class CustomHttpOperator(BaseOperator):
    """
    Enhanced HTTP operator for Airflow 2.X with advanced capabilities for API integrations.
    
    This operator extends the functionality of SimpleHttpOperator with:
    - Advanced retry mechanisms using tenacity
    - Robust error handling with alerting
    - Response filtering using JMESPath
    - File download/upload capabilities
    - Enhanced logging
    
    It is designed to work with Airflow 2.X provider packages and supports
    the migration from Airflow 1.10.15 to Airflow 2.X.
    
    :param endpoint: The relative endpoint to call
    :param method: The HTTP method to use (default: GET)
    :param http_conn_id: The Airflow connection ID to use
    :param data: The data to send in the request (dict for JSON, string for raw data)
    :param headers: Additional HTTP headers to send
    :param params: Query parameters to include in the request
    :param response_filter: JMESPath expression to filter the response
    :param response_check: Function to validate the response
    :param use_advanced_retry: Whether to use advanced retry mechanism with tenacity
    :param retry_limit: Maximum number of retry attempts
    :param retry_delay: Initial delay between retries in seconds
    :param retry_backoff: Multiplier for exponential backoff
    :param retry_status_codes: HTTP status codes that should trigger a retry
    :param extra_options: Additional options for the HTTP request
    :param alert_on_error: Whether to send alerts on HTTP errors
    """
    
    template_fields = ('endpoint', 'data', 'headers', 'params')
    template_fields_renderers = {'headers': 'json', 'data': 'json', 'params': 'json'}
    
    def __init__(
        self,
        endpoint: str,
        method: str = 'GET',
        http_conn_id: str = DEFAULT_HTTP_CONN_ID,
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        response_filter: Optional[str] = None,
        response_check: Optional[Callable] = None,
        use_advanced_retry: bool = True,
        retry_limit: int = DEFAULT_RETRY_LIMIT,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
        retry_status_codes: List[int] = None,
        extra_options: Optional[Dict] = None,
        alert_on_error: bool = True,
        **kwargs
    ) -> None:
        """
        Initialize the CustomHttpOperator.
        
        Args:
            endpoint: The relative endpoint to call
            method: The HTTP method to use (default: GET)
            http_conn_id: The Airflow connection ID to use
            data: The data to send in the request
            headers: Additional HTTP headers to send
            params: Query parameters to include in the request
            response_filter: JMESPath expression to filter the response
            response_check: Function to validate the response
            use_advanced_retry: Whether to use advanced retry mechanism
            retry_limit: Maximum number of retry attempts
            retry_delay: Initial delay between retries in seconds
            retry_backoff: Multiplier for exponential backoff
            retry_status_codes: HTTP status codes that should trigger a retry
            extra_options: Additional options for the HTTP request
            alert_on_error: Whether to send alerts on HTTP errors
            **kwargs: Additional arguments for BaseOperator
        """
        super().__init__(**kwargs)
        
        self.endpoint = endpoint
        self.method = method
        self.http_conn_id = http_conn_id
        
        self.data = data or {}
        self.headers = headers or {}
        self.params = params or {}
        
        self.response_filter = response_filter
        self.response_check = response_check
        
        self.use_advanced_retry = use_advanced_retry
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.retry_status_codes = retry_status_codes or DEFAULT_RETRY_STATUS_CODES
        
        self.extra_options = extra_options or {}
        self.alert_on_error = alert_on_error
        
        # Validate connection
        validate_connection(self.http_conn_id, 'http')
        
        logger.info(
            f"Initialized CustomHttpOperator with endpoint={endpoint}, "
            f"method={method}, http_conn_id={http_conn_id}, "
            f"use_advanced_retry={use_advanced_retry}"
        )
    
    def execute(self, context: Dict) -> Any:
        """
        Execute the HTTP request and process response.
        
        Args:
            context: Airflow task context
            
        Returns:
            Processed HTTP response data
            
        Raises:
            AirflowException: If the request fails after all retries
        """
        try:
            # Create HTTP hook
            hook = CustomHTTPHook(
                http_conn_id=self.http_conn_id,
                method=self.method,
                retry_limit=self.retry_limit,
                retry_delay=self.retry_delay,
                retry_backoff=self.retry_backoff,
                retry_status_codes=self.retry_status_codes,
                alert_on_error=self.alert_on_error
            )
            
            logger.info(f"Executing HTTP {self.method} request to {self.endpoint}")
            
            # Execute request with or without advanced retry
            if self.use_advanced_retry:
                response = hook.run_with_advanced_retry(
                    endpoint=self.endpoint,
                    data=self.data,
                    headers=self.headers,
                    params=self.params,
                    extra_options=self.extra_options
                )
            else:
                response = hook.run(
                    endpoint=self.endpoint,
                    data=self.data,
                    headers=self.headers,
                    params=self.params,
                    extra_options=self.extra_options
                )
            
            # Process the response
            result = self.process_response(
                response=response,
                response_filter=self.response_filter,
                response_check=self.response_check
            )
            
            # Log success and return result
            logger.info(f"HTTP request to {self.endpoint} completed successfully")
            return result
            
        except Exception as e:
            # Handle exceptions
            error_msg = f"Error executing HTTP request to {self.endpoint}: {str(e)}"
            logger.error(error_msg)
            
            # Send alert if configured
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context={
                        **context,
                        'status': 'HTTP Request Failed',
                        'endpoint': self.endpoint,
                        'method': self.method,
                        'details': error_msg
                    },
                    exception=e
                )
            
            raise AirflowException(error_msg) from e
    
    def download_file(self, local_path: str, create_dirs: bool = True, context: Dict = None) -> str:
        """
        Download file from HTTP endpoint to local path.
        
        Args:
            local_path: Path where the file should be saved
            create_dirs: Whether to create parent directories if they don't exist
            context: Task context (for error handling)
            
        Returns:
            Path to the downloaded file
            
        Raises:
            AirflowException: If the download fails
        """
        context = context or {}
        
        try:
            # Create HTTP hook
            hook = CustomHTTPHook(
                http_conn_id=self.http_conn_id,
                method='GET',  # Always use GET for downloads
                alert_on_error=self.alert_on_error
            )
            
            logger.info(f"Downloading file from {self.endpoint} to {local_path}")
            
            # Download the file
            downloaded_path = hook.download_file(
                endpoint=self.endpoint,
                local_path=local_path,
                params=self.params,
                headers=self.headers,
                create_dirs=create_dirs
            )
            
            logger.info(f"File successfully downloaded to {downloaded_path}")
            return downloaded_path
            
        except Exception as e:
            # Handle exceptions
            error_msg = f"Error downloading file from {self.endpoint}: {str(e)}"
            logger.error(error_msg)
            
            # Send alert if configured
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context={
                        **context,
                        'status': 'File Download Failed',
                        'endpoint': self.endpoint,
                        'local_path': local_path,
                        'details': error_msg
                    },
                    exception=e
                )
            
            raise AirflowException(error_msg) from e
    
    def upload_file(self, file_path: str, field_name: str = 'file', context: Dict = None) -> object:
        """
        Upload file to HTTP endpoint.
        
        Args:
            file_path: Path to the file to upload
            field_name: Name of the form field for the file
            context: Task context (for error handling)
            
        Returns:
            HTTP response object
            
        Raises:
            AirflowException: If the upload fails
        """
        context = context or {}
        
        try:
            # Create HTTP hook
            hook = CustomHTTPHook(
                http_conn_id=self.http_conn_id,
                method='POST',  # Always use POST for uploads
                alert_on_error=self.alert_on_error
            )
            
            logger.info(f"Uploading file {file_path} to {self.endpoint}")
            
            # Upload the file
            response = hook.upload_file(
                endpoint=self.endpoint,
                file_path=file_path,
                field_name=field_name,
                data=self.data,
                headers=self.headers,
                params=self.params
            )
            
            logger.info(f"File {file_path} successfully uploaded to {self.endpoint}")
            return response
            
        except Exception as e:
            # Handle exceptions
            error_msg = f"Error uploading file to {self.endpoint}: {str(e)}"
            logger.error(error_msg)
            
            # Send alert if configured
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context={
                        **context,
                        'status': 'File Upload Failed',
                        'endpoint': self.endpoint,
                        'file_path': file_path,
                        'details': error_msg
                    },
                    exception=e
                )
            
            raise AirflowException(error_msg) from e
    
    def process_response(self, response: object, response_filter: str = None, response_check: callable = None) -> object:
        """
        Process HTTP response with filtering and validation (public wrapper around _process_response).
        
        Args:
            response: The HTTP response object
            response_filter: JMESPath expression to filter the response data
            response_check: Function to validate the response
            
        Returns:
            Processed response data
        """
        # Use response_filter from instance if not provided
        if response_filter is None:
            response_filter = self.response_filter
            
        # Use response_check from instance if not provided
        if response_check is None:
            response_check = self.response_check
            
        return _process_response(
            response=response,
            response_filter=response_filter,
            response_check=response_check
        )


class CustomHttpSensorOperator(BaseOperator):
    """
    HTTP sensor operator that monitors endpoint until a condition is met.
    
    This sensor polls an HTTP endpoint and succeeds when the response meets
    certain criteria determined by response_filter and response_check.
    
    :param endpoint: The relative endpoint to poll
    :param http_conn_id: The Airflow connection ID to use
    :param method: The HTTP method to use (default: GET)
    :param data: The data to send in the request
    :param headers: Additional HTTP headers to send
    :param params: Query parameters to include in the request
    :param response_filter: JMESPath expression to filter the response
    :param response_check: Function to determine if the condition is met
    :param retry_limit: Maximum number of retry attempts on connection error
    :param extra_options: Additional options for the HTTP request
    :param alert_on_error: Whether to send alerts on HTTP errors
    """
    
    template_fields = ('endpoint', 'data', 'headers', 'params')
    template_fields_renderers = {'headers': 'json', 'data': 'json', 'params': 'json'}
    
    def __init__(
        self,
        endpoint: str,
        http_conn_id: str = DEFAULT_HTTP_CONN_ID,
        method: str = 'GET',
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        response_filter: Optional[str] = None,
        response_check: Optional[Callable] = None,
        retry_limit: int = DEFAULT_RETRY_LIMIT,
        extra_options: Optional[Dict] = None,
        alert_on_error: bool = True,
        **kwargs
    ) -> None:
        """
        Initialize the CustomHttpSensorOperator.
        
        Args:
            endpoint: The relative endpoint to poll
            http_conn_id: The Airflow connection ID to use
            method: The HTTP method to use (default: GET)
            data: The data to send in the request
            headers: Additional HTTP headers to send
            params: Query parameters to include in the request
            response_filter: JMESPath expression to filter the response
            response_check: Function to determine if the condition is met
            retry_limit: Maximum number of retry attempts on connection error
            extra_options: Additional options for the HTTP request
            alert_on_error: Whether to send alerts on HTTP errors
            **kwargs: Additional arguments for BaseOperator
        """
        super().__init__(**kwargs)
        
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.method = method
        
        self.data = data or {}
        self.headers = headers or {}
        self.params = params or {}
        
        self.response_filter = response_filter
        self.response_check = response_check
        self.retry_limit = retry_limit
        
        self.extra_options = extra_options or {}
        self.alert_on_error = alert_on_error
        
        # Validate connection
        validate_connection(self.http_conn_id, 'http')
        
        logger.info(
            f"Initialized CustomHttpSensorOperator with endpoint={endpoint}, "
            f"method={method}, http_conn_id={http_conn_id}"
        )
    
    def poke(self, context: Dict) -> bool:
        """
        Check HTTP endpoint and return True if condition is met.
        
        Args:
            context: Airflow task context
            
        Returns:
            True if condition is met, False otherwise
        """
        try:
            # Create HTTP hook
            hook = CustomHTTPHook(
                http_conn_id=self.http_conn_id,
                method=self.method,
                retry_limit=self.retry_limit,
                alert_on_error=self.alert_on_error
            )
            
            logger.info(f"Poking HTTP {self.method} request to {self.endpoint}")
            
            # Execute request
            response = hook.run(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                params=self.params,
                extra_options=self.extra_options
            )
            
            # Process the response
            result = _process_response(
                response=response,
                response_filter=self.response_filter,
                response_check=None  # Don't use response_check here, we'll apply it below
            )
            
            # Apply response_check to determine if poke condition is met
            if self.response_check:
                is_condition_met = self.response_check(result)
                logger.info(
                    f"Poke condition {'met' if is_condition_met else 'not met'} "
                    f"for HTTP request to {self.endpoint}"
                )
                return is_condition_met
            else:
                # If no response_check provided, we consider the condition met if we got a response
                logger.info(f"No response_check provided, considering poke successful "
                           f"for HTTP request to {self.endpoint}")
                return True
                
        except Exception as e:
            # Handle exceptions
            error_msg = f"Error poking HTTP endpoint {self.endpoint}: {str(e)}"
            logger.error(error_msg)
            
            # Send alert if configured
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.WARNING,
                    context={
                        **context,
                        'status': 'HTTP Poke Failed',
                        'endpoint': self.endpoint,
                        'method': self.method,
                        'details': error_msg
                    },
                    exception=e
                )
            
            # Return False on error (poke failed)
            return False