"""
Custom HTTP Hook for Apache Airflow 2.X.

This module provides an enhanced HTTP hook that extends the standard
Airflow HTTP hook with additional functionality optimized for API integrations.
It includes robust error handling, advanced retry capabilities, OAuth support,
and simplified methods for working with HTTP responses in different formats.

This hook is designed to maintain compatibility with existing Airflow 1.10.15 code
while leveraging Airflow 2.X provider packages.
"""

import os
import logging
import json
from typing import Dict, List, Optional, Any, Union, Callable

import requests
from requests import Response
import tenacity
from tenacity import retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

# Internal imports
from ...dags.utils.validation_utils import validate_connection
from ...dags.utils.alert_utils import send_alert, AlertLevel

# Set up logging
logger = logging.getLogger('airflow.hooks.custom_http_hook')

# Default constants
DEFAULT_HTTP_CONN_ID = 'http_default'
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0  # seconds
DEFAULT_RETRY_BACKOFF = 2.0  # exponential backoff multiplier
DEFAULT_RETRY_STATUS_CODES = [500, 502, 503, 504]


def _check_response(response: Response, alert_on_error: bool = True) -> Response:
    """
    Check if response has a success status code, raise exception with error details otherwise.
    
    Args:
        response: HTTP response object to check
        alert_on_error: Whether to send alert on error status code
        
    Returns:
        The original response if status code indicates success
        
    Raises:
        AirflowException: If the response has an error status code
    """
    if not response.ok:
        error_msg = f"HTTP error: {response.status_code} - {response.reason}"
        
        # Add response body details if available
        try:
            response_json = response.json()
            if isinstance(response_json, dict):
                error_details = response_json.get('error', response_json)
                error_msg += f", details: {error_details}"
        except ValueError:
            # Not JSON, use text content if available
            if response.text:
                error_msg += f", content: {response.text[:200]}..."
                
        logger.error(error_msg)
        
        # Send alert if configured
        if alert_on_error:
            send_alert(
                alert_level=AlertLevel.ERROR,
                context={
                    'status': f"HTTP Error {response.status_code}",
                    'url': response.url,
                    'method': response.request.method if response.request else 'UNKNOWN',
                    'details': error_msg
                }
            )
            
        raise AirflowException(error_msg)
        
    return response


def _retry_if_request_error(exception: Exception, retry_status_codes: List[int]) -> bool:
    """
    Determine if a request should be retried based on the exception.
    
    Args:
        exception: The exception that caused the request to fail
        retry_status_codes: List of HTTP status codes that should trigger a retry
        
    Returns:
        True if the request should be retried, False otherwise
    """
    # Always retry on connection-related errors
    if isinstance(exception, (
            requests.ConnectionError,
            requests.Timeout,
            requests.TooManyRedirects
    )):
        logger.info(f"Retrying on connection error: {type(exception).__name__}")
        return True
    
    # Retry on specific HTTP status codes
    if isinstance(exception, requests.HTTPError):
        response = getattr(exception, 'response', None)
        if response and hasattr(response, 'status_code'):
            should_retry = response.status_code in retry_status_codes
            logger.info(f"Status code {response.status_code}: {'Will retry' if should_retry else 'Will not retry'}")
            return should_retry
    
    # Check for RequestException with response attribute
    if isinstance(exception, requests.RequestException):
        response = getattr(exception, 'response', None)
        if response and hasattr(response, 'status_code'):
            should_retry = response.status_code in retry_status_codes
            logger.info(f"Status code {response.status_code}: {'Will retry' if should_retry else 'Will not retry'}")
            return should_retry
    
    logger.info(f"Will not retry for exception type: {type(exception).__name__}")
    return False


class CustomHTTPHook(HttpHook):
    """
    An enhanced HTTP hook for Airflow 2.X with additional capabilities for API integrations.
    
    This hook extends the standard Airflow HTTP hook with:
    - Advanced retry capabilities using tenacity
    - Improved error handling and alerting
    - OAuth support
    - Simplified methods for working with HTTP responses (JSON, text)
    - File upload and download capabilities
    - Connection testing capabilities
    
    Attributes:
        http_conn_id: The Airflow connection ID to use
        method: The HTTP method to use (GET, POST, etc.)
        retry_limit: Maximum number of retry attempts 
        retry_delay: Initial delay between retries in seconds
        retry_backoff: Multiplier for exponential backoff
        retry_status_codes: HTTP status codes that should trigger a retry
        alert_on_error: Whether to send alerts on HTTP errors
    """
    
    def __init__(
        self,
        http_conn_id: str = DEFAULT_HTTP_CONN_ID,
        method: str = 'GET',
        retry_limit: int = DEFAULT_RETRY_ATTEMPTS,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
        retry_status_codes: List[int] = None,
        alert_on_error: bool = True,
    ):
        """
        Initialize the CustomHTTPHook.
        
        Args:
            http_conn_id: The Airflow connection ID to use
            method: The HTTP method to use (GET, POST, etc.)
            retry_limit: Maximum number of retry attempts
            retry_delay: Initial delay between retries in seconds
            retry_backoff: Multiplier for exponential backoff
            retry_status_codes: HTTP status codes that should trigger a retry
            alert_on_error: Whether to send alerts on HTTP errors
        """
        super().__init__(http_conn_id=http_conn_id, method=method)
        
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.retry_status_codes = retry_status_codes or DEFAULT_RETRY_STATUS_CODES
        self.alert_on_error = alert_on_error
        
        # Initialize session if needed
        self.session = None
        
        logger.debug(
            f"Initialized CustomHTTPHook - conn_id: {http_conn_id}, method: {method}, "
            f"retry_limit: {retry_limit}, retry_delay: {retry_delay}, "
            f"retry_backoff: {retry_backoff}, alert_on_error: {alert_on_error}"
        )
        
    def get_conn(self) -> requests.Session:
        """
        Get connection with proper authentication and headers.
        
        Gets connection from the base HttpHook and adds OAuth header if configured.
        Sets default headers for content-type and adds timeout based on connection config.
        
        Returns:
            Configured requests session object
        """
        session = super().get_conn()
        
        # Check if connection has OAuth configuration
        conn = self.get_connection(self.http_conn_id)
        extras = conn.extra_dejson or {}
        
        # Add OAuth2 token if provided in the connection
        if extras.get('oauth_token'):
            oauth_token = extras.get('oauth_token')
            token_type = extras.get('token_type', 'Bearer')
            session.headers.update({
                'Authorization': f"{token_type} {oauth_token}"
            })
            logger.debug(f"Added OAuth authorization header with token type {token_type}")
        
        # Set default headers if not already set
        if 'Content-Type' not in session.headers:
            session.headers.update({
                'Content-Type': 'application/json'
            })
            
        # Set timeout from connection configuration or default
        timeout = extras.get('timeout')
        if timeout:
            try:
                timeout_value = float(timeout)
                session.request_kwargs = {
                    'timeout': timeout_value
                }
                logger.debug(f"Set request timeout to {timeout_value}s")
            except (ValueError, TypeError):
                logger.warning(f"Invalid timeout value in connection: {timeout}")
        
        return session
    
    def run(
        self,
        endpoint: str = None,
        data: Dict = None,
        headers: Dict = None,
        params: Dict = None,
        method: str = None,
        extra_options: Dict = None,
    ) -> Response:
        """
        Execute HTTP request with basic retry functionality.
        
        This method extends the base HttpHook.run method with error checking
        and uses the connection with all configured authentication.
        
        Args:
            endpoint: The URL endpoint to call
            data: Payload to send (dict for JSON, string for raw data)
            headers: Additional HTTP headers
            params: Query parameters
            method: HTTP method to override instance method
            extra_options: Additional options for requests library
            
        Returns:
            HTTP response object from the requests library
            
        Raises:
            AirflowException: If the request fails
        """
        try:
            # Use method parameter or instance method
            self.method = method or self.method
            
            # Get session with authentication
            session = self.get_conn()
            
            # Execute the request
            response = super().run(
                endpoint=endpoint,
                data=data,
                headers=headers,
                params=params,
                extra_options=extra_options
            )
            
            # Check response for errors
            _check_response(response, self.alert_on_error)
            
            logger.info(f"HTTP {self.method} request to {endpoint} successful: {response.status_code}")
            return response
            
        except AirflowException:
            # Re-raise AirflowExceptions (likely from _check_response)
            raise
        except Exception as e:
            error_msg = f"HTTP request failed: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context={
                        'status': "HTTP Request Failed",
                        'endpoint': endpoint,
                        'method': method or self.method,
                        'details': error_msg
                    },
                    exception=e
                )
                
            raise AirflowException(error_msg) from e
    
    def run_with_advanced_retry(
        self,
        endpoint: str = None,
        data: Dict = None,
        headers: Dict = None,
        params: Dict = None,
        method: str = None,
        extra_options: Dict = None,
        retry_limit: int = None,
        retry_delay: float = None,
        retry_backoff: float = None,
        retry_status_codes: List[int] = None,
    ) -> Response:
        """
        Execute HTTP request with advanced retry capabilities using tenacity.
        
        This method provides more sophisticated retry capabilities than the
        basic run method, using exponential backoff and configurable retry conditions.
        
        Args:
            endpoint: The URL endpoint to call
            data: Payload to send (dict for JSON, string for raw data)
            headers: Additional HTTP headers
            params: Query parameters
            method: HTTP method to override instance method
            extra_options: Additional options for requests library
            retry_limit: Maximum number of retry attempts
            retry_delay: Initial delay between retries in seconds
            retry_backoff: Multiplier for exponential backoff
            retry_status_codes: HTTP status codes that should trigger a retry
            
        Returns:
            HTTP response object from the requests library
            
        Raises:
            AirflowException: If the request fails after all retries
        """
        # Use method parameter or instance method
        self.method = method or self.method
        
        # Use provided retry parameters or instance defaults
        retry_limit = retry_limit if retry_limit is not None else self.retry_limit
        retry_delay = retry_delay if retry_delay is not None else self.retry_delay
        retry_backoff = retry_backoff if retry_backoff is not None else self.retry_backoff
        retry_status_codes = retry_status_codes or self.retry_status_codes
        
        # Create retry decorator with tenacity
        retry_decorator = tenacity.retry(
            retry=tenacity.retry_if_exception(
                lambda exc: _retry_if_request_error(exc, retry_status_codes)
            ),
            stop=tenacity.stop_after_attempt(retry_limit),
            wait=tenacity.wait_exponential(
                multiplier=retry_delay,
                exp_base=retry_backoff,
                min=retry_delay
            ),
            reraise=True,
            before_sleep=tenacity.before_sleep_log(logger, logging.INFO)
        )
        
        # Define inner function for HTTP request execution
        @retry_decorator
        def _execute_with_retry():
            try:
                return self.run(
                    endpoint=endpoint,
                    data=data,
                    headers=headers,
                    params=params,
                    method=self.method,
                    extra_options=extra_options,
                )
            except AirflowException as e:
                # Convert AirflowException to HTTPError for retry evaluation
                if "HTTP error:" in str(e) and "status code" in str(e).lower():
                    status_code = int(str(e).split("HTTP error:")[1].split("-")[0].strip())
                    if status_code in retry_status_codes:
                        # Create a RequestException that will be evaluated by retry logic
                        http_error = requests.HTTPError(str(e))
                        http_error.response = type('obj', (object,), {
                            'status_code': status_code,
                            'url': endpoint
                        })
                        raise http_error from e
                # Re-raise original exception if we can't convert it
                raise
        
        try:
            # Execute with retry
            response = _execute_with_retry()
            
            # Check response for errors (final check after all retries)
            _check_response(response, self.alert_on_error)
            
            logger.info(
                f"HTTP {self.method} request to {endpoint} completed successfully "
                f"with retry settings (limit={retry_limit}, delay={retry_delay}, "
                f"backoff={retry_backoff})"
            )
            
            return response
            
        except Exception as e:
            error_msg = f"HTTP request failed after {retry_limit} retries: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context={
                        'status': "HTTP Request Failed After Retries",
                        'endpoint': endpoint,
                        'method': self.method,
                        'retry_limit': retry_limit,
                        'details': error_msg
                    },
                    exception=e
                )
                
            raise AirflowException(error_msg) from e
    
    def run_and_get_json(
        self,
        endpoint: str = None,
        data: Dict = None,
        headers: Dict = None,
        params: Dict = None,
        method: str = None,
    ) -> Dict:
        """
        Execute HTTP request and return the response as JSON.
        
        Args:
            endpoint: The URL endpoint to call
            data: Payload to send (dict for JSON, string for raw data)
            headers: Additional HTTP headers
            params: Query parameters
            method: HTTP method to override instance method
            
        Returns:
            JSON response parsed as a dictionary
            
        Raises:
            AirflowException: If the request fails or response is not valid JSON
        """
        try:
            response = self.run_with_advanced_retry(
                endpoint=endpoint,
                data=data,
                headers=headers,
                params=params,
                method=method,
            )
            
            try:
                json_response = response.json()
                logger.debug(f"Successfully parsed JSON response from {endpoint}")
                return json_response
            except ValueError as e:
                error_msg = f"Failed to parse JSON response: {str(e)}"
                logger.error(error_msg)
                if self.alert_on_error:
                    send_alert(
                        alert_level=AlertLevel.ERROR,
                        context={
                            'status': "JSON Parsing Error",
                            'endpoint': endpoint,
                            'method': method or self.method,
                            'details': error_msg,
                            'response_content': response.text[:200] + "..." if len(response.text) > 200 else response.text
                        }
                    )
                raise AirflowException(error_msg) from e
                
        except Exception as e:
            # Enhanced error handling for specific status codes
            if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                if e.response.status_code == 401:
                    error_msg = f"Authentication error (401) from API: {endpoint}"
                elif e.response.status_code == 403:
                    error_msg = f"Permission denied (403) from API: {endpoint}"
                elif e.response.status_code == 404:
                    error_msg = f"Resource not found (404) from API: {endpoint}"
                else:
                    error_msg = f"Error calling API: {str(e)}"
            else:
                error_msg = f"Error calling API: {str(e)}"
                
            logger.error(error_msg)
            raise AirflowException(error_msg) from e
    
    def run_and_get_text(
        self,
        endpoint: str = None,
        data: Dict = None,
        headers: Dict = None,
        params: Dict = None,
        method: str = None,
    ) -> str:
        """
        Execute HTTP request and return the response as text.
        
        Args:
            endpoint: The URL endpoint to call
            data: Payload to send (dict for JSON, string for raw data)
            headers: Additional HTTP headers
            params: Query parameters
            method: HTTP method to override instance method
            
        Returns:
            Text response content
            
        Raises:
            AirflowException: If the request fails
        """
        response = self.run_with_advanced_retry(
            endpoint=endpoint,
            data=data,
            headers=headers,
            params=params,
            method=method,
        )
        
        logger.debug(f"Successfully got text response from {endpoint}")
        return response.text
    
    def download_file(
        self,
        endpoint: str,
        local_path: str,
        params: Dict = None,
        headers: Dict = None,
        create_dirs: bool = True,
    ) -> str:
        """
        Download a file from a URL to a local path.
        
        Args:
            endpoint: The URL endpoint where the file is located
            local_path: Local file path to save the downloaded file
            params: Query parameters for the request
            headers: Additional HTTP headers
            create_dirs: Whether to create parent directories if they don't exist
            
        Returns:
            Path to the downloaded file
            
        Raises:
            AirflowException: If the download fails
        """
        # Create directory structure if needed
        if create_dirs:
            directory = os.path.dirname(local_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                logger.info(f"Created directory: {directory}")
                
        try:
            # Set method to GET
            self.method = 'GET'
            
            # Add headers for binary content if needed
            request_headers = headers or {}
            
            # Execute request with streaming
            logger.info(f"Downloading file from {endpoint} to {local_path}")
            
            # Use session to make the request with streaming
            session = self.get_conn()
            url = self.base_url + endpoint if self.base_url else endpoint
            
            # Stream the response to a file
            with session.get(
                url,
                params=params,
                headers=request_headers,
                stream=True,
            ) as response:
                # Check response
                _check_response(response, self.alert_on_error)
                
                # Save file
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            # Verify file exists and has content
            file_size = os.path.getsize(local_path)
            logger.info(f"Successfully downloaded file to {local_path} ({file_size} bytes)")
            
            return local_path
            
        except Exception as e:
            error_msg = f"Failed to download file from {endpoint}: {str(e)}"
            logger.error(error_msg)
            
            # Remove partial file if it exists
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                    logger.info(f"Removed partial download: {local_path}")
                except Exception as remove_err:
                    logger.warning(f"Failed to remove partial download: {str(remove_err)}")
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context={
                        'status': "File Download Failed",
                        'endpoint': endpoint,
                        'local_path': local_path,
                        'details': error_msg
                    },
                    exception=e
                )
                
            raise AirflowException(error_msg) from e
    
    def upload_file(
        self,
        endpoint: str,
        file_path: str,
        field_name: str = 'file',
        data: Dict = None,
        headers: Dict = None,
        params: Dict = None,
    ) -> Response:
        """
        Upload a file to a URL.
        
        Args:
            endpoint: The URL endpoint to upload to
            file_path: Path to the file to upload
            field_name: Name of the form field for the file
            data: Additional form data to include
            headers: Additional HTTP headers
            params: Query parameters
            
        Returns:
            HTTP response object
            
        Raises:
            AirflowException: If the upload fails or the file does not exist
        """
        # Check if file exists
        if not os.path.exists(file_path):
            error_msg = f"File does not exist: {file_path}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
            
        try:
            # Set method to POST
            self.method = 'POST'
            
            # Prepare form data if provided
            form_data = data or {}
            
            logger.info(f"Uploading file {file_path} to {endpoint}")
            
            # Open file for reading in binary mode
            with open(file_path, 'rb') as file_obj:
                # Prepare file upload
                files = {
                    field_name: (
                        os.path.basename(file_path),
                        file_obj,
                        'application/octet-stream'
                    )
                }
                
                # Make request with file payload
                response = self.run_with_advanced_retry(
                    endpoint=endpoint,
                    data=form_data,
                    headers=headers,
                    params=params,
                    extra_options={'files': files}
                )
            
            file_size = os.path.getsize(file_path)
            logger.info(
                f"Successfully uploaded file {file_path} ({file_size} bytes) "
                f"to {endpoint}, status: {response.status_code}"
            )
            
            return response
            
        except Exception as e:
            error_msg = f"Failed to upload file {file_path} to {endpoint}: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context={
                        'status': "File Upload Failed",
                        'endpoint': endpoint,
                        'file_path': file_path,
                        'details': error_msg
                    },
                    exception=e
                )
                
            raise AirflowException(error_msg) from e
    
    def test_connection(self) -> bool:
        """
        Test connectivity to the HTTP endpoint.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            conn = self.get_connection(self.http_conn_id)
            
            # Get connection details
            schema = conn.schema or 'http'
            host = conn.host or ''
            port = conn.port or (443 if schema == 'https' else 80)
            
            # Use conn.extra to get test_endpoint if specified
            extras = conn.extra_dejson or {}
            test_endpoint = extras.get('test_endpoint', '/')
            
            # Determine base URL
            if host:
                if schema and not host.startswith(f"{schema}://"):
                    base_url = f"{schema}://{host}"
                else:
                    base_url = host
                    
                # Add port if not default
                if port and port not in (80, 443):
                    # Check if host already has port
                    if not (host.endswith(f":{port}") or ':' in host.split('/')[-1]):
                        base_url = f"{base_url}:{port}"
            else:
                logger.error("No host specified in connection")
                return False
                
            # Use the session with short timeout
            session = self.get_conn()
            
            # Make a HEAD request to the test endpoint
            logger.info(f"Testing connection to {base_url}{test_endpoint}")
            
            url = f"{base_url.rstrip('/')}/{test_endpoint.lstrip('/')}"
            response = session.head(
                url,
                timeout=5.0,  # Short timeout for connection test
                allow_redirects=True
            )
            
            # Check if response is successful
            if response.ok:
                logger.info(f"Connection test successful: {response.status_code}")
                return True
            else:
                logger.warning(
                    f"Connection test failed with status code: {response.status_code}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False