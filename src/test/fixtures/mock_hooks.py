"""
Provides mock implementations of custom Airflow hooks for testing purposes during the migration
from Airflow 1.10.15 to Cloud Composer 2 with Airflow 2.X. This module contains mock classes
and factory functions for GCP, HTTP, and PostgreSQL hooks to enable isolated testing without
actual service dependencies.
"""

import io
import unittest.mock  # Python standard library
from typing import Dict, Union

import pandas  # pandas v1.3.0+
import pytest  # pytest v6.0+
import requests  # requests v2.25.0+

# Internal imports
from ..utils.airflow2_compatibility_utils import is_airflow2
from .mock_connections import (
    create_mock_gcp_connection,
    create_mock_http_connection,
    create_mock_postgres_connection,
    get_mock_connection,
    patch_airflow_connections,
)
from .mock_gcp_services import (
    create_mock_bigquery_client,
    create_mock_secret_manager_client,
    create_mock_storage_client,
    patch_gcp_services,
)
from backend.plugins.hooks.custom_gcp_hook import CustomGCPHook
from backend.plugins.hooks.custom_http_hook import CustomHTTPHook
from backend.plugins.hooks.custom_postgres_hook import CustomPostgresHook

# Global constants
DEFAULT_MOCK_CONN_ID = "mock_default"
MOCK_HOOKS = dict()


def create_mock_custom_gcp_hook(
    conn_id: str, delegate_to: str, mock_responses: Dict
) -> "MockCustomGCPHook":
    """
    Creates a mock CustomGCPHook instance with controlled behavior

    Args:
        conn_id: The connection ID for the hook
        delegate_to: The account to impersonate (optional)
        mock_responses: Dictionary of mock responses for GCP services

    Returns:
        Configured mock GCP hook
    """
    hook = MockCustomGCPHook(conn_id, delegate_to, mock_responses)
    MOCK_HOOKS[conn_id] = hook
    return hook


def create_mock_custom_http_hook(
    conn_id: str, method: str, mock_responses: Dict
) -> "MockCustomHTTPHook":
    """
    Creates a mock CustomHTTPHook instance with controlled behavior

    Args:
        conn_id: The connection ID for the hook
        method: The HTTP method to use
        mock_responses: Dictionary of mock responses for different endpoints

    Returns:
        Configured mock HTTP hook
    """
    hook = MockCustomHTTPHook(conn_id, method, mock_responses)
    MOCK_HOOKS[conn_id] = hook
    return hook


def create_mock_custom_postgres_hook(
    conn_id: str, schema: str, mock_responses: Dict
) -> "MockCustomPostgresHook":
    """
    Creates a mock CustomPostgresHook instance with controlled behavior

    Args:
        conn_id: The connection ID for the hook
        schema: The database schema to use
        mock_responses: Dictionary of mock responses for different queries

    Returns:
        Configured mock Postgres hook
    """
    hook = MockCustomPostgresHook(conn_id, schema, mock_responses)
    MOCK_HOOKS[conn_id] = hook
    return hook


@pytest.fixture
def patch_custom_hooks(mock_responses: Dict):
    """
    Context manager to patch custom hook classes for testing

    Args:
        mock_responses: Dictionary of mock responses for different hooks

    Returns:
        Dictionary of patchers that can be started/stopped
    """
    patchers = {}
    patchers["gcp"] = unittest.mock.patch(
        "backend.plugins.hooks.custom_gcp_hook.CustomGCPHook",
        new=lambda gcp_conn_id, delegate_to: create_mock_custom_gcp_hook(
            gcp_conn_id, delegate_to, mock_responses
        ),
    )
    patchers["http"] = unittest.mock.patch(
        "backend.plugins.hooks.custom_http_hook.CustomHTTPHook",
        new=lambda http_conn_id, method: create_mock_custom_http_hook(
            http_conn_id, method, mock_responses
        ),
    )
    patchers["postgres"] = unittest.mock.patch(
        "backend.plugins.hooks.custom_postgres_hook.CustomPostgresHook",
        new=lambda postgres_conn_id, schema: create_mock_custom_postgres_hook(
            postgres_conn_id, schema, mock_responses
        ),
    )
    return patchers


def get_mock_hook(hook_id: str) -> object:
    """
    Retrieves a mock hook by ID

    Args:
        hook_id: The ID of the hook to retrieve

    Returns:
        Mock hook instance

    Raises:
        Exception: If the hook ID is not found
    """
    if hook_id in MOCK_HOOKS:
        return MOCK_HOOKS[hook_id]
    raise Exception(f"Mock hook with id '{hook_id}' not found")


def reset_mock_hooks() -> None:
    """
    Clears all mock hooks and resets to default state
    """
    MOCK_HOOKS.clear()


def mock_gcs_file_exists(bucket_name: str, object_name: str) -> bool:
    """
    Function for mocking the gcs_file_exists method

    Args:
        bucket_name: The name of the bucket
        object_name: The name of the object

    Returns:
        True if file exists, False otherwise
    """
    # Check if the file is in the mock storage system
    # Return appropriate boolean result
    return True


def mock_bigquery_execute_query(
    sql: str, query_params: Dict = None, as_dataframe: bool = False
) -> Union[list, pandas.DataFrame]:
    """
    Function for mocking the bigquery_execute_query method

    Args:
        sql: The SQL query to execute
        query_params: The query parameters
        as_dataframe: Whether to return the results as a DataFrame

    Returns:
        Mock query results
    """
    # Return predefined results based on SQL query
    # Format as DataFrame if as_dataframe is True
    # Otherwise return list of records
    return []


class MockHookManager:
    """Context manager for managing mock hooks during tests"""

    def __init__(self):
        """Initialize the MockHookManager"""
        self._original_hooks = {}
        self._patches = []
        self._hooks = MOCK_HOOKS.copy()

    def __enter__(self) -> "MockHookManager":
        """
        Set up hook mocking when entering context

        Returns:
            Self reference
        """
        self._original_hooks = MOCK_HOOKS.copy()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Clean up hook mocking when exiting context

        Args:
            exc_type: Exception type if raised
            exc_val: Exception value if raised
            exc_tb: Exception traceback if raised
        """
        global MOCK_HOOKS
        MOCK_HOOKS = self._original_hooks
        self._patches = []
        self._hooks = {}

    def add_hook(self, hook: object, hook_id: str) -> None:
        """
        Add a mock hook during the context

        Args:
            hook: The hook to add
            hook_id: The ID of the hook
        """
        self._hooks[hook_id] = hook
        global MOCK_HOOKS
        MOCK_HOOKS[hook_id] = hook

    def get_hook(self, hook_id: str) -> object:
        """
        Get a mock hook by ID

        Args:
            hook_id: The ID of the hook to retrieve

        Returns:
            Mock hook instance

        Raises:
            Exception: If the hook ID is not found
        """
        if hook_id in self._hooks:
            return self._hooks[hook_id]
        raise Exception(f"Mock hook with id '{hook_id}' not found")


class MockCustomGCPHook:
    """Mock implementation of CustomGCPHook for testing"""

    def __init__(self, gcp_conn_id: str, delegate_to: str, mock_responses: Dict):
        """
        Initialize the MockCustomGCPHook

        Args:
            gcp_conn_id: The connection ID for GCP
            delegate_to: The account to impersonate (optional)
            mock_responses: Dictionary of mock responses for GCP services
        """
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self._mock_responses = mock_responses
        self._storage_client = create_mock_storage_client()
        self._bigquery_client = create_mock_bigquery_client()
        self._secretmanager_client = create_mock_secret_manager_client()

    def get_conn(self) -> object:
        """
        Mock implementation of get_conn method

        Returns:
            Mock connection object
        """
        return get_mock_connection(self.gcp_conn_id)

    def get_storage_client(self) -> object:
        """
        Get the mock storage client

        Returns:
            Mock storage client
        """
        return self._storage_client

    def get_bigquery_client(self) -> object:
        """
        Get the mock BigQuery client

        Returns:
            Mock BigQuery client
        """
        return self._bigquery_client

    def get_secretmanager_client(self) -> object:
        """
        Get the mock Secret Manager client

        Returns:
            Mock Secret Manager client
        """
        return self._secretmanager_client

    def gcs_file_exists(self, bucket_name: str, object_name: str) -> bool:
        """
        Mock implementation of gcs_file_exists

        Args:
            bucket_name: The name of the bucket
            object_name: The name of the object

        Returns:
            Predefined mock result
        """
        if (
            "gcs_file_exists" in self._mock_responses
            and bucket_name in self._mock_responses["gcs_file_exists"]
            and object_name in self._mock_responses["gcs_file_exists"][bucket_name]
        ):
            return self._mock_responses["gcs_file_exists"][bucket_name][object_name]
        return True

    def gcs_upload_file(
        self, local_file_path: str, bucket_name: str, object_name: str, chunk_size: int
    ) -> str:
        """
        Mock implementation of gcs_upload_file

        Args:
            local_file_path: The path to the local file
            bucket_name: The name of the bucket
            object_name: The name of the object
            chunk_size: The chunk size

        Returns:
            Mock GCS URI
        """
        # Simulate file upload to mock storage
        # Return mock GCS URI
        return f"gs://{bucket_name}/{object_name}"

    def gcs_download_file(self, bucket_name: str, object_name: str, local_file_path: str) -> str:
        """
        Mock implementation of gcs_download_file

        Args:
            bucket_name: The name of the bucket
            object_name: The name of the object
            local_file_path: The path to the local file

        Returns:
            Mock local file path
        """
        # Simulate file download from mock storage
        # Return mock local file path
        return local_file_path

    def gcs_list_files(self, bucket_name: str, prefix: str, delimiter: bool) -> list:
        """
        Mock implementation of gcs_list_files

        Args:
            bucket_name: The name of the bucket
            prefix: The prefix to filter by
            delimiter: Whether to use a delimiter

        Returns:
            List of mock files
        """
        if (
            "gcs_list_files" in self._mock_responses
            and bucket_name in self._mock_responses["gcs_list_files"]
            and prefix in self._mock_responses["gcs_list_files"][bucket_name]
        ):
            return self._mock_responses["gcs_list_files"][bucket_name][prefix]
        return []

    def bigquery_execute_query(
        self, sql: str, query_params: Dict, location: str, as_dataframe: bool
    ) -> Union[list, pandas.DataFrame]:
        """
        Mock implementation of bigquery_execute_query

        Args:
            sql: The SQL query to execute
            query_params: The query parameters
            location: The location of the dataset
            as_dataframe: Whether to return the results as a DataFrame

        Returns:
            Mock query result
        """
        if "bigquery_execute_query" in self._mock_responses and sql in self._mock_responses["bigquery_execute_query"]:
            return self._mock_responses["bigquery_execute_query"][sql]
        return []

    def get_secret(self, secret_id: str, version_id: str) -> str:
        """
        Mock implementation of get_secret

        Args:
            secret_id: The ID of the secret
            version_id: The version of the secret

        Returns:
            Mock secret value
        """
        if "get_secret" in self._mock_responses and secret_id in self._mock_responses["get_secret"]:
            return self._mock_responses["get_secret"][secret_id]
        return "mock_secret_value"


class MockCustomHTTPHook:
    """Mock implementation of CustomHTTPHook for testing"""

    def __init__(self, conn_id: str, method: str, mock_responses: Dict):
        """
        Initialize the MockCustomHTTPHook

        Args:
            conn_id: The connection ID for HTTP
            method: The HTTP method to use
            mock_responses: Dictionary of mock responses for different endpoints
        """
        self.http_conn_id = conn_id
        self.method = method
        self._mock_responses = mock_responses
        self._session = requests.Session()

    def get_conn(self) -> object:
        """
        Mock implementation of get_conn method

        Returns:
            Mock session object
        """
        return self._session

    def run(
        self, endpoint: str, data: Dict, headers: Dict, params: Dict, method: str, extra_options: Dict
    ) -> object:
        """
        Mock implementation of run method

        Args:
            endpoint: The endpoint to call
            data: The data to send
            headers: The headers to send
            params: The parameters to send
            method: The HTTP method to use
            extra_options: Extra options to pass to the request

        Returns:
            Mock HTTP response
        """
        # Find matching mock response for endpoint
        # Create and return mock response object
        # Raise exception if configured to do so
        if endpoint in self._mock_responses:
            response_data = self._mock_responses[endpoint]
            if isinstance(response_data, Exception):
                raise response_data
            mock_response = requests.Response()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response._content = str(response_data).encode("utf-8")
            return mock_response
        else:
            mock_response = requests.Response()
            mock_response.status_code = 404
            mock_response.reason = "Not Found"
            mock_response._content = b""
            return mock_response

    def run_and_get_json(
        self, endpoint: str, data: Dict, headers: Dict, params: Dict, method: str
    ) -> Dict:
        """
        Mock implementation of run_and_get_json

        Args:
            endpoint: The endpoint to call
            data: The data to send
            headers: The headers to send
            params: The parameters to send
            method: The HTTP method to use

        Returns:
            Mock JSON response
        """
        # Find matching mock JSON response for endpoint
        # Return mock JSON data
        if endpoint in self._mock_responses:
            return self._mock_responses[endpoint]
        return {}

    def download_file(self, endpoint: str, local_path: str, params: Dict, headers: Dict, create_dirs: bool) -> str:
        """
        Mock implementation of download_file

        Args:
            endpoint: The endpoint to call
            local_path: The path to download the file to
            params: The parameters to send
            headers: The headers to send
            create_dirs: Whether to create directories

        Returns:
            Mock local file path
        """
        # Simulate file download
        # Return mock local file path
        return local_path


class MockCustomPostgresHook:
    """Mock implementation of CustomPostgresHook for testing"""

    def __init__(self, postgres_conn_id: str, schema: str, mock_responses: Dict):
        """
        Initialize the MockCustomPostgresHook

        Args:
            postgres_conn_id: The connection ID for Postgres
            schema: The database schema to use
            mock_responses: Dictionary of mock responses for different queries
        """
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self._mock_responses = mock_responses
        self._conn = None
        self._engine = None

    def get_conn(self) -> object:
        """
        Mock implementation of get_conn method

        Returns:
            Mock database connection
        """
        return self._conn

    def get_sqlalchemy_engine(self) -> object:
        """
        Mock implementation of get_sqlalchemy_engine

        Returns:
            Mock SQLAlchemy engine
        """
        return self._engine

    def execute_query(self, sql: str, parameters: Dict, autocommit: bool, return_dict: bool) -> list:
        """
        Mock implementation of execute_query

        Args:
            sql: The SQL query to execute
            parameters: The query parameters
            autocommit: Whether to autocommit
            return_dict: Whether to return a dictionary

        Returns:
            Mock query results
        """
        # Find matching mock response for SQL query
        # Format result based on return_dict parameter
        # Return mock query result
        if sql in self._mock_responses:
            return self._mock_responses[sql]
        return []

    def query_to_df(self, sql: str, parameters: Dict, columns: List) -> pandas.DataFrame:
        """
        Mock implementation of query_to_df

        Args:
            sql: The SQL query to execute
            parameters: The query parameters
            columns: The columns to return

        Returns:
            Mock DataFrame result
        """
        # Find matching mock DataFrame for SQL query
        # Apply columns if provided
        # Return mock DataFrame
        if sql in self._mock_responses:
            df = self._mock_responses[sql]
            if columns:
                df.columns = columns
            return df
        return pandas.DataFrame()