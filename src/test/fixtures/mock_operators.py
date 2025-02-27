# src/test/fixtures/mock_operators.py
import os  # Python standard library
from datetime import datetime  # Python standard library
from typing import Dict, List, Union, Callable  # Python standard library

import pandas  # pandas v1.3.0+
import pytest  # pytest 6.0+
from unittest.mock import MagicMock, patch  # Python standard library

from airflow.models.baseoperator import BaseOperator  # airflow.models.baseoperator 2.0.0+

# Internal imports
from backend.plugins.hooks.custom_gcp_hook import CustomGCPHook  # src/backend/plugins/hooks/custom_gcp_hook.py
from backend.plugins.hooks.custom_http_hook import CustomHTTPHook  # src/backend/plugins/hooks/custom_http_hook.py
from backend.plugins.hooks.custom_postgres_hook import CustomPostgresHook  # src/backend/plugins/hooks/custom_postgres_hook.py
from test.fixtures.mock_hooks import MockCustomGCPHook, MockCustomHTTPHook, MockCustomPostgresHook  # src/test/fixtures/mock_hooks.py
from test.utils.airflow2_compatibility_utils import is_airflow2  # src/test/utils/airflow2_compatibility_utils.py

# Global constants
DEFAULT_CONTEXT = {
    'ds': '2021-01-01',
    'ds_nodash': '20210101',
    'execution_date': datetime(2021, 1, 1),
    'prev_execution_date': datetime(2020, 12, 31),
    'next_execution_date': datetime(2021, 1, 2),
    'dag': MagicMock(),
    'task': MagicMock(),
    'params': {},
    'ti': MagicMock(),
    'task_instance': MagicMock(),
    'run_id': 'test_run',
}
MOCK_OPERATORS = dict()
DEFAULT_MOCK_PARAMS = {
    'gcp_conn_id': 'mock_gcp_conn',
    'delegate_to': None,
    'http_conn_id': 'mock_http_conn',
    'postgres_conn_id': 'mock_postgres_conn',
    'schema': 'public',
    'bucket_name': 'mock-bucket',
    'object_name': 'mock-object.txt',
    'local_file_path': '/tmp/mock-file.txt',
    'sql': 'SELECT * FROM mock_table',
    'endpoint': '/api/mock',
    'method': 'GET'
}


class MockOperatorManager:
    """Context manager for managing mock operators during tests"""

    def __init__(self):
        """Initialize the MockOperatorManager"""
        self._original_operators = {}
        self._patches = []
        self._operators = MOCK_OPERATORS.copy()

    def __enter__(self) -> "MockOperatorManager":
        """Set up operator mocking when entering context

        Returns:
            MockOperatorManager: Self reference
        """
        self._original_operators = MOCK_OPERATORS.copy()
        # Start patches for all operator classes
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clean up operator mocking when exiting context

        Args:
            exc_type: Exception type if raised
            exc_val: Exception value if raised
            exc_tb: Exception traceback if raised
        """
        global MOCK_OPERATORS
        MOCK_OPERATORS = self._original_operators
        # Stop all patches
        self._patches = []

    def add_operator(self, operator: object, operator_id: str) -> None:
        """Add a mock operator during the context

        Args:
            operator: The operator to add
            operator_id: The ID of the operator
        """
        self._operators[operator_id] = operator
        global MOCK_OPERATORS
        MOCK_OPERATORS[operator_id] = operator

    def get_operator(self, operator_id: str) -> object:
        """Get a mock operator by ID

        Args:
            operator_id: The ID of the operator to retrieve

        Returns:
            Mock operator instance

        Raises:
            Exception: If the operator ID is not found
        """
        if operator_id in self._operators:
            return self._operators[operator_id]
        raise Exception(f"Mock operator with id '{operator_id}' not found")


class MockGCSFileExistsOperator:
    """Mock implementation of GCSFileExistsOperator"""

    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        gcp_conn_id: str,
        delegate_to: str,
        mock_responses: Dict,
        **kwargs
    ):
        """Initialize the MockGCSFileExistsOperator

        Args:
            bucket_name: The name of the bucket
            object_name: The name of the object
            gcp_conn_id: The connection ID for GCP
            delegate_to: The account to impersonate (optional)
            mock_responses: Dictionary of mock responses for GCP services
            kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self) -> MockCustomGCPHook:
        """Get the mock GCP hook

        Returns:
            MockCustomGCPHook: Mock GCP hook
        """
        if self._hook is None:
            self._hook = MockCustomGCPHook(self.gcp_conn_id, self.delegate_to, self._mock_responses)
        return self._hook

    def execute(self, context: Dict) -> bool:
        """Mock implementation of execute method

        Args:
            context: Airflow context dictionary

        Returns:
            bool: Mocked existence result
        """
        hook = self.get_hook()
        return hook.gcs_file_exists(self.bucket_name, self.object_name)


class MockGCSUploadOperator:
    """Mock implementation of GCSUploadOperator"""

    def __init__(
        self,
        local_file_path: str,
        bucket_name: str,
        object_name: str,
        gcp_conn_id: str,
        delegate_to: str,
        chunk_size: int,
        mock_responses: Dict,
        **kwargs
    ):
        """Initialize the MockGCSUploadOperator

        Args:
            local_file_path: The path to the local file
            bucket_name: The name of the bucket
            object_name: The name of the object
            gcp_conn_id: The connection ID for GCP
            delegate_to: The account to impersonate (optional)
            chunk_size: The chunk size
            mock_responses: Dictionary of mock responses for GCP services
            kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.local_file_path = local_file_path
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.chunk_size = chunk_size
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self) -> MockCustomGCPHook:
        """Get the mock GCP hook

        Returns:
            MockCustomGCPHook: Mock GCP hook
        """
        if self._hook is None:
            self._hook = MockCustomGCPHook(self.gcp_conn_id, self.delegate_to, self._mock_responses)
        return self._hook

    def execute(self, context: Dict) -> str:
        """Mock implementation of execute method

        Args:
            context: Airflow context dictionary

        Returns:
            str: Mocked upload result (GCS URI)
        """
        hook = self.get_hook()
        hook.gcs_upload_file(self.local_file_path, self.bucket_name, self.object_name, self.chunk_size)
        return f"gs://{self.bucket_name}/{self.object_name}"


class MockGCSDownloadOperator:
    """Mock implementation of GCSDownloadOperator"""

    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        local_file_path: str,
        gcp_conn_id: str,
        delegate_to: str,
        mock_responses: Dict,
        **kwargs
    ):
        """Initialize the MockGCSDownloadOperator

        Args:
            bucket_name: The name of the bucket
            object_name: The name of the object
            local_file_path: The path to the local file
            gcp_conn_id: The connection ID for GCP
            delegate_to: The account to impersonate (optional)
            mock_responses: Dictionary of mock responses for GCP services
            kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.local_file_path = local_file_path
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self) -> MockCustomGCPHook:
        """Get the mock GCP hook

        Returns:
            MockCustomGCPHook: Mock GCP hook
        """
        if self._hook is None:
            self._hook = MockCustomGCPHook(self.gcp_conn_id, self.delegate_to, self._mock_responses)
        return self._hook

    def execute(self, context: Dict) -> str:
        """Mock implementation of execute method

        Args:
            context: Airflow context dictionary

        Returns:
            str: Mocked download result (local file path)
        """
        hook = self.get_hook()
        hook.gcs_download_file(self.bucket_name, self.object_name, self.local_file_path)
        return self.local_file_path


class MockGCSListFilesOperator:
    """Mock implementation of GCSListFilesOperator"""

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        delimiter: bool,
        gcp_conn_id: str,
        delegate_to: str,
        mock_responses: Dict,
        **kwargs
    ):
        """Initialize the MockGCSListFilesOperator

        Args:
            bucket_name: The name of the bucket
            prefix: The prefix to filter by
            delimiter: Whether to use a delimiter
            gcp_conn_id: The connection ID for GCP
            delegate_to: The account to impersonate (optional)
            mock_responses: Dictionary of mock responses for GCP services
            kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.delimiter = delimiter
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self) -> MockCustomGCPHook:
        """Get the mock GCP hook

        Returns:
            MockCustomGCPHook: Mock GCP hook
        """
        if self._hook is None:
            self._hook = MockCustomGCPHook(self.gcp_conn_id, self.delegate_to, self._mock_responses)
        return self._hook

    def execute(self, context: Dict) -> List[str]:
        """Mock implementation of execute method

        Args:
            context: Airflow context dictionary

        Returns:
            list: Mocked list of file names
        """
        hook = self.get_hook()
        return hook.gcs_list_files(self.bucket_name, self.prefix, self.delimiter)


class MockGCSDeleteFileOperator:
    """Mock implementation of GCSDeleteFileOperator"""

    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        gcp_conn_id: str,
        delegate_to: str,
        mock_responses: Dict,
        **kwargs
    ):
        """Initialize the MockGCSDeleteFileOperator

        Args:
            bucket_name: The name of the bucket
            object_name: The name of the object
            gcp_conn_id: The connection ID for GCP
            delegate_to: The account to impersonate (optional)
            mock_responses: Dictionary of mock responses for GCP services
            kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self) -> MockCustomGCPHook:
        """Get the mock GCP hook

        Returns:
            MockCustomGCPHook: Mock GCP hook
        """
        if self._hook is None:
            self._hook = MockCustomGCPHook(self.gcp_conn_id, self.delegate_to, self._mock_responses)
        return self._hook

    def execute(self, context: Dict) -> bool:
        """Mock implementation of execute method

        Args:
            context: Airflow context dictionary

        Returns:
            bool: Mocked deletion result (True)
        """
        hook = self.get_hook()
        hook.gcs_delete_file(self.bucket_name, self.object_name)
        return True


class MockCustomHttpOperator:
    """Mock implementation of CustomHttpOperator"""

    def __init__(
        self,
        endpoint: str,
        method: str,
        http_conn_id: str,
        data: Dict,
        headers: Dict,
        params: Dict,
        mock_responses: Dict,
        **kwargs
    ):
        """Initialize the MockCustomHttpOperator

        Args:
            endpoint: The endpoint to call
            method: The HTTP method to use
            http_conn_id: The connection ID for HTTP
            data: The data to send
            headers: The headers to send
            params: The parameters to send
            mock_responses: Dictionary of mock responses for different endpoints
            kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.method = method
        self.http_conn_id = http_conn_id
        self.data = data
        self.headers = headers
        self.params = params
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self) -> MockCustomHTTPHook:
        """Get the mock HTTP hook

        Returns:
            MockCustomHTTPHook: Mock HTTP hook
        """
        if self._hook is None:
            self._hook = MockCustomHTTPHook(self.http_conn_id, self.method, self._mock_responses)
        return self._hook

    def execute(self, context: Dict) -> Dict:
        """Mock implementation of execute method

        Args:
            context: Airflow context dictionary

        Returns:
            dict: Mocked HTTP response
        """
        hook = self.get_hook()
        return hook.run(self.endpoint, self.data, self.headers, self.params, self.method)

    def download_file(self, local_path: str, create_dirs: bool, context: Dict) -> str:
        """Mock implementation of download_file method

        Args:
            local_path: The path to download the file to
            create_dirs: Whether to create directories
            context: Airflow context dictionary

        Returns:
            str: Mocked local file path
        """
        hook = self.get_hook()
        return hook.download_file(self.endpoint, local_path, self.params, self.headers, create_dirs)

    def upload_file(self, file_path: str, field_name: str, context: Dict) -> Dict:
        """Mock implementation of upload_file method

        Args:
            file_path: The path to upload
            field_name: The name of the field
            context: Airflow context dictionary

        Returns:
            dict: Mocked upload response
        """
        hook = self.get_hook()
        return hook.upload_file(self.endpoint, file_path, field_name, self.data, self.headers, self.params)


class MockCustomHttpSensorOperator:
    """Mock implementation of CustomHttpSensorOperator"""

    def __init__(
        self,
        endpoint: str,
        http_conn_id: str,
        method: str,
        data: Dict,
        headers: Dict,
        params: Dict,
        response_check: Callable,
        mock_responses: Dict,
        **kwargs
    ):
        """Initialize the MockCustomHttpSensorOperator

        Args:
            endpoint: The endpoint to call
            http_conn_id: The connection ID for HTTP
            method: The HTTP method to use
            data: The data to send
            headers: The headers to send
            params: The parameters to send
            response_check: The function to check the response
            mock_responses: Dictionary of mock responses for different endpoints
            kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.method = method
        self.data = data
        self.headers = headers
        self.params = params
        self.response_check = response_check
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self) -> MockCustomHTTPHook:
        """Get the mock HTTP hook

        Returns:
            MockCustomHTTPHook: Mock HTTP hook
        """
        if self._hook is None:
            self._hook = MockCustomHTTPHook(self.http_conn_id, self.method, self._mock_responses)
        return self._hook

    def poke(self, context: Dict) -> bool:
        """Mock implementation of poke method

        Args:
            context: Airflow context dictionary

        Returns:
            bool: Mocked poke result (True/False)
        """
        hook = self.get_hook()
        response = hook.run(self.endpoint, self.data, self.headers, self.params, self.method)
        return self.response_check(response)


class MockCustomPostgresOperator:
    """Mock implementation of CustomPostgresOperator"""

    def __init__(
        self,
        sql: str,
        postgres_conn_id: str,
        schema: str,
        parameters: Dict,
        autocommit: bool,
        use_transaction: bool,
        mock_responses: Dict,
        **kwargs
    ):
        """Initialize the MockCustomPostgresOperator

        Args:
            sql: The SQL query to execute
            postgres_conn_id: The connection ID for Postgres
            schema: The database schema to use
            parameters: The query parameters
            autocommit: Whether to autocommit
            use_transaction: Whether to use a transaction
            mock_responses: Dictionary of mock responses for different queries
            kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.parameters = parameters
        self.autocommit = autocommit
        self.use_transaction = use_transaction
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self) -> MockCustomPostgresHook:
        """Get the mock Postgres hook

        Returns:
            MockCustomPostgresHook: Mock Postgres hook
        """
        if self._hook is None:
            self._hook = MockCustomPostgresHook(self.postgres_conn_id, self.schema, self._mock_responses)
        return self._hook

    def execute(self, context: Dict) -> List:
        """Mock implementation of execute method

        Args:
            context: Airflow context dictionary

        Returns:
            list: Mocked query results
        """
        hook = self.get_hook()
        return hook.execute_query(self.sql, self.parameters, self.autocommit)


def create_mock_gcs_operator(
    bucket_name: str,
    object_name: str,
    gcp_conn_id: str,
    delegate_to: str,
    mock_responses: Dict
) -> MockGCSFileExistsOperator:
    """Creates a mock GCSFileExistsOperator with controlled behavior

    Args:
        bucket_name: The name of the bucket
        object_name: The name of the object
        gcp_conn_id: The connection ID for GCP
        delegate_to: The account to impersonate (optional)
        mock_responses: Dictionary of mock responses for GCP services

    Returns:
        MockGCSFileExistsOperator: Configured mock GCS operator
    """
    operator = MockGCSFileExistsOperator(
        task_id="mock_gcs_file_exists",
        bucket_name=bucket_name,
        object_name=object_name,
        gcp_conn_id=gcp_conn_id,
        delegate_to=delegate_to,
        mock_responses=mock_responses
    )
    global MOCK_OPERATORS
    MOCK_OPERATORS[operator.task_id] = operator
    return operator


def create_mock_gcs_upload_operator(
    local_file_path: str,
    bucket_name: str,
    object_name: str,
    gcp_conn_id: str,
    delegate_to: str,
    mock_responses: Dict
) -> MockGCSUploadOperator:
    """Creates a mock GCSUploadOperator with controlled behavior

    Args:
        local_file_path: The path to the local file
        bucket_name: The name of the bucket
        object_name: The name of the object
        gcp_conn_id: The connection ID for GCP
        delegate_to: The account to impersonate (optional)
        mock_responses: Dictionary of mock responses for GCP services

    Returns:
        MockGCSUploadOperator: Configured mock GCS upload operator
    """
    operator = MockGCSUploadOperator(
        task_id="mock_gcs_upload",
        local_file_path=local_file_path,
        bucket_name=bucket_name,
        object_name=object_name,
        gcp_conn_id=gcp_conn_id,
        delegate_to=delegate_to,
        chunk_size=1024,
        mock_responses=mock_responses
    )
    global MOCK_OPERATORS
    MOCK_OPERATORS[operator.task_id] = operator
    return operator


def create_mock_gcs_download_operator(
    bucket_name: str,
    object_name: str,
    local_file_path: str,
    gcp_conn_id: str,
    delegate_to: str,
    mock_responses: Dict
) -> MockGCSDownloadOperator:
    """Creates a mock GCSDownloadOperator with controlled behavior

    Args:
        bucket_name: The name of the bucket
        object_name: The name of the object
        local_file_path: The path to the local file
        gcp_conn_id: The connection ID for GCP
        delegate_to: The account to impersonate (optional)
        mock_responses: Dictionary of mock responses for GCP services

    Returns:
        MockGCSDownloadOperator: Configured mock GCS download operator
    """
    operator = MockGCSDownloadOperator(
        task_id="mock_gcs_download",
        bucket_name=bucket_name,
        object_name=object_name,
        local_file_path=local_file_path,
        gcp_conn_id=gcp_conn_id,
        delegate_to=delegate_to,
        mock_responses=mock_responses
    )
    global MOCK_OPERATORS
    MOCK_OPERATORS[operator.task_id] = operator
    return operator


def create_mock_http_operator(
    endpoint: str,
    method: str,
    http_conn_id: str,
    data: Dict,
    headers: Dict,
    mock_responses: Dict
) -> MockCustomHttpOperator:
    """Creates a mock HTTP operator with controlled behavior

    Args:
        endpoint: The endpoint to call
        method: The HTTP method to use
        http_conn_id: The connection ID for HTTP
        data: The data to send
        headers: The headers to send
        mock_responses: Dictionary of mock responses for different endpoints

    Returns:
        MockCustomHttpOperator: Configured mock HTTP operator
    """
    operator = MockCustomHttpOperator(
        task_id="mock_http",
        endpoint=endpoint,
        method=method,
        http_conn_id=http_conn_id,
        data=data,
        headers=headers,
        mock_responses=mock_responses
    )
    global MOCK_OPERATORS
    MOCK_OPERATORS[operator.task_id] = operator
    return operator


def create_mock_postgres_operator(
    sql: str,
    postgres_conn_id: str,
    schema: str,
    parameters: Dict,
    autocommit: bool,
    mock_responses: Dict
) -> MockCustomPostgresOperator:
    """Creates a mock PostgreSQL operator with controlled behavior

    Args:
        sql: The SQL query to execute
        postgres_conn_id: The connection ID for Postgres
        schema: The database schema to use
        parameters: The query parameters
        autocommit: Whether to autocommit
        mock_responses: Dictionary of mock responses for different queries

    Returns:
        MockCustomPostgresOperator: Configured mock PostgreSQL operator
    """
    operator = MockCustomPostgresOperator(
        task_id="mock_postgres",
        sql=sql,
        postgres_conn_id=postgres_conn_id,
        schema=schema,
        parameters=parameters,
        autocommit=autocommit,
        use_transaction=False,
        mock_responses=mock_responses
    )
    global MOCK_OPERATORS
    MOCK_OPERATORS[operator.task_id] = operator
    return operator


def patch_operators(mock_responses: Dict) -> Dict:
    """Context manager to patch operator classes for testing

    Args:
        mock_responses: Dictionary of mock responses for different operators

    Returns:
        dict: Dictionary of patchers
    """
    patchers = {
        "gcs": patch(
            "backend.plugins.operators.custom_gcp_operator.GCSFileExistsOperator",
            new=lambda task_id, bucket_name, object_name, gcp_conn_id, delegate_to, **kwargs: create_mock_gcs_operator(
                bucket_name, object_name, gcp_conn_id, delegate_to, mock_responses
            ),
        ),
        "http": patch(
            "backend.plugins.operators.custom_http_operator.CustomHttpOperator",
            new=lambda task_id, endpoint, method, http_conn_id, data, headers, **kwargs: create_mock_http_operator(
                endpoint, method, http_conn_id, data, headers, mock_responses
            ),
        ),
        "postgres": patch(
            "backend.plugins.operators.custom_postgres_operator.CustomPostgresOperator",
            new=lambda task_id, sql, postgres_conn_id, schema, parameters, autocommit, **kwargs: create_mock_postgres_operator(
                sql, postgres_conn_id, schema, parameters, autocommit, mock_responses
            ),
        ),
    }
    return patchers


def get_mock_operator(operator_id: str) -> object:
    """Retrieves a mock operator by ID

    Args:
        operator_id: The ID of the operator to retrieve

    Returns:
        Mock operator instance

    Raises:
        Exception: If the operator ID is not found
    """
    if operator_id in MOCK_OPERATORS:
        return MOCK_OPERATORS[operator_id]
    raise Exception(f"Mock operator with id '{operator_id}' not found")


def reset_mock_operators() -> None:
    """Clears all mock operators and resets to default state"""
    global MOCK_OPERATORS
    MOCK_OPERATORS = dict()


def create_mock_context(execution_date: datetime = None, custom_params: Dict = None) -> Dict:
    """Creates a mock Airflow task context dictionary for testing

    Args:
        execution_date: The execution date for the context
        custom_params: Custom parameters to add to the context

    Returns:
        dict: Airflow task context dictionary
    """
    context = DEFAULT_CONTEXT.copy()
    if execution_date:
        context['execution_date'] = execution_date
        context['ds'] = execution_date.strftime('%Y-%m-%d')
        context['ds_nodash'] = execution_date.strftime('%Y%m%d')
    if custom_params:
        context.update(custom_params)
    return context