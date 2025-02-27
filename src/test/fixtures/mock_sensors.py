"""
Provides mock implementations of custom Apache Airflow sensors for testing the migration from Airflow 1.10.15 to Airflow 2.X.
This module contains mock sensor classes and utility functions to simplify unit testing of DAGs without requiring actual connection to external services like GCP, HTTP endpoints, or PostgreSQL databases.
"""

import os  # Python standard library
import pytest  # pytest v6.0+
from datetime import datetime  # Python standard library
from airflow.sensors.base import BaseSensorOperator  # airflow v2.0.0+
from unittest.mock import MagicMock, patch  # Python standard library
from airflow.exceptions import AirflowException  # airflow v2.0.0+

# Internal imports
from src.backend.plugins.sensors.custom_gcp_sensor import CustomGCSFileSensor, CustomBigQueryTableSensor, CustomBigQueryJobSensor, CustomGCSObjectsWithPrefixExistenceSensor  # Reference for mocking the GCS file sensor
from src.backend.plugins.sensors.custom_http_sensor import CustomHttpSensor, CustomHttpJsonSensor, CustomHttpStatusSensor  # Reference for mocking the HTTP sensor
from src.backend.plugins.sensors.custom_postgres_sensor import CustomPostgresSensor, CustomPostgresTableExistenceSensor, CustomPostgresRowCountSensor, CustomPostgresValueCheckSensor  # Reference for mocking the PostgreSQL sensor
from src.test.fixtures.mock_hooks import MockCustomGCPHook, MockCustomHTTPHook, MockCustomPostgresHook, create_mock_custom_gcp_hook, create_mock_custom_http_hook, create_mock_custom_postgres_hook  # Use mock GCP hook for sensor testing
from src.test.utils.airflow2_compatibility_utils import is_airflow2  # Check if running in Airflow 2.X environment

# Global variables
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

MOCK_SENSORS = dict()

DEFAULT_MOCK_PARAMS = {
    'gcp_conn_id': 'mock_gcp_conn',
    'http_conn_id': 'mock_http_conn',
    'postgres_conn_id': 'mock_postgres_conn',
    'bucket_name': 'mock-bucket',
    'object_name': 'mock-object.txt',
    'prefix': 'mock-prefix/',
    'project_id': 'mock-project',
    'dataset_id': 'mock_dataset',
    'table_id': 'mock_table',
    'job_id': 'mock_job_id',
    'location': 'US',
    'endpoint': '/api/mock',
    'method': 'GET',
    'schema': 'public',
    'table_name': 'mock_table',
    'min_rows': 1,
    'sql': 'SELECT * FROM mock_table',
    'expected_value': 'expected_value'
}


class MockSensorManager:
    """Context manager for managing mock sensors during tests"""

    def __init__(self):
        """Initialize the MockSensorManager"""
        self._original_sensors = {}
        self._patches = []
        self._sensors = MOCK_SENSORS.copy()

    def __enter__(self):
        """Set up sensor mocking when entering context"""
        self._original_sensors = MOCK_SENSORS.copy()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up sensor mocking when exiting context"""
        global MOCK_SENSORS
        MOCK_SENSORS = self._original_sensors
        self._patches = []

    def add_sensor(self, sensor, sensor_id):
        """Add a mock sensor during the context"""
        self._sensors[sensor_id] = sensor
        global MOCK_SENSORS
        MOCK_SENSORS[sensor_id] = sensor

    def get_sensor(self, sensor_id):
        """Get a mock sensor by ID"""
        if sensor_id in self._sensors:
            return self._sensors[sensor_id]
        raise Exception(f"Mock sensor with id '{sensor_id}' not found")


class MockGCSFileSensor(BaseSensorOperator):
    """Mock implementation of CustomGCSFileSensor"""

    def __init__(self, bucket_name, object_name, gcp_conn_id, mock_responses, **kwargs):
        """Initialize the MockGCSFileSensor"""
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self):
        """Get the mock GCP hook"""
        if self._hook is None:
            self._hook = create_mock_custom_gcp_hook(self.gcp_conn_id, None, self._mock_responses)
        return self._hook

    def poke(self, context):
        """Mock implementation of poke method"""
        hook = self.get_hook()
        if self.object_name is None:
            return hook.gcs_list_files(bucket_name=self.bucket_name, prefix=self.prefix)
        else:
            return hook.gcs_file_exists(bucket_name=self.bucket_name, object_name=self.object_name)


class MockBigQueryTableSensor(BaseSensorOperator):
    """Mock implementation of CustomBigQueryTableSensor"""

    def __init__(self, project_id, dataset_id, table_id, gcp_conn_id, mock_responses, **kwargs):
        """Initialize the MockBigQueryTableSensor"""
        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcp_conn_id = gcp_conn_id
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self):
        """Get the mock GCP hook"""
        if self._hook is None:
            self._hook = create_mock_custom_gcp_hook(self.gcp_conn_id, None, self._mock_responses)
        return self._hook

    def poke(self, context):
        """Mock implementation of poke method"""
        hook = self.get_hook()
        bigquery_client = hook.get_bigquery_client()
        if self.table_id in self._mock_responses:
            return True
        else:
            return False


class MockBigQueryJobSensor(BaseSensorOperator):
    """Mock implementation of CustomBigQueryJobSensor"""

    def __init__(self, project_id, job_id, location, gcp_conn_id, mock_responses, **kwargs):
        """Initialize the MockBigQueryJobSensor"""
        super().__init__(**kwargs)
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self):
        """Get the mock GCP hook"""
        if self._hook is None:
            self._hook = create_mock_custom_gcp_hook(self.gcp_conn_id, None, self._mock_responses)
        return self._hook

    def poke(self, context):
        """Mock implementation of poke method"""
        hook = self.get_hook()
        bigquery_client = hook.get_bigquery_client()
        if self._mock_responses[self.job_id] == 'DONE':
            return True
        elif self._mock_responses[self.job_id] == 'RUNNING':
            return False
        elif self._mock_responses[self.job_id] == 'ERROR':
            raise AirflowException("Job failed")
        else:
            return False


class MockGCSObjectsWithPrefixExistenceSensor(BaseSensorOperator):
    """Mock implementation of CustomGCSObjectsWithPrefixExistenceSensor"""

    def __init__(self, bucket_name, prefix, min_objects, gcp_conn_id, delimiter, mock_responses, **kwargs):
        """Initialize the MockGCSObjectsWithPrefixExistenceSensor"""
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.min_objects = min_objects
        self.delimiter = delimiter
        self.gcp_conn_id = gcp_conn_id
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self):
        """Get the mock GCP hook"""
        if self._hook is None:
            self._hook = create_mock_custom_gcp_hook(self.gcp_conn_id, None, self._mock_responses)
        return self._hook

    def poke(self, context):
        """Mock implementation of poke method"""
        hook = self.get_hook()
        objects = hook.gcs_list_files(bucket_name=self.bucket_name, prefix=self.prefix, delimiter=self.delimiter)
        return len(objects) >= self.min_objects


class MockHttpSensor(BaseSensorOperator):
    """Mock implementation of CustomHttpSensor"""

    def __init__(self, endpoint, http_conn_id, method, headers, params, data, response_check, mock_responses, **kwargs):
        """Initialize the MockHttpSensor"""
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.method = method
        self.headers = headers
        self.params = params
        self.data = data
        self.response_check = response_check
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self):
        """Get the mock HTTP hook"""
        if self._hook is None:
            self._hook = create_mock_custom_http_hook(self.http_conn_id, self.method, self._mock_responses)
        return self._hook

    def poke(self, context):
        """Mock implementation of poke method"""
        hook = self.get_hook()
        response = hook.run(endpoint=self.endpoint, data=self.data, headers=self.headers, params=self.params)
        return self.response_check(response)


class MockHttpJsonSensor(MockHttpSensor):
    """Mock implementation of CustomHttpJsonSensor"""

    def __init__(self, endpoint, json_path, expected_value, http_conn_id, method, headers, params, data, mock_responses, **kwargs):
        """Initialize the MockHttpJsonSensor"""
        self.json_path = json_path
        self.expected_value = expected_value
        super().__init__(endpoint=endpoint, http_conn_id=http_conn_id, method=method, headers=headers, params=params, data=data, response_check=self._json_path_response_check, mock_responses=mock_responses, **kwargs)

    def _json_path_response_check(self, response):
        """Check if JSON response matches expected value"""
        import jsonpath_ng.ext as jp
        json_data = response.json()
        jsonpath_expr = jp.parse(self.json_path)
        matches = jsonpath_expr.find(json_data)
        return any(match.value == self.expected_value for match in matches)


class MockHttpStatusSensor(MockHttpSensor):
    """Mock implementation of CustomHttpStatusSensor"""

    def __init__(self, endpoint, expected_status_codes, http_conn_id, method, headers, params, data, mock_responses, **kwargs):
        """Initialize the MockHttpStatusSensor"""
        self.expected_status_codes = expected_status_codes or [200]
        super().__init__(endpoint=endpoint, http_conn_id=http_conn_id, method=method, headers=headers, params=params, data=data, response_check=self._status_code_response_check, mock_responses=mock_responses, **kwargs)

    def _status_code_response_check(self, response):
        """Check if response status code matches expected codes"""
        return response.status_code in self.expected_status_codes


class MockPostgresSensor(BaseSensorOperator):
    """Mock implementation of CustomPostgresSensor"""

    def __init__(self, sql, postgres_conn_id, schema, params, fail_on_error, alert_on_error, mock_responses, **kwargs):
        """Initialize the MockPostgresSensor"""
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.params = params
        self.fail_on_error = fail_on_error
        self.alert_on_error = alert_on_error
        self._mock_responses = mock_responses
        self._hook = None

    def get_hook(self):
        """Get the mock Postgres hook"""
        if self._hook is None:
            self._hook = create_mock_custom_postgres_hook(self.postgres_conn_id, self.schema, self._mock_responses)
        return self._hook

    def poke(self, context):
        """Mock implementation of poke method"""
        hook = self.get_hook()
        try:
            records = hook.execute_query(sql=self.sql, parameters=self.params)
            return bool(records)
        except Exception as e:
            if self.fail_on_error:
                raise
            return False


class MockPostgresTableExistenceSensor(MockPostgresSensor):
    """Mock implementation of CustomPostgresTableExistenceSensor"""

    def __init__(self, table_name, postgres_conn_id, schema, fail_on_error, alert_on_error, mock_responses, **kwargs):
        """Initialize the MockPostgresTableExistenceSensor"""
        self.table_name = table_name
        sql = f"SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = '{schema}' AND tablename = '{table_name}'"
        super().__init__(sql=sql, postgres_conn_id=postgres_conn_id, schema=schema, params={}, fail_on_error=fail_on_error, alert_on_error=alert_on_error, mock_responses=mock_responses, **kwargs)


class MockPostgresRowCountSensor(MockPostgresSensor):
    """Mock implementation of CustomPostgresRowCountSensor"""

    def __init__(self, table_name, min_rows, where_clause, postgres_conn_id, schema, fail_on_error, alert_on_error, mock_responses, **kwargs):
        """Initialize the MockPostgresRowCountSensor"""
        self.table_name = table_name
        self.min_rows = min_rows
        self.where_clause = where_clause
        sql = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        super().__init__(sql=sql, postgres_conn_id=postgres_conn_id, schema=schema, params={}, fail_on_error=fail_on_error, alert_on_error=alert_on_error, mock_responses=mock_responses, **kwargs)


class MockPostgresValueCheckSensor(MockPostgresSensor):
    """Mock implementation of CustomPostgresValueCheckSensor"""

    def __init__(self, sql, expected_value, exact_match, postgres_conn_id, schema, params, fail_on_error, alert_on_error, mock_responses, **kwargs):
        """Initialize the MockPostgresValueCheckSensor"""
        self.expected_value = expected_value
        self.exact_match = exact_match
        super().__init__(sql=sql, postgres_conn_id=postgres_conn_id, schema=schema, params=params, fail_on_error=fail_on_error, alert_on_error=alert_on_error, mock_responses=mock_responses, **kwargs)

    def poke(self, context):
        """Mock implementation of poke method"""
        records = super().poke(context)
        if not records:
            return False
        actual_value = records[0][0]
        if self.exact_match:
            return actual_value == self.expected_value
        else:
            return str(self.expected_value) in str(actual_value)


def create_mock_gcs_file_sensor(bucket_name, object_name, gcp_conn_id, mock_responses):
    """Creates a mock GCSFileSensor with controlled behavior"""
    sensor = MockGCSFileSensor(
        task_id='mock_gcs_file_sensor',
        bucket_name=bucket_name,
        object_name=object_name,
        gcp_conn_id=gcp_conn_id,
        mock_responses=mock_responses
    )
    MOCK_SENSORS[sensor.task_id] = sensor
    return sensor


def create_mock_bigquery_table_sensor(project_id, dataset_id, table_id, gcp_conn_id, mock_responses):
    """Creates a mock BigQueryTableSensor with controlled behavior"""
    sensor = MockBigQueryTableSensor(
        task_id='mock_bigquery_table_sensor',
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        gcp_conn_id=gcp_conn_id,
        mock_responses=mock_responses
    )
    MOCK_SENSORS[sensor.task_id] = sensor
    return sensor


def create_mock_bigquery_job_sensor(project_id, job_id, location, gcp_conn_id, mock_responses):
    """Creates a mock BigQueryJobSensor with controlled behavior"""
    sensor = MockBigQueryJobSensor(
        task_id='mock_bigquery_job_sensor',
        project_id=project_id,
        job_id=job_id,
        location=location,
        gcp_conn_id=gcp_conn_id,
        mock_responses=mock_responses
    )
    MOCK_SENSORS[sensor.task_id] = sensor
    return sensor


def create_mock_gcs_objects_with_prefix_sensor(bucket_name, prefix, min_objects, gcp_conn_id, mock_responses):
    """Creates a mock GCSObjectsWithPrefixExistenceSensor with controlled behavior"""
    sensor = MockGCSObjectsWithPrefixExistenceSensor(
        task_id='mock_gcs_objects_with_prefix_sensor',
        bucket_name=bucket_name,
        prefix=prefix,
        min_objects=min_objects,
        gcp_conn_id=gcp_conn_id,
        delimiter=None,
        mock_responses=mock_responses
    )
    MOCK_SENSORS[sensor.task_id] = sensor
    return sensor


def create_mock_http_sensor(endpoint, http_conn_id, method, data, headers, mock_responses):
    """Creates a mock HttpSensor with controlled behavior"""
    sensor = MockHttpSensor(
        task_id='mock_http_sensor',
        endpoint=endpoint,
        http_conn_id=http_conn_id,
        method=method,
        headers=headers,
        params={},
        data=data,
        response_check=lambda response: True,
        mock_responses=mock_responses
    )
    MOCK_SENSORS[sensor.task_id] = sensor
    return sensor


def create_mock_postgres_sensor(sql, postgres_conn_id, schema, params, mock_responses):
    """Creates a mock PostgresSensor with controlled behavior"""
    sensor = MockPostgresSensor(
        task_id='mock_postgres_sensor',
        sql=sql,
        postgres_conn_id=postgres_conn_id,
        schema=schema,
        params=params,
        fail_on_error=False,
        alert_on_error=False,
        mock_responses=mock_responses
    )
    MOCK_SENSORS[sensor.task_id] = sensor
    return sensor


def patch_sensors(mock_responses):
    """Context manager to patch sensor classes for testing"""
    patchers = {
        'gcs': patch('src.test.fixtures.mock_sensors.CustomGCSFileSensor', new=lambda task_id, bucket_name, object_name, gcp_conn_id, **kwargs: MockGCSFileSensor(bucket_name, object_name, gcp_conn_id, mock_responses, task_id=task_id, **kwargs)),
        'bigquery_table': patch('src.test.fixtures.mock_sensors.CustomBigQueryTableSensor', new=lambda task_id, project_id, dataset_id, table_id, gcp_conn_id, **kwargs: MockBigQueryTableSensor(project_id, dataset_id, table_id, gcp_conn_id, mock_responses, task_id=task_id, **kwargs)),
        'bigquery_job': patch('src.test.fixtures.mock_sensors.CustomBigQueryJobSensor', new=lambda task_id, project_id, job_id, location, gcp_conn_id, **kwargs: MockBigQueryJobSensor(project_id, job_id, location, gcp_conn_id, mock_responses, task_id=task_id, **kwargs)),
        'gcs_objects_with_prefix': patch('src.test.fixtures.mock_sensors.CustomGCSObjectsWithPrefixExistenceSensor', new=lambda task_id, bucket_name, prefix, min_objects, gcp_conn_id, **kwargs: MockGCSObjectsWithPrefixExistenceSensor(bucket_name, prefix, min_objects, gcp_conn_id, None, mock_responses, task_id=task_id, **kwargs)),
        'http': patch('src.test.fixtures.mock_sensors.CustomHttpSensor', new=lambda task_id, endpoint, http_conn_id, method, headers, data, response_check, **kwargs: MockHttpSensor(endpoint, http_conn_id, method, headers, {}, data, response_check, mock_responses, task_id=task_id, **kwargs)),
        'postgres': patch('src.test.fixtures.mock_sensors.CustomPostgresSensor', new=lambda task_id, sql, postgres_conn_id, schema, params, fail_on_error, alert_on_error, **kwargs: MockPostgresSensor(sql, postgres_conn_id, schema, params, fail_on_error, alert_on_error, mock_responses, task_id=task_id, **kwargs))
    }
    return patchers


def get_mock_sensor(sensor_id):
    """Retrieves a mock sensor by ID"""
    if sensor_id in MOCK_SENSORS:
        return MOCK_SENSORS[sensor_id]
    raise Exception(f"Mock sensor with id '{sensor_id}' not found")


def reset_mock_sensors():
    """Clears all mock sensors and resets to default state"""
    MOCK_SENSORS.clear()


def create_mock_context(execution_date=None, custom_params=None):
    """Creates a mock Airflow task context dictionary for testing"""
    context = DEFAULT_CONTEXT.copy()
    if execution_date:
        context['execution_date'] = execution_date
        context['ds'] = execution_date.strftime('%Y-%m-%d')
        context['ds_nodash'] = execution_date.strftime('%Y%m%d')
    if custom_params:
        context.update(custom_params)
    return context