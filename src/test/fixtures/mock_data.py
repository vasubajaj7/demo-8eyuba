"""
A comprehensive fixture module that provides mock data structures for testing Apache Airflow DAGs, 
operators, and connections during the migration from Airflow 1.10.15 to Airflow 2.X.

This module contains constants, functions, and classes that generate consistent test data
across both Airflow versions, facilitating compatibility testing and validation of the
Cloud Composer migration.
"""

# Standard library imports
import datetime
from datetime import datetime, timedelta
import uuid
import json
import os
import random
import copy
from typing import Any, Dict, List, Optional, Union

# Third-party imports
import pytest  # v6.0.0+
# pendulum is required for Airflow 2.X timezone-aware datetimes
try:
    import pendulum  # v2.0.0+
except ImportError:
    # Create a fallback for environments without pendulum installed
    pendulum = None

# pandas for DataFrame mock data (v1.3.0+)
try:
    import pandas as pd
except ImportError:
    pd = None

# unittest.mock for creating mock objects
from unittest.mock import MagicMock, patch

# Define global constants for testing
DEFAULT_DATE = datetime(2023, 1, 1)
DEFAULT_DATE_ISO = '2023-01-01T00:00:00+00:00'
DEFAULT_DATE_AIRFLOW1 = datetime(2023, 1, 1)
DEFAULT_DATE_AIRFLOW2 = pendulum.datetime(2023, 1, 1, tz='UTC') if pendulum else DEFAULT_DATE

MOCK_DAG_ID = 'example_dag_basic'
MOCK_PROJECT_ID = 'test-project-id'
MOCK_BUCKET_NAME = 'test-data-bucket'
MOCK_DATASET_ID = 'test_dataset'
MOCK_TABLE_ID = 'test_table'
MOCK_SECRET_ID = 'test-secret'

TASK_STATES = ['success', 'failed', 'running', 'scheduled', 'upstream_failed', 'skipped']
DEFAULT_TASK_RETRIES = 1
DEFAULT_RETRY_DELAY = timedelta(minutes=5)
DEFAULT_TASK_TIMEOUT = timedelta(hours=1)
DEFAULT_DAG_TAGS = ['test', 'example', 'migration']

# Mock connection configurations
MOCK_CONNECTIONS_CONFIG = {
    'gcp_conn': {
        'conn_type': 'google_cloud_platform',
        'host': 'https://www.googleapis.com',
        'extra': {
            'project_id': MOCK_PROJECT_ID,
            'key_path': None,
            'scope': 'https://www.googleapis.com/auth/cloud-platform'
        }
    },
    'postgres_conn': {
        'conn_type': 'postgres',
        'host': 'localhost',
        'login': 'airflow',
        'password': 'airflow',
        'schema': 'airflow',
        'port': 5432
    },
    'http_conn': {
        'conn_type': 'http',
        'host': 'https://example.com',
        'login': 'user',
        'password': 'pass'
    },
    'ftp_conn': {
        'conn_type': 'ftp',
        'host': 'ftp.example.com',
        'login': 'anonymous',
        'password': 'guest'
    },
    'sftp_conn': {
        'conn_type': 'sftp',
        'host': 'sftp.example.com',
        'login': 'testuser',
        'password': 'testpass',
        'port': 22
    },
    'redis_conn': {
        'conn_type': 'redis',
        'host': 'localhost',
        'port': 6379,
        'password': None
    }
}

# Mock Airflow variables
MOCK_VARIABLES = {
    'env': 'test',
    'data_bucket': MOCK_BUCKET_NAME,
    'project_id': MOCK_PROJECT_ID,
    'dataset_id': MOCK_DATASET_ID,
    'table_id': MOCK_TABLE_ID,
    'retry_count': '3',
    'email_on_failure': 'airflow@example.com'
}

# Default DAG arguments for testing
DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': DEFAULT_TASK_RETRIES,
    'retry_delay': DEFAULT_RETRY_DELAY
}


def is_airflow2(override: Optional[bool] = None) -> bool:
    """
    Determines if code should use Airflow 2.X compatible features.
    
    Args:
        override: If provided, override the auto-detection
        
    Returns:
        True if should use Airflow 2.X features, False for Airflow 1.X
    """
    if override is not None:
        return override
    
    # Check environment variable first
    if os.environ.get('AIRFLOW2_TESTING', '').lower() in ('true', '1', 'yes'):
        return True
    
    # Try to detect Airflow version
    try:
        import airflow
        version = airflow.__version__
        return int(version.split('.')[0]) >= 2
    except (ImportError, AttributeError, IndexError, ValueError):
        # Default to False if unable to determine
        return False


def generate_dag_run(
    dag_id: str,
    execution_date: Optional[datetime] = None,
    state: Optional[str] = None,
    conf: Optional[Dict[str, Any]] = None,
    airflow2_compatible: bool = False
) -> Dict[str, Any]:
    """
    Generates a mock DAG run with configurable properties.
    
    Args:
        dag_id: The ID of the DAG
        execution_date: The execution date for the DAG run
        state: The state of the DAG run
        conf: The configuration for the DAG run
        airflow2_compatible: Whether to use Airflow 2.X compatible format
        
    Returns:
        A dictionary representing a DAG run with all necessary fields
    """
    if execution_date is None:
        execution_date = DEFAULT_DATE
    
    if state is None:
        state = random.choice(['success', 'running', 'failed'])
        
    if conf is None:
        conf = {}
    
    # Create run_id differently based on Airflow version
    if airflow2_compatible:
        run_id = f"manual__{execution_date.strftime('%Y-%m-%dT%H:%M:%S')}"
    else:
        run_id = f"{dag_id}_{execution_date.strftime('%Y-%m-%d_%H:%M:%S')}"
    
    # Calculate dates
    start_date = execution_date
    if state in ['success', 'failed']:
        end_date = start_date + timedelta(minutes=random.randint(1, 60))
    else:
        end_date = None
    
    # Create the base structure
    dag_run = {
        'dag_id': dag_id,
        'run_id': run_id,
        'execution_date': execution_date,
        'start_date': start_date,
        'end_date': end_date,
        'state': state,
        'conf': conf,
    }
    
    # Add Airflow 2.X specific fields if needed
    if airflow2_compatible:
        dag_run['data_interval_start'] = execution_date
        dag_run['data_interval_end'] = execution_date + timedelta(days=1)
        # In Airflow 2.X, the execution_date field is called logical_date for better clarity
        dag_run['logical_date'] = execution_date
    
    return dag_run


def generate_task_instance(
    task_id: str,
    dag_id: str,
    execution_date: Optional[datetime] = None,
    state: Optional[str] = None,
    xcom_push: Optional[Dict[str, Any]] = None,
    airflow2_compatible: bool = False
) -> Dict[str, Any]:
    """
    Generates a mock task instance for testing.
    
    Args:
        task_id: The ID of the task
        dag_id: The ID of the DAG
        execution_date: The execution date for the task instance
        state: The state of the task instance
        xcom_push: XCom values to push
        airflow2_compatible: Whether to use Airflow 2.X compatible format
        
    Returns:
        A dictionary representing a task instance
    """
    if execution_date is None:
        execution_date = DEFAULT_DATE
    
    if state is None:
        state = random.choice(TASK_STATES)
    
    # Calculate mock dates
    if state in ['success', 'failed']:
        start_date = execution_date
        duration = random.randint(1, 300)  # 1-300 seconds
        end_date = start_date + timedelta(seconds=duration)
    elif state == 'running':
        start_date = execution_date
        end_date = None
        duration = None
    else:
        start_date = None
        end_date = None
        duration = None
    
    # Create base task instance
    task_instance = {
        'task_id': task_id,
        'dag_id': dag_id,
        'execution_date': execution_date,
        'state': state,
        'start_date': start_date,
        'end_date': end_date,
        'duration': duration,
        'try_number': 1,
        'max_tries': DEFAULT_TASK_RETRIES + 1,
    }
    
    # Handle error messages for failed tasks
    if state == 'failed':
        task_instance['error'] = "Mock task failure for testing"
    
    # Add XCom data if provided
    if xcom_push:
        task_instance['xcom_push'] = xcom_push
    
    # Add Airflow 2.X specific fields
    if airflow2_compatible:
        run_id = f"manual__{execution_date.strftime('%Y-%m-%dT%H:%M:%S')}"
        task_instance['run_id'] = run_id
        # In Airflow 2.X, map_index is used for dynamic task mapping
        task_instance['map_index'] = -1
        # For compatibility with newer operators
        task_instance['operator'] = 'PythonOperator'
    
    return task_instance


def create_mock_airflow_context(
    task_id: str,
    dag_id: str,
    execution_date: Optional[datetime] = None,
    params: Optional[Dict[str, Any]] = None,
    conf: Optional[Dict[str, Any]] = None,
    airflow2_compatible: bool = False
) -> Dict[str, Any]:
    """
    Creates a context dictionary for task execution testing.
    
    Args:
        task_id: The ID of the task
        dag_id: The ID of the DAG
        execution_date: The execution date for the task instance
        params: The parameters for the task
        conf: The configuration for the DAG
        airflow2_compatible: Whether to use Airflow 2.X compatible format
        
    Returns:
        A context dictionary for Airflow operator testing
    """
    if execution_date is None:
        execution_date = DEFAULT_DATE
    
    if params is None:
        params = {}
    
    if conf is None:
        conf = {}
    
    # Create task instance data
    ti_data = generate_task_instance(
        task_id=task_id,
        dag_id=dag_id,
        execution_date=execution_date,
        state='running',
        airflow2_compatible=airflow2_compatible
    )
    
    # Create run_id according to version convention
    if airflow2_compatible:
        run_id = f"manual__{execution_date.strftime('%Y-%m-%dT%H:%M:%S')}"
    else:
        run_id = f"{dag_id}_{execution_date.strftime('%Y-%m-%d_%H:%M:%S')}"
    
    # Create base context (common to both versions)
    context = {
        'dag_id': dag_id,
        'task_id': task_id,
        'execution_date': execution_date,
        'ds': execution_date.strftime('%Y-%m-%d'),
        'ts': execution_date.strftime('%Y-%m-%d %H:%M:%S'),
        'run_id': run_id,
        'params': params,
        'conf': conf,
        'macros': {},  # Add mock macros if needed
    }
    
    # Add version-specific elements
    if airflow2_compatible:
        # Airflow 2.X uses task_instance consistently
        context['task_instance'] = ti_data
        context['logical_date'] = execution_date
        # Data intervals in Airflow 2.X
        context['data_interval_start'] = execution_date
        context['data_interval_end'] = execution_date + timedelta(days=1)
        # Task object would be mocked separately if needed
    else:
        # Airflow 1.X uses ti and sometimes task_instance
        context['ti'] = ti_data
        # Older context keys
        context['ds_nodash'] = execution_date.strftime('%Y%m%d')
    
    return context


def generate_mock_dag(
    dag_id: Optional[str] = None,
    schedule_interval: Optional[str] = None,
    start_date: Optional[datetime] = None,
    default_args: Optional[Dict[str, Any]] = None,
    task_ids: Optional[List[str]] = None,
    dag_type: Optional[str] = None,
    airflow2_compatible: bool = False
) -> Dict[str, Any]:
    """
    Creates a mock DAG structure with tasks.
    
    Args:
        dag_id: The ID of the DAG
        schedule_interval: The schedule interval for the DAG
        start_date: The start date for the DAG
        default_args: The default arguments for the DAG
        task_ids: List of task IDs to include in the DAG
        dag_type: Type of DAG structure to generate
        airflow2_compatible: Whether to use Airflow 2.X compatible format
        
    Returns:
        A dictionary representing a complete DAG structure
    """
    if dag_id is None:
        dag_id = MOCK_DAG_ID
    
    if schedule_interval is None:
        schedule_interval = '@daily'
    
    if start_date is None:
        start_date = DEFAULT_DATE
    
    if default_args is None:
        default_args = copy.deepcopy(DAG_DEFAULT_ARGS)
    
    # Create basic DAG structure
    dag = {
        'dag_id': dag_id,
        'schedule_interval': schedule_interval,
        'start_date': start_date,
        'default_args': default_args,
    }
    
    # Add Airflow 2.X specific fields
    if airflow2_compatible:
        dag['catchup'] = False
        dag['tags'] = DEFAULT_DAG_TAGS
    
    # Add tasks to the DAG
    if task_ids is None:
        # Default ETL pattern
        if dag_type == 'etl' or dag_type is None:
            task_ids = ['extract', 'transform', 'load']
        elif dag_type == 'simple':
            task_ids = ['start', 'process', 'end']
        else:
            # Generate some random tasks
            num_tasks = random.randint(3, 6)
            task_ids = [f'task_{i}' for i in range(1, num_tasks + 1)]
    
    # Create task list
    dag['tasks'] = []
    for i, task_id in enumerate(task_ids):
        task = {
            'task_id': task_id,
            'dag_id': dag_id,
            'operator': 'PythonOperator' if i % 2 == 0 else 'BashOperator',
            'upstream_task_ids': [] if i == 0 else [task_ids[i-1]],
            'downstream_task_ids': [] if i == len(task_ids) - 1 else [task_ids[i+1]],
        }
        
        # Add operator-specific fields
        if task['operator'] == 'PythonOperator':
            task['python_callable'] = f"mock_python_func_{task_id}"
        elif task['operator'] == 'BashOperator':
            task['bash_command'] = f"echo 'Running {task_id}'"
        
        dag['tasks'].append(task)
    
    # Set dependencies differently based on Airflow version
    if airflow2_compatible:
        # In Airflow 2, we often use the >> operator for dependencies
        dag['dependency_structure'] = []
        for i in range(len(task_ids) - 1):
            dag['dependency_structure'].append(f"{task_ids[i]} >> {task_ids[i+1]}")
    else:
        # In Airflow 1, we might use set_upstream/set_downstream
        dag['dependency_structure'] = []
        for i in range(1, len(task_ids)):
            dag['dependency_structure'].append(f"{task_ids[i]}.set_upstream({task_ids[i-1]})")
    
    return dag


def generate_mock_connections(
    conn_ids: Optional[List[str]] = None,
    airflow2_compatible: bool = False
) -> Dict[str, Dict[str, Any]]:
    """
    Generates mock connection configurations for testing.
    
    Args:
        conn_ids: List of connection IDs to generate
        airflow2_compatible: Whether to use Airflow 2.X compatible format
        
    Returns:
        Dictionary of connection configurations keyed by conn_id
    """
    if conn_ids is None:
        # Use all available connections
        conn_ids = list(MOCK_CONNECTIONS_CONFIG.keys())
    
    connections = {}
    for conn_id in conn_ids:
        if conn_id in MOCK_CONNECTIONS_CONFIG:
            # Deep copy to prevent modifications affecting the original
            conn_config = copy.deepcopy(MOCK_CONNECTIONS_CONFIG[conn_id])
            
            # Format connection based on Airflow version
            if airflow2_compatible:
                # In Airflow 2.X, extra fields are often stored as a JSON string
                if 'extra' in conn_config and isinstance(conn_config['extra'], dict):
                    conn_config['extra'] = json.dumps(conn_config['extra'])
            
            connections[conn_id] = conn_config
    
    return connections


def generate_mock_variables(
    variable_names: Optional[List[str]] = None,
    as_json: bool = False
) -> Dict[str, Any]:
    """
    Generates mock Airflow variables for testing.
    
    Args:
        variable_names: List of variable names to generate
        as_json: Whether to return complex values as JSON strings
        
    Returns:
        Dictionary of variable values keyed by variable name
    """
    if variable_names is None:
        # Use all available variables
        variable_names = list(MOCK_VARIABLES.keys())
    
    variables = {}
    for var_name in variable_names:
        if var_name in MOCK_VARIABLES:
            val = copy.deepcopy(MOCK_VARIABLES[var_name])
            
            # Convert to JSON if needed
            if as_json and isinstance(val, (dict, list)):
                val = json.dumps(val)
            
            variables[var_name] = val
    
    return variables


def generate_mock_xcom(
    task_id: str,
    dag_id: str,
    key: str,
    value: Any,
    execution_date: Optional[datetime] = None,
    airflow2_compatible: bool = False
) -> Dict[str, Any]:
    """
    Generates mock XCom data for testing task communication.
    
    Args:
        task_id: The ID of the task
        dag_id: The ID of the DAG
        key: The XCom key
        value: The XCom value
        execution_date: The execution date
        airflow2_compatible: Whether to use Airflow 2.X compatible format
        
    Returns:
        A dictionary representing an XCom record
    """
    if execution_date is None:
        execution_date = DEFAULT_DATE
    
    # If value is not a primitive type, serialize it
    if not isinstance(value, (str, int, float, bool, type(None))):
        serialized_value = json.dumps(value)
    else:
        serialized_value = value
    
    # Create base XCom structure
    xcom = {
        'key': key,
        'value': serialized_value,
        'task_id': task_id,
        'dag_id': dag_id,
        'execution_date': execution_date,
        'timestamp': datetime.now(),
    }
    
    # Add Airflow 2.X specific fields
    if airflow2_compatible:
        run_id = f"manual__{execution_date.strftime('%Y-%m-%dT%H:%M:%S')}"
        xcom['run_id'] = run_id
        # In Airflow 2.X, map_index is used for dynamically mapped tasks
        xcom['map_index'] = -1
    
    return xcom


def generate_mock_gcp_data(
    service_type: str,
    params: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generates mock GCP service data for testing.
    
    Args:
        service_type: Type of GCP service ('gcs', 'bigquery', 'secretmanager')
        params: Parameters specific to the service
        
    Returns:
        Mock GCP service data structure
    """
    if service_type == 'gcs':
        # Generate GCS bucket/object data
        bucket_name = params.get('bucket_name', MOCK_BUCKET_NAME)
        object_name = params.get('object_name', f"test_file_{random.randint(1000, 9999)}.csv")
        
        return {
            'kind': 'storage#object',
            'id': f"{bucket_name}/{object_name}",
            'selfLink': f"https://www.googleapis.com/storage/v1/b/{bucket_name}/o/{object_name}",
            'name': object_name,
            'bucket': bucket_name,
            'generation': str(random.randint(10000000, 99999999)),
            'metageneration': '1',
            'contentType': 'text/csv',
            'timeCreated': datetime.now().isoformat(),
            'updated': datetime.now().isoformat(),
            'storageClass': 'STANDARD',
            'size': str(random.randint(1000, 100000)),
            'md5Hash': 'mock_md5_hash',
            'metadata': params.get('metadata', {}),
            'crc32c': 'mock_crc32c',
            'etag': 'mock_etag'
        }
    
    elif service_type == 'bigquery':
        # Generate BigQuery dataset/table data
        dataset_id = params.get('dataset_id', MOCK_DATASET_ID)
        table_id = params.get('table_id', MOCK_TABLE_ID)
        project_id = params.get('project_id', MOCK_PROJECT_ID)
        
        return {
            'kind': 'bigquery#table',
            'id': f"{project_id}:{dataset_id}.{table_id}",
            'tableReference': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_id
            },
            'schema': {
                'fields': [
                    {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                    {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
                ]
            },
            'numRows': str(random.randint(100, 10000)),
            'numBytes': str(random.randint(10000, 1000000)),
            'creationTime': str(int(datetime.now().timestamp() * 1000)),
            'lastModifiedTime': str(int(datetime.now().timestamp() * 1000)),
            'type': 'TABLE',
            'location': 'US'
        }
    
    elif service_type == 'secretmanager':
        # Generate Secret Manager data
        secret_id = params.get('secret_id', MOCK_SECRET_ID)
        secret_value = params.get('secret_value', f"mock_secret_value_{uuid.uuid4()}")
        version = params.get('version', '1')
        
        return {
            'name': f"projects/{MOCK_PROJECT_ID}/secrets/{secret_id}/versions/{version}",
            'payload': {
                'data': secret_value if isinstance(secret_value, str) else json.dumps(secret_value)
            },
            'metadata': {
                'createTime': datetime.now().isoformat(),
                'replicationStatus': {
                    'automatic': {
                        'customerManagedEncryption': None
                    }
                },
                'state': 'ENABLED'
            }
        }
    
    else:
        # Unknown service type
        return {
            'error': f"Unknown GCP service type: {service_type}",
            'params': params
        }


def generate_mock_gcs_file_list(
    bucket_name: str,
    prefix: Optional[str] = None,
    count: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Generates a list of mock GCS files with metadata.
    
    Args:
        bucket_name: GCS bucket name
        prefix: File prefix to use
        count: Number of files to generate
        
    Returns:
        List of GCS file objects with metadata
    """
    if bucket_name is None:
        bucket_name = MOCK_BUCKET_NAME
    
    if prefix is None:
        prefix = 'data/'
    
    if count is None:
        count = 10
    
    file_list = []
    for i in range(count):
        # Generate file name with random component
        random_part = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))
        file_name = f"{prefix}{random_part}.csv"
        
        # Generate file metadata
        file_obj = {
            'name': file_name,
            'bucket': bucket_name,
            'contentType': 'text/csv',
            'size': str(random.randint(1000, 100000)),
            'updated': datetime.now().isoformat(),
            'generation': str(random.randint(10000000, 99999999)),
            'metageneration': '1',
            'etag': f"mock_etag_{random_part}",
            'crc32c': f"mock_crc32c_{random_part}",
            'md5Hash': f"mock_md5_hash_{random_part}",
            'storageClass': 'STANDARD',
            'timeCreated': (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
        }
        
        file_list.append(file_obj)
    
    return file_list


def generate_mock_bigquery_data(
    data_type: str,
    params: Dict[str, Any]
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Generates mock BigQuery data (tables, schemas, query results).
    
    Args:
        data_type: Type of BigQuery data to generate
        params: Parameters for the data generation
        
    Returns:
        Mock BigQuery data structure
    """
    if data_type == 'table_schema':
        # Generate a table schema
        fields = params.get('fields', [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ])
        
        return {
            'schema': {
                'fields': fields
            }
        }
    
    elif data_type == 'query_result':
        # Generate mock query results
        schema = params.get('schema', [
            {'name': 'id', 'type': 'INTEGER'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'value', 'type': 'FLOAT'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}
        ])
        
        row_count = params.get('row_count', 10)
        
        # Generate rows based on schema
        rows = []
        for i in range(row_count):
            row = {}
            for field in schema:
                field_name = field['name']
                field_type = field['type']
                
                if field_type == 'INTEGER':
                    row[field_name] = random.randint(1, 1000)
                elif field_type == 'STRING':
                    row[field_name] = f"mock_value_{i}_{field_name}"
                elif field_type == 'FLOAT':
                    row[field_name] = random.uniform(1.0, 100.0)
                elif field_type == 'TIMESTAMP':
                    # Generate a random timestamp in the past
                    days_ago = random.randint(1, 30)
                    row[field_name] = (datetime.now() - timedelta(days=days_ago)).isoformat()
                else:
                    row[field_name] = None
            
            rows.append(row)
        
        return {
            'schema': schema,
            'rows': rows,
            'totalRows': row_count,
            'pageToken': None,
            'totalBytesProcessed': str(random.randint(10000, 1000000))
        }
    
    elif data_type == 'dataset_list':
        # Generate list of datasets
        project_id = params.get('project_id', MOCK_PROJECT_ID)
        dataset_count = params.get('dataset_count', 5)
        
        datasets = []
        for i in range(dataset_count):
            dataset_id = f"{MOCK_DATASET_ID}_{i}"
            datasets.append({
                'kind': 'bigquery#dataset',
                'id': f"{project_id}:{dataset_id}",
                'datasetReference': {
                    'datasetId': dataset_id,
                    'projectId': project_id
                },
                'location': 'US',
                'creationTime': str(int((datetime.now() - timedelta(days=random.randint(1, 365))).timestamp() * 1000))
            })
        
        return {
            'kind': 'bigquery#datasetList',
            'datasets': datasets
        }
    
    else:
        # Unknown data type
        return {
            'error': f"Unknown BigQuery data type: {data_type}",
            'params': params
        }


def generate_mock_dataframe(
    columns: Optional[List[str]] = None,
    row_count: Optional[int] = None,
    column_types: Optional[Dict[str, str]] = None
) -> 'pandas.DataFrame':
    """
    Generates a pandas DataFrame with mock data.
    
    Args:
        columns: List of column names
        row_count: Number of rows to generate
        column_types: Dictionary mapping column names to data types
        
    Returns:
        DataFrame with mock data
    """
    if pd is None:
        raise ImportError("pandas is required for generate_mock_dataframe")
    
    if columns is None:
        columns = ['id', 'name', 'value', 'timestamp']
    
    if row_count is None:
        row_count = 100
    
    if column_types is None:
        column_types = {
            'id': 'int',
            'name': 'str',
            'value': 'float',
            'timestamp': 'datetime'
        }
    
    # Generate data for each column
    data = {}
    for col in columns:
        col_type = column_types.get(col, 'str')
        
        if col_type == 'int':
            data[col] = [random.randint(1, 1000) for _ in range(row_count)]
        elif col_type == 'float':
            data[col] = [random.uniform(1.0, 100.0) for _ in range(row_count)]
        elif col_type == 'datetime':
            data[col] = [(datetime.now() - timedelta(days=random.randint(1, 365))) for _ in range(row_count)]
        elif col_type == 'bool':
            data[col] = [random.choice([True, False]) for _ in range(row_count)]
        else:  # Default to string
            data[col] = [f"mock_value_{i}_{col}" for i in range(row_count)]
    
    # Create DataFrame
    return pd.DataFrame(data)


def generate_mock_secret(
    secret_id: Optional[str] = None,
    secret_value: Optional[str] = None,
    version: Optional[int] = None
) -> Dict[str, Any]:
    """
    Generates mock secret data for testing Secret Manager integration.
    
    Args:
        secret_id: The ID of the secret
        secret_value: The value of the secret
        version: The version of the secret
        
    Returns:
        Mock secret data structure
    """
    if secret_id is None:
        secret_id = MOCK_SECRET_ID
    
    if secret_value is None:
        # Generate a random secret value
        secret_value = f"mock_secret_{uuid.uuid4()}"
    
    if version is None:
        version = 1
    
    # Create mock secret structure
    secret = {
        'name': f"projects/{MOCK_PROJECT_ID}/secrets/{secret_id}/versions/{version}",
        'value': secret_value,
        'metadata': {
            'name': f"projects/{MOCK_PROJECT_ID}/secrets/{secret_id}/versions/{version}",
            'create_time': datetime.now().isoformat(),
            'state': 'ENABLED',
            'version': str(version)
        }
    }
    
    return secret


class MockDataGenerator:
    """
    A class for generating consistent mock data across tests.
    """
    
    def __init__(self, airflow2_compatible: bool = None):
        """
        Initialize the MockDataGenerator.
        
        Args:
            airflow2_compatible: Whether to generate Airflow 2.X compatible data
        """
        self.airflow2_compatible = airflow2_compatible if airflow2_compatible is not None else is_airflow2()
        
        # Initialize storage for generated data
        self.connections = {}
        self.variables = {}
        self.dags = {}
        self.xcoms = {}
        
        # Pre-populate with some common test data
        self._initialize_common_data()
    
    def _initialize_common_data(self):
        """Initialize with common test data."""
        # Add a basic test DAG
        self.generate_dag(MOCK_DAG_ID)
        
        # Add common connections
        self.generate_connection('gcp_conn')
        self.generate_connection('postgres_conn')
        
        # Add common variables
        for key, value in MOCK_VARIABLES.items():
            self.generate_variable(key, value)
    
    def generate_dag(
        self,
        dag_id: Optional[str] = None,
        dag_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate a mock DAG and store in internal dictionary.
        
        Args:
            dag_id: The ID of the DAG
            dag_params: Parameters for DAG generation
            
        Returns:
            The generated DAG structure
        """
        if dag_id is None:
            dag_id = MOCK_DAG_ID
        
        if dag_params is None:
            dag_params = {}
        
        # Generate DAG using the helper function
        dag = generate_mock_dag(
            dag_id=dag_id,
            airflow2_compatible=self.airflow2_compatible,
            **dag_params
        )
        
        # Store in internal dictionary
        self.dags[dag_id] = dag
        
        return dag
    
    def generate_task(
        self,
        dag_id: str,
        task_id: str,
        task_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate a mock task for a specific DAG.
        
        Args:
            dag_id: The ID of the DAG
            task_id: The ID of the task
            task_params: Parameters for task generation
            
        Returns:
            The generated task structure
        """
        if task_params is None:
            task_params = {}
        
        # Ensure DAG exists
        if dag_id not in self.dags:
            self.generate_dag(dag_id)
        
        # Create task
        task = {
            'task_id': task_id,
            'dag_id': dag_id,
            'operator': task_params.get('operator', 'PythonOperator'),
            'upstream_task_ids': task_params.get('upstream_task_ids', []),
            'downstream_task_ids': task_params.get('downstream_task_ids', []),
        }
        
        # Add operator-specific fields
        if task['operator'] == 'PythonOperator':
            task['python_callable'] = task_params.get('python_callable', f"mock_python_func_{task_id}")
        elif task['operator'] == 'BashOperator':
            task['bash_command'] = task_params.get('bash_command', f"echo 'Running {task_id}'")
        
        # Add task to DAG
        self.dags[dag_id]['tasks'].append(task)
        
        return task
    
    def generate_connection(
        self,
        conn_id: str,
        conn_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate a mock connection and store in internal dictionary.
        
        Args:
            conn_id: The ID of the connection
            conn_params: Parameters for connection generation
            
        Returns:
            The generated connection structure
        """
        # Get base parameters from config or use provided ones
        if conn_params is None and conn_id in MOCK_CONNECTIONS_CONFIG:
            conn_params = copy.deepcopy(MOCK_CONNECTIONS_CONFIG[conn_id])
        elif conn_params is None:
            conn_params = {
                'conn_type': 'http',
                'host': f"https://{conn_id}.example.com",
                'login': 'user',
                'password': 'pass'
            }
        
        # Format for Airflow 2.X if needed
        if self.airflow2_compatible and 'extra' in conn_params and isinstance(conn_params['extra'], dict):
            conn_params['extra'] = json.dumps(conn_params['extra'])
        
        # Store in internal dictionary
        self.connections[conn_id] = conn_params
        
        return conn_params
    
    def generate_variable(
        self,
        key: str,
        value: Any
    ) -> Any:
        """
        Generate a mock Airflow variable and store in internal dictionary.
        
        Args:
            key: The variable key
            value: The variable value
            
        Returns:
            The stored variable value
        """
        # If value is complex, determine if it should be serialized
        if isinstance(value, (dict, list)):
            # In Airflow, variables can be stored as JSON strings or retrieved as objects
            # We'll store as objects and serialize as needed
            self.variables[key] = value
        else:
            self.variables[key] = value
        
        return value
    
    def generate_xcom(
        self,
        task_id: str,
        dag_id: str,
        key: str,
        value: Any,
        execution_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Generate a mock XCom entry and store in internal dictionary.
        
        Args:
            task_id: The ID of the task
            dag_id: The ID of the DAG
            key: The XCom key
            value: The XCom value
            execution_date: The execution date
            
        Returns:
            The generated XCom structure
        """
        if execution_date is None:
            execution_date = DEFAULT_DATE
        
        # Generate XCom using helper function
        xcom = generate_mock_xcom(
            task_id=task_id,
            dag_id=dag_id,
            key=key,
            value=value,
            execution_date=execution_date,
            airflow2_compatible=self.airflow2_compatible
        )
        
        # Store in internal dictionary
        # Use composite key for lookup
        composite_key = f"{dag_id}_{task_id}_{key}_{execution_date.isoformat()}"
        self.xcoms[composite_key] = xcom
        
        return xcom
    
    def get_all_dags(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all stored mock DAGs.
        
        Returns:
            Dictionary of all mock DAGs
        """
        return self.dags
    
    def get_all_connections(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all stored mock connections.
        
        Returns:
            Dictionary of all mock connections
        """
        return self.connections
    
    def get_all_variables(self) -> Dict[str, Any]:
        """
        Get all stored mock variables.
        
        Returns:
            Dictionary of all mock variables
        """
        return self.variables
    
    def get_all_xcoms(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all stored mock XComs.
        
        Returns:
            Dictionary of all mock XComs
        """
        return self.xcoms


class MockAirflowModels:
    """
    Class that provides mock Airflow model objects supporting both Airflow versions.
    """
    
    def __init__(self, airflow2_compatible: bool = None):
        """
        Initialize the MockAirflowModels.
        
        Args:
            airflow2_compatible: Whether to create Airflow 2.X compatible models
        """
        self.airflow2_compatible = airflow2_compatible if airflow2_compatible is not None else is_airflow2()
    
    def create_mock_dag(self, dag_data: Dict[str, Any]) -> object:
        """
        Create a mock DAG object that matches the Airflow version.
        
        Args:
            dag_data: Dictionary of DAG attributes
            
        Returns:
            Mock DAG object with appropriate structure
        """
        mock_dag = MagicMock()
        
        # Set all attributes from dag_data
        for key, value in dag_data.items():
            setattr(mock_dag, key, value)
        
        # Special attributes based on version
        if self.airflow2_compatible:
            # In Airflow 2.X, 'schedule_interval' is deprecated in favor of 'schedule'
            # but we'll support both for backward compatibility
            if 'schedule_interval' in dag_data and 'schedule' not in dag_data:
                mock_dag.schedule = dag_data['schedule_interval']
            
            # Add tags if not present
            if not hasattr(mock_dag, 'tags'):
                mock_dag.tags = DEFAULT_DAG_TAGS
            
            # Airflow 2.X DAG has a has_task method
            mock_dag.has_task = MagicMock(return_value=True)
            
            # Airflow 2.X DAG has a get_task method
            def mock_get_task(task_id):
                for task in dag_data.get('tasks', []):
                    if task['task_id'] == task_id:
                        return self.create_mock_task(task)
                return None
            
            mock_dag.get_task = mock_get_task
        else:
            # Airflow 1.X specific attributes or methods if needed
            pass
        
        return mock_dag
    
    def create_mock_task(self, task_data: Dict[str, Any]) -> object:
        """
        Create a mock Task object that matches the Airflow version.
        
        Args:
            task_data: Dictionary of task attributes
            
        Returns:
            Mock Task object with appropriate structure
        """
        mock_task = MagicMock()
        
        # Set all attributes from task_data
        for key, value in task_data.items():
            setattr(mock_task, key, value)
        
        # Add execution methods based on version
        if self.airflow2_compatible:
            # In Airflow 2.X, execute takes context as arg
            def mock_execute(context=None):
                return f"Executed task {task_data['task_id']}"
            
            mock_task.execute = mock_execute
            
            # Decorated version of callable for TaskFlow API if applicable
            if 'python_callable' in task_data:
                mock_task.python_callable = MagicMock(return_value=f"Executed {task_data['task_id']}")
        else:
            # In Airflow 1.X, execute might take different args
            def mock_execute(context=None):
                return f"Executed task {task_data['task_id']}"
            
            mock_task.execute = mock_execute
        
        return mock_task
    
    def create_mock_ti(self, ti_data: Dict[str, Any]) -> object:
        """
        Create a mock TaskInstance object that matches the Airflow version.
        
        Args:
            ti_data: Dictionary of task instance attributes
            
        Returns:
            Mock TaskInstance object with appropriate structure
        """
        mock_ti = MagicMock()
        
        # Set all attributes from ti_data
        for key, value in ti_data.items():
            setattr(mock_ti, key, value)
        
        # Add XCom methods
        xcom_push_values = {}
        
        def mock_xcom_push(key, value, execution_date=None):
            if execution_date is None:
                execution_date = ti_data.get('execution_date', DEFAULT_DATE)
            xcom_key = f"{ti_data['task_id']}_{key}_{execution_date.isoformat()}"
            xcom_push_values[xcom_key] = value
            return value
        
        def mock_xcom_pull(task_ids=None, key=None, dag_id=None, execution_date=None):
            if execution_date is None:
                execution_date = ti_data.get('execution_date', DEFAULT_DATE)
            
            # Handle different versions
            if self.airflow2_compatible:
                # In Airflow 2.X, xcom_pull can handle lists of task_ids
                if isinstance(task_ids, list):
                    result = {}
                    for task_id in task_ids:
                        xcom_key = f"{task_id}_{key}_{execution_date.isoformat()}"
                        if xcom_key in xcom_push_values:
                            result[task_id] = xcom_push_values[xcom_key]
                    return result
            
            # Common implementation for both versions
            if task_ids is None:
                task_ids = ti_data['task_id']
            
            xcom_key = f"{task_ids}_{key}_{execution_date.isoformat()}"
            return xcom_push_values.get(xcom_key)
        
        mock_ti.xcom_push = mock_xcom_push
        mock_ti.xcom_pull = mock_xcom_pull
        
        # Handle state differently based on version
        if self.airflow2_compatible:
            # In Airflow 2.X, we might have specific state handling
            def mock_set_state(state, session=None):
                mock_ti.state = state
                return state
            
            mock_ti.set_state = mock_set_state
        
        return mock_ti
    
    def create_mock_connection(self, conn_data: Dict[str, Any]) -> object:
        """
        Create a mock Connection object that matches the Airflow version.
        
        Args:
            conn_data: Dictionary of connection attributes
            
        Returns:
            Mock Connection object with appropriate structure
        """
        mock_conn = MagicMock()
        
        # Set all attributes from conn_data
        for key, value in conn_data.items():
            setattr(mock_conn, key, value)
        
        # Handle 'extra' field differently based on version
        if 'extra' in conn_data:
            extra = conn_data['extra']
            
            if self.airflow2_compatible:
                # In Airflow 2.X, extra is often stored as JSON string
                if isinstance(extra, str):
                    # Provide a get_extra method that parses the JSON
                    def mock_get_extra(extra_key=None):
                        try:
                            extra_dict = json.loads(extra)
                            if extra_key is not None:
                                return extra_dict.get(extra_key)
                            return extra_dict
                        except (json.JSONDecodeError, TypeError):
                            return extra if extra_key is None else None
                    
                    mock_conn.get_extra = mock_get_extra
                else:
                    # Handle case where extra is already a dict
                    def mock_get_extra(extra_key=None):
                        if extra_key is not None:
                            return extra.get(extra_key)
                        return extra
                    
                    mock_conn.get_extra = mock_get_extra
                    # Also provide the JSON string for compatibility
                    mock_conn.extra = json.dumps(extra) if isinstance(extra, dict) else str(extra)
            else:
                # In Airflow 1.X, handling might be different
                def mock_get_extra(extra_key=None):
                    if isinstance(extra, dict) and extra_key is not None:
                        return extra.get(extra_key)
                    return extra
                
                mock_conn.get_extra = mock_get_extra
        
        return mock_conn