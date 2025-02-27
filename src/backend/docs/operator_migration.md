# Operator Migration Guide: Airflow 1.10.15 to Airflow 2.X

## Introduction

This document provides a comprehensive guide for migrating Airflow operators from version 1.10.15 to Airflow 2.X as part of the Cloud Composer 2 migration project. The transition to Airflow 2.X introduces significant changes to the operator framework, including package restructuring, enhanced functionality, and new programming paradigms.

### Objectives

- Document the migration process for commonly used operators
- Provide clear examples of migration patterns with before and after code
- Highlight new features and capabilities in Airflow 2.X operators
- Address potential issues and their solutions during migration
- Establish best practices for operator development in Airflow 2.X

### Migration Approach

Our migration approach focuses on incremental adaptation rather than complete rewrites, allowing existing DAGs to maintain functionality while leveraging new Airflow 2.X features. The recommended approach follows these steps:

1. Update import statements to align with Airflow 2.X package structure
2. Migrate operator parameters that have been renamed or deprecated
3. Adopt new features like the TaskFlow API where appropriate
4. Test thoroughly to ensure behavior consistency
5. Apply best practices for improved maintainability

## Understanding Airflow 2.X Changes

### Package Structure Changes

Airflow 2.X introduces a provider-based architecture that separates core functionality from integrations with external services. This reorganization affects how operators are imported and used.

#### Provider Packages

In Airflow 1.10.15, many operators were bundled within the core Airflow package. Airflow 2.X moves these operators to dedicated provider packages:

- `airflow.providers.google.cloud`: Google Cloud operators
- `airflow.providers.http`: HTTP-related operators
- `airflow.providers.postgres`: PostgreSQL operators
- and many others

These packages must be explicitly installed alongside the core Airflow installation:

```bash
pip install apache-airflow-providers-google>=2.0.0
pip install apache-airflow-providers-http>=2.0.0
pip install apache-airflow-providers-postgres>=2.0.0
```

#### Core Package Reorganization

Even within the core Airflow package, operator imports have changed:

- `airflow.operators.python_operator` → `airflow.operators.python`
- `airflow.operators.bash_operator` → `airflow.operators.bash`
- `airflow.sensors.base_sensor_operator` → `airflow.sensors.base`

### Operator Interface Changes

Airflow 2.X standardizes operator interfaces and parameter naming for consistency across all operators:

- Consistent naming conventions (e.g., `postgres_conn_id` instead of `postgres_conn_id` and `postgres_connection_id` being used inconsistently)
- Explicit typing support with Python type hints
- Enhanced XCom functionality with easier value pushing and pulling
- More granular hook interfaces

### New Operator Features

Airflow 2.X introduces several new features that enhance operator capabilities:

- TaskFlow API for Python-centric workflow definitions
- Cross-DAG dependencies
- Improved error handling with more detailed exception information
- Enhanced security features for connection handling
- Better serialization of operator data

## Import Statement Migration

### Standard Operator Imports

**Airflow 1.10.15:**
```python
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
```

**Airflow 2.X:**
```python
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
```

### Provider Package Imports

**Airflow 1.10.15:**
```python
# Google Cloud imports
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# HTTP imports
from airflow.operators.http_operator import SimpleHttpOperator
```

**Airflow 2.X:**
```python
# Google Cloud imports
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# HTTP imports
from airflow.providers.http.operators.http import SimpleHttpOperator
```

### Import Mapping Reference

| Airflow 1.10.15 | Airflow 2.X |
|-----------------|-------------|
| `airflow.operators.python_operator` | `airflow.operators.python` |
| `airflow.operators.bash_operator` | `airflow.operators.bash` |
| `airflow.operators.dummy_operator` | `airflow.operators.dummy` |
| `airflow.operators.http_operator` | `airflow.providers.http.operators.http` |
| `airflow.operators.postgres_operator` | `airflow.providers.postgres.operators.postgres` |
| `airflow.contrib.operators.gcs_to_gcs` | `airflow.providers.google.cloud.transfers.gcs_to_gcs` |
| `airflow.contrib.operators.bigquery_operator` | `airflow.providers.google.cloud.operators.bigquery` |
| `airflow.contrib.operators.gcs_to_bq` | `airflow.providers.google.cloud.transfers.gcs_to_bigquery` |
| `airflow.contrib.hooks.gcs_hook` | `airflow.providers.google.cloud.hooks.gcs` |
| `airflow.hooks.postgres_hook` | `airflow.providers.postgres.hooks.postgres` |
| `airflow.hooks.http_hook` | `airflow.providers.http.hooks.http` |
| `airflow.sensors.base_sensor_operator` | `airflow.sensors.base` |
| `airflow.hooks.base_hook` | `airflow.hooks.base` |

## GCP Operator Migration

### GCS Operators

**Airflow 1.10.15:**
```python
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator

# Example usage
download_task = GoogleCloudStorageDownloadOperator(
    task_id='download_file',
    bucket='my-bucket',
    object='path/to/file.csv',
    filename='/tmp/file.csv',
    google_cloud_storage_conn_id='google_cloud_default'
)
```

**Airflow 2.X:**
```python
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

# Example usage
download_task = GCSToLocalFilesystemOperator(
    task_id='download_file',
    bucket='my-bucket',
    object_name='path/to/file.csv',
    filename='/tmp/file.csv',
    gcp_conn_id='google_cloud_default'
)
```

Note the parameter changes:
- `google_cloud_storage_conn_id` → `gcp_conn_id`
- `object` → `object_name`

### BigQuery Operators

**Airflow 1.10.15:**
```python
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# Example usage
query_task = BigQueryOperator(
    task_id='execute_query',
    sql='SELECT * FROM `project.dataset.table` LIMIT 1000',
    use_legacy_sql=False,
    destination_dataset_table='project.dataset.destination_table',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='bigquery_default'
)
```

**Airflow 2.X:**
```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Example usage
query_task = BigQueryExecuteQueryOperator(
    task_id='execute_query',
    sql='SELECT * FROM `project.dataset.table` LIMIT 1000',
    use_legacy_sql=False,
    destination_dataset_table='project.dataset.destination_table',
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id='bigquery_default'
)
```

Note the changes:
- Class name: `BigQueryOperator` → `BigQueryExecuteQueryOperator`
- `bigquery_conn_id` → `gcp_conn_id`

### Other GCP Operators

**Airflow 1.10.15:**
```python
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.pubsub_operator import PubSubPublishOperator

# Example usage
dataflow_task = DataFlowPythonOperator(
    task_id='dataflow_python',
    py_file='gs://bucket/dataflow/wordcount.py',
    options={'project': 'my-project'},
    dataflow_default_options={'region': 'us-central1'},
    gcp_conn_id='google_cloud_default'
)
```

**Airflow 2.X:**
```python
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

# Example usage
dataflow_task = DataflowStartPythonJobOperator(
    task_id='dataflow_python',
    py_file='gs://bucket/dataflow/wordcount.py',
    job_args={'project': 'my-project'},
    options={'region': 'us-central1'},
    gcp_conn_id='google_cloud_default'
)
```

Note the changes:
- Class renamed: `DataFlowPythonOperator` → `DataflowStartPythonJobOperator`
- Parameter renamed: `options` → `job_args`
- Parameter renamed: `dataflow_default_options` → `options`

### Custom GCP Operator Example

Below is an example of migrating a custom GCP operator using our project's codebase:

**Airflow 1.10.15:**
```python
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator

class GCSFileExistsOperator(BaseOperator):
    def __init__(
        self,
        bucket_name,
        object_name,
        google_cloud_storage_conn_id='google_cloud_default',
        delegate_to=None,
        *args,
        **kwargs
    ):
        super(GCSFileExistsOperator, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        
    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        
        exists = hook.exists(self.bucket_name, self.object_name)
        return exists
```

**Airflow 2.X:**
```python
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import BaseOperator

class GCSFileExistsOperator(BaseOperator):
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        
    def execute(self, context: Dict) -> bool:
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
        
        exists = hook.exists(bucket_name=self.bucket_name, object_name=self.object_name)
        return exists
```

Key changes:
- `GoogleCloudStorageHook` → `GCSHook`
- `google_cloud_storage_conn_id` → `gcp_conn_id`
- Added type hints
- Use named parameters for hook methods

## HTTP Operator Migration

### SimpleHttpOperator Changes

**Airflow 1.10.15:**
```python
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook

# Example usage
http_task = SimpleHttpOperator(
    task_id='get_api_data',
    method='GET',
    endpoint='api/data',
    data={"param": "value"},
    headers={"Content-Type": "application/json"},
    response_check=lambda response: len(response.json()) > 0,
    http_conn_id='http_api_connection',
    dag=dag
)
```

**Airflow 2.X:**
```python
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook

# Example usage
http_task = SimpleHttpOperator(
    task_id='get_api_data',
    method='GET',
    endpoint='api/data',
    data={"param": "value"},
    headers={"Content-Type": "application/json"},
    response_check=lambda response: len(response.json()) > 0,
    http_conn_id='http_api_connection',
    dag=dag
)
```

The main change here is the import path; the parameters and functionality remain largely the same.

### HTTP Hook Updates

**Airflow 1.10.15:**
```python
from airflow.hooks.http_hook import HttpHook

hook = HttpHook(method='GET', http_conn_id='http_api_connection')
response = hook.run('api/endpoint', data={'param': 'value'})
```

**Airflow 2.X:**
```python
from airflow.providers.http.hooks.http import HttpHook

hook = HttpHook(method='GET', http_conn_id='http_api_connection')
response = hook.run('api/endpoint', data={'param': 'value'})
```

Again, the primary change is the import path.

### Custom HTTP Operator Example

Here's an example of migrating a custom HTTP operator from our codebase:

**Airflow 1.10.15:**
```python
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException

class CustomHttpOperator(SimpleHttpOperator):
    def __init__(
        self,
        endpoint,
        method='GET',
        http_conn_id='http_default',
        response_filter=None,
        *args,
        **kwargs
    ):
        super(CustomHttpOperator, self).__init__(
            endpoint=endpoint,
            method=method,
            http_conn_id=http_conn_id,
            *args,
            **kwargs
        )
        self.response_filter = response_filter
        
    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        
        response = http.run(self.endpoint,
                           self.data,
                           self.headers,
                           self.extra_options)
        
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check failed")
        
        response_data = response.json()
        
        if self.response_filter:
            filtered_data = self.response_filter(response_data)
            return filtered_data
            
        return response_data
```

**Airflow 2.X:**
```python
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
from typing import Dict, Any, Optional, Callable

class CustomHttpOperator(SimpleHttpOperator):
    def __init__(
        self,
        endpoint: str,
        method: str = 'GET',
        http_conn_id: str = 'http_default',
        response_filter: Optional[Callable] = None,
        *args,
        **kwargs
    ):
        super().__init__(
            endpoint=endpoint,
            method=method,
            http_conn_id=http_conn_id,
            *args,
            **kwargs
        )
        self.response_filter = response_filter
        
    def execute(self, context: Dict) -> Any:
        http = HttpHook(method=self.method, http_conn_id=self.http_conn_id)
        
        response = http.run(
            endpoint=self.endpoint,
            data=self.data,
            headers=self.headers,
            extra_options=self.extra_options
        )
        
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check failed")
        
        response_data = response.json()
        
        if self.response_filter:
            filtered_data = self.response_filter(response_data)
            return filtered_data
            
        return response_data
```

Key changes:
- Updated import paths
- Added type hints
- Used keyword arguments explicitly
- Used `super()` with no arguments (Python 3 style)

## Database Operator Migration

### PostgresOperator Changes

**Airflow 1.10.15:**
```python
from airflow.operators.postgres_operator import PostgresOperator

# Example usage
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql='''
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''',
    autocommit=True,
    dag=dag
)
```

**Airflow 2.X:**
```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Example usage
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql='''
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''',
    autocommit=True,
    dag=dag
)
```

The primary change is the import path; the operator's API remains mostly the same.

### SQL Execution Patterns

**Airflow 1.10.15:**
```python
from airflow.hooks.postgres_hook import PostgresHook

def execute_query(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("SELECT * FROM users")
```

**Airflow 2.X:**
```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

def execute_query(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run(sql="SELECT * FROM users")
```

The changes include:
- Updated import path
- Named parameters in method calls (recommended but not required)

### Custom Postgres Operator Example

Here's an example of migrating a custom Postgres operator from our codebase:

**Airflow 1.10.15:**
```python
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

class CustomPostgresOperator(PostgresOperator):
    def __init__(
        self,
        sql,
        postgres_conn_id='postgres_default',
        parameters=None,
        autocommit=False,
        database=None,
        *args,
        **kwargs
    ):
        super(CustomPostgresOperator, self).__init__(
            sql=sql,
            postgres_conn_id=postgres_conn_id,
            parameters=parameters,
            autocommit=autocommit,
            *args,
            **kwargs
        )
        self.database = database
        
    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                database=self.database)
        return self.hook.run(self.sql, self.parameters, self.autocommit)
```

**Airflow 2.X:**
```python
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Dict, List, Optional, Union, Any

class CustomPostgresOperator(PostgresOperator):
    def __init__(
        self,
        sql: str,
        postgres_conn_id: str = 'postgres_default',
        parameters: Optional[Dict] = None,
        autocommit: bool = False,
        database: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(
            sql=sql,
            postgres_conn_id=postgres_conn_id,
            parameters=parameters,
            autocommit=autocommit,
            *args,
            **kwargs
        )
        self.database = database
        
    def execute(self, context: Dict) -> Any:
        self.hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            schema=self.database  # Note: 'database' is now 'schema' in Airflow 2.X
        )
        return self.hook.run(
            sql=self.sql, 
            parameters=self.parameters, 
            autocommit=self.autocommit
        )
```

Key changes:
- Updated import paths
- Added type hints
- Renamed parameter from `database` to `schema` in `PostgresHook`
- Used explicit keyword arguments in method calls

## TaskFlow API Migration

### TaskFlow API Overview

Airflow 2.X introduces the TaskFlow API, which simplifies DAG creation by using Python functions decorated with `@task`. This approach automatically handles dependencies and XComs, reducing boilerplate code.

Benefits of TaskFlow API:
- More Pythonic way to define workflows
- Automatic XCom handling
- Simpler dependency definitions
- Better type checking
- Reduced boilerplate code

### Converting Python Operators

**Airflow 1.10.15:**
```python
from airflow.operators.python_operator import PythonOperator

def process_data(ds, **kwargs):
    # Process data
    return {"result": "processed"}

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)
```

**Airflow 2.X TaskFlow:**
```python
from airflow.decorators import task

@task
def process_data(ds):
    # Process data
    return {"result": "processed"}

# In the DAG context:
process_task = process_data()
```

Key differences:
- No need for `PythonOperator` instantiation
- `@task` decorator automatically creates the task
- No need for `provide_context=True` as context is always provided
- Return values automatically pushed to XCom
- Dependencies can be created using direct function calls

### XCom with TaskFlow

**Airflow 1.10.15:**
```python
from airflow.operators.python_operator import PythonOperator

def extract_data(**kwargs):
    # Extract data
    data = {"extracted": "data"}
    kwargs['ti'].xcom_push(key='extracted_data', value=data)

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='extracted_data')
    # Transform data
    transformed = {"transformed": data}
    ti.xcom_push(key='transformed_data', value=transformed)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task
```

**Airflow 2.X TaskFlow:**
```python
from airflow.decorators import task

@task
def extract_data():
    # Extract data
    return {"extracted": "data"}

@task
def transform_data(extracted_data):
    # Transform data
    return {"transformed": extracted_data}

# In the DAG context:
extracted = extract_data()
transformed = transform_data(extracted)
```

Key differences:
- Return values are automatically pushed to XCom
- Parameters to task functions automatically pull from XCom
- Dependencies are inferred from function calls
- No explicit task_id needed (generated from function name)
- No need for ti.xcom_push/pull calls

### TaskFlow Example DAG

Here's a complete example from our codebase that demonstrates TaskFlow API usage:

```python
import datetime
from airflow.models import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2023, 1, 1)
}

# Define the DAG
with DAG(
    dag_id='example_taskflow_api',
    description='Example DAG demonstrating TaskFlow API in Airflow 2.X',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'taskflow']
) as dag:
    
    # Define start task
    start = DummyOperator(task_id='start')
    
    # Define TaskFlow functions
    @task
    def check_file():
        """Check if a file exists"""
        print("Checking if file exists...")
        return True
    
    @task
    def process_file(file_exists):
        """Process the file if it exists"""
        if not file_exists:
            return {'status': 'skipped'}
            
        print("Processing file...")
        return {'status': 'success', 'records': 100}
    
    @task
    def save_results(processing_results):
        """Save processing results"""
        print(f"Saving results: {processing_results}")
        return True
    
    # Define end task
    end = DummyOperator(task_id='end')
    
    # Define task dependencies
    file_exists = check_file()
    process_results = process_file(file_exists)
    save_success = save_results(process_results)
    
    start >> file_exists >> process_results >> save_success >> end
```

## Parameter Migration

### Renamed Parameters

Several parameters have been renamed in Airflow 2.X for consistency:

| Operator | Airflow 1.10.15 | Airflow 2.X |
|----------|-----------------|-------------|
| All Google Operators | `google_cloud_storage_conn_id` | `gcp_conn_id` |
| All Google Operators | `delegate_to` (still supported) | `impersonation_chain` (new preferred) |
| PostgresHook | `database` | `schema` |
| DataflowOperator | `dataflow_default_options` | `options` |
| DataflowOperator | `options` | `job_args` |
| MsSqlOperator | `mssql_conn_id` | `conn_id` |

### Deprecated Parameters

Some parameters have been deprecated in Airflow 2.X:

| Operator | Deprecated Parameter | Alternative |
|----------|---------------------|-------------|
| PythonOperator | `provide_context` | Context always provided, parameter not needed |
| BaseOperator | `queue_pool` | Use `pool` instead |
| BaseOperator | `queue_retry_delay` | Use `retry_delay` instead |
| DAG | `concurrency` | Use `max_active_tasks` instead |
| DAG | `max_active_runs_per_dag` | Use `max_active_runs` instead |

### New Required Parameters

Some operators have new required parameters in Airflow 2.X:

| Operator | New Required Parameter | Description |
|----------|------------------------|-------------|
| CloudSQLExportInstanceOperator | `project_id` | GCP project ID |
| DataprocSubmitJobOperator | `project_id` | GCP project ID |
| CloudSQLCreateInstanceOperator | `instance_name` | Instance name (was automatically inferred) |

## Testing Migrated Operators

### Compatibility Test Patterns

To ensure operators are correctly migrated, test both the import path and functionality:

```python
def test_operator_import():
    try:
        from airflow.providers.postgres.operators.postgres import PostgresOperator
        # Import successful
        assert True
    except ImportError:
        assert False, "Failed to import PostgresOperator from new path"

def test_operator_functionality():
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    
    operator = PostgresOperator(
        task_id="test",
        sql="SELECT 1",
        postgres_conn_id="postgres_default"
    )
    
    # Test execute method, mocking connections as needed
    # ...
```

### Mock Testing Approaches

Use Airflow's testing utilities to mock connections and hooks:

```python
from unittest import mock
from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook

def test_postgres_operator_execute():
    # Mock the connection and hook
    mock_conn = Connection(
        conn_id='postgres_default',
        conn_type='postgres',
        host='localhost',
        schema='test_db'
    )
    
    with mock.patch.object(
        PostgresHook, 'get_connection', return_value=mock_conn
    ), mock.patch.object(
        PostgresHook, 'run', return_value=None
    ) as mock_run:
        # Create and execute operator
        from airflow.providers.postgres.operators.postgres import PostgresOperator
        
        op = PostgresOperator(
            task_id='test_postgres',
            postgres_conn_id='postgres_default',
            sql='SELECT * FROM test_table'
        )
        
        op.execute(context={})
        
        # Verify the hook's run method was called with expected parameters
        mock_run.assert_called_once_with(
            sql='SELECT * FROM test_table', 
            parameters=None, 
            autocommit=False
        )
```

### Integration Test Strategies

For comprehensive testing, perform integration tests with actual services:

1. Create a test DAG that uses the migrated operators
2. Run the DAG in a local Airflow environment or test container
3. Verify task execution status and results

Example test DAG:
```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    'integration_test_postgres',
    start_date=days_ago(1),
    schedule_interval=None
) as dag:
    
    test_task = PostgresOperator(
        task_id='test_task',
        postgres_conn_id='postgres_test',
        sql='SELECT 1 as test',
        dag=dag
    )
```

## Best Practices

### Code Organization

For migrating operators and maintaining compatibility:

1. Create a compatibility layer if needed:
    ```python
    try:
        # Try Airflow 2.X import
        from airflow.providers.postgres.operators.postgres import PostgresOperator
    except ImportError:
        # Fall back to Airflow 1.X import (for backward compatibility)
        from airflow.operators.postgres_operator import PostgresOperator
    ```

2. Use abstract wrappers:
    ```python
    # my_operators.py
    from typing import Dict, Any
    
    # Import with compatibility handling
    try:
        from airflow.providers.postgres.operators.postgres import PostgresOperator as AirflowPostgresOperator
    except ImportError:
        from airflow.operators.postgres_operator import PostgresOperator as AirflowPostgresOperator
    
    class MyPostgresOperator(AirflowPostgresOperator):
        """Custom PostgreSQL operator that works with both Airflow 1.X and 2.X."""
        
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            
        def execute(self, context: Dict) -> Any:
            return super().execute(context)
    ```

3. Organize custom operators by service:
    ```
    my_project/
    ├── operators/
    │   ├── __init__.py
    │   ├── gcp/
    │   │   ├── __init__.py
    │   │   ├── bigquery.py
    │   │   └── gcs.py
    │   ├── database/
    │   │   ├── __init__.py
    │   │   └── postgres.py
    │   └── http/
    │       ├── __init__.py
    │       └── custom_http.py
    ```

### Error Handling

Enhanced error handling for migrated operators:

```python
from airflow.exceptions import AirflowException
from typing import Dict, Any

def execute(self, context: Dict) -> Any:
    try:
        # Task execution code
        return result
    except Exception as e:
        error_context = {
            'task_id': self.task_id,
            'params': self.params,
            'error': str(e)
        }
        self.log.error(f"Task failed with error: {error_context}")
        # Optional: send alert or notification
        raise AirflowException(f"Task failed: {str(e)}")
```

### Documentation

Add clear documentation for migrated operators:

```python
class MyMigratedOperator(BaseOperator):
    """
    Custom operator for performing specific tasks, compatible with Airflow 2.X.
    
    This operator was migrated from Airflow 1.10.15 to Airflow 2.X.
    
    :param param1: Description of parameter 1
    :type param1: str
    :param param2: Description of parameter 2
    :type param2: int
    
    .. seealso::
        For more information on how to use this operator, see:
        https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/
    """
```

### Compatibility Layers

For gradual migration, consider creating compatibility layers:

```python
# airflow1_compat.py
"""
Compatibility layer for Airflow 1.10.15 operators.
This module provides shims that make Airflow 2.X operators
usable with Airflow 1.10.15 import statements.
"""

# Re-export Airflow 2.X operators with Airflow 1.X import paths
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator as BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator as GoogleCloudStorageToGoogleCloudStorageOperator
```

## Troubleshooting

### Common Import Errors

| Error | Solution |
|-------|----------|
| `ImportError: No module named 'airflow.providers.google'` | Install the required provider packages: `pip install apache-airflow-providers-google` |
| `ImportError: cannot import name 'BigQueryOperator'` | Replace with new class name: `from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator` |
| `ImportError: No module named 'airflow.operators.python_operator'` | Update import path: `from airflow.operators.python import PythonOperator` |

### Parameter Handling Issues

| Issue | Solution |
|-------|----------|
| `TypeError: __init__() got an unexpected keyword argument 'provide_context'` | Remove the `provide_context` parameter - context is always provided in Airflow 2.X |
| `TypeError: __init__() got an unexpected keyword argument 'bigquery_conn_id'` | Replace with `gcp_conn_id` |
| `TypeError: PostgresHook.__init__() got an unexpected keyword argument 'database'` | Replace `database` with `schema` |

### Runtime Behavior Differences

| Issue | Solution |
|-------|----------|
| XCom values not available | Check if `do_xcom_push` is set to `True` (default in Airflow 2.X) |
| Task fails with context key error | Make sure callback functions accept a `context` parameter |
| Connections not found | Verify connection IDs are correct and provider packages are installed |
| Different retry behavior | Review the retry policy parameters and update as needed |

## Migration Checklist

### Pre-Migration Tasks

- [ ] Create a detailed inventory of all operators used in your DAGs
- [ ] Identify and list all custom operators
- [ ] Document current import paths and parameter usage
- [ ] Ensure all needed provider packages are identified
- [ ] Set up a test environment with Airflow 2.X
- [ ] Create a backup of all DAG files

### Migration Steps

1. Update imports:
   - [ ] Replace deprecated import paths with new provider package paths
   - [ ] Update hook import paths

2. Update operator parameters:
   - [ ] Remove deprecated parameters
   - [ ] Rename parameters that have changed
   - [ ] Add new required parameters

3. Update task execution:
   - [ ] Modify XCom handling if needed
   - [ ] Update callback functions to accept context
   - [ ] Consider converting appropriate tasks to TaskFlow API

4. Testing:
   - [ ] Run unit tests for each migrated operator
   - [ ] Perform integration tests with actual connections
   - [ ] Test DAGs in a staging environment

5. Documentation:
   - [ ] Update operator docstrings
   - [ ] Document any compatibility layers created
   - [ ] Create migration notes for teammates

### Post-Migration Validation

- [ ] Verify all DAGs load successfully
- [ ] Check operator execution with various inputs
- [ ] Validate XCom behavior between tasks
- [ ] Monitor task duration and resource usage
- [ ] Review error handling and retry behavior
- [ ] Confirm connections are working properly

## Reference: Operator Migration Examples

### GCP Operator Examples

**GoogleCloudStorageToBigQueryOperator:**

```python
# Airflow 1.10.15
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

load_data = GoogleCloudStorageToBigQueryOperator(
    task_id='load_gcs_to_bq',
    bucket='example-bucket',
    source_objects=['path/to/data.csv'],
    destination_project_dataset_table='project.dataset.table',
    schema_fields=[
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    source_format='CSV',
    skip_leading_rows=1,
    bigquery_conn_id='bigquery_default',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)
```

```python
# Airflow 2.X
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

load_data = GCSToBigQueryOperator(
    task_id='load_gcs_to_bq',
    bucket='example-bucket',
    source_objects=['path/to/data.csv'],
    destination_project_dataset_table='project.dataset.table',
    schema_fields=[
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    source_format='CSV',
    skip_leading_rows=1,
    gcp_conn_id='google_cloud_default',
    dag=dag
)
```

### HTTP Operator Examples

**SimpleHttpOperator:**

```python
# Airflow 1.10.15
from airflow.operators.http_operator import SimpleHttpOperator
import json

get_data = SimpleHttpOperator(
    task_id='get_api_data',
    http_conn_id='http_api',
    endpoint='api/v1/data',
    method='GET',
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.status_code == 200,
    xcom_push=True,
    log_response=True,
    dag=dag
)
```

```python
# Airflow 2.X
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

get_data = SimpleHttpOperator(
    task_id='get_api_data',
    http_conn_id='http_api',
    endpoint='api/v1/data',
    method='GET',
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.status_code == 200,
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
    dag=dag
)
```

### PostgreSQL Operator Examples

**PostgresOperator:**

```python
# Airflow 1.10.15
from airflow.operators.postgres_operator import PostgresOperator

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql='''
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        email VARCHAR UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''',
    autocommit=True,
    database='airflow',
    dag=dag
)
```

```python
# Airflow 2.X
from airflow.providers.postgres.operators.postgres import PostgresOperator

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql='''
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        email VARCHAR UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''',
    autocommit=True,
    schema='airflow',  # Changed from 'database' to 'schema'
    dag=dag
)
```

### TaskFlow API Examples

**Traditional Python Operator:**

```python
# Airflow 1.10.15
from airflow.operators.python_operator import PythonOperator

def extract_data(**kwargs):
    ti = kwargs['ti']
    data = {"source": "api", "data": [1, 2, 3]}
    ti.xcom_push(key='extracted_data', value=data)
    return "Extraction completed"

def transform_data(**kwargs):
    ti = kwargs['ti']
    extracted = ti.xcom_pull(task_ids='extract_data', key='extracted_data')
    transformed = {
        "source": extracted["source"],
        "data": [x * 2 for x in extracted["data"]],
        "processed_at": kwargs['execution_date'].isoformat()
    }
    ti.xcom_push(key='transformed_data', value=transformed)
    return "Transformation completed"

def load_data(**kwargs):
    ti = kwargs['ti']
    transformed = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    # Load data to destination
    return f"Loaded {len(transformed['data'])} records"

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

extract >> transform >> load
```

```python
# Airflow 2.X with TaskFlow API
from airflow.decorators import task

@task
def extract_data():
    return {"source": "api", "data": [1, 2, 3]}

@task
def transform_data(extracted, execution_date):
    return {
        "source": extracted["source"],
        "data": [x * 2 for x in extracted["data"]],
        "processed_at": execution_date.isoformat()
    }

@task
def load_data(transformed):
    # Load data to destination
    return f"Loaded {len(transformed['data'])} records"

# In the DAG context:
extracted = extract_data()
transformed = transform_data(extracted, "{{ execution_date }}")
loaded = load_data(transformed)

# Dependencies are automatically created
```