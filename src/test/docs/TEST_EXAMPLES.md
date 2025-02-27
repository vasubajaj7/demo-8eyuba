# Testing Apache Airflow 2.X Components

## Introduction

This document provides comprehensive examples, best practices, and strategies for testing Apache Airflow 2.X components during and after migration from Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2. Effective testing is crucial for ensuring the successful migration of existing workflows while maintaining functionality and business logic.

### Testing Objectives

- Verify DAG integrity and functionality in Airflow 2.X
- Validate operator compatibility after migration
- Ensure hook and sensor functionality
- Test TaskFlow API implementations
- Measure and compare performance between versions
- Validate integration with Google Cloud services
- Identify and fix migration issues early

### Testing Tools

- **pytest**: Primary testing framework
- **pytest-airflow**: Plugin for Airflow-specific testing
- **moto**: AWS service mocking
- **unittest.mock**: Python mocking library
- **Airflow CLI**: Command-line testing capabilities

## Test Environment Setup

### Local Development Environment

#### Setting Up Local Airflow 2.X for Testing

```bash
# Create a virtual environment
python -m venv airflow-test-env
source airflow-test-env/bin/activate  # Linux/Mac
# or 
# airflow-test-env\Scripts\activate  # Windows

# Install Airflow 2.X
pip install "apache-airflow==2.5.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.1/constraints-3.8.txt"

# Install testing tools
pip install pytest pytest-cov moto

# Install GCP providers
pip install apache-airflow-providers-google==8.8.0

# Initialize Airflow database
export AIRFLOW_HOME=$(pwd)/airflow-test
airflow db init
```

#### Docker-based Test Environment

```bash
# Create docker-compose.yml
cat > docker-compose.yaml << 'EOL'
version: '3'
services:
  airflow:
    image: apache/airflow:2.5.1
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
    volumes:
      - ./dags:/opt/airflow/dags
      - ./tests:/opt/airflow/tests
      - ./plugins:/opt/airflow/plugins
    ports:
      - "8080:8080"
    command: >
      bash -c "airflow db init && 
               airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
               airflow webserver -p 8080"
EOL

# Start the container
docker-compose up -d
```

### Setting Up pytest.ini

```
# pytest.ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = --verbose
markers =
    unit: mark test as a unit test
    integration: mark test as an integration test
    dag_structure: tests that verify DAG structure
    operator: tests for operators
    hook: tests for hooks
    sensor: tests for sensors
    taskflow: tests for TaskFlow API
    migration: tests for migration compatibility
```

### Test Directory Structure

```
tests/
├── conftest.py                # Common fixtures
├── dags/                      # DAG test files
│   ├── test_dag_structure.py
│   └── test_dag_execution.py
├── operators/                 # Operator test files
│   ├── test_custom_operators.py
│   └── test_operator_migration.py
├── hooks/                     # Hook test files
│   └── test_custom_hooks.py
└── integration/               # Integration test files
    ├── test_end_to_end.py
    └── test_performance.py
```

## DAG Testing Examples

### Testing DAG Integrity and Structure

```python
import pytest
from airflow.models import DagBag

def test_dag_integrity():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    assert 'example_dag_basic' in dag_bag.dags, "example_dag_basic not found in DAGs"
    dag = dag_bag.dags['example_dag_basic']
    assert len(dag.tasks) == 3, "DAG should have 3 tasks"
```

### Testing DAG Task Dependencies

```python
def test_dag_task_dependencies():
    dag_bag = DagBag()
    dag = dag_bag.dags['example_dag_basic']
    
    # Get tasks
    task1 = dag.get_task('extract')
    task2 = dag.get_task('transform')
    task3 = dag.get_task('load')
    
    # Test dependencies
    upstream_task_ids = list(map(lambda task: task.task_id, task2.upstream_list))
    assert 'extract' in upstream_task_ids
    
    downstream_task_ids = list(map(lambda task: task.task_id, task2.downstream_list))
    assert 'load' in downstream_task_ids
```

### Testing DAG Parameters

```python
def test_dag_default_args():
    dag_bag = DagBag()
    dag = dag_bag.dags['example_dag_basic']
    
    # Test default args
    assert dag.default_args['owner'] == 'airflow'
    assert dag.default_args['retries'] == 3
    assert 'retry_delay' in dag.default_args
    
    # Test DAG properties
    assert dag.schedule_interval == '@daily'
    assert not dag.catchup
```

### Testing for DAG ID Conventions

```python
def test_dag_id_convention():
    dag_bag = DagBag()
    for dag_id, dag in dag_bag.dags.items():
        # Enforce naming convention
        assert dag_id.islower(), f"DAG ID {dag_id} should be lowercase"
        assert '_' in dag_id, f"DAG ID {dag_id} should use underscores"
        assert not dag_id.startswith('test_'), f"DAG ID {dag_id} should not start with 'test_'"
```

## Operator Testing Examples

### Testing Standard Operators

```python
import pytest
from airflow.operators.python import PythonOperator
from airflow.models import DagBag, TaskInstance
from airflow.utils.dates import days_ago
import datetime

def test_python_operator():
    dag_bag = DagBag()
    dag = dag_bag.dags['example_dag_basic']
    
    def _callable():
        return "success"
    
    task = PythonOperator(
        task_id='test_python_operator',
        python_callable=_callable,
        dag=dag
    )
    
    execution_date = days_ago(1)
    ti = TaskInstance(task=task, execution_date=execution_date)
    result = ti.run()
    
    assert ti.state == 'success'
    assert ti.xcom_pull() == "success"
```

### Testing Custom Operators

```python
from plugins.operators.custom_gcp_operator import CustomGCPOperator
from unittest.mock import patch, MagicMock

def test_custom_gcp_operator():
    # Mock the GCP client
    with patch('plugins.operators.custom_gcp_operator.CustomGCPHook') as mock_hook:
        # Configure the mock
        mock_hook_instance = MagicMock()
        mock_hook.return_value = mock_hook_instance
        mock_hook_instance.get_conn.return_value = MagicMock()
        
        # Create operator instance
        operator = CustomGCPOperator(
            task_id='test_custom_gcp',
            gcp_conn_id='google_cloud_default',
            project_id='test-project',
            bucket_name='test-bucket',
            file_path='path/to/file.txt'
        )
        
        # Test execute method
        result = operator.execute(context={})
        
        # Assert hook was called correctly
        mock_hook.assert_called_once_with(gcp_conn_id='google_cloud_default')
        mock_hook_instance.upload_file.assert_called_once()
```

### Testing Operator Migration

```python
import pytest
from airflow.models import DagBag
from airflow.operators.python import PythonOperator
from plugins.operators.custom_gcp_operator import CustomGCPOperator

def test_operator_migration():
    dag_bag = DagBag()
    dag = dag_bag.dags['etl_main']
    
    # Check for deprecated operators
    for task in dag.tasks:
        # Verify no deprecated operators are used
        assert not isinstance(task, DeprecatedOperator), f"Task {task.task_id} uses deprecated operator"
        
        # Check custom operator compatibility
        if isinstance(task, CustomGCPOperator):
            assert hasattr(task, 'execute'), "Operator missing execute method"
            assert task.gcp_conn_id == 'google_cloud_default', "Incorrect connection ID"
```

### Testing Operator Parameters

```python
def test_operator_parameters():
    dag_bag = DagBag()
    dag = dag_bag.dags['example_dag_basic']
    
    # Get a specific task
    task = dag.get_task('extract')
    
    # Test task parameters
    assert task.retries == 3
    assert task.retry_delay == datetime.timedelta(minutes=5)
    assert task.email_on_failure is True
```

## Hook Testing Examples

### Testing Standard Hooks

```python
import pytest
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from unittest.mock import patch, MagicMock

def test_gcs_hook():
    # Mock the actual GCP client 
    with patch('airflow.providers.google.cloud.hooks.gcs.GCSHook._get_credentials') as mock_creds, \
         patch('airflow.providers.google.cloud.hooks.gcs.storage') as mock_storage:
         
        # Set up mocks
        mock_creds.return_value = MagicMock()
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        
        # Create and test the hook
        hook = GCSHook(gcp_conn_id='google_cloud_default')
        hook.upload('test-bucket', 'source_path', 'destination_path')
        
        # Verify calls
        mock_client.bucket.assert_called_once_with('test-bucket')
        mock_bucket.blob.assert_called_once_with('destination_path')
        mock_blob.upload_from_filename.assert_called_once_with('source_path')
```

### Testing Custom Hooks

```python
from plugins.hooks.custom_hook import CustomHook
import pytest
from unittest.mock import patch, MagicMock

def test_custom_hook():
    # Setup connection
    with patch.object(BaseHook, 'get_connection') as mock_get_connection:
        mock_conn = MagicMock()
        mock_conn.host = 'test-host'
        mock_conn.login = 'test-user'
        mock_conn.password = 'test-password'
        mock_get_connection.return_value = mock_conn
        
        # Create the hook
        hook = CustomHook(conn_id='custom_conn')
        
        # Test hook methods
        with patch.object(hook, '_get_client') as mock_client:
            mock_client_instance = MagicMock()
            mock_client.return_value = mock_client_instance
            
            result = hook.run_query('SELECT * FROM table')
            
            # Assert client was called with proper parameters
            mock_client.assert_called_once()
            mock_client_instance.execute_query.assert_called_once_with('SELECT * FROM table')
```

### Testing Connection Handling

```python
def test_hook_connection_error():
    # Test connection error handling
    with patch.object(BaseHook, 'get_connection') as mock_get_connection:
        mock_get_connection.side_effect = Exception("Connection error")
        
        # Test connection error is handled
        with pytest.raises(Exception) as exc:
            hook = CustomHook(conn_id='custom_conn')
            hook.get_conn()
            
        assert "Connection error" in str(exc.value)
```

## Sensor Testing Examples

### Testing Standard Sensors

```python
import pytest
from airflow.sensors.filesystem import FileSensor
from airflow.models import TaskInstance
from airflow.utils.dates import days_ago
from unittest.mock import patch, MagicMock

def test_file_sensor():
    # Create a FileSensor
    sensor = FileSensor(
        task_id='test_file_sensor',
        filepath='/tmp/test_file.txt',
        poke_interval=1,
        timeout=5
    )
    
    # Test with file existing
    with patch('airflow.sensors.filesystem.os.path.exists') as mock_exists:
        mock_exists.return_value = True
        
        execution_date = days_ago(1)
        ti = TaskInstance(task=sensor, execution_date=execution_date)
        result = ti.run()
        
        assert ti.state == 'success'
        
    # Test with file not existing
    with patch('airflow.sensors.filesystem.os.path.exists') as mock_exists:
        mock_exists.return_value = False
        
        execution_date = days_ago(1)
        ti = TaskInstance(task=sensor, execution_date=execution_date)
        
        # This should time out and raise an exception
        with pytest.raises(Exception):
            result = ti.run()
```

### Testing Custom Sensors

```python
from plugins.sensors.custom_gcs_sensor import CustomGCSSensor
from unittest.mock import patch, MagicMock

def test_custom_gcs_sensor():
    # Create the sensor
    sensor = CustomGCSSensor(
        task_id='test_custom_sensor',
        bucket='test-bucket',
        object='test/path/file.csv',
        google_cloud_conn_id='google_cloud_default',
        poke_interval=1,
        timeout=5
    )
    
    # Test poke method
    with patch('plugins.sensors.custom_gcs_sensor.GCSHook') as mock_hook:
        # Configure the mock
        mock_hook_instance = MagicMock()
        mock_hook.return_value = mock_hook_instance
        
        # Test with file existing
        mock_hook_instance.exists.return_value = True
        assert sensor.poke(context={}) is True
        
        # Test with file not existing
        mock_hook_instance.exists.return_value = False
        assert sensor.poke(context={}) is False
        
        # Verify hook was called correctly
        mock_hook.assert_called_with(google_cloud_conn_id='google_cloud_default')
        mock_hook_instance.exists.assert_called_with('test-bucket', 'test/path/file.csv')
```

### Testing Sensor Timeouts

```python
def test_sensor_timeout():
    # Create sensor with short timeout
    sensor = CustomGCSSensor(
        task_id='test_timeout_sensor',
        bucket='test-bucket',
        object='test/path/file.csv',
        google_cloud_conn_id='google_cloud_default',
        poke_interval=1,
        timeout=2  # Very short timeout
    )
    
    # Test poke method
    with patch('plugins.sensors.custom_gcs_sensor.GCSHook') as mock_hook:
        # Configure the mock to always return False (file not found)
        mock_hook_instance = MagicMock()
        mock_hook.return_value = mock_hook_instance
        mock_hook_instance.exists.return_value = False
        
        # Execute the sensor (should timeout)
        execution_date = days_ago(1)
        ti = TaskInstance(task=sensor, execution_date=execution_date)
        
        with pytest.raises(Exception) as exc_info:
            ti.run()
            
        assert "Sensor timed out" in str(exc_info.value)
```

## TaskFlow API Testing Examples

### Testing TaskFlow Basics

```python
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import DagBag

def test_taskflow_dag_structure():
    dag_bag = DagBag()
    dag = dag_bag.dags['example_taskflow_dag']
    
    # Check tasks are correctly created
    task_ids = [task.task_id for task in dag.tasks]
    assert 'extract_data' in task_ids
    assert 'process_data' in task_ids
    assert 'load_data' in task_ids
    
    # Check dependencies are correctly set
    process_task = dag.get_task('process_data')
    upstream_tasks = [task.task_id for task in process_task.upstream_list]
    downstream_tasks = [task.task_id for task in process_task.downstream_list]
    
    assert 'extract_data' in upstream_tasks
    assert 'load_data' in downstream_tasks
```

### Testing TaskFlow Execution

```python
import pytest
from airflow.models import DagBag
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.models import TaskInstance, XCom

def test_taskflow_execution():
    dag_bag = DagBag()
    dag = dag_bag.dags['example_dag_taskflow']
    execution_date = dag.start_date
    
    # Execute DAG
    dag.clear(start_date=execution_date, end_date=execution_date)
    dag.run(start_date=execution_date, end_date=execution_date, donot_pickle=True)
    
    # Check XCom results
    with create_session() as session:
        tis = session.query(
            TaskInstance
        ).filter(
            TaskInstance.dag_id == 'example_dag_taskflow',
            TaskInstance.execution_date == execution_date
        ).all()
        
        assert len(tis) > 0, "No task instances found"
        for ti in tis:
            assert ti.state == State.SUCCESS, f"Task {ti.task_id} failed"
        
        # Check XCom for decorated task
        xcoms = session.query(XCom).filter(
            XCom.dag_id == 'example_dag_taskflow',
            XCom.execution_date == execution_date,
            XCom.task_id == 'process_data'
        ).all()
        
        assert len(xcoms) > 0, "No XComs found for process_data task"
        assert xcoms[0].value is not None, "XCom value should not be None"
```

### Testing TaskFlow with Complex Dependencies

```python
def test_taskflow_complex_dependencies():
    dag_bag = DagBag()
    dag = dag_bag.dags['example_complex_taskflow']
    
    # Test branching logic
    branch_task = dag.get_task('branch_task')
    downstream_task_ids = [task.task_id for task in branch_task.downstream_list]
    
    assert 'path_a' in downstream_task_ids
    assert 'path_b' in downstream_task_ids
    
    # Test task groups
    task_group_tasks = [task for task in dag.tasks if task.task_id.startswith('group_tasks')]
    assert len(task_group_tasks) > 0, "Task group tasks not found"
```

## Migration Testing Examples

### Testing API Changes

```python
def test_api_changes():
    # Test for proper imports
    # In Airflow 1.10.15, many objects were in different modules
    
    # Test Airflow 2.X imports
    from airflow.operators.python import PythonOperator
    from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    
    # These should all load without errors
    assert PythonOperator is not None
    assert BigQueryExecuteQueryOperator is not None
    assert GCSToBigQueryOperator is not None
```

### Testing Deprecated Features

```python
from airflow.models import DagBag
import pytest

def test_deprecated_features():
    dag_bag = DagBag()
    
    # Check for DAGs using deprecated features
    for dag_id, dag in dag_bag.dags.items():
        # Check if any DAG still uses legacy config
        assert not hasattr(dag, 'user_defined_macros') or dag.user_defined_macros is None, \
            f"DAG {dag_id} uses deprecated 'user_defined_macros'"
        
        # Check for deprecated parameters in operators
        for task in dag.tasks:
            # Example: queue is now deprecated in BashOperator
            if hasattr(task, 'queue'):
                assert task.queue is None, f"Task {task.task_id} in DAG {dag_id} uses deprecated 'queue' parameter"
```

### Comparing Behavior Between Versions

```python
import pytest
import subprocess
import json

def test_behavior_comparison():
    # This test executes a DAG in both Airflow 1.x and 2.x environments
    # and compares the results
    
    # Run Airflow 1.x test (assuming it's in a separate environment)
    airflow1_result = subprocess.run(
        ["airflow1-env/bin/airflow", "dags", "test", "example_dag", "2023-01-01"],
        capture_output=True, text=True
    )
    
    # Run Airflow 2.x test
    airflow2_result = subprocess.run(
        ["airflow", "dags", "test", "example_dag", "2023-01-01"],
        capture_output=True, text=True
    )
    
    # Compare task states
    airflow1_states = extract_task_states(airflow1_result.stdout)
    airflow2_states = extract_task_states(airflow2_result.stdout)
    
    # Tasks should have the same success/failure status
    for task_id in airflow1_states:
        assert task_id in airflow2_states, f"Task {task_id} missing in Airflow 2.x"
        assert airflow1_states[task_id] == airflow2_states[task_id], \
            f"Task {task_id} has different state in Airflow 2.x"
```

## Integration Testing Examples

### End-to-End Testing

```python
import pytest
from airflow.models import DagBag
from airflow.utils.session import create_session
from airflow.utils.state import State
import datetime

def test_end_to_end_workflow():
    dag_bag = DagBag()
    dag = dag_bag.dags['etl_main']
    execution_date = datetime.datetime(2023, 1, 1)
    
    # Clear previous runs
    dag.clear(start_date=execution_date, end_date=execution_date)
    
    # Execute the DAG
    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date,
        run_type="manual"
    )
    
    # Let DAG run to completion
    dag.run(start_date=execution_date, end_date=execution_date)
    
    # Check all tasks succeeded
    with create_session() as session:
        tis = dag_run.get_task_instances(session=session)
        
        for ti in tis:
            assert ti.state == State.SUCCESS, f"Task {ti.task_id} did not succeed"
        
        # Check expected output data
        # This part depends on your specific workflow output
        # Example: Check if output file exists in a bucket
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        assert gcs_hook.exists('output-bucket', f'data/output_{execution_date.strftime("%Y%m%d")}.csv'), \
            "Output file not found"
```

### Testing External Dependencies

```python
import pytest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag
import datetime

def test_external_dependencies():
    # Mock external services
    with patch('plugins.hooks.custom_api_hook.requests.get') as mock_get, \
         patch('airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service') as mock_bq:
        
        # Configure mocks
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test_data"}
        mock_get.return_value = mock_response
        
        mock_bq_service = MagicMock()
        mock_bq.return_value = mock_bq_service
        
        # Run the DAG
        dag_bag = DagBag()
        dag = dag_bag.dags['api_to_bigquery']
        execution_date = datetime.datetime(2023, 1, 1)
        
        dag.clear(start_date=execution_date, end_date=execution_date)
        dag.run(start_date=execution_date, end_date=execution_date)
        
        # Verify external API was called
        mock_get.assert_called()
        
        # Verify BigQuery service was used
        mock_bq.assert_called()
```

## Performance Testing Examples

### DAG Parsing Performance

```python
import pytest
import time
from airflow.models import DagBag

def test_dag_parsing_performance():
    start_time = time.time()
    dag_bag = DagBag()
    end_time = time.time()
    
    # Verify DAG parsing time is within acceptable limits
    parsing_time = end_time - start_time
    print(f"DAG parsing time: {parsing_time} seconds")
    assert parsing_time < 30, f"DAG parsing time ({parsing_time} seconds) exceeded 30 seconds"
    
    # Check for specific DAGs
    assert 'etl_main' in dag_bag.dags, "etl_main DAG not found"
    assert 'data_sync' in dag_bag.dags, "data_sync DAG not found"
    assert 'reports_gen' in dag_bag.dags, "reports_gen DAG not found"
```

### Task Execution Performance

```python
import pytest
import time
from airflow.models import DagBag, TaskInstance
from airflow.utils.dates import days_ago

def test_task_execution_performance():
    dag_bag = DagBag()
    dag = dag_bag.dags['etl_main']
    
    # Get a specific compute-intensive task
    task = dag.get_task('process_large_dataset')
    
    # Measure execution time
    execution_date = days_ago(1)
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    start_time = time.time()
    ti.run()
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"Task execution time: {execution_time} seconds")
    
    # Compare against expected performance threshold
    assert execution_time < 60, f"Task execution time ({execution_time} seconds) exceeded 60 seconds"
```

### Resource Utilization Testing

```python
import pytest
import psutil
import threading
import time
from airflow.models import DagBag

def monitor_resources(duration, interval=0.1):
    """Monitor CPU and memory usage for the specified duration."""
    cpu_readings = []
    memory_readings = []
    
    end_time = time.time() + duration
    while time.time() < end_time:
        cpu_readings.append(psutil.cpu_percent())
        memory_readings.append(psutil.virtual_memory().percent)
        time.sleep(interval)
    
    return {
        'cpu_avg': sum(cpu_readings) / len(cpu_readings),
        'cpu_max': max(cpu_readings),
        'memory_avg': sum(memory_readings) / len(memory_readings),
        'memory_max': max(memory_readings)
    }

def test_dag_resource_utilization():
    # Start resource monitoring in a separate thread
    monitor_thread = threading.Thread(
        target=lambda: monitor_resources(30),
        daemon=True
    )
    monitor_results = {}
    
    monitor_thread.start()
    
    # Execute the DAG
    dag_bag = DagBag()
    dag = dag_bag.dags['heavy_processing_dag']
    execution_date = days_ago(1)
    dag.clear(start_date=execution_date, end_date=execution_date)
    dag.run(start_date=execution_date, end_date=execution_date)
    
    monitor_thread.join()
    
    # Check resource utilization is within acceptable limits
    assert monitor_results['cpu_max'] < 80, f"CPU usage too high: {monitor_results['cpu_max']}%"
    assert monitor_results['memory_max'] < 80, f"Memory usage too high: {monitor_results['memory_max']}%"
```

## Test Fixtures and Utilities

### Common Test Fixtures

```python
# conftest.py
import pytest
from airflow.models import DagBag, DAG, Connection
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session
import datetime
import os
import tempfile

@pytest.fixture(scope="session")
def dag_bag():
    """Returns a DagBag for testing."""
    return DagBag()

@pytest.fixture(scope="function")
def session():
    """Creates a SQLAlchemy session for testing."""
    with create_session() as session:
        yield session

@pytest.fixture
def test_dag():
    """Creates a test DAG for testing."""
    dag = DAG(
        'test_dag',
        default_args={
            'owner': 'airflow',
            'start_date': days_ago(1),
            'retries': 1,
        },
        schedule_interval=None,
        catchup=False
    )
    return dag

@pytest.fixture
def mock_gcp_connection():
    """Creates a mock GCP connection."""
    with create_session() as session:
        conn = Connection(
            conn_id='google_cloud_default',
            conn_type='google_cloud_platform',
            extra='{"project": "test-project", "keyfile_json": "{}"}'
        )
        session.add(conn)
        session.commit()
        
        yield conn
        
        # Clean up
        session.delete(conn)
        session.commit()
```

### Testing Utility Functions

```python
# test_utils.py
import os
import tempfile
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
import datetime

def run_task(task, execution_date=None):
    """Helper function to run a task and return the result."""
    if execution_date is None:
        execution_date = days_ago(1)
    
    ti = TaskInstance(task=task, execution_date=execution_date)
    return ti.run()

def create_temp_file(content):
    """Create a temporary file with the given content."""
    temp_dir = tempfile.mkdtemp()
    temp_file = os.path.join(temp_dir, 'test_file.txt')
    
    with open(temp_file, 'w') as f:
        f.write(content)
    
    return temp_file, temp_dir

def cleanup_temp_files(file_path, dir_path):
    """Clean up temporary files."""
    if os.path.exists(file_path):
        os.remove(file_path)
    
    if os.path.exists(dir_path):
        os.rmdir(dir_path)
```

## Best Practices

### Test Isolation

Always ensure tests are isolated from each other and can run independently. Use fixtures to set up and tear down test environments.

```python
@pytest.fixture(autouse=True)
def isolate_db():
    """Reset the database for each test."""
    from airflow.utils.db import resetdb
    resetdb()
    yield
```

### Mocking External Dependencies

Always mock external APIs, services and databases to make tests reliable and fast:

```python
# Example of mocking BigQuery
from unittest.mock import patch, MagicMock

@patch('airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service')
def test_bigquery_operator(mock_get_service):
    mock_service = MagicMock()
    mock_get_service.return_value = mock_service
    
    # Configure the mock service
    mock_service.jobs().insert().execute.return_value = {'jobReference': {'jobId': 'test_job_id'}}
    mock_service.jobs().get().execute.return_value = {'status': {'state': 'DONE'}}
    
    # Test your operator that uses BigQuery
```

### Test Coverage

Aim for comprehensive test coverage of:

1. All DAGs
2. All Custom Operators
3. All Custom Hooks
4. All Custom Sensors
5. Migration-specific components
6. TaskFlow API usage

Use pytest-cov to measure and enforce coverage:

```bash
pytest --cov=dags --cov=plugins --cov-report=term-missing
```

### Testing Sensitive Operations

For operations that might incur costs or affect production systems:

1. Always mock external API calls
2. Use the `--dry-run` option when available
3. Create separate test environments
4. Use test fixtures that simulate the response

```python
def test_gcp_cost_sensitive_operation():
    # Mock the actual service
    with patch('google.cloud.storage.Client') as mock_client:
        # Test your code that would normally call GCP
        # ...
        
        # Verify the mock was called with correct parameters
        # but no actual API calls were made
```

## Troubleshooting

### Common Testing Issues

| Issue | Solution |
|-------|----------|
| DAGs not found in DagBag | Check PYTHONPATH and AIRFLOW_HOME environment variables |
| Tests hanging | Look for infinite loops or timeouts in sensors |
| ModuleNotFoundError | Ensure requirements are installed in test environment |
| Permission errors with GCP | Use mock services instead of real API calls |
| Random test failures | Check for test isolation issues or race conditions |
| XCom data missing | Use test context properly to access XCom data |

### Debugging Failed Tests

```python
import logging

def test_with_debug_logging():
    # Set up debug logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('airflow.task')
    logger.setLevel(logging.DEBUG)
    
    # Run your test
    dag_bag = DagBag()
    dag = dag_bag.dags['example_dag']
    # ... rest of test
```

### Environment Troubleshooting

If you encounter issues with your test environment:

1. Check Airflow version compatibility:
   ```bash
   airflow version
   ```

2. Verify database connection:
   ```bash
   airflow db check
   ```

3. Test provider package installation:
   ```bash
   pip list | grep airflow-providers
   ```

4. Verify DAG discovery:
   ```bash
   airflow dags list
   ```

5. Reset the test database if needed:
   ```bash
   airflow db reset -y
   ```