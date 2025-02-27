#!/usr/bin/env python3
"""
Centralized pytest configuration file for Airflow 2.X migration project that provides fixtures,
test environment setup, and utilities for mocking GCP services, database connections, and
Airflow components required for testing the migration from Airflow 1.10.15 to Airflow 2.X.
"""

# Standard library imports
import os  # Python standard library
import sys  # Python standard library
import logging  # Python standard library
import tempfile  # Python standard library
import shutil  # Python standard library
import json  # Python standard library
from typing import Dict, List, Optional, Any

# Third-party imports
import pytest  # version 6.0+
import pendulum  # version 2.0+
from unittest.mock import MagicMock, patch  # Python standard library

# Airflow imports
import airflow  # version 2.X

# Internal imports
from src.test.utils.airflow2_compatibility_utils import (
    is_airflow2,
    mock_airflow2_imports,
    Airflow2CompatibilityTestMixin
)
from src.test.fixtures.mock_gcp_services import (
    create_mock_storage_client,
    create_mock_secret_manager_client,
    create_mock_bigquery_client,
    patch_gcp_services
)

# Global variables
AIRFLOW_HOME = os.path.join(tempfile.gettempdir(), 'airflow_home')
DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')
DEFAULT_DATE = pendulum.datetime(2023, 1, 1, tz='UTC')
DEFAULT_TASK_ARGS = {
    "owner": "airflow",
    "start_date": DEFAULT_DATE,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5)
}
logger = logging.getLogger('airflow.test')


def pytest_configure(config):
    """
    Configure pytest environment before test collection
    
    Args:
        config: pytest configuration object
    """
    # Register custom markers
    config.addinivalue_line("markers", "unit: mark a test as a unit test")
    config.addinivalue_line("markers", "integration: mark a test as an integration test")
    config.addinivalue_line("markers", "migration: mark a test as a migration-specific test")
    config.addinivalue_line("markers", "airflow1: mark a test for Airflow 1.X compatibility")
    config.addinivalue_line("markers", "airflow2: mark a test for Airflow 2.X compatibility")
    
    # Set up environment variables
    os.environ['AIRFLOW_HOME'] = AIRFLOW_HOME
    os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = DAGS_FOLDER
    
    # Configure logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    
    # Create necessary directories
    os.makedirs(AIRFLOW_HOME, exist_ok=True)
    os.makedirs(DAGS_FOLDER, exist_ok=True)
    os.makedirs(os.path.join(AIRFLOW_HOME, 'plugins'), exist_ok=True)
    os.makedirs(os.path.join(AIRFLOW_HOME, 'logs'), exist_ok=True)


def reset_db():
    """Reset the Airflow database to a clean state"""
    try:
        # Import airflow's resetdb utility
        from airflow.utils.db import resetdb
        
        logger.info("Resetting Airflow database...")
        resetdb()
        logger.info("Database reset complete")
    except Exception as e:
        logger.error(f"Failed to reset database: {str(e)}")
        raise


def setup_airflow_environment():
    """Set up Airflow environment variables for testing"""
    # Configure Airflow for testing
    os.environ['AIRFLOW__CORE__UNIT_TEST_MODE'] = 'True'
    os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'
    os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = DAGS_FOLDER
    os.environ['AIRFLOW__CORE__PLUGINS_FOLDER'] = os.path.join(AIRFLOW_HOME, 'plugins')
    
    # Use in-memory SQLite database for tests
    os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'] = 'sqlite:////:memory:'
    
    # Disable parallelism to avoid race conditions
    os.environ['AIRFLOW__CORE__PARALLELISM'] = '1'
    os.environ['AIRFLOW__CORE__DAG_CONCURRENCY'] = '1'
    os.environ['AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG'] = '1'
    
    # Additional settings for testing
    os.environ['AIRFLOW__WEBSERVER__EXPOSE_CONFIG'] = 'True'
    os.environ['AIRFLOW__CORE__FERNET_KEY'] = ''
    os.environ['AIRFLOW__CORE__EXECUTOR'] = 'SequentialExecutor'


def create_mock_connection(conn_id, conn_type, extra=None):
    """
    Create a mock Airflow connection object
    
    Args:
        conn_id: Connection ID
        conn_type: Connection type
        extra: Extra connection parameters as dict
        
    Returns:
        Connection: Mock Airflow connection object
    """
    from airflow.models import Connection
    
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        extra=json.dumps(extra) if extra else None
    )
    
    # Set reasonable defaults for common connection types
    if conn_type == 'google_cloud_platform':
        if not extra:
            conn.extra = json.dumps({
                'project_id': 'mock-project',
                'key_path': '/tmp/mock-key.json',
                'scopes': 'https://www.googleapis.com/auth/cloud-platform'
            })
    elif conn_type == 'postgres':
        conn.host = conn.host or 'localhost'
        conn.login = conn.login or 'postgres'
        conn.password = conn.password or 'postgres'
        conn.schema = conn.schema or 'postgres'
        conn.port = conn.port or 5432
    
    return conn


def setup_mock_connections(connections):
    """
    Set up mock connections in Airflow environment
    
    Args:
        connections: List of connection configurations
        
    Returns:
        dict: Dictionary mapping connection IDs to Connection objects
    """
    from airflow.models import Connection
    from airflow.utils.session import create_session
    
    result = {}
    
    with create_session() as session:
        for conn_config in connections:
            conn_id = conn_config['conn_id']
            conn_type = conn_config['conn_type']
            extra = conn_config.get('extra')
            
            # Create or get the connection
            conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            if not conn:
                conn = create_mock_connection(conn_id, conn_type, extra)
                session.add(conn)
            else:
                conn.conn_type = conn_type
                if extra:
                    conn.extra = json.dumps(extra)
            
            result[conn_id] = conn
    
    return result


@pytest.fixture(scope="session", autouse=True)
def airflow_home():
    """
    Session-scoped fixture that provides a temporary Airflow home directory
    
    Returns:
        str: Path to the Airflow home directory
    """
    # Create temporary directories
    os.makedirs(AIRFLOW_HOME, exist_ok=True)
    os.makedirs(DAGS_FOLDER, exist_ok=True)
    os.makedirs(os.path.join(AIRFLOW_HOME, 'plugins'), exist_ok=True)
    os.makedirs(os.path.join(AIRFLOW_HOME, 'logs'), exist_ok=True)
    
    yield AIRFLOW_HOME
    
    # Clean up after all tests
    try:
        shutil.rmtree(AIRFLOW_HOME)
    except Exception as e:
        logger.warning(f"Failed to clean up Airflow home: {str(e)}")


@pytest.fixture(scope="session", autouse=True)
def airflow_session():
    """
    Session-scoped fixture that provides an Airflow database session
    
    Returns:
        Session: SQLAlchemy session for Airflow database
    """
    # Set up Airflow environment
    setup_airflow_environment()
    
    # Reset the database to ensure a clean state
    reset_db()
    
    # Create and yield a session
    from airflow.utils.session import create_session
    with create_session() as session:
        yield session


@pytest.fixture
def mock_dag():
    """
    Function-scoped fixture that provides a mock DAG for testing
    
    Returns:
        DAG: Airflow DAG object
    """
    from airflow.models import DAG
    
    dag_id = f"test_dag_{pendulum.now().timestamp()}"
    dag = DAG(
        dag_id,
        default_args=DEFAULT_TASK_ARGS,
        schedule_interval=None,
        start_date=DEFAULT_DATE
    )
    
    yield dag
    
    # Clean up DAG
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session
    
    with create_session() as session:
        session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
        session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()


@pytest.fixture
def mock_task(mock_dag):
    """
    Function-scoped fixture that provides a mock task for testing
    
    Args:
        mock_dag: The mock DAG fixture
        
    Returns:
        BaseOperator: Airflow operator object
    """
    # Import the appropriate operator based on Airflow version
    if is_airflow2():
        from airflow.operators.dummy import DummyOperator
    else:
        from airflow.operators.dummy_operator import DummyOperator
    
    task = DummyOperator(
        task_id="test_task",
        dag=mock_dag
    )
    
    yield task


@pytest.fixture
def mock_gcp_connection():
    """
    Function-scoped fixture that provides a mock GCP connection
    
    Returns:
        Connection: Airflow GCP connection object
    """
    conn = create_mock_connection(
        conn_id="google_cloud_default",
        conn_type="google_cloud_platform",
        extra={
            "project_id": "mock-project",
            "key_path": "/tmp/mock-key.json",
            "scopes": "https://www.googleapis.com/auth/cloud-platform"
        }
    )
    
    yield conn


@pytest.fixture
def mock_postgres_connection():
    """
    Function-scoped fixture that provides a mock Postgres connection
    
    Returns:
        Connection: Airflow Postgres connection object
    """
    conn = create_mock_connection(
        conn_id="postgres_default",
        conn_type="postgres",
        extra={}
    )
    
    conn.host = "localhost"
    conn.login = "postgres"
    conn.password = "postgres"
    conn.schema = "postgres"
    conn.port = 5432
    
    yield conn


@pytest.fixture
def mock_gcs_client():
    """
    Function-scoped fixture that provides a mock GCS client
    
    Returns:
        MockStorageClient: Mock GCS client object
    """
    client = create_mock_storage_client(
        mock_responses={
            "mock-bucket": {
                "blobs": {
                    "test-object.txt": "test content"
                }
            }
        }
    )
    
    yield client


@pytest.fixture
def mock_secret_manager_client():
    """
    Function-scoped fixture that provides a mock Secret Manager client
    
    Returns:
        MockSecretManagerClient: Mock Secret Manager client object
    """
    client = create_mock_secret_manager_client(
        mock_responses={
            "secrets": {
                "test-secret": {
                    "versions": {
                        "latest": {
                            "payload": "test-secret-value"
                        }
                    }
                }
            }
        }
    )
    
    yield client


@pytest.fixture
def mock_bigquery_client():
    """
    Function-scoped fixture that provides a mock BigQuery client
    
    Returns:
        MockBigQueryClient: Mock BigQuery client object
    """
    client = create_mock_bigquery_client(
        mock_responses={
            "datasets": {
                "test_dataset": {
                    "tables": {
                        "test_table": {
                            "schema": [],
                            "rows": []
                        }
                    }
                }
            },
            "queries": {
                "SELECT 1": [{"col1": 1}]
            }
        }
    )
    
    yield client


@pytest.fixture
def mock_airflow_context():
    """
    Function-scoped fixture that provides a mock Airflow task context
    
    Returns:
        dict: Airflow task context dictionary
    """
    from airflow.models import TaskInstance, DAG
    from airflow.utils.dates import days_ago
    
    # Create a minimal context
    dag = DAG("test_dag", start_date=days_ago(1))
    
    if is_airflow2():
        from airflow.operators.dummy import DummyOperator
    else:
        from airflow.operators.dummy_operator import DummyOperator
    
    task = DummyOperator(task_id="test_task", dag=dag)
    task_instance = TaskInstance(task, DEFAULT_DATE)
    
    context = {
        "dag": dag,
        "task": task,
        "task_instance": task_instance,
        "ti": task_instance,
        "execution_date": DEFAULT_DATE,
        "ds": DEFAULT_DATE.strftime("%Y-%m-%d"),
        "ts": DEFAULT_DATE.isoformat(),
        "params": {},
    }
    
    # Add Airflow 2.X specific context items
    if is_airflow2():
        context.update({
            "data_interval_start": DEFAULT_DATE,
            "data_interval_end": DEFAULT_DATE + pendulum.duration(days=1),
            "logical_date": DEFAULT_DATE,
        })
    
    yield context


@pytest.fixture(scope="session")
def mock_airflow2_environment(airflow_home):
    """
    Session-scoped fixture that sets up an Airflow 2.X environment
    
    Args:
        airflow_home: The airflow_home fixture
        
    Returns:
        dict: Environment configuration information
    """
    # Check if we're already running under Airflow 2.X
    if is_airflow2():
        yield {"is_airflow2": True, "version": airflow.__version__}
        return
    
    # Mock Airflow 2.X imports
    with mock_airflow2_imports():
        # Set additional environment variables specific to Airflow 2.X
        os.environ['AIRFLOW__CORE__LAZY_LOAD_PLUGINS'] = 'True'
        os.environ['AIRFLOW__CORE__STORE_DAG_CODE'] = 'True'
        os.environ['AIRFLOW__CORE__STORE_SERIALIZED_DAGS'] = 'True'
        
        env_info = {
            "is_airflow2": True,
            "version": "2.0.0",  # Mocked version
            "home": airflow_home
        }
        
        yield env_info
        
        # Cleanup
        for key in ['AIRFLOW__CORE__LAZY_LOAD_PLUGINS', 
                    'AIRFLOW__CORE__STORE_DAG_CODE',
                    'AIRFLOW__CORE__STORE_SERIALIZED_DAGS']:
            if key in os.environ:
                del os.environ[key]


@pytest.fixture
def migration_test_environment(mock_airflow2_environment):
    """
    Fixture that provides an environment for testing Airflow 1.X to 2.X migration
    
    Args:
        mock_airflow2_environment: The mock_airflow2_environment fixture
        
    Returns:
        dict: Test environment context
    """
    # Create test context with migration utilities
    context = {
        "airflow2_compat_mixin": Airflow2CompatibilityTestMixin(),
        "is_airflow2": is_airflow2(),
        "airflow_home": mock_airflow2_environment["home"],
        "version": mock_airflow2_environment["version"]
    }
    
    yield context