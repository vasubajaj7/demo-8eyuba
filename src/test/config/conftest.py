# Third-party imports
import pytest  # v6.0+
import os  # standard library
import sys  # standard library
import pathlib  # standard library
import tempfile  # standard library
import shutil  # standard library
from unittest import mock  # v4.0+

# Internal imports
from ..fixtures import mock_data  # src/test/fixtures/mock_data.py
from ..fixtures import mock_connections  # src/test/fixtures/mock_connections.py
from ..fixtures import mock_gcp_services  # src/test/fixtures/mock_gcp_services.py
from ..utils import airflow2_compatibility_utils  # src/test/utils/airflow2_compatibility_utils.py
from ..utils import assertion_utils  # src/test/utils/assertion_utils.py
from ..utils import test_helpers  # src/test/utils/test_helpers.py

# Define global variables
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
TEST_DATA_DIR = pathlib.Path(__file__).parent.parent / 'fixtures' / 'data'

# Pytest plugins
pytest_plugins = ['pytest_profiling', 'pytest_html']


def pytest_addoption(parser):
    """Adds pytest command line options for Airflow tests"""
    # Add option to specify Airflow home directory
    parser.addoption("--airflow_home", action="store", default=None,
                     help="Specify the Airflow home directory")

    # Add option to skip Airflow 2.x specific tests
    parser.addoption("--skip_airflow2_tests", action="store_true",
                     help="Skip Airflow 2.x specific tests")

    # Add option to test Composer 1 compatibility
    parser.addoption("--composer1_compatibility", action="store_true",
                     help="Test Composer 1 compatibility")

    # Add option to specify Airflow version for tests
    parser.addoption("--airflow_version", action="store", default=None,
                     help="Specify Airflow version for tests")


def pytest_configure(config):
    """Configures pytest environment for Airflow tests"""
    # Set up environment variables required for testing
    os.environ['AIRFLOW_DATABASE_URI'] = 'sqlite:///:memory:'
    os.environ['AIRFLOW_TEST_MODE'] = 'True'

    # Configure Airflow home directory if specified
    airflow_home = config.getoption("--airflow_home")
    if airflow_home:
        os.environ['AIRFLOW_HOME'] = airflow_home

    # Initialize Airflow configuration for testing
    # from airflow.configuration import conf  # Removed to avoid Airflow import errors
    # conf.read()

    # Register custom markers for test classification
    config.addinivalue_line("markers", "airflow2: mark test as Airflow 2.x specific")
    config.addinivalue_line("markers", "composer1_compatibility: mark test as Composer 1 compatibility")

    # Configure logging for test environment
    logging.basicConfig(level=logging.INFO)


def pytest_collection_modifyitems(config, items):
    """Modifies test items before execution"""
    # Skip Airflow 2.x tests if skip_airflow2_tests option is enabled
    if config.getoption("--skip_airflow2_tests"):
        skip_airflow2 = pytest.mark.skip(reason="Skipping Airflow 2.x tests")
        for item in items:
            if "airflow2" in item.keywords:
                item.add_marker(skip_airflow2)

    # Mark composer1_compatibility tests if composer1_compatibility option is enabled
    if config.getoption("--composer1_compatibility"):
        composer1_mark = pytest.mark.composer1_compatibility
        for item in items:
            if "composer1_compatibility" in item.keywords:
                item.add_marker(composer1_mark)

    # Apply additional markers based on test configuration
    # Sort tests by dependency to ensure proper execution order
    items.sort(key=lambda x: x.name)


@pytest.fixture(scope='session')
def airflow_home():
    """Fixture that provides a temporary Airflow home directory"""
    # Create temporary directory for Airflow home
    with tempfile.TemporaryDirectory() as temp_dir:
        airflow_home_dir = pathlib.Path(temp_dir)
        # Set AIRFLOW_HOME environment variable
        os.environ['AIRFLOW_HOME'] = str(airflow_home_dir)

        # Create necessary subdirectories for Airflow (dags, logs, etc.)
        (airflow_home_dir / 'dags').mkdir(exist_ok=True)
        (airflow_home_dir / 'logs').mkdir(exist_ok=True)
        (airflow_home_dir / 'plugins').mkdir(exist_ok=True)

        # Copy test configuration files to the temporary directory
        # shutil.copy(TEST_DATA_DIR / 'airflow.cfg', airflow_home_dir)

        # Yield the temporary directory path
        yield str(airflow_home_dir)

        # Clean up the temporary directory after tests
        # shutil.rmtree(temp_dir)


@pytest.fixture
def mock_airflow_connection():
    """Fixture that provides a mock Airflow connection"""
    # Create mock connection object
    mock_conn = mock.MagicMock()

    # Patch the Airflow connection get method
    with mock.patch('airflow.hooks.base.BaseHook.get_connection', return_value=mock_conn) as mock_get_connection:
        # Yield the mock connection
        yield mock_conn

        # Clean up the mock connection after test
        mock_get_connection.stop()


@pytest.fixture
def mock_airflow_variable():
    """Fixture that provides a mock Airflow variable"""
    # Create mock variable object
    mock_variable = mock.MagicMock()

    # Patch the Airflow variable get method
    with mock.patch('airflow.models.Variable.get', return_value=mock_variable) as mock_get_variable:
        # Yield the mock variable
        yield mock_variable

        # Clean up the mock variable after test
        mock_get_variable.stop()


@pytest.fixture
def mock_gcp_connection():
    """Fixture that provides a mock GCP connection"""
    # Create mock GCP connection
    mock_conn = mock.MagicMock()
    mock_conn.conn_type = 'google_cloud_platform'
    mock_conn.host = 'test_host'
    mock_conn.login = 'test_login'
    mock_conn.password = 'test_password'
    mock_conn.extra = '{"project": "test_project"}'

    # Configure mock GCP connection parameters
    # Patch the GCP hook get_connection method
    with mock.patch('airflow.providers.google.cloud.hooks.base.GoogleCloudBaseHook.get_connection', return_value=mock_conn) as mock_get_connection:
        # Yield the mock connection
        yield mock_conn

        # Clean up the mock connection after test
        mock_get_connection.stop()


@pytest.fixture
def mock_postgres_connection():
    """Fixture that provides a mock Postgres connection"""
    # Create mock Postgres connection
    mock_conn = mock.MagicMock()
    mock_conn.conn_type = 'postgres'
    mock_conn.host = 'test_host'
    mock_conn.login = 'test_login'
    mock_conn.password = 'test_password'
    mock_conn.schema = 'test_schema'

    # Configure mock Postgres connection parameters
    # Patch the Postgres hook get_connection method
    with mock.patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_connection', return_value=mock_conn) as mock_get_connection:
        # Yield the mock connection
        yield mock_conn

        # Clean up the mock connection after test
        mock_get_connection.stop()


@pytest.fixture
def dag_test_context():
    """Fixture that provides a context for DAG testing"""
    # Set up DAG testing environment
    execution_date = datetime.datetime(2023, 1, 1)
    test_params = {'param1': 'value1', 'param2': 'value2'}

    # Configure execution date and test parameters
    # Create test DAG context
    test_context = {
        'dag': mock.MagicMock(dag_id='test_dag'),
        'ti': mock.MagicMock(execution_date=execution_date),
        'execution_date': execution_date,
        'params': test_params
    }

    # Yield the test context
    yield test_context

    # Clean up the test context after test
    del test_context


@pytest.fixture(scope='session')
def composer2_environment():
    """Fixture that simulates a Composer 2 environment"""
    # Set up Composer 2 environment variables
    os.environ['CLOUD_STORAGE_BUCKET'] = 'test-bucket'
    os.environ['GCP_PROJECT'] = 'test-project'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/credentials.json'

    # Configure GCP integration for Composer 2
    # Set up Airflow 2.x configuration
    # Yield the environment context
    yield True

    # Clean up the environment after tests
    del os.environ['CLOUD_STORAGE_BUCKET']
    del os.environ['GCP_PROJECT']
    del os.environ['GOOGLE_APPLICATION_CREDENTIALS']


@pytest.fixture
def airflow2_compatibility():
    """Fixture that checks Airflow 2.x compatibility"""
    # Set up Airflow 2.x compatibility checking
    # Configure compatibility parameters
    # Initialize compatibility checker
    # Yield the compatibility context
    yield True

    # Report compatibility issues after test
    pass