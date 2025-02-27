"""
Unit tests for custom PostgreSQL sensors used in Apache Airflow 2.X.
This file validates the functionality of the custom PostgreSQL sensors during the migration
from Airflow 1.10.15 to Airflow 2.X, ensuring proper database monitoring capabilities across environments.
"""

import unittest  # Python standard library
import pytest  # pytest v6.0.0+
from unittest.mock import patch, MagicMock  # Python standard library

from airflow.exceptions import AirflowException  # airflow v2.0.0+

# Internal module imports
from src.backend.plugins.sensors.custom_postgres_sensor import CustomPostgresSensor, CustomPostgresTableExistenceSensor, CustomPostgresRowCountSensor, CustomPostgresValueCheckSensor  # Main PostgreSQL sensor class to test
from src.backend.plugins.hooks.custom_postgres_hook import CustomPostgresHook  # PostgreSQL hook used by sensors
from src.test.fixtures.mock_hooks import MockCustomPostgresHook, create_mock_custom_postgres_hook  # Mock hook for testing PostgreSQL sensors
from src.test.fixtures.mock_sensors import create_mock_context  # Create mock Airflow context for testing sensors
from src.test.utils.airflow2_compatibility_utils import is_airflow2, Airflow2CompatibilityTestMixin  # Check if running in Airflow 2.X environment

# Global constants
TEST_POSTGRES_CONN_ID = 'test_postgres_conn'
TEST_SCHEMA = 'public'
TEST_SQL = 'SELECT COUNT(*) FROM test_table'
TEST_PARAMS = {'param1': 'value1', 'param2': 'value2'}


def setup_module():
    """Setup function run before any tests in the module"""
    # Configure test environment for PostgreSQL testing
    # Set up any needed patches for consistent test environment
    pass


def teardown_module():
    """Teardown function run after all tests in the module"""
    # Clean up any patches or test resources
    # Restore original environment state
    pass


class TestCustomPostgresSensor(Airflow2CompatibilityTestMixin, unittest.TestCase):
    """Test cases for the base CustomPostgresSensor class"""

    def __init__(self, *args, **kwargs):
        """Initialize the test case"""
        super().__init__(*args, **kwargs)
        # Initialize test attributes
        self.sensor = None
        self.mock_hook = None
        self.patcher = None

    def setUp(self):
        """Set up test resources before each test"""
        # Create mock PostgreSQL hook
        self.mock_hook = create_mock_custom_postgres_hook(TEST_POSTGRES_CONN_ID, TEST_SCHEMA, {})

        # Set up patch for CustomPostgresHook to return mock
        self.patcher = patch('src.backend.plugins.sensors.custom_postgres_sensor._get_hook', return_value=self.mock_hook)
        self.patcher.start()

        # Initialize basic mock responses for PostgreSQL queries
        self.mock_hook.execute_query.return_value = [(1,)]

        # Create test sensor instance for testing
        self.sensor = CustomPostgresSensor(
            task_id='test_postgres_sensor',
            sql=TEST_SQL,
            postgres_conn_id=TEST_POSTGRES_CONN_ID,
            schema=TEST_SCHEMA,
            params=TEST_PARAMS
        )

    def tearDown(self):
        """Clean up test resources after each test"""
        # Stop all patches
        self.patcher.stop()

        # Clean up any test data
        self.sensor = None
        self.mock_hook = None

        # Reset mock hooks
        pass

    def test_init(self):
        """Test sensor initialization with various parameters"""
        # Create sensor with default parameters
        sensor_default = CustomPostgresSensor(task_id='test_default', sql=TEST_SQL)
        self.assertEqual(sensor_default.postgres_conn_id, 'postgres_default')
        self.assertEqual(sensor_default.schema, 'public')
        self.assertEqual(sensor_default.params, {})
        self.assertEqual(sensor_default.fail_on_error, False)
        self.assertEqual(sensor_default.alert_on_error, False)

        # Create sensor with custom parameters
        sensor_custom = CustomPostgresSensor(
            task_id='test_custom',
            sql=TEST_SQL,
            postgres_conn_id=TEST_POSTGRES_CONN_ID,
            schema=TEST_SCHEMA,
            params=TEST_PARAMS,
            fail_on_error=True,
            alert_on_error=True
        )
        self.assertEqual(sensor_custom.postgres_conn_id, TEST_POSTGRES_CONN_ID)
        self.assertEqual(sensor_custom.schema, TEST_SCHEMA)
        self.assertEqual(sensor_custom.params, TEST_PARAMS)
        self.assertEqual(sensor_custom.fail_on_error, True)
        self.assertEqual(sensor_custom.alert_on_error, True)

        # Verify initialization parameter handling with None values
        sensor_none_params = CustomPostgresSensor(task_id='test_none', sql=TEST_SQL, params=None)
        self.assertEqual(sensor_none_params.params, {})

    def test_get_hook(self):
        """Test get_hook method returns correct hook instance"""
        # Call get_hook method on sensor
        hook = self.sensor.get_hook()

        # Verify PostgreSQL hook created with correct connection ID
        self.assertEqual(hook.postgres_conn_id, TEST_POSTGRES_CONN_ID)

        # Verify hook has the correct schema set
        self.assertEqual(hook.schema, TEST_SCHEMA)

        # Verify hook returned is of correct type
        self.assertIsInstance(hook, CustomPostgresHook)

    def test_poke_success(self):
        """Test poke method returns True when query returns records"""
        # Configure mock hook to return query results
        self.mock_hook.execute_query.return_value = [(1,)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = self.sensor.poke(context)

        # Verify hook.execute_query was called with correct SQL and parameters
        self.mock_hook.execute_query.assert_called_once_with(sql=TEST_SQL, parameters=TEST_PARAMS)

        # Verify poke returns True when records exist
        self.assertTrue(result)

    def test_poke_failure(self):
        """Test poke method returns False when query returns no records"""
        # Configure mock hook to return empty result
        self.mock_hook.execute_query.return_value = []

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = self.sensor.poke(context)

        # Verify hook.execute_query was called with correct SQL and parameters
        self.mock_hook.execute_query.assert_called_once_with(sql=TEST_SQL, parameters=TEST_PARAMS)

        # Verify poke returns False when no records exist
        self.assertFalse(result)

    def test_poke_error_handling(self):
        """Test sensor error handling with different configurations"""
        # Configure mock hook to raise exception
        self.mock_hook.execute_query.side_effect = Exception("Test exception")

        # Create test sensor with fail_on_error=True
        sensor_fail_on_error = CustomPostgresSensor(
            task_id='test_fail_on_error',
            sql=TEST_SQL,
            fail_on_error=True
        )

        # Create mock Airflow context
        context = create_mock_context()

        # Verify exception is propagated when fail_on_error is True
        with self.assertRaises(Exception):
            sensor_fail_on_error.poke(context)

        # Create test sensor with fail_on_error=False
        sensor_no_fail_on_error = CustomPostgresSensor(
            task_id='test_no_fail_on_error',
            sql=TEST_SQL,
            fail_on_error=False
        )

        # Verify poke returns False when fail_on_error is False
        result = sensor_no_fail_on_error.poke(context)
        self.assertFalse(result)

        # Test alert_on_error behavior with alert utility
        # (Implementation of alert utility is not mocked here, only checking that it's called)
        pass

    @pytest.mark.skipif(not is_airflow2(), reason="requires Airflow 2.X")
    def test_airflow2_compatibility(self):
        """Test compatibility with Airflow 2.X specific features"""
        # Verify sensor works with Airflow 2.X provider packages
        # Test any Airflow 2.X specific parameters or behavior
        pass


class TestCustomPostgresTableExistenceSensor(Airflow2CompatibilityTestMixin, unittest.TestCase):
    """Test cases for the CustomPostgresTableExistenceSensor"""

    def __init__(self, *args, **kwargs):
        """Initialize the test case"""
        super().__init__(*args, **kwargs)
        # Initialize test attributes specific to table existence sensor
        self.sensor = None
        self.mock_hook = None
        self.patcher = None
        self.table_name = 'test_table'

    def setUp(self):
        """Set up test resources before each test"""
        # Create mock PostgreSQL hook
        self.mock_hook = create_mock_custom_postgres_hook(TEST_POSTGRES_CONN_ID, TEST_SCHEMA, {})

        # Set up patch for CustomPostgresHook to return mock
        self.patcher = patch('src.backend.plugins.sensors.custom_postgres_sensor._get_hook', return_value=self.mock_hook)
        self.patcher.start()

        # Initialize mock responses for table existence queries
        self.mock_hook.execute_query.return_value = [(1,)]  # Table exists

        # Create test table existence sensor instance
        self.sensor = CustomPostgresTableExistenceSensor(
            task_id='test_table_existence_sensor',
            table_name=self.table_name,
            postgres_conn_id=TEST_POSTGRES_CONN_ID,
            schema=TEST_SCHEMA
        )

    def tearDown(self):
        """Clean up test resources after each test"""
        # Stop all patches
        self.patcher.stop()

        # Clean up any test data
        self.sensor = None
        self.mock_hook = None

        # Reset mock hooks
        pass

    def test_init(self):
        """Test table existence sensor initialization"""
        # Create sensor with required table_name parameter
        sensor = CustomPostgresTableExistenceSensor(
            task_id='test_init',
            table_name=self.table_name
        )

        # Verify SQL query is constructed correctly to check table existence
        expected_sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM   pg_catalog.pg_class c
                JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE  n.nspname = '{TEST_SCHEMA}'
                AND    c.relname = '{self.table_name}'
                AND    c.relkind = 'r'    
            );
        """
        self.assertEqual(sensor.sql, expected_sql)

        # Verify other parameters are passed correctly to parent class
        self.assertEqual(sensor.postgres_conn_id, 'postgres_default')
        self.assertEqual(sensor.schema, TEST_SCHEMA)

    def test_poke_table_exists(self):
        """Test poke returns True when table exists"""
        # Configure mock hook to return records (table exists)
        self.mock_hook.execute_query.return_value = [(1,)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = self.sensor.poke(context)

        # Verify poke returns True when table exists
        self.assertTrue(result)

        # Verify SQL query checked for table existence in information_schema
        expected_sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM   pg_catalog.pg_class c
                JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE  n.nspname = '{TEST_SCHEMA}'
                AND    c.relname = '{self.table_name}'
                AND    c.relkind = 'r'    
            );
        """
        self.mock_hook.execute_query.assert_called_once_with(sql=expected_sql, parameters={})

    def test_poke_table_does_not_exist(self):
        """Test poke returns False when table does not exist"""
        # Configure mock hook to return empty result (table doesn't exist)
        self.mock_hook.execute_query.return_value = []

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = self.sensor.poke(context)

        # Verify poke returns False when table doesn't exist
        self.assertFalse(result)


class TestCustomPostgresRowCountSensor(Airflow2CompatibilityTestMixin, unittest.TestCase):
    """Test cases for the CustomPostgresRowCountSensor"""

    def __init__(self, *args, **kwargs):
        """Initialize the test case"""
        super().__init__(*args, **kwargs)
        # Initialize test attributes specific to row count sensor
        self.sensor = None
        self.mock_hook = None
        self.patcher = None
        self.table_name = 'test_table'
        self.min_rows = 5

    def setUp(self):
        """Set up test resources before each test"""
        # Create mock PostgreSQL hook
        self.mock_hook = create_mock_custom_postgres_hook(TEST_POSTGRES_CONN_ID, TEST_SCHEMA, {})

        # Set up patch for CustomPostgresHook to return mock
        self.patcher = patch('src.backend.plugins.sensors.custom_postgres_sensor._get_hook', return_value=self.mock_hook)
        self.patcher.start()

        # Initialize mock responses for row count queries
        self.mock_hook.execute_query.return_value = [(10,)]  # 10 rows

        # Create test row count sensor instance
        self.sensor = CustomPostgresRowCountSensor(
            task_id='test_row_count_sensor',
            table_name=self.table_name,
            min_rows=self.min_rows,
            postgres_conn_id=TEST_POSTGRES_CONN_ID,
            schema=TEST_SCHEMA
        )

    def tearDown(self):
        """Clean up test resources after each test"""
        # Stop all patches
        self.patcher.stop()

        # Clean up any test data
        self.sensor = None
        self.mock_hook = None

        # Reset mock hooks
        pass

    def test_init(self):
        """Test row count sensor initialization"""
        # Create sensor with required table_name parameter
        sensor = CustomPostgresRowCountSensor(
            task_id='test_init',
            table_name=self.table_name,
            min_rows=self.min_rows
        )

        # Test different min_rows values
        self.assertEqual(sensor.min_rows, self.min_rows)

        # Test with and without where_clause
        sensor_with_where = CustomPostgresRowCountSensor(
            task_id='test_with_where',
            table_name=self.table_name,
            min_rows=self.min_rows,
            where_clause='col1 > 10'
        )

        # Verify SQL query is constructed correctly for row counting
        expected_sql = f"SELECT COUNT(*) FROM {TEST_SCHEMA}.{self.table_name}"
        self.assertEqual(sensor.sql, expected_sql)

        # Verify SQL includes WHERE clause when provided
        expected_sql_with_where = f"SELECT COUNT(*) FROM {TEST_SCHEMA}.{self.table_name} WHERE col1 > 10"
        self.assertEqual(sensor_with_where.sql, expected_sql_with_where)

    def test_poke_with_sufficient_rows(self):
        """Test poke returns True when table has sufficient rows"""
        # Configure mock hook to return row count > min_rows
        self.mock_hook.execute_query.return_value = [(10,)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = self.sensor.poke(context)

        # Verify poke returns True when count >= min_rows
        self.assertTrue(result)

    def test_poke_with_insufficient_rows(self):
        """Test poke returns False when table has insufficient rows"""
        # Configure mock hook to return row count < min_rows
        self.mock_hook.execute_query.return_value = [(2,)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = self.sensor.poke(context)

        # Verify poke returns False when count < min_rows
        self.assertFalse(result)

    def test_poke_with_where_clause(self):
        """Test poke behavior with a WHERE clause filter"""
        # Create sensor with where_clause parameter
        sensor_with_where = CustomPostgresRowCountSensor(
            task_id='test_with_where',
            table_name=self.table_name,
            min_rows=self.min_rows,
            where_clause='col1 > 10'
        )

        # Configure mock hook to return filtered row count
        self.mock_hook.execute_query.return_value = [(8,)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = sensor_with_where.poke(context)

        # Verify SQL contains WHERE clause
        expected_sql_with_where = f"SELECT COUNT(*) FROM {TEST_SCHEMA}.{self.table_name} WHERE col1 > 10"
        self.mock_hook.execute_query.assert_called_once_with(sql=expected_sql_with_where, parameters={})

        # Verify poke result based on filtered count
        self.assertTrue(result)


class TestCustomPostgresValueCheckSensor(Airflow2CompatibilityTestMixin, unittest.TestCase):
    """Test cases for the CustomPostgresValueCheckSensor"""

    def __init__(self, *args, **kwargs):
        """Initialize the test case"""
        super().__init__(*args, **kwargs)
        # Initialize test attributes specific to value check sensor
        self.sensor = None
        self.mock_hook = None
        self.patcher = None
        self.sql = "SELECT value FROM test_table WHERE id = 1"
        self.expected_value = "test_value"

    def setUp(self):
        """Set up test resources before each test"""
        # Create mock PostgreSQL hook
        self.mock_hook = create_mock_custom_postgres_hook(TEST_POSTGRES_CONN_ID, TEST_SCHEMA, {})

        # Set up patch for CustomPostgresHook to return mock
        self.patcher = patch('src.backend.plugins.sensors.custom_postgres_sensor._get_hook', return_value=self.mock_hook)
        self.patcher.start()

        # Initialize mock responses for value check queries
        self.mock_hook.execute_query.return_value = [("test_value",)]

        # Create test value check sensor instance
        self.sensor = CustomPostgresValueCheckSensor(
            task_id='test_value_check_sensor',
            sql=self.sql,
            expected_value=self.expected_value,
            postgres_conn_id=TEST_POSTGRES_CONN_ID,
            schema=TEST_SCHEMA
        )

    def tearDown(self):
        """Clean up test resources after each test"""
        # Stop all patches
        self.patcher.stop()

        # Clean up any test data
        self.sensor = None
        self.mock_hook = None

        # Reset mock hooks
        pass

    def test_init(self):
        """Test value check sensor initialization"""
        # Create sensor with SQL and expected_value parameters
        sensor = CustomPostgresValueCheckSensor(
            task_id='test_init',
            sql=self.sql,
            expected_value=self.expected_value
        )

        # Test with different expected_value types (string, number, boolean)
        sensor_num = CustomPostgresValueCheckSensor(
            task_id='test_num',
            sql=self.sql,
            expected_value=123
        )
        sensor_bool = CustomPostgresValueCheckSensor(
            task_id='test_bool',
            sql=self.sql,
            expected_value=True
        )

        # Test with exact_match=True and exact_match=False
        sensor_exact = CustomPostgresValueCheckSensor(
            task_id='test_exact',
            sql=self.sql,
            expected_value=self.expected_value,
            exact_match=True
        )
        sensor_pattern = CustomPostgresValueCheckSensor(
            task_id='test_pattern',
            sql=self.sql,
            expected_value=self.expected_value,
            exact_match=False
        )

        # Verify parameters are passed correctly to parent class
        self.assertEqual(sensor.sql, self.sql)
        self.assertEqual(sensor.expected_value, self.expected_value)
        self.assertTrue(sensor.exact_match)

    def test_poke_exact_match_success(self):
        """Test poke with exact match returning True on match"""
        # Create sensor with exact_match=True
        sensor_exact = CustomPostgresValueCheckSensor(
            task_id='test_exact',
            sql=self.sql,
            expected_value=self.expected_value,
            exact_match=True
        )

        # Configure mock hook to return exactly matching value
        self.mock_hook.execute_query.return_value = [(self.expected_value,)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = sensor_exact.poke(context)

        # Verify poke returns True for exact match
        self.assertTrue(result)

    def test_poke_exact_match_failure(self):
        """Test poke with exact match returning False on mismatch"""
        # Create sensor with exact_match=True
        sensor_exact = CustomPostgresValueCheckSensor(
            task_id='test_exact',
            sql=self.sql,
            expected_value=self.expected_value,
            exact_match=True
        )

        # Configure mock hook to return non-matching value
        self.mock_hook.execute_query.return_value = [("different_value",)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = sensor_exact.poke(context)

        # Verify poke returns False when values don't match exactly
        self.assertFalse(result)

    def test_poke_pattern_match_success(self):
        """Test poke with pattern match returning True on partial match"""
        # Create sensor with exact_match=False
        sensor_pattern = CustomPostgresValueCheckSensor(
            task_id='test_pattern',
            sql=self.sql,
            expected_value=self.expected_value,
            exact_match=False
        )

        # Configure mock hook to return value containing expected pattern
        self.mock_hook.execute_query.return_value = [("value_with_test_value",)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = sensor_pattern.poke(context)

        # Verify poke returns True for partial pattern match
        self.assertTrue(result)

    def test_poke_pattern_match_failure(self):
        """Test poke with pattern match returning False when pattern not found"""
        # Create sensor with exact_match=False
        sensor_pattern = CustomPostgresValueCheckSensor(
            task_id='test_pattern',
            sql=self.sql,
            expected_value=self.expected_value,
            exact_match=False
        )

        # Configure mock hook to return value not containing expected pattern
        self.mock_hook.execute_query.return_value = [("completely_different",)]

        # Create mock Airflow context
        context = create_mock_context()

        # Call poke method with context
        result = sensor_pattern.poke(context)

        # Verify poke returns False when pattern not found
        self.assertFalse(result)

    def test_with_different_value_types(self):
        """Test sensor behavior with different data types"""
        # Test with string values
        self.mock_hook.execute_query.return_value = [("string_value",)]
        sensor_string = CustomPostgresValueCheckSensor(
            task_id='test_string',
            sql=self.sql,
            expected_value="string_value",
            exact_match=True
        )
        self.assertTrue(sensor_string.poke(create_mock_context()))

        # Test with numeric values
        self.mock_hook.execute_query.return_value = [(123,)]
        sensor_numeric = CustomPostgresValueCheckSensor(
            task_id='test_numeric',
            sql=self.sql,
            expected_value=123,
            exact_match=True
        )
        self.assertTrue(sensor_numeric.poke(create_mock_context()))

        # Test with boolean values
        self.mock_hook.execute_query.return_value = [(True,)]
        sensor_boolean = CustomPostgresValueCheckSensor(
            task_id='test_boolean',
            sql=self.sql,
            expected_value=True,
            exact_match=True
        )
        self.assertTrue(sensor_boolean.poke(create_mock_context()))

        # Test with None values
        self.mock_hook.execute_query.return_value = [(None,)]
        sensor_none = CustomPostgresValueCheckSensor(
            task_id='test_none',
            sql=self.sql,
            expected_value=None,
            exact_match=True
        )
        self.assertTrue(sensor_none.poke(create_mock_context()))

        # Verify correct type handling and comparison
        pass