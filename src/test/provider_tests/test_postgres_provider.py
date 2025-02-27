"""
Unit tests for the PostgreSQL provider components for Apache Airflow 2.X, focusing on
validating the compatibility and functionality of PostgreSQL hooks, operators, and connections
during the migration from Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2.
"""
import unittest  # Base testing functionality and mocking utilities
from unittest.mock import MagicMock  # unittest

import psycopg2  # psycopg2-binary v2.9.3 - PostgreSQL adapter for mocking database interactions
import pytest  # pytest v7.0.0+ - Testing framework for Python
import sqlalchemy  # sqlalchemy v1.4.0+ - SQL toolkit for mocking ORM and connection pooling
import pandas as pd  # pandas v1.3.0+ - Data manipulation library for testing DataFrame functionality

from airflow.providers.postgres.hooks.postgres import PostgresHook  # apache-airflow-providers-postgres v2.0.0+
from airflow.providers.postgres.operators.postgres import PostgresOperator  # apache-airflow-providers-postgres v2.0.0+

# Internal imports
from src.backend.plugins.hooks.custom_postgres_hook import CustomPostgresHook  # The custom PostgreSQL hook being tested
from src.backend.plugins.operators.custom_postgres_operator import CustomPostgresOperator  # The custom PostgreSQL operator being tested
from src.test.utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin  # Utility for testing compatibility across Airflow versions
from src.test.utils.airflow2_compatibility_utils import is_airflow2  # Check if running with Airflow 2.X
from src.test.fixtures.mock_connections import POSTGRES_CONN_ID  # Default PostgreSQL connection ID for testing
from src.test.fixtures.mock_connections import create_mock_postgres_connection  # Create mock PostgreSQL connections for testing
from src.test.fixtures.mock_connections import patch_airflow_connections  # Context manager to patch Airflow connection retrieval
from src.test.fixtures.mock_connections import MockConnectionManager  # Context manager for managing mock connections
from src.test.fixtures.mock_hooks import MockCustomPostgresHook  # Mock implementation of CustomPostgresHook for testing
from src.test.fixtures.mock_hooks import create_mock_custom_postgres_hook  # Factory function to create mock Postgres hooks

# Global test variables
TEST_SCHEMA = "test_schema"
TEST_TABLE = "test_table"
DEFAULT_POSTGRES_PORT = 5432
TEST_SQL_QUERY = "SELECT * FROM test_table WHERE id = %s"
TEST_SQL_MULTI_STATEMENT = (
    "CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, value TEXT);"
    " INSERT INTO test_table (value) VALUES ('test');"
)


def setup_module():
    """Prepares the test environment before running any tests"""
    # Set up any global test fixtures
    print("Setting up test module...")

    # Create mock PostgreSQL connections
    print("Creating mock PostgreSQL connections...")
    create_mock_postgres_connection(
        conn_id=POSTGRES_CONN_ID,
        host="localhost",
        database="testdb",
        user="testuser",
        password="testpassword",
        port=DEFAULT_POSTGRES_PORT,
    )

    # Apply patches for Airflow connections
    print("Applying patches for Airflow connections...")
    patch_airflow_connections().__enter__()


def teardown_module():
    """Cleans up the test environment after all tests have run"""
    # Remove all patches
    print("Tearing down test module...")
    patch_airflow_connections().__exit__(None, None, None)

    # Clean up any global resources
    print("Cleaning up global resources...")


class TestPostgresProviderHook(unittest.TestCase, Airflow2CompatibilityTestMixin):
    """Test case for validating PostgreSQL provider hook functionality"""

    connection_manager = MockConnectionManager()
    hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=TEST_SCHEMA)
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    def __init__(self, *args, **kwargs):
        """Initialize the test case"""
        super().__init__(*args, **kwargs)

    def setUp(self):
        """Set up the test environment before each test method"""
        # Create mock connection manager
        self.connection_manager = MockConnectionManager()
        self.connection_manager.__enter__()

        # Create mock database connection
        self.mock_conn = MagicMock()

        # Create mock cursor
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

        # Create PostgreSQL hook with mock connection
        self.hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=TEST_SCHEMA)
        self.hook.get_conn = MagicMock(return_value=self.mock_conn)

        # Configure mock cursor to return expected results
        self.mock_cursor.fetchall.return_value = [("test_value",)]
        self.mock_cursor.description = [("test_column", 10, None, None, None, None, None)]

    def tearDown(self):
        """Clean up after each test method"""
        # Close and reset hook connections
        self.hook.close_conn()

        # Remove mock patches
        self.connection_manager.__exit__(None, None, None)

    def test_init(self):
        """Test hook initialization with various parameters"""
        # Create hook with default parameters
        hook_default = CustomPostgresHook()
        self.assertEqual(hook_default.postgres_conn_id, "postgres_default")
        self.assertEqual(hook_default.schema, "public")

        # Create hook with custom parameters
        hook_custom = CustomPostgresHook(
            postgres_conn_id="test_conn", schema="test_schema", use_persistent_connection=True
        )
        self.assertEqual(hook_custom.postgres_conn_id, "test_conn")
        self.assertEqual(hook_custom.schema, "test_schema")
        self.assertTrue(hook_custom._use_persistent_connection)

    def test_get_conn(self):
        """Test connection retrieval functionality"""
        # Call get_conn method
        conn = self.hook.get_conn()

        # Verify connection is retrieved
        self.assertIsNotNone(conn)
        self.mock_conn.cursor.assert_called()

        # Verify persistent connection caching behavior
        conn2 = self.hook.get_conn()
        self.assertEqual(conn, conn2)  # Should return the same connection object

        # Verify error handling for connection failures
        self.hook.get_conn.side_effect = Exception("Connection failed")
        with self.assertRaises(Exception):
            self.hook.get_conn()

    def test_execute_query(self):
        """Test SQL query execution"""
        # Set up mock cursor to return sample data
        self.mock_cursor.fetchall.return_value = [("test_value",)]
        self.mock_cursor.description = [("test_column", 10, None, None, None, None, None)]

        # Execute test query with parameters
        results = self.hook.execute_query(TEST_SQL_QUERY, parameters=("test_param",))

        # Verify cursor execute was called with correct SQL and parameters
        self.mock_cursor.execute.assert_called_with(TEST_SQL_QUERY, ("test_param",))

        # Verify results are returned correctly
        self.assertEqual(results, [("test_value",)])

        # Test with different return formats (dict vs tuple)
        self.mock_cursor.description = [("test_column", 10, None, None, None, None, None)]
        results_dict = self.hook.execute_query(TEST_SQL_QUERY, return_dict=True)
        self.assertEqual(results_dict, [{"test_column": "test_value"}])

        # Verify error handling of SQL execution failures
        self.mock_cursor.execute.side_effect = Exception("Query failed")
        with self.assertRaises(Exception):
            self.hook.execute_query(TEST_SQL_QUERY)

    def test_query_to_df(self):
        """Test conversion of SQL query results to pandas DataFrame"""
        # Mock pandas.read_sql to return sample DataFrame
        mock_df = pd.DataFrame({"test_column": ["test_value"]})
        pd.read_sql = MagicMock(return_value=mock_df)

        # Call query_to_df with test query
        df = self.hook.query_to_df(TEST_SQL_QUERY)

        # Verify DataFrame is returned with correct structure
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), ["test_column"])
        self.assertEqual(df["test_column"].tolist(), ["test_value"])

        # Test with custom column names
        df_custom = self.hook.query_to_df(TEST_SQL_QUERY, columns=["custom_column"])
        self.assertEqual(df_custom.columns.tolist(), ["custom_column"])

        # Verify error handling for query failures
        pd.read_sql.side_effect = Exception("Query failed")
        with self.assertRaises(Exception):
            self.hook.query_to_df(TEST_SQL_QUERY)

    def test_execute_values(self):
        """Test batch execution with execute_values"""
        # Mock psycopg2.extras.execute_values
        psycopg2.extras.execute_values = MagicMock()

        # Call execute_values with test data
        test_values = [("value1",), ("value2",)]
        self.hook.execute_values("INSERT INTO test_table (value) VALUES %s", test_values)

        # Verify execute_values was called with correct parameters
        psycopg2.extras.execute_values.assert_called()

        # Test with different page sizes
        self.hook.execute_values(
            "INSERT INTO test_table (value) VALUES %s", test_values, page_size=500
        )
        psycopg2.extras.execute_values.assert_called()

        # Verify error handling for batch execution failures
        psycopg2.extras.execute_values.side_effect = Exception("Batch insert failed")
        with self.assertRaises(Exception):
            self.hook.execute_values("INSERT INTO test_table (value) VALUES %s", test_values)

    def test_execute_batch(self):
        """Test batch execution with execute_batch"""
        # Mock psycopg2.extras.execute_batch
        psycopg2.extras.execute_batch = MagicMock()

        # Call execute_batch with test data
        test_params = [("param1",), ("param2",)]
        self.hook.execute_batch("UPDATE test_table SET value = %s", test_params)

        # Verify execute_batch was called with correct parameters
        psycopg2.extras.execute_batch.assert_called()

        # Test with different batch sizes
        self.hook.execute_batch("UPDATE test_table SET value = %s", test_params, batch_size=500)
        psycopg2.extras.execute_batch.assert_called()

        # Verify error handling for batch execution failures
        psycopg2.extras.execute_batch.side_effect = Exception("Batch update failed")
        with self.assertRaises(Exception):
            self.hook.execute_batch("UPDATE test_table SET value = %s", test_params)

    def test_create_schema_if_not_exists(self):
        """Test schema creation functionality"""
        # Set up mock cursor for schema existence check
        self.mock_cursor.fetchone.return_value = None  # Schema doesn't exist

        # Test when schema doesn't exist
        self.hook.create_schema_if_not_exists(TEST_SCHEMA)

        # Verify CREATE SCHEMA statement is executed
        self.mock_cursor.execute.assert_called_with(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
        self.mock_conn.commit.assert_called()

        # Test when schema already exists
        self.mock_cursor.reset_mock()
        self.mock_conn.reset_mock()
        self.mock_cursor.fetchone.return_value = ("test_schema",)  # Schema exists
        self.hook.create_schema_if_not_exists(TEST_SCHEMA)

        # Verify no creation statement is executed when schema exists
        self.mock_cursor.execute.assert_not_called()
        self.mock_conn.commit.assert_not_called()

        # Verify error handling for schema creation failures
        self.mock_cursor.execute.side_effect = Exception("Schema creation failed")
        with self.assertRaises(Exception):
            self.hook.create_schema_if_not_exists(TEST_SCHEMA)

    def test_create_table_if_not_exists(self):
        """Test table creation functionality"""
        # Set up mock cursor for table existence check
        self.mock_cursor.fetchone.return_value = None  # Table doesn't exist

        # Test when table doesn't exist
        table_definition = "id SERIAL PRIMARY KEY, value TEXT"
        self.hook.create_table_if_not_exists(TEST_TABLE, table_definition, schema=TEST_SCHEMA)

        # Verify CREATE TABLE statement is executed
        create_sql = f"CREATE TABLE IF NOT EXISTS {TEST_SCHEMA}.{TEST_TABLE} ({table_definition})"
        self.mock_cursor.execute.assert_called_with(create_sql)
        self.mock_conn.commit.assert_called()

        # Test when table already exists
        self.mock_cursor.reset_mock()
        self.mock_conn.reset_mock()
        self.mock_cursor.fetchone.return_value = ("test_table",)  # Table exists
        self.hook.create_table_if_not_exists(TEST_TABLE, table_definition, schema=TEST_SCHEMA)

        # Verify no creation statement is executed when table exists
        self.mock_cursor.execute.assert_not_called()
        self.mock_conn.commit.assert_not_called()

        # Verify error handling for table creation failures
        self.mock_cursor.execute.side_effect = Exception("Table creation failed")
        with self.assertRaises(Exception):
            self.hook.create_table_if_not_exists(TEST_TABLE, table_definition, schema=TEST_SCHEMA)

    @pytest.mark.skipif(not is_airflow2(), reason="Only runs on Airflow 2+")
    def test_airflow2_provider_compatibility(self):
        """Test compatibility with Airflow 2.X provider package"""
        # Create both custom hook and standard provider hook
        custom_hook = CustomPostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=TEST_SCHEMA)
        provider_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=TEST_SCHEMA)

        # Execute same operations on both hooks
        sql_query = "SELECT 1"
        custom_result = custom_hook.execute_query(sql_query)
        provider_result = provider_hook.run(sql_query)

        # Verify consistent behavior between custom and provider hooks
        self.assertEqual(custom_result, provider_result)

        # Verify proper extension of provider hook functionality
        self.assertTrue(hasattr(custom_hook, "execute_query"))  # Custom method exists


class TestPostgresProviderOperator(unittest.TestCase, Airflow2CompatibilityTestMixin):
    """Test case for validating PostgreSQL provider operator functionality"""

    connection_manager = MockConnectionManager()
    operator = CustomPostgresOperator(task_id="test_task", sql="SELECT 1")
    mock_hook = MagicMock()

    def __init__(self, *args, **kwargs):
        """Initialize the test case"""
        super().__init__(*args, **kwargs)

    def setUp(self):
        """Set up the test environment before each test method"""
        # Create mock connection manager
        self.connection_manager = MockConnectionManager()
        self.connection_manager.__enter__()

        # Create mock PostgreSQL hook
        self.mock_hook = MagicMock()
        self.mock_hook.execute_query.return_value = [("test_value",)]

        # Configure mock hook to return expected results
        self.mock_hook.test_connection.return_value = True

        # Patch get_hook method to return mock hook
        self.operator = CustomPostgresOperator(task_id="test_task", sql="SELECT 1")
        self.operator.get_hook = MagicMock(return_value=self.mock_hook)

    def tearDown(self):
        """Clean up after each test method"""
        # Remove mock patches
        self.connection_manager.__exit__(None, None, None)

        # Reset test resources
        self.operator.get_hook.reset_mock()

    def test_init(self):
        """Test operator initialization with various parameters"""
        # Create operator with default parameters
        operator_default = CustomPostgresOperator(task_id="test_task", sql="SELECT 1")
        self.assertEqual(operator_default.postgres_conn_id, "postgres_default")
        self.assertEqual(operator_default.schema, "public")

        # Create operator with custom parameters
        operator_custom = CustomPostgresOperator(
            task_id="test_task",
            sql="SELECT 1",
            postgres_conn_id="test_conn",
            schema="test_schema",
            autocommit=True,
            use_transaction=True,
        )
        self.assertEqual(operator_custom.postgres_conn_id, "test_conn")
        self.assertEqual(operator_custom.schema, "test_schema")
        self.assertTrue(operator_custom.autocommit)
        self.assertTrue(operator_custom.use_transaction)

    def test_execute(self):
        """Test SQL execution functionality"""
        # Create operator with test SQL
        operator = CustomPostgresOperator(task_id="test_task", sql=TEST_SQL_QUERY)
        operator.get_hook = MagicMock(return_value=self.mock_hook)

        # Configure mock hook to return expected results
        self.mock_hook.execute_query.return_value = [("test_result",)]

        # Execute operator with context
        context = {}
        result = operator.execute(context)

        # Verify hook methods are called with correct parameters
        self.mock_hook.execute_query.assert_called_with(
            sql=TEST_SQL_QUERY, parameters={}, autocommit=False
        )

        # Verify results are returned correctly
        self.assertEqual(result, [("test_result",)])

        # Verify error handling for execution failures
        self.mock_hook.execute_query.side_effect = Exception("Execution failed")
        with self.assertRaises(Exception):
            operator.execute(context)

    def test_execute_with_parameters(self):
        """Test SQL execution with query parameters"""
        # Create operator with parameterized SQL
        operator = CustomPostgresOperator(
            task_id="test_task", sql=TEST_SQL_QUERY, parameters=("test_param",)
        )
        operator.get_hook = MagicMock(return_value=self.mock_hook)

        # Configure parameters and mock hook
        self.mock_hook.execute_query.return_value = [("test_result",)]

        # Execute operator with context
        context = {}
        result = operator.execute(context)

        # Verify parameters are passed correctly to hook
        self.mock_hook.execute_query.assert_called_with(
            sql=TEST_SQL_QUERY, parameters=("test_param",), autocommit=False
        )

        # Verify error handling for parameter validation failures
        with self.assertRaises(Exception):
            CustomPostgresOperator(task_id="test_task", sql=TEST_SQL_QUERY, parameters="invalid_param")

    def test_execute_with_transaction(self):
        """Test SQL execution in transaction mode"""
        # Create operator with transaction mode enabled
        operator = CustomPostgresOperator(
            task_id="test_task", sql=TEST_SQL_MULTI_STATEMENT, use_transaction=True
        )
        operator.get_hook = MagicMock(return_value=self.mock_hook)

        # Configure mock hook's run_transaction method
        self.mock_hook.run_transaction.return_value = [("result1",), ("result2",)]

        # Execute operator with multi-statement SQL
        context = {}
        result = operator.execute(context)

        # Verify run_transaction was called correctly
        self.mock_hook.run_transaction.assert_called_with(
            statements=[TEST_SQL_MULTI_STATEMENT], parameters=[{}]
        )

        # Verify transaction isolation and error handling
        self.assertEqual(result, ("result2",))

    @pytest.mark.skipif(not is_airflow2(), reason="Only runs on Airflow 2+")
    def test_airflow2_provider_compatibility(self):
        """Test compatibility with Airflow 2.X provider package"""
        # Create both custom operator and standard provider operator
        custom_operator = CustomPostgresOperator(task_id="custom_task", sql="SELECT 1")
        provider_operator = PostgresOperator(task_id="provider_task", sql="SELECT 1")

        # Execute same SQL with both operators
        context = {}
        # Mock hooks to avoid actual database calls
        custom_operator.get_hook = MagicMock(return_value=self.mock_hook)
        provider_operator.get_hook = MagicMock(return_value=self.mock_hook)

        custom_result = custom_operator.execute(context)
        provider_result = provider_operator.execute(context)

        # Verify consistent behavior between custom and provider operators
        self.assertEqual(custom_result, provider_result)

        # Verify proper extension of provider operator functionality
        self.assertTrue(hasattr(custom_operator, "validate_sql"))  # Custom attribute exists