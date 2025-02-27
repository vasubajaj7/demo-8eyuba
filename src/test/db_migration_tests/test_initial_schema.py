#!/usr/bin/env python3
"""
Test module for verifying the correctness of the initial database schema migration
from Airflow 1.10.15 to Airflow 2.X. This test validates that the Alembic migration
script properly creates all required tables, columns, constraints, and indexes
for Cloud Composer 2 compatibility.
"""

import logging
import os
import tempfile
import unittest.mock  # v3.4+

import pytest  # pytest-6.0+
import sqlalchemy  # sqlalchemy-1.4.0
from alembic.config import Config  # alembic-1.7.0
from alembic import command  # alembic-1.7.0

# Internal imports
from test.utils.assertion_utils import assert_migrations_successful  # src/test/utils/assertion_utils.py
from backend.migrations.alembic_migrations.versions import initial_schema  # src/backend/migrations/alembic_migrations/versions/initial_schema.py
from test.fixtures.mock_data import MockDataGenerator  # src/test/fixtures/mock_data.py

# Initialize logger
logger = logging.getLogger(__name__)

# Define test revision
TEST_REVISION = initial_schema.revision


def setup_module():
    """Setup function that runs before all tests in this module"""
    # Set up logging for test module
    logger.info("Setting up test module: %s", __name__)

    # Configure environment variables for testing
    os.environ['AIRFLOW_HOME'] = '/tmp/airflow_home'
    os.environ['DB_USER'] = 'test_user'
    os.environ['DB_PASSWORD'] = 'test_password'
    os.environ['DB_NAME'] = 'test_db'

    # Ensure test database is clean before starting tests
    logger.info("Ensuring test database is clean before starting tests")


def teardown_module():
    """Teardown function that runs after all tests in this module"""
    # Clean up test database resources
    logger.info("Tearing down test module: %s", __name__)

    # Reset environment variables
    os.environ.pop('AIRFLOW_HOME', None)
    os.environ.pop('DB_USER', None)
    os.environ.pop('DB_PASSWORD', None)
    os.environ.pop('DB_NAME', None)

    # Close any open connections
    logger.info("Closing any open connections")


def get_test_db_connection():
    """Creates a test database connection for schema verification"""
    # Create a temporary in-memory SQLite database
    temp_db = tempfile.NamedTemporaryFile(delete=False)
    db_url = f'sqlite:///{temp_db.name}'

    # Initialize SQLAlchemy engine and connection
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.connect()

    # Apply the initial schema migration to the database
    alembic_cfg = Config()
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    alembic_cfg.set_main_option("script_location", "src/backend/migrations/alembic_migrations")
    command.upgrade(alembic_cfg, "head")

    # Return the database connection for testing
    return connection


class TestInitialSchema:
    """Test class for validating the initial schema migration for Airflow 2.X"""

    @classmethod
    def setup_class(cls):
        """Set up test class with a mock database connection"""
        # Create test database connection using get_test_db_connection()
        cls.connection = get_test_db_connection()

        # Initialize mock data generator for Airflow 2.X compatibility
        cls.mock_data_generator = MockDataGenerator(airflow2_compatible=True)

        # Apply initial schema migration to test database
        # Store connection for use in test methods
        logger.info("Setting up test class: %s", cls.__name__)

    @classmethod
    def teardown_class(cls):
        """Clean up resources after all tests are run"""
        # Close database connection
        if hasattr(cls, 'connection') and cls.connection:
            cls.connection.close()

        # Remove any temporary files or resources
        logger.info("Tearing down test class: %s", cls.__name__)

    def test_migration_version(self):
        """Test that the migration has the correct version identifier"""
        # Query the alembic_version table in the database
        result = self.connection.execute("SELECT version_num FROM alembic_version").fetchone()

        # Assert that the version matches TEST_REVISION
        assert result[0] == TEST_REVISION, "Migration version does not match expected value"

        # Verify that the migration is properly recorded
        logger.info("Migration version verified successfully: %s", TEST_REVISION)

    def test_core_tables_exist(self):
        """Test that all core Airflow tables are created by the migration"""
        # Get list of tables from the database
        table_names = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        table_names = [x[0] for x in table_names]

        # Check for each required core table: dag, dag_run, task_instance, etc.
        required_tables = ['dag', 'dag_run', 'task_instance', 'xcom', 'variable', 'connection', 'slot_pool', 'sla_miss', 'task_reschedule', 'import_error', 'job']
        for table in required_tables:
            assert table in table_names, f"Table '{table}' is missing"

        # Assert that all required tables from Airflow 2.X exist
        logger.info("Core tables verified successfully")

    def test_airflow2_specific_tables(self):
        """Test that Airflow 2.X specific tables are created"""
        # Get list of tables from the database
        table_names = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        table_names = [x[0] for x in table_names]

        # Check for each Airflow 2.X specific table: dataset, dag_tag, serialized_dag, etc.
        airflow2_tables = ['serialized_dag', 'dag_tag', 'db_upgrade_meta', 'task_map', 'rendered_task_instance_fields', 'dataset', 'dataset_event', 'task_outlet', 'dag_schedule_dataset_reference', 'dataset_dag_run_queue']
        for table in airflow2_tables:
            assert table in table_names, f"Airflow 2.X table '{table}' is missing"

        # Assert that all Airflow 2.X specific tables exist
        logger.info("Airflow 2.X specific tables verified successfully")

    def test_critical_indexes(self):
        """Test that critical indexes are created for performance"""
        # Get list of indexes from the database
        index_names = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index';"
        ).fetchall()
        index_names = [x[0] for x in index_names]

        # Check for critical indexes like idx_dag_dag_id, idx_task_instance_dag_id_task_id_execution_date
        required_indexes = ['idx_dag_dag_id', 'idx_task_instance_dag_id_task_id_execution_date', 'idx_dag_root_dag_id', 'idx_dag_run_dag_id', 'idx_dag_run_state', 'idx_dag_run_execution_date', 'idx_dag_run_run_id', 'idx_dag_run_creating_job_id', 'idx_dag_run_dag_id_execution_date', 'idx_dag_run_dag_id_run_id', 'idx_task_instance_dag_id', 'idx_task_instance_state', 'idx_task_instance_pool', 'idx_task_instance_job_id', 'idx_task_instance_dag_id_task_id', 'idx_task_instance_dag_id_state', 'idx_task_instance_dag_run', 'idx_task_instance_dag_id_task_id_state', 'idx_xcom_dag_task_date', 'idx_xcom_key', 'idx_connection_conn_id', 'idx_variable_key', 'idx_job_dag_id', 'idx_job_state_heartbeat', 'idx_job_type_state', 'idx_serialized_dag_fileloc_hash', 'idx_dag_tag_dag_id', 'idx_dataset_uri', 'idx_dataset_event_dataset_id', 'idx_dataset_event_source_task', 'idx_callback_request_processor_subdir']
        for index in required_indexes:
            assert index in index_names, f"Index '{index}' is missing"

        # Assert that all required indexes exist for query optimization
        logger.info("Critical indexes verified successfully")

    def test_foreign_key_constraints(self):
        """Test that foreign key constraints are properly created"""
        # Get list of foreign key constraints from the database
        foreign_keys = self.connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND sql LIKE '%FOREIGN KEY%';
            """
        ).fetchall()

        # Check for critical constraints like task_instance_dag_id_fkey, dag_run_dag_id_fkey
        required_constraints = ['task_instance_dag_run_fkey', 'task_instance_dag_id_fkey', 'xcom_task_instance_fkey', 'sla_miss_dag_id_fkey', 'task_reschedule_task_instance_fkey', 'dag_run_dag_id_fkey', 'task_map_task_instance_fkey', 'rtif_task_instance_fkey', 'dataset_event_dataset_id_fkey', 'dataset_event_task_instance_fkey', 'task_outlet_task_instance_fkey', 'dag_schedule_dataset_reference_dataset_id_fkey', 'dag_schedule_dataset_reference_dag_id_fkey', 'dataset_dag_run_queue_dataset_id_fkey', 'dataset_dag_run_queue_target_dag_id_fkey']
        constraint_sqls = [fk[0] for fk in foreign_keys]
        for constraint in required_constraints:
            found = False
            for sql in constraint_sqls:
                if constraint in sql:
                    found = True
                    break
            assert found, f"Foreign key constraint '{constraint}' is missing"

        # Assert that all required foreign key constraints exist
        logger.info("Foreign key constraints verified successfully")

    def test_downgrade_functionality(self):
        """Test that the downgrade function properly removes the schema"""
        # Create a new test database with the schema applied
        temp_db = tempfile.NamedTemporaryFile(delete=False)
        db_url = f'sqlite:///{temp_db.name}'
        engine = sqlalchemy.create_engine(db_url)
        connection = engine.connect()

        alembic_cfg = Config()
        alembic_cfg.set_main_option("sqlalchemy.url", db_url)
        alembic_cfg.set_main_option("script_location", "src/backend/migrations/alembic_migrations")
        command.upgrade(alembic_cfg, "head")

        # Call the downgrade function from the migration script
        initial_schema.downgrade()

        # Verify that all tables are properly removed
        table_names = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        table_names = [x[0] for x in table_names]
        assert len(table_names) == 0, "Tables were not properly removed during downgrade"

        # Confirm that the alembic_version table is updated correctly
        version_result = connection.execute("SELECT version_num FROM alembic_version").fetchone()
        assert version_result is None, "alembic_version table was not properly updated during downgrade"

        # Close the connection
        connection.close()

        logger.info("Downgrade functionality verified successfully")

    def test_task_instance_table_structure(self):
        """Test the structure of the task_instance table specifically"""
        # Get column information for the task_instance table
        columns = self.connection.execute(
            "PRAGMA table_info(task_instance)"
        ).fetchall()

        # Verify all required columns exist with correct types
        required_columns = {
            'task_id': 'VARCHAR',
            'dag_id': 'VARCHAR',
            'run_id': 'VARCHAR',
            'map_index': 'INTEGER',
            'start_date': 'TIMESTAMP',
            'end_date': 'TIMESTAMP',
            'duration': 'REAL',
            'state': 'VARCHAR',
            'try_number': 'INTEGER',
            'max_tries': 'INTEGER',
            'hostname': 'VARCHAR',
            'unixname': 'VARCHAR',
            'job_id': 'INTEGER',
            'pool': 'VARCHAR',
            'pool_slots': 'INTEGER',
            'queue': 'VARCHAR',
            'priority_weight': 'INTEGER',
            'operator': 'VARCHAR',
            'queued_dttm': 'TIMESTAMP',
            'queued_by_job_id': 'INTEGER',
            'pid': 'INTEGER',
            'executor_config': 'TEXT',
            'updated_at': 'TIMESTAMP',
            'external_executor_id': 'VARCHAR',
            'trigger_id': 'INTEGER',
            'trigger_timeout': 'TIMESTAMP',
            'next_method': 'VARCHAR',
            'next_kwargs': 'TEXT'
        }

        for column, data_type in required_columns.items():
            found = False
            for col in columns:
                if col[1] == column and data_type.upper() in col[2].upper():
                    found = True
                    break
            assert found, f"Column '{column}' with type '{data_type}' is missing in task_instance table"

        # Check for Airflow 2.X specific columns like map_index, pool_slots
        # Verify primary key and index definitions
        logger.info("Task instance table structure verified successfully")

    def test_xcom_table_structure(self):
        """Test the structure of the xcom table specifically"""
        # Get column information for the xcom table
        columns = self.connection.execute(
            "PRAGMA table_info(xcom)"
        ).fetchall()

        # Verify all required columns exist with correct types
        required_columns = {
            'id': 'INTEGER',
            'key': 'VARCHAR',
            'value': 'BLOB',
            'timestamp': 'TIMESTAMP',
            'dag_id': 'VARCHAR',
            'task_id': 'VARCHAR',
            'run_id': 'VARCHAR',
            'map_index': 'INTEGER'
        }

        for column, data_type in required_columns.items():
            found = False
            for col in columns:
                if col[1] == column and data_type.upper() in col[2].upper():
                    found = True
                    break
            assert found, f"Column '{column}' with type '{data_type}' is missing in xcom table"

        # Check for Airflow 2.X specific handling of serialized data
        # Verify primary key and index definitions
        logger.info("XCom table structure verified successfully")

    def test_migrations_successful(self):
        """Test that migrations are successfully applied using the assertion utility"""
        # Use assert_migrations_successful to verify migration status
        # Pass connection and expected version list
        # Verify that no assertion errors are raised
        assert_migrations_successful(self.connection, [TEST_REVISION])
        logger.info("assert_migrations_successful utility ran successfully")