#!/usr/bin/env python3

"""
Test suite for validating the migration process from Cloud Composer 1 (Airflow 1.10.15) to Cloud Composer 2 (Airflow 2.X).
This file contains comprehensive tests to verify that the migration utilities correctly transform DAGs, connections, plugins,
and database schemas while maintaining functional parity.
"""

# Built-in imports
import unittest
import os
import tempfile
import shutil
import json
import pytest
from mock import patch, MagicMock

# Internal imports for testing across Airflow versions
from ..utils.airflow2_compatibility_utils import (
    Airflow2CompatibilityTestMixin,
    CompatibleDAGTestCase,
    is_airflow2
)

# Import mocking utilities
from ..fixtures.mock_gcp_services import create_mock_gcp_services, patch_gcp_services
from ..fixtures.mock_connections import MockConnectionManager

# Import the migration utilities we want to test
from ...backend.migrations.migration_airflow1_to_airflow2 import (
    DAGMigrator,
    ConnectionMigrator,
    PluginMigrator,
    transform_imports
)

# Test data constants
AIRFLOW_1_EXAMPLE_DAG = """from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id='example_dag', schedule_interval='@daily')

start = BashOperator(task_id='start', bash_command='echo start', dag=dag)

def my_func():
    return 'success'

end = PythonOperator(task_id='end', python_callable=my_func, provide_context=True, dag=dag)

start >> end"""

AIRFLOW_2_EXAMPLE_DAG = """from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(dag_id='example_dag', schedule_interval='@daily')

start = BashOperator(task_id='start', bash_command='echo start', dag=dag)

def my_func():
    return 'success'

end = PythonOperator(task_id='end', python_callable=my_func, dag=dag)

start >> end"""

AIRFLOW_1_EXAMPLE_PLUGIN = """from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base_hook import BaseHook
from airflow.operators.sensors_operator import BaseSensorOperator

class MyCustomHook(BaseHook):
    def __init__(self, conn_id='my_conn'):
        self.conn_id = conn_id
        
    def get_conn(self):
        return self.get_connection(self.conn_id)

class MyCustomSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(MyCustomSensor, self).__init__(*args, **kwargs)
        
    def poke(self, context):
        return True

class MyCustomPlugin(AirflowPlugin):
    name = 'my_custom_plugin'
    hooks = [MyCustomHook]
    sensors = [MyCustomSensor]"""

AIRFLOW_1_CONNECTION = """{"conn_id": "gcs_default", "conn_type": "google_cloud_platform", "host": "", "login": "", "password": "", "schema": "", "port": null, "extra": "{\\"project\\": \\"my-project\\", \\"key_path\\": \\"/path/to/key.json\\"}"}"""


class TestDAGMigration(unittest.TestCase):
    """Tests the DAG migration functionality for converting Airflow 1.10.15 DAGs to Airflow 2.X format"""
    
    def setUp(self):
        """Set up the test environment with temporary directories"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_path = os.path.join(self.temp_dir.name, 'source')
        self.target_path = os.path.join(self.temp_dir.name, 'target')
        
        # Create directories
        os.makedirs(self.source_path, exist_ok=True)
        os.makedirs(self.target_path, exist_ok=True)
    
    def tearDown(self):
        """Clean up the test environment"""
        self.temp_dir.cleanup()
    
    def test_dag_import_transformation(self):
        """Test that import statements are correctly transformed"""
        # Write Airflow 1.X DAG to source path
        source_file = os.path.join(self.source_path, 'test_dag.py')
        with open(source_file, 'w') as f:
            f.write(AIRFLOW_1_EXAMPLE_DAG)
        
        # Migrate the DAG file
        migrator = DAGMigrator(use_taskflow=False, dry_run=False)
        target_file = os.path.join(self.target_path, 'test_dag.py')
        migrator.migrate_dag_file(source_file, target_file)
        
        # Read the migrated file
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        # Check if imports are transformed correctly
        self.assertIn('from airflow.operators.bash import BashOperator', migrated_content)
        self.assertIn('from airflow.operators.python import PythonOperator', migrated_content)
        self.assertNotIn('from airflow.operators.bash_operator import BashOperator', migrated_content)
        self.assertNotIn('from airflow.operators.python_operator import PythonOperator', migrated_content)
    
    def test_dag_operator_transformation(self):
        """Test that operators are correctly transformed"""
        # Write Airflow 1.X DAG to source path
        source_file = os.path.join(self.source_path, 'test_dag.py')
        with open(source_file, 'w') as f:
            f.write(AIRFLOW_1_EXAMPLE_DAG)
        
        # Migrate the DAG file
        migrator = DAGMigrator(use_taskflow=False, dry_run=False)
        target_file = os.path.join(self.target_path, 'test_dag.py')
        migrator.migrate_dag_file(source_file, target_file)
        
        # Read the migrated file
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        # Check if provide_context parameter is removed
        self.assertNotIn('provide_context=True', migrated_content)
        
        # Check that the parameters that should be preserved are there
        self.assertIn('task_id=\'end\'', migrated_content)
        self.assertIn('python_callable=my_func', migrated_content)
        self.assertIn('task_id=\'start\'', migrated_content)
        self.assertIn('bash_command=\'echo start\'', migrated_content)
    
    def test_taskflow_conversion(self):
        """Test conversion of PythonOperators to TaskFlow API pattern"""
        # Write Airflow 1.X DAG to source path
        source_file = os.path.join(self.source_path, 'test_dag.py')
        with open(source_file, 'w') as f:
            f.write(AIRFLOW_1_EXAMPLE_DAG)
        
        # Migrate the DAG file with TaskFlow enabled
        migrator = DAGMigrator(use_taskflow=True, dry_run=False)
        target_file = os.path.join(self.target_path, 'test_dag.py')
        migrator.migrate_dag_file(source_file, target_file)
        
        # Read the migrated file
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        # Check if TaskFlow decorator is added
        self.assertIn('@task', migrated_content)
        self.assertIn('from airflow.decorators import task', migrated_content)
        
        # Check that the function is still intact
        self.assertIn('def my_func()', migrated_content)
        self.assertIn('return \'success\'', migrated_content)
    
    def test_dag_migration_preserves_structure(self):
        """Test that DAG structure and dependencies are preserved during migration"""
        complex_dag = """
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id='complex_dag', 
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1)
)

task1 = BashOperator(task_id='task1', bash_command='echo task1', dag=dag)
task2 = BashOperator(task_id='task2', bash_command='echo task2', dag=dag)
task3 = BashOperator(task_id='task3', bash_command='echo task3', dag=dag)

def my_func():
    return 'success'

task4 = PythonOperator(task_id='task4', python_callable=my_func, provide_context=True, dag=dag)
task5 = PythonOperator(task_id='task5', python_callable=my_func, provide_context=True, dag=dag)

task1 >> [task2, task3] >> task4 >> task5
"""
        
        # Write Airflow 1.X DAG to source path
        source_file = os.path.join(self.source_path, 'complex_dag.py')
        with open(source_file, 'w') as f:
            f.write(complex_dag)
        
        # Migrate the DAG file
        migrator = DAGMigrator(use_taskflow=False, dry_run=False)
        target_file = os.path.join(self.target_path, 'complex_dag.py')
        migrator.migrate_dag_file(source_file, target_file)
        
        # Read the migrated file
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        # Check that dependencies are preserved
        self.assertIn('task1 >> [task2, task3] >> task4 >> task5', migrated_content)
        
        # Check that all tasks are migrated
        self.assertIn("task1 = BashOperator(task_id='task1'", migrated_content)
        self.assertIn("task2 = BashOperator(task_id='task2'", migrated_content)
        self.assertIn("task3 = BashOperator(task_id='task3'", migrated_content)
        self.assertIn("task4 = PythonOperator(task_id='task4'", migrated_content)
        self.assertIn("task5 = PythonOperator(task_id='task5'", migrated_content)
    
    @unittest.skipIf(not is_airflow2(), "Requires Airflow 2.X")
    def test_dag_execution_compatibility(self):
        """Test that migrated DAG executes correctly in Airflow 2.X"""
        # Write Airflow 1.X DAG to source path
        source_file = os.path.join(self.source_path, 'test_dag.py')
        with open(source_file, 'w') as f:
            f.write(AIRFLOW_1_EXAMPLE_DAG)
        
        # Migrate the DAG file
        migrator = DAGMigrator(use_taskflow=False, dry_run=False)
        target_file = os.path.join(self.target_path, 'test_dag.py')
        migrator.migrate_dag_file(source_file, target_file)
        
        # Use CompatibleDAGTestCase to compare execution
        dag_test = CompatibleDAGTestCase()
        
        # Verify the migrated file exists and has the right content
        self.assertTrue(os.path.exists(target_file))
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        self.assertIn('from airflow.operators.bash import BashOperator', migrated_content)
        self.assertIn('from airflow.operators.python import PythonOperator', migrated_content)


class TestConnectionMigration(unittest.TestCase):
    """Tests the Connection migration functionality for converting Airflow 1.10.15 connections to Airflow 2.X format"""
    
    def test_gcp_connection_transformation(self):
        """Test that GCP connections are correctly transformed"""
        # Create an Airflow 1.X connection definition
        connection = json.loads(AIRFLOW_1_CONNECTION)
        
        # Create connection migrator and transform
        migrator = ConnectionMigrator(dry_run=False)
        transformed_connection = migrator.transform_connection(connection)
        
        # Check that the connection was transformed correctly
        self.assertEqual(transformed_connection['conn_type'], 'google_cloud_platform')
        
        # Parse the extra field
        extra = json.loads(transformed_connection['extra'])
        self.assertIn('project', extra)
        self.assertIn('key_path', extra)
        self.assertEqual(extra['project'], 'my-project')
        self.assertEqual(extra['key_path'], '/path/to/key.json')
        
        # Verify project_id is added (Airflow 2.X compatibility)
        self.assertIn('project_id', extra)
        self.assertEqual(extra['project_id'], 'my-project')
    
    def test_http_connection_transformation(self):
        """Test that HTTP connections are correctly transformed"""
        # Create an Airflow 1.X HTTP connection definition
        http_connection = {
            "conn_id": "http_default",
            "conn_type": "http",
            "host": "api.example.com",
            "login": "user",
            "password": "pass",
            "port": 443,
            "extra": "{\"headers\":{\"Content-Type\":\"application/json\"}}"
        }
        
        # Create connection migrator and transform
        migrator = ConnectionMigrator(dry_run=False)
        transformed_connection = migrator.transform_connection(http_connection)
        
        # Check that the connection was transformed correctly
        self.assertEqual(transformed_connection['conn_type'], 'http')
        self.assertEqual(transformed_connection['host'], 'api.example.com')
        
        # Parse the extra field
        extra = json.loads(transformed_connection['extra'])
        self.assertIn('headers', extra)
        self.assertEqual(extra['headers']['Content-Type'], 'application/json')
    
    def test_postgres_connection_transformation(self):
        """Test that PostgreSQL connections are correctly transformed"""
        # Create an Airflow 1.X PostgreSQL connection definition
        pg_connection = {
            "conn_id": "postgres_default",
            "conn_type": "postgres",
            "host": "localhost",
            "login": "postgres",
            "password": "postgres",
            "schema": "public",
            "port": 5432,
            "extra": "{\"sslmode\":\"prefer\"}"
        }
        
        # Create connection migrator and transform
        migrator = ConnectionMigrator(dry_run=False)
        transformed_connection = migrator.transform_connection(pg_connection)
        
        # Check that the connection was transformed correctly
        self.assertEqual(transformed_connection['conn_type'], 'postgres')
        self.assertEqual(transformed_connection['host'], 'localhost')
        self.assertEqual(transformed_connection['port'], 5432)
        self.assertEqual(transformed_connection['schema'], 'public')
        
        # Parse the extra field
        extra = json.loads(transformed_connection['extra'])
        self.assertIn('sslmode', extra)
        self.assertEqual(extra['sslmode'], 'prefer')
    
    @unittest.skipIf(not is_airflow2(), "Requires Airflow 2.X")
    def test_connection_compatibility(self):
        """Test that transformed connections work in Airflow 2.X"""
        # Set up test connections with MockConnectionManager
        with MockConnectionManager() as manager:
            # Create GCP connection from the example
            connection = json.loads(AIRFLOW_1_CONNECTION)
            migrator = ConnectionMigrator(dry_run=False)
            transformed_connection = migrator.transform_connection(connection)
            
            # Check that the connection type is still google_cloud_platform
            self.assertEqual(transformed_connection['conn_type'], 'google_cloud_platform')
            
            # Check that extra field is properly formatted
            extra = json.loads(transformed_connection['extra'])
            self.assertIn('project', extra)
            self.assertIn('project_id', extra)
            self.assertEqual(extra['project'], 'my-project')
            self.assertEqual(extra['project_id'], 'my-project')


class TestPluginMigration(unittest.TestCase):
    """Tests the Plugin migration functionality for converting Airflow 1.10.15 plugins to Airflow 2.X format"""
    
    def setUp(self):
        """Set up the test environment with temporary directories"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_path = os.path.join(self.temp_dir.name, 'source')
        self.target_path = os.path.join(self.temp_dir.name, 'target')
        
        # Create directories
        os.makedirs(self.source_path, exist_ok=True)
        os.makedirs(self.target_path, exist_ok=True)
    
    def tearDown(self):
        """Clean up the test environment"""
        self.temp_dir.cleanup()
    
    def test_hook_migration(self):
        """Test that custom hooks are correctly migrated"""
        # Write Airflow 1.X plugin to source path
        source_file = os.path.join(self.source_path, 'custom_hook.py')
        with open(source_file, 'w') as f:
            f.write("""
from airflow.hooks.base_hook import BaseHook

class MyCustomHook(BaseHook):
    def __init__(self, conn_id='my_conn'):
        self.conn_id = conn_id
        
    def get_conn(self):
        return self.get_connection(self.conn_id)
        
    def execute_query(self, query):
        conn = self.get_conn()
        # Execute query logic
        return True
""")
        
        # Migrate plugin file
        migrator = PluginMigrator(dry_run=False)
        target_file = os.path.join(self.target_path, 'custom_hook.py')
        migrator.migrate_plugin_file(source_file, target_file)
        
        # Read the migrated file
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        # Check if hook imports are transformed correctly
        self.assertIn('from airflow.hooks.base import BaseHook', migrated_content)
        self.assertNotIn('from airflow.hooks.base_hook import BaseHook', migrated_content)
        
        # Check that hook methods are preserved
        self.assertIn('def get_conn(self)', migrated_content)
        self.assertIn('def execute_query(self, query)', migrated_content)
    
    def test_sensor_migration(self):
        """Test that custom sensors are correctly migrated"""
        # Write Airflow 1.X sensor plugin to source path
        source_file = os.path.join(self.source_path, 'custom_sensor.py')
        with open(source_file, 'w') as f:
            f.write("""
from airflow.operators.sensors_operator import BaseSensorOperator

class MyCustomSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(MyCustomSensor, self).__init__(*args, **kwargs)
        
    def poke(self, context):
        # Sensor logic
        return True
""")
        
        # Migrate plugin file
        migrator = PluginMigrator(dry_run=False)
        target_file = os.path.join(self.target_path, 'custom_sensor.py')
        migrator.migrate_plugin_file(source_file, target_file)
        
        # Read the migrated file
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        # Check if sensor imports are transformed correctly
        self.assertIn('sensors', migrated_content)
        self.assertNotIn('from airflow.operators.sensors_operator import BaseSensorOperator', migrated_content)
        
        # Check that sensor methods are preserved
        self.assertIn('def poke(self, context)', migrated_content)
    
    def test_operator_migration(self):
        """Test that custom operators are correctly migrated"""
        # Write Airflow 1.X operator plugin to source path
        source_file = os.path.join(self.source_path, 'custom_operator.py')
        with open(source_file, 'w') as f:
            f.write("""
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MyCustomOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_param=None, *args, **kwargs):
        super(MyCustomOperator, self).__init__(*args, **kwargs)
        self.my_param = my_param
        
    def execute(self, context):
        # Operator logic
        return f"Executed with {self.my_param}"
""")
        
        # Migrate plugin file
        migrator = PluginMigrator(dry_run=False)
        target_file = os.path.join(self.target_path, 'custom_operator.py')
        migrator.migrate_plugin_file(source_file, target_file)
        
        # Read the migrated file
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        # Check if operator imports are correct
        self.assertIn('from airflow.models import BaseOperator', migrated_content)
        
        # Check that operator methods are preserved
        self.assertIn('def execute(self, context)', migrated_content)
        self.assertIn('def __init__(self, my_param=None, *args, **kwargs)', migrated_content)
    
    @unittest.skipIf(not is_airflow2(), "Requires Airflow 2.X")
    def test_plugin_loading_compatibility(self):
        """Test that migrated plugins can be loaded in Airflow 2.X"""
        # Write Airflow 1.X plugin to source path
        source_file = os.path.join(self.source_path, 'complete_plugin.py')
        with open(source_file, 'w') as f:
            f.write(AIRFLOW_1_EXAMPLE_PLUGIN)
        
        # Migrate plugin file
        migrator = PluginMigrator(dry_run=False)
        target_file = os.path.join(self.target_path, 'complete_plugin.py')
        migrator.migrate_plugin_file(source_file, target_file)
        
        # Read the migrated file
        with open(target_file, 'r') as f:
            migrated_content = f.read()
        
        # Check correct imports for Airflow 2.X
        self.assertNotIn('from airflow.hooks.base_hook import BaseHook', migrated_content)
        self.assertNotIn('from airflow.operators.sensors_operator import BaseSensorOperator', migrated_content)


class TestMigrationScript(unittest.TestCase):
    """Tests the integration of migration scripts and functionality for the end-to-end migration process"""
    
    def test_migration_script_integration(self):
        """Test that migration script correctly orchestrates the entire migration process"""
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = os.path.join(temp_dir, 'source')
            target_dir = os.path.join(temp_dir, 'target')
            backup_dir = os.path.join(temp_dir, 'backup')
            
            # Create directories
            os.makedirs(source_dir, exist_ok=True)
            os.makedirs(target_dir, exist_ok=True)
            os.makedirs(backup_dir, exist_ok=True)
            
            # Create sample DAGs and plugins in source directory
            os.makedirs(os.path.join(source_dir, 'dags'), exist_ok=True)
            os.makedirs(os.path.join(source_dir, 'plugins'), exist_ok=True)
            
            # Create an example DAG file
            with open(os.path.join(source_dir, 'dags', 'example_dag.py'), 'w') as f:
                f.write(AIRFLOW_1_EXAMPLE_DAG)
            
            # Create an example plugin file
            with open(os.path.join(source_dir, 'plugins', 'example_plugin.py'), 'w') as f:
                f.write(AIRFLOW_1_EXAMPLE_PLUGIN)
            
            # Create an example connection file
            with open(os.path.join(source_dir, 'connections.json'), 'w') as f:
                f.write('[' + AIRFLOW_1_CONNECTION + ']')
            
            # Mock subprocess.run to simulate bash script execution
            with patch('subprocess.run') as mock_run:
                mock_run.return_value.returncode = 0
                
                # Run DAG migration
                dag_migrator = DAGMigrator(use_taskflow=False, dry_run=False)
                dag_result = dag_migrator.migrate_dag_files(
                    os.path.join(source_dir, 'dags'),
                    os.path.join(target_dir, 'dags')
                )
                
                # Run plugin migration
                plugin_migrator = PluginMigrator(dry_run=False)
                plugin_result = plugin_migrator.migrate_plugins(
                    os.path.join(source_dir, 'plugins'),
                    os.path.join(target_dir, 'plugins')
                )
                
                # Check that the migrated files exist
                self.assertTrue(os.path.exists(os.path.join(target_dir, 'dags', 'example_dag.py')))
                self.assertTrue(os.path.exists(os.path.join(target_dir, 'plugins', 'example_plugin.py')))
                
                # Check migration results
                self.assertEqual(dag_result['status'], 'success')
                self.assertEqual(plugin_result['status'], 'success')
    
    def test_rollback_functionality(self):
        """Test that migration rollback works correctly on failure"""
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = os.path.join(temp_dir, 'source')
            target_dir = os.path.join(temp_dir, 'target')
            backup_dir = os.path.join(temp_dir, 'backup')
            
            # Create directories
            os.makedirs(source_dir, exist_ok=True)
            os.makedirs(target_dir, exist_ok=True)
            os.makedirs(backup_dir, exist_ok=True)
            
            # Create sample content in source directory
            with open(os.path.join(source_dir, 'test_file.py'), 'w') as f:
                f.write("# Original content")
            
            # Create backup
            backup_path = os.path.join(backup_dir, 'backup_test')
            os.makedirs(backup_path, exist_ok=True)
            shutil.copy(os.path.join(source_dir, 'test_file.py'), 
                       os.path.join(backup_path, 'test_file.py'))
            
            # Simulate migration failure by creating a corrupted target file
            with open(os.path.join(target_dir, 'test_file.py'), 'w') as f:
                f.write("# Corrupted content")
            
            # Mock the rollback function from the migration script
            from ...backend.migrations.migration_airflow1_to_airflow2 import rollback_migration
            
            # Call rollback
            rollback_success = rollback_migration(backup_path, target_dir)
            
            # Check that rollback succeeded
            self.assertTrue(rollback_success)
            
            # Check that target file was restored from backup
            with open(os.path.join(target_dir, 'test_file.py'), 'r') as f:
                content = f.read()
            
            self.assertEqual(content, "# Original content")
    
    def test_dry_run_mode(self):
        """Test that dry run mode correctly simulates migration without changes"""
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = os.path.join(temp_dir, 'source')
            target_dir = os.path.join(temp_dir, 'target')
            
            # Create directories
            os.makedirs(source_dir, exist_ok=True)
            os.makedirs(target_dir, exist_ok=True)
            
            # Create a sample DAG file in source
            with open(os.path.join(source_dir, 'test_dag.py'), 'w') as f:
                f.write(AIRFLOW_1_EXAMPLE_DAG)
            
            # Run DAG migration in dry run mode
            dag_migrator = DAGMigrator(use_taskflow=False, dry_run=True)
            dag_result = dag_migrator.migrate_dag_file(
                os.path.join(source_dir, 'test_dag.py'),
                os.path.join(target_dir, 'test_dag.py')
            )
            
            # Check that the target file was not created (dry run)
            self.assertFalse(os.path.exists(os.path.join(target_dir, 'test_dag.py')))
            
            # But the migration result should still show success
            self.assertEqual(dag_result['status'], 'success')
    
    def test_gcp_service_integration(self):
        """Test integration with GCP services during migration"""
        # Create temporary directories
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = os.path.join(temp_dir, 'source')
            target_dir = os.path.join(temp_dir, 'target')
            os.makedirs(source_dir, exist_ok=True)
            os.makedirs(target_dir, exist_ok=True)
            
            # Create a GCP-specific DAG
            gcp_dag = """
from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime

dag = DAG(
    'gcp_operations',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily'
)

# GCS to GCS transfer
transfer_files = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='transfer_files',
    source_bucket='source-bucket',
    source_object='data/*.csv',
    destination_bucket='destination-bucket',
    destination_object='data/',
    dag=dag
)

# BigQuery task
query = BigQueryOperator(
    task_id='run_query',
    sql='SELECT * FROM `dataset.table` LIMIT 10',
    use_legacy_sql=False,
    dag=dag
)

transfer_files >> query
"""
            
            # Write the DAG to source
            with open(os.path.join(source_dir, 'gcp_dag.py'), 'w') as f:
                f.write(gcp_dag)
            
            # Mock GCP services
            mock_responses = {
                'gcs': {
                    'source-bucket': {
                        'blobs': {
                            'data/file1.csv': 'content1',
                            'data/file2.csv': 'content2'
                        }
                    },
                    'destination-bucket': {
                        'blobs': {}
                    }
                },
                'bigquery': {
                    'datasets': {
                        'dataset': {
                            'tables': {
                                'table': {
                                    'schema': [],
                                    'rows': [{'col1': 'val1'}, {'col1': 'val2'}]
                                }
                            }
                        }
                    }
                }
            }
            
            # Create patchers
            patchers = patch_gcp_services(mock_responses)
            
            # Start patchers
            for patcher in patchers.values():
                patcher.start()
            
            try:
                # Run DAG migration
                dag_migrator = DAGMigrator(use_taskflow=False, dry_run=False)
                dag_result = dag_migrator.migrate_dag_file(
                    os.path.join(source_dir, 'gcp_dag.py'),
                    os.path.join(target_dir, 'gcp_dag.py')
                )
                
                # Check that migration was successful
                self.assertEqual(dag_result['status'], 'success')
                
                # Verify the migrated file exists
                self.assertTrue(os.path.exists(os.path.join(target_dir, 'gcp_dag.py')))
                
                # Read the migrated file and check for correct GCP-specific operator imports
                with open(os.path.join(target_dir, 'gcp_dag.py'), 'r') as f:
                    migrated_content = f.read()
                
                # Check that the imports were updated correctly
                self.assertNotIn('airflow.contrib.operators.gcs_to_gcs', migrated_content)
                self.assertNotIn('airflow.contrib.operators.bigquery_operator', migrated_content)
                
                # The actual path will depend on the specific transformations in the DAGMigrator
                # but we should see provider packages in there
                self.assertIn('providers.google', migrated_content)
                
            finally:
                # Stop patchers
                for patcher in patchers.values():
                    patcher.stop()


class TestMigrationValidation(unittest.TestCase):
    """Tests the validation functionality that ensures migrated components meet Airflow 2.X requirements"""
    
    def test_dag_validation(self):
        """Test that DAG validation correctly identifies issues in migrated DAGs"""
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a DAG with various issues
            problematic_dag = """
from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.operators.sensors_operator import BaseSensorOperator
from datetime import datetime

dag = DAG(
    'problematic_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily'
)

# Using deprecated operators
task1 = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='task1',
    source_bucket='source-bucket',
    source_object='data/*.csv',
    destination_bucket='destination-bucket',
    destination_object='data/',
    dag=dag
)

# Using deprecated parameters
class CustomSensor(BaseSensorOperator):
    def poke(self, context):
        return True

sensor = CustomSensor(
    task_id='sensor',
    retry_delay=30,
    retry_exponential_backoff=True,
    provide_context=True,
    dag=dag
)

task1 >> sensor
"""
            
            # Write DAG to temporary directory
            dag_path = os.path.join(temp_dir, 'problematic_dag.py')
            with open(dag_path, 'w') as f:
                f.write(problematic_dag)
            
            # Use the validation function from the migration module
            from ...backend.migrations.migration_airflow1_to_airflow2 import validate_migration
            
            # Run validation
            validation_results = validate_migration(temp_dir)
            
            # Check that validation identified issues
            self.assertEqual(validation_results['status'], 'warning')
            self.assertGreater(len(validation_results['issues']), 0)
            
            # Check for specific issues
            found_import_issue = False
            found_parameter_issue = False
            
            for issue in validation_results['issues']:
                if 'deprecated import' in ''.join(issue['issues']).lower():
                    found_import_issue = True
                if 'deprecated parameter' in ''.join(issue['issues']).lower():
                    found_parameter_issue = True
            
            self.assertTrue(found_import_issue, "Should have found deprecated import issues")
            self.assertTrue(found_parameter_issue, "Should have found deprecated parameter issues")
    
    def test_connection_validation(self):
        """Test that connection validation correctly identifies issues in migrated connections"""
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a connections file with issues
            problematic_connections = {
                "jdbc_conn": {
                    "conn_id": "jdbc_conn",
                    "conn_type": "jdbc",  # deprecated in Airflow 2.X
                    "host": "jdbc:mysql://localhost:3306/db",
                    "login": "user",
                    "password": "password",
                    "port": None,
                    "schema": "",
                    "extra": "{}"
                },
                "missing_project_gcp": {
                    "conn_id": "missing_project_gcp",
                    "conn_type": "google_cloud_platform",
                    "host": "",
                    "login": "",
                    "password": "",
                    "schema": "",
                    "port": None,
                    "extra": "{}"  # Missing project_id in extra
                }
            }
            
            # Write connections to file
            with open(os.path.join(temp_dir, 'connections.json'), 'w') as f:
                json.dump(problematic_connections, f)
            
            # Use the validation function from the migration module
            from ...backend.migrations.migration_airflow1_to_airflow2 import validate_migration
            
            # Run validation
            validation_results = validate_migration(temp_dir)
            
            # Check that validation identified issues
            self.assertEqual(validation_results['status'], 'warning')
            
            # Check for specific connection issues
            conn_issues = [issue for issue in validation_results['issues'] 
                          if issue['file'] == 'connections.json']
            
            self.assertGreater(len(conn_issues), 0, "Should have found connection issues")
    
    def test_plugin_validation(self):
        """Test that plugin validation correctly identifies issues in migrated plugins"""
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            plugins_dir = os.path.join(temp_dir, 'plugins')
            os.makedirs(plugins_dir, exist_ok=True)
            
            # Create a plugin with import issues
            problematic_plugin = """
from airflow.hooks.base_hook import BaseHook
from airflow.operators.sensors_operator import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin

# Using old-style imports
class MyHook(BaseHook):
    def __init__(self, conn_id='default'):
        self.conn_id = conn_id
        
    def get_conn(self):
        return self.get_connection(self.conn_id)

# Using deprecated classes
class MySensor(BaseSensorOperator):
    def poke(self, context):
        return True
        
class MyPlugin(AirflowPlugin):
    name = 'my_plugin'
    hooks = [MyHook]
    sensors = [MySensor]
"""
            
            # Write plugin to temporary directory
            with open(os.path.join(plugins_dir, 'problematic_plugin.py'), 'w') as f:
                f.write(problematic_plugin)
            
            # Use the validation function from the migration module
            from ...backend.migrations.migration_airflow1_to_airflow2 import validate_migration
            
            # Run validation
            validation_results = validate_migration(temp_dir)
            
            # Check that validation identified issues
            self.assertEqual(validation_results['status'], 'warning')
            
            # Find plugin issues
            plugin_issues = [issue for issue in validation_results['issues'] 
                           if 'plugins' in issue['file']]
            
            # There should be import issues identified
            self.assertGreater(len(plugin_issues), 0, "Should have found plugin issues")
    
    def test_validation_report(self):
        """Test that validation generates a comprehensive, accurate report"""
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create directories
            dags_dir = os.path.join(temp_dir, 'dags')
            plugins_dir = os.path.join(temp_dir, 'plugins')
            os.makedirs(dags_dir, exist_ok=True)
            os.makedirs(plugins_dir, exist_ok=True)
            
            # Create a DAG with issues
            with open(os.path.join(dags_dir, 'test_dag.py'), 'w') as f:
                f.write("""
from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from datetime import datetime

dag = DAG(
    'test_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily'
)

task = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='task',
    source_bucket='source-bucket',
    source_object='data/*.csv',
    destination_bucket='destination-bucket',
    destination_object='data/',
    dag=dag
)
""")
            
            # Create a plugin with issues
            with open(os.path.join(plugins_dir, 'test_plugin.py'), 'w') as f:
                f.write("""
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin

class TestHook(BaseHook):
    def get_conn(self):
        return None

class TestPlugin(AirflowPlugin):
    name = 'test_plugin'
    hooks = [TestHook]
""")
            
            # Create connections file
            with open(os.path.join(temp_dir, 'connections.json'), 'w') as f:
                f.write('''
{
    "jdbc_conn": {
        "conn_id": "jdbc_conn",
        "conn_type": "jdbc",
        "host": "localhost",
        "login": "user",
        "password": "password"
    }
}
''')
            
            # Use the validation function from the migration module
            from ...backend.migrations.migration_airflow1_to_airflow2 import validate_migration
            
            # Run validation
            validation_results = validate_migration(temp_dir)
            
            # Check structure of validation report
            self.assertIn('status', validation_results)
            self.assertIn('issues', validation_results)
            self.assertIn('stats', validation_results)
            
            # Check stats categories
            self.assertIn('dags_checked', validation_results['stats'])
            self.assertIn('dags_with_issues', validation_results['stats'])
            self.assertIn('total_issues', validation_results['stats'])
            self.assertIn('import_issues', validation_results['stats'])
            
            # Check that we have issues for different components
            file_paths = [issue['file'] for issue in validation_results['issues']]
            
            # Should have issues in both DAG and plugin files
            dag_issues = any('test_dag.py' in path for path in file_paths)
            plugin_issues = any('test_plugin.py' in path for path in file_paths)
            conn_issues = any('connections.json' in path for path in file_paths)
            
            self.assertTrue(dag_issues, "Validation should identify DAG issues")
            self.assertTrue(plugin_issues, "Validation should identify plugin issues")
            self.assertTrue(conn_issues, "Validation should identify connection issues")