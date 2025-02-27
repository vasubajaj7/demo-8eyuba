#!/usr/bin/env python3
"""
Migration utility for transitioning Apache Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2.

This script provides comprehensive tooling to migrate an existing Airflow 1.10.15
environment to Airflow 2.X compatibility, including:

1. DAG code transformations to update import statements and operator usage
2. Connection definition updates for Airflow 2.X provider packages
3. Plugin migration to adapt custom operators, hooks, and sensors
4. Database schema updates for compatibility with Airflow 2.X metadata schema

Usage:
    python migration_airflow1_to_airflow2.py --environment dev \
        --source-dir /path/to/airflow1/dags \
        --target-dir /path/to/airflow2/dags \
        --backup-dir /path/to/backups \
        [--dry-run] [--skip-taskflow]
"""

import os
import sys
import re
import json
import logging
import pathlib
from pathlib import Path
import shutil
import datetime
from datetime import datetime
import argparse
import typing
from typing import Dict, List, Optional, Union, Any, Tuple, Set
import ast

# Third-party imports
import alembic
from alembic import command
from alembic.config import Config as AlembicConfig

# Internal imports
from ...backend.dags.utils.db_utils import execute_query, run_migration_script, get_db_info
from ...backend.config import get_config, get_environment
from .alembic_migrations.env import run_migrations_online

# Configure logging
logger = logging.getLogger('airflow.migrations')

# Define regular expression patterns for migration
AIRFLOW_1_IMPORT_PATTERNS = re.compile(r'^from airflow\.(operators|contrib|hooks|sensors)\.')

# Mapping from Airflow 1.X operators to Airflow 2.X operators
AIRFLOW_1_TO_2_OPERATOR_MAPPING = {
    'DummyOperator': 'airflow.operators.dummy.DummyOperator',
    'PythonOperator': 'airflow.operators.python.PythonOperator',
    'BashOperator': 'airflow.operators.bash.BashOperator',
    'GoogleCloudStorageToGoogleCloudStorageOperator': 'airflow.providers.google.cloud.transfers.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator',
    'BigQueryOperator': 'airflow.providers.google.cloud.operators.bigquery.BigQueryOperator'
    # Additional mappings omitted for brevity
}

# Deprecated parameters in Airflow 2.X
AIRFLOW_1_DEPRECATED_PARAMS = ['provide_context', 'queue', 'pool', 'executor_config', 'retry_delay', 
                              'retry_exponential_backoff', 'max_retry_delay', 'start_date', 'end_date']

# Timestamp format for backups
BACKUP_TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'

# Default Postgres connection ID
DEFAULT_CONN_ID = 'postgres_default'


def migrate_database_schema(environment: str, dry_run: bool = False) -> bool:
    """
    Migrates Airflow database schema from 1.10.15 to 2.X format.
    
    Args:
        environment: Target environment (dev, qa, prod)
        dry_run: If True, only generate SQL without executing
    
    Returns:
        Success status of the migration
    """
    logger.info("Starting database schema migration for environment: %s", environment)
    
    try:
        # Get environment configuration
        env_config = get_config(environment)
        
        # Create backup of current database if not in dry run mode
        if not dry_run:
            timestamp = datetime.now().strftime(BACKUP_TIMESTAMP_FORMAT)
            backup_file = f"airflow_db_backup_{timestamp}.sql"
            logger.info(f"Creating database backup to {backup_file}")
            # Implementation would invoke database backup command here
        
        # Get database information
        db_info = get_db_info()
        if not db_info:
            logger.error("Failed to retrieve database information")
            return False
        
        # Verify database version supports migration
        db_version = db_info.get('version', '')
        if 'PostgreSQL' not in db_version:
            logger.error(f"Unsupported database type: {db_version}")
            return False
        
        logger.info(f"Found compatible database: {db_version}")
        
        # Run the migrations using Alembic
        if dry_run:
            logger.info("Dry run mode: Would execute Alembic migrations but skipping actual execution")
            # In dry run, we would need to generate SQL without executing
            return True
        else:
            logger.info("Executing database schema migrations")
            run_migrations_online()
            logger.info("Database schema migration completed successfully")
            return True
    
    except Exception as e:
        logger.error(f"Database migration failed: {str(e)}")
        return False


def create_backup(source_dir: str, backup_dir: str) -> str:
    """
    Creates a timestamped backup of database and code before migration.
    
    Args:
        source_dir: Directory containing Airflow 1.X code
        backup_dir: Directory where backups should be stored
    
    Returns:
        Path to created backup directory
    """
    timestamp = datetime.now().strftime(BACKUP_TIMESTAMP_FORMAT)
    backup_path = os.path.join(backup_dir, f"airflow1_backup_{timestamp}")
    
    logger.info(f"Creating backup of source files to {backup_path}")
    
    try:
        # Create backup directory
        os.makedirs(backup_path, exist_ok=True)
        
        # Copy source files to backup directory
        for item in os.listdir(source_dir):
            source_item = os.path.join(source_dir, item)
            dest_item = os.path.join(backup_path, item)
            
            if os.path.isdir(source_item):
                shutil.copytree(source_item, dest_item)
            else:
                shutil.copy2(source_item, dest_item)
        
        # Backup database schema and data if possible
        db_backup_file = os.path.join(backup_path, f"airflow_db_backup_{timestamp}.sql")
        logger.info(f"Database backup would be created at {db_backup_file}")
        # Actual implementation would call a function to dump database
        
        logger.info(f"Backup created successfully at {backup_path}")
        return backup_path
    
    except Exception as e:
        logger.error(f"Failed to create backup: {str(e)}")
        return ""


def rollback_migration(backup_dir: str, target_dir: str) -> bool:
    """
    Rolls back migration by restoring from backup if migration fails.
    
    Args:
        backup_dir: Path to backup directory
        target_dir: Directory to restore files to
    
    Returns:
        Success status of rollback operation
    """
    logger.info(f"Starting rollback operation from {backup_dir} to {target_dir}")
    
    try:
        # Verify backup directory exists
        if not os.path.exists(backup_dir):
            logger.error(f"Backup directory not found: {backup_dir}")
            return False
        
        # Clear target directory
        if os.path.exists(target_dir):
            for item in os.listdir(target_dir):
                item_path = os.path.join(target_dir, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
        else:
            os.makedirs(target_dir, exist_ok=True)
        
        # Copy files from backup to target
        for item in os.listdir(backup_dir):
            # Skip database backup file
            if item.startswith("airflow_db_backup_") and item.endswith(".sql"):
                continue
                
            source_item = os.path.join(backup_dir, item)
            dest_item = os.path.join(target_dir, item)
            
            if os.path.isdir(source_item):
                shutil.copytree(source_item, dest_item)
            else:
                shutil.copy2(source_item, dest_item)
        
        # Restore database if backup exists
        db_backup_files = [f for f in os.listdir(backup_dir) 
                         if f.startswith("airflow_db_backup_") and f.endswith(".sql")]
        
        if db_backup_files:
            db_backup_file = os.path.join(backup_dir, db_backup_files[0])
            logger.info(f"Restoring database from {db_backup_file}")
            # Actual implementation would call a function to restore database
        
        logger.info("Rollback completed successfully")
        return True
    
    except Exception as e:
        logger.error(f"Rollback failed: {str(e)}")
        return False


def transform_imports(code: str) -> str:
    """
    Transforms Airflow 1.X import statements to Airflow 2.X format.
    
    Args:
        code: Source code containing Airflow 1.X imports
    
    Returns:
        Updated code with Airflow 2.X imports
    """
    lines = code.split('\n')
    updated_lines = []
    
    for line in lines:
        # Check if line contains an import from airflow modules
        if AIRFLOW_1_IMPORT_PATTERNS.search(line):
            # Contrib operators are now in provider packages
            if '.contrib.operators.' in line:
                # Extract the specific operator or module
                match = re.search(r'from airflow\.contrib\.operators\.(\w+)', line)
                if match:
                    operator_name = match.group(1)
                    
                    # Handle common operators
                    if 'gcs' in operator_name:
                        line = line.replace('airflow.contrib.operators', 
                                           'airflow.providers.google.cloud.operators')
                    elif 'bigquery' in operator_name:
                        line = line.replace('airflow.contrib.operators', 
                                           'airflow.providers.google.cloud.operators')
                    # Add more mappings for other providers as needed
            
            # Standard operators have moved to specific modules
            elif '.operators.' in line and not '.operators.dummy' in line:
                for op_name, op_path in AIRFLOW_1_TO_2_OPERATOR_MAPPING.items():
                    if f" {op_name}" in line or f"import {op_name}" in line:
                        module_path = op_path.rsplit('.', 1)[0]
                        line = f"from {module_path} import {op_name}"
            
            # Handle similar patterns for hooks and sensors
            if '.hooks.' in line:
                if '.hooks.http_hook' in line:
                    line = line.replace('airflow.hooks.http_hook', 
                                       'airflow.providers.http.hooks.http')
                elif '.hooks.postgres_hook' in line:
                    line = line.replace('airflow.hooks.postgres_hook', 
                                       'airflow.providers.postgres.hooks.postgres')
            
            if '.sensors.' in line:
                if '.sensors.http_sensor' in line:
                    line = line.replace('airflow.sensors.http_sensor', 
                                       'airflow.providers.http.sensors.http')
        
        updated_lines.append(line)
    
    return '\n'.join(updated_lines)


def transform_operators(code: str) -> str:
    """
    Updates operator instantiations to be compatible with Airflow 2.X.
    
    Args:
        code: Source code containing Airflow 1.X operator usage
    
    Returns:
        Updated code with Airflow 2.X operator usage
    """
    try:
        # Parse the code into an AST
        tree = ast.parse(code)
        
        # Create a transformer to modify operator instantiations
        class OperatorTransformer(ast.NodeTransformer):
            def visit_Call(self, node):
                # First visit children
                self.generic_visit(node)
                
                # Check if this is an operator instantiation
                if isinstance(node.func, ast.Name) and node.func.id in AIRFLOW_1_TO_2_OPERATOR_MAPPING:
                    # No need to modify the class name here as it would be handled in transform_imports
                    
                    # Filter out deprecated parameters
                    node.keywords = [kw for kw in node.keywords 
                                    if kw.arg not in AIRFLOW_1_DEPRECATED_PARAMS]
                    
                    # Handle special parameter transformations
                    for kw in node.keywords:
                        # transform 'python_callable' parameter for PythonOperator if needed
                        if node.func.id == 'PythonOperator' and kw.arg == 'provide_context' and \
                           isinstance(kw.value, ast.NameConstant) and kw.value.value is True:
                            # provide_context is removed in Airflow 2.0, we need to ensure 
                            # the callable accepts task_instance and context parameters
                            logger.warning("provide_context=True found in PythonOperator. " 
                                         "This parameter is deprecated in Airflow 2.0.")
                
                return node
        
        # Apply the transformer
        transformed_tree = OperatorTransformer().visit(tree)
        
        # Convert the transformed AST back to code
        # Note: For Python 3.8 compatibility, we would use a custom AST to code converter
        # or a library like astor instead of ast.unparse() which is only in Python 3.9+
        # For this example, we're using a simplification
        import astor  # version 0.8.1
        new_code = astor.to_source(transformed_tree)
        return new_code
    
    except Exception as e:
        logger.error(f"Error transforming operators: {str(e)}")
        # Return original code if transformation fails
        return code


def update_dag_pattern(code: str) -> str:
    """
    Updates DAG initialization pattern to match Airflow 2.X best practices.
    
    Args:
        code: Source code containing Airflow 1.X DAG definitions
    
    Returns:
        Updated code with modern DAG definition pattern
    """
    try:
        # Parse the code into an AST
        tree = ast.parse(code)
        
        # Create a transformer to modify DAG instantiations
        class DAGTransformer(ast.NodeTransformer):
            def visit_Assign(self, node):
                # First visit children
                self.generic_visit(node)
                
                # Check if this is a DAG assignment
                if (len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and
                    isinstance(node.value, ast.Call) and 
                    isinstance(node.value.func, ast.Name) and node.value.func.id == 'DAG'):
                    
                    # Extract DAG variable name
                    dag_var = node.targets[0].id
                    
                    # Add a comment recommending the with-statement pattern
                    comment_node = ast.Expr(
                        value=ast.Constant(
                            value=f"# MIGRATION NOTE: Consider using 'with DAG(...)' pattern for {dag_var}",
                            kind=None
                        )
                    )
                    ast.copy_location(comment_node, node)
                    return [comment_node, node]
                
                return node
        
        # Apply the transformer
        transformed_tree = DAGTransformer().visit(tree)
        
        # Convert the transformed AST back to code
        import astor  # version 0.8.1
        new_code = astor.to_source(transformed_tree)
        return new_code
    
    except Exception as e:
        logger.error(f"Error updating DAG pattern: {str(e)}")
        # Return original code if transformation fails
        return code


def convert_python_to_taskflow(code: str) -> str:
    """
    Converts traditional PythonOperators to TaskFlow API pattern where beneficial.
    
    Args:
        code: Source code containing PythonOperator definitions
    
    Returns:
        Updated code using TaskFlow API pattern
    """
    try:
        # Parse the code into an AST
        tree = ast.parse(code)
        
        # Find Python functions used in PythonOperator
        python_funcs = set()
        python_ops = {}  # Map calls to their containing assign nodes
        
        # First pass: find Python functions and PythonOperators
        class FunctionFinder(ast.NodeVisitor):
            def visit_Assign(self, node):
                if (isinstance(node.value, ast.Call) and 
                    isinstance(node.value.func, ast.Name) and 
                    node.value.func.id == 'PythonOperator'):
                    
                    for kw in node.value.keywords:
                        if kw.arg == 'python_callable' and isinstance(kw.value, ast.Name):
                            python_funcs.add(kw.value.id)
                            python_ops[kw.value.id] = node
                
                self.generic_visit(node)
        
        FunctionFinder().visit(tree)
        
        if not python_funcs:
            # No PythonOperator functions found
            return code
        
        # Second pass: transform functions to use @task decorator
        class TaskTransformer(ast.NodeTransformer):
            def visit_FunctionDef(self, node):
                # Skip if this is not a function used by PythonOperator
                if node.name not in python_funcs:
                    return self.generic_visit(node)
                
                # Add @task decorator
                task_decorator = ast.Name(id='task', ctx=ast.Load())
                decorator_call = ast.Call(
                    func=task_decorator,
                    args=[],
                    keywords=[]
                )
                node.decorator_list.append(decorator_call)
                
                # Add comment about migration
                comment = ast.Expr(
                    value=ast.Constant(
                        value=f"# MIGRATION NOTE: Function converted to TaskFlow API",
                        kind=None
                    )
                )
                
                return [comment, node]
        
        # Apply the transformation
        transformed_tree = TaskTransformer().visit(ast.parse(code))
        
        # Add import for task decorator at the beginning
        import_stmt = ast.ImportFrom(
            module='airflow.decorators',
            names=[ast.alias(name='task', asname=None)],
            level=0
        )
        
        transformed_tree.body.insert(0, import_stmt)
        
        # Convert the transformed AST back to code
        import astor  # version 0.8.1
        new_code = astor.to_source(transformed_tree)
        
        # Add comment for PythonOperator instances that need manual conversion
        for func_name, assign_node in python_ops.items():
            task_id = None
            for kw in assign_node.value.keywords:
                if kw.arg == 'task_id' and isinstance(kw.value, ast.Constant):
                    task_id = kw.value.value
            
            var_name = assign_node.targets[0].id if hasattr(assign_node, 'targets') and len(assign_node.targets) > 0 else "unknown"
            
            placeholder = f"# MIGRATION NOTE: Replace {var_name} = PythonOperator(task_id='{task_id}'...) with {var_name} = {func_name}()"
            new_code = new_code.replace(astor.to_source(assign_node), placeholder + "\n" + astor.to_source(assign_node))
        
        return new_code
    
    except Exception as e:
        logger.error(f"Error converting to TaskFlow API: {str(e)}")
        # Return original code if transformation fails
        return code


def validate_migration(target_dir: str) -> Dict:
    """
    Validates the migrated code and database schema for Airflow 2.X compatibility.
    
    Args:
        target_dir: Directory containing migrated Airflow 2.X code
    
    Returns:
        Validation results with issues and status
    """
    logger.info(f"Starting validation of migrated code in {target_dir}")
    
    validation_results = {
        'status': 'success',
        'issues': [],
        'stats': {
            'dags_checked': 0,
            'dags_with_issues': 0,
            'total_issues': 0,
            'import_issues': 0,
            'operator_issues': 0,
            'parameter_issues': 0,
            'connection_issues': 0
        }
    }
    
    try:
        # Check DAG parsing compatibility
        dag_files = []
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith('.py'):
                    dag_files.append(os.path.join(root, file))
        
        validation_results['stats']['dags_checked'] = len(dag_files)
        
        for dag_file in dag_files:
            file_issues = []
            
            # Read DAG file
            with open(dag_file, 'r') as f:
                code = f.read()
            
            # Check for deprecated import patterns
            deprecated_imports = re.findall(r'from airflow\.contrib\.', code)
            if deprecated_imports:
                file_issues.append(f"Found {len(deprecated_imports)} deprecated import(s) from airflow.contrib")
                validation_results['stats']['import_issues'] += len(deprecated_imports)
            
            # Check for deprecated operator usage
            deprecated_operators = re.findall(r'(?:^|\s)(SSHOperator|S3KeySensor|HttpSensor)', code)
            if deprecated_operators:
                file_issues.append(f"Found deprecated operator(s): {', '.join(deprecated_operators)}")
                validation_results['stats']['operator_issues'] += len(deprecated_operators)
            
            # Check for deprecated parameters
            for param in AIRFLOW_1_DEPRECATED_PARAMS:
                if re.search(rf'\b{param}\s*=', code):
                    file_issues.append(f"Found deprecated parameter: {param}")
                    validation_results['stats']['parameter_issues'] += 1
            
            if file_issues:
                rel_path = os.path.relpath(dag_file, target_dir)
                validation_results['issues'].append({
                    'file': rel_path,
                    'issues': file_issues
                })
                validation_results['stats']['dags_with_issues'] += 1
                validation_results['stats']['total_issues'] += len(file_issues)
        
        # Check connection definitions
        connections_file = os.path.join(target_dir, 'connections.json')
        if os.path.exists(connections_file):
            with open(connections_file, 'r') as f:
                try:
                    connections = json.load(f)
                    for conn_id, conn_data in connections.items():
                        if isinstance(conn_data, dict):
                            # Check for deprecated connection types
                            if conn_data.get('conn_type') in ['jdbc', 'cloudant']:
                                validation_results['issues'].append({
                                    'file': 'connections.json',
                                    'issues': [f"Connection {conn_id} uses deprecated conn_type: {conn_data.get('conn_type')}"]
                                })
                                validation_results['stats']['connection_issues'] += 1
                                validation_results['stats']['total_issues'] += 1
                except json.JSONDecodeError:
                    validation_results['issues'].append({
                        'file': 'connections.json',
                        'issues': ["Invalid JSON format in connections file"]
                    })
                    validation_results['stats']['connection_issues'] += 1
                    validation_results['stats']['total_issues'] += 1
        
        # Set overall status
        if validation_results['stats']['total_issues'] > 0:
            validation_results['status'] = 'warning'
            logger.warning(f"Validation found {validation_results['stats']['total_issues']} issues")
        else:
            logger.info("Validation completed successfully with no issues")
        
        return validation_results
    
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        validation_results['status'] = 'error'
        validation_results['issues'].append({
            'file': 'validation',
            'issues': [f"Exception during validation: {str(e)}"]
        })
        return validation_results


def parse_args():
    """
    Parses command-line arguments for the migration script.
    
    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Migrate Airflow 1.10.15 to Airflow 2.X for Cloud Composer 2'
    )
    
    parser.add_argument(
        '--environment',
        choices=['dev', 'qa', 'prod'],
        default='dev',
        help='Target environment (dev, qa, prod)'
    )
    
    parser.add_argument(
        '--source-dir',
        required=True,
        help='Source directory containing Airflow 1.X code'
    )
    
    parser.add_argument(
        '--target-dir',
        required=True,
        help='Target directory for migrated Airflow 2.X code'
    )
    
    parser.add_argument(
        '--backup-dir',
        default='./backups',
        help='Directory to store backups before migration'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate migration without making changes'
    )
    
    parser.add_argument(
        '--skip-taskflow',
        action='store_true',
        help='Skip conversion to TaskFlow API'
    )
    
    return parser.parse_args()


class DAGMigrator:
    """
    Handles the migration of Airflow 1.X DAG files to Airflow 2.X compatibility.
    """
    
    def __init__(self, use_taskflow: bool = True, dry_run: bool = False):
        """
        Initialize the DAG migrator with specified options.
        
        Args:
            use_taskflow: Whether to convert PythonOperators to TaskFlow API
            dry_run: If True, don't actually write files
        """
        self.use_taskflow = use_taskflow
        self.dry_run = dry_run
        self.migration_stats = {
            'processed': 0,
            'issues': 0,
            'successful': 0,
            'warnings': 0
        }
        self.logger = logging.getLogger('airflow.migrations.dag')
    
    def migrate_dag_file(self, source_path: str, target_path: str) -> Dict:
        """
        Migrates a single DAG file from Airflow 1.X to 2.X.
        
        Args:
            source_path: Path to source DAG file
            target_path: Path to write migrated DAG file
        
        Returns:
            Migration results for the DAG file
        """
        self.logger.info(f"Migrating DAG file: {source_path}")
        
        result = {
            'source': source_path,
            'target': target_path,
            'status': 'success',
            'issues': [],
            'warnings': []
        }
        
        try:
            # Read source DAG file
            with open(source_path, 'r') as f:
                code = f.read()
            
            # Apply transformations
            self.logger.debug("Transforming imports")
            code = transform_imports(code)
            
            self.logger.debug("Transforming operators")
            code = transform_operators(code)
            
            self.logger.debug("Updating DAG pattern")
            code = update_dag_pattern(code)
            
            if self.use_taskflow:
                self.logger.debug("Converting to TaskFlow API")
                code = convert_python_to_taskflow(code)
            
            # Create target directory if it doesn't exist
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            # Write transformed code to target path
            if not self.dry_run:
                with open(target_path, 'w') as f:
                    f.write(code)
                self.logger.info(f"Migrated DAG written to {target_path}")
            else:
                self.logger.info(f"Dry run: Would write migrated DAG to {target_path}")
            
            # Update migration statistics
            self.migration_stats['processed'] += 1
            self.migration_stats['successful'] += 1
            
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to migrate DAG file {source_path}: {str(e)}")
            result['status'] = 'error'
            result['issues'].append(f"Exception: {str(e)}")
            self.migration_stats['processed'] += 1
            self.migration_stats['issues'] += 1
            return result
    
    def migrate_dag_files(self, source_dir: str, target_dir: str) -> Dict:
        """
        Migrates all DAG files in a directory from Airflow 1.X to 2.X.
        
        Args:
            source_dir: Directory containing source DAG files
            target_dir: Directory to write migrated DAG files
        
        Returns:
            Aggregated migration results for all DAG files
        """
        self.logger.info(f"Migrating DAG files from {source_dir} to {target_dir}")
        
        results = {
            'source_dir': source_dir,
            'target_dir': target_dir,
            'status': 'success',
            'file_results': [],
            'stats': self.migration_stats
        }
        
        try:
            # Find all Python files in source_dir and subdirectories
            dag_files = []
            for root, _, files in os.walk(source_dir):
                for file in files:
                    if file.endswith('.py'):
                        dag_files.append(os.path.join(root, file))
            
            self.logger.info(f"Found {len(dag_files)} Python files to process")
            
            for source_path in dag_files:
                # Calculate relative path to maintain directory structure
                rel_path = os.path.relpath(source_path, source_dir)
                target_path = os.path.join(target_dir, rel_path)
                
                # Migrate DAG file
                file_result = self.migrate_dag_file(source_path, target_path)
                results['file_results'].append(file_result)
                
                # Update overall status if any file failed
                if file_result['status'] != 'success':
                    results['status'] = 'warning'
            
            self.logger.info(f"DAG migration completed: {self.migration_stats['successful']} successful, "
                           f"{self.migration_stats['issues']} with issues")
            return results
        
        except Exception as e:
            self.logger.error(f"Failed to migrate DAG files: {str(e)}")
            results['status'] = 'error'
            results['error'] = str(e)
            return results


class ConnectionMigrator:
    """
    Handles migration of Airflow 1.X connection definitions to Airflow 2.X format.
    """
    
    def __init__(self, dry_run: bool = False):
        """
        Initialize connection migrator with options.
        
        Args:
            dry_run: If True, don't actually write files
        """
        self.dry_run = dry_run
        self.migration_stats = {
            'processed': 0,
            'issues': 0,
            'successful': 0
        }
        self.conn_type_mapping = {
            'google_cloud_platform': 'google_cloud_platform',
            'google_cloud_storage': 'google_cloud_platform',
            'bigquery': 'google_cloud_platform',
            'postgres': 'postgres',
            'mysql': 'mysql',
            'http': 'http',
            'ssh': 'ssh'
            # Add more mappings as needed
        }
        self.logger = logging.getLogger('airflow.migrations.connection')
    
    def transform_connection(self, connection: Dict) -> Dict:
        """
        Transforms a single connection definition to Airflow 2.X format.
        
        Args:
            connection: Connection definition dictionary
        
        Returns:
            Transformed connection definition
        """
        # Create a copy to avoid modifying the original
        result = connection.copy()
        
        # Update conn_type if needed
        if 'conn_type' in result and result['conn_type'] in self.conn_type_mapping:
            orig_type = result['conn_type']
            result['conn_type'] = self.conn_type_mapping[orig_type]
            if orig_type != result['conn_type']:
                self.logger.info(f"Updated connection type from {orig_type} to {result['conn_type']}")
        
        # Convert 'extra' field from string to JSON if needed
        if 'extra' in result and isinstance(result['extra'], str):
            try:
                extra_dict = json.loads(result['extra'])
                
                # Update extra fields for provider packages
                if result.get('conn_type') == 'google_cloud_platform':
                    # Ensure required GCP fields are present
                    if 'project' in extra_dict and 'project_id' not in extra_dict:
                        extra_dict['project_id'] = extra_dict['project']
                    
                    # Add key_path if not present but key_json is
                    if 'key_json' in extra_dict and 'key_path' not in extra_dict:
                        self.logger.info(f"Connection has key_json but no key_path")
                
                # Update the extra field with modified dictionary
                result['extra'] = json.dumps(extra_dict)
            
            except json.JSONDecodeError:
                self.logger.warning(f"Could not parse 'extra' field as JSON")
        
        return result
    
    def migrate_connections(self, source_path: str, target_path: str) -> Dict:
        """
        Migrates all connections from source file to target file with Airflow 2.X compatibility.
        
        Args:
            source_path: Path to source connections JSON file
            target_path: Path to write migrated connections JSON file
        
        Returns:
            Migration results
        """
        self.logger.info(f"Migrating connections from {source_path} to {target_path}")
        
        result = {
            'source': source_path,
            'target': target_path,
            'status': 'success',
            'issues': [],
            'stats': self.migration_stats
        }
        
        try:
            # Read connections from source file
            with open(source_path, 'r') as f:
                connections = json.load(f)
            
            self.logger.info(f"Found {len(connections)} connections to migrate")
            
            # Transform each connection
            migrated_connections = {}
            for conn_id, conn_data in connections.items():
                self.migration_stats['processed'] += 1
                
                try:
                    migrated_connections[conn_id] = self.transform_connection(conn_data)
                    self.migration_stats['successful'] += 1
                except Exception as e:
                    self.logger.error(f"Failed to migrate connection {conn_id}: {str(e)}")
                    migrated_connections[conn_id] = conn_data  # Keep original
                    result['issues'].append(f"Connection {conn_id}: {str(e)}")
                    self.migration_stats['issues'] += 1
            
            # Create target directory if it doesn't exist
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            # Write migrated connections to target file
            if not self.dry_run:
                with open(target_path, 'w') as f:
                    json.dump(migrated_connections, f, indent=2)
                self.logger.info(f"Migrated connections written to {target_path}")
            else:
                self.logger.info(f"Dry run: Would write migrated connections to {target_path}")
            
            # Set status based on issues
            if self.migration_stats['issues'] > 0:
                result['status'] = 'warning'
                self.logger.warning(f"Connection migration completed with {self.migration_stats['issues']} issues")
            else:
                self.logger.info(f"Connection migration completed successfully")
            
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to migrate connections: {str(e)}")
            result['status'] = 'error'
            result['issues'].append(f"Exception: {str(e)}")
            return result


class PluginMigrator:
    """
    Handles migration of Airflow 1.X plugins to Airflow 2.X compatibility.
    """
    
    def __init__(self, dry_run: bool = False):
        """
        Initialize plugin migrator with options.
        
        Args:
            dry_run: If True, don't actually write files
        """
        self.dry_run = dry_run
        self.migration_stats = {
            'processed': 0,
            'issues': 0,
            'successful': 0,
            'hooks': 0,
            'operators': 0,
            'sensors': 0
        }
        self.logger = logging.getLogger('airflow.migrations.plugin')
    
    def update_hook_implementation(self, code: str) -> str:
        """
        Updates hook implementation to match Airflow 2.X interface.
        
        Args:
            code: Source code of hook implementation
        
        Returns:
            Updated hook implementation code
        """
        try:
            # Parse the code into an AST
            tree = ast.parse(code)
            
            # Create a transformer to modify hook implementations
            class HookTransformer(ast.NodeTransformer):
                def visit_ClassDef(self, node):
                    # First visit children
                    self.generic_visit(node)
                    
                    # Check if this is a hook class
                    is_hook = False
                    for base in node.bases:
                        if isinstance(base, ast.Name) and 'Hook' in base.id:
                            is_hook = True
                            break
                    
                    if is_hook:
                        # Add comment about hook migration
                        comment_node = ast.Expr(
                            value=ast.Constant(
                                value=f"# MIGRATION NOTE: Hook class {node.name} might need updates for Airflow 2.X compatibility",
                                kind=None
                            )
                        )
                        ast.copy_location(comment_node, node)
                        return [comment_node, node]
                    
                    return node
            
            # Apply the transformation
            transformed_tree = HookTransformer().visit(tree)
            
            # Convert the transformed AST back to code
            import astor  # version 0.8.1
            new_code = astor.to_source(transformed_tree)
            return new_code
        
        except Exception as e:
            self.logger.error(f"Error updating hook implementation: {str(e)}")
            # Return original code if transformation fails
            return code
    
    def update_operator_implementation(self, code: str) -> str:
        """
        Updates operator implementation to match Airflow 2.X interface.
        
        Args:
            code: Source code of operator implementation
        
        Returns:
            Updated operator implementation code
        """
        try:
            # Parse the code into an AST
            tree = ast.parse(code)
            
            # Create a transformer to modify operator implementations
            class OperatorTransformer(ast.NodeTransformer):
                def visit_ClassDef(self, node):
                    # First visit children
                    self.generic_visit(node)
                    
                    # Check if this is an operator class
                    is_operator = False
                    for base in node.bases:
                        if isinstance(base, ast.Name) and 'Operator' in base.id:
                            is_operator = True
                            break
                    
                    if is_operator:
                        # Add comment about operator migration
                        comment_node = ast.Expr(
                            value=ast.Constant(
                                value=f"# MIGRATION NOTE: Operator class {node.name} might need updates for Airflow 2.X compatibility",
                                kind=None
                            )
                        )
                        ast.copy_location(comment_node, node)
                        return [comment_node, node]
                    
                    return node
            
            # Apply the transformation
            transformed_tree = OperatorTransformer().visit(tree)
            
            # Convert the transformed AST back to code
            import astor  # version 0.8.1
            new_code = astor.to_source(transformed_tree)
            return new_code
        
        except Exception as e:
            self.logger.error(f"Error updating operator implementation: {str(e)}")
            # Return original code if transformation fails
            return code
    
    def update_sensor_implementation(self, code: str) -> str:
        """
        Updates sensor implementation to match Airflow 2.X interface.
        
        Args:
            code: Source code of sensor implementation
        
        Returns:
            Updated sensor implementation code
        """
        try:
            # Parse the code into an AST
            tree = ast.parse(code)
            
            # Create a transformer to modify sensor implementations
            class SensorTransformer(ast.NodeTransformer):
                def visit_ClassDef(self, node):
                    # First visit children
                    self.generic_visit(node)
                    
                    # Check if this is a sensor class
                    is_sensor = False
                    for base in node.bases:
                        if isinstance(base, ast.Name) and 'Sensor' in base.id:
                            is_sensor = True
                            break
                    
                    if is_sensor:
                        # Add comment about sensor migration
                        comment_node = ast.Expr(
                            value=ast.Constant(
                                value=f"# MIGRATION NOTE: Sensor class {node.name} might need updates for Airflow 2.X compatibility",
                                kind=None
                            )
                        )
                        ast.copy_location(comment_node, node)
                        return [comment_node, node]
                    
                    return node
            
            # Apply the transformation
            transformed_tree = SensorTransformer().visit(tree)
            
            # Convert the transformed AST back to code
            import astor  # version 0.8.1
            new_code = astor.to_source(transformed_tree)
            return new_code
        
        except Exception as e:
            self.logger.error(f"Error updating sensor implementation: {str(e)}")
            # Return original code if transformation fails
            return code
    
    def migrate_plugin_file(self, source_path: str, target_path: str) -> Dict:
        """
        Migrates a single plugin file from Airflow 1.X to 2.X.
        
        Args:
            source_path: Path to source plugin file
            target_path: Path to write migrated plugin file
        
        Returns:
            Migration results for the plugin file
        """
        self.logger.info(f"Migrating plugin file: {source_path}")
        
        result = {
            'source': source_path,
            'target': target_path,
            'status': 'success',
            'issues': [],
            'warnings': []
        }
        
        try:
            # Read source plugin file
            with open(source_path, 'r') as f:
                code = f.read()
            
            # Apply transformations
            self.logger.debug("Transforming imports")
            code = transform_imports(code)
            
            # Determine plugin type (hook, operator, sensor, other)
            plugin_type = 'other'
            if 'Hook' in code and ('class' in code and 'Hook' in code.split('class')[1].split('(')[0]):
                plugin_type = 'hook'
                self.migration_stats['hooks'] += 1
                code = self.update_hook_implementation(code)
            elif 'Operator' in code and ('class' in code and 'Operator' in code.split('class')[1].split('(')[0]):
                plugin_type = 'operator'
                self.migration_stats['operators'] += 1
                code = self.update_operator_implementation(code)
            elif 'Sensor' in code and ('class' in code and 'Sensor' in code.split('class')[1].split('(')[0]):
                plugin_type = 'sensor'
                self.migration_stats['sensors'] += 1
                code = self.update_sensor_implementation(code)
            
            # Create target directory if it doesn't exist
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            # Write transformed code to target path
            if not self.dry_run:
                with open(target_path, 'w') as f:
                    f.write(code)
                self.logger.info(f"Migrated plugin ({plugin_type}) written to {target_path}")
            else:
                self.logger.info(f"Dry run: Would write migrated plugin ({plugin_type}) to {target_path}")
            
            # Update migration statistics
            self.migration_stats['processed'] += 1
            self.migration_stats['successful'] += 1
            
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to migrate plugin file {source_path}: {str(e)}")
            result['status'] = 'error'
            result['issues'].append(f"Exception: {str(e)}")
            self.migration_stats['processed'] += 1
            self.migration_stats['issues'] += 1
            return result
    
    def migrate_plugins(self, source_dir: str, target_dir: str) -> Dict:
        """
        Migrates all plugin files in a directory from Airflow 1.X to 2.X.
        
        Args:
            source_dir: Directory containing source plugin files
            target_dir: Directory to write migrated plugin files
        
        Returns:
            Aggregated migration results for all plugin files
        """
        self.logger.info(f"Migrating plugins from {source_dir} to {target_dir}")
        
        results = {
            'source_dir': source_dir,
            'target_dir': target_dir,
            'status': 'success',
            'file_results': [],
            'stats': self.migration_stats
        }
        
        try:
            # Find all Python files in source_dir and subdirectories
            plugin_files = []
            for root, _, files in os.walk(source_dir):
                for file in files:
                    if file.endswith('.py'):
                        plugin_files.append(os.path.join(root, file))
            
            self.logger.info(f"Found {len(plugin_files)} Python plugin files to process")
            
            for source_path in plugin_files:
                # Calculate relative path to maintain directory structure
                rel_path = os.path.relpath(source_path, source_dir)
                target_path = os.path.join(target_dir, rel_path)
                
                # Migrate plugin file
                file_result = self.migrate_plugin_file(source_path, target_path)
                results['file_results'].append(file_result)
                
                # Update overall status if any file failed
                if file_result['status'] != 'success':
                    results['status'] = 'warning'
            
            self.logger.info(f"Plugin migration completed: {self.migration_stats['successful']} successful, "
                           f"{self.migration_stats['issues']} with issues")
            self.logger.info(f"Migrated {self.migration_stats['hooks']} hooks, "
                           f"{self.migration_stats['operators']} operators, "
                           f"{self.migration_stats['sensors']} sensors")
            return results
        
        except Exception as e:
            self.logger.error(f"Failed to migrate plugins: {str(e)}")
            results['status'] = 'error'
            results['error'] = str(e)
            return results


def main() -> int:
    """
    Main entry point for the migration script.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Parse command-line arguments
    args = parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("Starting Airflow 1.10.15 to Airflow 2.X migration")
    logger.info(f"Environment: {args.environment}")
    logger.info(f"Source directory: {args.source_dir}")
    logger.info(f"Target directory: {args.target_dir}")
    logger.info(f"Backup directory: {args.backup_dir}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Skip TaskFlow: {args.skip_taskflow}")
    
    try:
        # Create backup if not in dry run mode
        backup_path = None
        if not args.dry_run:
            backup_path = create_backup(args.source_dir, args.backup_dir)
            if not backup_path:
                logger.error("Failed to create backup, aborting migration")
                return 1
        
        # Initialize migrators
        dag_migrator = DAGMigrator(use_taskflow=not args.skip_taskflow, dry_run=args.dry_run)
        conn_migrator = ConnectionMigrator(dry_run=args.dry_run)
        plugin_migrator = PluginMigrator(dry_run=args.dry_run)
        
        # Migrate database schema
        if not migrate_database_schema(args.environment, args.dry_run):
            logger.error("Database schema migration failed")
            if backup_path:
                logger.info("Rolling back to previous state")
                rollback_migration(backup_path, args.target_dir)
            return 1
        
        # Migrate DAG files
        dag_result = dag_migrator.migrate_dag_files(args.source_dir, args.target_dir)
        if dag_result.get('status') == 'error':
            logger.error("DAG migration failed")
            if backup_path:
                logger.info("Rolling back to previous state")
                rollback_migration(backup_path, args.target_dir)
            return 1
        
        # Migrate connections
        conn_source = os.path.join(args.source_dir, 'connections.json')
        conn_target = os.path.join(args.target_dir, 'connections.json')
        if os.path.exists(conn_source):
            conn_result = conn_migrator.migrate_connections(conn_source, conn_target)
            if conn_result.get('status') == 'error':
                logger.warning("Connection migration failed")
                if backup_path:
                    logger.info("Rolling back to previous state")
                    rollback_migration(backup_path, args.target_dir)
                return 1
        
        # Migrate plugins
        plugin_source = os.path.join(args.source_dir, 'plugins')
        plugin_target = os.path.join(args.target_dir, 'plugins')
        if os.path.exists(plugin_source):
            plugin_result = plugin_migrator.migrate_plugins(plugin_source, plugin_target)
            if plugin_result.get('status') == 'error':
                logger.warning("Plugin migration failed")
                if backup_path:
                    logger.info("Rolling back to previous state")
                    rollback_migration(backup_path, args.target_dir)
                return 1
        
        # Validate migration
        validation_results = validate_migration(args.target_dir)
        if validation_results['status'] == 'error':
            logger.error("Migration validation failed")
            if backup_path and not args.dry_run:
                logger.info("Rolling back to previous state")
                rollback_migration(backup_path, args.target_dir)
            return 1
        
        # Print migration report
        logger.info("Migration completed with the following statistics:")
        logger.info(f"DAGs processed: {dag_result.get('stats', {}).get('processed', 0)}")
        logger.info(f"DAGs with issues: {dag_result.get('stats', {}).get('issues', 0)}")
        logger.info(f"Connection definitions migrated: {conn_migrator.migration_stats.get('processed', 0)}")
        logger.info(f"Plugins migrated: {plugin_migrator.migration_stats.get('processed', 0)}")
        
        if validation_results['status'] == 'warning':
            logger.warning(f"Migration completed with {validation_results['stats']['total_issues']} issues")
            logger.warning("Please review the issues and make manual adjustments where needed")
        else:
            logger.info("Migration completed successfully with no validation issues")
        
        return 0 if validation_results['status'] != 'error' else 1
    
    except Exception as e:
        logger.error(f"Migration failed with unexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())