"""
Utility module for validating Apache Airflow DAGs, tasks, and operators for compatibility
with Airflow 2.X during migration from Airflow 1.10.15.

This module provides comprehensive validation functions, error detection, and migration
guidance to support the migration process from Airflow 1.10.15 to Airflow 2.X in
Cloud Composer 2.

Features:
    - DAG structure and parameter validation
    - Task and operator compatibility checking
    - Import path verification and correction
    - Connection validation for Airflow 2.X
    - Identification of deprecated features
    - Migration report generation
    - TaskFlow API conversion analysis
"""

import os
import re
import inspect
import logging
import json
import importlib
from typing import Dict, List, Any, Optional, Union, Tuple, Set, Callable

# For cron expression validation
# croniter v1.0.0+
import croniter

# Airflow imports
# apache-airflow v2.0.0+
import airflow
from airflow.models import DAG, BaseOperator, Connection
from airflow.exceptions import AirflowException

# Internal imports
from .gcp_utils import gcs_file_exists, gcs_upload_file

# Set up logging
logger = logging.getLogger('airflow.utils.validation')

# Validation severity levels
VALIDATION_LEVELS = {
    'ERROR': 30,
    'WARNING': 20,
    'INFO': 10
}

# Default validation level
DEFAULT_VALIDATION_LEVEL = 'WARNING'

# Dictionary of deprecated parameters in Airflow 1.x with migration guidance
AIRFLOW_1_DEPRECATED_PARAMS = {
    'provide_context': 'No longer needed in Airflow 2',
    'queue_pool': 'Use pool parameter instead',
    'queue_retry_delay': 'Use retry_delay parameter instead',
    'owner_links': 'Removed in Airflow 2',
    'schedule_interval': 'Use the DAG parameter directly'
}

# Dictionary of renamed operators in Airflow 2.X with old and new import paths
AIRFLOW_2_RENAMED_OPERATORS = {
    'BaseSensorOperator': {
        'old_path': 'airflow.sensors.base_sensor_operator',
        'new_path': 'airflow.sensors.base'
    },
    'BashOperator': {
        'old_path': 'airflow.operators.bash_operator',
        'new_path': 'airflow.operators.bash'
    },
    'PythonOperator': {
        'old_path': 'airflow.operators.python_operator',
        'new_path': 'airflow.operators.python'
    },
    'EmailOperator': {
        'old_path': 'airflow.operators.email_operator',
        'new_path': 'airflow.operators.email'
    },
    'GoogleCloudStorageToGoogleCloudStorageOperator': {
        'old_path': 'airflow.contrib.operators.gcs_to_gcs',
        'new_path': 'airflow.providers.google.cloud.transfers.gcs_to_gcs'
    }
}

# List of deprecated provider packages in Airflow 1.x
DEPRECATED_PROVIDER_PACKAGES = [
    'airflow.contrib',
    'airflow.hooks.base_hook',
    'airflow.hooks.dbapi_hook',
    'airflow.hooks.http_hook',
    'airflow.operators.bash_operator',
    'airflow.operators.python_operator',
    'airflow.sensors.base_sensor_operator'
]


def validate_dag(dag: Any, validation_level: str = DEFAULT_VALIDATION_LEVEL) -> Dict:
    """
    Validates an Airflow DAG for structural integrity and Airflow 2.X compatibility.
    
    This function performs comprehensive validation of a DAG object, checking for:
    - Proper DAG structure
    - Deprecated parameters and configurations
    - Task compatibility
    - Dependency correctness
    - Schedule interval validity
    
    Args:
        dag: The DAG object to validate
        validation_level: Minimum validation level to report (ERROR, WARNING, INFO)
        
    Returns:
        Dict containing validation results with success status and detailed reports
    
    Example:
        >>> from airflow import DAG
        >>> dag = DAG(dag_id='example_dag')
        >>> results = validate_dag(dag)
        >>> if results['success']:
        >>>     print("DAG is compatible with Airflow 2.X")
        >>> else:
        >>>     print(f"Issues found: {results['issues']}")
    """
    # Initialize validation results
    validation_results = {
        'success': True,
        'dag_id': getattr(dag, 'dag_id', 'unknown'),
        'issues': {
            'errors': [],
            'warnings': [],
            'info': []
        }
    }
    
    level_threshold = VALIDATION_LEVELS.get(validation_level.upper(), VALIDATION_LEVELS['WARNING'])
    
    # Validate that this is actually a DAG instance
    if not isinstance(dag, airflow.models.DAG):
        validation_results['success'] = False
        validation_results['issues']['errors'].append({
            'component': 'DAG',
            'message': f"Object is not an Airflow DAG instance: {type(dag)}"
        })
        return validation_results
    
    # Check schedule_interval for compatibility
    schedule_interval_results = validate_schedule_interval(dag.schedule_interval)
    if not schedule_interval_results['valid']:
        if schedule_interval_results['level'] == 'ERROR':
            validation_results['success'] = False
            validation_results['issues']['errors'].append({
                'component': 'schedule_interval',
                'message': schedule_interval_results['message']
            })
        elif schedule_interval_results['level'] == 'WARNING' and level_threshold <= VALIDATION_LEVELS['WARNING']:
            validation_results['issues']['warnings'].append({
                'component': 'schedule_interval',
                'message': schedule_interval_results['message']
            })
    
    # Validate default_args if present
    if hasattr(dag, 'default_args') and dag.default_args:
        default_args_result = validate_default_args(dag.default_args)
        
        for issue in default_args_result.get('errors', []):
            validation_results['success'] = False
            validation_results['issues']['errors'].append({
                'component': 'default_args',
                'message': issue
            })
            
        if level_threshold <= VALIDATION_LEVELS['WARNING']:
            for issue in default_args_result.get('warnings', []):
                validation_results['issues']['warnings'].append({
                    'component': 'default_args',
                    'message': issue
                })
                
        if level_threshold <= VALIDATION_LEVELS['INFO']:
            for issue in default_args_result.get('info', []):
                validation_results['issues']['info'].append({
                    'component': 'default_args',
                    'message': issue
                })
    
    # Check for proper DAG tags usage (new in Airflow 2.X)
    if not hasattr(dag, 'tags') or not dag.tags:
        if level_threshold <= VALIDATION_LEVELS['INFO']:
            validation_results['issues']['info'].append({
                'component': 'tags',
                'message': "DAG does not use tags. Consider adding tags for better organization in Airflow 2.X."
            })
    elif not all(isinstance(tag, str) for tag in dag.tags):
        validation_results['issues']['warnings'].append({
            'component': 'tags',
            'message': "All DAG tags must be strings in Airflow 2.X."
        })
    
    # Validate catchup parameter (behavior changed in Airflow 2.X)
    if not hasattr(dag, 'catchup'):
        if level_threshold <= VALIDATION_LEVELS['WARNING']:
            validation_results['issues']['warnings'].append({
                'component': 'catchup',
                'message': "catchup parameter not specified. Default behavior changed in Airflow 2.X (default: True)."
            })
    
    # Validate tasks
    task_count = 0
    for task_id, task in dag.task_dict.items():
        task_count += 1
        task_results = validate_task(task, validation_level)
        
        if not task_results['valid']:
            for issue in task_results.get('errors', []):
                validation_results['success'] = False
                validation_results['issues']['errors'].append({
                    'component': f"Task[{task_id}]",
                    'message': issue
                })
                
            if level_threshold <= VALIDATION_LEVELS['WARNING']:
                for issue in task_results.get('warnings', []):
                    validation_results['issues']['warnings'].append({
                        'component': f"Task[{task_id}]",
                        'message': issue
                    })
                    
            if level_threshold <= VALIDATION_LEVELS['INFO']:
                for issue in task_results.get('info', []):
                    validation_results['issues']['info'].append({
                        'component': f"Task[{task_id}]",
                        'message': issue
                    })
    
    # Check task dependencies (for proper set_upstream/set_downstream usage)
    for task_id, task in dag.task_dict.items():
        # Check for deprecated syntax in dependencies
        for upstream_task in task.upstream_list:
            if not upstream_task.task_id in dag.task_dict:
                validation_results['success'] = False
                validation_results['issues']['errors'].append({
                    'component': f"Task[{task_id}].upstream",
                    'message': f"Upstream task '{upstream_task.task_id}' not in DAG task_dict. Possible circular import or missing task."
                })
                
        for downstream_task in task.downstream_list:
            if not downstream_task.task_id in dag.task_dict:
                validation_results['success'] = False
                validation_results['issues']['errors'].append({
                    'component': f"Task[{task_id}].downstream",
                    'message': f"Downstream task '{downstream_task.task_id}' not in DAG task_dict. Possible circular import or missing task."
                })
    
    # Add summary information
    validation_results['summary'] = {
        'task_count': task_count,
        'error_count': len(validation_results['issues']['errors']),
        'warning_count': len(validation_results['issues']['warnings']),
        'info_count': len(validation_results['issues']['info'])
    }
    
    if task_count == 0 and level_threshold <= VALIDATION_LEVELS['WARNING']:
        validation_results['issues']['warnings'].append({
            'component': 'DAG',
            'message': "DAG contains no tasks."
        })
    
    return validation_results


def validate_task(task: Any, validation_level: str = DEFAULT_VALIDATION_LEVEL) -> Dict:
    """
    Validates an Airflow task or operator for compatibility with Airflow 2.X.
    
    This function examines a task/operator for:
    - Deprecated parameters
    - Renamed operator usage
    - XCom pattern compatibility
    - Callback function compatibility
    - Connection handling compatibility
    
    Args:
        task: The task or operator object to validate
        validation_level: Minimum validation level to report (ERROR, WARNING, INFO)
        
    Returns:
        Dict containing validation results specific to the task
    
    Example:
        >>> from airflow.operators.bash import BashOperator
        >>> task = BashOperator(task_id='my_task', bash_command='echo "Hello"')
        >>> results = validate_task(task)
        >>> if not results['valid']:
        >>>     print(f"Task issues: {results}")
    """
    validation_results = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'info': []
    }
    
    level_threshold = VALIDATION_LEVELS.get(validation_level.upper(), VALIDATION_LEVELS['WARNING'])
    
    # Validate task is an Airflow operator instance
    if not isinstance(task, airflow.models.BaseOperator):
        validation_results['valid'] = False
        validation_results['errors'].append(
            f"Object is not an Airflow task/operator instance: {type(task)}"
        )
        return validation_results
    
    # Identify operator class and module
    operator_class = task.__class__.__name__
    operator_module = task.__class__.__module__
    
    # Check if operator is using a deprecated import path
    import_path_validation = validate_operator_import(
        operator_name=operator_class,
        import_path=operator_module
    )
    
    if not import_path_validation['valid']:
        if import_path_validation['level'] == 'ERROR':
            validation_results['valid'] = False
            validation_results['errors'].append(import_path_validation['message'])
        elif import_path_validation['level'] == 'WARNING' and level_threshold <= VALIDATION_LEVELS['WARNING']:
            validation_results['warnings'].append(import_path_validation['message'])
    
    # Check for deprecated parameters in the operator
    task_dict = task.__dict__
    for param in AIRFLOW_1_DEPRECATED_PARAMS:
        if param in task_dict:
            validation_results['warnings'].append(
                f"Deprecated parameter '{param}': {AIRFLOW_1_DEPRECATED_PARAMS[param]}"
            )
    
    # Check for special case: PythonOperator with provide_context
    if operator_class == 'PythonOperator' and task_dict.get('provide_context', False):
        validation_results['warnings'].append(
            "PythonOperator uses provide_context=True which is no longer needed in Airflow 2.X "
            "as task context is automatically provided to the python_callable."
        )
    
    # Check XCom usage patterns
    if hasattr(task, 'do_xcom_push') and not task.do_xcom_push:
        if level_threshold <= VALIDATION_LEVELS['INFO']:
            validation_results['info'].append(
                "Task has do_xcom_push=False. In Airflow 2.X, consider using the TaskFlow API "
                "for cleaner XCom handling."
            )
    
    # Check for template_fields (different behavior in Airflow 2.X)
    if hasattr(task, 'template_fields') and task.template_fields:
        if level_threshold <= VALIDATION_LEVELS['INFO']:
            validation_results['info'].append(
                f"Task uses template_fields {task.template_fields}. Verify these fields are compatible "
                "with Airflow 2.X templating."
            )
    
    # Validate callback functions if present
    callback_fields = [
        'on_success_callback', 'on_failure_callback', 'on_retry_callback'
    ]
    
    for callback_field in callback_fields:
        if hasattr(task, callback_field) and getattr(task, callback_field) is not None:
            callback_func = getattr(task, callback_field)
            # Check if callback function signature accepts context
            try:
                sig = inspect.signature(callback_func)
                if 'context' not in sig.parameters:
                    validation_results['warnings'].append(
                        f"{callback_field} function doesn't have a 'context' parameter, "
                        "which is required in Airflow 2.X."
                    )
            except (ValueError, TypeError):
                # Can't inspect, possibly a lambda or partial
                validation_results['warnings'].append(
                    f"Could not validate signature of {callback_field}. Ensure it accepts a 'context' parameter."
                )
    
    # Check connection usage if any hook or operator that uses connections
    if hasattr(task, 'conn_id') and task.conn_id:
        conn_type = None
        
        # Try to infer connection type from operator class
        if 'Google' in operator_class or 'GCP' in operator_class or 'Gcs' in operator_class:
            conn_type = 'google_cloud_platform'
        elif 'Postgres' in operator_class:
            conn_type = 'postgres'
        elif 'MySQL' in operator_class:
            conn_type = 'mysql'
        elif 'Http' in operator_class:
            conn_type = 'http'
        elif 'Sftp' in operator_class:
            conn_type = 'sftp'
        
        if conn_type:
            conn_validation = validate_connection(task.conn_id, conn_type)
            if not conn_validation['valid']:
                if conn_validation['level'] == 'ERROR':
                    validation_results['valid'] = False
                    validation_results['errors'].append(conn_validation['message'])
                elif conn_validation['level'] == 'WARNING' and level_threshold <= VALIDATION_LEVELS['WARNING']:
                    validation_results['warnings'].append(conn_validation['message'])
    
    return validation_results


def validate_operator_import(operator_name: str, import_path: str) -> Dict:
    """
    Validates and provides updates for operator import paths in Airflow 2.X.
    
    Many operator import paths changed in Airflow 2.X, particularly those
    from contrib packages which moved to provider packages.
    
    Args:
        operator_name: The name of the operator class
        import_path: The current import path of the operator
        
    Returns:
        Dict containing validation results with correct import paths for Airflow 2.X
    
    Example:
        >>> results = validate_operator_import(
        >>>     operator_name='BashOperator',
        >>>     import_path='airflow.operators.bash_operator'
        >>> )
        >>> print(f"New import path: {results['new_path']}")
    """
    validation_result = {
        'valid': True,
        'level': 'INFO',
        'message': '',
        'old_path': import_path,
        'new_path': import_path  # Default to current path
    }
    
    # Check if operator is in the renamed operators dictionary
    if operator_name in AIRFLOW_2_RENAMED_OPERATORS:
        old_path = AIRFLOW_2_RENAMED_OPERATORS[operator_name]['old_path']
        new_path = AIRFLOW_2_RENAMED_OPERATORS[operator_name]['new_path']
        
        if import_path == old_path:
            validation_result['valid'] = False
            validation_result['level'] = 'WARNING'
            validation_result['message'] = (
                f"Operator '{operator_name}' uses deprecated import path '{old_path}'. "
                f"Update to '{new_path}' for Airflow 2.X compatibility."
            )
            validation_result['new_path'] = new_path
    
    # Check for any deprecated provider package usage
    for deprecated_package in DEPRECATED_PROVIDER_PACKAGES:
        if import_path.startswith(deprecated_package):
            # Determine the appropriate provider package
            provider_package = None
            
            if deprecated_package == 'airflow.contrib':
                if 'gcp' in import_path or 'google' in import_path:
                    provider_package = 'airflow.providers.google.cloud'
                elif 'aws' in import_path:
                    provider_package = 'airflow.providers.amazon.aws'
                elif 'azure' in import_path:
                    provider_package = 'airflow.providers.microsoft.azure'
                else:
                    # General case for other contrib modules
                    provider_package = 'airflow.providers' + import_path[len('airflow.contrib'):]
            
            elif deprecated_package == 'airflow.hooks.base_hook':
                provider_package = 'airflow.hooks.base'
            elif deprecated_package == 'airflow.hooks.dbapi_hook':
                provider_package = 'airflow.providers.common.sql'
            elif deprecated_package == 'airflow.hooks.http_hook':
                provider_package = 'airflow.providers.http.hooks.http'
            elif deprecated_package == 'airflow.operators.bash_operator':
                provider_package = 'airflow.operators.bash'
            elif deprecated_package == 'airflow.operators.python_operator':
                provider_package = 'airflow.operators.python'
            elif deprecated_package == 'airflow.sensors.base_sensor_operator':
                provider_package = 'airflow.sensors.base'
            
            if provider_package:
                validation_result['valid'] = False
                validation_result['level'] = 'WARNING'
                validation_result['message'] = (
                    f"Import path '{import_path}' is deprecated in Airflow 2.X. "
                    f"Consider using a provider package: '{provider_package}'"
                )
                validation_result['new_path'] = provider_package
    
    # Special case for direct usage of contrib
    if 'contrib' in import_path:
        validation_result['valid'] = False
        validation_result['level'] = 'WARNING'
        validation_result['message'] = (
            f"Import path '{import_path}' contains 'contrib' which is deprecated in Airflow 2.X. "
            f"Use the corresponding provider package instead."
        )
    
    return validation_result


def validate_connection(conn_id: str, conn_type: str) -> Dict:
    """
    Validates Airflow connection for compatibility with Airflow 2.X.
    
    Certain connection types in Airflow 2.X require provider packages
    to be installed, and some connection parameters have changed.
    
    Args:
        conn_id: The connection ID to validate
        conn_type: The type of connection (e.g., 'google_cloud_platform', 'postgres')
        
    Returns:
        Dict containing validation results for connection compatibility
    
    Example:
        >>> results = validate_connection('my_gcp_conn', 'google_cloud_platform')
        >>> if not results['valid']:
        >>>     print(f"Connection issue: {results['message']}")
    """
    validation_result = {
        'valid': True,
        'level': 'INFO',
        'message': '',
        'conn_id': conn_id,
        'conn_type': conn_type,
        'provider_package': None
    }
    
    # Map connection types to required provider packages
    conn_type_to_package = {
        'google_cloud_platform': 'apache-airflow-providers-google',
        'postgres': 'apache-airflow-providers-postgres',
        'mysql': 'apache-airflow-providers-mysql',
        'http': 'apache-airflow-providers-http',
        'aws': 'apache-airflow-providers-amazon',
        'ssh': 'apache-airflow-providers-ssh',
        'sftp': 'apache-airflow-providers-sftp',
        'redis': 'apache-airflow-providers-redis',
        'jdbc': 'apache-airflow-providers-jdbc',
        'mssql': 'apache-airflow-providers-microsoft-mssql'
        # Add more mappings as needed
    }
    
    # Check if connection type requires a provider package
    if conn_type in conn_type_to_package:
        provider_package = conn_type_to_package[conn_type]
        validation_result['provider_package'] = provider_package
        
        # For informational purposes, not a warning or error
        validation_result['message'] = (
            f"Connection type '{conn_type}' requires '{provider_package}' "
            f"to be installed in Airflow 2.X."
        )
    
    # Special case validations for specific connection types
    if conn_type == 'google_cloud_platform':
        try:
            # Try to check if google provider is importable
            importlib.import_module('airflow.providers.google')
        except ImportError:
            validation_result['valid'] = False
            validation_result['level'] = 'WARNING'
            validation_result['message'] = (
                f"Connection '{conn_id}' of type '{conn_type}' requires "
                f"'apache-airflow-providers-google' package to be installed."
            )
    
    # Check if connection exists in Airflow meta database
    try:
        connection = airflow.models.Connection.get_connection_from_secrets(conn_id)
        
        # Additional connection-specific validations could be added here
        
    except AirflowException:
        # Connection doesn't exist in the database, just log it
        validation_result['valid'] = False
        validation_result['level'] = 'WARNING'
        validation_result['message'] = (
            f"Connection '{conn_id}' not found in Airflow connections. "
            f"Ensure it exists before deploying to Airflow 2.X."
        )
    except Exception as e:
        # Other exceptions when trying to get the connection
        validation_result['valid'] = False
        validation_result['level'] = 'ERROR'
        validation_result['message'] = (
            f"Error validating connection '{conn_id}': {str(e)}"
        )
    
    return validation_result


def check_deprecated_features(obj: Any) -> List[Dict]:
    """
    Identifies deprecated features from Airflow 1.X in DAGs or tasks.
    
    This function examines an object (DAG, task, or other component)
    to identify deprecated features that need migration for Airflow 2.X.
    
    Args:
        obj: The object to check for deprecated features
        
    Returns:
        List of deprecated features with migration recommendations
    
    Example:
        >>> from airflow import DAG
        >>> dag = DAG(dag_id='example_dag')
        >>> deprecated_features = check_deprecated_features(dag)
        >>> for feature in deprecated_features:
        >>>     print(f"{feature['feature']}: {feature['recommendation']}")
    """
    deprecated_features = []
    
    # Determine object type
    if isinstance(obj, airflow.models.DAG):
        # Check for deprecated DAG parameters
        obj_dict = obj.__dict__
        
        # Check for deprecated params in DAG constructor
        deprecated_dag_params = {
            'concurrency': 'Use max_active_tasks instead',
            'max_active_runs_per_dag': 'Use max_active_runs instead',
            'non_pooled_task_slot_count': 'Removed in Airflow 2.X, use pool for all tasks'
        }
        
        for param, recommendation in deprecated_dag_params.items():
            if hasattr(obj, param):
                deprecated_features.append({
                    'feature': f"DAG parameter '{param}'",
                    'recommendation': recommendation,
                    'level': 'WARNING'
                })
        
        # Check for user_defined_macros (behavior slightly different in 2.X)
        if hasattr(obj, 'user_defined_macros') and obj.user_defined_macros:
            deprecated_features.append({
                'feature': 'user_defined_macros',
                'recommendation': 'Verify macros compatibility with Airflow 2.X',
                'level': 'INFO'
            })
    
    elif isinstance(obj, airflow.models.BaseOperator):
        # Check for deprecated operator parameters
        obj_dict = obj.__dict__
        
        for param, recommendation in AIRFLOW_1_DEPRECATED_PARAMS.items():
            if param in obj_dict:
                deprecated_features.append({
                    'feature': f"Operator parameter '{param}'",
                    'recommendation': recommendation,
                    'level': 'WARNING'
                })
        
        # Specific operator checks
        operator_class = obj.__class__.__name__
        
        if operator_class == 'PythonOperator':
            if obj_dict.get('provide_context', False):
                deprecated_features.append({
                    'feature': 'PythonOperator.provide_context',
                    'recommendation': 'Remove provide_context=True, context is automatically provided in Airflow 2.X',
                    'level': 'WARNING'
                })
            
            # Check for potential TaskFlow API conversion
            python_callable = obj.python_callable if hasattr(obj, 'python_callable') else None
            if python_callable and callable(python_callable):
                deprecated_features.append({
                    'feature': 'Traditional PythonOperator',
                    'recommendation': 'Consider refactoring to use TaskFlow API in Airflow 2.X',
                    'level': 'INFO'
                })
        
        # Check for deprecated methods or patterns in operators
        if hasattr(obj, 'xcom_pull') and hasattr(obj.xcom_pull, '__self__'):
            deprecated_features.append({
                'feature': 'Direct xcom_pull method usage',
                'recommendation': 'Use task instance (ti) context variable to pull XComs in Airflow 2.X',
                'level': 'WARNING'
            })
    
    # More general checks for any object type
    if hasattr(obj, '__dict__'):
        obj_dict = obj.__dict__
        
        # Look for attributes that might indicate deprecated features
        deprecated_attrs = {
            '_schedule_interval': 'schedule_interval has changed in Airflow 2.X',
            'retry_exponential_backoff': 'retry_exponential_backoff is renamed in Airflow 2.X',
            'execution_timeout': 'Verify execution_timeout behavior in Airflow 2.X'
        }
        
        for attr, recommendation in deprecated_attrs.items():
            if attr in obj_dict:
                deprecated_features.append({
                    'feature': attr,
                    'recommendation': recommendation,
                    'level': 'INFO'
                })
    
    return deprecated_features


def validate_schedule_interval(schedule_interval: Any) -> Dict:
    """
    Validates a DAG's schedule_interval for Airflow 2.X compatibility.
    
    Schedule interval behavior has some differences in Airflow 2.X,
    particularly around preset schedules and timezone handling.
    
    Args:
        schedule_interval: The schedule_interval to validate (string, timedelta, or None)
        
    Returns:
        Dict containing validation results for schedule_interval
    
    Example:
        >>> results = validate_schedule_interval('@daily')
        >>> if results['valid']:
        >>>     print("Schedule interval is compatible")
        >>> else:
        >>>     print(f"Issue: {results['message']}")
    """
    result = {
        'valid': True,
        'level': 'INFO',
        'message': '',
        'schedule_interval': schedule_interval
    }
    
    # None is always valid
    if schedule_interval is None:
        return result
    
    # Check if schedule_interval is a cron expression
    if isinstance(schedule_interval, str):
        # Check preset schedules
        presets = ['@once', '@hourly', '@daily', '@weekly', '@monthly', '@yearly']
        if schedule_interval in presets:
            # Presets are still valid
            return result
        
        # Check if it's a custom cron expression
        if not schedule_interval.startswith('@'):
            try:
                # Validate cron expression
                croniter.croniter(schedule_interval)
            except (ValueError, KeyError) as e:
                result['valid'] = False
                result['level'] = 'ERROR'
                result['message'] = f"Invalid cron expression in schedule_interval: {str(e)}"
                return result
    
    # Check if schedule_interval is a timedelta
    elif hasattr(schedule_interval, 'total_seconds'):
        # timedeltas are still valid in Airflow 2.X
        return result
    
    # If we get here with an unknown type, it's not valid
    else:
        result['valid'] = False
        result['level'] = 'ERROR'
        result['message'] = (
            f"Unsupported schedule_interval type: {type(schedule_interval)}. "
            f"Use None, string cron expression, or timedelta."
        )
    
    return result


def validate_default_args(default_args: Dict) -> Dict:
    """
    Validates DAG default_args for Airflow 2.X compatibility.
    
    Several default_args parameters have been deprecated or behave
    differently in Airflow 2.X.
    
    Args:
        default_args: The default_args dictionary to validate
        
    Returns:
        Dict containing validation results for default_args
    
    Example:
        >>> default_args = {'owner': 'airflow', 'provide_context': True}
        >>> results = validate_default_args(default_args)
        >>> if results.get('errors'):
        >>>     print(f"Errors in default_args: {results['errors']}")
    """
    results = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'info': []
    }
    
    if not isinstance(default_args, dict):
        results['valid'] = False
        results['errors'].append(
            f"default_args must be a dictionary, got {type(default_args)}"
        )
        return results
    
    # Check for deprecated parameters
    for param, message in AIRFLOW_1_DEPRECATED_PARAMS.items():
        if param in default_args:
            results['warnings'].append(
                f"Deprecated parameter '{param}' in default_args: {message}"
            )
    
    # Check for provide_context specifically (common issue)
    if 'provide_context' in default_args and default_args['provide_context']:
        results['warnings'].append(
            "provide_context=True is deprecated in Airflow 2.X. Task context is automatically provided."
        )
    
    # Check for deprecated callback patterns
    callback_keys = ['on_success_callback', 'on_failure_callback', 'on_retry_callback']
    for key in callback_keys:
        if key in default_args and default_args[key] is not None:
            callback_func = default_args[key]
            # Check if callback signature accepts context
            try:
                sig = inspect.signature(callback_func)
                if 'context' not in sig.parameters:
                    results['warnings'].append(
                        f"{key} function doesn't have a 'context' parameter, which is required in Airflow 2.X."
                    )
            except (ValueError, TypeError):
                # Can't inspect, possibly a lambda or partial
                results['info'].append(
                    f"Could not validate signature of {key}. Ensure it accepts a 'context' parameter."
                )
    
    # Check email notifications setup (behavior change in Airflow 2.X)
    if 'email' in default_args and default_args['email']:
        if not 'email_on_failure' in default_args and not 'email_on_retry' in default_args:
            results['info'].append(
                "Email address is provided but neither email_on_failure nor email_on_retry is specified. "
                "In Airflow 2.X, emails are sent only when these flags are explicitly set to True."
            )
    
    # Check for retry parameters
    if 'retries' in default_args and default_args['retries'] > 0:
        if 'retry_delay' not in default_args:
            results['info'].append(
                "retries is specified without retry_delay. Default retry_delay is 5 minutes in Airflow 2.X."
            )
    
    return results


def validate_dag_file(file_path: str, validation_level: str = DEFAULT_VALIDATION_LEVEL) -> Dict:
    """
    Validates a Python file containing DAGs for Airflow 2.X compatibility.
    
    This function analyzes a DAG file, checking:
    - Import statements for Airflow 2.X compatibility
    - DAG definitions and their parameters
    - Task definitions and operators used
    - Potential TaskFlow API usage
    
    Args:
        file_path: Path to the Python file to validate
        validation_level: Minimum validation level to report (ERROR, WARNING, INFO)
        
    Returns:
        Dict containing validation results for the entire DAG file
    
    Example:
        >>> results = validate_dag_file('/path/to/my_dag.py')
        >>> print(f"Found {len(results['dags'])} DAGs in file")
        >>> if not results['success']:
        >>>     print(f"Issues found: {results['issues']}")
    """
    validation_results = {
        'success': True,
        'file_path': file_path,
        'dags': [],
        'issues': {
            'errors': [],
            'warnings': [],
            'info': []
        }
    }
    
    level_threshold = VALIDATION_LEVELS.get(validation_level.upper(), VALIDATION_LEVELS['WARNING'])
    
    # Check if file exists
    if not os.path.isfile(file_path):
        validation_results['success'] = False
        validation_results['issues']['errors'].append({
            'component': 'file',
            'message': f"File not found: {file_path}"
        })
        return validation_results
    
    try:
        # Read the file content to analyze imports
        with open(file_path, 'r') as f:
            file_content = f.read()
        
        # Check for deprecated imports using regex
        import_patterns = {
            r'from\s+airflow\.contrib\s+import': 'airflow.contrib is deprecated in Airflow 2.X, use provider packages instead',
            r'from\s+airflow\.hooks\.base_hook\s+import': 'airflow.hooks.base_hook is replaced by airflow.hooks.base in Airflow 2.X',
            r'from\s+airflow\.hooks\.dbapi_hook\s+import': 'airflow.hooks.dbapi_hook is replaced by airflow.providers.common.sql in Airflow 2.X',
            r'from\s+airflow\.operators\.bash_operator\s+import': 'airflow.operators.bash_operator is replaced by airflow.operators.bash in Airflow 2.X',
            r'from\s+airflow\.operators\.python_operator\s+import': 'airflow.operators.python_operator is replaced by airflow.operators.python in Airflow 2.X',
            r'from\s+airflow\.sensors\.base_sensor_operator\s+import': 'airflow.sensors.base_sensor_operator is replaced by airflow.sensors.base in Airflow 2.X'
        }
        
        for pattern, message in import_patterns.items():
            if re.search(pattern, file_content):
                if level_threshold <= VALIDATION_LEVELS['WARNING']:
                    validation_results['issues']['warnings'].append({
                        'component': 'imports',
                        'message': message
                    })
        
        # Check for TaskFlow API usage
        taskflow_pattern = r'@task\s*\('
        if re.search(taskflow_pattern, file_content):
            if level_threshold <= VALIDATION_LEVELS['INFO']:
                validation_results['issues']['info'].append({
                    'component': 'TaskFlow API',
                    'message': "File uses TaskFlow API, which is new in Airflow 2.X. Ensure proper implementation."
                })
        
        # Try to load the module to inspect DAGs
        # This approach requires the file to be importable
        module_name = os.path.basename(file_path).replace('.py', '')
        module_dir = os.path.dirname(file_path)
        
        # Add the module directory to sys.path temporarily if it's not already there
        import sys
        original_path = sys.path.copy()
        if module_dir not in sys.path:
            sys.path.insert(0, module_dir)
        
        try:
            # Try to import the module
            module = importlib.import_module(module_name)
            
            # Find DAG objects in the module
            dag_objects = []
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, airflow.models.DAG):
                    dag_objects.append(attr)
            
            # Validate each DAG found
            for dag in dag_objects:
                dag_validation = validate_dag(dag, validation_level)
                validation_results['dags'].append({
                    'dag_id': dag.dag_id,
                    'validation': dag_validation
                })
                
                # Update the overall success status
                if not dag_validation['success']:
                    validation_results['success'] = False
                
                # Add the DAG's issues to the file's issues
                for error in dag_validation['issues']['errors']:
                    validation_results['issues']['errors'].append({
                        'component': f"DAG[{dag.dag_id}].{error['component']}",
                        'message': error['message']
                    })
                
                if level_threshold <= VALIDATION_LEVELS['WARNING']:
                    for warning in dag_validation['issues']['warnings']:
                        validation_results['issues']['warnings'].append({
                            'component': f"DAG[{dag.dag_id}].{warning['component']}",
                            'message': warning['message']
                        })
                
                if level_threshold <= VALIDATION_LEVELS['INFO']:
                    for info in dag_validation['issues']['info']:
                        validation_results['issues']['info'].append({
                            'component': f"DAG[{dag.dag_id}].{info['component']}",
                            'message': info['message']
                        })
            
            # Add a warning if no DAGs were found
            if not dag_objects and level_threshold <= VALIDATION_LEVELS['WARNING']:
                validation_results['issues']['warnings'].append({
                    'component': 'file',
                    'message': f"No DAGs found in file: {file_path}"
                })
                
        except Exception as e:
            # Error importing or analyzing the module
            validation_results['success'] = False
            validation_results['issues']['errors'].append({
                'component': 'import',
                'message': f"Error importing DAG file: {str(e)}"
            })
            
        finally:
            # Restore the original sys.path
            sys.path = original_path
    
    except Exception as e:
        # Error reading or analyzing the file
        validation_results['success'] = False
        validation_results['issues']['errors'].append({
            'component': 'file',
            'message': f"Error analyzing DAG file: {str(e)}"
        })
    
    # Add summary information
    validation_results['summary'] = {
        'dag_count': len(validation_results['dags']),
        'error_count': len(validation_results['issues']['errors']),
        'warning_count': len(validation_results['issues']['warnings']),
        'info_count': len(validation_results['issues']['info'])
    }
    
    return validation_results


def check_taskflow_convertible(python_operator: Any) -> Dict:
    """
    Analyzes if a PythonOperator can be converted to use TaskFlow API.
    
    TaskFlow API is a new feature in Airflow 2.X that simplifies DAG authoring,
    especially for Python functions. This function checks if a PythonOperator
    is suitable for conversion to TaskFlow API.
    
    Args:
        python_operator: The PythonOperator to analyze
        
    Returns:
        Dict containing analysis results with conversion guidance
    
    Example:
        >>> from airflow.operators.python import PythonOperator
        >>> def my_func(x, y):
        >>>     return x + y
        >>> task = PythonOperator(task_id='add', python_callable=my_func, op_args=[1, 2])
        >>> results = check_taskflow_convertible(task)
        >>> if results['convertible']:
        >>>     print(f"TaskFlow conversion: {results['taskflow_example']}")
    """
    result = {
        'convertible': False,
        'task_id': getattr(python_operator, 'task_id', 'unknown'),
        'reasons': [],
        'taskflow_example': None
    }
    
    # Check if this is actually a PythonOperator
    if not hasattr(python_operator, '__class__') or python_operator.__class__.__name__ != 'PythonOperator':
        result['reasons'].append("Not a PythonOperator instance")
        return result
    
    # Check for python_callable attribute
    if not hasattr(python_operator, 'python_callable') or not callable(python_operator.python_callable):
        result['reasons'].append("Missing or non-callable python_callable function")
        return result
    
    python_callable = python_operator.python_callable
    task_id = python_operator.task_id
    
    # Analyze the python_callable function
    try:
        # Inspect the function signature
        sig = inspect.signature(python_callable)
        
        # Check for op_args and op_kwargs
        has_op_args = hasattr(python_operator, 'op_args') and python_operator.op_args
        has_op_kwargs = hasattr(python_operator, 'op_kwargs') and python_operator.op_kwargs
        
        # Check for provide_context
        provide_context = getattr(python_operator, 'provide_context', False)
        
        # Check for other attributes that might complicate conversion
        uses_templates = (
            hasattr(python_operator, 'templates_dict') and 
            python_operator.templates_dict
        )
        
        # Determine if XCom push is disabled
        xcom_push_disabled = (
            hasattr(python_operator, 'do_xcom_push') and 
            not python_operator.do_xcom_push
        )
        
        # Check for conditions that might prevent conversion
        if xcom_push_disabled:
            result['reasons'].append("do_xcom_push is False, TaskFlow API always pushes return value to XCom")
        
        if uses_templates:
            result['reasons'].append("Uses templates_dict, which has a different pattern in TaskFlow API")
        
        # Function argument analysis
        args_from_sig = list(sig.parameters.keys())
        context_in_sig = 'context' in args_from_sig
        ti_in_sig = 'ti' in args_from_sig
        kwargs_in_sig = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in sig.parameters.values())
        
        # If the function uses context or ti explicitly, it will need adjustment
        if context_in_sig or ti_in_sig:
            result['reasons'].append(
                f"Function explicitly uses {'context' if context_in_sig else 'ti'} parameter, "
                f"which needs adjustment in TaskFlow API"
            )
        
        # If too many reasons not to convert, return early
        if len(result['reasons']) >= 3:
            return result
        
        # If we got here, it's likely convertible
        result['convertible'] = True
        
        # Generate a TaskFlow example
        func_name = python_callable.__name__
        args_str = ""
        
        # Add args from op_args
        if has_op_args:
            for i, arg in enumerate(python_operator.op_args):
                if i < len(args_from_sig) and args_from_sig[i] != 'context' and args_from_sig[i] != 'ti':
                    # Use the parameter name from the signature if available
                    args_str += f"{args_from_sig[i]}={repr(arg)}, "
                else:
                    # Otherwise use a generic name
                    args_str += f"arg{i}={repr(arg)}, "
        
        # Add kwargs from op_kwargs
        if has_op_kwargs:
            for k, v in python_operator.op_kwargs.items():
                if k != 'context' and k != 'ti':
                    args_str += f"{k}={repr(v)}, "
        
        # Assemble the TaskFlow example
        taskflow_example = f"""
from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2021, 1, 1), schedule_interval='@daily')
def {task_id}_dag():
    
    @task
    def {func_name}({args_str[:-2]}):
        # Original function code here
        pass
    
    {func_name}()

{task_id}_dag_instance = {task_id}_dag()
"""
        result['taskflow_example'] = taskflow_example.strip()
        
    except Exception as e:
        result['reasons'].append(f"Error analyzing function: {str(e)}")
        result['convertible'] = False
    
    return result


def generate_migration_report(dag_directory: str, output_file: str, format: str = 'json') -> bool:
    """
    Generates a comprehensive migration report for a DAG directory.
    
    This function analyzes all DAG files in a directory and produces
    a detailed report of migration issues and recommendations.
    
    Args:
        dag_directory: Path to the directory containing DAG files
        output_file: Path where the report should be saved (local or GCS)
        format: Report format ('json', 'html', or 'txt')
        
    Returns:
        True if report was generated successfully, False otherwise
    
    Example:
        >>> success = generate_migration_report(
        >>>     dag_directory='/path/to/dags', 
        >>>     output_file='gs://bucket/reports/migration_report.json',
        >>>     format='json'
        >>> )
        >>> if success:
        >>>     print("Migration report generated successfully")
    """
    if not os.path.isdir(dag_directory):
        logger.error(f"Directory not found: {dag_directory}")
        return False
    
    # Validate format
    valid_formats = ['json', 'html', 'txt']
    if format.lower() not in valid_formats:
        logger.error(f"Invalid format: {format}. Must be one of {valid_formats}")
        return False
    
    try:
        # Find all .py files in the directory (and subdirectories)
        dag_files = []
        for root, _, files in os.walk(dag_directory):
            for file in files:
                if file.endswith('.py'):
                    dag_files.append(os.path.join(root, file))
        
        if not dag_files:
            logger.warning(f"No Python files found in {dag_directory}")
            return False
        
        logger.info(f"Found {len(dag_files)} Python files to analyze")
        
        # Prepare the report structure
        report = {
            'generated_at': str(airflow.utils.timezone.utcnow()),
            'dag_directory': dag_directory,
            'num_files_analyzed': len(dag_files),
            'summary': {
                'total_dags': 0,
                'total_files': len(dag_files),
                'compatible_dags': 0,
                'incompatible_dags': 0,
                'total_errors': 0,
                'total_warnings': 0,
                'total_info': 0,
                'common_issues': []
            },
            'file_results': []
        }
        
        # Track issues for summary
        issue_counts = {}
        
        # Analyze each file
        for file_path in dag_files:
            logger.info(f"Analyzing file: {file_path}")
            
            file_validation = validate_dag_file(file_path)
            file_rel_path = os.path.relpath(file_path, dag_directory)
            
            # Add file results to the report
            file_result = {
                'file_path': file_rel_path,
                'success': file_validation['success'],
                'dags': file_validation.get('dags', []),
                'issues': file_validation.get('issues', {
                    'errors': [],
                    'warnings': [],
                    'info': []
                })
            }
            
            report['file_results'].append(file_result)
            
            # Update summary statistics
            num_dags = len(file_validation.get('dags', []))
            report['summary']['total_dags'] += num_dags
            
            num_errors = len(file_validation.get('issues', {}).get('errors', []))
            num_warnings = len(file_validation.get('issues', {}).get('warnings', []))
            num_info = len(file_validation.get('issues', {}).get('info', []))
            
            report['summary']['total_errors'] += num_errors
            report['summary']['total_warnings'] += num_warnings
            report['summary']['total_info'] += num_info
            
            if file_validation['success']:
                report['summary']['compatible_dags'] += num_dags
            else:
                report['summary']['incompatible_dags'] += num_dags
            
            # Count issue occurrences for summary
            for issue_type in ['errors', 'warnings', 'info']:
                for issue in file_validation.get('issues', {}).get(issue_type, []):
                    issue_key = f"{issue_type.upper()}: {issue['message']}"
                    if issue_key in issue_counts:
                        issue_counts[issue_key] += 1
                    else:
                        issue_counts[issue_key] = 1
        
        # Add common issues to summary (top 10)
        sorted_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)
        report['summary']['common_issues'] = [
            {'issue': issue, 'count': count}
            for issue, count in sorted_issues[:10]
        ]
        
        # Add migration recommendations
        report['recommendations'] = {
            'general': [
                "Install required provider packages for your specific operators and hooks",
                "Update import paths according to Airflow 2.X conventions",
                "Remove provide_context=True from PythonOperators",
                "Ensure callback functions accept a 'context' parameter",
                "Consider adopting TaskFlow API for Python functions"
            ],
            'provider_packages': [
                "apache-airflow-providers-google for GCP integrations",
                "apache-airflow-providers-amazon for AWS integrations",
                "apache-airflow-providers-postgres for PostgreSQL connections",
                "apache-airflow-providers-http for HTTP connections",
                "apache-airflow-providers-ssh for SSH connections"
            ]
        }
        
        # Format and save the report
        if format.lower() == 'json':
            report_content = json.dumps(report, indent=2)
        elif format.lower() == 'html':
            # Simple HTML report
            html_template = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Airflow 2.X Migration Report</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    h1, h2, h3 { color: #333; }
                    .summary { background: #f5f5f5; padding: 15px; border-radius: 5px; }
                    .file { margin-top: 20px; border: 1px solid #ddd; padding: 10px; border-radius: 5px; }
                    .success { color: green; }
                    .failure { color: red; }
                    .error { color: red; }
                    .warning { color: orange; }
                    .info { color: blue; }
                </style>
            </head>
            <body>
                <h1>Airflow 2.X Migration Report</h1>
                <div class="summary">
                    <h2>Summary</h2>
                    <p>Generated at: %s</p>
                    <p>Directory: %s</p>
                    <p>Files analyzed: %d</p>
                    <p>Total DAGs: %d</p>
                    <p>Compatible DAGs: <span class="success">%d</span></p>
                    <p>Incompatible DAGs: <span class="failure">%d</span></p>
                    <p>Total issues: %d (%d errors, %d warnings, %d info)</p>
                    
                    <h3>Common Issues</h3>
                    <ul>
                        %s
                    </ul>
                    
                    <h3>Recommendations</h3>
                    <ul>
                        %s
                    </ul>
                </div>
                
                <h2>File Results</h2>
                %s
            </body>
            </html>
            """
            
            # Format common issues
            common_issues_html = ""
            for issue in report['summary']['common_issues']:
                common_issues_html += f"<li>{issue['issue']} (Count: {issue['count']})</li>"
            
            # Format recommendations
            recommendations_html = ""
            for rec in report['recommendations']['general']:
                recommendations_html += f"<li>{rec}</li>"
            
            # Format file results
            file_results_html = ""
            for file_result in report['file_results']:
                status_class = "success" if file_result['success'] else "failure"
                
                file_results_html += f"""
                <div class="file">
                    <h3>{file_result['file_path']} <span class="{status_class}">({status_class})</span></h3>
                    <p>DAGs: {len(file_result['dags'])}</p>
                    
                    <h4>Issues</h4>
                """
                
                if file_result['issues']['errors']:
                    file_results_html += "<h5>Errors</h5><ul>"
                    for issue in file_result['issues']['errors']:
                        file_results_html += f"<li class='error'>{issue['component']}: {issue['message']}</li>"
                    file_results_html += "</ul>"
                
                if file_result['issues']['warnings']:
                    file_results_html += "<h5>Warnings</h5><ul>"
                    for issue in file_result['issues']['warnings']:
                        file_results_html += f"<li class='warning'>{issue['component']}: {issue['message']}</li>"
                    file_results_html += "</ul>"
                
                if file_result['issues']['info']:
                    file_results_html += "<h5>Info</h5><ul>"
                    for issue in file_result['issues']['info']:
                        file_results_html += f"<li class='info'>{issue['component']}: {issue['message']}</li>"
                    file_results_html += "</ul>"
                
                file_results_html += "</div>"
            
            report_content = html_template % (
                report['generated_at'],
                report['dag_directory'],
                report['summary']['total_files'],
                report['summary']['total_dags'],
                report['summary']['compatible_dags'],
                report['summary']['incompatible_dags'],
                report['summary']['total_errors'] + report['summary']['total_warnings'] + report['summary']['total_info'],
                report['summary']['total_errors'],
                report['summary']['total_warnings'],
                report['summary']['total_info'],
                common_issues_html,
                recommendations_html,
                file_results_html
            )
        else:  # txt format
            report_content = f"Airflow 2.X Migration Report\n"
            report_content += f"=========================\n\n"
            report_content += f"Generated at: {report['generated_at']}\n"
            report_content += f"Directory: {report['dag_directory']}\n"
            report_content += f"Files analyzed: {report['summary']['total_files']}\n\n"
            
            report_content += f"Summary\n-------\n"
            report_content += f"Total DAGs: {report['summary']['total_dags']}\n"
            report_content += f"Compatible DAGs: {report['summary']['compatible_dags']}\n"
            report_content += f"Incompatible DAGs: {report['summary']['incompatible_dags']}\n"
            report_content += f"Total issues: {report['summary']['total_errors'] + report['summary']['total_warnings'] + report['summary']['total_info']}\n"
            report_content += f"  - Errors: {report['summary']['total_errors']}\n"
            report_content += f"  - Warnings: {report['summary']['total_warnings']}\n"
            report_content += f"  - Info: {report['summary']['total_info']}\n\n"
            
            report_content += f"Common Issues\n-------------\n"
            for i, issue in enumerate(report['summary']['common_issues']):
                report_content += f"{i+1}. {issue['issue']} (Count: {issue['count']})\n"
            
            report_content += f"\nRecommendations\n---------------\n"
            for i, rec in enumerate(report['recommendations']['general']):
                report_content += f"{i+1}. {rec}\n"
            
            report_content += f"\nFile Results\n------------\n"
            for file_result in report['file_results']:
                status = "PASS" if file_result['success'] else "FAIL"
                report_content += f"\n{file_result['file_path']} ({status})\n"
                report_content += f"  DAGs: {len(file_result['dags'])}\n"
                
                if file_result['issues']['errors']:
                    report_content += f"  Errors:\n"
                    for issue in file_result['issues']['errors']:
                        report_content += f"    - {issue['component']}: {issue['message']}\n"
                
                if file_result['issues']['warnings']:
                    report_content += f"  Warnings:\n"
                    for issue in file_result['issues']['warnings']:
                        report_content += f"    - {issue['component']}: {issue['message']}\n"
                
                if file_result['issues']['info']:
                    report_content += f"  Info:\n"
                    for issue in file_result['issues']['info']:
                        report_content += f"    - {issue['component']}: {issue['message']}\n"
        
        # Save the report
        if output_file.lower().startswith('gs://'):
            # Google Cloud Storage path
            bucket_name, object_name = output_file[5:].split('/', 1)
            
            # Create a temporary local file
            temp_file_path = f"/tmp/migration_report_{os.getpid()}.{format}"
            with open(temp_file_path, 'w') as f:
                f.write(report_content)
            
            # Upload to GCS
            try:
                gcs_path = gcs_upload_file(
                    local_file_path=temp_file_path,
                    bucket_name=bucket_name,
                    object_name=object_name
                )
                logger.info(f"Migration report uploaded to {gcs_path}")
                
                # Clean up temp file
                os.remove(temp_file_path)
                
            except Exception as e:
                logger.error(f"Failed to upload report to GCS: {str(e)}")
                return False
                
        else:
            # Local file path
            try:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
                
                # Write the report
                with open(output_file, 'w') as f:
                    f.write(report_content)
                
                logger.info(f"Migration report saved to {output_file}")
                
            except Exception as e:
                logger.error(f"Failed to save report: {str(e)}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error generating migration report: {str(e)}")
        return False


class DAGValidator:
    """
    Class for validating DAGs for Airflow 2.X compatibility.
    
    This class provides methods to validate individual DAGs or entire DAG files,
    and to generate comprehensive reports on compatibility issues.
    
    Attributes:
        validation_level: The minimum level for reporting issues (ERROR, WARNING, INFO)
        validation_results: Dictionary containing validation results
        errors: List of error messages
        warnings: List of warning messages
        info: List of informational messages
    
    Example:
        >>> validator = DAGValidator(validation_level='WARNING')
        >>> dag = DAG(dag_id='example_dag')
        >>> results = validator.validate_dag(dag)
        >>> report = validator.generate_report(format='json')
    """
    
    def __init__(self, validation_level: str = DEFAULT_VALIDATION_LEVEL):
        """
        Initialize the DAG validator with specified validation level.
        
        Args:
            validation_level: The minimum level for reporting issues (ERROR, WARNING, INFO)
        """
        self.validation_level = validation_level
        self.validation_results = {
            'success': True,
            'dags_validated': 0,
            'issues': {
                'errors': [],
                'warnings': [],
                'info': []
            }
        }
        self.errors = []
        self.warnings = []
        self.info = []
    
    def validate_dag(self, dag: Any) -> Dict:
        """
        Validates a single DAG for Airflow 2.X compatibility.
        
        This method is a wrapper around the validate_dag function that also
        stores the validation results in the validator's state.
        
        Args:
            dag: The DAG object to validate
        
        Returns:
            Dict containing validation results for the DAG
        """
        dag_validation = validate_dag(dag, self.validation_level)
        
        # Store results in validator's state
        self.validation_results['dags_validated'] += 1
        
        if not dag_validation['success']:
            self.validation_results['success'] = False
            
        # Add the DAG's issues to the validator's state
        for error in dag_validation['issues']['errors']:
            self.add_error(error['message'], error['component'])
            
        for warning in dag_validation['issues']['warnings']:
            self.add_warning(warning['message'], warning['component'])
            
        for info in dag_validation['issues']['info']:
            self.add_info(info['message'], info['component'])
        
        return dag_validation
    
    def validate_dags_in_file(self, file_path: str) -> Dict:
        """
        Validates all DAGs in a Python file.
        
        This method is a wrapper around the validate_dag_file function that also
        stores the validation results in the validator's state.
        
        Args:
            file_path: Path to the Python file containing DAGs
        
        Returns:
            Dict containing validation results for the entire file
        """
        file_validation = validate_dag_file(file_path, self.validation_level)
        
        # Store results in validator's state
        self.validation_results['dags_validated'] += len(file_validation.get('dags', []))
        
        if not file_validation['success']:
            self.validation_results['success'] = False
            
        # Add the file's issues to the validator's state
        for error in file_validation['issues']['errors']:
            self.add_error(error['message'], error['component'])
            
        for warning in file_validation['issues']['warnings']:
            self.add_warning(warning['message'], warning['component'])
            
        for info in file_validation['issues']['info']:
            self.add_info(info['message'], info['component'])
        
        return file_validation
    
    def add_error(self, message: str, component: str) -> None:
        """
        Adds an error message to validation results.
        
        Args:
            message: The error message
            component: The component that has the error
        """
        error = {'component': component, 'message': message}
        self.errors.append(error)
        self.validation_results['issues']['errors'].append(error)
        logger.error(f"{component}: {message}")
    
    def add_warning(self, message: str, component: str) -> None:
        """
        Adds a warning message to validation results.
        
        Args:
            message: The warning message
            component: The component that has the warning
        """
        warning = {'component': component, 'message': message}
        self.warnings.append(warning)
        self.validation_results['issues']['warnings'].append(warning)
        logger.warning(f"{component}: {message}")
    
    def add_info(self, message: str, component: str) -> None:
        """
        Adds an info message to validation results.
        
        Args:
            message: The info message
            component: The component that has the info
        """
        info = {'component': component, 'message': message}
        self.info.append(info)
        self.validation_results['issues']['info'].append(info)
        logger.info(f"{component}: {message}")
    
    def generate_report(self, format: str = 'json') -> str:
        """
        Generates a formatted validation report.
        
        Args:
            format: The format of the report ('json', 'html', or 'txt')
        
        Returns:
            String containing the formatted report
        """
        # Add summary information
        summary = {
            'dags_validated': self.validation_results['dags_validated'],
            'success': self.validation_results['success'],
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'info_count': len(self.info)
        }
        
        report = {
            'summary': summary,
            'validation_level': self.validation_level,
            'results': self.validation_results
        }
        
        # Format the report based on the specified format
        if format.lower() == 'json':
            return json.dumps(report, indent=2)
        
        elif format.lower() == 'html':
            # Simple HTML report
            html_template = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Airflow 2.X Compatibility Report</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    h1, h2, h3 { color: #333; }
                    .summary { background: #f5f5f5; padding: 15px; border-radius: 5px; }
                    .error { color: red; }
                    .warning { color: orange; }
                    .info { color: blue; }
                    .success { color: green; }
                    .failure { color: red; }
                </style>
            </head>
            <body>
                <h1>Airflow 2.X Compatibility Report</h1>
                
                <div class="summary">
                    <h2>Summary</h2>
                    <p>DAGs validated: %d</p>
                    <p>Overall status: <span class="%s">%s</span></p>
                    <p>Issues found: %d</p>
                    <ul>
                        <li class="error">Errors: %d</li>
                        <li class="warning">Warnings: %d</li>
                        <li class="info">Info: %d</li>
                    </ul>
                </div>
                
                <h2>Issues</h2>
                
                <h3 class="error">Errors</h3>
                <ul>
                %s
                </ul>
                
                <h3 class="warning">Warnings</h3>
                <ul>
                %s
                </ul>
                
                <h3 class="info">Information</h3>
                <ul>
                %s
                </ul>
            </body>
            </html>
            """
            
            status_class = "success" if self.validation_results['success'] else "failure"
            status_text = "COMPATIBLE" if self.validation_results['success'] else "INCOMPATIBLE"
            
            errors_html = ""
            for error in self.errors:
                errors_html += f"<li>{error['component']}: {error['message']}</li>"
            
            warnings_html = ""
            for warning in self.warnings:
                warnings_html += f"<li>{warning['component']}: {warning['message']}</li>"
            
            info_html = ""
            for info in self.info:
                info_html += f"<li>{info['component']}: {info['message']}</li>"
            
            return html_template % (
                summary['dags_validated'],
                status_class,
                status_text,
                summary['error_count'] + summary['warning_count'] + summary['info_count'],
                summary['error_count'],
                summary['warning_count'],
                summary['info_count'],
                errors_html or "<li>No errors found.</li>",
                warnings_html or "<li>No warnings found.</li>",
                info_html or "<li>No informational messages.</li>"
            )
        
        else:  # txt format
            report_str = "Airflow 2.X Compatibility Report\n"
            report_str += "===============================\n\n"
            
            report_str += "Summary\n-------\n"
            report_str += f"DAGs validated: {summary['dags_validated']}\n"
            report_str += f"Overall status: {('COMPATIBLE' if summary['success'] else 'INCOMPATIBLE')}\n"
            report_str += f"Total issues: {summary['error_count'] + summary['warning_count'] + summary['info_count']}\n"
            report_str += f"  - Errors: {summary['error_count']}\n"
            report_str += f"  - Warnings: {summary['warning_count']}\n"
            report_str += f"  - Info: {summary['info_count']}\n\n"
            
            report_str += "Errors\n------\n"
            for error in self.errors:
                report_str += f"{error['component']}: {error['message']}\n"
            if not self.errors:
                report_str += "No errors found.\n"
            
            report_str += "\nWarnings\n--------\n"
            for warning in self.warnings:
                report_str += f"{warning['component']}: {warning['message']}\n"
            if not self.warnings:
                report_str += "No warnings found.\n"
            
            report_str += "\nInformation\n-----------\n"
            for info in self.info:
                report_str += f"{info['component']}: {info['message']}\n"
            if not self.info:
                report_str += "No informational messages.\n"
            
            return report_str


class OperatorValidator:
    """
    Class for validating Airflow operators for Airflow 2.X compatibility.
    
    This class provides methods to validate Airflow operators and operator classes,
    and to determine the Airflow 2.X equivalents of Airflow 1.X operators.
    
    Attributes:
        validation_level: The minimum level for reporting issues (ERROR, WARNING, INFO)
        validation_results: Dictionary containing validation results
    
    Example:
        >>> validator = OperatorValidator()
        >>> from airflow.operators.bash_operator import BashOperator
        >>> task = BashOperator(task_id='bash_task', bash_command='echo "Hello"')
        >>> results = validator.validate_operator(task)
    """
    
    def __init__(self, validation_level: str = DEFAULT_VALIDATION_LEVEL):
        """
        Initialize the operator validator with specified validation level.
        
        Args:
            validation_level: The minimum level for reporting issues (ERROR, WARNING, INFO)
        """
        self.validation_level = validation_level
        self.validation_results = {
            'valid': True,
            'operators_validated': 0,
            'issues': []
        }
    
    def validate_operator(self, operator: Any) -> Dict:
        """
        Validates an operator instance for Airflow 2.X compatibility.
        
        This method is a wrapper around the validate_task function that also
        stores the validation results in the validator's state.
        
        Args:
            operator: The operator instance to validate
        
        Returns:
            Dict containing validation results for the operator
        """
        operator_validation = validate_task(operator, self.validation_level)
        
        # Store results in validator's state
        self.validation_results['operators_validated'] += 1
        
        if not operator_validation['valid']:
            self.validation_results['valid'] = False
            
        # Add the operator's issues to the validator's state
        for issue_type in ['errors', 'warnings', 'info']:
            for issue in operator_validation.get(issue_type, []):
                self.validation_results['issues'].append({
                    'type': issue_type,
                    'operator': operator.__class__.__name__,
                    'message': issue
                })
        
        return operator_validation
    
    def validate_operator_class(self, operator_class_path: str) -> Dict:
        """
        Validates an operator class for Airflow 2.X compatibility.
        
        This method checks if an operator class is deprecated or renamed
        in Airflow 2.X based on its import path.
        
        Args:
            operator_class_path: The import path of the operator class
        
        Returns:
            Dict containing validation results for the operator class
        """
        # Extract the operator name from the class path
        parts = operator_class_path.split('.')
        operator_name = parts[-1]
        import_path = '.'.join(parts[:-1])
        
        # Validate the import path
        import_validation = validate_operator_import(operator_name, import_path)
        
        # Store results in validator's state
        self.validation_results['operators_validated'] += 1
        
        if not import_validation['valid']:
            self.validation_results['valid'] = False
            
            # Add the issue to the validator's state
            self.validation_results['issues'].append({
                'type': import_validation['level'].lower(),
                'operator': operator_name,
                'message': import_validation['message']
            })
        
        # Get Airflow 2.X equivalent if available
        airflow2_equivalent = self.get_airflow2_equivalent(operator_name, import_path)
        
        # Combine the import validation with the Airflow 2.X equivalent info
        result = {**import_validation, 'airflow2_equivalent': airflow2_equivalent}
        
        return result
    
    def get_airflow2_equivalent(self, operator_name: str, import_path: str) -> Dict:
        """
        Gets the Airflow 2.X equivalent for an Airflow 1.X operator.
        
        Args:
            operator_name: The name of the operator class
            import_path: The import path of the operator
        
        Returns:
            Dict containing Airflow 2.X equivalent information
        """
        result = {
            'name': operator_name,
            'import_path': import_path,
            'new_import_path': import_path,  # Default to current path
            'provider_package': None,
            'example': None
        }
        
        # Check if operator is in the renamed operators dictionary
        if operator_name in AIRFLOW_2_RENAMED_OPERATORS:
            new_path = AIRFLOW_2_RENAMED_OPERATORS[operator_name]['new_path']
            result['new_import_path'] = new_path
            
            # Determine if a provider package is needed
            if 'providers' in new_path:
                provider_parts = new_path.split('providers.')[1].split('.')
                provider_package = f"apache-airflow-providers-{provider_parts[0]}"
                if len(provider_parts) > 1:
                    provider_package += f"-{provider_parts[1]}"
                result['provider_package'] = provider_package
            
            # Generate example usage
            result['example'] = f"from {new_path} import {operator_name}"
        
        # Check for any deprecated provider package usage
        elif any(import_path.startswith(pkg) for pkg in DEPRECATED_PROVIDER_PACKAGES):
            # Determine the appropriate provider package
            if 'contrib' in import_path:
                if 'gcp' in import_path or 'google' in import_path:
                    provider = 'google.cloud'
                    result['provider_package'] = 'apache-airflow-providers-google'
                elif 'aws' in import_path:
                    provider = 'amazon.aws'
                    result['provider_package'] = 'apache-airflow-providers-amazon'
                elif 'azure' in import_path:
                    provider = 'microsoft.azure'
                    result['provider_package'] = 'apache-airflow-providers-microsoft'
                else:
                    # General case - guess based on name
                    provider_part = import_path.split('.')[-2] if len(import_path.split('.')) > 2 else 'generic'
                    provider = provider_part
                    result['provider_package'] = f"apache-airflow-providers-{provider_part}"
                
                new_path = f"airflow.providers.{provider}"
                if 'operators' in import_path:
                    new_path += '.operators'
                elif 'sensors' in import_path:
                    new_path += '.sensors'
                elif 'hooks' in import_path:
                    new_path += '.hooks'
                
                result['new_import_path'] = new_path
                result['example'] = f"from {new_path} import {operator_name}"
            
            elif import_path == 'airflow.operators.bash_operator':
                result['new_import_path'] = 'airflow.operators.bash'
                result['example'] = f"from airflow.operators.bash import {operator_name}"
            
            elif import_path == 'airflow.operators.python_operator':
                result['new_import_path'] = 'airflow.operators.python'
                result['example'] = f"from airflow.operators.python import {operator_name}"
            
            elif import_path == 'airflow.sensors.base_sensor_operator':
                result['new_import_path'] = 'airflow.sensors.base'
                result['example'] = f"from airflow.sensors.base import {operator_name}"
        
        return result


class ConnectionValidator:
    """
    Class for validating Airflow connections for Airflow 2.X compatibility.
    
    This class provides methods to validate Airflow connections and connection types,
    and to test connections to verify they work in Airflow 2.X.
    
    Attributes:
        validation_level: The minimum level for reporting issues (ERROR, WARNING, INFO)
        validation_results: Dictionary containing validation results
    
    Example:
        >>> validator = ConnectionValidator()
        >>> results = validator.validate_connection('my_postgres_conn', 'postgres')
        >>> if not results['valid']:
        >>>     print(f"Connection issue: {results['message']}")
    """
    
    def __init__(self, validation_level: str = DEFAULT_VALIDATION_LEVEL):
        """
        Initialize the connection validator with specified validation level.
        
        Args:
            validation_level: The minimum level for reporting issues (ERROR, WARNING, INFO)
        """
        self.validation_level = validation_level
        self.validation_results = {
            'valid': True,
            'connections_validated': 0,
            'issues': []
        }
    
    def validate_connection(self, conn_id: str, conn_type: str) -> Dict:
        """
        Validates an Airflow connection for Airflow 2.X compatibility.
        
        This method is a wrapper around the validate_connection function that also
        stores the validation results in the validator's state.
        
        Args:
            conn_id: The connection ID to validate
            conn_type: The type of connection
        
        Returns:
            Dict containing validation results for the connection
        """
        connection_validation = validate_connection(conn_id, conn_type)
        
        # Store results in validator's state
        self.validation_results['connections_validated'] += 1
        
        if not connection_validation['valid']:
            self.validation_results['valid'] = False
            
            # Add the issue to the validator's state
            self.validation_results['issues'].append({
                'type': connection_validation['level'].lower(),
                'conn_id': conn_id,
                'conn_type': conn_type,
                'message': connection_validation['message']
            })
        
        return connection_validation
    
    def validate_connection_type(self, conn_type: str) -> Dict:
        """
        Validates a connection type for Airflow 2.X compatibility.
        
        This method checks if a connection type requires provider packages
        in Airflow 2.X.
        
        Args:
            conn_type: The connection type to validate
        
        Returns:
            Dict containing validation results for the connection type
        """
        result = {
            'valid': True,
            'conn_type': conn_type,
            'provider_package': None,
            'message': ''
        }
        
        # Map connection types to required provider packages
        conn_type_to_package = {
            'google_cloud_platform': 'apache-airflow-providers-google',
            'postgres': 'apache-airflow-providers-postgres',
            'mysql': 'apache-airflow-providers-mysql',
            'mssql': 'apache-airflow-providers-microsoft-mssql',
            'oracle': 'apache-airflow-providers-oracle',
            'jdbc': 'apache-airflow-providers-jdbc',
            'sqlite': 'apache-airflow-providers-sqlite',
            'aws': 'apache-airflow-providers-amazon',
            'ssh': 'apache-airflow-providers-ssh',
            'sftp': 'apache-airflow-providers-sftp',
            'ftp': 'apache-airflow-providers-ftp',
            'http': 'apache-airflow-providers-http',
            'mongo': 'apache-airflow-providers-mongo',
            'redis': 'apache-airflow-providers-redis',
            'slack': 'apache-airflow-providers-slack',
            'spark': 'apache-airflow-providers-apache-spark',
            'snowflake': 'apache-airflow-providers-snowflake',
            'segment': 'apache-airflow-providers-segment',
            'jira': 'apache-airflow-providers-jira',
            'salesforce': 'apache-airflow-providers-salesforce'
        }
        
        # Check if connection type requires a provider package
        if conn_type in conn_type_to_package:
            result['provider_package'] = conn_type_to_package[conn_type]
            result['message'] = (
                f"Connection type '{conn_type}' requires '{conn_type_to_package[conn_type]}' "
                f"to be installed in Airflow 2.X."
            )
        else:
            # Connection type not found in known mapping
            if conn_type not in ['http', 'sqlite']:  # Basic types that don't need providers
                result['message'] = (
                    f"Connection type '{conn_type}' may require a provider package in Airflow 2.X. "
                    f"Check the Airflow documentation for required packages."
                )
        
        return result
    
    def test_connection(self, conn_id: str) -> Dict:
        """
        Tests a connection to verify it works in Airflow 2.X.
        
        This method attempts to establish a connection using the provided
        connection ID to verify it's configured correctly for Airflow 2.X.
        
        Args:
            conn_id: The connection ID to test
        
        Returns:
            Dict containing test results with success status
        """
        result = {
            'success': False,
            'conn_id': conn_id,
            'message': '',
            'details': {}
        }
        
        try:
            # Get the connection object
            connection = airflow.models.Connection.get_connection_from_secrets(conn_id)
            
            # Get the connection type
            conn_type = connection.conn_type
            result['details']['conn_type'] = conn_type
            
            # Validate the connection type
            type_validation = self.validate_connection_type(conn_type)
            result['details']['provider_package'] = type_validation.get('provider_package')
            
            # Attempt to get the appropriate hook for this connection type
            try:
                hook = None
                
                if conn_type == 'postgres':
                    try:
                        from airflow.providers.postgres.hooks.postgres import PostgresHook
                        hook = PostgresHook(postgres_conn_id=conn_id)
                    except ImportError:
                        result['message'] = "PostgresHook not available. Install apache-airflow-providers-postgres."
                        return result
                
                elif conn_type == 'mysql':
                    try:
                        from airflow.providers.mysql.hooks.mysql import MySqlHook
                        hook = MySqlHook(mysql_conn_id=conn_id)
                    except ImportError:
                        result['message'] = "MySqlHook not available. Install apache-airflow-providers-mysql."
                        return result
                
                elif conn_type == 'google_cloud_platform':
                    try:
                        from airflow.providers.google.cloud.hooks.gcs import GCSHook
                        hook = GCSHook(gcp_conn_id=conn_id)
                    except ImportError:
                        result['message'] = "GCSHook not available. Install apache-airflow-providers-google."
                        return result
                
                elif conn_type == 'http':
                    try:
                        from airflow.providers.http.hooks.http import HttpHook
                        hook = HttpHook(http_conn_id=conn_id)
                    except ImportError:
                        result['message'] = "HttpHook not available. Install apache-airflow-providers-http."
                        return result
                
                elif conn_type == 'ssh':
                    try:
                        from airflow.providers.ssh.hooks.ssh import SSHHook
                        hook = SSHHook(ssh_conn_id=conn_id)
                    except ImportError:
                        result['message'] = "SSHHook not available. Install apache-airflow-providers-ssh."
                        return result
                
                else:
                    # For other connection types, just validate the connection object
                    result['success'] = True
                    result['message'] = (
                        f"Connection '{conn_id}' of type '{conn_type}' exists, "
                        f"but testing is not implemented for this connection type."
                    )
                    return result
                
                # Test the connection if a hook was created
                if hook:
                    if hasattr(hook, 'test_connection'):
                        hook_test = hook.test_connection()
                        result['success'] = hook_test[0]
                        result['message'] = hook_test[1]
                    else:
                        # If no test_connection method, consider it a success if we got this far
                        result['success'] = True
                        result['message'] = f"Connection '{conn_id}' appears to be valid but couldn't be tested directly."
                
            except Exception as e:
                result['message'] = f"Error testing connection: {str(e)}"
                return result
            
        except AirflowException:
            result['message'] = f"Connection '{conn_id}' not found in Airflow connections."
            return result
        
        except Exception as e:
            result['message'] = f"Error validating connection '{conn_id}': {str(e)}"
            return result
        
        return result