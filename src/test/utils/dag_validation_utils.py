#!/usr/bin/env python3
"""
Utility module providing functions for validating Airflow DAG structure, integrity,
and compatibility with Airflow 2.X during the migration from Airflow 1.10.15.

This module supplies core test functions for verifying DAG validity, detecting
structural issues, cyclic dependencies, and ensuring compatibility with the
Cloud Composer 2 environment.
"""

import logging
import time
import datetime
from typing import Dict, List, Set, Any, Optional, Tuple, Union

# For graph analysis of DAG structure
import networkx as nx

# For testing framework integration
import pytest

# Airflow imports
import airflow
from airflow.models import DAG, DagBag, TaskInstance, DagRun
from airflow.utils.session import create_session
from airflow.utils.state import State

# Internal imports - for Airflow version compatibility checking
from .airflow2_compatibility_utils import (
    is_airflow2,
    AIRFLOW_1_TO_2_OPERATOR_MAPPING,
    AIRFLOW_1_DEPRECATED_PARAMS,
)

# Import validation utilities from backend
from ...backend.dags.utils.validation_utils import check_deprecated_features, VALIDATION_LEVELS

# Configure logging
logger = logging.getLogger('airflow.test.dag_validation')

# Constants
DAG_PARSE_TIME_THRESHOLD_SECONDS = 30
DEFAULT_EXECUTION_DATE = datetime.datetime(2023, 1, 1)


def validate_dag_integrity(dag: object) -> bool:
    """
    Validates the structural integrity of a DAG, checking for issues like cycles,
    invalid dependencies, and duplicated task IDs.
    
    Args:
        dag: The DAG to validate
        
    Returns:
        True if DAG is valid, False otherwise
    """
    if not isinstance(dag, airflow.models.DAG):
        logger.error(f"Invalid DAG object type: {type(dag)}")
        return False
    
    # Check for cyclic dependencies using networkx
    try:
        cycles = check_for_cycles(dag)
        if cycles:
            cycle_str = ', '.join([' -> '.join(cycle) for cycle in cycles])
            logger.error(f"DAG '{dag.dag_id}' contains cyclic dependencies: {cycle_str}")
            return False
    except Exception as e:
        logger.error(f"Error checking for cycles in DAG '{dag.dag_id}': {str(e)}")
        return False
    
    # Check for duplicate task IDs
    task_ids = [task.task_id for task in dag.tasks]
    if len(task_ids) != len(set(task_ids)):
        duplicates = [id for id in task_ids if task_ids.count(id) > 1]
        logger.error(f"DAG '{dag.dag_id}' contains duplicate task IDs: {set(duplicates)}")
        return False
    
    # Validate task relationships
    relationship_issues = validate_task_relationships(dag)
    if relationship_issues:
        for issue in relationship_issues:
            logger.error(f"Task relationship issue in DAG '{dag.dag_id}': {issue}")
        return False
    
    # Check for isolated tasks (no upstream or downstream connections)
    isolated_tasks = [
        task.task_id for task in dag.tasks
        if not task.upstream_list and not task.downstream_list
    ]
    
    if isolated_tasks:
        logger.warning(f"DAG '{dag.dag_id}' contains isolated tasks with no dependencies: {isolated_tasks}")
        # Not failing the validation for isolated tasks, just a warning
    
    logger.info(f"DAG '{dag.dag_id}' passed integrity validation")
    return True


def validate_task_relationships(dag: object) -> List:
    """
    Validates the task dependency relationships in a DAG.
    
    Args:
        dag: The DAG to validate
        
    Returns:
        List of dependency validation issues, empty if valid
    """
    issues = []
    
    if not isinstance(dag, airflow.models.DAG):
        issues.append(f"Invalid DAG object type: {type(dag)}")
        return issues
    
    # Build a directed graph of task dependencies
    try:
        task_dict = {task.task_id: task for task in dag.tasks}
        dependency_graph = nx.DiGraph()
        
        # Add all tasks as nodes
        for task_id in task_dict:
            dependency_graph.add_node(task_id)
        
        # Add edges for dependencies
        for task_id, task in task_dict.items():
            for upstream_task in task.upstream_list:
                upstream_id = upstream_task.task_id
                
                # Verify that upstream_id exists in the DAG
                if upstream_id not in task_dict:
                    issues.append(f"Task '{task_id}' has upstream dependency on non-existent task '{upstream_id}'")
                    continue
                
                dependency_graph.add_edge(upstream_id, task_id)
        
        # Check for cycles in the dependency graph
        try:
            cycles = list(nx.simple_cycles(dependency_graph))
            if cycles:
                cycle_str = ', '.join([' -> '.join(c) for c in cycles])
                issues.append(f"Cyclic dependencies detected: {cycle_str}")
        except nx.NetworkXNoCycle:
            # No cycles found, which is good
            pass
    
    except Exception as e:
        issues.append(f"Error validating task relationships: {str(e)}")
    
    return issues


def check_task_ids(dag: object) -> List:
    """
    Validates that all task IDs are unique and follow naming conventions.
    
    Args:
        dag: The DAG to validate
        
    Returns:
        List of task ID validation issues, empty if valid
    """
    issues = []
    
    if not isinstance(dag, airflow.models.DAG):
        issues.append(f"Invalid DAG object type: {type(dag)}")
        return issues
    
    task_ids = [task.task_id for task in dag.tasks]
    
    # Check for duplicate task IDs
    id_counts = {}
    for task_id in task_ids:
        id_counts[task_id] = id_counts.get(task_id, 0) + 1
    
    duplicates = [id for id, count in id_counts.items() if count > 1]
    if duplicates:
        issues.append(f"Duplicate task IDs detected: {duplicates}")
    
    # Check task ID naming conventions
    invalid_chars = [' ', '&', '?', '!', '.', ',', ':', ';']
    for task_id in task_ids:
        # Check for invalid characters
        for char in invalid_chars:
            if char in task_id:
                issues.append(f"Task ID '{task_id}' contains invalid character: '{char}'")
        
        # Check for other naming issues
        if task_id.startswith('_'):
            issues.append(f"Task ID '{task_id}' starts with underscore, which may cause issues")
        
        if len(task_id) > 250:
            issues.append(f"Task ID '{task_id}' exceeds 250 characters, which may cause storage issues")
    
    return issues


def measure_dag_parse_time(dag_file_path: str) -> float:
    """
    Measures the time taken to parse a DAG file for performance validation.
    
    Args:
        dag_file_path: Path to the DAG file to measure
        
    Returns:
        Time in seconds taken to parse the DAG
    """
    start_time = time.time()
    
    try:
        # Create a DagBag to parse the file
        dag_bag = airflow.models.DagBag(
            dag_folder=dag_file_path,
            include_examples=False
        )
        
        # Check for import errors
        if dag_bag.import_errors:
            logger.warning(f"Import errors while parsing DAG file '{dag_file_path}': {dag_bag.import_errors}")
    
    except Exception as e:
        logger.error(f"Error measuring parse time for '{dag_file_path}': {str(e)}")
    
    end_time = time.time()
    parse_time = end_time - start_time
    
    logger.info(f"DAG file '{dag_file_path}' parsed in {parse_time:.2f} seconds")
    return parse_time


def validate_dag_loading(dag_file_path: str) -> Tuple[bool, List]:
    """
    Validates that a DAG file loads successfully without errors.
    
    Args:
        dag_file_path: Path to the DAG file to validate
        
    Returns:
        (success_flag, import_errors) tuple
    """
    success = True
    import_errors = []
    
    try:
        # Create a DagBag to load the file
        dag_bag = airflow.models.DagBag(
            dag_folder=dag_file_path,
            include_examples=False
        )
        
        # Check for import errors
        if dag_bag.import_errors:
            success = False
            for filename, error in dag_bag.import_errors.items():
                error_msg = f"Error importing '{filename}': {error}"
                import_errors.append(error_msg)
                logger.error(error_msg)
        
        # Check if any DAGs were loaded
        if not dag_bag.dags:
            logger.warning(f"No DAGs found in file '{dag_file_path}'")
            # Not considering this an error, but worth noting
    
    except Exception as e:
        success = False
        error_msg = f"Error loading DAG file '{dag_file_path}': {str(e)}"
        import_errors.append(error_msg)
        logger.error(error_msg)
    
    return success, import_errors


def validate_airflow2_compatibility(dag: object) -> Dict:
    """
    Validates that a DAG is compatible with Airflow 2.X.
    
    Args:
        dag: The DAG to validate
        
    Returns:
        Dictionary of compatibility issues categorized by severity
    """
    compatibility_issues = {
        "errors": [],
        "warnings": [],
        "info": []
    }
    
    if not isinstance(dag, airflow.models.DAG):
        compatibility_issues["errors"].append(f"Invalid DAG object type: {type(dag)}")
        return compatibility_issues
    
    # Check DAG parameters
    param_issues = validate_dag_parameters(dag)
    if param_issues:
        for issue in param_issues:
            compatibility_issues["warnings"].append(f"Parameter issue: {issue}")
    
    # Check for deprecated features using the imported function
    deprecated_features = check_deprecated_features(dag)
    for feature in deprecated_features:
        level = feature.get('level', 'WARNING').lower()
        # Convert level to our categories (errors, warnings, info)
        if level == 'error':
            category = "errors"
        elif level == 'warning':
            category = "warnings"
        else:
            category = "info"
        
        compatibility_issues[category].append(
            f"Deprecated feature '{feature['feature']}': {feature['recommendation']}"
        )
    
    # Check task compatibility
    task_issues = validate_tasks_compatibility(dag)
    for task_id, issues in task_issues.items():
        for level, messages in issues.items():
            for message in messages:
                compatibility_issues[level].append(f"Task '{task_id}': {message}")
    
    return compatibility_issues


def validate_dag_parameters(dag: object) -> List:
    """
    Validates DAG parameters for Airflow 2.X compatibility.
    
    Args:
        dag: The DAG to validate
        
    Returns:
        List of parameter validation issues
    """
    issues = []
    
    if not isinstance(dag, airflow.models.DAG):
        issues.append(f"Invalid DAG object type: {type(dag)}")
        return issues
    
    # Check schedule_interval parameter
    if hasattr(dag, 'schedule_interval'):
        schedule_interval = dag.schedule_interval
        
        # None is valid
        if schedule_interval is not None:
            # Check if it's a cron expression
            if isinstance(schedule_interval, str):
                # Check preset schedules
                presets = ['@once', '@hourly', '@daily', '@weekly', '@monthly', '@yearly']
                if schedule_interval not in presets and not schedule_interval.startswith('@'):
                    # Attempt to validate cron expression
                    try:
                        import croniter
                        if not croniter.croniter.is_valid(schedule_interval):
                            issues.append(f"Invalid cron expression in schedule_interval: '{schedule_interval}'")
                    except ImportError:
                        # Can't validate without croniter package
                        logger.warning("croniter package not available, skipping cron validation")
            
            # Check if it's not a timedelta
            elif not hasattr(schedule_interval, 'total_seconds'):
                issues.append(f"Unsupported schedule_interval type: {type(schedule_interval)}")
    
    # Check default_args
    if hasattr(dag, 'default_args') and dag.default_args:
        default_args = dag.default_args
        
        # Check for deprecated parameters
        for param in AIRFLOW_1_DEPRECATED_PARAMS:
            if param in default_args:
                issues.append(f"Deprecated parameter '{param}' in default_args")
        
        # Special case for provide_context
        if default_args.get('provide_context', False):
            issues.append("provide_context=True is deprecated in Airflow 2.X (context is automatically provided)")
        
        # Check for callback functions that might not accept context parameter
        callback_fields = ['on_success_callback', 'on_failure_callback', 'on_retry_callback']
        for field in callback_fields:
            if field in default_args and default_args[field] is not None:
                # We can't easily check the signature of the callback function here,
                # so we'll just add an informational message
                issues.append(f"Ensure {field} accepts a 'context' parameter in Airflow 2.X")
    
    # Check for deprecated top-level parameters
    deprecated_params = {
        'concurrency': 'Use max_active_tasks instead',
        'max_active_runs_per_dag': 'Verify this parameter exists in Airflow 2.X',
        'non_pooled_task_slot_count': 'Parameter may be removed in Airflow 2.X'
    }
    
    for param, message in deprecated_params.items():
        if hasattr(dag, param):
            issues.append(f"Deprecated DAG parameter '{param}': {message}")
    
    return issues


def check_parsing_performance(dag_file_path: str, threshold_seconds: float = DAG_PARSE_TIME_THRESHOLD_SECONDS) -> Tuple[bool, float]:
    """
    Checks if DAG parse time meets performance requirements.
    
    Args:
        dag_file_path: Path to the DAG file to check
        threshold_seconds: Maximum acceptable parse time in seconds
        
    Returns:
        (pass_flag, parse_time) tuple
    """
    parse_time = measure_dag_parse_time(dag_file_path)
    
    if parse_time > threshold_seconds:
        logger.warning(
            f"DAG file '{dag_file_path}' parse time ({parse_time:.2f}s) exceeds "
            f"threshold of {threshold_seconds}s"
        )
        return False, parse_time
    
    logger.info(
        f"DAG file '{dag_file_path}' parse time ({parse_time:.2f}s) is within "
        f"threshold of {threshold_seconds}s"
    )
    return True, parse_time


def validate_tasks_compatibility(dag: object) -> Dict:
    """
    Validates tasks in a DAG for Airflow 2.X compatibility.
    
    Args:
        dag: The DAG to validate
        
    Returns:
        Dictionary of compatibility issues by task
    """
    task_issues = {}
    
    if not isinstance(dag, airflow.models.DAG):
        return {"DAG": {"errors": [f"Invalid DAG object type: {type(dag)}"]}}
    
    for task in dag.tasks:
        task_id = task.task_id
        task_issues[task_id] = {
            "errors": [],
            "warnings": [],
            "info": []
        }
        
        # Check operator type compatibility
        operator_class = task.__class__.__name__
        operator_module = task.__class__.__module__
        
        # Check if operator is using old import paths
        if operator_class in AIRFLOW_1_TO_2_OPERATOR_MAPPING:
            airflow2_path = AIRFLOW_1_TO_2_OPERATOR_MAPPING[operator_class]
            
            if '.contrib.' in operator_module or operator_module.endswith('_operator'):
                task_issues[task_id]["warnings"].append(
                    f"Operator '{operator_class}' may be using deprecated import path: "
                    f"{operator_module}. Consider using {airflow2_path}."
                )
        
        # Check deprecated parameters
        for param in AIRFLOW_1_DEPRECATED_PARAMS:
            if hasattr(task, param) and getattr(task, param) is not None:
                task_issues[task_id]["warnings"].append(
                    f"Deprecated parameter '{param}' used in task"
                )
        
        # Special cases for common operators
        if operator_class == "PythonOperator":
            # Check for provide_context
            if hasattr(task, 'provide_context') and task.provide_context:
                task_issues[task_id]["warnings"].append(
                    "provide_context=True is deprecated in Airflow 2.X "
                    "(context is automatically provided)"
                )
            
            # Check for op_kwargs use versus TaskFlow API
            task_issues[task_id]["info"].append(
                "Consider using TaskFlow API for this PythonOperator in Airflow 2.X"
            )
        
        elif operator_class == "BashOperator":
            # No major changes for BashOperator, but worth noting
            pass
        
        # Check XCom usage
        if hasattr(task, 'do_xcom_push') and not task.do_xcom_push:
            task_issues[task_id]["info"].append(
                "Task has do_xcom_push=False. XCom behavior is different in Airflow 2.X."
            )
        
        # Remove empty categories
        for level in list(task_issues[task_id].keys()):
            if not task_issues[task_id][level]:
                del task_issues[task_id][level]
        
        # Remove task entry if no issues found
        if not task_issues[task_id]:
            del task_issues[task_id]
    
    return task_issues


def validate_dag_scheduling(dag: object) -> List:
    """
    Validates DAG scheduling parameters for Airflow 2.X compatibility.
    
    Args:
        dag: The DAG to validate
        
    Returns:
        List of scheduling parameter validation issues
    """
    issues = []
    
    if not isinstance(dag, airflow.models.DAG):
        issues.append(f"Invalid DAG object type: {type(dag)}")
        return issues
    
    # Check start_date
    if not hasattr(dag, 'start_date') or dag.start_date is None:
        issues.append("DAG has no start_date, which is required in Airflow")
    
    # Check schedule_interval
    if hasattr(dag, 'schedule_interval'):
        schedule_interval = dag.schedule_interval
        
        # None is valid, but we should note it
        if schedule_interval is None:
            # This is just informational
            pass
        else:
            # Check if it's a cron expression
            if isinstance(schedule_interval, str):
                # Check preset schedules
                presets = ['@once', '@hourly', '@daily', '@weekly', '@monthly', '@yearly']
                if schedule_interval not in presets and not schedule_interval.startswith('@'):
                    # Attempt to validate cron expression
                    try:
                        import croniter
                        if not croniter.croniter.is_valid(schedule_interval):
                            issues.append(f"Invalid cron expression in schedule_interval: '{schedule_interval}'")
                    except ImportError:
                        # Can't validate without croniter package
                        logger.warning("croniter package not available, skipping cron validation")
            
            # Check if it's not a timedelta
            elif not hasattr(schedule_interval, 'total_seconds'):
                issues.append(f"Unsupported schedule_interval type: {type(schedule_interval)}")
    
    # Check catchup parameter
    if not hasattr(dag, 'catchup'):
        issues.append(
            "catchup parameter not specified. Default is different in Airflow 2.X "
            "(changed from True to False)"
        )
    
    # Check timezone settings
    if hasattr(dag, 'timezone') and dag.timezone:
        # Timezone usage was updated in Airflow 2.X
        issues.append(
            "Ensure timezone usage is compatible with Airflow 2.X. "
            "Consider using pendulum timezones."
        )
    
    return issues


def check_for_cycles(dag: object) -> List:
    """
    Checks for cyclic dependencies in a DAG.
    
    Args:
        dag: The DAG to check for cycles
        
    Returns:
        List of cycles found in the DAG, empty if none
    """
    cycles = []
    
    if not isinstance(dag, airflow.models.DAG):
        logger.error(f"Invalid DAG object type: {type(dag)}")
        return [["INVALID DAG OBJECT"]]
    
    try:
        # Build a directed graph from the DAG
        task_dict = {task.task_id: task for task in dag.tasks}
        dag_graph = nx.DiGraph()
        
        # Add all tasks as nodes
        for task_id in task_dict:
            dag_graph.add_node(task_id)
        
        # Add edges for task dependencies
        for task_id, task in task_dict.items():
            for downstream_task in task.downstream_list:
                downstream_id = downstream_task.task_id
                dag_graph.add_edge(task_id, downstream_id)
        
        # Find cycles in the graph
        try:
            cycles = list(nx.simple_cycles(dag_graph))
        except nx.NetworkXNoCycle:
            # No cycles found, which is good
            cycles = []
    
    except Exception as e:
        logger.error(f"Error checking for cycles in DAG '{dag.dag_id}': {str(e)}")
        cycles = [["ERROR CHECKING CYCLES"]]
    
    return cycles


def verify_dag_execution(dag: object, execution_date: datetime.datetime = None) -> Dict:
    """
    Verifies that a DAG executes successfully through a test run.
    
    Args:
        dag: The DAG to verify
        execution_date: The execution date to use (defaults to DEFAULT_EXECUTION_DATE)
        
    Returns:
        Dictionary with execution results and task statuses
    """
    if execution_date is None:
        execution_date = DEFAULT_EXECUTION_DATE
    
    results = {
        "success": False,
        "dag_id": getattr(dag, "dag_id", "unknown"),
        "execution_date": execution_date,
        "tasks": {},
        "error": None
    }
    
    if not isinstance(dag, airflow.models.DAG):
        results["error"] = f"Invalid DAG object type: {type(dag)}"
        return results
    
    try:
        # Create a test run_id
        run_id = f"test_{execution_date.isoformat()}"
        
        # Create a DAG run
        dag_run = dag.create_dagrun(
            state=airflow.utils.state.State.RUNNING,
            execution_date=execution_date,
            run_id=run_id,
            start_date=airflow.utils.timezone.utcnow(),
            external_trigger=True,
            conf={},
            run_type=airflow.utils.types.DagRunType.MANUAL
        )
        
        # Execute the DAG
        dag.run(start_date=execution_date, end_date=execution_date)
        
        # Check execution results for each task
        all_successful = True
        
        for task in dag.tasks:
            task_id = task.task_id
            try:
                # Get the task instance
                task_instance = dag_run.get_task_instance(task_id)
                
                if task_instance is None:
                    # Create and initialize a new task instance
                    task_instance = TaskInstance(task, execution_date)
                    task_instance.run(ignore_all_deps=True, ignore_ti_state=True)
                
                # Refresh the task instance state
                task_instance.refresh_from_db()
                
                # Record task state and details
                results["tasks"][task_id] = {
                    "state": task_instance.state,
                    "start_date": task_instance.start_date,
                    "end_date": task_instance.end_date,
                    "duration": task_instance.duration,
                    "success": task_instance.state == State.SUCCESS
                }
                
                # Check if task was successful
                if task_instance.state != State.SUCCESS:
                    all_successful = False
            
            except Exception as e:
                results["tasks"][task_id] = {
                    "state": "ERROR",
                    "error": str(e),
                    "success": False
                }
                all_successful = False
        
        results["success"] = all_successful
    
    except Exception as e:
        results["error"] = str(e)
        logger.error(f"Error verifying DAG execution: {str(e)}")
    
    return results


class DAGValidator:
    """
    Class for comprehensive validation of Airflow DAGs.
    
    This class provides methods for validating DAGs for structural integrity
    and Airflow 2.X compatibility.
    """
    
    def __init__(self):
        """Initialize the DAG validator."""
        self.validation_results = {}
        self.errors = []
        self.warnings = []
        self.info = []
    
    def validate(self, dag: object) -> Dict:
        """
        Performs comprehensive validation on a DAG.
        
        Args:
            dag: The DAG to validate
            
        Returns:
            Dictionary of validation results
        """
        if not isinstance(dag, airflow.models.DAG):
            self.add_error(f"Invalid DAG object type: {type(dag)}", "dag_type")
            return self.validation_results
        
        # Initialize results for this DAG
        dag_id = dag.dag_id
        self.validation_results = {
            "dag_id": dag_id,
            "valid": True,
            "integrity": {
                "valid": False,
                "issues": []
            },
            "airflow2_compatibility": {
                "compatible": False,
                "issues": {
                    "errors": [],
                    "warnings": [],
                    "info": []
                }
            },
            "tasks": {},
            "cycles": []
        }
        
        # Validate DAG integrity
        integrity_valid = validate_dag_integrity(dag)
        self.validation_results["integrity"]["valid"] = integrity_valid
        
        if not integrity_valid:
            self.validation_results["valid"] = False
            self.add_error("DAG integrity check failed", "integrity")
        
        # Check for cycles
        cycles = check_for_cycles(dag)
        if cycles:
            self.validation_results["cycles"] = cycles
            self.validation_results["valid"] = False
            cycle_str = ', '.join([' -> '.join(cycle) for cycle in cycles])
            self.add_error(f"DAG contains cyclic dependencies: {cycle_str}", "cycles")
        
        # Validate task relationships
        relationship_issues = validate_task_relationships(dag)
        if relationship_issues:
            self.validation_results["integrity"]["issues"].extend(relationship_issues)
            self.validation_results["valid"] = False
            for issue in relationship_issues:
                self.add_error(issue, "task_relationships")
        
        # Validate DAG parameters
        param_issues = validate_dag_parameters(dag)
        if param_issues:
            self.validation_results["airflow2_compatibility"]["issues"]["warnings"].extend(param_issues)
            for issue in param_issues:
                self.add_warning(issue, "dag_parameters")
        
        # Validate Airflow 2.X compatibility
        compatibility_issues = validate_airflow2_compatibility(dag)
        self.validation_results["airflow2_compatibility"]["issues"] = compatibility_issues
        self.validation_results["airflow2_compatibility"]["compatible"] = not compatibility_issues["errors"]
        
        if compatibility_issues["errors"]:
            self.validation_results["valid"] = False
            for issue in compatibility_issues["errors"]:
                self.add_error(issue, "airflow2_compatibility")
        
        for issue in compatibility_issues.get("warnings", []):
            self.add_warning(issue, "airflow2_compatibility")
        
        for issue in compatibility_issues.get("info", []):
            self.add_info(issue, "airflow2_compatibility")
        
        # Validate tasks
        task_issues = validate_tasks_compatibility(dag)
        self.validation_results["tasks"] = task_issues
        
        for task_id, issues in task_issues.items():
            for issue in issues.get("errors", []):
                self.validation_results["valid"] = False
                self.add_error(f"Task '{task_id}': {issue}", f"task_{task_id}")
            
            for issue in issues.get("warnings", []):
                self.add_warning(f"Task '{task_id}': {issue}", f"task_{task_id}")
            
            for issue in issues.get("info", []):
                self.add_info(f"Task '{task_id}': {issue}", f"task_{task_id}")
        
        return self.validation_results
    
    def validate_file(self, file_path: str) -> Dict:
        """
        Validates a DAG file.
        
        Args:
            file_path: Path to the DAG file to validate
            
        Returns:
            Dictionary of validation results for the file
        """
        file_results = {
            "file_path": file_path,
            "valid": False,
            "parsing": {
                "success": False,
                "import_errors": []
            },
            "performance": {
                "parse_time": None,
                "meets_threshold": False
            },
            "dags": []
        }
        
        # Validate DAG loading
        success, import_errors = validate_dag_loading(file_path)
        file_results["parsing"]["success"] = success
        file_results["parsing"]["import_errors"] = import_errors
        
        if not success:
            return file_results
        
        # Check parsing performance
        meets_threshold, parse_time = check_parsing_performance(file_path)
        file_results["performance"]["parse_time"] = parse_time
        file_results["performance"]["meets_threshold"] = meets_threshold
        
        # Load DAGs from the file
        dag_bag = airflow.models.DagBag(
            dag_folder=file_path,
            include_examples=False
        )
        
        # Validate each DAG in the file
        all_valid = True
        for dag_id, dag in dag_bag.dags.items():
            dag_results = self.validate(dag)
            file_results["dags"].append(dag_results)
            
            if not dag_results["valid"]:
                all_valid = False
        
        file_results["valid"] = all_valid and meets_threshold
        
        return file_results
    
    def add_error(self, message: str, component: str) -> None:
        """
        Adds an error to the validation results.
        
        Args:
            message: The error message
            component: The component that has the error
        """
        self.errors.append({"component": component, "message": message})
        logger.error(f"{component}: {message}")
    
    def add_warning(self, message: str, component: str) -> None:
        """
        Adds a warning to the validation results.
        
        Args:
            message: The warning message
            component: The component that has the warning
        """
        self.warnings.append({"component": component, "message": message})
        logger.warning(f"{component}: {message}")
    
    def add_info(self, message: str, component: str) -> None:
        """
        Adds an informational message to the validation results.
        
        Args:
            message: The informational message
            component: The component that the message is about
        """
        self.info.append({"component": component, "message": message})
        logger.info(f"{component}: {message}")
    
    def get_validation_report(self, format: str = 'json') -> str:
        """
        Generates a formatted validation report.
        
        Args:
            format: Format for the report ('json', 'text', or 'html')
            
        Returns:
            Formatted validation report
        """
        import json
        
        if format == 'json':
            report = {
                "validation_results": self.validation_results,
                "summary": {
                    "errors": len(self.errors),
                    "warnings": len(self.warnings),
                    "info": len(self.info)
                },
                "errors": self.errors,
                "warnings": self.warnings,
                "info": self.info
            }
            return json.dumps(report, indent=2)
        
        elif format == 'html':
            # Basic HTML report
            html = "<html><head><title>DAG Validation Report</title></head><body>"
            html += "<h1>DAG Validation Report</h1>"
            
            html += "<h2>Summary</h2>"
            html += f"<p>Errors: {len(self.errors)}</p>"
            html += f"<p>Warnings: {len(self.warnings)}</p>"
            html += f"<p>Info: {len(self.info)}</p>"
            
            if self.errors:
                html += "<h2>Errors</h2><ul>"
                for error in self.errors:
                    html += f"<li><strong>{error['component']}:</strong> {error['message']}</li>"
                html += "</ul>"
            
            if self.warnings:
                html += "<h2>Warnings</h2><ul>"
                for warning in self.warnings:
                    html += f"<li><strong>{warning['component']}:</strong> {warning['message']}</li>"
                html += "</ul>"
            
            if self.info:
                html += "<h2>Info</h2><ul>"
                for info in self.info:
                    html += f"<li><strong>{info['component']}:</strong> {info['message']}</li>"
                html += "</ul>"
            
            html += "</body></html>"
            return html
        
        else:  # Text format
            report = "DAG Validation Report\n"
            report += "=====================\n\n"
            
            report += "Summary:\n"
            report += f"- Errors: {len(self.errors)}\n"
            report += f"- Warnings: {len(self.warnings)}\n"
            report += f"- Info: {len(self.info)}\n\n"
            
            if self.errors:
                report += "Errors:\n"
                for error in self.errors:
                    report += f"- {error['component']}: {error['message']}\n"
                report += "\n"
            
            if self.warnings:
                report += "Warnings:\n"
                for warning in self.warnings:
                    report += f"- {warning['component']}: {warning['message']}\n"
                report += "\n"
            
            if self.info:
                report += "Info:\n"
                for info in self.info:
                    report += f"- {info['component']}: {info['message']}\n"
            
            return report


class TaskValidator:
    """
    Class for validating Airflow tasks and operators.
    
    This class provides methods for validating tasks and operators for
    Airflow 2.X compatibility.
    """
    
    def __init__(self):
        """Initialize the task validator."""
        self.validation_results = {}
    
    def validate_task(self, task: object) -> Dict:
        """
        Validates a task for Airflow 2.X compatibility.
        
        Args:
            task: The task to validate
            
        Returns:
            Dictionary of validation results for the task
        """
        if not isinstance(task, airflow.models.BaseOperator):
            return {
                "valid": False,
                "errors": [f"Invalid task object type: {type(task)}"],
                "task_id": getattr(task, "task_id", "unknown")
            }
        
        task_id = task.task_id
        operator_class = task.__class__.__name__
        operator_module = task.__class__.__module__
        
        results = {
            "valid": True,
            "task_id": task_id,
            "operator_type": operator_class,
            "operator_module": operator_module,
            "errors": [],
            "warnings": [],
            "info": []
        }
        
        # Check operator import path compatibility
        import_issues = self.validate_operator_compatibility(task)
        if import_issues.get("errors"):
            results["valid"] = False
            results["errors"].extend(import_issues["errors"])
        
        if import_issues.get("warnings"):
            results["warnings"].extend(import_issues["warnings"])
        
        if import_issues.get("info"):
            results["info"].extend(import_issues["info"])
        
        # Check for deprecated parameters
        for param in AIRFLOW_1_DEPRECATED_PARAMS:
            if hasattr(task, param) and getattr(task, param) is not None:
                results["warnings"].append(f"Deprecated parameter '{param}' used in task")
        
        # Special operator-specific checks
        if operator_class == "PythonOperator":
            # Check provide_context
            if hasattr(task, 'provide_context') and task.provide_context:
                results["warnings"].append(
                    "provide_context=True is deprecated in Airflow 2.X "
                    "(context is automatically provided)"
                )
            
            # Suggest TaskFlow API
            results["info"].append(
                "Consider using TaskFlow API for this PythonOperator in Airflow 2.X"
            )
        
        # Check callback functions
        callback_fields = ['on_success_callback', 'on_failure_callback', 'on_retry_callback']
        for field in callback_fields:
            if hasattr(task, field) and getattr(task, field) is not None:
                results["info"].append(
                    f"Ensure {field} function accepts a 'context' parameter in Airflow 2.X"
                )
        
        # Check XCom behavior
        if hasattr(task, 'do_xcom_push'):
            results["info"].append(
                "XCom behavior may be different in Airflow 2.X. "
                "Verify XCom push/pull operations."
            )
        
        return results
    
    def validate_operator_compatibility(self, operator: object) -> Dict:
        """
        Validates operator compatibility with Airflow 2.X.
        
        Args:
            operator: The operator to validate
            
        Returns:
            Dictionary of compatibility issues
        """
        issues = {
            "errors": [],
            "warnings": [],
            "info": []
        }
        
        if not isinstance(operator, airflow.models.BaseOperator):
            issues["errors"].append(f"Invalid operator object type: {type(operator)}")
            return issues
        
        operator_class = operator.__class__.__name__
        operator_module = operator.__class__.__module__
        
        # Check if operator is in the renamed operators dictionary
        if operator_class in AIRFLOW_1_TO_2_OPERATOR_MAPPING:
            airflow2_path = AIRFLOW_1_TO_2_OPERATOR_MAPPING[operator_class]
            
            # Check if using old import path
            if '.contrib.' in operator_module or operator_module.endswith('_operator'):
                issues["warnings"].append(
                    f"Operator '{operator_class}' is using deprecated import path: "
                    f"{operator_module}. Consider using {airflow2_path}."
                )
        
        # Check for deprecated parameters
        for param in AIRFLOW_1_DEPRECATED_PARAMS:
            if hasattr(operator, param) and getattr(operator, param) is not None:
                issues["warnings"].append(
                    f"Deprecated parameter '{param}' used in operator"
                )
        
        # Add operator-specific checks
        if operator_class == "BashOperator":
            # No major changes for BashOperator in Airflow 2.X
            pass
        
        elif operator_class == "PythonOperator":
            # Check for provide_context
            if hasattr(operator, 'provide_context') and operator.provide_context:
                issues["warnings"].append(
                    "provide_context=True is deprecated in Airflow 2.X "
                    "(context is automatically provided)"
                )
        
        # Check if operator is from a contrib package
        if '.contrib.' in operator_module:
            issues["warnings"].append(
                f"Operator is from a contrib package ({operator_module}) "
                "which has been moved to provider packages in Airflow 2.X"
            )
        
        return issues


class DAGTestCase:
    """
    Base test case class for DAG validation in pytest tests.
    
    This class provides methods for writing pytest tests to validate DAGs.
    """
    
    def __init__(self):
        """Initialize the DAG test case."""
        self.validator = DAGValidator()
    
    def assert_dag_integrity(self, dag: object) -> None:
        """
        Asserts that a DAG has structural integrity.
        
        Args:
            dag: The DAG to validate
            
        Raises:
            AssertionError: If the DAG fails integrity validation
        """
        is_valid = validate_dag_integrity(dag)
        assert is_valid, f"DAG '{dag.dag_id}' failed integrity check"
    
    def assert_dag_airflow2_compatible(self, dag: object) -> None:
        """
        Asserts that a DAG is compatible with Airflow 2.X.
        
        Args:
            dag: The DAG to validate
            
        Raises:
            AssertionError: If the DAG is not compatible with Airflow 2.X
        """
        compatibility_issues = validate_airflow2_compatibility(dag)
        
        assert not compatibility_issues["errors"], (
            f"DAG '{dag.dag_id}' is not compatible with Airflow 2.X: "
            f"{', '.join(compatibility_issues['errors'])}"
        )
    
    def assert_dag_load_performance(self, dag_file_path: str, threshold_seconds: float = DAG_PARSE_TIME_THRESHOLD_SECONDS) -> None:
        """
        Asserts that a DAG's parse time meets performance requirements.
        
        Args:
            dag_file_path: Path to the DAG file to check
            threshold_seconds: Maximum acceptable parse time in seconds
            
        Raises:
            AssertionError: If the DAG parse time exceeds the threshold
        """
        meets_threshold, parse_time = check_parsing_performance(dag_file_path, threshold_seconds)
        
        assert meets_threshold, (
            f"DAG file '{dag_file_path}' parse time ({parse_time:.2f}s) exceeds "
            f"threshold of {threshold_seconds}s"
        )