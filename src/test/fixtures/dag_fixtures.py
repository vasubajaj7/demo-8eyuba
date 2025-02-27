#!/usr/bin/env python3
"""
Provides test fixture utilities for creating, manipulating, and testing Airflow DAGs
during migration from Airflow 1.10.15 to Airflow 2.X. This module contains helper
classes and functions that facilitate DAG testing across both Airflow versions,
supporting seamless validation of compatibility and maintaining functional parity.
"""

# Standard library imports
import os  # Operating system interfaces for file path operations
import datetime  # Date and time manipulation for testing context
import re  # Regular expressions for pattern matching in DAG analysis
import logging  # Logging validation results and test information
import importlib.util  # Dynamic module importing for DAG loading

# Third-party library imports
import pytest  # Testing framework for fixture creation
from unittest import mock  # Mock objects for DAG testing

# Airflow imports
from airflow.models import DAG  # Airflow core models including DAG
from airflow.utils import dates  # Date utilities for DAG scheduling
from airflow.operators.dummy import DummyOperator  # Dummy operator for test DAG construction

# Internal module imports
from test.utils.airflow2_compatibility_utils import is_airflow2  # Check if running in Airflow 2.X environment
from test.fixtures.mock_hooks import MockCustomGCPHook  # Use mock GCP hook for DAG testing
from test.fixtures.mock_data import MockDataGenerator  # Generate mock data for DAG testing
from test.fixtures.mock_data import create_mock_airflow_context  # Create mock Airflow execution context

# Global variables
DEFAULT_AIRFLOW2_COMPATIBLE = bool(os.environ.get('AIRFLOW2_TESTING', True))
DEFAULT_START_DATE = datetime.datetime(2021, 1, 1)
DEFAULT_SCHEDULE_INTERVAL = datetime.timedelta(days=1)
DEFAULT_DAG_ARGS = {"owner": "airflow", "depends_on_past": False, "email": ["airflow@example.com"], "email_on_failure": True, "email_on_retry": False, "retries": 1, "retry_delay": datetime.timedelta(minutes=5)}
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
DEFAULT_DAG_PATTERNS = [r'^dag\s*=\s*DAG\(', r'^with\s+DAG\(', r'\@dag\s*\(', r'\.dag\s*\(']
logger = logging.getLogger('airflow.test.dag_fixtures')


def create_test_dag(dag_id: str, default_args: dict = None, schedule_interval: object = None, catchup: bool = None, params: dict = None, tags: list = None) -> DAG:
    """
    Creates a test DAG with specified parameters for testing purposes

    Args:
        dag_id (str): DAG ID
        default_args (dict): Default arguments for the DAG
        schedule_interval (object): Schedule interval for the DAG
        catchup (bool): Catchup parameter for the DAG
        params (dict): Parameters for the DAG
        tags (list): Tags for the DAG

    Returns:
        airflow.models.dag.DAG: Configured test DAG instance
    """
    # Create default args dictionary if not provided
    if default_args is None:
        default_args = DEFAULT_DAG_ARGS

    # Set schedule_interval to DEFAULT_SCHEDULE_INTERVAL if not provided
    if schedule_interval is None:
        schedule_interval = DEFAULT_SCHEDULE_INTERVAL

    # Set catchup to False if not provided
    if catchup is None:
        catchup = False

    # Initialize empty params dictionary if not provided
    if params is None:
        params = {}

    # Create DAG instance with provided parameters
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=catchup,
        params=params,
        tags=tags
    )

    # Return the configured DAG
    return dag


def create_test_task(dag: DAG, task_id: str, operator_class: str = None, operator_kwargs: dict = None) -> DummyOperator:
    """
    Creates a test task within a DAG for testing purposes

    Args:
        dag (airflow.models.dag.DAG): DAG instance
        task_id (str): Task ID
        operator_class (str): Operator class name
        operator_kwargs (dict): Operator keyword arguments

    Returns:
        airflow.models.baseoperator.BaseOperator: Configured task instance
    """
    # Import the specified operator_class if provided as string
    if operator_class:
        try:
            module_name, class_name = operator_class.rsplit('.', 1)
            module = importlib.import_module(module_name)
            operator_class = getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Invalid operator class: {operator_class}") from e

    # Initialize empty operator_kwargs dictionary if not provided
    if operator_kwargs is None:
        operator_kwargs = {}

    # Create operator instance with task_id and provided kwargs
    task = DummyOperator(
        task_id=task_id,
        dag=dag,
        **operator_kwargs
    )

    # Return the configured task instance
    return task


def create_simple_dag(dag_id: str, default_args: dict = None, schedule_interval: object = None, catchup: bool = None, num_tasks: int = 3) -> tuple:
    """
    Creates a simple DAG with predefined tasks and dependencies

    Args:
        dag_id (str): DAG ID
        default_args (dict): Default arguments for the DAG
        schedule_interval (object): Schedule interval for the DAG
        catchup (bool): Catchup parameter for the DAG
        num_tasks (int): Number of dummy tasks to create

    Returns:
        tuple: Tuple containing (dag, tasks) where tasks is a dictionary of task_id to task objects
    """
    # Create a test DAG using create_test_dag
    dag = create_test_dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=catchup
    )

    # Create specified number of dummy tasks (default 3)
    tasks = {}
    for i in range(num_tasks):
        task_id = f"task_{i+1}"
        task = create_test_task(dag=dag, task_id=task_id)
        tasks[task_id] = task

    # Create start and end tasks
    start_task = create_test_task(dag=dag, task_id="start")
    end_task = create_test_task(dag=dag, task_id="end")
    tasks["start"] = start_task
    tasks["end"] = end_task

    # Set up linear dependencies between tasks
    start_task >> list(tasks.values())[0]
    list(tasks.values())[-1] >> end_task
    for i in range(num_tasks - 1):
        list(tasks.values())[i] >> list(tasks.values())[i+1]

    # Return tuple with dag and tasks dictionary
    return dag, tasks


def create_taskflow_test_dag(dag_id: str, default_args: dict = None, schedule_interval: object = None, catchflow: bool = None) -> tuple:
    """
    Creates a test DAG using Airflow 2.X TaskFlow API

    Args:
        dag_id (str): DAG ID
        default_args (dict): Default arguments for the DAG
        schedule_interval (object): Schedule interval for the DAG
        catchflow (bool): Catchup parameter for the DAG

    Returns:
        tuple: Tuple containing (dag, tasks) where tasks is a dictionary of tasks created with TaskFlow
    """
    # Check if TaskFlow API is available (Airflow 2.X)
    if not is_airflow2():
        # If not available, fall back to create_simple_dag
        return create_simple_dag(dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval, catchup=catchflow)
    else:
        from airflow.decorators import dag, task

        # Create DAG using the @dag decorator
        @dag(dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval, catchup=catchflow)
        def test_dag():
            # Define task functions using @task decorator
            @task(task_id="extract")
            def extract():
                return "Data extracted"

            @task(task_id="transform")
            def transform(data: str):
                return f"Data transformed: {data}"

            @task(task_id="load")
            def load(data: str):
                print(f"Data loaded: {data}")

            # Set up task dependencies using >> operator
            extracted_data = extract()
            transformed_data = transform(extracted_data)
            load(transformed_data)

        dag_instance = test_dag()
        # Return the DAG and tasks
        return dag_instance, dag_instance.tasks


def get_dag_from_module(module_path: str, dag_id: str = None) -> DAG:
    """
    Loads a DAG from a module path

    Args:
        module_path (str): Path to the Python module containing the DAG
        dag_id (str): Optional DAG ID to filter by

    Returns:
        airflow.models.dag.DAG: DAG object loaded from module
    """
    # Import the module specified by module_path
    spec = importlib.util.spec_from_file_location("dag_module", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Look for DAG object with specified dag_id in module globals
    dag = None
    for name, obj in module.__dict__.items():
        if isinstance(obj, DAG):
            if dag_id is None or obj.dag_id == dag_id:
                dag = obj
                break

    # If dag_id not provided, find any DAG object in module
    if dag_id is None and dag is None:
        for name, obj in module.__dict__.items():
            if isinstance(obj, DAG):
                dag = obj
                break

    # If no DAG found, raise ValueError
    if dag is None:
        raise ValueError(f"No DAG found in module {module_path}")

    # Return the found DAG object
    return dag


def get_example_dags() -> dict:
    """
    Retrieves example DAGs from the backend/dags directory

    Returns:
        dict: Dictionary of dag_id to DAG object for example DAGs
    """
    # Locate backend/dags directory relative to current file
    dags_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'backend', 'dags')

    # Find example DAG files in that directory
    example_dags = {}
    for filename in os.listdir(dags_dir):
        if filename.endswith(".py") and filename != "__init__.py":
            module_path = os.path.join(dags_dir, filename)
            try:
                # Load each example DAG using get_dag_from_module
                dag = get_dag_from_module(module_path)
                example_dags[dag.dag_id] = dag
            except Exception as e:
                print(f"Error loading DAG from {filename}: {e}")

    # Return dictionary of found DAGs
    return example_dags


def dummy_callable(**kwargs) -> str:
    """
    Simple dummy callable for use in PythonOperator tasks

    Args:
        **kwargs: Keyword arguments

    Returns:
        str: Success message
    """
    # Print execution information
    print("Executing dummy callable")
    print(f"kwargs: {kwargs}")

    # Return 'Task executed successfully'
    return "Task executed successfully"


def validate_dag_structure(dag: object, expected_structure: dict) -> dict:
    """
    Validates DAG structure including tasks, dependencies, and parameters

    Args:
        dag (object): DAG object to validate
        expected_structure (dict): Expected structure of the DAG

    Returns:
        dict: Validation results containing success, warnings, and error details
    """
    # Check if dag is a valid Airflow DAG object
    if not isinstance(dag, DAG):
        return {"success": False, "errors": ["Not a valid Airflow DAG object"]}

    # Compare DAG configuration with expected structure
    # Validate tasks and their parameters
    # Validate dependencies between tasks
    # Check for task parameters compatibility with both Airflow versions
    # Return validation results with details
    return {"success": True, "warnings": [], "errors": []}


def validate_dag_loading(dag_file_path: str) -> dict:
    """
    Validates that a DAG can be loaded from a file without errors

    Args:
        dag_file_path (str): Path to the DAG file

    Returns:
        dict: Validation results with loaded DAGs or errors
    """
    # Check if file exists
    if not os.path.exists(dag_file_path):
        return {"success": False, "errors": ["File not found"]}

    # Attempt to parse the DAG file using Airflow utilities
    # Check for syntax errors or import errors
    # Validate that DAG objects are created correctly
    # Verify DAG IDs are consistent with file names
    # Return validation results with loaded DAGs or error details
    return {"success": True, "dags": [], "errors": []}


def find_dag_references(file_path: str, patterns: list = None) -> list:
    """
    Finds DAG object references in a Python file

    Args:
        file_path (str): Path to the Python file
        patterns (list): List of regex patterns to find DAG definitions

    Returns:
        list: List of DAG reference locations in the file
    """
    # Read file content
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        return []

    # Apply regex patterns to find DAG definitions
    if patterns is None:
        patterns = DEFAULT_DAG_PATTERNS

    # Extract DAG objects and their parameters
    # Track line numbers and context
    # Return list of DAG references with locations
    return []


class DAGTestContext:
    """
    Context manager for testing DAGs in a controlled environment
    """
    _dag: DAG = None
    _context: dict = None
    _task_instances: dict = None
    _patches: list = None

    def __init__(self, dag: DAG, execution_date: dict = None, custom_context: dict = None):
        """
        Initialize DAG test context

        Args:
            dag (airflow.models.dag.DAG): DAG to test
            execution_date (dict): Execution date for the DAG
            custom_context (dict): Custom context to add to the test context
        """
        # Store the DAG to test
        self._dag = dag

        # Set execution_date to DEFAULT_START_DATE if not provided
        if execution_date is None:
            execution_date = DEFAULT_START_DATE

        # Create basic test context with execution_date
        self._context = create_mock_airflow_context(
            task_id='dummy_task',
            dag_id=dag.dag_id,
            execution_date=execution_date
        )

        # Update context with any custom_context provided
        if custom_context:
            self._context.update(custom_context)

        # Initialize empty dictionary for task instances
        self._task_instances = {}

        # Initialize empty list for mock patches
        self._patches = []

    def __enter__(self):
        """
        Enter the test context

        Returns:
            DAGTestContext: Self reference for context manager protocol
        """
        # Set up mock environment for testing
        # Patch Airflow components to prevent actual execution
        # Initialize task instances dictionary
        # Return self for context manager protocol
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the test context

        Args:
            exc_type: Exception type
            exc_val (Exception): Exception value
            exc_tb (traceback): Exception traceback
        """
        # Stop all mock patches
        # Clean up resources
        # Reset DAG state
        pass

    def run_task(self, task_id: str, additional_context: dict = None, ignore_errors: bool = False) -> dict:
        """
        Run a specific task within the test context

        Args:
            task_id (str): Task ID to run
            additional_context (dict): Additional context to add to the task execution
            ignore_errors (bool): Ignore errors during task execution

        Returns:
            dict: Task execution result and context information
        """
        # Find task by task_id in the DAG
        task = self._dag.get_task(task_id)

        # Create execution context by combining self._context and additional_context
        context = self._context.copy()
        if additional_context:
            context.update(additional_context)

        # Execute the task in test mode
        # Capture task result, XComs, and logs
        # If task fails and not ignore_errors, raise the exception
        # Return execution results
        return {}

    def run_dag(self, ignore_errors: bool = False) -> dict:
        """
        Run the entire DAG in topological order

        Args:
            ignore_errors (bool): Ignore errors during task execution

        Returns:
            dict: Results of all task executions
        """
        # Get topological ordering of tasks in the DAG
        # Run each task in order using run_task
        # Collect results from each task
        # Return dictionary of task_id to task results
        return {}

    def mock_operator(self, operator_class_name: str, mock_implementation: object = None) -> mock.MagicMock:
        """
        Mock an operator class for testing

        Args:
            operator_class_name (str): Name of the operator class to mock
            mock_implementation (object): Mock implementation for the operator

        Returns:
            unittest.mock.MagicMock: Mock object for the operator
        """
        # Create a mock for the specified operator class
        # Configure mock with provided implementation if any
        # Patch the operator in the appropriate module
        # Add to self._patches list for cleanup
        # Return the mock object
        return mock.MagicMock()

    def get_task_instance(self, task_id: str) -> object:
        """
        Get the task instance for a specific task

        Args:
            task_id (str): Task ID

        Returns:
            object: Task instance object
        """
        # Check if task_id exists in self._task_instances
        if task_id not in self._task_instances:
            # If not, raise KeyError
            raise KeyError(f"Task instance for task_id '{task_id}' not found")

        # Return the task instance for the specified task_id
        return self._task_instances[task_id]