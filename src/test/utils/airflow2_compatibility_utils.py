#!/usr/bin/env python3
"""
Utility module providing functions and classes for handling Airflow version compatibility
during the migration from Apache Airflow 1.10.15 to 2.X. Facilitates testing,
code transformation, and compatibility checks across Airflow versions.

This module helps testing teams verify that DAGs work correctly in both Airflow versions
and helps identify potential compatibility issues during migration.
"""

import os
import sys
import re
import inspect
import importlib
import contextlib
import ast
import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
from unittest.mock import MagicMock, patch

# Internal imports
from ...backend.migrations.migration_airflow1_to_airflow2 import DAGMigrator

# Configure logging
logger = logging.getLogger('airflow.test.compatibility')

# Get current Airflow version (defaults to 1.10.15 if not found)
AIRFLOW_VERSION = getattr(importlib.import_module('airflow'), '__version__', '1.10.15')

# Regular expression for Airflow 1.X style imports
AIRFLOW_1_IMPORT_PATTERNS = re.compile(r'^from airflow\.(operators|contrib|hooks|sensors)\.')

# Mapping of Airflow 1.X import paths to Airflow 2.X import paths
AIRFLOW_1_TO_2_IMPORT_MAPPING = {
    "operators.bash_operator": "operators.bash",
    "operators.python_operator": "operators.python", 
    "contrib.operators.gcs_to_gcs": "providers.google.cloud.transfers.gcs_to_gcs",
    "hooks.http_hook": "providers.http.hooks.http"
}

# Mapping of Airflow 1.X operator class names to fully qualified Airflow 2.X class names
AIRFLOW_1_TO_2_OPERATOR_MAPPING = {
    "BashOperator": "airflow.operators.bash.BashOperator",
    "PythonOperator": "airflow.operators.python.PythonOperator",
    "GoogleCloudStorageToGoogleCloudStorageOperator": "airflow.providers.google.cloud.transfers.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator"
}

# Parameters that are deprecated in Airflow 2.X
AIRFLOW_1_DEPRECATED_PARAMS = [
    "provide_context", 
    "queue", 
    "pool", 
    "executor_config", 
    "retry_delay", 
    "retry_exponential_backoff", 
    "max_retry_delay"
]


def is_airflow2() -> bool:
    """
    Determines if the current runtime is Airflow 2.X
    
    Returns:
        bool: True if running Airflow 2.X, False otherwise
    """
    try:
        major_version = int(AIRFLOW_VERSION.split('.')[0])
        return major_version >= 2
    except (ValueError, IndexError):
        # If version parsing fails, assume it's not Airflow 2.X
        return False


def is_taskflow_available() -> bool:
    """
    Checks if TaskFlow API is available in the current Airflow installation
    
    Returns:
        bool: True if TaskFlow API is available, False otherwise
    """
    if not is_airflow2():
        return False
    
    try:
        # Try to import TaskFlow decorator
        importlib.import_module('airflow.decorators')
        return True
    except ImportError:
        return False


def get_airflow_version_info() -> Dict[str, Any]:
    """
    Returns detailed version information about the current Airflow installation
    
    Returns:
        dict: Dictionary containing Airflow version details
    """
    version_parts = AIRFLOW_VERSION.split('.')
    
    try:
        major = int(version_parts[0])
        minor = int(version_parts[1]) if len(version_parts) > 1 else 0
        patch = int(version_parts[2]) if len(version_parts) > 2 else 0
    except (ValueError, IndexError):
        major, minor, patch = 1, 10, 15  # Default to 1.10.15 if parsing fails
    
    version_info = {
        'version': AIRFLOW_VERSION,
        'major': major,
        'minor': minor,
        'patch': patch,
        'is_airflow2': major >= 2,
        'has_taskflow': is_taskflow_available(),
        'features': {
            'providers': major >= 2,
            'smart_sensors': major >= 2,
            'scheduler_ha': major >= 2,
            'dag_serialization': (major == 1 and minor >= 10 and patch >= 10) or major >= 2
        }
    }
    
    return version_info


def transform_import(import_line: str) -> str:
    """
    Transforms a single Airflow 1.X import statement to Airflow 2.X format
    
    Args:
        import_line: Import statement to transform
        
    Returns:
        Transformed import statement compatible with Airflow 2.X
    """
    # Check if this is an Airflow 1.X style import
    if not AIRFLOW_1_IMPORT_PATTERNS.search(import_line):
        return import_line
    
    # Handle explicit mappings first
    for old_import, new_import in AIRFLOW_1_TO_2_IMPORT_MAPPING.items():
        if f"from airflow.{old_import}" in import_line:
            return import_line.replace(f"from airflow.{old_import}", f"from airflow.{new_import}")
    
    # Handle contrib operators (now in provider packages)
    if '.contrib.operators.' in import_line:
        # Extract the module and imported elements
        match = re.search(r'from airflow\.contrib\.operators\.(\w+) import (.*)', import_line)
        if match:
            module, imports = match.groups()
            
            # Handle specific provider mappings
            if 'gcs' in module or 'gcp' in module or 'bigquery' in module:
                return f"from airflow.providers.google.cloud.operators.{module} import {imports}"
            elif 'aws' in module or 's3' in module:
                return f"from airflow.providers.amazon.aws.operators.{module} import {imports}"
            elif 'jdbc' in module or 'sql' in module or 'postgres' in module or 'mysql' in module:
                return f"from airflow.providers.common.sql.operators.{module} import {imports}"
            # Default to keeping the import but adding a warning comment
            else:
                return f"{import_line}  # WARNING: This import may need to be updated for Airflow 2.X"
    
    # Handle contrib hooks
    if '.contrib.hooks.' in import_line:
        match = re.search(r'from airflow\.contrib\.hooks\.(\w+) import (.*)', import_line)
        if match:
            module, imports = match.groups()
            
            # Handle specific provider mappings
            if 'gcs' in module or 'gcp' in module or 'bigquery' in module:
                return f"from airflow.providers.google.cloud.hooks.{module} import {imports}"
            elif 'aws' in module or 's3' in module:
                return f"from airflow.providers.amazon.aws.hooks.{module} import {imports}"
            elif 'sql' in module or 'postgres' in module or 'mysql' in module:
                return f"from airflow.providers.common.sql.hooks.{module} import {imports}"
            else:
                return f"{import_line}  # WARNING: This import may need to be updated for Airflow 2.X"
    
    # Handle operators that moved but don't have explicit mappings
    if '.operators.' in import_line and 'import' in import_line:
        for operator, full_path in AIRFLOW_1_TO_2_OPERATOR_MAPPING.items():
            if f" {operator}" in import_line or f",{operator}" in import_line:
                module_path = full_path.rsplit('.', 1)[0]
                # Replace just the import path, preserving the imported operator name
                return re.sub(r'from airflow\.\w+\.\w+', f"from {module_path}", import_line)
    
    # If we can't transform it, return original with a warning comment
    if any(x in import_line for x in ['operators', 'hooks', 'sensors', 'contrib']):
        return f"{import_line}  # WARNING: This import may need to be updated for Airflow 2.X"
    
    return import_line


def transform_code_imports(code: str) -> str:
    """
    Transforms all Airflow 1.X import statements in a code block to Airflow 2.X format
    
    Args:
        code: Source code containing Airflow 1.X imports
        
    Returns:
        Code with all imports updated to Airflow 2.X format
    """
    # Split code into lines
    lines = code.split('\n')
    transformed_lines = []
    
    # Process each line
    for line in lines:
        if line.strip().startswith('from airflow.') or line.strip().startswith('import airflow.'):
            transformed_lines.append(transform_import(line))
        else:
            transformed_lines.append(line)
    
    # Join lines back into a single string
    return '\n'.join(transformed_lines)


def transform_operator_references(code: str) -> str:
    """
    Updates operator class references in code to use Airflow 2.X naming
    
    Args:
        code: Source code containing Airflow 1.X operator references
        
    Returns:
        Code with updated operator class references
    """
    try:
        # Parse the code into an AST
        tree = ast.parse(code)
        
        # Create a transformer to modify class references
        class OperatorReferenceTransformer(ast.NodeTransformer):
            def visit_Name(self, node):
                # Check if this is an operator class name that needs to be updated
                if node.id in AIRFLOW_1_TO_2_OPERATOR_MAPPING:
                    # For fully qualified imports, we just need to keep the class name
                    # The import statement will be updated by transform_code_imports
                    return node
                return node
                
            def visit_Attribute(self, node):
                # Handle cases like airflow.operators.PythonOperator -> airflow.operators.python.PythonOperator
                # First visit any child nodes
                self.generic_visit(node)
                
                # Check for operator references like airflow.operators.PythonOperator
                if (isinstance(node.value, ast.Attribute) and 
                    isinstance(node.value.value, ast.Name) and 
                    node.value.value.id == 'airflow' and
                    (node.value.attr == 'operators' or node.value.attr == 'contrib')):
                    
                    # Check if the attribute is an operator name that needs updating
                    if node.attr in AIRFLOW_1_TO_2_OPERATOR_MAPPING:
                        # We'll need to update this in the source code as it's hard to reconstruct in the AST
                        logger.warning(f"Found operator reference that may need updating: airflow.{node.value.attr}.{node.attr}")
                
                return node
        
        # Apply the transformation
        new_tree = OperatorReferenceTransformer().visit(tree)
        
        # Generate modified source code
        # Using ast.unparse() for Python 3.9+, but using a different approach for compatibility
        try:
            # For Python 3.9+
            if hasattr(ast, 'unparse'):
                return ast.unparse(new_tree)
            else:
                # For Python 3.8 and earlier, use a third-party library
                import astor
                return astor.to_source(new_tree)
        except ImportError:
            logger.warning("Could not convert modified AST back to code - astor library not available")
            return code
            
    except SyntaxError as e:
        logger.error(f"Syntax error in code: {e}")
        return code
    except Exception as e:
        logger.error(f"Error transforming operator references: {e}")
        return code


def convert_to_taskflow(code: str, simple_conversion: bool = True) -> str:
    """
    Converts a PythonOperator-based function to TaskFlow API pattern
    
    Args:
        code: Code with PythonOperator to convert
        simple_conversion: If True, uses a simpler conversion approach
        
    Returns:
        Code with PythonOperator converted to TaskFlow API pattern
    """
    if not is_taskflow_available():
        logger.warning("TaskFlow API not available. Skipping conversion.")
        return code
    
    try:
        # Parse the code into an AST
        tree = ast.parse(code)
        
        # Track Python callables used in PythonOperators
        python_callables = {}
        python_operator_assignments = {}
        
        # First pass: find Python callables used in PythonOperators
        class PythonCallableFinder(ast.NodeVisitor):
            def visit_Assign(self, node):
                # Look for assignments with PythonOperator
                if (isinstance(node.value, ast.Call) and 
                    isinstance(node.value.func, ast.Name) and 
                    node.value.func.id == 'PythonOperator'):
                    
                    # Get the task_id and python_callable
                    task_id = None
                    python_callable = None
                    provide_context = False
                    
                    for kw in node.value.keywords:
                        if kw.arg == 'task_id' and isinstance(kw.value, ast.Str):
                            task_id = kw.value.s
                        elif kw.arg == 'python_callable' and isinstance(kw.value, ast.Name):
                            python_callable = kw.value.id
                        elif kw.arg == 'provide_context' and isinstance(kw.value, ast.NameConstant):
                            provide_context = kw.value.value
                    
                    if python_callable and task_id:
                        python_callables[python_callable] = {
                            'task_id': task_id,
                            'provide_context': provide_context
                        }
                        
                        # Store the assignment node for later replacement
                        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                            task_var = node.targets[0].id
                            python_operator_assignments[task_var] = {
                                'python_callable': python_callable,
                                'task_id': task_id,
                                'node': node
                            }
                
                self.generic_visit(node)
        
        # Second pass: add @task decorator to functions
        class TaskflowTransformer(ast.NodeTransformer):
            def visit_FunctionDef(self, node):
                # Check if this function is used as a python_callable
                if node.name in python_callables:
                    # Create a @task decorator
                    task_decorator = ast.Name(id='task', ctx=ast.Load())
                    
                    # Add parameters if needed
                    task_id = python_callables[node.name]['task_id']
                    keywords = [ast.keyword(arg='task_id', value=ast.Str(s=task_id))]
                    
                    decorator_call = ast.Call(
                        func=task_decorator,
                        args=[],
                        keywords=keywords
                    )
                    
                    # Add the decorator to the function
                    node.decorator_list.append(decorator_call)
                    
                    # If provide_context was True, we need to modify the function parameters
                    # to accept **kwargs or specific parameters
                    if python_callables[node.name]['provide_context']:
                        # Check if function already accepts **kwargs
                        has_kwargs = any(isinstance(arg, ast.arg) and arg.arg == 'kwargs' 
                                         for arg in node.args.kwonlyargs)
                        
                        if not has_kwargs:
                            # Add a comment explaining the change
                            comment = ast.Expr(
                                value=ast.Str(s=f"# Function parameters modified for TaskFlow API (was provide_context=True)")
                            )
                            return [comment, node]
                
                return node
        
        # Apply transformers
        finder = PythonCallableFinder()
        finder.visit(tree)
        
        if not python_callables:
            # No PythonOperators found, return original code
            return code
        
        # Add TaskFlow import if needed
        import_node = ast.ImportFrom(
            module='airflow.decorators',
            names=[ast.alias(name='task', asname=None)],
            level=0
        )
        
        # Insert import at the beginning of the file
        tree.body.insert(0, import_node)
        
        # Apply the transformation to add decorators
        tree = TaskflowTransformer().visit(tree)
        
        # Convert back to source code
        try:
            # For Python 3.9+
            if hasattr(ast, 'unparse'):
                new_code = ast.unparse(tree)
            else:
                # For Python 3.8 and earlier, use a third-party library
                import astor
                new_code = astor.to_source(tree)
                
            # Add comments for operator replacements
            for task_var, task_info in python_operator_assignments.items():
                python_callable = task_info['python_callable']
                replacement_comment = (
                    f"\n# AIRFLOW 2.X MIGRATION: Replace '{task_var} = PythonOperator(task_id='{task_info['task_id']}', "
                    f"python_callable={python_callable}, ...)' with '{task_var} = {python_callable}()'"
                )
                new_code += replacement_comment
                
            return new_code
        except ImportError:
            logger.warning("Could not convert modified AST back to code - astor library not available")
            return code
            
    except SyntaxError as e:
        logger.error(f"Syntax error in code: {e}")
        return code
    except Exception as e:
        logger.error(f"Error converting to TaskFlow API: {e}")
        return code


def create_mock_airflow_module(version: str) -> MagicMock:
    """
    Creates a mock Airflow module for compatibility testing
    
    Args:
        version: Version string to assign to the mock module
        
    Returns:
        Mock Airflow module with specified version
    """
    mock_airflow = MagicMock()
    mock_airflow.__version__ = version
    
    # Setup common Airflow components
    mock_airflow.models = MagicMock()
    mock_airflow.operators = MagicMock()
    mock_airflow.hooks = MagicMock()
    mock_airflow.sensors = MagicMock()
    mock_airflow.utils = MagicMock()
    mock_airflow.executors = MagicMock()
    mock_airflow.DAG = MagicMock()
    
    # Configure version-specific features
    major_version = int(version.split('.')[0])
    
    if major_version >= 2:
        # Airflow 2.X specific structure
        mock_airflow.providers = MagicMock()
        mock_airflow.decorators = MagicMock()
        mock_airflow.decorators.task = MagicMock(return_value=lambda func: func)
        
        # Setup operator modules
        mock_airflow.operators.python = MagicMock()
        mock_airflow.operators.python.PythonOperator = MagicMock()
        mock_airflow.operators.bash = MagicMock()
        mock_airflow.operators.bash.BashOperator = MagicMock()
        
        # Setup Google provider
        mock_airflow.providers.google = MagicMock()
        mock_airflow.providers.google.cloud = MagicMock()
        mock_airflow.providers.google.cloud.operators = MagicMock()
        mock_airflow.providers.google.cloud.hooks = MagicMock()
        mock_airflow.providers.google.cloud.transfers = MagicMock()
    else:
        # Airflow 1.X specific structure
        mock_airflow.contrib = MagicMock()
        mock_airflow.contrib.operators = MagicMock()
        mock_airflow.contrib.hooks = MagicMock()
        mock_airflow.contrib.sensors = MagicMock()
        
        # Setup operator modules
        mock_airflow.operators.python_operator = MagicMock()
        mock_airflow.operators.python_operator.PythonOperator = MagicMock()
        mock_airflow.operators.bash_operator = MagicMock()
        mock_airflow.operators.bash_operator.BashOperator = MagicMock()
    
    return mock_airflow


@contextlib.contextmanager
def mock_airflow2_imports():
    """
    Creates a context manager that mocks Airflow 2.X imports when running in Airflow 1.X
    
    Returns:
        Context manager for mocking Airflow 2.X imports
    """
    # Only apply mocks if we're not already in Airflow 2.X
    if is_airflow2():
        yield
        return
    
    mock_airflow = create_mock_airflow_module('2.0.0')
    
    patches = [
        patch('airflow.__version__', '2.0.0'),
        patch('airflow.decorators', mock_airflow.decorators),
        patch('airflow.providers', mock_airflow.providers),
        patch('airflow.operators.python', mock_airflow.operators.python),
        patch('airflow.operators.bash', mock_airflow.operators.bash)
    ]
    
    # Apply all patches
    for p in patches:
        p.start()
    
    try:
        yield
    finally:
        # Remove all patches
        for p in patches:
            p.stop()


@contextlib.contextmanager
def mock_airflow1_imports():
    """
    Creates a context manager that mocks Airflow 1.X imports when running in Airflow 2.X
    
    Returns:
        Context manager for mocking Airflow 1.X imports
    """
    # Only apply mocks if we're not already in Airflow 1.X
    if not is_airflow2():
        yield
        return
    
    mock_airflow = create_mock_airflow_module('1.10.15')
    
    patches = [
        patch('airflow.__version__', '1.10.15'),
        patch('airflow.contrib', mock_airflow.contrib),
        patch('airflow.operators.python_operator', mock_airflow.operators.python_operator),
        patch('airflow.operators.bash_operator', mock_airflow.operators.bash_operator)
    ]
    
    # Apply all patches
    for p in patches:
        p.start()
    
    try:
        yield
    finally:
        # Remove all patches
        for p in patches:
            p.stop()


class Airflow2CompatibilityTestMixin:
    """
    Mixin class providing utilities for testing code with both Airflow 1.X and 2.X
    
    This mixin provides decorators and utility methods to help write tests that can
    run with both Airflow versions and detect version-specific issues.
    """
    
    def __init__(self):
        """Initialize the compatibility mixin"""
        self._using_airflow2 = is_airflow2()
        self._has_taskflow = is_taskflow_available()
    
    def skipIfAirflow1(self, test_func):
        """
        Decorator to skip a test when running with Airflow 1.X
        
        Args:
            test_func: Test function to conditionally skip
            
        Returns:
            Wrapped test function
        """
        if not self._using_airflow2:
            return unittest.skip("Test requires Airflow 2.X")(test_func)
        return test_func
    
    def skipIfAirflow2(self, test_func):
        """
        Decorator to skip a test when running with Airflow 2.X
        
        Args:
            test_func: Test function to conditionally skip
            
        Returns:
            Wrapped test function
        """
        if self._using_airflow2:
            return unittest.skip("Test requires Airflow 1.X")(test_func)
        return test_func
    
    def skipIfNoTaskflow(self, test_func):
        """
        Decorator to skip a test when TaskFlow API is not available
        
        Args:
            test_func: Test function to conditionally skip
            
        Returns:
            Wrapped test function
        """
        if not self._has_taskflow:
            return unittest.skip("Test requires TaskFlow API")(test_func)
        return test_func
    
    def runWithAirflow2(self, func, *args, **kwargs):
        """
        Runs a test function with Airflow 2.X imports mocked
        
        Args:
            func: Function to run
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            
        Returns:
            Result of function execution
        """
        if self._using_airflow2:
            # Already using Airflow 2.X, run directly
            return func(*args, **kwargs)
        
        # Mock Airflow 2.X imports and run
        with mock_airflow2_imports():
            return func(*args, **kwargs)
    
    def runWithAirflow1(self, func, *args, **kwargs):
        """
        Runs a test function with Airflow 1.X imports mocked
        
        Args:
            func: Function to run
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            
        Returns:
            Result of function execution
        """
        if not self._using_airflow2:
            # Already using Airflow 1.X, run directly
            return func(*args, **kwargs)
        
        # Mock Airflow 1.X imports and run
        with mock_airflow1_imports():
            return func(*args, **kwargs)


class Airflow2CodeComparator:
    """
    Class for comparing code compatibility between Airflow 1.X and 2.X
    
    This class analyzes code intended for both Airflow versions and reports
    on compatibility issues, differences, and potential migration problems.
    """
    
    def __init__(self, airflow1_code: str, airflow2_code: str):
        """
        Initialize the code comparator
        
        Args:
            airflow1_code: Original Airflow 1.X code
            airflow2_code: Migrated Airflow 2.X code
        """
        self.airflow1_code = airflow1_code
        self.airflow2_code = airflow2_code
        self.comparison_result = {}
    
    def compare_imports(self) -> Dict:
        """
        Compares import statements between Airflow 1.X and 2.X code
        
        Returns:
            Import comparison results
        """
        # Extract import statements from both code versions
        airflow1_imports = [line.strip() for line in self.airflow1_code.split('\n') 
                           if line.strip().startswith('from airflow') or 
                              line.strip().startswith('import airflow')]
        
        airflow2_imports = [line.strip() for line in self.airflow2_code.split('\n') 
                           if line.strip().startswith('from airflow') or 
                              line.strip().startswith('import airflow')]
        
        # Transform Airflow 1.X imports to expected Airflow 2.X format
        expected_airflow2_imports = [transform_import(imp) for imp in airflow1_imports]
        
        # Find imports that don't match expectations
        import_issues = []
        for i, (expected, actual) in enumerate(zip(expected_airflow2_imports, airflow2_imports)):
            if expected.split('#')[0].strip() != actual.split('#')[0].strip():
                import_issues.append({
                    'airflow1': airflow1_imports[i],
                    'expected_airflow2': expected,
                    'actual_airflow2': actual
                })
        
        # Prepare comparison result
        result = {
            'airflow1_import_count': len(airflow1_imports),
            'airflow2_import_count': len(airflow2_imports),
            'import_issues': import_issues,
            'import_mismatch_count': len(import_issues)
        }
        
        self.comparison_result['imports'] = result
        return result
    
    def compare_operators(self) -> Dict:
        """
        Compares operator usage between Airflow 1.X and 2.X code
        
        Returns:
            Operator comparison results
        """
        # Extract operator instantiations using regex patterns
        # This is a simple approach - a full AST parse would be more accurate but complex
        
        operator_pattern = r'(\w+)\s*=\s*(\w+Operator)\('
        airflow1_operators = re.findall(operator_pattern, self.airflow1_code)
        airflow2_operators = re.findall(operator_pattern, self.airflow2_code)
        
        # Check for deprecated parameters
        deprecated_param_issues = []
        for param in AIRFLOW_1_DEPRECATED_PARAMS:
            pattern = rf'\b{param}\s*='
            if re.search(pattern, self.airflow2_code):
                deprecated_param_issues.append({
                    'parameter': param,
                    'usage_count': len(re.findall(pattern, self.airflow2_code))
                })
        
        # Check for operator class differences
        operator_issues = []
        for var_name, op_type in airflow1_operators:
            matching_ops = [op for v, op in airflow2_operators if v == var_name]
            if not matching_ops:
                operator_issues.append({
                    'variable': var_name,
                    'airflow1_operator': op_type,
                    'airflow2_operator': 'Not found',
                    'issue': 'Operator missing in Airflow 2.X code'
                })
            elif matching_ops[0] != op_type and op_type in AIRFLOW_1_TO_2_OPERATOR_MAPPING:
                operator_issues.append({
                    'variable': var_name,
                    'airflow1_operator': op_type,
                    'airflow2_operator': matching_ops[0],
                    'issue': 'Operator class name changed'
                })
        
        # Prepare comparison result
        result = {
            'airflow1_operator_count': len(airflow1_operators),
            'airflow2_operator_count': len(airflow2_operators),
            'operator_issues': operator_issues,
            'deprecated_parameter_issues': deprecated_param_issues,
            'operator_issue_count': len(operator_issues),
            'deprecated_parameter_count': len(deprecated_param_issues)
        }
        
        self.comparison_result['operators'] = result
        return result
    
    def compare_taskflow_usage(self) -> Dict:
        """
        Compares TaskFlow API usage in Airflow 2.X code
        
        Returns:
            TaskFlow comparison results
        """
        # Check if @task decorator is used
        taskflow_used = '@task' in self.airflow2_code
        
        # Find PythonOperator usage in Airflow 1.X code
        python_op_count = len(re.findall(r'PythonOperator\(', self.airflow1_code))
        taskflow_count = len(re.findall(r'@task', self.airflow2_code))
        
        # Check for potential conversion candidates
        potential_conversions = []
        python_op_matches = re.finditer(r'(\w+)\s*=\s*PythonOperator\(\s*task_id\s*=\s*[\'"]([^\'"]+)[\'"],\s*python_callable\s*=\s*(\w+)', self.airflow1_code)
        
        for match in python_op_matches:
            var_name, task_id, callable_name = match.groups()
            
            # Check if this callable was converted to TaskFlow
            if re.search(rf'@task\(.*?\)\s*def\s+{callable_name}\b', self.airflow2_code):
                continue
                
            potential_conversions.append({
                'variable': var_name,
                'task_id': task_id,
                'python_callable': callable_name
            })
        
        # Prepare comparison result
        result = {
            'taskflow_used': taskflow_used,
            'python_operator_count_airflow1': python_op_count,
            'taskflow_decorator_count_airflow2': taskflow_count,
            'potential_conversions': potential_conversions,
            'potential_conversion_count': len(potential_conversions)
        }
        
        self.comparison_result['taskflow'] = result
        return result
    
    def get_comparison_summary(self) -> Dict:
        """
        Generates a summary of all code comparisons
        
        Returns:
            Summary of comparison results
        """
        # Run all comparisons if not already run
        if 'imports' not in self.comparison_result:
            self.compare_imports()
        
        if 'operators' not in self.comparison_result:
            self.compare_operators()
        
        if 'taskflow' not in self.comparison_result:
            self.compare_taskflow_usage()
        
        # Calculate overall compatibility score (0-100)
        issue_count = (
            self.comparison_result['imports']['import_mismatch_count'] +
            self.comparison_result['operators']['operator_issue_count'] +
            self.comparison_result['operators']['deprecated_parameter_count']
        )
        
        # Simple scoring formula
        max_score = 100
        penalty_per_issue = 5
        compatibility_score = max(0, max_score - (issue_count * penalty_per_issue))
        
        # Prepare summary
        summary = {
            'compatibility_score': compatibility_score,
            'total_issues': issue_count,
            'recommendations': []
        }
        
        # Add recommendations based on findings
        if self.comparison_result['imports']['import_mismatch_count'] > 0:
            summary['recommendations'].append(
                f"Fix {self.comparison_result['imports']['import_mismatch_count']} import mismatches"
            )
        
        if self.comparison_result['operators']['deprecated_parameter_count'] > 0:
            summary['recommendations'].append(
                f"Remove {self.comparison_result['operators']['deprecated_parameter_count']} deprecated parameters"
            )
        
        if (self.comparison_result['taskflow']['python_operator_count_airflow1'] > 0 and
            self.comparison_result['taskflow']['taskflow_decorator_count_airflow2'] == 0):
            summary['recommendations'].append(
                "Consider using TaskFlow API for PythonOperators"
            )
        
        return summary


class CompatibleDAGTestCase:
    """
    Base test case class for writing tests that work across Airflow versions
    
    This class provides methods for running and comparing DAG execution across
    Airflow 1.X and 2.X environments.
    """
    
    def __init__(self):
        """Initialize the compatible DAG test case"""
        self._mixin = Airflow2CompatibilityTestMixin()
    
    def setUp(self):
        """Sets up the test environment for cross-version compatibility"""
        # Set up any needed environment variables or configurations
        os.environ['AIRFLOW_TEST_MODE'] = 'True'
    
    def tearDown(self):
        """Cleans up after tests"""
        # Clean up any environment variables or configurations
        if 'AIRFLOW_TEST_MODE' in os.environ:
            del os.environ['AIRFLOW_TEST_MODE']
    
    def run_dag_in_airflow1(self, dag, execution_date) -> Dict:
        """
        Runs a DAG using Airflow 1.X environment
        
        Args:
            dag: The DAG to run
            execution_date: The execution date
            
        Returns:
            Execution results
        """
        return self._mixin.runWithAirflow1(self._run_dag, dag, execution_date)
    
    def run_dag_in_airflow2(self, dag, execution_date) -> Dict:
        """
        Runs a DAG using Airflow 2.X environment
        
        Args:
            dag: The DAG to run
            execution_date: The execution date
            
        Returns:
            Execution results
        """
        return self._mixin.runWithAirflow2(self._run_dag, dag, execution_date)
    
    def compare_dag_execution(self, dag, execution_date) -> Dict:
        """
        Compares DAG execution between Airflow 1.X and 2.X
        
        Args:
            dag: The DAG to test
            execution_date: The execution date
            
        Returns:
            Comparison of execution results
        """
        # Run DAG in both environments
        airflow1_results = self.run_dag_in_airflow1(dag, execution_date)
        airflow2_results = self.run_dag_in_airflow2(dag, execution_date)
        
        # Compare results
        comparison = {
            'airflow1': airflow1_results,
            'airflow2': airflow2_results,
            'task_differences': [],
            'task_count': len(airflow1_results.get('tasks', {})),
            'successful_in_both': True,
            'execution_time_diff_seconds': (
                airflow2_results.get('execution_time', 0) - 
                airflow1_results.get('execution_time', 0)
            )
        }
        
        # Compare individual tasks
        for task_id, airflow1_task in airflow1_results.get('tasks', {}).items():
            airflow2_task = airflow2_results.get('tasks', {}).get(task_id)
            
            if not airflow2_task:
                comparison['task_differences'].append({
                    'task_id': task_id,
                    'issue': 'Task exists in Airflow 1.X but not in Airflow 2.X'
                })
                comparison['successful_in_both'] = False
                continue
            
            # Compare task state
            if airflow1_task.get('state') != airflow2_task.get('state'):
                comparison['task_differences'].append({
                    'task_id': task_id,
                    'issue': 'Task state differs',
                    'airflow1_state': airflow1_task.get('state'),
                    'airflow2_state': airflow2_task.get('state')
                })
                comparison['successful_in_both'] = False
        
        # Check for tasks in Airflow 2.X but not in Airflow 1.X
        for task_id in airflow2_results.get('tasks', {}):
            if task_id not in airflow1_results.get('tasks', {}):
                comparison['task_differences'].append({
                    'task_id': task_id,
                    'issue': 'Task exists in Airflow 2.X but not in Airflow 1.X'
                })
                comparison['successful_in_both'] = False
        
        return comparison
    
    def _run_dag(self, dag, execution_date) -> Dict:
        """
        Helper method to run a DAG and collect execution results
        
        This is an internal method called by run_dag_in_airflow1 and run_dag_in_airflow2
        """
        # Import here to avoid potential circular imports
        from airflow.models import DagRun, TaskInstance
        
        try:
            # Create a DagRun
            dag_run = dag.create_dagrun(
                state='running',
                execution_date=execution_date,
                run_id=f'test_{execution_date.isoformat()}'
            )
            
            # Run the DAG
            start_time = time.time()
            dag.run(execution_date=execution_date)
            execution_time = time.time() - start_time
            
            # Collect results
            task_results = {}
            for task in dag.tasks:
                task_instance = TaskInstance(task, execution_date)
                task_instance.refresh_from_db()
                
                xcom_values = {}
                try:
                    # Get XCom values returned by the task
                    xcom_values = task_instance.xcom_pull(task_ids=task.task_id)
                except Exception:
                    # XCom might not be available
                    pass
                
                task_results[task.task_id] = {
                    'state': task_instance.state,
                    'duration': task_instance.duration,
                    'start_date': task_instance.start_date,
                    'end_date': task_instance.end_date,
                    'xcom_values': xcom_values
                }
            
            # Prepare execution summary
            return {
                'dag_id': dag.dag_id,
                'execution_date': execution_date,
                'run_id': dag_run.run_id,
                'execution_time': execution_time,
                'tasks': task_results,
                'success': all(task.get('state') == 'success' for task in task_results.values())
            }
        except Exception as e:
            # Handle execution errors
            return {
                'dag_id': dag.dag_id,
                'execution_date': execution_date,
                'error': str(e),
                'tasks': {},
                'success': False
            }