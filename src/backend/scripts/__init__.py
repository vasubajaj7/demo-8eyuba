"""
Initialization file for the backend scripts package, which provides utility scripts
for Apache Airflow 2.X and Cloud Composer 2 migration and management. It exposes common
functions and utilities from individual script modules for easier importing throughout
the application.

This package supports:
- Migration from Airflow 1.10.15 to Airflow 2.X
- Continuous integration and deployment pipelines
- Multi-environment deployment processes
"""

import logging  # standard library
import importlib  # standard library
import os  # standard library

# Internal module imports
from .validate_dags import validate_dag_files, create_airflow2_compatibility_report
from .deploy_dags import deploy_to_environment
from .import_variables import load_variables_from_file, import_variables
from .import_connections import load_connections_from_file, import_connections
from .backup_metadata import backup
from .restore_metadata import restore

# Global constants
VERSION = '1.0.0'
LOGGER = logging.getLogger('airflow.scripts')
SUPPORTED_ENVIRONMENTS = ['dev', 'qa', 'prod']


def detect_airflow_version():
    """
    Detect the installed Airflow version across the application.
    
    Returns:
        tuple: Major, minor, patch version numbers
    """
    try:
        # Import airflow dynamically to check version
        airflow = importlib.import_module('airflow')
        version_str = getattr(airflow, '__version__', '0.0.0')
        
        # Parse version string into components
        try:
            # Split version string and handle potential extra components
            parts = version_str.split('.')
            major = int(parts[0]) if len(parts) > 0 else 0
            minor = int(parts[1]) if len(parts) > 1 else 0
            patch = int(parts[2].split('+')[0].split('-')[0]) if len(parts) > 2 else 0
            return major, minor, patch
        except (IndexError, ValueError):
            LOGGER.warning(f"Could not parse Airflow version: {version_str}")
            return 0, 0, 0
        
    except ImportError:
        LOGGER.warning("Could not import Airflow. Make sure it is installed.")
        return 0, 0, 0


def setup_package_logging(log_level):
    """
    Configure logging for the entire scripts package.
    
    Args:
        log_level (str): Log level to set for the package
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Convert string log level to numeric value if needed
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    # Configure the root logger with specified log level
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Set common logging format for all script modules
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create console handler with formatter
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    LOGGER.addHandler(console_handler)
    LOGGER.setLevel(numeric_level)
    
    return LOGGER


def get_script_base_path():
    """
    Get the base path for all scripts in the package.
    
    Returns:
        str: Absolute path to the scripts directory
    """
    # Get the current file's directory using os.path
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    # Ensure path is normalized and absolute
    return os.path.normpath(current_file_dir)