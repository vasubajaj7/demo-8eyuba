#!/usr/bin/env python3
"""
Utility script for importing Airflow variables from JSON configuration files
into Cloud Composer 2 environments.

This script facilitates migration from Airflow 1.10.15 to Airflow 2.X by handling
version-specific differences in variable management. It provides flexible options
for importing from local files or GCS buckets with environment-specific variable
processing.

Usage:
    python import_variables.py --file_path /path/to/variables.json
    python import_variables.py --gcs_path gs://bucket/path/to/variables.json --environment dev
    python import_variables.py --gcs_path gs://bucket/path/to/variables.json --environment prod --force
"""

import os
import sys
import json
import argparse
import logging
import tempfile
import importlib
import re
from typing import Dict, Tuple, Any, List, Optional, Union, Type

# Import GCP utilities for Cloud Storage operations
from ..dags.utils.gcp_utils import gcs_file_exists, gcs_download_file

# Set up global logger
logger = logging.getLogger(__name__)

# Define supported environments
SUPPORTED_ENVIRONMENTS = ['dev', 'qa', 'prod']

# Airflow Variable module paths for different versions
AIRFLOW1_VARIABLE_MODULE = 'airflow.models.Variable'
AIRFLOW2_VARIABLE_MODULE = 'airflow.models.variable.Variable'


def setup_logging(log_level: str) -> logging.Logger:
    """
    Configure logging with appropriate format and level
    
    Args:
        log_level: Desired logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Configured logger instance
    """
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    # Configure basic logging
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=numeric_level
    )
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    logger.addHandler(console_handler)
    
    return logger


def parse_arguments(args=None) -> argparse.Namespace:
    """
    Parse command-line arguments for the script
    
    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Import Airflow variables from a JSON file into Airflow/Cloud Composer'
    )
    
    # Input source options (mutually exclusive)
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        '--file_path', 
        type=str, 
        help='Path to local JSON file containing variables'
    )
    source_group.add_argument(
        '--gcs_path', 
        type=str, 
        help='GCS path to JSON file containing variables (format: gs://bucket/object)'
    )
    
    # Environment option
    parser.add_argument(
        '--environment', 
        type=str, 
        choices=SUPPORTED_ENVIRONMENTS,
        help='Target environment (dev, qa, prod) for environment-specific variables'
    )
    
    # Import behavior flags
    parser.add_argument(
        '--dry_run', 
        action='store_true', 
        help='Simulate import without making changes'
    )
    parser.add_argument(
        '--skip_existing', 
        action='store_true', 
        help='Skip variables that already exist'
    )
    parser.add_argument(
        '--force', 
        action='store_true', 
        help='Force update of existing variables'
    )
    
    # Logging options
    parser.add_argument(
        '--log_level', 
        type=str, 
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set logging level'
    )
    
    # Connection ID for GCS operations
    parser.add_argument(
        '--conn_id', 
        type=str, 
        default='google_cloud_default',
        help='Airflow connection ID for GCP'
    )
    
    return parser.parse_args(args)


def detect_airflow_version() -> Tuple[int, int, int]:
    """
    Detect the installed Airflow version
    
    Returns:
        Major, minor, patch version numbers
    """
    try:
        import airflow
        version_str = getattr(airflow, '__version__', '0.0.0')
        
        # Parse version string into components
        match = re.match(r'(\d+)\.(\d+)\.(\d+)', version_str)
        if match:
            major, minor, patch = map(int, match.groups())
            logger.debug(f"Detected Airflow version: {major}.{minor}.{patch}")
            return major, minor, patch
        else:
            logger.warning(f"Could not parse Airflow version: {version_str}")
            return 0, 0, 0
            
    except ImportError:
        logger.error("Could not import Airflow. Make sure it is installed.")
        return 0, 0, 0


def get_variable_class() -> Type:
    """
    Get the appropriate Variable class based on Airflow version
    
    Returns:
        Airflow Variable class
    """
    major_version, _, _ = detect_airflow_version()
    
    try:
        # For Airflow 2.X
        if major_version >= 2:
            module_path = AIRFLOW2_VARIABLE_MODULE
        # For Airflow 1.X
        else:
            module_path = AIRFLOW1_VARIABLE_MODULE
            
        # Dynamically import the appropriate module
        module_parts = module_path.split('.')
        class_name = module_parts[-1]
        module_path = '.'.join(module_parts[:-1])
        
        module = importlib.import_module(module_path)
        variable_class = getattr(module, class_name)
        
        logger.debug(f"Using Variable class from {module_path}.{class_name}")
        return variable_class
        
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to import Airflow Variable class: {str(e)}")
        sys.exit(1)


def load_variables_from_file(file_path: str) -> Dict:
    """
    Load variables from a local JSON file
    
    Args:
        file_path: Path to the local JSON file
        
    Returns:
        Loaded variables dictionary
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        with open(file_path, 'r') as f:
            variables = json.load(f)
            
        if not isinstance(variables, dict):
            raise ValueError(f"Expected JSON object, got {type(variables).__name__}")
            
        logger.info(f"Loaded {len(variables)} variables from {file_path}")
        return variables
        
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {file_path}: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading variables from {file_path}: {str(e)}")
        sys.exit(1)


def load_variables_from_gcs(gcs_path: str, conn_id: str = 'google_cloud_default') -> Dict:
    """
    Load variables from a GCS JSON file
    
    Args:
        gcs_path: GCS path in format gs://bucket/object
        conn_id: Airflow connection ID for GCP
        
    Returns:
        Loaded variables dictionary
    """
    try:
        # Parse GCS path
        if not gcs_path.startswith('gs://'):
            raise ValueError(f"Invalid GCS path: {gcs_path}. Must start with gs://")
            
        bucket_name = gcs_path.split('gs://')[1].split('/')[0]
        object_name = '/'.join(gcs_path.split('gs://')[1].split('/')[1:])
        
        if not object_name:
            raise ValueError(f"Invalid GCS path: {gcs_path}. Missing object name")
            
        # Check if file exists in GCS
        if not gcs_file_exists(bucket_name, object_name, conn_id):
            raise FileNotFoundError(f"GCS file not found: {gcs_path}")
            
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            # Download GCS file to temporary file
            gcs_download_file(bucket_name, object_name, temp_path, conn_id)
            
            # Load variables from temporary file
            variables = load_variables_from_file(temp_path)
            
            return variables
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)
                
    except (ValueError, FileNotFoundError) as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading variables from GCS {gcs_path}: {str(e)}")
        sys.exit(1)


def process_environment_variables(variables: Dict, environment: str) -> Dict:
    """
    Process variables based on target environment
    
    Args:
        variables: Original variables dictionary
        environment: Target environment (dev, qa, prod)
        
    Returns:
        Processed variables for the target environment
    """
    if environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(f"Unsupported environment: {environment}. "
                         f"Must be one of: {', '.join(SUPPORTED_ENVIRONMENTS)}")
    
    # Create a copy of the original variables
    processed_variables = variables.copy()
    
    # Process environment-specific sections
    env_section_key = f"{environment}_only"
    common_section_key = "common"
    
    # Initialize result dictionary
    result = {}
    
    # Add common variables if present
    if common_section_key in processed_variables:
        if isinstance(processed_variables[common_section_key], dict):
            result.update(processed_variables[common_section_key])
            logger.debug(f"Added {len(processed_variables[common_section_key])} common variables")
        else:
            logger.warning(f"'{common_section_key}' section is not a dictionary, skipping")
    
    # Add environment-specific variables if present
    if env_section_key in processed_variables:
        if isinstance(processed_variables[env_section_key], dict):
            result.update(processed_variables[env_section_key])
            logger.debug(f"Added {len(processed_variables[env_section_key])} {environment}-specific variables")
        else:
            logger.warning(f"'{env_section_key}' section is not a dictionary, skipping")
    
    # Process top-level variables (excluding special sections)
    special_sections = [f"{env}_only" for env in SUPPORTED_ENVIRONMENTS] + [common_section_key]
    for key, value in processed_variables.items():
        if key not in special_sections:
            result[key] = value
    
    # Replace environment placeholders in values
    for key, value in result.items():
        if isinstance(value, str):
            # Replace ${ENV} with the current environment
            result[key] = value.replace("${ENV}", environment)
    
    # Add environment variable
    result['environment'] = environment
    
    logger.info(f"Processed {len(result)} variables for environment: {environment}")
    return result


def import_variables(variables: Dict, dry_run: bool = False, 
                    skip_existing: bool = False, force: bool = False) -> Tuple[int, int, int]:
    """
    Import variables into Airflow
    
    Args:
        variables: Dictionary of variables to import
        dry_run: Simulate import without making changes
        skip_existing: Skip variables that already exist
        force: Force update of existing variables
        
    Returns:
        Counters for success, skipped, and failed variables
    """
    Variable = get_variable_class()
    
    success_count = 0
    skipped_count = 0
    failed_count = 0
    
    for key, value in variables.items():
        try:
            # Check if variable already exists
            exists = False
            try:
                existing_value = Variable.get(key, deserialize_json=False)
                exists = True
            except:
                exists = False
            
            # Skip if it exists and skip_existing is True
            if exists and skip_existing:
                logger.info(f"Variable '{key}' already exists, skipping")
                skipped_count += 1
                continue
            
            # Skip if it exists and force is False
            if exists and not force:
                logger.info(f"Variable '{key}' already exists, not updating (use --force to override)")
                skipped_count += 1
                continue
            
            # Handle JSON serialization for dictionary/list values
            if isinstance(value, (dict, list)):
                serialized_value = json.dumps(value)
                is_json = True
            else:
                serialized_value = str(value)
                is_json = False
            
            operation = "Updating" if exists else "Creating"
            
            if dry_run:
                logger.info(f"[DRY RUN] {operation} variable '{key}' with value type {type(value).__name__}")
                success_count += 1
            else:
                # Set the variable in Airflow
                Variable.set(key, serialized_value, serialize_json=is_json)
                logger.info(f"{operation} variable '{key}' with value type {type(value).__name__}")
                success_count += 1
                
        except Exception as e:
            logger.error(f"Failed to import variable '{key}': {str(e)}")
            failed_count += 1
    
    # Log summary
    logger.info(f"Import summary: {success_count} successful, {skipped_count} skipped, {failed_count} failed")
    
    return success_count, skipped_count, failed_count


def main(args: List[str] = None) -> int:
    """
    Main function to coordinate the variable import process
    
    Args:
        args: Command-line arguments (defaults to sys.argv[1:])
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Parse arguments
        parsed_args = parse_arguments(args)
        
        # Set up logging
        logger = setup_logging(parsed_args.log_level)
        
        # Load variables from file or GCS
        if parsed_args.file_path:
            logger.info(f"Loading variables from file: {parsed_args.file_path}")
            variables = load_variables_from_file(parsed_args.file_path)
        else:  # Must be GCS path due to mutually exclusive group
            logger.info(f"Loading variables from GCS: {parsed_args.gcs_path}")
            variables = load_variables_from_gcs(parsed_args.gcs_path, parsed_args.conn_id)
        
        # Process environment-specific variables if specified
        if parsed_args.environment:
            logger.info(f"Processing variables for environment: {parsed_args.environment}")
            variables = process_environment_variables(variables, parsed_args.environment)
        
        # Import variables
        logger.info(f"Importing {len(variables)} variables (dry_run={parsed_args.dry_run})")
        success, skipped, failed = import_variables(
            variables, 
            parsed_args.dry_run, 
            parsed_args.skip_existing,
            parsed_args.force
        )
        
        # Determine exit code
        if failed == 0 and success > 0:
            logger.info("Variable import completed successfully")
            return 0
        elif failed > 0 and success > 0:
            logger.warning("Variable import completed with some failures")
            return 1
        else:
            logger.error("Variable import failed completely")
            return 2
            
    except Exception as e:
        logger.error(f"Unhandled exception during variable import: {str(e)}")
        return 2


# Execute main function if script is run directly
if __name__ == "__main__":
    sys.exit(main())