#!/usr/bin/env python3
"""
Script for deploying Apache Airflow DAG files to Cloud Composer 2 environments with Airflow 2.X.

This script provides a robust deployment process with environment-specific configuration,
validation, and secure authentication. It supports CI/CD pipeline integration with proper
approval workflows for DEV, QA, and PROD environments.
"""

import argparse
import os
import sys
import logging
import glob
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# Google Cloud imports
from google.cloud.storage import Client as StorageClient
from google.auth import exceptions as auth_exceptions

# Internal imports
from validate_dags import validate_dag_files
from ..dags.utils.gcp_utils import gcs_upload_file, authenticate_gcp, GCSClient
from ..config.composer_dev import config as dev_config
from ..config.composer_qa import config as qa_config
from ..config.composer_prod import config as prod_config
from import_variables import load_variables_from_file, import_variables, process_environment_variables
from import_connections import load_connections_from_file, import_connections, process_environment_connections

# Set up logger
LOGGER = logging.getLogger(__name__)

# Default DAG folder
DEFAULT_DAG_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dags')

# Supported environments
SUPPORTED_ENVIRONMENTS = ['dev', 'qa', 'prod']

# Maximum number of workers for parallel uploads
MAX_WORKERS = 4


def setup_logging(log_level):
    """
    Sets up logging configuration for the script
    
    Args:
        log_level (str): Log level to set (DEBUG, INFO, WARNING, ERROR)
    """
    # Convert string log level to numeric value
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create a stream handler for console output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    
    # Add the handler to the root logger
    root_logger.addHandler(console_handler)


def parse_arguments():
    """
    Parses command-line arguments for the deployment script
    
    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Deploy DAG files to Cloud Composer 2 environments'
    )
    
    # Required arguments
    parser.add_argument(
        'environment',
        choices=SUPPORTED_ENVIRONMENTS,
        help='Target environment for deployment (dev, qa, prod)'
    )
    
    # Optional arguments
    parser.add_argument(
        '--source-folder',
        default=DEFAULT_DAG_FOLDER,
        help=f'Folder containing DAG files to deploy (default: {DEFAULT_DAG_FOLDER})'
    )
    
    parser.add_argument(
        '--variables-file',
        help='Path to JSON file containing Airflow variables to import'
    )
    
    parser.add_argument(
        '--connections-file',
        help='Path to JSON file containing Airflow connections to import'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without making actual changes (simulation mode)'
    )
    
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Upload files in parallel for faster deployment'
    )
    
    parser.add_argument(
        '--include-patterns',
        nargs='+',
        help='Glob patterns to include specific DAG files'
    )
    
    parser.add_argument(
        '--exclude-patterns',
        nargs='+',
        help='Glob patterns to exclude specific DAG files'
    )
    
    return parser.parse_args()


def get_environment_config(environment):
    """
    Retrieves the appropriate environment configuration based on target environment
    
    Args:
        environment (str): Target environment (dev, qa, prod)
        
    Returns:
        dict: Environment configuration settings
        
    Raises:
        ValueError: If an invalid environment is specified
    """
    if environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(f"Unsupported environment: {environment}. Must be one of: {', '.join(SUPPORTED_ENVIRONMENTS)}")
    
    if environment == 'dev':
        return dev_config
    elif environment == 'qa':
        return qa_config
    elif environment == 'prod':
        return prod_config
    else:
        # This should not happen due to the check above, but just in case
        raise ValueError(f"Unsupported environment: {environment}")


def validate_source_folder(source_folder):
    """
    Validates that the source folder exists and contains DAG files
    
    Args:
        source_folder (str): Path to the source folder
        
    Returns:
        bool: True if folder exists and contains DAG files, False otherwise
    """
    # Check if folder exists
    if not os.path.exists(source_folder):
        LOGGER.error(f"Source folder does not exist: {source_folder}")
        return False
    
    # Check if folder is readable
    if not os.path.isdir(source_folder) or not os.access(source_folder, os.R_OK):
        LOGGER.error(f"Source folder is not readable: {source_folder}")
        return False
    
    # Check if folder contains Python files
    python_files = glob.glob(os.path.join(source_folder, "**", "*.py"), recursive=True)
    if not python_files:
        LOGGER.warning(f"No Python files found in source folder: {source_folder}")
        return False
    
    LOGGER.info(f"Source folder validation successful: {source_folder}")
    return True


def collect_dag_files(source_folder, include_patterns=None, exclude_patterns=None):
    """
    Collects all Python files in the source folder that may contain DAGs
    
    Args:
        source_folder (str): Path to the source folder
        include_patterns (list): Glob patterns to include specific files
        exclude_patterns (list): Glob patterns to exclude specific files
        
    Returns:
        list: List of file paths to potential DAG files
    """
    # Find all Python files in the source folder and subdirectories
    all_files = glob.glob(os.path.join(source_folder, "**", "*.py"), recursive=True)
    
    # Filter out __pycache__ directories
    filtered_files = [f for f in all_files if "__pycache__" not in f]
    
    # Apply include patterns if specified
    if include_patterns:
        included_files = []
        for pattern in include_patterns:
            # If pattern doesn't have an extension, assume it's a Python file
            if not pattern.endswith('.py'):
                pattern = f"{pattern}*.py"
            # If pattern doesn't start with the source folder, prepend it
            if not pattern.startswith(source_folder):
                pattern = os.path.join(source_folder, pattern)
            # Collect files matching the pattern
            included_files.extend(glob.glob(pattern, recursive=True))
        # Use only the included files that were in the original list
        filtered_files = [f for f in filtered_files if f in included_files]
    
    # Apply exclude patterns if specified
    if exclude_patterns:
        for pattern in exclude_patterns:
            # If pattern doesn't have an extension, assume it's a Python file
            if not pattern.endswith('.py'):
                pattern = f"{pattern}*.py"
            # If pattern doesn't start with the source folder, prepend it
            if not pattern.startswith(source_folder):
                pattern = os.path.join(source_folder, pattern)
            # Remove files matching the pattern
            exclude_matches = glob.glob(pattern, recursive=True)
            filtered_files = [f for f in filtered_files if f not in exclude_matches]
    
    LOGGER.info(f"Collected {len(filtered_files)} DAG files from {source_folder}")
    return filtered_files


def authenticate_to_gcp(env_config):
    """
    Authenticates to Google Cloud Platform using service account or application default credentials
    
    Args:
        env_config (dict): Environment configuration containing authentication details
        
    Returns:
        google.auth.credentials.Credentials: GCP credentials object
        
    Raises:
        Exception: If authentication fails
    """
    try:
        # Extract service account info from environment config
        gcp_config = env_config.get('gcp', {})
        security_config = env_config.get('security', {})
        
        project_id = gcp_config.get('project_id')
        service_account = security_config.get('service_account')
        
        # Authenticate using gcp_utils
        credentials = authenticate_gcp(
            project_id=project_id,
            service_account=service_account
        )
        
        LOGGER.info(f"Successfully authenticated to GCP project: {project_id}")
        return credentials
        
    except auth_exceptions.DefaultCredentialsError:
        LOGGER.error("Failed to authenticate: No valid credentials found")
        raise
    except Exception as e:
        LOGGER.error(f"Authentication failed: {str(e)}")
        raise


def upload_dag_file(file_path, gcs_client, bucket_name, base_folder, dry_run=False):
    """
    Uploads a single DAG file to GCS bucket
    
    Args:
        file_path (str): Path to the local file
        gcs_client (GCSClient): GCS client for upload
        bucket_name (str): Name of the GCS bucket
        base_folder (str): Base folder path in the bucket
        dry_run (bool): If True, simulate without uploading
        
    Returns:
        tuple: (bool, str) - Success status and message
    """
    # Get the file name without the source folder path
    file_name = os.path.basename(file_path)
    
    # Construct the target object name in the GCS bucket
    object_name = f"{base_folder}/{file_name}"
    
    try:
        if dry_run:
            LOGGER.info(f"[DRY RUN] Would upload {file_path} to gs://{bucket_name}/{object_name}")
            return True, f"[DRY RUN] Would upload to gs://{bucket_name}/{object_name}"
        
        # Upload the file using the GCS client
        gcs_path = gcs_client.upload_file(
            local_file_path=file_path,
            bucket_name=bucket_name,
            object_name=object_name
        )
        
        LOGGER.info(f"Successfully uploaded {file_path} to {gcs_path}")
        return True, f"Uploaded to {gcs_path}"
        
    except Exception as e:
        error_msg = f"Failed to upload {file_path}: {str(e)}"
        LOGGER.error(error_msg)
        return False, error_msg


def deploy_to_environment(dag_files, env_config, dry_run=False, parallel=False):
    """
    Deploys validated DAG files to the specified environment
    
    Args:
        dag_files (list): List of DAG file paths to deploy
        env_config (dict): Environment configuration
        dry_run (bool): If True, simulate without making changes
        parallel (bool): If True, upload files in parallel
        
    Returns:
        dict: Deployment results with success and failure counts
    """
    # Get the GCS bucket name from environment config
    storage_config = env_config.get('storage', {})
    bucket_name = storage_config.get('dag_bucket')
    
    if not bucket_name:
        error_msg = "No DAG bucket specified in environment configuration"
        LOGGER.error(error_msg)
        return {
            'success': False,
            'message': error_msg,
            'success_count': 0,
            'failure_count': len(dag_files)
        }
    
    # Create GCS client
    try:
        # Authenticate and create GCS client
        credentials = authenticate_to_gcp(env_config)
        gcs_client = GCSClient()
        
        # Set base folder for DAGs in GCS (usually 'dags')
        base_folder = 'dags'
        
        # Initialize counters
        success_count = 0
        failure_count = 0
        results = []
        
        # Start deployment
        LOGGER.info(f"Starting deployment of {len(dag_files)} DAG files to {bucket_name}")
        
        # Upload files in parallel or sequentially
        if parallel:
            LOGGER.info(f"Using parallel upload with {MAX_WORKERS} workers")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all upload tasks
                future_to_file = {
                    executor.submit(
                        upload_dag_file, file_path, gcs_client, bucket_name, base_folder, dry_run
                    ): file_path for file_path in dag_files
                }
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        success, message = future.result()
                        if success:
                            success_count += 1
                        else:
                            failure_count += 1
                        results.append({
                            'file': file_path,
                            'success': success,
                            'message': message
                        })
                    except Exception as e:
                        LOGGER.error(f"Error deploying {file_path}: {str(e)}")
                        failure_count += 1
                        results.append({
                            'file': file_path,
                            'success': False,
                            'message': str(e)
                        })
        else:
            # Upload files sequentially
            LOGGER.info("Using sequential upload")
            for file_path in dag_files:
                success, message = upload_dag_file(
                    file_path, gcs_client, bucket_name, base_folder, dry_run
                )
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                results.append({
                    'file': file_path,
                    'success': success,
                    'message': message
                })
        
        # Compile results
        deployment_success = failure_count == 0
        deployment_results = {
            'success': deployment_success,
            'message': f"Deployed {success_count} DAG files successfully, {failure_count} failed",
            'success_count': success_count,
            'failure_count': failure_count,
            'results': results
        }
        
        # Log summary
        LOGGER.info(f"Deployment completed: {success_count} successful, {failure_count} failed")
        
        return deployment_results
        
    except Exception as e:
        error_msg = f"Deployment failed: {str(e)}"
        LOGGER.error(error_msg)
        return {
            'success': False,
            'message': error_msg,
            'success_count': 0,
            'failure_count': len(dag_files)
        }


def handle_variables_and_connections(variables_file, connections_file, env_config, dry_run=False):
    """
    Imports Airflow variables and connections if specified
    
    Args:
        variables_file (str): Path to variables JSON file
        connections_file (str): Path to connections JSON file
        env_config (dict): Environment configuration
        dry_run (bool): If True, simulate without making changes
        
    Returns:
        dict: Results of variables and connections import
    """
    results = {
        'variables': {'processed': False, 'success': 0, 'skipped': 0, 'failed': 0},
        'connections': {'processed': False, 'success': 0, 'skipped': 0, 'failed': 0}
    }
    
    environment = env_config.get('environment', {}).get('name')
    
    # Handle variables if a file is provided
    if variables_file:
        try:
            LOGGER.info(f"Loading variables from {variables_file}")
            variables = load_variables_from_file(variables_file)
            
            # Process variables for target environment
            if environment:
                LOGGER.info(f"Processing variables for environment: {environment}")
                variables = process_environment_variables(variables, environment)
            
            # Import variables
            LOGGER.info(f"Importing {len(variables)} variables (dry_run={dry_run})")
            success, skipped, failed = import_variables(
                variables,
                dry_run=dry_run,
                skip_existing=True,
                force=False
            )
            
            results['variables'] = {
                'processed': True,
                'success': success,
                'skipped': skipped,
                'failed': failed
            }
            
            LOGGER.info(f"Variables import: {success} successful, {skipped} skipped, {failed} failed")
            
        except Exception as e:
            LOGGER.error(f"Failed to import variables: {str(e)}")
            results['variables']['error'] = str(e)
    
    # Handle connections if a file is provided
    if connections_file:
        try:
            LOGGER.info(f"Loading connections from {connections_file}")
            connections = load_connections_from_file(connections_file)
            
            # Process connections for target environment
            if environment:
                LOGGER.info(f"Processing connections for environment: {environment}")
                connections = process_environment_connections(connections, environment)
            
            # Import connections
            LOGGER.info(f"Importing {len(connections)} connections (dry_run={dry_run})")
            success, skipped, failed = import_connections(
                connections,
                dry_run=dry_run,
                skip_existing=True,
                force=False,
                validate=True
            )
            
            results['connections'] = {
                'processed': True,
                'success': success,
                'skipped': skipped,
                'failed': failed
            }
            
            LOGGER.info(f"Connections import: {success} successful, {skipped} skipped, {failed} failed")
            
        except Exception as e:
            LOGGER.error(f"Failed to import connections: {str(e)}")
            results['connections']['error'] = str(e)
    
    return results


def main():
    """
    Main entry point for the script
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logging(log_level)
    
    try:
        # Get environment configuration
        LOGGER.info(f"Deploying to environment: {args.environment}")
        env_config = get_environment_config(args.environment)
        
        # Validate source folder
        LOGGER.info(f"Validating source folder: {args.source_folder}")
        if not validate_source_folder(args.source_folder):
            LOGGER.error("Source folder validation failed")
            return 1
        
        # Collect DAG files
        LOGGER.info(f"Collecting DAG files from {args.source_folder}")
        dag_files = collect_dag_files(
            args.source_folder,
            args.include_patterns,
            args.exclude_patterns
        )
        
        if not dag_files:
            LOGGER.error("No DAG files found for deployment")
            return 1
        
        # Validate DAG files for Airflow 2.X compatibility
        LOGGER.info(f"Validating {len(dag_files)} DAG files for Airflow 2.X compatibility")
        validation_results = validate_dag_files(dag_files)
        
        if not validation_results['success']:
            LOGGER.error("DAG validation failed. Please fix the issues before deployment.")
            LOGGER.error(f"Errors: {validation_results['summary']['error_count']}, " +
                       f"Warnings: {validation_results['summary']['warning_count']}")
            return 1
        
        # Deploy validated DAGs to the environment
        LOGGER.info(f"Deploying {len(dag_files)} validated DAG files to {args.environment}")
        deployment_results = deploy_to_environment(
            dag_files,
            env_config,
            args.dry_run,
            args.parallel
        )
        
        # Handle variables and connections if specified
        if args.variables_file or args.connections_file:
            LOGGER.info("Processing variables and connections")
            import_results = handle_variables_and_connections(
                args.variables_file,
                args.connections_file,
                env_config,
                args.dry_run
            )
            
            # Add import results to deployment results
            deployment_results['imports'] = import_results
        
        # Determine exit code based on deployment results
        if deployment_results['success']:
            LOGGER.info("Deployment completed successfully")
            return 0
        else:
            LOGGER.error(f"Deployment failed: {deployment_results['message']}")
            return 1
            
    except Exception as e:
        LOGGER.error(f"An error occurred during deployment: {str(e)}")
        return 2


if __name__ == '__main__':
    sys.exit(main())