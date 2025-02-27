#!/usr/bin/env python3
"""
Script for automating the setup and configuration of Google Cloud Composer 2 environments with Airflow 2.X.
It handles the provisioning, configuration, and initialization of Composer environments for development,
QA, and production, facilitating migration from Airflow 1.10.15 to Airflow 2.X.
"""

import os
import sys
import argparse
import logging
import json
import time
import subprocess
from google.cloud.composer import environments_v1
from google.api_core.exceptions import NotFound, PermissionDenied, AlreadyExists

# Internal imports
from .import_variables import main as import_variables_main, process_environment_variables
from .import_connections import main as import_connections_main, process_environment_connections
from .validate_dags import validate_dag_files, create_airflow2_compatibility_report
from ..dags.utils.gcp_utils import gcs_upload_file, gcs_download_file, gcs_file_exists, get_secret

# Import environment configurations
from ..config.composer_dev import config as dev_config
from ..config.composer_qa import config as qa_config
from ..config.composer_prod import config as prod_config

# Set up logger
logger = logging.getLogger('airflow.scripts.setup_composer')

# Constants
SUPPORTED_ENVIRONMENTS = ['dev', 'qa', 'prod']
DEFAULT_REGION = 'us-central1'
DEFAULT_ZONE = 'us-central1-a'
CONFIG_MAP = {"dev": dev_config, "qa": qa_config, "prod": prod_config}


def setup_logging(log_level):
    """
    Configure logging with appropriate format and level
    
    Args:
        log_level: Desired logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Configured logger instance
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=numeric_level
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    logger.addHandler(console_handler)
    
    return logger


def parse_arguments():
    """
    Parse command-line arguments for the script
    
    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Setup and configure Google Cloud Composer 2 environments'
    )
    
    # Environment selection
    parser.add_argument(
        '--environment',
        choices=SUPPORTED_ENVIRONMENTS,
        required=True,
        help='Target environment (dev, qa, prod)'
    )
    
    # Operation type
    parser.add_argument(
        '--operation',
        choices=['create', 'update', 'delete', 'validate'],
        required=True,
        help='Operation to perform on the environment'
    )
    
    # Airflow version
    parser.add_argument(
        '--airflow-version',
        default='2.2.5',
        help='Airflow version to use (default: 2.2.5)'
    )
    
    # Region and zone
    parser.add_argument(
        '--region',
        default=DEFAULT_REGION,
        help=f'GCP region for the Composer environment (default: {DEFAULT_REGION})'
    )
    
    parser.add_argument(
        '--zone',
        default=DEFAULT_ZONE,
        help=f'GCP zone for the Composer environment (default: {DEFAULT_ZONE})'
    )
    
    # Variables and connections files
    parser.add_argument(
        '--variables-file',
        help='Path to Airflow variables JSON file (local or GCS)'
    )
    
    parser.add_argument(
        '--connections-file',
        help='Path to Airflow connections JSON file (local or GCS)'
    )
    
    # DAG validation options
    parser.add_argument(
        '--dag-directory',
        help='Directory containing DAG files to validate'
    )
    
    parser.add_argument(
        '--validation-report',
        help='Path to save DAG validation report (local or GCS)'
    )
    
    # Other options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate operations without making changes'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force destructive operations without confirmation'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    return parser.parse_args()


def get_environment_config(environment):
    """
    Load environment configuration based on specified environment
    
    Args:
        environment: The target environment (dev, qa, prod)
        
    Returns:
        Environment configuration dictionary
        
    Raises:
        ValueError: If environment is not supported or configuration is missing
    """
    if environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(f"Unsupported environment: {environment}. "
                         f"Must be one of: {', '.join(SUPPORTED_ENVIRONMENTS)}")
    
    # Get config from the config map
    if environment not in CONFIG_MAP:
        raise ValueError(f"Configuration not found for environment: {environment}")
    
    config = CONFIG_MAP[environment]
    logger.info(f"Loaded configuration for environment: {environment}")
    
    return config


def create_composer_environment(environment, region, config, dry_run=False):
    """
    Create a new Cloud Composer 2 environment
    
    Args:
        environment: Target environment name (dev, qa, prod)
        region: GCP region for the environment
        config: Environment configuration dictionary
        dry_run: If True, simulate without making changes
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Creating Cloud Composer 2 environment: {environment} in region {region}")
    
    # Build environment configuration from config dictionary
    composer_config = config.get('composer', {})
    environment_name = composer_config.get('environment_name', f"composer2-{environment}")
    
    # Build the parent path for the environment
    parent = f"projects/{config['gcp']['project_id']}/locations/{region}"
    
    # Create environment configuration
    env_config = {
        'name': f"{parent}/environments/{environment_name}",
        'config': {
            'node_count': composer_config.get('node_count', 3),
            'software_config': {
                'image_version': f"composer-2.0.0-airflow-{config['airflow']['version']}",
                'env_variables': composer_config.get('environment_variables', {}),
                'airflow_config_overrides': {
                    'core-dags_are_paused_at_creation': 'True',
                    'core-load_examples': str(config['airflow'].get('load_examples', False)),
                    'core-dag_concurrency': str(config['airflow'].get('dag_concurrency', 16)),
                    'core-parallelism': str(config['airflow'].get('parallelism', 32)),
                    'core-max_active_runs_per_dag': str(config['airflow'].get('max_active_runs_per_dag', 16)),
                    'core-default_timezone': config['airflow'].get('default_timezone', 'UTC'),
                    'webserver-default_ui_timezone': config['airflow'].get('default_ui_timezone', 'UTC'),
                    'email-email_backend': 'airflow.utils.email.send_email_smtp',
                    'email-smtp_host': 'smtp.gmail.com',
                    'email-smtp_user': 'airflow@example.com',
                    'email-smtp_port': '587',
                    'email-smtp_password': '{{ var.json.smtp_password }}',
                    'email-smtp_mail_from': config['airflow'].get('default_email_recipient', 'airflow@example.com'),
                },
                'python_version': '3',
            },
            'node_config': {
                'network': config['network'].get('network_name', f"composer2-{environment}-network"),
                'subnetwork': config['network'].get('subnetwork_name', f"composer2-{environment}-subnet"),
                'service_account': config['security'].get('service_account', f"composer2-{environment}-sa@{config['gcp']['project_id']}.iam.gserviceaccount.com"),
                'oauth_scopes': config['security'].get('oauth_scopes', ['https://www.googleapis.com/auth/cloud-platform']),
                'ip_allocation_policy': {
                    'cluster_secondary_range_name': 'pods',
                    'services_secondary_range_name': 'services',
                },
                'tags': ['composer', environment],
            },
            'private_environment_config': {
                'enable_private_endpoint': config['network'].get('enable_private_endpoint', False),
                'cloud_sql_ipv4_cidr_block': config['network'].get('cloud_sql_ipv4_cidr', '172.16.0.16/28'),
                'web_server_ipv4_cidr_block': config['network'].get('web_server_ipv4_cidr', '172.16.0.0/28'),
                'enable_private_builds': True,
            },
            'web_server_config': {
                'machine_type': composer_config.get('web_server_machine_type', 'composer-n1-standard-2'),
            },
            'database_config': {
                'machine_type': composer_config.get('cloud_sql_machine_type', 'db-n1-standard-2'),
            },
            'web_server_network_access_control': {
                'allowed_ip_ranges': [
                    {'value': ip, 'description': f'Allowed IP range {i+1}'} 
                    for i, ip in enumerate(config['security'].get('web_server_allow_ip_ranges', ['0.0.0.0/0']))
                ],
            },
            'environment_size': composer_config.get('environment_size', 'ENVIRONMENT_SIZE_SMALL'),
        }
    }
    
    # Add encryption key if specified
    if 'kms_key' in config['security'] and config['security']['kms_key']:
        env_config['config']['encryption_config'] = {
            'kms_key_name': config['security']['kms_key']
        }
    
    # Add recovery config if high availability is enabled
    if composer_config.get('enable_high_availability', False):
        env_config['config']['recovery_config'] = {
            'scheduled_snapshots_config': {
                'enabled': True,
                'snapshot_location': f"gs://{config['storage'].get('backup_bucket', f'composer2-{environment}-backup')}/snapshots",
                'snapshot_creation_schedule': "0 0 * * *",
                'time_zone': "UTC",
            }
        }
    
    # Add maintenance window if specified
    if 'maintenance_window_start_time' in composer_config and 'maintenance_window_end_time' in composer_config:
        env_config['config']['maintenance_window'] = {
            'start_time': composer_config['maintenance_window_start_time'],
            'end_time': composer_config['maintenance_window_end_time'],
            'recurrence': composer_config.get('maintenance_recurrence', 'FREQ=WEEKLY;BYDAY=SU'),
        }
    
    # If dry run, just log what would be done
    if dry_run:
        logger.info("DRY RUN: Would create Cloud Composer environment with the following configuration:")
        logger.info(json.dumps(env_config, indent=2))
        return True
    
    # Initialize Cloud Composer client
    try:
        client = environments_v1.EnvironmentsClient()
        
        # Check if environment already exists
        try:
            env_name = f"{parent}/environments/{environment_name}"
            client.get_environment(name=env_name)
            logger.error(f"Environment {environment_name} already exists in {region}")
            return False
        except NotFound:
            # Environment doesn't exist, which is what we want for create
            pass
        
        # Create the environment
        logger.info(f"Creating environment {environment_name} in {region}...")
        operation = client.create_environment(
            parent=parent,
            environment=env_config
        )
        
        # Get operation name for logging
        operation_name = operation.operation.name
        logger.info(f"Environment creation started. Operation: {operation_name}")
        
        # Wait for operation to complete (this could take an hour or more)
        result = operation.result()
        
        logger.info(f"Environment {environment_name} created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create environment: {str(e)}")
        return False


def update_composer_environment(environment, region, config, dry_run=False):
    """
    Update an existing Cloud Composer 2 environment
    
    Args:
        environment: Target environment name (dev, qa, prod)
        region: GCP region for the environment
        config: Environment configuration dictionary  
        dry_run: If True, simulate without making changes
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Updating Cloud Composer 2 environment: {environment} in region {region}")
    
    # Build environment configuration from config dictionary
    composer_config = config.get('composer', {})
    environment_name = composer_config.get('environment_name', f"composer2-{environment}")
    
    # Build the environment path
    env_path = f"projects/{config['gcp']['project_id']}/locations/{region}/environments/{environment_name}"
    
    try:
        # Initialize Cloud Composer client
        client = environments_v1.EnvironmentsClient()
        
        # Get current environment configuration
        try:
            current_env = client.get_environment(name=env_path)
            logger.info(f"Retrieved current configuration for environment {environment_name}")
        except NotFound:
            logger.error(f"Environment {environment_name} not found in {region}")
            return False
        
        # Build update mask and environment object
        update_environment = {
            'name': env_path,
            'config': {
                'software_config': {
                    'env_variables': composer_config.get('environment_variables', {}),
                    'airflow_config_overrides': {
                        'core-dags_are_paused_at_creation': 'True',
                        'core-load_examples': str(config['airflow'].get('load_examples', False)),
                        'core-dag_concurrency': str(config['airflow'].get('dag_concurrency', 16)),
                        'core-parallelism': str(config['airflow'].get('parallelism', 32)),
                        'core-max_active_runs_per_dag': str(config['airflow'].get('max_active_runs_per_dag', 16)),
                        'core-default_timezone': config['airflow'].get('default_timezone', 'UTC'),
                        'webserver-default_ui_timezone': config['airflow'].get('default_ui_timezone', 'UTC'),
                    },
                },
                'web_server_network_access_control': {
                    'allowed_ip_ranges': [
                        {'value': ip, 'description': f'Allowed IP range {i+1}'} 
                        for i, ip in enumerate(config['security'].get('web_server_allow_ip_ranges', ['0.0.0.0/0']))
                    ],
                },
            }
        }
        
        # Define the update mask
        update_mask = "config.software_config.env_variables,config.software_config.airflow_config_overrides,config.web_server_network_access_control.allowed_ip_ranges"
        
        # If node count is specified, update it
        if 'node_count' in composer_config:
            update_environment['config']['node_count'] = composer_config['node_count']
            update_mask += ",config.node_count"
        
        # If worker configuration is specified, update it if supported
        if 'worker' in composer_config and 'min_count' in composer_config['worker'] and 'max_count' in composer_config['worker']:
            # For Composer 2, this would be set through autoscaling policy
            # This is a simplification for the purpose of this script
            logger.warning("Worker count updates may require recreating the environment in Composer 2")
        
        # If dry run, just log what would be done
        if dry_run:
            logger.info("DRY RUN: Would update Cloud Composer environment with the following configuration:")
            logger.info(json.dumps(update_environment, indent=2))
            logger.info(f"Update mask: {update_mask}")
            return True
        
        # Perform the update
        logger.info(f"Updating environment {environment_name} in {region}...")
        operation = client.update_environment(
            request={
                'name': env_path,
                'environment': update_environment,
                'update_mask': update_mask
            }
        )
        
        # Get operation name for logging
        operation_name = operation.operation.name
        logger.info(f"Environment update started. Operation: {operation_name}")
        
        # Wait for operation to complete
        result = operation.result()
        
        logger.info(f"Environment {environment_name} updated successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update environment: {str(e)}")
        return False


def delete_composer_environment(environment, region, force=False, dry_run=False):
    """
    Delete an existing Cloud Composer 2 environment
    
    Args:
        environment: Target environment name (dev, qa, prod)
        region: GCP region for the environment
        force: If True, delete without confirmation
        dry_run: If True, simulate without making changes
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Deleting Cloud Composer 2 environment: {environment} in region {region}")
    
    # Get environment name from environment
    environment_name = f"composer2-{environment}"
    
    # Build the environment path
    env_path = f"projects/{CONFIG_MAP[environment]['gcp']['project_id']}/locations/{region}/environments/{environment_name}"
    
    try:
        # Initialize Cloud Composer client
        client = environments_v1.EnvironmentsClient()
        
        # Verify that the environment exists
        try:
            client.get_environment(name=env_path)
            logger.info(f"Found environment {environment_name} in {region}")
        except NotFound:
            logger.error(f"Environment {environment_name} not found in {region}")
            return False
        
        # If not force, prompt for confirmation
        if not force and not dry_run:
            confirmation = input(f"Are you sure you want to delete environment {environment_name} in {region}? This action cannot be undone. (y/n): ")
            if confirmation.lower() != 'y':
                logger.info("Deletion cancelled by user")
                return False
        
        # If dry run, just log what would be done
        if dry_run:
            logger.info(f"DRY RUN: Would delete Cloud Composer environment {environment_name} in {region}")
            return True
        
        # Delete the environment
        logger.info(f"Deleting environment {environment_name} in {region}...")
        operation = client.delete_environment(name=env_path)
        
        # Get operation name for logging
        operation_name = operation.operation.name
        logger.info(f"Environment deletion started. Operation: {operation_name}")
        
        # Wait for operation to complete
        result = operation.result()
        
        logger.info(f"Environment {environment_name} deleted successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to delete environment: {str(e)}")
        return False


def validate_composer_environment(environment, config):
    """
    Validate a Cloud Composer 2 environment configuration
    
    Args:
        environment: Target environment name (dev, qa, prod)
        config: Environment configuration dictionary
        
    Returns:
        Dictionary with validation results and any issues found
    """
    results = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    # Check for required sections
    required_sections = ['environment', 'gcp', 'airflow', 'composer', 'network', 'security']
    for section in required_sections:
        if section not in config:
            results['valid'] = False
            results['errors'].append(f"Missing required configuration section: {section}")
    
    # If any required sections are missing, return early
    if not results['valid']:
        return results
    
    # Network validation
    network_config = config.get('network', {})
    if network_config.get('use_public_ips', False) and network_config.get('enable_private_endpoint', False):
        results['warnings'].append(
            "Both use_public_ips and enable_private_endpoint are set to True. "
            "This might lead to unexpected behavior."
        )
    
    # Airflow settings validation
    airflow_config = config.get('airflow', {})
    if airflow_config.get('load_examples', False) and environment != 'dev':
        results['warnings'].append(
            f"Airflow example DAGs are enabled in {environment} environment. "
            "Consider disabling for non-dev environments."
        )
    
    # Resource allocation validation
    composer_config = config.get('composer', {})
    if environment == 'prod':
        # Ensure HA is enabled for prod
        if not composer_config.get('enable_high_availability', False):
            results['warnings'].append(
                "High availability is not enabled for production environment."
            )
        
        # Ensure minimum number of workers for prod
        min_workers = composer_config.get('worker', {}).get('min_count', 0)
        if min_workers < 3:
            results['warnings'].append(
                f"Production environment has only {min_workers} minimum workers. "
                "Consider setting at least 3 for reliability."
            )
    
    # Log validation results
    if results['errors']:
        logger.error(f"Configuration validation failed with {len(results['errors'])} errors")
        for error in results['errors']:
            logger.error(f"Validation error: {error}")
    
    if results['warnings']:
        logger.warning(f"Configuration validation has {len(results['warnings'])} warnings")
        for warning in results['warnings']:
            logger.warning(f"Validation warning: {warning}")
    
    return results


def setup_composer_environment(environment, variables_file=None, connections_file=None, dry_run=False):
    """
    Set up a Cloud Composer 2 environment with configurations, variables and connections
    
    Args:
        environment: Target environment name (dev, qa, prod)
        variables_file: Path to variables JSON file (local or GCS)
        connections_file: Path to connections JSON file (local or GCS)
        dry_run: If True, simulate without making changes
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Setting up Cloud Composer 2 environment for {environment}")
    
    # Get environment configuration
    config = get_environment_config(environment)
    region = config['gcp']['region']
    
    # Validate environment configuration
    validation_results = validate_composer_environment(environment, config)
    if not validation_results['valid']:
        logger.error("Environment configuration validation failed")
        return False
    
    # Create or update environment
    environment_name = config['composer']['environment_name']
    
    # Check if environment exists
    client = environments_v1.EnvironmentsClient()
    env_path = f"projects/{config['gcp']['project_id']}/locations/{region}/environments/{environment_name}"
    try:
        client.get_environment(name=env_path)
        logger.info(f"Environment {environment_name} exists, updating...")
        success = update_composer_environment(environment, region, config, dry_run)
    except NotFound:
        logger.info(f"Environment {environment_name} does not exist, creating...")
        success = create_composer_environment(environment, region, config, dry_run)
    
    if not success:
        logger.error("Failed to setup environment")
        return False
    
    # Wait for environment to be ready
    if not dry_run:
        if not wait_for_environment_ready(environment, region):
            logger.error("Environment did not become ready within the timeout period")
            return False
    
    # Import variables if specified
    if variables_file:
        if not import_environment_variables(environment, variables_file, dry_run):
            logger.error("Failed to import variables")
            return False
    
    # Import connections if specified
    if connections_file:
        if not import_environment_connections(environment, connections_file, dry_run):
            logger.error("Failed to import connections")
            return False
    
    logger.info(f"Environment {environment_name} setup completed successfully")
    return True


def wait_for_environment_ready(environment, region, timeout_minutes=30):
    """
    Wait for a Cloud Composer environment to become fully operational
    
    Args:
        environment: Target environment name (dev, qa, prod)
        region: GCP region for the environment
        timeout_minutes: Maximum time to wait in minutes
        
    Returns:
        True if environment is ready, False if timeout or error
    """
    logger.info(f"Waiting for environment {environment} to be ready...")
    
    # Get environment name and project from environment config
    config = get_environment_config(environment)
    environment_name = config['composer']['environment_name']
    project_id = config['gcp']['project_id']
    
    # Initialize Cloud Composer client
    client = environments_v1.EnvironmentsClient()
    
    # Build the environment path
    env_path = f"projects/{project_id}/locations/{region}/environments/{environment_name}"
    
    # Calculate timeout
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()
    
    while True:
        # Check if we've exceeded the timeout
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout_seconds:
            logger.error(f"Timeout ({timeout_minutes} minutes) exceeded waiting for environment to be ready")
            return False
        
        try:
            # Get the current environment state
            environment = client.get_environment(name=env_path)
            state = environment.state
            
            if state.name == 'RUNNING':
                logger.info(f"Environment {environment_name} is ready (state: RUNNING)")
                return True
            
            logger.info(f"Environment {environment_name} is not ready yet (state: {state.name}). Waiting...")
            
            # Sleep for a while before checking again
            time.sleep(60)  # Check every minute
            
        except Exception as e:
            logger.warning(f"Error checking environment state: {str(e)}. Retrying...")
            time.sleep(30)  # Shorter sleep on error


def import_environment_variables(environment, variables_file, dry_run=False):
    """
    Import Airflow variables into the Composer environment
    
    Args:
        environment: Target environment name (dev, qa, prod)
        variables_file: Path to variables JSON file (local or GCS)
        dry_run: If True, simulate without making changes
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Importing variables from {variables_file} into {environment} environment")
    
    # Check if variables file exists
    if variables_file.startswith('gs://'):
        # GCS path
        bucket_name = variables_file.split('gs://')[1].split('/')[0]
        object_name = '/'.join(variables_file.split('gs://')[1].split('/')[1:])
        
        if not gcs_file_exists(bucket_name, object_name):
            logger.error(f"Variables file not found in GCS: {variables_file}")
            return False
        
        # Download to temp file
        import tempfile
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        local_path = temp_file.name
        temp_file.close()
        
        try:
            gcs_download_file(bucket_name, object_name, local_path)
            logger.info(f"Downloaded variables file from GCS to {local_path}")
            
            # Process variables for target environment
            with open(local_path, 'r') as f:
                variables = json.load(f)
            
            variables = process_environment_variables(variables, environment)
            
            # Write processed variables back to temp file
            with open(local_path, 'w') as f:
                json.dump(variables, f)
            
            # Import variables
            args = ['--file-path', local_path, '--environment', environment]
            if dry_run:
                args.append('--dry-run')
            
            result = import_variables_main(args)
            
            # Clean up temp file
            os.unlink(local_path)
            
            return result == 0
            
        except Exception as e:
            logger.error(f"Error importing variables: {str(e)}")
            # Clean up temp file
            if os.path.exists(local_path):
                os.unlink(local_path)
            return False
    else:
        # Local file
        if not os.path.exists(variables_file):
            logger.error(f"Variables file not found: {variables_file}")
            return False
        
        try:
            # Process variables for target environment
            with open(variables_file, 'r') as f:
                variables = json.load(f)
            
            variables = process_environment_variables(variables, environment)
            
            # Create a temp file for processed variables
            import tempfile
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            temp_path = temp_file.name
            temp_file.close()
            
            with open(temp_path, 'w') as f:
                json.dump(variables, f)
            
            # Import variables
            args = ['--file-path', temp_path, '--environment', environment]
            if dry_run:
                args.append('--dry-run')
            
            result = import_variables_main(args)
            
            # Clean up temp file
            os.unlink(temp_path)
            
            return result == 0
            
        except Exception as e:
            logger.error(f"Error importing variables: {str(e)}")
            return False


def import_environment_connections(environment, connections_file, dry_run=False):
    """
    Import Airflow connections into the Composer environment
    
    Args:
        environment: Target environment name (dev, qa, prod)
        connections_file: Path to connections JSON file (local or GCS)
        dry_run: If True, simulate without making changes
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Importing connections from {connections_file} into {environment} environment")
    
    # Check if connections file exists
    if connections_file.startswith('gs://'):
        # GCS path
        bucket_name = connections_file.split('gs://')[1].split('/')[0]
        object_name = '/'.join(connections_file.split('gs://')[1].split('/')[1:])
        
        if not gcs_file_exists(bucket_name, object_name):
            logger.error(f"Connections file not found in GCS: {connections_file}")
            return False
        
        # Download to temp file
        import tempfile
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        local_path = temp_file.name
        temp_file.close()
        
        try:
            gcs_download_file(bucket_name, object_name, local_path)
            logger.info(f"Downloaded connections file from GCS to {local_path}")
            
            # Process connections for target environment
            with open(local_path, 'r') as f:
                connections = json.load(f)
            
            connections = process_environment_connections(connections, environment)
            
            # Write processed connections back to temp file
            with open(local_path, 'w') as f:
                json.dump(connections, f)
            
            # Import connections
            args = ['--file-path', local_path, '--environment', environment]
            if dry_run:
                args.append('--dry-run')
            
            result = import_connections_main(args)
            
            # Clean up temp file
            os.unlink(local_path)
            
            return result == 0
            
        except Exception as e:
            logger.error(f"Error importing connections: {str(e)}")
            # Clean up temp file
            if os.path.exists(local_path):
                os.unlink(local_path)
            return False
    else:
        # Local file
        if not os.path.exists(connections_file):
            logger.error(f"Connections file not found: {connections_file}")
            return False
        
        try:
            # Process connections for target environment
            with open(connections_file, 'r') as f:
                connections = json.load(f)
            
            connections = process_environment_connections(connections, environment)
            
            # Create a temp file for processed connections
            import tempfile
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            temp_path = temp_file.name
            temp_file.close()
            
            with open(temp_path, 'w') as f:
                json.dump(connections, f)
            
            # Import connections
            args = ['--file-path', temp_path, '--environment', environment]
            if dry_run:
                args.append('--dry-run')
            
            result = import_connections_main(args)
            
            # Clean up temp file
            os.unlink(temp_path)
            
            return result == 0
            
        except Exception as e:
            logger.error(f"Error importing connections: {str(e)}")
            return False


def validate_environment_dags(dag_directory, validation_report=None):
    """
    Validate DAGs for compatibility with Airflow 2.X
    
    Args:
        dag_directory: Directory containing DAG files to validate
        validation_report: Path to save validation report (local or GCS)
        
    Returns:
        True if all DAGs are compatible, False otherwise
    """
    logger.info(f"Validating DAGs in {dag_directory} for Airflow 2.X compatibility")
    
    # Check if directory exists
    if not os.path.isdir(dag_directory):
        logger.error(f"DAG directory not found: {dag_directory}")
        return False
    
    try:
        # Generate validation report
        result = create_airflow2_compatibility_report(
            dag_directory=dag_directory,
            output_file=validation_report,
            output_format='json' if validation_report and validation_report.endswith('.json') else 'html',
            validation_level='WARNING'
        )
        
        if result:
            logger.info(f"DAG validation completed successfully. Report saved to {validation_report}")
            return True
        else:
            logger.error("DAG validation failed")
            return False
            
    except Exception as e:
        logger.error(f"Error during DAG validation: {str(e)}")
        return False


class ComposerEnvironmentManager:
    """Manager class for Cloud Composer 2 environments"""
    
    def __init__(self, environment, region=None):
        """
        Initialize ComposerEnvironmentManager
        
        Args:
            environment: Target environment name (dev, qa, prod)
            region: GCP region for the environment (optional, will use config if not provided)
        """
        self.environment = environment
        
        # Load environment configuration
        self.config = get_environment_config(environment)
        
        # Set region from config if not provided
        self.region = region or self.config['gcp']['region']
        
        # Initialize client lazily
        self.client = None
    
    def create(self, dry_run=False):
        """
        Create a new Composer environment
        
        Args:
            dry_run: If True, simulate without making changes
            
        Returns:
            True if successful, False otherwise
        """
        return create_composer_environment(self.environment, self.region, self.config, dry_run)
    
    def update(self, dry_run=False):
        """
        Update an existing Composer environment
        
        Args:
            dry_run: If True, simulate without making changes
            
        Returns:
            True if successful, False otherwise
        """
        return update_composer_environment(self.environment, self.region, self.config, dry_run)
    
    def delete(self, force=False, dry_run=False):
        """
        Delete an existing Composer environment
        
        Args:
            force: If True, delete without confirmation
            dry_run: If True, simulate without making changes
            
        Returns:
            True if successful, False otherwise
        """
        return delete_composer_environment(self.environment, self.region, force, dry_run)
    
    def validate(self):
        """
        Validate environment configuration
        
        Returns:
            True if valid, False otherwise
        """
        results = validate_composer_environment(self.environment, self.config)
        return results['valid']
    
    def wait_until_ready(self, timeout_minutes=30):
        """
        Wait for environment to be fully operational
        
        Args:
            timeout_minutes: Maximum time to wait in minutes
            
        Returns:
            True if environment is ready, False if timeout or error
        """
        return wait_for_environment_ready(self.environment, self.region, timeout_minutes)
    
    def import_variables(self, variables_file, dry_run=False):
        """
        Import variables to the environment
        
        Args:
            variables_file: Path to variables JSON file (local or GCS)
            dry_run: If True, simulate without making changes
            
        Returns:
            True if successful, False otherwise
        """
        return import_environment_variables(self.environment, variables_file, dry_run)
    
    def import_connections(self, connections_file, dry_run=False):
        """
        Import connections to the environment
        
        Args:
            connections_file: Path to connections JSON file (local or GCS)
            dry_run: If True, simulate without making changes
            
        Returns:
            True if successful, False otherwise
        """
        return import_environment_connections(self.environment, connections_file, dry_run)
    
    def validate_dags(self, dag_directory, validation_report=None):
        """
        Validate DAGs for Airflow 2.X compatibility
        
        Args:
            dag_directory: Directory containing DAG files to validate
            validation_report: Path to save validation report (local or GCS)
            
        Returns:
            True if all DAGs are compatible, False otherwise
        """
        return validate_environment_dags(dag_directory, validation_report)


def main(args=None):
    """
    Main function to coordinate the Composer setup process
    
    Args:
        args: Command-line arguments (defaults to sys.argv[1:])
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Parse arguments if not provided
    if args is None:
        args = parse_arguments()
    
    # Set up logging
    logger = setup_logging(args.log_level)
    
    try:
        # Validate environment
        if args.environment not in SUPPORTED_ENVIRONMENTS:
            logger.error(f"Unsupported environment: {args.environment}")
            return 1
        
        logger.info(f"Operation: {args.operation} on environment: {args.environment}")
        
        # Create environment manager
        manager = ComposerEnvironmentManager(args.environment, args.region)
        
        # Determine operation
        if args.operation == 'create':
            # Create new environment
            if not manager.create(args.dry_run):
                logger.error("Failed to create environment")
                return 1
            
            # Wait for environment to be ready if not dry run
            if not args.dry_run and not manager.wait_until_ready():
                logger.error("Environment did not become ready in the expected timeframe")
                return 1
            
            # Import variables if specified
            if args.variables_file:
                if not manager.import_variables(args.variables_file, args.dry_run):
                    logger.error("Failed to import variables")
                    return 1
            
            # Import connections if specified
            if args.connections_file:
                if not manager.import_connections(args.connections_file, args.dry_run):
                    logger.error("Failed to import connections")
                    return 1
            
            logger.info("Environment creation and setup completed successfully")
            
        elif args.operation == 'update':
            # Update existing environment
            if not manager.update(args.dry_run):
                logger.error("Failed to update environment")
                return 1
            
            # Wait for environment to be ready if not dry run
            if not args.dry_run and not manager.wait_until_ready():
                logger.error("Environment did not become ready after update")
                return 1
            
            # Import variables if specified
            if args.variables_file:
                if not manager.import_variables(args.variables_file, args.dry_run):
                    logger.error("Failed to import variables")
                    return 1
            
            # Import connections if specified
            if args.connections_file:
                if not manager.import_connections(args.connections_file, args.dry_run):
                    logger.error("Failed to import connections")
                    return 1
            
            logger.info("Environment update completed successfully")
            
        elif args.operation == 'delete':
            # Delete environment
            if not manager.delete(args.force, args.dry_run):
                logger.error("Failed to delete environment")
                return 1
            
            logger.info("Environment deletion completed successfully")
            
        elif args.operation == 'validate':
            # Validate environment configuration
            if not manager.validate():
                logger.error("Environment configuration validation failed")
                return 1
            
            logger.info("Environment configuration validation passed")
            
            # Validate DAGs if specified
            if args.dag_directory:
                if not manager.validate_dags(args.dag_directory, args.validation_report):
                    logger.error("DAG validation failed")
                    return 1
                
                logger.info("DAG validation completed successfully")
        
        # Success
        return 0
        
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())