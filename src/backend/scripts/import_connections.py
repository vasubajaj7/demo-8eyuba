#!/usr/bin/env python
"""
Utility script for importing Apache Airflow connection definitions from JSON configuration files
into Cloud Composer 2 environments.

This script facilitates migration from Airflow 1.10.15 to Airflow 2.X by handling version-specific
differences in connection management. It provides flexible options for importing from local files
or GCS buckets with environment-specific connection processing and secure credential handling.
"""

import os
import sys
import json
import argparse
import logging
import tempfile
import importlib
import re
from typing import List, Dict, Tuple, Any, Optional

# Import utility functions from the internal gcp_utils module
from ..dags.utils.gcp_utils import (
    gcs_file_exists,
    gcs_download_file,
    get_secret
)

# Set up logger
logger = logging.getLogger(__name__)

# Supported environments
SUPPORTED_ENVIRONMENTS = ['dev', 'qa', 'prod']

# Module paths for different Airflow versions
AIRFLOW1_CONNECTION_MODULE = 'airflow.models.Connection'
AIRFLOW2_CONNECTION_MODULE = 'airflow.models.connection.Connection'

# Regular expression pattern for secret references
SECRET_PATTERN = r'\{SECRET:([^:]+):([^}]+)\}'


def setup_logging(log_level: str) -> logging.Logger:
    """
    Configure logging with appropriate format and level.
    
    Args:
        log_level: Desired log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Configured logger instance
    """
    # Convert string log level to logging constant if needed
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    # Configure basic logging
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=numeric_level
    )
    
    # Add console handler
    console = logging.StreamHandler()
    console.setLevel(numeric_level)
    console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger = logging.getLogger(__name__)
    logger.addHandler(console)
    
    return logger


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the script.
    
    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Import Airflow connections from JSON file to Cloud Composer environment'
    )
    
    # Source arguments - mutually exclusive
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        '--file-path',
        help='Path to local JSON file containing connection definitions'
    )
    source_group.add_argument(
        '--gcs-path',
        help='GCS path (gs://bucket-name/path/to/file.json) to JSON file containing connection definitions'
    )
    
    # Target environment
    parser.add_argument(
        '--environment',
        choices=SUPPORTED_ENVIRONMENTS,
        help='Target environment (dev, qa, prod) for environment-specific processing'
    )
    
    # Import options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate the import without making actual changes'
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Skip connections that already exist'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force update of existing connections'
    )
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate connection configurations before importing'
    )
    
    # Filter options
    parser.add_argument(
        '--conn-id',
        help='Import only the specified connection ID'
    )
    
    # Authentication
    parser.add_argument(
        '--gcp-conn-id',
        default='google_cloud_default',
        help='Airflow connection ID for GCP authentication'
    )
    
    # Logging options
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level'
    )
    
    return parser.parse_args()


def detect_airflow_version() -> Tuple[int, int, int]:
    """
    Detect the installed Airflow version.
    
    Returns:
        Major, minor, patch version numbers as a tuple
    """
    try:
        import airflow
        version_str = getattr(airflow, '__version__', '0.0.0')
        
        # Parse version string (x.y.z)
        match = re.match(r'^(\d+)\.(\d+)\.(\d+)', version_str)
        if match:
            major, minor, patch = map(int, match.groups())
            logger.debug(f"Detected Airflow version: {major}.{minor}.{patch}")
            return major, minor, patch
        else:
            logger.warning(f"Could not parse Airflow version: {version_str}, assuming 1.0.0")
            return 1, 0, 0
    except ImportError:
        logger.error("Airflow not installed or not found in PYTHONPATH")
        return 0, 0, 0


def get_connection_class():
    """
    Get the appropriate Connection class based on Airflow version.
    
    Returns:
        Airflow Connection class
    """
    major, _, _ = detect_airflow_version()
    
    try:
        if major >= 2:
            # Airflow 2.X
            module_name = AIRFLOW2_CONNECTION_MODULE
        else:
            # Airflow 1.X
            module_name = AIRFLOW1_CONNECTION_MODULE
            
        # Import the module and get the class
        module_path, class_name = module_name.rsplit('.', 1)
        module = importlib.import_module(module_path)
        connection_class = getattr(module, class_name)
        
        logger.debug(f"Using Connection class from {module_name}")
        return connection_class
    except (ImportError, AttributeError) as e:
        logger.error(f"Error loading Airflow Connection class: {str(e)}")
        raise ImportError(f"Could not import Airflow Connection class: {str(e)}")


def load_connections_from_file(file_path: str) -> List[Dict]:
    """
    Load connections from a local JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        List of connection definitions
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        with open(file_path, 'r') as f:
            connections = json.load(f)
            
        if not isinstance(connections, list):
            connections = [connections]
            
        logger.info(f"Loaded {len(connections)} connection(s) from {file_path}")
        return connections
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in {file_path}: {str(e)}")
        raise


def load_connections_from_gcs(gcs_path: str, conn_id: str = None) -> List[Dict]:
    """
    Load connections from a GCS JSON file.
    
    Args:
        gcs_path: GCS path (gs://bucket-name/path/to/file.json)
        conn_id: GCP connection ID for authentication
        
    Returns:
        List of connection definitions
    """
    try:
        # Parse GCS path
        if not gcs_path.startswith('gs://'):
            raise ValueError(f"Invalid GCS path: {gcs_path}. Path must start with 'gs://'")
            
        path_parts = gcs_path[5:].split('/', 1)
        if len(path_parts) != 2:
            raise ValueError(f"Invalid GCS path format: {gcs_path}")
            
        bucket_name, object_name = path_parts
        
        # Check if file exists
        if not gcs_file_exists(bucket_name, object_name, conn_id):
            raise FileNotFoundError(f"File not found in GCS: {gcs_path}")
            
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            # Download file from GCS
            gcs_download_file(bucket_name, object_name, temp_path, conn_id)
            
            # Load connections from the downloaded file
            connections = load_connections_from_file(temp_path)
            
            return connections
        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
    except Exception as e:
        logger.error(f"Error loading connections from GCS: {str(e)}")
        raise


def process_environment_connections(connections: List[Dict], environment: str) -> List[Dict]:
    """
    Process connections based on target environment.
    
    Args:
        connections: List of connection definitions
        environment: Target environment (dev, qa, prod)
        
    Returns:
        Processed connections for the target environment
    """
    if not environment or environment not in SUPPORTED_ENVIRONMENTS:
        logger.warning(f"Invalid environment: {environment}. Must be one of {SUPPORTED_ENVIRONMENTS}")
        return connections
        
    processed_connections = []
    
    for conn in connections:
        # Create a copy of the connection to avoid modifying the original
        conn_copy = conn.copy()
        
        # Check if connection has environment restrictions
        if 'environments' in conn_copy:
            # Skip if connection is not meant for this environment
            if environment not in conn_copy['environments']:
                logger.debug(f"Skipping connection {conn_copy.get('conn_id')} - not for {environment}")
                continue
            # Remove environments field from the copy
            del conn_copy['environments']
            
        # Process environment-specific overrides
        env_section = f"{environment}_config"
        if env_section in conn_copy:
            # Apply environment-specific overrides
            for key, value in conn_copy[env_section].items():
                conn_copy[key] = value
            # Remove the environment section
            del conn_copy[env_section]
            
        # Process any environment placeholders in string values
        for key, value in conn_copy.items():
            if isinstance(value, str):
                # Replace {ENV} with the actual environment
                conn_copy[key] = value.replace('{ENV}', environment)
                
        processed_connections.append(conn_copy)
        
    logger.info(f"Processed {len(processed_connections)} connection(s) for environment: {environment}")
    return processed_connections


def resolve_secrets(connections: List[Dict], conn_id: str = None) -> List[Dict]:
    """
    Resolve secret references in connection definitions.
    
    Args:
        connections: List of connection definitions
        conn_id: GCP connection ID for Secret Manager
        
    Returns:
        Connections with secrets resolved
    """
    secret_fields = ['password', 'extra', 'auth_token', 'key_path', 'key_secret']
    resolved_connections = []
    secrets_found = 0
    
    for conn in connections:
        # Create a copy to avoid modifying the original
        conn_copy = conn.copy()
        
        # Check each field that might contain secrets
        for field in secret_fields:
            if field in conn_copy and isinstance(conn_copy[field], str):
                value = conn_copy[field]
                
                # Look for secret references
                matches = re.finditer(SECRET_PATTERN, value)
                for match in matches:
                    secret_id = match.group(1)
                    version = match.group(2)
                    
                    try:
                        # Retrieve the secret
                        secret_value = get_secret(secret_id, version, conn_id)
                        
                        # Replace the secret reference with the actual value
                        placeholder = match.group(0)  # The full match {SECRET:id:version}
                        conn_copy[field] = conn_copy[field].replace(placeholder, secret_value)
                        
                        secrets_found += 1
                        logger.debug(f"Resolved secret reference for {secret_id}")
                    except Exception as e:
                        logger.error(f"Failed to resolve secret {secret_id}: {str(e)}")
        
        resolved_connections.append(conn_copy)
        
    if secrets_found > 0:
        logger.info(f"Resolved {secrets_found} secret reference(s)")
        
    return resolved_connections


def validate_connection(connection: Dict) -> bool:
    """
    Validate a connection configuration.
    
    Args:
        connection: Connection definition
        
    Returns:
        True if valid, False otherwise
    """
    # Check required fields
    if not connection.get('conn_id'):
        logger.error("Missing required field: conn_id")
        return False
        
    if not connection.get('conn_type'):
        logger.error(f"Missing required field: conn_type for {connection.get('conn_id')}")
        return False
        
    # Validate conn_id format
    conn_id = connection['conn_id']
    if re.search(r'[^a-zA-Z0-9_]', conn_id):
        logger.error(f"Invalid conn_id format: {conn_id}. Use only letters, numbers, and underscores.")
        return False
        
    # Validate conn_type and type-specific required fields
    conn_type = connection['conn_type']
    
    # Check type-specific requirements
    if conn_type == 'postgres':
        required = ['host', 'schema', 'login']
    elif conn_type == 'google_cloud_platform':
        # For GCP connections, we need either key_path or key_json in extra
        if 'extra' not in connection:
            logger.error(f"GCP connection {conn_id} requires 'extra' field with credentials")
            return False
    elif conn_type == 'http':
        required = ['host']
    elif conn_type == 'aws':
        required = ['login', 'password']
    else:
        # Other connection types might have different requirements
        required = []
        
    # Check for required fields based on conn_type
    for field in required:
        if field not in connection or not connection[field]:
            logger.error(f"Connection {conn_id} ({conn_type}) missing required field: {field}")
            return False
            
    return True


def import_connections(connections: List[Dict], dry_run: bool = False, 
                      skip_existing: bool = False, force: bool = False,
                      validate: bool = True, filter_conn_id: str = None) -> Tuple[int, int, int]:
    """
    Import connections into Airflow.
    
    Args:
        connections: List of connection definitions
        dry_run: Simulate the import without making actual changes
        skip_existing: Skip connections that already exist
        force: Force update of existing connections
        validate: Validate connections before importing
        filter_conn_id: Import only the specified connection ID
        
    Returns:
        Counters for success, skipped, and failed connections
    """
    # Get the appropriate Connection class
    Connection = get_connection_class()
    
    # Initialize counters
    success_count = 0
    skipped_count = 0
    failed_count = 0
    
    # Filter connections if specified
    if filter_conn_id:
        original_count = len(connections)
        connections = [c for c in connections if c.get('conn_id') == filter_conn_id]
        logger.info(f"Filtered {original_count} connection(s) to {len(connections)} with conn_id: {filter_conn_id}")
    
    for conn_def in connections:
        conn_id = conn_def.get('conn_id')
        
        try:
            # Validate connection if required
            if validate and not validate_connection(conn_def):
                logger.error(f"Validation failed for connection: {conn_id}")
                failed_count += 1
                continue
                
            # Check if connection already exists
            existing_conn = None
            try:
                # Depending on the Airflow version, get_connection might work differently
                existing_conn = Connection.get_connection_from_secrets(conn_id)
                logger.debug(f"Connection {conn_id} already exists")
            except Exception as e:
                logger.debug(f"Connection {conn_id} does not exist: {str(e)}")
                
            if existing_conn and skip_existing:
                logger.info(f"Skipping existing connection: {conn_id}")
                skipped_count += 1
                continue
                
            # If it's a dry run, just log what would happen
            if dry_run:
                if existing_conn and force:
                    logger.info(f"[DRY RUN] Would update existing connection: {conn_id}")
                elif existing_conn:
                    logger.info(f"[DRY RUN] Would modify existing connection: {conn_id}")
                else:
                    logger.info(f"[DRY RUN] Would create new connection: {conn_id}")
                    
                success_count += 1
                continue
                
            # Delete existing connection if force flag is set
            if existing_conn and force:
                logger.info(f"Deleting existing connection: {conn_id}")
                # The way to delete connections depends on Airflow version
                try:
                    # Try Airflow 2.X way
                    session = Connection.get_session()
                    session.delete(existing_conn)
                    session.commit()
                except Exception:
                    # Fallback to Airflow 1.X way
                    existing_conn.delete()
                    
            # Create the new connection
            new_conn = Connection(
                conn_id=conn_id,
                conn_type=conn_def.get('conn_type'),
                host=conn_def.get('host'),
                login=conn_def.get('login'),
                password=conn_def.get('password'),
                schema=conn_def.get('schema'),
                port=conn_def.get('port'),
                extra=conn_def.get('extra')
            )
            
            new_conn.set_password(conn_def.get('password') or '')
            
            # Save the connection
            new_conn.save()
            
            logger.info(f"Successfully {'updated' if existing_conn else 'created'} connection: {conn_id}")
            success_count += 1
            
        except Exception as e:
            logger.error(f"Failed to import connection {conn_id}: {str(e)}")
            failed_count += 1
            
    # Log summary
    logger.info(f"Import summary: {success_count} successful, {skipped_count} skipped, {failed_count} failed")
    
    return success_count, skipped_count, failed_count


def main(args=None) -> int:
    """
    Main function to coordinate the connection import process.
    
    Args:
        args: Command line arguments (defaults to sys.argv[1:])
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Parse arguments
    if args is None:
        args = parse_arguments()
    else:
        # If args is a list (like sys.argv), parse it
        if isinstance(args, list):
            parser = argparse.ArgumentParser()
            args = parse_arguments()
            
    # Set up logging
    setup_logging(args.log_level)
    
    try:
        # Load connections from file or GCS
        if args.file_path:
            logger.info(f"Loading connections from file: {args.file_path}")
            connections = load_connections_from_file(args.file_path)
        elif args.gcs_path:
            logger.info(f"Loading connections from GCS: {args.gcs_path}")
            connections = load_connections_from_gcs(args.gcs_path, args.gcp_conn_id)
        else:
            logger.error("No source specified (--file-path or --gcs-path)")
            return 2
            
        # Process environment-specific connections
        if args.environment:
            logger.info(f"Processing connections for environment: {args.environment}")
            connections = process_environment_connections(connections, args.environment)
            
        # Resolve secrets in connection definitions
        logger.info("Resolving secret references")
        connections = resolve_secrets(connections, args.gcp_conn_id)
        
        # Import the connections
        logger.info("Importing connections")
        success, skipped, failed = import_connections(
            connections,
            dry_run=args.dry_run,
            skip_existing=args.skip_existing,
            force=args.force,
            validate=args.validate,
            filter_conn_id=args.conn_id
        )
        
        # Determine exit code based on results
        if failed == 0 and success > 0:
            # All requested connections were imported successfully
            logger.info("Connection import completed successfully")
            return 0
        elif failed > 0 and success > 0:
            # Some connections failed but some succeeded
            logger.warning("Connection import completed with some failures")
            return 1
        elif failed > 0 and success == 0:
            # All connections failed
            logger.error("Connection import failed completely")
            return 2
        else:
            # No connections were processed (all skipped or none found)
            logger.warning("No connections were imported")
            return 0
            
    except Exception as e:
        logger.error(f"Error during connection import: {str(e)}")
        return 2


if __name__ == "__main__":
    sys.exit(main())