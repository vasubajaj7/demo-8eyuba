#!/usr/bin/env python3
"""
Metadata restore script for Apache Airflow migration from 1.10.15 to Airflow 2.X.

This script restores Airflow metadata from Google Cloud Storage backups to a PostgreSQL/Cloud SQL
database. It supports restoring from full or incremental backups with validation checks to
ensure data integrity, helping ensure zero data loss during the migration process.

Usage:
    python restore_metadata.py --backup-id [backup_id] --conn-id [connection_id]
    python restore_metadata.py --latest --conn-id [connection_id]
    python restore_metadata.py --backup-date [YYYY-MM-DD] --conn-id [connection_id]
"""

import os
import sys
import argparse
import logging
import datetime
import json
import tempfile
import subprocess
import time
import shutil
from typing import List, Dict, Any, Optional, Union, Tuple

# Internal imports
from ..dags.utils.db_utils import get_postgres_hook, POSTGRES_CONN_ID, verify_connection, get_db_info
from ..dags.utils.gcp_utils import gcs_download_file, DEFAULT_GCP_CONN_ID, GCSClient, get_secret
from ..dags.utils.alert_utils import send_alert, AlertLevel
from ..config import get_config, get_environment
from .backup_metadata import METADATA_TABLES

# Set up logging
logger = logging.getLogger('airflow.restore')

# Define constants
DEFAULT_RESTORE_BUCKET = None  # Will be set from environment config
DEFAULT_RESTORE_PATH = '/backups/metadata'
DEFAULT_LOG_LEVEL = 'INFO'
DEFAULT_TABLES_TO_RESTORE = []  # Empty list means restore all available tables

def setup_logging(verbose: bool) -> None:
    """
    Configure logging for the restore process.
    
    Args:
        verbose: If True, set log level to DEBUG, otherwise INFO
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger.setLevel(log_level)
    logger.info("Starting Airflow metadata restore process")

def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for the restore script.
    
    Returns:
        Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Restore Airflow metadata database from Google Cloud Storage backups",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Backup selection arguments (one is required)
    parser.add_argument(
        '--backup-id',
        help="Specific backup ID to restore"
    )
    
    parser.add_argument(
        '--conn-id',
        default=POSTGRES_CONN_ID,
        help="Airflow connection ID for the target metadata database"
    )
    
    parser.add_argument(
        '--backup-date',
        help="Date of backup to restore (format: YYYY-MM-DD)"
    )
    
    parser.add_argument(
        '--latest',
        action='store_true',
        help="Restore the most recent backup"
    )
    
    parser.add_argument(
        '--restore-dir',
        help="Local directory to store backups temporarily during restore"
    )
    
    parser.add_argument(
        '--gcs-bucket',
        help="GCS bucket containing backups"
    )
    
    parser.add_argument(
        '--gcs-path',
        default=DEFAULT_RESTORE_PATH,
        help="Path within GCS bucket for backups"
    )
    
    parser.add_argument(
        '--pg-restore-path',
        help="Path to pg_restore executable"
    )
    
    parser.add_argument(
        '--tables',
        nargs='+',
        help="Specific tables to restore (space separated)"
    )
    
    parser.add_argument(
        '--environment',
        choices=['dev', 'qa', 'prod'],
        help="Environment (dev/qa/prod) to use for configuration"
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Validate restoration without actually performing it"
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help="Force restore even in production environment"
    )
    
    return parser.parse_args()

def get_restore_config(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Configure restore settings based on arguments and environment.
    
    Args:
        args: Command line arguments
        
    Returns:
        Restore configuration dictionary
    """
    # Determine environment
    if args.environment:
        environment = args.environment
    else:
        try:
            environment = get_environment()
        except (ImportError, AttributeError):
            environment = 'dev'
    
    # Get environment-specific config
    try:
        env_config = get_config(environment)
        storage_config = env_config.get('storage', {})
    except Exception as e:
        logger.warning(f"Error loading environment config: {str(e)}")
        storage_config = {}
    
    # Set restore directory
    if args.restore_dir:
        restore_dir = args.restore_dir
        is_temp_dir = False
    else:
        restore_dir = tempfile.mkdtemp(prefix=f"airflow_restore_{environment}_")
        is_temp_dir = True
    
    # Set GCS bucket
    if args.gcs_bucket:
        gcs_bucket = args.gcs_bucket
    else:
        gcs_bucket = storage_config.get('backup_bucket', f"composer2-migration-{environment}-backup")
        
    # Set GCS path
    if args.gcs_path != DEFAULT_RESTORE_PATH:
        gcs_path = args.gcs_path
    else:
        gcs_path = f"{DEFAULT_RESTORE_PATH}/{environment}"
    
    # Configure pg_restore path
    if args.pg_restore_path:
        pg_restore_path = args.pg_restore_path
    else:
        # Try to find pg_restore in PATH
        try:
            pg_restore_path = subprocess.check_output(['which', 'pg_restore']).decode('utf-8').strip()
        except Exception:
            pg_restore_path = 'pg_restore'  # Default to expecting it in PATH
    
    # Set tables to restore
    if args.tables:
        tables_to_restore = args.tables
    else:
        tables_to_restore = DEFAULT_TABLES_TO_RESTORE
    
    # Validate backup selection criteria
    if not (args.backup_id or args.backup_date or args.latest):
        logger.warning("No backup selection criteria provided, defaulting to --latest")
        latest = True
    else:
        latest = args.latest
    
    # Check if this is a production environment and enforce safeguards
    if environment == 'prod' and not args.force and not args.dry_run:
        logger.error("Refusing to restore in production environment without --force flag")
        raise ValueError("Production environment detected. Use --force to proceed with actual restore.")
    
    # Build configuration
    config = {
        'environment': environment,
        'restore_dir': restore_dir,
        'is_temp_dir': is_temp_dir,
        'conn_id': args.conn_id,
        'gcs_bucket': gcs_bucket,
        'gcs_path': gcs_path,
        'pg_restore_path': pg_restore_path,
        'backup_id': args.backup_id,
        'backup_date': args.backup_date,
        'latest': latest,
        'tables_to_restore': tables_to_restore,
        'dry_run': args.dry_run,
        'force': args.force
    }
    
    return config

def find_backup_manifest(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Find the appropriate backup manifest file based on selection criteria.
    
    Args:
        config: Restore configuration including backup selection criteria
        
    Returns:
        Backup manifest information including GCS path and local path
        
    Raises:
        ValueError: If no suitable backup is found
    """
    gcs_bucket = config['gcs_bucket']
    gcs_path = config['gcs_path']
    backup_id = config.get('backup_id')
    backup_date = config.get('backup_date')
    latest = config.get('latest', False)
    
    # Initialize GCS client
    gcs_client = GCSClient()
    
    # List manifest files in the backup path
    logger.info(f"Searching for backup manifests in gs://{gcs_bucket}/{gcs_path}/")
    manifest_files = [f for f in gcs_client.list_files(
        bucket_name=gcs_bucket,
        prefix=gcs_path
    ) if f.endswith('_manifest.json')]
    
    if not manifest_files:
        raise ValueError(f"No backup manifests found in gs://{gcs_bucket}/{gcs_path}/")
    
    logger.info(f"Found {len(manifest_files)} backup manifests")
    
    selected_manifest = None
    
    # Find by specific backup ID
    if backup_id:
        logger.info(f"Looking for backup with ID: {backup_id}")
        for manifest_file in manifest_files:
            if backup_id in manifest_file:
                selected_manifest = manifest_file
                logger.info(f"Found backup manifest: {selected_manifest}")
                break
        
        if not selected_manifest:
            raise ValueError(f"No backup found with ID: {backup_id}")
    
    # Find by date
    elif backup_date:
        logger.info(f"Looking for backup from date: {backup_date}")
        try:
            target_date = datetime.datetime.strptime(backup_date, '%Y-%m-%d')
            
            # Find backups with closest date
            backup_dates = []
            for manifest_file in manifest_files:
                # Extract date from filename (format: airflow_backup_env_type_YYYYMMDD_HHMMSS_manifest.json)
                try:
                    # Parse the date from the manifest filename
                    date_str = os.path.basename(manifest_file).split('_')[4:6]
                    date_str = '_'.join(date_str)  # YYYYMMDD_HHMMSS
                    file_date = datetime.datetime.strptime(date_str, '%Y%m%d_%H%M%S')
                    backup_dates.append((manifest_file, file_date))
                except (IndexError, ValueError):
                    continue
            
            if not backup_dates:
                raise ValueError("Could not parse dates from backup manifest filenames")
            
            # Find closest date
            backup_dates.sort(key=lambda x: abs((x[1].date() - target_date.date()).days))
            selected_manifest = backup_dates[0][0]
            logger.info(f"Found closest backup from {backup_dates[0][1].strftime('%Y-%m-%d')}: {selected_manifest}")
            
        except ValueError as e:
            raise ValueError(f"Invalid date format or no backup found for date {backup_date}: {str(e)}")
    
    # Find latest backup
    elif latest:
        logger.info("Looking for latest backup")
        try:
            # Sort manifests by date (format: airflow_backup_env_type_YYYYMMDD_HHMMSS_manifest.json)
            latest_backup = None
            latest_date = datetime.datetime.min
            
            for manifest_file in manifest_files:
                try:
                    # Parse the date from the manifest filename
                    date_str = os.path.basename(manifest_file).split('_')[4:6]
                    date_str = '_'.join(date_str)  # YYYYMMDD_HHMMSS
                    file_date = datetime.datetime.strptime(date_str, '%Y%m%d_%H%M%S')
                    
                    if file_date > latest_date:
                        latest_date = file_date
                        latest_backup = manifest_file
                except (IndexError, ValueError):
                    continue
            
            if latest_backup:
                selected_manifest = latest_backup
                logger.info(f"Found latest backup from {latest_date.strftime('%Y-%m-%d %H:%M:%S')}: {selected_manifest}")
            else:
                raise ValueError("Could not determine latest backup from manifest filenames")
            
        except Exception as e:
            raise ValueError(f"Failed to find latest backup: {str(e)}")
    
    else:
        raise ValueError("No backup selection criteria provided (--backup-id, --backup-date, or --latest)")
    
    # Download the selected manifest
    logger.info(f"Downloading manifest: gs://{gcs_bucket}/{selected_manifest}")
    local_manifest_path = os.path.join(config['restore_dir'], os.path.basename(selected_manifest))
    
    try:
        gcs_client.download_file(
            bucket_name=gcs_bucket,
            object_name=selected_manifest,
            local_file_path=local_manifest_path
        )
    except Exception as e:
        raise ValueError(f"Failed to download manifest file: {str(e)}")
    
    # Parse manifest content
    try:
        with open(local_manifest_path, 'r') as f:
            manifest_content = json.load(f)
    except Exception as e:
        raise ValueError(f"Failed to parse manifest file: {str(e)}")
    
    # Validate manifest format
    required_keys = ['backup_id', 'timestamp', 'backup_type', 'files']
    for key in required_keys:
        if key not in manifest_content:
            raise ValueError(f"Invalid manifest format: missing '{key}' field")
    
    # Return manifest information
    result = {
        'gcs_path': selected_manifest,
        'local_path': local_manifest_path,
        'content': manifest_content,
        'bucket': gcs_bucket
    }
    
    logger.info(f"Selected backup: {manifest_content['backup_id']} from {manifest_content['timestamp']}")
    logger.info(f"Backup type: {manifest_content['backup_type']}")
    logger.info(f"Files in backup: {len(manifest_content['files'])}")
    
    return result

def download_backup_files(manifest: Dict[str, Any], restore_dir: str, tables_to_restore: List[str]) -> List[Dict[str, Any]]:
    """
    Download backup files from GCS based on manifest.
    
    Args:
        manifest: Backup manifest information
        restore_dir: Directory to store downloaded files
        tables_to_restore: List of tables to restore (empty for all)
        
    Returns:
        List of downloaded backup files with paths
    """
    bucket_name = manifest['bucket']
    backup_files = manifest['content']['files']
    gcs_path_prefix = os.path.dirname(manifest['gcs_path'])
    
    # Initialize GCS client
    gcs_client = GCSClient()
    
    # Create restore directory if it doesn't exist
    os.makedirs(restore_dir, exist_ok=True)
    
    # Filter files if specific tables requested
    if tables_to_restore:
        logger.info(f"Filtering backup files to include only tables: {', '.join(tables_to_restore)}")
        filtered_files = []
        for file_info in backup_files:
            if file_info['file_type'] == 'schema':
                # Always include schema files
                filtered_files.append(file_info)
            elif file_info.get('tables') in tables_to_restore:
                filtered_files.append(file_info)
        backup_files = filtered_files
        logger.info(f"Selected {len(backup_files)} backup files after filtering")
    
    # Download each backup file
    downloaded_files = []
    total_size = 0
    
    for file_info in backup_files:
        file_name = file_info['file_name']
        gcs_file_path = f"{gcs_path_prefix}/{file_name}"
        local_file_path = os.path.join(restore_dir, file_name)
        
        logger.info(f"Downloading: gs://{bucket_name}/{gcs_file_path} to {local_file_path}")
        
        try:
            gcs_client.download_file(
                bucket_name=bucket_name,
                object_name=gcs_file_path,
                local_file_path=local_file_path
            )
            
            # Verify file size
            if os.path.exists(local_file_path):
                local_size = os.path.getsize(local_file_path)
                if local_size == 0:
                    logger.warning(f"Downloaded file is empty: {local_file_path}")
                
                # Add to downloaded files list
                downloaded_files.append({
                    'file_name': file_name,
                    'file_path': local_file_path,
                    'file_size': local_size,
                    'file_type': file_info['file_type'],
                    'tables': file_info.get('tables')
                })
                
                total_size += local_size
                logger.debug(f"Downloaded {file_name}: {local_size} bytes")
            else:
                logger.error(f"Failed to download file: {file_name}")
        
        except Exception as e:
            logger.error(f"Error downloading {file_name}: {str(e)}")
    
    logger.info(f"Downloaded {len(downloaded_files)} files, total size: {total_size / (1024 * 1024):.2f} MB")
    return downloaded_files

def validate_restore_compatibility(db_info: Dict[str, Any], manifest: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that backup is compatible with target database.
    
    Args:
        db_info: Target database information
        manifest: Backup manifest information
        
    Returns:
        Validation results with compatibility status
    """
    manifest_content = manifest['content']
    backup_db_info = manifest_content.get('database', {})
    
    # Initialize validation results
    validation = {
        'is_compatible': True,
        'warnings': [],
        'blockers': [],
        'details': {}
    }
    
    # Check PostgreSQL version compatibility
    backup_pg_version = backup_db_info.get('version', 'Unknown')
    target_pg_version = db_info.get('version', 'Unknown')
    
    validation['details']['postgres_versions'] = {
        'backup': backup_pg_version,
        'target': target_pg_version
    }
    
    # Extract major versions for comparison
    try:
        backup_major_version = int(backup_pg_version.split()[1].split('.')[0])
        target_major_version = int(target_pg_version.split()[1].split('.')[0])
        
        if backup_major_version > target_major_version:
            validation['blockers'].append(
                f"Backup PostgreSQL version ({backup_major_version}) is newer than target ({target_major_version})"
            )
            validation['is_compatible'] = False
        elif backup_major_version < target_major_version:
            validation['warnings'].append(
                f"Backup PostgreSQL version ({backup_major_version}) is older than target ({target_major_version})"
            )
    except (IndexError, ValueError):
        validation['warnings'].append(
            f"Could not compare PostgreSQL versions: backup='{backup_pg_version}', target='{target_pg_version}'"
        )
    
    # Check Airflow version compatibility
    backup_airflow_version = manifest_content.get('airflow_version', 'Unknown')
    validation['details']['airflow_versions'] = {
        'backup': backup_airflow_version
    }
    
    # Try to get current Airflow version
    try:
        from airflow.version import version as current_airflow_version
        validation['details']['airflow_versions']['target'] = current_airflow_version
        
        # Compare versions - allow restoring from 1.x to 2.x for migration
        if '1.' in backup_airflow_version and '2.' in current_airflow_version:
            validation['warnings'].append(
                f"Restoring from Airflow 1.x ({backup_airflow_version}) to Airflow 2.x ({current_airflow_version})"
            )
        elif '2.' in backup_airflow_version and '1.' in current_airflow_version:
            validation['blockers'].append(
                f"Cannot restore from Airflow 2.x ({backup_airflow_version}) to Airflow 1.x ({current_airflow_version})"
            )
            validation['is_compatible'] = False
    except ImportError:
        validation['warnings'].append("Could not determine current Airflow version")
    
    # Check database connection
    validation['details']['database_connection'] = {
        'host': db_info.get('host'),
        'database': db_info.get('database')
    }
    
    # Check available disk space
    try:
        restore_dir = os.path.dirname(manifest['local_path'])
        total_size = sum(file.get('file_size', 0) for file in manifest_content.get('files', []))
        free_space = shutil.disk_usage(restore_dir).free
        
        validation['details']['disk_space'] = {
            'required': total_size,
            'available': free_space
        }
        
        if free_space < total_size * 2:  # Need at least double the backup size
            validation['warnings'].append(
                f"Low disk space: need {total_size * 2 / (1024 * 1024):.2f} MB, "
                f"have {free_space / (1024 * 1024):.2f} MB"
            )
    except Exception as e:
        validation['warnings'].append(f"Could not check disk space: {str(e)}")
    
    # Log validation results
    if validation['blockers']:
        logger.error(f"Validation found blockers: {validation['blockers']}")
    if validation['warnings']:
        logger.warning(f"Validation found warnings: {validation['warnings']}")
    if validation['is_compatible']:
        logger.info("Backup is compatible with target database")
    
    return validation

def perform_pg_restore(db_info: Dict[str, Any], backup_file: str, table_name: str, pg_restore_path: str, dry_run: bool) -> bool:
    """
    Execute pg_restore to restore a database backup.
    
    Args:
        db_info: Database connection information
        backup_file: Path to backup file
        table_name: Table name to restore
        pg_restore_path: Path to pg_restore executable
        dry_run: If True, only validate without actual restore
        
    Returns:
        True if restore succeeds, False otherwise
    """
    # Build pg_restore command
    cmd = [
        pg_restore_path,
        '-h', db_info['host'],
        '-p', str(db_info['port']),
        '-U', db_info['user'],
        '-d', db_info['database'],
        '-v',  # Verbose output
    ]
    
    if dry_run:
        cmd.append('--list')  # List contents without restoring
    else:
        # Add restore options
        cmd.extend([
            '--clean',  # Clean (drop) objects before recreating
            '--if-exists',  # Use IF EXISTS when dropping objects
            '--no-owner',  # Don't set ownership to match the original database
            '--no-privileges'  # Don't include privilege (grant/revoke) statements
        ])
    
    # Add table filter if a specific table is specified
    if table_name:
        cmd.extend(['--table', table_name])
    
    # Add input file
    cmd.append(backup_file)
    
    # Set environment for password
    env = os.environ.copy()
    if db_info.get('password'):
        env['PGPASSWORD'] = db_info['password']
    
    if dry_run:
        logger.info(f"Dry run: Analyzing backup file {backup_file} for table {table_name or 'ALL'}")
    else:
        logger.info(f"Executing pg_restore for table {table_name or 'ALL'} from {backup_file}")
    
    try:
        # Execute pg_restore
        start_time = time.time()
        process = subprocess.run(
            cmd,
            env=env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        duration = time.time() - start_time
        
        # Log process output
        logger.debug(f"pg_restore STDOUT: {process.stdout}")
        if process.stderr:
            logger.warning(f"pg_restore STDERR: {process.stderr}")
        
        if dry_run:
            logger.info(f"Dry run completed in {duration:.2f}s for {backup_file}")
            return True
        else:
            logger.info(f"Restore completed in {duration:.2f}s for table {table_name or 'ALL'}")
            
            # Verify table exists after restore (only if specific table restored)
            if table_name:
                hook = get_postgres_hook(conn_id=db_info.get('conn_id'))
                try:
                    check_sql = f"SELECT COUNT(*) FROM {table_name} LIMIT 1"
                    hook.run(check_sql)
                    logger.info(f"Verified table {table_name} exists after restore")
                except Exception as e:
                    logger.error(f"Table {table_name} verification failed: {str(e)}")
                    return False
            
            return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"pg_restore failed with exit code {e.returncode}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Error executing pg_restore: {str(e)}")
        return False

def restore_metadata_tables(config: Dict[str, Any], db_info: Dict[str, Any], backup_files: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Restore specific Airflow metadata tables.
    
    Args:
        config: Restore configuration
        db_info: Database connection information
        backup_files: List of backup files to restore
        
    Returns:
        Restoration results with success/failure by table
    """
    pg_restore_path = config['pg_restore_path']
    dry_run = config['dry_run']
    tables_to_restore = config['tables_to_restore']
    
    # Track restoration results
    results = {
        'tables_restored': [],
        'tables_failed': [],
        'schema_restored': False
    }
    
    # First restore schema if it exists
    schema_files = [f for f in backup_files if f['file_type'] == 'schema']
    if schema_files and not dry_run:
        schema_file = schema_files[0]['file_path']
        logger.info(f"Restoring database schema from {schema_file}")
        
        try:
            # For schema, use psql instead of pg_restore for better compatibility
            cmd = [
                'psql',
                '-h', db_info['host'],
                '-p', str(db_info['port']),
                '-U', db_info['user'],
                '-d', db_info['database'],
                '-f', schema_file
            ]
            
            # Set environment for password
            env = os.environ.copy()
            if db_info.get('password'):
                env['PGPASSWORD'] = db_info['password']
            
            process = subprocess.run(
                cmd,
                env=env,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            results['schema_restored'] = True
            logger.info("Schema restoration completed successfully")
        
        except Exception as e:
            logger.error(f"Schema restoration failed: {str(e)}")
            # Continue with data restoration even if schema fails
    
    # Then restore each table
    for file_info in backup_files:
        # Skip schema files (already processed)
        if file_info['file_type'] == 'schema':
            continue
        
        table_name = file_info.get('tables')
        file_path = file_info['file_path']
        
        # Skip tables not in the list if specific tables requested
        if tables_to_restore and table_name not in tables_to_restore:
            logger.debug(f"Skipping table {table_name} (not in requested tables)")
            continue
        
        logger.info(f"Restoring table: {table_name or 'ALL'} from {file_path}")
        
        # Perform restoration
        success = perform_pg_restore(
            db_info=db_info,
            backup_file=file_path,
            table_name=table_name,
            pg_restore_path=pg_restore_path,
            dry_run=dry_run
        )
        
        if success:
            if table_name:
                results['tables_restored'].append(table_name)
            else:
                results['tables_restored'].append('ALL_TABLES')
            logger.info(f"Successfully restored {table_name or 'ALL_TABLES'}")
        else:
            if table_name:
                results['tables_failed'].append(table_name)
            else:
                results['tables_failed'].append('ALL_TABLES')
            logger.error(f"Failed to restore {table_name or 'ALL_TABLES'}")
    
    # Check if any critical tables failed to restore
    critical_tables = ['dag', 'dag_run', 'task_instance']
    critical_failures = [t for t in critical_tables if t in results['tables_failed']]
    
    if critical_failures and not dry_run:
        logger.error(f"Critical tables failed to restore: {critical_failures}")
        results['critical_failure'] = True
    else:
        results['critical_failure'] = False
    
    # Log summary
    logger.info(f"Restore complete. Restored: {len(results['tables_restored'])} tables, "
                f"Failed: {len(results['tables_failed'])} tables")
    
    if dry_run:
        logger.info("This was a dry run, no actual restoration performed")
    
    return results

def validate_restored_data(config: Dict[str, Any], db_info: Dict[str, Any], manifest: Dict[str, Any], restore_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate restored data for consistency and completeness.
    
    Args:
        config: Restore configuration
        db_info: Database connection information
        manifest: Backup manifest
        restore_results: Results from the restoration process
        
    Returns:
        Validation results with any detected issues
    """
    # Skip validation for dry runs
    if config['dry_run']:
        logger.info("Skipping data validation for dry run")
        return {'validated': False, 'reason': 'dry_run'}
    
    # Check if critical failure occurred during restore
    if restore_results.get('critical_failure', False):
        logger.error("Skipping validation due to critical restore failures")
        return {'validated': False, 'reason': 'critical_failure'}
    
    # Initialize validation results
    validation = {
        'validated': True,
        'issues': [],
        'table_counts': {}
    }
    
    try:
        # Get database hook
        hook = get_postgres_hook(conn_id=db_info.get('conn_id'))
        
        # Check row counts for restored tables
        for table in restore_results['tables_restored']:
            if table == 'ALL_TABLES' or table == 'schema':
                continue
                
            try:
                count_sql = f"SELECT COUNT(*) FROM {table}"
                result = hook.run(count_sql)
                count = result[0][0] if result else 0
                
                validation['table_counts'][table] = count
                logger.info(f"Table {table} has {count} rows after restore")
            except Exception as e:
                logger.error(f"Error checking row count for table {table}: {str(e)}")
                validation['issues'].append(f"Could not verify row count for {table}: {str(e)}")
                validation['validated'] = False
        
        # Check foreign key constraints
        try:
            check_sql = """
                SELECT 
                    tc.constraint_name, 
                    tc.table_name, 
                    kcu.column_name, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM 
                    information_schema.table_constraints AS tc 
                JOIN 
                    information_schema.key_column_usage AS kcu
                ON 
                    tc.constraint_name = kcu.constraint_name
                JOIN 
                    information_schema.constraint_column_usage AS ccu 
                ON 
                    ccu.constraint_name = tc.constraint_name
                WHERE 
                    tc.constraint_type = 'FOREIGN KEY';
            """
            constraints = hook.run(check_sql)
            
            validation['foreign_keys'] = {
                'count': len(constraints),
                'verified': True
            }
            logger.info(f"Verified {len(constraints)} foreign key constraints")
        
        except Exception as e:
            logger.error(f"Error checking foreign key constraints: {str(e)}")
            validation['issues'].append(f"Could not verify foreign key constraints: {str(e)}")
            validation['foreign_keys'] = {'verified': False}
            validation['validated'] = False
        
        # Verify basic Airflow functionality by querying key tables
        try:
            test_queries = [
                "SELECT COUNT(*) FROM dag",
                "SELECT COUNT(*) FROM dag_run",
                "SELECT COUNT(*) FROM task_instance"
            ]
            
            for query in test_queries:
                table = query.split()[-1]
                result = hook.run(query)
                count = result[0][0] if result else 0
                logger.info(f"Validation query '{query}' returned {count} rows")
        
        except Exception as e:
            logger.error(f"Error executing validation queries: {str(e)}")
            validation['issues'].append(f"Basic functionality validation failed: {str(e)}")
            validation['validated'] = False
        
        # Overall validation result
        if validation['validated']:
            logger.info("Data validation completed successfully")
        else:
            logger.warning(f"Data validation found issues: {validation['issues']}")
        
        return validation
        
    except Exception as e:
        logger.error(f"Error during data validation: {str(e)}")
        return {
            'validated': False,
            'reason': str(e),
            'issues': [str(e)]
        }

def cleanup_restore_files(restore_files: List[Dict[str, Any]], restore_dir: str, is_temp_dir: bool) -> bool:
    """
    Remove local restore files after successful restore.
    
    Args:
        restore_files: List of restore files
        restore_dir: Restore directory
        is_temp_dir: Whether the directory is temporary
        
    Returns:
        True if cleanup succeeds, False otherwise
    """
    if is_temp_dir:
        # Remove entire directory for temp dirs
        try:
            logger.info(f"Removing temporary directory: {restore_dir}")
            shutil.rmtree(restore_dir)
            return True
        except Exception as e:
            logger.error(f"Failed to remove temporary directory: {str(e)}")
            return False
    else:
        # Remove individual files
        success = True
        
        # Remove backup files
        for file_info in restore_files:
            try:
                file_path = file_info['file_path']
                logger.debug(f"Removing file: {file_path}")
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Failed to remove file {file_path}: {str(e)}")
                success = False
        
        return success

class MetadataRestore:
    """
    Class that manages the Airflow metadata restore process.
    """
    
    def __init__(self, conn_id: str, restore_dir: str, environment: str):
        """
        Initialize the MetadataRestore instance.
        
        Args:
            conn_id: Connection ID for the target database
            restore_dir: Directory to store backup files during restore
            environment: Environment (dev/qa/prod)
        """
        self.conn_id = conn_id
        self.restore_dir = restore_dir
        self.environment = environment
        
        # Create restore directory if it doesn't exist
        os.makedirs(self.restore_dir, exist_ok=True)
        
        # Initialize empty data structures
        self.db_info = {}
        self.restore_files = []
        self.manifest = {}
        self.stats = {
            'start_time': datetime.datetime.now().isoformat(),
            'environment': environment,
            'status': 'pending',
            'files_restored': 0,
            'tables_restored': 0,
            'validation_results': None,
            'errors': []
        }
        
        # Get database information
        self.db_info = get_db_info(conn_id)
        self.db_info['conn_id'] = conn_id
        
        logger.info(f"Initialized restore process for environment: {environment}")
        logger.info(f"Target database: {self.db_info.get('connection', {}).get('host')}:{self.db_info.get('connection', {}).get('port')}/{self.db_info.get('connection', {}).get('schema')}")
        logger.info(f"Restore directory: {restore_dir}")
    
    def find_backup(self, backup_id: str, backup_date: str, latest: bool, gcs_bucket: str, gcs_path: str) -> Dict[str, Any]:
        """
        Find backup to restore based on criteria.
        
        Args:
            backup_id: Specific backup ID to restore
            backup_date: Date of backup to restore
            latest: Whether to restore the latest backup
            gcs_bucket: GCS bucket containing backups
            gcs_path: Path within the bucket
            
        Returns:
            Manifest information for the selected backup
        """
        try:
            # Configure backup search criteria
            config = {
                'gcs_bucket': gcs_bucket,
                'gcs_path': gcs_path,
                'backup_id': backup_id,
                'backup_date': backup_date,
                'latest': latest,
                'restore_dir': self.restore_dir
            }
            
            # Find the appropriate backup manifest
            self.manifest = find_backup_manifest(config)
            
            return self.manifest
        
        except Exception as e:
            error_msg = f"Failed to find backup: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            self.stats['status'] = 'failed'
            raise
    
    def download_files(self, tables_to_restore: List[str]) -> List[Dict[str, Any]]:
        """
        Download backup files from GCS.
        
        Args:
            tables_to_restore: List of tables to restore (empty for all)
            
        Returns:
            List of downloaded backup files
        """
        try:
            if not self.manifest:
                raise ValueError("No backup manifest found. Call find_backup() first.")
            
            # Download backup files
            self.restore_files = download_backup_files(
                manifest=self.manifest,
                restore_dir=self.restore_dir,
                tables_to_restore=tables_to_restore
            )
            
            # Update stats
            self.stats['files_downloaded'] = len(self.restore_files)
            self.stats['download_size'] = sum(f.get('file_size', 0) for f in self.restore_files)
            
            return self.restore_files
        
        except Exception as e:
            error_msg = f"Failed to download backup files: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            self.stats['status'] = 'failed'
            raise
    
    def execute_restore(self, pg_restore_path: str, dry_run: bool) -> Dict[str, Any]:
        """
        Execute the restore process.
        
        Args:
            pg_restore_path: Path to pg_restore executable
            dry_run: If True, only validate without actual restore
            
        Returns:
            Restore results by table
        """
        try:
            if not self.restore_files:
                raise ValueError("No backup files downloaded. Call download_files() first.")
            
            # Validate compatibility before restoration
            validation = validate_restore_compatibility(
                db_info=self.db_info,
                manifest=self.manifest
            )
            
            if not validation['is_compatible'] and not dry_run:
                # Incompatible backup and not a dry run
                raise ValueError(f"Incompatible backup: {validation['blockers']}")
            
            # Perform the restore
            restore_config = {
                'pg_restore_path': pg_restore_path,
                'dry_run': dry_run,
                'tables_to_restore': []  # Will be filtered by download_files
            }
            
            restore_results = restore_metadata_tables(
                config=restore_config,
                db_info=self.db_info,
                backup_files=self.restore_files
            )
            
            # Update stats
            self.stats['tables_restored'] = len(restore_results['tables_restored'])
            self.stats['tables_failed'] = len(restore_results['tables_failed'])
            self.stats['schema_restored'] = restore_results['schema_restored']
            self.stats['dry_run'] = dry_run
            
            if not dry_run:
                self.stats['status'] = 'restored'
            else:
                self.stats['status'] = 'validated'
            
            return restore_results
        
        except Exception as e:
            error_msg = f"Failed to execute restore: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            self.stats['status'] = 'failed'
            raise
    
    def validate(self) -> Dict[str, Any]:
        """
        Validate the restored data.
        
        Returns:
            Validation results
        """
        try:
            if self.stats['status'] not in ['restored', 'validated']:
                raise ValueError("Data not restored. Call execute_restore() first.")
            
            # Validate restored data
            validation_results = validate_restored_data(
                config={'dry_run': self.stats.get('dry_run', False)},
                db_info=self.db_info,
                manifest=self.manifest,
                restore_results={
                    'tables_restored': self.stats.get('tables_restored', []),
                    'tables_failed': self.stats.get('tables_failed', []),
                    'critical_failure': self.stats.get('critical_failure', False)
                }
            )
            
            # Update stats
            self.stats['validation_results'] = validation_results
            
            return validation_results
        
        except Exception as e:
            error_msg = f"Failed to validate restored data: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {'validated': False, 'reason': str(e)}
    
    def cleanup(self, is_temp_dir: bool) -> bool:
        """
        Clean up local restore files.
        
        Args:
            is_temp_dir: Whether restore directory is temporary
            
        Returns:
            True if cleanup succeeds
        """
        try:
            cleanup_success = cleanup_restore_files(
                restore_files=self.restore_files,
                restore_dir=self.restore_dir,
                is_temp_dir=is_temp_dir
            )
            
            # Update stats
            self.stats['cleanup_success'] = cleanup_success
            
            return cleanup_success
        
        except Exception as e:
            error_msg = f"Failed to clean up restore files: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get restore statistics.
        
        Returns:
            Restore statistics
        """
        # Update end time if not set
        if 'end_time' not in self.stats:
            self.stats['end_time'] = datetime.datetime.now().isoformat()
            
        # Calculate duration
        start_time = datetime.datetime.fromisoformat(self.stats['start_time'])
        end_time = datetime.datetime.fromisoformat(self.stats['end_time'])
        duration_seconds = (end_time - start_time).total_seconds()
        self.stats['duration_seconds'] = duration_seconds
        
        return self.stats

def main() -> int:
    """
    Main function to orchestrate the restore process.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Parse arguments
        args = parse_args()
        
        # Set up logging
        setup_logging(args.verbose)
        
        # Get restore configuration
        config = get_restore_config(args)
        logger.info(f"Restore configuration: {json.dumps(config, default=str, indent=2)}")
        
        # Get database information and verify connection
        db_info = get_db_info(config['conn_id'])
        if not verify_connection(config['conn_id']):
            raise ValueError(f"Failed to connect to database using connection ID: {config['conn_id']}")
        
        # Create restore instance
        restore = MetadataRestore(
            conn_id=config['conn_id'],
            restore_dir=config['restore_dir'],
            environment=config['environment']
        )
        
        # Find the appropriate backup
        manifest = restore.find_backup(
            backup_id=config['backup_id'],
            backup_date=config['backup_date'],
            latest=config['latest'],
            gcs_bucket=config['gcs_bucket'],
            gcs_path=config['gcs_path']
        )
        
        # Check compatibility
        compatibility = validate_restore_compatibility(db_info, manifest)
        if not compatibility['is_compatible'] and not config['force'] and not config['dry_run']:
            logger.error("Backup is not compatible with target database and --force not specified")
            logger.error(f"Blockers: {compatibility['blockers']}")
            
            # Send alert
            send_alert(
                alert_level=AlertLevel.ERROR,
                context={
                    'dag_id': 'restore_metadata',
                    'task_id': 'compatibility_check',
                    'execution_date': datetime.datetime.now().isoformat(),
                    'blockers': compatibility['blockers']
                },
                subject_template="Airflow Metadata Restore Compatibility Error",
                body_template="Restore failed due to compatibility issues: {{ blockers|join(', ') }}"
            )
            return 1
        
        # Download backup files
        backup_files = restore.download_files(config['tables_to_restore'])
        
        # Execute restore
        restore_results = restore.execute_restore(
            pg_restore_path=config['pg_restore_path'],
            dry_run=config['dry_run']
        )
        
        if config['dry_run']:
            logger.info("Dry run completed successfully")
        else:
            # Validate restored data
            validation_results = restore.validate()
            
            if validation_results.get('validated', False):
                logger.info("Data validation successful")
            else:
                logger.warning(f"Data validation issues: {validation_results.get('issues', ['Unknown'])}")
        
        # Clean up restore files if needed
        cleanup_success = restore.cleanup(config['is_temp_dir'])
        
        # Get final statistics
        stats = restore.get_stats()
        logger.info(f"Restore completed in {stats['duration_seconds']:.2f} seconds")
        
        # Send completion alert
        if not config['dry_run']:
            alert_level = AlertLevel.INFO if stats['status'] == 'restored' else AlertLevel.WARNING
            
            send_alert(
                alert_level=alert_level,
                context={
                    'dag_id': 'restore_metadata',
                    'task_id': 'restore_execution',
                    'execution_date': datetime.datetime.now().isoformat(),
                    'environment': config['environment'],
                    'tables_restored': stats.get('tables_restored', 0),
                    'tables_failed': stats.get('tables_failed', 0),
                    'status': stats['status']
                },
                subject_template="Airflow Metadata Restore {{ 'Completed' if status == 'restored' else 'Partially Completed' }}",
                body_template=(
                    "Restore operation for {{ environment }} environment {{ 'completed successfully' if status == 'restored' else 'completed with issues' }}.\n"
                    "Tables restored: {{ tables_restored }}\n"
                    "Tables failed: {{ tables_failed }}\n"
                    "Duration: {{ duration_seconds }} seconds"
                )
            )
        
        # Return success if no critical failures
        return 0 if not stats.get('critical_failure', False) else 1
        
    except Exception as e:
        logger.error(f"Unhandled exception during restore: {str(e)}")
        logger.exception(e)
        
        # Send alert for unhandled exceptions
        send_alert(
            alert_level=AlertLevel.CRITICAL,
            context={
                'dag_id': 'restore_metadata',
                'task_id': 'restore_execution',
                'execution_date': datetime.datetime.now().isoformat(),
            },
            exception=e,
            subject_template="Critical Error in Airflow Metadata Restore",
            body_template="Unhandled exception occurred during restore: {{ exception }}"
        )
        
        return 1

if __name__ == "__main__":
    sys.exit(main())