#!/usr/bin/env python3
"""
Metadata backup script for Apache Airflow migration from 1.10.15 to Airflow 2.X.

This script creates backups of Airflow metadata from a PostgreSQL/Cloud SQL database
and uploads them to Google Cloud Storage. It supports both full and incremental
backups with configurable retention policies, ensuring zero data loss during migration.

Usage:
    python backup_metadata.py --type [full|incremental] --conn-id [connection_id] --gcs-bucket [bucket_name]
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
from ..dags.utils.gcp_utils import gcs_upload_file, DEFAULT_GCP_CONN_ID, GCSClient, get_secret
from ..dags.utils.alert_utils import send_alert, AlertLevel
from ..config import get_config, get_environment

# Set up logging
logger = logging.getLogger('airflow.backup')

# Define constants
METADATA_TABLES = [
    'dag', 'dag_run', 'job', 'log', 'sla_miss', 'task_fail', 
    'task_instance', 'xcom', 'variable', 'connection', 'slot_pool',
    'dag_tag', 'dag_pickle'
]

DEFAULT_BACKUP_BUCKET = None  # Will be set from environment config
DEFAULT_BACKUP_PATH = '/backups/metadata'
DEFAULT_LOG_LEVEL = 'INFO'

BACKUP_TYPES = {
    'full': 'Full database backup',
    'incremental': 'Incremental data backup'
}

def setup_logging(verbose: bool) -> None:
    """
    Configure logging for the backup process.
    
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
    logger.info("Starting Airflow metadata backup process")

def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for the backup script.
    
    Returns:
        Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Backup Airflow metadata database to Google Cloud Storage",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--type',
        choices=['full', 'incremental'],
        default='full',
        help="Type of backup to perform"
    )
    
    parser.add_argument(
        '--conn-id',
        default=POSTGRES_CONN_ID,
        help="Airflow connection ID for the metadata database"
    )
    
    parser.add_argument(
        '--output-dir',
        help="Local directory to store backups temporarily"
    )
    
    parser.add_argument(
        '--gcs-bucket',
        help="GCS bucket for storing backups"
    )
    
    parser.add_argument(
        '--gcs-path',
        default=DEFAULT_BACKUP_PATH,
        help="Path within GCS bucket for backups"
    )
    
    parser.add_argument(
        '--pg-dump-path',
        help="Path to pg_dump executable"
    )
    
    parser.add_argument(
        '--environment',
        choices=['dev', 'qa', 'prod'],
        help="Environment (dev/qa/prod) to use for configuration"
    )
    
    parser.add_argument(
        '--retention-days',
        type=int,
        help="Number of days to retain backups (0 for indefinite)"
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        '--no-upload',
        action='store_true',
        help="Skip upload to GCS (for local testing)"
    )
    
    return parser.parse_args()

def get_backup_config(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Configure backup settings based on arguments and environment.
    
    Args:
        args: Command line arguments
        
    Returns:
        Backup configuration dictionary
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
    
    # Set output directory
    if args.output_dir:
        backup_dir = args.output_dir
        is_temp_dir = False
    else:
        backup_dir = tempfile.mkdtemp(prefix=f"airflow_backup_{environment}_")
        is_temp_dir = True
    
    # Set GCS bucket
    if args.gcs_bucket:
        gcs_bucket = args.gcs_bucket
    else:
        gcs_bucket = storage_config.get('backup_bucket', f"composer2-migration-{environment}-backup")
        
    # Set GCS path
    if args.gcs_path != DEFAULT_BACKUP_PATH:
        gcs_path = args.gcs_path
    else:
        gcs_path = f"{DEFAULT_BACKUP_PATH}/{environment}"
    
    # Configure pg_dump path
    if args.pg_dump_path:
        pg_dump_path = args.pg_dump_path
    else:
        # Try to find pg_dump in PATH
        try:
            pg_dump_path = subprocess.check_output(['which', 'pg_dump']).decode('utf-8').strip()
        except Exception:
            pg_dump_path = 'pg_dump'  # Default to expecting it in PATH
    
    # Get retention policy
    if args.retention_days is not None:
        retention_days = args.retention_days
    else:
        retention_days = {
            'dev': 7,
            'qa': 14,
            'prod': 30
        }.get(environment, 7)
    
    # Build configuration
    config = {
        'environment': environment,
        'backup_type': args.type,
        'backup_dir': backup_dir,
        'is_temp_dir': is_temp_dir,
        'conn_id': args.conn_id,
        'gcs_bucket': gcs_bucket,
        'gcs_path': gcs_path,
        'pg_dump_path': pg_dump_path,
        'retention_days': retention_days,
        'upload_enabled': not args.no_upload,
        'verbose': args.verbose
    }
    
    return config

def get_database_info(conn_id: str) -> Dict[str, Any]:
    """
    Get database connection and version information.
    
    Args:
        conn_id: Connection ID to use
        
    Returns:
        Database information including connection details and version
    """
    # Verify connection works
    if not verify_connection(conn_id):
        raise Exception(f"Failed to verify database connection: {conn_id}")
    
    # Get database hook
    hook = get_postgres_hook(conn_id=conn_id)
    connection = hook.get_connection(conn_id)
    
    # Get database details
    db_info = get_db_info(conn_id)
    
    # Compile connection information
    db_details = {
        'host': connection.host,
        'port': connection.port,
        'database': connection.schema,
        'user': connection.login,
        'password': connection.password,
        'version': db_info.get('version', 'Unknown'),
        'is_cloud_sql': 'cloudsql' in connection.host.lower() if connection.host else False
    }
    
    return db_details

def create_backup_manifest(config: Dict[str, Any], db_info: Dict[str, Any], backup_files: List[Dict[str, Any]]) -> str:
    """
    Create a JSON manifest file for the backup.
    
    Args:
        config: Backup configuration
        db_info: Database information
        backup_files: List of backup files with metadata
        
    Returns:
        Path to created manifest file
    """
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_id = f"airflow_backup_{config['environment']}_{config['backup_type']}_{timestamp}"
    
    # Build manifest content
    manifest = {
        'backup_id': backup_id,
        'timestamp': datetime.datetime.now().isoformat(),
        'backup_type': config['backup_type'],
        'environment': config['environment'],
        'database': {
            'host': db_info['host'],
            'port': db_info['port'],
            'database': db_info['database'],
            'version': db_info['version'],
            'is_cloud_sql': db_info['is_cloud_sql']
        },
        'airflow_version': getattr(sys, 'airflow_version', 'Unknown'),
        'files': backup_files
    }
    
    # Write manifest to file
    manifest_path = os.path.join(config['backup_dir'], f"{backup_id}_manifest.json")
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    logger.info(f"Created backup manifest: {manifest_path}")
    return manifest_path

def perform_pg_dump(db_info: Dict[str, Any], output_file: str, backup_type: str, pg_dump_path: str) -> bool:
    """
    Execute pg_dump to create a database backup.
    
    Args:
        db_info: Database connection information
        output_file: Path to output file
        backup_type: Type of backup (full or incremental)
        pg_dump_path: Path to pg_dump executable
        
    Returns:
        True if backup succeeded, False otherwise
    """
    # Build pg_dump command
    cmd = [
        pg_dump_path,
        '-h', db_info['host'],
        '-p', str(db_info['port']),
        '-U', db_info['user'],
        '-d', db_info['database'],
        '-F', 'c',  # Custom format for better compression and flexibility
    ]
    
    # Add backup type specific options
    if backup_type == 'full':
        # Full backup - include schema and data
        cmd.append('--verbose')
    elif backup_type == 'incremental':
        # Incremental backup - data only, no schema
        cmd.extend(['--data-only', '--verbose'])
    
    # Add output file
    cmd.extend(['-f', output_file])
    
    # Set environment for password
    env = os.environ.copy()
    if db_info.get('password'):
        env['PGPASSWORD'] = db_info['password']
    
    logger.info(f"Executing pg_dump: {' '.join(cmd)}")
    
    try:
        # Execute pg_dump
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
        
        # Check if output file was created and has content
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
            logger.info(f"Backup completed successfully in {duration:.2f}s - File size: {file_size_mb:.2f} MB")
            return True
        else:
            logger.error(f"pg_dump completed but output file is missing or empty: {output_file}")
            return False
            
    except subprocess.CalledProcessError as e:
        logger.error(f"pg_dump failed with exit code {e.returncode}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Error executing pg_dump: {str(e)}")
        return False

def backup_metadata_tables(config: Dict[str, Any], db_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Backup specific Airflow metadata tables.
    
    Args:
        config: Backup configuration
        db_info: Database connection information
        
    Returns:
        List of created backup files with metadata
    """
    backup_type = config['backup_type']
    backup_dir = config['backup_dir']
    pg_dump_path = config['pg_dump_path']
    
    # Ensure backup directory exists
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_files = []
    
    # For full backup, first create a schema-only backup
    if backup_type == 'full':
        schema_file = os.path.join(backup_dir, f"airflow_schema_{timestamp}.sql")
        logger.info(f"Creating schema backup: {schema_file}")
        
        # Execute pg_dump for schema
        cmd = [
            pg_dump_path,
            '-h', db_info['host'],
            '-p', str(db_info['port']),
            '-U', db_info['user'],
            '-d', db_info['database'],
            '--schema-only',
            '-f', schema_file
        ]
        
        # Set environment for password
        env = os.environ.copy()
        if db_info.get('password'):
            env['PGPASSWORD'] = db_info['password']
        
        try:
            subprocess.run(cmd, env=env, check=True)
            
            if os.path.exists(schema_file) and os.path.getsize(schema_file) > 0:
                file_size = os.path.getsize(schema_file)
                backup_files.append({
                    'file_name': os.path.basename(schema_file),
                    'file_path': schema_file,
                    'file_size': file_size,
                    'file_type': 'schema',
                    'tables': 'all',
                    'timestamp': timestamp
                })
                logger.info(f"Schema backup completed - Size: {file_size / 1024:.2f} KB")
            else:
                logger.error("Schema backup failed - output file is missing or empty")
        except Exception as e:
            logger.error(f"Error creating schema backup: {str(e)}")
    
    # Now backup each metadata table or group
    for table in METADATA_TABLES:
        table_file = os.path.join(
            backup_dir, 
            f"airflow_{table}_{backup_type}_{timestamp}.backup"
        )
        
        logger.info(f"Backing up table: {table} to {table_file}")
        
        # Perform the backup
        success = perform_pg_dump(
            db_info=db_info,
            output_file=table_file,
            backup_type=backup_type,
            pg_dump_path=pg_dump_path
        )
        
        if success and os.path.exists(table_file):
            file_size = os.path.getsize(table_file)
            backup_files.append({
                'file_name': os.path.basename(table_file),
                'file_path': table_file,
                'file_size': file_size,
                'file_type': backup_type,
                'tables': table,
                'timestamp': timestamp
            })
            logger.info(f"Table {table} backup completed - Size: {file_size / (1024 * 1024):.2f} MB")
        else:
            logger.error(f"Backup failed for table: {table}")
    
    logger.info(f"Backup completed: {len(backup_files)} files created")
    return backup_files

def upload_to_gcs(backup_files: List[Dict[str, Any]], manifest_path: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upload backup files to Google Cloud Storage.
    
    Args:
        backup_files: List of backup files to upload
        manifest_path: Path to manifest file
        config: Backup configuration
        
    Returns:
        Upload results with GCS URIs
    """
    bucket_name = config['gcs_bucket']
    gcs_path = config['gcs_path']
    environment = config['environment']
    backup_type = config['backup_type']
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Initialize GCS client
    gcs_client = GCSClient()
    
    results = {
        'uploaded_files': [],
        'failed_files': [],
        'manifest_uri': None,
        'total_size': 0
    }
    
    # Upload each backup file
    for file_info in backup_files:
        local_file = file_info['file_path']
        file_name = file_info['file_name']
        object_name = f"{gcs_path}/{backup_type}/{timestamp}/{file_name}"
        
        try:
            logger.info(f"Uploading {local_file} to gs://{bucket_name}/{object_name}")
            gcs_uri = gcs_client.upload_file(
                local_file_path=local_file,
                bucket_name=bucket_name,
                object_name=object_name
            )
            
            results['uploaded_files'].append({
                'local_file': local_file,
                'gcs_uri': gcs_uri,
                'size': file_info['file_size']
            })
            results['total_size'] += file_info['file_size']
            logger.info(f"Upload successful: {gcs_uri}")
            
        except Exception as e:
            logger.error(f"Failed to upload {local_file}: {str(e)}")
            results['failed_files'].append({
                'local_file': local_file,
                'error': str(e)
            })
    
    # Upload manifest file last
    if manifest_path:
        manifest_name = os.path.basename(manifest_path)
        manifest_object = f"{gcs_path}/{backup_type}/{timestamp}/{manifest_name}"
        
        try:
            logger.info(f"Uploading manifest to gs://{bucket_name}/{manifest_object}")
            manifest_uri = gcs_client.upload_file(
                local_file_path=manifest_path,
                bucket_name=bucket_name,
                object_name=manifest_object
            )
            results['manifest_uri'] = manifest_uri
            logger.info(f"Manifest uploaded: {manifest_uri}")
            
        except Exception as e:
            logger.error(f"Failed to upload manifest: {str(e)}")
            results['failed_files'].append({
                'local_file': manifest_path,
                'error': str(e)
            })
    
    total_mb = results['total_size'] / (1024 * 1024)
    logger.info(f"Upload complete: {len(results['uploaded_files'])} files ({total_mb:.2f} MB)")
    
    return results

def cleanup_local_files(backup_files: List[Dict[str, Any]], manifest_path: str, backup_dir: str, is_temp_dir: bool) -> bool:
    """
    Remove local backup files after successful upload.
    
    Args:
        backup_files: List of backup files
        manifest_path: Path to manifest file
        backup_dir: Backup directory
        is_temp_dir: Whether the directory is temporary
        
    Returns:
        True if cleanup succeeded, False otherwise
    """
    if is_temp_dir:
        # Remove entire directory for temp dirs
        try:
            logger.info(f"Removing temporary directory: {backup_dir}")
            shutil.rmtree(backup_dir)
            return True
        except Exception as e:
            logger.error(f"Failed to remove temporary directory: {str(e)}")
            return False
    else:
        # Remove individual files
        success = True
        
        # Remove backup files
        for file_info in backup_files:
            try:
                file_path = file_info['file_path']
                logger.debug(f"Removing file: {file_path}")
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Failed to remove file {file_path}: {str(e)}")
                success = False
        
        # Remove manifest file
        if manifest_path and os.path.exists(manifest_path):
            try:
                logger.debug(f"Removing manifest: {manifest_path}")
                os.remove(manifest_path)
            except Exception as e:
                logger.error(f"Failed to remove manifest {manifest_path}: {str(e)}")
                success = False
        
        return success

def manage_backup_retention(config: Dict[str, Any]) -> int:
    """
    Apply retention policy to remove old backups.
    
    Args:
        config: Backup configuration
        
    Returns:
        Number of old backups removed
    """
    retention_days = config['retention_days']
    
    if retention_days <= 0:
        logger.info("Retention policy disabled (infinite retention)")
        return 0
    
    bucket_name = config['gcs_bucket']
    gcs_path = config['gcs_path']
    backup_type = config['backup_type']
    
    # Calculate cutoff date
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
    logger.info(f"Applying retention policy: removing backups older than {cutoff_date.isoformat()}")
    
    # List all manifests
    gcs_client = GCSClient()
    manifests = gcs_client.list_files(
        bucket_name=bucket_name,
        prefix=f"{gcs_path}/{backup_type}/"
    )
    
    # Filter manifest files
    manifest_files = [f for f in manifests if f.endswith('_manifest.json')]
    
    # Initialize counter
    removed_count = 0
    
    # Process each manifest
    for manifest_file in manifest_files:
        try:
            # Download manifest
            with tempfile.NamedTemporaryFile() as temp_file:
                gcs_client.download_file(
                    bucket_name=bucket_name,
                    object_name=manifest_file,
                    local_file_path=temp_file.name
                )
                
                # Load manifest content
                with open(temp_file.name, 'r') as f:
                    manifest = json.load(f)
            
            # Check timestamp
            timestamp = datetime.datetime.fromisoformat(manifest['timestamp'])
            
            if timestamp < cutoff_date:
                logger.info(f"Removing backup from {timestamp.isoformat()} (older than retention period)")
                
                # Delete all files referenced in manifest
                for file_info in manifest.get('files', []):
                    file_name = file_info['file_name']
                    file_dir = os.path.dirname(manifest_file)
                    file_path = f"{file_dir}/{file_name}"
                    
                    try:
                        gcs_client.delete_file(
                            bucket_name=bucket_name,
                            object_name=file_path
                        )
                        logger.debug(f"Deleted file: gs://{bucket_name}/{file_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete file {file_path}: {str(e)}")
                
                # Delete manifest file
                try:
                    gcs_client.delete_file(
                        bucket_name=bucket_name,
                        object_name=manifest_file
                    )
                    logger.debug(f"Deleted manifest: gs://{bucket_name}/{manifest_file}")
                except Exception as e:
                    logger.warning(f"Failed to delete manifest {manifest_file}: {str(e)}")
                
                removed_count += 1
                
        except Exception as e:
            logger.error(f"Error processing manifest {manifest_file}: {str(e)}")
    
    logger.info(f"Retention policy applied: removed {removed_count} old backups")
    return removed_count

class MetadataBackup:
    """
    Class that manages the Airflow metadata backup process.
    """
    
    def __init__(self, conn_id: str, backup_type: str, backup_dir: str, environment: str):
        """
        Initialize the MetadataBackup instance.
        
        Args:
            conn_id: Database connection ID
            backup_type: Type of backup (full/incremental)
            backup_dir: Directory to store backup files
            environment: Environment (dev/qa/prod)
        """
        self.conn_id = conn_id
        self.backup_type = backup_type
        self.backup_dir = backup_dir
        self.environment = environment
        
        # Validate backup type
        if backup_type not in BACKUP_TYPES:
            raise ValueError(f"Invalid backup type: {backup_type}. Must be one of: {', '.join(BACKUP_TYPES.keys())}")
        
        # Create backup directory if it doesn't exist
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Initialize empty data structures
        self.db_info = {}
        self.backup_files = []
        self.manifest_path = None
        self.stats = {
            'start_time': datetime.datetime.now().isoformat(),
            'backup_type': backup_type,
            'environment': environment,
            'status': 'pending',
            'files_backed_up': 0,
            'total_size_bytes': 0,
            'upload_stats': None,
            'cleanup_stats': None,
            'errors': []
        }
        
        # Get database information
        self.db_info = get_database_info(conn_id)
        
        logger.info(f"Initialized {backup_type} backup for environment: {environment}")
        logger.info(f"Database: {self.db_info['host']}:{self.db_info['port']}/{self.db_info['database']}")
        logger.info(f"Backup directory: {backup_dir}")
    
    def execute_backup(self, pg_dump_path: str) -> bool:
        """
        Execute the backup process.
        
        Args:
            pg_dump_path: Path to pg_dump executable
            
        Returns:
            True if backup succeeds, False otherwise
        """
        try:
            logger.info(f"Starting {self.backup_type} backup execution")
            
            # Perform the backup
            self.backup_files = backup_metadata_tables(
                config={
                    'backup_type': self.backup_type,
                    'backup_dir': self.backup_dir,
                    'pg_dump_path': pg_dump_path
                },
                db_info=self.db_info
            )
            
            # Generate manifest
            self.manifest_path = create_backup_manifest(
                config={
                    'backup_type': self.backup_type,
                    'backup_dir': self.backup_dir,
                    'environment': self.environment
                },
                db_info=self.db_info,
                backup_files=self.backup_files
            )
            
            # Update stats
            self.stats['files_backed_up'] = len(self.backup_files)
            self.stats['total_size_bytes'] = sum(f['file_size'] for f in self.backup_files)
            self.stats['status'] = 'completed'
            
            logger.info(f"Backup execution completed: {len(self.backup_files)} files, " +
                       f"{self.stats['total_size_bytes'] / (1024 * 1024):.2f} MB")
            
            return True
            
        except Exception as e:
            error_msg = f"Error executing backup: {str(e)}"
            logger.error(error_msg)
            self.stats['status'] = 'failed'
            self.stats['errors'].append(error_msg)
            return False
    
    def upload_backup(self, gcs_bucket: str, gcs_path: str) -> Dict[str, Any]:
        """
        Upload backup files to Google Cloud Storage.
        
        Args:
            gcs_bucket: GCS bucket name
            gcs_path: Path within the bucket
            
        Returns:
            Upload results including GCS URIs
        """
        try:
            if not self.backup_files:
                raise ValueError("No backup files to upload")
            
            upload_results = upload_to_gcs(
                backup_files=self.backup_files,
                manifest_path=self.manifest_path,
                config={
                    'gcs_bucket': gcs_bucket,
                    'gcs_path': gcs_path,
                    'environment': self.environment,
                    'backup_type': self.backup_type
                }
            )
            
            # Update stats
            self.stats['upload_stats'] = {
                'files_uploaded': len(upload_results['uploaded_files']),
                'files_failed': len(upload_results['failed_files']),
                'total_size_bytes': upload_results['total_size'],
                'manifest_uri': upload_results['manifest_uri']
            }
            
            return upload_results
            
        except Exception as e:
            error_msg = f"Error uploading backup: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {
                'uploaded_files': [],
                'failed_files': [{'error': error_msg}],
                'manifest_uri': None,
                'total_size': 0
            }
    
    def cleanup(self, is_temp_dir: bool) -> bool:
        """
        Clean up local backup files.
        
        Args:
            is_temp_dir: Whether backup directory is temporary
            
        Returns:
            True if cleanup succeeds
        """
        try:
            cleanup_success = cleanup_local_files(
                backup_files=self.backup_files,
                manifest_path=self.manifest_path,
                backup_dir=self.backup_dir,
                is_temp_dir=is_temp_dir
            )
            
            # Update stats
            self.stats['cleanup_stats'] = {
                'success': cleanup_success,
                'is_temp_dir': is_temp_dir
            }
            
            return cleanup_success
            
        except Exception as e:
            error_msg = f"Error during cleanup: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get backup statistics.
        
        Returns:
            Backup statistics
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
    Main function to orchestrate the backup process.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Parse arguments
        args = parse_args()
        
        # Set up logging
        setup_logging(args.verbose)
        
        # Get backup configuration
        config = get_backup_config(args)
        logger.info(f"Backup configuration: {config}")
        
        # Create backup instance
        backup = MetadataBackup(
            conn_id=config['conn_id'],
            backup_type=config['backup_type'],
            backup_dir=config['backup_dir'],
            environment=config['environment']
        )
        
        # Execute backup
        backup_success = backup.execute_backup(
            pg_dump_path=config['pg_dump_path']
        )
        
        if not backup_success:
            logger.error("Backup execution failed")
            # Send alert
            send_alert(
                alert_level=AlertLevel.ERROR,
                context={
                    'dag_id': 'backup_metadata',
                    'task_id': 'backup_execution',
                    'execution_date': datetime.datetime.now().isoformat(),
                },
                subject_template="Airflow Metadata Backup Failed",
                body_template="Backup execution failed for {{ environment }} environment. See logs for details."
            )
            return 1
        
        # Upload to GCS if enabled
        if config['upload_enabled']:
            upload_results = backup.upload_backup(
                gcs_bucket=config['gcs_bucket'],
                gcs_path=config['gcs_path']
            )
            
            if len(upload_results['failed_files']) > 0:
                logger.warning("Some files failed to upload")
        
        # Apply retention policy if specified
        if config['retention_days'] > 0 and config['upload_enabled']:
            removed_count = manage_backup_retention(config)
            logger.info(f"Retention policy applied: {removed_count} old backups removed")
        
        # Clean up local files
        cleanup_success = backup.cleanup(config['is_temp_dir'])
        
        # Get and log statistics
        stats = backup.get_stats()
        logger.info(f"Backup completed in {stats['duration_seconds']:.2f} seconds")
        logger.info(f"Files backed up: {stats['files_backed_up']}")
        logger.info(f"Total size: {stats['total_size_bytes'] / (1024 * 1024):.2f} MB")
        
        # Send completion alert
        send_alert(
            alert_level=AlertLevel.INFO,
            context={
                'dag_id': 'backup_metadata',
                'task_id': 'backup_execution',
                'execution_date': datetime.datetime.now().isoformat(),
                'backup_type': config['backup_type'],
                'environment': config['environment'],
                'stats': stats
            },
            subject_template="Airflow Metadata Backup Completed",
            body_template="Backup completed for {{ environment }} environment.\n" +
                         "Type: {{ backup_type }}\n" +
                         "Files: {{ stats.files_backed_up }}\n" +
                         "Size: {{ stats.total_size_bytes / 1024 / 1024 }} MB"
        )
        
        return 0
        
    except Exception as e:
        logger.error(f"Unhandled exception during backup: {str(e)}")
        logger.exception(e)
        
        # Send alert for unhandled exceptions
        send_alert(
            alert_level=AlertLevel.CRITICAL,
            context={
                'dag_id': 'backup_metadata',
                'task_id': 'backup_execution',
                'execution_date': datetime.datetime.now().isoformat(),
            },
            exception=e,
            subject_template="Critical Error in Airflow Metadata Backup",
            body_template="Unhandled exception occurred during backup: {{ exception }}"
        )
        
        return 1

if __name__ == "__main__":
    sys.exit(main())