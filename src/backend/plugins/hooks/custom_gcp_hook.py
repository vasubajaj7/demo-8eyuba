"""
Custom Google Cloud Platform hook for Apache Airflow 2.X.

This module provides an enhanced hook for Google Cloud Platform services,
extending the standard hooks provided in the Airflow provider packages.
It offers streamlined interfaces for GCS, BigQuery, and Secret Manager operations
with improved error handling, logging, and compatibility between Airflow 1.10.15
and Airflow 2.X for migration purposes.
"""

import os
import logging
from typing import Any, Dict, List, Optional, Union

# Airflow imports
from airflow.hooks.base import BaseHook  # airflow v2.0.0+
from airflow.models import Connection  # airflow v2.0.0+
from airflow.exceptions import AirflowException  # airflow v2.0.0+

# GCP provider hooks
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # airflow-providers-google v2.0.0+
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # airflow-providers-google v2.0.0+
from airflow.providers.google.cloud.hooks.secret_manager import SecretManagerHook  # airflow-providers-google v2.0.0+

# Third-party imports
import google.cloud.storage  # google-cloud-storage v2.0.0+
import google.cloud.bigquery  # google-cloud-bigquery v2.0.0+
import google.cloud.secretmanager  # google-cloud-secret-manager v2.0.0+
import pandas  # pandas v1.3.0+
from pandas import DataFrame

# Internal imports
from dags.utils.gcp_utils import get_gcp_connection, initialize_gcp_client

# Set up logging
logger = logging.getLogger(__name__)

# Global constants
DEFAULT_GCP_CONN_ID = 'google_cloud_default'
DEFAULT_CHUNK_SIZE = 104857600  # 100 MB in bytes


def _validate_service_account_key_file(key_file: str) -> bool:
    """
    Validates the service account key file exists and is readable.
    
    Args:
        key_file: Path to the service account key file
        
    Returns:
        True if valid, False otherwise
    """
    if key_file is None:
        logger.warning("No service account key file provided")
        return False
    
    if not os.path.exists(key_file):
        logger.warning(f"Service account key file does not exist: {key_file}")
        return False
    
    if not os.access(key_file, os.R_OK):
        logger.warning(f"Service account key file is not readable: {key_file}")
        return False
    
    return True


def _validate_client_options(client_options: Dict) -> Dict:
    """
    Validates client options for GCP clients.
    
    Args:
        client_options: Dictionary of client options
        
    Returns:
        Validated client options dictionary
        
    Raises:
        ValueError: If invalid options are provided
    """
    if client_options is None:
        return {}
    
    if not isinstance(client_options, dict):
        raise ValueError("client_options must be a dictionary")
    
    # Return a copy to avoid modifying the original
    return client_options.copy()


class CustomGCPHook(BaseHook):
    """
    Enhanced hook for Google Cloud Platform services that provides standardized
    interfaces for GCS, BigQuery, and Secret Manager operations.
    
    This hook extends the standard Airflow provider hooks with improved error
    handling, logging, and compatibility features to support migration from
    Airflow 1.10.15 to Airflow 2.X.
    """
    
    def __init__(self, gcp_conn_id: str = DEFAULT_GCP_CONN_ID, delegate_to: Optional[str] = None):
        """
        Initialize the CustomGCPHook with connection ID and delegation.
        
        Args:
            gcp_conn_id: Airflow connection ID for GCP
            delegate_to: The account to impersonate using domain-wide delegation of authority
        """
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        
        # Initialize hook attributes to None for lazy initialization
        self._gcs_hook = None
        self._bigquery_hook = None
        self._secretmanager_hook = None
        self._storage_client = None
        self._bigquery_client = None
        self._secretmanager_client = None
    
    def get_conn(self) -> Connection:
        """
        Gets the connection object for GCP.
        
        Returns:
            Airflow connection object for GCP
            
        Raises:
            AirflowException: If connection not found or invalid
        """
        try:
            conn = get_gcp_connection(self.gcp_conn_id)
            
            if not conn.conn_type == 'google_cloud_platform':
                raise AirflowException(
                    f"Connection {self.gcp_conn_id} is not of type 'google_cloud_platform', "
                    f"got '{conn.conn_type}' instead"
                )
            
            # Log connection details (excluding sensitive information)
            logger.debug(f"Using GCP connection: {self.gcp_conn_id}")
            
            return conn
        except AirflowException as e:
            logger.error(f"Failed to get GCP connection '{self.gcp_conn_id}': {str(e)}")
            raise
    
    def get_gcs_hook(self) -> GCSHook:
        """
        Get or create GCSHook instance.
        
        Returns:
            Initialized GCSHook
        """
        if self._gcs_hook is None:
            self._gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        return self._gcs_hook
    
    def get_bigquery_hook(self) -> BigQueryHook:
        """
        Get or create BigQueryHook instance.
        
        Returns:
            Initialized BigQueryHook
        """
        if self._bigquery_hook is None:
            self._bigquery_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        return self._bigquery_hook
    
    def get_secretmanager_hook(self) -> SecretManagerHook:
        """
        Get or create SecretManagerHook instance.
        
        Returns:
            Initialized SecretManagerHook
        """
        if self._secretmanager_hook is None:
            self._secretmanager_hook = SecretManagerHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        return self._secretmanager_hook
    
    def get_storage_client(self) -> google.cloud.storage.Client:
        """
        Get or create GCS Client instance.
        
        Returns:
            Authenticated GCS client
        """
        if self._storage_client is None:
            self._storage_client = initialize_gcp_client('storage', self.gcp_conn_id)
        return self._storage_client
    
    def get_bigquery_client(self) -> google.cloud.bigquery.Client:
        """
        Get or create BigQuery Client instance.
        
        Returns:
            Authenticated BigQuery client
        """
        if self._bigquery_client is None:
            self._bigquery_client = initialize_gcp_client('bigquery', self.gcp_conn_id)
        return self._bigquery_client
    
    def get_secretmanager_client(self) -> google.cloud.secretmanager.SecretManagerServiceClient:
        """
        Get or create Secret Manager Client instance.
        
        Returns:
            Authenticated Secret Manager client
        """
        if self._secretmanager_client is None:
            self._secretmanager_client = initialize_gcp_client('secretmanager', self.gcp_conn_id)
        return self._secretmanager_client
    
    # GCS Operations
    
    def gcs_file_exists(self, bucket_name: str, object_name: str) -> bool:
        """
        Check if a file exists in Google Cloud Storage.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to check
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            if not bucket_name or not object_name:
                raise ValueError("Both bucket_name and object_name must be provided")
            
            hook = self.get_gcs_hook()
            exists = hook.exists(bucket_name=bucket_name, object_name=object_name)
            
            if exists:
                logger.info(f"File gs://{bucket_name}/{object_name} exists")
            else:
                logger.debug(f"File gs://{bucket_name}/{object_name} does not exist")
            
            return exists
        
        except Exception as e:
            logger.error(f"Error checking if gs://{bucket_name}/{object_name} exists: {str(e)}")
            return False
    
    def gcs_upload_file(
        self, 
        local_file_path: str, 
        bucket_name: str, 
        object_name: str,
        chunk_size: Optional[int] = None
    ) -> str:
        """
        Upload a local file to Google Cloud Storage.
        
        Args:
            local_file_path: Path to the local file to upload
            bucket_name: Name of the GCS bucket
            object_name: Name to give the uploaded object
            chunk_size: Size of chunks for uploading large files
            
        Returns:
            GCS URI of uploaded file (gs://bucket-name/object-name)
            
        Raises:
            AirflowException: If upload fails or file does not exist
        """
        try:
            if not local_file_path or not bucket_name or not object_name:
                raise ValueError("local_file_path, bucket_name, and object_name must be provided")
            
            # Check if local file exists
            if not os.path.exists(local_file_path):
                error_msg = f"Local file does not exist: {local_file_path}"
                logger.error(error_msg)
                raise AirflowException(error_msg)
            
            # Use default chunk size if none provided
            chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
            
            hook = self.get_gcs_hook()
            hook.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=local_file_path,
                chunk_size=chunk_size
            )
            
            file_size = os.path.getsize(local_file_path)
            logger.info(
                f"Successfully uploaded {local_file_path} "
                f"({file_size} bytes) to gs://{bucket_name}/{object_name}"
            )
            
            return f"gs://{bucket_name}/{object_name}"
            
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path} to GCS: {str(e)}")
            raise AirflowException(f"Failed to upload file to GCS: {str(e)}")
    
    def gcs_download_file(
        self, 
        bucket_name: str, 
        object_name: str, 
        local_file_path: str
    ) -> str:
        """
        Download a file from Google Cloud Storage to a local path.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to download
            local_file_path: Local path where the file should be saved
            
        Returns:
            Local path of downloaded file
            
        Raises:
            AirflowException: If download fails
        """
        try:
            if not bucket_name or not object_name or not local_file_path:
                raise ValueError("bucket_name, object_name, and local_file_path must be provided")
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            hook = self.get_gcs_hook()
            hook.download(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=local_file_path
            )
            
            # Get file size after download
            file_size = os.path.getsize(local_file_path)
            logger.info(
                f"Successfully downloaded gs://{bucket_name}/{object_name} "
                f"({file_size} bytes) to {local_file_path}"
            )
            
            return local_file_path
            
        except Exception as e:
            logger.error(f"Failed to download from GCS: {str(e)}")
            raise AirflowException(f"Failed to download file from GCS: {str(e)}")
    
    def gcs_list_files(
        self, 
        bucket_name: str, 
        prefix: Optional[str] = None, 
        delimiter: Optional[bool] = None
    ) -> List[str]:
        """
        List files in a Google Cloud Storage bucket with optional prefix filter.
        
        Args:
            bucket_name: Name of the GCS bucket
            prefix: Prefix to filter objects (optional)
            delimiter: Whether to use a delimiter (folder-like listing)
            
        Returns:
            List of object names matching the prefix
        """
        try:
            if not bucket_name:
                raise ValueError("bucket_name must be provided")
            
            prefix = prefix or ''
            delimiter_char = '/' if delimiter else None
            
            hook = self.get_gcs_hook()
            objects = hook.list(
                bucket_name=bucket_name,
                prefix=prefix,
                delimiter=delimiter_char
            )
            
            logger.info(f"Found {len(objects)} files in gs://{bucket_name}/{prefix}")
            return objects
            
        except Exception as e:
            logger.error(f"Failed to list files in gs://{bucket_name}/{prefix or ''}: {str(e)}")
            return []
    
    def gcs_delete_file(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete a file from Google Cloud Storage.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not bucket_name or not object_name:
                raise ValueError("Both bucket_name and object_name must be provided")
            
            hook = self.get_gcs_hook()
            hook.delete(bucket_name=bucket_name, object_name=object_name)
            
            logger.info(f"Successfully deleted gs://{bucket_name}/{object_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete gs://{bucket_name}/{object_name}: {str(e)}")
            return False
    
    # BigQuery Operations
    
    def bigquery_execute_query(
        self, 
        sql: str, 
        query_params: Optional[Dict] = None, 
        location: Optional[str] = None,
        as_dataframe: Optional[bool] = False
    ) -> Union[List, DataFrame]:
        """
        Execute a BigQuery SQL query and return results.
        
        Args:
            sql: SQL query to execute
            query_params: Query parameters (optional)
            location: BigQuery dataset location (optional)
            as_dataframe: Return results as pandas DataFrame if True
            
        Returns:
            Query results as list or pandas DataFrame
            
        Raises:
            AirflowException: If query execution fails
        """
        try:
            if not sql:
                raise ValueError("SQL query must be provided")
            
            query_params = query_params or {}
            location = location or 'US'
            
            hook = self.get_bigquery_hook()
            
            start_time = pandas.Timestamp.now()
            
            if as_dataframe:
                results = hook.get_pandas_df(
                    sql=sql,
                    parameters=query_params
                )
            else:
                results = hook.get_records(
                    sql=sql,
                    parameters=query_params
                )
            
            duration = (pandas.Timestamp.now() - start_time).total_seconds()
            row_count = len(results)
            
            logger.info(
                f"Query executed successfully in {duration:.2f}s, "
                f"returned {row_count} rows"
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to execute BigQuery query: {str(e)}")
            raise AirflowException(f"Failed to execute BigQuery query: {str(e)}")
    
    def bigquery_create_dataset(
        self, 
        dataset_id: str, 
        location: Optional[str] = None
    ) -> bool:
        """
        Create a BigQuery dataset if it doesn't exist.
        
        Args:
            dataset_id: ID of the dataset to create
            location: Geographic location of the dataset
            
        Returns:
            True if created or already exists, False on failure
        """
        try:
            if not dataset_id:
                raise ValueError("dataset_id must be provided")
            
            location = location or 'US'
            
            hook = self.get_bigquery_hook()
            client = hook.get_client(project_id=hook.project_id)
            
            try:
                client.get_dataset(dataset_id)
                logger.info(f"Dataset {dataset_id} already exists")
                return True
            except Exception:
                # Dataset doesn't exist, create it
                dataset = google.cloud.bigquery.Dataset(f"{client.project}.{dataset_id}")
                dataset.location = location
                client.create_dataset(dataset)
                
                logger.info(f"Successfully created dataset {dataset_id} in {location}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create dataset {dataset_id}: {str(e)}")
            return False
    
    def bigquery_create_table(
        self, 
        dataset_id: str, 
        table_id: str, 
        schema: List
    ) -> bool:
        """
        Create a BigQuery table with the specified schema.
        
        Args:
            dataset_id: ID of the dataset containing the table
            table_id: ID of the table to create
            schema: List of SchemaField objects defining the table schema
            
        Returns:
            True if created or already exists, False on failure
        """
        try:
            if not dataset_id or not table_id:
                raise ValueError("dataset_id and table_id must be provided")
            
            hook = self.get_bigquery_hook()
            client = hook.get_client(project_id=hook.project_id)
            
            table_ref = f"{client.project}.{dataset_id}.{table_id}"
            
            try:
                client.get_table(table_ref)
                logger.info(f"Table {table_ref} already exists")
                return True
            except Exception:
                # Table doesn't exist, create it
                table = google.cloud.bigquery.Table(table_ref, schema=schema)
                client.create_table(table)
                
                logger.info(f"Successfully created table {table_ref}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create table {dataset_id}.{table_id}: {str(e)}")
            return False
    
    def bigquery_load_data(
        self, 
        bucket_name: str, 
        object_name: str, 
        dataset_id: str, 
        table_id: str,
        source_format: Optional[str] = None
    ) -> bool:
        """
        Load data from GCS into a BigQuery table.
        
        Args:
            bucket_name: GCS bucket containing the data
            object_name: GCS object to load
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            source_format: Format of the data (CSV, JSON, AVRO, etc.)
            
        Returns:
            True if load job completed successfully, False otherwise
        """
        try:
            if not bucket_name or not object_name or not dataset_id or not table_id:
                raise ValueError("bucket_name, object_name, dataset_id, and table_id must be provided")
            
            source_format = source_format or 'CSV'
            
            hook = self.get_bigquery_hook()
            gcs_uri = f"gs://{bucket_name}/{object_name}"
            
            job_config = {
                'source_format': source_format,
                'skip_leading_rows': 1 if source_format == 'CSV' else 0,
                'allow_quoted_newlines': True if source_format == 'CSV' else False,
                'autodetect': True,
                'write_disposition': 'WRITE_TRUNCATE',
            }
            
            job = hook.run_load(
                destination_project_dataset_table=f"{dataset_id}.{table_id}",
                source_uris=[gcs_uri],
                schema_fields=None,  # Use autodetect
                **job_config
            )
            
            logger.info(
                f"Successfully loaded data from {gcs_uri} to "
                f"{dataset_id}.{table_id}, job_id: {job.job_id}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data from GCS to BigQuery: {str(e)}")
            return False
    
    # Secret Manager Operations
    
    def get_secret(
        self, 
        secret_id: str, 
        version_id: Optional[str] = None
    ) -> str:
        """
        Retrieve a secret from Google Secret Manager.
        
        Args:
            secret_id: The ID of the secret to retrieve
            version_id: The version of the secret (default: 'latest')
            
        Returns:
            Secret value
            
        Raises:
            AirflowException: If secret retrieval fails
        """
        try:
            if not secret_id:
                raise ValueError("secret_id must be provided")
            
            version_id = version_id or 'latest'
            
            hook = self.get_secretmanager_hook()
            secret = hook.get_secret(secret_id=secret_id, secret_version=version_id)
            
            logger.info(f"Successfully retrieved secret {secret_id} (version: {version_id})")
            return secret
            
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_id}: {str(e)}")
            raise AirflowException(f"Failed to retrieve secret {secret_id}: {str(e)}")
    
    def create_secret(
        self, 
        secret_id: str, 
        secret_value: str
    ) -> bool:
        """
        Create a new secret in Google Secret Manager.
        
        Args:
            secret_id: The ID to give the new secret
            secret_value: The value of the secret
            
        Returns:
            True if created successfully, False otherwise
        """
        try:
            if not secret_id or secret_value is None:
                raise ValueError("secret_id and secret_value must be provided")
            
            hook = self.get_secretmanager_hook()
            conn = self.get_conn()
            project_id = conn.extra_dejson.get('project')
            
            # First check if secret exists
            client = hook.get_conn()
            secret_path = f"projects/{project_id}/secrets/{secret_id}"
            
            try:
                client.get_secret(name=secret_path)
                logger.info(f"Secret {secret_id} already exists, adding new version")
            except Exception:
                # Secret doesn't exist, create it
                parent = f"projects/{project_id}"
                client.create_secret(
                    parent=parent,
                    secret_id=secret_id,
                    secret={"replication": {"automatic": {}}}
                )
                logger.info(f"Created new secret {secret_id}")
            
            # Add the secret version
            client.add_secret_version(
                parent=secret_path,
                payload={"data": secret_value.encode("UTF-8")}
            )
            
            logger.info(f"Successfully added new version to secret {secret_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create/update secret {secret_id}: {str(e)}")
            return False