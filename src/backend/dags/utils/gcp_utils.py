"""
Google Cloud Platform utilities for Apache Airflow 2.X DAGs.

This module provides a centralized set of functions and classes for interacting 
with GCP services like Cloud Storage, BigQuery, and Secret Manager.
It abstracts common GCP operations to simplify DAG development while ensuring
consistent error handling and logging.

The utilities are compatible with Airflow 2.X provider packages, supporting the
migration from Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2.
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Union, Optional, Any, TypeVar, cast

# Google Cloud libraries
# google-cloud-storage v2.0.0+
import google.cloud.storage
from google.cloud.storage import Client as StorageClient
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket

# google-cloud-bigquery v2.0.0+
import google.cloud.bigquery
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import LoadJobConfig, QueryJobConfig
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.bigquery.dataset import DatasetReference

# google-cloud-secret-manager v2.0.0+
import google.cloud.secretmanager
from google.cloud.secretmanager import SecretManagerServiceClient
from google.cloud.secretmanager_v1.types import Secret, SecretVersion

# pandas v1.3.0+
import pandas as pd
from pandas import DataFrame

# Airflow imports
# apache-airflow v2.0.0+
from airflow.models import Connection
from airflow.hooks.base import BaseHook

# apache-airflow-providers-google v2.0.0+
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretManagerHook
from airflow.exceptions import AirflowException

# Set up logging
logger = logging.getLogger('airflow.utils.gcp')

# Global constants
DEFAULT_GCP_CONN_ID = 'google_cloud_default'
DEFAULT_CHUNK_SIZE = 104857600  # 100 MB in bytes


def get_gcp_connection(conn_id: str = DEFAULT_GCP_CONN_ID) -> Connection:
    """
    Retrieve a GCP connection from Airflow's connection storage.
    
    Args:
        conn_id: The connection ID to retrieve (defaults to google_cloud_default)
        
    Returns:
        Connection object with credentials
        
    Raises:
        AirflowException: If connection cannot be found or is invalid
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        
        if not conn.conn_type == 'google_cloud_platform':
            raise AirflowException(
                f"Connection {conn_id} is not of type 'google_cloud_platform', "
                f"got '{conn.conn_type}' instead"
            )
            
        logger.debug(f"Retrieved GCP connection: {conn_id}")
        return conn
    except AirflowException as e:
        logger.error(f"Failed to get GCP connection '{conn_id}': {str(e)}")
        raise


def initialize_gcp_client(service_name: str, conn_id: str = DEFAULT_GCP_CONN_ID) -> Any:
    """
    Initialize a Google Cloud client for a specific service.
    
    Args:
        service_name: The GCP service to initialize ('storage', 'bigquery', or 'secretmanager')
        conn_id: The Airflow connection ID to use for authentication
    
    Returns:
        Authenticated GCP client for specified service
        
    Raises:
        AirflowException: If client initialization fails
    """
    try:
        conn = get_gcp_connection(conn_id)
        
        client = None
        if service_name.lower() == 'storage':
            client = StorageClient(
                project=conn.extra_dejson.get('project'),
                credentials=conn._get_credentials()
            )
        elif service_name.lower() == 'bigquery':
            client = BigQueryClient(
                project=conn.extra_dejson.get('project'),
                credentials=conn._get_credentials()
            )
        elif service_name.lower() == 'secretmanager':
            client = SecretManagerServiceClient(
                credentials=conn._get_credentials()
            )
        else:
            raise AirflowException(f"Unsupported GCP service: {service_name}")
        
        logger.info(f"Initialized {service_name} client with connection {conn_id}")
        return client
    
    except Exception as e:
        logger.error(f"Failed to initialize {service_name} client: {str(e)}")
        raise AirflowException(f"Failed to initialize {service_name} client: {str(e)}")


def gcs_file_exists(bucket_name: str, object_name: str, 
                    conn_id: str = DEFAULT_GCP_CONN_ID) -> bool:
    """
    Check if a file exists in Google Cloud Storage.
    
    Args:
        bucket_name: Name of the GCS bucket
        object_name: Name of the object to check
        conn_id: Airflow connection ID for GCP
        
    Returns:
        True if file exists, False otherwise
    """
    try:
        hook = GCSHook(gcp_conn_id=conn_id)
        exists = hook.exists(bucket_name=bucket_name, object_name=object_name)
        
        if exists:
            logger.info(f"File gs://{bucket_name}/{object_name} exists")
        else:
            logger.info(f"File gs://{bucket_name}/{object_name} does not exist")
            
        return exists
    
    except Exception as e:
        logger.error(f"Error checking if gs://{bucket_name}/{object_name} exists: {str(e)}")
        return False


def gcs_upload_file(local_file_path: str, bucket_name: str, object_name: str,
                    conn_id: str = DEFAULT_GCP_CONN_ID, 
                    chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    """
    Upload a local file to Google Cloud Storage.
    
    Args:
        local_file_path: Path to the local file to upload
        bucket_name: Name of the GCS bucket
        object_name: Name to give the uploaded object
        conn_id: Airflow connection ID for GCP
        chunk_size: Size of chunks for uploading large files
        
    Returns:
        GCS URI of uploaded file (gs://bucket-name/object-name)
        
    Raises:
        AirflowException: If upload fails or file does not exist
    """
    local_path = Path(local_file_path)
    
    if not local_path.exists():
        error_msg = f"Local file does not exist: {local_file_path}"
        logger.error(error_msg)
        raise AirflowException(error_msg)
    
    try:
        hook = GCSHook(gcp_conn_id=conn_id)
        hook.upload(
            bucket_name=bucket_name,
            object_name=object_name,
            filename=local_file_path,
            chunk_size=chunk_size
        )
        
        file_size = local_path.stat().st_size
        logger.info(
            f"Successfully uploaded {local_file_path} "
            f"({file_size} bytes) to gs://{bucket_name}/{object_name}"
        )
        
        return f"gs://{bucket_name}/{object_name}"
    
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to GCS: {str(e)}")
        raise AirflowException(f"Failed to upload file to GCS: {str(e)}")


def gcs_download_file(bucket_name: str, object_name: str, local_file_path: str,
                      conn_id: str = DEFAULT_GCP_CONN_ID) -> str:
    """
    Download a file from Google Cloud Storage to a local path.
    
    Args:
        bucket_name: Name of the GCS bucket
        object_name: Name of the object to download
        local_file_path: Local path where the file should be saved
        conn_id: Airflow connection ID for GCP
        
    Returns:
        Local path of downloaded file
        
    Raises:
        AirflowException: If download fails
    """
    try:
        # Ensure the directory exists
        local_path = Path(local_file_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        hook = GCSHook(gcp_conn_id=conn_id)
        hook.download(
            bucket_name=bucket_name,
            object_name=object_name,
            filename=local_file_path
        )
        
        # Get file size after download
        file_size = local_path.stat().st_size
        logger.info(
            f"Successfully downloaded gs://{bucket_name}/{object_name} "
            f"({file_size} bytes) to {local_file_path}"
        )
        
        return local_file_path
    
    except Exception as e:
        logger.error(f"Failed to download from GCS: {str(e)}")
        raise AirflowException(f"Failed to download file from GCS: {str(e)}")


def gcs_list_files(bucket_name: str, prefix: str = None, delimiter: bool = False,
                   conn_id: str = DEFAULT_GCP_CONN_ID) -> List[str]:
    """
    List files in a Google Cloud Storage bucket with optional prefix filter.
    
    Args:
        bucket_name: Name of the GCS bucket
        prefix: Prefix to filter objects (optional)
        delimiter: Whether to use a delimiter (folder-like listing) 
        conn_id: Airflow connection ID for GCP
        
    Returns:
        List of object names matching the prefix
    """
    try:
        hook = GCSHook(gcp_conn_id=conn_id)
        delimiter_char = '/' if delimiter else None
        
        objects = hook.list(
            bucket_name=bucket_name,
            prefix=prefix,
            delimiter=delimiter_char
        )
        
        logger.info(f"Found {len(objects)} files in gs://{bucket_name}/{prefix or ''}")
        return objects
    
    except Exception as e:
        logger.error(f"Failed to list files in gs://{bucket_name}/{prefix or ''}: {str(e)}")
        return []


def gcs_delete_file(bucket_name: str, object_name: str,
                    conn_id: str = DEFAULT_GCP_CONN_ID) -> bool:
    """
    Delete a file from Google Cloud Storage.
    
    Args:
        bucket_name: Name of the GCS bucket
        object_name: Name of the object to delete
        conn_id: Airflow connection ID for GCP
        
    Returns:
        True if successful, False otherwise
    """
    try:
        hook = GCSHook(gcp_conn_id=conn_id)
        hook.delete(bucket_name=bucket_name, object_name=object_name)
        
        logger.info(f"Successfully deleted gs://{bucket_name}/{object_name}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to delete gs://{bucket_name}/{object_name}: {str(e)}")
        return False


def bigquery_execute_query(sql: str, query_params: Dict = None, location: str = None,
                          conn_id: str = DEFAULT_GCP_CONN_ID, 
                          as_dataframe: bool = False) -> Union[List, DataFrame]:
    """
    Execute a BigQuery SQL query and return results.
    
    Args:
        sql: SQL query to execute
        query_params: Query parameters (optional)
        location: BigQuery dataset location (optional)
        conn_id: Airflow connection ID for GCP
        as_dataframe: Return results as pandas DataFrame if True
        
    Returns:
        Query results as list or pandas DataFrame
        
    Raises:
        AirflowException: If query execution fails
    """
    try:
        start_time = pd.Timestamp.now()
        hook = BigQueryHook(gcp_conn_id=conn_id, location=location)
        
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
            
        duration = (pd.Timestamp.now() - start_time).total_seconds()
        row_count = len(results)
        
        logger.info(
            f"Query executed successfully in {duration:.2f}s, "
            f"returned {row_count} rows"
        )
        
        return results
    
    except Exception as e:
        logger.error(f"Failed to execute BigQuery query: {str(e)}")
        raise AirflowException(f"Failed to execute BigQuery query: {str(e)}")


def bigquery_create_dataset(dataset_id: str, location: str = 'US',
                           conn_id: str = DEFAULT_GCP_CONN_ID) -> bool:
    """
    Create a BigQuery dataset if it doesn't exist.
    
    Args:
        dataset_id: ID of the dataset to create
        location: Geographic location of the dataset
        conn_id: Airflow connection ID for GCP
        
    Returns:
        True if created or already exists, False on failure
    """
    try:
        hook = BigQueryHook(gcp_conn_id=conn_id)
        conn = hook.get_conn()
        client = hook.get_client(project_id=conn.project)
        
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


def bigquery_create_table(dataset_id: str, table_id: str, schema: List,
                         conn_id: str = DEFAULT_GCP_CONN_ID) -> bool:
    """
    Create a BigQuery table with the specified schema.
    
    Args:
        dataset_id: ID of the dataset containing the table
        table_id: ID of the table to create
        schema: List of SchemaField objects defining the table schema
        conn_id: Airflow connection ID for GCP
        
    Returns:
        True if created or already exists, False on failure
    """
    try:
        hook = BigQueryHook(gcp_conn_id=conn_id)
        conn = hook.get_conn()
        client = hook.get_client(project_id=conn.project)
        
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


def bigquery_load_data(bucket_name: str, object_name: str, dataset_id: str, table_id: str,
                      source_format: str = 'CSV', conn_id: str = DEFAULT_GCP_CONN_ID) -> bool:
    """
    Load data from GCS into a BigQuery table.
    
    Args:
        bucket_name: GCS bucket containing the data
        object_name: GCS object to load
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        source_format: Format of the data (CSV, JSON, AVRO, etc.)
        conn_id: Airflow connection ID for GCP
        
    Returns:
        True if load job completed successfully, False otherwise
    """
    try:
        hook = BigQueryHook(gcp_conn_id=conn_id)
        gcs_uri = f"gs://{bucket_name}/{object_name}"
        
        job_config = {
            'source_format': source_format,
            'skip_leading_rows': 1 if source_format == 'CSV' else 0,
            'allow_quoted_newlines': True if source_format == 'CSV' else False,
            'allow_jagged_rows': True if source_format == 'CSV' else False,
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


def get_secret(secret_id: str, version_id: str = 'latest',
              conn_id: str = DEFAULT_GCP_CONN_ID) -> str:
    """
    Retrieve a secret from Google Secret Manager.
    
    Args:
        secret_id: The ID of the secret to retrieve
        version_id: The version of the secret (default: 'latest')
        conn_id: Airflow connection ID for GCP
        
    Returns:
        Secret value as a string
        
    Raises:
        AirflowException: If secret retrieval fails
    """
    try:
        hook = SecretManagerHook(gcp_conn_id=conn_id)
        secret = hook.get_secret(secret_id=secret_id, secret_version=version_id)
        
        logger.info(f"Successfully retrieved secret {secret_id} (version: {version_id})")
        return secret
    
    except Exception as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {str(e)}")
        raise AirflowException(f"Failed to retrieve secret {secret_id}: {str(e)}")


def create_secret(secret_id: str, secret_value: str,
                 conn_id: str = DEFAULT_GCP_CONN_ID) -> bool:
    """
    Create a new secret in Google Secret Manager.
    
    Args:
        secret_id: The ID to give the new secret
        secret_value: The value of the secret
        conn_id: Airflow connection ID for GCP
        
    Returns:
        True if created successfully, False otherwise
    """
    try:
        hook = SecretManagerHook(gcp_conn_id=conn_id)
        conn = get_gcp_connection(conn_id)
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


def dataframe_to_bigquery(dataframe: DataFrame, dataset_id: str, table_id: str,
                         conn_id: str = DEFAULT_GCP_CONN_ID, 
                         if_exists: str = 'replace') -> bool:
    """
    Load pandas DataFrame directly to BigQuery table.
    
    Args:
        dataframe: Pandas DataFrame to load
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        conn_id: Airflow connection ID for GCP
        if_exists: Action if table exists ('fail', 'replace', or 'append')
        
    Returns:
        True if load job completed successfully, False otherwise
        
    Raises:
        AirflowException: If loading fails
    """
    try:
        if dataframe.empty:
            logger.warning("DataFrame is empty, no data to load to BigQuery")
            return False
        
        hook = BigQueryHook(gcp_conn_id=conn_id)
        conn = hook.get_conn()
        client = hook.get_client(project_id=conn.project)
        
        full_table_id = f"{client.project}.{dataset_id}.{table_id}"
        
        # First make sure the dataset exists
        if not bigquery_create_dataset(dataset_id=dataset_id, conn_id=conn_id):
            raise AirflowException(f"Failed to create dataset {dataset_id}")
        
        # Upload the DataFrame
        job_config = google.cloud.bigquery.LoadJobConfig()
        
        if if_exists == 'replace':
            job_config.write_disposition = 'WRITE_TRUNCATE'
        elif if_exists == 'append':
            job_config.write_disposition = 'WRITE_APPEND'
        else:
            job_config.write_disposition = 'WRITE_EMPTY'
        
        job = client.load_table_from_dataframe(
            dataframe=dataframe,
            destination=full_table_id,
            job_config=job_config
        )
        
        # Wait for the job to complete
        job.result()
        
        logger.info(
            f"Successfully loaded DataFrame with {len(dataframe)} rows to "
            f"{full_table_id}, job_id: {job.job_id}"
        )
        
        return True
    
    except Exception as e:
        logger.error(f"Failed to load DataFrame to BigQuery: {str(e)}")
        raise AirflowException(f"Failed to load DataFrame to BigQuery: {str(e)}")


class GCSClient:
    """
    Helper class that provides simplified access to Google Cloud Storage.
    
    This class wraps the GCS functions to provide an object-oriented interface
    and maintains connection state.
    """
    
    def __init__(self, conn_id: str = DEFAULT_GCP_CONN_ID):
        """
        Initialize the GCSClient.
        
        Args:
            conn_id: Airflow connection ID for GCP
        """
        self.conn_id = conn_id
        self.client = None
        self.hook = None
    
    def get_hook(self) -> GCSHook:
        """
        Get or create a GCSHook instance.
        
        Returns:
            Instance of GCSHook
        """
        if self.hook is None:
            self.hook = GCSHook(gcp_conn_id=self.conn_id)
        return self.hook
    
    def get_client(self) -> StorageClient:
        """
        Get or create a Google Cloud Storage client.
        
        Returns:
            Authenticated GCS client
        """
        if self.client is None:
            self.client = initialize_gcp_client('storage', self.conn_id)
        return self.client
    
    def file_exists(self, bucket_name: str, object_name: str) -> bool:
        """
        Check if a file exists in Google Cloud Storage.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to check
            
        Returns:
            True if file exists, False otherwise
        """
        return gcs_file_exists(
            bucket_name=bucket_name,
            object_name=object_name,
            conn_id=self.conn_id
        )
    
    def upload_file(self, local_file_path: str, bucket_name: str, 
                    object_name: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
        """
        Upload a local file to Google Cloud Storage.
        
        Args:
            local_file_path: Path to the local file to upload
            bucket_name: Name of the GCS bucket
            object_name: Name to give the uploaded object
            chunk_size: Size of chunks for uploading large files
            
        Returns:
            GCS URI of uploaded file (gs://bucket-name/object-name)
        """
        return gcs_upload_file(
            local_file_path=local_file_path,
            bucket_name=bucket_name,
            object_name=object_name,
            conn_id=self.conn_id,
            chunk_size=chunk_size
        )
    
    def download_file(self, bucket_name: str, object_name: str, 
                      local_file_path: str) -> str:
        """
        Download a file from Google Cloud Storage.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to download
            local_file_path: Local path where the file should be saved
            
        Returns:
            Local path of downloaded file
        """
        return gcs_download_file(
            bucket_name=bucket_name,
            object_name=object_name,
            local_file_path=local_file_path,
            conn_id=self.conn_id
        )
    
    def list_files(self, bucket_name: str, prefix: str = None, 
                   delimiter: bool = False) -> List[str]:
        """
        List files in a bucket with optional prefix.
        
        Args:
            bucket_name: Name of the GCS bucket
            prefix: Prefix to filter objects (optional)
            delimiter: Whether to use a delimiter (folder-like listing)
            
        Returns:
            List of object names
        """
        return gcs_list_files(
            bucket_name=bucket_name,
            prefix=prefix,
            delimiter=delimiter,
            conn_id=self.conn_id
        )
    
    def delete_file(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete a file from Google Cloud Storage.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to delete
            
        Returns:
            True if successful
        """
        return gcs_delete_file(
            bucket_name=bucket_name,
            object_name=object_name,
            conn_id=self.conn_id
        )


class BigQueryClient:
    """
    Helper class that provides simplified access to BigQuery.
    
    This class wraps the BigQuery functions to provide an object-oriented interface
    and maintains connection state.
    """
    
    def __init__(self, conn_id: str = DEFAULT_GCP_CONN_ID):
        """
        Initialize the BigQueryClient.
        
        Args:
            conn_id: Airflow connection ID for GCP
        """
        self.conn_id = conn_id
        self.client = None
        self.hook = None
    
    def get_hook(self) -> BigQueryHook:
        """
        Get or create a BigQueryHook instance.
        
        Returns:
            Instance of BigQueryHook
        """
        if self.hook is None:
            self.hook = BigQueryHook(gcp_conn_id=self.conn_id)
        return self.hook
    
    def get_client(self) -> BigQueryClient:
        """
        Get or create a BigQuery client.
        
        Returns:
            Authenticated BigQuery client
        """
        if self.client is None:
            self.client = initialize_gcp_client('bigquery', self.conn_id)
        return self.client
    
    def execute_query(self, sql: str, query_params: Dict = None,
                      location: str = None, as_dataframe: bool = False) -> Union[List, DataFrame]:
        """
        Execute a BigQuery SQL query.
        
        Args:
            sql: SQL query to execute
            query_params: Query parameters (optional)
            location: BigQuery dataset location (optional)
            as_dataframe: Return results as pandas DataFrame if True
            
        Returns:
            Query results
        """
        return bigquery_execute_query(
            sql=sql,
            query_params=query_params,
            location=location,
            conn_id=self.conn_id,
            as_dataframe=as_dataframe
        )
    
    def create_dataset(self, dataset_id: str, location: str = 'US') -> bool:
        """
        Create a BigQuery dataset.
        
        Args:
            dataset_id: ID of the dataset to create
            location: Geographic location of the dataset
            
        Returns:
            True if successful
        """
        return bigquery_create_dataset(
            dataset_id=dataset_id,
            location=location,
            conn_id=self.conn_id
        )
    
    def create_table(self, dataset_id: str, table_id: str, schema: List) -> bool:
        """
        Create a BigQuery table.
        
        Args:
            dataset_id: ID of the dataset containing the table
            table_id: ID of the table to create
            schema: List of SchemaField objects defining the table schema
            
        Returns:
            True if successful
        """
        return bigquery_create_table(
            dataset_id=dataset_id,
            table_id=table_id,
            schema=schema,
            conn_id=self.conn_id
        )
    
    def load_data(self, bucket_name: str, object_name: str,
                  dataset_id: str, table_id: str, source_format: str = 'CSV') -> bool:
        """
        Load data from GCS to BigQuery.
        
        Args:
            bucket_name: GCS bucket containing the data
            object_name: GCS object to load
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            source_format: Format of the data (CSV, JSON, AVRO, etc.)
            
        Returns:
            True if successful
        """
        return bigquery_load_data(
            bucket_name=bucket_name,
            object_name=object_name,
            dataset_id=dataset_id,
            table_id=table_id,
            source_format=source_format,
            conn_id=self.conn_id
        )
    
    def load_from_dataframe(self, dataframe: DataFrame, dataset_id: str,
                           table_id: str, if_exists: str = 'replace') -> bool:
        """
        Load data from DataFrame to BigQuery.
        
        Args:
            dataframe: Pandas DataFrame to load
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            if_exists: Action if table exists ('fail', 'replace', or 'append')
            
        Returns:
            True if successful
        """
        return dataframe_to_bigquery(
            dataframe=dataframe,
            dataset_id=dataset_id,
            table_id=table_id,
            conn_id=self.conn_id,
            if_exists=if_exists
        )


class SecretManagerClient:
    """
    Helper class that provides simplified access to Secret Manager.
    
    This class wraps the Secret Manager functions to provide an object-oriented
    interface and maintains connection state.
    """
    
    def __init__(self, conn_id: str = DEFAULT_GCP_CONN_ID):
        """
        Initialize the SecretManagerClient.
        
        Args:
            conn_id: Airflow connection ID for GCP
        """
        self.conn_id = conn_id
        self.client = None
        self.hook = None
    
    def get_hook(self) -> SecretManagerHook:
        """
        Get or create a SecretManagerHook instance.
        
        Returns:
            Instance of SecretManagerHook
        """
        if self.hook is None:
            self.hook = SecretManagerHook(gcp_conn_id=self.conn_id)
        return self.hook
    
    def get_client(self) -> SecretManagerServiceClient:
        """
        Get or create a Secret Manager client.
        
        Returns:
            Authenticated Secret Manager client
        """
        if self.client is None:
            self.client = initialize_gcp_client('secretmanager', self.conn_id)
        return self.client
    
    def get_secret(self, secret_id: str, version_id: str = 'latest') -> str:
        """
        Get a secret from Secret Manager.
        
        Args:
            secret_id: The ID of the secret to retrieve
            version_id: The version of the secret (default: 'latest')
            
        Returns:
            Secret value
        """
        return get_secret(
            secret_id=secret_id,
            version_id=version_id,
            conn_id=self.conn_id
        )
    
    def create_secret(self, secret_id: str, secret_value: str) -> bool:
        """
        Create a new secret in Secret Manager.
        
        Args:
            secret_id: The ID to give the new secret
            secret_value: The value of the secret
            
        Returns:
            True if successful
        """
        return create_secret(
            secret_id=secret_id,
            secret_value=secret_value,
            conn_id=self.conn_id
        )