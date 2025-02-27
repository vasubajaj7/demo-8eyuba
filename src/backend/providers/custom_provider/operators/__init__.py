"""
Custom operator package for Apache Airflow 2.X.

This module centralizes access to all custom operators, making them directly
importable from the provider package. It supports the migration from 
Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2.
"""

import logging

# Import airflow modules
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

# Import custom GCP operators
from ....plugins.operators.custom_gcp_operator import (
    CustomGCSOperator,
    GCSFileExistsOperator,
    GCSUploadOperator,
    GCSDownloadOperator,
    GCSListFilesOperator,
    GCSDeleteFileOperator,
    DEFAULT_CHUNK_SIZE,
)

# Import custom HTTP operators
from ....plugins.operators.custom_http_operator import (
    CustomHttpOperator,
    CustomHttpSensorOperator,
    DEFAULT_HTTP_CONN_ID,
)

# Import custom PostgreSQL operators
from ....plugins.operators.custom_postgres_operator import (
    CustomPostgresOperator,
    MAX_RETRIES,
    DEFAULT_RETRY_DELAY,
)

# Import custom hooks and utilities
from ....plugins.hooks.custom_gcp_hook import CustomGCPHook
from ....dags.utils.alert_utils import send_alert, AlertLevel

# Configure logging
logger = logging.getLogger(__name__)

# Define the list of all exported components
__all__ = [
    'CustomGCSOperator',
    'GCSFileExistsOperator',
    'GCSUploadOperator',
    'GCSDownloadOperator',
    'GCSListFilesOperator',
    'GCSDeleteFileOperator',
    'CustomBigQueryOperator',
    'BigQueryExecuteQueryOperator',
    'BigQueryCreateDatasetOperator',
    'BigQueryCreateTableOperator',
    'BigQueryLoadDataOperator',
    'SecretManagerGetSecretOperator',
    'CustomHttpOperator',
    'CustomHttpSensorOperator',
    'CustomPostgresOperator',
    'DEFAULT_CHUNK_SIZE',
    'DEFAULT_HTTP_CONN_ID',
    'MAX_RETRIES',
    'DEFAULT_RETRY_DELAY',
]

class CustomBigQueryOperator(BaseOperator):
    """
    Base class for BigQuery operations that provides common functionality.
    """
    
    def __init__(
        self,
        gcp_conn_id: str,
        delegate_to: str = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the operator with GCP connection details.
        
        Args:
            gcp_conn_id: Airflow connection ID for GCP
            delegate_to: The account to impersonate, if any
            alert_on_error: Whether to send alerts on operation failure
        """
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.alert_on_error = alert_on_error
        
        logger.debug(f"Initialized CustomBigQueryOperator with connection ID {gcp_conn_id}")
    
    def get_hook(self):
        """
        Get a CustomGCPHook instance.
        
        Returns:
            Configured GCP hook
        """
        return CustomGCPHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
    
    def execute(self, context):
        """
        Execute method to be implemented by subclasses.
        
        Args:
            context: Airflow task context
            
        Raises:
            NotImplementedError: This method must be implemented by subclasses
        """
        raise NotImplementedError(
            "CustomBigQueryOperator is an abstract base class and cannot be executed directly. "
            "Use a derived operator class instead."
        )


class BigQueryExecuteQueryOperator(CustomBigQueryOperator):
    """
    Operator that executes a BigQuery SQL query.
    """
    
    def __init__(
        self,
        sql: str,
        query_params: dict = None,
        location: str = 'US',
        as_dataframe: bool = False,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the BigQueryExecuteQueryOperator.
        
        Args:
            sql: SQL query to execute
            query_params: Query parameters (optional)
            location: BigQuery dataset location
            as_dataframe: Return results as pandas DataFrame if True
            gcp_conn_id: Airflow connection ID for GCP
            delegate_to: The account to impersonate, if any
            alert_on_error: Whether to send alerts on operation failure
        """
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            alert_on_error=alert_on_error,
            *args,
            **kwargs
        )
        self.sql = sql
        self.query_params = query_params or {}
        self.location = location
        self.as_dataframe = as_dataframe
        
        # Validate parameters
        if not sql:
            raise ValueError("SQL query must be provided")
    
    def execute(self, context):
        """
        Execute the BigQuery SQL query.
        
        Args:
            context: Airflow task context
            
        Returns:
            Query results as list or pandas DataFrame
            
        Raises:
            AirflowException: If query execution fails
        """
        try:
            hook = self.get_hook()
            
            # Execute query
            result = hook.bigquery_execute_query(
                sql=self.sql,
                query_params=self.query_params,
                location=self.location,
                as_dataframe=self.as_dataframe
            )
            
            logger.info(f"Successfully executed BigQuery query, returned {len(result)} rows/items")
            return result
            
        except Exception as e:
            error_msg = f"Failed to execute BigQuery query: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)


class BigQueryCreateDatasetOperator(CustomBigQueryOperator):
    """
    Operator that creates a BigQuery dataset if it doesn't exist.
    """
    
    def __init__(
        self,
        dataset_id: str,
        location: str = 'US',
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the BigQueryCreateDatasetOperator.
        
        Args:
            dataset_id: ID of the dataset to create
            location: Geographic location of the dataset
            gcp_conn_id: Airflow connection ID for GCP
            delegate_to: The account to impersonate, if any
            alert_on_error: Whether to send alerts on operation failure
        """
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            alert_on_error=alert_on_error,
            *args,
            **kwargs
        )
        self.dataset_id = dataset_id
        self.location = location
        
        # Validate parameters
        if not dataset_id:
            raise ValueError("dataset_id must be provided")
    
    def execute(self, context):
        """
        Execute the dataset creation operation.
        
        Args:
            context: Airflow task context
            
        Returns:
            True if dataset was created or already exists
            
        Raises:
            AirflowException: If dataset creation fails
        """
        try:
            hook = self.get_hook()
            
            # Create dataset
            result = hook.bigquery_create_dataset(
                dataset_id=self.dataset_id,
                location=self.location
            )
            
            logger.info(f"Successfully created or confirmed existence of dataset {self.dataset_id}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to create dataset {self.dataset_id}: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)


class BigQueryCreateTableOperator(CustomBigQueryOperator):
    """
    Operator that creates a BigQuery table with the specified schema.
    """
    
    def __init__(
        self,
        dataset_id: str,
        table_id: str,
        schema: list,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the BigQueryCreateTableOperator.
        
        Args:
            dataset_id: ID of the dataset containing the table
            table_id: ID of the table to create
            schema: List of SchemaField objects defining the table schema
            gcp_conn_id: Airflow connection ID for GCP
            delegate_to: The account to impersonate, if any
            alert_on_error: Whether to send alerts on operation failure
        """
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            alert_on_error=alert_on_error,
            *args,
            **kwargs
        )
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema = schema
        
        # Validate parameters
        if not dataset_id:
            raise ValueError("dataset_id must be provided")
        if not table_id:
            raise ValueError("table_id must be provided")
        if not schema:
            raise ValueError("schema must be provided")
    
    def execute(self, context):
        """
        Execute the table creation operation.
        
        Args:
            context: Airflow task context
            
        Returns:
            True if table was created or already exists
            
        Raises:
            AirflowException: If table creation fails
        """
        try:
            hook = self.get_hook()
            
            # Create table
            result = hook.bigquery_create_table(
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                schema=self.schema
            )
            
            logger.info(f"Successfully created or confirmed existence of table {self.dataset_id}.{self.table_id}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to create table {self.dataset_id}.{self.table_id}: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)


class BigQueryLoadDataOperator(CustomBigQueryOperator):
    """
    Operator that loads data from GCS into a BigQuery table.
    """
    
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        dataset_id: str,
        table_id: str,
        source_format: str = 'CSV',
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the BigQueryLoadDataOperator.
        
        Args:
            bucket_name: GCS bucket containing the data
            object_name: GCS object to load
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            source_format: Format of the data (CSV, JSON, AVRO, etc.)
            gcp_conn_id: Airflow connection ID for GCP
            delegate_to: The account to impersonate, if any
            alert_on_error: Whether to send alerts on operation failure
        """
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            alert_on_error=alert_on_error,
            *args,
            **kwargs
        )
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.source_format = source_format
        
        # Validate parameters
        if not bucket_name:
            raise ValueError("bucket_name must be provided")
        if not object_name:
            raise ValueError("object_name must be provided")
        if not dataset_id:
            raise ValueError("dataset_id must be provided")
        if not table_id:
            raise ValueError("table_id must be provided")
    
    def execute(self, context):
        """
        Execute the data loading operation.
        
        Args:
            context: Airflow task context
            
        Returns:
            True if load job completed successfully
            
        Raises:
            AirflowException: If data loading fails
        """
        try:
            hook = self.get_hook()
            
            # Load data
            result = hook.bigquery_load_data(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                source_format=self.source_format
            )
            
            logger.info(f"Successfully loaded data from gs://{self.bucket_name}/{self.object_name} to {self.dataset_id}.{self.table_id}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to load data to {self.dataset_id}.{self.table_id}: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)


class SecretManagerGetSecretOperator(CustomBigQueryOperator):
    """
    Operator that retrieves a secret from Google Cloud Secret Manager.
    """
    
    def __init__(
        self,
        secret_id: str,
        version_id: str = 'latest',
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the SecretManagerGetSecretOperator.
        
        Args:
            secret_id: The ID of the secret to retrieve
            version_id: The version of the secret (default: 'latest')
            gcp_conn_id: Airflow connection ID for GCP
            delegate_to: The account to impersonate, if any
            alert_on_error: Whether to send alerts on operation failure
        """
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            alert_on_error=alert_on_error,
            *args,
            **kwargs
        )
        self.secret_id = secret_id
        self.version_id = version_id
        
        # Validate parameters
        if not secret_id:
            raise ValueError("secret_id must be provided")
    
    def execute(self, context):
        """
        Execute the secret retrieval operation.
        
        Args:
            context: Airflow task context
            
        Returns:
            Secret value
            
        Raises:
            AirflowException: If secret retrieval fails
        """
        try:
            hook = self.get_hook()
            
            # Get secret
            secret = hook.get_secret(
                secret_id=self.secret_id,
                version_id=self.version_id
            )
            
            logger.info(f"Successfully retrieved secret {self.secret_id} (version: {self.version_id})")
            return secret
            
        except Exception as e:
            error_msg = f"Failed to retrieve secret {self.secret_id}: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)