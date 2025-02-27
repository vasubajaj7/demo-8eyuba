"""
Custom Google Cloud Platform operators for Apache Airflow 2.X.

This module provides specialized operators for GCS operations that extend
the standard GCP operators from Airflow provider packages with improved
error handling, logging, and compatibility between Airflow 1.10.15
and Airflow 2.X during migration to Cloud Composer 2.
"""

import os
import logging
from typing import Any, Dict, List, Optional, Union

# Airflow imports
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

# Import the GCS hook from Airflow provider package
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # airflow-providers-google v2.0.0+

# Import custom utilities
from ..hooks.custom_gcp_hook import CustomGCPHook, DEFAULT_GCP_CONN_ID
from ../../dags.utils.validation_utils import validate_input
from ../../dags.utils.alert_utils import send_alert, AlertLevel

# Set up logging
logger = logging.getLogger('airflow.operators.custom_gcp_operator')

# Global constants
DEFAULT_CHUNK_SIZE = 104857600  # 100 MB in bytes


def _validate_gcp_file_params(bucket_name: str, object_name: str) -> bool:
    """
    Validates parameters for GCP file operations.
    
    Args:
        bucket_name: Name of the GCS bucket
        object_name: Name of the object in GCS
        
    Returns:
        True if parameters are valid
        
    Raises:
        ValueError: If parameters are invalid
    """
    if not bucket_name or not isinstance(bucket_name, str):
        raise ValueError("bucket_name must be a non-empty string")
    
    if not object_name or not isinstance(object_name, str):
        raise ValueError("object_name must be a non-empty string")
    
    return True


def _validate_local_file_path(file_path: str, must_exist: bool) -> bool:
    """
    Validates a local file path for upload or download operations.
    
    Args:
        file_path: Path to the local file
        must_exist: Whether the file must already exist
        
    Returns:
        True if file path is valid
        
    Raises:
        ValueError: If file path is invalid
    """
    if not file_path or not isinstance(file_path, str):
        raise ValueError("file_path must be a non-empty string")
    
    if must_exist and not os.path.exists(file_path):
        raise ValueError(f"File does not exist: {file_path}")
    
    # If file must not exist, at least ensure directory exists
    if not must_exist:
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            raise ValueError(f"Directory does not exist: {directory}")
    
    return True


class CustomGCSOperator(BaseOperator):
    """
    Base class for GCS operations operators that provides common functionality.
    
    This abstract base class handles common GCP connection parameters and
    provides foundation for specific GCS operators.
    """
    
    def __init__(
        self,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        delegate_to: Optional[str] = None,
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
        
        logger.debug(f"Initialized CustomGCSOperator with connection ID {gcp_conn_id}")
    
    def get_hook(self) -> CustomGCPHook:
        """
        Get a CustomGCPHook instance.
        
        Returns:
            Initialized CustomGCPHook configured with connection details
        """
        return CustomGCPHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
    
    def execute(self, context: Dict) -> Any:
        """
        Execute method to be implemented by subclasses.
        
        Args:
            context: Airflow context dictionary
            
        Raises:
            NotImplementedError: This method must be implemented by subclasses
        """
        logger.error("execute() method not implemented")
        raise NotImplementedError(
            "CustomGCSOperator is an abstract base class and cannot be executed directly. "
            "Use a derived operator class instead."
        )
    
    def on_kill(self) -> None:
        """
        Handle task instance being killed.
        
        Sends an alert if alert_on_error is enabled.
        """
        logger.info(f"Task {self.task_id} is being killed.")
        
        if self.alert_on_error:
            context = {
                'task': self,
                'task_id': self.task_id,
                'dag_id': self.dag_id if hasattr(self, 'dag_id') else 'unknown'
            }
            
            send_alert(
                alert_level=AlertLevel.WARNING,
                context=context,
                exception=None
            )


class GCSFileExistsOperator(CustomGCSOperator):
    """
    Operator that checks if a file exists in Google Cloud Storage.
    
    This operator checks whether an object exists in the specified GCS bucket
    and returns a boolean result.
    """
    
    template_fields = ['bucket_name', 'object_name']
    
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        delegate_to: Optional[str] = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the GCSFileExistsOperator.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to check
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
        
        # Validate parameters
        _validate_gcp_file_params(bucket_name, object_name)
    
    def execute(self, context: Dict) -> bool:
        """
        Execute the file existence check.
        
        Args:
            context: Airflow context dictionary
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            hook = self.get_hook()
            exists = hook.gcs_file_exists(
                bucket_name=self.bucket_name,
                object_name=self.object_name
            )
            
            if exists:
                logger.info(f"File gs://{self.bucket_name}/{self.object_name} exists")
            else:
                logger.warning(f"File gs://{self.bucket_name}/{self.object_name} does not exist")
            
            return exists
            
        except Exception as e:
            error_msg = f"Error checking if gs://{self.bucket_name}/{self.object_name} exists: {str(e)}"
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)


class GCSUploadOperator(CustomGCSOperator):
    """
    Operator that uploads a local file to Google Cloud Storage.
    
    This operator takes a local file and uploads it to a specified location in GCS.
    """
    
    template_fields = ['local_file_path', 'bucket_name', 'object_name']
    
    def __init__(
        self,
        local_file_path: str,
        bucket_name: str,
        object_name: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        delegate_to: Optional[str] = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the GCSUploadOperator.
        
        Args:
            local_file_path: Path to the local file to upload
            bucket_name: Name of the GCS bucket
            object_name: Name to give the uploaded object
            chunk_size: Size of chunks for uploading large files
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
        self.local_file_path = local_file_path
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.chunk_size = chunk_size
        
        # Validate parameters
        _validate_gcp_file_params(bucket_name, object_name)
        _validate_local_file_path(local_file_path, must_exist=True)
    
    def execute(self, context: Dict) -> str:
        """
        Execute the file upload operation.
        
        Args:
            context: Airflow context dictionary
            
        Returns:
            GCS URI of uploaded file (gs://bucket-name/object-name)
        """
        try:
            hook = self.get_hook()
            gcs_uri = hook.gcs_upload_file(
                local_file_path=self.local_file_path,
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                chunk_size=self.chunk_size
            )
            
            file_size = os.path.getsize(self.local_file_path)
            logger.info(
                f"Successfully uploaded {self.local_file_path} "
                f"({file_size} bytes) to {gcs_uri}"
            )
            
            return gcs_uri
            
        except Exception as e:
            error_msg = (
                f"Error uploading {self.local_file_path} to "
                f"gs://{self.bucket_name}/{self.object_name}: {str(e)}"
            )
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)


class GCSDownloadOperator(CustomGCSOperator):
    """
    Operator that downloads a file from Google Cloud Storage to a local path.
    
    This operator retrieves an object from GCS and saves it to a specified local path.
    """
    
    template_fields = ['bucket_name', 'object_name', 'local_file_path']
    
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        local_file_path: str,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        delegate_to: Optional[str] = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the GCSDownloadOperator.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to download
            local_file_path: Local path where the file should be saved
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
        self.local_file_path = local_file_path
        
        # Validate parameters
        _validate_gcp_file_params(bucket_name, object_name)
        _validate_local_file_path(local_file_path, must_exist=False)
    
    def execute(self, context: Dict) -> str:
        """
        Execute the file download operation.
        
        Args:
            context: Airflow context dictionary
            
        Returns:
            Local path of downloaded file
        """
        try:
            hook = self.get_hook()
            local_path = hook.gcs_download_file(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                local_file_path=self.local_file_path
            )
            
            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path)
                logger.info(
                    f"Successfully downloaded gs://{self.bucket_name}/{self.object_name} "
                    f"({file_size} bytes) to {local_path}"
                )
            
            return local_path
            
        except Exception as e:
            error_msg = (
                f"Error downloading gs://{self.bucket_name}/{self.object_name} to "
                f"{self.local_file_path}: {str(e)}"
            )
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)


class GCSListFilesOperator(CustomGCSOperator):
    """
    Operator that lists files in a Google Cloud Storage bucket with optional prefix filter.
    
    This operator retrieves a list of object names from a GCS bucket,
    optionally filtered by prefix.
    """
    
    template_fields = ['bucket_name', 'prefix']
    
    def __init__(
        self,
        bucket_name: str,
        prefix: str = '',
        delimiter: bool = None,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        delegate_to: Optional[str] = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the GCSListFilesOperator.
        
        Args:
            bucket_name: Name of the GCS bucket
            prefix: Prefix to filter objects (optional)
            delimiter: Whether to use a delimiter for folder-like listing
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
        self.prefix = prefix
        self.delimiter = delimiter
        
        # Validate bucket name
        if not bucket_name or not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be a non-empty string")
    
    def execute(self, context: Dict) -> List[str]:
        """
        Execute the file listing operation.
        
        Args:
            context: Airflow context dictionary
            
        Returns:
            List of object names matching the prefix
        """
        try:
            hook = self.get_hook()
            file_list = hook.gcs_list_files(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                delimiter=self.delimiter
            )
            
            logger.info(
                f"Listed {len(file_list)} files from gs://{self.bucket_name}/{self.prefix or ''}"
            )
            
            return file_list
            
        except Exception as e:
            error_msg = (
                f"Error listing files in gs://{self.bucket_name}/{self.prefix or ''}: {str(e)}"
            )
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)


class GCSDeleteFileOperator(CustomGCSOperator):
    """
    Operator that deletes a file from Google Cloud Storage.
    
    This operator removes an object from a GCS bucket.
    """
    
    template_fields = ['bucket_name', 'object_name']
    
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        delegate_to: Optional[str] = None,
        alert_on_error: bool = False,
        *args,
        **kwargs
    ):
        """
        Initialize the GCSDeleteFileOperator.
        
        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object to delete
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
        
        # Validate parameters
        _validate_gcp_file_params(bucket_name, object_name)
    
    def execute(self, context: Dict) -> bool:
        """
        Execute the file deletion operation.
        
        Args:
            context: Airflow context dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            hook = self.get_hook()
            success = hook.gcs_delete_file(
                bucket_name=self.bucket_name,
                object_name=self.object_name
            )
            
            if success:
                logger.info(f"Successfully deleted gs://{self.bucket_name}/{self.object_name}")
            else:
                logger.warning(f"Failed to delete gs://{self.bucket_name}/{self.object_name}")
            
            return success
            
        except Exception as e:
            error_msg = (
                f"Error deleting gs://{self.bucket_name}/{self.object_name}: {str(e)}"
            )
            logger.error(error_msg)
            
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            raise AirflowException(error_msg)