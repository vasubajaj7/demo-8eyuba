"""
Custom GCP sensors for Apache Airflow 2.X.

This module provides a set of sensors for monitoring Google Cloud Platform resources:
- CustomGCSFileSensor: Monitors existence of GCS files
- CustomBigQueryTableSensor: Monitors existence of BigQuery tables
- CustomBigQueryJobSensor: Monitors status of BigQuery jobs
- CustomGCSObjectsWithPrefixExistenceSensor: Monitors existence of GCS objects with a prefix

These sensors are designed to work with Cloud Composer 2 and Airflow 2.X,
migrated from the Airflow 1.10.15 implementation used in Cloud Composer 1.
"""

import logging
from typing import Dict, List, Optional, Union, Any

# Airflow imports
from airflow.sensors.base import BaseSensorOperator  # airflow v2.0.0+
from airflow.exceptions import AirflowException  # airflow v2.0.0+

# Internal imports
from ..hooks.custom_gcp_hook import CustomGCPHook, DEFAULT_GCP_CONN_ID

# Set up logging
logger = logging.getLogger(__name__)

# Global constants
DEFAULT_DELIMITER = None
DEFAULT_MIN_OBJECTS = 1


class CustomGCSFileSensor(BaseSensorOperator):
    """
    Sensor that checks for the existence of a file in Google Cloud Storage.
    
    This sensor uses the CustomGCPHook to check if a specific file exists in a GCS bucket,
    or if any files exist with a specified prefix.
    
    Args:
        bucket_name: The GCS bucket where the file should exist
        object_name: The name of the object to check for, can be a specific file or a prefix
        gcp_conn_id: The connection ID to use for GCP authentication
        **kwargs: Additional arguments to pass to the BaseSensorOperator
    """
    
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        **kwargs
    ) -> None:
        """
        Initialize the GCS file sensor.
        
        Args:
            bucket_name: The GCS bucket where the file should exist
            object_name: The name of the object to check for
            gcp_conn_id: The connection ID to use for GCP authentication
            **kwargs: Additional arguments to pass to the BaseSensorOperator
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self.hook = None
    
    def poke(self, context: Dict) -> bool:
        """
        Check if the file exists in the specified GCS bucket.
        
        Args:
            context: Airflow context dict
            
        Returns:
            True if the file exists, False otherwise
        """
        if self.hook is None:
            self.hook = CustomGCPHook(gcp_conn_id=self.gcp_conn_id)
        
        exists = self.hook.gcs_file_exists(
            bucket_name=self.bucket_name,
            object_name=self.object_name
        )
        
        if exists:
            logger.info(f"File gs://{self.bucket_name}/{self.object_name} exists")
        else:
            logger.info(f"File gs://{self.bucket_name}/{self.object_name} does not exist")
        
        return exists


class CustomBigQueryTableSensor(BaseSensorOperator):
    """
    Sensor that checks for the existence of a table in Google BigQuery.
    
    This sensor uses the CustomGCPHook to check if a specified table exists
    in a BigQuery dataset.
    
    Args:
        project_id: The GCP project ID containing the table
        dataset_id: The BigQuery dataset ID
        table_id: The BigQuery table ID
        gcp_conn_id: The connection ID to use for GCP authentication
        **kwargs: Additional arguments to pass to the BaseSensorOperator
    """
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        **kwargs
    ) -> None:
        """
        Initialize the BigQuery table sensor.
        
        Args:
            project_id: The GCP project ID containing the table
            dataset_id: The BigQuery dataset ID
            table_id: The BigQuery table ID
            gcp_conn_id: The connection ID to use for GCP authentication
            **kwargs: Additional arguments to pass to the BaseSensorOperator
        """
        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcp_conn_id = gcp_conn_id
        self.hook = None
    
    def poke(self, context: Dict) -> bool:
        """
        Check if the table exists in the specified BigQuery dataset.
        
        Args:
            context: Airflow context dict
            
        Returns:
            True if the table exists, False otherwise
        """
        if self.hook is None:
            self.hook = CustomGCPHook(gcp_conn_id=self.gcp_conn_id)
        
        try:
            # Get the BigQuery client
            bigquery_client = self.hook.get_bigquery_client()
            
            # Create reference to the table
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            
            # Try to get the table metadata to check if it exists
            bigquery_client.get_table(table_ref)
            
            logger.info(f"Table {table_ref} exists")
            return True
            
        except Exception as e:
            logger.info(f"Table {self.project_id}.{self.dataset_id}.{self.table_id} does not exist: {str(e)}")
            return False


class CustomBigQueryJobSensor(BaseSensorOperator):
    """
    Sensor that checks for the status of a BigQuery job.
    
    This sensor uses the CustomGCPHook to check if a specific BigQuery job
    has completed successfully.
    
    Args:
        project_id: The GCP project ID containing the job
        job_id: The BigQuery job ID to check
        location: The location of the BigQuery job
        gcp_conn_id: The connection ID to use for GCP authentication
        **kwargs: Additional arguments to pass to the BaseSensorOperator
    """
    
    def __init__(
        self,
        project_id: str,
        job_id: str,
        location: str = 'US',
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        **kwargs
    ) -> None:
        """
        Initialize the BigQuery job sensor.
        
        Args:
            project_id: The GCP project ID containing the job
            job_id: The BigQuery job ID to check
            location: The location of the BigQuery job (default: US)
            gcp_conn_id: The connection ID to use for GCP authentication
            **kwargs: Additional arguments to pass to the BaseSensorOperator
        """
        super().__init__(**kwargs)
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.hook = None
    
    def poke(self, context: Dict) -> bool:
        """
        Check if the BigQuery job has completed successfully.
        
        Args:
            context: Airflow context dict
            
        Returns:
            True if the job has completed successfully, False if still running
            
        Raises:
            AirflowException: If the job has failed
        """
        if self.hook is None:
            self.hook = CustomGCPHook(gcp_conn_id=self.gcp_conn_id)
        
        try:
            # Get the BigQuery client
            bigquery_client = self.hook.get_bigquery_client()
            
            # Get the job
            job = bigquery_client.get_job(
                job_id=self.job_id,
                project=self.project_id,
                location=self.location
            )
            
            # Check the job status
            if job.state == 'DONE':
                if job.error_result:
                    error_msg = f"BigQuery job {self.job_id} failed: {job.error_result}"
                    logger.error(error_msg)
                    raise AirflowException(error_msg)
                    
                logger.info(f"BigQuery job {self.job_id} completed successfully")
                return True
                
            elif job.state == 'RUNNING':
                logger.info(f"BigQuery job {self.job_id} is still running")
                return False
                
            else:
                error_msg = f"BigQuery job {self.job_id} has unexpected state: {job.state}"
                logger.error(error_msg)
                raise AirflowException(error_msg)
                
        except Exception as e:
            logger.error(f"Error checking BigQuery job {self.job_id}: {str(e)}")
            raise AirflowException(f"Error checking BigQuery job {self.job_id}: {str(e)}")


class CustomGCSObjectsWithPrefixExistenceSensor(BaseSensorOperator):
    """
    Sensor that checks for the existence of objects with a specific prefix in Google Cloud Storage.
    
    This sensor uses the CustomGCPHook to check if a minimum number of objects
    with a specified prefix exist in a GCS bucket.
    
    Args:
        bucket_name: The GCS bucket to check
        prefix: The object prefix to filter by
        min_objects: The minimum number of objects that should exist (default: 1)
        gcp_conn_id: The connection ID to use for GCP authentication
        delimiter: Whether to use a delimiter for hierarchical listing
        **kwargs: Additional arguments to pass to the BaseSensorOperator
    """
    
    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        min_objects: int = DEFAULT_MIN_OBJECTS,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        delimiter: Optional[bool] = DEFAULT_DELIMITER,
        **kwargs
    ) -> None:
        """
        Initialize the GCS objects with prefix sensor.
        
        Args:
            bucket_name: The GCS bucket to check
            prefix: The object prefix to filter by
            min_objects: The minimum number of objects that should exist (default: 1)
            gcp_conn_id: The connection ID to use for GCP authentication
            delimiter: Whether to use a delimiter for hierarchical listing
            **kwargs: Additional arguments to pass to the BaseSensorOperator
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        
        if not prefix:
            raise ValueError("The prefix parameter is required")
        self.prefix = prefix
        
        self.min_objects = min_objects
        self.delimiter = delimiter
        self.gcp_conn_id = gcp_conn_id
        self.hook = None
    
    def poke(self, context: Dict) -> bool:
        """
        Check if there are sufficient objects with the specified prefix in GCS.
        
        Args:
            context: Airflow context dict
            
        Returns:
            True if there are at least min_objects with the prefix, False otherwise
        """
        if self.hook is None:
            self.hook = CustomGCPHook(gcp_conn_id=self.gcp_conn_id)
        
        try:
            # List objects with the prefix
            objects = self.hook.gcs_list_files(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                delimiter=self.delimiter
            )
            
            # Check if we have enough objects
            object_count = len(objects)
            
            if object_count >= self.min_objects:
                logger.info(
                    f"Found {object_count} objects in gs://{self.bucket_name}/{self.prefix}, "
                    f"which satisfies the minimum of {self.min_objects}"
                )
                return True
            else:
                logger.info(
                    f"Found only {object_count} objects in gs://{self.bucket_name}/{self.prefix}, "
                    f"which does not satisfy the minimum of {self.min_objects}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Error checking for objects in gs://{self.bucket_name}/{self.prefix}: {str(e)}")
            return False