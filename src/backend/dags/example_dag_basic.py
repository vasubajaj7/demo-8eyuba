"""
Basic example DAG for Apache Airflow 2.X demonstrating the migration
from Airflow 1.10.15 to Airflow 2.X. This DAG showcases common GCS operations,
task dependencies, and best practices for Cloud Composer 2.
"""

import os
import logging
from datetime import datetime, timedelta

# Airflow imports (Airflow 2.0.0+)
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import path in Airflow 2.X
from airflow.operators.bash import BashOperator  # Updated import path in Airflow 2.X
from airflow.operators.dummy import DummyOperator  # Updated import path in Airflow 2.X

# Internal imports
from .utils.gcp_utils import gcs_file_exists, gcs_download_file
from .utils.alert_utils import configure_dag_alerts
from plugins.operators.custom_gcp_operator import GCSFileExistsOperator, GCSDownloadOperator

# Set up logging
logger = logging.getLogger(__name__)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Define constants
BUCKET_NAME = 'example-bucket'
OBJECT_NAME = 'example-data.csv'
LOCAL_FILE_PATH = '/tmp/example-data.csv'


def check_file_callable(**kwargs):
    """
    Python callable that checks if a file exists in GCS.
    
    Args:
        **kwargs: Keyword arguments passed from the PythonOperator
        
    Returns:
        bool: True if file exists, False otherwise
    """
    bucket_name = kwargs.get('bucket_name', BUCKET_NAME)
    object_name = kwargs.get('object_name', OBJECT_NAME)
    
    logger.info(f"Checking if file exists: gs://{bucket_name}/{object_name}")
    exists = gcs_file_exists(bucket_name=bucket_name, object_name=object_name)
    
    if exists:
        logger.info(f"File gs://{bucket_name}/{object_name} exists")
    else:
        logger.warning(f"File gs://{bucket_name}/{object_name} does not exist")
        
    return exists


def download_file_callable(**kwargs):
    """
    Python callable that downloads a file from GCS.
    
    Args:
        **kwargs: Keyword arguments passed from the PythonOperator
        
    Returns:
        str: Local path where file was downloaded
    """
    bucket_name = kwargs.get('bucket_name', BUCKET_NAME)
    object_name = kwargs.get('object_name', OBJECT_NAME)
    local_file_path = kwargs.get('local_file_path', LOCAL_FILE_PATH)
    
    logger.info(f"Downloading file from gs://{bucket_name}/{object_name} to {local_file_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
    
    # Download the file
    downloaded_path = gcs_download_file(
        bucket_name=bucket_name,
        object_name=object_name,
        local_file_path=local_file_path
    )
    
    logger.info(f"Successfully downloaded file to {downloaded_path}")
    
    # In Airflow 2.X, the return value is automatically pushed to XCom
    return downloaded_path


def process_file_callable(**kwargs):
    """
    Python callable that processes a downloaded file.
    
    Args:
        **kwargs: Keyword arguments passed from the PythonOperator
        
    Returns:
        dict: Processing results including record count and status
    """
    # In Airflow 2.X, the task_instance is directly available in the kwargs
    ti = kwargs['ti']
    
    # Get the local file path from the upstream task
    local_file_path = ti.xcom_pull(task_ids='download_file')
    
    logger.info(f"Processing file: {local_file_path}")
    
    # Check if file exists
    if not os.path.exists(local_file_path):
        logger.error(f"File does not exist: {local_file_path}")
        return {"status": "error", "reason": "File not found", "record_count": 0}
    
    # Process the file (count lines)
    try:
        with open(local_file_path, 'r') as f:
            lines = f.readlines()
            
        record_count = len(lines)
        logger.info(f"Processed {record_count} records from {local_file_path}")
        
        # Return processing statistics
        return {
            "status": "success",
            "record_count": record_count,
            "file_size_bytes": os.path.getsize(local_file_path),
            "processing_time": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return {"status": "error", "reason": str(e), "record_count": 0}


# Define the DAG
with DAG(
    dag_id='example_dag_basic',
    description='Basic example DAG for Airflow 2.X migration',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'migration', 'airflow2'],
) as example_dag:
    
    # Configure alerts for the DAG
    example_dag = configure_dag_alerts(
        dag=example_dag,
        on_failure_alert=True,
        on_retry_alert=True,
        on_success_alert=False,
    )
    
    # Define tasks
    start = DummyOperator(
        task_id='start',
        doc_md="Start point of DAG execution"
    )
    
    # Using PythonOperator for file check
    check_file = PythonOperator(
        task_id='check_file',
        python_callable=check_file_callable,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'object_name': OBJECT_NAME
        },
        doc_md="Check if file exists in GCS"
    )
    
    # Using PythonOperator for file download
    download_file = PythonOperator(
        task_id='download_file',
        python_callable=download_file_callable,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'object_name': OBJECT_NAME,
            'local_file_path': LOCAL_FILE_PATH
        },
        doc_md="Download file from GCS to local path"
    )
    
    # Using PythonOperator for file processing
    process_file = PythonOperator(
        task_id='process_file',
        python_callable=process_file_callable,
        doc_md="Process downloaded file"
    )
    
    # Using BashOperator for shell command execution
    bash_example = BashOperator(
        task_id='bash_example',
        bash_command='echo "Processing completed on {{ ds }}"',
        doc_md="Example Bash command execution"
    )
    
    end = DummyOperator(
        task_id='end',
        doc_md="End point of DAG execution"
    )
    
    # Define task dependencies
    start >> check_file >> download_file >> process_file >> bash_example >> end

# Make DAG available for import
if __name__ == "__main__":
    example_dag.test()