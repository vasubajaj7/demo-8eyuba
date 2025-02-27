"""
Example DAG demonstrating the TaskFlow API in Airflow 2.X.

This DAG shows how to migrate traditional operator-based patterns to the more
Pythonic TaskFlow approach with automatic XCom handling, providing a clear
migration path from Airflow 1.10.15 to Airflow 2.X.

Key TaskFlow API benefits demonstrated:
1. Automatic XCom usage - return values are automatically pushed to XCom
2. Simplified task dependencies - direct function calls create dependencies
3. Type hints can be used to validate task inputs and outputs
4. More Pythonic code structure with less boilerplate

Migration path from Airflow 1.X to TaskFlow API:
- Traditional PythonOperator with explicit XCom pushes/pulls
- Use provide_context=True in Airflow 1.X for context access
- In Airflow 2.X, convert to @task decorated functions
- Return values are automatically pushed to XCom
- Explicit dependencies via direct function calls
"""

import os
import datetime

from airflow.models import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Import utility functions
from .utils.gcp_utils import gcs_file_exists, gcs_download_file
from .utils.db_utils import execute_query
from .utils.alert_utils import configure_dag_alerts
from . import get_default_args

# Define default arguments for the DAG
default_args = get_default_args({
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2023, 1, 1)
})

# Define constants for the DAG
BUCKET_NAME = 'example-bucket'
OBJECT_NAME = 'example-data.csv'
LOCAL_FILE_PATH = '/tmp/example-data.csv'

# Define TaskFlow functions

@task
def check_file():
    """
    TaskFlow function that checks if a file exists in GCS.
    
    Returns:
        bool: True if file exists, False otherwise
    """
    print(f"Checking if file gs://{BUCKET_NAME}/{OBJECT_NAME} exists...")
    file_exists = gcs_file_exists(BUCKET_NAME, OBJECT_NAME)
    
    if file_exists:
        print(f"File found: gs://{BUCKET_NAME}/{OBJECT_NAME}")
    else:
        print(f"File not found: gs://{BUCKET_NAME}/{OBJECT_NAME}")
    
    return file_exists

@task
def download_file(file_exists):
    """
    TaskFlow function that downloads a file from GCS if it exists.
    
    Args:
        file_exists: Boolean indicating if the file exists
        
    Returns:
        str: Local path where file was downloaded or None if file doesn't exist
    """
    if not file_exists:
        print(f"File does not exist, skipping download.")
        return None
    
    print(f"Downloading file from gs://{BUCKET_NAME}/{OBJECT_NAME} to {LOCAL_FILE_PATH}...")
    local_path = gcs_download_file(BUCKET_NAME, OBJECT_NAME, LOCAL_FILE_PATH)
    print(f"File downloaded successfully to {local_path}")
    
    return local_path

@task
def process_file(file_path):
    """
    TaskFlow function that processes a downloaded file.
    
    Args:
        file_path: Local path of the downloaded file
        
    Returns:
        dict: Processing results including record count and status
    """
    if not file_path:
        print("No file path provided, skipping processing.")
        return {
            'status': 'error',
            'error_message': 'No file path provided',
            'record_count': 0,
            'processed_at': datetime.datetime.now().isoformat()
        }
    
    try:
        # Open and process the file
        record_count = 0
        with open(file_path, 'r') as f:
            # Skip header
            next(f)
            # Count lines and process
            for line in f:
                record_count += 1
                # Additional processing could be done here
        
        result = {
            'status': 'success',
            'file_path': file_path,
            'record_count': record_count,
            'processed_at': datetime.datetime.now().isoformat()
        }
        
        print(f"File processed successfully. Found {record_count} records.")
        return result
    
    except Exception as e:
        error_message = str(e)
        print(f"Error processing file: {error_message}")
        
        return {
            'status': 'error',
            'error_message': error_message,
            'record_count': 0,
            'processed_at': datetime.datetime.now().isoformat()
        }

@task
def save_results(results):
    """
    TaskFlow function that saves processing results to a database.
    
    Args:
        results: Dictionary containing processing results
        
    Returns:
        bool: True if results were saved successfully, False otherwise
    """
    if results['status'] == 'error':
        print(f"Processing reported an error: {results.get('error_message', 'Unknown error')}")
        return False
    
    try:
        # Construct SQL query
        sql = """
        INSERT INTO file_processing_results
        (file_name, record_count, status, processed_at)
        VALUES (%(file_name)s, %(record_count)s, %(status)s, %(processed_at)s)
        """
        
        # Execute query
        execute_query(
            sql=sql,
            parameters={
                'file_name': OBJECT_NAME,
                'record_count': results['record_count'],
                'status': results['status'],
                'processed_at': results['processed_at']
            }
        )
        
        print(f"Results saved to database. File: {OBJECT_NAME}, Records: {results['record_count']}")
        return True
    
    except Exception as e:
        error_message = str(e)
        print(f"Error saving results to database: {error_message}")
        return False

# Define the DAG
with DAG(
    dag_id='example_dag_taskflow',
    description='Example DAG demonstrating TaskFlow API in Airflow 2.X',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'taskflow', 'airflow2', 'migration']
) as dag:
    
    # Define start task
    start = DummyOperator(
        task_id='start',
    )
    
    # TaskFlow functions are automatically converted to tasks
    # No need to explicitly create tasks
    file_exists = check_file()
    local_file_path = download_file(file_exists)
    processing_results = process_file(local_file_path)
    save_success = save_results(processing_results)
    
    # Define a traditional BashOperator task
    bash_example = BashOperator(
        task_id='bash_example',
        bash_command='echo "Processing completed on {{ ds }} with TaskFlow API"',
    )
    
    # Define end task
    end = DummyOperator(
        task_id='end',
    )
    
    # Define task dependencies
    start >> file_exists >> local_file_path >> processing_results >> save_success >> bash_example >> end

# Configure alerting for the DAG and make it available for import
example_dag_taskflow = configure_dag_alerts(
    dag=dag,
    on_failure_alert=True,
    on_retry_alert=True,
    on_success_alert=False,
    email_recipients=['airflow-alerts@example.com']
)