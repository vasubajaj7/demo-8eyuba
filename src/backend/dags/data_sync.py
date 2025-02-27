"""
Data synchronization DAG for Apache Airflow 2.X that orchestrates the data transfer between
different storage systems and databases. This DAG periodically extracts data from source systems,
transforms it as needed, and loads it into target destinations with appropriate error handling
and monitoring.
"""

import os
import logging
from datetime import datetime, timedelta
import pandas as pd

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Import custom utility modules
from .utils.gcp_utils import GCSClient, BigQueryClient
from .utils.db_utils import execute_query, execute_query_as_df, bulk_load_from_df
from .utils.alert_utils import configure_dag_alerts, on_failure_callback

# Import custom operators
from plugins.operators.custom_gcp_operator import (
    GCSFileExistsOperator,
    GCSDownloadOperator,
    GCSUploadOperator,
    BigQueryExecuteQueryOperator
)
from plugins.operators.custom_postgres_operator import CustomPostgresOperator

# Set up logging
logger = logging.getLogger('airflow.data_sync')

# Global constants
GCS_BUCKET = 'data-sync-bucket'
SOURCE_OBJECT_PREFIX = 'source/data/'
TARGET_OBJECT_PREFIX = 'processed/data/'
POSTGRES_CONN_ID = 'postgres_default'
BQ_DATASET = 'data_sync_dataset'
BQ_TABLE = 'data_sync_table'
TEMP_DATA_PATH = '/tmp/data_sync/'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1)
}

def check_source_data(**kwargs):
    """
    Check if source data exists in GCS storage.
    
    Args:
        **kwargs: Keyword arguments from the context
        
    Returns:
        bool: True if source data exists, False otherwise
    """
    try:
        task_instance = kwargs['ti']
        execution_date = kwargs['execution_date']
        date_str = execution_date.strftime('%Y/%m/%d')
        
        # Initialize GCS client
        gcs_client = GCSClient()
        
        # Construct source path with date partitioning
        source_prefix = f"{SOURCE_OBJECT_PREFIX}{date_str}/"
        
        # List objects in the source prefix
        source_files = gcs_client.list_files(
            bucket_name=GCS_BUCKET,
            prefix=source_prefix
        )
        
        num_files = len(source_files)
        logger.info(f"Found {num_files} files in gs://{GCS_BUCKET}/{source_prefix}")
        
        # Push file list to XCom for downstream tasks
        if num_files > 0:
            task_instance.xcom_push(key='source_files', value=source_files)
            task_instance.xcom_push(key='source_prefix', value=source_prefix)
            return True
        else:
            logger.warning(f"No files found in gs://{GCS_BUCKET}/{source_prefix}")
            return False
            
    except Exception as e:
        logger.error(f"Error checking source data: {str(e)}")
        raise

def extract_data_from_postgres(**kwargs):
    """
    Extract data from PostgreSQL database.
    
    Args:
        **kwargs: Keyword arguments from the context
        
    Returns:
        str: Path to extracted data file
    """
    try:
        task_instance = kwargs['ti']
        execution_date = kwargs['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Create temp directory if it doesn't exist
        os.makedirs(TEMP_DATA_PATH, exist_ok=True)
        
        # Create filename for extracted data
        output_file = os.path.join(TEMP_DATA_PATH, f"postgres_extract_{date_str}.csv")
        
        # Define SQL query with date filter
        sql_query = f"""
            SELECT *
            FROM source_table
            WHERE date_column::date = '{date_str}'
        """
        
        # Execute query and get results as DataFrame
        df = execute_query_as_df(
            sql=sql_query,
            conn_id=POSTGRES_CONN_ID
        )
        
        # Save DataFrame to CSV
        df.to_csv(output_file, index=False)
        
        record_count = len(df)
        logger.info(f"Extracted {record_count} records from PostgreSQL to {output_file}")
        
        # Push metadata to XCom
        metadata = {
            'record_count': record_count,
            'file_path': output_file,
            'source': 'postgres'
        }
        task_instance.xcom_push(key='postgres_extract', value=metadata)
        
        return output_file
        
    except Exception as e:
        logger.error(f"Error extracting data from PostgreSQL: {str(e)}")
        raise

def transform_data(**kwargs):
    """
    Transform data from source format to target format.
    
    Args:
        **kwargs: Keyword arguments from the context
        
    Returns:
        str: Path to transformed data file
    """
    try:
        task_instance = kwargs['ti']
        execution_date = kwargs['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Get source data paths from XCom
        postgres_metadata = task_instance.xcom_pull(task_ids='extract_data_from_postgres', key='postgres_extract')
        
        # Could also get data from GCS download task if needed
        gcs_download_path = task_instance.xcom_pull(task_ids='download_from_gcs', key='return_value')
        
        # Determine which source to use based on what's available
        if postgres_metadata:
            source_path = postgres_metadata['file_path']
            source_type = 'postgres'
        elif gcs_download_path:
            source_path = gcs_download_path
            source_type = 'gcs'
        else:
            raise ValueError("No source data available for transformation")
        
        # Create output path for transformed data
        transformed_file = os.path.join(TEMP_DATA_PATH, f"transformed_data_{date_str}.csv")
        
        # Read data from source
        df = pd.read_csv(source_path)
        input_records = len(df)
        
        # Apply transformations
        # Example transformations - modify as needed:
        # 1. Clean up column names
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # 2. Handle missing values
        df = df.fillna({
            'numeric_col': 0,
            'string_col': '',
            'date_col': pd.Timestamp('1970-01-01')
        })
        
        # 3. Apply data type conversions
        if 'date_col' in df.columns:
            df['date_col'] = pd.to_datetime(df['date_col'], errors='coerce')
            
        # 4. Add computed columns
        if 'value_a' in df.columns and 'value_b' in df.columns:
            df['calculated_value'] = df['value_a'] + df['value_b']
            
        # 5. Filter rows if needed
        df = df[df['status'].notnull()] if 'status' in df.columns else df
        
        # Save transformed data
        df.to_csv(transformed_file, index=False)
        
        output_records = len(df)
        logger.info(f"Transformed data: {input_records} input records -> {output_records} output records")
        logger.info(f"Transformed data saved to {transformed_file}")
        
        # Push metadata to XCom
        transform_metadata = {
            'input_records': input_records,
            'output_records': output_records,
            'input_source': source_type,
            'input_path': source_path,
            'output_path': transformed_file
        }
        task_instance.xcom_push(key='transform_data', value=transform_metadata)
        
        return transformed_file
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def load_data_to_bigquery(**kwargs):
    """
    Load transformed data to BigQuery.
    
    Args:
        **kwargs: Keyword arguments from the context
        
    Returns:
        dict: Load statistics including record count and status
    """
    try:
        task_instance = kwargs['ti']
        
        # Get transformed data path from XCom
        transform_metadata = task_instance.xcom_pull(task_ids='transform_data', key='transform_data')
        transformed_file = transform_metadata['output_path']
        
        # Initialize BigQuery client
        bq_client = BigQueryClient()
        
        # Read data from CSV
        df = pd.read_csv(transformed_file)
        record_count = len(df)
        
        # Load DataFrame to BigQuery
        success = bq_client.load_from_dataframe(
            dataframe=df,
            dataset_id=BQ_DATASET,
            table_id=BQ_TABLE,
            if_exists='replace'
        )
        
        if success:
            logger.info(f"Successfully loaded {record_count} records to BigQuery {BQ_DATASET}.{BQ_TABLE}")
        else:
            raise Exception(f"Failed to load data to BigQuery {BQ_DATASET}.{BQ_TABLE}")
        
        # Prepare and return load statistics
        load_stats = {
            'destination': 'bigquery',
            'dataset': BQ_DATASET,
            'table': BQ_TABLE,
            'record_count': record_count,
            'status': 'success'
        }
        
        # Push load results to XCom
        task_instance.xcom_push(key='bq_load_results', value=load_stats)
        
        return load_stats
        
    except Exception as e:
        logger.error(f"Error loading data to BigQuery: {str(e)}")
        raise

def load_data_to_postgres(**kwargs):
    """
    Load transformed data to PostgreSQL.
    
    Args:
        **kwargs: Keyword arguments from the context
        
    Returns:
        dict: Load statistics including record count and status
    """
    try:
        task_instance = kwargs['ti']
        
        # Get transformed data path from XCom
        transform_metadata = task_instance.xcom_pull(task_ids='transform_data', key='transform_data')
        transformed_file = transform_metadata['output_path']
        
        # Read data from CSV
        df = pd.read_csv(transformed_file)
        record_count = len(df)
        
        # Define target table
        target_table = 'target_table'
        target_schema = 'public'
        
        # Load DataFrame to PostgreSQL
        success = bulk_load_from_df(
            df=df,
            table_name=target_table,
            conn_id=POSTGRES_CONN_ID,
            schema=target_schema,
            if_exists='replace'
        )
        
        if success:
            logger.info(f"Successfully loaded {record_count} records to PostgreSQL {target_schema}.{target_table}")
        else:
            raise Exception(f"Failed to load data to PostgreSQL {target_schema}.{target_table}")
        
        # Prepare and return load statistics
        load_stats = {
            'destination': 'postgres',
            'schema': target_schema,
            'table': target_table,
            'record_count': record_count,
            'status': 'success'
        }
        
        # Push load results to XCom
        task_instance.xcom_push(key='postgres_load_results', value=load_stats)
        
        return load_stats
        
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise

def upload_processed_file(**kwargs):
    """
    Upload processed data file to GCS.
    
    Args:
        **kwargs: Keyword arguments from the context
        
    Returns:
        str: GCS URI of uploaded file
    """
    try:
        task_instance = kwargs['ti']
        execution_date = kwargs['execution_date']
        date_path = execution_date.strftime('%Y/%m/%d')
        date_str = execution_date.strftime('%Y-%m-%d')
        
        # Get transformed data path from XCom
        transform_metadata = task_instance.xcom_pull(task_ids='transform_data', key='transform_data')
        transformed_file = transform_metadata['output_path']
        
        # Initialize GCS client
        gcs_client = GCSClient()
        
        # Generate target object name
        target_object = f"{TARGET_OBJECT_PREFIX}{date_path}/processed_data_{date_str}.csv"
        
        # Upload file to GCS
        gcs_uri = gcs_client.upload_file(
            local_file_path=transformed_file,
            bucket_name=GCS_BUCKET,
            object_name=target_object
        )
        
        logger.info(f"Successfully uploaded processed file to {gcs_uri}")
        
        # Push GCS URI to XCom
        task_instance.xcom_push(key='processed_file_uri', value=gcs_uri)
        
        return gcs_uri
        
    except Exception as e:
        logger.error(f"Error uploading processed file to GCS: {str(e)}")
        raise

def cleanup_temp_files(**kwargs):
    """
    Remove temporary files created during execution.
    
    Args:
        **kwargs: Keyword arguments from the context
        
    Returns:
        bool: True if cleanup successful, False otherwise
    """
    try:
        task_instance = kwargs['ti']
        
        # Get file paths from XCom
        paths_to_cleanup = []
        
        # Check for postgres extract
        postgres_metadata = task_instance.xcom_pull(task_ids='extract_data_from_postgres', key='postgres_extract')
        if postgres_metadata and 'file_path' in postgres_metadata:
            paths_to_cleanup.append(postgres_metadata['file_path'])
        
        # Check for GCS download
        gcs_download_path = task_instance.xcom_pull(task_ids='download_from_gcs', key='return_value')
        if gcs_download_path:
            paths_to_cleanup.append(gcs_download_path)
        
        # Check for transformed data
        transform_metadata = task_instance.xcom_pull(task_ids='transform_data', key='transform_data')
        if transform_metadata and 'output_path' in transform_metadata:
            paths_to_cleanup.append(transform_metadata['output_path'])
        
        # Delete temporary files
        deleted_count = 0
        for file_path in paths_to_cleanup:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted temporary file: {file_path}")
                deleted_count += 1
        
        logger.info(f"Cleanup completed: {deleted_count} files deleted")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        return False

# Create the DAG
data_sync = DAG(
    dag_id='data_sync',
    description='Data synchronization pipeline for Airflow 2.X',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    concurrency=5,
    tags=['data_sync', 'etl', 'airflow2']
)

# Configure alerts for the DAG
data_sync = configure_dag_alerts(
    dag=data_sync,
    on_failure_alert=True,
    on_retry_alert=True,
    on_success_alert=False,
    on_sla_miss_alert=True,
    email_recipients=['data-team@example.com']
)

# Define tasks
start = DummyOperator(
    task_id='start',
    dag=data_sync,
    doc_md="Start point of DAG execution"
)

check_source_gcs = GCSFileExistsOperator(
    task_id='check_source_gcs',
    bucket_name='{{ var.value.data_sync_source_bucket }}',
    object_name='{{ var.value.data_sync_source_prefix }}',
    gcp_conn_id='google_cloud_default',
    dag=data_sync,
    doc_md="Check if source files exist in GCS"
)

extract_data_from_postgres_task = PythonOperator(
    task_id='extract_data_from_postgres',
    python_callable=extract_data_from_postgres,
    dag=data_sync,
    doc_md="Extract data from PostgreSQL database"
)

download_from_gcs = GCSDownloadOperator(
    task_id='download_from_gcs',
    bucket_name='{{ var.value.data_sync_source_bucket }}',
    object_name='{{ var.value.data_sync_source_object }}',
    local_file_path='{{ var.value.data_sync_temp_path }}',
    gcp_conn_id='google_cloud_default',
    dag=data_sync,
    doc_md="Download source files from GCS"
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=data_sync,
    doc_md="Transform data from source format to target format"
)

load_to_bigquery = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_data_to_bigquery,
    dag=data_sync,
    doc_md="Load transformed data to BigQuery"
)

load_to_postgres = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_data_to_postgres,
    dag=data_sync,
    doc_md="Load transformed data to PostgreSQL"
)

upload_to_gcs = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_processed_file,
    dag=data_sync,
    doc_md="Upload processed data file to GCS"
)

cleanup = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_temp_files,
    dag=data_sync,
    doc_md="Clean up temporary files"
)

end = DummyOperator(
    task_id='end',
    dag=data_sync,
    doc_md="End point of DAG execution"
)

# Define task dependencies
start >> check_source_gcs >> download_from_gcs
start >> extract_data_from_postgres_task
download_from_gcs >> transform_data_task
extract_data_from_postgres_task >> transform_data_task
transform_data_task >> [load_to_bigquery, load_to_postgres, upload_to_gcs]
load_to_bigquery >> end
load_to_postgres >> end
upload_to_gcs >> cleanup >> end