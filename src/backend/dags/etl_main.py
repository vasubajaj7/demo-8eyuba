"""
Main ETL (Extract, Transform, Load) DAG for Apache Airflow 2.X that orchestrates data 
pipeline processes within Cloud Composer 2. This DAG extracts data from source systems, 
performs transformations, and loads the results into target storage systems with 
comprehensive error handling, monitoring, and alerting capabilities.
"""

# Standard library imports
import datetime
import os
import logging
import json

# Data processing imports
import pandas as pd

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

# Internal utilities import
from .utils.gcp_utils import (
    GCSClient, 
    BigQueryClient, 
    gcs_file_exists,
    gcs_upload_file, 
    gcs_download_file, 
    bigquery_execute_query, 
    dataframe_to_bigquery
)
from .utils.db_utils import (
    execute_query, 
    execute_query_as_df, 
    bulk_load_from_df, 
    verify_connection
)
from .utils.alert_utils import (
    configure_dag_alerts, 
    on_failure_callback, 
    on_retry_callback, 
    AlertLevel
)
from .utils.validation_utils import validate_dag

# Custom operator imports
from plugins.operators.custom_gcp_operator import (
    GCSFileExistsOperator,
    GCSDownloadOperator,
    GCSUploadOperator,
    BigQueryExecuteQueryOperator,
    BigQueryLoadDataOperator
)
from plugins.operators.custom_postgres_operator import CustomPostgresOperator

# Set up logging
logger = logging.getLogger('airflow.etl_main')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2023, 1, 1),
    'on_failure_callback': on_failure_callback,
    'on_retry_callback': on_retry_callback
}

# Global variables
GCS_SOURCE_BUCKET = '{{ var.value.etl_source_bucket }}'
GCS_TARGET_BUCKET = '{{ var.value.etl_target_bucket }}'
SOURCE_DATA_PREFIX = '{{ var.value.etl_source_prefix }}'
PROCESSED_DATA_PREFIX = '{{ var.value.etl_processed_prefix }}'
TEMP_DATA_DIR = '/tmp/etl_main/'
POSTGRES_CONN_ID = '{{ var.value.postgres_conn_id }}'
BQ_CONN_ID = '{{ var.value.bq_conn_id }}'
BQ_DATASET = '{{ var.value.etl_bq_dataset }}'
BQ_TABLE = '{{ var.value.etl_bq_table }}'

# Define the DAG
etl_main_dag = DAG(
    'etl_main',
    default_args=default_args,
    description='Main ETL pipeline for data processing',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'gcp', 'data_pipeline'],
    max_active_runs=1
)

# Configure alerts for the DAG
etl_main_dag = configure_dag_alerts(
    dag=etl_main_dag,
    on_failure_alert=True,
    on_retry_alert=True,
    on_success_alert=False,
    on_sla_miss_alert=True
)


def check_source_data_availability(**kwargs):
    """
    Check if source data is available for processing.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        bool: True if source data exists, False otherwise
    """
    # Get execution date from context
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y/%m/%d')
    
    # Initialize GCSClient with default GCP connection
    gcs_client = GCSClient()
    
    # Define source prefix with date partitioning
    source_prefix = f"{SOURCE_DATA_PREFIX}/{date_str}/"
    
    logger.info(f"Checking for source data with prefix: gs://{GCS_SOURCE_BUCKET}/{source_prefix}")
    
    # List files in GCS bucket with the date prefix
    file_list = gcs_client.list_files(bucket_name=GCS_SOURCE_BUCKET, prefix=source_prefix)
    
    # Log the number of files found
    if file_list:
        logger.info(f"Found {len(file_list)} files to process:")
        for file_path in file_list:
            logger.info(f"  - {file_path}")
        
        # Push file list to XCom for downstream tasks
        ti = kwargs['ti']
        ti.xcom_push(key='source_file_list', value=file_list)
        
        return True
    else:
        logger.warning(f"No files found with prefix: gs://{GCS_SOURCE_BUCKET}/{source_prefix}")
        return False


def select_workflow_branch(**kwargs):
    """
    Determine which processing branch to follow based on data availability.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        str: ID of the next task to execute based on condition
    """
    ti = kwargs['ti']
    data_available = ti.xcom_pull(task_ids='check_source_data', key='return_value')
    
    if data_available:
        logger.info("Source data is available in GCS, proceeding with GCS extraction path")
        return 'extract_from_gcs'
    else:
        logger.info("Source data not available in GCS, falling back to database extraction")
        return 'extract_from_database'


def extract_from_gcs(**kwargs):
    """
    Extract data from GCS storage.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        str: Path to the downloaded data file
    """
    ti = kwargs['ti']
    file_list = ti.xcom_pull(task_ids='check_source_data', key='source_file_list')
    
    # Ensure temp directory exists
    os.makedirs(TEMP_DATA_DIR, exist_ok=True)
    
    # Initialize GCS client
    gcs_client = GCSClient()
    
    downloaded_files = []
    total_size = 0
    
    # Download each file from GCS to local temp storage
    for file_path in file_list:
        # Parse bucket path to get object name
        object_name = file_path
        
        # Define local file path
        file_name = os.path.basename(object_name)
        local_file_path = os.path.join(TEMP_DATA_DIR, file_name)
        
        # Download the file
        gcs_client.download_file(
            bucket_name=GCS_SOURCE_BUCKET,
            object_name=object_name,
            local_file_path=local_file_path
        )
        
        # Track downloaded files and size
        file_size = os.path.getsize(local_file_path)
        total_size += file_size
        downloaded_files.append(local_file_path)
        
        logger.info(f"Downloaded gs://{GCS_SOURCE_BUCKET}/{object_name} to {local_file_path} ({file_size} bytes)")
    
    # Log download statistics
    logger.info(f"Successfully downloaded {len(downloaded_files)} files, total size: {total_size} bytes")
    
    # Push download results to XCom
    ti.xcom_push(key='extracted_files', value=downloaded_files)
    ti.xcom_push(key='extraction_source', value='gcs')
    ti.xcom_push(key='extraction_stats', value={'files': len(downloaded_files), 'size': total_size})
    
    return downloaded_files


def extract_from_database(**kwargs):
    """
    Extract data from PostgreSQL database.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        str: Path to extracted data file
    """
    # Get execution date from context
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Verify database connection
    if not verify_connection(conn_id=POSTGRES_CONN_ID):
        raise ValueError(f"Failed to connect to database using connection ID: {POSTGRES_CONN_ID}")
    
    # Ensure temp directory exists
    os.makedirs(TEMP_DATA_DIR, exist_ok=True)
    
    # Define extraction query with date filter
    extraction_query = f"""
        SELECT * FROM etl_source_data 
        WHERE data_date = %s
        ORDER BY id
    """
    
    # Execute query and get results as DataFrame
    start_time = datetime.datetime.now()
    df = execute_query_as_df(
        sql=extraction_query,
        parameters={'data_date': date_str},
        conn_id=POSTGRES_CONN_ID
    )
    end_time = datetime.datetime.now()
    
    # Calculate execution time
    execution_time = (end_time - start_time).total_seconds()
    
    # Save DataFrame to CSV
    local_file_path = os.path.join(TEMP_DATA_DIR, f"db_extract_{date_str}.csv")
    df.to_csv(local_file_path, index=False)
    
    # Log extraction statistics
    row_count = len(df)
    file_size = os.path.getsize(local_file_path)
    logger.info(f"Extracted {row_count} rows from database in {execution_time:.2f}s")
    logger.info(f"Saved extracted data to {local_file_path} ({file_size} bytes)")
    
    # Push extraction metadata to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='extracted_files', value=[local_file_path])
    ti.xcom_push(key='extraction_source', value='database')
    ti.xcom_push(key='extraction_stats', value={
        'rows': row_count,
        'size': file_size,
        'execution_time': execution_time
    })
    
    return local_file_path


def transform_data(**kwargs):
    """
    Transform extracted data according to business rules.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        str: Path to transformed data file
    """
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Get source data files from upstream tasks
    extracted_files = ti.xcom_pull(task_ids=[
        'extract_from_gcs', 
        'extract_from_database'
    ], key='extracted_files')
    
    # Flatten the list if needed (might be a list of lists)
    if isinstance(extracted_files[0], list):
        extracted_files = [item for sublist in extracted_files for item in sublist if sublist]
    
    # Ensure we have files to process
    if not extracted_files:
        raise ValueError("No extracted files available for transformation")
    
    logger.info(f"Transforming {len(extracted_files)} files")
    
    # Read all data files into a single DataFrame
    dataframes = []
    total_input_rows = 0
    
    for file_path in extracted_files:
        if file_path and os.path.exists(file_path):
            df = pd.read_csv(file_path)
            total_input_rows += len(df)
            dataframes.append(df)
        else:
            logger.warning(f"File does not exist or is None: {file_path}")
    
    if not dataframes:
        raise ValueError("No valid data files found for transformation")
    
    # Combine all DataFrames
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Start the transformation process
    start_time = datetime.datetime.now()
    
    # Apply transformations and business rules
    transformed_df = combined_df.copy()
    
    # 1. Clean data: Remove duplicates
    transformed_df = transformed_df.drop_duplicates()
    
    # 2. Handle missing values
    for column in transformed_df.columns:
        # Fill numeric columns with 0
        if pd.api.types.is_numeric_dtype(transformed_df[column]):
            transformed_df[column] = transformed_df[column].fillna(0)
        # Fill string columns with empty string
        elif pd.api.types.is_string_dtype(transformed_df[column]):
            transformed_df[column] = transformed_df[column].fillna('')
        # Fill date columns with execution date
        elif pd.api.types.is_datetime64_dtype(transformed_df[column]):
            transformed_df[column] = transformed_df[column].fillna(execution_date)
    
    # 3. Apply type conversions
    # Example: Convert certain columns to appropriate types
    for column in transformed_df.columns:
        if column.endswith('_date') and not pd.api.types.is_datetime64_dtype(transformed_df[column]):
            transformed_df[column] = pd.to_datetime(transformed_df[column], errors='coerce')
        elif column.endswith('_amount') and not pd.api.types.is_numeric_dtype(transformed_df[column]):
            transformed_df[column] = pd.to_numeric(transformed_df[column], errors='coerce')
    
    # 4. Apply business rules and calculations
    # This is a placeholder - add specific business transformations here
    if 'revenue' in transformed_df.columns and 'cost' in transformed_df.columns:
        transformed_df['profit'] = transformed_df['revenue'] - transformed_df['cost']
        transformed_df['profit_margin'] = transformed_df['profit'] / transformed_df['revenue']
    
    # 5. Add metadata columns
    transformed_df['etl_execution_date'] = execution_date
    transformed_df['etl_process_timestamp'] = datetime.datetime.now()
    
    # End transformation tracking
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    
    # Save transformed DataFrame to CSV
    transformed_file_path = os.path.join(TEMP_DATA_DIR, f"transformed_data_{date_str}.csv")
    transformed_df.to_csv(transformed_file_path, index=False)
    
    # Log transformation metrics
    output_rows = len(transformed_df)
    file_size = os.path.getsize(transformed_file_path)
    
    logger.info(f"Transformation completed in {execution_time:.2f}s")
    logger.info(f"Input: {total_input_rows} rows, Output: {output_rows} rows")
    logger.info(f"Saved transformed data to {transformed_file_path} ({file_size} bytes)")
    
    # Push transformation results to XCom
    transformation_stats = {
        'input_rows': total_input_rows,
        'output_rows': output_rows,
        'execution_time': execution_time,
        'file_size': file_size
    }
    ti.xcom_push(key='transformed_file', value=transformed_file_path)
    ti.xcom_push(key='transformation_stats', value=transformation_stats)
    
    return transformed_file_path


def load_to_bigquery(**kwargs):
    """
    Load transformed data to BigQuery.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        dict: Load statistics including record count and status
    """
    ti = kwargs['ti']
    
    # Get transformed data file
    transformed_file_path = ti.xcom_pull(task_ids='transform_data', key='transformed_file')
    
    if not transformed_file_path or not os.path.exists(transformed_file_path):
        raise ValueError(f"Transformed data file not found: {transformed_file_path}")
    
    # Read the transformed data
    df = pd.read_csv(transformed_file_path)
    
    # Initialize BigQuery client
    bq_client = BigQueryClient(conn_id=BQ_CONN_ID)
    
    # Ensure dataset exists
    if not bq_client.create_dataset(dataset_id=BQ_DATASET):
        raise ValueError(f"Failed to create or verify BigQuery dataset: {BQ_DATASET}")
    
    # Start upload tracking
    start_time = datetime.datetime.now()
    
    # Upload data to BigQuery
    success = bq_client.load_from_dataframe(
        dataframe=df,
        dataset_id=BQ_DATASET,
        table_id=BQ_TABLE,
        if_exists='replace'  # Could be 'append' based on requirements
    )
    
    if not success:
        raise ValueError(f"Failed to load data to BigQuery table: {BQ_DATASET}.{BQ_TABLE}")
    
    # End upload tracking
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    
    # Log load statistics
    row_count = len(df)
    logger.info(f"Successfully loaded {row_count} rows to BigQuery table {BQ_DATASET}.{BQ_TABLE}")
    logger.info(f"BigQuery load completed in {execution_time:.2f}s")
    
    # Push load results to XCom
    load_stats = {
        'rows': row_count,
        'execution_time': execution_time,
        'destination': f"{BQ_DATASET}.{BQ_TABLE}",
        'status': 'success'
    }
    ti.xcom_push(key='bigquery_load_stats', value=load_stats)
    
    return load_stats


def load_to_postgres(**kwargs):
    """
    Load transformed data to PostgreSQL database.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        dict: Load statistics including record count and status
    """
    ti = kwargs['ti']
    
    # Get transformed data file
    transformed_file_path = ti.xcom_pull(task_ids='transform_data', key='transformed_file')
    
    if not transformed_file_path or not os.path.exists(transformed_file_path):
        raise ValueError(f"Transformed data file not found: {transformed_file_path}")
    
    # Verify database connection
    if not verify_connection(conn_id=POSTGRES_CONN_ID):
        raise ValueError(f"Failed to connect to database using connection ID: {POSTGRES_CONN_ID}")
    
    # Read the transformed data
    df = pd.read_csv(transformed_file_path)
    
    # Start upload tracking
    start_time = datetime.datetime.now()
    
    # Define the target table
    target_table = 'etl_processed_data'
    
    # Load data to PostgreSQL
    success = bulk_load_from_df(
        df=df,
        table_name=target_table,
        conn_id=POSTGRES_CONN_ID,
        if_exists='replace'  # Could be 'append' based on requirements
    )
    
    if not success:
        raise ValueError(f"Failed to load data to PostgreSQL table: {target_table}")
    
    # End upload tracking
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    
    # Log load statistics
    row_count = len(df)
    logger.info(f"Successfully loaded {row_count} rows to PostgreSQL table {target_table}")
    logger.info(f"PostgreSQL load completed in {execution_time:.2f}s")
    
    # Push load results to XCom
    load_stats = {
        'rows': row_count,
        'execution_time': execution_time,
        'destination': target_table,
        'status': 'success'
    }
    ti.xcom_push(key='postgres_load_stats', value=load_stats)
    
    return load_stats


def upload_processed_data(**kwargs):
    """
    Upload processed data files to GCS.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        list: List of GCS URIs for uploaded files
    """
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y/%m/%d')
    
    # Get transformed data file
    transformed_file_path = ti.xcom_pull(task_ids='transform_data', key='transformed_file')
    
    if not transformed_file_path or not os.path.exists(transformed_file_path):
        raise ValueError(f"Transformed data file not found: {transformed_file_path}")
    
    # Initialize GCS client
    gcs_client = GCSClient()
    
    # Generate target object name with timestamp
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = os.path.basename(transformed_file_path)
    target_object = f"{PROCESSED_DATA_PREFIX}/{date_str}/{timestamp}_{file_name}"
    
    # Upload the file to GCS
    gcs_uri = gcs_client.upload_file(
        local_file_path=transformed_file_path,
        bucket_name=GCS_TARGET_BUCKET,
        object_name=target_object
    )
    
    # Log upload information
    file_size = os.path.getsize(transformed_file_path)
    logger.info(f"Uploaded transformed data ({file_size} bytes) to {gcs_uri}")
    
    # Push upload results to XCom
    upload_results = {
        'file_size': file_size,
        'gcs_uri': gcs_uri,
        'timestamp': timestamp
    }
    ti.xcom_push(key='uploaded_file', value=gcs_uri)
    ti.xcom_push(key='upload_stats', value=upload_results)
    
    return [gcs_uri]


def validate_etl_results(**kwargs):
    """
    Validate ETL process results and record metrics.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        dict: Validation results and process metrics
    """
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    
    # Collect metrics from all upstream tasks
    extraction_stats = ti.xcom_pull(
        task_ids=['extract_from_gcs', 'extract_from_database'], 
        key='extraction_stats'
    )
    # Handle the case where we might have multiple stats (one will be None)
    extraction_stats = next((stats for stats in extraction_stats if stats), {})
    
    transformation_stats = ti.xcom_pull(task_ids='transform_data', key='transformation_stats')
    bigquery_load_stats = ti.xcom_pull(task_ids='load_to_bigquery', key='bigquery_load_stats')
    postgres_load_stats = ti.xcom_pull(task_ids='load_to_postgres', key='postgres_load_stats')
    upload_stats = ti.xcom_pull(task_ids='upload_processed_data', key='upload_stats')
    
    # Calculate validation metrics
    input_rows = extraction_stats.get('rows', 0)
    if 'files' in extraction_stats and 'rows' not in extraction_stats:
        # If we extracted from GCS, use transformation input rows
        input_rows = transformation_stats.get('input_rows', 0)
    
    output_rows = transformation_stats.get('output_rows', 0)
    bq_rows = bigquery_load_stats.get('rows', 0)
    pg_rows = postgres_load_stats.get('rows', 0)
    
    # Record counts should match across the pipeline
    records_match = (output_rows == bq_rows == pg_rows)
    
    # Data quality metrics
    data_quality = {
        'record_count_match': records_match,
        'input_to_output_ratio': output_rows / input_rows if input_rows > 0 else 0,
        'success_rate': 1.0 if records_match else 0.0
    }
    
    # Overall ETL metrics
    etl_metrics = {
        'execution_date': execution_date.isoformat(),
        'extraction': extraction_stats,
        'transformation': transformation_stats,
        'bigquery_load': bigquery_load_stats,
        'postgres_load': postgres_load_stats,
        'gcs_upload': upload_stats,
        'data_quality': data_quality,
        'success': records_match
    }
    
    # Log validation results
    validation_success = records_match
    logger.info(f"ETL validation {'successful' if validation_success else 'failed'}")
    logger.info(f"Input records: {input_rows}, Output records: {output_rows}")
    logger.info(f"BigQuery records: {bq_rows}, PostgreSQL records: {pg_rows}")
    
    # Push validation results to XCom
    ti.xcom_push(key='validation_results', value=etl_metrics)
    
    return etl_metrics


def cleanup_temp_files(**kwargs):
    """
    Remove temporary files created during ETL process.
    
    Args:
        **kwargs: Context dictionary provided by Airflow
        
    Returns:
        bool: True if cleanup successful, False otherwise
    """
    ti = kwargs['ti']
    
    # Get all file paths from XComs
    all_file_paths = []
    
    # Extracted files
    extracted_files = ti.xcom_pull(
        task_ids=['extract_from_gcs', 'extract_from_database'], 
        key='extracted_files'
    )
    # Handle the case where we might have multiple lists (one will be None)
    for files in extracted_files:
        if files:
            if isinstance(files, list):
                all_file_paths.extend(files)
            else:
                all_file_paths.append(files)
    
    # Transformed file
    transformed_file = ti.xcom_pull(task_ids='transform_data', key='transformed_file')
    if transformed_file:
        all_file_paths.append(transformed_file)
    
    # Remove duplicates
    all_file_paths = list(set(all_file_paths))
    
    # Track cleanup statistics
    files_removed = 0
    space_freed = 0
    
    # Delete each file
    for file_path in all_file_paths:
        if file_path and os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            try:
                os.remove(file_path)
                files_removed += 1
                space_freed += file_size
                logger.debug(f"Removed temporary file: {file_path} ({file_size} bytes)")
            except Exception as e:
                logger.warning(f"Failed to remove file {file_path}: {str(e)}")
    
    # Try to remove the temp directory if it's empty
    try:
        if os.path.exists(TEMP_DATA_DIR) and not os.listdir(TEMP_DATA_DIR):
            os.rmdir(TEMP_DATA_DIR)
            logger.debug(f"Removed empty temporary directory: {TEMP_DATA_DIR}")
    except Exception as e:
        logger.warning(f"Failed to remove directory {TEMP_DATA_DIR}: {str(e)}")
    
    # Log cleanup statistics
    logger.info(f"Cleanup completed: removed {files_removed} files, freed {space_freed} bytes")
    
    return True


# Define tasks
start = DummyOperator(
    task_id='start',
    dag=etl_main_dag
)

check_source_data = PythonOperator(
    task_id='check_source_data',
    python_callable=check_source_data_availability,
    dag=etl_main_dag
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=select_workflow_branch,
    dag=etl_main_dag
)

extract_gcs = PythonOperator(
    task_id='extract_from_gcs',
    python_callable=extract_from_gcs,
    dag=etl_main_dag
)

extract_db = PythonOperator(
    task_id='extract_from_database',
    python_callable=extract_from_database,
    dag=etl_main_dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=etl_main_dag
)

load_bq = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=etl_main_dag
)

load_pg = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=etl_main_dag
)

upload_to_gcs = PythonOperator(
    task_id='upload_processed_data',
    python_callable=upload_processed_data,
    dag=etl_main_dag
)

validate = PythonOperator(
    task_id='validate_etl_results',
    python_callable=validate_etl_results,
    dag=etl_main_dag
)

cleanup = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=etl_main_dag
)

end = DummyOperator(
    task_id='end',
    dag=etl_main_dag,
    trigger_rule='none_failed'  # Run even if upstream tasks are skipped
)

# Set up task dependencies
start >> check_source_data >> branch_task

branch_task >> extract_gcs
branch_task >> extract_db

extract_gcs >> transform
extract_db >> transform

transform >> [load_bq, load_pg, upload_to_gcs]

[load_bq, load_pg, upload_to_gcs] >> validate >> cleanup >> end