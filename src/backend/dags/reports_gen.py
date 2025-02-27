"""
DAG for automated report generation in Apache Airflow 2.X.

This workflow orchestrates the creation and distribution of regular reports by
extracting data from source systems, transforming it into reportable formats,
and delivering the results to specified destinations like GCS buckets, databases, 
or via email notifications.
"""

import os
import logging
from datetime import datetime, timedelta
import pandas as pd

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator

# Internal imports
from .utils.gcp_utils import bigquery_execute_query, gcs_upload_file, BigQueryClient
from .utils.db_utils import execute_query_as_df, get_postgres_hook
from .utils.alert_utils import configure_dag_alerts, send_email_alert
from .utils.validation_utils import validate_dag
from plugins.operators.custom_gcp_operator import BigQueryExecuteQueryOperator
from plugins.operators.custom_postgres_operator import CustomPostgresOperator

# Set up logging
logger = logging.getLogger('airflow.reports_gen')

# Default arguments for all DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['reports-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1)
}

# GCS bucket for storing reports
REPORTS_BUCKET = 'reports-output-bucket'

# Base path template for reports in GCS
REPORTS_BASE_PATH = 'reports/{report_type}/{date_str}'

# Default configuration for different report types
DEFAULT_REPORT_CONFIG = {
    'weekly': {
        'schedule': '@weekly',
        'sql_path': 'sql/weekly_reports.sql',
        'recipients': ['weekly-reports@example.com']
    },
    'monthly': {
        'schedule': '@monthly',
        'sql_path': 'sql/monthly_reports.sql',
        'recipients': ['monthly-reports@example.com']
    },
    'quarterly': {
        'schedule': '0 0 1 */3 *',  # First day of every third month
        'sql_path': 'sql/quarterly_reports.sql',
        'recipients': ['quarterly-reports@example.com']
    }
}


def get_report_config(report_type: str) -> dict:
    """
    Get configuration for a specific report type.
    
    Args:
        report_type: Type of report (weekly, monthly, quarterly)
        
    Returns:
        Report configuration including schedule, SQL, and recipients
    """
    if report_type not in DEFAULT_REPORT_CONFIG:
        logger.warning(f"Unknown report type: {report_type}. Using default configuration.")
        return DEFAULT_REPORT_CONFIG['monthly']  # Default to monthly
    
    return DEFAULT_REPORT_CONFIG[report_type]


def extract_report_data(**kwargs) -> pd.DataFrame:
    """
    Extract data for reports from PostgreSQL or BigQuery source systems.
    
    Args:
        **kwargs: Task context containing execution_date and report_type
    
    Returns:
        Extracted data for the report
    """
    # Extract task context
    task_instance = kwargs['ti']
    execution_date = kwargs['execution_date']
    report_type = kwargs.get('report_type', 'monthly')
    
    # Get report configuration
    config = get_report_config(report_type)
    sql_path = config['sql_path']
    
    # Format date strings for queries
    date_str = execution_date.strftime('%Y-%m-%d')
    month_start = execution_date.replace(day=1).strftime('%Y-%m-%d')
    month_end = execution_date.strftime('%Y-%m-%d')
    
    logger.info(f"Extracting data for {report_type} report, date: {date_str}")
    
    # Determine data source based on report type (PostgreSQL or BigQuery)
    if report_type in ['weekly', 'monthly']:
        # Use PostgreSQL for weekly and monthly reports
        try:
            # Read SQL from file
            sql_file_path = os.path.join(os.path.dirname(__file__), sql_path)
            with open(sql_file_path, 'r') as f:
                sql = f.read()
                
            # Replace parameters in SQL
            sql = sql.replace('{{date}}', date_str)
            sql = sql.replace('{{month_start}}', month_start)
            sql = sql.replace('{{month_end}}', month_end)
            
            # Execute query and get DataFrame
            df = execute_query_as_df(
                sql=sql,
                conn_id='postgres_default'
            )
            
            logger.info(f"Successfully extracted {len(df)} rows from PostgreSQL for {report_type} report")
            
            # Push to XCom for downstream tasks
            task_instance.xcom_push(key='extracted_data', value=df.to_json())
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from PostgreSQL: {str(e)}")
            raise
            
    else:
        # Use BigQuery for quarterly reports
        try:
            # Read SQL from file
            sql_file_path = os.path.join(os.path.dirname(__file__), sql_path)
            with open(sql_file_path, 'r') as f:
                sql = f.read()
                
            # Replace parameters in SQL
            sql = sql.replace('{{date}}', date_str)
            sql = sql.replace('{{month_start}}', month_start)
            sql = sql.replace('{{month_end}}', month_end)
            
            # Execute BigQuery
            df = bigquery_execute_query(
                sql=sql,
                as_dataframe=True
            )
            
            logger.info(f"Successfully extracted {len(df)} rows from BigQuery for {report_type} report")
            
            # Push to XCom for downstream tasks
            task_instance.xcom_push(key='extracted_data', value=df.to_json())
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from BigQuery: {str(e)}")
            raise


def transform_report_data(**kwargs) -> pd.DataFrame:
    """
    Transform and format extracted data into report format.
    
    Args:
        **kwargs: Task context
    
    Returns:
        Transformed data ready for report generation
    """
    # Extract task context
    task_instance = kwargs['ti']
    execution_date = kwargs['execution_date']
    report_type = kwargs.get('report_type', 'monthly')
    
    # Get extracted data from upstream task
    json_data = task_instance.xcom_pull(task_ids=f'extract_data', key='extracted_data')
    df = pd.read_json(json_data)
    
    if df.empty:
        logger.warning(f"No data available for {report_type} report transformation")
        return df
    
    logger.info(f"Transforming data for {report_type} report, rows before: {len(df)}")
    
    # Apply transformations based on report type
    if report_type == 'weekly':
        # Weekly report transformations
        # - Aggregate by day
        # - Calculate weekly totals and averages
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df['day_of_week'] = df['date'].dt.day_name()
        
        # Example aggregation
        if 'value' in df.columns:
            df['weekly_pct'] = df['value'] / df['value'].sum() * 100
            
        # Summary statistics
        if len(df) > 0:
            summary_df = pd.DataFrame({
                'metric': ['Total', 'Average', 'Min', 'Max'],
                'value': [
                    df['value'].sum() if 'value' in df.columns else 0,
                    df['value'].mean() if 'value' in df.columns else 0,
                    df['value'].min() if 'value' in df.columns else 0,
                    df['value'].max() if 'value' in df.columns else 0
                ]
            })
            # Append summary to original data
            df = pd.concat([df, summary_df], ignore_index=True)
            
    elif report_type == 'monthly':
        # Monthly report transformations
        # - Aggregate by week
        # - Calculate monthly trends
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df['week_of_month'] = df['date'].dt.isocalendar().week
            
        # Example: Calculate week-over-week growth
        if 'value' in df.columns and 'week_of_month' in df.columns:
            df = df.sort_values('week_of_month')
            df['prev_week_value'] = df['value'].shift(1)
            df['wow_growth'] = (df['value'] - df['prev_week_value']) / df['prev_week_value'] * 100
            
    elif report_type == 'quarterly':
        # Quarterly report transformations
        # - Aggregate by month
        # - Calculate quarterly summaries
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df['month'] = df['date'].dt.month
            
        # Example: Calculate month-over-month comparisons
        if 'value' in df.columns and 'month' in df.columns:
            df = df.sort_values('month')
            df['prev_month_value'] = df['value'].shift(1)
            df['mom_growth'] = (df['value'] - df['prev_month_value']) / df['prev_month_value'] * 100
            
        # Add quarter summary
        if 'value' in df.columns:
            quarter_total = df['value'].sum()
            quarter_avg = df['value'].mean()
            df['pct_of_quarter'] = df['value'] / quarter_total * 100
    
    # Add metadata
    df['report_generation_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['report_type'] = report_type
    df['report_period'] = execution_date.strftime('%Y-%m-%d')
    
    logger.info(f"Data transformation complete, rows after: {len(df)}")
    
    # Push to XCom for downstream tasks
    task_instance.xcom_push(key='transformed_data', value=df.to_json())
    return df


def generate_report(**kwargs) -> str:
    """
    Generate report file in specified format (CSV, Excel, PDF).
    
    Args:
        **kwargs: Task context
    
    Returns:
        Local path to generated report file
    """
    # Extract task context
    task_instance = kwargs['ti']
    execution_date = kwargs['execution_date']
    report_type = kwargs.get('report_type', 'monthly')
    report_format = kwargs.get('report_format', 'csv')
    
    # Get transformed data from upstream task
    json_data = task_instance.xcom_pull(task_ids=f'transform_data', key='transformed_data')
    df = pd.read_json(json_data)
    
    if df.empty:
        logger.warning(f"No data available for {report_type} report generation")
        # Create an empty DataFrame with metadata
        df = pd.DataFrame({
            'report_type': [report_type],
            'report_date': [execution_date.strftime('%Y-%m-%d')],
            'message': ['No data available for this period']
        })
    
    # Create appropriate file name
    date_str = execution_date.strftime('%Y%m%d')
    file_name = f"{report_type}_report_{date_str}.{report_format}"
    
    # Create local temp path
    temp_dir = '/tmp/airflow_reports'
    os.makedirs(temp_dir, exist_ok=True)
    local_path = os.path.join(temp_dir, file_name)
    
    logger.info(f"Generating {report_format} report for {report_type} at {local_path}")
    
    # Generate report in the specified format
    if report_format.lower() == 'csv':
        df.to_csv(local_path, index=False)
        
    elif report_format.lower() == 'xlsx':
        # Create Excel with formatting
        with pd.ExcelWriter(local_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name=f'{report_type.capitalize()} Report', index=False)
            
            # Get workbook and worksheet objects for formatting
            workbook = writer.book
            worksheet = writer.sheets[f'{report_type.capitalize()} Report']
            
            # Add formats
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            # Write headers with formatting
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                
            # Auto-adjust columns width
            for i, col in enumerate(df.columns):
                column_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, column_width)
                
    elif report_format.lower() == 'pdf':
        try:
            # This requires additional libraries like fpdf or another PDF generation library
            # For simplicity, we'll create a CSV and note that it should be converted to PDF
            df.to_csv(local_path.replace('.pdf', '.csv'), index=False)
            
            # For a real implementation, you would use a PDF library like this:
            # from fpdf import FPDF
            # pdf = FPDF()
            # pdf.add_page()
            # pdf.set_font("Arial", size=12)
            # # Add table data
            # # ...
            # pdf.output(local_path)
            
            logger.warning("PDF generation is a placeholder - would require a PDF library")
            with open(local_path, 'w') as f:
                f.write(f"PDF Report for {report_type} - {date_str}\n")
                f.write("This is a placeholder for actual PDF generation")
                
        except Exception as e:
            logger.error(f"Error generating PDF: {str(e)}")
            # Fall back to CSV
            local_path = local_path.replace('.pdf', '.csv')
            df.to_csv(local_path, index=False)
    
    logger.info(f"Successfully generated {report_format} report at {local_path}")
    
    # Push to XCom for downstream tasks
    task_instance.xcom_push(key='report_path', value=local_path)
    task_instance.xcom_push(key='report_format', value=report_format)
    
    return local_path


def upload_report(**kwargs) -> str:
    """
    Upload generated report to GCS destination.
    
    Args:
        **kwargs: Task context
    
    Returns:
        GCS URI of uploaded report
    """
    # Extract task context
    task_instance = kwargs['ti']
    execution_date = kwargs['execution_date']
    report_type = kwargs.get('report_type', 'monthly')
    report_format = kwargs.get('report_format', 'csv')
    
    # Get the local file path from upstream task
    local_path = task_instance.xcom_pull(
        task_ids=f'generate_{report_format}_report', 
        key='report_path'
    )
    
    if not local_path or not os.path.exists(local_path):
        error_msg = f"Report file not found at {local_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Format GCS destination path
    date_str = execution_date.strftime('%Y/%m/%d')
    base_path = REPORTS_BASE_PATH.format(report_type=report_type, date_str=date_str)
    file_name = os.path.basename(local_path)
    object_name = f"{base_path}/{file_name}"
    
    logger.info(f"Uploading report to gs://{REPORTS_BUCKET}/{object_name}")
    
    try:
        # Upload to GCS
        gcs_uri = gcs_upload_file(
            local_file_path=local_path,
            bucket_name=REPORTS_BUCKET,
            object_name=object_name
        )
        
        logger.info(f"Successfully uploaded report to {gcs_uri}")
        
        # Push to XCom for downstream tasks
        task_instance.xcom_push(key='gcs_uri', value=gcs_uri)
        
        # Remove local file after successful upload
        try:
            os.remove(local_path)
            logger.info(f"Removed local file: {local_path}")
        except Exception as e:
            logger.warning(f"Failed to remove local file {local_path}: {str(e)}")
        
        return gcs_uri
        
    except Exception as e:
        logger.error(f"Failed to upload report to GCS: {str(e)}")
        raise


def notify_report_ready(**kwargs) -> bool:
    """
    Send notification that report is ready.
    
    Args:
        **kwargs: Task context
    
    Returns:
        True if notification was successfully sent
    """
    # Extract task context
    task_instance = kwargs['ti']
    execution_date = kwargs['execution_date']
    report_type = kwargs.get('report_type', 'monthly')
    
    # Get configuration
    config = get_report_config(report_type)
    recipients = config.get('recipients', ['reports-team@example.com'])
    
    # Get GCS URI from upstream task
    # For quarterly reports, we might have multiple URIs (Excel and PDF)
    if report_type == 'quarterly':
        excel_uri = task_instance.xcom_pull(task_ids='upload_excel_to_gcs', key='gcs_uri')
        pdf_uri = task_instance.xcom_pull(task_ids='upload_pdf_to_gcs', key='gcs_uri')
        uris = [uri for uri in [excel_uri, pdf_uri] if uri]
    else:
        uri = task_instance.xcom_pull(task_ids='upload_to_gcs', key='gcs_uri')
        uris = [uri] if uri else []
    
    if not uris:
        logger.warning(f"No report URIs found for notification")
        uris = ["<Report not available>"]
    
    # Format date for email
    formatted_date = execution_date.strftime('%B %d, %Y')
    
    # Format email subject and body
    subject = f"{report_type.capitalize()} Report Ready - {formatted_date}"
    
    body = f"""
    <h2>{report_type.capitalize()} Report - {formatted_date}</h2>
    <p>Your {report_type} report for {formatted_date} is now available.</p>
    <p>You can access the report using the following link(s):</p>
    <ul>
    """
    
    for uri in uris:
        body += f"<li><a href='{uri}'>{uri}</a></li>"
    
    body += """
    </ul>
    <p>This is an automated notification from the Airflow reporting system.</p>
    """
    
    try:
        # Send email notification
        success = send_email_alert(
            subject=subject,
            body=body,
            to=recipients
        )
        
        if success:
            logger.info(f"Successfully sent notification to {recipients}")
        else:
            logger.warning(f"Failed to send notification to {recipients}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error sending notification: {str(e)}")
        return False


# Weekly Reports DAG
weekly_reports_dag = DAG(
    dag_id='weekly_reports',
    description='Weekly reports generation workflow',
    schedule_interval='@weekly',
    default_args=default_args,
    catchup=False,
    tags=['reports', 'weekly', 'airflow2']
)

# Configure alerts for the DAG
weekly_reports_dag = configure_dag_alerts(
    dag=weekly_reports_dag,
    email_recipients=DEFAULT_REPORT_CONFIG['weekly']['recipients']
)

with weekly_reports_dag:
    start = DummyOperator(
        task_id='start'
    )
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_report_data,
        op_kwargs={'report_type': 'weekly'}
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_report_data,
        op_kwargs={'report_type': 'weekly'}
    )
    
    generate_csv_report = PythonOperator(
        task_id='generate_csv_report',
        python_callable=generate_report,
        op_kwargs={
            'report_type': 'weekly',
            'report_format': 'csv'
        }
    )
    
    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_report,
        op_kwargs={'report_type': 'weekly'}
    )
    
    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=notify_report_ready,
        op_kwargs={'report_type': 'weekly'}
    )
    
    end = DummyOperator(
        task_id='end'
    )
    
    # Define task dependencies
    start >> extract_data >> transform_data >> generate_csv_report >> upload_to_gcs >> send_notification >> end


# Monthly Reports DAG
monthly_reports_dag = DAG(
    dag_id='monthly_reports',
    description='Monthly reports generation workflow',
    schedule_interval='@monthly',
    default_args=default_args,
    catchup=False,
    tags=['reports', 'monthly', 'airflow2']
)

# Configure alerts for the DAG
monthly_reports_dag = configure_dag_alerts(
    dag=monthly_reports_dag,
    email_recipients=DEFAULT_REPORT_CONFIG['monthly']['recipients']
)

with monthly_reports_dag:
    start = DummyOperator(
        task_id='start'
    )
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_report_data,
        op_kwargs={'report_type': 'monthly'}
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_report_data,
        op_kwargs={'report_type': 'monthly'}
    )
    
    generate_excel_report = PythonOperator(
        task_id='generate_excel_report',
        python_callable=generate_report,
        op_kwargs={
            'report_type': 'monthly',
            'report_format': 'xlsx'
        }
    )
    
    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_report,
        op_kwargs={'report_type': 'monthly'}
    )
    
    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=notify_report_ready,
        op_kwargs={'report_type': 'monthly'}
    )
    
    end = DummyOperator(
        task_id='end'
    )
    
    # Define task dependencies
    start >> extract_data >> transform_data >> generate_excel_report >> upload_to_gcs >> send_notification >> end


# Quarterly Reports DAG
quarterly_reports_dag = DAG(
    dag_id='quarterly_reports',
    description='Quarterly reports generation workflow',
    schedule_interval='0 0 1 */3 *',  # First day of every third month
    default_args=default_args,
    catchup=False,
    tags=['reports', 'quarterly', 'airflow2']
)

# Configure alerts for the DAG
quarterly_reports_dag = configure_dag_alerts(
    dag=quarterly_reports_dag,
    email_recipients=DEFAULT_REPORT_CONFIG['quarterly']['recipients']
)

with quarterly_reports_dag:
    start = DummyOperator(
        task_id='start'
    )
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_report_data,
        op_kwargs={'report_type': 'quarterly'}
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_report_data,
        op_kwargs={'report_type': 'quarterly'}
    )
    
    generate_excel_report = PythonOperator(
        task_id='generate_excel_report',
        python_callable=generate_report,
        op_kwargs={
            'report_type': 'quarterly',
            'report_format': 'xlsx'
        }
    )
    
    generate_pdf_report = PythonOperator(
        task_id='generate_pdf_report',
        python_callable=generate_report,
        op_kwargs={
            'report_type': 'quarterly',
            'report_format': 'pdf'
        }
    )
    
    upload_excel_to_gcs = PythonOperator(
        task_id='upload_excel_to_gcs',
        python_callable=upload_report,
        op_kwargs={
            'report_type': 'quarterly',
            'report_format': 'xlsx'
        }
    )
    
    upload_pdf_to_gcs = PythonOperator(
        task_id='upload_pdf_to_gcs',
        python_callable=upload_report,
        op_kwargs={
            'report_type': 'quarterly',
            'report_format': 'pdf'
        }
    )
    
    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=notify_report_ready,
        op_kwargs={'report_type': 'quarterly'}
    )
    
    end = DummyOperator(
        task_id='end'
    )
    
    # Define task dependencies
    start >> extract_data >> transform_data
    transform_data >> generate_excel_report >> upload_excel_to_gcs
    transform_data >> generate_pdf_report >> upload_pdf_to_gcs
    upload_excel_to_gcs >> send_notification
    upload_pdf_to_gcs >> send_notification
    send_notification >> end