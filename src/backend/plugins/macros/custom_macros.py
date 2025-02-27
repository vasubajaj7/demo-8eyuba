"""
Custom Apache Airflow templating macros for Cloud Composer 2 with Airflow 2.X.

This module provides a centralized collection of template macros that extend Airflow's
Jinja templating capabilities to support date manipulation, environment detection,
GCP operations, SQL formatting, and data transformation functions.
"""

import os
import logging
import datetime
import json
from typing import Dict, List, Optional, Union, Any

# Third-party imports
import pendulum  # pendulum v2.1.2

# Internal imports
from src.backend.dags.utils.gcp_utils import gcs_file_exists, get_secret
from src.backend.dags.utils.db_utils import execute_query, execute_query_as_df

# Configure logging
logger = logging.getLogger(__name__)

# Global constants
ENV_VAR_PREFIX = 'AIRFLOW_VAR_'
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_ENV_NAME = 'dev'
GCS_URL_PREFIX = 'gs://'
DEFAULT_GCP_CONN_ID = 'google_cloud_default'


# Date Manipulation Functions
def ds_format(ds: str, input_format: str, output_format: str) -> str:
    """
    Format a date string from one format to another using Airflow's ds standard.
    
    Args:
        ds: The date string to reformat
        input_format: The current format of the date string
        output_format: The desired output format
        
    Returns:
        Reformatted date string
    """
    try:
        date_obj = datetime.datetime.strptime(ds, input_format)
        formatted_date = date_obj.strftime(output_format)
        logger.debug(f"Reformatted date '{ds}' from '{input_format}' to '{output_format}': {formatted_date}")
        return formatted_date
    except Exception as e:
        logger.error(f"Error formatting date '{ds}' from '{input_format}' to '{output_format}': {str(e)}")
        return ds  # Return original if formatting fails


def ds_add(ds: str, days: int, date_format: str = DEFAULT_DATE_FORMAT) -> str:
    """
    Add a specified number of days to a date string.
    
    Args:
        ds: The date string
        days: Number of days to add
        date_format: Format of the date string (default: YYYY-MM-DD)
        
    Returns:
        Date string with days added
    """
    try:
        date_obj = datetime.datetime.strptime(ds, date_format)
        new_date = date_obj + datetime.timedelta(days=days)
        new_date_str = new_date.strftime(date_format)
        logger.debug(f"Added {days} days to '{ds}', result: {new_date_str}")
        return new_date_str
    except Exception as e:
        logger.error(f"Error adding {days} days to '{ds}': {str(e)}")
        return ds  # Return original if operation fails


def ds_subtract(ds: str, days: int, date_format: str = DEFAULT_DATE_FORMAT) -> str:
    """
    Subtract a specified number of days from a date string.
    
    Args:
        ds: The date string
        days: Number of days to subtract
        date_format: Format of the date string (default: YYYY-MM-DD)
        
    Returns:
        Date string with days subtracted
    """
    try:
        date_obj = datetime.datetime.strptime(ds, date_format)
        new_date = date_obj - datetime.timedelta(days=days)
        new_date_str = new_date.strftime(date_format)
        logger.debug(f"Subtracted {days} days from '{ds}', result: {new_date_str}")
        return new_date_str
    except Exception as e:
        logger.error(f"Error subtracting {days} days from '{ds}': {str(e)}")
        return ds  # Return original if operation fails


def month_start(ds: str, date_format: str = DEFAULT_DATE_FORMAT) -> str:
    """
    Get the first day of the month for a given date string.
    
    Args:
        ds: The date string
        date_format: Format of the date string (default: YYYY-MM-DD)
        
    Returns:
        First day of the month as a date string
    """
    try:
        date_obj = datetime.datetime.strptime(ds, date_format)
        first_day = date_obj.replace(day=1)
        first_day_str = first_day.strftime(date_format)
        logger.debug(f"First day of month for '{ds}': {first_day_str}")
        return first_day_str
    except Exception as e:
        logger.error(f"Error finding first day of month for '{ds}': {str(e)}")
        return ds  # Return original if operation fails


def month_end(ds: str, date_format: str = DEFAULT_DATE_FORMAT) -> str:
    """
    Get the last day of the month for a given date string.
    
    Args:
        ds: The date string
        date_format: Format of the date string (default: YYYY-MM-DD)
        
    Returns:
        Last day of the month as a date string
    """
    try:
        # Use pendulum for easier last day of month calculation
        date_obj = datetime.datetime.strptime(ds, date_format)
        
        # Convert to pendulum date
        pendulum_date = pendulum.from_timestamp(date_obj.timestamp())
        
        # Get last day of month
        last_day = pendulum_date.end_of('month')
        
        # Convert back to datetime and format
        last_day_str = last_day.strftime(date_format)
        
        logger.debug(f"Last day of month for '{ds}': {last_day_str}")
        return last_day_str
    except Exception as e:
        logger.error(f"Error finding last day of month for '{ds}': {str(e)}")
        return ds  # Return original if operation fails


def date_range_array(start_date: str, end_date: str, date_format: str = DEFAULT_DATE_FORMAT) -> List[str]:
    """
    Generate an array of date strings between start and end dates.
    
    Args:
        start_date: Start date string
        end_date: End date string
        date_format: Format of the date strings (default: YYYY-MM-DD)
        
    Returns:
        List of date strings between start and end date (inclusive)
    """
    try:
        start = datetime.datetime.strptime(start_date, date_format)
        end = datetime.datetime.strptime(end_date, date_format)
        
        if start > end:
            logger.warning(f"Start date '{start_date}' is after end date '{end_date}', swapping dates")
            start, end = end, start
        
        dates = []
        current = start
        
        while current <= end:
            dates.append(current.strftime(date_format))
            current += datetime.timedelta(days=1)
        
        logger.debug(f"Generated {len(dates)} dates between '{start_date}' and '{end_date}'")
        return dates
    except Exception as e:
        logger.error(f"Error generating date range between '{start_date}' and '{end_date}': {str(e)}")
        return [start_date, end_date]  # Return just the start and end dates if operation fails


# Environment and Configuration Functions
def get_env_var(var_name: str, default_value: str = '') -> str:
    """
    Get an environment variable with an optional default value.
    
    Args:
        var_name: Name of the environment variable
        default_value: Default value if not found
        
    Returns:
        Environment variable value or default if not found
    """
    # Check if the variable name already has the prefix
    if not var_name.startswith(ENV_VAR_PREFIX):
        prefixed_var_name = f"{ENV_VAR_PREFIX}{var_name}"
    else:
        prefixed_var_name = var_name
    
    value = os.environ.get(prefixed_var_name, default_value)
    
    # For security, don't log the actual value if it might be sensitive
    if 'password' in var_name.lower() or 'secret' in var_name.lower() or 'key' in var_name.lower():
        log_value = '******'
    else:
        log_value = value
    
    logger.debug(f"Retrieved environment variable '{prefixed_var_name}': {log_value}")
    return value


def get_env_name() -> str:
    """
    Get the current environment name (dev, qa, prod).
    
    Returns:
        Current environment name
    """
    # Check common environment variable names
    env_name = os.environ.get('ENVIRONMENT', 
                os.environ.get('AIRFLOW_ENV', 
                os.environ.get('ENV', DEFAULT_ENV_NAME)))
    
    logger.debug(f"Current environment: {env_name}")
    return env_name


def is_env(env_name: str) -> bool:
    """
    Check if current environment matches the specified name.
    
    Args:
        env_name: Environment name to check
        
    Returns:
        True if current environment matches, False otherwise
    """
    current_env = get_env_name()
    result = current_env.lower() == env_name.lower()
    
    logger.debug(f"Environment check: current={current_env}, requested={env_name}, match={result}")
    return result


# GCP Operations Functions
def gcs_path(bucket: str, object_path: str) -> str:
    """
    Format a GCS path with bucket and object.
    
    Args:
        bucket: GCS bucket name
        object_path: Object path within bucket
        
    Returns:
        Fully qualified GCS URI (gs://bucket/object)
    """
    # Ensure bucket doesn't already have gs:// prefix
    if bucket.startswith(GCS_URL_PREFIX):
        bucket = bucket[len(GCS_URL_PREFIX):]
    
    # Ensure there's a single slash between bucket and object
    if object_path.startswith('/'):
        object_path = object_path[1:]
    
    gcs_uri = f"{GCS_URL_PREFIX}{bucket}/{object_path}"
    
    logger.debug(f"Formatted GCS path: {gcs_uri}")
    return gcs_uri


def check_gcs_file(bucket: str, object_path: str, conn_id: str = DEFAULT_GCP_CONN_ID) -> bool:
    """
    Check if a file exists in GCS bucket.
    
    Args:
        bucket: GCS bucket name
        object_path: Object path within bucket
        conn_id: GCP connection ID
        
    Returns:
        True if file exists, False otherwise
    """
    # Ensure bucket doesn't already have gs:// prefix
    if bucket.startswith(GCS_URL_PREFIX):
        bucket = bucket[len(GCS_URL_PREFIX):]
    
    # Ensure there's no leading slash in object path
    if object_path.startswith('/'):
        object_path = object_path[1:]
    
    exists = gcs_file_exists(bucket, object_path, conn_id)
    
    logger.debug(f"GCS file check for gs://{bucket}/{object_path}: {'exists' if exists else 'does not exist'}")
    return exists


def get_gcp_secret(secret_id: str, version_id: str = 'latest', conn_id: str = DEFAULT_GCP_CONN_ID) -> str:
    """
    Retrieve a secret from Google Cloud Secret Manager.
    
    Args:
        secret_id: ID of the secret to retrieve
        version_id: Version of the secret (default: 'latest')
        conn_id: GCP connection ID
        
    Returns:
        Secret value
    """
    try:
        secret_value = get_secret(secret_id, version_id, conn_id)
        logger.info(f"Retrieved secret '{secret_id}' (version: {version_id})")
        return secret_value
    except Exception as e:
        logger.error(f"Error retrieving secret '{secret_id}': {str(e)}")
        return ""


# SQL and Database Functions
def format_sql(sql: str, params: Dict[str, Any] = None) -> str:
    """
    Format SQL template with parameters for logging and debugging.
    
    Args:
        sql: SQL template to format
        params: Parameters to substitute
        
    Returns:
        Formatted SQL statement with parameter values
    """
    if not params:
        return sql
    
    formatted_sql = sql
    try:
        # Handle different SQL parameter styles: %s, %(name)s, :name, $name
        for key, value in params.items():
            # Format for display (this is for logging only, not for execution)
            if value is None:
                display_value = 'NULL'
            elif isinstance(value, str):
                display_value = f"'{value}'"
            elif isinstance(value, (int, float)):
                display_value = str(value)
            elif isinstance(value, (list, tuple)):
                display_value = str(value)
            elif isinstance(value, dict):
                display_value = json.dumps(value)
            else:
                display_value = str(value)
            
            # Replace different parameter styles
            formatted_sql = formatted_sql.replace(f"%({key})s", display_value)  # psycopg2 named params
            formatted_sql = formatted_sql.replace(f":{key}", display_value)      # SQLAlchemy named params
            formatted_sql = formatted_sql.replace(f"${key}", display_value)      # PostgreSQL jsonb params
        
        logger.debug(f"Formatted SQL for debugging: {formatted_sql}")
        return formatted_sql
    except Exception as e:
        logger.error(f"Error formatting SQL: {str(e)}")
        return sql  # Return original if formatting fails


def query_result_as_dict(sql: str, params: Dict[str, Any] = None, conn_id: str = None) -> Dict[str, Any]:
    """
    Execute a SQL query and return first row as a dictionary.
    
    Args:
        sql: SQL query to execute
        params: Query parameters
        conn_id: Database connection ID
        
    Returns:
        First row of query results as a dictionary
    """
    try:
        # Format the SQL for logging
        log_sql = format_sql(sql, params)
        logger.info(f"Executing query: {log_sql}")
        
        # Execute the query with return_dict=True to get dictionary results
        results = execute_query(sql=sql, parameters=params, conn_id=conn_id, return_dict=True)
        
        if results and len(results) > 0:
            logger.info(f"Query returned {len(results)} rows, using first row")
            return results[0]
        else:
            logger.info("Query returned no results")
            return {}
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return {}


def fiscal_quarter(ds: str, fiscal_start_month: int = 1, date_format: str = DEFAULT_DATE_FORMAT) -> Dict[str, Any]:
    """
    Calculate fiscal quarter from a date string based on fiscal year start month.
    
    Args:
        ds: The date string
        fiscal_start_month: Month number (1-12) when fiscal year starts (default: 1 = January)
        date_format: Format of the date string (default: YYYY-MM-DD)
        
    Returns:
        Dictionary with fiscal quarter, year, and quarter start/end dates
    """
    try:
        date_obj = datetime.datetime.strptime(ds, date_format)
        month = date_obj.month
        year = date_obj.year
        
        # Calculate fiscal year
        if month < fiscal_start_month:
            fiscal_year = year - 1
        else:
            fiscal_year = year
        
        # Calculate fiscal month (1-12, starting from fiscal_start_month)
        fiscal_month = (month - fiscal_start_month) % 12 + 1
        
        # Calculate fiscal quarter (1-4)
        fiscal_quarter_num = (fiscal_month - 1) // 3 + 1
        
        # Calculate quarter start date
        quarter_month_start = ((fiscal_quarter_num - 1) * 3 + 1 + fiscal_start_month - 1) % 12 + 1
        quarter_year_start = fiscal_year if quarter_month_start >= fiscal_start_month else fiscal_year + 1
        quarter_start_date = datetime.datetime(quarter_year_start, quarter_month_start, 1)
        
        # Calculate quarter end date (last day of last month in quarter)
        quarter_month_end = (quarter_month_start + 2) % 12 + 1
        quarter_year_end = quarter_year_start if quarter_month_end > quarter_month_start else quarter_year_start + 1
        
        # Get the last day of the end month
        if quarter_month_end == 12:  # December
            quarter_end_date = datetime.datetime(quarter_year_end, 12, 31)
        else:
            # Last day of month: first day of next month - 1 day
            next_month = quarter_month_end + 1
            next_month_year = quarter_year_end if next_month > quarter_month_end else quarter_year_end + 1
            first_of_next_month = datetime.datetime(next_month_year, next_month, 1)
            quarter_end_date = first_of_next_month - datetime.timedelta(days=1)
        
        # Format the dates
        quarter_start_str = quarter_start_date.strftime(date_format)
        quarter_end_str = quarter_end_date.strftime(date_format)
        
        result = {
            'fiscal_quarter': fiscal_quarter_num,
            'fiscal_year': fiscal_year,
            'quarter_start': quarter_start_str,
            'quarter_end': quarter_end_str
        }
        
        logger.debug(f"Fiscal quarter for '{ds}' (fiscal year starting month {fiscal_start_month}): {result}")
        return result
    except Exception as e:
        logger.error(f"Error calculating fiscal quarter for '{ds}': {str(e)}")
        return {
            'fiscal_quarter': None,
            'fiscal_year': None,
            'quarter_start': None,
            'quarter_end': None
        }