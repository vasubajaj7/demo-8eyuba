"""
Custom Apache Airflow macros package for Cloud Composer 2 with Airflow 2.X.

This package provides template macros for DAG development compatible with Airflow 2.X,
supporting the migration from Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2.
"""

import logging

# Import all macro functions from the custom_macros module
from .custom_macros import (
    ds_format,
    ds_add,
    ds_subtract,
    month_start,
    month_end,
    date_range_array,
    get_env_var,
    get_env_name,
    is_env,
    gcs_path,
    format_sql,
    check_gcs_file,
    get_gcp_secret,
    query_result_as_dict,
    fiscal_quarter
)

# Set up logging
logger = logging.getLogger(__name__)

# Define package version
__version__ = '2.0.0'

# Define exported functions
__all__ = [
    'ds_format',
    'ds_add',
    'ds_subtract',
    'month_start',
    'month_end',
    'date_range_array',
    'get_env_var',
    'get_env_name',
    'is_env',
    'gcs_path',
    'format_sql',
    'check_gcs_file',
    'get_gcp_secret',
    'query_result_as_dict',
    'fiscal_quarter'
]