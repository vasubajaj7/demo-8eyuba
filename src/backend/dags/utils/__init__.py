"""
Package initialization module for the Airflow 2.X utility functions.

This module provides a centralized import interface for utility modules related to GCP, 
database, alerts, and validation. It simplifies imports by exposing the most commonly used 
functions from each submodule directly at the package level.

By centralizing utility function imports, this module helps maintain consistent code 
organization and supports the migration from Airflow 1.10.15 to Airflow 2.X in
Cloud Composer 2 environments.
"""

import logging

# Internal imports - import the entire utility modules
from . import gcp_utils
from . import db_utils
from . import alert_utils
from . import validation_utils

# Set up logging for the utilities package
logger = logging.getLogger('airflow.utils')

# Package version
__version__ = '2.0.0'

# Re-export GCP utilities for direct access
gcs_file_exists = gcp_utils.gcs_file_exists
gcs_upload_file = gcp_utils.gcs_upload_file
gcs_download_file = gcp_utils.gcs_download_file
gcs_list_files = gcp_utils.gcs_list_files
gcs_delete_file = gcp_utils.gcs_delete_file
get_secret = gcp_utils.get_secret

# Re-export database utilities for direct access
execute_query = db_utils.execute_query
execute_query_as_df = db_utils.execute_query_as_df
bulk_load_from_df = db_utils.bulk_load_from_df
verify_connection = db_utils.verify_connection

# Re-export alert utilities for direct access
send_alert = alert_utils.send_alert
on_failure_callback = alert_utils.on_failure_callback
on_success_callback = alert_utils.on_success_callback
AlertManager = alert_utils.AlertManager
AlertLevel = alert_utils.AlertLevel

# Re-export validation utilities for direct access
validate_dag = validation_utils.validate_dag
validate_dag_file = validation_utils.validate_dag_file
DAGValidator = validation_utils.DAGValidator