"""
Package initialization module for Apache Airflow 2.X DAGs.

This module provides essential utilities, common constants, and default configurations
for all DAGs in the Cloud Composer 2 environment. It centralizes shared functionality 
and makes migration from Airflow 1.10.15 to Airflow 2.X more consistent across the 
organization's workflow orchestration platform.
"""

import os  # Python standard library
import logging  # Python standard library
import datetime  # Python standard library

# Airflow imports (apache-airflow v2.0.0+)
import airflow
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.utils import timezone

# Internal imports
from .utils import utils
from .utils.gcp_utils import gcs_file_exists, gcs_upload_file, gcs_download_file
from .utils.db_utils import execute_query, execute_query_as_df
from src.backend.config import get_config, detect_environment

# Define global constants
DAG_FOLDER = os.environ.get('AIRFLOW_HOME', '/opt/airflow') + '/dags'
AIRFLOW_VERSION = '2.0.0'
DEFAULT_OWNER = 'data-engineering-team'
DEFAULT_RETRY_DELAY = datetime.timedelta(minutes=5)
DEFAULT_RETRIES = 3
DEFAULT_START_DATE = datetime.datetime(2023, 1, 1)
DEFAULT_EMAIL = ['airflow-alerts@example.com']

# Configure logging
logger = logging.getLogger('airflow.dags')


def get_default_args(overrides=None):
    """
    Creates a default arguments dictionary for DAGs with standard settings and optional overrides.
    
    Args:
        overrides (dict, optional): Dictionary containing argument overrides. Defaults to None.
        
    Returns:
        dict: Default arguments dictionary for DAGs with provided overrides applied.
    """
    # Create base default arguments dictionary with Airflow 2.X standards
    default_args = {
        'owner': DEFAULT_OWNER,
        'depends_on_past': False,
        'email': DEFAULT_EMAIL,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': DEFAULT_RETRIES,
        'retry_delay': DEFAULT_RETRY_DELAY,
        'start_date': DEFAULT_START_DATE,
        'execution_timeout': datetime.timedelta(hours=1),
        'on_failure_callback': default_failure_handler,
        'on_success_callback': default_success_handler,
    }
    
    # Get environment-specific configurations
    env = detect_environment()
    env_config = get_environment_config(env)
    
    # Apply environment-specific overrides
    if env_config and 'airflow' in env_config:
        if 'default_email_recipient' in env_config['airflow']:
            default_args['email'] = [env_config['airflow']['default_email_recipient']]
        
        if 'email_on_failure' in env_config['airflow']:
            default_args['email_on_failure'] = env_config['airflow']['email_on_failure']
            
        if 'email_on_retry' in env_config['airflow']:
            default_args['email_on_retry'] = env_config['airflow']['email_on_retry']
    
    # Apply user-provided overrides
    if overrides:
        default_args.update(overrides)
    
    return default_args


def setup_dag_logging(dag_id, log_level=None):
    """
    Configures logging for a specific DAG and returns a logger instance.
    
    Args:
        dag_id (str): The ID of the DAG to configure logging for
        log_level (str, optional): The logging level to set. Defaults to None.
        
    Returns:
        logging.Logger: Logger instance for the DAG
    """
    # Create a child logger for the specified DAG
    dag_logger = logging.getLogger(f'airflow.dags.{dag_id}')
    
    # Set the log level based on input parameter or environment configuration
    if log_level:
        numeric_level = getattr(logging, log_level.upper(), None)
        if isinstance(numeric_level, int):
            dag_logger.setLevel(numeric_level)
    else:
        # Get log level from environment configuration
        env = detect_environment()
        env_config = get_environment_config(env)
        if env_config and 'composer' in env_config and 'log_level' in env_config['composer']:
            log_level = env_config['composer']['log_level']
            numeric_level = getattr(logging, log_level.upper(), None)
            if isinstance(numeric_level, int):
                dag_logger.setLevel(numeric_level)
        else:
            # Default to INFO if not specified
            dag_logger.setLevel(logging.INFO)
    
    # Ensure the logger has at least one handler
    if not dag_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        dag_logger.addHandler(handler)
    
    return dag_logger


def default_failure_handler(context):
    """
    Default callback function for task failures with enhanced Airflow 2.X features.
    
    Args:
        context (dict): The task context dictionary containing information about the task instance
        
    Returns:
        None
    """
    # Extract information from context
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    
    # Log the failure
    log_message = f"Task failed: DAG={dag_id}, Task={task_id}, Execution Date={execution_date}"
    logger.error(log_message)
    
    if exception:
        logger.error(f"Exception: {str(exception)}")
    
    # Get environment-specific configuration
    env = detect_environment()
    env_config = get_environment_config(env)
    
    # Send notifications based on environment configuration
    if env_config and 'airflow' in env_config and env_config['airflow'].get('email_notifications', True):
        # Format the email subject and content
        subject = f"Airflow Task Failure: {dag_id}.{task_id} - {execution_date}"
        html_content = f"""
        <h3>Task Failed</h3>
        <p><b>DAG</b>: {dag_id}</p>
        <p><b>Task</b>: {task_id}</p>
        <p><b>Execution Date</b>: {execution_date}</p>
        """
        
        if exception:
            html_content += f"<p><b>Exception</b>: {str(exception)}</p>"
        
        # Add log URL if available
        if hasattr(task_instance, 'log_url') and task_instance.log_url:
            html_content += f'<p><b>Logs</b>: <a href="{task_instance.log_url}">View Logs</a></p>'
        
        # Get the recipient email addresses
        emails = context.get('email', DEFAULT_EMAIL)
        if env_config and 'airflow' in env_config and 'default_email_recipient' in env_config['airflow']:
            emails = [env_config['airflow']['default_email_recipient']]
        
        # Send the email
        try:
            send_email(
                to=emails,
                subject=subject,
                html_content=html_content
            )
        except Exception as e:
            logger.error(f"Failed to send email notification: {str(e)}")
    
    # Record failure metrics for monitoring
    try:
        # Get the task duration
        if task_instance.end_date and task_instance.start_date:
            duration = (task_instance.end_date - task_instance.start_date).total_seconds()
            logger.info(f"Task duration: {duration} seconds")
        
        # Implement Airflow 2.X specific error handling features
        # In Airflow 2.X, we can mark the task instance with notes about the failure
        if hasattr(task_instance, 'note') and callable(getattr(task_instance, 'note', None)):
            task_instance.note = f"Failed with error: {str(exception)[:100]}..."
    except Exception as e:
        logger.error(f"Error in failure handler: {str(e)}")


def default_success_handler(context):
    """
    Default callback function for task successes.
    
    Args:
        context (dict): The task context dictionary containing information about the task instance
        
    Returns:
        None
    """
    # Extract information from context
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    
    # Log the success
    log_message = f"Task succeeded: DAG={dag_id}, Task={task_id}, Execution Date={execution_date}"
    logger.info(log_message)
    
    # Record success metrics for monitoring
    try:
        # Get the task duration
        if task_instance.end_date and task_instance.start_date:
            duration = (task_instance.end_date - task_instance.start_date).total_seconds()
            logger.info(f"Task duration: {duration} seconds")
        
        # Check if xcom values were pushed
        xcom_values = task_instance.xcom_pull(task_ids=task_id, include_prior_dates=False)
        if xcom_values:
            logger.info(f"Task pushed XCom values: {type(xcom_values)}")
    except Exception as e:
        logger.error(f"Error in success handler: {str(e)}")


def get_environment_config(env_name=None):
    """
    Gets environment-specific configuration for DAGs.
    
    Args:
        env_name (str, optional): The environment name to get config for. Defaults to None.
        
    Returns:
        dict: Environment configuration dictionary
    """
    # If env_name is not provided, detect the current environment
    if env_name is None:
        env_name = detect_environment()
    
    # Get the configuration for the specified environment
    try:
        env_config = get_config(env_name)
        return env_config
    except Exception as e:
        logger.error(f"Failed to get environment config for {env_name}: {str(e)}")
        return {}


class DAGConfig:
    """
    Class for managing DAG-specific configurations with Airflow 2.X features.
    
    This class provides a unified interface for accessing DAG configuration values,
    connection IDs, and environment-specific settings, supporting the migration
    from Airflow 1.10.15 to Airflow 2.X.
    """
    
    def __init__(self, dag_id, config_overrides=None):
        """
        Initialize a DAG configuration object.
        
        Args:
            dag_id (str): The ID of the DAG to configure
            config_overrides (dict, optional): Dictionary containing configuration overrides. Defaults to None.
        """
        self.dag_id = dag_id
        self.environment = detect_environment()
        
        # Get base configuration for the environment
        base_config = get_environment_config(self.environment)
        self.config = base_config.copy() if base_config else {}
        
        # Add DAG-specific section if it doesn't exist
        if 'dags' not in self.config:
            self.config['dags'] = {}
        
        # Add this DAG's section if it doesn't exist
        if dag_id not in self.config['dags']:
            self.config['dags'][dag_id] = {}
        
        # Apply any overrides from config_overrides parameter
        if config_overrides:
            if isinstance(config_overrides, dict):
                # Update the DAG's config section
                self.config['dags'][dag_id].update(config_overrides)
        
        # Initialize the logger for this DAG
        self.logger = setup_dag_logging(dag_id)
    
    def get_connection_id(self, connection_name):
        """
        Gets the appropriate connection ID for the current environment.
        
        Args:
            connection_name (str): The base name of the connection
            
        Returns:
            str: Environment-specific connection ID
        """
        # Get environment prefix (e.g., 'dev_', 'qa_', 'prod_')
        prefix = f"{self.environment}_" if self.environment != 'prod' else ""
        
        # Check if there's a specific mapping in the config
        conn_mapping = self.config.get('connections', {}).get(connection_name, None)
        if conn_mapping:
            return conn_mapping
        
        # Construct connection ID with environment prefix and connection name
        connection_id = f"{prefix}{connection_name}"
        self.logger.info(f"Using connection ID: {connection_id}")
        
        return connection_id
    
    def get_value(self, key, default=None):
        """
        Gets a configuration value with optional default.
        
        Args:
            key (str): The configuration key to look up
            default (any, optional): Default value to return if key not found. Defaults to None.
            
        Returns:
            any: Configuration value or default if not found
        """
        # First check in DAG-specific config
        dag_config = self.config.get('dags', {}).get(self.dag_id, {})
        if key in dag_config:
            return dag_config[key]
        
        # Then check in environment config sections
        for section in ['airflow', 'composer', 'storage', 'environment', 'gcp']:
            if section in self.config and key in self.config[section]:
                return self.config[section][key]
        
        # Try to get from Airflow variable
        try:
            var_name = f"{self.environment}_{key}" if self.environment != 'prod' else key
            var_value = Variable.get(var_name, default_var=None)
            if var_value is not None:
                return var_value
        except:
            pass
        
        # Return the default if not found
        return default
    
    def get_gcs_bucket(self):
        """
        Gets the appropriate GCS bucket for the current environment.
        
        Returns:
            str: Environment-specific GCS bucket name
        """
        # First try to get from DAG-specific config
        dag_bucket = self.config.get('dags', {}).get(self.dag_id, {}).get('bucket')
        if dag_bucket:
            return dag_bucket
        
        # Then try to get from storage config
        if 'storage' in self.config and 'data_bucket' in self.config['storage']:
            return self.config['storage']['data_bucket']
        
        # Construct bucket name with environment prefix
        bucket_name = f"composer2-migration-{self.environment}-data"
        self.logger.info(f"Using GCS bucket: {bucket_name}")
        
        return bucket_name