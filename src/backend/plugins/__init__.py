"""
Main entry point for Apache Airflow plugins that defines the plugin classes for Cloud Composer 2 with Airflow 2.X.

This file integrates all custom hooks, operators, sensors, and macros into a single plugin
that can be registered with the Airflow plugin system. It serves as the central integration point
for all custom components during the migration from Airflow 1.10.15 to Airflow 2.X.
"""

import logging
from airflow.plugins_manager import AirflowPlugin
from airflow import __version__

# Import hooks
from .hooks import (
    CustomGCPHook,
    CustomHTTPHook,
    CustomPostgresHook,
    DEFAULT_GCP_CONN_ID,
    DEFAULT_HTTP_CONN_ID,
    DEFAULT_POSTGRES_CONN_ID
)

# Import operators
from .operators import (
    CustomGCSOperator,
    GCSFileExistsOperator,
    GCSUploadOperator,
    GCSDownloadOperator,
    GCSListFilesOperator,
    GCSDeleteFileOperator,
    CustomHttpOperator,
    CustomHttpSensorOperator,
    CustomPostgresOperator,
    DEFAULT_CHUNK_SIZE,
    MAX_RETRIES,
    DEFAULT_RETRY_DELAY
)

# Import sensors
from .sensors import (
    CustomGCSFileSensor,
    CustomBigQueryTableSensor,
    CustomBigQueryJobSensor,
    CustomGCSObjectsWithPrefixExistenceSensor,
    CustomHttpSensor,
    CustomHttpJsonSensor,
    CustomHttpStatusSensor,
    CustomPostgresSensor,
    CustomPostgresTableExistenceSensor,
    CustomPostgresRowCountSensor,
    CustomPostgresValueCheckSensor,
    DEFAULT_SCHEMA
)

# Import macros
from .macros import custom_macros

# Setup logging
logger = logging.getLogger(__name__)

# Define plugin version to match Airflow 2.X
__version__ = '2.0.0'


def check_airflow_version():
    """
    Verify the current Airflow version is compatible with the plugin.
    
    Returns:
        bool: True if Airflow version is compatible, False otherwise
    """
    try:
        major, minor, *_ = __version__.split('.')
        if int(major) >= 2:
            logger.info(f"Airflow version {__version__} is compatible with this plugin.")
            return True
        else:
            logger.warning(
                f"Airflow version {__version__} may not be compatible with this plugin. "
                f"This plugin is designed for Airflow 2.X."
            )
            return False
    except Exception as e:
        logger.error(f"Error checking Airflow version: {str(e)}")
        return False


class ComposerMigrationPlugin(AirflowPlugin):
    """
    Airflow plugin that registers all custom hooks, operators, sensors, and macros with Airflow 2.X.
    """
    
    def __init__(self):
        """
        Initialize the ComposerMigrationPlugin with all custom components.
        """
        super().__init__()
        
        # Check Airflow version for compatibility
        check_airflow_version()
        
        logger.info("Initializing ComposerMigrationPlugin")
        
        # Set plugin metadata
        self.name = 'composer_migration_plugin'
        self.version = __version__
        self.description = (
            "Plugin that registers custom hooks, operators, sensors, and macros "
            "for the migration from Airflow 1.10.15 to 2.X in Cloud Composer 2."
        )
        
        # Register hooks
        self.hooks = [
            CustomGCPHook,
            CustomHTTPHook,
            CustomPostgresHook
        ]
        logger.info(f"Registered {len(self.hooks)} custom hooks")
        
        # Register operators
        self.operators = [
            CustomGCSOperator,
            GCSFileExistsOperator,
            GCSUploadOperator,
            GCSDownloadOperator,
            GCSListFilesOperator,
            GCSDeleteFileOperator,
            CustomHttpOperator,
            CustomHttpSensorOperator,
            CustomPostgresOperator
        ]
        logger.info(f"Registered {len(self.operators)} custom operators")
        
        # Register sensors
        self.sensors = [
            CustomGCSFileSensor,
            CustomBigQueryTableSensor,
            CustomBigQueryJobSensor,
            CustomGCSObjectsWithPrefixExistenceSensor,
            CustomHttpSensor,
            CustomHttpJsonSensor,
            CustomHttpStatusSensor,
            CustomPostgresSensor,
            CustomPostgresTableExistenceSensor,
            CustomPostgresRowCountSensor,
            CustomPostgresValueCheckSensor
        ]
        logger.info(f"Registered {len(self.sensors)} custom sensors")
        
        # Register macros
        self.macros = {
            'ds_format': custom_macros.ds_format,
            'ds_add': custom_macros.ds_add,
            'ds_subtract': custom_macros.ds_subtract,
            'month_start': custom_macros.month_start,
            'month_end': custom_macros.month_end,
            'date_range_array': custom_macros.date_range_array,
            'get_env_var': custom_macros.get_env_var,
            'get_env_name': custom_macros.get_env_name,
            'is_env': custom_macros.is_env,
            'gcs_path': custom_macros.gcs_path,
            'format_sql': custom_macros.format_sql,
            'check_gcs_file': custom_macros.check_gcs_file,
            'get_gcp_secret': custom_macros.get_gcp_secret,
            'query_result_as_dict': custom_macros.query_result_as_dict,
            'fiscal_quarter': custom_macros.fiscal_quarter
        }
        logger.info(f"Registered {len(self.macros)} custom macros")
    
    def on_load(self, plugin_context):
        """
        Executed when the plugin is loaded by Airflow.
        
        Args:
            plugin_context (object): Context provided by Airflow when loading the plugin
        
        Returns:
            None: No return value
        """
        logger.info(f"Plugin {self.name} v{self.version} has been loaded")
        logger.info(
            f"Registered components: {len(self.hooks)} hooks, "
            f"{len(self.operators)} operators, {len(self.sensors)} sensors, "
            f"{len(self.macros)} macros"
        )
        
        # Check version again to warn about any compatibility issues
        if not check_airflow_version():
            logger.warning(
                "Warning: This plugin may not be fully compatible with your Airflow version. "
                "It is designed for Airflow 2.X."
            )