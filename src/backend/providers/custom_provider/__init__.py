"""
Root initialization file for the custom Airflow provider package that follows the
Airflow 2.X provider architecture. This file defines provider metadata, registers
entry points, and makes provider components accessible to Apache Airflow. It serves
as the main integration point for custom operators, hooks, and sensors during the
migration from Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2.
"""

import logging
from importlib.metadata import __version__

# Import components from package submodules
from . import hooks
from . import operators
from . import sensors

# Set up package logging
logger = logging.getLogger(__name__)

# Define package version
__version__ = "2.0.0"

# Define list of exposed modules
__all__ = ["hooks", "operators", "sensors", "get_provider_info"]


def get_provider_info():
    """
    Returns metadata about the custom provider package for Airflow integration.
    
    This function creates a dictionary with provider metadata including details about
    available hooks, operators, and sensors. Airflow uses this information to 
    register the provider components within its framework.
    
    Returns:
        dict: Provider metadata dictionary
    """
    return {
        "package-name": "custom-provider",
        "name": "Custom Provider",
        "description": "Custom provider package for Apache Airflow migrated from Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2.",
        "version": __version__,
        
        # Hook class definitions
        "hook-class-names": [
            "backend.plugins.hooks.custom_gcp_hook.CustomGCPHook",
            "backend.plugins.hooks.custom_http_hook.CustomHTTPHook",
            "backend.plugins.hooks.custom_postgres_hook.CustomPostgresHook",
        ],
        
        # Connection types
        "connection-types": [
            {
                "hook-class-name": "backend.plugins.hooks.custom_gcp_hook.CustomGCPHook",
                "connection-type": "custom_gcp",
            },
            {
                "hook-class-name": "backend.plugins.hooks.custom_http_hook.CustomHTTPHook", 
                "connection-type": "custom_http",
            },
            {
                "hook-class-name": "backend.plugins.hooks.custom_postgres_hook.CustomPostgresHook",
                "connection-type": "custom_postgres",
            },
        ],
        
        # Operator modules
        "operators": [
            {
                "integration-name": "Custom GCS",
                "python-modules": [
                    "backend.plugins.operators.custom_gcp_operator",
                ],
            },
            {
                "integration-name": "Custom BigQuery",
                "python-modules": [
                    "backend.providers.custom_provider.operators",  # Contains BigQuery operators
                ],
            },
            {
                "integration-name": "Custom HTTP",
                "python-modules": [
                    "backend.plugins.operators.custom_http_operator",
                ],
            },
            {
                "integration-name": "Custom PostgreSQL",
                "python-modules": [
                    "backend.plugins.operators.custom_postgres_operator",
                ],
            },
        ],
        
        # Sensor modules (placeholders for future implementation)
        "sensors": [],
        
        # Extra links (none implemented yet)
        "extra-links": [],
    }