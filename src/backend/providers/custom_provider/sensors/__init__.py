"""
Initialization module for custom sensors in the Airflow 2.X provider architecture.

This module exposes custom sensor implementations to make them available as part of
the provider package. It follows the Airflow 2.X provider architecture requirements
for packaging custom components.

When adding new sensor implementations:
1. Import the sensor class
2. Add the sensor class name to the __all__ list
"""

import logging  # standard library

# Set up module logger
logger = logging.getLogger(__name__)
logger.debug("Initializing custom sensors package")

# List of sensor classes to expose from this package
# This should be updated when new sensors are added
__all__ = [
    # Example: "PostgreSQLTableSensor",
    # Example: "PostgreSQLQuerySensor",
]

# Import sensor modules to make them accessible when importing from this package
# Example: from .postgres_sensors import PostgreSQLTableSensor, PostgreSQLQuerySensor

logger.info("Custom sensors package initialized successfully")