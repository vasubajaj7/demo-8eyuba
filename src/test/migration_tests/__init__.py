"""Migration tests package for validating the migration from Apache Airflow 1.10.15 to Apache Airflow 2.X in Cloud Composer environments."""

import logging  # Python standard library
import pytest  # Testing framework for Python - version: 6.0+

# Internal imports
from src.test.utils.airflow2_compatibility_utils import is_airflow2  # Import utility function to detect Airflow version for tests
from src.test.utils.airflow2_compatibility_utils import Airflow2CompatibilityTestMixin  # Import mixin class for version compatibility in test cases
from src.test.utils.assertion_utils import assertion_utils  # Import utility module for version-specific assertions

# Logger for the migration tests package
logger = logging.getLogger('airflow.test.migration')

# Constants defining Airflow versions for testing
AIRFLOW_1_VERSION = "1.10.15"
AIRFLOW_2_MIN_VERSION = "2.0.0"


# Re-export items for convenience
__all__ = [
    'is_airflow2',
    'Airflow2CompatibilityTestMixin',
    'assertion_utils'
]