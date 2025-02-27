#!/usr/bin/env python3

"""
Package initialization file for the connection_tests module which contains test suites
for connection configuration, security, and compatibility during the migration from
Apache Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2. This module exposes key
test classes and utilities to validate connection handling across environments.
"""

import os  # v3.0+ - Access environment variables and file path handling
import re  # v3.0+ - Regular expression operations for pattern matching

# Internal imports
from .test_connection_config import (  # src/test/connection_tests/test_connection_config.py
    TestConnectionConfigBase,  # Import base class for connection configuration tests
    TestConnectionConfigLoading,  # Import connection configuration loading tests
    TestConnectionConfigEnvironments,  # Import environment-specific connection tests
    TestConnectionConfigSecurity,  # Import connection security tests
    TestConnectionConfigAirflow2Compatibility,  # Import Airflow 2.X compatibility tests for connections
    load_test_connections,  # Import function for loading test connections
)
from .test_connection_secrets import (  # src/test/connection_tests/test_connection_secrets.py
    TestConnectionSecrets,  # Import base class for connection secret tests
    TestSecretManagerIntegration,  # Import Secret Manager integration tests
)
from .test_connections_json import (  # src/test/connection_tests/test_connections_json.py
    TestConnectionsJsonBase,  # Import base class for connections.json file tests
    load_connections_json,  # Import function for loading connections.json files
)

# Define global constants
__version__ = "1.0.0"
CONNECTIONS_JSON_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'backend', 'config', 'connections.json')
SECRET_PATTERN = r'\\{SECRET:([^:]+):([^}]+)\\}'