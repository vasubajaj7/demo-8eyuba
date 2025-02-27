#!/usr/bin/env python3

"""
Package initialization file for the container tests module.
This module provides centralized imports, utilities, and constants for
validating Docker containers in the Airflow 1.10.15 to Airflow 2.X
migration project. It facilitates testing of Dockerfile configuration,
Docker image functionality, and multi-container Docker Compose environments
for Cloud Composer 2 compatibility.
"""

# Standard library imports
import os  # Python standard library - File and path operations for Docker files
import logging  # Python standard library - Logging for container tests

# Internal module imports
from ..utils import assertion_utils  # Provides assertion utilities for Airflow 2.X compatibility checks
from ..utils import test_helpers  # Provides helper functions for testing containers
from ..fixtures.mock_data import MOCK_PROJECT_ID  # Mock GCP project ID for container tests

# Import test classes from other modules in this package
from .test_dockerfile import TestDockerfileSyntax  # Import and expose TestDockerfileSyntax class from test_dockerfile module
from .test_dockerfile import TestDockerfileSecurity  # Import and expose TestDockerfileSecurity class from test_dockerfile module
from .test_dockerfile import TestDockerfileAirflow2Compatibility  # Import and expose TestDockerfileAirflow2Compatibility class from test_dockerfile module
from .test_docker_image import TestDockerImage  # Import and expose TestDockerImage class from test_docker_image module
from .test_docker_image import TestDockerImageSecurity  # Import and expose TestDockerImageSecurity class from test_docker_image module
from .test_docker_image import TestDockerImageAirflow2Compatibility  # Import and expose TestDockerImageAirflow2Compatibility class from test_docker_image module
from .test_docker_compose import TestDockerComposeSyntax  # Import and expose TestDockerComposeSyntax class from test_docker_compose module
from .test_docker_compose import TestDockerComposeServices  # Import and expose TestDockerComposeServices class from test_docker_compose module
from .test_docker_compose import TestDockerComposeAirflow2Compatibility  # Import and expose TestDockerComposeAirflow2Compatibility class from test_docker_compose module

# Initialize logger for the container tests module
logger = logging.getLogger('airflow.test.container')

# Define the root path for Docker-related files
DOCKER_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'backend')

# Define the path to the Dockerfile
DOCKERFILE_PATH = os.path.join(DOCKER_ROOT_PATH, 'Dockerfile')

# Define the path to the docker-compose.yml file
DOCKER_COMPOSE_PATH = os.path.join(DOCKER_ROOT_PATH, 'docker-compose.yml')

# Define the expected Airflow version for compatibility checks
EXPECTED_AIRFLOW_VERSION = '2.5.1'


def setup_container_tests():
    """
    Initialize and configure the container tests module
    """
    # Configure logging for container tests
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Verify Docker environment paths exist
    if not os.path.exists(DOCKER_ROOT_PATH):
        raise FileNotFoundError(f"Docker root path not found: {DOCKER_ROOT_PATH}")
    if not os.path.exists(DOCKERFILE_PATH):
        raise FileNotFoundError(f"Dockerfile not found: {DOCKERFILE_PATH}")
    if not os.path.exists(DOCKER_COMPOSE_PATH):
        raise FileNotFoundError(f"docker-compose.yml not found: {DOCKER_COMPOSE_PATH}")

    # Initialize environment variables for container testing
    os.environ['MOCK_PROJECT_ID'] = MOCK_PROJECT_ID
    logger.info("Container tests module initialized")


__all__ = [
    'TestDockerfileSyntax',
    'TestDockerfileSecurity',
    'TestDockerfileAirflow2Compatibility',
    'TestDockerImage',
    'TestDockerImageSecurity',
    'TestDockerImageAirflow2Compatibility',
    'TestDockerComposeSyntax',
    'TestDockerComposeServices',
    'TestDockerComposeAirflow2Compatibility',
    'DOCKER_ROOT_PATH',
    'DOCKERFILE_PATH',
    'DOCKER_COMPOSE_PATH',
    'EXPECTED_AIRFLOW_VERSION'
]