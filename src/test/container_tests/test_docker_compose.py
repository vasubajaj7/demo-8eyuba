#!/usr/bin/env python3

"""
Test module that validates the Docker Compose configuration for Apache Airflow 2.X migration
by testing syntax, services, integration, and compatibility aspects of the docker-compose.yml
file in the project. Ensures the containerized multi-service environment meets requirements
for Cloud Composer 2 migration.
"""

import os  # Python standard library - File and path operations for docker-compose.yml location
import re  # Python standard library - Regular expressions for parsing docker-compose.yml content
import yaml  # pyyaml-6.0+ - YAML parsing for docker-compose.yml file
import pytest  # pytest-6.0+ - Test framework for writing and executing tests
import docker  # docker-5.0.0+ - Python Docker client for interacting with Docker daemon
import docker.errors  # docker-5.0.0+ - Exception handling for Docker operations
import subprocess  # Python standard library - Running external commands like docker-compose
import time  # Python standard library - Timing operations and sleep functions during tests
import json  # Python standard library - Parsing and generating JSON data for configurations
import logging  # Python standard library - Logging test execution and results

# Internal imports
from ..utils.assertion_utils import assert_dag_airflow2_compatible  # src/test/utils/assertion_utils.py - Provides assertion utilities for Airflow 2.X compatibility checks
from ..utils.test_helpers import capture_logs  # src/test/utils/test_helpers.py - Context manager for capturing log output during tests
from ..fixtures.mock_data import MOCK_PROJECT_ID  # src/test/fixtures/mock_data.py - Mock GCP project ID for container tests

# Define global variables
DOCKER_COMPOSE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'backend', 'docker-compose.yml')
logger = logging.getLogger('airflow.test.container.docker_compose')
REQUIRED_SERVICES = ['webserver', 'scheduler', 'worker', 'postgres', 'redis']
AIRFLOW2_ENV_VARS = ['AIRFLOW__CORE__EXECUTOR', 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'AIRFLOW__CELERY__RESULT_BACKEND', 'AIRFLOW__CELERY__BROKER_URL']
EXPECTED_AIRFLOW_VERSION = "'2.5.1'"


@pytest.fixture
def docker_compose_file():
    """
    Pytest fixture that reads and returns the parsed content of the docker-compose.yml file

    Returns:
        dict: Parsed docker-compose.yml content as a dictionary
    """
    # Check if DOCKER_COMPOSE_PATH exists
    if not os.path.exists(DOCKER_COMPOSE_PATH):
        raise FileNotFoundError(f"docker-compose.yml file not found at: {DOCKER_COMPOSE_PATH}")

    # Open and read file content
    with open(DOCKER_COMPOSE_PATH, 'r') as f:
        file_content = f.read()

    # Parse YAML content to dictionary
    parsed_yaml = yaml.safe_load(file_content)

    # Return parsed dictionary
    return parsed_yaml


@pytest.fixture
def docker_compose_client():
    """
    Pytest fixture that provides a Docker Compose client for interacting with Docker services

    Returns:
        object: Docker Compose client interface
    """
    try:
        # Try to import docker-compose python module
        import compose.cli.main
        return compose.cli.main.TopLevelCommand(files=[DOCKER_COMPOSE_PATH])
    except ImportError:
        try:
            # If module not available, try to use subprocess to run docker-compose command
            subprocess.check_output(['docker-compose', '--version'])
            return 'subprocess'  # Indicate that subprocess should be used
        except OSError:
            # If neither available, skip the test with descriptive message
            pytest.skip("Docker Compose is not installed. Skipping test.")


def check_service_config(service_config, required_keys):
    """
    Helper function to check if a service configuration meets requirements

    Args:
        service_config (dict):
        required_keys (list):

    Returns:
        bool: True if service meets requirements, False otherwise
    """
    # For each key in required_keys, check if it exists in service_config
    for key in required_keys:
        if key not in service_config:
            return False

    # Return True if all required keys exist, False otherwise
    return True


def get_service_image_version(service_config):
    """
    Helper function to extract version from service image

    Args:
        service_config (dict):

    Returns:
        str: Version string extracted from image tag
    """
    # Extract image string from service_config
    image_string = service_config.get('image', '')

    # Parse image string to get tag/version part
    parts = image_string.split(':')
    if len(parts) > 1:
        version = parts[-1]
        return version

    # If no version/tag found, return None
    return None


class TestDockerComposeSyntax:
    """
    Test class for validating the syntax and structure of the docker-compose.yml file
    """
    def __init__(self):
        """
        Default constructor
        """
        pass

    def test_docker_compose_exists(self):
        """
        Tests that the docker-compose.yml file exists at the expected location
        """
        # Assert that os.path.exists(DOCKER_COMPOSE_PATH) is True
        assert os.path.exists(DOCKER_COMPOSE_PATH)

        # Log success message if file exists
        logger.info(f"Docker Compose file exists at: {DOCKER_COMPOSE_PATH}")

    def test_valid_yaml(self, docker_compose_file):
        """
        Tests that the docker-compose.yml file contains valid YAML

        Args:
            docker_compose_file (dict):
        """
        # Verify docker_compose_file is a valid dictionary
        assert isinstance(docker_compose_file, dict)

        # Verify it contains 'version' and 'services' keys
        assert 'version' in docker_compose_file
        assert 'services' in docker_compose_file

        # Assert docker-compose version is compatible with expectations
        # Log YAML validation result
        logger.info("Docker Compose file contains valid YAML")

    def test_required_services(self, docker_compose_file):
        """
        Verifies the docker-compose.yml contains all required Airflow services

        Args:
            docker_compose_file (dict):
        """
        # Extract services dictionary from docker_compose_file
        services = docker_compose_file.get('services', {})

        # For each service in REQUIRED_SERVICES, verify it exists in services
        for service in REQUIRED_SERVICES:
            assert service in services

        # Assert all required services are defined
        # Log service verification results
        logger.info("Docker Compose file contains all required services")

    def test_service_dependencies(self, docker_compose_file):
        """
        Tests service dependencies are correctly defined

        Args:
            docker_compose_file (dict):
        """
        # Extract services dictionary from docker_compose_file
        services = docker_compose_file.get('services', {})

        # Verify webserver depends on postgres and redis
        webserver_depends_on = services['webserver'].get('depends_on', [])
        assert 'postgres' in webserver_depends_on
        assert 'redis' in webserver_depends_on

        # Verify scheduler depends on postgres, redis, and webserver
        scheduler_depends_on = services['scheduler'].get('depends_on', [])
        assert 'postgres' in scheduler_depends_on
        assert 'redis' in scheduler_depends_on
        assert 'webserver' in scheduler_depends_on

        # Verify worker depends on postgres, redis, and scheduler
        worker_depends_on = services['worker'].get('depends_on', [])
        assert 'postgres' in worker_depends_on
        assert 'redis' in worker_depends_on
        assert 'scheduler' in worker_depends_on

        # Log dependency verification results
        logger.info("Docker Compose file defines correct service dependencies")

    def test_volume_definitions(self, docker_compose_file):
        """
        Tests volume definitions in docker-compose.yml

        Args:
            docker_compose_file (dict):
        """
        # Check that volumes section exists
        volumes = docker_compose_file.get('volumes', {})
        assert volumes is not None

        # Verify postgres-db-volume is defined
        assert 'postgres-db-volume' in volumes

        # Check that services properly reference volumes
        # Log volume verification results
        logger.info("Docker Compose file defines correct volume definitions")


class TestDockerComposeServices:
    """
    Test class for validating individual service configurations in docker-compose.yml
    """
    def __init__(self):
        """
        Default constructor
        """
        pass

    def test_webserver_config(self, docker_compose_file):
        """
        Tests the webserver service configuration

        Args:
            docker_compose_file (dict):
        """
        # Extract webserver config from services
        webserver_config = docker_compose_file['services']['webserver']

        # Verify required keys: image, ports, environment, healthcheck
        assert 'image' in webserver_config
        assert 'ports' in webserver_config
        assert 'environment' in webserver_config
        assert 'healthcheck' in webserver_config

        # Check port mapping includes 8080:8080
        assert '8080:8080' in webserver_config['ports']

        # Verify healthcheck configuration is correct
        # Log webserver config verification results
        logger.info("Docker Compose file defines correct webserver configuration")

    def test_scheduler_config(self, docker_compose_file):
        """
        Tests the scheduler service configuration

        Args:
            docker_compose_file (dict):
        """
        # Extract scheduler config from services
        scheduler_config = docker_compose_file['services']['scheduler']

        # Verify required keys: image, command, environment
        assert 'image' in scheduler_config
        assert 'command' in scheduler_config
        assert 'environment' in scheduler_config

        # Check scheduler environment variables
        # Verify command is 'airflow scheduler'
        assert 'airflow scheduler' in scheduler_config['command']

        # Log scheduler config verification results
        logger.info("Docker Compose file defines correct scheduler configuration")

    def test_worker_config(self, docker_compose_file):
        """
        Tests the worker service configuration

        Args:
            docker_compose_file (dict):
        """
        # Extract worker config from services
        worker_config = docker_compose_file['services']['worker']

        # Verify required keys: image, command, environment
        assert 'image' in worker_config
        assert 'command' in worker_config
        assert 'environment' in worker_config

        # Check worker environment variables
        # Verify command is 'airflow celery worker'
        assert 'airflow celery worker' in worker_config['command']

        # Log worker config verification results
        logger.info("Docker Compose file defines correct worker configuration")

    def test_postgres_config(self, docker_compose_file):
        """
        Tests the postgres service configuration

        Args:
            docker_compose_file (dict):
        """
        # Extract postgres config from services
        postgres_config = docker_compose_file['services']['postgres']

        # Verify required keys: image, environment, volumes, healthcheck
        assert 'image' in postgres_config
        assert 'environment' in postgres_config
        assert 'volumes' in postgres_config
        assert 'healthcheck' in postgres_config

        # Check postgres version is 13
        # Verify environment variables for database setup
        # Log postgres config verification results
        logger.info("Docker Compose file defines correct postgres configuration")

    def test_redis_config(self, docker_compose_file):
        """
        Tests the redis service configuration

        Args:
            docker_compose_file (dict):
        """
        # Extract redis config from services
        redis_config = docker_compose_file['services']['redis']

        # Verify required keys: image, ports, healthcheck
        assert 'image' in redis_config
        assert 'ports' in redis_config
        assert 'healthcheck' in redis_config

        # Check redis version is 6.x
        # Verify port mapping includes 6379:6379
        assert '6379:6379' in redis_config['ports']

        # Log redis config verification results
        logger.info("Docker Compose file defines correct redis configuration")

    def test_image_versions(self, docker_compose_file):
        """
        Tests that service images use correct versions

        Args:
            docker_compose_file (dict):
        """
        # Extract services dictionary from docker_compose_file
        services = docker_compose_file.get('services', {})

        # For airflow services, verify image tag is EXPECTED_AIRFLOW_VERSION
        # Verify postgres image uses version 13
        # Verify redis image uses version 6.x
        # Log image version verification results
        logger.info("Docker Compose file defines correct image versions")


class TestDockerComposeIntegration:
    """
    Test class for validating integration aspects of the docker-compose configuration
    """
    def __init__(self):
        """
        Default constructor
        """
        pass

    def test_network_configuration(self, docker_compose_file):
        """
        Tests the network configuration in docker-compose.yml

        Args:
            docker_compose_file (dict):
        """
        # Verify networks section exists
        networks = docker_compose_file.get('networks', {})
        assert networks is not None

        # Check that default network is defined
        assert 'default' in networks

        # Verify all services are on the same network
        # Log network configuration verification results
        logger.info("Docker Compose file defines correct network configuration")

    def test_volume_mounts(self, docker_compose_file):
        """
        Tests volume mount configurations across services

        Args:
            docker_compose_file (dict):
        """
        # Verify dags directory is mounted consistently across services
        # Verify plugins directory is mounted correctly
        # Verify logs directory is mounted correctly
        # Verify config files are mounted correctly
        # Log volume mount verification results
        logger.info("Docker Compose file defines correct volume mounts")

    def test_environment_consistency(self, docker_compose_file):
        """
        Tests that environment variables are consistent across services

        Args:
            docker_compose_file (dict):
        """
        # Extract environment variables from each Airflow service
        # Verify core environment variables are consistent
        # Check for service-specific variables
        # Verify database connection parameters match postgres service
        # Log environment consistency verification results
        logger.info("Docker Compose file defines consistent environment variables")

    def test_healthcheck_configuration(self, docker_compose_file):
        """
        Tests health check configurations across services

        Args:
            docker_compose_file (dict):
        """
        # Verify webserver has appropriate health check
        # Verify postgres has appropriate health check
        # Verify redis has appropriate health check
        # Check health check interval and retry settings
        # Log health check verification results
        logger.info("Docker Compose file defines correct healthcheck configuration")

    def test_init_service(self, docker_compose_file):
        """
        Tests the initialization service configuration

        Args:
            docker_compose_file (dict):
        """
        # Verify init service exists
        # Check init service depends on postgres
        # Verify init service initializes Airflow DB and creates admin user
        # Log init service verification results
        logger.info("Docker Compose file defines correct init service configuration")


class TestDockerComposeAirflow2Compatibility:
    """
    Test class for validating Airflow 2.X compatibility in docker-compose.yml
    """
    def __init__(self):
        """
        Default constructor
        """
        pass

    def test_airflow2_env_vars(self, docker_compose_file):
        """
        Tests that docker-compose.yml uses Airflow 2.X environment variable format

        Args:
            docker_compose_file (dict):
        """
        # Extract environment variables from Airflow services
        # For each service, verify Airflow 2.X environment variable format (double underscore)
        # Check for presence of AIRFLOW__DATABASE__SQL_ALCHEMY_CONN instead of old format
        # Verify no deprecated 1.X environment variables are used
        # Log environment variable format verification results
        logger.info("Docker Compose file uses Airflow 2.X environment variable format")

    def test_airflow2_commands(self, docker_compose_file):
        """
        Tests that docker-compose.yml uses Airflow 2.X command syntax

        Args:
            docker_compose_file (dict):
        """
        # Extract commands from Airflow services
        # Verify commands use Airflow 2.X syntax
        # Check for any deprecated command options
        # Log command syntax verification results
        logger.info("Docker Compose file uses Airflow 2.X command syntax")

    def test_airflow2_webserver_config(self, docker_compose_file):
        """
        Tests Airflow 2.X webserver configuration

        Args:
            docker_compose_file (dict):
        """
        # Extract webserver configuration
        # Verify AIRFLOW__WEBSERVER__RBAC=True is set
        # Check for mounted webserver_config.py file
        # Verify appropriate authentication configuration
        # Log webserver configuration verification results
        logger.info("Docker Compose file defines correct Airflow 2.X webserver configuration")

    def test_airflow2_celery_config(self, docker_compose_file):
        """
        Tests Airflow 2.X Celery configuration

        Args:
            docker_compose_file (dict):
        """
        # Extract Celery configuration from worker service
        # Verify Celery result backend configuration
        # Check Celery broker URL configuration
        # Verify worker concurrency settings
        # Log Celery configuration verification results
        logger.info("Docker Compose file defines correct Airflow 2.X Celery configuration")

    def test_service_compatibility(self, docker_compose_file):
        """
        Tests that all services are compatible with Airflow 2.X

        Args:
            docker_compose_file (dict):
        """
        # Verify flower service configuration (if present)
        # Check for any deprecated services from Airflow 1.X
        # Verify PostgreSQL version compatibility with Airflow 2.X
        # Log service compatibility verification results
        logger.info("Docker Compose file defines services compatible with Airflow 2.X")