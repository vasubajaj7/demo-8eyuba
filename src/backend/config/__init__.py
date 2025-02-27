#!/usr/bin/env python3
"""
Package initialization file for the configuration module that provides a unified
interface to access environment-specific configurations, variables, connections,
and pools for the Cloud Composer 2 migration project. This module detects the
current environment and loads the appropriate configuration settings.
"""

import os  # standard library
import json  # standard library
import logging  # standard library
import pathlib  # standard library

from . import composer_dev  # Import development environment configuration
from . import composer_qa  # Import QA environment configuration
from . import composer_prod  # Import production environment configuration

# Initialize logger
logger = logging.getLogger(__name__)

# Define global variables
AIRFLOW_ENV = os.environ.get('AIRFLOW_ENV', 'dev')
CONFIG_DIR = pathlib.Path(__file__).parent
VARIABLES_FILE = CONFIG_DIR / 'variables.json'
CONNECTIONS_FILE = CONFIG_DIR / 'connections.json'
POOLS_FILE = CONFIG_DIR / 'pools.json'


def get_environment() -> str:
    """
    Detects and returns the current environment (dev, qa, or prod).

    Returns:
        str: Current environment name (dev, qa, prod).
    """
    env = os.environ.get('AIRFLOW_ENV', 'dev')

    if env not in ['dev', 'qa', 'prod']:
        logger.warning(
            "Invalid AIRFLOW_ENV '%s'. Defaulting to 'dev'.", env)
        env = 'dev'

    logger.info("Detected environment: %s", env)
    return env


def get_config(env: str) -> dict:
    """
    Returns the configuration dictionary for the specified environment.

    Args:
        env (str): The environment to retrieve the configuration for (dev, qa, prod).

    Returns:
        dict: Environment-specific configuration dictionary.
    """
    if env == 'dev':
        config = composer_dev.config
    elif env == 'qa':
        config = composer_qa.config
    elif env == 'prod':
        config = composer_prod.config
    else:
        raise ValueError(f"Invalid environment: {env}")

    return config


def load_json_file(file_path: pathlib.Path) -> dict:
    """
    Loads and returns a JSON file as a dictionary.

    Args:
        file_path (pathlib.Path): The path to the JSON file.

    Returns:
        dict: Dictionary containing the JSON file contents.
    """
    if file_path.exists():
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            logger.info("Loaded JSON file: %s", file_path)
            return data
        except FileNotFoundError:
            logger.error("File not found: %s", file_path)
            return {}
        except json.JSONDecodeError:
            logger.error("Invalid JSON in file: %s", file_path)
            return {}
    else:
        logger.warning("File does not exist: %s", file_path)
        return {}


def get_variables(env: str) -> dict:
    """
    Loads variables from variables.json and filters them for the specified environment.

    Args:
        env (str): The environment to retrieve variables for (dev, qa, prod).

    Returns:
        dict: Dictionary of environment-specific variables.
    """
    all_variables = load_json_file(VARIABLES_FILE)
    environment_variables = {}

    if all_variables:
        for key, value in all_variables.items():
            if isinstance(value, dict) and 'env' in value and value['env'] == env:
                environment_variables[key] = value['value']
            elif isinstance(value, dict) and 'pattern' in value:
                # Process environment-specific variable patterns
                pattern = value['pattern']
                environment_variables[key] = pattern.format(env=env)
            else:
                logger.warning(
                    "Variable '%s' is not environment-specific and will not be loaded.", key)
    else:
        logger.warning("No variables loaded from %s", VARIABLES_FILE)

    return environment_variables


def get_connections(env: str) -> dict:
    """
    Loads connections from connections.json and filters them for the specified environment.

    Args:
        env (str): The environment to retrieve connections for (dev, qa, prod).

    Returns:
        dict: Dictionary of environment-specific connections.
    """
    all_connections = load_json_file(CONNECTIONS_FILE)
    environment_connections = {}

    if all_connections:
        for conn_id, conn_data in all_connections.items():
            if isinstance(conn_data, dict) and 'env' in conn_data and conn_data['env'] == env:
                environment_connections[conn_id] = conn_data['config']
            elif isinstance(conn_data, dict) and 'secure' in conn_data:
                # Handle secure credential references
                logger.info(
                    "Secure credential reference found for connection '%s'.", conn_id)
                # Placeholder for secure credential retrieval logic
                environment_connections[conn_id] = conn_data['config']
            else:
                logger.warning(
                    "Connection '%s' is not environment-specific and will not be loaded.", conn_id)
    else:
        logger.warning("No connections loaded from %s", CONNECTIONS_FILE)

    return environment_connections


def get_pools(env: str) -> list:
    """
    Loads pools from pools.json and applies environment-specific slot configurations.

    Args:
        env (str): The environment to retrieve pools for (dev, qa, prod).

    Returns:
        list: List of pool configurations for the specified environment.
    """
    all_pools = load_json_file(POOLS_FILE)
    environment_pools = []

    if all_pools:
        for pool_data in all_pools:
            if isinstance(pool_data, dict) and 'env' in pool_data and pool_data['env'] == env:
                environment_pools.append(pool_data['config'])
            elif isinstance(pool_data, dict) and 'slots' in pool_data:
                # Apply environment-specific slot counts to each pool
                environment_pools.append(pool_data)
            else:
                logger.warning(
                    "Pool '%s' is not environment-specific and will not be loaded.", pool_data.get('name', 'Unnamed Pool'))
    else:
        logger.warning("No pools loaded from %s", POOLS_FILE)

    return environment_pools


# Load environment-specific configurations
environment = get_environment()
config = get_config(environment)
variables = get_variables(environment)
connections = get_connections(environment)
pools = get_pools(environment)

# Export the configurations
__all__ = ['environment', 'config', 'variables', 'connections', 'pools',
           'get_environment', 'get_config', 'get_variables', 'get_connections', 'get_pools']