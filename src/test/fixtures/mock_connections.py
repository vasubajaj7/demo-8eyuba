#!/usr/bin/env python3
"""
Provides mock Airflow connection objects and utilities for testing components that require
database or service connections. Supports both Airflow 1.10.15 and Airflow 2.X connection
APIs to facilitate testing during the Cloud Composer migration project.
"""

import os
import json
import typing
from typing import Dict, List, Optional, Union, Any
import base64
from unittest.mock import patch

# Third-party imports
import pytest
from airflow.exceptions import AirflowException

# Internal imports
from ..utils.airflow2_compatibility_utils import is_airflow2, AIRFLOW_VERSION

# Connection ID constants
GCS_CONN_ID = "gcs_default"
BQ_CONN_ID = "bigquery_default"
SECRET_MANAGER_CONN_ID = "secretmanager_default"
POSTGRES_CONN_ID = "postgres_default"
CLOUD_SQL_CONN_ID = "cloudsql_default"
HTTP_CONN_ID = "http_default"
FTP_CONN_ID = "ftp_default"
SFTP_CONN_ID = "sftp_default"
SSH_CONN_ID = "ssh_default"
REDIS_CONN_ID = "redis_default"

# List of all default connection IDs for testing
TEST_CONN_IDS = [
    GCS_CONN_ID, BQ_CONN_ID, SECRET_MANAGER_CONN_ID, 
    POSTGRES_CONN_ID, CLOUD_SQL_CONN_ID, HTTP_CONN_ID, 
    FTP_CONN_ID, SFTP_CONN_ID, SSH_CONN_ID, REDIS_CONN_ID
]

# Global dictionary to store mock connections
MOCK_CONNECTIONS = {}


def get_connection_class():
    """
    Returns the appropriate Connection class based on the installed Airflow version
    
    Returns:
        Connection class for the current Airflow version
    """
    if is_airflow2():
        # Airflow 2.X: Connection is in airflow.models.connection
        from airflow.models.connection import Connection
    else:
        # Airflow 1.10.15: Connection is in airflow.models
        from airflow.models import Connection
    
    return Connection


def get_base_hook_class():
    """
    Returns the appropriate BaseHook class based on the installed Airflow version
    
    Returns:
        BaseHook class for the current Airflow version
    """
    if is_airflow2():
        # Airflow 2.X: BaseHook is in airflow.hooks.base
        from airflow.hooks.base import BaseHook
    else:
        # Airflow 1.10.15: BaseHook is in airflow.hooks.base_hook
        from airflow.hooks.base_hook import BaseHook
    
    return BaseHook


def create_mock_connection(
    conn_id: str,
    conn_type: str,
    host: str = None,
    login: str = None,
    password: str = None,
    schema: str = None,
    port: int = None,
    extra: dict = None
):
    """
    Creates a mock Airflow Connection object with specified parameters
    
    Args:
        conn_id: Connection ID
        conn_type: Connection type
        host: Connection host
        login: Connection username/login
        password: Connection password
        schema: Connection schema
        port: Connection port
        extra: Dictionary of extra parameters
        
    Returns:
        Mock Airflow Connection object
    """
    Connection = get_connection_class()
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        schema=schema,
        port=port
    )
    
    # Handle extra parameters
    if extra:
        conn.set_extra(json.dumps(extra))
    
    # Add to global mock connections dictionary
    MOCK_CONNECTIONS[conn_id] = conn
    
    return conn


def get_mock_connection(conn_id: str):
    """
    Retrieves a mock connection by ID
    
    Args:
        conn_id: Connection ID to retrieve
        
    Returns:
        Mock Airflow Connection object
        
    Raises:
        AirflowException: If connection ID is not found
    """
    if conn_id in MOCK_CONNECTIONS:
        return MOCK_CONNECTIONS[conn_id]
    
    raise AirflowException(f"Mock connection '{conn_id}' not found")


def reset_mock_connections():
    """
    Clears all mock connections and resets to default state
    """
    MOCK_CONNECTIONS.clear()
    create_default_connections()


def create_default_connections():
    """
    Creates a set of default mock connections for common services
    
    Returns:
        Dictionary of created mock connections
    """
    # GCS connection
    create_mock_gcp_connection(
        conn_id=GCS_CONN_ID,
        project_id="mock-project",
        key_path="/path/to/keyfile.json",
        extra={"scope": "https://www.googleapis.com/auth/cloud-platform"}
    )
    
    # BigQuery connection
    create_mock_gcp_connection(
        conn_id=BQ_CONN_ID,
        project_id="mock-project",
        key_path="/path/to/keyfile.json",
        extra={"scope": "https://www.googleapis.com/auth/bigquery"}
    )
    
    # Secret Manager connection
    create_mock_gcp_connection(
        conn_id=SECRET_MANAGER_CONN_ID,
        project_id="mock-project",
        key_path="/path/to/keyfile.json",
        extra={"scope": "https://www.googleapis.com/auth/cloud-platform"}
    )
    
    # PostgreSQL connection
    create_mock_postgres_connection(
        conn_id=POSTGRES_CONN_ID,
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
        port=5432
    )
    
    # Cloud SQL connection
    create_mock_postgres_connection(
        conn_id=CLOUD_SQL_CONN_ID,
        host="/cloudsql/mock-project:us-central1:mock-instance",
        database="postgres",
        user="postgres",
        password="postgres",
        port=5432
    )
    
    # HTTP connection
    create_mock_http_connection(
        conn_id=HTTP_CONN_ID,
        host="https://example.com",
        extra={"timeout": 60}
    )
    
    # FTP connection
    create_mock_connection(
        conn_id=FTP_CONN_ID,
        conn_type="ftp",
        host="ftp.example.com",
        login="ftpuser",
        password="ftppass",
        port=21
    )
    
    # SFTP connection
    create_mock_connection(
        conn_id=SFTP_CONN_ID,
        conn_type="sftp",
        host="sftp.example.com",
        login="sftpuser",
        password="sftppass",
        port=22
    )
    
    # SSH connection
    create_mock_connection(
        conn_id=SSH_CONN_ID,
        conn_type="ssh",
        host="ssh.example.com",
        login="sshuser",
        password="sshpass",
        port=22
    )
    
    # Redis connection
    create_mock_redis_connection(
        conn_id=REDIS_CONN_ID,
        host="localhost",
        port=6379,
        password="redispass"
    )
    
    return MOCK_CONNECTIONS


def get_mock_airflow_connection(conn_id: str):
    """
    Function to patch airflow.hooks.base.BaseHook.get_connection
    
    Args:
        conn_id: Connection ID to retrieve
        
    Returns:
        Mock Airflow Connection object
        
    Raises:
        AirflowException: If connection ID is not found
    """
    if conn_id in MOCK_CONNECTIONS:
        return MOCK_CONNECTIONS[conn_id]
    
    # Mimic Airflow's behavior of raising an exception for missing connections
    raise AirflowException(f"The conn_id '{conn_id}' isn't defined")


def create_mock_gcp_connection(
    conn_id: str,
    project_id: str = "mock-project",
    key_path: str = None,
    extra: dict = None
):
    """
    Creates a mock GCP connection with appropriate defaults
    
    Args:
        conn_id: Connection ID
        project_id: GCP project ID
        key_path: Path to service account key file
        extra: Additional extra parameters
        
    Returns:
        Mock GCP Connection object
    """
    # Default key path if not provided
    if not key_path:
        key_path = "/path/to/service-account.json"
    
    # Create extra dict with GCP parameters
    gcp_extra = {
        "project_id": project_id,
        "key_path": key_path
    }
    
    # Merge with additional extra parameters if provided
    if extra:
        gcp_extra.update(extra)
    
    return create_mock_connection(
        conn_id=conn_id,
        conn_type="google_cloud_platform",
        extra=gcp_extra
    )


def create_mock_postgres_connection(
    conn_id: str,
    host: str = "localhost",
    database: str = "postgres",
    user: str = "postgres",
    password: str = "postgres",
    port: int = 5432
):
    """
    Creates a mock PostgreSQL connection with appropriate defaults
    
    Args:
        conn_id: Connection ID
        host: Database host
        database: Database name
        user: Database username
        password: Database password
        port: Database port
        
    Returns:
        Mock PostgreSQL Connection object
    """
    return create_mock_connection(
        conn_id=conn_id,
        conn_type="postgres",
        host=host,
        login=user,
        password=password,
        schema=database,
        port=port
    )


def create_mock_http_connection(
    conn_id: str,
    host: str = "https://example.com",
    extra: dict = None
):
    """
    Creates a mock HTTP connection with appropriate defaults
    
    Args:
        conn_id: Connection ID
        host: HTTP host URL
        extra: Additional parameters
        
    Returns:
        Mock HTTP Connection object
    """
    # Create extra dict with HTTP parameters
    http_extra = {}
    
    # Merge with additional extra parameters if provided
    if extra:
        http_extra.update(extra)
    
    return create_mock_connection(
        conn_id=conn_id,
        conn_type="http",
        host=host,
        extra=http_extra
    )


def create_mock_redis_connection(
    conn_id: str,
    host: str = "localhost",
    port: int = 6379,
    password: str = None
):
    """
    Creates a mock Redis connection with appropriate defaults
    
    Args:
        conn_id: Connection ID
        host: Redis host
        port: Redis port
        password: Redis password
        
    Returns:
        Mock Redis Connection object
    """
    return create_mock_connection(
        conn_id=conn_id,
        conn_type="redis",
        host=host,
        port=port,
        password=password
    )


def patch_airflow_connections():
    """
    Context manager to patch Airflow connection retrieval with mock connections
    
    Returns:
        Context manager for patching
    """
    BaseHook = get_base_hook_class()
    return patch.object(BaseHook, 'get_connection', side_effect=get_mock_airflow_connection)


def get_connection_uri(connection):
    """
    Generates a connection URI from a mock connection object
    
    Args:
        connection: Connection object
        
    Returns:
        Connection URI string
    """
    conn_type = connection.conn_type
    login = connection.login or ''
    password = connection.password or ''
    host = connection.host or ''
    schema = connection.schema or ''
    port = connection.port or ''
    
    if password:
        userpass = f"{login}:{password}"
    else:
        userpass = login
    
    if conn_type == 'postgres':
        if port:
            host_port = f"{host}:{port}"
        else:
            host_port = host
        
        uri = f"postgres://{userpass}@{host_port}/{schema}"
    elif conn_type == 'google_cloud_platform':
        # GCP connections don't have a standard URI format
        # Return a placeholder URI
        uri = f"google_cloud_platform://{login}@{host}"
    else:
        # Generic URI format
        if port:
            host_port = f"{host}:{port}"
        else:
            host_port = host
        
        uri = f"{conn_type}://{userpass}@{host_port}/{schema}"
    
    return uri


def get_mock_connections():
    """
    Returns the current dictionary of mock connections
    
    Returns:
        Dictionary of mock connections
    """
    if not MOCK_CONNECTIONS:
        create_default_connections()
    
    return MOCK_CONNECTIONS


class MockConnectionManager:
    """
    Context manager for managing mock connections during tests
    """
    
    def __init__(self):
        """Initialize the MockConnectionManager"""
        self._original_connections = {}
        self._patches = []
        self._connections = {}
    
    def __enter__(self):
        """
        Set up connection mocking when entering context
        
        Returns:
            Self reference for context manager protocol
        """
        # Store original connections
        self._original_connections = MOCK_CONNECTIONS.copy()
        self._connections = MOCK_CONNECTIONS.copy()
        
        # Create a patch for the BaseHook.get_connection method
        BaseHook = get_base_hook_class()
        patch_obj = patch.object(BaseHook, 'get_connection', side_effect=self.get_connection)
        self._patches.append(patch_obj)
        
        # Start the patch
        patch_obj.start()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up connection mocking when exiting context
        
        Args:
            exc_type: Exception type if raised
            exc_val: Exception value if raised
            exc_tb: Exception traceback if raised
        """
        # Restore original connections
        global MOCK_CONNECTIONS
        MOCK_CONNECTIONS = self._original_connections.copy()
        
        # Stop all patches
        for p in self._patches:
            p.stop()
        
        self._patches = []
        self._connections = {}
    
    def get_connection(self, conn_id: str):
        """
        Custom connection retrieval for mocked environment
        
        Args:
            conn_id: Connection ID to retrieve
            
        Returns:
            Mock connection object
            
        Raises:
            AirflowException: If connection ID is not found
        """
        if conn_id in self._connections:
            return self._connections[conn_id]
        
        raise AirflowException(f"The conn_id '{conn_id}' isn't defined")
    
    def add_connection(self, connection):
        """
        Add a mock connection during the context
        
        Args:
            connection: Connection object to add
        """
        conn_id = connection.conn_id
        self._connections[conn_id] = connection
        
        # Also update the global MOCK_CONNECTIONS for consistency
        global MOCK_CONNECTIONS
        MOCK_CONNECTIONS[conn_id] = connection
    
    def add_connections(self, connections: List):
        """
        Add multiple mock connections at once
        
        Args:
            connections: List of connection objects to add
        """
        for conn in connections:
            self.add_connection(conn)
    
    def remove_connection(self, conn_id: str):
        """
        Remove a mock connection by ID
        
        Args:
            conn_id: Connection ID to remove
            
        Returns:
            True if removed, False if not found
        """
        if conn_id in self._connections:
            del self._connections[conn_id]
            
            # Also update the global MOCK_CONNECTIONS for consistency
            global MOCK_CONNECTIONS
            if conn_id in MOCK_CONNECTIONS:
                del MOCK_CONNECTIONS[conn_id]
            
            return True
        
        return False
    
    def clear_connections(self):
        """
        Remove all mock connections
        """
        self._connections.clear()
        
        # Also update the global MOCK_CONNECTIONS for consistency
        global MOCK_CONNECTIONS
        MOCK_CONNECTIONS.clear()
    
    def get_all_connections(self):
        """
        Get all currently mocked connections
        
        Returns:
            Dictionary of all mock connections
        """
        return self._connections.copy()


# Initialize default connections
create_default_connections()