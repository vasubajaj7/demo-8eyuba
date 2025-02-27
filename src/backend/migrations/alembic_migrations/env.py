"""
Alembic migration environment configuration for Apache Airflow 1.10.15 to 2.X migration in Cloud Composer 2.

This file provides the core setup for database schema migrations, including context handling,
connection establishment, and migration execution for both online and offline modes.
"""

import logging
import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Import configuration utilities
from ...config import get_config, get_environment

# Base path and script location for Alembic
BASE_PATH = Path(__file__).parent
SCRIPT_LOCATION = BASE_PATH / 'versions'

# Configuration file path
config_file_path = BASE_PATH / 'alembic.ini'

# Set up logging
logger = logging.getLogger('alembic.env')
fileConfig(config_file_path)

def get_url():
    """
    Constructs the database URL from environment configuration.

    Returns:
        str: SQLAlchemy database URL for current environment
    """
    # Get current environment (dev, qa, prod)
    env = get_environment()
    
    # Load environment-specific configuration
    config_dict = get_config(env)
    
    # Extract database connection parameters
    db_config = config_dict.get('database', {})
    
    # Extract connection details
    db_instance = db_config.get('instance_name', f'composer2-migration-{env}-db')
    project_id = config_dict.get('gcp', {}).get('project_id', f'composer2-migration-project-{env}')
    region = config_dict.get('gcp', {}).get('region', 'us-central1')
    
    # Determine if we're using Cloud SQL Proxy or direct connection
    if os.environ.get('CLOUD_SQL_PROXY_ENABLED', 'false').lower() == 'true':
        # Using Cloud SQL Proxy
        host = 'localhost'
        port = os.environ.get('CLOUD_SQL_PROXY_PORT', '5432')
        user = os.environ.get('DB_USER', 'airflow')
        password = os.environ.get('DB_PASSWORD', '')
        dbname = os.environ.get('DB_NAME', 'airflow')
        
        db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    else:
        # Direct connection to Cloud SQL
        connection_name = f"{project_id}:{region}:{db_instance}"
        user = os.environ.get('DB_USER', 'airflow')
        password = os.environ.get('DB_PASSWORD', '')
        dbname = os.environ.get('DB_NAME', 'airflow')
        
        db_url = f"postgresql+psycopg2://{user}:{password}@/{dbname}?host=/cloudsql/{connection_name}"
    
    logger.info(f"Using database connection for environment: {env}")
    # Log obfuscated URL to avoid exposing credentials
    safe_url = db_url.replace(password, '******') if password else db_url
    logger.debug(f"Database URL: {safe_url}")
    
    return db_url

def get_metadata():
    """
    Retrieves the Airflow metadata schema based on version.

    Returns:
        sqlalchemy.MetaData: SQLAlchemy metadata object containing Airflow schema
    """
    # Dynamically import the appropriate Airflow models based on version
    try:
        # Try to import from Airflow 2.X path
        from airflow.models import Base
        metadata = Base.metadata
        logger.info("Using Airflow 2.X schema metadata")
    except ImportError:
        try:
            # Fall back to Airflow 1.10.15 path
            from airflow.models.base import Base
            metadata = Base.metadata
            logger.info("Using Airflow 1.10.15 schema metadata")
        except ImportError:
            logger.error("Failed to import Airflow models. Ensure Airflow is installed correctly.")
            raise
    
    return metadata

def run_migrations_offline():
    """
    Executes migrations in 'offline' mode, generating SQL scripts.

    This function configures context and runs migrations in offline mode,
    which outputs SQL to stdout or file without executing against a database.
    
    Returns:
        None
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=get_metadata(),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """
    Executes migrations directly against the database in 'online' mode.

    This function configures context and runs migrations in online mode,
    which executes SQL directly against the connected database.
    
    Returns:
        None
    """
    # Configure SQLAlchemy engine
    alembic_config = context.config.get_section(context.config.config_ini_section)
    alembic_config['sqlalchemy.url'] = get_url()
    
    # Create engine with appropriate connection parameters
    connectable = engine_from_config(
        alembic_config,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # Execute migrations within a transaction
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=get_metadata(),
            compare_type=True,
            compare_server_default=True,
            include_schemas=True,
        )

        try:
            with context.begin_transaction():
                logger.info("Running database migrations")
                context.run_migrations()
            logger.info("Database migrations completed successfully")
        except Exception as e:
            logger.error("Error during migration: %s", str(e))
            raise

# Determine whether to run in offline or online mode
if context.is_offline_mode():
    logger.info("Running migrations in offline mode")
    run_migrations_offline()
else:
    logger.info("Running migrations in online mode")
    run_migrations_online()