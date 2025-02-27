#!/usr/bin/env python3
"""
Package initialization file for the Alembic migrations package.

This module provides core functionality for database schema migrations from 
Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2. It exposes migration 
functions and version utilities for both programmatic and command-line 
migration usage.
"""

import logging
import os

# Import Alembic for database migrations
import alembic  # version: 1.7.0
from alembic.runtime import migration

# Import SQLAlchemy for database operations
import sqlalchemy  # version: 1.4.0
from sqlalchemy import create_engine

# Import migration environment functions
from .env import run_migrations_online as _env_run_migrations_online
from .env import run_migrations_offline as _env_run_migrations_offline
from .env import get_url

# Import migration version utilities
from .versions import get_available_migrations, get_migration_heads

# Define what's available for import from this package
__all__ = [
    "run_migrations_online",
    "run_migrations_offline",
    "get_version_info",
    "get_current_revision",
    "get_head_revision",
    "check_migration_needed"
]

# Configure logger
logger = logging.getLogger('alembic.migrations')


def run_migrations_online(environment, dry_run=False):
    """
    Runs Alembic migrations directly against the database in 'online' mode.
    
    Args:
        environment (str): The target environment (dev, qa, prod)
        dry_run (bool): If True, only prints what would be done without executing
    
    Returns:
        bool: True if migrations were successful, False otherwise
    """
    logger.info("Starting online database migration for environment: %s", environment)
    logger.info("Dry run mode: %s", "Enabled" if dry_run else "Disabled")
    
    # Set environment variable for the specified environment
    os.environ['AIRFLOW_ENV'] = environment
    
    try:
        if dry_run:
            logger.info("DRY RUN: Would execute migrations for environment %s", environment)
            success = True
        else:
            # Execute the actual migrations
            _env_run_migrations_online()
            success = True
            logger.info("Migrations completed successfully")
    except Exception as e:
        logger.exception("Error during migration: %s", str(e))
        success = False
    
    return success


def run_migrations_offline(environment, output_path):
    """
    Runs Alembic migrations in 'offline' mode, generating SQL scripts without executing them.
    
    Args:
        environment (str): The target environment (dev, qa, prod)
        output_path (str): File path where the SQL script should be saved
    
    Returns:
        str: Path to the generated SQL script file
    """
    logger.info("Generating offline migration SQL script for environment: %s", environment)
    logger.info("Output will be written to: %s", output_path)
    
    # Set environment variable for the specified environment
    os.environ['AIRFLOW_ENV'] = environment
    
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        # Temporarily redirect stdout to capture the SQL output
        import sys
        original_stdout = sys.stdout
        
        try:
            with open(output_path, 'w') as f:
                sys.stdout = f
                # Run migrations in offline mode, which will output SQL to stdout
                _env_run_migrations_offline()
        finally:
            # Restore stdout
            sys.stdout = original_stdout
        
        logger.info("SQL migration script generated successfully at: %s", output_path)
        return output_path
    except Exception as e:
        logger.exception("Error generating migration script: %s", str(e))
        # Ensure stdout is restored in case of an exception
        if 'original_stdout' in locals() and sys.stdout != original_stdout:
            sys.stdout = original_stdout
        raise


def get_version_info():
    """
    Retrieves metadata about available database migrations.
    
    Returns:
        dict: Dictionary containing migration version information
    """
    logger.debug("Retrieving migration version information")
    
    try:
        # Get available migrations
        migrations = get_available_migrations()
        
        # Get head revisions
        heads = get_migration_heads()
        
        # Create version info dictionary
        version_info = {
            'migrations': {rev: {
                'down_revision': getattr(mod, 'down_revision', None),
                'description': mod.__doc__.split('\n')[0] if mod.__doc__ else 'No description'
            } for rev, mod in migrations.items()},
            'heads': heads
        }
        
        logger.debug("Found %d migrations with %d heads", 
                     len(version_info['migrations']),
                     len(version_info['heads']))
        
        return version_info
    except Exception as e:
        logger.exception("Error retrieving version information: %s", str(e))
        raise


def get_current_revision(environment):
    """
    Gets the current revision of the database schema.
    
    Args:
        environment (str): The target environment (dev, qa, prod)
    
    Returns:
        str: Current revision identifier, or None if no revision is found
    """
    logger.debug("Getting current database revision for environment: %s", environment)
    
    # Set environment variable for the specified environment
    os.environ['AIRFLOW_ENV'] = environment
    
    try:
        # Get database connection URL
        url = get_url()
        
        # Create SQLAlchemy engine
        engine = create_engine(url)
        
        # Connect and get current revision
        with engine.connect() as connection:
            context = migration.MigrationContext.configure(connection)
            current_rev = context.get_current_revision()
        
        if current_rev:
            logger.debug("Current database revision: %s", current_rev)
        else:
            logger.debug("No current revision found, database may not be initialized")
        
        return current_rev
    except Exception as e:
        logger.exception("Error getting current revision: %s", str(e))
        raise


def get_head_revision():
    """
    Gets the latest head revision available in the migration scripts.
    
    Returns:
        str: Head revision identifier, or None if no head is found
    """
    logger.debug("Getting head migration revision")
    
    try:
        # Get head revisions
        heads = get_migration_heads()
        
        if not heads:
            logger.warning("No head revisions found in migration scripts")
            return None
        
        # Return the first head (typically only one)
        head_revision = heads[0]
        logger.debug("Head revision: %s", head_revision)
        
        return head_revision
    except Exception as e:
        logger.exception("Error getting head revision: %s", str(e))
        raise


def check_migration_needed(environment):
    """
    Checks if a database migration is needed by comparing current and head revisions.
    
    Args:
        environment (str): The target environment (dev, qa, prod)
    
    Returns:
        bool: True if migration is needed, False otherwise
    """
    logger.debug("Checking if migration is needed for environment: %s", environment)
    
    try:
        # Get current and head revisions
        current_rev = get_current_revision(environment)
        head_rev = get_head_revision()
        
        # If no current revision, database needs initialization
        if not current_rev:
            logger.info("No current revision found, migration/initialization needed")
            return True
        
        # Compare revisions
        if current_rev != head_rev:
            logger.info("Migration needed: current=%s, head=%s", current_rev, head_rev)
            return True
        else:
            logger.info("No migration needed: current=%s already at head", current_rev)
            return False
    except Exception as e:
        logger.exception("Error checking if migration is needed: %s", str(e))
        raise