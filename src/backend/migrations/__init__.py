#!/usr/bin/env python3
"""
Package initialization file for the migrations module, exposing components needed for database schema 
and code migration from Apache Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2. Acts as a central 
access point for migration utilities and version management.
"""

import logging  # standard library
import importlib  # standard library

# Import Alembic migrations functionality
from .alembic_migrations import (
    run_migrations_online,
    run_migrations_offline,
    get_version_info,
    get_current_revision,
    get_head_revision,
    check_migration_needed
)

# Import Airflow migration utilities
from .migration_airflow1_to_airflow2 import (
    DAGMigrator,
    ConnectionMigrator,
    PluginMigrator,
    migrate_database_schema,
    validate_migration,
    create_backup,
    rollback_migration
)

# Module version
__version__ = '1.0.0'

# Define what's available for import from this package
__all__ = [
    "DAGMigrator",
    "ConnectionMigrator", 
    "PluginMigrator",
    "migrate_database_schema",
    "validate_migration",
    "create_backup",
    "rollback_migration",
    "run_migrations_online",
    "run_migrations_offline",
    "get_version_info",
    "get_current_revision",
    "get_head_revision",
    "check_migration_needed"
]

# Configure logger
logger = logging.getLogger('airflow.migrations')