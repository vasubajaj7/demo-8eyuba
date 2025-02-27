"""
Package initialization file for Alembic migration versions.

This module defines the migration version collection and provides utilities
for version discovery, making it possible to track and manage database schema
migrations for the Airflow 1.10.15 to Airflow 2.X transition in Cloud Composer 2.
"""

import logging
import os
import importlib.util

# Define public exports
__all__ = ['get_available_migrations', 'get_migration_heads', 'get_migration_by_revision']

# Configure logger
logger = logging.getLogger('alembic.versions')

# Get the current directory path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Dictionary to store migrations by revision ID
MIGRATIONS = {}


def get_available_migrations():
    """
    Retrieves a dictionary of all available migration scripts in the versions directory.
    
    Returns:
        dict: Dictionary mapping revision IDs to migration module objects
    """
    migrations = {}
    
    # Get all Python files in the current directory
    for filename in os.listdir(CURRENT_DIR):
        # Skip __init__.py and non-Python files
        if filename == "__init__.py" or not filename.endswith(".py"):
            continue
        
        # Get the full path to the file
        filepath = os.path.join(CURRENT_DIR, filename)
        
        # Get the module name (filename without .py extension)
        module_name = os.path.splitext(filename)[0]
        
        try:
            # Dynamically import the module
            spec = importlib.util.spec_from_file_location(module_name, filepath)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Check if the module has a revision attribute
            if hasattr(module, 'revision'):
                revision = module.revision
                migrations[revision] = module
                logger.debug(f"Loaded migration {revision} from {filename}")
            else:
                logger.warning(f"Skipping {filename}: Missing revision attribute")
        except Exception as e:
            logger.error(f"Error loading migration from {filename}: {str(e)}")
    
    return migrations


def get_migration_heads():
    """
    Identifies and returns the head migration revision(s) that have no downstream dependencies.
    
    Returns:
        list: List of revision IDs representing head migrations
    """
    migrations = _load_migrations()
    
    # Collect all down_revisions (migrations that are depended on)
    down_revisions = set()
    for module in migrations.values():
        if hasattr(module, 'down_revision') and module.down_revision:
            down_revisions.add(module.down_revision)
    
    # Heads are revisions that are not in down_revisions
    heads = [rev for rev in migrations.keys() if rev not in down_revisions]
    
    return heads


def get_migration_by_revision(revision_id):
    """
    Retrieves a specific migration module by its revision ID.
    
    Args:
        revision_id (str): The revision ID to retrieve
        
    Returns:
        module: Migration module corresponding to the specified revision ID
        
    Raises:
        KeyError: If the revision ID is not found
    """
    migrations = _load_migrations()
    
    if revision_id not in migrations:
        raise KeyError(f"Migration revision '{revision_id}' not found")
    
    return migrations[revision_id]


def _load_migrations():
    """
    Internal function to load and cache all migration modules.
    
    Returns:
        dict: Dictionary of loaded migration modules
    """
    global MIGRATIONS
    
    if not MIGRATIONS:
        MIGRATIONS = get_available_migrations()
    
    return MIGRATIONS