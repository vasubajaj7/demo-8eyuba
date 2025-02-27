"""
Root initialization file for the Airflow providers package structure that centralizes access to custom provider implementations (hooks, operators, sensors) in the migration from Airflow 1.10.15 to Airflow 2.X in Cloud Composer 2. This file acts as the entry point for provider discovery and registration.
"""

import os
import logging
import importlib
from typing import List, Dict
import pkg_resources

# Import the custom provider package for registration with Airflow
from .custom_provider import get_provider_info

# Set up package-level logging
logger = logging.getLogger(__name__)

# Package version
__version__ = "2.0.0"

# Define what should be imported with `from backend.providers import *`
__all__ = ["custom_provider", "get_available_providers"]


def get_available_providers() -> List[str]:
    """
    Returns a list of all available custom providers in this package
    
    Returns:
        List[str]: List of provider package names available in this package
    """
    providers = []
    
    # Get the current directory path of this package
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Iterate through subdirectories in the current directory
    for item in os.listdir(current_dir):
        item_path = os.path.join(current_dir, item)
        
        # Check if the item is a directory and potentially a provider package
        if os.path.isdir(item_path) and not item.startswith('__'):
            # Look for __init__.py with get_provider_info function
            init_file = os.path.join(item_path, '__init__.py')
            if os.path.isfile(init_file):
                try:
                    # Try to import the module dynamically
                    module = importlib.import_module(f".{item}", package="backend.providers")
                    
                    # Check if it has get_provider_info function
                    if hasattr(module, 'get_provider_info'):
                        providers.append(item)
                except (ImportError, AttributeError) as e:
                    logger.warning(f"Failed to import potential provider package '{item}': {str(e)}")
    
    logger.info(f"Discovered {len(providers)} provider packages: {providers}")
    return providers


def discover_providers() -> List[Dict]:
    """
    Discovers and loads all available providers for registration with Airflow
    
    Returns:
        List[Dict]: List of provider metadata dictionaries
    """
    providers_info = []
    available_providers = get_available_providers()
    
    for provider_name in available_providers:
        try:
            # Import the provider module
            provider_module = importlib.import_module(f".{provider_name}", package="backend.providers")
            
            # Get provider info by calling get_provider_info() from each provider
            if hasattr(provider_module, 'get_provider_info'):
                provider_info = provider_module.get_provider_info()
                
                # Validate provider metadata format
                if isinstance(provider_info, dict) and 'package-name' in provider_info:
                    providers_info.append(provider_info)
                    logger.info(f"Discovered provider: {provider_info['package-name']} v{provider_info.get('version', 'unknown')}")
                else:
                    logger.warning(f"Invalid provider info format for {provider_name}")
        except Exception as e:
            logger.error(f"Error loading provider {provider_name}: {str(e)}")
    
    return providers_info