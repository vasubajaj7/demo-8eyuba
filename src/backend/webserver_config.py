"""
Airflow 2.X Webserver Configuration for Cloud Composer 2

This file configures the Airflow web server component in Cloud Composer 2,
including authentication, authorization, UI customization, and security settings.
It's a critical component for implementing secure and compliant access to the
workflow orchestration platform during migration from Airflow 1.10.15.

This configuration implements:
- Google SSO integration for secure authentication
- Role-based access control (RBAC) for authorization
- Environment-specific UI customization
- Session security and CSRF protection
- Multi-environment deployment support
"""

import os
import sys
import base64
import secrets
from datetime import datetime, timedelta

# Authentication imports
from flask_appbuilder.security.manager import AUTH_OAUTH
import google.auth

# Environment detection and base directory setup
ENV = os.environ.get('AIRFLOW_VAR_ENV', 'dev')
BASEDIR = os.path.abspath(os.path.dirname(__file__))

# Import environment-specific configuration
if ENV == 'dev':
    from config.composer_dev import config
elif ENV == 'qa':
    from config.composer_qa import config
elif ENV == 'prod':
    from config.composer_prod import config
else:
    # Default to dev if unknown environment
    from config.composer_dev import config
    print(f"Warning: Unknown environment '{ENV}', defaulting to 'dev'")

# Access specific configuration sections
security_config = config['security']
environment_config = config['environment']
composer_config = config['composer']

# Generate a secure secret key or use the one from environment
SECRET_KEY = secrets.token_hex(32) if os.environ.get('WEBSERVER_SECRET_KEY') is None else os.environ.get('WEBSERVER_SECRET_KEY')

# Core authentication settings
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = 'Viewer'

# Security-related session settings
PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
SESSION_COOKIE_SAMESITE = 'Lax'

# CSRF Protection
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = 7200

# API Documentation
FAB_API_SWAGGER_UI = True

# Configure Airflow security manager class
FAB_SECURITY_MANAGER_CLASS = 'airflow.www.security.AirflowSecurityManager'

def get_oauth_providers():
    """
    Configures OAuth providers based on environment variables with secure defaults
    
    Returns:
        dict: Dictionary containing OAuth provider configuration
    """
    client_id = os.environ.get('OAUTH_CLIENT_ID', '')
    client_secret = os.environ.get('OAUTH_CLIENT_SECRET', '')
    
    if not client_id or not client_secret:
        print("Warning: OAUTH_CLIENT_ID or OAUTH_CLIENT_SECRET is not set. OAuth authentication may not work correctly.")
    
    # Google OAuth provider configuration
    providers = {
        'google': {
            'icon': 'fa-google',
            'token_key': 'access_token',
            'remote_app': {
                'client_id': client_id,
                'client_secret': client_secret,
                'api_base_url': 'https://www.googleapis.com/oauth2/v2/',
                'client_kwargs': {
                    'scope': 'email profile'
                },
                'request_token_url': None,
                'access_token_url': 'https://accounts.google.com/o/oauth2/token',
                'authorize_url': 'https://accounts.google.com/o/oauth2/auth',
                'jwks_uri': 'https://www.googleapis.com/oauth2/v3/certs'
            }
        }
    }
    
    return providers

# Define OAuth providers
OAUTH_PROVIDERS = get_oauth_providers()

def get_environment_config():
    """
    Detects and loads the correct environment configuration based on environment variables
    
    Returns:
        dict: Environment-specific configuration dictionary
    """
    return config

def security_manager_factory(app):
    """
    Creates and configures the security manager with appropriate roles and permissions
    
    Parameters:
        app: The Flask application instance
        
    Returns:
        AirflowSecurityManager: Configured security manager instance
    """
    from airflow.www.security import AirflowSecurityManager
    
    # Initialize security manager
    security_manager = AirflowSecurityManager(app)
    
    # Define role permissions based on environment
    admin_permissions = [
        'can_create', 'can_read', 'can_edit', 'can_delete',
        'can_dag_edit', 'can_dag_read', 'can_dag_run',
        'can_variable_edit', 'can_variable_read',
        'can_pool_edit', 'can_pool_read',
        'can_connection_edit', 'can_connection_read',
        'can_configuration_edit', 'can_configuration_read',
        'menu_access'
    ]
    
    user_permissions = [
        'can_read', 'can_edit', 'can_create',
        'can_dag_read', 'can_dag_run',
        'can_variable_read',
        'can_pool_read',
        'can_connection_read',
        'menu_access'
    ]
    
    viewer_permissions = [
        'can_read',
        'can_dag_read',
        'menu_access'
    ]
    
    op_permissions = [
        'can_read',
        'can_dag_read', 'can_dag_run',
        'can_task_instance_read', 'can_task_instance_clear',
        'menu_access'
    ]
    
    # Apply environment-specific permission adjustments
    if ENV == 'dev':
        # Development environment may have more relaxed permissions
        user_permissions.extend(['can_variable_edit', 'can_pool_edit'])
        viewer_permissions.append('can_dag_run')
    elif ENV == 'qa':
        # QA environment - slightly restricted from dev
        user_permissions.append('can_dag_edit')
    elif ENV == 'prod':
        # Production environment has strict permissions
        # Remove potentially dangerous permissions in production
        if 'can_variable_edit' in user_permissions:
            user_permissions.remove('can_variable_edit')
        if 'can_pool_edit' in user_permissions:
            user_permissions.remove('can_pool_edit')
    
    # Update roles with permissions
    try:
        # Check if roles exist before updating
        roles = security_manager.get_all_roles()
        role_names = [role.name for role in roles]
        
        # Update Admin role
        if 'Admin' in role_names:
            security_manager.update_role('Admin', permissions=admin_permissions)
        else:
            security_manager.add_role('Admin', permissions=admin_permissions)
            
        # Update User role
        if 'User' in role_names:
            security_manager.update_role('User', permissions=user_permissions)
        else:
            security_manager.add_role('User', permissions=user_permissions)
            
        # Update Viewer role
        if 'Viewer' in role_names:
            security_manager.update_role('Viewer', permissions=viewer_permissions)
        else:
            security_manager.add_role('Viewer', permissions=viewer_permissions)
            
        # Update Op role
        if 'Op' in role_names:
            security_manager.update_role('Op', permissions=op_permissions)
        else:
            security_manager.add_role('Op', permissions=op_permissions)
            
    except Exception as e:
        print(f"Error updating roles and permissions: {e}")
    
    return security_manager

# UI Customization based on environment
if ENV == 'dev':
    APP_NAME = "Airflow - Development"
    navbar_color = "#007A87"
    SHOW_STACKTRACE = True
    AUTH_USER_REGISTRATION = True
elif ENV == 'qa':
    APP_NAME = "Airflow - QA"
    navbar_color = "#2E86C1"
    SHOW_STACKTRACE = True
    AUTH_USER_REGISTRATION = True
elif ENV == 'prod':
    APP_NAME = "Airflow - Production"
    navbar_color = "#1A5276"
    SHOW_STACKTRACE = False
    AUTH_USER_REGISTRATION = False
else:
    APP_NAME = "Airflow"
    navbar_color = "#007A87"
    SHOW_STACKTRACE = False
    AUTH_USER_REGISTRATION = True

# Theme configuration
FAB_THEME = "cosmo"  # Flask-AppBuilder theme
APP_ICON = "/static/pin_64.png"
PREFERRED_URL_SCHEME = 'https'
TIMEZONE = 'UTC'

# DAG view settings
DAG_DEFAULT_VIEW = 'grid'
DAG_ORIENTATION = 'TB'
FILTER_BY_OWNER = False
HIDE_PAUSED_DAGS_BY_DEFAULT = False
MAX_DAG_TITLE_LEN = 50

# Configure roles and permissions for RBAC
FAB_ROLES = {
    'Admin': {
        'description': 'Full administrator access',
        'permissions': [
            'all_dags', 'all_dag_runs', 'all_variables', 
            'all_connections', 'all_pools', 'all_configuration'
        ]
    },
    'User': {
        'description': 'Edit permissions for non-production environments',
        'permissions': [
            'can_edit', 'can_create', 'can_delete_on_non_production'
        ]
    },
    'Viewer': {
        'description': 'Read-only access to the Airflow UI',
        'permissions': [
            'can_read', 'can_view_dags'
        ]
    },
    'Op': {
        'description': 'Operational user for workflow management',
        'permissions': [
            'can_run_tasks', 'can_trigger_dags', 'can_view_logs'
        ]
    }
}

# Apply IP restrictions if configured
web_server_allow_ip_ranges = security_config.get('web_server_allow_ip_ranges', [])
if web_server_allow_ip_ranges:
    # Set IP allow list for web server
    ALLOWED_HOSTS = web_server_allow_ip_ranges

# Apply additional security settings for production
if ENV == 'prod':
    # Enhanced security for production
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_SECURE = True
    REMEMBER_COOKIE_HTTPONLY = True
    
    # Apply private endpoint configuration if enabled
    if security_config.get('enable_private_endpoint', False):
        PUBLIC_ROLE_LIKE_DAG_VIEW = False  # Prevent public access to DAG view
    
    # Enable CMEK if configured
    if security_config.get('enable_cmek', False):
        ENCRYPT_SENSITIVE_DATA = True