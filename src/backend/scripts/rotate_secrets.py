#!/usr/bin/env python
"""
Script to automate the rotation of secrets in GCP Secret Manager and update related
Airflow connections and variables. This tool helps maintain security best practices
by regularly rotating credentials used in Cloud Composer 2 environments according to
configurable policies.
"""

import argparse
import logging
import os
import sys
import json
import datetime
from datetime import timedelta
import secrets
import string
import re
import yaml
from google.cloud.secretmanager import SecretManagerServiceClient

# Internal imports
# Adjust the imports to make sure Python can find these modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.utils.gcp_utils import get_secret, create_secret, SecretManagerClient
from dags.utils.db_utils import execute_query
from dags.utils.alert_utils import send_email_alert, AlertLevel

# Configure logging
LOGGER = logging.getLogger(__name__)

# Default paths and configuration
DEFAULT_CONFIG_PATH = '../config/'
SECRET_TYPES = ['connection', 'variable', 'api_key', 'password']
ROTATION_FREQUENCY_DAYS = 90
DEFAULT_PASSWORD_LENGTH = 16
SECRET_REFERENCE_PATTERN = r'{SECRET:(.*?)}'


def setup_logging(log_level='INFO'):
    """
    Configure logging for the script with appropriate format and level.

    Args:
        log_level (str): Logging level (INFO, DEBUG, etc.)

    Returns:
        logging.Logger: Configured logger instance
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger = logging.getLogger(__name__)
    if not logger.handlers:  # Avoid adding multiple handlers
        logger.addHandler(console_handler)
    
    return logger


def parse_args():
    """
    Parse command line arguments for script execution.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Rotate secrets in GCP Secret Manager and update Airflow connections/variables'
    )
    
    parser.add_argument(
        '--env',
        required=True,
        choices=['dev', 'qa', 'prod'],
        help='Environment to run the rotation in (dev, qa, prod)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate rotation without actually changing secrets'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    
    parser.add_argument(
        '--secret-id',
        help='Rotate only a specific secret (by ID)'
    )
    
    parser.add_argument(
        '--config-path',
        default=DEFAULT_CONFIG_PATH,
        help=f'Path to configuration files (default: {DEFAULT_CONFIG_PATH})'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force rotation regardless of age'
    )
    
    return parser.parse_args()


def load_config(env, config_path):
    """
    Load configuration from the appropriate environment config file.

    Args:
        env (str): Environment name (dev, qa, prod)
        config_path (str): Path to configuration directory

    Returns:
        dict: Configuration values for the specified environment
    """
    config_file_path = os.path.join(config_path, f'secrets_config_{env}.json')
    yaml_config_path = os.path.join(config_path, f'secrets_config_{env}.yaml')
    
    # Try JSON file first
    if os.path.exists(config_file_path):
        try:
            LOGGER.info(f"Loading config from {config_file_path}")
            with open(config_file_path, 'r') as f:
                config = json.load(f)
            return config
        except json.JSONDecodeError as e:
            LOGGER.error(f"Error parsing JSON config: {str(e)}")
            sys.exit(1)
    
    # Try YAML file if JSON not found
    elif os.path.exists(yaml_config_path):
        try:
            LOGGER.info(f"Loading config from {yaml_config_path}")
            with open(yaml_config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except yaml.YAMLError as e:
            LOGGER.error(f"Error parsing YAML config: {str(e)}")
            sys.exit(1)
    
    # No config file found
    else:
        LOGGER.error(f"Config file not found for environment '{env}'")
        LOGGER.error(f"Looked for: {config_file_path} or {yaml_config_path}")
        sys.exit(1)


def find_secrets_in_connections(connections_config):
    """
    Identify secret references in connection configurations.

    Args:
        connections_config (dict): Connection configurations

    Returns:
        dict: Mapping of secret IDs to their references in connections
    """
    secret_references = {}
    
    def search_dict_for_secrets(d, path=""):
        if not isinstance(d, dict):
            return
            
        for key, value in d.items():
            current_path = f"{path}.{key}" if path else key
            
            if isinstance(value, str):
                # Look for {SECRET:secret_id} pattern
                matches = re.findall(SECRET_REFERENCE_PATTERN, value)
                for secret_id in matches:
                    if secret_id not in secret_references:
                        secret_references[secret_id] = []
                    secret_references[secret_id].append({
                        "location": "connection",
                        "path": current_path,
                        "reference": value
                    })
            
            elif isinstance(value, dict):
                search_dict_for_secrets(value, current_path)
            
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        search_dict_for_secrets(item, f"{current_path}[{i}]")
    
    search_dict_for_secrets(connections_config)
    
    LOGGER.info(f"Found {len(secret_references)} unique secret references in connections")
    return secret_references


def find_secrets_in_variables(variables_config):
    """
    Identify secret references in variable configurations.

    Args:
        variables_config (dict): Variable configurations

    Returns:
        dict: Mapping of secret IDs to their references in variables
    """
    secret_references = {}
    
    def search_dict_for_secrets(d, path=""):
        if not isinstance(d, dict):
            return
            
        for key, value in d.items():
            current_path = f"{path}.{key}" if path else key
            
            if isinstance(value, str):
                # Look for {SECRET:secret_id} pattern
                matches = re.findall(SECRET_REFERENCE_PATTERN, value)
                for secret_id in matches:
                    if secret_id not in secret_references:
                        secret_references[secret_id] = []
                    secret_references[secret_id].append({
                        "location": "variable",
                        "path": current_path,
                        "reference": value
                    })
            
            elif isinstance(value, dict):
                search_dict_for_secrets(value, current_path)
            
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        search_dict_for_secrets(item, f"{current_path}[{i}]")
    
    search_dict_for_secrets(variables_config)
    
    LOGGER.info(f"Found {len(secret_references)} unique secret references in variables")
    return secret_references


def check_secret_rotation_needed(secret_id, secret_client, project_id, rotation_period_days):
    """
    Determine if a secret needs rotation based on age.

    Args:
        secret_id (str): The ID of the secret to check
        secret_client (object): Secret Manager client
        project_id (str): GCP project ID
        rotation_period_days (int): Number of days after which rotation is needed

    Returns:
        bool: True if rotation is needed, False otherwise
    """
    try:
        # Get the secret metadata
        secret_path = f"projects/{project_id}/secrets/{secret_id}"
        
        try:
            # Get the secret
            secret = secret_client.get_secret(name=secret_path)
            
            # Get all versions
            versions = list(secret_client.list_secret_versions(parent=secret_path))
            
            # Find the latest enabled version
            latest_version = None
            for version in versions:
                if version.state.name == 'ENABLED':
                    latest_version = version
                    break
            
            if not latest_version:
                LOGGER.warning(f"No enabled versions found for secret {secret_id}")
                return True  # If no enabled version, rotation is needed
            
            # Get creation time from the latest version
            create_time = latest_version.create_time
            
            # Calculate age in days
            now = datetime.datetime.now(datetime.timezone.utc)
            age_days = (now - create_time).days
            
            LOGGER.info(f"Secret {secret_id} is {age_days} days old (rotation needed after {rotation_period_days} days)")
            
            # Return True if rotation is needed
            return age_days >= rotation_period_days
            
        except Exception as e:
            LOGGER.warning(f"Error checking secret {secret_id}: {str(e)}")
            return True  # If we can't check, better to rotate for safety
            
    except Exception as e:
        LOGGER.error(f"Failed to check if rotation needed for {secret_id}: {str(e)}")
        return False  # In case of critical error, don't attempt rotation


def generate_new_secret_value(secret_id, current_value, secret_type=None):
    """
    Generate a new secure value for a given secret based on its type.

    Args:
        secret_id (str): The ID of the secret
        current_value (str): Current secret value
        secret_type (str): Type of secret (connection, variable, api_key, password)

    Returns:
        str: New secure secret value
    """
    # Determine secret type if not provided
    if not secret_type:
        # Try to infer type from secret_id
        if secret_id.startswith('conn_'):
            secret_type = 'connection'
        elif secret_id.startswith('var_'):
            secret_type = 'variable'
        elif secret_id.startswith('api_') or secret_id.endswith('_key'):
            secret_type = 'api_key'
        elif secret_id.startswith('pwd_') or secret_id.endswith('_password'):
            secret_type = 'password'
        else:
            # Default to password if can't determine
            secret_type = 'password'
    
    LOGGER.debug(f"Generating new value for {secret_type} secret: {secret_id}")
    
    # Generate new value based on type
    if secret_type == 'connection':
        try:
            # Connection secrets are often JSON
            conn_data = json.loads(current_value)
            
            # Generate new password if present
            if 'password' in conn_data:
                password_chars = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
                new_password = ''.join(secrets.choice(password_chars) for _ in range(DEFAULT_PASSWORD_LENGTH))
                conn_data['password'] = new_password
                
            # Generate new key if present    
            if 'key' in conn_data:
                key_chars = string.ascii_letters + string.digits + "_-"
                new_key = ''.join(secrets.choice(key_chars) for _ in range(DEFAULT_PASSWORD_LENGTH * 2))
                conn_data['key'] = new_key
                
            return json.dumps(conn_data)
            
        except json.JSONDecodeError:
            # If not JSON, generate completely new value
            LOGGER.warning(f"Connection value for {secret_id} is not valid JSON, generating new value")
            password_chars = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
            return ''.join(secrets.choice(password_chars) for _ in range(DEFAULT_PASSWORD_LENGTH))
    
    elif secret_type == 'api_key':
        # API keys are usually alphanumeric
        key_chars = string.ascii_letters + string.digits
        return ''.join(secrets.choice(key_chars) for _ in range(DEFAULT_PASSWORD_LENGTH * 2))
    
    elif secret_type == 'password':
        # Complex password with special characters
        password_chars = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
        password = ''.join(secrets.choice(password_chars) for _ in range(DEFAULT_PASSWORD_LENGTH))
        
        # Ensure complexity requirements
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(c in "!@#$%^&*()-_=+[]{}|;:,.<>?" for c in password)
        
        # If missing any requirement, regenerate
        if not (has_upper and has_lower and has_digit and has_special):
            return generate_new_secret_value(secret_id, current_value, 'password')
            
        return password
    
    elif secret_type == 'variable':
        # For variables, try to keep the same format but replace any sensitive parts
        try:
            # Check if it's JSON
            var_data = json.loads(current_value)
            # Recursively update any password or key fields
            def update_sensitive_fields(obj):
                if isinstance(obj, dict):
                    for key in obj:
                        if key.lower() in ('password', 'key', 'secret', 'token'):
                            if isinstance(obj[key], str):
                                password_chars = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
                                obj[key] = ''.join(secrets.choice(password_chars) for _ in range(len(obj[key])))
                        elif isinstance(obj[key], (dict, list)):
                            update_sensitive_fields(obj[key])
                elif isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, (dict, list)):
                            update_sensitive_fields(item)
            
            update_sensitive_fields(var_data)
            return json.dumps(var_data)
            
        except json.JSONDecodeError:
            # If not JSON, generate new value with same length as original
            if len(current_value) > 0:
                password_chars = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
                return ''.join(secrets.choice(password_chars) for _ in range(len(current_value)))
            else:
                # Empty string case
                return ""
    
    else:
        # Default case - generate secure random string
        password_chars = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
        return ''.join(secrets.choice(password_chars) for _ in range(DEFAULT_PASSWORD_LENGTH))


def rotate_secret(secret_id, secret_client, project_id, dry_run=False):
    """
    Rotate a single secret in Secret Manager.

    Args:
        secret_id (str): The ID of the secret to rotate
        secret_client (object): Secret Manager client
        project_id (str): GCP project ID
        dry_run (bool): If True, only simulate the rotation

    Returns:
        dict: Result with status and values
    """
    result = {
        "secret_id": secret_id,
        "status": "failed",
        "old_value": None,
        "new_value": None,
        "error": None
    }
    
    try:
        LOGGER.info(f"Rotating secret: {secret_id}")
        
        # Get the current secret value
        secret_path = f"projects/{project_id}/secrets/{secret_id}"
        
        try:
            # Get latest version
            response = secret_client.access_secret_version(
                name=f"{secret_path}/versions/latest"
            )
            current_value = response.payload.data.decode('UTF-8')
            result["old_value"] = current_value
            
            # Determine secret type
            secret_type = None
            for t in SECRET_TYPES:
                if t in secret_id.lower():
                    secret_type = t
                    break
            
            if not secret_type:
                # Try to infer from content
                if current_value.startswith('{') and current_value.endswith('}'):
                    try:
                        json.loads(current_value)
                        secret_type = 'connection'
                    except:
                        secret_type = 'variable'
                else:
                    # Default to password
                    secret_type = 'password'
            
            # Generate new value
            new_value = generate_new_secret_value(secret_id, current_value, secret_type)
            result["new_value"] = new_value
            
            # Create a new version of the secret with the new value
            if not dry_run:
                secret_client.add_secret_version(
                    parent=secret_path,
                    payload={"data": new_value.encode('UTF-8')}
                )
                LOGGER.info(f"Successfully created new version of secret {secret_id}")
            else:
                LOGGER.info(f"DRY RUN: Would create new version of secret {secret_id}")
            
            result["status"] = "success"
            return result
            
        except Exception as e:
            error_msg = f"Failed to access or update secret {secret_id}: {str(e)}"
            LOGGER.error(error_msg)
            result["error"] = error_msg
            return result
            
    except Exception as e:
        error_msg = f"Unexpected error rotating secret {secret_id}: {str(e)}"
        LOGGER.error(error_msg)
        result["error"] = error_msg
        return result


def update_connections_with_new_secrets(connections_config, rotated_secrets):
    """
    Update connection configurations with new secret values.

    Args:
        connections_config (dict): Connection configurations
        rotated_secrets (dict): Dict of rotated secrets with old and new values

    Returns:
        dict: Updated connections configuration
    """
    if not connections_config or not rotated_secrets:
        return connections_config
    
    # Clone the config to avoid modifying the original
    updated_config = json.loads(json.dumps(connections_config))
    
    # Find all secret references in the config
    secret_refs = find_secrets_in_connections(connections_config)
    
    for secret_id, secret_result in rotated_secrets.items():
        if secret_id in secret_refs and secret_result["status"] == "success":
            # Get all references to this secret
            for ref in secret_refs[secret_id]:
                location = ref["location"]
                path = ref["path"]
                reference = ref["reference"]
                
                # Navigate to the correct path in the config
                path_parts = path.split('.')
                current = updated_config
                
                # Navigate to parent of the value to update
                for i, part in enumerate(path_parts[:-1]):
                    # Handle array indices in path
                    if '[' in part and ']' in part:
                        base_part = part.split('[')[0]
                        idx = int(part.split('[')[1].split(']')[0])
                        current = current[base_part][idx]
                    else:
                        current = current[part]
                
                # Get the last path part
                last_part = path_parts[-1]
                if '[' in last_part and ']' in last_part:
                    base_part = last_part.split('[')[0]
                    idx = int(last_part.split('[')[1].split(']')[0])
                    
                    # Replace the secret reference with the new value
                    old_value = current[base_part][idx]
                    new_value = old_value.replace(reference, reference.replace(
                        secret_result["old_value"], secret_result["new_value"]))
                    current[base_part][idx] = new_value
                else:
                    # Replace the secret reference with the new value
                    old_value = current[last_part]
                    new_value = old_value.replace(reference, reference.replace(
                        secret_result["old_value"], secret_result["new_value"]))
                    current[last_part] = new_value
                
                LOGGER.debug(f"Updated {location} reference to {secret_id} at {path}")
    
    LOGGER.info(f"Updated connections configuration with {len(rotated_secrets)} new secret values")
    return updated_config


def update_variables_with_new_secrets(variables_config, rotated_secrets):
    """
    Update variable configurations with new secret values.

    Args:
        variables_config (dict): Variable configurations
        rotated_secrets (dict): Dict of rotated secrets with old and new values

    Returns:
        dict: Updated variables configuration
    """
    if not variables_config or not rotated_secrets:
        return variables_config
    
    # Clone the config to avoid modifying the original
    updated_config = json.loads(json.dumps(variables_config))
    
    # Find all secret references in the config
    secret_refs = find_secrets_in_variables(variables_config)
    
    for secret_id, secret_result in rotated_secrets.items():
        if secret_id in secret_refs and secret_result["status"] == "success":
            # Get all references to this secret
            for ref in secret_refs[secret_id]:
                location = ref["location"]
                path = ref["path"]
                reference = ref["reference"]
                
                # Navigate to the correct path in the config
                path_parts = path.split('.')
                current = updated_config
                
                # Navigate to parent of the value to update
                for i, part in enumerate(path_parts[:-1]):
                    # Handle array indices in path
                    if '[' in part and ']' in part:
                        base_part = part.split('[')[0]
                        idx = int(part.split('[')[1].split(']')[0])
                        current = current[base_part][idx]
                    else:
                        current = current[part]
                
                # Get the last path part
                last_part = path_parts[-1]
                if '[' in last_part and ']' in last_part:
                    base_part = last_part.split('[')[0]
                    idx = int(last_part.split('[')[1].split(']')[0])
                    
                    # Replace the secret reference with the new value
                    old_value = current[base_part][idx]
                    new_value = old_value.replace(reference, reference.replace(
                        secret_result["old_value"], secret_result["new_value"]))
                    current[base_part][idx] = new_value
                else:
                    # Replace the secret reference with the new value
                    old_value = current[last_part]
                    new_value = old_value.replace(reference, reference.replace(
                        secret_result["old_value"], secret_result["new_value"]))
                    current[last_part] = new_value
                
                LOGGER.debug(f"Updated {location} reference to {secret_id} at {path}")
    
    LOGGER.info(f"Updated variables configuration with {len(rotated_secrets)} new secret values")
    return updated_config


def save_updated_config(updated_config, config_file_path, dry_run=False):
    """
    Save updated configurations back to files.

    Args:
        updated_config (dict): Updated configuration
        config_file_path (str): Path to the config file
        dry_run (bool): If True, only simulate the save

    Returns:
        bool: True if saved successfully, False otherwise
    """
    if dry_run:
        LOGGER.info(f"DRY RUN: Would save updated config to {config_file_path}")
        return True
    
    try:
        # Determine file format
        if config_file_path.endswith('.json'):
            with open(config_file_path, 'w') as f:
                json.dump(updated_config, f, indent=2)
        elif config_file_path.endswith('.yaml') or config_file_path.endswith('.yml'):
            with open(config_file_path, 'w') as f:
                yaml.dump(updated_config, f, default_flow_style=False)
        else:
            # Default to JSON
            with open(config_file_path, 'w') as f:
                json.dump(updated_config, f, indent=2)
        
        LOGGER.info(f"Saved updated config to {config_file_path}")
        return True
        
    except Exception as e:
        LOGGER.error(f"Failed to save updated config: {str(e)}")
        return False


def notify_rotation_complete(config, rotated_secrets, failed_rotations, dry_run=False):
    """
    Send notification about completed secret rotation.

    Args:
        config (dict): Configuration containing notification settings
        rotated_secrets (list): List of successfully rotated secrets
        failed_rotations (list): List of secrets that failed rotation
        dry_run (bool): If True, only simulate the notification

    Returns:
        bool: Success status of notification
    """
    if dry_run:
        LOGGER.info("DRY RUN: Would send rotation completion notification")
        return True
    
    try:
        # Get notification configuration from config
        notification_config = config.get('notifications', {})
        recipients = notification_config.get('email_recipients', [])
        
        if not recipients:
            LOGGER.warning("No recipients configured for notification")
            return False
        
        # Create notification message
        success_count = len(rotated_secrets)
        failure_count = len(failed_rotations)
        total_count = success_count + failure_count
        
        subject = f"Secret Rotation Complete - {success_count}/{total_count} Successful"
        
        # Create HTML body
        body = f"""
        <h2>Secret Rotation Summary</h2>
        <p>Secret rotation completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h3>Summary</h3>
        <ul>
            <li><strong>Total Secrets:</strong> {total_count}</li>
            <li><strong>Successfully Rotated:</strong> {success_count}</li>
            <li><strong>Failed Rotations:</strong> {failure_count}</li>
        </ul>
        """
        
        # Add details for rotated secrets
        if rotated_secrets:
            body += "<h3>Successfully Rotated Secrets</h3><ul>"
            for secret_id in rotated_secrets:
                body += f"<li>{secret_id}</li>"
            body += "</ul>"
        
        # Add details for failed rotations
        if failed_rotations:
            body += "<h3>Failed Rotations</h3><ul>"
            for secret_id, error in failed_rotations.items():
                body += f"<li><strong>{secret_id}</strong>: {error}</li>"
            body += "</ul>"
        
        # Send notification email
        send_email_alert(
            subject=subject,
            body=body,
            to=recipients
        )
        
        LOGGER.info(f"Sent rotation completion notification to {', '.join(recipients)}")
        return True
        
    except Exception as e:
        LOGGER.error(f"Failed to send rotation notification: {str(e)}")
        return False


def update_airflow_database(config, rotated_secrets, dry_run=False):
    """
    Update Airflow metadata database with new secret values when applicable.

    Args:
        config (dict): Configuration containing database settings
        rotated_secrets (dict): Dict of rotated secrets with old and new values
        dry_run (bool): If True, only simulate the update

    Returns:
        bool: True if updated successfully, False otherwise
    """
    if dry_run:
        LOGGER.info("DRY RUN: Would update Airflow database with new secret values")
        return True
    
    try:
        # Get database configuration
        db_config = config.get('database', {})
        conn_id = db_config.get('connection_id')
        
        # Try to identify which secrets are used in Airflow connections
        connection_secrets = {}
        variable_secrets = {}
        
        # Check if the secrets follow naming conventions that indicate their purpose
        for secret_id, secret_data in rotated_secrets.items():
            if secret_data["status"] != "success":
                continue
                
            if secret_id.startswith('conn_'):
                # This is likely a connection secret
                connection_name = secret_id[5:]  # Remove 'conn_' prefix
                connection_secrets[connection_name] = secret_data
                
            elif secret_id.startswith('var_'):
                # This is likely a variable secret
                variable_name = secret_id[4:]  # Remove 'var_' prefix
                variable_secrets[variable_name] = secret_data
        
        # Update connections in the database
        if connection_secrets and not dry_run:
            LOGGER.info(f"Updating {len(connection_secrets)} connections in Airflow database")
            
            for conn_name, secret_data in connection_secrets.items():
                try:
                    # Check if connection exists
                    check_query = "SELECT count(1) FROM connection WHERE conn_id = %s"
                    result = execute_query(check_query, {'conn_id': conn_name}, conn_id, return_dict=True)
                    
                    if result and result[0]['count'] > 0:
                        # Get the current connection
                        conn_query = "SELECT * FROM connection WHERE conn_id = %s"
                        conn_result = execute_query(conn_query, {'conn_id': conn_name}, conn_id, return_dict=True)
                        
                        if conn_result:
                            # Update connection with new value
                            # Depends on how the connection is stored and what format the secret has
                            conn_data = conn_result[0]
                            
                            # Try to parse the new value as JSON
                            try:
                                new_conn_data = json.loads(secret_data["new_value"])
                                
                                # Construct update query based on new values
                                update_query = """
                                    UPDATE connection SET
                                    conn_type = %s,
                                    host = %s,
                                    schema = %s,
                                    login = %s,
                                    password = %s,
                                    port = %s,
                                    extra = %s
                                    WHERE conn_id = %s
                                """
                                
                                params = {
                                    'conn_type': new_conn_data.get('conn_type', conn_data.get('conn_type')),
                                    'host': new_conn_data.get('host', conn_data.get('host')),
                                    'schema': new_conn_data.get('schema', conn_data.get('schema')),
                                    'login': new_conn_data.get('login', conn_data.get('login')),
                                    'password': new_conn_data.get('password', conn_data.get('password')),
                                    'port': new_conn_data.get('port', conn_data.get('port')),
                                    'extra': json.dumps(new_conn_data.get('extra', {})),
                                    'conn_id': conn_name
                                }
                                
                                execute_query(update_query, params, conn_id, autocommit=True)
                                LOGGER.info(f"Updated connection {conn_name} in Airflow database")
                                
                            except (json.JSONDecodeError, KeyError) as e:
                                LOGGER.error(f"Failed to parse connection data for {conn_name}: {str(e)}")
                
                except Exception as e:
                    LOGGER.error(f"Failed to update connection {conn_name}: {str(e)}")
        
        # Update variables in the database
        if variable_secrets and not dry_run:
            LOGGER.info(f"Updating {len(variable_secrets)} variables in Airflow database")
            
            for var_name, secret_data in variable_secrets.items():
                try:
                    # Check if variable exists
                    check_query = "SELECT count(1) FROM variable WHERE key = %s"
                    result = execute_query(check_query, {'key': var_name}, conn_id, return_dict=True)
                    
                    if result and result[0]['count'] > 0:
                        # Update variable with new value
                        update_query = "UPDATE variable SET val = %s WHERE key = %s"
                        execute_query(update_query, {'val': secret_data["new_value"], 'key': var_name}, conn_id, autocommit=True)
                        LOGGER.info(f"Updated variable {var_name} in Airflow database")
                
                except Exception as e:
                    LOGGER.error(f"Failed to update variable {var_name}: {str(e)}")
        
        return True
        
    except Exception as e:
        LOGGER.error(f"Failed to update Airflow database: {str(e)}")
        return False


def main():
    """
    Main execution function for the secret rotation script.

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    # Parse command line arguments
    args = parse_args()
    
    # Set up logging
    log_level = 'DEBUG' if args.verbose else 'INFO'
    logger = setup_logging(log_level)
    
    try:
        # Load configuration for the specified environment
        config = load_config(args.env, args.config_path)
        
        # Get project ID from config
        project_id = config.get('project_id')
        if not project_id:
            logger.error("No project_id specified in config")
            return 1
        
        # Get configured rotation period
        rotation_period_days = config.get('rotation_period_days', ROTATION_FREQUENCY_DAYS)
        
        # Initialize Secret Manager client
        gcp_conn_id = config.get('gcp_connection_id')
        logger.info(f"Initializing Secret Manager client for project {project_id}")
        if gcp_conn_id:
            secret_client = SecretManagerClient(conn_id=gcp_conn_id).get_client()
        else:
            secret_client = SecretManagerServiceClient()
        
        # Get list of secrets to check
        secrets_to_check = []
        
        if args.secret_id:
            # Rotate specific secret
            secrets_to_check = [args.secret_id]
            logger.info(f"Will rotate specific secret: {args.secret_id}")
        else:
            # Get secrets from config
            managed_secrets = config.get('managed_secrets', [])
            secrets_to_check = managed_secrets
            logger.info(f"Found {len(secrets_to_check)} managed secrets in config")
        
        # Check which secrets need rotation
        secrets_to_rotate = []
        
        for secret_id in secrets_to_check:
            if args.force:
                # Force rotation regardless of age
                secrets_to_rotate.append(secret_id)
                logger.info(f"Forcing rotation of secret: {secret_id}")
            else:
                # Check if rotation is needed based on age
                if check_secret_rotation_needed(secret_id, secret_client, project_id, rotation_period_days):
                    secrets_to_rotate.append(secret_id)
                    logger.info(f"Secret {secret_id} needs rotation")
                else:
                    logger.info(f"Secret {secret_id} does not need rotation yet")
        
        if not secrets_to_rotate:
            logger.info("No secrets need rotation at this time")
            return 0
        
        # Perform rotation
        successful_rotations = {}
        failed_rotations = {}
        
        for secret_id in secrets_to_rotate:
            result = rotate_secret(secret_id, secret_client, project_id, args.dry_run)
            
            if result["status"] == "success":
                successful_rotations[secret_id] = result
                logger.info(f"Successfully rotated secret: {secret_id}")
            else:
                failed_rotations[secret_id] = result["error"]
                logger.error(f"Failed to rotate secret: {secret_id} - {result['error']}")
        
        # Update configurations with new secret values
        if successful_rotations:
            # Update connections
            connections_config = config.get('connections', {})
            if connections_config:
                updated_connections = update_connections_with_new_secrets(connections_config, successful_rotations)
                config['connections'] = updated_connections
            
            # Update variables
            variables_config = config.get('variables', {})
            if variables_config:
                updated_variables = update_variables_with_new_secrets(variables_config, successful_rotations)
                config['variables'] = updated_variables
            
            # Save updated config
            config_file_path = os.path.join(args.config_path, f'secrets_config_{args.env}.json')
            save_updated_config(config, config_file_path, args.dry_run)
            
            # Update Airflow database if needed
            update_airflow_database(config, successful_rotations, args.dry_run)
        
        # Send notification
        notify_rotation_complete(config, successful_rotations, failed_rotations, args.dry_run)
        
        # Log summary
        success_count = len(successful_rotations)
        failure_count = len(failed_rotations)
        logger.info(f"Secret rotation complete: {success_count} successful, {failure_count} failed")
        
        # Return appropriate exit code
        if failure_count > 0:
            return 1
        return 0
        
    except Exception as e:
        logger.error(f"Unexpected error during secret rotation: {str(e)}")
        logger.debug("Stack trace:", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())