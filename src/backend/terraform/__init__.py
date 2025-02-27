"""
Terraform Infrastructure Management Module for Cloud Composer 2.

This module provides utility functions for managing Terraform configurations that provision
and maintain Cloud Composer 2 environments running Airflow 2.X. It enables programmatic
interaction with Terraform for infrastructure deployment across development, QA, and
production environments as part of the CI/CD pipeline.
"""

import os  # standard library
import logging  # standard library
import subprocess  # standard library
import json  # standard library
from typing import Dict, List, Optional, Any, Union, Tuple  # standard library

# Module version
VERSION = "0.1.0"

# Terraform directory paths
TERRAFORM_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODULES_DIR = os.path.join(TERRAFORM_BASE_DIR, 'composer2_modules')
ENV_DIRS = {
    'dev': os.path.join(TERRAFORM_BASE_DIR, 'composer2_dev'),
    'qa': os.path.join(TERRAFORM_BASE_DIR, 'composer2_qa'),
    'prod': os.path.join(TERRAFORM_BASE_DIR, 'composer2_prod')
}

# Configure logger
logger = logging.getLogger('terraform')


def get_module_path(module_name: str) -> str:
    """
    Returns the absolute path to a specified Terraform module.

    Args:
        module_name: Name of the Terraform module

    Returns:
        Full path to the specified module directory

    Raises:
        ValueError: If module_name is invalid or module directory doesn't exist
    """
    if not module_name or not isinstance(module_name, str):
        raise ValueError("Module name must be a non-empty string")

    module_path = os.path.join(MODULES_DIR, module_name)
    
    if not os.path.isdir(module_path):
        raise ValueError(f"Module '{module_name}' not found at {module_path}")
    
    return module_path


def get_environment_path(environment: str) -> str:
    """
    Returns the absolute path to a specified environment's Terraform configuration.

    Args:
        environment: The environment name ('dev', 'qa', or 'prod')

    Returns:
        Full path to the environment's Terraform directory

    Raises:
        ValueError: If environment is invalid or environment directory doesn't exist
    """
    if environment not in ENV_DIRS:
        raise ValueError(f"Environment must be one of: {list(ENV_DIRS.keys())}")
    
    env_path = ENV_DIRS[environment]
    
    if not os.path.isdir(env_path):
        raise ValueError(f"Environment directory not found at {env_path}")
    
    return env_path


def run_terraform_command(command: str, working_dir: str, env_vars: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Executes a Terraform command in the specified working directory.

    Args:
        command: Terraform command to execute
        working_dir: Directory where the command should be executed
        env_vars: Additional environment variables for the command

    Returns:
        Dictionary containing command result with stdout, stderr, and return code

    Raises:
        ValueError: If command or working_dir is invalid
        subprocess.SubprocessError: If command execution fails
    """
    if not command or not isinstance(command, str):
        raise ValueError("Command must be a non-empty string")
    
    if not working_dir or not os.path.isdir(working_dir):
        raise ValueError(f"Working directory not found: {working_dir}")
    
    # Prepare environment variables
    command_env = os.environ.copy()
    if env_vars:
        command_env.update(env_vars)
    
    logger.info(f"Running terraform command in {working_dir}: {command}")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=working_dir,
            env=command_env,
            capture_output=True,
            text=True,
            check=False  # Don't raise exception on non-zero return code
        )
        
        return {
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode,
            'success': result.returncode == 0
        }
    except subprocess.SubprocessError as e:
        logger.error(f"Error executing terraform command: {e}")
        raise


def terraform_init(working_dir: str, env_vars: Dict[str, str] = None) -> bool:
    """
    Initializes a Terraform working directory.

    Args:
        working_dir: Terraform configuration directory
        env_vars: Additional environment variables for the command

    Returns:
        True if initialization was successful, False otherwise
    """
    if not os.path.isdir(working_dir):
        raise ValueError(f"Working directory not found: {working_dir}")
    
    command = "terraform init"
    result = run_terraform_command(command, working_dir, env_vars)
    
    if result['success']:
        logger.info(f"Terraform initialization successful in {working_dir}")
    else:
        logger.error(f"Terraform initialization failed in {working_dir}. Error: {result['stderr']}")
    
    return result['success']


def terraform_plan(working_dir: str, out_file: str = None, variables: Dict[str, str] = None, 
                   env_vars: Dict[str, str] = None) -> bool:
    """
    Creates a Terraform execution plan.

    Args:
        working_dir: Terraform configuration directory
        out_file: Optional path to save the plan
        variables: Terraform variables to use
        env_vars: Additional environment variables for the command

    Returns:
        True if plan creation was successful, False otherwise
    """
    if not os.path.isdir(working_dir):
        raise ValueError(f"Working directory not found: {working_dir}")
    
    command = "terraform plan"
    
    # Add variables if provided
    if variables:
        for key, value in variables.items():
            command += f' -var="{key}={value}"'
    
    # Add output file if provided
    if out_file:
        command += f" -out={out_file}"
    
    result = run_terraform_command(command, working_dir, env_vars)
    
    if result['success']:
        logger.info(f"Terraform plan created successfully in {working_dir}")
        if out_file:
            logger.info(f"Plan saved to {out_file}")
    else:
        logger.error(f"Terraform plan failed in {working_dir}. Error: {result['stderr']}")
    
    return result['success']


def terraform_apply(working_dir: str, plan_file: str = None, auto_approve: bool = False,
                    env_vars: Dict[str, str] = None) -> bool:
    """
    Applies a Terraform execution plan.

    Args:
        working_dir: Terraform configuration directory
        plan_file: Optional path to a previously created plan file
        auto_approve: Whether to skip interactive approval
        env_vars: Additional environment variables for the command

    Returns:
        True if apply was successful, False otherwise
    """
    if not os.path.isdir(working_dir):
        raise ValueError(f"Working directory not found: {working_dir}")
    
    command = "terraform apply"
    
    # Use plan file if provided
    if plan_file:
        command += f" {plan_file}"
    
    # Add auto-approve if requested
    if auto_approve:
        command += " -auto-approve"
    
    result = run_terraform_command(command, working_dir, env_vars)
    
    if result['success']:
        logger.info(f"Terraform apply successful in {working_dir}")
    else:
        logger.error(f"Terraform apply failed in {working_dir}. Error: {result['stderr']}")
    
    return result['success']


def terraform_destroy(working_dir: str, auto_approve: bool = False,
                      env_vars: Dict[str, str] = None) -> bool:
    """
    Destroys Terraform-managed infrastructure.

    Args:
        working_dir: Terraform configuration directory
        auto_approve: Whether to skip interactive approval
        env_vars: Additional environment variables for the command

    Returns:
        True if destroy was successful, False otherwise
    """
    if not os.path.isdir(working_dir):
        raise ValueError(f"Working directory not found: {working_dir}")
    
    command = "terraform destroy"
    
    # Add auto-approve if requested
    if auto_approve:
        command += " -auto-approve"
    
    result = run_terraform_command(command, working_dir, env_vars)
    
    if result['success']:
        logger.info(f"Terraform destroy successful in {working_dir}")
    else:
        logger.error(f"Terraform destroy failed in {working_dir}. Error: {result['stderr']}")
    
    return result['success']


def terraform_output(working_dir: str, output_name: str = None) -> Union[Dict[str, Any], Any]:
    """
    Retrieves and parses the output from a Terraform configuration.

    Args:
        working_dir: Terraform configuration directory
        output_name: Optional name of a specific output to retrieve

    Returns:
        Parsed Terraform output data (dictionary or specific value)

    Raises:
        ValueError: If working directory is invalid
        json.JSONDecodeError: If output cannot be parsed as JSON
    """
    if not os.path.isdir(working_dir):
        raise ValueError(f"Working directory not found: {working_dir}")
    
    command = "terraform output -json"
    
    # Add specific output name if provided
    if output_name:
        command += f" {output_name}"
    
    result = run_terraform_command(command, working_dir)
    
    if not result['success']:
        logger.error(f"Failed to get Terraform outputs. Error: {result['stderr']}")
        return {} if output_name is None else None
    
    try:
        output_data = json.loads(result['stdout'])
        return output_data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Terraform output as JSON: {e}")
        raise


def terraform_validate(working_dir: str) -> bool:
    """
    Validates the Terraform configuration.

    Args:
        working_dir: Terraform configuration directory

    Returns:
        True if configuration is valid, False otherwise
    """
    if not os.path.isdir(working_dir):
        raise ValueError(f"Working directory not found: {working_dir}")
    
    command = "terraform validate -json"
    
    result = run_terraform_command(command, working_dir)
    
    try:
        validation_result = json.loads(result['stdout'])
        is_valid = validation_result.get('valid', False)
        
        if is_valid:
            logger.info(f"Terraform configuration in {working_dir} is valid")
        else:
            errors = validation_result.get('diagnostics', [])
            for error in errors:
                logger.error(f"Validation error: {error.get('summary', '')}")
        
        return is_valid
    except json.JSONDecodeError:
        logger.error("Failed to parse validation result as JSON")
        return False


def setup_environment(environment: str, variables: Dict[str, str] = None, apply: bool = False) -> bool:
    """
    Sets up a Terraform environment for Cloud Composer 2.

    Args:
        environment: Target environment ('dev', 'qa', or 'prod')
        variables: Terraform variables for the environment
        apply: Whether to apply the configuration or just plan

    Returns:
        True if setup was successful, False otherwise
    """
    try:
        env_path = get_environment_path(environment)
        
        logger.info(f"Setting up {environment} environment at {env_path}")
        
        # Initialize the configuration
        if not terraform_init(env_path):
            logger.error(f"Failed to initialize {environment} environment")
            return False
        
        # Create a plan
        plan_file = os.path.join(env_path, "terraform.tfplan")
        if not terraform_plan(env_path, plan_file, variables):
            logger.error(f"Failed to create plan for {environment} environment")
            return False
        
        # Apply if requested
        if apply:
            if not terraform_apply(env_path, plan_file, auto_approve=True):
                logger.error(f"Failed to apply {environment} environment")
                return False
            logger.info(f"Successfully applied {environment} environment")
        else:
            logger.info(f"Plan created for {environment} environment at {plan_file}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error setting up {environment} environment: {e}")
        return False


def get_composer_outputs(environment: str) -> Dict[str, Any]:
    """
    Retrieves Cloud Composer 2 outputs from Terraform state.

    Args:
        environment: Target environment ('dev', 'qa', or 'prod')

    Returns:
        Dictionary containing Composer environment outputs
    """
    try:
        env_path = get_environment_path(environment)
        
        # Get all outputs
        all_outputs = terraform_output(env_path)
        
        # Filter for Composer-related outputs
        composer_outputs = {}
        for key, value in all_outputs.items():
            if key.startswith('composer_') or 'composer' in key:
                # Terraform output format typically includes a 'value' field
                if isinstance(value, dict) and 'value' in value:
                    composer_outputs[key] = value['value']
                else:
                    composer_outputs[key] = value
        
        return composer_outputs
    
    except Exception as e:
        logger.error(f"Error retrieving Composer outputs for {environment}: {e}")
        return {}