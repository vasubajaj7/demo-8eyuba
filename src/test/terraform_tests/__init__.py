"""
Initialization module for Terraform test package that provides common utilities and functions for testing Terraform configurations used in the Cloud Composer 2 migration. Includes helper functions for creating test workspaces, running Terraform commands, and parsing outputs.
"""
import os  # Python standard library
import tempfile  # Python standard library
import shutil  # Python standard library
import subprocess  # Python standard library
import json  # Python standard library

# python-terraform version: ~0.10.1
import python_terraform

# Internal imports
from ..utils.test_helpers import reset_test_environment  # src/test/utils/test_helpers.py

# Define global variables for Terraform directories
TERRAFORM_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'backend', 'terraform')
COMPOSER2_MODULES_DIR = os.path.join(TERRAFORM_DIR, 'composer2_modules')
COMPOSER2_DEV_DIR = os.path.join(TERRAFORM_DIR, 'composer2_dev')
COMPOSER2_QA_DIR = os.path.join(TERRAFORM_DIR, 'composer2_qa')
COMPOSER2_PROD_DIR = os.path.join(TERRAFORM_DIR, 'composer2_prod')


def create_terraform_workspace(module_dir: str) -> str:
    """
    Creates a temporary workspace for Terraform testing by copying source files

    Args:
        module_dir: Path to the Terraform module directory

    Returns:
        Path to the created temporary workspace
    """
    # Create a temporary directory using tempfile.mkdtemp()
    temp_dir = tempfile.mkdtemp()
    # Copy all files from module_dir to the temporary directory
    shutil.copytree(module_dir, os.path.join(temp_dir, 'module'))
    # Return the path to the temporary workspace
    return temp_dir


def run_terraform_init(workspace: str) -> bool:
    """
    Initializes Terraform in the specified workspace

    Args:
        workspace: Path to the Terraform workspace

    Returns:
        True if initialization succeeds, False otherwise
    """
    # Create a python_terraform.Terraform object for the workspace
    tf = python_terraform.Terraform(working_dir=workspace)
    # Configure backend to use local state instead of remote
    # Run the terraform init command
    return_code, stdout, stderr = tf.init(capture_output=True)
    # Return True if successful, False otherwise
    return return_code == 0


def run_terraform_validate(workspace: str) -> tuple[bool, str]:
    """
    Validates Terraform configuration in the workspace

    Args:
        workspace: Path to the Terraform workspace

    Returns:
        (bool, str) indicating success status and validation message
    """
    # Create a python_terraform.Terraform object for the workspace
    tf = python_terraform.Terraform(working_dir=workspace)
    # Run the terraform validate command
    return_code, stdout, stderr = tf.validate(capture_output=True)
    # Return a tuple of (success_boolean, validation_message)
    return return_code == 0, stdout


def run_terraform_plan(workspace: str, variables: dict) -> dict:
    """
    Runs terraform plan in the workspace with specified variables

    Args:
        workspace: Path to the Terraform workspace
        variables: Dictionary of variables to pass to Terraform

    Returns:
        Parsed plan output as a Python dictionary
    """
    # Create a python_terraform.Terraform object for the workspace
    tf = python_terraform.Terraform(working_dir=workspace)
    # Format variables for terraform
    var_options = []
    for key, value in variables.items():
        var_options.append(f'-var={key}={value}')
    # Run terraform plan with output in JSON format
    return_code, stdout, stderr = tf.plan(*var_options, '-out=tfplan', capture_output=True)
    # Parse and return the plan output
    return parse_terraform_output(stdout)


def run_terraform_apply(workspace: str, variables: dict, auto_approve: bool) -> dict:
    """
    Applies Terraform configuration in the workspace with specified variables

    Args:
        workspace: Path to the Terraform workspace
        variables: Dictionary of variables to pass to Terraform
        auto_approve: Whether to automatically approve the apply

    Returns:
        Parsed apply output
    """
    # Create a python_terraform.Terraform object for the workspace
    tf = python_terraform.Terraform(working_dir=workspace)
    # Format variables for terraform
    var_options = []
    for key, value in variables.items():
        var_options.append(f'-var={key}={value}')
    # Set auto_approve to True by default
    approve_option = '-auto-approve' if auto_approve else ''
    # Run terraform apply command
    return_code, stdout, stderr = tf.apply(approve_option, *var_options, capture_output=True)
    # Parse and return the apply output
    return parse_terraform_output(stdout)


def parse_terraform_output(output_string: str) -> dict:
    """
    Parses Terraform command output into a Python dictionary

    Args:
        output_string: String containing Terraform command output

    Returns:
        Parsed Terraform output as a Python dictionary
    """
    # Check if output is JSON formatted
    try:
        # If JSON, parse using json.loads()
        return json.loads(output_string)
    except json.JSONDecodeError:
        # Otherwise, parse text output format into structured dictionary
        output_dict = {}
        lines = output_string.splitlines()
        for line in lines:
            if '=' in line:
                key, value = line.split('=', 1)
                output_dict[key.strip()] = value.strip()
        # Return the parsed dictionary
        return output_dict


def extract_resources_from_plan(plan_output: dict, resource_type: str) -> list:
    """
    Extracts specific resources from a Terraform plan output

    Args:
        plan_output: Parsed Terraform plan output
        resource_type: Type of resource to extract

    Returns:
        List of resources of the specified type
    """
    # Extract planned_values from the plan output
    planned_values = plan_output.get('planned_values', {})
    # Extract root_module and child_modules if present
    root_module = planned_values.get('root_module', {})
    child_modules = root_module.get('child_modules', [])
    resources = root_module.get('resources', [])

    for module in child_modules:
        module_resources = module.get('resources', [])
        if module_resources:
            resources.extend(module_resources)
    # Filter resources by the specified resource_type
    filtered_resources = [
        resource for resource in resources if resource['type'] == resource_type
    ]
    # Return the filtered resource list
    return filtered_resources


def setup_terraform_test_environment() -> dict:
    """
    Sets up a clean environment for Terraform tests

    Returns:
        Dictionary with original environment variables
    """
    # Store original environment variables
    original_env = os.environ.copy()
    # Set up mock GCP environment variables
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/mock/credentials.json'
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'mock-gcp-project'
    # Configure Terraform environment variables
    os.environ['TF_VAR_gcp_project_id'] = 'mock-gcp-project'
    # Return dictionary with original environment for restoration
    return original_env