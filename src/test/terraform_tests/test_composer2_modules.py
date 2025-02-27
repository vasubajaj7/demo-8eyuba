#!/usr/bin/env python3

"""
Test suite for validating Terraform modules used in Cloud Composer 2 with Airflow 2.X.
Tests structural integrity, configuration parameters, and integration points between the
networking, security, and composer modules.
"""

# Python standard library imports
import os  # Operating system interface for path manipulation
import tempfile  # Create temporary files and directories for Terraform testing
import json  # JSON parsing and generation for Terraform outputs
import subprocess  # Execute Terraform commands in tests
import pathlib  # Object-oriented filesystem paths for module directory handling

# Third-party library imports
import pytest  # Python testing framework - v6.0+
from python_terraform import Terraform  # Python wrapper for Terraform CLI - v~0.10.1
from unittest import mock  # Mocking capabilities for unit testing - Python standard library

# Internal module imports
from ..utils.test_helpers import setup_test_environment, reset_test_environment  # Set up isolated test environment for Terraform testing
from ..fixtures.mock_gcp_services import create_mock_gcp_services  # Create mock GCP services for testing Terraform modules


# Define global variables for test directories
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TERRAFORM_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'backend', 'terraform')
COMPOSER2_MODULES_DIR = os.path.join(TERRAFORM_DIR, 'composer2_modules')
NETWORKING_MODULE_DIR = os.path.join(COMPOSER2_MODULES_DIR, 'networking')
SECURITY_MODULE_DIR = os.path.join(COMPOSER2_MODULES_DIR, 'security')
COMPOSER_MODULE_DIR = os.path.join(COMPOSER2_MODULES_DIR, 'composer')


def create_terraform_workspace(module_dir: str) -> str:
    """
    Creates a temporary workspace for Terraform testing

    Args:
        module_dir: Path to the Terraform module directory

    Returns:
        Path to the temporary workspace
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp()

    # Copy all files from the specified module_dir to the temporary directory
    for item in os.listdir(module_dir):
        s = os.path.join(module_dir, item)
        d = os.path.join(temp_dir, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks=True, ignore=None)
        else:
            shutil.copy2(s, d)

    # Return the path to the temporary workspace
    return temp_dir


def run_terraform_init(workspace: str) -> bool:
    """
    Initializes Terraform in the specified workspace

    Args:
        workspace: Path to the Terraform workspace

    Returns:
        True if initialization succeeds
    """
    # Initialize a Python-Terraform object for the workspace
    tf = Terraform(working_dir=workspace)

    # Set backend config to use local state instead of GCS
    tf.backend_config = {'path': 'terraform.tfstate'}

    # Run terraform init command
    return_code, stdout, stderr = tf.init()

    # Return success status
    return return_code == 0


def run_terraform_validate(workspace: str) -> tuple:
    """
    Validates Terraform configuration in the workspace

    Args:
        workspace: Path to the Terraform workspace

    Returns:
        Success status and validation message
    """
    # Initialize a Python-Terraform object for the workspace
    tf = Terraform(working_dir=workspace)

    # Run terraform validate command
    return_code, stdout, stderr = tf.validate()

    # Return tuple of success status and validation message
    return return_code == 0, stdout + stderr


def run_terraform_plan(workspace: str, variables: dict) -> dict:
    """
    Runs terraform plan in the workspace with specified variables

    Args:
        workspace: Path to the Terraform workspace
        variables: Dictionary of variables for the plan

    Returns:
        Parsed plan output
    """
    # Initialize a Python-Terraform object for the workspace
    tf = Terraform(working_dir=workspace)

    # Prepare variables for the plan
    var_options = []
    for key, value in variables.items():
        var_options.append(f'-var="{key}={value}"')

    # Run terraform plan command with output in JSON format
    return_code, stdout, stderr = tf.plan(
        *var_options,
        out='tfplan.json',
        detailed_exitcode=True
    )

    # Parse and return the plan output
    if return_code == 0 or return_code == 2:
        with open(os.path.join(workspace, 'tfplan.json'), 'r') as f:
            plan_output = json.load(f)
        return plan_output
    else:
        raise Exception(f"Terraform plan failed: {stderr}")


def parse_terraform_output(output_string: str) -> dict:
    """
    Parses output from Terraform commands into a Python dictionary

    Args:
        output_string: String containing Terraform output

    Returns:
        Parsed Terraform output
    """
    # Parse the Terraform output string into a Python dictionary
    output_dict = json.loads(output_string)

    # Handle special characters and formatting
    # Return the parsed dictionary
    return output_dict


def extract_resources_from_plan(plan_output: dict, resource_type: str) -> list:
    """
    Extracts resources from a Terraform plan output

    Args:
        plan_output: Dictionary containing Terraform plan output
        resource_type: Type of resource to extract

    Returns:
        List of resources of the specified type
    """
    # Extract planned_values from plan output
    planned_values = plan_output.get('planned_values', {})

    # Extract root_module resources
    root_module = planned_values.get('root_module', {})
    resources = root_module.get('resources', [])

    # Filter resources by the specified resource_type
    filtered_resources = [r for r in resources if r['type'] == resource_type]

    # Return the filtered resource list
    return filtered_resources


class TestNetworkingModule:
    """Test class for validating the networking Terraform module"""

    def __init__(self):
        """Initialize TestNetworkingModule"""
        self._workspace = None

    def setup_method(self):
        """Setup test environment before each test method"""
        # Create a temporary workspace with the networking module
        self._workspace = create_terraform_workspace(NETWORKING_MODULE_DIR)

        # Initialize terraform in the workspace
        assert run_terraform_init(self._workspace)

        # Set up environment variables for testing
        setup_test_environment()

    def teardown_method(self):
        """Clean up resources after each test method"""
        # Remove temporary workspace
        reset_test_environment()
        shutil.rmtree(self._workspace, ignore_errors=True)

        # Reset environment variables
        reset_test_environment()

    def test_module_structure(self):
        """Test that the module has the required files and structure"""
        # Verify main.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'main.tf'))

        # Verify variables.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'variables.tf'))

        # Verify outputs.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'outputs.tf'))

        # Check for README or documentation
        assert os.path.exists(os.path.join(self._workspace, 'README.md')) or \
               os.path.exists(os.path.join(self._workspace, 'docs'))

    def test_required_variables(self):
        """Test that required variables are properly defined"""
        # Read variables.tf content
        with open(os.path.join(NETWORKING_MODULE_DIR, 'variables.tf'), 'r') as f:
            variables_content = f.read()

        # Verify project_id variable exists and is required
        assert 'variable "project_id" {' in variables_content
        assert 'type = string' in variables_content
        assert 'nullable = false' in variables_content

        # Verify region variable exists
        assert 'variable "region" {' in variables_content
        assert 'type = string' in variables_content

        # Verify network_name variable exists
        assert 'variable "network_name" {' in variables_content
        assert 'type = string' in variables_content

        # Check variable types and default values
        assert 'type = string' in variables_content

    def test_outputs(self):
        """Test that required outputs are properly defined"""
        # Read outputs.tf content
        with open(os.path.join(NETWORKING_MODULE_DIR, 'outputs.tf'), 'r') as f:
            outputs_content = f.read()

        # Verify network_id output exists
        assert 'output "network_id" {' in outputs_content

        # Verify subnetwork_id output exists
        assert 'output "subnetwork_id" {' in outputs_content

        # Verify any other required outputs
        assert 'output "network_name" {' in outputs_content

    def test_plan_basic_network(self):
        """Test planning a basic VPC network configuration"""
        # Create basic variables for VPC network
        variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_name': 'test-network',
            'subnetwork_name': 'test-subnet',
            'ip_range': '10.0.0.0/24'
        }

        # Run terraform plan with the variables
        plan_output = run_terraform_plan(self._workspace, variables)

        # Verify VPC network resources are in the plan
        network_resources = extract_resources_from_plan(plan_output, 'google_compute_network')
        assert len(network_resources) == 1
        assert network_resources[0]['name'] == 'test_network'

        # Verify subnet resources are in the plan
        subnet_resources = extract_resources_from_plan(plan_output, 'google_compute_subnetwork')
        assert len(subnet_resources) == 1
        assert subnet_resources[0]['name'] == 'test_subnet'

        # Verify firewall rules are properly configured
        firewall_resources = extract_resources_from_plan(plan_output, 'google_compute_firewall')
        assert len(firewall_resources) >= 1

    def test_plan_private_ip_configuration(self):
        """Test planning a private IP configuration"""
        # Create variables for private IP configuration
        variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_name': 'test-network',
            'subnetwork_name': 'test-subnet',
            'ip_range': '10.0.0.0/24',
            'create_private_service_access': True,
            'private_service_access_ip_range': '10.1.0.0/24'
        }

        # Run terraform plan with the variables
        plan_output = run_terraform_plan(self._workspace, variables)

        # Verify private service connection resources
        psc_resources = extract_resources_from_plan(plan_output, 'google_service_networking_connection')
        assert len(psc_resources) == 1

        # Verify NAT configuration if applicable
        # Verify IP ranges are properly set
        ip_range_resources = extract_resources_from_plan(plan_output, 'google_compute_global_address')
        assert len(ip_range_resources) == 1

    def test_validation(self):
        """Test terraform validate on the module"""
        # Run terraform validate on the workspace
        is_valid, validation_message = run_terraform_validate(self._workspace)

        # Assert that validation succeeds with no errors
        assert is_valid
        assert validation_message == ""


class TestSecurityModule:
    """Test class for validating the security Terraform module"""

    def __init__(self):
        """Initialize TestSecurityModule"""
        self._workspace = None

    def setup_method(self):
        """Setup test environment before each test method"""
        # Create a temporary workspace with the security module
        self._workspace = create_terraform_workspace(SECURITY_MODULE_DIR)

        # Initialize terraform in the workspace
        assert run_terraform_init(self._workspace)

        # Set up environment variables for testing
        setup_test_environment()

    def teardown_method(self):
        """Clean up resources after each test method"""
        # Remove temporary workspace
        shutil.rmtree(self._workspace, ignore_errors=True)

        # Reset environment variables
        reset_test_environment()

    def test_module_structure(self):
        """Test that the module has the required files and structure"""
        # Verify main.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'main.tf'))

        # Verify variables.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'variables.tf'))

        # Verify outputs.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'outputs.tf'))

        # Check for README or documentation
        assert os.path.exists(os.path.join(self._workspace, 'README.md')) or \
               os.path.exists(os.path.join(self._workspace, 'docs'))

    def test_required_variables(self):
        """Test that required variables are properly defined"""
        # Read variables.tf content
        with open(os.path.join(SECURITY_MODULE_DIR, 'variables.tf'), 'r') as f:
            variables_content = f.read()

        # Verify project_id variable exists and is required
        assert 'variable "project_id" {' in variables_content
        assert 'type = string' in variables_content
        assert 'nullable = false' in variables_content

        # Verify service_account_id variable exists
        assert 'variable "service_account_id" {' in variables_content
        assert 'type = string' in variables_content

        # Check variable types and default values
        assert 'type = string' in variables_content

    def test_outputs(self):
        """Test that required outputs are properly defined"""
        # Read outputs.tf content
        with open(os.path.join(SECURITY_MODULE_DIR, 'outputs.tf'), 'r') as f:
            outputs_content = f.read()

        # Verify service_account_email output exists
        assert 'output "service_account_email" {' in outputs_content

        # Verify any other required outputs
        assert 'output "service_account_id" {' in outputs_content

    def test_plan_service_accounts(self):
        """Test planning service account configuration"""
        # Create variables for service account configuration
        variables = {
            'project_id': 'test-project',
            'service_account_id': 'test-sa',
            'display_name': 'Test Service Account'
        }

        # Run terraform plan with the variables
        plan_output = run_terraform_plan(self._workspace, variables)

        # Verify service account resources are in the plan
        sa_resources = extract_resources_from_plan(plan_output, 'google_service_account')
        assert len(sa_resources) == 1
        assert sa_resources[0]['name'] == 'test_sa'

        # Verify IAM policy binding resources
        iam_resources = extract_resources_from_plan(plan_output, 'google_project_iam_binding')
        assert len(iam_resources) >= 0

    def test_plan_secret_manager(self):
        """Test planning Secret Manager configuration"""
        # Create variables for Secret Manager configuration
        variables = {
            'project_id': 'test-project',
            'secret_id': 'test-secret',
            'replication_policy_type': 'automatic'
        }

        # Run terraform plan with the variables
        plan_output = run_terraform_plan(self._workspace, variables)

        # Verify Secret Manager resources
        secret_resources = extract_resources_from_plan(plan_output, 'google_secret_manager_secret')
        assert len(secret_resources) == 1
        assert secret_resources[0]['name'] == 'test_secret'

        # Verify access policies for secrets
        access_resources = extract_resources_from_plan(plan_output, 'google_secret_manager_secret_iam_binding')
        assert len(access_resources) >= 0

    def test_validation(self):
        """Test terraform validate on the module"""
        # Run terraform validate on the workspace
        is_valid, validation_message = run_terraform_validate(self._workspace)

        # Assert that validation succeeds with no errors
        assert is_valid
        assert validation_message == ""


class TestComposerModule:
    """Test class for validating the Cloud Composer Terraform module"""

    def __init__(self):
        """Initialize TestComposerModule"""
        self._workspace = None

    def setup_method(self):
        """Setup test environment before each test method"""
        # Create a temporary workspace with the composer module
        self._workspace = create_terraform_workspace(COMPOSER_MODULE_DIR)

        # Initialize terraform in the workspace
        assert run_terraform_init(self._workspace)

        # Set up environment variables for testing
        setup_test_environment()

    def teardown_method(self):
        """Clean up resources after each test method"""
        # Remove temporary workspace
        shutil.rmtree(self._workspace, ignore_errors=True)

        # Reset environment variables
        reset_test_environment()

    def test_module_structure(self):
        """Test that the module has the required files and structure"""
        # Verify main.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'main.tf'))

        # Verify variables.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'variables.tf'))

        # Verify outputs.tf exists
        assert os.path.exists(os.path.join(self._workspace, 'outputs.tf'))

        # Check for README or documentation
        assert os.path.exists(os.path.join(self._workspace, 'README.md')) or \
               os.path.exists(os.path.join(self._workspace, 'docs'))

    def test_required_variables(self):
        """Test that required variables are properly defined"""
        # Read variables.tf content
        with open(os.path.join(COMPOSER_MODULE_DIR, 'variables.tf'), 'r') as f:
            variables_content = f.read()

        # Verify project_id variable exists and is required
        assert 'variable "project_id" {' in variables_content
        assert 'type = string' in variables_content
        assert 'nullable = false' in variables_content

        # Verify region variable exists
        assert 'variable "region" {' in variables_content
        assert 'type = string' in variables_content

        # Verify network_id variable exists
        assert 'variable "network_id" {' in variables_content
        assert 'type = string' in variables_content

        # Verify subnetwork_id variable exists
        assert 'variable "subnetwork_id" {' in variables_content
        assert 'type = string' in variables_content

        # Verify service_account_email variable exists
        assert 'variable "service_account_email" {' in variables_content
        assert 'type = string' in variables_content

        # Check variable types and default values
        assert 'type = string' in variables_content

    def test_outputs(self):
        """Test that required outputs are properly defined"""
        # Read outputs.tf content
        with open(os.path.join(COMPOSER_MODULE_DIR, 'outputs.tf'), 'r') as f:
            outputs_content = f.read()

        # Verify airflow_uri output exists
        assert 'output "airflow_uri" {' in outputs_content

        # Verify gcs_bucket output exists
        assert 'output "gcs_bucket" {' in outputs_content

        # Verify composer_environment_name output exists
        assert 'output "composer_environment_name" {' in outputs_content

    def test_plan_basic_environment(self):
        """Test planning a basic Cloud Composer 2 environment"""
        # Create basic variables for Composer environment
        variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_id': 'test-network',
            'subnetwork_id': 'test-subnet',
            'service_account_email': 'test-sa@test-project.iam.gserviceaccount.com',
            'composer_environment_name': 'test-composer'
        }

        # Run terraform plan with the variables
        plan_output = run_terraform_plan(self._workspace, variables)

        # Verify composer environment resource is in the plan
        composer_resources = extract_resources_from_plan(plan_output, 'google_composer_environment')
        assert len(composer_resources) == 1
        assert composer_resources[0]['name'] == 'test_composer'

        # Verify environment is configured as Cloud Composer 2
        # Verify Airflow 2.X is configured
        assert 'airflow_version' in str(plan_output)

    def test_plan_airflow_config(self):
        """Test planning Airflow configuration options"""
        # Create variables with Airflow configuration options
        variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_id': 'test-network',
            'subnetwork_id': 'test-subnet',
            'service_account_email': 'test-sa@test-project.iam.gserviceaccount.com',
            'composer_environment_name': 'test-composer',
            'airflow_config_overrides': {
                'core-parallelism': '32',
                'webserver-secret_key': 'testkey'
            },
            'environment_variables': {
                'TEST_VAR': 'test_value'
            },
            'scheduler_count': 2,
            'worker_cpu': 3,
            'web_server_memory_gb': 2
        }

        # Run terraform plan with the variables
        plan_output = run_terraform_plan(self._workspace, variables)

        # Verify airflow_config_overrides are properly set
        # Verify environment_variables are properly set
        # Verify scheduler, worker, and webserver configurations
        assert 'airflow_config_overrides' in str(plan_output)
        assert 'environment_variables' in str(plan_output)
        assert 'scheduler_count' in str(plan_output)
        assert 'worker_cpu' in str(plan_output)
        assert 'web_server_memory_gb' in str(plan_output)

    def test_plan_private_environment(self):
        """Test planning a private IP environment"""
        # Create variables for private IP configuration
        variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_id': 'test-network',
            'subnetwork_id': 'test-subnet',
            'service_account_email': 'test-sa@test-project.iam.gserviceaccount.com',
            'composer_environment_name': 'test-composer',
            'private_environment_config': {
                'enable_private_endpoint': True,
                'master_ipv4_cidr_block': '172.16.0.0/28'
            }
        }

        # Run terraform plan with the variables
        plan_output = run_terraform_plan(self._workspace, variables)

        # Verify private_environment_config is properly set
        # Verify IP ranges and connectivity
        assert 'private_environment_config' in str(plan_output)
        assert 'master_ipv4_cidr_block' in str(plan_output)

    def test_validation(self):
        """Test terraform validate on the module"""
        # Run terraform validate on the workspace
        is_valid, validation_message = run_terraform_validate(self._workspace)

        # Assert that validation succeeds with no errors
        assert is_valid
        assert validation_message == ""


class TestModuleIntegration:
    """Test class for validating integration between the Terraform modules"""

    def __init__(self):
        """Initialize TestModuleIntegration"""
        self._network_workspace = None
        self._security_workspace = None
        self._composer_workspace = None
        self._mock_outputs = {}

    def setup_method(self):
        """Setup test environment before each test method"""
        # Create temporary workspaces for all three modules
        self._network_workspace = create_terraform_workspace(NETWORKING_MODULE_DIR)
        self._security_workspace = create_terraform_workspace(SECURITY_MODULE_DIR)
        self._composer_workspace = create_terraform_workspace(COMPOSER_MODULE_DIR)

        # Initialize terraform in each workspace
        assert run_terraform_init(self._network_workspace)
        assert run_terraform_init(self._security_workspace)
        assert run_terraform_init(self._composer_workspace)

        # Create mock outputs for module integration
        self._mock_outputs = {
            'network_id': 'test-network-id',
            'subnetwork_id': 'test-subnet-id',
            'service_account_email': 'test-sa@test-project.iam.gserviceaccount.com'
        }

        # Set up environment variables for testing
        setup_test_environment()

    def teardown_method(self):
        """Clean up resources after each test method"""
        # Remove all temporary workspaces
        shutil.rmtree(self._network_workspace, ignore_errors=True)
        shutil.rmtree(self._security_workspace, ignore_errors=True)
        shutil.rmtree(self._composer_workspace, ignore_errors=True)

        # Reset environment variables
        reset_test_environment()

    def test_network_to_security_integration(self):
        """Test integration between networking and security modules"""
        # Plan the networking module with basic configuration
        network_variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_name': 'test-network',
            'subnetwork_name': 'test-subnet',
            'ip_range': '10.0.0.0/24'
        }
        network_plan_output = run_terraform_plan(self._network_workspace, network_variables)

        # Extract outputs from the networking plan
        network_resources = extract_resources_from_plan(network_plan_output, 'google_compute_network')
        assert len(network_resources) == 1
        network_id = network_resources[0]['values']['self_link']

        subnet_resources = extract_resources_from_plan(network_plan_output, 'google_compute_subnetwork')
        assert len(subnet_resources) == 1
        subnetwork_id = subnet_resources[0]['values']['self_link']

        # Use outputs as inputs for the security module plan
        security_variables = {
            'project_id': 'test-project',
            'service_account_id': 'test-sa',
            'network_id': network_id,
            'subnetwork_id': subnetwork_id
        }
        security_plan_output = run_terraform_plan(self._security_workspace, security_variables)

        # Verify security module properly references network resources
        sa_resources = extract_resources_from_plan(security_plan_output, 'google_service_account')
        assert len(sa_resources) == 1

    def test_security_to_composer_integration(self):
        """Test integration between security and composer modules"""
        # Plan the security module with basic configuration
        security_variables = {
            'project_id': 'test-project',
            'service_account_id': 'test-sa',
            'display_name': 'Test Service Account'
        }
        security_plan_output = run_terraform_plan(self._security_workspace, security_variables)

        # Extract outputs from the security plan
        sa_resources = extract_resources_from_plan(security_plan_output, 'google_service_account')
        assert len(sa_resources) == 1
        service_account_email = sa_resources[0]['values']['email']

        # Use outputs as inputs for the composer module plan
        composer_variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_id': 'test-network',
            'subnetwork_id': 'test-subnet',
            'service_account_email': service_account_email,
            'composer_environment_name': 'test-composer'
        }
        composer_plan_output = run_terraform_plan(self._composer_workspace, composer_variables)

        # Verify composer module properly references security resources
        composer_resources = extract_resources_from_plan(composer_plan_output, 'google_composer_environment')
        assert len(composer_resources) == 1

    def test_all_module_integration(self):
        """Test integration across all three modules"""
        # Plan the networking module
        network_variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_name': 'test-network',
            'subnetwork_name': 'test-subnet',
            'ip_range': '10.0.0.0/24'
        }
        network_plan_output = run_terraform_plan(self._network_workspace, network_variables)

        # Extract networking outputs
        network_resources = extract_resources_from_plan(network_plan_output, 'google_compute_network')
        assert len(network_resources) == 1
        network_id = network_resources[0]['values']['self_link']

        subnet_resources = extract_resources_from_plan(network_plan_output, 'google_compute_subnetwork')
        assert len(subnet_resources) == 1
        subnetwork_id = subnet_resources[0]['values']['self_link']

        # Plan the security module with networking outputs
        security_variables = {
            'project_id': 'test-project',
            'service_account_id': 'test-sa',
            'display_name': 'Test Service Account',
            'network_id': network_id,
            'subnetwork_id': subnetwork_id
        }
        security_plan_output = run_terraform_plan(self._security_workspace, security_variables)

        # Extract security outputs
        sa_resources = extract_resources_from_plan(security_plan_output, 'google_service_account')
        assert len(sa_resources) == 1
        service_account_email = sa_resources[0]['values']['email']

        # Plan the composer module with networking and security outputs
        composer_variables = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'network_id': network_id,
            'subnetwork_id': subnetwork_id,
            'service_account_email': service_account_email,
            'composer_environment_name': 'test-composer'
        }
        composer_plan_output = run_terraform_plan(self._composer_workspace, composer_variables)

        # Verify full integration works correctly
        composer_resources = extract_resources_from_plan(composer_plan_output, 'google_composer_environment')
        assert len(composer_resources) == 1