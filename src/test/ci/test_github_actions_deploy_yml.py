#!/usr/bin/env python3

"""
Test module for validating the GitHub Actions deployment workflow file (.github/workflows/deploy.yml)
that handles multi-environment deployment to Cloud Composer 2 environments with appropriate approval
processes and validations. Ensures the workflow correctly implements the multi-stage deployment
process with proper environment promotion and governance controls.
"""

import unittest  # standard library
from pathlib import Path  # standard library

import pytest  # pytest-6.0+
import yaml  # pyyaml-5.0+

# Internal imports
from .test_github_actions import TestGitHubActions, load_workflow_file, compare_workflow_structure  # src/test/ci/test_github_actions.py
from ..utils.assertion_utils import assert_contains  # src/test/utils/assertion_utils.py

# Define global variables
DEPLOY_WORKFLOW_PATH = Path('.github/workflows/deploy.yml')
DEPLOY_WORKFLOW_TEMPLATE_PATH = Path('src/backend/ci-cd/github-actions-deploy.yml')
REQUIRED_ENVIRONMENTS = ['dev', 'qa', 'prod']
REQUIRED_JOBS = ['validate-deployment', 'deploy-to-dev', 'deploy-to-qa', 'deploy-to-prod']
REQUIRED_APPROVERS = {'dev': ['peer'], 'qa': ['peer', 'qa'], 'prod': ['peer', 'qa', 'cab', 'architect', 'stakeholder']}


def verify_environment_promotion_flow(workflow: dict) -> bool:
    """
    Verifies the environment promotion flow and dependencies in the workflow

    Args:
        workflow (dict): The workflow dictionary

    Returns:
        bool: True if environment promotion flow is correct, False otherwise
    """
    # Extract all jobs from the workflow
    jobs = workflow.get('jobs', {})

    # Verify 'validate-deployment' has no dependencies
    if 'needs' in jobs['validate-deployment']:
        print("'validate-deployment' should have no dependencies")
        return False

    # Verify 'deploy-to-dev' depends on 'validate-deployment'
    if jobs['deploy-to-dev']['needs'] != 'validate-deployment':
        print("'deploy-to-dev' should depend on 'validate-deployment'")
        return False

    # Verify 'deploy-to-qa' depends on 'deploy-to-dev'
    if jobs['deploy-to-qa']['needs'] != 'deploy-to-dev':
        print("'deploy-to-qa' should depend on 'deploy-to-dev'")
        return False

    # Verify 'deploy-to-prod' depends on 'deploy-to-qa'
    if jobs['deploy-to-prod']['needs'] != 'deploy-to-qa':
        print("'deploy-to-prod' should depend on 'deploy-to-qa'")
        return False

    # Return True if all dependencies are correct, False otherwise
    return True


def check_approval_requirements(workflow: dict) -> dict:
    """
    Checks that the workflow implements the correct approval requirements for each environment

    Args:
        workflow (dict): The workflow dictionary

    Returns:
        dict: Dictionary of approval requirements and their validation results
    """
    # Extract all jobs from the workflow
    jobs = workflow.get('jobs', {})
    approval_validation = {}

    # For each deployment job, check for environment field
    for env in REQUIRED_ENVIRONMENTS:
        job_id = f'deploy-to-{env}'
        if job_id not in jobs:
            print(f"Job '{job_id}' not found in workflow")
            approval_validation[env] = False
            continue

        job = jobs[job_id]
        env_config = job.get('environment', {})

        # Check for approval requirements in the environment configuration
        if 'required_approvers' not in env_config:
            print(f"Environment '{env}' is missing approval requirements")
            approval_validation[env] = False
            continue

        approvers = env_config['required_approvers']
        expected_approvers = REQUIRED_APPROVERS[env]

        # Verify dev has peer review requirement
        if env == 'dev' and approvers != expected_approvers:
            print(f"Dev environment should have peer review requirement")
            approval_validation[env] = False

        # Verify qa has peer and QA review requirements
        elif env == 'qa' and approvers != expected_approvers:
            print(f"QA environment should have peer and QA review requirements")
            approval_validation[env] = False

        # Verify prod has peer, QA, CAB, architect, and stakeholder approval requirements
        elif env == 'prod' and approvers != expected_approvers:
            print(f"Prod environment should have peer, QA, CAB, architect, and stakeholder approval requirements")
            approval_validation[env] = False
        else:
            approval_validation[env] = True

    # Return dictionary mapping environments to their approval validation status
    return approval_validation


def extract_deployment_scripts(workflow: dict) -> dict:
    """
    Extracts deployment script references from the workflow

    Args:
        workflow (dict): The workflow dictionary

    Returns:
        dict: Dictionary of environments and their deployment scripts
    """
    # Initialize empty result dictionary
    deployment_scripts = {}

    # Extract all steps from deployment jobs
    jobs = workflow.get('jobs', {})
    for env in REQUIRED_ENVIRONMENTS:
        job_id = f'deploy-to-{env}'
        if job_id not in jobs:
            print(f"Job '{job_id}' not found in workflow")
            deployment_scripts[env] = None
            continue

        job = jobs[job_id]
        steps = job.get('steps', [])
        script_path = None

        # Identify script execution steps (run: commands)
        for step in steps:
            if 'run' in step:
                run_command = step['run']
                if f'deploy-{env}.sh' in run_command:
                    script_path = run_command
                    break

        # Parse script references and map to environments
        deployment_scripts[env] = script_path

    # Return dictionary mapping environments to their deployment scripts
    return deployment_scripts


@pytest.mark.ci
class TestGitHubActionsDeployYML(TestGitHubActions):
    """
    Test class for validating the GitHub Actions deploy.yml workflow file
    """
    deploy_workflow = None
    template_workflow = None

    def __init__(self):
        """
        Initialize the test class
        """
        # Call parent class constructor using super().__init__()
        super().__init__()

    def setUp(self):
        """
        Set up test environment before each test method
        """
        # Load deploy workflow using load_workflow_file(DEPLOY_WORKFLOW_PATH)
        try:
            self.deploy_workflow = load_workflow_file(DEPLOY_WORKFLOW_PATH)
        except Exception as e:
            self.fail(f"Failed to load deploy workflow: {e}")

        # Load template workflow using load_workflow_file(DEPLOY_WORKFLOW_TEMPLATE_PATH)
        try:
            self.template_workflow = load_workflow_file(DEPLOY_WORKFLOW_TEMPLATE_PATH)
        except Exception as e:
            self.fail(f"Failed to load template workflow: {e}")

        # Verify both workflows loaded successfully
        self.assertIsNotNone(self.deploy_workflow, "Deploy workflow not loaded")
        self.assertIsNotNone(self.template_workflow, "Template workflow not loaded")

    def tearDown(self):
        """
        Clean up after each test method
        """
        # Reset workflow properties to None
        self.deploy_workflow = None
        self.template_workflow = None

    def test_deploy_workflow_exists(self):
        """
        Test that the deploy.yml workflow file exists
        """
        # Check if DEPLOY_WORKFLOW_PATH exists
        file_exists = DEPLOY_WORKFLOW_PATH.exists()

        # Assert that file exists with valid message
        self.assertTrue(file_exists, f"Workflow file does not exist: {DEPLOY_WORKFLOW_PATH}")

    def test_workflow_name(self):
        """
        Test that the workflow has the correct name
        """
        # Extract name from deploy_workflow
        name = self.deploy_workflow.get('name')

        # Assert name contains 'Deploy to Cloud Composer 2 Environments'
        self.assertIn("Deploy to Cloud Composer 2 Environments", name, "Workflow name is incorrect")

    def test_workflow_trigger_events(self):
        """
        Test that the workflow has the correct trigger events
        """
        # Extract 'on' section from deploy_workflow
        on = self.deploy_workflow.get('on', {})

        # Verify workflow_dispatch event is included
        self.assertIn('workflow_dispatch', on, "workflow_dispatch trigger is missing")

        # Verify push event with correct branch filters
        self.assertIn('push', on, "push trigger is missing")
        push_branches = on['push'].get('branches', [])
        self.assertIn('main', push_branches, "push trigger is missing 'main' branch filter")

        # Verify path filters include DAGs, plugins, and config folders
        paths = on['push'].get('paths', [])
        self.assertIn('dags/**', paths, "push trigger is missing 'dags/**' path filter")
        self.assertIn('plugins/**', paths, "push trigger is missing 'plugins/**' path filter")
        self.assertIn('config/**', paths, "push trigger is missing 'config/**' path filter")

        # Assert all required triggers are present
        pass

    def test_workflow_inputs(self):
        """
        Test that the workflow has the correct workflow_dispatch inputs
        """
        # Extract workflow_dispatch inputs
        inputs = self.deploy_workflow['on']['workflow_dispatch'].get('inputs', {})

        # Verify environment input with dev, qa, prod options
        environment_input = inputs.get('environment', {})
        self.assertEqual(environment_input.get('type'), 'choice', "Environment input type is incorrect")
        options = environment_input.get('options', [])
        self.assertIn('dev', options, "Environment input is missing 'dev' option")
        self.assertIn('qa', options, "Environment input is missing 'qa' option")
        self.assertIn('prod', options, "Environment input is missing 'prod' option")

        # Verify ref input for Git reference selection
        ref_input = inputs.get('ref', {})
        self.assertEqual(ref_input.get('type'), 'string', "Ref input type is incorrect")
        self.assertEqual(ref_input.get('description'), 'Git ref to deploy', "Ref input description is incorrect")
        self.assertEqual(ref_input.get('default'), 'main', "Ref input default is incorrect")

        # Assert input configurations are correct
        pass

    def test_required_jobs_present(self):
        """
        Test that all required jobs are present in the workflow
        """
        # Extract jobs from deploy_workflow
        jobs = self.deploy_workflow.get('jobs', {})

        # For each job in REQUIRED_JOBS, check if it exists in the workflow
        for job in REQUIRED_JOBS:
            self.assertIn(job, jobs, f"Deploy workflow is missing required job: {job}")

        # Assert all required jobs are present
        pass

    def test_environment_promotion_flow(self):
        """
        Test that jobs are sequenced correctly for environment promotion
        """
        # Call verify_environment_promotion_flow(deploy_workflow)
        is_valid = verify_environment_promotion_flow(self.deploy_workflow)

        # Assert function returns True
        self.assertTrue(is_valid, "Environment promotion flow is incorrect")

        # Verify job dependencies form correct promotion sequence
        pass

    def test_validation_job_configuration(self):
        """
        Test that the validation job is configured correctly
        """
        # Extract validate-deployment job from deploy_workflow
        job = self.deploy_workflow['jobs']['validate-deployment']

        # Verify runs-on is 'ubuntu-latest'
        self.assertEqual(job['runs-on'], 'ubuntu-latest', "Validation job runs-on is incorrect")

        # Check for required steps: checkout, setup-python, install dependencies
        steps = [step['name'] for step in job['steps']]
        self.assertIn('Checkout code', steps, "Validation job is missing 'Checkout code' step")
        self.assertIn('Set up Python', steps, "Validation job is missing 'Set up Python' step")
        self.assertIn('Install dependencies', steps, "Validation job is missing 'Install dependencies' step")

        # Verify DAG validation script is called correctly
        self.assertIn('Validate DAGs', steps, "Validation job is missing 'Validate DAGs' step")

        # Verify compatibility report generation
        self.assertIn('Generate compatibility report', steps, "Validation job is missing 'Generate compatibility report' step")

        # Verify artifact upload for validation reports
        self.assertIn('Upload validation reports', steps, "Validation job is missing 'Upload validation reports' step")

        # Assert all required configurations are present
        pass

    def test_dev_deployment_job_configuration(self):
        """
        Test that the dev deployment job is configured correctly
        """
        # Extract deploy-to-dev job from deploy_workflow
        job = self.deploy_workflow['jobs']['deploy-to-dev']

        # Verify needs dependency on validate-deployment job
        self.assertEqual(job['needs'], 'validate-deployment', "Dev deployment job needs is incorrect")

        # Verify environment configuration with required approvals
        self.assertEqual(job['environment']['name'], 'dev', "Dev deployment job environment name is incorrect")
        self.assertEqual(job['environment']['required_approvers'], ['peer'], "Dev deployment job required_approvers is incorrect")

        # Verify runs-on is 'ubuntu-latest'
        self.assertEqual(job['runs-on'], 'ubuntu-latest', "Dev deployment job runs-on is incorrect")

        # Check for required steps: checkout, setup-python, setup Google Cloud SDK
        steps = [step['name'] for step in job['steps']]
        self.assertIn('Checkout code', steps, "Dev deployment job is missing 'Checkout code' step")
        self.assertIn('Set up Python', steps, "Dev deployment job is missing 'Set up Python' step")
        self.assertIn('Set up Google Cloud SDK', steps, "Dev deployment job is missing 'Set up Google Cloud SDK' step")

        # Verify Google Cloud authentication
        self.assertIn('Authenticate to Google Cloud', steps, "Dev deployment job is missing 'Authenticate to Google Cloud' step")

        # Verify execution of deploy-dev.sh script
        self.assertIn('Deploy to dev environment', steps, "Dev deployment job is missing 'Deploy to dev environment' step")

        # Verify artifact upload for deployment reports
        self.assertIn('Upload deployment reports', steps, "Dev deployment job is missing 'Upload deployment reports' step")

        # Assert all required configurations are present
        pass

    def test_qa_deployment_job_configuration(self):
        """
        Test that the qa deployment job is configured correctly
        """
        # Extract deploy-to-qa job from deploy_workflow
        job = self.deploy_workflow['jobs']['deploy-to-qa']

        # Verify needs dependency on deploy-to-dev job
        self.assertEqual(job['needs'], 'deploy-to-dev', "QA deployment job needs is incorrect")

        # Verify environment configuration with required approvals
        self.assertEqual(job['environment']['name'], 'qa', "QA deployment job environment name is incorrect")
        self.assertEqual(job['environment']['required_approvers'], ['peer', 'qa'], "QA deployment job required_approvers is incorrect")

        # Verify runs-on is 'ubuntu-latest'
        self.assertEqual(job['runs-on'], 'ubuntu-latest', "QA deployment job runs-on is incorrect")

        # Check for required steps: checkout, setup-python, setup Google Cloud SDK
        steps = [step['name'] for step in job['steps']]
        self.assertIn('Checkout code', steps, "QA deployment job is missing 'Checkout code' step")
        self.assertIn('Set up Python', steps, "QA deployment job is missing 'Set up Python' step")
        self.assertIn('Set up Google Cloud SDK', steps, "QA deployment job is missing 'Set up Google Cloud SDK' step")

        # Verify Google Cloud authentication
        self.assertIn('Authenticate to Google Cloud', steps, "QA deployment job is missing 'Authenticate to Google Cloud' step")

        # Verify execution of deploy-qa.sh script
        self.assertIn('Deploy to qa environment', steps, "QA deployment job is missing 'Deploy to qa environment' step")

        # Verify additional validation steps specific to QA
        # (e.g., integration tests, smoke tests)
        # self.assertIn('Run integration tests', steps, "QA deployment job is missing 'Run integration tests' step")

        # Verify artifact upload for deployment reports
        self.assertIn('Upload deployment reports', steps, "QA deployment job is missing 'Upload deployment reports' step")

        # Assert all required configurations are present
        pass

    def test_prod_deployment_job_configuration(self):
        """
        Test that the prod deployment job is configured correctly
        """
        # Extract deploy-to-prod job from deploy_workflow
        job = self.deploy_workflow['jobs']['deploy-to-prod']

        # Verify needs dependency on deploy-to-qa job
        self.assertEqual(job['needs'], 'deploy-to-qa', "Prod deployment job needs is incorrect")

        # Verify environment configuration with required approvals
        self.assertEqual(job['environment']['name'], 'prod', "Prod deployment job environment name is incorrect")
        self.assertEqual(job['environment']['required_approvers'], ['peer', 'qa', 'cab', 'architect', 'stakeholder'], "Prod deployment job required_approvers is incorrect")

        # Verify runs-on is 'ubuntu-latest'
        self.assertEqual(job['runs-on'], 'ubuntu-latest', "Prod deployment job runs-on is incorrect")

        # Check for required steps: checkout, setup-python, setup Google Cloud SDK
        steps = [step['name'] for step in job['steps']]
        self.assertIn('Checkout code', steps, "Prod deployment job is missing 'Checkout code' step")
        self.assertIn('Set up Python', steps, "Prod deployment job is missing 'Set up Python' step")
        self.assertIn('Set up Google Cloud SDK', steps, "Prod deployment job is missing 'Set up Google Cloud SDK' step")

        # Verify Google Cloud authentication
        self.assertIn('Authenticate to Google Cloud', steps, "Prod deployment job is missing 'Authenticate to Google Cloud' step")

        # Verify execution of deploy-prod.sh script
        self.assertIn('Deploy to prod environment', steps, "Prod deployment job is missing 'Deploy to prod environment' step")

        # Verify additional validation and verification steps for production
        # (e.g., smoke tests, data validation)
        # self.assertIn('Run smoke tests', steps, "Prod deployment job is missing 'Run smoke tests' step")

        # Verify deployment notification steps
        # self.assertIn('Send deployment notification', steps, "Prod deployment job is missing 'Send deployment notification' step")

        # Verify artifact upload for deployment reports
        self.assertIn('Upload deployment reports', steps, "Prod deployment job is missing 'Upload deployment reports' step")

        # Assert all required configurations are present
        pass

    def test_approval_requirements(self):
        """
        Test that the workflow implements the correct approval requirements for each environment
        """
        # Call check_approval_requirements(deploy_workflow)
        approval_validation = check_approval_requirements(self.deploy_workflow)

        # Verify result contains all required environments
        self.assertEqual(set(approval_validation.keys()), set(REQUIRED_ENVIRONMENTS), "Approval validation is missing environments")

        # Verify all environments have correct approval requirements
        for env in REQUIRED_ENVIRONMENTS:
            self.assertTrue(approval_validation[env], f"Approval requirements are incorrect for environment {env}")

        # Assert all approval requirements are correctly implemented
        pass

    def test_deployment_scripts(self):
        """
        Test that the workflow references the correct deployment scripts
        """
        # Call extract_deployment_scripts(deploy_workflow)
        deployment_scripts = extract_deployment_scripts(self.deploy_workflow)

        # Verify dev environment uses deploy-dev.sh
        self.assertIn('deploy-dev.sh', deployment_scripts['dev'], "Dev environment uses incorrect deployment script")

        # Verify qa environment uses deploy-qa.sh
        self.assertIn('deploy-qa.sh', deployment_scripts['qa'], "QA environment uses incorrect deployment script")

        # Verify prod environment uses deploy-prod.sh
        self.assertIn('deploy-prod.sh', deployment_scripts['prod'], "Prod environment uses incorrect deployment script")

        # Assert all deployment scripts are correctly referenced
        pass

    def test_github_actions_versions(self):
        """
        Test that the workflow uses appropriate GitHub Action versions
        """
        # Extract all 'uses' directives from workflow steps
        uses_directives = []
        for job_name, job in self.deploy_workflow['jobs'].items():
            for step in job.get('steps', []):
                if 'uses' in step:
                    uses_directives.append(step['uses'])

        # Verify checkout action uses v3
        checkout_actions = [action for action in uses_directives if 'actions/checkout' in action]
        self.assertTrue(all('v3' in action for action in checkout_actions), "Checkout action uses incorrect version")

        # Verify setup-python action uses v4
        setup_python_actions = [action for action in uses_directives if 'actions/setup-python' in action]
        self.assertTrue(all('v4' in action for action in setup_python_actions), "Setup-python action uses incorrect version")

        # Verify google-github-actions/auth uses v1
        google_auth_actions = [action for action in uses_directives if 'google-github-actions/auth' in action]
        self.assertTrue(all('v1' in action for action in google_auth_actions), "google-github-actions/auth action uses incorrect version")

        # Verify google-github-actions/setup-gcloud uses v1
        google_setup_gcloud_actions = [action for action in uses_directives if 'google-github-actions/setup-gcloud' in action]
        self.assertTrue(all('v1' in action for action in google_setup_gcloud_actions), "google-github-actions/setup-gcloud action uses incorrect version")

        # Verify cache action uses v3
        cache_actions = [action for action in uses_directives if 'actions/cache' in action]
        self.assertTrue(all('v3' in action for action in cache_actions), "Cache action uses incorrect version")

        # Verify upload-artifact action uses v3
        upload_artifact_actions = [action for action in uses_directives if 'actions/upload-artifact' in action]
        self.assertTrue(all('v3' in action for action in upload_artifact_actions), "Upload-artifact action uses incorrect version")

        # Assert all actions use appropriate versions
        pass

    def test_environment_variables(self):
        """
        Test that the workflow sets the required environment variables
        """
        # Extract env section from deploy_workflow
        env = self.deploy_workflow['jobs']['deploy-to-dev'].get('env', {})

        # Verify AIRFLOW_HOME is set correctly
        self.assertEqual(env.get('AIRFLOW_HOME'), '/opt/airflow', "AIRFLOW_HOME is not set correctly")

        # Verify AIRFLOW_VERSION is set to 2.0.0+
        self.assertEqual(env.get('AIRFLOW_VERSION'), '2.0.0+', "AIRFLOW_VERSION is not set correctly")

        # Verify PYTHONPATH is set correctly
        self.assertEqual(env.get('PYTHONPATH'), '${AIRFLOW_HOME}/dags:${AIRFLOW_HOME}/plugins', "PYTHONPATH is not set correctly")

        # Assert all required environment variables are present
        pass

    def test_permissions(self):
        """
        Test that the workflow has the correct GitHub permissions
        """
        # Extract permissions section from deploy_workflow
        permissions = self.deploy_workflow.get('permissions', {})

        # Verify contents permission is read
        self.assertEqual(permissions.get('contents'), 'read', "contents permission is incorrect")

        # Verify id-token permission is write
        self.assertEqual(permissions.get('id-token'), 'write', "id-token permission is incorrect")

        # Verify deployments permission is write
        self.assertEqual(permissions.get('deployments'), 'write', "deployments permission is incorrect")

        # Verify environments permission is write
        self.assertEqual(permissions.get('environments'), 'write', "environments permission is incorrect")

        # Assert all required permissions are present with correct access levels
        pass

    def test_concurrency_configuration(self):
        """
        Test that the workflow has correct concurrency configuration
        """
        # Extract concurrency section from deploy_workflow
        concurrency = self.deploy_workflow.get('concurrency', {})

        # Verify group setting uses environment variable
        self.assertEqual(concurrency.get('group'), '${{ github.workflow }}-${{ github.event.inputs.environment }}', "Concurrency group is incorrect")

        # Verify cancel-in-progress is false
        self.assertEqual(concurrency.get('cancel-in-progress'), False, "Concurrency cancel-in-progress is incorrect")

        # Assert concurrency configuration is correct
        pass

    def test_artifact_retention(self):
        """
        Test that workflow artifacts have appropriate retention settings
        """
        # Extract all upload-artifact steps
        upload_artifact_steps = []
        for job_name, job in self.deploy_workflow['jobs'].items():
            for step in job.get('steps', []):
                if step.get('uses') == 'actions/upload-artifact@v3':
                    upload_artifact_steps.append(step)

        # Verify retention-days setting is present
        self.assertTrue(all('retention-days' in step for step in upload_artifact_steps), "Artifact retention-days is missing")

        # Verify retention period is appropriate
        self.assertTrue(all(step['retention-days'] == 30 for step in upload_artifact_steps), "Artifact retention period is incorrect")

        # Assert artifact retention is correctly configured
        pass

    def test_template_consistency(self):
        """
        Test that the deploy workflow is consistent with its template
        """
        # Call compare_workflow_structure(deploy_workflow, template_workflow)
        differences = compare_workflow_structure(self.deploy_workflow, self.template_workflow)

        # Verify minimal differences (expected customizations only)
        # (e.g., environment-specific variables, approval requirements)
        self.assertEqual(differences, {}, f"Workflow structure is inconsistent with template: {differences}")

        # Assert workflows have consistent structure
        pass