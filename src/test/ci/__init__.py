#!/usr/bin/env python3

"""
Package initialization module for the CI testing suite that validates CI/CD pipeline components for the Airflow 1.10.15 to 2.X migration project.
Makes the CI test package importable and exposes key test classes and utilities for GitHub Actions workflows, Cloud Build configurations, and approval workflow validation.
"""

__all__ = ["TestGitHubActions", "TestWorkflowTemplates", "TestApprovalWorkflow", "TestApprovalIntegration", "load_workflow_file", "load_json_file", "load_approval_workflow_config"]

# Internal imports
from .test_github_actions import TestGitHubActions  # src/test/ci/test_github_actions.py - Import test class for GitHub Actions workflow validation
from .test_github_actions import TestWorkflowTemplates  # src/test/ci/test_github_actions.py - Import test class for workflow template validation
from .test_approval_workflow import TestApprovalWorkflow  # src/test/ci/test_approval_workflow.py - Import test class for approval workflow validation
from .test_approval_workflow import TestApprovalIntegration  # src/test/ci/test_approval_workflow.py - Import test class for approval workflow integration testing
from .test_github_actions import load_workflow_file  # src/test/ci/test_github_actions.py - Import utility function for loading workflow files
from .test_github_actions import load_json_file  # src/test/ci/test_github_actions.py - Import utility function for loading JSON configuration files
from .test_approval_workflow import load_approval_workflow_config  # src/test/ci/test_approval_workflow.py - Import utility function for loading approval workflow configuration

# Version identifier for the CI tests package
CI_TESTS_VERSION = "1.0.0"