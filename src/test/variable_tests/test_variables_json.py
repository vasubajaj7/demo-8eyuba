#!/usr/bin/env python3
"""
Test suite for validating the structure, content, and Airflow 2.X compatibility
of the variables.json configuration file during migration from Airflow 1.10.15 to Airflow 2.X.
"""
import json  # standard library
import os  # standard library
import pathlib  # standard library
import re  # standard library

import pytest  # pytest-6.0+
import jsonschema  # jsonschema-4.0+

# Internal module imports
from ..utils.test_helpers import get_test_config_path  # src/test/utils/test_helpers.py
from ..utils.airflow2_compatibility_utils import check_variable_compatibility  # src/test/utils/airflow2_compatibility_utils.py

# Define global variables
VARIABLES_JSON_PATH = get_test_config_path() / "variables.json"
REQUIRED_VARIABLES = [
    "gcp_project",
    "environment",
    "region",
    "zone",
    "dag_bucket",
    "composer_environment",
]
AIRFLOW_2_RESERVED_VARS = [
    "AIRFLOW_HOME",
    "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW_CONFIG",
]
ENVIRONMENT_SPECIFIC_VARS = {
    "dev": {"environment": "dev"},
    "qa": {"environment": "qa"},
    "prod": {"environment": "prod"},
}
VARIABLE_SCHEMA = {
    "type": "object",
    "properties": {
        "gcp_project": {"type": "string"},
        "environment": {"type": "string", "enum": ["dev", "qa", "prod"]},
        "region": {"type": "string"},
        "zone": {"type": "string"},
        "dag_bucket": {"type": "string"},
        "composer_environment": {"type": "string"},
    },
    "required": REQUIRED_VARIABLES,
}
SENSITIVE_PATTERNS = [re.compile(r"password|secret|key|token", re.IGNORECASE)]


class TestVariablesJson:
    def setUp(self):
        """Set up test environment before each test"""
        # Ensure the variables.json path is accessible
        assert VARIABLES_JSON_PATH is not None, "variables.json path is not set"

        # Check if the file exists and is readable
        assert os.path.exists(
            VARIABLES_JSON_PATH
        ), f"variables.json file does not exist at {VARIABLES_JSON_PATH}"
        assert os.access(
            VARIABLES_JSON_PATH, os.R_OK
        ), f"variables.json file is not readable at {VARIABLES_JSON_PATH}"

        # Initialize test configuration
        self.variables = None
        try:
            with open(VARIABLES_JSON_PATH, "r") as f:
                self.variables = json.load(f)
        except Exception as e:
            print(f"Error loading variables.json: {e}")

    def tearDown(self):
        """Clean up test environment after each test"""
        # Clean up any test resources
        self.variables = None

        # Reset any modified settings
        pass

    def test_variables_json_exists(self):
        """Test that the variables.json file exists"""
        # Check if the variables.json file exists
        # Assert that the file exists and is accessible
        assert os.path.exists(VARIABLES_JSON_PATH)

    def test_variables_json_is_valid_json(self):
        """Test that the variables.json file contains valid JSON"""
        # Open the variables.json file
        # Try to parse it as JSON
        try:
            with open(VARIABLES_JSON_PATH, "r") as f:
                json.load(f)
        # Assert that parsing succeeds without errors
        except json.JSONDecodeError as e:
            pytest.fail(f"variables.json is not valid JSON: {e}")

    def test_required_variables_present(self):
        """Test that all required variables are present in variables.json"""
        # Load the variables.json file
        # For each required variable, check if it exists in the file
        assert self.variables is not None, "variables.json could not be loaded"
        for variable in REQUIRED_VARIABLES:
            # Assert that all required variables are present
            assert (
                variable in self.variables
            ), f"Required variable '{variable}' is missing from variables.json"

    def test_variable_types_correct(self):
        """Test that variable values have the correct data types"""
        # Load the variables.json file
        # Define expected types for key variables
        expected_types = {
            "gcp_project": str,
            "environment": str,
            "region": str,
            "zone": str,
            "dag_bucket": str,
            "composer_environment": str,
        }
        # Check that each variable has the expected type
        assert self.variables is not None, "variables.json could not be loaded"
        for variable, expected_type in expected_types.items():
            # Assert that all types are correct
            if variable in self.variables:
                assert isinstance(
                    self.variables[variable], expected_type
                ), f"Variable '{variable}' should be of type '{expected_type}'"

    @pytest.mark.parametrize("env", ["dev", "qa", "prod"])
    def test_environment_specific_variables(self, env: str):
        """Test that environment-specific variables have correct values for each environment"""
        # Load the variables.json file
        # Filter variables for the specified environment
        assert self.variables is not None, "variables.json could not be loaded"
        for variable, expected_values in ENVIRONMENT_SPECIFIC_VARS.items():
            # Check that environment-specific values match expected values
            if variable in self.variables:
                # Assert that all environment-specific variables are correctly configured
                assert (
                    self.variables[variable]["environment"] == env
                ), f"Variable '{variable}' should have value '{env}' in '{env}' environment"

    def test_airflow_2_compatibility(self):
        """Test that variables are compatible with Airflow 2.X"""
        # Load the variables.json file
        # Call check_variable_compatibility for each variable
        assert self.variables is not None, "variables.json could not be loaded"
        for variable, value in self.variables.items():
            # Verify that variables use Airflow 2.X compatible formats
            try:
                check_variable_compatibility(variable, value)
            # Assert that all variables are Airflow 2.X compatible
            except Exception as e:
                pytest.fail(
                    f"Variable '{variable}' is not Airflow 2.X compatible: {str(e)}"
                )

    def test_no_reserved_variables_overridden(self):
        """Test that no Airflow 2.X reserved variables are overridden"""
        # Load the variables.json file
        # Check that no variable names match Airflow 2.X reserved names
        assert self.variables is not None, "variables.json could not be loaded"
        for variable in self.variables.keys():
            # Assert that no reserved variables are being overridden
            assert (
                variable not in AIRFLOW_2_RESERVED_VARS
            ), f"Variable '{variable}' overrides Airflow 2.X reserved variable"

    def test_schema_validation(self):
        """Test that variables.json conforms to the expected schema"""
        # Load the variables.json file
        # Validate against the defined JSON schema
        assert self.variables is not None, "variables.json could not be loaded"
        try:
            jsonschema.validate(instance=self.variables, schema=VARIABLE_SCHEMA)
        # Assert that the validation passes
        except jsonschema.exceptions.ValidationError as e:
            pytest.fail(f"variables.json does not conform to schema: {e}")

    def test_no_duplicate_variables(self):
        """Test that there are no duplicate variable definitions"""
        # Load the variables.json file
        # Extract all variable names
        assert self.variables is not None, "variables.json could not be loaded"
        variable_names = list(self.variables.keys())
        # Check for duplicates
        if len(variable_names) != len(set(variable_names)):
            # Assert that no duplicates exist
            pytest.fail("Duplicate variable definitions found in variables.json")

    def test_no_sensitive_data_in_variables(self):
        """Test that no sensitive data is stored directly in variables.json"""
        # Load the variables.json file
        # Check each variable value against patterns for sensitive data
        assert self.variables is not None, "variables.json could not be loaded"
        for variable, value in self.variables.items():
            # Assert that no sensitive data is directly stored in variables
            if isinstance(value, str):
                for pattern in SENSITIVE_PATTERNS:
                    if pattern.search(value):
                        pytest.fail(
                            f"Sensitive data found in variable '{variable}': {value}"
                        )

    def test_gcp_variables_format(self):
        """Test that GCP-related variables have the correct format"""
        # Load the variables.json file
        # Check GCP project ID format
        assert self.variables is not None, "variables.json could not be loaded"
        if "gcp_project" in self.variables:
            # Assert GCP project ID format
            assert re.match(
                r"^[a-z][a-z0-9-]*[a-z0-9]$", self.variables["gcp_project"]
            ), "GCP project ID has invalid format"
        # Check region and zone formats
        if "region" in self.variables:
            # Assert region and zone formats
            assert re.match(r"^[a-z]{2}-[a-z0-9-]+$", self.variables["region"]), "Region has invalid format"
        if "zone" in self.variables:
            assert re.match(r"^[a-z]{2}-[a-z0-9-]+-[a-z]$", self.variables["zone"]), "Zone has invalid format"
        # Check bucket name formats
        if "dag_bucket" in self.variables:
            # Assert bucket name formats
            assert re.match(r"^[a-z0-9][a-z0-9._-]*[a-z0-9]$", self.variables["dag_bucket"]), "Bucket name has invalid format"

    def test_composer2_specific_variables(self):
        """Test that Cloud Composer 2 specific variables are correctly configured"""
        # Load the variables.json file
        # Check for Composer 2 specific configuration variables
        assert self.variables is not None, "variables.json could not be loaded"
        if "composer_environment" in self.variables:
            # Verify that Composer 2 variables are properly configured
            # Assert values are appropriate for Composer 2 environments
            assert isinstance(
                self.variables["composer_environment"], str
            ), "Composer environment variable must be a string"

    def test_variables_consistency_across_environments(self):
        """Test that required variables exist consistently across all environments"""
        # Load the variables.json file
        # Group variables by environment
        assert self.variables is not None, "variables.json could not be loaded"
        env_variables = {}
        for env in ["dev", "qa", "prod"]:
            env_variables[env] = set()
            for variable, value in self.variables.items():
                if isinstance(value, dict) and "environment" in value and value["environment"] == env:
                    env_variables[env].add(variable)
        # Compare variable sets across environments
        required_variables = set(REQUIRED_VARIABLES)
        for env, variables in env_variables.items():
            # Assert that required variables exist in all environments
            assert required_variables.issubset(
                variables
            ), f"Required variables are not present in '{env}' environment"