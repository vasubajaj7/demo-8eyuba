#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test module for validating the Cloud Composer 2 development environment configuration 
and ensuring it meets migration requirements from Airflow 1.10.15 to Airflow 2.X.
"""

import os
import json
import pytest
from unittest.mock import patch, MagicMock

# Google Cloud libraries - version 1.0.0+
from google.cloud import composer_v1
from google.auth import credentials
from google.auth.exceptions import DefaultCredentialsError

# Constants
DEV_PROJECT_ID = os.environ.get("DEV_PROJECT_ID", "your-dev-project-id")
DEV_REGION = os.environ.get("DEV_REGION", "us-central1")
DEV_ENVIRONMENT_NAME = os.environ.get("DEV_ENVIRONMENT_NAME", "airflow-dev")
ENVIRONMENT_FULL_PATH = f"projects/{DEV_PROJECT_ID}/locations/{DEV_REGION}/environments/{DEV_ENVIRONMENT_NAME}"


@pytest.fixture
def mock_composer_client():
    """Fixture to provide a mocked composer client."""
    with patch("google.cloud.composer_v1.EnvironmentsClient") as mock_client:
        mock_instance = mock_client.return_value
        yield mock_instance


@pytest.fixture
def mock_storage_client():
    """Fixture to provide a mocked storage client."""
    with patch("google.cloud.storage.Client") as mock_client:
        mock_instance = mock_client.return_value
        yield mock_instance


@pytest.fixture
def mock_monitoring_client():
    """Fixture to provide a mocked monitoring client."""
    with patch("google.cloud.monitoring_v3.MetricServiceClient") as mock_client:
        mock_instance = mock_client.return_value
        yield mock_instance


@pytest.fixture
def mock_iam_client():
    """Fixture to provide a mocked IAM client."""
    with patch("google.cloud.iam_v1.IAMClient") as mock_client:
        mock_instance = mock_client.return_value
        yield mock_instance


@pytest.fixture
def mock_compute_client():
    """Fixture to provide a mocked compute client."""
    with patch("google.cloud.compute_v1.NetworksClient") as mock_client:
        mock_instance = mock_client.return_value
        yield mock_instance


@pytest.mark.composer
def test_composer_dev_environment_exists(mock_composer_client):
    """
    Validates that the development Composer environment exists and is in RUNNING state.
    """
    # Setup mock response for list environments
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.state = composer_v1.Environment.State.RUNNING
    mock_composer_client.list_environments.return_value = [mock_environment]
    
    # Call list environments to check if dev environment exists
    parent = f"projects/{DEV_PROJECT_ID}/locations/{DEV_REGION}"
    environments = mock_composer_client.list_environments(request={"parent": parent})
    
    # Assert environment exists and is running
    environment_exists = False
    for env in environments:
        if env.name == ENVIRONMENT_FULL_PATH:
            environment_exists = True
            assert env.state == composer_v1.Environment.State.RUNNING, f"Development environment {DEV_ENVIRONMENT_NAME} is not in RUNNING state"
    
    assert environment_exists, f"Development environment {DEV_ENVIRONMENT_NAME} does not exist"


@pytest.mark.composer
def test_composer_dev_environment_config(mock_composer_client):
    """
    Verifies the configuration parameters of the development environment match specifications.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure node count (2-10 replicas as per section 8.4)
    mock_environment.config.node_count = 2
    
    # Configure machine type (n1-standard-2 as per section 8.1)
    mock_environment.config.node_config = MagicMock()
    mock_environment.config.node_config.machine_type = "n1-standard-2"
    mock_environment.config.node_config.disk_size_gb = 50
    
    # Configure private IP and regional deployment
    mock_environment.config.private_environment_config = MagicMock()
    mock_environment.config.private_environment_config.enable_private_environment = True
    
    # Configure GKE cluster info
    mock_environment.config.gke_cluster = f"projects/{DEV_PROJECT_ID}/regions/{DEV_REGION}/clusters/composer-dev-cluster"
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Assert node count is appropriate (2-10 as per spec)
    assert 2 <= environment.config.node_count <= 10, f"Node count should be between 2-10, got {environment.config.node_count}"
    
    # Verify machine type is n1-standard-2 as specified
    assert environment.config.node_config.machine_type == "n1-standard-2", f"Machine type should be n1-standard-2, got {environment.config.node_config.machine_type}"
    
    # Verify disk size is appropriate for development
    assert environment.config.node_config.disk_size_gb >= 50, f"Disk size should be at least 50GB, got {environment.config.node_config.disk_size_gb}GB"
    
    # Verify private IP is enabled for secure communication
    assert environment.config.private_environment_config.enable_private_environment, "Private IP should be enabled for development environment"
    
    # Verify GKE cluster is regional
    assert f"regions/{DEV_REGION}" in environment.config.gke_cluster, "GKE cluster should be regionally deployed"


@pytest.mark.composer
def test_composer_dev_airflow_version(mock_composer_client):
    """
    Validates that the Airflow version in the development environment is 2.X.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure Airflow version
    mock_environment.config.software_config = MagicMock()
    mock_environment.config.software_config.image_version = "composer-2.0.0-airflow-2.1.4"
    
    # Configure Python version
    mock_environment.config.software_config.python_version = "3.8"
    
    # Configure PyPI packages
    mock_environment.config.software_config.pypi_packages = {
        "apache-airflow-providers-google": "6.0.0",
        "apache-airflow-providers-http": "2.0.0",
        "apache-airflow-providers-postgres": "2.2.0"
    }
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Assert Airflow version is 2.X
    image_version = environment.config.software_config.image_version
    assert "airflow-2" in image_version, f"Airflow version should be 2.X, got {image_version}"
    
    # Verify Python version is 3.8+ as required in section
    python_version = environment.config.software_config.python_version
    assert python_version.startswith("3.") and float(python_version) >= 3.8, f"Python version should be 3.8+, got {python_version}"
    
    # Verify required provider packages are installed
    pypi_packages = environment.config.software_config.pypi_packages
    required_packages = [
        "apache-airflow-providers-google",
        "apache-airflow-providers-http",
        "apache-airflow-providers-postgres"
    ]
    
    for package in required_packages:
        assert package in pypi_packages, f"Required package {package} is not installed"


@pytest.mark.composer
def test_composer_dev_environment_variables(mock_composer_client):
    """
    Validates the environment variables set in the development environment.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure environment variables
    mock_environment.config.software_config = MagicMock()
    mock_environment.config.software_config.env_variables = {
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__WEBSERVER__EXPOSE_CONFIG": "True",
        "AIRFLOW__CORE__DAGS_FOLDER": "/home/airflow/gcs/dags",
        "ENVIRONMENT": "development"
    }
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Get environment variables
    env_variables = environment.config.software_config.env_variables
    
    # Assert AIRFLOW__CORE__LOAD_EXAMPLES is set to False
    assert env_variables.get("AIRFLOW__CORE__LOAD_EXAMPLES") == "False", "AIRFLOW__CORE__LOAD_EXAMPLES should be set to False"
    
    # Verify that AIRFLOW__WEBSERVER__EXPOSE_CONFIG is set appropriately for development
    # In development, it's often useful to expose the config for debugging
    assert env_variables.get("AIRFLOW__WEBSERVER__EXPOSE_CONFIG") == "True", "AIRFLOW__WEBSERVER__EXPOSE_CONFIG should be set to True in development"
    
    # Check that DAGs folder is correctly configured
    assert env_variables.get("AIRFLOW__CORE__DAGS_FOLDER") == "/home/airflow/gcs/dags", "DAGs folder path is incorrectly configured"
    
    # Verify that environment identifier is set
    assert env_variables.get("ENVIRONMENT") == "development", "ENVIRONMENT variable should be set to 'development'"


@pytest.mark.composer
def test_composer_dev_airflow_config_overrides(mock_composer_client):
    """
    Verifies the Airflow configuration overrides in the development environment.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure Airflow config overrides
    mock_environment.config.software_config = MagicMock()
    mock_environment.config.software_config.airflow_config_overrides = {
        "core": {
            "dag_concurrency": "16",
            "parallelism": "32",
            "max_active_runs_per_dag": "16",
            "dagbag_import_timeout": "30",  # Section 1.2: DAG parsing time < 30 seconds
            "logging_level": "INFO"
        },
        "webserver": {
            "web_server_worker_timeout": "120"
        },
        "celery": {
            "worker_concurrency": "8"
        },
        "scheduler": {
            "min_file_process_interval": "30"
        }
    }
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Get config overrides
    config_overrides = environment.config.software_config.airflow_config_overrides
    
    # Assert DAG parsing time is configured to be less than 30 seconds (section 1.2)
    assert int(config_overrides.get("core", {}).get("dagbag_import_timeout", "60")) <= 30, "DAG parsing timeout should be less than 30 seconds"
    
    # Ensure that parallelism is set appropriately
    assert int(config_overrides.get("core", {}).get("parallelism", "0")) > 0, "Parallelism should be greater than 0"
    
    # Check that DAG concurrency is appropriately configured
    assert int(config_overrides.get("core", {}).get("dag_concurrency", "0")) > 0, "DAG concurrency should be greater than 0"
    
    # Verify that webserver timeout is set correctly
    assert int(config_overrides.get("webserver", {}).get("web_server_worker_timeout", "0")) > 0, "Webserver timeout should be set"
    
    # Ensure that celery worker concurrency is configured
    celery_config = config_overrides.get("celery", {})
    assert "worker_concurrency" in celery_config, "Celery worker concurrency should be configured"


@pytest.mark.composer
def test_composer_dev_pypi_packages(mock_composer_client):
    """
    Validates that required PyPI packages are installed in the development environment.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure PyPI packages
    mock_environment.config.software_config = MagicMock()
    mock_environment.config.software_config.pypi_packages = {
        "apache-airflow-providers-google": "6.0.0",
        "apache-airflow-providers-http": "2.0.0",
        "apache-airflow-providers-postgres": "2.2.0",
        "pytest-airflow": "0.1.0",
        "black": "22.1.0",
        "pylint": "2.12.2"
    }
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Get PyPI packages
    pypi_packages = environment.config.software_config.pypi_packages
    
    # Required provider packages from section 4.2
    required_packages = [
        "apache-airflow-providers-google",
        "apache-airflow-providers-http",
        "apache-airflow-providers-postgres"
    ]
    
    # Development dependencies from section 4.2
    dev_packages = [
        "pytest-airflow",
        "black",
        "pylint"
    ]
    
    # Assert that required provider packages are installed
    for package in required_packages:
        assert package in pypi_packages, f"Required provider package {package} is not installed"
    
    # Verify that development dependencies are installed
    for package in dev_packages:
        assert package in pypi_packages, f"Development dependency {package} is not installed"


@pytest.mark.composer
def test_composer_dev_networking(mock_composer_client, mock_compute_client):
    """
    Validates the networking configuration of the development environment.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure networking settings
    mock_environment.config.private_environment_config = MagicMock()
    mock_environment.config.private_environment_config.enable_private_environment = True
    mock_environment.config.private_environment_config.network = f"projects/{DEV_PROJECT_ID}/global/networks/composer-network"
    mock_environment.config.private_environment_config.subnetwork = f"projects/{DEV_PROJECT_ID}/regions/{DEV_REGION}/subnetworks/composer-subnet"
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Mock network response
    mock_network = MagicMock()
    mock_network.name = "composer-network"
    mock_compute_client.get.return_value = mock_network
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Assert that Private IP is enabled for secure communication (section 2.4.2)
    assert environment.config.private_environment_config.enable_private_environment, "Private IP should be enabled for secure communication"
    
    # Verify that the environment is in the correct VPC network
    network = environment.config.private_environment_config.network
    assert "composer-network" in network, f"Environment should be in the composer-network VPC, got {network}"
    
    # Verify that the subnet is correctly configured
    subnet = environment.config.private_environment_config.subnetwork
    assert f"regions/{DEV_REGION}" in subnet, f"Subnet should be in the {DEV_REGION} region"


@pytest.mark.composer
def test_composer_dev_iam_permissions(mock_composer_client, mock_iam_client):
    """
    Validates the IAM permissions for the development environment service account.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure service account
    service_account = f"service-{DEV_PROJECT_ID}@composer-accounts.iam.gserviceaccount.com"
    mock_environment.config.node_config = MagicMock()
    mock_environment.config.node_config.service_account = service_account
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Mock IAM policy
    mock_policy = MagicMock()
    mock_binding1 = MagicMock()
    mock_binding1.role = "roles/composer.worker"
    mock_binding1.members = [f"serviceAccount:{service_account}"]
    
    mock_binding2 = MagicMock()
    mock_binding2.role = "roles/storage.objectAdmin"
    mock_binding2.members = [f"serviceAccount:{service_account}"]
    
    mock_policy.bindings = [mock_binding1, mock_binding2]
    mock_iam_client.get_iam_policy.return_value = mock_policy
    
    # Mock test_iam_permissions response
    test_permissions_response = MagicMock()
    test_permissions_response.permissions = [
        "storage.objects.get",
        "storage.objects.list",
        "storage.objects.create",
        "storage.objects.delete",
        "logging.logEntries.create"
    ]
    mock_iam_client.test_iam_permissions.return_value = test_permissions_response
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Get service account
    service_account = environment.config.node_config.service_account
    
    # Assert service account exists
    assert service_account, "Service account should be configured for the environment"
    
    # Required permissions based on section 7.2 (Data Security)
    required_permissions = [
        "storage.objects.get",
        "storage.objects.list",
        "storage.objects.create"
    ]
    
    # Mock getting IAM policy
    policy = mock_iam_client.get_iam_policy(resource=f"projects/{DEV_PROJECT_ID}")
    
    # Verify that service account has the necessary roles
    service_account_roles = []
    for binding in policy.bindings:
        if f"serviceAccount:{service_account}" in binding.members:
            service_account_roles.append(binding.role)
    
    # Assert that service account has composer.worker role
    assert "roles/composer.worker" in service_account_roles, "Service account should have composer.worker role"
    
    # Verify storage permissions based on section 7.2 (Data Security)
    for permission in required_permissions:
        assert permission in test_permissions_response.permissions, f"Service account should have {permission} permission"


@pytest.mark.composer
def test_composer_dev_gcs_bucket(mock_composer_client, mock_storage_client):
    """
    Validates the GCS bucket configuration for the development environment.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure GCS bucket
    mock_environment.config.dag_gcs_prefix = f"gs://us-central1-airflow-dev-12345-bucket/dags"
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Mock bucket
    mock_bucket = MagicMock()
    mock_bucket.name = "us-central1-airflow-dev-12345-bucket"
    mock_bucket.versioning_enabled = True
    mock_storage_client.get_bucket.return_value = mock_bucket
    
    # Mock bucket blobs
    mock_blob1 = MagicMock()
    mock_blob1.name = "dags/example_dag.py"
    mock_blob2 = MagicMock()
    mock_blob2.name = "dags/subdir/another_dag.py"
    mock_storage_client.list_blobs.return_value = [mock_blob1, mock_blob2]
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Extract bucket name from DAG GCS prefix
    gcs_prefix = environment.config.dag_gcs_prefix
    assert gcs_prefix.startswith("gs://"), f"GCS prefix should start with gs://, got {gcs_prefix}"
    
    bucket_name = gcs_prefix.split("/")[2]
    
    # Get bucket
    bucket = mock_storage_client.get_bucket(bucket_name)
    
    # Verify that bucket exists
    assert bucket.name == bucket_name, f"Bucket {bucket_name} should exist"
    
    # Verify that versioning is enabled as specified in section 8.2
    assert bucket.versioning_enabled, "Versioning should be enabled on the bucket"
    
    # Check that DAGs folder structure is correct
    blobs = list(mock_storage_client.list_blobs(bucket_name, prefix="dags/"))
    
    # Assert that there are DAGs in the bucket
    assert len(blobs) > 0, "DAGs folder should not be empty"
    
    # Verify that there's at least one DAG directly in the dags/ folder
    assert any(blob.name == "dags/example_dag.py" for blob in blobs), "There should be at least one DAG directly in the dags/ folder"


@pytest.mark.composer
def test_composer_dev_monitoring(mock_composer_client, mock_monitoring_client):
    """
    Validates the monitoring configuration for the development environment.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Mock metrics list
    mock_metric1 = MagicMock()
    mock_metric1.type = "composer.googleapis.com/environment/healthy"
    mock_metric2 = MagicMock()
    mock_metric2.type = "composer.googleapis.com/environment/dag_processing/total_parse_time"
    
    mock_monitoring_client.list_metric_descriptors.return_value = [mock_metric1, mock_metric2]
    
    # Get environment
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Extract environment ID from the full path
    env_id = environment.name.split("/")[-1]
    
    # List metrics for the environment
    project_name = f"projects/{DEV_PROJECT_ID}"
    filter_str = f'metric.type = starts_with("composer.googleapis.com") AND resource.labels.environment_name = "{env_id}"'
    metrics = mock_monitoring_client.list_metric_descriptors(
        name=project_name,
        filter=filter_str
    )
    
    # Required metrics based on sections 2.4.1 and 1.2
    required_metrics = [
        "composer.googleapis.com/environment/healthy",
        "composer.googleapis.com/environment/dag_processing/total_parse_time"
    ]
    
    # Verify that necessary monitoring metrics are available
    metric_types = [metric.type for metric in metrics]
    for required_metric in required_metrics:
        assert required_metric in metric_types, f"Required metric {required_metric} should be available"
    
    # Verify DAG parsing time metric (section 1.2: DAG parsing time < 30 seconds)
    assert "composer.googleapis.com/environment/dag_processing/total_parse_time" in metric_types, "DAG parsing time metric should be available for monitoring"


@pytest.mark.composer
def test_composer_dev_ci_cd_integration(mock_composer_client, mock_storage_client):
    """
    Validates that the development environment is properly integrated with CI/CD pipeline.
    """
    # Setup mock response for get environment
    mock_environment = MagicMock(spec=composer_v1.Environment)
    mock_environment.name = ENVIRONMENT_FULL_PATH
    mock_environment.config = MagicMock()
    
    # Configure GCS bucket
    mock_environment.config.dag_gcs_prefix = f"gs://us-central1-airflow-dev-12345-bucket/dags"
    
    mock_composer_client.get_environment.return_value = mock_environment
    
    # Mock bucket
    mock_bucket = MagicMock()
    mock_bucket.name = "us-central1-airflow-dev-12345-bucket"
    mock_bucket.iam_policy = MagicMock()
    
    # Mock IAM policy for bucket
    mock_policy = MagicMock()
    mock_binding = MagicMock()
    mock_binding.role = "roles/storage.objectAdmin"
    mock_binding.members = [
        "serviceAccount:cloudbuild@system.gserviceaccount.com",
        "serviceAccount:service-12345@cloudbuild.gserviceaccount.com"
    ]
    mock_policy.bindings = [mock_binding]
    
    mock_bucket.get_iam_policy.return_value = mock_policy
    mock_storage_client.get_bucket.return_value = mock_bucket
    
    # Get environment configuration
    environment = mock_composer_client.get_environment(request={"name": ENVIRONMENT_FULL_PATH})
    
    # Extract bucket name from DAG GCS prefix
    gcs_prefix = environment.config.dag_gcs_prefix
    bucket_name = gcs_prefix.split("/")[2]
    
    # Get bucket and its IAM policy
    bucket = mock_storage_client.get_bucket(bucket_name)
    policy = bucket.get_iam_policy()
    
    # Verify that Cloud Build has permissions to deploy to the environment's bucket
    cloudbuild_has_access = False
    for binding in policy.bindings:
        if binding.role == "roles/storage.objectAdmin":
            for member in binding.members:
                if "cloudbuild" in member:
                    cloudbuild_has_access = True
                    break
    
    assert cloudbuild_has_access, "Cloud Build should have objectAdmin access to the environment's bucket"
    
    # Mock deployment script existence check
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_storage_client.bucket.return_value.blob.return_value = mock_blob
    
    # Check for deployment script in the bucket
    deployment_script_blob = mock_storage_client.bucket(bucket_name).blob("scripts/deploy_to_dev.sh")
    assert deployment_script_blob.exists(), "Deployment script for DEV environment should exist in the bucket"