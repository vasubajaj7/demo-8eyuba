#!/bin/bash
#
# deploy-dev.sh
# Description: Deployment script for Cloud Composer 2 development environment
# Version: 1.0.0
# Author: Cloud Composer Migration Team
#
# This script deploys Apache Airflow DAGs and configurations to the Cloud Composer 2
# development environment as part of the CI/CD pipeline.

# Exit on any error
set -e

# Global variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ID=""
ENVIRONMENT="dev"
COMPOSER_ENV=""
COMPOSER_REGION=""
GCS_BUCKET=""
DAG_SOURCE_DIR=""
CONFIG_DIR=""
VARIABLES_FILE=""
CONNECTIONS_FILE=""
LOG_FILE=""
VALIDATION_THRESHOLD=20  # Higher threshold for development than QA/PROD

# Exit codes
EXIT_SUCCESS=0
EXIT_FAILURE=1

print_header() {
    echo "============================================================"
    echo "Apache Airflow DAG Deployment Script for Development Environment"
    echo "Version: 1.0.0"
    echo "Environment: Development (dev)"
    echo "Date: $(date)"
    echo "============================================================"
}

setup_environment() {
    echo "Setting up environment variables..."
    
    # Set SCRIPT_DIR to the directory containing this script
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    
    # Set environment to dev
    ENVIRONMENT="dev"
    
    # Load development environment configuration
    if [ -f "${SCRIPT_DIR}/../config/composer_dev.py" ]; then
        # Extract configuration variables from Python config
        PROJECT_ID=$(python3 -c "import sys; sys.path.append('${SCRIPT_DIR}/../config'); import composer_dev; print(composer_dev.config['gcp']['project_id'])")
        COMPOSER_ENV=$(python3 -c "import sys; sys.path.append('${SCRIPT_DIR}/../config'); import composer_dev; print(composer_dev.config['composer']['environment_name'])")
        COMPOSER_REGION=$(python3 -c "import sys; sys.path.append('${SCRIPT_DIR}/../config'); import composer_dev; print(composer_dev.config['gcp']['region'])")
    else
        echo "Error: Could not find configuration file at ${SCRIPT_DIR}/../config/composer_dev.py"
        return $EXIT_FAILURE
    fi
    
    # Set paths
    DAG_SOURCE_DIR="${SCRIPT_DIR}/../../dags"
    CONFIG_DIR="${SCRIPT_DIR}/../config"
    VARIABLES_FILE="${CONFIG_DIR}/variables_dev.json"
    CONNECTIONS_FILE="${CONFIG_DIR}/connections_dev.json"
    LOG_FILE="/tmp/airflow_deploy_dev_$(date +%Y%m%d_%H%M%S).log"
    
    # Set validation threshold for development environment
    # For dev we allow more warnings than QA/PROD since it's a development environment
    VALIDATION_THRESHOLD=20
    
    # Export variables for child processes
    export PROJECT_ID
    export ENVIRONMENT
    export COMPOSER_ENV
    export COMPOSER_REGION
    
    echo "Environment setup complete."
    return $EXIT_SUCCESS
}

check_prerequisites() {
    echo "Checking prerequisites..."
    
    # Check Python
    if ! command -v python3 &>/dev/null; then
        echo "Error: Python 3 is not installed or not in PATH"
        return $EXIT_FAILURE
    fi
    
    # Check Python version (3.8+)
    PYTHON_VERSION=$(python3 --version | cut -d " " -f 2)
    if [[ $(echo "$PYTHON_VERSION" | cut -d. -f1) -lt 3 ]] || [[ $(echo "$PYTHON_VERSION" | cut -d. -f1) -eq 3 && $(echo "$PYTHON_VERSION" | cut -d. -f2) -lt 8 ]]; then
        echo "Error: Python 3.8+ is required, found $PYTHON_VERSION"
        return $EXIT_FAILURE
    fi
    
    # Check gcloud
    if ! command -v gcloud &>/dev/null; then
        echo "Error: gcloud CLI is not installed or not in PATH"
        return $EXIT_FAILURE
    fi
    
    # Check gsutil
    if ! command -v gsutil &>/dev/null; then
        echo "Error: gsutil is not installed or not in PATH"
        return $EXIT_FAILURE
    fi
    
    # Check required Python scripts
    if [ ! -f "${SCRIPT_DIR}/../scripts/deploy_dags.py" ]; then
        echo "Error: deploy_dags.py not found at ${SCRIPT_DIR}/../scripts/deploy_dags.py"
        return $EXIT_FAILURE
    fi
    
    if [ ! -f "${SCRIPT_DIR}/../scripts/validate_dags.py" ]; then
        echo "Error: validate_dags.py not found at ${SCRIPT_DIR}/../scripts/validate_dags.py"
        return $EXIT_FAILURE
    fi
    
    if [ ! -f "${SCRIPT_DIR}/../scripts/import_variables.py" ]; then
        echo "Error: import_variables.py not found at ${SCRIPT_DIR}/../scripts/import_variables.py"
        return $EXIT_FAILURE
    fi
    
    if [ ! -f "${SCRIPT_DIR}/../scripts/import_connections.py" ]; then
        echo "Error: import_connections.py not found at ${SCRIPT_DIR}/../scripts/import_connections.py"
        return $EXIT_FAILURE
    fi
    
    echo "All prerequisites checked."
    return $EXIT_SUCCESS
}

authenticate_gcp() {
    echo "Authenticating with Google Cloud Platform..."
    
    # Check if already authenticated
    if gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
        echo "Already authenticated with GCP"
    else
        echo "Not authenticated with GCP, attempting to authenticate..."
        
        # If service account key provided, activate it
        if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ] && [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
            gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
        else
            # Otherwise use application default credentials for interactive sessions
            gcloud auth application-default login
        fi
    fi
    
    # Set project
    gcloud config set project "$PROJECT_ID"
    
    # Verify authentication by trying to list project details
    if gcloud projects describe "$PROJECT_ID" >/dev/null 2>&1; then
        echo "Successfully authenticated with GCP and set project to $PROJECT_ID"
        return $EXIT_SUCCESS
    else
        echo "Error: Failed to authenticate with GCP or access project $PROJECT_ID"
        return $EXIT_FAILURE
    fi
}

get_composer_details() {
    echo "Getting Cloud Composer environment details..."
    
    # Get Composer environment details using gcloud
    ENV_DETAILS=$(gcloud composer environments describe "$COMPOSER_ENV" \
        --location "$COMPOSER_REGION" \
        --project "$PROJECT_ID" \
        --format="json")
    
    # Extract GCS bucket from environment details
    GCS_BUCKET=$(echo "$ENV_DETAILS" | python3 -c "import sys, json; print(json.load(sys.stdin)['config']['dagGcsPrefix'].split('gs://')[1].split('/')[0])")
    
    # Extract Airflow web server URL
    AIRFLOW_URI=$(echo "$ENV_DETAILS" | python3 -c "import sys, json; print(json.load(sys.stdin)['config']['airflowUri'])")
    
    if [ -z "$GCS_BUCKET" ]; then
        echo "Error: Could not extract GCS bucket from Composer environment details"
        return $EXIT_FAILURE
    fi
    
    echo "Composer Environment: $COMPOSER_ENV"
    echo "Region: $COMPOSER_REGION"
    echo "GCS Bucket: $GCS_BUCKET"
    echo "Airflow Web UI: $AIRFLOW_URI"
    
    return $EXIT_SUCCESS
}

validate_dags() {
    echo "Validating DAGs for Airflow 2.X compatibility..."
    
    # Run validate_dags.py script
    python3 "${SCRIPT_DIR}/../scripts/validate_dags.py" \
        "$DAG_SOURCE_DIR" \
        --format json \
        --output "/tmp/dag_validation_report.json" \
        --level WARNING \
        --verbose
    
    # Check validation results
    VALIDATION_RESULT=$?
    WARNING_COUNT=$(cat /tmp/dag_validation_report.json | python3 -c "import sys, json; print(json.load(sys.stdin)['summary']['warning_count'])")
    ERROR_COUNT=$(cat /tmp/dag_validation_report.json | python3 -c "import sys, json; print(json.load(sys.stdin)['summary']['error_count'])")
    
    echo "Validation results: $ERROR_COUNT errors, $WARNING_COUNT warnings"
    
    # For development environment, we allow more warnings but no errors
    if [ "$ERROR_COUNT" -gt 0 ]; then
        echo "Error: DAG validation failed with $ERROR_COUNT errors"
        return $EXIT_FAILURE
    fi
    
    if [ "$WARNING_COUNT" -gt "$VALIDATION_THRESHOLD" ]; then
        echo "Error: DAG validation has too many warnings: $WARNING_COUNT (threshold: $VALIDATION_THRESHOLD)"
        return $EXIT_FAILURE
    fi
    
    echo "DAGs validated successfully for development environment"
    return $EXIT_SUCCESS
}

deploy_dags() {
    echo "Deploying DAGs to Cloud Composer environment..."
    
    # Run deploy_dags.py script with appropriate parameters for dev environment
    python3 "${SCRIPT_DIR}/../scripts/deploy_dags.py" \
        dev \
        --source-folder "$DAG_SOURCE_DIR" \
        --parallel \
        --verbose
    
    DEPLOY_RESULT=$?
    
    if [ $DEPLOY_RESULT -ne 0 ]; then
        echo "Error: DAG deployment failed with exit code $DEPLOY_RESULT"
        return $EXIT_FAILURE
    fi
    
    echo "DAGs deployed successfully to Cloud Composer environment"
    return $EXIT_SUCCESS
}

import_airflow_variables() {
    echo "Importing Airflow variables for development environment..."
    
    if [ ! -f "$VARIABLES_FILE" ]; then
        echo "Warning: Variables file not found at $VARIABLES_FILE, skipping variable import"
        return $EXIT_SUCCESS
    fi
    
    # Run import_variables.py script
    python3 "${SCRIPT_DIR}/../scripts/import_variables.py" \
        --file_path "$VARIABLES_FILE" \
        --environment dev \
        --skip_existing
    
    IMPORT_RESULT=$?
    
    if [ $IMPORT_RESULT -ne 0 ]; then
        echo "Error: Variables import failed with exit code $IMPORT_RESULT"
        return $EXIT_FAILURE
    fi
    
    echo "Airflow variables imported successfully"
    return $EXIT_SUCCESS
}

import_airflow_connections() {
    echo "Importing Airflow connections for development environment..."
    
    if [ ! -f "$CONNECTIONS_FILE" ]; then
        echo "Warning: Connections file not found at $CONNECTIONS_FILE, skipping connection import"
        return $EXIT_SUCCESS
    fi
    
    # Run import_connections.py script
    python3 "${SCRIPT_DIR}/../scripts/import_connections.py" \
        --file-path "$CONNECTIONS_FILE" \
        --environment dev \
        --skip-existing \
        --validate
    
    IMPORT_RESULT=$?
    
    if [ $IMPORT_RESULT -ne 0 ]; then
        echo "Error: Connections import failed with exit code $IMPORT_RESULT"
        return $EXIT_FAILURE
    fi
    
    echo "Airflow connections imported successfully"
    return $EXIT_SUCCESS
}

verify_deployment() {
    echo "Verifying deployment..."
    
    # List DAGs in GCS to verify they were uploaded
    echo "Listing DAGs in GCS bucket..."
    gsutil ls -r "gs://$GCS_BUCKET/dags/" | grep -E '\.py$'
    
    # Wait for DAG parsing cycle to complete (longer for dev environment to account for slower parsing)
    echo "Waiting for DAG parsing cycle to complete (45 seconds)..."
    sleep 45
    
    # Run dag-list command to verify DAGs are visible in Airflow
    echo "Checking DAGs in Airflow..."
    gcloud composer environments run "$COMPOSER_ENV" \
        --location "$COMPOSER_REGION" \
        --project "$PROJECT_ID" \
        dags list
    
    # Check exit code of the command
    VERIFY_RESULT=$?
    
    if [ $VERIFY_RESULT -ne 0 ]; then
        echo "Error: Deployment verification failed with exit code $VERIFY_RESULT"
        return $EXIT_FAILURE
    fi
    
    echo "Deployment verified successfully"
    return $EXIT_SUCCESS
}

notify_developers() {
    local status_code=$1
    
    if [ $status_code -eq 0 ]; then
        status="SUCCESS"
    else
        status="FAILURE"
    fi
    
    echo "Notifying developers about deployment status: $status"
    
    # For development environment, we'll just log a message 
    # In a real environment, this would send emails and/or Slack notifications
    echo "========================================================================"
    echo "Deployment Notification: Development environment deployment $status"
    echo "Airflow UI URL: $AIRFLOW_URI"
    echo "Deployment log: $LOG_FILE"
    echo "========================================================================" 
    
    # Add implementation for email/Slack notifications here in a real environment
    
    return $EXIT_SUCCESS
}

print_summary() {
    local exit_code=$1
    
    echo "============================================================"
    echo "Deployment Summary"
    echo "============================================================"
    echo "Environment: Development (dev)"
    echo "Project: $PROJECT_ID"
    echo "Composer: $COMPOSER_ENV ($COMPOSER_REGION)"
    echo "Timestamp: $(date)"
    
    if [ $exit_code -eq 0 ]; then
        echo "Status: SUCCESS"
    else
        echo "Status: FAILURE"
    fi
    
    echo "Airflow Web UI: $AIRFLOW_URI"
    echo "GCS Bucket: gs://$GCS_BUCKET"
    echo "Log File: $LOG_FILE"
    
    echo "Next Steps:"
    if [ $exit_code -eq 0 ]; then
        echo "  - Verify DAGs in Airflow UI"
        echo "  - Run tests in development environment"
        echo "  - When ready, proceed to QA deployment using deploy-qa.sh"
    else
        echo "  - Check log file for errors"
        echo "  - Fix issues and retry deployment"
    fi
    
    echo "============================================================"
}

cleanup() {
    echo "Performing cleanup..."
    
    # Remove temporary validation report
    if [ -f "/tmp/dag_validation_report.json" ]; then
        rm /tmp/dag_validation_report.json
    fi
    
    # Upload log file to GCS for record keeping
    if [ -f "$LOG_FILE" ]; then
        LOG_GCS_PATH="gs://$GCS_BUCKET/logs/deployment/$(basename "$LOG_FILE")"
        gsutil cp "$LOG_FILE" "$LOG_GCS_PATH"
        echo "Deployment log uploaded to $LOG_GCS_PATH"
    fi
    
    echo "Cleanup complete"
}

main() {
    # Start logging
    exec > >(tee -a "$LOG_FILE") 2>&1
    
    print_header
    
    # Set up environment
    setup_environment
    if [ $? -ne 0 ]; then
        echo "Error: Failed to set up environment"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Check prerequisites
    check_prerequisites
    if [ $? -ne 0 ]; then
        echo "Error: Prerequisites check failed"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Authenticate with GCP
    authenticate_gcp
    if [ $? -ne 0 ]; then
        echo "Error: GCP authentication failed"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Get Composer environment details
    get_composer_details
    if [ $? -ne 0 ]; then
        echo "Error: Failed to get Composer environment details"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Validate DAGs
    validate_dags
    if [ $? -ne 0 ]; then
        echo "Error: DAG validation failed"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Deploy DAGs
    deploy_dags
    if [ $? -ne 0 ]; then
        echo "Error: DAG deployment failed"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Import Airflow variables
    import_airflow_variables
    if [ $? -ne 0 ]; then
        echo "Error: Airflow variables import failed"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Import Airflow connections
    import_airflow_connections
    if [ $? -ne 0 ]; then
        echo "Error: Airflow connections import failed"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Verify deployment
    verify_deployment
    if [ $? -ne 0 ]; then
        echo "Error: Deployment verification failed"
        print_summary $EXIT_FAILURE
        return $EXIT_FAILURE
    fi
    
    # Notify developers
    notify_developers $EXIT_SUCCESS
    
    # Print summary
    print_summary $EXIT_SUCCESS
    
    # Cleanup
    cleanup
    
    echo "Deployment to development environment completed successfully"
    return $EXIT_SUCCESS
}

# Run main function
main
exit $?