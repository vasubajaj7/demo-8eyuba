#!/bin/bash
#
# deploy-qa.sh - Deployment script for Cloud Composer 2 QA environment
# Version: 1.0.0
# Author: Cloud Composer Migration Team
#
# This script manages the deployment of Apache Airflow DAGs and configurations
# to the Cloud Composer 2 QA environment as part of the CI/CD pipeline.
# It handles authentication, enhanced validation, deployment, verification, and
# follows the QA approval workflow with proper governance controls.

# Exit immediately if a command exits with a non-zero status
set -e

# Define exit codes
EXIT_SUCCESS=0
EXIT_FAILURE=1

# ===================================================================
# Functions
# ===================================================================

print_header() {
    echo "============================================================"
    echo "Cloud Composer 2 QA Environment Deployment"
    echo "Version: 1.0.0"
    echo "============================================================"
    echo "Target Environment: QA"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"
}

setup_environment() {
    # Get directory where this script lives
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    # Set environment to QA
    export ENVIRONMENT="qa"
    
    # Use Python to extract configuration values from composer_qa.py
    if ! command -v python3 &> /dev/null; then
        echo "ERROR: Python3 is required but not found."
        return 1
    fi

    # Create a temporary Python script to extract config
    temp_script=$(mktemp)
    cat > "${temp_script}" << EOL
import sys
sys.path.insert(0, '${SCRIPT_DIR}/..')
try:
    from config.composer_qa import config
    print(f"PROJECT_ID={config['gcp']['project_id']}")
    print(f"COMPOSER_ENV={config['composer']['environment_name']}")
    print(f"COMPOSER_REGION={config['gcp']['region']}")
    print(f"GCS_DAG_BUCKET={config['storage']['dag_bucket']}")
except Exception as e:
    print(f"ERROR: {str(e)}", file=sys.stderr)
    sys.exit(1)
EOL

    # Execute temporary script and source the output
    if ! python3 "${temp_script}" > /tmp/qa_env_config.sh; then
        rm "${temp_script}"
        echo "ERROR: Failed to extract configuration from composer_qa.py"
        return 1
    fi
    
    source /tmp/qa_env_config.sh
    rm "${temp_script}" /tmp/qa_env_config.sh
    
    # Set path variables
    DAG_SOURCE_DIR="${SCRIPT_DIR}/../../dags"
    CONFIG_DIR="${SCRIPT_DIR}/../config"
    VARIABLES_FILE="${CONFIG_DIR}/variables-qa.json"
    CONNECTIONS_FILE="${CONFIG_DIR}/connections-qa.json"
    
    # QA-specific settings
    APPROVAL_WORKFLOW_FILE="${SCRIPT_DIR}/approval-workflow.json"
    APPROVAL_TOKEN_FILE="${SCRIPT_DIR}/.qa_approval_token"
    
    # Create timestamped log file
    mkdir -p "${SCRIPT_DIR}/logs"
    LOG_FILE="${SCRIPT_DIR}/logs/deploy-qa-$(date '+%Y%m%d-%H%M%S').log"
    
    # QA validation thresholds - stricter than DEV
    VALIDATION_THRESHOLD=5 # Maximum allowable validation warnings for QA
    
    # Define required approvers based on approval workflow configuration
    REQUIRED_APPROVERS=("PEER" "QA")
    
    # Export variables for other processes
    export PROJECT_ID COMPOSER_ENV COMPOSER_REGION DAG_SOURCE_DIR
    export CONFIG_DIR VARIABLES_FILE CONNECTIONS_FILE LOG_FILE
    export VALIDATION_THRESHOLD REQUIRED_APPROVERS
    
    echo "Environment configuration:"
    echo "  - Project ID: ${PROJECT_ID}"
    echo "  - Composer Environment: ${COMPOSER_ENV}"
    echo "  - Region: ${COMPOSER_REGION}"
    echo "  - DAG Source Directory: ${DAG_SOURCE_DIR}"
    
    return 0
}

check_prerequisites() {
    echo "Checking prerequisites..."
    
    # Check Python version (3.8+ required)
    if ! command -v python3 &> /dev/null; then
        echo "ERROR: Python3 is required but not found."
        return 1
    fi
    
    python_version=$(python3 --version 2>&1 | awk '{print $2}')
    if [[ -z "$python_version" ]]; then
        echo "ERROR: Could not determine Python version."
        return 1
    fi
    
    # Compare major.minor version to 3.8
    python_major=$(echo $python_version | cut -d. -f1)
    python_minor=$(echo $python_version | cut -d. -f2)
    
    if [[ $python_major -lt 3 || ($python_major -eq 3 && $python_minor -lt 8) ]]; then
        echo "ERROR: Python 3.8+ is required. Found: $python_version"
        return 1
    fi
    
    # Check if gcloud is installed
    if ! command -v gcloud &> /dev/null; then
        echo "ERROR: gcloud CLI not found. Please install Google Cloud SDK."
        return 1
    fi
    
    # Check if gsutil is installed
    if ! command -v gsutil &> /dev/null; then
        echo "ERROR: gsutil not found. Please install Google Cloud SDK."
        return 1
    fi
    
    # Check if jq is installed (for JSON processing)
    if ! command -v jq &> /dev/null; then
        echo "ERROR: jq not found. Please install jq for JSON processing."
        return 1
    fi
    
    # Check for required Python scripts
    required_scripts=(
        "../scripts/deploy_dags.py"
        "../scripts/validate_dags.py"
        "../scripts/import_variables.py"
        "../scripts/import_connections.py"
    )
    
    for script in "${required_scripts[@]}"; do
        if [[ ! -f "${SCRIPT_DIR}/${script}" ]]; then
            echo "ERROR: Required script not found: ${script}"
            return 1
        fi
    done
    
    # Check if approval workflow configuration exists
    if [[ ! -f "${APPROVAL_WORKFLOW_FILE}" ]]; then
        echo "ERROR: Approval workflow configuration not found: ${APPROVAL_WORKFLOW_FILE}"
        return 1
    fi
    
    echo "All prerequisites satisfied."
    return 0
}

verify_approval() {
    echo "Verifying QA deployment approvals..."
    
    # Load approval workflow configuration
    if [[ ! -f "${APPROVAL_WORKFLOW_FILE}" ]]; then
        echo "ERROR: Approval workflow configuration not found: ${APPROVAL_WORKFLOW_FILE}"
        return 1
    fi
    
    # Extract required approvers for QA environment from workflow configuration
    qa_env_config=$(jq -r '.environments[] | select(.name=="qa")' "${APPROVAL_WORKFLOW_FILE}")
    if [[ -z "$qa_env_config" ]]; then
        echo "ERROR: QA environment configuration not found in approval workflow."
        return 1
    fi
    
    echo "Required approvers for QA environment: ${REQUIRED_APPROVERS[*]}"
    
    # Check if approval token exists
    if [[ ! -f "${APPROVAL_TOKEN_FILE}" ]]; then
        echo "ERROR: QA deployment requires approval. Approval token not found."
        echo "Please obtain approval from PEER and QA reviewers before proceeding."
        return 1
    fi
    
    # In a real implementation, we would verify the token's cryptographic signature
    # and check that it hasn't expired. For this script, we'll do basic validation:
    
    # 1. Check token format
    if ! jq empty "${APPROVAL_TOKEN_FILE}" 2>/dev/null; then
        echo "ERROR: Invalid approval token format. Token must be valid JSON."
        return 1
    fi
    
    # 2. Check token expiry
    token_expiry=$(jq -r '.expiry // 0' "${APPROVAL_TOKEN_FILE}")
    current_time=$(date +%s)
    
    if [[ $token_expiry -gt 0 && $current_time -gt $token_expiry ]]; then
        echo "ERROR: Approval token has expired. Please obtain fresh approvals."
        return 1
    fi
    
    # 3. Check required approvers
    approvers=$(jq -r '.approver_ids[]' "${APPROVAL_TOKEN_FILE}" 2>/dev/null)
    if [[ $? -ne 0 ]]; then
        echo "ERROR: Invalid approval token. Missing approver_ids field."
        return 1
    fi
    
    for required in "${REQUIRED_APPROVERS[@]}"; do
        if ! echo "${approvers}" | grep -q "${required}"; then
            echo "ERROR: Missing required approval from: ${required}"
            echo "Please obtain all required approvals before proceeding."
            return 1
        fi
    done
    
    echo "All required approvals verified."
    return 0
}

authenticate_gcp() {
    echo "Authenticating with Google Cloud Platform..."
    
    # Check if already authenticated
    if gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
        echo "Already authenticated with GCP."
    else
        # If service account key provided, use it
        if [[ -n "${SERVICE_ACCOUNT_KEY}" ]]; then
            echo "Authenticating using service account key..."
            if ! gcloud auth activate-service-account --key-file="${SERVICE_ACCOUNT_KEY}"; then
                echo "ERROR: Failed to authenticate using service account key."
                return 1
            fi
        else
            echo "No service account key provided. Using active credentials."
            # In a CI/CD pipeline, we would need to ensure credentials are properly set up
        fi
    fi
    
    # Set project
    if ! gcloud config set project "${PROJECT_ID}"; then
        echo "ERROR: Failed to set GCP project: ${PROJECT_ID}"
        return 1
    fi
    
    # Verify authentication by describing project
    if ! gcloud projects describe "${PROJECT_ID}" &> /dev/null; then
        echo "ERROR: Failed to authenticate with GCP project: ${PROJECT_ID}"
        return 1
    fi
    
    echo "Successfully authenticated with GCP project: ${PROJECT_ID}"
    return 0
}

get_composer_details() {
    echo "Retrieving Cloud Composer environment details..."
    
    # Get environment details
    if ! composer_details=$(gcloud composer environments describe "${COMPOSER_ENV}" \
        --location="${COMPOSER_REGION}" \
        --format=json); then
        echo "ERROR: Failed to retrieve Composer environment details."
        return 1
    fi
    
    # Extract GCS bucket and Airflow URI
    GCS_BUCKET=$(echo "${composer_details}" | jq -r '.config.dagGcsPrefix' | sed 's/\/dags//')
    AIRFLOW_URI=$(echo "${composer_details}" | jq -r '.config.airflowUri')
    
    if [[ -z "${GCS_BUCKET}" || "${GCS_BUCKET}" == "null" ]]; then
        echo "ERROR: Failed to retrieve GCS bucket from Composer environment"
        return 1
    fi
    
    if [[ -z "${AIRFLOW_URI}" || "${AIRFLOW_URI}" == "null" ]]; then
        echo "WARNING: Failed to retrieve Airflow UI URI from Composer environment"
        AIRFLOW_URI="unknown"
    fi
    
    echo "Composer environment details:"
    echo "  - Name: ${COMPOSER_ENV}"
    echo "  - Region: ${COMPOSER_REGION}"
    echo "  - GCS Bucket: ${GCS_BUCKET}"
    echo "  - Airflow URI: ${AIRFLOW_URI}"
    
    # Export for use in other functions
    export GCS_BUCKET AIRFLOW_URI
    
    return 0
}

validate_dags() {
    echo "Validating DAGs with QA-specific standards..."
    
    # Create validation output file path with timestamp
    validation_output="${SCRIPT_DIR}/logs/validation-report-qa-$(date '+%Y%m%d-%H%M%S')"
    
    # Run the validate_dags.py script with QA-specific parameters
    echo "Running DAG validation with QA-specific thresholds..."
    if ! python3 "${SCRIPT_DIR}/../scripts/validate_dags.py" \
        "${DAG_SOURCE_DIR}" \
        --output="${validation_output}.json" \
        --format="json" \
        --level="WARNING"; then
        echo "ERROR: DAG validation script failed."
        return 1
    fi
    
    # Check validation warnings against threshold
    warning_count=$(jq -r '.summary.warning_count // 0' "${validation_output}.json")
    error_count=$(jq -r '.summary.error_count // 0' "${validation_output}.json")
    
    echo "Validation results: ${error_count} errors, ${warning_count} warnings"
    
    if [[ $error_count -gt 0 ]]; then
        echo "ERROR: DAG validation failed with ${error_count} errors. Please fix issues before deploying to QA."
        return 1
    fi
    
    if [[ $warning_count -gt $VALIDATION_THRESHOLD ]]; then
        echo "ERROR: Too many validation warnings: ${warning_count}. Maximum allowed for QA: ${VALIDATION_THRESHOLD}"
        return 1
    fi
    
    # Generate HTML report for QA reviewers
    echo "Generating HTML validation report for QA reviewers..."
    if ! python3 "${SCRIPT_DIR}/../scripts/validate_dags.py" \
        "${DAG_SOURCE_DIR}" \
        --output="${validation_output}.html" \
        --format="html" \
        --level="INFO"; then
        echo "WARNING: Failed to generate HTML validation report."
    else
        echo "HTML validation report generated: ${validation_output}.html"
    fi
    
    echo "DAG validation passed QA standards."
    return 0
}

deploy_dags() {
    echo "Deploying DAGs to QA environment..."
    
    # Run the deploy_dags.py script
    if ! python3 "${SCRIPT_DIR}/../scripts/deploy_dags.py" \
        qa \
        --source-folder="${DAG_SOURCE_DIR}" \
        --verbose; then
        echo "ERROR: DAG deployment failed."
        return 1
    fi
    
    echo "DAGs successfully deployed to QA environment."
    return 0
}

import_airflow_variables() {
    echo "Importing Airflow variables for QA environment..."
    
    if [[ ! -f "${VARIABLES_FILE}" ]]; then
        echo "Variables file not found: ${VARIABLES_FILE}"
        echo "Skipping variable import."
        return 0
    fi
    
    # Run the import_variables.py script
    if ! python3 "${SCRIPT_DIR}/../scripts/import_variables.py" \
        --file_path="${VARIABLES_FILE}" \
        --environment="qa"; then
        echo "WARNING: Variable import encountered issues."
        return 1
    fi
    
    echo "Variables successfully imported to QA environment."
    return 0
}

import_airflow_connections() {
    echo "Importing Airflow connections for QA environment..."
    
    if [[ ! -f "${CONNECTIONS_FILE}" ]]; then
        echo "Connections file not found: ${CONNECTIONS_FILE}"
        echo "Skipping connection import."
        return 0
    fi
    
    # Run the import_connections.py script
    if ! python3 "${SCRIPT_DIR}/../scripts/import_connections.py" \
        --file-path="${CONNECTIONS_FILE}" \
        --environment="qa" \
        --skip-existing; then
        echo "WARNING: Connection import encountered issues."
        return 1
    fi
    
    echo "Connections successfully imported to QA environment."
    return 0
}

verify_deployment() {
    echo "Verifying deployment in QA environment..."
    
    # List DAGs in GCS bucket to verify upload
    echo "Checking uploaded DAGs in GCS bucket..."
    if ! gsutil ls "${GCS_BUCKET}/dags/" | grep -v '__pycache__'; then
        echo "WARNING: Failed to list DAGs in GCS bucket or no DAGs found."
        return 1
    fi
    
    # Wait for DAG parsing cycle to complete (approximately 5 minutes)
    echo "Waiting for DAG parsing cycle to complete (5 minutes)..."
    sleep 300
    
    # Get list of DAGs from Airflow
    echo "Retrieving DAG list from Airflow..."
    if ! gcloud composer environments run "${COMPOSER_ENV}" \
        --location="${COMPOSER_REGION}" \
        dags list; then
        echo "WARNING: Failed to list DAGs in Airflow. They may still be parsing."
        return 1
    fi
    
    # Run QA-specific tests for deployed DAGs
    echo "Running QA verification tests..."
    
    # Check for Airflow parsing errors in the logs
    echo "Checking for Airflow parsing errors..."
    if gcloud logging read "resource.type=cloud_composer_environment AND resource.labels.environment_name=${COMPOSER_ENV} AND resource.labels.location=${COMPOSER_REGION} AND textPayload:\"Error parsing\"" --limit 10 --format="json" | jq -e '.[] | select(.textPayload | contains("Error parsing"))' > /dev/null; then
        echo "WARNING: Found DAG parsing errors in logs. Please check the Airflow UI for details."
        return 1
    fi
    
    # Generate QA verification report
    echo "Generating QA verification report..."
    cat > "${SCRIPT_DIR}/logs/qa-verification-report-$(date '+%Y%m%d-%H%M%S').txt" << EOL
QA Deployment Verification Report
================================
Timestamp: $(date '+%Y-%m-%d %H:%M:%S')
Environment: QA
Composer Environment: ${COMPOSER_ENV}
Project: ${PROJECT_ID}

Verification Steps:
- DAGs uploaded to GCS: PASS
- DAGs visible in Airflow: PASS
- No parsing errors detected: PASS

Result: PASS

Next Steps:
- QA team will perform manual verification
- Upon QA approval, deployment to PROD can be initiated
EOL
    
    echo "Deployment verification completed successfully."
    return 0
}

notify_qa_team() {
    local status_code=$1
    
    echo "Notifying QA team about deployment..."
    
    # Status text for notification
    local status_text="Successful"
    if [[ ${status_code} -ne 0 ]]; then
        status_text="Failed"
    fi
    
    # Prepare notification content
    local subject="[QA] Airflow DAG Deployment ${status_text}"
    local message="
Airflow DAG deployment to QA environment ${status_text}.

Environment: QA
Project: ${PROJECT_ID}
Composer Environment: ${COMPOSER_ENV}
Deployment Time: $(date '+%Y-%m-%d %H:%M:%S')

Airflow UI: ${AIRFLOW_URI}
Logs: ${LOG_FILE}

Please review the deployment and report any issues.
"
    
    # In a real implementation, this would send an email using a tool like 'mail'
    # and possibly post to Slack using curl or a similar tool.
    
    echo "Email notification content:"
    echo "${message}"
    
    # Simulate sending email
    echo "[SIMULATED] Sending email to qa-team@example.com with subject: ${subject}"
    
    # Simulate Slack notification
    echo "[SIMULATED] Posting notification to #airflow-qa-deployments channel"
    
    echo "QA team notification completed."
    return 0
}

record_deployment_audit() {
    echo "Recording deployment audit information..."
    
    # Create audit record
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local user=$(whoami)
    local audit_record="{
        \"timestamp\": \"${timestamp}\",
        \"user\": \"${user}\",
        \"environment\": \"qa\",
        \"project\": \"${PROJECT_ID}\",
        \"composer_env\": \"${COMPOSER_ENV}\",
        \"action\": \"deployment\",
        \"status\": \"success\"
    }"
    
    # Create local audit log
    audit_file="${SCRIPT_DIR}/logs/deployment-audit-$(date '+%Y%m%d-%H%M%S').json"
    echo "${audit_record}" > "${audit_file}"
    
    # Upload audit record to GCS for auditing
    if [[ -n "${GCS_BUCKET}" ]]; then
        echo "Uploading audit record to GCS..."
        gsutil cp "${audit_file}" "${GCS_BUCKET}/audit/qa/$(date '+%Y/%m/%d')/deployment-$(date '+%Y%m%d-%H%M%S').json"
    fi
    
    echo "Deployment audit information recorded."
    return 0
}

print_summary() {
    local exit_code=$1
    
    echo "============================================================"
    echo "QA Deployment Summary"
    echo "============================================================"
    echo "Environment: QA"
    echo "Project: ${PROJECT_ID}"
    echo "Composer Environment: ${COMPOSER_ENV}"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    
    if [[ ${exit_code} -eq 0 ]]; then
        echo "Status: SUCCESS"
    else
        echo "Status: FAILED"
    fi
    
    echo "Airflow Web UI: ${AIRFLOW_URI}"
    echo "Log File: ${LOG_FILE}"
    
    if [[ ${exit_code} -eq 0 ]]; then
        echo ""
        echo "Next Steps:"
        echo "1. QA team will perform testing on the deployed DAGs"
        echo "2. If successful, proceed with PROD deployment using deploy-prod.sh"
    else
        echo ""
        echo "Troubleshooting:"
        echo "1. Check the log file for specific errors"
        echo "2. Fix issues and re-run deployment"
        echo "3. Contact DevOps if assistance is needed"
    fi
    
    echo "============================================================"
}

cleanup() {
    echo "Performing cleanup..."
    
    # Compress logs that are older than a day
    find "${SCRIPT_DIR}/logs" -name "*.log" -type f -mtime +1 -exec gzip {} \;
    
    # Upload logs to GCS for auditing
    if [[ -n "${GCS_BUCKET}" ]]; then
        echo "Uploading deployment logs to GCS..."
        gsutil -m cp "${SCRIPT_DIR}/logs/*" "${GCS_BUCKET}/logs/deployments/qa/$(date '+%Y/%m/%d')/"
    fi
    
    echo "Cleanup completed."
}

main() {
    local exit_code=0
    
    # Print script header
    print_header
    
    # Setup environment variables
    if ! setup_environment; then
        echo "ERROR: Failed to setup environment."
        exit $EXIT_FAILURE
    fi
    
    # Create logs directory if it doesn't exist
    mkdir -p "$(dirname "${LOG_FILE}")"
    
    # Redirect output to log file and console
    exec &> >(tee -a "${LOG_FILE}")
    
    # Record start time
    start_time=$(date +%s)
    
    # Check prerequisites
    if ! check_prerequisites; then
        echo "ERROR: Prerequisites check failed."
        print_summary $EXIT_FAILURE
        exit $EXIT_FAILURE
    fi
    
    # Verify QA approvals
    if ! verify_approval; then
        echo "ERROR: QA approval verification failed."
        print_summary $EXIT_FAILURE
        exit $EXIT_FAILURE
    fi
    
    # Authenticate with GCP
    if ! authenticate_gcp; then
        echo "ERROR: GCP authentication failed."
        print_summary $EXIT_FAILURE
        exit $EXIT_FAILURE
    fi
    
    # Get Composer environment details
    if ! get_composer_details; then
        echo "ERROR: Failed to retrieve Composer environment details."
        print_summary $EXIT_FAILURE
        exit $EXIT_FAILURE
    fi
    
    # Validate DAGs with QA-specific standards
    if ! validate_dags; then
        echo "ERROR: DAG validation failed for QA standards."
        print_summary $EXIT_FAILURE
        exit $EXIT_FAILURE
    fi
    
    # Deploy DAGs to QA environment
    if ! deploy_dags; then
        echo "ERROR: DAG deployment failed."
        exit_code=$EXIT_FAILURE
    else
        # Import Airflow variables (non-fatal if fails)
        import_airflow_variables
        
        # Import Airflow connections (non-fatal if fails)
        import_airflow_connections
        
        # Verify deployment
        if ! verify_deployment; then
            echo "WARNING: Deployment verification encountered issues."
            # Non-fatal warning
        fi
        
        # Record deployment audit information
        record_deployment_audit
    fi
    
    # Notify QA team
    notify_qa_team ${exit_code}
    
    # Print deployment summary
    print_summary ${exit_code}
    
    # Perform cleanup
    cleanup
    
    # Record end time and duration
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    echo "Total deployment time: $((duration / 60)) minutes $((duration % 60)) seconds"
    
    exit ${exit_code}
}

# Execute main function
main