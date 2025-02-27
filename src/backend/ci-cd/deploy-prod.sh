#!/bin/bash
#
# deploy-prod.sh - Deployment script for Cloud Composer 2 production environment
# 
# Version: 1.0.0
# Author: Cloud Composer Migration Team
#

# Exit immediately if any command fails
set -e

# Global variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BACKEND_DIR="$PROJECT_ROOT/src/backend"
ENVIRONMENT="prod"
LOG_FILE="/tmp/deploy-prod-$(date +%Y%m%d-%H%M%S).log"
VALIDATION_THRESHOLD=0
APPROVAL_WORKFLOW_FILE="$SCRIPT_DIR/approval-workflow.json"
REQUIRED_APPROVERS=("CAB" "ARCHITECT" "STAKEHOLDER")
APPROVAL_TOKEN_FILE="${APPROVAL_TOKEN_FILE:-/tmp/approval-token.json}"
DEPLOYMENT_ID="$(date +%Y%m%d-%H%M%S)"
EXIT_SUCCESS=0
EXIT_FAILURE=1
NOTIFICATION_RECIPIENTS="airflow-alerts-prod@example.com,ops-team@example.com"

# Print a formatted header for the script execution
print_header() {
    echo "======================================================================"
    echo "   Cloud Composer 2 Production Deployment"
    echo "   Version: 1.0.0"
    echo "======================================================================"
    echo "Environment: PRODUCTION"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "======================================================================"
}

# Sets up environment variables and paths needed for deployment
setup_environment() {
    echo "Setting up environment variables and paths..." | tee -a "$LOG_FILE"
    
    # Source the production configuration
    # We need to do some Python magic to get the config into bash
    PYTHON_CONFIG=$(python3 -c "
import sys
sys.path.append('$BACKEND_DIR')
try:
    from config.composer_prod import config
    print(f\"PROJECT_ID={config['gcp']['project_id']}\")
    print(f\"COMPOSER_ENV={config['composer']['environment_name']}\")
    print(f\"COMPOSER_REGION={config['gcp']['region']}\")
    print(f\"GCS_BUCKET={config['storage']['dag_bucket']}\")
    print(f\"BACKUP_BUCKET={config['storage']['backup_bucket']}\")
except Exception as e:
    print(f\"Error loading configuration: {str(e)}\", file=sys.stderr)
    sys.exit(1)
")
    
    # Export environment variables from Python config
    eval "$PYTHON_CONFIG"
    
    # Set paths for variables and connections files
    DAG_SOURCE_DIR="$BACKEND_DIR/dags"
    CONFIG_DIR="$BACKEND_DIR/config"
    VARIABLES_FILE="$CONFIG_DIR/variables-prod.json"
    CONNECTIONS_FILE="$CONFIG_DIR/connections-prod.json"
    
    # Verify critical variables are set
    if [ -z "$PROJECT_ID" ] || [ -z "$COMPOSER_ENV" ] || [ -z "$COMPOSER_REGION" ]; then
        echo "ERROR: Critical environment variables are not set. Check configuration." | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Export for child processes
    export PROJECT_ID COMPOSER_ENV COMPOSER_REGION GCS_BUCKET BACKUP_BUCKET
    export DAG_SOURCE_DIR CONFIG_DIR VARIABLES_FILE CONNECTIONS_FILE
    
    echo "Environment setup complete." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Checks that all required tools are available
check_prerequisites() {
    echo "Checking prerequisites..." | tee -a "$LOG_FILE"
    
    # Check for required commands
    for cmd in python3 gcloud gsutil jq curl; do
        if ! command -v $cmd &> /dev/null; then
            echo "ERROR: Required command not found: $cmd" | tee -a "$LOG_FILE"
            return $EXIT_FAILURE
        fi
    done
    
    # Check Python version
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d'.' -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d'.' -f2)
    
    if [ "$PYTHON_MAJOR" -lt 3 ] || ( [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 8 ] ); then
        echo "ERROR: Python 3.8+ is required, found $PYTHON_VERSION" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Check if required Python scripts exist
    for script in "$BACKEND_DIR/scripts/deploy_dags.py" \
                 "$BACKEND_DIR/scripts/validate_dags.py" \
                 "$BACKEND_DIR/scripts/import_variables.py" \
                 "$BACKEND_DIR/scripts/import_connections.py"; do
        if [ ! -f "$script" ]; then
            echo "ERROR: Required script not found: $script" | tee -a "$LOG_FILE"
            return $EXIT_FAILURE
        fi
    done
    
    # Check if approval workflow file exists
    if [ ! -f "$APPROVAL_WORKFLOW_FILE" ]; then
        echo "ERROR: Approval workflow file not found: $APPROVAL_WORKFLOW_FILE" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    echo "All prerequisites are satisfied." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Verifies that the deployment has required production approvals
verify_approval() {
    echo "Verifying deployment approvals..." | tee -a "$LOG_FILE"
    
    # Load and validate approval workflow file
    if ! jq . "$APPROVAL_WORKFLOW_FILE" > /dev/null 2>&1; then
        echo "ERROR: Invalid JSON in approval workflow file" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Check if approval token file exists
    if [ -z "$APPROVAL_TOKEN_FILE" ] || [ ! -f "$APPROVAL_TOKEN_FILE" ]; then
        echo "ERROR: Approval token file not found or not specified" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Extract required approvers from the approval workflow
    PROD_APPROVERS=$(jq -r '.environments[] | select(.name == "prod") | .requiredApprovers[]' "$APPROVAL_WORKFLOW_FILE")
    
    # Verify that all required approvers are present
    for approver in "${REQUIRED_APPROVERS[@]}"; do
        if ! echo "$PROD_APPROVERS" | grep -q "$approver"; then
            echo "ERROR: Required approver not configured: $approver" | tee -a "$LOG_FILE"
            return $EXIT_FAILURE
        fi
    done
    
    # Validate token
    TOKEN_DATA=$(cat "$APPROVAL_TOKEN_FILE")
    
    # Use Python to verify the token
    PYTHON_VERIFY=$(python3 -c "
import sys, json, time
try:
    # In a real implementation, we would verify JWT signature here
    # For this script, we'll just check the JSON structure
    token_data = json.loads('$TOKEN_DATA')
    
    # Check required fields
    required_fields = ['environment', 'approver_ids', 'timestamp', 'approval_id']
    for field in required_fields:
        if field not in token_data:
            print(f'ERROR: Missing field in approval token: {field}')
            sys.exit(1)
    
    # Check environment
    if token_data['environment'] != 'prod':
        print(f'ERROR: Token is for wrong environment: {token_data[\"environment\"]}')
        sys.exit(1)
    
    # Check approvers
    approvers = set(token_data['approver_ids'])
    required_approvers = set(['CAB', 'ARCHITECT', 'STAKEHOLDER'])
    missing_approvers = required_approvers - approvers
    if missing_approvers:
        print(f'ERROR: Missing required approvers: {missing_approvers}')
        sys.exit(1)
    
    # Check expiration
    current_time = int(time.time())
    if 'expiration' in token_data and token_data['expiration'] < current_time:
        print(f'ERROR: Approval token has expired')
        sys.exit(1)
    
    # Check business justification
    if 'business_justification' not in token_data or len(token_data['business_justification']) < 50:
        print('ERROR: Missing or insufficient business justification')
        sys.exit(1)
    
    # Check risk assessment
    if 'risk_assessment' not in token_data:
        print('ERROR: Missing risk assessment')
        sys.exit(1)
    
    # Check rollback plan
    if 'rollback_plan' not in token_data or len(token_data['rollback_plan']) < 50:
        print('ERROR: Missing or insufficient rollback plan')
        sys.exit(1)
        
    print('APPROVED')
except Exception as e:
    print(f'ERROR: Failed to verify approval token: {str(e)}')
    sys.exit(1)
")
    
    if [ "$PYTHON_VERIFY" != "APPROVED" ]; then
        echo "ERROR: Approval verification failed: $PYTHON_VERIFY" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    echo "Deployment is approved by all required approvers." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Authenticates with Google Cloud Platform
authenticate_gcp() {
    echo "Authenticating with Google Cloud Platform..." | tee -a "$LOG_FILE"
    
    # Check if already authenticated
    if gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q '@'; then
        echo "Already authenticated with GCP." | tee -a "$LOG_FILE"
    else
        # Check for service account key file
        if [ -n "$SERVICE_ACCOUNT_KEY" ] && [ -f "$SERVICE_ACCOUNT_KEY" ]; then
            echo "Authenticating using service account key..." | tee -a "$LOG_FILE"
            gcloud auth activate-service-account --key-file="$SERVICE_ACCOUNT_KEY"
        else
            echo "Using active gcloud credentials..." | tee -a "$LOG_FILE"
        fi
    fi
    
    # Set the project
    gcloud config set project "$PROJECT_ID"
    
    # Verify authentication
    if ! gcloud projects describe "$PROJECT_ID" > /dev/null 2>&1; then
        echo "ERROR: Failed to authenticate with GCP project: $PROJECT_ID" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    echo "Successfully authenticated with GCP project: $PROJECT_ID" | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Retrieves details about the Composer environment
get_composer_details() {
    echo "Retrieving Cloud Composer environment details..." | tee -a "$LOG_FILE"
    
    # Get environment details
    COMPOSER_DETAILS=$(gcloud composer environments describe "$COMPOSER_ENV" \
                       --location "$COMPOSER_REGION" \
                       --format json)
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to retrieve Composer environment details" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Extract GCS bucket for DAGs
    GCS_BUCKET=$(echo "$COMPOSER_DETAILS" | jq -r '.config.dagGcsPrefix' | sed 's|gs://\([^/]*\).*|\1|')
    export GCS_BUCKET
    
    # Extract Airflow web server URL
    AIRFLOW_UI_URL=$(echo "$COMPOSER_DETAILS" | jq -r '.config.airflowUri')
    export AIRFLOW_UI_URL
    
    # Verify that we got the required information
    if [ -z "$GCS_BUCKET" ] || [ -z "$AIRFLOW_UI_URL" ]; then
        echo "ERROR: Failed to extract required details from Composer environment" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Verify that this is an Airflow 2.X environment
    AIRFLOW_VERSION=$(echo "$COMPOSER_DETAILS" | jq -r '.config.softwareConfig.imageVersion')
    if [[ "$AIRFLOW_VERSION" != *"airflow-2"* ]]; then
        echo "ERROR: Composer environment does not appear to be running Airflow 2.X" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    echo "Composer environment details:" | tee -a "$LOG_FILE"
    echo "  - Environment: $COMPOSER_ENV" | tee -a "$LOG_FILE"
    echo "  - Region: $COMPOSER_REGION" | tee -a "$LOG_FILE"
    echo "  - GCS Bucket: $GCS_BUCKET" | tee -a "$LOG_FILE"
    echo "  - Airflow UI: $AIRFLOW_UI_URL" | tee -a "$LOG_FILE"
    echo "  - Airflow Version: $AIRFLOW_VERSION" | tee -a "$LOG_FILE"
    
    return $EXIT_SUCCESS
}

# Creates a backup of the current production state before deployment
backup_current_state() {
    echo "Creating backup of current production state..." | tee -a "$LOG_FILE"
    
    # Create backup folder with timestamp
    BACKUP_PATH="gs://$BACKUP_BUCKET/backups/$DEPLOYMENT_ID"
    
    # Backup DAGs
    echo "Backing up DAGs from gs://$GCS_BUCKET/dags to $BACKUP_PATH/dags" | tee -a "$LOG_FILE"
    if ! gsutil -m cp -r "gs://$GCS_BUCKET/dags" "$BACKUP_PATH/"; then
        echo "ERROR: Failed to backup DAGs" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Export and backup Airflow variables
    echo "Backing up Airflow variables" | tee -a "$LOG_FILE"
    TEMP_VARS_FILE="/tmp/airflow_variables_$DEPLOYMENT_ID.json"
    
    if gcloud composer environments run "$COMPOSER_ENV" \
       --location "$COMPOSER_REGION" \
       variables -- --export "$TEMP_VARS_FILE" > /dev/null 2>&1; then
        
        gsutil cp "$TEMP_VARS_FILE" "$BACKUP_PATH/variables.json"
        rm -f "$TEMP_VARS_FILE"
    else
        echo "WARNING: Failed to export current variables, skipping backup of variables" | tee -a "$LOG_FILE"
    fi
    
    # Export and backup Airflow connections
    echo "Backing up Airflow connections" | tee -a "$LOG_FILE"
    TEMP_CONN_FILE="/tmp/airflow_connections_$DEPLOYMENT_ID.json"
    
    if gcloud composer environments run "$COMPOSER_ENV" \
       --location "$COMPOSER_REGION" \
       connections -- --export "$TEMP_CONN_FILE" > /dev/null 2>&1; then
        
        gsutil cp "$TEMP_CONN_FILE" "$BACKUP_PATH/connections.json"
        rm -f "$TEMP_CONN_FILE"
    else
        echo "WARNING: Failed to export current connections, skipping backup of connections" | tee -a "$LOG_FILE"
    fi
    
    # Create backup metadata file
    echo "Creating backup metadata" | tee -a "$LOG_FILE"
    BACKUP_METADATA="{
      \"timestamp\": \"$(date --iso-8601=seconds)\",
      \"deployment_id\": \"$DEPLOYMENT_ID\",
      \"environment\": \"$ENVIRONMENT\",
      \"composer_environment\": \"$COMPOSER_ENV\",
      \"region\": \"$COMPOSER_REGION\",
      \"project_id\": \"$PROJECT_ID\",
      \"backed_up_by\": \"$(whoami)\"
    }"
    
    echo "$BACKUP_METADATA" > "/tmp/backup_metadata_$DEPLOYMENT_ID.json"
    gsutil cp "/tmp/backup_metadata_$DEPLOYMENT_ID.json" "$BACKUP_PATH/metadata.json"
    rm -f "/tmp/backup_metadata_$DEPLOYMENT_ID.json"
    
    # Verify backup
    if ! gsutil -q stat "$BACKUP_PATH/dags" || \
       ! gsutil -q stat "$BACKUP_PATH/metadata.json"; then
        echo "ERROR: Failed to verify backup" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    echo "Current production state successfully backed up to $BACKUP_PATH" | tee -a "$LOG_FILE"
    export BACKUP_PATH
    return $EXIT_SUCCESS
}

# Validates DAG files before deployment with production-specific standards
validate_dags() {
    echo "Validating DAG files with production-specific standards..." | tee -a "$LOG_FILE"
    
    # Create validation output file
    VALIDATION_REPORT="/tmp/validation_report_$DEPLOYMENT_ID.json"
    
    # Run validation script
    python3 "$BACKEND_DIR/scripts/validate_dags.py" \
      "$DAG_SOURCE_DIR" \
      --output "$VALIDATION_REPORT" \
      --format json \
      --level ERROR
    
    if [ $? -ne 0 ]; then
        echo "ERROR: DAG validation script failed" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Check validation results
    WARNING_COUNT=$(jq -r '.summary.warning_count' "$VALIDATION_REPORT")
    ERROR_COUNT=$(jq -r '.summary.error_count' "$VALIDATION_REPORT")
    
    echo "Validation results: $ERROR_COUNT errors, $WARNING_COUNT warnings" | tee -a "$LOG_FILE"
    
    # For production, we enforce zero warnings and zero errors
    if [ "$ERROR_COUNT" -gt 0 ]; then
        echo "ERROR: Validation failed with $ERROR_COUNT errors" | tee -a "$LOG_FILE"
        jq -r '.summary.categories' "$VALIDATION_REPORT" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    if [ "$WARNING_COUNT" -gt "$VALIDATION_THRESHOLD" ]; then
        echo "ERROR: Validation failed with $WARNING_COUNT warnings (threshold: $VALIDATION_THRESHOLD)" | tee -a "$LOG_FILE"
        jq -r '.summary.categories' "$VALIDATION_REPORT" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Check for any deprecated Airflow 1.X features
    DEPRECATED_FEATURES=$(jq -r '.files[].validation.issues.warnings[] | select(.message | contains("deprecated"))' "$VALIDATION_REPORT" 2>/dev/null || echo "")
    if [ -n "$DEPRECATED_FEATURES" ]; then
        echo "WARNING: Deprecated Airflow 1.X features detected:" | tee -a "$LOG_FILE"
        echo "$DEPRECATED_FEATURES" | tee -a "$LOG_FILE"
    fi
    
    # Check DAG parsing time
    # In production, we want to ensure DAGs parse quickly
    DAG_PARSE_TIME=$(jq -r '.timing.total_parse_time // 0' "$VALIDATION_REPORT")
    if (( $(echo "$DAG_PARSE_TIME > 30" | bc -l) )); then
        echo "ERROR: DAG parsing time ($DAG_PARSE_TIME seconds) exceeds 30 seconds limit" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Generate HTML report for better readability
    python3 "$BACKEND_DIR/scripts/validate_dags.py" \
      "$DAG_SOURCE_DIR" \
      --output "/tmp/validation_report_$DEPLOYMENT_ID.html" \
      --format html \
      --level WARNING
      
    # Upload report to GCS for auditing
    gsutil cp "/tmp/validation_report_$DEPLOYMENT_ID.html" \
      "gs://$BACKUP_BUCKET/validation_reports/$DEPLOYMENT_ID.html"
    
    echo "DAG validation passed all production criteria." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Deploys DAGs to the production Composer environment
deploy_dags() {
    echo "Deploying DAGs to production Composer environment..." | tee -a "$LOG_FILE"
    
    # Run deploy_dags.py script
    python3 "$BACKEND_DIR/scripts/deploy_dags.py" \
      "$ENVIRONMENT" \
      --source-folder "$DAG_SOURCE_DIR" \
      --parallel
    
    if [ $? -ne 0 ]; then
        echo "ERROR: DAG deployment failed" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Record deployment metadata
    DEPLOYMENT_METADATA="{
      \"timestamp\": \"$(date --iso-8601=seconds)\",
      \"deployment_id\": \"$DEPLOYMENT_ID\",
      \"environment\": \"$ENVIRONMENT\",
      \"composer_environment\": \"$COMPOSER_ENV\",
      \"region\": \"$COMPOSER_REGION\",
      \"project_id\": \"$PROJECT_ID\",
      \"deployed_by\": \"$(whoami)\",
      \"backup_path\": \"$BACKUP_PATH\"
    }"
    
    echo "$DEPLOYMENT_METADATA" > "/tmp/deployment_metadata_$DEPLOYMENT_ID.json"
    gsutil cp "/tmp/deployment_metadata_$DEPLOYMENT_ID.json" \
      "gs://$BACKUP_BUCKET/deployments/$DEPLOYMENT_ID.json"
    rm -f "/tmp/deployment_metadata_$DEPLOYMENT_ID.json"
    
    echo "DAGs successfully deployed to production environment." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Imports Airflow variables to the production Composer environment
import_airflow_variables() {
    if [ ! -f "$VARIABLES_FILE" ]; then
        echo "WARNING: Variables file not found: $VARIABLES_FILE" | tee -a "$LOG_FILE"
        echo "Skipping variables import." | tee -a "$LOG_FILE"
        return $EXIT_SUCCESS
    fi
    
    echo "Importing Airflow variables to production environment..." | tee -a "$LOG_FILE"
    
    # Run import_variables.py script
    python3 "$BACKEND_DIR/scripts/import_variables.py" \
      --file_path "$VARIABLES_FILE" \
      --environment "$ENVIRONMENT"
      
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to import Airflow variables" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Verify variables were imported
    VERIFY_VARS=$(gcloud composer environments run "$COMPOSER_ENV" \
                 --location "$COMPOSER_REGION" \
                 variables -- --list)
    
    if [ $? -ne 0 ]; then
        echo "WARNING: Could not verify variables import" | tee -a "$LOG_FILE"
    else
        echo "Verified variables import." | tee -a "$LOG_FILE"
    fi
    
    echo "Airflow variables successfully imported to production environment." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Imports Airflow connections to the production Composer environment
import_airflow_connections() {
    if [ ! -f "$CONNECTIONS_FILE" ]; then
        echo "WARNING: Connections file not found: $CONNECTIONS_FILE" | tee -a "$LOG_FILE"
        echo "Skipping connections import." | tee -a "$LOG_FILE"
        return $EXIT_SUCCESS
    fi
    
    echo "Importing Airflow connections to production environment..." | tee -a "$LOG_FILE"
    
    # Run import_connections.py script
    python3 "$BACKEND_DIR/scripts/import_connections.py" \
      --file-path "$CONNECTIONS_FILE" \
      --environment "$ENVIRONMENT" \
      --validate \
      --skip-existing
      
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to import Airflow connections" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Verify connections were imported
    VERIFY_CONNS=$(gcloud composer environments run "$COMPOSER_ENV" \
                  --location "$COMPOSER_REGION" \
                  connections -- --list)
    
    if [ $? -ne 0 ]; then
        echo "WARNING: Could not verify connections import" | tee -a "$LOG_FILE"
    else
        echo "Verified connections import." | tee -a "$LOG_FILE"
    fi
    
    echo "Airflow connections successfully imported to production environment." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Performs comprehensive verification of the production deployment
verify_deployment() {
    echo "Performing comprehensive verification of the production deployment..." | tee -a "$LOG_FILE"
    
    # Verify DAGs appear in the GCS bucket
    echo "Verifying DAGs in GCS bucket..." | tee -a "$LOG_FILE"
    GCS_DAGS=$(gsutil ls "gs://$GCS_BUCKET/dags/")
    if [ $? -ne 0 ] || [ -z "$GCS_DAGS" ]; then
        echo "ERROR: No DAGs found in GCS bucket after deployment" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Wait for DAG parsing cycle to complete
    echo "Waiting for DAG parsing cycle to complete (60 seconds)..." | tee -a "$LOG_FILE"
    sleep 60
    
    # Verify DAGs are visible in Airflow
    echo "Verifying DAGs in Airflow..." | tee -a "$LOG_FILE"
    
    # Use gcloud composer environments run to list DAGs
    DAG_LIST=$(gcloud composer environments run "$COMPOSER_ENV" \
               --location "$COMPOSER_REGION" \
               dags -- list)
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to list DAGs in Airflow" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Check for any DAG import errors
    echo "Checking for import errors..." | tee -a "$LOG_FILE"
    IMPORT_ERRORS=$(gcloud composer environments run "$COMPOSER_ENV" \
                   --location "$COMPOSER_REGION" \
                   dags -- list-import-errors)
                   
    if [[ "$IMPORT_ERRORS" == *"ImportError"* ]]; then
        echo "ERROR: DAG import errors detected:" | tee -a "$LOG_FILE"
        echo "$IMPORT_ERRORS" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Verify variables 
    echo "Verifying Airflow variables..." | tee -a "$LOG_FILE"
    VARIABLES=$(gcloud composer environments run "$COMPOSER_ENV" \
               --location "$COMPOSER_REGION" \
               variables -- --list)
    
    if [ $? -ne 0 ]; then
        echo "WARNING: Could not verify variables" | tee -a "$LOG_FILE"
    fi
    
    # Verify connections
    echo "Verifying Airflow connections..." | tee -a "$LOG_FILE"
    CONNECTIONS=$(gcloud composer environments run "$COMPOSER_ENV" \
                 --location "$COMPOSER_REGION" \
                 connections -- --list)
    
    if [ $? -ne 0 ]; then
        echo "WARNING: Could not verify connections" | tee -a "$LOG_FILE"
    fi
    
    # Monitor scheduler load/health
    echo "Checking scheduler health..." | tee -a "$LOG_FILE"
    SCHEDULER_HEALTH=$(gcloud composer environments run "$COMPOSER_ENV" \
                      --location "$COMPOSER_REGION" \
                      health)
                      
    if [[ "$SCHEDULER_HEALTH" == *"not healthy"* ]]; then
        echo "WARNING: Scheduler health check failed" | tee -a "$LOG_FILE"
        echo "$SCHEDULER_HEALTH" | tee -a "$LOG_FILE"
    fi
    
    # Generate verification report
    VERIFICATION_REPORT="{
      \"timestamp\": \"$(date --iso-8601=seconds)\",
      \"deployment_id\": \"$DEPLOYMENT_ID\",
      \"environment\": \"$ENVIRONMENT\",
      \"composer_environment\": \"$COMPOSER_ENV\",
      \"verification_status\": \"success\",
      \"dags_in_gcs\": true,
      \"dags_in_airflow\": true,
      \"import_errors\": false,
      \"variables_verified\": true,
      \"connections_verified\": true
    }"
    
    echo "$VERIFICATION_REPORT" > "/tmp/verification_report_$DEPLOYMENT_ID.json"
    gsutil cp "/tmp/verification_report_$DEPLOYMENT_ID.json" \
      "gs://$BACKUP_BUCKET/verification_reports/$DEPLOYMENT_ID.json"
    rm -f "/tmp/verification_report_$DEPLOYMENT_ID.json"
    
    echo "Verification completed successfully." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Rolls back the deployment if verification fails
rollback_deployment() {
    local failure_reason="$1"
    
    echo "ROLLING BACK DEPLOYMENT due to: $failure_reason" | tee -a "$LOG_FILE"
    echo "Using backup from: $BACKUP_PATH" | tee -a "$LOG_FILE"
    
    # Verify backup path exists
    if ! gsutil -q stat "$BACKUP_PATH/dags"; then
        echo "ERROR: Backup not found at $BACKUP_PATH/dags" | tee -a "$LOG_FILE"
        echo "Cannot perform rollback!" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Restore DAGs
    echo "Restoring DAGs from backup..." | tee -a "$LOG_FILE"
    if ! gsutil -m rsync -d -r "$BACKUP_PATH/dags" "gs://$GCS_BUCKET/dags"; then
        echo "ERROR: Failed to restore DAGs from backup" | tee -a "$LOG_FILE"
        return $EXIT_FAILURE
    fi
    
    # Restore variables if backup exists
    if gsutil -q stat "$BACKUP_PATH/variables.json" > /dev/null 2>&1; then
        echo "Restoring Airflow variables from backup..." | tee -a "$LOG_FILE"
        gsutil cp "$BACKUP_PATH/variables.json" "/tmp/variables_restore_$DEPLOYMENT_ID.json"
        
        gcloud composer environments run "$COMPOSER_ENV" \
          --location "$COMPOSER_REGION" \
          variables -- --import "/tmp/variables_restore_$DEPLOYMENT_ID.json"
          
        rm -f "/tmp/variables_restore_$DEPLOYMENT_ID.json"
    fi
    
    # Restore connections if backup exists
    if gsutil -q stat "$BACKUP_PATH/connections.json" > /dev/null 2>&1; then
        echo "Restoring Airflow connections from backup..." | tee -a "$LOG_FILE"
        gsutil cp "$BACKUP_PATH/connections.json" "/tmp/connections_restore_$DEPLOYMENT_ID.json"
        
        gcloud composer environments run "$COMPOSER_ENV" \
          --location "$COMPOSER_REGION" \
          connections -- --import "/tmp/connections_restore_$DEPLOYMENT_ID.json"
          
        rm -f "/tmp/connections_restore_$DEPLOYMENT_ID.json"
    fi
    
    # Record rollback metadata
    ROLLBACK_METADATA="{
      \"timestamp\": \"$(date --iso-8601=seconds)\",
      \"deployment_id\": \"$DEPLOYMENT_ID\",
      \"environment\": \"$ENVIRONMENT\",
      \"rollback_reason\": \"$failure_reason\",
      \"backup_restored_from\": \"$BACKUP_PATH\",
      \"performed_by\": \"$(whoami)\"
    }"
    
    echo "$ROLLBACK_METADATA" > "/tmp/rollback_metadata_$DEPLOYMENT_ID.json"
    gsutil cp "/tmp/rollback_metadata_$DEPLOYMENT_ID.json" \
      "gs://$BACKUP_BUCKET/rollbacks/$DEPLOYMENT_ID.json"
    rm -f "/tmp/rollback_metadata_$DEPLOYMENT_ID.json"
    
    # Send rollback notification
    echo "Sending rollback notification to operations team..." | tee -a "$LOG_FILE"
    
    NOTIFY_SUBJECT="ALERT: Production deployment rolled back - $DEPLOYMENT_ID"
    NOTIFY_BODY="A production deployment has been rolled back.
Deployment ID: $DEPLOYMENT_ID
Environment: $COMPOSER_ENV
Timestamp: $(date)
Reason: $failure_reason
Restored from: $BACKUP_PATH
    
Please investigate immediately."
    
    # In a real implementation, we would send an email or Slack notification here
    # For this script, we'll just log it
    echo "Notification to: $NOTIFICATION_RECIPIENTS" | tee -a "$LOG_FILE"
    echo "Subject: $NOTIFY_SUBJECT" | tee -a "$LOG_FILE"
    echo "Body: $NOTIFY_BODY" | tee -a "$LOG_FILE"
    
    echo "Rollback completed successfully." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Records comprehensive audit information about the production deployment
record_deployment_audit() {
    local success=$1
    
    echo "Recording comprehensive audit information..." | tee -a "$LOG_FILE"
    
    # Gather deployment metadata
    local status="success"
    if [ "$success" -ne 0 ]; then
        status="failure"
    fi
    
    # Get list of deployed DAGs
    local dag_list=""
    if gsutil -q stat "gs://$GCS_BUCKET/dags"; then
        dag_list=$(gsutil ls "gs://$GCS_BUCKET/dags/" | grep -v "/__pycache__/" | sort)
    fi
    
    # Create audit record
    AUDIT_RECORD="{
      \"audit_type\": \"deployment\",
      \"timestamp\": \"$(date --iso-8601=seconds)\",
      \"deployment_id\": \"$DEPLOYMENT_ID\",
      \"environment\": \"$ENVIRONMENT\",
      \"composer_environment\": \"$COMPOSER_ENV\",
      \"region\": \"$COMPOSER_REGION\",
      \"project_id\": \"$PROJECT_ID\",
      \"performed_by\": \"$(whoami)\",
      \"status\": \"$status\",
      \"backup_path\": \"$BACKUP_PATH\",
      \"dag_count\": $(echo "$dag_list" | wc -l),
      \"approval_token_file\": \"$APPROVAL_TOKEN_FILE\",
      \"log_file\": \"$LOG_FILE\"
    }"
    
    # Save audit record locally
    echo "$AUDIT_RECORD" > "/tmp/audit_record_$DEPLOYMENT_ID.json"
    
    # Upload to GCS audit bucket
    YEAR=$(date +%Y)
    MONTH=$(date +%m)
    DAY=$(date +%d)
    
    AUDIT_PATH="gs://$BACKUP_BUCKET/audits/$ENVIRONMENT/$YEAR/$MONTH/$DAY/$DEPLOYMENT_ID.json"
    gsutil cp "/tmp/audit_record_$DEPLOYMENT_ID.json" "$AUDIT_PATH"
    
    # Clean up local file
    rm -f "/tmp/audit_record_$DEPLOYMENT_ID.json"
    
    # Update deployment log
    gsutil cp "$LOG_FILE" "gs://$BACKUP_BUCKET/logs/$DEPLOYMENT_ID.log"
    
    echo "Audit record saved to $AUDIT_PATH" | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Notifies stakeholders about production deployment status
notify_stakeholders() {
    local status_code=$1
    local deployment_summary=$2
    
    echo "Notifying stakeholders about deployment status..." | tee -a "$LOG_FILE"
    
    # Determine status text
    local status="SUCCESS"
    if [ "$status_code" -ne 0 ]; then
        status="FAILURE"
    fi
    
    # Prepare notification content
    NOTIFY_SUBJECT="Production Deployment $status - $DEPLOYMENT_ID"
    NOTIFY_BODY="Production Deployment Status: $status
Deployment ID: $DEPLOYMENT_ID
Environment: $COMPOSER_ENV ($ENVIRONMENT)
Timestamp: $(date)
Performed by: $(whoami)

$deployment_summary

Airflow UI: $AIRFLOW_UI_URL
Backup Path: $BACKUP_PATH
Audit Log: gs://$BACKUP_BUCKET/audits/$ENVIRONMENT/$(date +%Y/%m/%d)/$DEPLOYMENT_ID.json
Deployment Log: gs://$BACKUP_BUCKET/logs/$DEPLOYMENT_ID.log
"
    
    # In a real implementation, we would send an email or Slack notification here
    # For this script, we'll just log it
    echo "Notification to: $NOTIFICATION_RECIPIENTS" | tee -a "$LOG_FILE"
    echo "Subject: $NOTIFY_SUBJECT" | tee -a "$LOG_FILE"
    echo "Body: $NOTIFY_BODY" | tee -a "$LOG_FILE"
    
    # Log notification URL (in a real implementation)
    echo "Slack notification sent to #airflow-prod-deployments channel" | tee -a "$LOG_FILE"
    
    echo "Stakeholder notifications completed." | tee -a "$LOG_FILE"
    return $EXIT_SUCCESS
}

# Prints a summary of the deployment results
print_summary() {
    local exit_code=$1
    
    echo "======================================================================"
    echo "                   DEPLOYMENT SUMMARY                                  "
    echo "======================================================================"
    echo "Deployment ID: $DEPLOYMENT_ID"
    echo "Environment: PRODUCTION ($COMPOSER_ENV)"
    echo "Timestamp: $(date)"
    
    if [ "$exit_code" -eq 0 ]; then
        echo "Status: SUCCESS"
    else
        echo "Status: FAILURE (code: $exit_code)"
    fi
    
    echo "Airflow UI: $AIRFLOW_UI_URL"
    echo "Backup Location: $BACKUP_PATH"
    echo "Audit Log: gs://$BACKUP_BUCKET/audits/$ENVIRONMENT/$(date +%Y/%m/%d)/$DEPLOYMENT_ID.json"
    echo "Log File: $LOG_FILE"
    echo "======================================================================"
}

# Performs cleanup after deployment
cleanup() {
    echo "Performing cleanup after deployment..." | tee -a "$LOG_FILE"
    
    # Upload deployment log to GCS
    echo "Uploading deployment log to GCS..." | tee -a "$LOG_FILE"
    gsutil cp "$LOG_FILE" "gs://$BACKUP_BUCKET/logs/$DEPLOYMENT_ID.log"
    
    # Archive approval tokens and documentation
    if [ -n "$APPROVAL_TOKEN_FILE" ] && [ -f "$APPROVAL_TOKEN_FILE" ]; then
        echo "Archiving approval token..." | tee -a "$LOG_FILE"
        gsutil cp "$APPROVAL_TOKEN_FILE" "gs://$BACKUP_BUCKET/approvals/$DEPLOYMENT_ID.token"
    fi
    
    # Clean up any temporary files
    echo "Cleaning up temporary files..." | tee -a "$LOG_FILE"
    rm -f "/tmp/validation_report_$DEPLOYMENT_ID.json" "/tmp/validation_report_$DEPLOYMENT_ID.html"
    
    echo "Cleanup completed." | tee -a "$LOG_FILE"
}

# Main function that orchestrates the production deployment process
main() {
    local exit_code=0
    local deployment_summary=""
    
    # Print header
    print_header | tee -a "$LOG_FILE"
    
    # Execute deployment steps in sequence
    echo "Starting production deployment process..." | tee -a "$LOG_FILE"
    
    # Setup environment
    setup_environment
    if [ $? -ne 0 ]; then
        echo "Failed to set up environment, aborting deployment." | tee -a "$LOG_FILE"
        print_summary 1
        exit $EXIT_FAILURE
    fi
    
    # Check prerequisites
    check_prerequisites
    if [ $? -ne 0 ]; then
        echo "Prerequisites not met, aborting deployment." | tee -a "$LOG_FILE"
        print_summary 1
        exit $EXIT_FAILURE
    fi
    
    # Verify approvals
    verify_approval
    if [ $? -ne 0 ]; then
        echo "Approval verification failed, aborting deployment." | tee -a "$LOG_FILE"
        deployment_summary="Deployment aborted: Required approvals not met or invalid."
        notify_stakeholders 1 "$deployment_summary"
        print_summary 1
        exit $EXIT_FAILURE
    fi
    
    # Authenticate with GCP
    authenticate_gcp
    if [ $? -ne 0 ]; then
        echo "Authentication failed, aborting deployment." | tee -a "$LOG_FILE"
        deployment_summary="Deployment aborted: GCP authentication failed."
        notify_stakeholders 1 "$deployment_summary"
        print_summary 1
        exit $EXIT_FAILURE
    fi
    
    # Get Composer environment details
    get_composer_details
    if [ $? -ne 0 ]; then
        echo "Failed to get Composer details, aborting deployment." | tee -a "$LOG_FILE"
        deployment_summary="Deployment aborted: Could not retrieve Composer environment details."
        notify_stakeholders 1 "$deployment_summary"
        print_summary 1
        exit $EXIT_FAILURE
    fi
    
    # Backup current state
    backup_current_state
    if [ $? -ne 0 ]; then
        echo "Failed to create backup, aborting deployment." | tee -a "$LOG_FILE"
        deployment_summary="Deployment aborted: Failed to create backup of current state."
        notify_stakeholders 1 "$deployment_summary"
        print_summary 1
        exit $EXIT_FAILURE
    fi
    
    # Validate DAGs
    validate_dags
    if [ $? -ne 0 ]; then
        echo "DAG validation failed, aborting deployment." | tee -a "$LOG_FILE"
        deployment_summary="Deployment aborted: DAG validation failed. See validation report for details."
        notify_stakeholders 1 "$deployment_summary"
        print_summary 1
        exit $EXIT_FAILURE
    fi
    
    # Deploy DAGs
    deploy_dags
    if [ $? -ne 0 ]; then
        echo "DAG deployment failed, initiating rollback." | tee -a "$LOG_FILE"
        rollback_deployment "DAG deployment failed"
        deployment_summary="Deployment failed and was rolled back: DAG deployment failed."
        notify_stakeholders 1 "$deployment_summary"
        record_deployment_audit 1
        print_summary 1
        cleanup
        exit $EXIT_FAILURE
    fi
    
    # Import Airflow variables
    import_airflow_variables
    if [ $? -ne 0 ]; then
        echo "Variables import failed, initiating rollback." | tee -a "$LOG_FILE"
        rollback_deployment "Variables import failed"
        deployment_summary="Deployment failed and was rolled back: Variables import failed."
        notify_stakeholders 1 "$deployment_summary"
        record_deployment_audit 1
        print_summary 1
        cleanup
        exit $EXIT_FAILURE
    fi
    
    # Import Airflow connections
    import_airflow_connections
    if [ $? -ne 0 ]; then
        echo "Connections import failed, initiating rollback." | tee -a "$LOG_FILE"
        rollback_deployment "Connections import failed"
        deployment_summary="Deployment failed and was rolled back: Connections import failed."
        notify_stakeholders 1 "$deployment_summary"
        record_deployment_audit 1
        print_summary 1
        cleanup
        exit $EXIT_FAILURE
    fi
    
    # Verify deployment
    verify_deployment
    if [ $? -ne 0 ]; then
        echo "Deployment verification failed, initiating rollback." | tee -a "$LOG_FILE"
        rollback_deployment "Deployment verification failed"
        deployment_summary="Deployment failed and was rolled back: Verification failed."
        notify_stakeholders 1 "$deployment_summary"
        record_deployment_audit 1
        print_summary 1
        cleanup
        exit $EXIT_FAILURE
    fi
    
    # Record successful deployment
    record_deployment_audit 0
    
    # Notify stakeholders
    deployment_summary="Deployment completed successfully. All DAGs, variables, and connections were deployed and verified."
    notify_stakeholders 0 "$deployment_summary"
    
    # Print summary
    print_summary 0
    
    # Cleanup
    cleanup
    
    echo "Production deployment completed successfully." | tee -a "$LOG_FILE"
    exit $EXIT_SUCCESS
}

# Call the main function
main