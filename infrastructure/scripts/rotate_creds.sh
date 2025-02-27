#!/bin/bash
#
# Bash script that automates the rotation of credentials across all Cloud Composer 2 environments.
# It handles the secure rotation of database connections, API keys, and service account credentials
# while updating all necessary configuration files and secrets in GCP Secret Manager.
#
# Version: 1.0
# Last Updated: 2023-08-15
#

# Exit on any error
set -e

# Determine script directory for relative paths
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
CONFIG_DIR="${REPO_ROOT}/src/backend/config"
SECRETS_SCRIPT="${REPO_ROOT}/src/backend/scripts/rotate_secrets.py"
ENVIRONMENTS=('dev' 'qa' 'prod')
DRY_RUN=false
VERBOSE=false
FORCE_ROTATION=false
LOG_FILE="${SCRIPT_DIR}/rotate_creds.log"

# Display usage information
usage() {
    echo "Usage: $(basename $0) [options]"
    echo
    echo "Automates credential rotation across Cloud Composer 2 environments."
    echo
    echo "Options:"
    echo "  -e ENV        Specify environment (dev, qa, prod)"
    echo "  -d            Dry run (does not make actual changes)"
    echo "  -v            Verbose output"
    echo "  -f            Force rotation (ignore age check)"
    echo "  -s SECRET_ID  Rotate specific secret only"
    echo "  -h            Show this help message"
    echo
    echo "Examples:"
    echo "  $(basename $0) -e dev                # Rotate all credentials in dev environment"
    echo "  $(basename $0) -e prod -d            # Dry run for production environment"
    echo "  $(basename $0) -e qa -s api_key_main # Rotate specific secret in QA environment"
    echo "  $(basename $0) -f                    # Force rotation of all creds in all environments"
}

# Log message to both console and log file
log() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local message="[${timestamp}] $1"
    echo "${message}"
    echo "${message}" >> "${LOG_FILE}"
}

# Log error and optionally exit
error() {
    local message="[ERROR] $1"
    echo "${message}" >&2
    echo "${message}" >> "${LOG_FILE}"
    if [ -n "$2" ]; then
        exit $2
    fi
}

# Check if required dependencies are installed
check_dependencies() {
    log "Checking required dependencies..."
    
    # Check for gcloud
    if ! command -v gcloud &> /dev/null; then
        error "gcloud CLI is not installed. Please install Google Cloud SDK." 2
        return 1
    fi
    
    # Check for python3
    if ! command -v python3 &> /dev/null; then
        error "python3 is not installed. Please install Python 3.8 or higher." 2
        return 1
    fi
    
    # Check python version
    local python_version=$(python3 --version | cut -d' ' -f2)
    if [[ "$(echo -e "3.8\n$python_version" | sort -V | head -n1)" != "3.8" ]]; then
        error "Python version must be 3.8 or higher. Found: $python_version" 2
        return 1
    fi
    
    # Check for jq
    if ! command -v jq &> /dev/null; then
        error "jq is not installed. Please install jq for JSON processing." 2
        return 1
    fi
    
    # Check required Python packages
    if ! python3 -c "import google.cloud.secretmanager" &> /dev/null; then
        error "Required Python package 'google-cloud-secretmanager' is not installed." 2
        return 1
    fi
    
    log "All dependencies are installed."
    return 0
}

# Verify GCP authentication and project access
verify_gcp_auth() {
    local environment=$1
    
    log "Verifying GCP authentication for environment: $environment"
    
    # Get project ID from environment config
    local config_file="${CONFIG_DIR}/secrets_config_${environment}.json"
    local yaml_config="${CONFIG_DIR}/secrets_config_${environment}.yaml"
    local project_id=""
    
    if [ -f "$config_file" ]; then
        project_id=$(jq -r '.project_id // empty' "$config_file")
    elif [ -f "$yaml_config" ]; then
        # Extract project_id from YAML using Python
        project_id=$(python3 -c "import yaml; print(yaml.safe_load(open('$yaml_config'))['project_id'])" 2>/dev/null || echo "")
    fi
    
    if [ -z "$project_id" ]; then
        error "Could not determine project ID for environment: $environment" 3
        return 1
    fi
    
    # Check if user is authenticated
    if ! gcloud auth print-identity-token &>/dev/null; then
        error "Not authenticated with Google Cloud. Please run 'gcloud auth login'." 3
        return 1
    fi
    
    # Check if user has access to the project
    if ! gcloud projects describe "$project_id" &>/dev/null; then
        error "You don't have access to project: $project_id" 3
        return 1
    fi
    
    # Verify Secret Manager API is enabled
    if ! gcloud services list --project="$project_id" --filter="NAME:secretmanager.googleapis.com" --format="value(NAME)" | grep -q "secretmanager.googleapis.com"; then
        error "Secret Manager API is not enabled for project: $project_id" 3
        return 1
    fi
    
    log "GCP authentication verified for project: $project_id"
    return 0
}

# Get list of connection names from connections.json
get_connection_names() {
    local environment=$1
    local conn_file="${CONFIG_DIR}/connections_${environment}.json"
    
    if [ ! -f "$conn_file" ]; then
        log "Connections file not found: $conn_file"
        return 0
    fi
    
    local connections=$(jq -r 'keys | .[]' "$conn_file" 2>/dev/null || echo "")
    echo "$connections"
}

# Rotate credentials for a specific environment
rotate_environment_creds() {
    local environment=$1
    
    log "Starting credential rotation for environment: $environment"
    
    # Verify GCP authentication for this environment
    if ! verify_gcp_auth "$environment"; then
        error "GCP authentication failed for environment: $environment" 3
        return 1
    fi
    
    # Build command arguments
    local cmd_args="--env $environment"
    
    if [ "$DRY_RUN" = true ]; then
        cmd_args="$cmd_args --dry-run"
    fi
    
    if [ "$VERBOSE" = true ]; then
        cmd_args="$cmd_args --verbose"
    fi
    
    if [ "$FORCE_ROTATION" = true ]; then
        cmd_args="$cmd_args --force"
    fi
    
    if [ -n "$SPECIFIC_SECRET" ]; then
        cmd_args="$cmd_args --secret-id $SPECIFIC_SECRET"
    fi
    
    # Set config path
    cmd_args="$cmd_args --config-path $CONFIG_DIR"
    
    log "Executing rotation script with arguments: $cmd_args"
    
    # Execute the Python rotation script
    if [ "$VERBOSE" = true ]; then
        python3 "$SECRETS_SCRIPT" $cmd_args
    else
        python3 "$SECRETS_SCRIPT" $cmd_args >/dev/null
    fi
    
    local exit_status=$?
    if [ $exit_status -ne 0 ]; then
        error "Rotation script failed for environment: $environment with exit code: $exit_status" 4
        return 1
    fi
    
    log "Successfully completed credential rotation for environment: $environment"
    return 0
}

# Parse command line arguments
parse_args() {
    local OPTIND
    while getopts "e:dvfs:h" opt; do
        case $opt in
            e) # Environment
                local specified_env="$OPTARG"
                # Validate environment
                local valid_env=false
                for env in "${ENVIRONMENTS[@]}"; do
                    if [ "$env" = "$specified_env" ]; then
                        valid_env=true
                        break
                    fi
                done
                
                if [ "$valid_env" = false ]; then
                    error "Invalid environment: $specified_env. Must be one of: ${ENVIRONMENTS[*]}" 1
                fi
                
                ENVIRONMENTS=("$specified_env")
                ;;
            d) # Dry run
                DRY_RUN=true
                ;;
            v) # Verbose
                VERBOSE=true
                ;;
            f) # Force rotation
                FORCE_ROTATION=true
                ;;
            s) # Specific secret
                SPECIFIC_SECRET="$OPTARG"
                ;;
            h) # Help
                usage
                exit 0
                ;;
            \?)
                error "Invalid option: -$OPTARG" 1
                ;;
        esac
    done
}

# Main function
main() {
    # Parse command line arguments
    parse_args "$@"
    
    # Initialize log file
    echo "--- Credential Rotation Log $(date '+%Y-%m-%d %H:%M:%S') ---" > "$LOG_FILE"
    
    # Check dependencies
    check_dependencies || exit $?
    
    log "Starting credential rotation process"
    log "Operating on environments: ${ENVIRONMENTS[*]}"
    
    if [ "$DRY_RUN" = true ]; then
        log "DRY RUN MODE: No actual changes will be made"
    fi
    
    if [ -n "$SPECIFIC_SECRET" ]; then
        log "Rotating specific secret: $SPECIFIC_SECRET"
    fi
    
    local overall_status=0
    local failed_envs=()
    
    # Process each environment
    for env in "${ENVIRONMENTS[@]}"; do
        if ! rotate_environment_creds "$env"; then
            overall_status=1
            failed_envs+=("$env")
        fi
    done
    
    # Final status message
    if [ $overall_status -eq 0 ]; then
        log "Credential rotation completed successfully for all environments"
    else
        error "Credential rotation failed for environments: ${failed_envs[*]}" 4
    fi
    
    log "Log file: $LOG_FILE"
    return $overall_status
}

# Execute main function
main "$@"
exit $?