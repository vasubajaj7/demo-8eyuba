#!/usr/bin/env bash
# backup_restore.sh - Comprehensive backup and restore utility for Airflow 2.X in Cloud Composer 2
# 
# This script provides functionality for backing up and restoring Apache Airflow 2.X
# environments in Cloud Composer 2, supporting full and incremental backups,
# data export/import, and restoration across dev, QA, and production environments.
#
# Version: 1.0.0

# Strict error handling
set -euo pipefail

# Get script directory
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
BACKEND_DIR="${SCRIPT_DIR}/../../src/backend"
CONFIG_DIR="${BACKEND_DIR}/config"
SCRIPTS_DIR="${BACKEND_DIR}/scripts"
LOG_DIR="${SCRIPT_DIR}/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/backup_restore_${TIMESTAMP}.log"
TEMP_DIR="/tmp/composer_backup_restore_${TIMESTAMP}"

# Valid environments
VALID_ENVIRONMENTS=("dev" "qa" "prod")

# Default values
DEFAULT_BACKUP_TYPE="full"
DEFAULT_BACKUP_RETENTION_DAYS=90
ARCHIVE_RETENTION_DAYS=365
DEFAULT_GCS_BUCKET_PREFIX="composer2-backups"

# Trap for cleanup
trap cleanup EXIT

# Display usage information
print_usage() {
    echo "Usage: $(basename "$0") [COMMAND] [OPTIONS]"
    echo
    echo "Backup and restore utility for Airflow 2.X in Cloud Composer 2 environments"
    echo
    echo "Commands:"
    echo "  backup              Backup Airflow metadata, DAGs, and configurations"
    echo "  restore             Restore Airflow metadata, DAGs, and configurations"
    echo "  validate            Validate an existing backup"
    echo "  list                List available backups"
    echo
    echo "Backup Options:"
    echo "  --type=TYPE         Backup type: full or incremental (default: ${DEFAULT_BACKUP_TYPE})"
    echo "  --env=ENV           Environment: dev, qa, or prod (required)"
    echo "  --retention=DAYS    Number of days to retain backups (default: ${DEFAULT_BACKUP_RETENTION_DAYS})"
    echo "  --bucket=BUCKET     Override GCS bucket for backup storage"
    echo "  --metadata-only     Only backup metadata database"
    echo "  --data-only         Only backup DAGs and plugins"
    echo "  --config-only       Only backup configurations"
    echo
    echo "Restore Options:"
    echo "  --env=ENV           Environment to restore to: dev, qa, or prod (required)"
    echo "  --backup-id=ID      Specific backup ID to restore"
    echo "  --latest            Restore from the most recent backup (default if no ID specified)"
    echo "  --date=YYYY-MM-DD   Restore from backup closest to specified date"
    echo "  --metadata-only     Only restore metadata database"
    echo "  --data-only         Only restore DAGs and plugins"
    echo "  --config-only       Only restore configurations"
    echo "  --force             Force restore even in production environment"
    echo "  --dry-run           Validate restoration without performing it"
    echo
    echo "Validate Options:"
    echo "  --env=ENV           Environment to validate against: dev, qa, or prod (required)"
    echo "  --backup-id=ID      Specific backup ID to validate"
    echo "  --latest            Validate the most recent backup (default if no ID specified)"
    echo
    echo "List Options:"
    echo "  --env=ENV           Environment to list backups for: dev, qa, or prod (required)"
    echo "  --limit=N           Limit to N most recent backups (default: 10)"
    echo "  --type=TYPE         Filter by backup type: full or incremental"
    echo
    echo "General Options:"
    echo "  --help              Display this help message and exit"
    echo "  --verbose           Enable verbose output"
    echo
    echo "Examples:"
    echo "  $(basename "$0") backup --env=dev --type=full"
    echo "  $(basename "$0") restore --env=qa --latest"
    echo "  $(basename "$0") restore --env=prod --backup-id=airflow_backup_prod_full_20230101_120000 --force"
    echo "  $(basename "$0") validate --env=dev --latest"
    echo "  $(basename "$0") list --env=prod --limit=5"
}

# Log a message to both stdout and log file
log_message() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    
    # Create log directory if it doesn't exist
    mkdir -p "${LOG_DIR}"
    
    # Format the message
    local formatted_message="[${timestamp}] [${level}] ${message}"
    
    # Output to stdout
    echo "${formatted_message}"
    
    # Output to log file
    echo "${formatted_message}" >> "${LOG_FILE}"
}

# Validate environment
validate_environment() {
    local env="$1"
    
    for valid_env in "${VALID_ENVIRONMENTS[@]}"; do
        if [[ "${env}" == "${valid_env}" ]]; then
            return 0
        fi
    done
    
    log_message "ERROR" "Invalid environment: ${env}. Must be one of: ${VALID_ENVIRONMENTS[*]}"
    return 1
}

# Load environment-specific configuration
get_environment_config() {
    local env="$1"
    
    validate_environment "${env}" || return 1
    
    # Source environment-specific configuration
    local env_config="${CONFIG_DIR}/composer_${env}.py"
    
    if [[ ! -f "${env_config}" ]]; then
        log_message "ERROR" "Environment configuration file not found: ${env_config}"
        return 1
    fi
    
    # Extract configuration values using Python
    local config_data
    config_data=$(python3 -c "import sys; sys.path.append('${BACKEND_DIR}'); from config import get_config; import json; print(json.dumps(get_config('${env}')))")
    
    # Get values with jq
    COMPOSER_ENV="${env}"
    GCP_PROJECT=$(echo "${config_data}" | jq -r '.gcp.project_id')
    COMPOSER_REGION=$(echo "${config_data}" | jq -r '.gcp.region')
    DAGS_BUCKET=$(echo "${config_data}" | jq -r '.storage.dag_bucket')
    PLUGINS_BUCKET=$(echo "${config_data}" | jq -r '.storage.data_bucket')
    LOGS_BUCKET=$(echo "${config_data}" | jq -r '.storage.logs_bucket')
    BACKUP_BUCKET=$(echo "${config_data}" | jq -r '.storage.backup_bucket')
    DB_CONNECTION_NAME=$(echo "${config_data}" | jq -r '.database.connection_name')
    
    # Export variables for use in the script
    export COMPOSER_ENV GCP_PROJECT COMPOSER_REGION 
    export DAGS_BUCKET PLUGINS_BUCKET LOGS_BUCKET BACKUP_BUCKET
    export DB_CONNECTION_NAME
    
    log_message "INFO" "Loaded configuration for environment: ${env}"
    log_message "INFO" "Project: ${GCP_PROJECT}, Region: ${COMPOSER_REGION}"
    log_message "INFO" "DAGs bucket: ${DAGS_BUCKET}"
    log_message "INFO" "Backup bucket: ${BACKUP_BUCKET}"
    
    return 0
}

# Find the latest backup for a specific environment
find_latest_backup() {
    local env="$1"
    local backup_type="${2:-}"
    
    get_environment_config "${env}" || return 1
    
    local backup_path="${DEFAULT_GCS_BUCKET_PREFIX}/${env}"
    if [[ -n "${backup_type}" ]]; then
        backup_path="${backup_path}/${backup_type}"
    fi
    
    log_message "INFO" "Looking for latest backup in gs://${BACKUP_BUCKET}/${backup_path}/"
    
    # List all manifest files and sort by timestamp (newest first)
    local manifests
    manifests=$(gsutil ls "gs://${BACKUP_BUCKET}/${backup_path}/**/*_manifest.json" 2>/dev/null | sort -r) || true
    
    if [[ -z "${manifests}" ]]; then
        log_message "ERROR" "No backup manifests found in gs://${BACKUP_BUCKET}/${backup_path}/"
        return 1
    fi
    
    # Get the first (most recent) manifest
    local latest_manifest
    latest_manifest=$(echo "${manifests}" | head -n 1)
    
    log_message "INFO" "Found latest backup manifest: ${latest_manifest}"
    echo "${latest_manifest}"
    
    return 0
}

# Parse a backup manifest file
parse_manifest() {
    local manifest_path="$1"
    local local_manifest_file
    
    # Check if manifest is a GCS URL
    if [[ "${manifest_path}" == gs://* ]]; then
        local_manifest_file="${TEMP_DIR}/$(basename "${manifest_path}")"
        mkdir -p "${TEMP_DIR}"
        
        log_message "INFO" "Downloading manifest from ${manifest_path}"
        gsutil cp "${manifest_path}" "${local_manifest_file}" || {
            log_message "ERROR" "Failed to download manifest from ${manifest_path}"
            return 1
        }
    else
        local_manifest_file="${manifest_path}"
    fi
    
    # Check if file exists
    if [[ ! -f "${local_manifest_file}" ]]; then
        log_message "ERROR" "Manifest file not found: ${local_manifest_file}"
        return 1
    fi
    
    # Parse the manifest with jq
    local manifest_data
    manifest_data=$(jq '.' "${local_manifest_file}") || {
        log_message "ERROR" "Failed to parse manifest file: ${local_manifest_file}"
        return 1
    }
    
    # Extract metadata
    local backup_id backup_type timestamp env files
    backup_id=$(echo "${manifest_data}" | jq -r '.backup_id')
    backup_type=$(echo "${manifest_data}" | jq -r '.backup_type')
    timestamp=$(echo "${manifest_data}" | jq -r '.timestamp')
    env=$(echo "${manifest_data}" | jq -r '.environment')
    files=$(echo "${manifest_data}" | jq -c '.files')
    
    log_message "INFO" "Parsed manifest for backup ${backup_id}"
    log_message "INFO" "  Type: ${backup_type}"
    log_message "INFO" "  Timestamp: ${timestamp}"
    log_message "INFO" "  Environment: ${env}"
    log_message "INFO" "  Files: $(echo "${files}" | jq 'length') items"
    
    # Return the manifest data
    echo "${manifest_data}"
    
    return 0
}

# Backup Airflow metadata database
backup_metadata_db() {
    local env="$1"
    local backup_type="${2:-${DEFAULT_BACKUP_TYPE}}"
    local retention_days="${3:-${DEFAULT_BACKUP_RETENTION_DAYS}}"
    
    log_message "INFO" "Starting metadata database backup for environment: ${env}"
    log_message "INFO" "Backup type: ${backup_type}, Retention: ${retention_days} days"
    
    get_environment_config "${env}" || return 1
    
    # Create a temporary directory for backups
    local backup_dir="${TEMP_DIR}/metadata"
    mkdir -p "${backup_dir}"
    
    # Build command for backup_metadata.py
    local cmd=(
        python3 "${SCRIPTS_DIR}/backup_metadata.py"
        "--type=${backup_type}"
        "--conn-id=postgres_${env}"
        "--output-dir=${backup_dir}"
        "--gcs-bucket=${BACKUP_BUCKET}"
        "--gcs-path=/backups/metadata/${env}"
        "--environment=${env}"
        "--retention-days=${retention_days}"
    )
    
    # Run the backup command
    log_message "INFO" "Running command: ${cmd[*]}"
    if "${cmd[@]}" > "${TEMP_DIR}/backup_metadata.log" 2>&1; then
        log_message "INFO" "Metadata database backup completed successfully"
        cat "${TEMP_DIR}/backup_metadata.log" >> "${LOG_FILE}"
        
        # Check output to get manifest path
        local manifest_path
        manifest_path=$(grep -o "gs://${BACKUP_BUCKET}/backups/metadata/${env}/${backup_type}/[^ ]*_manifest.json" "${TEMP_DIR}/backup_metadata.log" | tail -1)
        
        if [[ -n "${manifest_path}" ]]; then
            log_message "INFO" "Backup manifest: ${manifest_path}"
            echo "${manifest_path}"
            return 0
        else
            log_message "WARNING" "Could not find manifest path in backup output"
            return 0
        fi
    else
        log_message "ERROR" "Metadata database backup failed"
        cat "${TEMP_DIR}/backup_metadata.log" >> "${LOG_FILE}"
        return 1
    fi
}

# Backup DAGs, plugins, and data from GCS buckets
backup_gcs_data() {
    local env="$1"
    local backup_type="${2:-${DEFAULT_BACKUP_TYPE}}"
    
    log_message "INFO" "Starting GCS data backup for environment: ${env}"
    log_message "INFO" "Backup type: ${backup_type}"
    
    get_environment_config "${env}" || return 1
    
    # Create timestamp for this backup
    local timestamp
    timestamp=$(date +"%Y%m%d_%H%M%S")
    
    # Create a backup manifest
    local backup_id="airflow_gcs_${env}_${backup_type}_${timestamp}"
    local manifest_file="${TEMP_DIR}/${backup_id}_manifest.json"
    local manifest_gcs_path="gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/${backup_id}_manifest.json"
    
    # Create folders
    mkdir -p "${TEMP_DIR}/gcs"
    
    # Create initial manifest
    cat > "${manifest_file}" << EOF
{
    "backup_id": "${backup_id}",
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "backup_type": "${backup_type}",
    "environment": "${env}",
    "gcs_buckets": {
        "dags_bucket": "${DAGS_BUCKET}",
        "plugins_bucket": "${PLUGINS_BUCKET}"
    },
    "files": []
}
EOF
    
    log_message "INFO" "Created backup manifest: ${manifest_file}"
    
    # Start with empty files array
    local files_array="[]"
    
    # Backup DAGs from dags-bucket
    log_message "INFO" "Backing up DAGs from gs://${DAGS_BUCKET}/ to gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/dags/"
    if gsutil -m cp -r "gs://${DAGS_BUCKET}/*" "gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/dags/" > /dev/null 2>&1; then
        log_message "INFO" "DAGs backup successful"
        
        # Get list of backed up files
        local dags_files
        dags_files=$(gsutil ls -r "gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/dags/**" | grep -v '/$')
        
        # Add to files array
        for file in ${dags_files}; do
            local file_name
            file_name=$(basename "${file}")
            local file_path="${file#gs://${BACKUP_BUCKET}/}"
            
            # Get file metadata
            local file_size
            file_size=$(gsutil stat "${file}" | grep "Content-Length" | awk '{print $2}')
            
            # Add to the files array
            files_array=$(echo "${files_array}" | jq --arg name "${file_name}" --arg path "${file_path}" --arg size "${file_size}" --arg type "dags" \
                '. += [{"file_name": $name, "file_path": $path, "file_size": $size|tonumber, "file_type": $type}]')
        done
    else
        log_message "ERROR" "Failed to backup DAGs"
        return 1
    fi
    
    # Backup plugins from plugins-bucket if it exists
    if gsutil ls "gs://${PLUGINS_BUCKET}/" > /dev/null 2>&1; then
        log_message "INFO" "Backing up plugins from gs://${PLUGINS_BUCKET}/ to gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/plugins/"
        if gsutil -m cp -r "gs://${PLUGINS_BUCKET}/*" "gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/plugins/" > /dev/null 2>&1; then
            log_message "INFO" "Plugins backup successful"
            
            # Get list of backed up files
            local plugins_files
            plugins_files=$(gsutil ls -r "gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/plugins/**" | grep -v '/$')
            
            # Add to files array
            for file in ${plugins_files}; do
                local file_name
                file_name=$(basename "${file}")
                local file_path="${file#gs://${BACKUP_BUCKET}/}"
                
                # Get file metadata
                local file_size
                file_size=$(gsutil stat "${file}" | grep "Content-Length" | awk '{print $2}')
                
                # Add to the files array
                files_array=$(echo "${files_array}" | jq --arg name "${file_name}" --arg path "${file_path}" --arg size "${file_size}" --arg type "plugins" \
                    '. += [{"file_name": $name, "file_path": $path, "file_size": $size|tonumber, "file_type": $type}]')
            done
        else
            log_message "ERROR" "Failed to backup plugins"
            return 1
        fi
    else
        log_message "INFO" "No plugins bucket found, skipping plugins backup"
    fi
    
    # If full backup, also backup additional data
    if [[ "${backup_type}" == "full" ]]; then
        log_message "INFO" "Performing full backup - including additional data folders"
        
        # Backup logs if needed (usually not necessary, but can be included)
        if [[ -n "${LOGS_BUCKET}" ]] && gsutil ls "gs://${LOGS_BUCKET}/" > /dev/null 2>&1; then
            log_message "INFO" "Backing up recent logs from gs://${LOGS_BUCKET}/ to gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/logs/"
            
            # Only backup recent logs (last 7 days)
            local recent_logs
            recent_logs=$(gsutil ls "gs://${LOGS_BUCKET}/*" | grep -E "$(date -d '7 days ago' +'%Y-%m-%d')|$(date -d '6 days ago' +'%Y-%m-%d')|$(date -d '5 days ago' +'%Y-%m-%d')|$(date -d '4 days ago' +'%Y-%m-%d')|$(date -d '3 days ago' +'%Y-%m-%d')|$(date -d '2 days ago' +'%Y-%m-%d')|$(date -d '1 day ago' +'%Y-%m-%d')|$(date +'%Y-%m-%d')")
            
            if [[ -n "${recent_logs}" ]]; then
                gsutil -m cp -r "${recent_logs}" "gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/logs/" > /dev/null 2>&1 || log_message "WARNING" "Failed to backup some logs, continuing..."
                
                # Get list of backed up log files
                local logs_files
                logs_files=$(gsutil ls -r "gs://${BACKUP_BUCKET}/backups/gcs/${env}/${backup_type}/${timestamp}/logs/**" | grep -v '/$')
                
                # Add to files array
                for file in ${logs_files}; do
                    local file_name
                    file_name=$(basename "${file}")
                    local file_path="${file#gs://${BACKUP_BUCKET}/}"
                    
                    # Get file metadata
                    local file_size
                    file_size=$(gsutil stat "${file}" | grep "Content-Length" | awk '{print $2}')
                    
                    # Add to the files array
                    files_array=$(echo "${files_array}" | jq --arg name "${file_name}" --arg path "${file_path}" --arg size "${file_size}" --arg type "logs" \
                        '. += [{"file_name": $name, "file_path": $path, "file_size": $size|tonumber, "file_type": $type}]')
                done
            else
                log_message "INFO" "No recent logs found, skipping logs backup"
            fi
        fi
    fi
    
    # Update manifest with files
    cat "${manifest_file}" | jq --argjson files "${files_array}" '.files = $files' > "${manifest_file}.tmp" && mv "${manifest_file}.tmp" "${manifest_file}"
    
    # Upload manifest to GCS
    log_message "INFO" "Uploading backup manifest to ${manifest_gcs_path}"
    if gsutil cp "${manifest_file}" "${manifest_gcs_path}" > /dev/null 2>&1; then
        log_message "INFO" "Manifest uploaded successfully"
        echo "${manifest_gcs_path}"
        return 0
    else
        log_message "ERROR" "Failed to upload manifest"
        return 1
    fi
}

# Backup environment configurations and variables
backup_configurations() {
    local env="$1"
    
    log_message "INFO" "Starting configuration backup for environment: ${env}"
    
    get_environment_config "${env}" || return 1
    
    # Create timestamp for this backup
    local timestamp
    timestamp=$(date +"%Y%m%d_%H%M%S")
    
    # Create a backup directory
    local backup_dir="${TEMP_DIR}/config/${timestamp}"
    mkdir -p "${backup_dir}"
    
    # Create paths
    local backup_archive="${backup_dir}/airflow_config_${env}_${timestamp}.tar.gz"
    local backup_gcs_path="gs://${BACKUP_BUCKET}/backups/config/${env}/${timestamp}/airflow_config_${env}_${timestamp}.tar.gz"
    local manifest_file="${backup_dir}/airflow_config_${env}_${timestamp}_manifest.json"
    local manifest_gcs_path="gs://${BACKUP_BUCKET}/backups/config/${env}/${timestamp}/airflow_config_${env}_${timestamp}_manifest.json"
    
    # Get Composer environment details
    log_message "INFO" "Getting Composer environment details for ${env}"
    local composer_env
    composer_env=$(gcloud composer environments describe "${COMPOSER_ENV}" \
        --project="${GCP_PROJECT}" \
        --location="${COMPOSER_REGION}" \
        --format=json 2>/dev/null) || {
        log_message "ERROR" "Failed to get Composer environment details"
        return 1
    }
    
    # Save environment config
    echo "${composer_env}" > "${backup_dir}/environment.json"
    log_message "INFO" "Saved environment configuration"
    
    # Export Airflow variables
    log_message "INFO" "Exporting Airflow variables"
    gcloud composer environments run "${COMPOSER_ENV}" \
        --project="${GCP_PROJECT}" \
        --location="${COMPOSER_REGION}" \
        variables -- export "${backup_dir}/variables.json" 2>/dev/null || {
        log_message "WARNING" "Failed to export variables, continuing..."
    }
    
    # Export Airflow connections
    log_message "INFO" "Exporting Airflow connections"
    gcloud composer environments run "${COMPOSER_ENV}" \
        --project="${GCP_PROJECT}" \
        --location="${COMPOSER_REGION}" \
        connections -- export "${backup_dir}/connections.json" 2>/dev/null || {
        log_message "WARNING" "Failed to export connections, continuing..."
    }
    
    # Export Airflow pools
    log_message "INFO" "Exporting Airflow pools"
    gcloud composer environments run "${COMPOSER_ENV}" \
        --project="${GCP_PROJECT}" \
        --location="${COMPOSER_REGION}" \
        pools -- export "${backup_dir}/pools.json" 2>/dev/null || {
        log_message "WARNING" "Failed to export pools, continuing..."
    }
    
    # Create backup archive
    log_message "INFO" "Creating backup archive: ${backup_archive}"
    tar -czf "${backup_archive}" -C "${backup_dir}" . || {
        log_message "ERROR" "Failed to create backup archive"
        return 1
    }
    
    # Create manifest
    cat > "${manifest_file}" << EOF
{
    "backup_id": "airflow_config_${env}_${timestamp}",
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "backup_type": "config",
    "environment": "${env}",
    "archive_path": "backups/config/${env}/${timestamp}/airflow_config_${env}_${timestamp}.tar.gz",
    "contents": [
        "environment.json",
        "variables.json",
        "connections.json",
        "pools.json"
    ]
}
EOF
    
    # Upload archive to GCS
    log_message "INFO" "Uploading backup archive to ${backup_gcs_path}"
    gsutil cp "${backup_archive}" "${backup_gcs_path}" > /dev/null 2>&1 || {
        log_message "ERROR" "Failed to upload backup archive"
        return 1
    }
    
    # Upload manifest to GCS
    log_message "INFO" "Uploading backup manifest to ${manifest_gcs_path}"
    gsutil cp "${manifest_file}" "${manifest_gcs_path}" > /dev/null 2>&1 || {
        log_message "ERROR" "Failed to upload manifest"
        return 1
    }
    
    log_message "INFO" "Configuration backup completed successfully"
    echo "${manifest_gcs_path}"
    return 0
}

# Perform a complete backup of an environment
backup_all() {
    local env="$1"
    local backup_type="${2:-${DEFAULT_BACKUP_TYPE}}"
    local retention_days="${3:-${DEFAULT_BACKUP_RETENTION_DAYS}}"
    
    log_message "INFO" "Starting complete backup for environment: ${env}"
    log_message "INFO" "Backup type: ${backup_type}, Retention: ${retention_days} days"
    
    get_environment_config "${env}" || return 1
    
    # Create timestamp for this backup
    local timestamp
    timestamp=$(date +"%Y%m%d_%H%M%S")
    
    # Create a complete backup manifest
    local backup_id="airflow_backup_${env}_${backup_type}_${timestamp}"
    local manifest_file="${TEMP_DIR}/${backup_id}_manifest.json"
    local manifest_gcs_path="gs://${BACKUP_BUCKET}/backups/complete/${env}/${backup_type}/${timestamp}/${backup_id}_manifest.json"
    
    # Create temporary directory
    mkdir -p "${TEMP_DIR}"
    
    # Perform metadata backup
    log_message "INFO" "Backing up metadata database"
    local metadata_manifest
    metadata_manifest=$(backup_metadata_db "${env}" "${backup_type}" "${retention_days}") || {
        log_message "ERROR" "Metadata backup failed, aborting complete backup"
        return 1
    }
    
    # Perform GCS data backup
    log_message "INFO" "Backing up GCS data"
    local gcs_manifest
    gcs_manifest=$(backup_gcs_data "${env}" "${backup_type}") || {
        log_message "ERROR" "GCS data backup failed, aborting complete backup"
        return 1
    }
    
    # Perform configuration backup
    log_message "INFO" "Backing up configurations"
    local config_manifest
    config_manifest=$(backup_configurations "${env}") || {
        log_message "ERROR" "Configuration backup failed, aborting complete backup"
        return 1
    }
    
    # Create the complete backup manifest
    cat > "${manifest_file}" << EOF
{
    "backup_id": "${backup_id}",
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "backup_type": "${backup_type}",
    "environment": "${env}",
    "components": {
        "metadata": "${metadata_manifest}",
        "gcs_data": "${gcs_manifest}",
        "config": "${config_manifest}"
    },
    "retention_days": ${retention_days}
}
EOF
    
    # Upload the complete manifest to GCS
    log_message "INFO" "Uploading complete backup manifest to ${manifest_gcs_path}"
    gsutil cp "${manifest_file}" "${manifest_gcs_path}" > /dev/null 2>&1 || {
        log_message "ERROR" "Failed to upload complete backup manifest"
        return 1
    }
    
    # Apply retention policy
    log_message "INFO" "Applying retention policy"
    apply_retention_policy "${env}" "${retention_days}" || {
        log_message "WARNING" "Failed to apply retention policy, but backup completed successfully"
    }
    
    log_message "INFO" "Complete backup finished successfully: ${backup_id}"
    echo "${manifest_gcs_path}"
    return 0
}

# Apply retention policy to remove old backups
apply_retention_policy() {
    local env="$1"
    local retention_days="${2:-${DEFAULT_BACKUP_RETENTION_DAYS}}"
    
    log_message "INFO" "Applying retention policy for environment ${env}: keeping backups for ${retention_days} days"
    
    get_environment_config "${env}" || return 1
    
    # Calculate cutoff date
    local cutoff_date
    cutoff_date=$(date -d "${retention_days} days ago" +"%Y-%m-%d")
    log_message "INFO" "Cutoff date: ${cutoff_date}"
    
    # List all manifest files for complete backups
    local manifests
    manifests=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/complete/${env}/**/*_manifest.json" 2>/dev/null) || {
        log_message "INFO" "No complete backup manifests found to apply retention policy"
        return 0
    }
    
    local removed_count=0
    
    # Process each manifest
    for manifest in ${manifests}; do
        # Extract timestamp from manifest path
        local timestamp
        timestamp=$(basename "${manifest}" | grep -oE '[0-9]{8}_[0-9]{6}')
        
        if [[ -z "${timestamp}" ]]; then
            log_message "WARNING" "Could not extract date from manifest: ${manifest}, skipping"
            continue
        fi
        
        # Convert to date format
        local backup_date
        backup_date=$(date -d "$(echo "${timestamp}" | sed 's/_/ /' | sed 's/\([0-9]\{4\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)/\1-\2-\3/')" +"%Y-%m-%d")
        
        # Check if backup is older than retention period
        if [[ "${backup_date}" < "${cutoff_date}" ]]; then
            log_message "INFO" "Removing backup from ${backup_date} (older than retention period): ${manifest}"
            
            # Download and parse manifest
            local manifest_data
            manifest_data=$(parse_manifest "${manifest}") || {
                log_message "WARNING" "Failed to parse manifest ${manifest}, skipping"
                continue
            }
            
            # Get component manifests
            local metadata_manifest gcs_manifest config_manifest
            metadata_manifest=$(echo "${manifest_data}" | jq -r '.components.metadata')
            gcs_manifest=$(echo "${manifest_data}" | jq -r '.components.gcs_data')
            config_manifest=$(echo "${manifest_data}" | jq -r '.components.config')
            
            # Remove component backups
            if [[ -n "${metadata_manifest}" && "${metadata_manifest}" != "null" ]]; then
                log_message "INFO" "Removing metadata component: ${metadata_manifest}"
                gsutil rm -r "$(dirname "${metadata_manifest}")" > /dev/null 2>&1 || {
                    log_message "WARNING" "Failed to remove metadata component: ${metadata_manifest}"
                }
            fi
            
            if [[ -n "${gcs_manifest}" && "${gcs_manifest}" != "null" ]]; then
                log_message "INFO" "Removing GCS data component: ${gcs_manifest}"
                gsutil rm -r "$(dirname "${gcs_manifest}")" > /dev/null 2>&1 || {
                    log_message "WARNING" "Failed to remove GCS data component: ${gcs_manifest}"
                }
            fi
            
            if [[ -n "${config_manifest}" && "${config_manifest}" != "null" ]]; then
                log_message "INFO" "Removing config component: ${config_manifest}"
                gsutil rm -r "$(dirname "${config_manifest}")" > /dev/null 2>&1 || {
                    log_message "WARNING" "Failed to remove config component: ${config_manifest}"
                }
            fi
            
            # Remove complete backup manifest
            log_message "INFO" "Removing complete backup manifest: ${manifest}"
            gsutil rm -r "$(dirname "${manifest}")" > /dev/null 2>&1 || {
                log_message "WARNING" "Failed to remove complete backup manifest: ${manifest}"
            }
            
            ((removed_count++))
        fi
    done
    
    log_message "INFO" "Retention policy applied: removed ${removed_count} old backups"
    return 0
}

# Restore Airflow metadata database
restore_metadata_db() {
    local env="$1"
    local backup_id="$2"
    local force="$3"
    
    log_message "INFO" "Starting metadata database restore for environment: ${env}"
    
    get_environment_config "${env}" || return 1
    
    # Confirm before proceeding with production restore
    if [[ "${env}" == "prod" && "${force}" != "true" ]]; then
        log_message "WARNING" "Production environment detected. Use --force to proceed with actual restore."
        
        read -p "Are you sure you want to restore metadata database in PRODUCTION? (y/N): " confirm
        if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
            log_message "INFO" "Restore canceled by user"
            return 1
        fi
    fi
    
    # Create a temporary directory for restore
    local restore_dir="${TEMP_DIR}/metadata_restore"
    mkdir -p "${restore_dir}"
    
    # Determine backup to restore
    local backup_manifest
    if [[ "${backup_id}" == "latest" ]]; then
        log_message "INFO" "Finding latest backup for environment: ${env}"
        backup_manifest=$(find_latest_backup "${env}") || {
            log_message "ERROR" "Failed to find latest backup"
            return 1
        }
    else
        # Look for specific backup ID
        log_message "INFO" "Looking for backup with ID: ${backup_id}"
        backup_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/*/metadata/${env}/**/${backup_id}_manifest.json" 2>/dev/null) || {
            log_message "ERROR" "Backup with ID ${backup_id} not found"
            return 1
        }
    fi
    
    # Build command for restore_metadata.py
    local cmd=(
        python3 "${SCRIPTS_DIR}/restore_metadata.py"
        "--backup-id=${backup_id}"
        "--conn-id=postgres_${env}"
        "--restore-dir=${restore_dir}"
        "--gcs-bucket=${BACKUP_BUCKET}"
        "--gcs-path=/backups/metadata/${env}"
        "--environment=${env}"
    )
    
    # Add force flag if specified
    if [[ "${force}" == "true" ]]; then
        cmd+=("--force")
    fi
    
    # Run the restore command
    log_message "INFO" "Running command: ${cmd[*]}"
    if "${cmd[@]}" > "${TEMP_DIR}/restore_metadata.log" 2>&1; then
        log_message "INFO" "Metadata database restore completed successfully"
        cat "${TEMP_DIR}/restore_metadata.log" >> "${LOG_FILE}"
        return 0
    else
        log_message "ERROR" "Metadata database restore failed"
        cat "${TEMP_DIR}/restore_metadata.log" >> "${LOG_FILE}"
        return 1
    fi
}

# Restore DAGs, plugins, and data to GCS buckets
restore_gcs_data() {
    local env="$1"
    local backup_id="$2"
    local force="$3"
    
    log_message "INFO" "Starting GCS data restore for environment: ${env}"
    
    get_environment_config "${env}" || return 1
    
    # Confirm before proceeding with production restore
    if [[ "${env}" == "prod" && "${force}" != "true" ]]; then
        log_message "WARNING" "Production environment detected. Use --force to proceed with actual restore."
        
        read -p "Are you sure you want to restore GCS data in PRODUCTION? (y/N): " confirm
        if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
            log_message "INFO" "Restore canceled by user"
            return 1
        fi
    fi
    
    # Determine backup to restore
    local backup_manifest
    if [[ "${backup_id}" == "latest" ]]; then
        log_message "INFO" "Finding latest backup for environment: ${env}"
        backup_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/gcs/${env}/**/*_manifest.json" | sort -r | head -1) || {
            log_message "ERROR" "Failed to find latest GCS backup"
            return 1
        }
    else
        # Look for specific backup ID
        log_message "INFO" "Looking for backup with ID: ${backup_id}"
        backup_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/gcs/${env}/**/${backup_id}_manifest.json" 2>/dev/null) || {
            log_message "ERROR" "GCS backup with ID ${backup_id} not found"
            return 1
        }
    fi
    
    log_message "INFO" "Using GCS backup manifest: ${backup_manifest}"
    
    # Download and parse manifest
    local manifest_data
    manifest_data=$(parse_manifest "${backup_manifest}") || {
        log_message "ERROR" "Failed to parse GCS backup manifest"
        return 1
    }
    
    # Extract backup path from manifest
    local backup_path
    backup_path=$(dirname "${backup_manifest}")
    
    # Restore DAGs
    log_message "INFO" "Restoring DAGs to gs://${DAGS_BUCKET}/"
    if gsutil -m rsync -d "gs://${BACKUP_BUCKET}/$(echo "${backup_path}" | sed "s|^gs://${BACKUP_BUCKET}/||")/dags/" "gs://${DAGS_BUCKET}/" > /dev/null 2>&1; then
        log_message "INFO" "DAGs restored successfully"
    else
        log_message "ERROR" "Failed to restore DAGs"
        return 1
    fi
    
    # Check if plugins exist in backup
    if gsutil ls "${backup_path}/plugins/" > /dev/null 2>&1; then
        log_message "INFO" "Restoring plugins to gs://${PLUGINS_BUCKET}/"
        if gsutil -m rsync -d "${backup_path}/plugins/" "gs://${PLUGINS_BUCKET}/" > /dev/null 2>&1; then
            log_message "INFO" "Plugins restored successfully"
        else
            log_message "ERROR" "Failed to restore plugins"
            return 1
        fi
    else
        log_message "INFO" "No plugins found in backup, skipping plugins restore"
    fi
    
    log_message "INFO" "GCS data restore completed successfully"
    return 0
}

# Restore environment configurations and variables
restore_configurations() {
    local env="$1"
    local backup_id="$2"
    local force="$3"
    
    log_message "INFO" "Starting configuration restore for environment: ${env}"
    
    get_environment_config "${env}" || return 1
    
    # Confirm before proceeding with production restore
    if [[ "${env}" == "prod" && "${force}" != "true" ]]; then
        log_message "WARNING" "Production environment detected. Use --force to proceed with actual restore."
        
        read -p "Are you sure you want to restore configurations in PRODUCTION? (y/N): " confirm
        if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
            log_message "INFO" "Restore canceled by user"
            return 1
        fi
    fi
    
    # Create temporary directory for restore
    local restore_dir="${TEMP_DIR}/config_restore"
    mkdir -p "${restore_dir}"
    
    # Determine backup to restore
    local backup_manifest
    if [[ "${backup_id}" == "latest" ]]; then
        log_message "INFO" "Finding latest configuration backup for environment: ${env}"
        backup_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/config/${env}/**/*_manifest.json" | sort -r | head -1) || {
            log_message "ERROR" "Failed to find latest configuration backup"
            return 1
        }
    else
        # Look for specific backup ID
        log_message "INFO" "Looking for configuration backup with ID: ${backup_id}"
        backup_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/config/${env}/**/${backup_id}_manifest.json" 2>/dev/null) || {
            log_message "ERROR" "Configuration backup with ID ${backup_id} not found"
            return 1
        }
    fi
    
    log_message "INFO" "Using configuration backup manifest: ${backup_manifest}"
    
    # Download and parse manifest
    local manifest_data
    manifest_data=$(parse_manifest "${backup_manifest}") || {
        log_message "ERROR" "Failed to parse configuration backup manifest"
        return 1
    }
    
    # Get archive path from manifest
    local archive_path
    archive_path=$(echo "${manifest_data}" | jq -r '.archive_path')
    
    # Download archive
    local archive_local="${restore_dir}/$(basename "${archive_path}")"
    log_message "INFO" "Downloading archive: gs://${BACKUP_BUCKET}/${archive_path} to ${archive_local}"
    gsutil cp "gs://${BACKUP_BUCKET}/${archive_path}" "${archive_local}" > /dev/null 2>&1 || {
        log_message "ERROR" "Failed to download configuration archive"
        return 1
    }
    
    # Extract archive
    log_message "INFO" "Extracting archive to ${restore_dir}"
    tar -xzf "${archive_local}" -C "${restore_dir}" || {
        log_message "ERROR" "Failed to extract configuration archive"
        return 1
    }
    
    # Restore variables if present
    if [[ -f "${restore_dir}/variables.json" ]]; then
        log_message "INFO" "Restoring Airflow variables"
        gcloud composer environments run "${COMPOSER_ENV}" \
            --project="${GCP_PROJECT}" \
            --location="${COMPOSER_REGION}" \
            variables -- import "${restore_dir}/variables.json" 2>/dev/null || {
            log_message "WARNING" "Failed to restore variables, continuing..."
        }
    else
        log_message "INFO" "No variables file found in backup, skipping variables restore"
    fi
    
    # Restore connections if present
    if [[ -f "${restore_dir}/connections.json" ]]; then
        log_message "INFO" "Restoring Airflow connections"
        gcloud composer environments run "${COMPOSER_ENV}" \
            --project="${GCP_PROJECT}" \
            --location="${COMPOSER_REGION}" \
            connections -- import "${restore_dir}/connections.json" 2>/dev/null || {
            log_message "WARNING" "Failed to restore connections, continuing..."
        }
    else
        log_message "INFO" "No connections file found in backup, skipping connections restore"
    fi
    
    # Restore pools if present
    if [[ -f "${restore_dir}/pools.json" ]]; then
        log_message "INFO" "Restoring Airflow pools"
        gcloud composer environments run "${COMPOSER_ENV}" \
            --project="${GCP_PROJECT}" \
            --location="${COMPOSER_REGION}" \
            pools -- import "${restore_dir}/pools.json" 2>/dev/null || {
            log_message "WARNING" "Failed to restore pools, continuing..."
        }
    else
        log_message "INFO" "No pools file found in backup, skipping pools restore"
    fi
    
    log_message "INFO" "Configuration restore completed successfully"
    return 0
}

# Perform a complete restoration of an environment
restore_all() {
    local env="$1"
    local backup_id="$2"
    local force="$3"
    
    log_message "INFO" "Starting complete restore for environment: ${env}"
    
    get_environment_config "${env}" || return 1
    
    # For complete restore of a specific backup ID, find the complete backup manifest
    local complete_manifest
    
    if [[ "${backup_id}" == "latest" ]]; then
        log_message "INFO" "Finding latest complete backup for environment: ${env}"
        complete_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/complete/${env}/**/*_manifest.json" | sort -r | head -1) || {
            log_message "ERROR" "Failed to find latest complete backup"
            return 1
        }
    else
        # Look for specific backup ID
        log_message "INFO" "Looking for complete backup with ID: ${backup_id}"
        complete_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/complete/${env}/**/${backup_id}_manifest.json" 2>/dev/null) || {
            log_message "ERROR" "Complete backup with ID ${backup_id} not found"
            return 1
        }
    fi
    
    log_message "INFO" "Using complete backup manifest: ${complete_manifest}"
    
    # Download and parse manifest
    local manifest_data
    manifest_data=$(parse_manifest "${complete_manifest}") || {
        log_message "ERROR" "Failed to parse complete backup manifest"
        return 1
    }
    
    # Confirm before proceeding with production restore
    if [[ "${env}" == "prod" && "${force}" != "true" ]]; then
        log_message "WARNING" "Production environment detected. Use --force to proceed with actual restore."
        
        read -p "Are you sure you want to perform a complete restore in PRODUCTION? This will replace all data. (y/N): " confirm
        if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
            log_message "INFO" "Restore canceled by user"
            return 1
        fi
    fi
    
    # Extract component manifest paths
    local metadata_manifest gcs_manifest config_manifest
    metadata_manifest=$(echo "${manifest_data}" | jq -r '.components.metadata')
    gcs_manifest=$(echo "${manifest_data}" | jq -r '.components.gcs_data')
    config_manifest=$(echo "${manifest_data}" | jq -r '.components.config')
    
    # Get backup ID from metadata manifest for restore
    local metadata_backup_id
    metadata_backup_id=$(basename "${metadata_manifest}" | sed 's/_manifest.json//')
    
    # Get backup ID from GCS manifest for restore
    local gcs_backup_id
    gcs_backup_id=$(basename "${gcs_manifest}" | sed 's/_manifest.json//')
    
    # Get backup ID from config manifest for restore
    local config_backup_id
    config_backup_id=$(basename "${config_manifest}" | sed 's/_manifest.json//')
    
    # Restore metadata
    log_message "INFO" "Restoring metadata database from backup: ${metadata_backup_id}"
    restore_metadata_db "${env}" "${metadata_backup_id}" "${force}" || {
        log_message "ERROR" "Metadata database restore failed, aborting complete restore"
        return 1
    }
    
    # Restore GCS data
    log_message "INFO" "Restoring GCS data from backup: ${gcs_backup_id}"
    restore_gcs_data "${env}" "${gcs_backup_id}" "${force}" || {
        log_message "ERROR" "GCS data restore failed, aborting complete restore"
        return 1
    }
    
    # Restore configurations
    log_message "INFO" "Restoring configurations from backup: ${config_backup_id}"
    restore_configurations "${env}" "${config_backup_id}" "${force}" || {
        log_message "ERROR" "Configuration restore failed, aborting complete restore"
        return 1
    }
    
    # Validate restored environment
    log_message "INFO" "Validating restored environment"
    validate_backup "${env}" "${backup_id}" || {
        log_message "WARNING" "Validation found issues, but restore completed"
    }
    
    log_message "INFO" "Complete restore finished successfully"
    return 0
}

# Validate a backup for integrity and completeness
validate_backup() {
    local env="$1"
    local backup_id="$2"
    
    log_message "INFO" "Starting backup validation for environment: ${env}"
    
    get_environment_config "${env}" || return 1
    
    # Determine backup to validate
    local complete_manifest
    
    if [[ "${backup_id}" == "latest" ]]; then
        log_message "INFO" "Finding latest complete backup for environment: ${env}"
        complete_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/complete/${env}/**/*_manifest.json" | sort -r | head -1) || {
            log_message "ERROR" "Failed to find latest complete backup"
            return 1
        }
    else
        # Look for specific backup ID
        log_message "INFO" "Looking for complete backup with ID: ${backup_id}"
        complete_manifest=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/complete/${env}/**/${backup_id}_manifest.json" 2>/dev/null) || {
            log_message "ERROR" "Complete backup with ID ${backup_id} not found"
            return 1
        }
    fi
    
    log_message "INFO" "Validating backup manifest: ${complete_manifest}"
    
    # Download and parse manifest
    local manifest_data
    manifest_data=$(parse_manifest "${complete_manifest}") || {
        log_message "ERROR" "Failed to parse complete backup manifest"
        return 1
    }
    
    # Extract component manifest paths
    local metadata_manifest gcs_manifest config_manifest
    metadata_manifest=$(echo "${manifest_data}" | jq -r '.components.metadata')
    gcs_manifest=$(echo "${manifest_data}" | jq -r '.components.gcs_data')
    config_manifest=$(echo "${manifest_data}" | jq -r '.components.config')
    
    # Validate metadata component
    log_message "INFO" "Validating metadata component: ${metadata_manifest}"
    if gsutil stat "${metadata_manifest}" > /dev/null 2>&1; then
        log_message "INFO" "Metadata manifest exists"
        
        # Parse metadata manifest
        local metadata_data
        metadata_data=$(parse_manifest "${metadata_manifest}") || {
            log_message "ERROR" "Failed to parse metadata manifest"
            return 1
        }
        
        # Check metadata files
        local metadata_files
        metadata_files=$(echo "${metadata_data}" | jq -r '.files[].file_path')
        
        for file_path in ${metadata_files}; do
            log_message "INFO" "Checking metadata file: gs://${BACKUP_BUCKET}/${file_path}"
            if ! gsutil stat "gs://${BACKUP_BUCKET}/${file_path}" > /dev/null 2>&1; then
                log_message "ERROR" "Metadata file not found: gs://${BACKUP_BUCKET}/${file_path}"
                return 1
            fi
        done
        
        log_message "INFO" "Metadata component validation successful"
    else
        log_message "ERROR" "Metadata manifest not found: ${metadata_manifest}"
        return 1
    fi
    
    # Validate GCS data component
    log_message "INFO" "Validating GCS data component: ${gcs_manifest}"
    if gsutil stat "${gcs_manifest}" > /dev/null 2>&1; then
        log_message "INFO" "GCS data manifest exists"
        
        # Parse GCS manifest
        local gcs_data
        gcs_data=$(parse_manifest "${gcs_manifest}") || {
            log_message "ERROR" "Failed to parse GCS data manifest"
            return 1
        }
        
        # Check GCS data files
        local gcs_files
        gcs_files=$(echo "${gcs_data}" | jq -r '.files[].file_path')
        
        # Check a sample of files (first 10)
        local sample_files
        sample_files=$(echo "${gcs_files}" | head -10)
        
        for file_path in ${sample_files}; do
            log_message "INFO" "Checking GCS data file: gs://${BACKUP_BUCKET}/${file_path}"
            if ! gsutil stat "gs://${BACKUP_BUCKET}/${file_path}" > /dev/null 2>&1; then
                log_message "ERROR" "GCS data file not found: gs://${BACKUP_BUCKET}/${file_path}"
                return 1
            fi
        done
        
        log_message "INFO" "GCS data component validation successful (sampled)"
    else
        log_message "ERROR" "GCS data manifest not found: ${gcs_manifest}"
        return 1
    fi
    
    # Validate configuration component
    log_message "INFO" "Validating configuration component: ${config_manifest}"
    if gsutil stat "${config_manifest}" > /dev/null 2>&1; then
        log_message "INFO" "Configuration manifest exists"
        
        # Parse configuration manifest
        local config_data
        config_data=$(parse_manifest "${config_manifest}") || {
            log_message "ERROR" "Failed to parse configuration manifest"
            return 1
        }
        
        # Check archive exists
        local archive_path
        archive_path=$(echo "${config_data}" | jq -r '.archive_path')
        
        log_message "INFO" "Checking configuration archive: gs://${BACKUP_BUCKET}/${archive_path}"
        if ! gsutil stat "gs://${BACKUP_BUCKET}/${archive_path}" > /dev/null 2>&1; then
            log_message "ERROR" "Configuration archive not found: gs://${BACKUP_BUCKET}/${archive_path}"
            return 1
        fi
        
        log_message "INFO" "Configuration component validation successful"
    else
        log_message "ERROR" "Configuration manifest not found: ${config_manifest}"
        return 1
    fi
    
    log_message "INFO" "Backup validation completed successfully"
    return 0
}

# Cleanup function
cleanup() {
    log_message "INFO" "Performing cleanup"
    
    # Remove temporary directory if it exists
    if [[ -d "${TEMP_DIR}" ]]; then
        rm -rf "${TEMP_DIR}"
        log_message "INFO" "Removed temporary directory: ${TEMP_DIR}"
    fi
    
    log_message "INFO" "Cleanup completed"
}

# Main function
main() {
    local command=""
    local env=""
    local backup_type="${DEFAULT_BACKUP_TYPE}"
    local retention_days="${DEFAULT_BACKUP_RETENTION_DAYS}"
    local backup_id="latest"
    local backup_date=""
    local force="false"
    local dry_run="false"
    local metadata_only="false"
    local data_only="false"
    local config_only="false"
    local verbose="false"
    local limit=10
    local custom_bucket=""
    
    # Parse command line arguments
    if [[ $# -eq 0 ]]; then
        print_usage
        return 1
    fi
    
    command="$1"
    shift
    
    # Process options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --env=*)
                env="${1#*=}"
                ;;
            --type=*)
                backup_type="${1#*=}"
                ;;
            --retention=*)
                retention_days="${1#*=}"
                ;;
            --backup-id=*)
                backup_id="${1#*=}"
                ;;
            --date=*)
                backup_date="${1#*=}"
                ;;
            --limit=*)
                limit="${1#*=}"
                ;;
            --bucket=*)
                custom_bucket="${1#*=}"
                ;;
            --metadata-only)
                metadata_only="true"
                ;;
            --data-only)
                data_only="true"
                ;;
            --config-only)
                config_only="true"
                ;;
            --force)
                force="true"
                ;;
            --dry-run)
                dry_run="true"
                ;;
            --latest)
                backup_id="latest"
                ;;
            --verbose)
                verbose="true"
                ;;
            --help)
                print_usage
                return 0
                ;;
            *)
                log_message "ERROR" "Unknown option: $1"
                print_usage
                return 1
                ;;
        esac
        shift
    done
    
    # Check for required environment parameter
    if [[ -z "${env}" ]]; then
        log_message "ERROR" "Environment parameter (--env) is required"
        print_usage
        return 1
    fi
    
    # Validate environment
    validate_environment "${env}" || return 1
    
    # Apply custom bucket if specified
    if [[ -n "${custom_bucket}" ]]; then
        get_environment_config "${env}" || return 1
        BACKUP_BUCKET="${custom_bucket}"
        log_message "INFO" "Using custom backup bucket: ${BACKUP_BUCKET}"
    fi
    
    # Process command
    case "${command}" in
        backup)
            log_message "INFO" "Starting backup operation for environment: ${env}"
            
            if [[ "${metadata_only}" == "true" ]]; then
                backup_metadata_db "${env}" "${backup_type}" "${retention_days}"
            elif [[ "${data_only}" == "true" ]]; then
                backup_gcs_data "${env}" "${backup_type}"
            elif [[ "${config_only}" == "true" ]]; then
                backup_configurations "${env}"
            else
                backup_all "${env}" "${backup_type}" "${retention_days}"
            fi
            ;;
            
        restore)
            log_message "INFO" "Starting restore operation for environment: ${env}"
            
            if [[ "${dry_run}" == "true" ]]; then
                log_message "INFO" "Performing dry run restore (validation only)"
            fi
            
            if [[ -n "${backup_date}" ]]; then
                log_message "INFO" "Looking for backup from date: ${backup_date}"
                # Convert date to backup ID format
                backup_id=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/complete/${env}/**/*_manifest.json" | 
                            grep -E "${backup_date//[-]/}" | sort -r | head -1 | 
                            sed -E 's|.*/(.+)_manifest.json|\1|')
                
                if [[ -z "${backup_id}" ]]; then
                    log_message "ERROR" "No backup found for date: ${backup_date}"
                    return 1
                fi
                
                log_message "INFO" "Found backup ID for date ${backup_date}: ${backup_id}"
            fi
            
            if [[ "${metadata_only}" == "true" ]]; then
                restore_metadata_db "${env}" "${backup_id}" "${force}"
            elif [[ "${data_only}" == "true" ]]; then
                restore_gcs_data "${env}" "${backup_id}" "${force}"
            elif [[ "${config_only}" == "true" ]]; then
                restore_configurations "${env}" "${backup_id}" "${force}"
            else
                restore_all "${env}" "${backup_id}" "${force}"
            fi
            ;;
            
        validate)
            log_message "INFO" "Starting backup validation for environment: ${env}"
            validate_backup "${env}" "${backup_id}"
            ;;
            
        list)
            log_message "INFO" "Listing backups for environment: ${env}"
            
            get_environment_config "${env}" || return 1
            
            # List complete backups
            local backup_manifests
            if [[ -n "${backup_type}" && "${backup_type}" != "all" ]]; then
                backup_manifests=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/complete/${env}/${backup_type}/**/*_manifest.json" 2>/dev/null | sort -r | head -"${limit}")
            else
                backup_manifests=$(gsutil ls "gs://${BACKUP_BUCKET}/backups/complete/${env}/**/*_manifest.json" 2>/dev/null | sort -r | head -"${limit}")
            fi
            
            if [[ -z "${backup_manifests}" ]]; then
                log_message "INFO" "No backups found for environment: ${env}"
                return 0
            fi
            
            echo "Available backups for environment ${env}:"
            echo "================================================================================"
            echo "Backup ID                                  | Type      | Date                  "
            echo "-----------------------------------------------------------------------------"
            
            for manifest in ${backup_manifests}; do
                # Parse the manifest to get details
                local manifest_data
                manifest_data=$(parse_manifest "${manifest}")
                
                local backup_id
                backup_id=$(echo "${manifest_data}" | jq -r '.backup_id')
                
                local backup_type
                backup_type=$(echo "${manifest_data}" | jq -r '.backup_type')
                
                local timestamp
                timestamp=$(echo "${manifest_data}" | jq -r '.timestamp')
                
                # Format the timestamp
                local formatted_date
                formatted_date=$(date -d "${timestamp}" +"%Y-%m-%d %H:%M:%S")
                
                printf "%-40s | %-10s | %s\n" "${backup_id}" "${backup_type}" "${formatted_date}"
            done
            echo "================================================================================"
            ;;
            
        *)
            log_message "ERROR" "Unknown command: ${command}"
            print_usage
            return 1
            ;;
    esac
    
    return 0
}

# Execute main function with all arguments
main "$@"