#!/usr/bin/env bash
# setup_environments.sh - Script to automate the setup and provisioning of Cloud Composer 2 environments

# Set error handling options
set -eo pipefail

# Global variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ID=""
REGION="us-central1"
ZONES=""
ENVIRONMENTS=()
TERRAFORM_DIR=""
CONFIG_DIR=""
COMPOSER_VERSION="2.0.28"
AIRFLOW_VERSION="2.2.5"
LOG_FILE="/tmp/composer_setup_$(date +%Y%m%d_%H%M%S).log"
USE_TERRAFORM=false
DRY_RUN=false
VALIDATE_ONLY=false
VERBOSE=false
FORCE=false

# Function to display usage information
usage() {
    echo "Usage: $0 [options] <environment>"
    echo "Setup Cloud Composer 2 environments for Airflow 2.X migration"
    echo ""
    echo "Options:"
    echo "  -h, --help                 Show this help message and exit"
    echo "  -p, --project=PROJECT_ID   Google Cloud Project ID"
    echo "  -r, --region=REGION        Google Cloud region (default: us-central1)"
    echo "  -z, --zones=ZONES          Comma-separated list of zones"
    echo "  -t, --terraform            Use Terraform for infrastructure setup"
    echo "  -d, --dry-run              Validate and show changes without executing them"
    echo "  -v, --validate-only        Only validate the environment, don't create or update"
    echo "  -c, --composer-version=VER Cloud Composer version (default: 2.0.28)"
    echo "  -a, --airflow-version=VER  Airflow version (default: 2.2.5)"
    echo "  -f, --force                Skip confirmation prompts"
    echo "  --verbose                  Show verbose output"
    echo ""
    echo "Environment:"
    echo "  dev                        Setup Development environment"
    echo "  qa                         Setup QA environment"
    echo "  prod                       Setup Production environment"
    echo "  all                        Setup all environments"
    echo ""
    echo "Examples:"
    echo "  $0 --project=my-project dev              Setup Development environment"
    echo "  $0 --project=my-project --validate-only qa   Validate QA environment"
    echo "  $0 --project=my-project --terraform all     Setup all environments using Terraform"
    exit 1
}

# Function to log messages to both console and log file
log() {
    local level="$1"
    local message="$2"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    
    # Always log to file
    echo "$timestamp [$level] $message" >> "$LOG_FILE"
    
    # Log to console based on verbosity and level
    if [[ "$level" == "ERROR" ]]; then
        echo "$timestamp [$level] $message" >&2
    elif [[ "$VERBOSE" == true ]] || [[ "$level" != "DEBUG" ]]; then
        echo "$timestamp [$level] $message"
    fi
}

# Function to check if all required tools and configurations are available
check_prerequisites() {
    log "INFO" "Checking prerequisites..."
    
    # Check if gcloud is installed
    if ! command -v gcloud &> /dev/null; then
        log "ERROR" "gcloud CLI is not installed. Please install Google Cloud SDK."
        return 1
    fi
    
    # Check if gcloud is configured with a default project
    if [[ -z "$PROJECT_ID" ]]; then
        PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
        if [[ -z "$PROJECT_ID" ]]; then
            log "ERROR" "No project ID specified and no default project configured in gcloud."
            log "ERROR" "Please specify a project with --project=PROJECT_ID or run 'gcloud config set project PROJECT_ID'"
            return 1
        fi
    fi
    
    # Verify if the user has sufficient permissions
    local permissions_check=$(gcloud projects get-iam-policy "$PROJECT_ID" --format="json" 2>/dev/null)
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to verify IAM permissions. Make sure you have sufficient permissions."
        return 1
    fi
    
    # Check if Python 3.8+ is installed
    if ! command -v python3 &> /dev/null; then
        log "ERROR" "Python 3 is not installed. Please install Python 3.8 or higher."
        return 1
    fi
    
    python_version=$(python3 --version | awk '{print $2}')
    if [[ $(echo "$python_version" | cut -d. -f1) -lt 3 ]] || [[ $(echo "$python_version" | cut -d. -f1) -eq 3 && $(echo "$python_version" | cut -d. -f2) -lt 8 ]]; then
        log "ERROR" "Python 3.8 or higher is required. Found version $python_version."
        return 1
    fi
    
    # Check if Terraform is installed if USE_TERRAFORM is true
    if [[ "$USE_TERRAFORM" == true ]]; then
        if ! command -v terraform &> /dev/null; then
            log "ERROR" "Terraform is not installed. Please install Terraform 1.0.0 or higher."
            return 1
        fi
        
        # Check terraform version if jq is available
        if command -v jq &> /dev/null; then
            terraform_version=$(terraform version -json | jq -r '.terraform_version')
            if [[ $(echo "$terraform_version" | cut -d. -f1) -lt 1 ]]; then
                log "ERROR" "Terraform 1.0.0 or higher is required. Found version $terraform_version."
                return 1
            fi
        fi
    fi
    
    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        log "ERROR" "jq is not installed. Please install jq for JSON processing."
        return 1
    fi
    
    # Check if required APIs are enabled
    local required_apis=("composer.googleapis.com" "compute.googleapis.com" "container.googleapis.com" "secretmanager.googleapis.com")
    for api in "${required_apis[@]}"; do
        local api_enabled=$(gcloud services list --project="$PROJECT_ID" --filter="name:$api" --format="value(name)")
        if [[ -z "$api_enabled" ]]; then
            log "ERROR" "Required API $api is not enabled. Please enable it with:"
            log "ERROR" "gcloud services enable $api --project=$PROJECT_ID"
            return 1
        fi
    done
    
    log "INFO" "All prerequisites checked successfully."
    return 0
}

# Function to parse command line arguments
parse_args() {
    if [[ $# -eq 0 ]]; then
        usage
        return 1
    fi
    
    # Parse options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                usage
                ;;
            -p=*|--project=*)
                PROJECT_ID="${1#*=}"
                shift
                ;;
            -r=*|--region=*)
                REGION="${1#*=}"
                shift
                ;;
            -z=*|--zones=*)
                ZONES="${1#*=}"
                shift
                ;;
            -t|--terraform)
                USE_TERRAFORM=true
                shift
                ;;
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -v|--validate-only)
                VALIDATE_ONLY=true
                shift
                ;;
            -c=*|--composer-version=*)
                COMPOSER_VERSION="${1#*=}"
                shift
                ;;
            -a=*|--airflow-version=*)
                AIRFLOW_VERSION="${1#*=}"
                shift
                ;;
            -f|--force)
                FORCE=true
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            dev|qa|prod|all)
                if [[ "$1" == "all" ]]; then
                    ENVIRONMENTS=("dev" "qa" "prod")
                else
                    ENVIRONMENTS+=("$1")
                fi
                shift
                ;;
            *)
                log "ERROR" "Unknown option: $1"
                usage
                ;;
        esac
    done
    
    # Validate required options
    if [[ -z "$PROJECT_ID" ]]; then
        log "ERROR" "Project ID must be specified."
        usage
        return 1
    fi
    
    if [[ ${#ENVIRONMENTS[@]} -eq 0 ]]; then
        log "ERROR" "No environment specified. Please specify at least one environment (dev, qa, prod, or all)."
        usage
        return 1
    fi
    
    # Set default zones if not specified
    if [[ -z "$ZONES" ]]; then
        case "$REGION" in
            us-central1)
                ZONES="us-central1-a,us-central1-b,us-central1-c"
                ;;
            us-east1)
                ZONES="us-east1-b,us-east1-c,us-east1-d"
                ;;
            us-west1)
                ZONES="us-west1-a,us-west1-b,us-west1-c"
                ;;
            *)
                log "WARN" "No zones specified for region $REGION. Using REGION-a."
                ZONES="${REGION}-a"
                ;;
        esac
    fi
    
    # Set config directories
    CONFIG_DIR="${SCRIPT_DIR}/../../src/backend/config"
    TERRAFORM_DIR="${SCRIPT_DIR}/../terraform"
    
    log "INFO" "Arguments parsed successfully."
    log "INFO" "Project ID: $PROJECT_ID"
    log "INFO" "Region: $REGION"
    log "INFO" "Zones: $ZONES"
    log "INFO" "Environments: ${ENVIRONMENTS[*]}"
    log "INFO" "Composer Version: $COMPOSER_VERSION"
    log "INFO" "Airflow Version: $AIRFLOW_VERSION"
    log "INFO" "Use Terraform: $USE_TERRAFORM"
    log "INFO" "Dry Run: $DRY_RUN"
    log "INFO" "Validate Only: $VALIDATE_ONLY"
    log "INFO" "Force: $FORCE"
    log "INFO" "Verbose: $VERBOSE"
    
    return 0
}

# Function to load environment-specific configuration
load_config() {
    local environment="$1"
    local config_file="${CONFIG_DIR}/composer_${environment}.py"
    
    log "INFO" "Loading configuration for $environment environment from $config_file"
    
    if [[ ! -f "$config_file" ]]; then
        log "ERROR" "Configuration file not found: $config_file"
        return 1
    fi
    
    # Extract the Python configuration by executing the script and printing the config
    local config_json=$(python3 -c "import sys; sys.path.append('${CONFIG_DIR}/..'); from config.composer_${environment} import config; import json; print(json.dumps(config))")
    
    if [[ $? -ne 0 || -z "$config_json" ]]; then
        log "ERROR" "Failed to load configuration from $config_file"
        return 1
    fi
    
    log "DEBUG" "Configuration loaded successfully for $environment environment"
    echo "$config_json"
    return 0
}

# Function to set up network configuration for an environment
configure_environment_network() {
    local environment="$1"
    local config="$2"
    
    log "INFO" "Configuring network for $environment environment"
    
    # Extract network configuration from config
    local network_name=$(echo "$config" | jq -r '.network.network_name')
    local subnet_name=$(echo "$config" | jq -r '.network.subnetwork_name')
    local ip_range=$(echo "$config" | jq -r '.network.ip_range')
    local region=$(echo "$config" | jq -r '.gcp.region')
    local project_id=$(echo "$config" | jq -r '.gcp.project_id')
    
    # Check if network exists
    local network_exists=$(gcloud compute networks describe "$network_name" --project="$project_id" 2>/dev/null)
    if [[ $? -eq 0 ]]; then
        log "INFO" "Network $network_name already exists."
    else
        log "INFO" "Creating network $network_name..."
        if [[ "$DRY_RUN" == false ]]; then
            gcloud compute networks create "$network_name" \
                --project="$project_id" \
                --subnet-mode=custom
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create network $network_name"
                return 1
            fi
        else
            log "DRY_RUN" "Would create network: $network_name"
        fi
    fi
    
    # Check if subnet exists
    local subnet_exists=$(gcloud compute networks subnets describe "$subnet_name" \
        --region="$region" \
        --project="$project_id" 2>/dev/null)
    
    if [[ $? -eq 0 ]]; then
        log "INFO" "Subnet $subnet_name already exists."
    else
        log "INFO" "Creating subnet $subnet_name..."
        if [[ "$DRY_RUN" == false ]]; then
            gcloud compute networks subnets create "$subnet_name" \
                --project="$project_id" \
                --region="$region" \
                --network="$network_name" \
                --range="$ip_range"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create subnet $subnet_name"
                return 1
            fi
        else
            log "DRY_RUN" "Would create subnet: $subnet_name in network: $network_name with range: $ip_range"
        fi
    fi
    
    # Configure firewall rules
    local enable_private=$(echo "$config" | jq -r '.network.enable_private_endpoint')
    
    # Create firewall rule for SSH access
    local ssh_rule_name="${network_name}-allow-ssh"
    local ssh_rule_exists=$(gcloud compute firewall-rules describe "$ssh_rule_name" --project="$project_id" 2>/dev/null)
    
    if [[ $? -eq 0 ]]; then
        log "INFO" "Firewall rule $ssh_rule_name already exists."
    else
        log "INFO" "Creating firewall rule $ssh_rule_name..."
        if [[ "$DRY_RUN" == false ]]; then
            gcloud compute firewall-rules create "$ssh_rule_name" \
                --project="$project_id" \
                --network="$network_name" \
                --direction=INGRESS \
                --priority=1000 \
                --action=ALLOW \
                --rules=tcp:22 \
                --source-ranges=0.0.0.0/0
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create firewall rule $ssh_rule_name"
                return 1
            fi
        else
            log "DRY_RUN" "Would create firewall rule: $ssh_rule_name to allow SSH access"
        fi
    fi
    
    # Create firewall rule for internal communication
    local internal_rule_name="${network_name}-allow-internal"
    local internal_rule_exists=$(gcloud compute firewall-rules describe "$internal_rule_name" --project="$project_id" 2>/dev/null)
    
    if [[ $? -eq 0 ]]; then
        log "INFO" "Firewall rule $internal_rule_name already exists."
    else
        log "INFO" "Creating firewall rule $internal_rule_name..."
        if [[ "$DRY_RUN" == false ]]; then
            gcloud compute firewall-rules create "$internal_rule_name" \
                --project="$project_id" \
                --network="$network_name" \
                --direction=INGRESS \
                --priority=1000 \
                --action=ALLOW \
                --rules=tcp,udp,icmp \
                --source-ranges="$ip_range"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create firewall rule $internal_rule_name"
                return 1
            fi
        else
            log "DRY_RUN" "Would create firewall rule: $internal_rule_name to allow internal communication"
        fi
    fi
    
    # Set up Private Service Connection for Cloud SQL and GKE if private IP is enabled
    if [[ "$enable_private" == "true" || "$enable_private" == "True" ]]; then
        log "INFO" "Setting up private service access for $network_name..."
        
        # Check if private connection range exists
        local service_connection_name="google-managed-services-${subnet_name}"
        local service_connection_exists=$(gcloud compute addresses describe "$service_connection_name" \
            --global \
            --project="$project_id" 2>/dev/null)
        
        if [[ $? -eq 0 ]]; then
            log "INFO" "Private service connection $service_connection_name already exists."
        else
            # Get allocated IP range for private services
            local services_cidr=$(echo "$config" | jq -r '.network.cloud_sql_ipv4_cidr')
            
            log "INFO" "Creating private service connection $service_connection_name..."
            if [[ "$DRY_RUN" == false ]]; then
                # Allocate IP range for private services
                gcloud compute addresses create "$service_connection_name" \
                    --global \
                    --purpose=VPC_PEERING \
                    --addresses="${services_cidr%/*}" \
                    --prefix-length="${services_cidr#*/}" \
                    --description="Allocated for private service connection" \
                    --network="$network_name" \
                    --project="$project_id"
                
                if [[ $? -ne 0 ]]; then
                    log "ERROR" "Failed to allocate IP range for private service connection"
                    return 1
                fi
                
                # Create private connection
                gcloud services vpc-peerings connect \
                    --service=servicenetworking.googleapis.com \
                    --ranges="$service_connection_name" \
                    --network="$network_name" \
                    --project="$project_id"
                
                if [[ $? -ne 0 ]]; then
                    log "ERROR" "Failed to create private service connection"
                    return 1
                fi
            else
                log "DRY_RUN" "Would create private service connection: $service_connection_name with range: $services_cidr"
            fi
        fi
        
        # Set up Cloud NAT for outbound connectivity
        local router_name="${network_name}-nat-router"
        local nat_name="${network_name}-nat-config"
        local router_exists=$(gcloud compute routers describe "$router_name" \
            --region="$region" \
            --project="$project_id" 2>/dev/null)
        
        if [[ $? -eq 0 ]]; then
            log "INFO" "Router $router_name already exists."
            
            # Check if NAT config exists
            local nat_exists=$(gcloud compute routers describe "$router_name" \
                --region="$region" \
                --project="$project_id" \
                --format="json" | jq -r '.nats[0].name')
            
            if [[ -n "$nat_exists" ]]; then
                log "INFO" "NAT configuration $nat_name already exists on router $router_name."
            else
                log "INFO" "Creating NAT configuration $nat_name on router $router_name..."
                if [[ "$DRY_RUN" == false ]]; then
                    gcloud compute routers nats create "$nat_name" \
                        --router="$router_name" \
                        --region="$region" \
                        --nat-all-subnet-ip-ranges \
                        --auto-allocate-nat-external-ips \
                        --project="$project_id"
                    
                    if [[ $? -ne 0 ]]; then
                        log "ERROR" "Failed to create NAT configuration $nat_name"
                        return 1
                    fi
                else
                    log "DRY_RUN" "Would create NAT configuration: $nat_name on router: $router_name"
                fi
            fi
        else
            log "INFO" "Creating router $router_name and NAT configuration $nat_name..."
            if [[ "$DRY_RUN" == false ]]; then
                # Create router
                gcloud compute routers create "$router_name" \
                    --network="$network_name" \
                    --region="$region" \
                    --project="$project_id"
                
                if [[ $? -ne 0 ]]; then
                    log "ERROR" "Failed to create router $router_name"
                    return 1
                fi
                
                # Create NAT configuration
                gcloud compute routers nats create "$nat_name" \
                    --router="$router_name" \
                    --region="$region" \
                    --nat-all-subnet-ip-ranges \
                    --auto-allocate-nat-external-ips \
                    --project="$project_id"
                
                if [[ $? -ne 0 ]]; then
                    log "ERROR" "Failed to create NAT configuration $nat_name"
                    return 1
                fi
            else
                log "DRY_RUN" "Would create router: $router_name and NAT configuration: $nat_name"
            fi
        fi
    fi
    
    log "INFO" "Network configuration for $environment environment completed successfully."
    return 0
}

# Function to verify network connectivity for an environment
verify_network_connectivity() {
    local environment="$1"
    local config="$2"
    
    log "INFO" "Verifying network connectivity for $environment environment..."
    
    # Extract network configuration
    local network_name=$(echo "$config" | jq -r '.network.network_name')
    local subnet_name=$(echo "$config" | jq -r '.network.subnetwork_name')
    local region=$(echo "$config" | jq -r '.gcp.region')
    local project_id=$(echo "$config" | jq -r '.gcp.project_id')
    local enable_private=$(echo "$config" | jq -r '.network.enable_private_endpoint')
    
    # Verify network exists
    local network_exists=$(gcloud compute networks describe "$network_name" --project="$project_id" 2>/dev/null)
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Network $network_name does not exist."
        return 1
    fi
    
    # Verify subnet exists
    local subnet_exists=$(gcloud compute networks subnets describe "$subnet_name" \
        --region="$region" \
        --project="$project_id" 2>/dev/null)
    
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Subnet $subnet_name does not exist."
        return 1
    fi
    
    # Verify firewall rules exist
    local ssh_rule_name="${network_name}-allow-ssh"
    local ssh_rule_exists=$(gcloud compute firewall-rules describe "$ssh_rule_name" --project="$project_id" 2>/dev/null)
    
    if [[ $? -ne 0 ]]; then
        log "WARN" "Firewall rule $ssh_rule_name does not exist."
    fi
    
    local internal_rule_name="${network_name}-allow-internal"
    local internal_rule_exists=$(gcloud compute firewall-rules describe "$internal_rule_name" --project="$project_id" 2>/dev/null)
    
    if [[ $? -ne 0 ]]; then
        log "WARN" "Firewall rule $internal_rule_name does not exist."
    fi
    
    # Verify private service access if enabled
    if [[ "$enable_private" == "true" || "$enable_private" == "True" ]]; then
        local service_connection_name="google-managed-services-${subnet_name}"
        local service_connection_exists=$(gcloud compute addresses describe "$service_connection_name" \
            --global \
            --project="$project_id" 2>/dev/null)
        
        if [[ $? -ne 0 ]]; then
            log "ERROR" "Private service connection $service_connection_name does not exist."
            return 1
        fi
        
        # Verify Cloud NAT
        local router_name="${network_name}-nat-router"
        local router_exists=$(gcloud compute routers describe "$router_name" \
            --region="$region" \
            --project="$project_id" 2>/dev/null)
        
        if [[ $? -ne 0 ]]; then
            log "WARN" "Router $router_name does not exist. Outbound connectivity may be limited."
        else
            # Verify NAT configuration
            local nat_exists=$(gcloud compute routers describe "$router_name" \
                --region="$region" \
                --project="$project_id" \
                --format="json" | jq -r '.nats[0].name')
            
            if [[ -z "$nat_exists" ]]; then
                log "WARN" "NAT configuration not found on router $router_name. Outbound connectivity may be limited."
            fi
        fi
    fi
    
    log "INFO" "Network connectivity verification completed for $environment environment."
    return 0
}

# Function to set up network configuration for an environment
setup_environment_network() {
    local environment="$1"
    
    log "INFO" "Setting up network for $environment environment..."
    
    # Load environment config
    local config=$(load_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load configuration for $environment environment."
        return 1
    fi
    
    # Configure network
    configure_environment_network "$environment" "$config"
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to configure network for $environment environment."
        return 1
    fi
    
    # Verify network connectivity if not in validate-only mode
    if [[ "$VALIDATE_ONLY" == false && "$DRY_RUN" == false ]]; then
        verify_network_connectivity "$environment" "$config"
        if [[ $? -ne 0 ]]; then
            log "ERROR" "Network connectivity verification failed for $environment environment."
            return 1
        fi
    fi
    
    log "INFO" "Network setup completed for $environment environment."
    return 0
}

# Function to set up security configuration for an environment
setup_environment_security() {
    local environment="$1"
    
    log "INFO" "Setting up security configuration for $environment environment..."
    
    # Load environment config
    local config=$(load_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load configuration for $environment environment."
        return 1
    fi
    
    # Extract security configuration
    local project_id=$(echo "$config" | jq -r '.gcp.project_id')
    local service_account=$(echo "$config" | jq -r '.security.service_account')
    local service_account_name=$(echo "$service_account" | cut -d'@' -f1)
    local kms_key=$(echo "$config" | jq -r '.security.kms_key')
    
    # Create service account if it doesn't exist
    local sa_exists=$(gcloud iam service-accounts describe "$service_account" --project="$project_id" 2>/dev/null)
    if [[ $? -eq 0 ]]; then
        log "INFO" "Service account $service_account already exists."
    else
        log "INFO" "Creating service account $service_account..."
        if [[ "$DRY_RUN" == false ]]; then
            gcloud iam service-accounts create "$service_account_name" \
                --display-name="Cloud Composer 2 $environment Environment" \
                --description="Service Account for Cloud Composer 2 $environment Environment" \
                --project="$project_id"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create service account $service_account"
                return 1
            fi
        else
            log "DRY_RUN" "Would create service account: $service_account"
        fi
    fi
    
    # Assign required roles to the service account
    local required_roles=(
        "roles/composer.worker"
        "roles/composer.ServiceAgentV2Ext"
        "roles/logging.logWriter"
        "roles/monitoring.metricWriter"
        "roles/monitoring.viewer"
        "roles/iam.serviceAccountUser"
        "roles/storage.objectAdmin"
    )
    
    for role in "${required_roles[@]}"; do
        if [[ "$DRY_RUN" == false ]]; then
            log "INFO" "Assigning role $role to service account $service_account..."
            gcloud projects add-iam-policy-binding "$project_id" \
                --member="serviceAccount:$service_account" \
                --role="$role" \
                --condition=None
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to assign role $role to service account $service_account"
                return 1
            fi
        else
            log "DRY_RUN" "Would assign role $role to service account $service_account"
        fi
    done
    
    # Set up KMS key if specified and not empty
    if [[ -n "$kms_key" && "$kms_key" != "null" ]]; then
        # Extract key ring name and key name from the fully qualified key name
        local keyring_name=$(echo "$kms_key" | sed -n 's/.*\/keyRings\/\([^/]*\)\/.*/\1/p')
        local key_name=$(echo "$kms_key" | sed -n 's/.*\/cryptoKeys\/\([^/]*\).*/\1/p')
        local key_location=$(echo "$kms_key" | sed -n 's/.*\/locations\/\([^/]*\)\/.*/\1/p')
        
        if [[ -z "$keyring_name" || -z "$key_name" || -z "$key_location" ]]; then
            log "ERROR" "Invalid KMS key format: $kms_key"
            return 1
        fi
        
        # Check if key ring exists
        local keyring_exists=$(gcloud kms keyrings describe "$keyring_name" \
            --location="$key_location" \
            --project="$project_id" 2>/dev/null)
        
        if [[ $? -eq 0 ]]; then
            log "INFO" "KMS key ring $keyring_name already exists."
        else
            log "INFO" "Creating KMS key ring $keyring_name..."
            if [[ "$DRY_RUN" == false ]]; then
                gcloud kms keyrings create "$keyring_name" \
                    --location="$key_location" \
                    --project="$project_id"
                
                if [[ $? -ne 0 ]]; then
                    log "ERROR" "Failed to create KMS key ring $keyring_name"
                    return 1
                fi
            else
                log "DRY_RUN" "Would create KMS key ring: $keyring_name in location: $key_location"
            fi
        fi
        
        # Check if key exists
        local key_exists=$(gcloud kms keys describe "$key_name" \
            --keyring="$keyring_name" \
            --location="$key_location" \
            --project="$project_id" 2>/dev/null)
        
        if [[ $? -eq 0 ]]; then
            log "INFO" "KMS key $key_name already exists."
        else
            log "INFO" "Creating KMS key $key_name..."
            if [[ "$DRY_RUN" == false ]]; then
                gcloud kms keys create "$key_name" \
                    --keyring="$keyring_name" \
                    --location="$key_location" \
                    --purpose="encryption" \
                    --project="$project_id"
                
                if [[ $? -ne 0 ]]; then
                    log "ERROR" "Failed to create KMS key $key_name"
                    return 1
                fi
            else
                log "DRY_RUN" "Would create KMS key: $key_name in key ring: $keyring_name"
            fi
        fi
        
        # Grant service account permission to use the key
        if [[ "$DRY_RUN" == false ]]; then
            log "INFO" "Granting Composer service account permissions to use KMS key..."
            # Get project number for service agent
            local project_number=$(gcloud projects describe "$project_id" --format="value(projectNumber)")
            
            # Service agent for Cloud Composer
            local composer_service_agent="service-${project_number}@cloudcomposer-accounts.iam.gserviceaccount.com"
            
            gcloud kms keys add-iam-policy-binding "$key_name" \
                --keyring="$keyring_name" \
                --location="$key_location" \
                --member="serviceAccount:$composer_service_agent" \
                --role="roles/cloudkms.cryptoKeyEncrypterDecrypter" \
                --project="$project_id"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to grant permission on KMS key $key_name to Composer service agent"
                return 1
            fi
            
            # Also grant to the environment-specific service account
            gcloud kms keys add-iam-policy-binding "$key_name" \
                --keyring="$keyring_name" \
                --location="$key_location" \
                --member="serviceAccount:$service_account" \
                --role="roles/cloudkms.cryptoKeyEncrypterDecrypter" \
                --project="$project_id"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to grant permission on KMS key $key_name to service account $service_account"
                return 1
            fi
        else
            log "DRY_RUN" "Would grant permissions to use KMS key $key_name to service accounts"
        fi
    fi
    
    # Setup audit logging
    if [[ "$DRY_RUN" == false ]]; then
        log "INFO" "Configuring audit logging for $environment environment..."
        gcloud logging sinks create "composer-${environment}-audit-logs" \
            storage.googleapis.com/$(echo "$config" | jq -r '.storage.logs_bucket') \
            --log-filter="resource.type=cloud_composer_environment AND resource.labels.environment_name=$(echo "$config" | jq -r '.composer.environment_name')" \
            --project="$project_id"
        
        if [[ $? -ne 0 ]]; then
            log "WARN" "Failed to configure audit logging sink. This is not critical but recommended."
        fi
    else
        log "DRY_RUN" "Would configure audit logging sink for $environment environment"
    fi
    
    log "INFO" "Security configuration completed for $environment environment."
    return 0
}

# Function to set up an environment using Terraform
setup_environment_terraform() {
    local environment="$1"
    
    log "INFO" "Setting up $environment environment using Terraform..."
    
    # Load environment config
    local config=$(load_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load configuration for $environment environment."
        return 1
    fi
    
    # Check if Terraform directory exists
    if [[ ! -d "$TERRAFORM_DIR" ]]; then
        log "ERROR" "Terraform directory does not exist: $TERRAFORM_DIR"
        return 1
    fi
    
    # Create working directory for this environment
    local tf_working_dir="${TERRAFORM_DIR}/${environment}"
    mkdir -p "$tf_working_dir"
    
    # Copy Terraform files to working directory
    cp -r "${TERRAFORM_DIR}/modules" "$tf_working_dir/"
    cp "${TERRAFORM_DIR}/main.tf" "$tf_working_dir/"
    cp "${TERRAFORM_DIR}/variables.tf" "$tf_working_dir/"
    cp "${TERRAFORM_DIR}/outputs.tf" "$tf_working_dir/"
    cp "${TERRAFORM_DIR}/versions.tf" "$tf_working_dir/"
    
    # Generate terraform.tfvars file from configuration
    log "INFO" "Generating terraform.tfvars file for $environment environment..."
    local project_id=$(echo "$config" | jq -r '.gcp.project_id')
    local region=$(echo "$config" | jq -r '.gcp.region')
    local environment_name=$(echo "$config" | jq -r '.composer.environment_name')
    local node_count=$(echo "$config" | jq -r '.composer.node_count')
    local machine_type=$(echo "$config" | jq -r '.composer.web_server_machine_type')
    local env_size=$(echo "$config" | jq -r '.composer.environment_size')
    local network_name=$(echo "$config" | jq -r '.network.network_name')
    local subnet_name=$(echo "$config" | jq -r '.network.subnetwork_name')
    local service_account=$(echo "$config" | jq -r '.security.service_account')
    local env_vars=$(echo "$config" | jq -r '.composer.environment_variables | to_entries | map("\"\(.key)\"=\"\(.value)\"") | join(",")')
    local private_ip=$(echo "$config" | jq -r '.network.enable_private_endpoint')
    local kms_key=$(echo "$config" | jq -r '.security.kms_key')
    
    # Write terraform.tfvars file
    cat > "${tf_working_dir}/terraform.tfvars" << EOF
project_id              = "${project_id}"
region                  = "${region}"
environment             = "${environment}"
environment_name        = "${environment_name}"
composer_version        = "${COMPOSER_VERSION}"
airflow_version         = "${AIRFLOW_VERSION}"
node_count              = ${node_count}
environment_size        = "${env_size}"
scheduler_count         = $(echo "$config" | jq -r '.composer.scheduler.count // 1')
web_server_machine_type = "${machine_type}"
network_name            = "${network_name}"
subnetwork_name         = "${subnet_name}"
service_account         = "${service_account}"
environment_variables   = {${env_vars}}
enable_private_endpoint = ${private_ip}
EOF
    
    # Add KMS key if specified and not empty
    if [[ -n "$kms_key" && "$kms_key" != "null" ]]; then
        echo "kms_key                 = \"${kms_key}\"" >> "${tf_working_dir}/terraform.tfvars"
    fi
    
    # Change to working directory
    cd "$tf_working_dir"
    
    # Initialize Terraform
    log "INFO" "Initializing Terraform..."
    terraform init
    
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to initialize Terraform."
        return 1
    fi
    
    # Create Terraform plan
    log "INFO" "Creating Terraform plan..."
    terraform plan -out=tfplan
    
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to create Terraform plan."
        return 1
    fi
    
    # Apply Terraform plan if not in dry-run mode
    if [[ "$DRY_RUN" == false ]]; then
        log "INFO" "Applying Terraform plan..."
        
        # If force is not enabled, ask for confirmation
        if [[ "$FORCE" == false ]]; then
            read -p "Do you want to apply the Terraform plan? [y/N] " confirmation
            if [[ "$confirmation" != "y" && "$confirmation" != "Y" ]]; then
                log "INFO" "Terraform apply cancelled by user."
                return 0
            fi
        fi
        
        terraform apply tfplan
        
        if [[ $? -ne 0 ]]; then
            log "ERROR" "Failed to apply Terraform plan."
            return 1
        fi
        
        log "INFO" "Terraform apply completed successfully."
    else
        log "DRY_RUN" "Would apply Terraform plan for $environment environment."
    fi
    
    return 0
}

# Function to set up an environment using gcloud commands
setup_environment_gcloud() {
    local environment="$1"
    
    log "INFO" "Setting up $environment environment using gcloud commands..."
    
    # Load environment config
    local config=$(load_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load configuration for $environment environment."
        return 1
    fi
    
    # Extract configuration values
    local project_id=$(echo "$config" | jq -r '.gcp.project_id')
    local region=$(echo "$config" | jq -r '.gcp.region')
    local environment_name=$(echo "$config" | jq -r '.composer.environment_name')
    local node_count=$(echo "$config" | jq -r '.composer.node_count')
    local env_size=$(echo "$config" | jq -r '.composer.environment_size' | tr '[:lower:]' '[:upper:]')
    local network_name=$(echo "$config" | jq -r '.network.network_name')
    local subnet_name=$(echo "$config" | jq -r '.network.subnetwork_name')
    local service_account=$(echo "$config" | jq -r '.security.service_account')
    local private_ip=$(echo "$config" | jq -r '.network.enable_private_endpoint')
    local kms_key=$(echo "$config" | jq -r '.security.kms_key')
    local web_server_cpu=$(echo "$config" | jq -r '.composer.web_server.cpu // 2')
    local web_server_memory=$(echo "$config" | jq -r '.composer.web_server.memory_gb // 7.5')
    local scheduler_count=$(echo "$config" | jq -r '.composer.scheduler.count // 1')
    
    # Check if environment already exists
    local env_exists=$(gcloud composer environments describe "$environment_name" \
        --location="$region" \
        --project="$project_id" 2>/dev/null)
    
    if [[ $? -eq 0 ]]; then
        log "INFO" "Cloud Composer environment $environment_name already exists."
        
        # If force is not enabled and not in dry-run mode, ask for confirmation to update
        if [[ "$FORCE" == false && "$DRY_RUN" == false ]]; then
            read -p "Environment $environment_name already exists. Do you want to update it? [y/N] " confirmation
            if [[ "$confirmation" != "y" && "$confirmation" != "Y" ]]; then
                log "INFO" "Environment update cancelled by user."
                return 0
            fi
        fi
        
        log "INFO" "Updating Cloud Composer environment $environment_name..."
        
        # Build update command
        local update_cmd="gcloud composer environments update $environment_name \
            --location=$region \
            --project=$project_id"
        
        # Add optional parameters
        if [[ "$node_count" != "null" ]]; then
            update_cmd+=" --node-count=$node_count"
        fi
        
        if [[ "$scheduler_count" != "null" ]]; then
            update_cmd+=" --scheduler-count=$scheduler_count"
        fi
        
        if [[ "$web_server_cpu" != "null" && "$web_server_memory" != "null" ]]; then
            update_cmd+=" --web-server-cpu=$web_server_cpu --web-server-memory=${web_server_memory}GB"
        fi
        
        # Environment variables
        local env_vars=$(echo "$config" | jq -r '.composer.environment_variables | to_entries | map("\(.key)=\(.value)") | join(",")')
        if [[ -n "$env_vars" && "$env_vars" != "null" ]]; then
            update_cmd+=" --update-env-variables=$env_vars"
        fi
        
        # Add Airflow config overrides
        local airflow_config=$(echo "$config" | jq -r '.airflow | to_entries | map("core-\(.key)=\(.value)") | join(",")')
        if [[ -n "$airflow_config" && "$airflow_config" != "null" ]]; then
            update_cmd+=" --update-airflow-configs=$airflow_config"
        fi
        
        # Execute or dry-run
        if [[ "$DRY_RUN" == false ]]; then
            log "INFO" "Executing: $update_cmd"
            eval "$update_cmd"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to update Cloud Composer environment $environment_name."
                return 1
            fi
        else
            log "DRY_RUN" "Would update environment with command: $update_cmd"
        fi
    else
        log "INFO" "Creating new Cloud Composer 2 environment: $environment_name..."
        
        # Build create command
        local create_cmd="gcloud composer environments create $environment_name \
            --location=$region \
            --project=$project_id \
            --composer-version=$COMPOSER_VERSION \
            --image-version=composer-$COMPOSER_VERSION-airflow-$AIRFLOW_VERSION \
            --service-account=$service_account \
            --network=$network_name \
            --subnetwork=$subnet_name \
            --node-count=$node_count \
            --environment-size=$env_size"
        
        # Add web server resources
        if [[ "$web_server_cpu" != "null" && "$web_server_memory" != "null" ]]; then
            create_cmd+=" --web-server-cpu=$web_server_cpu --web-server-memory=${web_server_memory}GB"
        fi
        
        # Add scheduler count if not null
        if [[ "$scheduler_count" != "null" ]]; then
            create_cmd+=" --scheduler-count=$scheduler_count"
        fi
        
        # Add private IP if enabled
        if [[ "$private_ip" == "true" || "$private_ip" == "True" ]]; then
            create_cmd+=" --enable-private-environment --enable-ip-alias"
        fi
        
        # Add KMS key if specified and not empty
        if [[ -n "$kms_key" && "$kms_key" != "null" ]]; then
            create_cmd+=" --kms-key=$kms_key"
        fi
        
        # Environment variables
        local env_vars=$(echo "$config" | jq -r '.composer.environment_variables | to_entries | map("\(.key)=\(.value)") | join(",")')
        if [[ -n "$env_vars" && "$env_vars" != "null" ]]; then
            create_cmd+=" --env-variables=$env_vars"
        fi
        
        # Airflow config overrides
        local airflow_config=$(echo "$config" | jq -r '.airflow | to_entries | map("core-\(.key)=\(.value)") | join(",")')
        if [[ -n "$airflow_config" && "$airflow_config" != "null" ]]; then
            create_cmd+=" --airflow-configs=$airflow_config"
        fi
        
        # Execute or dry-run
        if [[ "$DRY_RUN" == false ]]; then
            log "INFO" "Executing: $create_cmd"
            eval "$create_cmd"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create Cloud Composer environment $environment_name."
                return 1
            fi
        else
            log "DRY_RUN" "Would create environment with command: $create_cmd"
        fi
    fi
    
    log "INFO" "Cloud Composer environment setup completed successfully for $environment environment."
    return 0
}

# Function to run Python setup script for advanced configuration
run_python_setup() {
    local environment="$1"
    
    log "INFO" "Running Python setup script for $environment environment..."
    
    # Load environment config
    local config=$(load_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load configuration for $environment environment."
        return 1
    fi
    
    # Prepare paths
    local python_script="${SCRIPT_DIR}/../../src/backend/scripts/setup_composer.py"
    local project_id=$(echo "$config" | jq -r '.gcp.project_id')
    local region=$(echo "$config" | jq -r '.gcp.region')
    local environment_name=$(echo "$config" | jq -r '.composer.environment_name')
    
    # Check if Python script exists
    if [[ ! -f "$python_script" ]]; then
        log "ERROR" "Python setup script not found: $python_script"
        return 1
    fi
    
    # Prepare command
    local cmd="python3 $python_script --environment=$environment --operation=update"
    cmd+=" --airflow-version=$AIRFLOW_VERSION --region=$region"
    
    # Prepare variables and connections files
    local variables_file="${SCRIPT_DIR}/../../src/backend/config/variables_${environment}.json"
    local connections_file="${SCRIPT_DIR}/../../src/backend/config/connections_${environment}.json"
    
    if [[ -f "$variables_file" ]]; then
        cmd+=" --variables-file=$variables_file"
    fi
    
    if [[ -f "$connections_file" ]]; then
        cmd+=" --connections-file=$connections_file"
    fi
    
    # Add dry-run flag if needed
    if [[ "$DRY_RUN" == true ]]; then
        cmd+=" --dry-run"
    fi
    
    # Add force flag if needed
    if [[ "$FORCE" == true ]]; then
        cmd+=" --force"
    fi
    
    # Execute command
    log "INFO" "Executing Python setup command: $cmd"
    eval "$cmd"
    
    local result=$?
    if [[ $result -ne 0 ]]; then
        log "ERROR" "Failed to run Python setup script with exit code: $result"
        return 1
    fi
    
    log "INFO" "Python setup script executed successfully for $environment environment."
    return 0
}

# Function to import variables, connections, and pools into the environment
import_environment_data() {
    local environment="$1"
    
    log "INFO" "Importing data for $environment environment..."
    
    # Load environment config
    local config=$(load_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load configuration for $environment environment."
        return 1
    fi
    
    # Locate files
    local variables_file="${SCRIPT_DIR}/../../src/backend/config/variables_${environment}.json"
    local connections_file="${SCRIPT_DIR}/../../src/backend/config/connections_${environment}.json"
    local pools_file="${SCRIPT_DIR}/../../src/backend/config/pools_${environment}.json"
    
    # Import variables if file exists
    if [[ -f "$variables_file" ]]; then
        log "INFO" "Importing variables from $variables_file..."
        
        if [[ "$DRY_RUN" == false ]]; then
            local cmd="python3 ${SCRIPT_DIR}/../../src/backend/scripts/import_variables.py --file-path=$variables_file --environment=$environment"
            
            # Add force flag if needed
            if [[ "$FORCE" == true ]]; then
                cmd+=" --force"
            fi
            
            log "INFO" "Executing: $cmd"
            eval "$cmd"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to import variables for $environment environment."
                return 1
            fi
        else
            log "DRY_RUN" "Would import variables from $variables_file"
        fi
    else
        log "WARN" "Variables file not found: $variables_file"
    fi
    
    # Import connections if file exists
    if [[ -f "$connections_file" ]]; then
        log "INFO" "Importing connections from $connections_file..."
        
        if [[ "$DRY_RUN" == false ]]; then
            local cmd="python3 ${SCRIPT_DIR}/../../src/backend/scripts/import_connections.py --file-path=$connections_file --environment=$environment"
            
            # Add force flag if needed
            if [[ "$FORCE" == true ]]; then
                cmd+=" --force"
            fi
            
            log "INFO" "Executing: $cmd"
            eval "$cmd"
            
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to import connections for $environment environment."
                return 1
            fi
        else
            log "DRY_RUN" "Would import connections from $connections_file"
        fi
    else
        log "WARN" "Connections file not found: $connections_file"
    fi
    
    # Import pools from configuration
    if [[ "$DRY_RUN" == false ]]; then
        local pools=$(echo "$config" | jq -r '.pools')
        
        if [[ -n "$pools" && "$pools" != "null" ]]; then
            log "INFO" "Importing pools for $environment environment..."
            
            # Get the Cloud Composer environment's Airflow endpoint URL
            local project_id=$(echo "$config" | jq -r '.gcp.project_id')
            local region=$(echo "$config" | jq -r '.gcp.region')
            local environment_name=$(echo "$config" | jq -r '.composer.environment_name')
            
            # Get the Airflow web server URI
            local airflow_uri=$(gcloud composer environments describe "$environment_name" \
                --location="$region" \
                --project="$project_id" \
                --format="value(config.airflowUri)" 2>/dev/null)
            
            if [[ -z "$airflow_uri" ]]; then
                log "WARN" "Could not get Airflow URI for $environment environment. Skipping pool import."
            else
                # Loop through each pool and create it
                echo "$pools" | jq -c '.[]' | while read -r pool; do
                    local pool_name=$(echo "$pool" | jq -r '.name')
                    local pool_slots=$(echo "$pool" | jq -r '.slots')
                    local pool_desc=$(echo "$pool" | jq -r '.description')
                    
                    log "INFO" "Creating pool $pool_name with $pool_slots slots..."
                    
                    # Create command to create or update the pool
                    gcloud composer environments run "$environment_name" \
                        --location="$region" \
                        --project="$project_id" \
                        pools -- --set \
                        --pool-name "$pool_name" \
                        --pool-slots "$pool_slots" \
                        --pool-description "$pool_desc"
                    
                    if [[ $? -ne 0 ]]; then
                        log "WARN" "Failed to create/update pool $pool_name. Continuing..."
                    fi
                done
            fi
        else
            log "INFO" "No pools defined in configuration for $environment environment."
        fi
    else
        log "DRY_RUN" "Would import pools from configuration"
    fi
    
    log "INFO" "Environment data import completed for $environment environment."
    return 0
}

# Function to validate an environment configuration and setup
validate_environment() {
    local environment="$1"
    
    log "INFO" "Validating $environment environment configuration..."
    
    # Load environment config
    local config=$(load_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load configuration for $environment environment."
        return 1
    fi
    
    # Validate network configuration
    log "INFO" "Validating network configuration..."
    local network_name=$(echo "$config" | jq -r '.network.network_name')
    local subnet_name=$(echo "$config" | jq -r '.network.subnetwork_name')
    local region=$(echo "$config" | jq -r '.gcp.region')
    local project_id=$(echo "$config" | jq -r '.gcp.project_id')
    
    if [[ -z "$network_name" || "$network_name" == "null" ]]; then
        log "ERROR" "Network name is not specified in configuration."
        return 1
    fi
    
    if [[ -z "$subnet_name" || "$subnet_name" == "null" ]]; then
        log "ERROR" "Subnet name is not specified in configuration."
        return 1
    fi
    
    # Validate security configuration
    log "INFO" "Validating security configuration..."
    local service_account=$(echo "$config" | jq -r '.security.service_account')
    
    if [[ -z "$service_account" || "$service_account" == "null" ]]; then
        log "ERROR" "Service account is not specified in configuration."
        return 1
    fi
    
    # Validate Composer configuration
    log "INFO" "Validating Composer configuration..."
    local environment_name=$(echo "$config" | jq -r '.composer.environment_name')
    local node_count=$(echo "$config" | jq -r '.composer.node_count')
    
    if [[ -z "$environment_name" || "$environment_name" == "null" ]]; then
        log "ERROR" "Environment name is not specified in configuration."
        return 1
    fi
    
    if [[ -z "$node_count" || "$node_count" == "null" ]]; then
        log "ERROR" "Node count is not specified in configuration."
        return 1
    fi
    
    # Environment-specific validations
    case "$environment" in
        prod)
            # Validate production-specific settings
            local high_availability=$(echo "$config" | jq -r '.composer.enable_high_availability')
            local scheduler_count=$(echo "$config" | jq -r '.composer.scheduler.count // 1')
            
            if [[ "$high_availability" != "true" && "$high_availability" != "True" ]]; then
                log "WARN" "High availability is not enabled for production environment."
            fi
            
            if [[ "$scheduler_count" -lt 2 ]]; then
                log "WARN" "Production environment should have at least 2 schedulers for reliability."
            fi
            
            # Validate network settings for production
            local private_ip=$(echo "$config" | jq -r '.network.enable_private_endpoint')
            if [[ "$private_ip" != "true" && "$private_ip" != "True" ]]; then
                log "WARN" "Private IP is not enabled for production environment."
            fi
            ;;
        
        qa)
            # Validate QA-specific settings
            local scheduler_count=$(echo "$config" | jq -r '.composer.scheduler.count // 1')
            
            if [[ "$scheduler_count" -lt 1 ]]; then
                log "WARN" "QA environment should have at least 1 scheduler."
            fi
            ;;
        
        dev)
            # Validate development-specific settings
            local load_examples=$(echo "$config" | jq -r '.airflow.load_examples')
            
            if [[ "$load_examples" == "true" || "$load_examples" == "True" ]]; then
                log "WARN" "Example DAGs are enabled in development environment, which may not be desired."
            fi
            ;;
    esac
    
    # Verify that the configuration matches the environment
    local config_env=$(echo "$config" | jq -r '.environment.name')
    if [[ "$config_env" != "$environment" ]]; then
        log "ERROR" "Configuration environment name ($config_env) does not match specified environment ($environment)."
        return 1
    fi
    
    # Run Python validation if environment exists and not in dry-run mode
    if [[ "$DRY_RUN" == false ]]; then
        local env_exists=$(gcloud composer environments describe "$environment_name" \
            --location="$region" \
            --project="$project_id" 2>/dev/null)
        
        if [[ $? -eq 0 ]]; then
            log "INFO" "Running Python validation for existing environment..."
            
            # Prepare Python command
            local python_script="${SCRIPT_DIR}/../../src/backend/scripts/setup_composer.py"
            local cmd="python3 $python_script --environment=$environment --operation=validate"
            cmd+=" --region=$region"
            
            log "INFO" "Executing: $cmd"
            eval "$cmd"
            
            if [[ $? -ne 0 ]]; then
                log "WARN" "Python validation reported issues with the environment."
            fi
        fi
    fi
    
    log "INFO" "Validation completed for $environment environment."
    return 0
}

# Main function to set up a specific environment
setup_environment() {
    local environment="$1"
    
    log "INFO" "Starting setup of $environment environment..."
    
    # Load environment config
    local config=$(load_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load configuration for $environment environment."
        return 1
    fi
    
    # If validate-only, run validation and return
    if [[ "$VALIDATE_ONLY" == true ]]; then
        validate_environment "$environment"
        return $?
    fi
    
    # Setup environment network
    setup_environment_network "$environment"
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to setup network for $environment environment."
        return 1
    fi
    
    # Setup environment security
    setup_environment_security "$environment"
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to setup security for $environment environment."
        return 1
    fi
    
    # Setup environment using Terraform or gcloud
    if [[ "$USE_TERRAFORM" == true ]]; then
        setup_environment_terraform "$environment"
        if [[ $? -ne 0 ]]; then
            log "ERROR" "Failed to setup $environment environment using Terraform."
            return 1
        fi
    else
        setup_environment_gcloud "$environment"
        if [[ $? -ne 0 ]]; then
            log "ERROR" "Failed to setup $environment environment using gcloud."
            return 1
        fi
    fi
    
    # Run Python setup script for advanced configuration
    run_python_setup "$environment"
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to run Python setup script for $environment environment."
        return 1
    fi
    
    # Import environment data (variables, connections, pools)
    import_environment_data "$environment"
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to import data for $environment environment."
        return 1
    fi
    
    # Validate final setup
    validate_environment "$environment"
    if [[ $? -ne 0 ]]; then
        log "WARN" "Environment validation found issues with $environment environment."
    fi
    
    log "INFO" "Setup completed successfully for $environment environment."
    return 0
}

# Function to perform cleanup operations
cleanup() {
    log "INFO" "Performing cleanup operations..."
    
    # Clean up temporary files if any
    if [[ -f "/tmp/composer_config_$$.json" ]]; then
        rm -f "/tmp/composer_config_$$.json"
    fi
    
    log "INFO" "Cleanup completed."
}

# Main entry point for the script
main() {
    # Initialize log file
    echo "Starting setup_environments.sh at $(date)" > "$LOG_FILE"
    
    # Parse command line arguments
    parse_args "$@"
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to parse command line arguments."
        return 1
    fi
    
    # Check prerequisites
    check_prerequisites
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Prerequisites check failed."
        return 1
    fi
    
    local overall_status=0
    
    # Setup each environment
    for environment in "${ENVIRONMENTS[@]}"; do
        log "INFO" "== Setting up $environment environment =="
        
        setup_environment "$environment"
        local status=$?
        
        if [[ $status -ne 0 ]]; then
            log "ERROR" "Failed to setup $environment environment."
            
            if [[ "$FORCE" != true ]]; then
                log "ERROR" "Stopping further environment setup due to failure."
                overall_status=1
                break
            else
                log "WARN" "Continuing with next environment despite failure (--force enabled)."
                overall_status=1
            fi
        fi
        
        log "INFO" "== Completed setup of $environment environment =="
    done
    
    if [[ $overall_status -eq 0 ]]; then
        log "INFO" "All environment setups completed successfully."
    else
        log "ERROR" "One or more environment setups failed. Check the log for details: $LOG_FILE"
    fi
    
    return $overall_status
}

# Set up error handling
trap cleanup EXIT

# Execute main function with all arguments
main "$@"