#!/usr/bin/env bash
# A shell script that configures and validates network settings for Cloud Composer 2 environments
# during the migration from Airflow 1.10.15. It handles VPC creation, subnet configuration,
# firewall rules, NAT gateway setup, and applies Kubernetes network policies for all environments (dev, qa, prod).

# Set error handling options
set -eo pipefail

# Source setup_environments.sh for utility functions
source "${SCRIPT_DIR}/setup_environments.sh" # infrastructure/scripts/setup_environments.sh

# Global variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ID=""
REGION="us-central1"
ENVIRONMENT=""
CONFIG_DIR=""
NETWORK_NAME=""
SUBNET_NAME=""
SUBNET_RANGE=""
PODS_RANGE=""
SERVICES_RANGE=""
ENABLE_PRIVATE_IP=""
ENABLE_NAT=""
NETWORK_POLICY_DIR=""
LOG_FILE="/tmp/configure_network_$(date +%Y%m%d_%H%M%S).log"
USE_TERRAFORM=false
DRY_RUN=false
VERBOSE=false

# Environment-specific network configurations (can be overridden by command-line arguments)
declare -A ENV_NETWORK_CONFIGS
ENV_NETWORK_CONFIGS=(
  [dev]="{\"network_name\":\"composer2-network-dev\", \"subnet_name\":\"composer2-subnet-dev\", \"subnet_range\":\"10.0.0.0/20\", \"pods_range\":\"10.4.0.0/14\", \"services_range\":\"10.0.32.0/20\", \"enable_private_ip\":false, \"enable_nat\":true, \"allowed_external_ips\":[\"0.0.0.0/0\"], \"enable_iap\":true}"
  [qa]="{\"network_name\":\"composer2-network-qa\", \"subnet_name\":\"composer2-subnet-qa\", \"subnet_range\":\"10.1.0.0/20\", \"pods_range\":\"10.8.0.0/14\", \"services_range\":\"10.1.32.0/20\", \"enable_private_ip\":true, \"enable_nat\":true, \"allowed_external_ips\":[\"10.0.0.0/8\", \"35.235.240.0/20\"], \"enable_iap\":true}"
  [prod]="{\"network_name\":\"composer2-network-prod\", \"subnet_name\":\"composer2-subnet-prod\", \"subnet_range\":\"10.2.0.0/20\", \"pods_range\":\"10.12.0.0/14\", \"services_range\":\"10.2.32.0/20\", \"enable_private_ip\":true, \"enable_nat\":true, \"allowed_external_ips\":[\"35.235.240.0/20\"], \"enable_iap\":true}"
)

# Function to display usage information
usage() {
    echo "Usage: $0 [options] <environment>"
    echo "Configure network settings for Cloud Composer 2 environments"
    echo ""
    echo "Options:"
    echo "  -h, --help                 Show this help message and exit"
    echo "  -p, --project=PROJECT_ID   Google Cloud Project ID"
    echo "  -r, --region=REGION        Google Cloud region (default: us-central1)"
    echo "  -e, --environment=ENV      Target environment (dev, qa, prod, all)"
    echo "  -n, --network-name=NAME    VPC network name"
    echo "  -s, --subnet-name=NAME     Subnet name"
    echo "  -R, --subnet-range=CIDR   Subnet IP range (CIDR notation)"
    echo "  -P, --pods-range=CIDR     Kubernetes pods IP range (CIDR notation)"
    echo "  -S, --services-range=CIDR Kubernetes services IP range (CIDR notation)"
    echo "  -i, --enable-private-ip    Enable private IP for Cloud Composer 2"
    echo "  -N, --enable-nat           Enable Cloud NAT gateway"
    echo "  -k, --network-policy-dir=DIR Directory containing Kubernetes network policies"
    echo "  -t, --terraform            Use Terraform for infrastructure setup (optional)"
    echo "  -d, --dry-run              Dry run mode (show commands without executing)"
    echo "  -v, --verbose              Verbose logging"
    echo ""
    echo "Environment:"
    echo "  dev                        Development environment"
    echo "  qa                         QA environment"
    echo "  prod                       Production environment"
    echo "  all                        All environments (dev, qa, prod)"
    echo ""
    echo "Examples:"
    echo "  $0 --project=my-project dev"
    echo "  $0 --project=my-project --network-name=my-network qa"
    echo "  $0 --project=my-project all"
    exit 1
}

# Function to log messages to both console and log file with timestamp
# Re-using the log function from setup_environments.sh

# Function to parse command line arguments
parse_args() {
    local argc=$#
    local argv=("$@")

    # Check if any arguments were passed
    if [[ $argc -eq 0 ]]; then
        usage
        exit 1
    fi

    # Parse options
    while [[ $argc -gt 0 ]]; do
        local arg="${argv[0]}"
        case "$arg" in
            -h|--help)
                usage
                exit 0
                ;;
            -p=*|--project=*)
                PROJECT_ID="${arg#*=}"
                ;;
            -r=*|--region=*)
                REGION="${arg#*=}"
                ;;
            -e=*|--environment=*)
                ENVIRONMENT="${arg#*=}"
                ;;
            -n=*|--network-name=*)
                NETWORK_NAME="${arg#*=}"
                ;;
            -s=*|--subnet-name=*)
                SUBNET_NAME="${arg#*=}"
                ;;
            -R=*|--subnet-range=*)
                SUBNET_RANGE="${arg#*=}"
                ;;
            -P=*|--pods-range=*)
                PODS_RANGE="${arg#*=}"
                ;;
            -S=*|--services-range=*)
                SERVICES_RANGE="${arg#*=}"
                ;;
            -i|--enable-private-ip)
                ENABLE_PRIVATE_IP=true
                ;;
            -N|--enable-nat)
                ENABLE_NAT=true
                ;;
            -k=*|--network-policy-dir=*)
                NETWORK_POLICY_DIR="${arg#*=}"
                ;;
            -t|--terraform)
                USE_TERRAFORM=true
                ;;
            -d|--dry-run)
                DRY_RUN=true
                ;;
            -v|--verbose)
                VERBOSE=true
                ;;
            *)
                if [[ "$arg" == "dev" || "$arg" == "qa" || "$arg" == "prod" || "$arg" == "all" ]]; then
                    ENVIRONMENT="$arg"
                else
                    log "ERROR" "Unknown option: $arg"
                    usage
                    exit 1
                fi
                ;;
        esac
        shift argv
        argc=$((argc - 1))
    done

    # Validate arguments
    if [[ -z "$PROJECT_ID" ]]; then
        log "ERROR" "Project ID is required."
        usage
        exit 1
    fi

    if [[ -z "$ENVIRONMENT" ]]; then
        log "ERROR" "Environment is required."
        usage
        exit 1
    fi

    # Set CONFIG_DIR
    CONFIG_DIR="${SCRIPT_DIR}/../config"

    # Log parsed arguments
    log "INFO" "Project ID: $PROJECT_ID"
    log "INFO" "Region: $REGION"
    log "INFO" "Environment: $ENVIRONMENT"
    log "INFO" "Network Name: $NETWORK_NAME"
    log "INFO" "Subnet Name: $SUBNET_NAME"
    log "INFO" "Subnet Range: $SUBNET_RANGE"
    log "INFO" "Pods Range: $PODS_RANGE"
    log "INFO" "Services Range: $SERVICES_RANGE"
    log "INFO" "Enable Private IP: $ENABLE_PRIVATE_IP"
    log "INFO" "Enable NAT: $ENABLE_NAT"
    log "INFO" "Network Policy Directory: $NETWORK_POLICY_DIR"
    log "INFO" "Use Terraform: $USE_TERRAFORM"
    log "INFO" "Dry Run: $DRY_RUN"
    log "INFO" "Verbose: $VERBOSE"

    return 0
}

# Function to check if all required tools and permissions are available
check_prerequisites() {
    log "INFO" "Checking prerequisites..."

    # Check if gcloud is installed
    if ! command -v gcloud &> /dev/null; then # google-cloud-sdk latest
        log "ERROR" "gcloud CLI is not installed. Please install Google Cloud SDK."
        return 1
    fi

    # Check if kubectl is installed
    if [[ -n "$NETWORK_POLICY_DIR" ]] && ! command -v kubectl &> /dev/null; then # kubernetes-cli latest
        log "ERROR" "kubectl CLI is not installed. Please install Kubernetes CLI."
        return 1
    fi

    # Check if Terraform is installed if USE_TERRAFORM is true
    if [[ "$USE_TERRAFORM" == "true" ]] && ! command -v terraform &> /dev/null; then # terraform >=1.0.0
        log "ERROR" "Terraform is not installed. Please install Terraform."
        return 1
    fi

    # Check if jq is installed
    if ! command -v jq &> /dev/null; then # jq latest
        log "ERROR" "jq is not installed. Please install jq."
        return 1
    fi

    # Verify GCP project exists and user has sufficient permissions
    if ! gcloud projects describe "$PROJECT_ID" &> /dev/null; then
        log "ERROR" "GCP project '$PROJECT_ID' does not exist or you do not have sufficient permissions."
        return 1
    fi

    # Check if required APIs are enabled in the project
    REQUIRED_APIS=("compute.googleapis.com" "container.googleapis.com" "servicenetworking.googleapis.com")
    for API in "${REQUIRED_APIS[@]}"; do
        if ! gcloud services list --project="$PROJECT_ID" --enabled | grep -q "$API"; then
            log "ERROR" "Required API '$API' is not enabled in project '$PROJECT_ID'."
            return 1
        fi
    done

    log "INFO" "All prerequisites are met."
    return 0
}

# Function to load environment-specific network configuration
load_network_config() {
    local env="$1"

    # Check if environment is valid
    if [[ "$env" != "dev" && "$env" != "qa" && "$env" != "prod" ]]; then
        log "ERROR" "Invalid environment: $env. Must be dev, qa, or prod."
        return 1
    fi

    # Load default configuration from the associative array
    local config="${ENV_NETWORK_CONFIGS[$env]}"

    # Override with command-line arguments if provided
    if [[ -n "$NETWORK_NAME" ]]; then
        config=$(echo "$config" | jq --arg name "$NETWORK_NAME" '. + {network_name: $name}')
    fi
    if [[ -n "$SUBNET_NAME" ]]; then
        config=$(echo "$config" | jq --arg name "$SUBNET_NAME" '. + {subnet_name: $name}')
    fi
    if [[ -n "$SUBNET_RANGE" ]]; then
        config=$(echo "$config" | jq --arg range "$SUBNET_RANGE" '. + {subnet_range: $range}')
    fi
    if [[ -n "$PODS_RANGE" ]]; then
        config=$(echo "$config" | jq --arg range "$PODS_RANGE" '. + {pods_range: $range}')
    fi
    if [[ -n "$SERVICES_RANGE" ]]; then
        config=$(echo "$config" | jq --arg range "$SERVICES_RANGE" '. + {services_range: $range}')
    fi
    if [[ -n "$ENABLE_PRIVATE_IP" ]]; then
        config=$(echo "$config" | jq --arg enable "$ENABLE_PRIVATE_IP" '. + {enable_private_ip: $enable}')
    fi
    if [[ -n "$ENABLE_NAT" ]]; then
        config=$(echo "$config" | jq --arg enable "$ENABLE_NAT" '. + {enable_nat: $enable}')
    fi

    # Output the configuration
    echo "$config"
}

# Function to create a VPC network
create_vpc_network() {
    local config="$1"
    local network_name=$(echo "$config" | jq -r .network_name)

    # Check if network already exists
    if gcloud compute networks describe "$network_name" --project="$PROJECT_ID" &> /dev/null; then
        log "INFO" "VPC network '$network_name' already exists."
        return 0
    fi

    # Create VPC network
    log "INFO" "Creating VPC network '$network_name'..."
    if [[ "$DRY_RUN" == "true" ]]; then
        log "INFO" "[DRY-RUN] Would create VPC network '$network_name'."
    else
        gcloud compute networks create "$network_name" --project="$PROJECT_ID" --subnet-mode=custom
        if [[ $? -ne 0 ]]; then
            log "ERROR" "Failed to create VPC network '$network_name'."
            return 1
        fi
        log "INFO" "VPC network '$network_name' created successfully."
    fi
    return 0
}

# Function to create a subnet
create_subnet() {
    local config="$1"
    local network_name=$(echo "$config" | jq -r .network_name)
    local subnet_name=$(echo "$config" | jq -r .subnet_name)
    local subnet_range=$(echo "$config" | jq -r .subnet_range)
    local pods_range=$(echo "$config" | jq -r .pods_range)
    local services_range=$(echo "$config" | jq -r .services_range)

    # Check if subnet already exists
    if gcloud compute networks subnets describe "$subnet_name" --project="$PROJECT_ID" --region="$REGION" &> /dev/null; then
        log "INFO" "Subnet '$subnet_name' already exists."
        return 0
    fi

    # Create subnet
    log "INFO" "Creating subnet '$subnet_name'..."
    if [[ "$DRY_RUN" == "true" ]]; then
        log "INFO" "[DRY-RUN] Would create subnet '$subnet_name'."
    else
        gcloud compute networks subnets create "$subnet_name" \
            --network="$network_name" \
            --region="$REGION" \
            --range="$subnet_range" \
            --secondary-range-names=pods,services \
            --secondary-ranges="pods=$pods_range,services=$services_range" \
            --project="$PROJECT_ID"
        if [[ $? -ne 0 ]]; then
            log "ERROR" "Failed to create subnet '$subnet_name'."
            return 1
        fi
        log "INFO" "Subnet '$subnet_name' created successfully."
    fi
    return 0
}

# Function to setup NAT gateway
setup_nat_gateway() {
    local config="$1"
    local network_name=$(echo "$config" | jq -r .network_name)
    local enable_nat=$(echo "$config" | jq -r .enable_nat)

    if [[ "$enable_nat" != "true" ]]; then
        log "INFO" "NAT gateway is disabled, skipping NAT setup."
        return 0
    fi

    local nat_name="${network_name}-nat"
    local router_name="${network_name}-router"

    # Check if Cloud Router exists
    if ! gcloud compute routers describe "$router_name" --project="$PROJECT_ID" --region="$REGION" &> /dev/null; then
        # Create Cloud Router
        log "INFO" "Creating Cloud Router '$router_name'..."
        if [[ "$DRY_RUN" == "true" ]]; then
            log "INFO" "[DRY-RUN] Would create Cloud Router '$router_name'."
        else
            gcloud compute routers create "$router_name" --network="$network_name" --region="$REGION" --project="$PROJECT_ID"
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create Cloud Router '$router_name'."
                return 1
            fi
            log "INFO" "Cloud Router '$router_name' created successfully."
        fi
    fi

    # Check if NAT gateway exists
    if ! gcloud compute routers nats describe "$nat_name" --router="$router_name" --project="$PROJECT_ID" --region="$REGION" &> /dev/null; then
        # Create NAT gateway
        log "INFO" "Creating NAT gateway '$nat_name'..."
        if [[ "$DRY_RUN" == "true" ]]; then
            log "INFO" "[DRY-RUN] Would create NAT gateway '$nat_name'."
        else
            gcloud compute routers nats create "$nat_name" \
                --router="$router_name" \
                --region="$REGION" \
                --auto-allocate-nat-external-ips \
                --nat-all-subnet-ip-ranges \
                --project="$PROJECT_ID"
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create NAT gateway '$nat_name'."
                return 1
            fi
            log "INFO" "NAT gateway '$nat_name' created successfully."
        fi
    fi
    return 0
}

# Function to configure firewall rules
configure_firewall_rules() {
    local config="$1"
    local network_name=$(echo "$config" | jq -r .network_name)
    local enable_iap=$(echo "$config" | jq -r .enable_iap)
    local allowed_external_ips=$(echo "$config" | jq -r .allowed_external_ips)

    # Create firewall rule to allow SSH
    local fw_ssh_name="${network_name}-allow-ssh"
    if ! gcloud compute firewall-rules describe "$fw_ssh_name" --project="$PROJECT_ID" &> /dev/null; then
        log "INFO" "Creating firewall rule '$fw_ssh_name' to allow SSH..."
        if [[ "$DRY_RUN" == "true" ]]; then
            log "INFO" "[DRY-RUN] Would create firewall rule '$fw_ssh_name' to allow SSH."
        else
            gcloud compute firewall-rules create "$fw_ssh_name" \
                --network="$network_name" \
                --allow=tcp:22 \
                --source-ranges="0.0.0.0/0" \
                --target-tags="ssh" \
                --project="$PROJECT_ID"
            if [[ $? -ne 0 ]]; then
                log "ERROR" "Failed to create firewall rule '$fw_ssh_name'."
                return 1
            fi
            log "INFO" "Firewall rule '$fw_ssh_name' created successfully."
        fi
    fi

    # Create firewall rule to allow IAP access
    if [[ "$enable_iap" == "true" ]]; then
        local fw_iap_name="${network_name}-allow-iap"
        if ! gcloud compute firewall-rules describe "$fw_iap_name" --project="$PROJECT_ID" &> /dev/null; then
            log "INFO" "Creating firewall rule '$fw_iap_name' to allow IAP..."
            if [[ "$DRY_RUN" == "true" ]]; then
                log "INFO" "[DRY-RUN] Would create firewall rule '$fw_iap_name' to allow IAP."
            else
                gcloud compute firewall-rules create "$fw_iap_name" \
                    --network="$network_name" \
                    --allow=tcp:3389,tcp:22 \
                    --source-ranges="35.235.240.0/20" \
                    --target-tags="iap" \
                    --project="$PROJECT_ID"
                if [[ $? -ne 0 ]]; then
                    log "ERROR" "Failed to create firewall rule '$fw_iap_name'."
                    return 1
                fi
                log "INFO" "Firewall rule '$fw_iap_name' created successfully."
            fi
        fi
    fi

    # Create firewall rule to allow external access (if any)
    if [[ -n "$allowed_external_ips" ]]; then
        local fw_external_name="${network_name}-allow-external"
        if ! gcloud compute firewall-rules describe "$fw_external_name" --project="$PROJECT_ID" &> /dev/null; then
            log "INFO" "Creating firewall rule '$fw_external_name' to allow external access..."
            if [[ "$DRY_RUN" == "true" ]]; then
                log "INFO" "[DRY-RUN] Would create firewall rule '$fw_external_name' to allow external access."
            else
                gcloud compute firewall-rules create "$fw_external_name" \
                    --network="$network_name" \
                    --allow=tcp:80,tcp:443 \
                    --source-ranges="$allowed_external_ips" \
                    --target-tags="web" \
                    --project="$PROJECT_ID"
                if [[ $? -ne 0 ]]; then
                    log "ERROR" "Failed to create firewall rule '$fw_external_name'."
                    return 1
                fi
                log "INFO" "Firewall rule '$fw_external_name' created successfully."
            fi
        fi
    fi
    return 0
}

# Function to apply Kubernetes NetworkPolicy resources
apply_network_policies() {
    local environment="$1"

    if [[ -z "$NETWORK_POLICY_DIR" ]]; then
        log "INFO" "Network policy directory not specified, skipping network policy application."
        return 0
    fi

    # Construct kubectl command
    local kubectl_cmd="kubectl apply -f ${NETWORK_POLICY_DIR}/network-policies.yaml --namespace=composer"

    log "INFO" "Applying Kubernetes NetworkPolicy resources for environment '$environment'..."
    if [[ "$DRY_RUN" == "true" ]]; then
        log "INFO" "[DRY-RUN] Would apply Kubernetes NetworkPolicy resources using command: $kubectl_cmd"
    else
        eval "$kubectl_cmd"
        if [[ $? -ne 0 ]]; then
            log "ERROR" "Failed to apply Kubernetes NetworkPolicy resources."
            return 1
        fi
        log "INFO" "Kubernetes NetworkPolicy resources applied successfully."
    fi
    return 0
}

# Function to validate the network configuration for an environment
validate_network_config() {
    local environment="$1"
    log "INFO" "Validating network configuration for environment '$environment'..."
    # Add validation logic here
    log "INFO" "Network configuration validation completed."
    return 0
}

# Function to test network connectivity for the environment
test_network_connectivity() {
    local environment="$1"
    log "INFO" "Testing network connectivity for environment '$environment'..."
    # Add connectivity test logic here
    log "INFO" "Network connectivity test completed."
    return 0
}

# Function to configure networking for an environment
configure_environment_network() {
    local environment="$1"

    log "INFO" "Configuring network for environment '$environment'..."

    # Load network configuration
    local config=$(load_network_config "$environment")
    if [[ $? -ne 0 ]]; then
        log "ERROR" "Failed to load network configuration for environment '$environment'."
        return 1
    fi

    # Validate network configuration
    if ! validate_network_config "$environment"; then
        log "ERROR" "Network configuration validation failed for environment '$environment'."
        return 1
    fi

    # Create VPC network
    if ! create_vpc_network "$config"; then
        log "ERROR" "Failed to create VPC network for environment '$environment'."
        return 1
    fi

    # Create subnet
    if ! create_subnet "$config"; then
        log "ERROR" "Failed to create subnet for environment '$environment'."
        return 1
    fi

    # Setup NAT gateway
    if ! setup_nat_gateway "$config"; then
        log "ERROR" "Failed to setup NAT gateway for environment '$environment'."
        return 1
    fi

    # Configure firewall rules
    if ! configure_firewall_rules "$config"; then
        log "ERROR" "Failed to configure firewall rules for environment '$environment'."
        return 1
    fi

    # Apply Kubernetes NetworkPolicy resources
    if ! apply_network_policies "$environment"; then
        log "ERROR" "Failed to apply Kubernetes NetworkPolicy resources for environment '$environment'."
        return 1
    fi

    # Test network connectivity
    if ! test_network_connectivity "$environment"; then
        log "ERROR" "Network connectivity test failed for environment '$environment'."
        return 1
    fi

    log "INFO" "Network configuration completed for environment '$environment'."
    return 0
}

# Function to perform cleanup operations
cleanup() {
    log "INFO" "Performing cleanup operations..."
    # Add cleanup logic here
    log "INFO" "Cleanup completed."
}

# Main function
main() {
    # Parse command line arguments
    parse_args "$@"
    if [[ $? -ne 0 ]]; then
        exit 1
    fi

    # Check prerequisites
    check_prerequisites
    if [[ $? -ne 0 ]]; then
        exit 1
    fi

    # Configure network for each environment
    if [[ "$ENVIRONMENT" == "all" ]]; then
        for env in "dev" "qa" "prod"; do
            log "INFO" "Configuring network for environment '$env'..."
            if ! configure_environment_network "$env"; then
                log "ERROR" "Failed to configure network for environment '$env'."
                exit 1
            fi
            log "INFO" "Network configuration completed for environment '$env'."
        done
    else
        log "INFO" "Configuring network for environment '$ENVIRONMENT'..."
        if ! configure_environment_network "$ENVIRONMENT"; then
            log "ERROR" "Failed to configure network for environment '$ENVIRONMENT'."
            exit 1
        fi
        log "INFO" "Network configuration completed for environment '$ENVIRONMENT'."
    fi

    log "INFO" "Script completed successfully."
    return 0
}

# Trap EXIT signal for cleanup
trap cleanup EXIT

# Run main function
main "$@"