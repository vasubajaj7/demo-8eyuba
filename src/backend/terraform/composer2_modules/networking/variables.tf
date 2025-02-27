# ---------------------------------------------------------------------------------------------------------------------
# REQUIRED PARAMETERS
# These variables must be passed in by the operator.
# ---------------------------------------------------------------------------------------------------------------------

variable "project_id" {
  description = "The GCP project ID where networking resources will be deployed"
  type        = string
}

# ---------------------------------------------------------------------------------------------------------------------
# OPTIONAL PARAMETERS
# These variables have defaults, but can be overridden by the operator.
# ---------------------------------------------------------------------------------------------------------------------

# General configuration
variable "region" {
  description = "The GCP region where networking resources will be deployed"
  type        = string
  default     = "us-central1"
}

# VPC configuration
variable "vpc_name" {
  description = "Name of the VPC network to create or use for Cloud Composer 2"
  type        = string
  default     = "composer2-network"
}

variable "create_vpc" {
  description = "Whether to create a new VPC network or use an existing one"
  type        = bool
  default     = true
}

variable "existing_vpc_id" {
  description = "ID of existing VPC network if not creating a new one"
  type        = string
  default     = ""
}

# Subnet configuration
variable "subnet_name" {
  description = "Name of the subnet to create for Cloud Composer 2"
  type        = string
  default     = "composer2-subnet"
}

variable "subnet_ip_range" {
  description = "Primary IP range for the subnet"
  type        = string
  default     = "10.0.0.0/20"
}

variable "existing_subnet_id" {
  description = "ID of existing subnet if not creating a new one"
  type        = string
  default     = ""
}

# Secondary IP ranges for GKE
variable "subnet_secondary_range_name_pods" {
  description = "Name for the secondary IP range used for GKE pods"
  type        = string
  default     = "pods"
}

variable "subnet_secondary_range_name_services" {
  description = "Name for the secondary IP range used for GKE services"
  type        = string
  default     = "services"
}

variable "subnet_secondary_ip_range_pods" {
  description = "Secondary IP range for GKE pods"
  type        = string
  default     = "10.4.0.0/14"
}

variable "subnet_secondary_ip_range_services" {
  description = "Secondary IP range for GKE services"
  type        = string
  default     = "10.0.32.0/20"
}

# Private network configuration
variable "enable_private_ip" {
  description = "Whether to enable private IP for the Cloud Composer 2 environment"
  type        = bool
  default     = true
}

variable "enable_private_endpoint" {
  description = "Whether to enable private endpoint for the Cloud Composer 2 environment"
  type        = bool
  default     = false
}

# NAT configuration
variable "enable_cloud_nat" {
  description = "Whether to create a Cloud NAT gateway for private instances"
  type        = bool
  default     = true
}

variable "nat_router_name" {
  description = "Name of the Cloud Router used for NAT"
  type        = string
  default     = "composer2-nat-router"
}

variable "nat_gateway_name" {
  description = "Name of the Cloud NAT gateway"
  type        = string
  default     = "composer2-nat-gateway"
}

# Firewall configuration
variable "create_firewall_rules" {
  description = "Whether to create firewall rules for the Cloud Composer 2 environment"
  type        = bool
  default     = true
}

variable "firewall_tags" {
  description = "Network tags to apply to firewall rules"
  type        = list(string)
  default     = ["composer2"]
}

variable "allow_internal_communication" {
  description = "Whether to allow internal communication between instances in the VPC"
  type        = bool
  default     = true
}

variable "allowed_external_ip_ranges" {
  description = "List of external IP ranges allowed to access Cloud Composer 2 web interface"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# IAP configuration
variable "enable_iap" {
  description = "Whether to enable Identity-Aware Proxy (IAP) for accessing Cloud Composer 2 web interface"
  type        = bool
  default     = true
}

# Shared VPC configuration
variable "enable_shared_vpc" {
  description = "Whether to use Shared VPC for the Cloud Composer 2 environment"
  type        = bool
  default     = false
}

variable "shared_vpc_host_project_id" {
  description = "Host project ID for Shared VPC if enabled"
  type        = string
  default     = ""
}

# Advanced networking options
variable "network_topology" {
  description = "Network topology to use (flat, hub-and-spoke)"
  type        = string
  default     = "flat"
  
  validation {
    condition     = contains(["flat", "hub-and-spoke"], var.network_topology)
    error_message = "Allowed values for network_topology are 'flat' or 'hub-and-spoke'."
  }
}

variable "peering_network_ids" {
  description = "List of VPC network IDs to peer with the Composer 2 network"
  type        = list(string)
  default     = []
}

# Debug options
variable "debug_output" {
  description = "Whether to output debug information during deployment"
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------------------------------------------------
# LOCALS
# Calculated values that are used in multiple places in this module.
# ---------------------------------------------------------------------------------------------------------------------

locals {
  # Determine the network ID based on creation flag and existing ID
  network_id = var.create_vpc ? var.vpc_name : var.existing_vpc_id
  
  # Full self-links for network and subnet
  network_self_link = var.create_vpc ? "projects/${var.project_id}/global/networks/${var.vpc_name}" : var.existing_vpc_id
  subnet_self_link = var.create_vpc ? "projects/${var.project_id}/regions/${var.region}/subnetworks/${var.subnet_name}" : var.existing_subnet_id
}