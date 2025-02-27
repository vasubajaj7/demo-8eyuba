terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.34.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.34.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

locals {
  network_id = var.create_vpc ? google_compute_network.composer_network[0].id : var.existing_vpc_id != "" ? var.existing_vpc_id : data.google_compute_network.existing_network[0].id
  network_self_link = var.create_vpc ? google_compute_network.composer_network[0].self_link : var.existing_vpc_id != "" ? "projects/${var.project_id}/global/networks/${var.vpc_name}" : data.google_compute_network.existing_network[0].self_link
  subnet_self_link = var.create_vpc ? google_compute_subnetwork.composer_subnet[0].self_link : var.existing_subnet_id != "" ? "projects/${var.project_id}/regions/${var.region}/subnetworks/${var.subnet_name}" : data.google_compute_subnetwork.existing_subnet[0].self_link
}

# Create a VPC network for Cloud Composer 2
resource "google_compute_network" "composer_network" {
  name                            = var.vpc_name
  project                         = var.project_id
  auto_create_subnetworks         = false
  routing_mode                    = "GLOBAL"
  count                           = var.create_vpc ? 1 : 0
  description                     = "Network for Cloud Composer 2 environment with Airflow 2.X"
  delete_default_routes_on_create = false
  mtu                             = 1460
}

# Create a subnet with secondary IP ranges for GKE pods and services
resource "google_compute_subnetwork" "composer_subnet" {
  name                     = var.subnet_name
  project                  = var.project_id
  region                   = var.region
  network                  = local.network_self_link
  ip_cidr_range            = var.subnet_ip_range
  private_ip_google_access = true
  count                    = var.create_vpc ? 1 : 0
  description              = "Subnet for Cloud Composer 2 environment with Airflow 2.X"

  secondary_ip_range {
    range_name    = var.subnet_secondary_range_name_pods
    ip_cidr_range = var.subnet_secondary_ip_range_pods
  }

  secondary_ip_range {
    range_name    = var.subnet_secondary_range_name_services
    ip_cidr_range = var.subnet_secondary_ip_range_services
  }
}

# Retrieve existing VPC network when not creating a new one
data "google_compute_network" "existing_network" {
  name    = var.vpc_name
  project = var.shared_vpc_host_project_id != "" ? var.shared_vpc_host_project_id : var.project_id
  count   = var.create_vpc || var.existing_vpc_id != "" ? 0 : 1
}

# Retrieve existing subnet when not creating a new one
data "google_compute_subnetwork" "existing_subnet" {
  name    = var.subnet_name
  project = var.shared_vpc_host_project_id != "" ? var.shared_vpc_host_project_id : var.project_id
  region  = var.region
  count   = var.create_vpc || var.existing_subnet_id != "" ? 0 : 1
}

# Create Cloud Router for NAT gateway
resource "google_compute_router" "nat_router" {
  name        = var.nat_router_name
  project     = var.project_id
  region      = var.region
  network     = local.network_self_link
  description = "Router for Cloud NAT used by Cloud Composer 2 environment"
  count       = var.enable_private_ip && var.enable_cloud_nat ? 1 : 0
}

# Create NAT gateway for private instances to access internet
resource "google_compute_router_nat" "nat_gateway" {
  name                                = var.nat_gateway_name
  project                             = var.project_id
  router                              = google_compute_router.nat_router[0].name
  region                              = var.region
  nat_ip_allocate_option              = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat  = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  min_ports_per_vm                    = 64
  enable_endpoint_independent_mapping = false
  count                               = var.enable_private_ip && var.enable_cloud_nat ? 1 : 0

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# Create firewall rule for internal communication within VPC network
resource "google_compute_firewall" "internal" {
  name        = "composer2-allow-internal"
  project     = var.project_id
  network     = local.network_self_link
  description = "Allows internal communication between instances on the same network"
  direction   = "INGRESS"
  
  source_ranges = [var.subnet_ip_range]
  target_tags   = var.firewall_tags
  priority      = 1000
  count         = var.create_firewall_rules && var.allow_internal_communication ? 1 : 0

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
  }

  allow {
    protocol = "udp"
  }
}

# Create firewall rule to allow IAP access to instances
resource "google_compute_firewall" "iap_ingress" {
  name        = "composer2-allow-iap-ingress"
  project     = var.project_id
  network     = local.network_self_link
  description = "Allows Identity-Aware Proxy traffic to instances"
  direction   = "INGRESS"
  
  source_ranges = ["35.235.240.0/20"]
  target_tags   = var.firewall_tags
  priority      = 1000
  count         = var.create_firewall_rules && var.enable_iap ? 1 : 0

  allow {
    protocol = "tcp"
    ports    = ["22", "3389", "8080", "8084", "80", "443"]
  }
}

# Create firewall rule to allow external access to Airflow web interface
resource "google_compute_firewall" "web_interface" {
  name        = "composer2-allow-web-interface"
  project     = var.project_id
  network     = local.network_self_link
  description = "Allows access to Airflow web interface from specified IP ranges"
  direction   = "INGRESS"
  
  source_ranges = var.allowed_external_ip_ranges
  target_tags   = var.firewall_tags
  priority      = 1000
  count         = var.create_firewall_rules && length(var.allowed_external_ip_ranges) > 0 ? 1 : 0

  allow {
    protocol = "tcp"
    ports    = ["80", "443", "8080", "8084"]
  }
}

# Create VPC network peering connections to other networks
resource "google_compute_network_peering" "network_peering" {
  name                  = "composer2-peering-${each.key}"
  network               = local.network_self_link
  peer_network          = each.value
  for_each              = var.network_topology == "hub-and-spoke" ? { for idx, peer_id in var.peering_network_ids : idx => peer_id } : {}
  auto_create_routes    = true
  import_custom_routes  = true
  export_custom_routes  = true
}

# Module outputs
output "network_id" {
  description = "The ID of the VPC network used for Cloud Composer 2"
  value       = local.network_id
}

output "network_name" {
  description = "The name of the VPC network"
  value       = var.vpc_name
}

output "network_self_link" {
  description = "The self-link URL of the VPC network"
  value       = local.network_self_link
}

output "subnet_id" {
  description = "The ID of the subnet created for Cloud Composer 2"
  value       = var.create_vpc ? google_compute_subnetwork.composer_subnet[0].id : var.existing_subnet_id != "" ? var.existing_subnet_id : data.google_compute_subnetwork.existing_subnet[0].id
}

output "subnet_name" {
  description = "The name of the subnet"
  value       = var.subnet_name
}

output "subnet_self_link" {
  description = "The self-link URL of the subnet"
  value       = local.subnet_self_link
}

output "subnet_region" {
  description = "The region where the subnet is deployed"
  value       = var.region
}

output "subnet_ip_cidr_range" {
  description = "The primary IP CIDR range of the subnet"
  value       = var.create_vpc ? google_compute_subnetwork.composer_subnet[0].ip_cidr_range : var.existing_subnet_id != "" ? var.subnet_ip_range : data.google_compute_subnetwork.existing_subnet[0].ip_cidr_range
}

output "pods_ip_range_name" {
  description = "The name of the secondary IP range used for GKE pods"
  value       = var.subnet_secondary_range_name_pods
}

output "services_ip_range_name" {
  description = "The name of the secondary IP range used for GKE services"
  value       = var.subnet_secondary_range_name_services
}

output "nat_ip" {
  description = "The external IP addresses allocated for the NAT gateway"
  value       = var.enable_private_ip && var.enable_cloud_nat ? google_compute_router_nat.nat_gateway[0].nat_ips : []
}

output "router_name" {
  description = "The name of the Cloud Router used for NAT"
  value       = var.enable_private_ip && var.enable_cloud_nat ? google_compute_router.nat_router[0].name : ""
}

output "nat_gateway_name" {
  description = "The name of the NAT gateway"
  value       = var.enable_private_ip && var.enable_cloud_nat ? google_compute_router_nat.nat_gateway[0].name : ""
}

output "firewall_tags" {
  description = "The network tags applied to firewall rules"
  value       = var.firewall_tags
}

output "private_ip_enabled" {
  description = "Whether private IP is enabled for the Composer environment"
  value       = var.enable_private_ip
}