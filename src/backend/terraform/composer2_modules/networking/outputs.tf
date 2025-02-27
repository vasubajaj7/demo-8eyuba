# outputs.tf - Networking module outputs for Cloud Composer 2 deployment

# VPC Network outputs
output "network_id" {
  description = "The ID of the VPC network used for Cloud Composer 2"
  value       = local.network_id
}

output "network_name" {
  description = "The name of the VPC network used for Cloud Composer 2"
  value       = local.network_name
}

output "network_self_link" {
  description = "The self-link URL of the VPC network for resource references"
  value       = local.network_self_link
}

# Subnet outputs
output "subnet_id" {
  description = "The ID of the subnet created or referenced for Cloud Composer 2"
  value       = local.subnet_id
}

output "subnet_name" {
  description = "The name of the subnet used for Cloud Composer 2"
  value       = local.subnet_name
}

output "subnet_self_link" {
  description = "The self-link URL of the subnet for resource references"
  value       = local.subnet_self_link
}

output "subnet_region" {
  description = "The region where the subnet is deployed"
  value       = local.subnet_region
}

output "subnet_ip_cidr_range" {
  description = "The primary IP CIDR range of the subnet"
  value       = local.subnet_ip_cidr_range
}

# Secondary IP ranges for GKE
output "pods_ip_range_name" {
  description = "The name of the secondary IP range used for GKE pods"
  value       = local.pods_ip_range_name
}

output "services_ip_range_name" {
  description = "The name of the secondary IP range used for GKE services"
  value       = local.services_ip_range_name
}

# NAT Gateway outputs
output "nat_ip" {
  description = "The external IP addresses allocated for the NAT gateway"
  value       = local.nat_ip
}

output "router_name" {
  description = "The name of the Cloud Router used for NAT"
  value       = local.router_name
}

output "nat_gateway_name" {
  description = "The name of the NAT gateway"
  value       = local.nat_gateway_name
}

# Firewall outputs
output "firewall_tags" {
  description = "The network tags applied to firewall rules for secure access to Cloud Composer 2"
  value       = local.firewall_tags
}

# General configuration
output "private_ip_enabled" {
  description = "Whether private IP is enabled for the Composer environment"
  value       = var.enable_private_ip
}