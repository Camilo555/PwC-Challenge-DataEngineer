# Multi-Cloud Networking Module Outputs
# Exposes network resources for other modules

# AWS Outputs
output "aws_vpc_id" {
  description = "ID of the AWS VPC"
  value       = aws_vpc.data_platform.id
}

output "aws_vpc_cidr" {
  description = "CIDR block of the AWS VPC"
  value       = aws_vpc.data_platform.cidr_block
}

output "aws_private_subnet_ids" {
  description = "IDs of the AWS private subnets"
  value       = aws_subnet.aws_private[*].id
}

output "aws_public_subnet_ids" {
  description = "IDs of the AWS public subnets"
  value       = aws_subnet.aws_public[*].id
}

output "aws_internet_gateway_id" {
  description = "ID of the AWS Internet Gateway"
  value       = aws_internet_gateway.aws_igw.id
}

output "aws_nat_gateway_ids" {
  description = "IDs of the AWS NAT Gateways"
  value       = aws_nat_gateway.aws_nat[*].id
}

output "aws_transit_gateway_id" {
  description = "ID of the AWS Transit Gateway"
  value       = aws_ec2_transit_gateway.main.id
}

output "aws_security_group_id" {
  description = "ID of the AWS multi-cloud security group"
  value       = aws_security_group.multi_cloud_sg.id
}

output "aws_load_balancer_arn" {
  description = "ARN of the AWS Application Load Balancer"
  value       = aws_lb.multi_cloud_alb.arn
}

output "aws_load_balancer_dns" {
  description = "DNS name of the AWS Application Load Balancer"
  value       = aws_lb.multi_cloud_alb.dns_name
}

output "aws_vpn_gateway_id" {
  description = "ID of the AWS VPN Gateway"
  value       = aws_vpn_gateway.aws_vpn.id
}

# Azure Outputs
output "azure_vnet_id" {
  description = "ID of the Azure Virtual Network"
  value       = azurerm_virtual_network.data_platform.id
}

output "azure_vnet_name" {
  description = "Name of the Azure Virtual Network"
  value       = azurerm_virtual_network.data_platform.name
}

output "azure_subnet_ids" {
  description = "IDs of the Azure subnets"
  value       = azurerm_subnet.azure_private[*].id
}

output "azure_resource_group_name" {
  description = "Name of the Azure resource group"
  value       = var.azure_resource_group_name
}

output "azure_vpn_gateway_id" {
  description = "ID of the Azure VPN Gateway"
  value       = azurerm_virtual_network_gateway.vpn_gateway.id
}

output "azure_vpn_gateway_ip" {
  description = "Public IP address of the Azure VPN Gateway"
  value       = azurerm_public_ip.vpn_gateway.ip_address
}

output "azure_network_security_group_id" {
  description = "ID of the Azure Network Security Group"
  value       = azurerm_network_security_group.multi_cloud_nsg.id
}

# GCP Outputs
output "gcp_vpc_id" {
  description = "ID of the GCP VPC network"
  value       = google_compute_network.data_platform.id
}

output "gcp_vpc_self_link" {
  description = "Self link of the GCP VPC network"
  value       = google_compute_network.data_platform.self_link
}

output "gcp_subnet_ids" {
  description = "IDs of the GCP subnets"
  value       = google_compute_subnetwork.gcp_private[*].id
}

output "gcp_subnet_self_links" {
  description = "Self links of the GCP subnets"
  value       = google_compute_subnetwork.gcp_private[*].self_link
}

output "gcp_cloud_router_id" {
  description = "ID of the GCP Cloud Router"
  value       = google_compute_router.cloud_router.id
}

output "gcp_vpn_gateway_id" {
  description = "ID of the GCP VPN Gateway"
  value       = google_compute_vpn_gateway.vpn_gateway.id
}

# Cross-Cloud Connectivity Outputs
output "cross_cloud_connectivity" {
  description = "Cross-cloud connectivity information"
  value = {
    aws_to_azure_vpn_id = aws_vpn_connection.aws_to_azure.id
    aws_transit_gateway = aws_ec2_transit_gateway.main.id
    azure_vpn_gateway   = azurerm_virtual_network_gateway.vpn_gateway.id
    gcp_cloud_router    = google_compute_router.cloud_router.id
  }
}

# Network Security Information
output "network_security" {
  description = "Network security configuration"
  value = {
    aws_security_group_id    = aws_security_group.multi_cloud_sg.id
    azure_nsg_id            = azurerm_network_security_group.multi_cloud_nsg.id
    gcp_firewall_rule       = google_compute_firewall.multi_cloud_firewall.name
  }
}

# Monitoring Resources
output "monitoring_resources" {
  description = "Network monitoring and logging resources"
  value = {
    aws_cloudwatch_log_group = aws_cloudwatch_log_group.network_monitoring.name
    aws_flow_log_id         = aws_flow_log.vpc_flow_log.id
    flow_log_role_arn       = aws_iam_role.flow_log_role.arn
  }
}

# Network Performance Information
output "network_performance" {
  description = "Network performance and optimization settings"
  value = {
    bandwidth_limit_mbps     = var.bandwidth_limit_mbps
    enable_enhanced_monitoring = var.enable_enhanced_monitoring
    multi_az_enabled        = var.enable_multi_az
    auto_scaling_enabled    = var.auto_scaling_enabled
  }
}

# Cost Optimization Information
output "cost_optimization" {
  description = "Cost optimization settings"
  value = {
    use_spot_instances      = var.use_spot_instances
    enable_direct_connect   = var.enable_direct_connect
    backup_retention_days   = var.backup_retention_days
  }
}

# High Availability Information
output "high_availability" {
  description = "High availability configuration"
  value = {
    availability_zones     = var.aws_availability_zones
    multi_az_enabled      = var.enable_multi_az
    nat_gateway_count     = length(aws_nat_gateway.aws_nat)
    load_balancer_zones   = aws_lb.multi_cloud_alb.availability_zone
  }
}

# Network CIDR Information
output "network_cidrs" {
  description = "CIDR blocks for all cloud networks"
  value = {
    aws_vpc_cidr    = var.aws_vpc_cidr
    azure_vnet_cidr = var.azure_vnet_cidr
    gcp_vpc_cidr    = var.gcp_vpc_cidr
  }
}

# Direct Connect Information (if enabled)
output "direct_connect" {
  description = "Direct Connect gateway information"
  value = var.enable_direct_connect ? {
    dx_gateway_id = aws_dx_gateway.dx_gateway[0].id
  } : null
}

# Regional Information
output "regional_deployment" {
  description = "Regional deployment information"
  value = {
    aws_region     = var.aws_region
    azure_location = var.azure_location
    gcp_region     = var.gcp_region
    gcp_project_id = var.gcp_project_id
  }
}