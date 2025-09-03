# Multi-Cloud Networking Module Variables
# Configuration for enterprise multi-cloud networking

variable "project_name" {
  description = "Name of the project for resource naming"
  type        = string
  default     = "data-platform"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

# AWS Configuration
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

variable "aws_vpc_cidr" {
  description = "CIDR block for AWS VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "aws_availability_zones" {
  description = "List of availability zones for AWS"
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b", "us-west-2c"]
}

# Azure Configuration
variable "azure_location" {
  description = "Azure region for resources"
  type        = string
  default     = "West US 2"
}

variable "azure_resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
}

variable "azure_vnet_cidr" {
  description = "CIDR block for Azure VNet"
  type        = string
  default     = "10.1.0.0/16"
}

variable "azure_subnets" {
  description = "List of subnet CIDR blocks for Azure"
  type        = list(string)
  default     = ["10.1.1.0/24", "10.1.2.0/24", "10.1.3.0/24"]
}

variable "azure_gateway_subnet" {
  description = "CIDR block for Azure VPN Gateway subnet"
  type        = string
  default     = "10.1.255.0/27"
}

# GCP Configuration
variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-west2"
}

variable "gcp_vpc_cidr" {
  description = "CIDR block for GCP VPC"
  type        = string
  default     = "10.2.0.0/16"
}

variable "gcp_subnets" {
  description = "List of subnet CIDR blocks for GCP"
  type        = list(string)
  default     = ["10.2.1.0/24", "10.2.2.0/24", "10.2.3.0/24"]
}

# Network Security Configuration
variable "allowed_cidr_blocks" {
  description = "List of CIDR blocks allowed for external access"
  type        = list(string)
  default     = ["0.0.0.0/0"]  # Restrict in production
}

variable "enable_deletion_protection" {
  description = "Enable deletion protection for load balancers"
  type        = bool
  default     = true
}

variable "enable_direct_connect" {
  description = "Enable AWS Direct Connect gateway"
  type        = bool
  default     = false
}

# Performance and Optimization
variable "enable_flow_logs" {
  description = "Enable VPC flow logs for network analysis"
  type        = bool
  default     = true
}

variable "enable_enhanced_monitoring" {
  description = "Enable enhanced network monitoring"
  type        = bool
  default     = true
}

variable "bandwidth_limit_mbps" {
  description = "Bandwidth limit for VPN connections in Mbps"
  type        = number
  default     = 1000
}

# High Availability Configuration
variable "enable_multi_az" {
  description = "Enable multi-AZ deployment"
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "Number of days to retain network configuration backups"
  type        = number
  default     = 30
}

# Cost Optimization
variable "use_spot_instances" {
  description = "Use spot instances for non-critical networking components"
  type        = bool
  default     = false
}

variable "auto_scaling_enabled" {
  description = "Enable auto-scaling for network components"
  type        = bool
  default     = true
}

# Security Configuration
variable "enable_network_acls" {
  description = "Enable network ACLs for additional security"
  type        = bool
  default     = true
}

variable "enable_private_endpoints" {
  description = "Enable private endpoints for cloud services"
  type        = bool
  default     = true
}

variable "ssl_policy" {
  description = "SSL policy for load balancers"
  type        = string
  default     = "ELBSecurityPolicy-TLS-1-2-2017-01"
}

# Monitoring and Alerting
variable "cloudwatch_retention_days" {
  description = "CloudWatch log retention period"
  type        = number
  default     = 30
}

variable "enable_detailed_monitoring" {
  description = "Enable detailed CloudWatch monitoring"
  type        = bool
  default     = true
}

# Tags
variable "additional_tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}