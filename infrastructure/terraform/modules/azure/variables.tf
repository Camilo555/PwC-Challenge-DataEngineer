# Azure Infrastructure Module Variables
# Configuration variables for Azure cloud resources

variable "environment" {
  description = "Environment name (development, staging, production)"
  type        = string
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "West US 2"
}

variable "resource_prefix" {
  description = "Prefix for all resource names"
  type        = string
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
}

variable "vnet_address_space" {
  description = "Address space for the virtual network"
  type        = list(string)
  default     = ["10.1.0.0/16"]
}

variable "availability_zones" {
  description = "List of availability zones to use"
  type        = list(string)
  default     = ["1", "2", "3"]
}

variable "kubernetes_version" {
  description = "Kubernetes version for AKS cluster"
  type        = string
  default     = "1.28.3"
}

variable "env_config" {
  description = "Environment-specific configuration"
  type = object({
    cluster_size        = string
    high_availability  = bool
    auto_scaling       = bool
    backup_retention   = number
    monitoring_level   = string
  })
}

variable "enable_disaster_recovery" {
  description = "Enable disaster recovery features"
  type        = bool
  default     = false
}

variable "enable_cost_optimization" {
  description = "Enable cost optimization features"
  type        = bool
  default     = true
}

# AKS Configuration
variable "aks_admin_group_object_ids" {
  description = "Azure AD group object IDs for AKS admin access"
  type        = list(string)
  default     = []
}

# Synapse Configuration
variable "synapse_sql_password" {
  description = "Password for Synapse SQL administrator"
  type        = string
  sensitive   = true
  default     = null
}

variable "synapse_aad_admin_login" {
  description = "Azure AD admin login for Synapse"
  type        = string
  default     = ""
}

variable "synapse_aad_admin_object_id" {
  description = "Azure AD admin object ID for Synapse"
  type        = string
  default     = ""
}

# PostgreSQL Configuration
variable "postgresql_password" {
  description = "Password for PostgreSQL administrator"
  type        = string
  sensitive   = true
  default     = null
}