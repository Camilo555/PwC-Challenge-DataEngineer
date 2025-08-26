# Data Lake Module Variables

variable "project_name" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

# Multi-cloud deployment flags
variable "deploy_to_aws" {
  description = "Deploy AWS resources"
  type        = bool
  default     = false
}

variable "deploy_to_azure" {
  description = "Deploy Azure resources"
  type        = bool
  default     = false
}

variable "deploy_to_gcp" {
  description = "Deploy GCP resources"
  type        = bool
  default     = false
}

variable "primary_cloud" {
  description = "Primary cloud provider"
  type        = string
  validation {
    condition     = contains(["aws", "azure", "gcp"], var.primary_cloud)
    error_message = "Primary cloud must be one of: aws, azure, gcp."
  }
}

# Storage Configuration
variable "storage_class" {
  description = "Default storage class"
  type        = string
  default     = "STANDARD"
}

variable "enable_versioning" {
  description = "Enable versioning on storage"
  type        = bool
  default     = true
}

variable "enable_encryption" {
  description = "Enable encryption at rest"
  type        = bool
  default     = true
}

variable "enable_cross_cloud_replication" {
  description = "Enable cross-cloud replication"
  type        = bool
  default     = false
}

# Data Catalog Configuration
variable "enable_data_catalog" {
  description = "Enable data catalog integration"
  type        = bool
  default     = true
}

variable "enable_data_lineage" {
  description = "Enable data lineage tracking"
  type        = bool
  default     = true
}

# Retention and Lifecycle
variable "data_retention_days" {
  description = "Data retention period in days"
  type        = number
  default     = 2555  # 7 years
}

# Azure-specific variables
variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = null
}

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "East US"
}

variable "azure_action_group_id" {
  description = "Azure Monitor action group ID for alerts"
  type        = string
  default     = null
}

# GCP-specific variables
variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcp_kms_key_name" {
  description = "GCP KMS key name for encryption"
  type        = string
  default     = null
}

# Tags and Labels
variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}