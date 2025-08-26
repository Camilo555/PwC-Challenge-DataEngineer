# Data Catalog Module Variables

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

# Data Lake Integration
variable "aws_data_lake_bucket" {
  description = "AWS S3 data lake bucket name"
  type        = string
  default     = null
}

variable "azure_data_lake_account" {
  description = "Azure Data Lake storage account name"
  type        = string
  default     = null
}

variable "gcp_data_lake_bucket" {
  description = "GCP data lake bucket name"
  type        = string
  default     = null
}

# Data Discovery and Classification
variable "enable_pii_detection" {
  description = "Enable automatic PII detection"
  type        = bool
  default     = true
}

variable "enable_data_quality_monitoring" {
  description = "Enable data quality monitoring"
  type        = bool
  default     = true
}

variable "deploy_unified_catalog" {
  description = "Deploy unified catalog (Apache Atlas)"
  type        = bool
  default     = true
}

# Database Connections
variable "database_connections" {
  description = "Database connection configurations"
  type = object({
    postgresql = object({
      host     = string
      database = string
      username = string
    })
  })
  default = {
    postgresql = {
      host     = ""
      database = ""
      username = ""
    }
  }
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

# GCP-specific variables
variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

# Tags and Labels
variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}