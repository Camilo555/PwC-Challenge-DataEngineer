# Data Lake Module Variables

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
}

variable "aws_s3_bucket" {
  description = "AWS S3 bucket for data lake"
  type        = string
  default     = null
}

variable "azure_storage_account" {
  description = "Azure storage account for data lake"
  type        = string
  default     = null
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = null
}

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = null
}

variable "gcp_storage_bucket" {
  description = "GCP storage bucket for data lake"
  type        = string
  default     = null
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = null
}

variable "gcp_kms_key_name" {
  description = "GCP KMS key for encryption"
  type        = string
  default     = null
}

variable "bronze_layer_enabled" {
  description = "Enable bronze layer"
  type        = bool
  default     = true
}

variable "silver_layer_enabled" {
  description = "Enable silver layer"
  type        = bool
  default     = true
}

variable "gold_layer_enabled" {
  description = "Enable gold layer"
  type        = bool
  default     = true
}

variable "bronze_retention_days" {
  description = "Data retention days for bronze layer"
  type        = number
  default     = 90
}

variable "silver_retention_days" {
  description = "Data retention days for silver layer"
  type        = number
  default     = 365
}

variable "gold_retention_days" {
  description = "Data retention days for gold layer"
  type        = number
  default     = 2555
}

variable "enable_encryption" {
  description = "Enable encryption for data lake"
  type        = bool
  default     = true
}

variable "enable_versioning" {
  description = "Enable versioning for data lake"
  type        = bool
  default     = true
}

variable "enable_logging" {
  description = "Enable access logging for data lake"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}