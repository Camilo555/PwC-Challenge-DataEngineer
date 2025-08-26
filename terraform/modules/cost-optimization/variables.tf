# Cost Optimization Module Variables

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

# Budget Configuration
variable "monthly_budget_limit" {
  description = "Monthly budget limit in USD"
  type        = number
  default     = 10000
}

variable "budget_alert_threshold" {
  description = "Budget alert threshold as percentage"
  type        = number
  default     = 80
}

variable "alert_email" {
  description = "Email address for budget alerts"
  type        = string
  default     = "alerts@example.com"
}

# Cost Control Settings
variable "auto_shutdown_enabled" {
  description = "Enable automatic shutdown of resources"
  type        = bool
  default     = true
}

variable "reserved_instance_coverage" {
  description = "Target reserved instance coverage percentage"
  type        = number
  default     = 70
}

variable "enable_spot_instances" {
  description = "Enable spot instances for cost savings"
  type        = bool
  default     = true
}

variable "spot_instance_percentage" {
  description = "Percentage of spot instances to use"
  type        = number
  default     = 50
}

# Scaling Configuration
variable "enable_intelligent_scaling" {
  description = "Enable intelligent auto-scaling"
  type        = bool
  default     = true
}

variable "scaling_metric_threshold" {
  description = "CPU utilization threshold for scaling"
  type        = number
  default     = 70
}

# Resource Optimization
variable "enable_resource_tagging" {
  description = "Enable comprehensive resource tagging"
  type        = bool
  default     = true
}

variable "cost_center" {
  description = "Cost center for billing allocation"
  type        = string
  default     = "data-engineering"
}

variable "business_unit" {
  description = "Business unit for cost allocation"
  type        = string
  default     = "analytics"
}

# AWS-specific variables
variable "asg_name" {
  description = "Auto Scaling Group name for scheduling"
  type        = string
  default     = null
}

variable "asg_min_size" {
  description = "Auto Scaling Group minimum size"
  type        = number
  default     = 1
}

variable "asg_max_size" {
  description = "Auto Scaling Group maximum size"
  type        = number
  default     = 10
}

variable "asg_desired_size" {
  description = "Auto Scaling Group desired size"
  type        = number
  default     = 3
}

# Azure-specific variables
variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "East US"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = null
}

# GCP-specific variables
variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = null
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcp_billing_account" {
  description = "GCP billing account ID"
  type        = string
  default     = null
}

# Tags and Labels
variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}