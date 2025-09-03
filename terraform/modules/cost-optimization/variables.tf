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

# Legacy spot instance variables - maintained for backward compatibility
variable "enable_spot_instances" {
  description = "Enable spot instances for cost savings (deprecated - use spot_instances_enabled)"
  type        = bool
  default     = true
}

variable "spot_instance_percentage" {
  description = "Percentage of spot instances to use"
  type        = number
  default     = 50
  validation {
    condition     = var.spot_instance_percentage >= 0 && var.spot_instance_percentage <= 100
    error_message = "Spot instance percentage must be between 0 and 100."
  }
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

# ============================================================================
# ADVANCED COST OPTIMIZATION FEATURES
# ============================================================================

# Rightsizing Configuration
variable "rightsizing_enabled" {
  description = "Enable automated rightsizing recommendations"
  type        = bool
  default     = true
}

variable "cpu_threshold_low" {
  description = "Low CPU utilization threshold for rightsizing (percentage)"
  type        = number
  default     = 20
  validation {
    condition     = var.cpu_threshold_low >= 0 && var.cpu_threshold_low <= 100
    error_message = "CPU threshold must be between 0 and 100."
  }
}

variable "memory_threshold_low" {
  description = "Low memory utilization threshold for rightsizing (percentage)"
  type        = number
  default     = 30
  validation {
    condition     = var.memory_threshold_low >= 0 && var.memory_threshold_low <= 100
    error_message = "Memory threshold must be between 0 and 100."
  }
}

variable "rightsizing_analysis_period_days" {
  description = "Number of days to analyze for rightsizing recommendations"
  type        = number
  default     = 14
  validation {
    condition     = var.rightsizing_analysis_period_days >= 1 && var.rightsizing_analysis_period_days <= 90
    error_message = "Analysis period must be between 1 and 90 days."
  }
}

# Intelligent Scaling Configuration
variable "predictive_scaling_enabled" {
  description = "Enable predictive scaling for auto scaling groups"
  type        = bool
  default     = true
}

variable "cpu_target_value" {
  description = "Target CPU utilization percentage for scaling"
  type        = number
  default     = 60
  validation {
    condition     = var.cpu_target_value >= 10 && var.cpu_target_value <= 90
    error_message = "CPU target value must be between 10 and 90."
  }
}

variable "auto_scaling_schedules" {
  description = "Custom auto scaling schedules for different time periods"
  type = map(object({
    min_size         = number
    max_size         = number
    desired_capacity = number
    recurrence      = string
  }))
  default = {
    "business_hours" = {
      min_size         = 2
      max_size         = 10
      desired_capacity = 3
      recurrence      = "0 8 * * 1-5"  # Monday-Friday 8 AM
    }
    "after_hours" = {
      min_size         = 1
      max_size         = 3
      desired_capacity = 1
      recurrence      = "0 18 * * 1-5"  # Monday-Friday 6 PM
    }
  }
}

# Spot Instance Configuration
variable "spot_instances_enabled" {
  description = "Enable spot instances for cost optimization"
  type        = bool
  default     = true
}

variable "spot_fleet_target_capacity" {
  description = "Target capacity for spot fleet"
  type        = number
  default     = 5
}

variable "spot_max_price" {
  description = "Maximum price for spot instances (USD per hour)"
  type        = string
  default     = "0.10"
}

# Reserved Instance Management
variable "reserved_instances_enabled" {
  description = "Enable reserved instance recommendations and management"
  type        = bool
  default     = true
}

variable "reserved_instance_term" {
  description = "Reserved instance term (1 year or 3 years)"
  type        = string
  default     = "1year"
  validation {
    condition     = contains(["1year", "3year"], var.reserved_instance_term)
    error_message = "Reserved instance term must be either '1year' or '3year'."
  }
}

variable "reserved_instance_payment_option" {
  description = "Payment option for reserved instances"
  type        = string
  default     = "PartialUpfront"
  validation {
    condition     = contains(["NoUpfront", "PartialUpfront", "AllUpfront"], var.reserved_instance_payment_option)
    error_message = "Payment option must be NoUpfront, PartialUpfront, or AllUpfront."
  }
}

# Storage and Data Lifecycle
variable "lifecycle_policies_enabled" {
  description = "Enable intelligent storage lifecycle policies"
  type        = bool
  default     = true
}

variable "data_retention_days" {
  description = "Data retention period in days"
  type        = number
  default     = 2555  # 7 years for compliance
  validation {
    condition     = var.data_retention_days >= 30 && var.data_retention_days <= 3653
    error_message = "Data retention must be between 30 and 3653 days (10 years)."
  }
}

variable "s3_bucket_name" {
  description = "S3 bucket name for lifecycle policies"
  type        = string
  default     = ""
}

# Cost Monitoring and Alerting
variable "cost_anomaly_threshold" {
  description = "Daily cost anomaly detection threshold (USD)"
  type        = number
  default     = 500
}

variable "enable_detailed_billing" {
  description = "Enable detailed billing reports and analysis"
  type        = bool
  default     = true
}

variable "cost_allocation_tags" {
  description = "Tags used for cost allocation and chargeback"
  type        = list(string)
  default     = ["Environment", "Project", "CostCenter", "BusinessUnit", "Team"]
}

# AWS Region and Infrastructure
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

variable "ami_id" {
  description = "AMI ID for EC2 instances"
  type        = string
  default     = ""
}

variable "subnet_id" {
  description = "Subnet ID for EC2 instances"
  type        = string
  default     = ""
}

variable "security_group_id" {
  description = "Security group ID for EC2 instances"
  type        = string
  default     = ""
}

variable "key_pair_name" {
  description = "EC2 key pair name"
  type        = string
  default     = ""
}

# GCP Instance Configuration
variable "gcp_machine_type" {
  description = "GCP machine type for cost-optimized instances"
  type        = string
  default     = "e2-medium"
}

variable "gcp_source_image" {
  description = "GCP source image for instances"
  type        = string
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
}

variable "gcp_network_name" {
  description = "GCP network name"
  type        = string
  default     = "default"
}

variable "gcp_instance_group_size" {
  description = "Initial size of GCP instance group"
  type        = number
  default     = 3
}

variable "gcp_min_replicas" {
  description = "Minimum number of GCP instances"
  type        = number
  default     = 1
}

variable "gcp_max_replicas" {
  description = "Maximum number of GCP instances"
  type        = number
  default     = 10
}

# Workspace and Schedule Configuration
variable "workspace_auto_shutdown_enabled" {
  description = "Enable automatic shutdown for development workspaces"
  type        = bool
  default     = true
}

variable "workspace_idle_timeout_minutes" {
  description = "Idle timeout for workspace auto-shutdown (minutes)"
  type        = number
  default     = 60
  validation {
    condition     = var.workspace_idle_timeout_minutes >= 15 && var.workspace_idle_timeout_minutes <= 480
    error_message = "Idle timeout must be between 15 and 480 minutes."
  }
}

variable "weekend_shutdown_enabled" {
  description = "Enable automatic shutdown during weekends for non-production environments"
  type        = bool
  default     = true
}

# Performance and Efficiency Monitoring
variable "performance_monitoring_enabled" {
  description = "Enable performance monitoring for cost optimization insights"
  type        = bool
  default     = true
}

variable "efficiency_score_threshold" {
  description = "Resource efficiency score threshold (0-100)"
  type        = number
  default     = 70
  validation {
    condition     = var.efficiency_score_threshold >= 0 && var.efficiency_score_threshold <= 100
    error_message = "Efficiency score threshold must be between 0 and 100."
  }
}

# Multi-Cloud Cost Comparison
variable "enable_multi_cloud_cost_comparison" {
  description = "Enable cost comparison across cloud providers"
  type        = bool
  default     = false
}

variable "cost_comparison_frequency" {
  description = "Frequency for multi-cloud cost comparison (daily, weekly, monthly)"
  type        = string
  default     = "weekly"
  validation {
    condition     = contains(["daily", "weekly", "monthly"], var.cost_comparison_frequency)
    error_message = "Cost comparison frequency must be daily, weekly, or monthly."
  }
}

# Tags and Labels
variable "tags" {
  description = "Resource tags for cost allocation and management"
  type        = map(string)
  default = {
    ManagedBy        = "Terraform"
    CostOptimization = "Enabled"
    AutoScaling      = "Enabled"
    Monitoring       = "Enabled"
  }
}