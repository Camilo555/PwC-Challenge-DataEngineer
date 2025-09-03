# Intelligent Infrastructure Optimization Variables

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

# ============================================================================
# OPTIMIZATION CONFIGURATION
# ============================================================================

variable "optimization_level" {
  description = "Level of optimization (conservative, balanced, aggressive)"
  type        = string
  default     = "balanced"
  validation {
    condition     = contains(["conservative", "balanced", "aggressive"], var.optimization_level)
    error_message = "Optimization level must be conservative, balanced, or aggressive."
  }
}

variable "analysis_period_days" {
  description = "Number of days to analyze for optimization recommendations"
  type        = number
  default     = 14
  validation {
    condition     = var.analysis_period_days >= 7 && var.analysis_period_days <= 90
    error_message = "Analysis period must be between 7 and 90 days."
  }
}

variable "analysis_schedule" {
  description = "Cron expression for analysis schedule"
  type        = string
  default     = "cron(0 6 * * ? *)"  # Daily at 6 AM
}

# ============================================================================
# CPU RIGHTSIZING CONFIGURATION
# ============================================================================

variable "cpu_rightsizing_enabled" {
  description = "Enable CPU rightsizing recommendations"
  type        = bool
  default     = true
}

variable "cpu_threshold_low" {
  description = "Low CPU utilization threshold for rightsizing (%)"
  type        = number
  default     = 20
  validation {
    condition     = var.cpu_threshold_low >= 5 && var.cpu_threshold_low <= 50
    error_message = "CPU threshold low must be between 5 and 50."
  }
}

variable "cpu_threshold_high" {
  description = "High CPU utilization threshold for scaling up (%)"
  type        = number
  default     = 80
  validation {
    condition     = var.cpu_threshold_high >= 60 && var.cpu_threshold_high <= 95
    error_message = "CPU threshold high must be between 60 and 95."
  }
}

# ============================================================================
# MEMORY RIGHTSIZING CONFIGURATION
# ============================================================================

variable "memory_rightsizing_enabled" {
  description = "Enable memory rightsizing recommendations"
  type        = bool
  default     = true
}

variable "memory_threshold_low" {
  description = "Low memory utilization threshold for rightsizing (%)"
  type        = number
  default     = 30
  validation {
    condition     = var.memory_threshold_low >= 10 && var.memory_threshold_low <= 60
    error_message = "Memory threshold low must be between 10 and 60."
  }
}

variable "memory_threshold_high" {
  description = "High memory utilization threshold for scaling up (%)"
  type        = number
  default     = 85
  validation {
    condition     = var.memory_threshold_high >= 70 && var.memory_threshold_high <= 95
    error_message = "Memory threshold high must be between 70 and 95."
  }
}

# ============================================================================
# PREDICTIVE SCALING CONFIGURATION
# ============================================================================

variable "predictive_scaling_enabled" {
  description = "Enable predictive scaling based on patterns"
  type        = bool
  default     = true
}

variable "seasonality_patterns" {
  description = "Seasonality patterns for predictive scaling"
  type = map(object({
    pattern_type = string
    scaling_factor = number
    time_windows = list(string)
  }))
  default = {
    business_hours = {
      pattern_type = "daily"
      scaling_factor = 1.5
      time_windows = ["08:00-18:00"]
    }
    weekend_low = {
      pattern_type = "weekly"
      scaling_factor = 0.7
      time_windows = ["Saturday", "Sunday"]
    }
    month_end = {
      pattern_type = "monthly"
      scaling_factor = 2.0
      time_windows = ["last_week"]
    }
  }
}

variable "scaling_buffer_percentage" {
  description = "Safety buffer for predictive scaling (%)"
  type        = number
  default     = 20
  validation {
    condition     = var.scaling_buffer_percentage >= 5 && var.scaling_buffer_percentage <= 50
    error_message = "Scaling buffer must be between 5 and 50 percent."
  }
}

variable "prediction_window_hours" {
  description = "Prediction window in hours"
  type        = number
  default     = 24
  validation {
    condition     = var.prediction_window_hours >= 1 && var.prediction_window_hours <= 168
    error_message = "Prediction window must be between 1 and 168 hours."
  }
}

# ============================================================================
# COST ANOMALY DETECTION
# ============================================================================

variable "anomaly_detection_enabled" {
  description = "Enable cost anomaly detection"
  type        = bool
  default     = true
}

variable "anomaly_threshold_percentage" {
  description = "Cost anomaly detection threshold (%)"
  type        = number
  default     = 25
  validation {
    condition     = var.anomaly_threshold_percentage >= 10 && var.anomaly_threshold_percentage <= 100
    error_message = "Anomaly threshold must be between 10 and 100 percent."
  }
}

variable "anomaly_evaluation_periods" {
  description = "Number of evaluation periods for anomaly detection"
  type        = number
  default     = 2
  validation {
    condition     = var.anomaly_evaluation_periods >= 1 && var.anomaly_evaluation_periods <= 5
    error_message = "Evaluation periods must be between 1 and 5."
  }
}

variable "cost_anomaly_threshold" {
  description = "Cost anomaly threshold in USD"
  type        = number
  default     = 500
}

variable "low_utilization_threshold" {
  description = "Low utilization threshold for alerts (%)"
  type        = number
  default     = 15
}

# ============================================================================
# AUTOMATED RESOURCE TAGGING
# ============================================================================

variable "automated_tagging_enabled" {
  description = "Enable automated resource tagging"
  type        = bool
  default     = true
}

variable "tagging_rules" {
  description = "Rules for automated resource tagging"
  type = map(object({
    resource_types = list(string)
    tags = map(string)
    conditions = optional(map(string))
  }))
  default = {
    cost_allocation = {
      resource_types = ["ec2", "rds", "s3", "lambda"]
      tags = {
        CostCenter = "data-engineering"
        BusinessUnit = "analytics"
        AutoTagged = "true"
      }
    }
    compliance = {
      resource_types = ["*"]
      tags = {
        Compliance = "required"
        DataClassification = "confidential"
      }
    }
  }
}

variable "cost_center" {
  description = "Cost center for resource tagging"
  type        = string
  default     = "data-engineering"
}

variable "business_unit" {
  description = "Business unit for resource tagging"
  type        = string
  default     = "analytics"
}

variable "compliance_tags" {
  description = "Compliance-related tags"
  type        = map(string)
  default = {
    ComplianceScope = "SOC2,GDPR"
    DataRetention = "7years"
    EncryptionRequired = "true"
  }
}

variable "auto_tag_resource_types" {
  description = "Resource types to automatically tag"
  type        = list(string)
  default     = ["ec2", "rds", "s3", "lambda", "ecs", "eks"]
}

# ============================================================================
# RESOURCE LIFECYCLE MANAGEMENT
# ============================================================================

variable "lifecycle_management_enabled" {
  description = "Enable automated resource lifecycle management"
  type        = bool
  default     = true
}

variable "lifecycle_policies" {
  description = "Resource lifecycle policies"
  type = map(object({
    resource_type = string
    rules = list(object({
      condition = string
      action = string
      schedule = optional(string)
    }))
  }))
  default = {
    dev_instances = {
      resource_type = "ec2"
      rules = [
        {
          condition = "environment=dev AND idle_time>2h"
          action = "stop"
          schedule = "rate(1 hour)"
        },
        {
          condition = "environment=dev AND stopped_time>24h"
          action = "terminate"
          schedule = "rate(24 hours)"
        }
      ]
    }
    old_snapshots = {
      resource_type = "ec2-snapshot"
      rules = [
        {
          condition = "age>30d AND not_tagged_critical"
          action = "delete"
          schedule = "rate(7 days)"
        }
      ]
    }
  }
}

variable "auto_shutdown_rules" {
  description = "Automatic shutdown rules for resources"
  type = map(object({
    enabled = bool
    schedule = string
    resource_filter = map(string)
    exclude_tags = optional(list(string))
  }))
  default = {
    weekend_shutdown = {
      enabled = true
      schedule = "0 18 * * 5"  # Friday 6 PM
      resource_filter = {
        environment = "dev,test"
      }
      exclude_tags = ["critical", "always-on"]
    }
    nightly_shutdown = {
      enabled = true
      schedule = "0 20 * * 1-5"  # Weekdays 8 PM
      resource_filter = {
        environment = "dev"
        resource_type = "ec2"
      }
    }
  }
}

variable "retention_policies" {
  description = "Data retention policies"
  type = map(object({
    resource_type = string
    retention_days = number
    archive_after_days = optional(number)
    delete_after_days = optional(number)
  }))
  default = {
    logs = {
      resource_type = "s3"
      retention_days = 90
      archive_after_days = 30
      delete_after_days = 2555  # 7 years
    }
    snapshots = {
      resource_type = "ec2-snapshot"
      retention_days = 30
      delete_after_days = 90
    }
  }
}

variable "backup_policies" {
  description = "Automated backup policies"
  type = map(object({
    resource_type = string
    backup_schedule = string
    retention_days = number
    cross_region_copy = optional(bool)
  }))
  default = {
    database_backup = {
      resource_type = "rds"
      backup_schedule = "cron(0 2 * * ? *)"  # Daily at 2 AM
      retention_days = 30
      cross_region_copy = true
    }
    volume_backup = {
      resource_type = "ec2-volume"
      backup_schedule = "cron(0 1 * * ? *)"  # Daily at 1 AM
      retention_days = 7
    }
  }
}

variable "lifecycle_dry_run_mode" {
  description = "Enable dry run mode for lifecycle actions"
  type        = bool
  default     = true
}

# ============================================================================
# CLOUD PROVIDER SPECIFIC CONFIGURATION
# ============================================================================

# AWS Configuration
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

# Azure Configuration
variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "East US"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = ""
}

variable "azure_automation_account_name" {
  description = "Azure Automation Account name"
  type        = string
  default     = ""
}

# GCP Configuration
variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

# ============================================================================
# ALERTING AND NOTIFICATIONS
# ============================================================================

variable "alert_email" {
  description = "Email address for optimization alerts"
  type        = string
  default     = ""
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for notifications"
  type        = string
  default     = ""
  sensitive   = true
}

variable "pagerduty_integration_key" {
  description = "PagerDuty integration key for critical alerts"
  type        = string
  default     = ""
  sensitive   = true
}

variable "notification_preferences" {
  description = "Notification preferences for different alert types"
  type = map(object({
    email = bool
    slack = bool
    pagerduty = bool
    sms = optional(bool)
  }))
  default = {
    cost_anomaly = {
      email = true
      slack = true
      pagerduty = true
    }
    optimization_recommendations = {
      email = true
      slack = true
      pagerduty = false
    }
    resource_lifecycle = {
      email = true
      slack = false
      pagerduty = false
    }
  }
}

# ============================================================================
# ADVANCED CONFIGURATION
# ============================================================================

variable "ml_model_configuration" {
  description = "Configuration for ML models used in optimization"
  type = object({
    model_type = string
    training_data_retention_days = number
    retraining_frequency_days = number
    prediction_confidence_threshold = number
  })
  default = {
    model_type = "lstm"
    training_data_retention_days = 90
    retraining_frequency_days = 7
    prediction_confidence_threshold = 0.8
  }
}

variable "performance_benchmarks" {
  description = "Performance benchmarks for optimization decisions"
  type = map(object({
    metric_name = string
    target_value = number
    tolerance_percentage = number
    priority_weight = number
  }))
  default = {
    response_time = {
      metric_name = "ResponseTime"
      target_value = 200
      tolerance_percentage = 10
      priority_weight = 0.3
    }
    throughput = {
      metric_name = "RequestsPerSecond"
      target_value = 1000
      tolerance_percentage = 15
      priority_weight = 0.3
    }
    availability = {
      metric_name = "Availability"
      target_value = 99.9
      tolerance_percentage = 0.1
      priority_weight = 0.4
    }
  }
}

variable "integration_settings" {
  description = "Settings for external system integrations"
  type = map(object({
    enabled = bool
    endpoint = optional(string)
    api_key = optional(string)
    timeout_seconds = optional(number)
  }))
  default = {
    datadog = {
      enabled = false
      timeout_seconds = 30
    }
    newrelic = {
      enabled = false
      timeout_seconds = 30
    }
    prometheus = {
      enabled = true
      timeout_seconds = 30
    }
  }
  sensitive = true
}

# ============================================================================
# TAGS
# ============================================================================

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default = {
    ManagedBy = "Terraform"
    Module = "IntelligentInfrastructure"
    OptimizationEnabled = "true"
  }
}