# Multi-Cloud Data Platform Infrastructure Variables
# Comprehensive configuration for enterprise-grade deployment

# ============================================================================
# CORE CONFIGURATION
# ============================================================================

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "pwc-data-platform"
}

variable "organization" {
  description = "Organization name for resource tagging"
  type        = string
  default     = "PwC"
}

# ============================================================================
# MULTI-CLOUD PROVIDER CONFIGURATION
# ============================================================================

variable "cloud_provider" {
  description = "Primary cloud provider (aws, azure, gcp, multi)"
  type        = string
  default     = "multi"
  validation {
    condition     = contains(["aws", "azure", "gcp", "multi"], var.cloud_provider)
    error_message = "Cloud provider must be aws, azure, gcp, or multi."
  }
}

variable "enable_multi_cloud" {
  description = "Enable multi-cloud deployment with cross-cloud connectivity"
  type        = bool
  default     = true
}

variable "primary_cloud" {
  description = "Primary cloud provider for the main workload"
  type        = string
  default     = "aws"
  validation {
    condition     = contains(["aws", "azure", "gcp"], var.primary_cloud)
    error_message = "Primary cloud must be aws, azure, or gcp."
  }
}

# ============================================================================
# AWS CONFIGURATION
# ============================================================================

variable "aws_region" {
  description = "Primary AWS region"
  type        = string
  default     = "us-west-2"
}

variable "aws_secondary_region" {
  description = "Secondary AWS region for disaster recovery"
  type        = string
  default     = "us-east-1"
}

variable "aws_availability_zones" {
  description = "AWS availability zones"
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b", "us-west-2c"]
}

variable "aws_account_id" {
  description = "AWS Account ID"
  type        = string
  default     = ""
}

# ============================================================================
# AZURE CONFIGURATION
# ============================================================================

variable "azure_location" {
  description = "Primary Azure location"
  type        = string
  default     = "West US 2"
}

variable "azure_secondary_location" {
  description = "Secondary Azure location for disaster recovery"
  type        = string
  default     = "East US 2"
}

variable "azure_subscription_id" {
  description = "Azure Subscription ID"
  type        = string
  default     = ""
}

variable "azure_tenant_id" {
  description = "Azure Tenant ID"
  type        = string
  default     = ""
}

# ============================================================================
# GCP CONFIGURATION
# ============================================================================

variable "gcp_project_id" {
  description = "GCP Project ID"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "Primary GCP region"
  type        = string
  default     = "us-west1"
}

variable "gcp_secondary_region" {
  description = "Secondary GCP region for disaster recovery"
  type        = string
  default     = "us-east1"
}

variable "gcp_zones" {
  description = "GCP zones"
  type        = list(string)
  default     = ["us-west1-a", "us-west1-b", "us-west1-c"]
}

# ============================================================================
# NETWORK CONFIGURATION
# ============================================================================

variable "vpc_cidr_blocks" {
  description = "CIDR blocks for VPCs in different clouds"
  type = object({
    aws   = string
    azure = string
    gcp   = string
  })
  default = {
    aws   = "10.0.0.0/16"
    azure = "10.1.0.0/16"
    gcp   = "10.2.0.0/16"
  }
}

variable "enable_cross_cloud_networking" {
  description = "Enable cross-cloud networking with VPN connections"
  type        = bool
  default     = true
}

variable "enable_private_endpoints" {
  description = "Enable private endpoints for enhanced security"
  type        = bool
  default     = true
}

# ============================================================================
# DATA PLATFORM CONFIGURATION
# ============================================================================

variable "enable_data_lake" {
  description = "Enable data lake infrastructure"
  type        = bool
  default     = true
}

variable "enable_data_warehouse" {
  description = "Enable data warehouse infrastructure"
  type        = bool
  default     = true
}

variable "enable_streaming" {
  description = "Enable streaming data infrastructure"
  type        = bool
  default     = true
}

variable "enable_ml_platform" {
  description = "Enable machine learning platform"
  type        = bool
  default     = true
}

variable "data_retention_days" {
  description = "Data retention period in days"
  type        = number
  default     = 365
  validation {
    condition     = var.data_retention_days >= 1 && var.data_retention_days <= 2555
    error_message = "Data retention must be between 1 and 2555 days (7 years)."
  }
}

# ============================================================================
# DISASTER RECOVERY CONFIGURATION
# ============================================================================

variable "enable_disaster_recovery" {
  description = "Enable disaster recovery across regions/clouds"
  type        = bool
  default     = true
}

variable "rto_minutes" {
  description = "Recovery Time Objective in minutes"
  type        = number
  default     = 15
  validation {
    condition     = var.rto_minutes >= 5 && var.rto_minutes <= 240
    error_message = "RTO must be between 5 and 240 minutes."
  }
}

variable "rpo_minutes" {
  description = "Recovery Point Objective in minutes"
  type        = number
  default     = 5
  validation {
    condition     = var.rpo_minutes >= 1 && var.rpo_minutes <= 60
    error_message = "RPO must be between 1 and 60 minutes."
  }
}

variable "backup_retention_days" {
  description = "Backup retention period in days"
  type        = number
  default     = 30
}

variable "enable_cross_region_replication" {
  description = "Enable cross-region data replication"
  type        = bool
  default     = true
}

# ============================================================================
# COST OPTIMIZATION CONFIGURATION
# ============================================================================

variable "enable_cost_optimization" {
  description = "Enable cost optimization features"
  type        = bool
  default     = true
}

variable "enable_auto_scaling" {
  description = "Enable auto-scaling for compute resources"
  type        = bool
  default     = true
}

variable "enable_spot_instances" {
  description = "Enable spot instances for cost savings"
  type        = bool
  default     = false
}

variable "cost_budget_monthly_usd" {
  description = "Monthly cost budget in USD"
  type        = number
  default     = 5000
}

variable "cost_alert_threshold_percent" {
  description = "Cost alert threshold as percentage of budget"
  type        = number
  default     = 80
  validation {
    condition     = var.cost_alert_threshold_percent >= 50 && var.cost_alert_threshold_percent <= 100
    error_message = "Cost alert threshold must be between 50 and 100 percent."
  }
}

# ============================================================================
# SECURITY CONFIGURATION
# ============================================================================

variable "enable_encryption_at_rest" {
  description = "Enable encryption at rest for all storage"
  type        = bool
  default     = true
}

variable "enable_encryption_in_transit" {
  description = "Enable encryption in transit for all communications"
  type        = bool
  default     = true
}

variable "enable_network_security" {
  description = "Enable advanced network security features"
  type        = bool
  default     = true
}

variable "enable_access_logging" {
  description = "Enable comprehensive access logging"
  type        = bool
  default     = true
}

variable "security_contact_email" {
  description = "Security contact email for alerts"
  type        = string
  default     = ""
}

# ============================================================================
# COMPLIANCE CONFIGURATION
# ============================================================================

variable "enable_compliance_monitoring" {
  description = "Enable compliance monitoring and reporting"
  type        = bool
  default     = true
}

variable "compliance_standards" {
  description = "List of compliance standards to adhere to"
  type        = list(string)
  default     = ["SOC2", "ISO27001", "PCI-DSS", "GDPR", "HIPAA"]
}

variable "enable_audit_logging" {
  description = "Enable comprehensive audit logging"
  type        = bool
  default     = true
}

variable "audit_log_retention_days" {
  description = "Audit log retention period in days"
  type        = number
  default     = 2555  # 7 years
}

# ============================================================================
# MONITORING AND ALERTING CONFIGURATION
# ============================================================================

variable "enable_advanced_monitoring" {
  description = "Enable advanced monitoring and observability"
  type        = bool
  default     = true
}

variable "enable_synthetic_monitoring" {
  description = "Enable synthetic monitoring for applications"
  type        = bool
  default     = true
}

variable "monitoring_retention_days" {
  description = "Monitoring data retention period in days"
  type        = number
  default     = 90
}

variable "alert_notification_channels" {
  description = "List of notification channels for alerts"
  type        = list(string)
  default     = ["email", "slack", "pagerduty"]
}

# ============================================================================
# KUBERNETES CONFIGURATION
# ============================================================================

variable "kubernetes_version" {
  description = "Kubernetes cluster version"
  type        = string
  default     = "1.28"
}

variable "enable_kubernetes_autoscaling" {
  description = "Enable Kubernetes cluster autoscaling"
  type        = bool
  default     = true
}

variable "kubernetes_node_pools" {
  description = "Kubernetes node pool configurations"
  type = map(object({
    min_nodes    = number
    max_nodes    = number
    machine_type = string
    disk_size_gb = number
  }))
  default = {
    system = {
      min_nodes    = 2
      max_nodes    = 5
      machine_type = "n1-standard-2"
      disk_size_gb = 50
    }
    data = {
      min_nodes    = 3
      max_nodes    = 10
      machine_type = "n1-standard-4"
      disk_size_gb = 100
    }
  }
}

# ============================================================================
# PERFORMANCE OPTIMIZATION
# ============================================================================

variable "enable_performance_monitoring" {
  description = "Enable performance monitoring and optimization"
  type        = bool
  default     = true
}

variable "enable_caching" {
  description = "Enable distributed caching"
  type        = bool
  default     = true
}

variable "enable_cdn" {
  description = "Enable Content Delivery Network"
  type        = bool
  default     = true
}

variable "performance_tier" {
  description = "Performance tier (basic, standard, premium)"
  type        = string
  default     = "standard"
  validation {
    condition     = contains(["basic", "standard", "premium"], var.performance_tier)
    error_message = "Performance tier must be basic, standard, or premium."
  }
}

# ============================================================================
# DEPLOYMENT CONFIGURATION
# ============================================================================

variable "deployment_strategy" {
  description = "Deployment strategy (blue-green, canary, rolling)"
  type        = string
  default     = "blue-green"
  validation {
    condition     = contains(["blue-green", "canary", "rolling"], var.deployment_strategy)
    error_message = "Deployment strategy must be blue-green, canary, or rolling."
  }
}

variable "enable_gitops" {
  description = "Enable GitOps deployment workflow"
  type        = bool
  default     = true
}

variable "ci_cd_integration" {
  description = "CI/CD integration platform"
  type        = string
  default     = "github-actions"
  validation {
    condition     = contains(["github-actions", "gitlab-ci", "azure-devops", "jenkins"], var.ci_cd_integration)
    error_message = "CI/CD integration must be github-actions, gitlab-ci, azure-devops, or jenkins."
  }
}

# ============================================================================
# COMMON TAGS
# ============================================================================

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project          = "PwC-DataEnginering-Challenge"
    ManagedBy        = "Terraform"
    Owner           = "DataEngineering"
    CostCenter      = "Technology"
    SecurityLevel   = "High"
    BackupRequired  = "true"
    ComplianceScope = "Enterprise"
  }
}

variable "additional_tags" {
  description = "Additional tags specific to the deployment"
  type        = map(string)
  default     = {}
}