# Multi-Cloud Foundation Variables
# Enterprise-grade configuration for all BMAD stories support

# =============================================================================
# GLOBAL CONFIGURATION
# =============================================================================

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "pwc-data-platform"
}

variable "environment" {
  description = "Environment name (dev, staging, production)"
  type        = string
  
  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Environment must be dev, staging, or production."
  }
}

variable "owner" {
  description = "Owner of the infrastructure"
  type        = string
  default     = "platform-team"
}

variable "cost_center" {
  description = "Cost center for billing"
  type        = string
  default     = "engineering"
}

variable "domain_name" {
  description = "Primary domain name for the platform"
  type        = string
  default     = "pwc-platform.com"
}

variable "kubernetes_version" {
  description = "Kubernetes version for all clusters"
  type        = string
  default     = "1.28"
}

# =============================================================================
# AWS CONFIGURATION
# =============================================================================

variable "aws_primary_region" {
  description = "AWS primary region"
  type        = string
  default     = "us-east-1"
}

variable "aws_secondary_region" {
  description = "AWS secondary region for DR"
  type        = string
  default     = "us-west-2"
}

variable "aws_tertiary_region" {
  description = "AWS tertiary region for global distribution"
  type        = string
  default     = "eu-west-1"
}

variable "aws_vpc_cidr" {
  description = "CIDR block for AWS VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "aws_secondary_vpc_cidr" {
  description = "CIDR block for AWS secondary region VPC"
  type        = string
  default     = "10.5.0.0/16"
}

variable "aws_availability_zones" {
  description = "Availability zones for AWS primary region"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "aws_secondary_availability_zones" {
  description = "Availability zones for AWS secondary region"
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b", "us-west-2c"]
}

# =============================================================================
# AZURE CONFIGURATION
# =============================================================================

variable "azure_primary_region" {
  description = "Azure primary region"
  type        = string
  default     = "East US"
}

variable "azure_secondary_region" {
  description = "Azure secondary region"
  type        = string
  default     = "West Europe"
}

variable "azure_tertiary_region" {
  description = "Azure tertiary region"
  type        = string
  default     = "Southeast Asia"
}

variable "azure_vnet_address_space" {
  description = "Address space for Azure VNet"
  type        = list(string)
  default     = ["10.1.0.0/16"]
}

variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
  default     = ""
}

variable "azure_tenant_id" {
  description = "Azure tenant ID"
  type        = string
  default     = ""
}

# =============================================================================
# GCP CONFIGURATION
# =============================================================================

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = "pwc-data-platform"
}

variable "gcp_primary_region" {
  description = "GCP primary region"
  type        = string
  default     = "us-central1"
}

variable "gcp_secondary_region" {
  description = "GCP secondary region"
  type        = string
  default     = "europe-west1"
}

variable "gcp_tertiary_region" {
  description = "GCP tertiary region"
  type        = string
  default     = "asia-southeast1"
}

variable "gcp_primary_zone" {
  description = "GCP primary zone"
  type        = string
  default     = "us-central1-a"
}

variable "gcp_credentials_file" {
  description = "Path to GCP credentials file"
  type        = string
  default     = ""
}

# =============================================================================
# PERFORMANCE AND SCALING CONFIGURATION
# =============================================================================

variable "enable_autoscaling" {
  description = "Enable intelligent autoscaling across all clouds"
  type        = bool
  default     = true
}

variable "min_node_count" {
  description = "Minimum number of nodes per cluster"
  type        = number
  default     = 3
}

variable "max_node_count" {
  description = "Maximum number of nodes per cluster"
  type        = number
  default     = 50
}

variable "enable_spot_instances" {
  description = "Enable spot instances for cost optimization"
  type        = bool
  default     = true
}

variable "spot_instance_percentage" {
  description = "Percentage of spot instances in auto-scaling groups"
  type        = number
  default     = 70
  
  validation {
    condition     = var.spot_instance_percentage >= 0 && var.spot_instance_percentage <= 100
    error_message = "Spot instance percentage must be between 0 and 100."
  }
}

# =============================================================================
# DATA LAKE CONFIGURATION
# =============================================================================

variable "enable_data_lake" {
  description = "Enable medallion data lake architecture"
  type        = bool
  default     = true
}

variable "data_retention_days" {
  description = "Data retention period in days"
  type        = number
  default     = 2555 # ~7 years for compliance
}

variable "enable_data_lifecycle" {
  description = "Enable automatic data lifecycle management"
  type        = bool
  default     = true
}

variable "data_lake_encryption" {
  description = "Enable encryption for data lake storage"
  type        = bool
  default     = true
}

# =============================================================================
# REAL-TIME PROCESSING CONFIGURATION
# =============================================================================

variable "enable_streaming" {
  description = "Enable real-time streaming infrastructure"
  type        = bool
  default     = true
}

variable "kafka_partitions" {
  description = "Number of Kafka partitions for streaming"
  type        = number
  default     = 12
}

variable "kafka_replication_factor" {
  description = "Kafka replication factor"
  type        = number
  default     = 3
}

variable "redis_memory_gb" {
  description = "Redis memory allocation in GB"
  type        = number
  default     = 16
}

# =============================================================================
# AI/ML CONFIGURATION
# =============================================================================

variable "enable_ml_infrastructure" {
  description = "Enable ML/AI infrastructure components"
  type        = bool
  default     = true
}

variable "enable_gpu_nodes" {
  description = "Enable GPU nodes for ML workloads"
  type        = bool
  default     = true
}

variable "gpu_node_count" {
  description = "Number of GPU nodes per cluster"
  type        = number
  default     = 2
}

variable "ml_model_storage_gb" {
  description = "Storage allocation for ML models in GB"
  type        = number
  default     = 1000
}

variable "enable_vector_database" {
  description = "Enable vector database for AI/LLM operations"
  type        = bool
  default     = true
}

# =============================================================================
# MOBILE INFRASTRUCTURE CONFIGURATION
# =============================================================================

variable "enable_mobile_infrastructure" {
  description = "Enable mobile-specific infrastructure"
  type        = bool
  default     = true
}

variable "enable_global_cdn" {
  description = "Enable global CDN for mobile performance"
  type        = bool
  default     = true
}

variable "cdn_cache_ttl_seconds" {
  description = "CDN cache TTL in seconds"
  type        = number
  default     = 3600
}

variable "enable_push_notifications" {
  description = "Enable push notification infrastructure"
  type        = bool
  default     = true
}

# =============================================================================
# SECURITY CONFIGURATION
# =============================================================================

variable "enable_encryption_at_rest" {
  description = "Enable encryption at rest for all storage"
  type        = bool
  default     = true
}

variable "enable_encryption_in_transit" {
  description = "Enable encryption in transit"
  type        = bool
  default     = true
}

variable "enable_network_policies" {
  description = "Enable Kubernetes network policies"
  type        = bool
  default     = true
}

variable "enable_pod_security_standards" {
  description = "Enable Pod Security Standards"
  type        = bool
  default     = true
}

variable "security_scanning_enabled" {
  description = "Enable security scanning for containers and infrastructure"
  type        = bool
  default     = true
}

# =============================================================================
# COMPLIANCE CONFIGURATION
# =============================================================================

variable "compliance_frameworks" {
  description = "List of compliance frameworks to implement"
  type        = list(string)
  default     = ["SOC2", "GDPR", "HIPAA", "PCI-DSS"]
}

variable "audit_log_retention_days" {
  description = "Audit log retention period in days"
  type        = number
  default     = 2555 # ~7 years
}

variable "enable_data_classification" {
  description = "Enable automatic data classification"
  type        = bool
  default     = true
}

variable "enable_policy_enforcement" {
  description = "Enable automated policy enforcement"
  type        = bool
  default     = true
}

# =============================================================================
# MONITORING CONFIGURATION
# =============================================================================

variable "monitoring_retention_days" {
  description = "Monitoring data retention in days"
  type        = number
  default     = 90
}

variable "enable_distributed_tracing" {
  description = "Enable distributed tracing with Jaeger"
  type        = bool
  default     = true
}

variable "log_level" {
  description = "Default log level for applications"
  type        = string
  default     = "INFO"
  
  validation {
    condition     = contains(["DEBUG", "INFO", "WARN", "ERROR"], var.log_level)
    error_message = "Log level must be DEBUG, INFO, WARN, or ERROR."
  }
}

variable "grafana_admin_password" {
  description = "Grafana admin password"
  type        = string
  sensitive   = true
  default     = ""
}

# =============================================================================
# COST OPTIMIZATION CONFIGURATION
# =============================================================================

variable "enable_cost_optimization" {
  description = "Enable intelligent cost optimization"
  type        = bool
  default     = true
}

variable "cost_optimization_schedule" {
  description = "Schedule for cost optimization runs (cron format)"
  type        = string
  default     = "0 2 * * *" # Daily at 2 AM
}

variable "reserved_instance_percentage" {
  description = "Target percentage of reserved instances"
  type        = number
  default     = 50
  
  validation {
    condition     = var.reserved_instance_percentage >= 0 && var.reserved_instance_percentage <= 100
    error_message = "Reserved instance percentage must be between 0 and 100."
  }
}

variable "enable_scheduled_scaling" {
  description = "Enable scheduled scaling based on usage patterns"
  type        = bool
  default     = true
}

# =============================================================================
# DISASTER RECOVERY CONFIGURATION
# =============================================================================

variable "enable_disaster_recovery" {
  description = "Enable disaster recovery across regions"
  type        = bool
  default     = true
}

variable "rto_minutes" {
  description = "Recovery Time Objective in minutes"
  type        = number
  default     = 15
}

variable "rpo_minutes" {
  description = "Recovery Point Objective in minutes"
  type        = number
  default     = 5
}

variable "backup_frequency_hours" {
  description = "Backup frequency in hours"
  type        = number
  default     = 4
}

variable "cross_region_replication" {
  description = "Enable cross-region replication for critical data"
  type        = bool
  default     = true
}

# =============================================================================
# DEVELOPER PLATFORM CONFIGURATION
# =============================================================================

variable "enable_developer_portal" {
  description = "Enable developer portal and SDK infrastructure"
  type        = bool
  default     = true
}

variable "enable_sandbox_environments" {
  description = "Enable automated sandbox environment provisioning"
  type        = bool
  default     = true
}

variable "sandbox_retention_hours" {
  description = "Sandbox environment retention in hours"
  type        = number
  default     = 168 # 1 week
}

variable "enable_api_gateway" {
  description = "Enable API gateway for developer access"
  type        = bool
  default     = true
}

variable "api_rate_limit_per_minute" {
  description = "API rate limit per minute per developer"
  type        = number
  default     = 1000
}

# =============================================================================
# EDGE COMPUTING CONFIGURATION
# =============================================================================

variable "enable_edge_computing" {
  description = "Enable edge computing infrastructure"
  type        = bool
  default     = true
}

variable "edge_locations" {
  description = "List of edge computing locations"
  type        = list(string)
  default     = ["us-east-1", "us-west-1", "eu-west-1", "ap-southeast-1"]
}

variable "edge_cache_size_gb" {
  description = "Edge cache size in GB per location"
  type        = number
  default     = 100
}

variable "edge_function_memory_mb" {
  description = "Memory allocation for edge functions in MB"
  type        = number
  default     = 512
}

# =============================================================================
# TESTING CONFIGURATION
# =============================================================================

variable "enable_chaos_engineering" {
  description = "Enable chaos engineering for resilience testing"
  type        = bool
  default     = false
}

variable "enable_load_testing" {
  description = "Enable automated load testing infrastructure"
  type        = bool
  default     = true
}

variable "load_test_max_users" {
  description = "Maximum concurrent users for load testing"
  type        = number
  default     = 10000
}

# =============================================================================
# FEATURE FLAGS
# =============================================================================

variable "feature_flags" {
  description = "Feature flags for enabling/disabling specific capabilities"
  type        = map(bool)
  default     = {
    enable_advanced_analytics   = true
    enable_realtime_dashboard  = true
    enable_ml_insights         = true
    enable_mobile_app          = true
    enable_voice_assistant     = true
    enable_automated_governance = true
    enable_plugin_marketplace  = true
    enable_multi_tenant        = true
  }
}

# =============================================================================
# RESOURCE TAGS
# =============================================================================

variable "additional_tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}