# PwC Retail Data Platform - Terraform Variables
# Comprehensive variable definitions for multi-cloud deployment

# ============================================================================
# GLOBAL CONFIGURATION
# ============================================================================

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "cloud_provider" {
  description = "Cloud provider to deploy to (aws, azure, gcp)"
  type        = string
  default     = "aws"
  
  validation {
    condition     = contains(["aws", "azure", "gcp"], var.cloud_provider)
    error_message = "Cloud provider must be one of: aws, azure, gcp."
  }
}

variable "region" {
  description = "Cloud provider region"
  type        = string
  default     = "us-east-1"
}

variable "availability_zones" {
  description = "List of availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

# ============================================================================
# NETWORKING CONFIGURATION
# ============================================================================

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.10.0/24", "10.0.20.0/24", "10.0.30.0/24"]
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access the infrastructure"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# ============================================================================
# KUBERNETES CONFIGURATION
# ============================================================================

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.27"
}

variable "node_instance_types" {
  description = "EC2 instance types for worker nodes"
  type        = list(string)
  default     = ["m5.large", "m5.xlarge"]
}

variable "node_desired_size" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 3
}

variable "node_max_size" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 10
}

variable "node_min_size" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "storage_class" {
  description = "Kubernetes storage class for persistent volumes"
  type        = string
  default     = "gp3"
}

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

variable "db_instance_class" {
  description = "Database instance class"
  type        = string
  default     = "db.r5.large"
}

variable "db_allocated_storage" {
  description = "Database allocated storage in GB"
  type        = number
  default     = 100
}

variable "db_backup_retention_period" {
  description = "Database backup retention period in days"
  type        = number
  default     = 7
}

variable "db_backup_window" {
  description = "Database backup window"
  type        = string
  default     = "03:00-04:00"
}

variable "db_maintenance_window" {
  description = "Database maintenance window"
  type        = string
  default     = "sun:04:00-sun:05:00"
}

# ============================================================================
# APPLICATION CONFIGURATION
# ============================================================================

variable "api_image" {
  description = "Docker image for API service"
  type        = string
  default     = "ghcr.io/camilo555/pwc-challenge-dataengineer-api:latest"
}

variable "etl_image" {
  description = "Docker image for ETL service"
  type        = string
  default     = "ghcr.io/camilo555/pwc-challenge-dataengineer-etl:latest"
}

variable "dbt_image" {
  description = "Docker image for dbt service"
  type        = string
  default     = "ghcr.io/camilo555/pwc-challenge-dataengineer-dbt:latest"
}

variable "dagster_image" {
  description = "Docker image for Dagster service"
  type        = string
  default     = "ghcr.io/camilo555/pwc-challenge-dataengineer-dagster:latest"
}

# ============================================================================
# RESOURCE LIMITS AND REQUESTS
# ============================================================================

variable "api_cpu_request" {
  description = "CPU request for API pods"
  type        = string
  default     = "100m"
}

variable "api_memory_request" {
  description = "Memory request for API pods"
  type        = string
  default     = "256Mi"
}

variable "api_cpu_limit" {
  description = "CPU limit for API pods"
  type        = string
  default     = "1000m"
}

variable "api_memory_limit" {
  description = "Memory limit for API pods"
  type        = string
  default     = "1Gi"
}

variable "etl_cpu_request" {
  description = "CPU request for ETL pods"
  type        = string
  default     = "500m"
}

variable "etl_memory_request" {
  description = "Memory request for ETL pods"
  type        = string
  default     = "1Gi"
}

variable "etl_cpu_limit" {
  description = "CPU limit for ETL pods"
  type        = string
  default     = "2000m"
}

variable "etl_memory_limit" {
  description = "Memory limit for ETL pods"
  type        = string
  default     = "4Gi"
}

# ============================================================================
# MONITORING AND OBSERVABILITY
# ============================================================================

variable "enable_monitoring" {
  description = "Enable monitoring stack (Prometheus, Grafana)"
  type        = bool
  default     = true
}

variable "enable_logging" {
  description = "Enable centralized logging"
  type        = bool
  default     = true
}

variable "prometheus_retention" {
  description = "Prometheus data retention period"
  type        = string
  default     = "15d"
}

variable "prometheus_storage" {
  description = "Prometheus storage size"
  type        = string
  default     = "50Gi"
}

variable "alert_manager_config" {
  description = "AlertManager configuration"
  type        = string
  default     = ""
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for alerts"
  type        = string
  default     = ""
  sensitive   = true
}

# ============================================================================
# SECURITY AND COMPLIANCE
# ============================================================================

variable "enable_encryption" {
  description = "Enable encryption at rest and in transit"
  type        = bool
  default     = true
}

variable "enable_audit_logs" {
  description = "Enable audit logging"
  type        = bool
  default     = true
}

variable "secrets_backend" {
  description = "Secrets management backend (aws-secrets-manager, azure-key-vault, gcp-secret-manager)"
  type        = string
  default     = "aws-secrets-manager"
  
  validation {
    condition = contains([
      "aws-secrets-manager",
      "azure-key-vault", 
      "gcp-secret-manager"
    ], var.secrets_backend)
    error_message = "Secrets backend must be one of: aws-secrets-manager, azure-key-vault, gcp-secret-manager."
  }
}

# ============================================================================
# CLOUD PROVIDER SPECIFIC VARIABLES
# ============================================================================

# AWS Specific
variable "aws_profile" {
  description = "AWS profile to use"
  type        = string
  default     = "default"
}

# Azure Specific
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

# GCP Specific
variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "gcp_credentials_file" {
  description = "Path to GCP service account credentials file"
  type        = string
  default     = ""
}

# ============================================================================
# FEATURE FLAGS
# ============================================================================

variable "enable_autoscaling" {
  description = "Enable horizontal pod autoscaling"
  type        = bool
  default     = true
}

variable "enable_ingress" {
  description = "Enable ingress controller"
  type        = bool
  default     = true
}

variable "enable_cert_manager" {
  description = "Enable cert-manager for TLS certificates"
  type        = bool
  default     = true
}

variable "enable_backup" {
  description = "Enable automated backups"
  type        = bool
  default     = true
}

variable "enable_disaster_recovery" {
  description = "Enable disaster recovery setup"
  type        = bool
  default     = false
}

# ============================================================================
# CUSTOM TAGS AND LABELS
# ============================================================================

variable "additional_tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "cost_center" {
  description = "Cost center for billing purposes"
  type        = string
  default     = "data-engineering"
}

variable "business_unit" {
  description = "Business unit"
  type        = string
  default     = "analytics"
}