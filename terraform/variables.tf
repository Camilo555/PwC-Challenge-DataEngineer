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

# ============================================================================
# MESSAGING CONFIGURATION (RabbitMQ & Kafka)
# ============================================================================

# RabbitMQ Configuration
variable "rabbitmq_instance_type" {
  description = "RabbitMQ instance type"
  type        = string
  default     = "mq.t3.micro"
}

variable "rabbitmq_username" {
  description = "RabbitMQ admin username"
  type        = string
  default     = "admin"
}

variable "rabbitmq_version" {
  description = "RabbitMQ version"
  type        = string
  default     = "3.11.28"
}

variable "rabbitmq_port" {
  description = "RabbitMQ port"
  type        = number
  default     = 5672
}

variable "rabbitmq_storage_size" {
  description = "RabbitMQ storage size in GB"
  type        = number
  default     = 20
}

# Kafka Configuration
variable "kafka_version" {
  description = "Kafka version"
  type        = string
  default     = "2.8.1"
}

variable "kafka_instance_type" {
  description = "Kafka instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "kafka_broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 3
}

variable "kafka_partition_count" {
  description = "Default number of partitions"
  type        = number
  default     = 3
}

variable "kafka_replication_factor" {
  description = "Default replication factor"
  type        = number
  default     = 3
}

variable "kafka_storage_size" {
  description = "Kafka storage size in GB per broker"
  type        = number
  default     = 100
}

variable "kafka_topic_prefix" {
  description = "Prefix for Kafka topics"
  type        = string
  default     = "pwc-retail"
}

# ============================================================================
# DATA LAKE AND ANALYTICS CONFIGURATION
# ============================================================================

variable "enable_data_lake" {
  description = "Enable data lake with Delta Lake/Iceberg"
  type        = bool
  default     = true
}

variable "data_lake_storage_class" {
  description = "Storage class for data lake (STANDARD, IA, GLACIER)"
  type        = string
  default     = "STANDARD_IA"
}

variable "enable_data_catalog" {
  description = "Enable managed data catalog"
  type        = bool
  default     = true
}

variable "enable_databricks" {
  description = "Enable Databricks integration"
  type        = bool
  default     = false
}

variable "enable_snowflake" {
  description = "Enable Snowflake integration"
  type        = bool
  default     = false
}

variable "enable_data_mesh" {
  description = "Enable data mesh architecture patterns"
  type        = bool
  default     = true
}

# ============================================================================
# MULTI-CLOUD AND DISASTER RECOVERY
# ============================================================================

variable "enable_multi_cloud" {
  description = "Enable multi-cloud deployment"
  type        = bool
  default     = false
}

variable "primary_cloud" {
  description = "Primary cloud provider for active workloads"
  type        = string
  default     = "aws"
  
  validation {
    condition     = contains(["aws", "azure", "gcp"], var.primary_cloud)
    error_message = "Primary cloud must be one of: aws, azure, gcp."
  }
}

variable "secondary_cloud" {
  description = "Secondary cloud provider for disaster recovery"
  type        = string
  default     = "azure"
  
  validation {
    condition     = contains(["aws", "azure", "gcp"], var.secondary_cloud)
    error_message = "Secondary cloud must be one of: aws, azure, gcp."
  }
}

variable "cross_cloud_replication" {
  description = "Enable cross-cloud data replication"
  type        = bool
  default     = false
}

variable "rto_minutes" {
  description = "Recovery Time Objective in minutes"
  type        = number
  default     = 60
}

variable "rpo_minutes" {
  description = "Recovery Point Objective in minutes"
  type        = number
  default     = 15
}

# ============================================================================
# COST OPTIMIZATION AND GOVERNANCE
# ============================================================================

variable "enable_cost_optimization" {
  description = "Enable automated cost optimization"
  type        = bool
  default     = true
}

variable "enable_resource_tagging" {
  description = "Enable comprehensive resource tagging"
  type        = bool
  default     = true
}

variable "budget_alert_threshold" {
  description = "Budget alert threshold as percentage"
  type        = number
  default     = 80
}

variable "auto_shutdown_enabled" {
  description = "Enable automatic shutdown of non-prod resources"
  type        = bool
  default     = true
}

variable "reserved_instance_coverage" {
  description = "Target reserved instance coverage percentage"
  type        = number
  default     = 70
}

# ============================================================================
# PERFORMANCE AND SCALING
# ============================================================================

variable "enable_intelligent_scaling" {
  description = "Enable AI-driven intelligent scaling"
  type        = bool
  default     = true
}

variable "scaling_metric_threshold" {
  description = "CPU utilization threshold for scaling"
  type        = number
  default     = 70
}

variable "enable_spot_instances" {
  description = "Enable spot instances for cost optimization"
  type        = bool
  default     = true
}

variable "spot_instance_percentage" {
  description = "Percentage of spot instances in node groups"
  type        = number
  default     = 50
}

# ============================================================================
# GITOPS AND CI/CD CONFIGURATION
# ============================================================================

variable "enable_gitops" {
  description = "Enable GitOps workflow with ArgoCD/Flux"
  type        = bool
  default     = true
}

variable "gitops_repo_url" {
  description = "Git repository URL for GitOps"
  type        = string
  default     = ""
}

variable "enable_infrastructure_drift_detection" {
  description = "Enable infrastructure drift detection"
  type        = bool
  default     = true
}

variable "drift_detection_schedule" {
  description = "Cron schedule for drift detection"
  type        = string
  default     = "0 2 * * *"
}

# ============================================================================
# COMPLIANCE AND SECURITY FRAMEWORKS
# ============================================================================

variable "compliance_frameworks" {
  description = "List of compliance frameworks to implement"
  type        = list(string)
  default     = ["SOC2", "PCI-DSS", "GDPR", "HIPAA"]
}

variable "enable_security_scanning" {
  description = "Enable continuous security scanning"
  type        = bool
  default     = true
}

variable "enable_vulnerability_assessment" {
  description = "Enable automated vulnerability assessments"
  type        = bool
  default     = true
}

variable "security_scan_schedule" {
  description = "Cron schedule for security scans"
  type        = string
  default     = "0 1 * * *"
}

# ============================================================================
# DATA GOVERNANCE AND QUALITY
# ============================================================================

variable "enable_data_lineage" {
  description = "Enable data lineage tracking"
  type        = bool
  default     = true
}

variable "enable_data_quality_monitoring" {
  description = "Enable automated data quality monitoring"
  type        = bool
  default     = true
}

variable "data_retention_days" {
  description = "Default data retention period in days"
  type        = number
  default     = 2555  # 7 years
}

variable "enable_pii_detection" {
  description = "Enable automatic PII detection and masking"
  type        = bool
  default     = true
}