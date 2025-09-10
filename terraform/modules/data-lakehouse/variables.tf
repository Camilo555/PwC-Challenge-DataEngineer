# Variables for Medallion Data Lakehouse Architecture Module

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "pwc-retail-platform"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
}

variable "tags" {
  description = "Common tags to be applied to resources"
  type        = map(string)
  default     = {}
}

# ============================================================================
# MULTI-CLOUD DEPLOYMENT FLAGS
# ============================================================================

variable "deploy_to_aws" {
  description = "Deploy to AWS"
  type        = bool
  default     = true
}

variable "deploy_to_azure" {
  description = "Deploy to Azure"
  type        = bool
  default     = false
}

variable "deploy_to_gcp" {
  description = "Deploy to GCP"
  type        = bool
  default     = false
}

# ============================================================================
# AWS CONFIGURATION
# ============================================================================

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "VPC ID for AWS resources"
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs"
  type        = list(string)
  default     = []
}

variable "ec2_key_name" {
  description = "EC2 Key Pair name for EMR cluster"
  type        = string
  default     = ""
}

# EMR Configuration
variable "emr_master_instance_type" {
  description = "EMR master node instance type"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "EMR core node instance type"
  type        = string
  default     = "m5.large"
}

variable "emr_task_instance_type" {
  description = "EMR task node instance type"
  type        = string
  default     = "m5.large"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 2
}

variable "emr_max_instance_count" {
  description = "Maximum number of EMR instances"
  type        = number
  default     = 20
}

# Hive Metastore Configuration
variable "hive_metastore_endpoint" {
  description = "Hive Metastore RDS endpoint"
  type        = string
  default     = ""
}

variable "hive_metastore_username" {
  description = "Hive Metastore username"
  type        = string
  default     = "hive"
}

variable "hive_metastore_password" {
  description = "Hive Metastore password"
  type        = string
  sensitive   = true
  default     = ""
}

# Data Replication
variable "enable_cross_region_replication" {
  description = "Enable cross-region replication"
  type        = bool
  default     = false
}

variable "backup_bucket_name" {
  description = "Backup bucket name prefix for replication"
  type        = string
  default     = "pwc-retail-backup"
}

# ============================================================================
# AZURE CONFIGURATION
# ============================================================================

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

variable "azure_subnet_ids" {
  description = "List of Azure subnet IDs"
  type        = list(string)
  default     = []
}

# Synapse Configuration
variable "synapse_admin_login" {
  description = "Synapse SQL administrator login"
  type        = string
  default     = "sqladmin"
}

variable "synapse_admin_password" {
  description = "Synapse SQL administrator password"
  type        = string
  sensitive   = true
  default     = ""
}

# ============================================================================
# GCP CONFIGURATION
# ============================================================================

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

variable "gcp_zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "gcp_service_account_email" {
  description = "GCP service account email"
  type        = string
  default     = ""
}

variable "organization_domain" {
  description = "Organization domain for BigQuery access"
  type        = string
  default     = ""
}

# Dataproc Configuration
variable "dataproc_master_machine_type" {
  description = "Dataproc master node machine type"
  type        = string
  default     = "n1-standard-4"
}

variable "dataproc_worker_machine_type" {
  description = "Dataproc worker node machine type"
  type        = string
  default     = "n1-standard-2"
}

variable "dataproc_worker_count" {
  description = "Number of Dataproc worker nodes"
  type        = number
  default     = 2
}

variable "dataproc_preemptible_count" {
  description = "Number of Dataproc preemptible nodes"
  type        = number
  default     = 0
}

variable "dataproc_min_instances" {
  description = "Minimum number of Dataproc instances"
  type        = number
  default     = 2
}

variable "dataproc_max_instances" {
  description = "Maximum number of Dataproc instances"
  type        = number
  default     = 50
}

# ============================================================================
# DATA GOVERNANCE AND QUALITY
# ============================================================================

variable "enable_data_governance" {
  description = "Enable data governance features"
  type        = bool
  default     = true
}

variable "enable_data_quality_monitoring" {
  description = "Enable data quality monitoring"
  type        = bool
  default     = true
}

variable "enable_data_lineage" {
  description = "Enable data lineage tracking"
  type        = bool
  default     = true
}

variable "enable_pii_detection" {
  description = "Enable PII detection and masking"
  type        = bool
  default     = true
}

# ============================================================================
# DATA RETENTION AND LIFECYCLE
# ============================================================================

variable "bronze_retention_days" {
  description = "Retention period for bronze layer data (days)"
  type        = number
  default     = 2555  # 7 years
}

variable "silver_retention_days" {
  description = "Retention period for silver layer data (days)"
  type        = number
  default     = 1825  # 5 years
}

variable "gold_retention_days" {
  description = "Retention period for gold layer data (days)"
  type        = number
  default     = 3650  # 10 years
}

# ============================================================================
# PROCESSING CONFIGURATION
# ============================================================================

variable "enable_streaming_processing" {
  description = "Enable streaming data processing"
  type        = bool
  default     = true
}

variable "enable_batch_processing" {
  description = "Enable batch data processing"
  type        = bool
  default     = true
}

variable "spark_executor_memory" {
  description = "Spark executor memory"
  type        = string
  default     = "4g"
}

variable "spark_executor_cores" {
  description = "Spark executor cores"
  type        = number
  default     = 2
}

variable "spark_driver_memory" {
  description = "Spark driver memory"
  type        = string
  default     = "2g"
}

variable "max_concurrent_jobs" {
  description = "Maximum concurrent processing jobs"
  type        = number
  default     = 10
}

# ============================================================================
# SECURITY CONFIGURATION
# ============================================================================

variable "enable_encryption_at_rest" {
  description = "Enable encryption at rest"
  type        = bool
  default     = true
}

variable "enable_encryption_in_transit" {
  description = "Enable encryption in transit"
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption"
  type        = string
  default     = ""
}

# ============================================================================
# MONITORING AND ALERTING
# ============================================================================

variable "enable_detailed_monitoring" {
  description = "Enable detailed monitoring"
  type        = bool
  default     = true
}

variable "monitoring_retention_days" {
  description = "Monitoring data retention period (days)"
  type        = number
  default     = 90
}

variable "alert_email" {
  description = "Email for alerts and notifications"
  type        = string
  default     = ""
}

# ============================================================================
# PERFORMANCE OPTIMIZATION
# ============================================================================

variable "enable_intelligent_tiering" {
  description = "Enable intelligent storage tiering"
  type        = bool
  default     = true
}

variable "enable_query_optimization" {
  description = "Enable query optimization features"
  type        = bool
  default     = true
}

variable "enable_caching" {
  description = "Enable data caching"
  type        = bool
  default     = true
}

variable "partition_strategy" {
  description = "Data partitioning strategy"
  type        = string
  default     = "date_based"
  
  validation {
    condition     = contains(["date_based", "hash_based", "range_based"], var.partition_strategy)
    error_message = "Partition strategy must be one of: date_based, hash_based, range_based."
  }
}

# ============================================================================
# COST OPTIMIZATION
# ============================================================================

variable "enable_spot_instances" {
  description = "Enable spot instances for cost optimization"
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

variable "auto_shutdown_enabled" {
  description = "Enable automatic shutdown of development resources"
  type        = bool
  default     = true
}

variable "reserved_capacity_percentage" {
  description = "Percentage of reserved capacity to maintain"
  type        = number
  default     = 30
  
  validation {
    condition     = var.reserved_capacity_percentage >= 0 && var.reserved_capacity_percentage <= 100
    error_message = "Reserved capacity percentage must be between 0 and 100."
  }
}

# ============================================================================
# DATABRICKS CONFIGURATION (OPTIONAL)
# ============================================================================

variable "enable_databricks" {
  description = "Enable Databricks integration"
  type        = bool
  default     = false
}

variable "databricks_workspace_name" {
  description = "Databricks workspace name"
  type        = string
  default     = ""
}

variable "databricks_sku" {
  description = "Databricks SKU"
  type        = string
  default     = "premium"
  
  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "Databricks SKU must be one of: standard, premium, trial."
  }
}

# ============================================================================
# SNOWFLAKE CONFIGURATION (OPTIONAL)
# ============================================================================

variable "enable_snowflake" {
  description = "Enable Snowflake integration"
  type        = bool
  default     = false
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
  default     = ""
}

variable "snowflake_region" {
  description = "Snowflake region"
  type        = string
  default     = ""
}

variable "snowflake_warehouse_size" {
  description = "Snowflake warehouse size"
  type        = string
  default     = "MEDIUM"
  
  validation {
    condition = contains([
      "X-SMALL", "SMALL", "MEDIUM", "LARGE", "X-LARGE", 
      "2X-LARGE", "3X-LARGE", "4X-LARGE", "5X-LARGE", "6X-LARGE"
    ], var.snowflake_warehouse_size)
    error_message = "Invalid Snowflake warehouse size."
  }
}