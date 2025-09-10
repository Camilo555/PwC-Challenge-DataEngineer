# BMAD Platform - Terraform Variables
# Multi-cloud infrastructure configuration variables

# Environment Configuration
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

# Multi-Cloud Region Configuration
variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-west-2"
}

variable "azure_region" {
  description = "Azure region for deployment"
  type        = string
  default     = "West US 2"
}

variable "gcp_region" {
  description = "Google Cloud region for deployment"
  type        = string
  default     = "us-west2"
}

variable "gcp_project_id" {
  description = "Google Cloud project ID"
  type        = string
}

# Kubernetes Configuration
variable "kubernetes_version" {
  description = "Kubernetes version for all clusters"
  type        = string
  default     = "1.28"
}

variable "node_instance_types" {
  description = "Instance types for Kubernetes nodes across clouds"
  type = object({
    aws = object({
      general = list(string)
      compute = list(string)
      memory  = list(string)
    })
    azure = object({
      general = string
      compute = string
      memory  = string
    })
    gcp = object({
      general = string
      compute = string
      memory  = string
    })
  })
  default = {
    aws = {
      general = ["m6i.large", "m6i.xlarge"]
      compute = ["c6i.xlarge", "c6i.2xlarge"]
      memory  = ["r6i.xlarge", "r6i.2xlarge"]
    }
    azure = {
      general = "Standard_D4s_v3"
      compute = "Standard_F8s_v2"
      memory  = "Standard_E4s_v3"
    }
    gcp = {
      general = "e2-standard-4"
      compute = "c2-standard-8"
      memory  = "n2-highmem-4"
    }
  }
}

# Scaling Configuration
variable "min_nodes" {
  description = "Minimum number of nodes in each cluster"
  type        = number
  default     = 3
  
  validation {
    condition     = var.min_nodes >= 3
    error_message = "Minimum nodes must be at least 3 for high availability."
  }
}

variable "max_nodes" {
  description = "Maximum number of nodes in each cluster"
  type        = number
  default     = 50
  
  validation {
    condition     = var.max_nodes <= 100
    error_message = "Maximum nodes cannot exceed 100."
  }
}

# Data Lake Configuration
variable "data_lake_config" {
  description = "Data lake configuration for medallion architecture"
  type = object({
    bronze_retention_days = number
    silver_retention_days = number
    gold_retention_days   = number
    enable_encryption     = bool
    enable_versioning     = bool
    enable_lifecycle      = bool
  })
  default = {
    bronze_retention_days = 90
    silver_retention_days = 365
    gold_retention_days   = 2555  # 7 years
    enable_encryption     = true
    enable_versioning     = true
    enable_lifecycle      = true
  }
}

# CDN Configuration for Mobile Story 4.1
variable "cdn_config" {
  description = "Global CDN configuration for mobile optimization"
  type = object({
    enable_mobile_optimization = bool
    enable_image_optimization  = bool
    enable_compression         = bool
    cache_ttl_api             = number
    cache_ttl_static          = number
    cache_ttl_mobile          = number
  })
  default = {
    enable_mobile_optimization = true
    enable_image_optimization  = true
    enable_compression         = true
    cache_ttl_api             = 300   # 5 minutes
    cache_ttl_static          = 86400 # 24 hours
    cache_ttl_mobile          = 3600  # 1 hour
  }
}

# Vector Database Configuration for AI Story 4.2
variable "vector_db_config" {
  description = "Vector database configuration for AI workloads"
  type = object({
    vector_dimension     = number
    index_type          = string
    similarity_metric   = string
    max_connections     = number
    storage_size        = string
    enable_backup       = bool
    replication_factor  = number
  })
  default = {
    vector_dimension   = 1536      # OpenAI embeddings
    index_type        = "HNSW"
    similarity_metric = "cosine"
    max_connections   = 1000
    storage_size      = "1Ti"
    enable_backup     = true
    replication_factor = 3
  }
}

# Security Configuration
variable "security_config" {
  description = "Security and compliance configuration"
  type = object({
    enable_encryption_at_rest   = bool
    enable_encryption_in_transit = bool
    enable_network_policies     = bool
    enable_pod_security_policies = bool
    enable_falco               = bool
    enable_trivy               = bool
    enable_opa_gatekeeper      = bool
    compliance_frameworks      = list(string)
  })
  default = {
    enable_encryption_at_rest    = true
    enable_encryption_in_transit = true
    enable_network_policies      = true
    enable_pod_security_policies = true
    enable_falco                = true
    enable_trivy                = true
    enable_opa_gatekeeper       = true
    compliance_frameworks       = ["SOC2", "GDPR", "HIPAA"]
  }
}

# Monitoring Configuration
variable "monitoring_config" {
  description = "Monitoring and observability configuration"
  type = object({
    enable_prometheus    = bool
    enable_grafana      = bool
    enable_jaeger       = bool
    enable_datadog      = bool
    enable_elasticsearch = bool
    enable_kibana       = bool
    retention_days      = number
    alert_channels      = list(string)
  })
  default = {
    enable_prometheus    = true
    enable_grafana      = true
    enable_jaeger       = true
    enable_datadog      = true
    enable_elasticsearch = true
    enable_kibana       = true
    retention_days      = 90
    alert_channels      = ["slack", "email", "pagerduty"]
  }
}

# Cost Optimization Configuration
variable "cost_optimization_config" {
  description = "Cost optimization configuration"
  type = object({
    target_reduction_percentage = number
    enable_spot_instances      = bool
    enable_reserved_instances  = bool
    enable_scheduled_scaling   = bool
    enable_auto_shutdown       = bool
    non_prod_shutdown_schedule = string
    non_prod_startup_schedule  = string
  })
  default = {
    target_reduction_percentage = 40
    enable_spot_instances      = true
    enable_reserved_instances  = true
    enable_scheduled_scaling   = true
    enable_auto_shutdown       = true
    non_prod_shutdown_schedule = "0 20 * * MON-FRI"
    non_prod_startup_schedule  = "0 8 * * MON-FRI"
  }
}

# Budget Configuration
variable "aws_monthly_budget" {
  description = "Monthly budget limit for AWS resources"
  type        = number
  default     = 50000
}

variable "azure_monthly_budget" {
  description = "Monthly budget limit for Azure resources"
  type        = number
  default     = 50000
}

variable "gcp_monthly_budget" {
  description = "Monthly budget limit for GCP resources"
  type        = number
  default     = 50000
}

# Disaster Recovery Configuration
variable "disaster_recovery_config" {
  description = "Disaster recovery configuration"
  type = object({
    rto_target_minutes        = number
    rpo_target_minutes        = number
    availability_target       = number
    enable_cross_region_backup = bool
    enable_cross_cloud_backup  = bool
    backup_retention_days     = number
    replication_schedule      = string
  })
  default = {
    rto_target_minutes        = 60     # 1 hour
    rpo_target_minutes        = 15     # 15 minutes
    availability_target       = 99.99  # 99.99%
    enable_cross_region_backup = true
    enable_cross_cloud_backup  = true
    backup_retention_days     = 90
    replication_schedule      = "0 */4 * * *"  # Every 4 hours
  }
}

# Database Configuration
variable "database_config" {
  description = "Multi-cloud database configuration"
  type = object({
    aws = object({
      engine_version     = string
      instance_class     = string
      allocated_storage  = number
      max_allocated_storage = number
      backup_retention_period = number
      multi_az          = bool
    })
    azure = object({
      sku_name          = string
      storage_mb        = number
      backup_retention_days = number
      geo_redundant_backup = bool
    })
    gcp = object({
      tier              = string
      disk_size         = number
      backup_start_time = string
      backup_retention_days = number
    })
  })
  default = {
    aws = {
      engine_version         = "15.4"
      instance_class         = "db.r6g.xlarge"
      allocated_storage      = 100
      max_allocated_storage  = 1000
      backup_retention_period = 7
      multi_az              = true
    }
    azure = {
      sku_name             = "GP_Gen5_4"
      storage_mb           = 102400  # 100GB
      backup_retention_days = 7
      geo_redundant_backup = true
    }
    gcp = {
      tier                 = "db-n1-standard-4"
      disk_size           = 100
      backup_start_time   = "03:00"
      backup_retention_days = 7
    }
  }
}

# Network Configuration
variable "network_config" {
  description = "Network configuration for all clouds"
  type = object({
    vpc_cidrs = object({
      aws   = string
      azure = string
      gcp   = string
    })
    enable_nat_gateway    = bool
    enable_vpn_gateway    = bool
    enable_vpc_peering    = bool
    enable_transit_gateway = bool
  })
  default = {
    vpc_cidrs = {
      aws   = "10.0.0.0/16"
      azure = "10.1.0.0/16"
      gcp   = "10.2.0.0/16"
    }
    enable_nat_gateway    = true
    enable_vpn_gateway    = true
    enable_vpc_peering    = true
    enable_transit_gateway = true
  }
}

# Alert Configuration
variable "slack_webhook_url" {
  description = "Slack webhook URL for alerts"
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

variable "alert_email" {
  description = "Email address for alert notifications"
  type        = string
  default     = "alerts@bmad-platform.com"
}

# Feature Flags
variable "feature_flags" {
  description = "Feature flags for enabling/disabling components"
  type = object({
    enable_aws_deployment     = bool
    enable_azure_deployment   = bool
    enable_gcp_deployment     = bool
    enable_data_lake         = bool
    enable_vector_database   = bool
    enable_global_cdn        = bool
    enable_ml_pipeline       = bool
    enable_real_time_streaming = bool
  })
  default = {
    enable_aws_deployment     = true
    enable_azure_deployment   = true
    enable_gcp_deployment     = true
    enable_data_lake         = true
    enable_vector_database   = true
    enable_global_cdn        = true
    enable_ml_pipeline       = true
    enable_real_time_streaming = true
  }
}

# Performance Configuration
variable "performance_config" {
  description = "Performance optimization configuration"
  type = object({
    enable_auto_scaling      = bool
    enable_predictive_scaling = bool
    cpu_target_utilization   = number
    memory_target_utilization = number
    scale_up_cooldown       = number
    scale_down_cooldown     = number
    max_surge              = string
    max_unavailable        = string
  })
  default = {
    enable_auto_scaling      = true
    enable_predictive_scaling = true
    cpu_target_utilization   = 70
    memory_target_utilization = 80
    scale_up_cooldown       = 60
    scale_down_cooldown     = 300
    max_surge              = "25%"
    max_unavailable        = "25%"
  }
}

# Storage Configuration
variable "storage_config" {
  description = "Storage configuration across clouds"
  type = object({
    storage_classes = object({
      hot     = string
      warm    = string
      cold    = string
      archive = string
    })
    encryption_enabled = bool
    versioning_enabled = bool
    lifecycle_enabled  = bool
    replication_enabled = bool
  })
  default = {
    storage_classes = {
      hot     = "STANDARD"
      warm    = "STANDARD_IA"
      cold    = "GLACIER"
      archive = "DEEP_ARCHIVE"
    }
    encryption_enabled = true
    versioning_enabled = true
    lifecycle_enabled  = true
    replication_enabled = true
  }
}

# Compliance Configuration
variable "compliance_config" {
  description = "Compliance and governance configuration"
  type = object({
    data_residency_requirements = list(string)
    encryption_standards       = list(string)
    audit_log_retention_days   = number
    enable_gdpr_compliance     = bool
    enable_hipaa_compliance    = bool
    enable_soc2_compliance     = bool
    enable_pci_compliance      = bool
  })
  default = {
    data_residency_requirements = ["US", "EU"]
    encryption_standards       = ["AES-256", "TLS-1.3"]
    audit_log_retention_days   = 2555  # 7 years
    enable_gdpr_compliance     = true
    enable_hipaa_compliance    = true
    enable_soc2_compliance     = true
    enable_pci_compliance      = false
  }
}

# API Configuration
variable "api_config" {
  description = "API gateway and service configuration"
  type = object({
    rate_limit_per_minute    = number
    burst_limit             = number
    enable_caching          = bool
    cache_ttl_seconds       = number
    enable_compression      = bool
    enable_cors             = bool
    cors_allowed_origins    = list(string)
    throttling_enabled      = bool
  })
  default = {
    rate_limit_per_minute = 1000
    burst_limit          = 2000
    enable_caching       = true
    cache_ttl_seconds    = 300
    enable_compression   = true
    enable_cors          = true
    cors_allowed_origins = ["https://app.bmad-platform.com", "https://mobile.bmad-platform.com"]
    throttling_enabled   = true
  }
}