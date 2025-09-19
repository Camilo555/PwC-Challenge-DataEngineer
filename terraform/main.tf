# PwC Retail Data Platform - Multi-Cloud Terraform Configuration
# Enterprise-grade infrastructure deployment for AWS, Azure, and GCP

terraform {
  required_version = ">= 1.6.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.70"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Backend configuration for remote state management
  backend "s3" {
    bucket         = "pwc-retail-terraform-state-${random_string.state_suffix.result}"
    key            = "infrastructure/${var.environment}/terraform.tfstate"
    region         = var.aws_region
    dynamodb_table = "pwc-retail-terraform-locks-${var.environment}"
    encrypt        = true
    versioning     = true
    
    # Server-side encryption with KMS
    kms_key_id = "arn:aws:kms:${var.aws_region}:${data.aws_caller_identity.current.account_id}:alias/terraform-state-key"
    
    # Workspace configuration for multi-environment support
    workspace_key_prefix = "env"
  }
}

# Local values for consistent naming and tagging
locals {
  project_name = "pwc-retail-platform"
  environment  = var.environment
  region       = var.region
  
  # Multi-cloud deployment flags
  deploy_to_aws   = var.enable_multi_cloud ? true : (var.cloud_provider == "aws")
  deploy_to_azure = var.enable_multi_cloud ? true : (var.cloud_provider == "azure")
  deploy_to_gcp   = var.enable_multi_cloud ? true : (var.cloud_provider == "gcp")
  
  # Primary and secondary cloud configuration
  primary_cloud   = var.enable_multi_cloud ? var.primary_cloud : var.cloud_provider
  secondary_cloud = var.enable_multi_cloud ? var.secondary_cloud : null
  
  # Enhanced tagging strategy
  common_tags = merge(
    {
      Project           = "PwC-Retail-Data-Platform"
      Environment       = var.environment
      ManagedBy         = "Terraform"
      Owner            = "Data-Engineering-Team"
      CostCenter       = var.cost_center
      BusinessUnit     = var.business_unit
      CreatedAt        = timestamp()
      LastUpdated      = timestamp()
      AutoShutdown     = var.auto_shutdown_enabled ? "enabled" : "disabled"
      BackupEnabled    = var.enable_backup ? "true" : "false"
      ComplianceScope  = join(",", var.compliance_frameworks)
      DataClassification = "restricted"
      DisasterRecovery = var.enable_disaster_recovery ? "enabled" : "disabled"
    },
    var.additional_tags
  )
  
  # Resource naming convention
  name_prefix = "${local.project_name}-${local.environment}"
  
  # Cloud-specific naming
  aws_name_prefix   = "${local.name_prefix}-aws"
  azure_name_prefix = "${local.name_prefix}-azure"
  gcp_name_prefix   = "${local.name_prefix}-gcp"
}

# Random resources for unique naming
resource "random_string" "state_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Random password for database admin
resource "random_password" "db_admin_password" {
  length  = 32
  special = true
  upper   = true
  lower   = true
  numeric = true
}

# Random password for RabbitMQ admin
resource "random_password" "rabbitmq_password" {
  length  = 24
  special = true
  upper   = true
  lower   = true
  numeric = true
}

# Random password for Kafka (if applicable)
resource "random_password" "kafka_password" {
  length  = 24
  special = true
  upper   = true
  lower   = true
  numeric = true
}

# Data sources
data "aws_caller_identity" "current" {
  count = var.cloud_provider == "aws" ? 1 : 0
}

data "aws_region" "current" {
  count = var.cloud_provider == "aws" ? 1 : 0
}

# Store sensitive values in AWS Systems Manager Parameter Store
resource "aws_ssm_parameter" "db_admin_password" {
  count = var.cloud_provider == "aws" ? 1 : 0
  
  name  = "/${local.project_name}/${local.environment}/database/admin-password"
  type  = "SecureString"
  value = random_password.db_admin_password.result
  
  tags = local.common_tags
}

# ============================================================================
# MULTI-CLOUD INFRASTRUCTURE MODULES
# ============================================================================

# AWS Infrastructure Module
module "aws_infrastructure" {
  count  = local.deploy_to_aws ? 1 : 0
  source = "./modules/aws"
  
  project_name     = local.project_name
  environment      = local.environment
  region           = var.region
  availability_zones = var.availability_zones
  
  # Networking
  vpc_cidr           = var.vpc_cidr
  public_subnet_cidrs = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  
  # EKS Configuration
  cluster_version     = var.kubernetes_version
  node_instance_types = var.node_instance_types
  node_desired_size   = var.node_desired_size
  node_max_size      = var.node_max_size
  node_min_size      = var.node_min_size
  
  # Database Configuration
  db_instance_class    = var.db_instance_class
  db_allocated_storage = var.db_allocated_storage
  db_admin_password   = random_password.db_admin_password.result
  
  # Monitoring and Security
  enable_monitoring = var.enable_monitoring
  enable_logging   = var.enable_logging
  
  tags = local.common_tags
}

# Azure Infrastructure Module
module "azure_infrastructure" {
  count  = local.deploy_to_azure ? 1 : 0
  source = "./modules/azure"
  
  project_name    = local.project_name
  environment     = local.environment
  location        = var.region
  
  # Resource Group
  resource_group_name = "${local.name_prefix}-rg"
  
  # Networking
  vnet_address_space     = var.vpc_cidr
  public_subnet_prefixes = var.public_subnet_cidrs
  private_subnet_prefixes = var.private_subnet_cidrs
  
  # AKS Configuration
  kubernetes_version  = var.kubernetes_version
  node_count         = var.node_desired_size
  node_vm_size       = var.node_instance_types[0]
  
  # Database Configuration
  postgres_sku_name      = var.db_instance_class
  postgres_storage_mb    = var.db_allocated_storage * 1024
  postgres_admin_password = random_password.db_admin_password.result
  
  # Monitoring and Security
  enable_monitoring = var.enable_monitoring
  enable_logging   = var.enable_logging
  
  tags = local.common_tags
}

# Google Cloud Infrastructure Module
module "gcp_infrastructure" {
  count  = local.deploy_to_gcp ? 1 : 0
  source = "./modules/gcp"
  
  project_name = local.project_name
  environment  = local.environment
  region       = var.region
  zones        = var.availability_zones
  
  # Project Configuration
  project_id = var.gcp_project_id
  
  # Networking
  vpc_cidr_range         = var.vpc_cidr
  public_subnet_cidrs   = var.public_subnet_cidrs
  private_subnet_cidrs  = var.private_subnet_cidrs
  
  # GKE Configuration
  kubernetes_version    = var.kubernetes_version
  node_count           = var.node_desired_size
  node_machine_type    = var.node_instance_types[0]
  
  # Database Configuration
  postgres_tier           = var.db_instance_class
  postgres_disk_size     = var.db_allocated_storage
  postgres_admin_password = random_password.db_admin_password.result
  
  # Monitoring and Security
  enable_monitoring = var.enable_monitoring
  enable_logging   = var.enable_logging
  
  labels = local.common_tags
}

# Messaging Infrastructure Module (RabbitMQ & Kafka)
module "messaging_infrastructure" {
  source = "./modules/messaging"
  
  project_name = local.project_name
  environment  = local.environment
  cloud_provider = var.cloud_provider
  
  # RabbitMQ Configuration
  rabbitmq_instance_type = var.rabbitmq_instance_type
  rabbitmq_username      = var.rabbitmq_username
  rabbitmq_password      = random_password.rabbitmq_password.result
  rabbitmq_version       = var.rabbitmq_version
  
  # Kafka Configuration
  kafka_version       = var.kafka_version
  kafka_instance_type = var.kafka_instance_type
  kafka_broker_count  = var.kafka_broker_count
  kafka_partition_count = var.kafka_partition_count
  kafka_replication_factor = var.kafka_replication_factor
  
  # Network Configuration
  vpc_id            = local.vpc_id
  private_subnets   = local.private_subnets
  security_group_id = local.security_group_id
  
  # Storage Configuration
  rabbitmq_storage_size = var.rabbitmq_storage_size
  kafka_storage_size    = var.kafka_storage_size
  
  # Monitoring and Logging
  enable_monitoring = var.enable_monitoring
  enable_logging   = var.enable_logging
  
  labels = local.common_tags
}

# ============================================================================
# DATA LAKE AND ANALYTICS MODULES
# ============================================================================

# Multi-Cloud Data Lake Module
module "data_lake" {
  count  = var.enable_data_lake ? 1 : 0
  source = "./modules/data-lake"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud configuration
  deploy_to_aws   = local.deploy_to_aws
  deploy_to_azure = local.deploy_to_azure
  deploy_to_gcp   = local.deploy_to_gcp
  primary_cloud   = local.primary_cloud
  
  # Data Lake Configuration
  storage_class           = var.data_lake_storage_class
  enable_versioning      = true
  enable_encryption      = var.enable_encryption
  enable_cross_cloud_replication = var.cross_cloud_replication
  
  # Data Catalog Integration
  enable_data_catalog = var.enable_data_catalog
  enable_data_lineage = var.enable_data_lineage
  
  # Retention and Lifecycle
  data_retention_days = var.data_retention_days
  
  tags = local.common_tags
}

# Data Catalog Module
module "data_catalog" {
  count  = var.enable_data_catalog ? 1 : 0
  source = "./modules/data-catalog"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud integration
  aws_data_lake_bucket    = local.deploy_to_aws ? try(module.data_lake[0].aws_bucket_name, null) : null
  azure_data_lake_account = local.deploy_to_azure ? try(module.data_lake[0].azure_storage_account, null) : null
  gcp_data_lake_bucket    = local.deploy_to_gcp ? try(module.data_lake[0].gcp_bucket_name, null) : null
  
  # Data Discovery and Classification
  enable_pii_detection        = var.enable_pii_detection
  enable_data_quality_monitoring = var.enable_data_quality_monitoring
  
  # Integration with external systems
  database_connections = {
    postgresql = {
      host     = local.database_host
      database = local.database_name
      username = local.database_username
    }
  }
  
  tags = local.common_tags
}

# Analytics Services Integration Module
module "analytics_services" {
  count  = var.enable_databricks || var.enable_snowflake ? 1 : 0
  source = "./modules/analytics-services"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Service Configuration
  enable_databricks = var.enable_databricks
  enable_snowflake  = var.enable_snowflake
  
  # Network Configuration
  vpc_id              = local.vpc_id
  private_subnet_ids  = local.private_subnets
  
  # Data Lake Integration
  data_lake_bucket = var.enable_data_lake ? try(module.data_lake[0].primary_bucket_name, null) : null
  
  tags = local.common_tags
}

# ============================================================================
# DATA MESH ARCHITECTURE MODULE
# ============================================================================

module "data_mesh" {
  count  = var.enable_data_mesh ? 1 : 0
  source = "./modules/data-mesh"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud configuration
  primary_cloud   = local.primary_cloud
  secondary_cloud = local.secondary_cloud
  
  # Data Lake Integration
  data_lake_buckets = var.enable_data_lake ? {
    aws   = local.deploy_to_aws ? try(module.data_lake[0].aws_bucket_name, null) : null
    azure = local.deploy_to_azure ? try(module.data_lake[0].azure_container_name, null) : null
    gcp   = local.deploy_to_gcp ? try(module.data_lake[0].gcp_bucket_name, null) : null
  } : {}
  
  # Kafka Integration for Data Products
  kafka_bootstrap_servers = local.kafka_bootstrap_servers
  kafka_topic_prefix     = var.kafka_topic_prefix
  
  # Data Catalog Integration
  data_catalog_endpoint = var.enable_data_catalog ? try(module.data_catalog[0].catalog_endpoint, null) : null
  
  # Data Quality and Governance
  enable_data_contracts       = true
  enable_schema_registry     = true
  enable_data_product_catalog = true
  
  tags = local.common_tags
}

# ============================================================================
# COST OPTIMIZATION AND GOVERNANCE MODULE
# ============================================================================

module "cost_optimization" {
  count  = var.enable_cost_optimization ? 1 : 0
  source = "./modules/cost-optimization"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud cost management
  deploy_to_aws   = local.deploy_to_aws
  deploy_to_azure = local.deploy_to_azure
  deploy_to_gcp   = local.deploy_to_gcp
  
  # Cost Controls
  budget_alert_threshold      = var.budget_alert_threshold
  auto_shutdown_enabled      = var.auto_shutdown_enabled
  reserved_instance_coverage = var.reserved_instance_coverage
  enable_spot_instances      = var.enable_spot_instances
  spot_instance_percentage   = var.spot_instance_percentage
  
  # Resource Optimization
  enable_intelligent_scaling = var.enable_intelligent_scaling
  scaling_metric_threshold   = var.scaling_metric_threshold
  
  # Tagging and Governance
  enable_resource_tagging = var.enable_resource_tagging
  cost_center            = var.cost_center
  business_unit          = var.business_unit
  
  tags = local.common_tags
}

# ============================================================================
# DISASTER RECOVERY AND BACKUP MODULE
# ============================================================================

module "disaster_recovery" {
  count  = var.enable_disaster_recovery ? 1 : 0
  source = "./modules/disaster-recovery"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud DR configuration
  primary_cloud   = local.primary_cloud
  secondary_cloud = local.secondary_cloud
  
  # Recovery Objectives
  rto_minutes = var.rto_minutes
  rpo_minutes = var.rpo_minutes
  
  # Cross-cloud replication
  enable_cross_cloud_replication = var.cross_cloud_replication
  
  # Database DR
  database_endpoints = {
    primary   = local.database_host
    secondary = var.enable_multi_cloud ? try(local.secondary_database_host, null) : null
  }
  
  # Data Lake DR
  data_lake_buckets = var.enable_data_lake ? {
    primary   = try(module.data_lake[0].primary_bucket_name, null)
    secondary = var.enable_multi_cloud ? try(module.data_lake[0].secondary_bucket_name, null) : null
  } : {}
  
  # Kubernetes DR
  cluster_endpoints = {
    primary   = local.cluster_name
    secondary = var.enable_multi_cloud ? try(local.secondary_cluster_name, null) : null
  }
  
  tags = local.common_tags
}

# ============================================================================
# GITOPS AND CI/CD MODULE
# ============================================================================

module "gitops" {
  count  = var.enable_gitops ? 1 : 0
  source = "./modules/gitops"
  
  project_name = local.project_name
  environment  = local.environment
  
  # GitOps Configuration
  gitops_repo_url = var.gitops_repo_url
  
  # Kubernetes clusters for GitOps
  cluster_contexts = compact([
    local.deploy_to_aws ? try(module.aws_infrastructure[0].cluster_context, "") : "",
    local.deploy_to_azure ? try(module.azure_infrastructure[0].cluster_context, "") : "",
    local.deploy_to_gcp ? try(module.gcp_infrastructure[0].cluster_context, "") : ""
  ])
  
  # Infrastructure Drift Detection
  enable_drift_detection = var.enable_infrastructure_drift_detection
  drift_detection_schedule = var.drift_detection_schedule
  
  # CI/CD Integration
  enable_automated_deployment = true
  enable_canary_deployment   = var.environment == "prod"
  
  tags = local.common_tags
}

# Kubernetes Application Deployment Module
module "kubernetes_apps" {
  source = "./modules/kubernetes"
  
  # Depends on cloud infrastructure being created first
  depends_on = [
    module.aws_infrastructure,
    module.azure_infrastructure,
    module.gcp_infrastructure,
    module.messaging_infrastructure
  ]
  
  project_name = local.project_name
  environment  = local.environment
  namespace    = "${local.project_name}-${local.environment}"
  
  # Application Configuration
  api_image      = var.api_image
  etl_image      = var.etl_image
  dbt_image      = var.dbt_image
  dagster_image  = var.dagster_image
  
  # Database Connection
  database_host     = local.database_host
  database_name     = local.database_name
  database_username = local.database_username
  database_password = random_password.db_admin_password.result
  
  # Messaging Configuration
  rabbitmq_host     = local.rabbitmq_host
  rabbitmq_port     = var.rabbitmq_port
  rabbitmq_username = var.rabbitmq_username
  rabbitmq_password = random_password.rabbitmq_password.result
  kafka_bootstrap_servers = local.kafka_bootstrap_servers
  kafka_topic_prefix = var.kafka_topic_prefix
  
  # Storage Configuration
  storage_class = var.storage_class
  
  # Resource Limits
  api_cpu_request    = var.api_cpu_request
  api_memory_request = var.api_memory_request
  etl_cpu_request    = var.etl_cpu_request
  etl_memory_request = var.etl_memory_request
  
  # Monitoring and Logging
  enable_monitoring = var.enable_monitoring
  enable_logging   = var.enable_logging
  
  labels = local.common_tags
}

# Monitoring and Observability Module
module "monitoring" {
  count  = var.enable_monitoring ? 1 : 0
  source = "./modules/monitoring"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Kubernetes cluster context
  cluster_name = local.cluster_name
  
  # Prometheus Configuration
  prometheus_retention = var.prometheus_retention
  prometheus_storage   = var.prometheus_storage
  
  # Grafana Configuration
  grafana_admin_password = random_password.db_admin_password.result
  
  # Alerting Configuration
  alert_manager_config = var.alert_manager_config
  slack_webhook_url   = var.slack_webhook_url
  
  labels = local.common_tags
}

# Security and Compliance Module
module "security" {
  source = "./modules/security"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud security
  deploy_to_aws   = local.deploy_to_aws
  deploy_to_azure = local.deploy_to_azure
  deploy_to_gcp   = local.deploy_to_gcp
  
  # Network Security
  allowed_cidr_blocks = var.allowed_cidr_blocks
  
  # Secrets Management
  secrets_backend = var.secrets_backend
  
  # Compliance Settings
  enable_encryption    = var.enable_encryption
  enable_audit_logs   = var.enable_audit_logs
  compliance_frameworks = var.compliance_frameworks
  
  # Security Scanning
  enable_security_scanning        = var.enable_security_scanning
  enable_vulnerability_assessment = var.enable_vulnerability_assessment
  security_scan_schedule         = var.security_scan_schedule
  
  # PII Protection
  enable_pii_detection = var.enable_pii_detection
  
  labels = local.common_tags
}

# Local values for cross-module references
locals {
  # Primary cloud database connection details
  database_host = local.primary_cloud == "aws" ? (
    local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].database_endpoint : ""
  ) : local.primary_cloud == "azure" ? (
    local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].database_fqdn : ""
  ) : local.primary_cloud == "gcp" ? (
    local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].database_connection_name : ""
  ) : ""
  
  # Secondary cloud database connection (for DR)
  secondary_database_host = var.enable_multi_cloud ? (
    local.secondary_cloud == "aws" ? (
      local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].database_endpoint : ""
    ) : local.secondary_cloud == "azure" ? (
      local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].database_fqdn : ""
    ) : local.secondary_cloud == "gcp" ? (
      local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].database_connection_name : ""
    ) : ""
  ) : null
  
  database_name = "${local.project_name}_${local.environment}"
  database_username = "admin"
  
  # Primary cloud Kubernetes cluster name
  cluster_name = local.primary_cloud == "aws" ? (
    local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_name : ""
  ) : local.primary_cloud == "azure" ? (
    local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_name : ""
  ) : local.primary_cloud == "gcp" ? (
    local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_name : ""
  ) : ""
  
  # Secondary cloud Kubernetes cluster name (for DR)
  secondary_cluster_name = var.enable_multi_cloud ? (
    local.secondary_cloud == "aws" ? (
      local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_name : ""
    ) : local.secondary_cloud == "azure" ? (
      local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_name : ""
    ) : local.secondary_cloud == "gcp" ? (
      local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_name : ""
    ) : ""
  ) : null
  
  # Messaging endpoints
  rabbitmq_host = module.messaging_infrastructure.rabbitmq_endpoint
  kafka_bootstrap_servers = module.messaging_infrastructure.kafka_bootstrap_servers
  
  # Primary cloud network configuration
  vpc_id = local.primary_cloud == "aws" ? (
    local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].vpc_id : ""
  ) : local.primary_cloud == "azure" ? (
    local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].virtual_network_id : ""
  ) : local.primary_cloud == "gcp" ? (
    local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].vpc_id : ""
  ) : ""
  
  private_subnets = local.primary_cloud == "aws" ? (
    local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].private_subnet_ids : []
  ) : local.primary_cloud == "azure" ? (
    local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].private_subnet_ids : []
  ) : local.primary_cloud == "gcp" ? (
    local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].private_subnet_ids : []
  ) : []
  
  security_group_id = local.primary_cloud == "aws" ? (
    local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].security_group_id : ""
  ) : local.primary_cloud == "azure" ? (
    local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].network_security_group_id : ""
  ) : local.primary_cloud == "gcp" ? (
    local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].firewall_rule_id : ""
  ) : ""
  
  # Multi-cloud network mappings
  all_vpc_ids = {
    aws   = local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].vpc_id : null
    azure = local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].virtual_network_id : null
    gcp   = local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].vpc_id : null
  }
  
  all_cluster_names = {
    aws   = local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_name : null
    azure = local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_name : null
    gcp   = local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_name : null
  }
}

# ============================================================================
# INTELLIGENT SCALING AND PERFORMANCE OPTIMIZATION MODULE
# ============================================================================

module "intelligent_scaling" {
  count  = var.enable_intelligent_scaling ? 1 : 0
  source = "./modules/intelligent-scaling"

  project_name = local.project_name
  environment  = local.environment

  # Multi-cloud scaling configuration
  primary_cloud   = local.primary_cloud
  secondary_cloud = local.secondary_cloud

  # Kubernetes cluster configuration
  cluster_names = local.all_cluster_names

  # HPA Configuration with custom metrics
  enable_hpa                    = var.enable_hpa
  hpa_metrics_server_enabled   = true
  custom_metrics_enabled       = true

  # VPA Configuration
  enable_vpa                 = var.enable_vpa
  vpa_update_mode           = var.vpa_update_mode
  vpa_recommender_enabled   = true

  # Cluster Autoscaler Configuration
  enable_cluster_autoscaler          = var.enable_cluster_autoscaler
  cluster_autoscaler_scale_down_delay = var.cluster_autoscaler_scale_down_delay
  cluster_autoscaler_max_nodes       = var.cluster_autoscaler_max_nodes

  # Business Intelligence Scaling
  enable_business_aware_scaling = var.enable_business_aware_scaling
  business_metrics_endpoint     = var.business_metrics_endpoint
  revenue_growth_threshold      = var.revenue_growth_threshold
  customer_activity_threshold   = var.customer_activity_threshold

  # ML-driven Predictive Scaling
  enable_predictive_scaling        = var.enable_predictive_scaling
  predictive_scaling_model_endpoint = var.predictive_scaling_model_endpoint
  scaling_prediction_window_hours   = var.scaling_prediction_window_hours

  # Performance SLA Compliance
  enable_sla_compliance_scaling = var.enable_sla_compliance_scaling
  target_api_response_time_ms   = var.target_api_response_time_ms
  target_throughput_rps        = var.target_throughput_rps
  target_error_rate_percentage = var.target_error_rate_percentage

  # Cost Optimization
  enable_cost_aware_scaling = var.enable_cost_aware_scaling
  max_hourly_cost_usd      = var.max_hourly_cost_usd
  preferred_instance_types = var.preferred_instance_types
  spot_instance_percentage = var.spot_instance_percentage

  tags = local.common_tags
}

# ============================================================================
# ENTERPRISE SECURITY AND COMPLIANCE MODULE
# ============================================================================

module "enterprise_security" {
  count  = var.enable_enterprise_security ? 1 : 0
  source = "./modules/enterprise-security"

  project_name = local.project_name
  environment  = local.environment

  # Multi-cloud security configuration
  deploy_to_aws   = local.deploy_to_aws
  deploy_to_azure = local.deploy_to_azure
  deploy_to_gcp   = local.deploy_to_gcp

  # Zero Trust Architecture
  enable_zero_trust             = var.enable_zero_trust
  enable_service_mesh          = var.enable_service_mesh
  service_mesh_type           = var.service_mesh_type  # istio, linkerd, consul
  enable_mTLS                 = var.enable_mTLS

  # Advanced Threat Detection
  enable_falco_runtime_security = var.enable_falco_runtime_security
  enable_sysdig_security       = var.enable_sysdig_security
  enable_aqua_security         = var.enable_aqua_security

  # Data Loss Prevention (DLP)
  enable_dlp                  = var.enable_dlp
  dlp_policy_templates       = var.dlp_policy_templates
  sensitive_data_patterns    = var.sensitive_data_patterns

  # SIEM Integration
  enable_siem_integration    = var.enable_siem_integration
  siem_endpoint             = var.siem_endpoint
  siem_api_key              = var.siem_api_key

  # Compliance Frameworks
  compliance_frameworks = var.compliance_frameworks
  enable_sox_compliance = contains(var.compliance_frameworks, "SOX")
  enable_gdpr_compliance = contains(var.compliance_frameworks, "GDPR")
  enable_hipaa_compliance = contains(var.compliance_frameworks, "HIPAA")
  enable_pci_compliance = contains(var.compliance_frameworks, "PCI-DSS")

  # Kubernetes Security
  enable_pod_security_policies = var.enable_pod_security_policies
  enable_network_policies      = var.enable_network_policies
  enable_admission_controllers = var.enable_admission_controllers

  # Container Security
  enable_container_scanning    = var.enable_container_scanning
  container_registry_scanning = var.container_registry_scanning
  vulnerability_scan_schedule = var.vulnerability_scan_schedule

  # API Security
  enable_api_gateway_security = var.enable_api_gateway_security
  api_rate_limiting_enabled  = var.api_rate_limiting_enabled
  api_authentication_methods = var.api_authentication_methods

  # Secrets Management
  secrets_management_backend = var.secrets_management_backend
  enable_secret_rotation     = var.enable_secret_rotation
  secret_rotation_schedule   = var.secret_rotation_schedule

  tags = local.common_tags
}

# ============================================================================
# ADVANCED MONITORING AND OBSERVABILITY MODULE
# ============================================================================

module "advanced_monitoring" {
  count  = var.enable_advanced_monitoring ? 1 : 0
  source = "./modules/advanced-monitoring"

  project_name = local.project_name
  environment  = local.environment

  # Multi-cloud monitoring
  cluster_names = local.all_cluster_names

  # Prometheus Enhanced Configuration
  prometheus_federation_enabled   = var.prometheus_federation_enabled
  prometheus_remote_write_enabled = var.prometheus_remote_write_enabled
  prometheus_recording_rules      = var.prometheus_recording_rules

  # Grafana Enterprise Features
  grafana_enterprise_enabled     = var.grafana_enterprise_enabled
  grafana_alerting_enabled       = var.grafana_alerting_enabled
  grafana_image_rendering_enabled = var.grafana_image_rendering_enabled

  # Advanced Alerting
  enable_multi_channel_alerting = var.enable_multi_channel_alerting
  alerting_channels = {
    slack     = var.slack_webhook_url
    pagerduty = var.pagerduty_integration_key
    email     = var.alert_email_recipients
    teams     = var.teams_webhook_url
    webhook   = var.custom_webhook_url
  }

  # Business Intelligence Monitoring
  enable_business_metrics_monitoring = var.enable_business_metrics_monitoring
  business_kpi_dashboards           = var.business_kpi_dashboards
  executive_summary_reports         = var.executive_summary_reports

  # Distributed Tracing
  enable_jaeger_tracing       = var.enable_jaeger_tracing
  enable_zipkin_tracing      = var.enable_zipkin_tracing
  enable_opentelemetry       = var.enable_opentelemetry
  tracing_sampling_rate      = var.tracing_sampling_rate

  # Log Management
  enable_centralized_logging = var.enable_centralized_logging
  log_aggregation_backend   = var.log_aggregation_backend  # elasticsearch, loki, splunk
  log_retention_days        = var.log_retention_days

  # Machine Learning for Anomaly Detection
  enable_ml_anomaly_detection = var.enable_ml_anomaly_detection
  anomaly_detection_models   = var.anomaly_detection_models
  anomaly_alert_threshold    = var.anomaly_alert_threshold

  # SLI/SLO Monitoring
  enable_sli_slo_monitoring = var.enable_sli_slo_monitoring
  service_level_objectives = var.service_level_objectives
  error_budget_policy      = var.error_budget_policy

  # Performance APM
  enable_application_performance_monitoring = var.enable_application_performance_monitoring
  apm_agent_configuration                  = var.apm_agent_configuration
  code_profiling_enabled                   = var.code_profiling_enabled

  tags = local.common_tags
}

# ============================================================================
# DATA ENGINEERING PLATFORM MODULE
# ============================================================================

module "data_engineering_platform" {
  count  = var.enable_data_engineering_platform ? 1 : 0
  source = "./modules/data-engineering-platform"

  project_name = local.project_name
  environment  = local.environment

  # Multi-cloud data platform
  primary_cloud   = local.primary_cloud
  secondary_cloud = local.secondary_cloud

  # Apache Airflow Configuration
  enable_airflow                = var.enable_airflow
  airflow_version              = var.airflow_version
  airflow_executor_type        = var.airflow_executor_type  # kubernetes, celery, sequential
  airflow_worker_replicas      = var.airflow_worker_replicas
  airflow_scheduler_replicas   = var.airflow_scheduler_replicas

  # Apache Spark Configuration
  enable_spark                 = var.enable_spark
  spark_version               = var.spark_version
  spark_driver_memory         = var.spark_driver_memory
  spark_executor_memory       = var.spark_executor_memory
  spark_executor_instances    = var.spark_executor_instances
  spark_history_server_enabled = var.spark_history_server_enabled

  # Dagster Configuration
  enable_dagster              = var.enable_dagster
  dagster_version            = var.dagster_version
  dagster_workspace_enabled  = var.dagster_workspace_enabled
  dagster_daemon_enabled     = var.dagster_daemon_enabled

  # dbt Configuration
  enable_dbt                 = var.enable_dbt
  dbt_version               = var.dbt_version
  dbt_profiles_config       = var.dbt_profiles_config
  dbt_docs_enabled          = var.dbt_docs_enabled

  # Kafka Streaming Platform
  enable_kafka_platform       = var.enable_kafka_platform
  kafka_cluster_size          = var.kafka_cluster_size
  kafka_replication_factor    = var.kafka_replication_factor
  kafka_retention_hours       = var.kafka_retention_hours
  kafka_schema_registry_enabled = var.kafka_schema_registry_enabled
  kafka_connect_enabled       = var.kafka_connect_enabled

  # Data Quality and Validation
  enable_great_expectations   = var.enable_great_expectations
  enable_data_quality_monitoring = var.enable_data_quality_monitoring
  data_quality_thresholds    = var.data_quality_thresholds

  # Data Lineage and Catalog
  enable_apache_atlas        = var.enable_apache_atlas
  enable_datahub            = var.enable_datahub
  enable_amundsen           = var.enable_amundsen

  # Real-time Processing
  enable_flink              = var.enable_flink
  flink_task_managers       = var.flink_task_managers
  flink_slots_per_task_manager = var.flink_slots_per_task_manager

  # Database and Storage
  database_host             = local.database_host
  database_name             = local.database_name
  database_username         = local.database_username
  database_password         = random_password.db_admin_password.result

  # Data Lake Integration
  data_lake_buckets = var.enable_data_lake ? {
    aws   = local.deploy_to_aws ? try(module.data_lake[0].aws_bucket_name, null) : null
    azure = local.deploy_to_azure ? try(module.data_lake[0].azure_container_name, null) : null
    gcp   = local.deploy_to_gcp ? try(module.data_lake[0].gcp_bucket_name, null) : null
  } : {}

  tags = local.common_tags
}

# ============================================================================
# MACHINE LEARNING PLATFORM MODULE
# ============================================================================

module "ml_platform" {
  count  = var.enable_ml_platform ? 1 : 0
  source = "./modules/ml-platform"

  project_name = local.project_name
  environment  = local.environment

  # Multi-cloud ML platform
  primary_cloud = local.primary_cloud

  # MLflow Configuration
  enable_mlflow              = var.enable_mlflow
  mlflow_version            = var.mlflow_version
  mlflow_tracking_uri       = var.mlflow_tracking_uri
  mlflow_artifact_store     = var.mlflow_artifact_store

  # Kubeflow Configuration
  enable_kubeflow           = var.enable_kubeflow
  kubeflow_version         = var.kubeflow_version
  kubeflow_pipelines_enabled = var.kubeflow_pipelines_enabled
  kubeflow_katib_enabled   = var.kubeflow_katib_enabled

  # Jupyter Hub for Data Scientists
  enable_jupyterhub         = var.enable_jupyterhub
  jupyterhub_authenticator  = var.jupyterhub_authenticator
  jupyter_notebook_images   = var.jupyter_notebook_images

  # Model Serving
  enable_seldon_core        = var.enable_seldon_core
  enable_kserve            = var.enable_kserve
  model_serving_replicas   = var.model_serving_replicas

  # Feature Store
  enable_feast_feature_store = var.enable_feast_feature_store
  feast_online_store        = var.feast_online_store
  feast_offline_store       = var.feast_offline_store

  # GPU Support
  enable_gpu_nodes          = var.enable_gpu_nodes
  gpu_node_instance_types   = var.gpu_node_instance_types
  gpu_nodes_max_size       = var.gpu_nodes_max_size

  # Model Training Infrastructure
  enable_distributed_training = var.enable_distributed_training
  training_job_queue_size     = var.training_job_queue_size

  # Data Integration
  database_host = local.database_host
  data_lake_buckets = var.enable_data_lake ? {
    aws   = local.deploy_to_aws ? try(module.data_lake[0].aws_bucket_name, null) : null
    azure = local.deploy_to_azure ? try(module.data_lake[0].azure_container_name, null) : null
    gcp   = local.deploy_to_gcp ? try(module.data_lake[0].gcp_bucket_name, null) : null
  } : {}

  tags = local.common_tags
}

# ============================================================================
# MULTI-CLOUD NETWORKING AND CONNECTIVITY MODULE
# ============================================================================

module "multi_cloud_networking" {
  count  = var.enable_multi_cloud && var.enable_cross_cloud_connectivity ? 1 : 0
  source = "./modules/multi-cloud-networking"

  project_name = local.project_name
  environment  = local.environment

  # Cloud provider configuration
  deploy_to_aws   = local.deploy_to_aws
  deploy_to_azure = local.deploy_to_azure
  deploy_to_gcp   = local.deploy_to_gcp

  # VPC/VNet IDs from each cloud
  aws_vpc_id   = local.deploy_to_aws ? try(module.aws_infrastructure[0].vpc_id, null) : null
  azure_vnet_id = local.deploy_to_azure ? try(module.azure_infrastructure[0].virtual_network_id, null) : null
  gcp_vpc_id   = local.deploy_to_gcp ? try(module.gcp_infrastructure[0].vpc_id, null) : null

  # Cross-cloud VPN configuration
  enable_aws_azure_vpn    = var.enable_aws_azure_vpn
  enable_aws_gcp_vpn     = var.enable_aws_gcp_vpn
  enable_azure_gcp_vpn   = var.enable_azure_gcp_vpn

  # Global load balancing
  enable_global_load_balancer = var.enable_global_load_balancer
  global_lb_backend_services = {
    aws   = local.deploy_to_aws ? try(module.aws_infrastructure[0].load_balancer_arn, null) : null
    azure = local.deploy_to_azure ? try(module.azure_infrastructure[0].load_balancer_id, null) : null
    gcp   = local.deploy_to_gcp ? try(module.gcp_infrastructure[0].load_balancer_id, null) : null
  }

  # DNS and traffic management
  enable_multi_cloud_dns     = var.enable_multi_cloud_dns
  domain_name               = var.domain_name
  health_check_enabled      = var.health_check_enabled
  traffic_routing_policy    = var.traffic_routing_policy  # weighted, latency, geolocation

  # Network security
  enable_cross_cloud_firewall = var.enable_cross_cloud_firewall
  allowed_cross_cloud_ports  = var.allowed_cross_cloud_ports

  tags = local.common_tags
}