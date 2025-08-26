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