# PwC Retail Data Platform - Multi-Cloud Terraform Configuration
# Enterprise-grade infrastructure deployment for AWS, Azure, and GCP

terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  # Backend configuration (uncomment and configure for remote state)
  # backend "s3" {
  #   bucket         = "pwc-retail-terraform-state"
  #   key            = "infrastructure/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "pwc-retail-terraform-locks"
  #   encrypt        = true
  # }
}

# Local values for consistent naming and tagging
locals {
  project_name = "pwc-retail-platform"
  environment  = var.environment
  region       = var.region
  
  common_tags = {
    Project     = "PwC-Retail-Data-Platform"
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = "Data-Engineering-Team"
    CreatedAt   = timestamp()
  }
  
  # Resource naming convention
  name_prefix = "${local.project_name}-${local.environment}"
}

# Random password for database admin
resource "random_password" "db_admin_password" {
  length  = 32
  special = true
  upper   = true
  lower   = true
  numeric = true
}

# Store sensitive values in AWS Systems Manager Parameter Store
resource "aws_ssm_parameter" "db_admin_password" {
  count = var.cloud_provider == "aws" ? 1 : 0
  
  name  = "/${local.project_name}/${local.environment}/database/admin-password"
  type  = "SecureString"
  value = random_password.db_admin_password.result
  
  tags = local.common_tags
}

# AWS Infrastructure Module
module "aws_infrastructure" {
  count  = var.cloud_provider == "aws" ? 1 : 0
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
  count  = var.cloud_provider == "azure" ? 1 : 0
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
  count  = var.cloud_provider == "gcp" ? 1 : 0
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

# Kubernetes Application Deployment Module
module "kubernetes_apps" {
  source = "./modules/kubernetes"
  
  # Depends on cloud infrastructure being created first
  depends_on = [
    module.aws_infrastructure,
    module.azure_infrastructure,
    module.gcp_infrastructure
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
  cloud_provider = var.cloud_provider
  
  # Network Security
  allowed_cidr_blocks = var.allowed_cidr_blocks
  
  # Secrets Management
  secrets_backend = var.secrets_backend
  
  # Compliance Settings
  enable_encryption = var.enable_encryption
  enable_audit_logs = var.enable_audit_logs
  
  labels = local.common_tags
}

# Local values for cross-module references
locals {
  # Database connection details (varies by cloud provider)
  database_host = var.cloud_provider == "aws" ? (
    length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].database_endpoint : ""
  ) : var.cloud_provider == "azure" ? (
    length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].database_fqdn : ""
  ) : var.cloud_provider == "gcp" ? (
    length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].database_connection_name : ""
  ) : ""
  
  database_name = "${local.project_name}_${local.environment}"
  database_username = "admin"
  
  # Kubernetes cluster name (varies by cloud provider)
  cluster_name = var.cloud_provider == "aws" ? (
    length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_name : ""
  ) : var.cloud_provider == "azure" ? (
    length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_name : ""
  ) : var.cloud_provider == "gcp" ? (
    length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_name : ""
  ) : ""
}