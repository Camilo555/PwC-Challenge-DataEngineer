# BMAD Platform - Multi-Cloud Infrastructure as Code
# Enterprise-grade Terraform configuration for AWS, Azure, and GCP

terraform {
  required_version = ">= 1.5"
  
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
      version = "~> 4.80"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }

  # Remote state backend with encryption
  backend "s3" {
    bucket         = "bmad-terraform-state-prod"
    key            = "infrastructure/terraform.tfstate"
    region         = "us-west-2"
    encrypt        = true
    dynamodb_table = "bmad-terraform-locks"
    
    # Multi-cloud state replication
    versioning = true
  }
}

# Local variables for configuration
locals {
  project_name = "bmad-platform"
  environment  = var.environment
  
  # Multi-cloud regions
  aws_region     = var.aws_region
  azure_region   = var.azure_region
  gcp_region     = var.gcp_region
  
  # Common tags across all providers
  common_tags = {
    Project     = local.project_name
    Environment = local.environment
    ManagedBy   = "terraform"
    Owner       = "data-platform-team"
    CostCenter  = "data-engineering"
    Compliance  = "SOC2,GDPR,HIPAA"
  }
  
  # Kubernetes cluster configuration
  k8s_version = "1.28"
  node_count_min = 3
  node_count_max = 50
  
  # Network CIDR blocks
  vpc_cidr = {
    aws   = "10.0.0.0/16"
    azure = "10.1.0.0/16"
    gcp   = "10.2.0.0/16"
  }
}

# AWS Provider Configuration
provider "aws" {
  region = local.aws_region
  
  default_tags {
    tags = local.common_tags
  }
}

# Azure Provider Configuration
provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

# Google Cloud Provider Configuration
provider "google" {
  project = var.gcp_project_id
  region  = local.gcp_region
}

# AWS Infrastructure Module
module "aws_infrastructure" {
  source = "./modules/aws"
  
  # Core configuration
  project_name = local.project_name
  environment  = local.environment
  region       = local.aws_region
  
  # Network configuration
  vpc_cidr             = local.vpc_cidr.aws
  availability_zones   = ["${local.aws_region}a", "${local.aws_region}b", "${local.aws_region}c"]
  
  # EKS configuration
  kubernetes_version   = local.k8s_version
  node_group_min_size  = local.node_count_min
  node_group_max_size  = local.node_count_max
  node_group_desired_size = 5
  
  # Instance types for different workloads
  general_instance_types = ["m6i.large", "m6i.xlarge"]
  compute_instance_types = ["c6i.xlarge", "c6i.2xlarge"]
  memory_instance_types  = ["r6i.xlarge", "r6i.2xlarge"]
  
  # Data services
  enable_rds           = true
  enable_elasticache   = true
  enable_elasticsearch = true
  enable_s3_data_lake  = true
  
  # Security and compliance
  enable_kms           = true
  enable_secrets_manager = true
  enable_waf           = true
  enable_cloudtrail    = true
  
  tags = local.common_tags
}

# Azure Infrastructure Module
module "azure_infrastructure" {
  source = "./modules/azure"
  
  # Core configuration
  project_name        = local.project_name
  environment         = local.environment
  location           = local.azure_region
  
  # Network configuration
  vnet_address_space = [local.vpc_cidr.azure]
  
  # AKS configuration
  kubernetes_version     = local.k8s_version
  default_node_pool_size = {
    min_count = local.node_count_min
    max_count = local.node_count_max
    node_count = 5
  }
  
  # Node pools for different workloads
  enable_spot_node_pool    = true
  enable_gpu_node_pool     = false
  enable_memory_node_pool  = true
  
  # Data services
  enable_cosmos_db        = true
  enable_sql_database     = true
  enable_storage_account  = true
  enable_data_factory     = true
  
  # Security and compliance
  enable_key_vault       = true
  enable_application_gateway = true
  enable_firewall        = true
  
  tags = local.common_tags
}

# Google Cloud Infrastructure Module
module "gcp_infrastructure" {
  source = "./modules/gcp"
  
  # Core configuration
  project_id   = var.gcp_project_id
  project_name = local.project_name
  environment  = local.environment
  region       = local.gcp_region
  
  # Network configuration
  vpc_cidr = local.vpc_cidr.gcp
  
  # GKE configuration
  kubernetes_version    = local.k8s_version
  initial_node_count   = 3
  min_node_count       = local.node_count_min
  max_node_count       = local.node_count_max
  
  # Node pool configuration
  machine_types = {
    general = "e2-standard-4"
    compute = "c2-standard-8"
    memory  = "n2-highmem-4"
  }
  
  # Data services
  enable_cloud_sql       = true
  enable_memorystore     = true
  enable_bigquery        = true
  enable_cloud_storage   = true
  enable_dataflow        = true
  
  # Security and compliance
  enable_kms            = true
  enable_secret_manager = true
  enable_cloud_armor    = true
  
  labels = local.common_tags
}

# Multi-Cloud Data Lake Module
module "data_lake" {
  source = "./modules/data-lake"
  
  # Configuration
  project_name = local.project_name
  environment  = local.environment
  
  # Storage backends
  aws_s3_bucket      = module.aws_infrastructure.s3_data_lake_bucket
  azure_storage_account = module.azure_infrastructure.storage_account_name
  gcp_storage_bucket = module.gcp_infrastructure.data_lake_bucket
  
  # Data lake layers
  bronze_layer_enabled = true
  silver_layer_enabled = true
  gold_layer_enabled   = true
  
  # Data retention policies
  bronze_retention_days = 90
  silver_retention_days = 365
  gold_retention_days   = 2555  # 7 years
  
  # Security configuration
  enable_encryption = true
  enable_versioning = true
  enable_logging    = true
  
  tags = local.common_tags
}

# Global CDN Module for Mobile Story 4.1
module "global_cdn" {
  source = "./modules/cdn"
  
  # Configuration
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud origins
  aws_origin_domain    = module.aws_infrastructure.alb_dns_name
  azure_origin_domain  = module.azure_infrastructure.app_gateway_fqdn
  gcp_origin_domain    = module.gcp_infrastructure.load_balancer_ip
  
  # Mobile optimization for Story 4.1
  enable_mobile_optimization = true
  enable_image_optimization  = true
  enable_compression         = true
  
  # Global edge locations
  edge_locations = {
    us_east      = "us-east-1"
    us_west      = "us-west-1"
    europe       = "eu-west-1"
    asia_pacific = "ap-southeast-1"
  }
  
  # Performance configuration
  cache_behaviors = {
    api_cache_ttl    = 300     # 5 minutes
    static_cache_ttl = 86400   # 24 hours
    mobile_cache_ttl = 3600    # 1 hour
  }
  
  tags = local.common_tags
}

# Vector Database Infrastructure for AI Story 4.2
module "vector_database" {
  source = "./modules/vector-db"
  
  # Configuration
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud deployment
  deploy_aws   = true
  deploy_azure = true
  deploy_gcp   = true
  
  # Kubernetes clusters
  aws_cluster_name   = module.aws_infrastructure.eks_cluster_name
  azure_cluster_name = module.azure_infrastructure.aks_cluster_name
  gcp_cluster_name   = module.gcp_infrastructure.gke_cluster_name
  
  # Vector database configuration
  vector_dimension = 1536  # OpenAI embeddings
  index_type      = "HNSW"
  similarity_metric = "cosine"
  
  # Performance settings
  max_connections = 1000
  index_build_threads = 8
  search_threads = 16
  
  # Storage configuration
  storage_size = "1Ti"
  storage_class = "ssd"
  backup_enabled = true
  
  # Security
  enable_tls = true
  enable_auth = true
  
  tags = local.common_tags
}

# Monitoring and Observability Module
module "monitoring" {
  source = "./modules/monitoring"
  
  # Configuration
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud clusters
  kubernetes_clusters = {
    aws   = module.aws_infrastructure.eks_cluster_endpoint
    azure = module.azure_infrastructure.aks_cluster_endpoint
    gcp   = module.gcp_infrastructure.gke_cluster_endpoint
  }
  
  # Monitoring stack
  enable_prometheus = true
  enable_grafana   = true
  enable_jaeger    = true
  enable_datadog   = true
  
  # Log aggregation
  enable_elasticsearch = true
  enable_kibana       = true
  enable_logstash     = true
  
  # Alerting
  enable_alertmanager = true
  alert_channels = {
    slack    = var.slack_webhook_url
    pagerduty = var.pagerduty_integration_key
    email    = var.alert_email
  }
  
  tags = local.common_tags
}

# Security and Compliance Module
module "security" {
  source = "./modules/security"
  
  # Configuration
  project_name = local.project_name
  environment  = local.environment
  
  # Multi-cloud security
  aws_account_id   = data.aws_caller_identity.current.account_id
  azure_tenant_id  = data.azurerm_client_config.current.tenant_id
  gcp_project_id   = var.gcp_project_id
  
  # Security tools
  enable_falco          = true
  enable_trivy          = true
  enable_opa_gatekeeper = true
  enable_network_policies = true
  
  # Compliance frameworks
  enable_soc2_controls  = true
  enable_gdpr_controls  = true
  enable_hipaa_controls = true
  
  # Secret management
  aws_kms_key_id     = module.aws_infrastructure.kms_key_id
  azure_key_vault_id = module.azure_infrastructure.key_vault_id
  gcp_kms_key_id     = module.gcp_infrastructure.kms_key_id
  
  tags = local.common_tags
}

# Cost Optimization Module
module "cost_optimization" {
  source = "./modules/cost-optimization"
  
  # Configuration
  project_name = local.project_name
  environment  = local.environment
  
  # Target cost reduction: 40%
  cost_reduction_target = 0.40
  
  # Multi-cloud cost management
  aws_cost_budget    = var.aws_monthly_budget
  azure_cost_budget  = var.azure_monthly_budget
  gcp_cost_budget    = var.gcp_monthly_budget
  
  # Optimization strategies
  enable_spot_instances      = true
  enable_reserved_instances  = true
  enable_scheduled_scaling   = true
  enable_resource_tagging    = true
  
  # Auto-shutdown schedules
  non_prod_shutdown_schedule = "0 20 * * MON-FRI"  # 8 PM weekdays
  non_prod_startup_schedule  = "0 8 * * MON-FRI"   # 8 AM weekdays
  
  tags = local.common_tags
}

# Disaster Recovery Module
module "disaster_recovery" {
  source = "./modules/disaster-recovery"
  
  # Configuration
  project_name = local.project_name
  environment  = local.environment
  
  # SLA targets
  rto_target = 3600      # 1 hour Recovery Time Objective
  rpo_target = 900       # 15 minutes Recovery Point Objective
  availability_target = 99.99  # 99.99% availability
  
  # Multi-cloud backup strategy
  primary_cloud   = "aws"
  secondary_cloud = "azure"
  tertiary_cloud  = "gcp"
  
  # Backup configuration
  enable_cross_region_backup  = true
  enable_cross_cloud_backup   = true
  backup_retention_days       = 90
  
  # Replication settings
  enable_database_replication = true
  enable_storage_replication  = true
  enable_k8s_backup          = true
  
  tags = local.common_tags
}

# Data sources
data "aws_caller_identity" "current" {}
data "azurerm_client_config" "current" {}

# Outputs for reference
output "infrastructure_endpoints" {
  description = "Multi-cloud infrastructure endpoints"
  value = {
    aws = {
      eks_cluster_endpoint = module.aws_infrastructure.eks_cluster_endpoint
      rds_endpoint        = module.aws_infrastructure.rds_endpoint
      load_balancer_dns   = module.aws_infrastructure.alb_dns_name
    }
    azure = {
      aks_cluster_endpoint = module.azure_infrastructure.aks_cluster_endpoint
      sql_server_fqdn     = module.azure_infrastructure.sql_server_fqdn
      app_gateway_fqdn    = module.azure_infrastructure.app_gateway_fqdn
    }
    gcp = {
      gke_cluster_endpoint = module.gcp_infrastructure.gke_cluster_endpoint
      cloud_sql_ip        = module.gcp_infrastructure.cloud_sql_ip
      load_balancer_ip    = module.gcp_infrastructure.load_balancer_ip
    }
  }
  sensitive = true
}

output "data_lake_configuration" {
  description = "Multi-cloud data lake configuration"
  value = {
    aws_s3_bucket      = module.aws_infrastructure.s3_data_lake_bucket
    azure_storage_account = module.azure_infrastructure.storage_account_name
    gcp_storage_bucket = module.gcp_infrastructure.data_lake_bucket
    bronze_layer      = module.data_lake.bronze_layer_paths
    silver_layer      = module.data_lake.silver_layer_paths
    gold_layer        = module.data_lake.gold_layer_paths
  }
}

output "cdn_endpoints" {
  description = "Global CDN endpoints for mobile optimization"
  value = {
    global_endpoint     = module.global_cdn.global_endpoint
    regional_endpoints  = module.global_cdn.regional_endpoints
    mobile_optimized_urls = module.global_cdn.mobile_optimized_urls
  }
}

output "vector_database_endpoints" {
  description = "Vector database endpoints for AI Story 4.2"
  value = {
    aws_endpoint   = module.vector_database.aws_endpoint
    azure_endpoint = module.vector_database.azure_endpoint
    gcp_endpoint   = module.vector_database.gcp_endpoint
    api_keys      = module.vector_database.api_keys
  }
  sensitive = true
}

output "monitoring_dashboards" {
  description = "Monitoring and observability dashboard URLs"
  value = {
    prometheus = module.monitoring.prometheus_url
    grafana    = module.monitoring.grafana_url
    jaeger     = module.monitoring.jaeger_url
    kibana     = module.monitoring.kibana_url
  }
}

output "cost_optimization_summary" {
  description = "Cost optimization metrics and projections"
  value = {
    estimated_monthly_savings = module.cost_optimization.estimated_monthly_savings
    cost_reduction_percentage = module.cost_optimization.cost_reduction_percentage
    optimization_recommendations = module.cost_optimization.optimization_recommendations
  }
}