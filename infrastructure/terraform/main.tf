# Multi-Cloud Data Platform Infrastructure
# Supports AWS, Azure, and GCP deployment strategies

terraform {
  required_version = ">= 1.0"
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
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }

  backend "s3" {
    # Multi-region backend with cross-region replication
    bucket         = "pwc-terraform-state-${var.environment}"
    key            = "data-platform/terraform.tfstate"
    region         = var.aws_region
    encrypt        = true
    dynamodb_table = "pwc-terraform-locks"
    
    # Cross-region replication for disaster recovery
    versioning = true
  }
}

# Local variables for common configurations
locals {
  common_tags = {
    Project          = "PwC-DataEnginering-Challenge"
    Environment      = var.environment
    ManagedBy        = "Terraform"
    CostCenter      = "DataEngineering"
    SecurityLevel   = "High"
    ComplianceScope = "GDPR,SOX,PCI-DSS"
    CreatedBy       = "cloud-data-platform-agent"
    Timestamp       = timestamp()
  }

  # Environment-specific configurations
  environments = {
    development = {
      cluster_size        = "small"
      high_availability  = false
      auto_scaling       = false
      backup_retention   = 7
      monitoring_level   = "basic"
    }
    staging = {
      cluster_size        = "medium"
      high_availability  = true
      auto_scaling       = true
      backup_retention   = 30
      monitoring_level   = "enhanced"
    }
    production = {
      cluster_size        = "large"
      high_availability  = true
      auto_scaling       = true
      backup_retention   = 365
      monitoring_level   = "comprehensive"
    }
  }

  current_env_config = local.environments[var.environment]
  
  # Multi-cloud resource naming
  resource_prefix = "pwc-${var.environment}"
}

# Variables
variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "cloud_provider" {
  description = "Primary cloud provider (aws, azure, gcp, multi)"
  type        = string
  default     = "aws"
  validation {
    condition     = contains(["aws", "azure", "gcp", "multi"], var.cloud_provider)
    error_message = "Cloud provider must be aws, azure, gcp, or multi."
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "West US 2"
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-west1"
}

variable "enable_disaster_recovery" {
  description = "Enable disaster recovery across regions/clouds"
  type        = bool
  default     = false
}

variable "enable_cost_optimization" {
  description = "Enable cost optimization features"
  type        = bool
  default     = true
}

variable "enable_compliance_monitoring" {
  description = "Enable compliance monitoring"
  type        = bool
  default     = true
}

# Multi-cloud provider configurations
provider "aws" {
  count  = contains(["aws", "multi"], var.cloud_provider) ? 1 : 0
  region = var.aws_region
  
  default_tags {
    tags = local.common_tags
  }
}

provider "azurerm" {
  count = contains(["azure", "multi"], var.cloud_provider) ? 1 : 0
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

provider "google" {
  count   = contains(["gcp", "multi"], var.cloud_provider) ? 1 : 0
  region  = var.gcp_region
  project = var.gcp_project_id
}

# AWS Infrastructure
module "aws_infrastructure" {
  count  = contains(["aws", "multi"], var.cloud_provider) ? 1 : 0
  source = "./modules/aws"

  environment              = var.environment
  region                  = var.aws_region
  resource_prefix         = local.resource_prefix
  common_tags            = local.common_tags
  enable_disaster_recovery = var.enable_disaster_recovery
  enable_cost_optimization = var.enable_cost_optimization
  env_config             = local.current_env_config
}

# Azure Infrastructure
module "azure_infrastructure" {
  count  = contains(["azure", "multi"], var.cloud_provider) ? 1 : 0
  source = "./modules/azure"

  environment              = var.environment
  location                = var.azure_location
  resource_prefix         = local.resource_prefix
  common_tags            = local.common_tags
  enable_disaster_recovery = var.enable_disaster_recovery
  enable_cost_optimization = var.enable_cost_optimization
  env_config             = local.current_env_config
}

# GCP Infrastructure
module "gcp_infrastructure" {
  count  = contains(["gcp", "multi"], var.cloud_provider) ? 1 : 0
  source = "./modules/gcp"

  environment              = var.environment
  region                  = var.gcp_region
  project_id              = var.gcp_project_id
  resource_prefix         = local.resource_prefix
  common_tags            = local.common_tags
  enable_disaster_recovery = var.enable_disaster_recovery
  enable_cost_optimization = var.enable_cost_optimization
  env_config             = local.current_env_config
}

# Kubernetes Infrastructure (cloud-agnostic)
module "kubernetes_infrastructure" {
  source = "./modules/kubernetes"

  environment     = var.environment
  resource_prefix = local.resource_prefix
  common_tags     = local.common_tags
  env_config      = local.current_env_config
  
  # Cloud-specific cluster endpoint and credentials
  cluster_endpoint = var.cloud_provider == "aws" ? module.aws_infrastructure[0].eks_cluster_endpoint : (
    var.cloud_provider == "azure" ? module.azure_infrastructure[0].aks_cluster_endpoint : 
    module.gcp_infrastructure[0].gke_cluster_endpoint
  )
  
  cluster_ca_certificate = var.cloud_provider == "aws" ? module.aws_infrastructure[0].eks_cluster_ca : (
    var.cloud_provider == "azure" ? module.azure_infrastructure[0].aks_cluster_ca : 
    module.gcp_infrastructure[0].gke_cluster_ca
  )

  depends_on = [
    module.aws_infrastructure,
    module.azure_infrastructure,
    module.gcp_infrastructure
  ]
}

# Data Platform Components
module "data_platform" {
  source = "./modules/data-platform"

  environment              = var.environment
  cloud_provider          = var.cloud_provider
  resource_prefix         = local.resource_prefix
  common_tags            = local.common_tags
  env_config             = local.current_env_config
  enable_disaster_recovery = var.enable_disaster_recovery
  
  # Multi-cloud data sources
  aws_data_sources = var.cloud_provider != "aws" ? null : {
    s3_bucket           = module.aws_infrastructure[0].data_lake_bucket
    redshift_cluster    = module.aws_infrastructure[0].redshift_cluster_endpoint
    kinesis_streams     = module.aws_infrastructure[0].kinesis_streams
  }
  
  azure_data_sources = var.cloud_provider != "azure" ? null : {
    storage_account     = module.azure_infrastructure[0].data_lake_storage
    synapse_workspace   = module.azure_infrastructure[0].synapse_workspace
    event_hubs          = module.azure_infrastructure[0].event_hubs
  }
  
  gcp_data_sources = var.cloud_provider != "gcp" ? null : {
    storage_bucket      = module.gcp_infrastructure[0].data_lake_bucket
    bigquery_dataset    = module.gcp_infrastructure[0].bigquery_dataset
    pub_sub_topics      = module.gcp_infrastructure[0].pubsub_topics
  }

  depends_on = [
    module.kubernetes_infrastructure
  ]
}

# Monitoring and Observability
module "monitoring" {
  source = "./modules/monitoring"

  environment     = var.environment
  cloud_provider  = var.cloud_provider
  resource_prefix = local.resource_prefix
  common_tags     = local.common_tags
  env_config      = local.current_env_config
  
  # Cloud-specific monitoring integrations
  aws_cloudwatch_log_groups  = var.cloud_provider == "aws" ? module.aws_infrastructure[0].cloudwatch_log_groups : null
  azure_log_analytics        = var.cloud_provider == "azure" ? module.azure_infrastructure[0].log_analytics_workspace : null
  gcp_logging_project        = var.cloud_provider == "gcp" ? var.gcp_project_id : null

  depends_on = [
    module.data_platform
  ]
}

# Security and Compliance
module "security" {
  source = "./modules/security"

  environment                  = var.environment
  cloud_provider              = var.cloud_provider
  resource_prefix             = local.resource_prefix
  common_tags                = local.common_tags
  enable_compliance_monitoring = var.enable_compliance_monitoring
  
  # Multi-cloud security configurations
  aws_security_config = var.cloud_provider == "aws" ? {
    vpc_id              = module.aws_infrastructure[0].vpc_id
    security_group_ids  = module.aws_infrastructure[0].security_group_ids
    kms_key_id         = module.aws_infrastructure[0].kms_key_id
  } : null
  
  azure_security_config = var.cloud_provider == "azure" ? {
    resource_group_name = module.azure_infrastructure[0].resource_group_name
    vnet_id            = module.azure_infrastructure[0].vnet_id
    key_vault_id       = module.azure_infrastructure[0].key_vault_id
  } : null
  
  gcp_security_config = var.cloud_provider == "gcp" ? {
    vpc_network        = module.gcp_infrastructure[0].vpc_network
    service_account    = module.gcp_infrastructure[0].service_account
    kms_key_ring      = module.gcp_infrastructure[0].kms_key_ring
  } : null

  depends_on = [
    module.monitoring
  ]
}

# Outputs
output "infrastructure_summary" {
  description = "Summary of deployed infrastructure"
  value = {
    environment          = var.environment
    cloud_provider      = var.cloud_provider
    deployment_timestamp = timestamp()
    
    # Multi-cloud endpoints
    aws_endpoints = var.cloud_provider == "aws" ? {
      eks_cluster   = module.aws_infrastructure[0].eks_cluster_endpoint
      data_lake_s3  = module.aws_infrastructure[0].data_lake_bucket
      redshift      = module.aws_infrastructure[0].redshift_cluster_endpoint
    } : null
    
    azure_endpoints = var.cloud_provider == "azure" ? {
      aks_cluster      = module.azure_infrastructure[0].aks_cluster_endpoint
      data_lake_adls   = module.azure_infrastructure[0].data_lake_storage
      synapse         = module.azure_infrastructure[0].synapse_workspace
    } : null
    
    gcp_endpoints = var.cloud_provider == "gcp" ? {
      gke_cluster     = module.gcp_infrastructure[0].gke_cluster_endpoint
      data_lake_gcs   = module.gcp_infrastructure[0].data_lake_bucket
      bigquery        = module.gcp_infrastructure[0].bigquery_dataset
    } : null
    
    # Common platform endpoints
    monitoring_urls = {
      prometheus = module.monitoring.prometheus_url
      grafana    = module.monitoring.grafana_url
      datadog    = module.monitoring.datadog_dashboard_url
    }
    
    security_status = {
      encryption_enabled    = true
      compliance_monitoring = var.enable_compliance_monitoring
      zero_trust_network   = true
      security_dashboard   = module.security.security_dashboard_url
    }
  }
  sensitive = false
}

output "cost_optimization_status" {
  description = "Cost optimization features status"
  value = {
    enabled = var.enable_cost_optimization
    spot_instances_enabled = local.current_env_config.auto_scaling
    auto_scaling_enabled = local.current_env_config.auto_scaling
    storage_tiering_enabled = true
    cost_alerting_configured = true
  }
}

output "disaster_recovery_endpoints" {
  description = "Disaster recovery configuration"
  value = var.enable_disaster_recovery ? {
    primary_region = var.cloud_provider == "aws" ? var.aws_region : (
      var.cloud_provider == "azure" ? var.azure_location : var.gcp_region
    )
    backup_regions_configured = true
    rto_target_minutes = local.current_env_config.cluster_size == "large" ? 15 : 30
    rpo_target_minutes = local.current_env_config.cluster_size == "large" ? 5 : 15
  } : null
}