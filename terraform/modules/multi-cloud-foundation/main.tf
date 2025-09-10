# Multi-Cloud Foundation Infrastructure
# Enterprise-grade cross-cloud deployment supporting all BMAD stories

terraform {
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
  }
}

# =============================================================================
# GLOBAL CONFIGURATION
# =============================================================================

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = var.owner
    CostCenter  = var.cost_center
    Created     = formatdate("YYYY-MM-DD", timestamp())
  }
  
  # Multi-cloud region mapping for global deployment
  regions = {
    primary = {
      aws   = var.aws_primary_region
      azure = var.azure_primary_region
      gcp   = var.gcp_primary_region
    }
    secondary = {
      aws   = var.aws_secondary_region
      azure = var.azure_secondary_region
      gcp   = var.gcp_secondary_region
    }
    tertiary = {
      aws   = var.aws_tertiary_region
      azure = var.azure_tertiary_region
      gcp   = var.gcp_tertiary_region
    }
  }
}

# =============================================================================
# AWS INFRASTRUCTURE
# =============================================================================

# AWS Primary Region Infrastructure
module "aws_primary" {
  source = "./aws"
  
  region = local.regions.primary.aws
  environment = var.environment
  
  # VPC Configuration
  vpc_cidr = var.aws_vpc_cidr
  availability_zones = var.aws_availability_zones
  
  # EKS Configuration
  cluster_name = "${var.project_name}-primary-${var.environment}"
  cluster_version = var.kubernetes_version
  
  # Node Groups for different workloads
  node_groups = {
    api_services = {
      instance_types = ["m5.xlarge", "m5.2xlarge"]
      scaling_config = {
        min_size     = 3
        max_size     = 50
        desired_size = 6
      }
      disk_size = 100
      labels = {
        workload = "api-services"
      }
    }
    data_processing = {
      instance_types = ["r5.4xlarge", "r5.8xlarge"]
      scaling_config = {
        min_size     = 2
        max_size     = 20
        desired_size = 4
      }
      disk_size = 200
      labels = {
        workload = "data-processing"
      }
    }
    ml_inference = {
      instance_types = ["c5.4xlarge", "c5.9xlarge"]
      scaling_config = {
        min_size     = 1
        max_size     = 15
        desired_size = 2
      }
      disk_size = 150
      labels = {
        workload = "ml-inference"
      }
      taints = [{
        key    = "workload"
        value  = "ml-inference"
        effect = "NO_SCHEDULE"
      }]
    }
  }
  
  # RDS Aurora PostgreSQL for OLTP workloads
  database_config = {
    engine_version    = "15.4"
    instance_class    = "db.r6g.xlarge"
    allocated_storage = 100
    max_allocated_storage = 1000
    backup_retention_period = 30
    backup_window = "03:00-04:00"
    maintenance_window = "sun:04:00-sun:05:00"
    multi_az = true
    encryption_at_rest = true
    performance_insights_enabled = true
  }
  
  # S3 Data Lake Configuration
  data_lake_config = {
    bronze_bucket = "${var.project_name}-bronze-${var.environment}-${random_id.bucket_suffix.hex}"
    silver_bucket = "${var.project_name}-silver-${var.environment}-${random_id.bucket_suffix.hex}"
    gold_bucket   = "${var.project_name}-gold-${var.environment}-${random_id.bucket_suffix.hex}"
    versioning_enabled = true
    lifecycle_enabled = true
    kms_encryption = true
  }
  
  # ElastiCache Redis for caching and real-time features
  redis_config = {
    node_type = "cache.r6g.large"
    num_cache_nodes = 3
    parameter_group_name = "default.redis7"
    engine_version = "7.0"
    port = 6379
    at_rest_encryption_enabled = true
    transit_encryption_enabled = true
  }
  
  # MSK (Managed Streaming for Apache Kafka)
  kafka_config = {
    kafka_version = "2.8.1"
    number_of_broker_nodes = 6
    instance_type = "kafka.m5.large"
    ebs_volume_size = 100
    encryption_in_transit = true
    encryption_at_rest = true
  }
  
  tags = local.common_tags
}

# AWS Secondary Region (Disaster Recovery)
module "aws_secondary" {
  source = "./aws"
  
  providers = {
    aws = aws.secondary
  }
  
  region = local.regions.secondary.aws
  environment = "${var.environment}-dr"
  
  # Reduced capacity for cost optimization
  vpc_cidr = var.aws_secondary_vpc_cidr
  availability_zones = var.aws_secondary_availability_zones
  
  cluster_name = "${var.project_name}-secondary-${var.environment}"
  cluster_version = var.kubernetes_version
  
  node_groups = {
    api_services = {
      instance_types = ["m5.large", "m5.xlarge"]
      scaling_config = {
        min_size     = 2
        max_size     = 30
        desired_size = 4
      }
      disk_size = 100
      labels = {
        workload = "api-services"
      }
    }
    data_processing = {
      instance_types = ["r5.2xlarge", "r5.4xlarge"]
      scaling_config = {
        min_size     = 1
        max_size     = 15
        desired_size = 2
      }
      disk_size = 200
      labels = {
        workload = "data-processing"
      }
    }
  }
  
  # Smaller database for DR
  database_config = {
    engine_version    = "15.4"
    instance_class    = "db.r6g.large"
    allocated_storage = 100
    max_allocated_storage = 500
    backup_retention_period = 7
    backup_window = "03:00-04:00"
    maintenance_window = "sun:04:00-sun:05:00"
    multi_az = false
    encryption_at_rest = true
    performance_insights_enabled = false
  }
  
  # Cross-region replication buckets
  data_lake_config = {
    bronze_bucket = "${var.project_name}-bronze-dr-${var.environment}-${random_id.bucket_suffix.hex}"
    silver_bucket = "${var.project_name}-silver-dr-${var.environment}-${random_id.bucket_suffix.hex}"
    gold_bucket   = "${var.project_name}-gold-dr-${var.environment}-${random_id.bucket_suffix.hex}"
    versioning_enabled = true
    lifecycle_enabled = true
    kms_encryption = true
    replication_enabled = true
    replication_source_region = local.regions.primary.aws
  }
  
  tags = local.common_tags
}

# =============================================================================
# AZURE INFRASTRUCTURE
# =============================================================================

module "azure_primary" {
  source = "./azure"
  
  location = local.regions.primary.azure
  environment = var.environment
  
  # Resource Group
  resource_group_name = "${var.project_name}-${var.environment}-primary-rg"
  
  # Virtual Network
  vnet_address_space = var.azure_vnet_address_space
  subnet_config = {
    aks = {
      address_prefixes = ["10.1.1.0/24"]
    }
    database = {
      address_prefixes = ["10.1.2.0/24"]
    }
    application_gateway = {
      address_prefixes = ["10.1.3.0/24"]
    }
  }
  
  # AKS Configuration
  cluster_name = "${var.project_name}-primary-${var.environment}"
  kubernetes_version = var.kubernetes_version
  
  # Node pools for different workloads
  node_pools = {
    system = {
      vm_size = "Standard_DS2_v2"
      node_count = 3
      min_count = 3
      max_count = 10
      enable_auto_scaling = true
      os_disk_size_gb = 100
    }
    api_services = {
      vm_size = "Standard_D4s_v3"
      node_count = 4
      min_count = 2
      max_count = 30
      enable_auto_scaling = true
      os_disk_size_gb = 100
      node_labels = {
        workload = "api-services"
      }
    }
    data_processing = {
      vm_size = "Standard_E8s_v3"
      node_count = 2
      min_count = 1
      max_count = 15
      enable_auto_scaling = true
      os_disk_size_gb = 200
      node_labels = {
        workload = "data-processing"
      }
    }
  }
  
  # Azure Database for PostgreSQL
  postgresql_config = {
    sku_name = "GP_Standard_D4s_v3"
    storage_mb = 102400
    backup_retention_days = 30
    geo_redundant_backup_enabled = true
    auto_grow_enabled = true
    ssl_enforcement_enabled = true
    ssl_minimal_tls_version_enforced = "TLS1_2"
  }
  
  # Azure Blob Storage for Data Lake
  storage_config = {
    account_tier = "Standard"
    account_replication_type = "GRS"
    is_hns_enabled = true # Hierarchical namespace for Data Lake Gen2
    containers = ["bronze", "silver", "gold"]
    lifecycle_management_enabled = true
  }
  
  # Azure Cache for Redis
  redis_config = {
    sku_name = "Standard"
    family = "C"
    capacity = 2
    enable_non_ssl_port = false
    minimum_tls_version = "1.2"
    redis_version = "6"
  }
  
  # Event Hubs for streaming
  eventhub_config = {
    sku = "Standard"
    capacity = 2
    hubs = {
      real_time_data = {
        partition_count = 8
        message_retention = 7
      }
      audit_logs = {
        partition_count = 4
        message_retention = 30
      }
    }
  }
  
  tags = local.common_tags
}

# =============================================================================
# GCP INFRASTRUCTURE
# =============================================================================

module "gcp_primary" {
  source = "./gcp"
  
  project_id = var.gcp_project_id
  region = local.regions.primary.gcp
  zone = var.gcp_primary_zone
  environment = var.environment
  
  # VPC Network
  network_name = "${var.project_name}-${var.environment}-network"
  subnet_config = {
    gke = {
      ip_range = "10.2.0.0/16"
      secondary_ranges = {
        pods = "10.3.0.0/16"
        services = "10.4.0.0/16"
      }
    }
    database = {
      ip_range = "10.2.1.0/24"
    }
  }
  
  # GKE Configuration
  cluster_name = "${var.project_name}-primary-${var.environment}"
  kubernetes_version = var.kubernetes_version
  
  # Node pools
  node_pools = {
    system = {
      machine_type = "e2-standard-4"
      node_count = 3
      min_node_count = 3
      max_node_count = 10
      disk_size_gb = 100
      auto_upgrade = true
      auto_repair = true
    }
    api_services = {
      machine_type = "n2-standard-4"
      node_count = 3
      min_node_count = 2
      max_node_count = 20
      disk_size_gb = 100
      auto_upgrade = true
      auto_repair = true
      node_labels = {
        workload = "api-services"
      }
    }
    data_processing = {
      machine_type = "n2-highmem-4"
      node_count = 2
      min_node_count = 1
      max_node_count = 15
      disk_size_gb = 200
      auto_upgrade = true
      auto_repair = true
      node_labels = {
        workload = "data-processing"
      }
    }
  }
  
  # Cloud SQL PostgreSQL
  database_config = {
    database_version = "POSTGRES_15"
    tier = "db-custom-4-16384"
    disk_size = 100
    disk_autoresize = true
    backup_enabled = true
    point_in_time_recovery_enabled = true
    availability_type = "REGIONAL"
    deletion_protection = true
  }
  
  # Cloud Storage for Data Lake
  storage_config = {
    location = "US"
    storage_class = "STANDARD"
    buckets = ["bronze", "silver", "gold"]
    versioning_enabled = true
    lifecycle_rules_enabled = true
    uniform_bucket_level_access = true
  }
  
  # Memorystore for Redis
  redis_config = {
    memory_size_gb = 4
    tier = "STANDARD_HA"
    redis_version = "REDIS_7_0"
    auth_enabled = true
    transit_encryption_mode = "SERVER_AUTHENTICATION"
    persistence_config = {
      persistence_mode = "RDB"
      rdb_snapshot_period = "TWELVE_HOURS"
    }
  }
  
  # Pub/Sub for messaging
  pubsub_config = {
    topics = ["real-time-data", "audit-logs", "notifications"]
    message_retention_duration = "604800s" # 7 days
    ack_deadline_seconds = 60
  }
  
  labels = local.common_tags
}

# =============================================================================
# GLOBAL RESOURCES
# =============================================================================

# Random ID for unique resource naming
resource "random_id" "bucket_suffix" {
  byte_length = 8
}

# Global DNS and CDN configuration
module "global_dns_cdn" {
  source = "./global-dns-cdn"
  
  project_name = var.project_name
  environment = var.environment
  
  # DNS configuration
  domain_name = var.domain_name
  subdomain_prefix = var.environment == "production" ? "api" : "${var.environment}-api"
  
  # CDN configuration
  cdn_config = {
    aws_cloudfront = {
      enabled = true
      price_class = "PriceClass_All"
      origins = {
        primary = module.aws_primary.alb_dns_name
        secondary = module.aws_secondary.alb_dns_name
      }
    }
    azure_cdn = {
      enabled = true
      sku = "Standard_Microsoft"
      origins = {
        primary = module.azure_primary.application_gateway_fqdn
      }
    }
    gcp_cdn = {
      enabled = true
      origins = {
        primary = module.gcp_primary.load_balancer_ip
      }
    }
  }
  
  # Global load balancing rules
  routing_rules = {
    default = {
      primary_cloud = "aws"
      fallback_clouds = ["azure", "gcp"]
      health_check_enabled = true
    }
    geographic = {
      us_east = { cloud = "aws", region = local.regions.primary.aws }
      us_west = { cloud = "aws", region = local.regions.secondary.aws }
      europe = { cloud = "azure", region = local.regions.primary.azure }
      asia = { cloud = "gcp", region = local.regions.primary.gcp }
    }
  }
  
  tags = local.common_tags
}

# Multi-cloud monitoring and observability
module "global_monitoring" {
  source = "./global-monitoring"
  
  project_name = var.project_name
  environment = var.environment
  
  # Prometheus and Grafana configuration
  prometheus_config = {
    retention = "30d"
    storage_size = "100Gi"
    replicas = 3
  }
  
  grafana_config = {
    admin_password = var.grafana_admin_password
    storage_size = "20Gi"
    replicas = 2
  }
  
  # Jaeger for distributed tracing
  jaeger_config = {
    storage_type = "elasticsearch"
    retention_days = 7
    replicas = 2
  }
  
  # Log aggregation
  log_aggregation = {
    elasticsearch_replicas = 3
    elasticsearch_storage = "100Gi"
    kibana_replicas = 2
  }
  
  # Multi-cloud integration
  cloud_integrations = {
    aws = {
      cloudwatch_enabled = true
      x_ray_enabled = true
    }
    azure = {
      monitor_enabled = true
      app_insights_enabled = true
    }
    gcp = {
      cloud_monitoring_enabled = true
      cloud_trace_enabled = true
    }
  }
  
  tags = local.common_tags
}

# Multi-cloud security and compliance
module "global_security" {
  source = "./global-security"
  
  project_name = var.project_name
  environment = var.environment
  
  # Certificate management
  certificate_config = {
    domain_name = var.domain_name
    sans = ["*.${var.domain_name}"]
    validation_method = "DNS"
    key_algorithm = "RSA_2048"
  }
  
  # Secrets management
  secrets_config = {
    vault_enabled = true
    external_secrets_enabled = true
    cloud_secrets_sync = {
      aws_secrets_manager = true
      azure_key_vault = true
      gcp_secret_manager = true
    }
  }
  
  # Network security
  network_security = {
    istio_enabled = true
    network_policies_enabled = true
    pod_security_standards = "restricted"
    admission_controllers = ["OPA", "Falco"]
  }
  
  # Compliance frameworks
  compliance_frameworks = ["SOC2", "GDPR", "HIPAA", "PCI-DSS"]
  
  tags = local.common_tags
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "aws_primary" {
  description = "AWS primary region infrastructure outputs"
  value = module.aws_primary
  sensitive = true
}

output "aws_secondary" {
  description = "AWS secondary region infrastructure outputs"
  value = module.aws_secondary
  sensitive = true
}

output "azure_primary" {
  description = "Azure primary region infrastructure outputs"
  value = module.azure_primary
  sensitive = true
}

output "gcp_primary" {
  description = "GCP primary region infrastructure outputs"
  value = module.gcp_primary
  sensitive = true
}

output "global_endpoints" {
  description = "Global access endpoints"
  value = {
    api_endpoint = module.global_dns_cdn.api_endpoint
    dashboard_endpoint = module.global_dns_cdn.dashboard_endpoint
    cdn_endpoints = module.global_dns_cdn.cdn_endpoints
  }
}

output "monitoring_endpoints" {
  description = "Monitoring and observability endpoints"
  value = {
    grafana_url = module.global_monitoring.grafana_url
    prometheus_url = module.global_monitoring.prometheus_url
    jaeger_url = module.global_monitoring.jaeger_url
    kibana_url = module.global_monitoring.kibana_url
  }
}

output "kubeconfig_commands" {
  description = "Commands to configure kubectl for each cluster"
  value = {
    aws_primary = module.aws_primary.kubeconfig_command
    aws_secondary = module.aws_secondary.kubeconfig_command
    azure_primary = module.azure_primary.kubeconfig_command
    gcp_primary = module.gcp_primary.kubeconfig_command
  }
}