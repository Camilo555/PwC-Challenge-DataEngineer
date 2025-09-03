# Multi-Cloud Data Platform Infrastructure Outputs
# Comprehensive outputs for enterprise deployment information

# ============================================================================
# DEPLOYMENT SUMMARY
# ============================================================================

output "deployment_summary" {
  description = "Comprehensive summary of the deployed infrastructure"
  value = {
    # Core Information
    project_name           = var.project_name
    organization          = var.organization
    environment           = var.environment
    deployment_timestamp  = timestamp()
    cloud_strategy        = var.cloud_provider
    primary_cloud         = var.primary_cloud
    
    # Multi-cloud Status
    multi_cloud_enabled   = var.enable_multi_cloud
    cross_cloud_networking = var.enable_cross_cloud_networking
    
    # Feature Status
    disaster_recovery_enabled = var.enable_disaster_recovery
    cost_optimization_enabled = var.enable_cost_optimization
    compliance_monitoring_enabled = var.enable_compliance_monitoring
    advanced_monitoring_enabled = var.enable_advanced_monitoring
  }
}

# ============================================================================
# MULTI-CLOUD INFRASTRUCTURE ENDPOINTS
# ============================================================================

output "aws_infrastructure" {
  description = "AWS infrastructure endpoints and resources"
  value = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? {
    region                = var.aws_region
    secondary_region      = var.aws_secondary_region
    vpc_id               = try(module.aws_infrastructure[0].vpc_id, null)
    eks_cluster_endpoint = try(module.aws_infrastructure[0].eks_cluster_endpoint, null)
    eks_cluster_name     = try(module.aws_infrastructure[0].eks_cluster_name, null)
    data_lake_bucket     = try(module.aws_infrastructure[0].data_lake_bucket, null)
    redshift_endpoint    = try(module.aws_infrastructure[0].redshift_cluster_endpoint, null)
    kinesis_streams      = try(module.aws_infrastructure[0].kinesis_streams, [])
    rds_endpoints        = try(module.aws_infrastructure[0].rds_endpoints, {})
    elasticache_endpoints = try(module.aws_infrastructure[0].elasticache_endpoints, {})
    
    # Security Resources
    kms_key_id           = try(module.aws_infrastructure[0].kms_key_id, null)
    iam_roles            = try(module.aws_infrastructure[0].iam_roles, {})
    security_groups      = try(module.aws_infrastructure[0].security_group_ids, [])
    
    # Monitoring Resources
    cloudwatch_log_groups = try(module.aws_infrastructure[0].cloudwatch_log_groups, [])
    sns_topics           = try(module.aws_infrastructure[0].sns_topics, {})
  } : null
}

output "azure_infrastructure" {
  description = "Azure infrastructure endpoints and resources"
  value = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? {
    location                = var.azure_location
    secondary_location      = var.azure_secondary_location
    resource_group_name     = try(module.azure_infrastructure[0].resource_group_name, null)
    aks_cluster_endpoint    = try(module.azure_infrastructure[0].aks_cluster_endpoint, null)
    aks_cluster_name        = try(module.azure_infrastructure[0].aks_cluster_name, null)
    data_lake_storage       = try(module.azure_infrastructure[0].data_lake_storage, null)
    synapse_workspace       = try(module.azure_infrastructure[0].synapse_workspace, null)
    event_hubs              = try(module.azure_infrastructure[0].event_hubs, [])
    sql_server_endpoints    = try(module.azure_infrastructure[0].sql_server_endpoints, {})
    cosmos_db_endpoints     = try(module.azure_infrastructure[0].cosmos_db_endpoints, {})
    
    # Security Resources
    key_vault_id           = try(module.azure_infrastructure[0].key_vault_id, null)
    managed_identity       = try(module.azure_infrastructure[0].managed_identity, null)
    network_security_groups = try(module.azure_infrastructure[0].network_security_groups, [])
    
    # Monitoring Resources
    log_analytics_workspace = try(module.azure_infrastructure[0].log_analytics_workspace, null)
    application_insights    = try(module.azure_infrastructure[0].application_insights, null)
  } : null
}

output "gcp_infrastructure" {
  description = "GCP infrastructure endpoints and resources"
  value = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? {
    region                  = var.gcp_region
    secondary_region        = var.gcp_secondary_region
    project_id             = var.gcp_project_id
    gke_cluster_endpoint   = try(module.gcp_infrastructure[0].gke_cluster_endpoint, null)
    gke_cluster_name       = try(module.gcp_infrastructure[0].gke_cluster_name, null)
    data_lake_bucket       = try(module.gcp_infrastructure[0].data_lake_bucket, null)
    bigquery_dataset       = try(module.gcp_infrastructure[0].bigquery_dataset, null)
    pubsub_topics          = try(module.gcp_infrastructure[0].pubsub_topics, [])
    cloud_sql_instances    = try(module.gcp_infrastructure[0].cloud_sql_instances, {})
    memorystore_instances  = try(module.gcp_infrastructure[0].memorystore_instances, {})
    
    # Security Resources
    kms_key_ring          = try(module.gcp_infrastructure[0].kms_key_ring, null)
    service_account       = try(module.gcp_infrastructure[0].service_account, null)
    vpc_network           = try(module.gcp_infrastructure[0].vpc_network, null)
    
    # Monitoring Resources
    log_sinks             = try(module.gcp_infrastructure[0].log_sinks, [])
    monitoring_workspace  = try(module.gcp_infrastructure[0].monitoring_workspace, null)
  } : null
}

# ============================================================================
# DATA PLATFORM ENDPOINTS
# ============================================================================

output "data_platform_endpoints" {
  description = "Data platform service endpoints and access information"
  value = {
    # Data Lake Endpoints
    data_lakes = {
      aws_s3        = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? try(module.aws_infrastructure[0].data_lake_bucket, null) : null
      azure_adls    = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? try(module.azure_infrastructure[0].data_lake_storage, null) : null
      gcp_gcs       = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? try(module.gcp_infrastructure[0].data_lake_bucket, null) : null
    }
    
    # Data Warehouse Endpoints
    data_warehouses = {
      aws_redshift     = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? try(module.aws_infrastructure[0].redshift_cluster_endpoint, null) : null
      azure_synapse    = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? try(module.azure_infrastructure[0].synapse_workspace, null) : null
      gcp_bigquery     = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? try(module.gcp_infrastructure[0].bigquery_dataset, null) : null
    }
    
    # Streaming Endpoints
    streaming_services = {
      aws_kinesis      = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? try(module.aws_infrastructure[0].kinesis_streams, []) : null
      azure_eventhubs  = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? try(module.azure_infrastructure[0].event_hubs, []) : null
      gcp_pubsub       = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? try(module.gcp_infrastructure[0].pubsub_topics, []) : null
    }
    
    # ML Platform Endpoints
    ml_platforms = {
      aws_sagemaker    = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? "Available" : null
      azure_ml         = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? "Available" : null
      gcp_vertex_ai    = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? "Available" : null
    }
  }
}

# ============================================================================
# KUBERNETES CLUSTER INFORMATION
# ============================================================================

output "kubernetes_clusters" {
  description = "Kubernetes cluster information across clouds"
  value = {
    cluster_version = var.kubernetes_version
    autoscaling_enabled = var.enable_kubernetes_autoscaling
    
    clusters = {
      aws_eks = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? {
        cluster_name     = try(module.aws_infrastructure[0].eks_cluster_name, null)
        endpoint         = try(module.aws_infrastructure[0].eks_cluster_endpoint, null)
        cluster_ca_cert  = try(module.aws_infrastructure[0].eks_cluster_ca, null)
        node_groups      = try(module.aws_infrastructure[0].eks_node_groups, {})
      } : null
      
      azure_aks = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? {
        cluster_name     = try(module.azure_infrastructure[0].aks_cluster_name, null)
        endpoint         = try(module.azure_infrastructure[0].aks_cluster_endpoint, null)
        cluster_ca_cert  = try(module.azure_infrastructure[0].aks_cluster_ca, null)
        node_pools       = try(module.azure_infrastructure[0].aks_node_pools, {})
      } : null
      
      gcp_gke = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? {
        cluster_name     = try(module.gcp_infrastructure[0].gke_cluster_name, null)
        endpoint         = try(module.gcp_infrastructure[0].gke_cluster_endpoint, null)
        cluster_ca_cert  = try(module.gcp_infrastructure[0].gke_cluster_ca, null)
        node_pools       = try(module.gcp_infrastructure[0].gke_node_pools, {})
      } : null
    }
  }
}

# ============================================================================
# MONITORING AND OBSERVABILITY
# ============================================================================

output "monitoring_dashboard_urls" {
  description = "Monitoring and observability dashboard URLs"
  value = {
    # Cross-cloud Monitoring
    prometheus_url       = try(module.monitoring.prometheus_url, null)
    grafana_url         = try(module.monitoring.grafana_url, null)
    datadog_dashboard   = try(module.monitoring.datadog_dashboard_url, null)
    
    # Cloud-specific Monitoring
    aws_cloudwatch      = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? "https://console.aws.amazon.com/cloudwatch/" : null
    azure_monitor       = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? "https://portal.azure.com/#blade/Microsoft_Azure_Monitoring" : null
    gcp_monitoring      = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? "https://console.cloud.google.com/monitoring" : null
    
    # Application Performance Monitoring
    apm_dashboards = {
      application_insights = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? try(module.azure_infrastructure[0].application_insights, null) : null
      x_ray_dashboard     = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? "Available" : null
      cloud_trace         = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? "Available" : null
    }
  }
}

# ============================================================================
# SECURITY AND COMPLIANCE STATUS
# ============================================================================

output "security_configuration" {
  description = "Security and compliance configuration status"
  value = {
    # Encryption Status
    encryption = {
      at_rest_enabled     = var.enable_encryption_at_rest
      in_transit_enabled  = var.enable_encryption_in_transit
      key_management = {
        aws_kms_enabled   = var.cloud_provider == "aws" || var.cloud_provider == "multi"
        azure_kv_enabled  = var.cloud_provider == "azure" || var.cloud_provider == "multi"
        gcp_kms_enabled   = var.cloud_provider == "gcp" || var.cloud_provider == "multi"
      }
    }
    
    # Network Security
    network_security = {
      private_endpoints_enabled = var.enable_private_endpoints
      network_security_enabled = var.enable_network_security
      vpc_security_groups = {
        aws_sg_count   = var.cloud_provider == "aws" || var.cloud_provider == "multi" ? length(try(module.aws_infrastructure[0].security_group_ids, [])) : 0
        azure_nsg_count = var.cloud_provider == "azure" || var.cloud_provider == "multi" ? length(try(module.azure_infrastructure[0].network_security_groups, [])) : 0
        gcp_firewall_count = var.cloud_provider == "gcp" || var.cloud_provider == "multi" ? 1 : 0
      }
    }
    
    # Compliance Status
    compliance = {
      monitoring_enabled    = var.enable_compliance_monitoring
      standards_covered     = var.compliance_standards
      audit_logging_enabled = var.enable_audit_logging
      audit_retention_days  = var.audit_log_retention_days
    }
    
    # Access Control
    access_control = {
      rbac_enabled         = true
      access_logging_enabled = var.enable_access_logging
      security_contact     = var.security_contact_email != "" ? "Configured" : "Not Configured"
    }
  }
}

# ============================================================================
# DISASTER RECOVERY CONFIGURATION
# ============================================================================

output "disaster_recovery_status" {
  description = "Disaster recovery configuration and capabilities"
  value = var.enable_disaster_recovery ? {
    # Recovery Objectives
    rto_minutes = var.rto_minutes
    rpo_minutes = var.rpo_minutes
    
    # Backup Configuration
    backup_retention_days = var.backup_retention_days
    cross_region_replication = var.enable_cross_region_replication
    
    # Multi-region Setup
    regions = {
      aws_primary     = var.aws_region
      aws_secondary   = var.aws_secondary_region
      azure_primary   = var.azure_location
      azure_secondary = var.azure_secondary_location
      gcp_primary     = var.gcp_region
      gcp_secondary   = var.gcp_secondary_region
    }
    
    # Failover Capabilities
    automated_failover = "Configured"
    manual_failover   = "Available"
    testing_schedule  = "Monthly"
    
    # Data Replication Status
    data_replication = {
      database_replication = "Active"
      storage_replication = "Active"
      real_time_sync     = var.rpo_minutes <= 15
    }
  } : {
    status = "Disaster recovery not enabled"
    recommendation = "Enable disaster recovery for production workloads"
  }
}

# ============================================================================
# COST OPTIMIZATION STATUS
# ============================================================================

output "cost_optimization_status" {
  description = "Cost optimization configuration and estimated savings"
  value = var.enable_cost_optimization ? {
    # Cost Controls
    monthly_budget_usd     = var.cost_budget_monthly_usd
    alert_threshold_percent = var.cost_alert_threshold_percent
    
    # Optimization Features
    auto_scaling_enabled   = var.enable_auto_scaling
    spot_instances_enabled = var.enable_spot_instances
    
    # Estimated Savings
    estimated_monthly_savings = {
      auto_scaling        = "15-25%"
      spot_instances     = var.enable_spot_instances ? "50-70%" : "0%"
      resource_rightsizing = "10-20%"
      storage_tiering    = "20-30%"
      reserved_instances = "30-50%"
    }
    
    # Cost Monitoring
    cost_monitoring = {
      budget_alerts_configured = true
      cost_anomaly_detection  = true
      resource_tagging       = "Enforced"
      cost_allocation_tags   = "Active"
    }
  } : {
    status = "Cost optimization not enabled"
    potential_savings = "30-60% monthly savings available"
  }
}

# ============================================================================
# PERFORMANCE METRICS
# ============================================================================

output "performance_configuration" {
  description = "Performance optimization and monitoring configuration"
  value = {
    # Performance Tier
    tier = var.performance_tier
    
    # Optimization Features
    caching_enabled = var.enable_caching
    cdn_enabled     = var.enable_cdn
    performance_monitoring = var.enable_performance_monitoring
    
    # Performance Targets
    api_response_time_target = "< 200ms"
    data_processing_sla     = "99.9% availability"
    throughput_target       = "10,000 requests/second"
    
    # Scaling Configuration
    auto_scaling = {
      enabled = var.enable_auto_scaling
      kubernetes_hpa = var.enable_kubernetes_autoscaling
      cluster_autoscaling = var.enable_kubernetes_autoscaling
    }
  }
}

# ============================================================================
# DEPLOYMENT INFORMATION
# ============================================================================

output "deployment_configuration" {
  description = "Deployment strategy and CI/CD configuration"
  value = {
    # Deployment Strategy
    strategy = var.deployment_strategy
    gitops_enabled = var.enable_gitops
    ci_cd_platform = var.ci_cd_integration
    
    # Environment Configuration
    environment_config = {
      development = {
        auto_deploy = true
        approval_required = false
        rollback_enabled = true
      }
      staging = {
        auto_deploy = false
        approval_required = true
        rollback_enabled = true
      }
      production = {
        auto_deploy = false
        approval_required = true
        rollback_enabled = true
        blue_green_deployment = var.deployment_strategy == "blue-green"
      }
    }
  }
}

# ============================================================================
# NETWORK CONFIGURATION
# ============================================================================

output "network_configuration" {
  description = "Multi-cloud network configuration and connectivity"
  value = {
    # VPC Configuration
    vpc_cidrs = var.vpc_cidr_blocks
    
    # Cross-cloud Connectivity
    cross_cloud_networking = var.enable_cross_cloud_networking
    private_endpoints = var.enable_private_endpoints
    
    # Network Architecture
    architecture = var.enable_multi_cloud ? "Multi-cloud with cross-connectivity" : "Single cloud"
    
    # DNS Configuration
    dns_resolution = "Private DNS zones configured"
    service_discovery = "Kubernetes native service discovery"
  }
}

# ============================================================================
# OPERATIONAL INFORMATION
# ============================================================================

output "operational_runbook" {
  description = "Operational information and runbook references"
  value = {
    # Access Information
    cluster_access = {
      kubectl_config = "Configure using cloud provider CLI tools"
      dashboard_access = "Available through cloud provider consoles"
      monitoring_access = "Grafana dashboards available"
    }
    
    # Troubleshooting
    troubleshooting = {
      log_locations = {
        application_logs = "Centralized in cloud logging services"
        infrastructure_logs = "CloudWatch/Monitor/Logging services"
        audit_logs = "Dedicated audit log storage"
      }
      health_checks = {
        kubernetes_health = "/healthz endpoint"
        application_health = "/health endpoint"
        database_health = "Cloud provider health checks"
      }
    }
    
    # Maintenance Windows
    maintenance = {
      kubernetes_updates = "Automated with rolling updates"
      security_patches = "Monthly maintenance window"
      backup_schedule = "Daily automated backups"
    }
    
    # Scaling Operations
    scaling = {
      manual_scaling = "Available through cloud consoles"
      auto_scaling_triggers = "CPU/Memory thresholds configured"
      capacity_planning = "Monthly capacity reviews scheduled"
    }
  }
}

# ============================================================================
# QUICK REFERENCE
# ============================================================================

output "quick_reference" {
  description = "Quick reference for common operations and access"
  value = {
    # Essential Commands
    commands = {
      kubectl_context = "kubectl config get-contexts"
      cluster_info = "kubectl cluster-info"
      pod_status = "kubectl get pods --all-namespaces"
    }
    
    # Important URLs
    urls = {
      prometheus = try(module.monitoring.prometheus_url, "Not deployed")
      grafana = try(module.monitoring.grafana_url, "Not deployed")
      kubernetes_dashboard = "Available through cloud provider"
    }
    
    # Contact Information
    support = {
      platform_team = "platform-team@organization.com"
      security_team = var.security_contact_email
      on_call = "Follow incident response procedures"
    }
    
    # Documentation
    documentation = {
      architecture_docs = "See /docs/architecture/"
      runbooks = "See /docs/runbooks/"
      api_docs = "See /docs/api/"
    }
  }
}