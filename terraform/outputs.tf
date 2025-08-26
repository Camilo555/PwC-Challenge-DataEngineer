# PwC Retail Data Platform - Terraform Outputs
# Essential outputs for connecting to deployed infrastructure

# ============================================================================
# GENERAL OUTPUTS
# ============================================================================

output "environment" {
  description = "Deployed environment"
  value       = var.environment
}

output "cloud_provider" {
  description = "Cloud provider used"
  value       = var.cloud_provider
}

output "region" {
  description = "Deployment region"
  value       = var.region
}

output "project_name" {
  description = "Project name"
  value       = local.project_name
}

# ============================================================================
# AWS OUTPUTS
# ============================================================================

output "aws_vpc_id" {
  description = "AWS VPC ID"
  value       = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].vpc_id : null
}

output "aws_eks_cluster_name" {
  description = "AWS EKS cluster name"
  value       = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_name : null
}

output "aws_eks_cluster_endpoint" {
  description = "AWS EKS cluster endpoint"
  value       = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_endpoint : null
  sensitive   = true
}

output "aws_rds_endpoint" {
  description = "AWS RDS database endpoint"
  value       = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].database_endpoint : null
  sensitive   = true
}

output "aws_rds_port" {
  description = "AWS RDS database port"
  value       = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].database_port : null
}

output "aws_s3_bucket_name" {
  description = "AWS S3 bucket for data storage"
  value       = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].s3_bucket_name : null
}

output "aws_ecr_repository_url" {
  description = "AWS ECR repository URL"
  value       = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].ecr_repository_url : null
}

# ============================================================================
# AZURE OUTPUTS
# ============================================================================

output "azure_resource_group_name" {
  description = "Azure resource group name"
  value       = var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].resource_group_name : null
}

output "azure_aks_cluster_name" {
  description = "Azure AKS cluster name"
  value       = var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_name : null
}

output "azure_aks_cluster_fqdn" {
  description = "Azure AKS cluster FQDN"
  value       = var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_fqdn : null
  sensitive   = true
}

output "azure_postgresql_fqdn" {
  description = "Azure PostgreSQL server FQDN"
  value       = var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].database_fqdn : null
  sensitive   = true
}

output "azure_postgresql_port" {
  description = "Azure PostgreSQL server port"
  value       = var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].database_port : null
}

output "azure_storage_account_name" {
  description = "Azure storage account name"
  value       = var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].storage_account_name : null
}

output "azure_container_registry_url" {
  description = "Azure Container Registry URL"
  value       = var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].acr_login_server : null
}

# ============================================================================
# GCP OUTPUTS
# ============================================================================

output "gcp_project_id" {
  description = "GCP project ID"
  value       = var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].project_id : null
}

output "gcp_gke_cluster_name" {
  description = "GCP GKE cluster name"
  value       = var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_name : null
}

output "gcp_gke_cluster_endpoint" {
  description = "GCP GKE cluster endpoint"
  value       = var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_endpoint : null
  sensitive   = true
}

output "gcp_sql_connection_name" {
  description = "GCP Cloud SQL connection name"
  value       = var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].database_connection_name : null
  sensitive   = true
}

output "gcp_sql_private_ip" {
  description = "GCP Cloud SQL private IP"
  value       = var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].database_private_ip : null
  sensitive   = true
}

output "gcp_storage_bucket_name" {
  description = "GCP storage bucket name"
  value       = var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].storage_bucket_name : null
}

output "gcp_container_registry_url" {
  description = "GCP Container Registry URL"
  value       = var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].container_registry_url : null
}

# ============================================================================
# DATABASE CONNECTION INFORMATION
# ============================================================================

output "database_host" {
  description = "Database host/endpoint"
  value       = local.database_host
  sensitive   = true
}

output "database_name" {
  description = "Database name"
  value       = local.database_name
}

output "database_username" {
  description = "Database username"
  value       = local.database_username
  sensitive   = true
}

output "database_port" {
  description = "Database port"
  value = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].database_port : (
    var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].database_port : (
      var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? 5432 : null
    )
  )
}

# ============================================================================
# KUBERNETES CLUSTER INFORMATION
# ============================================================================

output "cluster_name" {
  description = "Kubernetes cluster name"
  value       = local.cluster_name
}

output "cluster_endpoint" {
  description = "Kubernetes cluster endpoint"
  value = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_endpoint : (
    var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_fqdn : (
      var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_endpoint : null
    )
  )
  sensitive = true
}

output "cluster_ca_certificate" {
  description = "Kubernetes cluster CA certificate"
  value = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_ca_certificate : (
    var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_ca_certificate : (
      var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_ca_certificate : null
    )
  )
  sensitive = true
}

# ============================================================================
# APPLICATION ENDPOINTS
# ============================================================================

output "api_endpoint" {
  description = "API service endpoint"
  value       = "https://${var.environment}-api.${local.project_name}.com"
}

output "dagster_endpoint" {
  description = "Dagster UI endpoint"
  value       = "https://${var.environment}-dagster.${local.project_name}.com"
}

output "grafana_endpoint" {
  description = "Grafana dashboard endpoint"
  value       = var.enable_monitoring ? "https://${var.environment}-grafana.${local.project_name}.com" : null
}

output "prometheus_endpoint" {
  description = "Prometheus endpoint"
  value       = var.enable_monitoring ? "https://${var.environment}-prometheus.${local.project_name}.com" : null
}

# ============================================================================
# MONITORING AND OBSERVABILITY
# ============================================================================

output "monitoring_enabled" {
  description = "Whether monitoring is enabled"
  value       = var.enable_monitoring
}

output "logging_enabled" {
  description = "Whether centralized logging is enabled"
  value       = var.enable_logging
}

output "prometheus_storage_class" {
  description = "Storage class used for Prometheus"
  value       = var.enable_monitoring ? var.storage_class : null
}

# ============================================================================
# SECURITY INFORMATION
# ============================================================================

output "encryption_enabled" {
  description = "Whether encryption is enabled"
  value       = var.enable_encryption
}

output "audit_logs_enabled" {
  description = "Whether audit logging is enabled"
  value       = var.enable_audit_logs
}

output "secrets_backend" {
  description = "Secrets management backend in use"
  value       = var.secrets_backend
}

# ============================================================================
# CONNECTION COMMANDS
# ============================================================================

output "kubectl_config_command" {
  description = "Command to configure kubectl"
  value = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? (
    "aws eks update-kubeconfig --region ${var.region} --name ${module.aws_infrastructure[0].cluster_name}"
  ) : var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? (
    "az aks get-credentials --resource-group ${module.azure_infrastructure[0].resource_group_name} --name ${module.azure_infrastructure[0].cluster_name}"
  ) : var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? (
    "gcloud container clusters get-credentials ${module.gcp_infrastructure[0].cluster_name} --region ${var.region} --project ${var.gcp_project_id}"
  ) : "Cluster not deployed"
}

output "database_connection_string" {
  description = "Database connection string (without password)"
  value       = "postgresql://${local.database_username}@${local.database_host}:${local.database_port}/${local.database_name}"
  sensitive   = true
}

# ============================================================================
# DEPLOYMENT SUMMARY
# ============================================================================

output "deployment_summary" {
  description = "Deployment summary"
  value = {
    environment     = var.environment
    cloud_provider  = var.cloud_provider
    region         = var.region
    cluster_name   = local.cluster_name
    database_host  = local.database_host
    monitoring     = var.enable_monitoring
    logging        = var.enable_logging
    encryption     = var.enable_encryption
    deployed_at    = timestamp()
  }
}

# ============================================================================
# MULTI-CLOUD DATA PLATFORM OUTPUTS
# ============================================================================

# Data Lake Outputs
output "data_lake_configuration" {
  description = "Data lake configuration across clouds"
  value = var.enable_data_lake ? {
    enabled           = true
    primary_cloud     = local.primary_cloud
    secondary_cloud   = local.secondary_cloud
    buckets          = module.data_lake[0].all_data_lake_endpoints
    replication      = module.data_lake[0].replication_configuration
    monitoring       = module.data_lake[0].monitoring_configuration
  } : { enabled = false }
}

# Data Catalog Outputs
output "data_catalog_configuration" {
  description = "Data catalog and governance configuration"
  value = var.enable_data_catalog ? {
    enabled           = true
    unified_endpoint  = module.data_catalog[0].catalog_endpoint
    all_endpoints     = module.data_catalog[0].all_catalog_endpoints
    governance        = module.data_catalog[0].data_governance_configuration
  } : { enabled = false }
}

# Messaging Infrastructure Outputs
output "messaging_configuration" {
  description = "Messaging infrastructure configuration"
  value = {
    kafka_bootstrap_servers     = module.messaging_infrastructure.kafka_bootstrap_servers
    rabbitmq_endpoint          = module.messaging_infrastructure.rabbitmq_endpoint
    schema_registry_endpoint   = module.messaging_infrastructure.schema_registry_endpoint
    kafka_connect_endpoint     = module.messaging_infrastructure.kafka_connect_endpoint
    topics                     = module.messaging_infrastructure.kafka_topics
    monitoring                 = module.messaging_infrastructure.monitoring_configuration
    security                   = module.messaging_infrastructure.security_configuration
  }
}

# Cost Optimization Outputs
output "cost_optimization_configuration" {
  description = "Cost optimization and governance configuration"
  value = var.enable_cost_optimization ? {
    enabled                    = true
    budget_threshold          = var.budget_alert_threshold
    auto_shutdown_enabled     = var.auto_shutdown_enabled
    intelligent_scaling       = var.enable_intelligent_scaling
    spot_instances_enabled    = var.enable_spot_instances
    reserved_instance_target  = var.reserved_instance_coverage
  } : { enabled = false }
}

# Disaster Recovery Outputs
output "disaster_recovery_configuration" {
  description = "Disaster recovery configuration"
  value = var.enable_disaster_recovery ? {
    enabled         = true
    strategy        = var.rto_minutes <= 30 ? "active-active" : var.rto_minutes <= 120 ? "active-passive" : "backup-restore"
    rto_minutes     = var.rto_minutes
    rpo_minutes     = var.rpo_minutes
    primary_cloud   = local.primary_cloud
    secondary_cloud = local.secondary_cloud
    cross_cloud_replication = var.cross_cloud_replication
  } : { enabled = false }
}

# GitOps Configuration Outputs
output "gitops_configuration" {
  description = "GitOps and CI/CD configuration"
  value = var.enable_gitops ? {
    enabled                = true
    repository_url         = var.gitops_repo_url
    drift_detection        = var.enable_infrastructure_drift_detection
    automated_deployment   = true
    canary_deployment      = var.environment == "prod"
  } : { enabled = false }
}

# ============================================================================
# ENTERPRISE FEATURES SUMMARY
# ============================================================================

output "enterprise_features" {
  description = "Summary of enabled enterprise features"
  value = {
    multi_cloud_deployment     = var.enable_multi_cloud
    data_lake_enabled         = var.enable_data_lake
    data_catalog_enabled      = var.enable_data_catalog
    data_mesh_enabled         = var.enable_data_mesh
    cost_optimization_enabled = var.enable_cost_optimization
    disaster_recovery_enabled = var.enable_disaster_recovery
    gitops_enabled           = var.enable_gitops
    security_scanning_enabled = var.enable_security_scanning
    pii_detection_enabled    = var.enable_pii_detection
    intelligent_scaling      = var.enable_intelligent_scaling
    
    compliance_frameworks = var.compliance_frameworks
    data_retention_days   = var.data_retention_days
    backup_enabled       = var.enable_backup
    
    architecture_tier = var.rto_minutes <= 30 ? "tier1-mission-critical" : var.rto_minutes <= 240 ? "tier2-business-critical" : "tier3-standard"
  }
}

# ============================================================================
# PLATFORM HEALTH AND STATUS
# ============================================================================

output "platform_health" {
  description = "Platform health and status indicators"
  value = {
    deployment_status = "completed"
    health_checks = {
      kubernetes_cluster    = local.cluster_name != null && local.cluster_name != ""
      database_available   = local.database_host != null && local.database_host != ""
      messaging_ready      = module.messaging_infrastructure.kafka_bootstrap_servers != null
      monitoring_active    = var.enable_monitoring
      security_hardened    = var.enable_encryption && var.enable_audit_logs
    }
    
    resource_counts = {
      clouds_deployed    = length([for cloud in ["aws", "azure", "gcp"] : cloud if (cloud == "aws" && local.deploy_to_aws) || (cloud == "azure" && local.deploy_to_azure) || (cloud == "gcp" && local.deploy_to_gcp)])
      data_lake_zones   = var.enable_data_lake ? 4 : 0  # bronze, silver, gold, platinum
      kafka_topics      = length(module.messaging_infrastructure.kafka_topic_names)
      compliance_frameworks = length(var.compliance_frameworks)
    }
    
    deployed_at = timestamp()
    terraform_version = "1.6+"
  }
}

output "next_steps" {
  description = "Next steps after deployment"
  value = [
    "1. Configure kubectl: ${local.kubectl_config_command}",
    "2. Verify cluster access: kubectl get nodes",
    "3. Check application status: kubectl get pods -n ${local.project_name}-${var.environment}",
    "4. Access API: ${local.api_endpoint}",
    var.enable_monitoring ? "5. Access Grafana: ${local.grafana_endpoint}" : "",
    "6. View database connection details in sensitive outputs",
    var.enable_data_lake ? "7. Verify data lake buckets are accessible" : "",
    var.enable_data_catalog ? "8. Access data catalog for metadata management" : "",
    var.enable_disaster_recovery ? "9. Test disaster recovery procedures" : "",
    "10. Review cost optimization recommendations"
  ]
}

# ============================================================================
# QUICK ACCESS COMMANDS
# ============================================================================

output "quick_access_commands" {
  description = "Quick access commands for common operations"
  value = {
    kubectl_config = local.kubectl_config_command
    
    database_connect = "psql postgresql://${local.database_username}:<password>@${local.database_host}:5432/${local.database_name}"
    
    kafka_console_consumer = var.enable_data_lake ? (
      "kubectl exec -it deployment/${var.project_name}-${var.environment}-kafka-kafka-0 -n ${var.project_name}-kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${var.kafka_topic_prefix}-sales-events"
    ) : "Kafka not deployed"
    
    data_lake_access = var.enable_data_lake ? {
      aws   = local.deploy_to_aws ? "aws s3 ls s3://${try(module.data_lake[0].aws_bucket_name, "not-deployed")}" : null
      azure = local.deploy_to_azure ? "az storage blob list --account-name ${try(module.data_lake[0].azure_storage_account, "not-deployed")} --container-name bronze" : null
      gcp   = local.deploy_to_gcp ? "gsutil ls gs://${try(module.data_lake[0].gcp_bucket_name, "not-deployed")}" : null
    } : null
    
    monitoring_access = var.enable_monitoring ? {
      prometheus = "kubectl port-forward svc/prometheus-server 9090:80 -n monitoring"
      grafana    = "kubectl port-forward svc/grafana 3000:80 -n monitoring"
    } : null
  }
}

# Local values for computed outputs
locals {
  kubectl_config_command = local.primary_cloud == "aws" && local.deploy_to_aws && length(module.aws_infrastructure) > 0 ? (
    "aws eks update-kubeconfig --region ${var.region} --name ${module.aws_infrastructure[0].cluster_name}"
  ) : local.primary_cloud == "azure" && local.deploy_to_azure && length(module.azure_infrastructure) > 0 ? (
    "az aks get-credentials --resource-group ${module.azure_infrastructure[0].resource_group_name} --name ${module.azure_infrastructure[0].cluster_name}"
  ) : local.primary_cloud == "gcp" && local.deploy_to_gcp && length(module.gcp_infrastructure) > 0 ? (
    "gcloud container clusters get-credentials ${module.gcp_infrastructure[0].cluster_name} --region ${var.region} --project ${var.gcp_project_id}"
  ) : "Cluster not deployed"
  
  api_endpoint = "https://${var.environment}-api.${local.project_name}.com"
  grafana_endpoint = "https://${var.environment}-grafana.${local.project_name}.com"
}