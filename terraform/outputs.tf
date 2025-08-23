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

output "next_steps" {
  description = "Next steps after deployment"
  value = [
    "1. Configure kubectl: ${local.kubectl_config_command}",
    "2. Verify cluster access: kubectl get nodes",
    "3. Check application status: kubectl get pods -n ${local.project_name}-${var.environment}",
    "4. Access API: ${local.api_endpoint}",
    var.enable_monitoring ? "5. Access Grafana: ${local.grafana_endpoint}" : "",
    "6. View database connection details in sensitive outputs"
  ]
}

# Local values for computed outputs
locals {
  kubectl_config_command = var.cloud_provider == "aws" && length(module.aws_infrastructure) > 0 ? (
    "aws eks update-kubeconfig --region ${var.region} --name ${module.aws_infrastructure[0].cluster_name}"
  ) : var.cloud_provider == "azure" && length(module.azure_infrastructure) > 0 ? (
    "az aks get-credentials --resource-group ${module.azure_infrastructure[0].resource_group_name} --name ${module.azure_infrastructure[0].cluster_name}"
  ) : var.cloud_provider == "gcp" && length(module.gcp_infrastructure) > 0 ? (
    "gcloud container clusters get-credentials ${module.gcp_infrastructure[0].cluster_name} --region ${var.region} --project ${var.gcp_project_id}"
  ) : "Cluster not deployed"
  
  api_endpoint = "https://${var.environment}-api.${local.project_name}.com"
  grafana_endpoint = "https://${var.environment}-grafana.${local.project_name}.com"
}