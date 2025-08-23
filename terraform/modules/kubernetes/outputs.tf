# Kubernetes Module Outputs

# Namespace
output "namespace_name" {
  description = "Name of the Kubernetes namespace"
  value       = kubernetes_namespace.main.metadata[0].name
}

output "namespace_uid" {
  description = "UID of the Kubernetes namespace"
  value       = kubernetes_namespace.main.metadata[0].uid
}

# Service Account
output "service_account_name" {
  description = "Name of the service account"
  value       = kubernetes_service_account.app.metadata[0].name
}

# API Service
output "api_service_name" {
  description = "Name of the API service"
  value       = kubernetes_service.api.metadata[0].name
}

output "api_service_type" {
  description = "Type of the API service"
  value       = kubernetes_service.api.spec[0].type
}

output "api_load_balancer_ingress" {
  description = "Load balancer ingress for API service"
  value       = kubernetes_service.api.status[0].load_balancer[0].ingress
}

# Dagster Service
output "dagster_service_name" {
  description = "Name of the Dagster service"
  value       = kubernetes_service.dagster.metadata[0].name
}

output "dagster_service_cluster_ip" {
  description = "Cluster IP of the Dagster service"
  value       = kubernetes_service.dagster.spec[0].cluster_ip
}

# Deployments
output "api_deployment_name" {
  description = "Name of the API deployment"
  value       = kubernetes_deployment.api.metadata[0].name
}

output "etl_deployment_name" {
  description = "Name of the ETL deployment"
  value       = kubernetes_deployment.etl.metadata[0].name
}

output "dagster_deployment_name" {
  description = "Name of the Dagster deployment"
  value       = kubernetes_deployment.dagster.metadata[0].name
}

# Storage
output "storage_class_name" {
  description = "Name of the storage class"
  value       = kubernetes_storage_class.fast_ssd.metadata[0].name
}

output "persistent_volume_claim_name" {
  description = "Name of the persistent volume claim"
  value       = kubernetes_persistent_volume_claim.data_storage.metadata[0].name
}

# Secrets and ConfigMaps
output "database_secret_name" {
  description = "Name of the database secret"
  value       = kubernetes_secret.database.metadata[0].name
}

output "app_config_name" {
  description = "Name of the application config map"
  value       = kubernetes_config_map.app_config.metadata[0].name
}

# Jobs
output "dbt_job_name" {
  description = "Name of the dbt job"
  value       = kubernetes_job.dbt_run.metadata[0].name
}

output "dbt_cronjob_name" {
  description = "Name of the dbt scheduled cronjob"
  value       = kubernetes_cron_job_v1.dbt_scheduled.metadata[0].name
}

# Auto-scaling
output "api_hpa_name" {
  description = "Name of the API horizontal pod autoscaler"
  value       = kubernetes_horizontal_pod_autoscaler_v2.api.metadata[0].name
}

# Application Endpoints (internal cluster)
output "api_internal_endpoint" {
  description = "Internal cluster endpoint for API service"
  value       = "http://${kubernetes_service.api.metadata[0].name}.${kubernetes_namespace.main.metadata[0].name}.svc.cluster.local"
}

output "dagster_internal_endpoint" {
  description = "Internal cluster endpoint for Dagster service"
  value       = "http://${kubernetes_service.dagster.metadata[0].name}.${kubernetes_namespace.main.metadata[0].name}.svc.cluster.local:3000"
}

# Monitoring Annotations
output "prometheus_scrape_targets" {
  description = "Services configured for Prometheus scraping"
  value = {
    api = {
      scrape = "true"
      port   = "8000"
      path   = "/api/v1/metrics"
    }
  }
}

# Network Policies
output "api_network_policy_name" {
  description = "Name of the API network policy"
  value       = kubernetes_network_policy.api_network_policy.metadata[0].name
}

output "etl_network_policy_name" {
  description = "Name of the ETL network policy"
  value       = kubernetes_network_policy.etl_network_policy.metadata[0].name
}

# Resource Management
output "resource_quota_name" {
  description = "Name of the resource quota"
  value       = kubernetes_resource_quota.main.metadata[0].name
}

output "limit_range_name" {
  description = "Name of the limit range"
  value       = kubernetes_limit_range.main.metadata[0].name
}

# Elasticsearch
output "elasticsearch_service_name" {
  description = "Name of the Elasticsearch service"
  value       = kubernetes_service.elasticsearch.metadata[0].name
}

output "elasticsearch_deployment_name" {
  description = "Name of the Elasticsearch deployment"
  value       = kubernetes_deployment.elasticsearch.metadata[0].name
}

output "elasticsearch_pvc_name" {
  description = "Name of the Elasticsearch PVC"
  value       = kubernetes_persistent_volume_claim.elasticsearch_storage.metadata[0].name
}

output "elasticsearch_internal_endpoint" {
  description = "Internal cluster endpoint for Elasticsearch service"
  value       = "http://${kubernetes_service.elasticsearch.metadata[0].name}.${kubernetes_namespace.main.metadata[0].name}.svc.cluster.local:9200"
}