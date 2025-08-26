# Data Catalog Module Outputs

# ============================================================================
# AWS OUTPUTS
# ============================================================================

output "aws_glue_catalog_database" {
  description = "AWS Glue catalog database name"
  value       = var.deploy_to_aws ? aws_glue_catalog_database.main[0].name : null
}

output "aws_glue_crawler_name" {
  description = "AWS Glue crawler name"
  value       = var.deploy_to_aws && var.aws_data_lake_bucket != null ? aws_glue_crawler.data_lake_crawler[0].name : null
}

output "aws_data_quality_ruleset" {
  description = "AWS Glue data quality ruleset name"
  value       = var.deploy_to_aws && var.enable_data_quality_monitoring ? aws_glue_data_quality_ruleset.main[0].name : null
}

# ============================================================================
# AZURE OUTPUTS
# ============================================================================

output "azure_purview_account_name" {
  description = "Azure Purview account name"
  value       = var.deploy_to_azure ? azurerm_purview_account.main[0].name : null
}

output "azure_purview_account_id" {
  description = "Azure Purview account resource ID"
  value       = var.deploy_to_azure ? azurerm_purview_account.main[0].id : null
}

output "azure_purview_endpoint" {
  description = "Azure Purview catalog endpoint"
  value       = var.deploy_to_azure ? azurerm_purview_account.main[0].catalog_url : null
}

# ============================================================================
# GCP OUTPUTS
# ============================================================================

output "gcp_data_catalog_entry_group" {
  description = "GCP Data Catalog entry group name"
  value       = var.deploy_to_gcp ? google_data_catalog_entry_group.main[0].name : null
}

output "gcp_data_classification_template" {
  description = "GCP Data Catalog tag template for classification"
  value       = var.deploy_to_gcp ? google_data_catalog_tag_template.data_classification[0].name : null
}

output "gcp_pii_detection_job" {
  description = "GCP DLP job trigger for PII detection"
  value       = var.deploy_to_gcp && var.enable_pii_detection ? google_data_loss_prevention_job_trigger.pii_detection[0].name : null
}

output "gcp_pii_results_topic" {
  description = "GCP Pub/Sub topic for PII detection results"
  value       = var.deploy_to_gcp && var.enable_pii_detection ? google_pubsub_topic.pii_detection_results[0].name : null
}

# ============================================================================
# UNIFIED CATALOG OUTPUTS
# ============================================================================

output "atlas_namespace" {
  description = "Apache Atlas Kubernetes namespace"
  value       = var.deploy_unified_catalog ? kubernetes_namespace.atlas[0].metadata[0].name : null
}

output "atlas_service_name" {
  description = "Apache Atlas Kubernetes service name"
  value       = var.deploy_unified_catalog ? kubernetes_service.atlas[0].metadata[0].name : null
}

output "atlas_endpoint" {
  description = "Apache Atlas service endpoint"
  value = var.deploy_unified_catalog ? (
    "http://${kubernetes_service.atlas[0].metadata[0].name}.${kubernetes_namespace.atlas[0].metadata[0].name}.svc.cluster.local:21000"
  ) : null
}

# ============================================================================
# CATALOG ENDPOINTS
# ============================================================================

output "catalog_endpoint" {
  description = "Primary data catalog endpoint"
  value = var.deploy_unified_catalog ? (
    "http://${kubernetes_service.atlas[0].metadata[0].name}.${kubernetes_namespace.atlas[0].metadata[0].name}.svc.cluster.local:21000"
  ) : var.deploy_to_aws ? (
    "https://glue.${data.aws_region.current[0].name}.amazonaws.com"
  ) : var.deploy_to_azure ? (
    azurerm_purview_account.main[0].catalog_url
  ) : var.deploy_to_gcp ? (
    "https://datacatalog.googleapis.com"
  ) : null
}

output "all_catalog_endpoints" {
  description = "All data catalog endpoints across clouds"
  value = {
    unified = var.deploy_unified_catalog ? (
      "http://${kubernetes_service.atlas[0].metadata[0].name}.${kubernetes_namespace.atlas[0].metadata[0].name}.svc.cluster.local:21000"
    ) : null
    
    aws = var.deploy_to_aws ? (
      "https://glue.${data.aws_region.current[0].name}.amazonaws.com"
    ) : null
    
    azure = var.deploy_to_azure ? azurerm_purview_account.main[0].catalog_url : null
    
    gcp = var.deploy_to_gcp ? "https://datacatalog.googleapis.com" : null
  }
}

# ============================================================================
# DATA GOVERNANCE OUTPUTS
# ============================================================================

output "data_governance_configuration" {
  description = "Data governance and compliance configuration"
  value = {
    pii_detection_enabled     = var.enable_pii_detection
    data_quality_enabled      = var.enable_data_quality_monitoring
    unified_catalog_deployed  = var.deploy_unified_catalog
    
    compliance_features = {
      aws_data_quality_rules    = var.deploy_to_aws && var.enable_data_quality_monitoring
      gcp_dlp_pii_detection    = var.deploy_to_gcp && var.enable_pii_detection
      azure_purview_catalog    = var.deploy_to_azure
      unified_metadata_mgmt    = var.deploy_unified_catalog
    }
    
    integration_points = {
      aws_glue_integration     = var.deploy_to_aws && var.aws_data_lake_bucket != null
      azure_purview_integration = var.deploy_to_azure && var.azure_data_lake_account != null
      gcp_catalog_integration  = var.deploy_to_gcp && var.gcp_data_lake_bucket != null
    }
  }
}

# Data sources for dynamic outputs
data "aws_region" "current" {
  count = var.deploy_to_aws ? 1 : 0
}