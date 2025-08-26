# Data Lake Module Outputs

# ============================================================================
# AWS OUTPUTS
# ============================================================================

output "aws_bucket_names" {
  description = "AWS S3 bucket names for data lake zones"
  value = var.deploy_to_aws ? {
    for idx, zone in ["bronze", "silver", "gold", "platinum"] :
    zone => aws_s3_bucket.data_lake[idx].bucket
  } : {}
}

output "aws_bucket_name" {
  description = "Primary AWS S3 bucket name (bronze zone)"
  value       = var.deploy_to_aws ? aws_s3_bucket.data_lake[0].bucket : null
}

output "aws_glue_catalog_databases" {
  description = "AWS Glue catalog database names"
  value = var.deploy_to_aws && var.enable_data_catalog ? {
    for idx, zone in ["bronze", "silver", "gold", "platinum"] :
    zone => aws_glue_catalog_database.data_lake[idx].name
  } : {}
}

# ============================================================================
# AZURE OUTPUTS
# ============================================================================

output "azure_storage_account" {
  description = "Azure storage account name"
  value       = var.deploy_to_azure ? azurerm_storage_account.data_lake[0].name : null
}

output "azure_storage_account_id" {
  description = "Azure storage account resource ID"
  value       = var.deploy_to_azure ? azurerm_storage_account.data_lake[0].id : null
}

output "azure_container_names" {
  description = "Azure Data Lake container names"
  value = var.deploy_to_azure ? {
    for idx, zone in ["bronze", "silver", "gold", "platinum"] :
    zone => azurerm_storage_container.data_zones[idx].name
  } : {}
}

output "azure_container_name" {
  description = "Primary Azure container name (bronze zone)"
  value       = var.deploy_to_azure ? azurerm_storage_container.data_zones[0].name : null
}

output "azure_primary_connection_string" {
  description = "Azure storage account primary connection string"
  value       = var.deploy_to_azure ? azurerm_storage_account.data_lake[0].primary_connection_string : null
  sensitive   = true
}

# ============================================================================
# GCP OUTPUTS
# ============================================================================

output "gcp_bucket_names" {
  description = "GCP storage bucket names for data lake zones"
  value = var.deploy_to_gcp ? {
    for idx, zone in ["bronze", "silver", "gold", "platinum"] :
    zone => google_storage_bucket.data_lake[idx].name
  } : {}
}

output "gcp_bucket_name" {
  description = "Primary GCP bucket name (bronze zone)"
  value       = var.deploy_to_gcp ? google_storage_bucket.data_lake[0].name : null
}

output "gcp_bucket_urls" {
  description = "GCP storage bucket URLs"
  value = var.deploy_to_gcp ? {
    for idx, zone in ["bronze", "silver", "gold", "platinum"] :
    zone => google_storage_bucket.data_lake[idx].url
  } : {}
}

# ============================================================================
# MULTI-CLOUD OUTPUTS
# ============================================================================

output "primary_bucket_name" {
  description = "Primary bucket name based on primary cloud"
  value = var.primary_cloud == "aws" ? (
    var.deploy_to_aws ? aws_s3_bucket.data_lake[0].bucket : null
  ) : var.primary_cloud == "azure" ? (
    var.deploy_to_azure ? azurerm_storage_account.data_lake[0].name : null
  ) : var.primary_cloud == "gcp" ? (
    var.deploy_to_gcp ? google_storage_bucket.data_lake[0].name : null
  ) : null
}

output "secondary_bucket_name" {
  description = "Secondary bucket name for disaster recovery"
  value = var.enable_cross_cloud_replication ? (
    var.primary_cloud == "aws" ? (
      var.deploy_to_azure ? azurerm_storage_account.data_lake[0].name : null
    ) : var.primary_cloud == "azure" ? (
      var.deploy_to_aws ? aws_s3_bucket.data_lake[0].bucket : null
    ) : var.primary_cloud == "gcp" ? (
      var.deploy_to_aws ? aws_s3_bucket.data_lake[0].bucket : null
    ) : null
  ) : null
}

output "all_data_lake_endpoints" {
  description = "All data lake endpoints across clouds"
  value = {
    aws = var.deploy_to_aws ? {
      for idx, zone in ["bronze", "silver", "gold", "platinum"] :
      zone => "s3://${aws_s3_bucket.data_lake[idx].bucket}"
    } : {}
    
    azure = var.deploy_to_azure ? {
      for idx, zone in ["bronze", "silver", "gold", "platinum"] :
      zone => "abfss://${azurerm_storage_container.data_zones[idx].name}@${azurerm_storage_account.data_lake[0].name}.dfs.core.windows.net/"
    } : {}
    
    gcp = var.deploy_to_gcp ? {
      for idx, zone in ["bronze", "silver", "gold", "platinum"] :
      zone => "gs://${google_storage_bucket.data_lake[idx].name}"
    } : {}
  }
}

# ============================================================================
# REPLICATION OUTPUTS
# ============================================================================

output "replication_configuration" {
  description = "Cross-cloud replication configuration"
  value = var.enable_cross_cloud_replication ? {
    enabled         = true
    primary_cloud   = var.primary_cloud
    replication_key = var.deploy_to_aws && var.enable_encryption ? aws_kms_key.replication[0].arn : null
  } : {
    enabled = false
  }
}

# ============================================================================
# MONITORING OUTPUTS
# ============================================================================

output "monitoring_configuration" {
  description = "Data lake monitoring configuration"
  value = {
    aws_alarms = var.deploy_to_aws ? {
      for idx, zone in ["bronze", "silver", "gold", "platinum"] :
      zone => aws_cloudwatch_metric_alarm.s3_bucket_size[idx].arn
    } : {}
    
    azure_alerts = var.deploy_to_azure ? [
      azurerm_monitor_metric_alert.storage_capacity[0].id
    ] : []
    
    data_catalog_enabled = var.enable_data_catalog
    data_lineage_enabled = var.enable_data_lineage
  }
}