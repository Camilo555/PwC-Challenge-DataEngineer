# Multi-Cloud Data Lake Module
# Implements Delta Lake/Iceberg architecture across AWS, Azure, and GCP

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
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Delta Lake configuration
  delta_table_formats = ["delta", "iceberg", "hudi"]
  
  # Storage tiers for lifecycle management
  storage_tiers = {
    hot     = "STANDARD"
    warm    = "STANDARD_IA"
    cold    = "GLACIER"
    archive = "DEEP_ARCHIVE"
  }
  
  # Data zones for medallion architecture
  data_zones = ["bronze", "silver", "gold", "platinum"]
}

# ============================================================================
# AWS DATA LAKE IMPLEMENTATION
# ============================================================================

# AWS S3 Buckets for Data Lake
resource "aws_s3_bucket" "data_lake" {
  count = var.deploy_to_aws ? length(local.data_zones) : 0
  
  bucket = "${local.name_prefix}-${local.data_zones[count.index]}-${random_string.bucket_suffix.result}"
  
  tags = merge(var.tags, {
    DataZone = local.data_zones[count.index]
    Cloud    = "aws"
    Purpose  = "data-lake"
  })
}

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "data_lake" {
  count = var.deploy_to_aws && var.enable_versioning ? length(local.data_zones) : 0
  
  bucket = aws_s3_bucket.data_lake[count.index].id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  count = var.deploy_to_aws && var.enable_encryption ? length(local.data_zones) : 0
  
  bucket = aws_s3_bucket.data_lake[count.index].id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# S3 Bucket Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  count = var.deploy_to_aws ? length(local.data_zones) : 0
  
  bucket = aws_s3_bucket.data_lake[count.index].id
  
  # Bronze data - transition to IA after 30 days, Glacier after 90 days
  rule {
    id     = "bronze_lifecycle"
    status = "Enabled"
    
    filter {
      prefix = "bronze/"
    }
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    transition {
      days          = var.data_retention_days - 365
      storage_class = "DEEP_ARCHIVE"
    }
    
    expiration {
      days = var.data_retention_days
    }
  }
  
  # Silver and Gold data - keep hot longer
  rule {
    id     = "processed_data_lifecycle"
    status = "Enabled"
    
    filter {
      prefix = "${local.data_zones[count.index] != "bronze" ? "${local.data_zones[count.index]}/" : "silver/"}"
    }
    
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }
}

# AWS Glue Data Catalog
resource "aws_glue_catalog_database" "data_lake" {
  count = var.deploy_to_aws && var.enable_data_catalog ? length(local.data_zones) : 0
  
  name = "${local.name_prefix}-${local.data_zones[count.index]}-catalog"
  description = "Data catalog for ${local.data_zones[count.index]} zone"
  
  parameters = {
    "classification" = "delta"
    "compressionType" = "gzip"
    "typeOfData" = "file"
  }
}

# ============================================================================
# AZURE DATA LAKE IMPLEMENTATION
# ============================================================================

# Azure Storage Account for Data Lake
resource "azurerm_storage_account" "data_lake" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                     = replace("${local.name_prefix}datalake${random_string.bucket_suffix.result}", "-", "")
  resource_group_name      = var.azure_resource_group_name
  location                = var.azure_location
  account_tier             = "Standard"
  account_replication_type = var.enable_cross_cloud_replication ? "GRS" : "LRS"
  account_kind            = "StorageV2"
  is_hns_enabled          = true  # Enable hierarchical namespace for Data Lake Gen2
  
  blob_properties {
    versioning_enabled = var.enable_versioning
    
    delete_retention_policy {
      days = 30
    }
    
    container_delete_retention_policy {
      days = 30
    }
  }
  
  tags = merge(var.tags, {
    Cloud   = "azure"
    Purpose = "data-lake"
  })
}

# Azure Data Lake Gen2 Containers
resource "azurerm_storage_container" "data_zones" {
  count = var.deploy_to_azure ? length(local.data_zones) : 0
  
  name                  = local.data_zones[count.index]
  storage_account_name  = azurerm_storage_account.data_lake[0].name
  container_access_type = "private"
}

# Azure Data Lake Analytics Account
resource "azurerm_data_lake_analytics_account" "main" {
  count = var.deploy_to_azure && var.enable_data_catalog ? 1 : 0
  
  name                = "${local.name_prefix}-dla"
  resource_group_name = var.azure_resource_group_name
  location           = var.azure_location
  
  default_store_account_name = azurerm_storage_account.data_lake[0].name
  
  tags = var.tags
}

# ============================================================================
# GCP DATA LAKE IMPLEMENTATION
# ============================================================================

# GCP Storage Buckets for Data Lake
resource "google_storage_bucket" "data_lake" {
  count = var.deploy_to_gcp ? length(local.data_zones) : 0
  
  name          = "${local.name_prefix}-${local.data_zones[count.index]}-${random_string.bucket_suffix.result}"
  location      = var.gcp_region
  storage_class = var.storage_class
  
  versioning {
    enabled = var.enable_versioning
  }
  
  dynamic "encryption" {
    for_each = var.enable_encryption ? [1] : []
    content {
      default_kms_key_name = var.gcp_kms_key_name
    }
  }
  
  lifecycle_rule {
    condition {
      age = 30
      matches_prefix = ["bronze/"]
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 90
      matches_prefix = ["bronze/"]
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 365
      matches_prefix = ["bronze/"]
    }
    action {
      type = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }
  
  labels = merge(var.tags, {
    data_zone = local.data_zones[count.index]
    cloud     = "gcp"
    purpose   = "data-lake"
  })
}

# GCP Data Catalog
resource "google_data_catalog_entry_group" "data_lake" {
  count = var.deploy_to_gcp && var.enable_data_catalog ? length(local.data_zones) : 0
  
  entry_group_id = "${local.name_prefix}-${local.data_zones[count.index]}-catalog"
  display_name   = "${title(local.data_zones[count.index])} Data Catalog"
  description    = "Data catalog for ${local.data_zones[count.index]} zone"
  
  region = var.gcp_region
}

# ============================================================================
# CROSS-CLOUD REPLICATION SETUP
# ============================================================================

# AWS to Azure Cross-Cloud Replication
resource "aws_s3_bucket_replication_configuration" "cross_cloud_azure" {
  count = var.deploy_to_aws && var.deploy_to_azure && var.enable_cross_cloud_replication ? length(local.data_zones) : 0
  
  depends_on = [aws_s3_bucket_versioning.data_lake]
  
  role   = aws_iam_role.replication[0].arn
  bucket = aws_s3_bucket.data_lake[count.index].id
  
  rule {
    id     = "cross_cloud_replication_azure"
    status = "Enabled"
    
    destination {
      bucket = "arn:aws:s3:::${azurerm_storage_account.data_lake[0].name}-replica"
      
      replica_kms_key_id = var.enable_encryption ? aws_kms_key.replication[0].arn : null
    }
  }
}

# Replication IAM Role
resource "aws_iam_role" "replication" {
  count = var.deploy_to_aws && var.enable_cross_cloud_replication ? 1 : 0
  
  name = "${local.name_prefix}-replication-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "s3.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

# KMS Key for Cross-Cloud Replication
resource "aws_kms_key" "replication" {
  count = var.deploy_to_aws && var.enable_encryption && var.enable_cross_cloud_replication ? 1 : 0
  
  description = "KMS key for cross-cloud data lake replication"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "Enable IAM User Permissions"
      Effect = "Allow"
      Principal = {
        AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
      }
      Action   = "kms:*"
      Resource = "*"
    }]
  })
  
  tags = var.tags
}

resource "aws_kms_alias" "replication" {
  count = var.deploy_to_aws && var.enable_encryption && var.enable_cross_cloud_replication ? 1 : 0
  
  name          = "alias/${local.name_prefix}-replication"
  target_key_id = aws_kms_key.replication[0].key_id
}

# ============================================================================
# DATA LAKE MONITORING AND ALERTING
# ============================================================================

# CloudWatch Alarms for AWS S3
resource "aws_cloudwatch_metric_alarm" "s3_bucket_size" {
  count = var.deploy_to_aws ? length(local.data_zones) : 0
  
  alarm_name          = "${local.name_prefix}-${local.data_zones[count.index]}-bucket-size"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "BucketSize"
  namespace           = "AWS/S3"
  period              = "86400"  # 24 hours
  statistic           = "Average"
  threshold           = "100000000000"  # 100GB in bytes
  alarm_description   = "This metric monitors S3 bucket size for ${local.data_zones[count.index]} zone"
  
  dimensions = {
    BucketName  = aws_s3_bucket.data_lake[count.index].bucket
    StorageType = "StandardStorage"
  }
  
  tags = var.tags
}

# Azure Monitor Alerts
resource "azurerm_monitor_metric_alert" "storage_capacity" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                = "${local.name_prefix}-storage-capacity-alert"
  resource_group_name = var.azure_resource_group_name
  scopes              = [azurerm_storage_account.data_lake[0].id]
  description         = "Alert when storage capacity exceeds threshold"
  
  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "UsedCapacity"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 100000000000  # 100GB
  }
  
  action {
    action_group_id = var.azure_action_group_id
  }
  
  tags = var.tags
}

# Data sources
data "aws_caller_identity" "current" {
  count = var.deploy_to_aws ? 1 : 0
}