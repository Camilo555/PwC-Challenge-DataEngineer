# Cross-Cloud Data Replication and Backup Module
# Enterprise-grade data replication across AWS, Azure, and GCP

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
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
  }
}

# ============================================================================
# AWS DATA REPLICATION INFRASTRUCTURE
# ============================================================================

# Primary S3 bucket with versioning and cross-region replication
resource "aws_s3_bucket" "primary_data_lake" {
  count  = var.enable_aws_replication ? 1 : 0
  bucket = "${var.project_name}-primary-data-lake-${var.environment}"

  tags = merge(var.common_tags, {
    Purpose = "PrimaryDataLake"
    Tier    = "Critical"
  })
}

resource "aws_s3_bucket_versioning" "primary_data_lake" {
  count  = var.enable_aws_replication ? 1 : 0
  bucket = aws_s3_bucket.primary_data_lake[0].id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "primary_data_lake" {
  count  = var.enable_aws_replication ? 1 : 0
  bucket = aws_s3_bucket.primary_data_lake[0].id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_replication[0].arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

# Cross-region replication bucket
resource "aws_s3_bucket" "replica_data_lake" {
  count    = var.enable_aws_replication ? 1 : 0
  provider = aws.replica_region
  bucket   = "${var.project_name}-replica-data-lake-${var.environment}"

  tags = merge(var.common_tags, {
    Purpose = "ReplicaDataLake"
    Tier    = "Critical"
  })
}

resource "aws_s3_bucket_versioning" "replica_data_lake" {
  count    = var.enable_aws_replication ? 1 : 0
  provider = aws.replica_region
  bucket   = aws_s3_bucket.replica_data_lake[0].id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# KMS key for encryption
resource "aws_kms_key" "data_replication" {
  count                   = var.enable_aws_replication ? 1 : 0
  description             = "KMS key for data replication encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = merge(var.common_tags, {
    Purpose = "DataReplication"
  })
}

resource "aws_kms_alias" "data_replication" {
  count         = var.enable_aws_replication ? 1 : 0
  name          = "alias/${var.project_name}-data-replication"
  target_key_id = aws_kms_key.data_replication[0].key_id
}

# Cross-region replication configuration
resource "aws_s3_bucket_replication_configuration" "primary_to_replica" {
  count      = var.enable_aws_replication ? 1 : 0
  depends_on = [aws_s3_bucket_versioning.primary_data_lake]

  role   = aws_iam_role.replication[0].arn
  bucket = aws_s3_bucket.primary_data_lake[0].id

  rule {
    id     = "replicate-all-objects"
    status = "Enabled"

    filter {
      prefix = ""
    }

    destination {
      bucket        = aws_s3_bucket.replica_data_lake[0].arn
      storage_class = "STANDARD_IA"

      encryption_configuration {
        replica_kms_key_id = aws_kms_key.data_replication[0].arn
      }

      metrics {
        status = "Enabled"
        event_threshold {
          minutes = 15
        }
      }

      replication_time {
        status = "Enabled"
        time {
          minutes = 15
        }
      }
    }

    delete_marker_replication {
      status = "Enabled"
    }
  }
}

# IAM role for S3 replication
resource "aws_iam_role" "replication" {
  count = var.enable_aws_replication ? 1 : 0
  name  = "${var.project_name}-s3-replication-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
      }
    ]
  })

  tags = var.common_tags
}

resource "aws_iam_policy" "replication" {
  count = var.enable_aws_replication ? 1 : 0
  name  = "${var.project_name}-s3-replication-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObjectVersion",
          "s3:GetObjectVersionAcl",
          "s3:GetObjectVersionTagging"
        ]
        Resource = "${aws_s3_bucket.primary_data_lake[0].arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObjectVersion"
        ]
        Resource = aws_s3_bucket.primary_data_lake[0].arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ReplicateObject",
          "s3:ReplicateDelete",
          "s3:ReplicateTags"
        ]
        Resource = "${aws_s3_bucket.replica_data_lake[0].arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.data_replication[0].arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "replication" {
  count      = var.enable_aws_replication ? 1 : 0
  role       = aws_iam_role.replication[0].name
  policy_arn = aws_iam_policy.replication[0].arn
}

# DynamoDB Global Tables for metadata replication
resource "aws_dynamodb_table" "metadata_primary" {
  count          = var.enable_aws_replication ? 1 : 0
  name           = "${var.project_name}-metadata-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "object_key"
  range_key      = "version_id"
  stream_enabled = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "object_key"
    type = "S"
  }

  attribute {
    name = "version_id"
    type = "S"
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.data_replication[0].arn
  }

  point_in_time_recovery {
    enabled = true
  }

  replica {
    region_name = var.aws_replica_region
    kms_key_arn = aws_kms_key.data_replication[0].arn
  }

  tags = merge(var.common_tags, {
    Purpose = "MetadataReplication"
  })
}

# ============================================================================
# AZURE DATA REPLICATION INFRASTRUCTURE
# ============================================================================

# Azure Storage Account with geo-redundancy
resource "azurerm_storage_account" "primary_data_lake" {
  count                    = var.enable_azure_replication ? 1 : 0
  name                     = "${replace(var.project_name, "-", "")}primarydl${var.environment}"
  resource_group_name      = var.azure_resource_group_name
  location                = var.azure_location
  account_tier             = "Standard"
  account_replication_type = "RAGRS"  # Read-access geo-redundant storage
  account_kind            = "StorageV2"
  is_hns_enabled          = true  # Enable hierarchical namespace for Data Lake

  blob_properties {
    versioning_enabled  = true
    change_feed_enabled = true
    
    delete_retention_policy {
      days = 30
    }
    
    container_delete_retention_policy {
      days = 30
    }
  }

  tags = merge(var.common_tags, {
    Purpose = "PrimaryDataLake"
  })
}

# Azure Data Lake containers
resource "azurerm_storage_container" "bronze_layer" {
  count                 = var.enable_azure_replication ? 1 : 0
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.primary_data_lake[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver_layer" {
  count                 = var.enable_azure_replication ? 1 : 0
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.primary_data_lake[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold_layer" {
  count                 = var.enable_azure_replication ? 1 : 0
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.primary_data_lake[0].name
  container_access_type = "private"
}

# Azure Synapse Analytics for data warehousing with backup
resource "azurerm_synapse_workspace" "main" {
  count                                = var.enable_azure_replication ? 1 : 0
  name                                 = "${var.project_name}-synapse-${var.environment}"
  resource_group_name                  = var.azure_resource_group_name
  location                            = var.azure_location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse[0].id
  sql_administrator_login             = var.synapse_admin_username
  sql_administrator_login_password    = var.synapse_admin_password

  identity {
    type = "SystemAssigned"
  }

  tags = var.common_tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  count              = var.enable_azure_replication ? 1 : 0
  name               = "synapse"
  storage_account_id = azurerm_storage_account.primary_data_lake[0].id
}

# ============================================================================
# GCP DATA REPLICATION INFRASTRUCTURE
# ============================================================================

# Multi-regional Cloud Storage bucket
resource "google_storage_bucket" "primary_data_lake" {
  count         = var.enable_gcp_replication ? 1 : 0
  name          = "${var.project_name}-primary-data-lake-${var.environment}"
  location      = "US"  # Multi-regional
  storage_class = "STANDARD"
  
  versioning {
    enabled = true
  }
  
  encryption {
    default_kms_key_name = google_kms_crypto_key.data_replication[0].id
  }
  
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = var.common_tags
}

# GCP KMS for encryption
resource "google_kms_key_ring" "data_replication" {
  count    = var.enable_gcp_replication ? 1 : 0
  name     = "${var.project_name}-data-replication"
  location = var.gcp_region
}

resource "google_kms_crypto_key" "data_replication" {
  count           = var.enable_gcp_replication ? 1 : 0
  name            = "data-replication-key"
  key_ring        = google_kms_key_ring.data_replication[0].id
  rotation_period = "86400s"  # 24 hours

  version_template {
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }
}

# BigQuery dataset with backup
resource "google_bigquery_dataset" "data_warehouse" {
  count                       = var.enable_gcp_replication ? 1 : 0
  dataset_id                  = "${replace(var.project_name, "-", "_")}_data_warehouse_${var.environment}"
  friendly_name              = "Data Warehouse"
  description                = "Enterprise data warehouse with cross-region backup"
  location                   = var.gcp_region
  default_table_expiration_ms = 3600000

  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.data_replication[0].id
  }

  labels = var.common_tags
}

# ============================================================================
# CROSS-CLOUD DATA SYNC INFRASTRUCTURE
# ============================================================================

# AWS Lambda function for cross-cloud synchronization
resource "aws_lambda_function" "cross_cloud_sync" {
  count            = var.enable_cross_cloud_sync ? 1 : 0
  filename         = "cross_cloud_sync.zip"
  function_name    = "${var.project_name}-cross-cloud-sync"
  role            = aws_iam_role.lambda_cross_cloud_sync[0].arn
  handler         = "index.handler"
  source_code_hash = data.archive_file.cross_cloud_sync_zip[0].output_base64sha256
  runtime         = "python3.9"
  timeout         = 900  # 15 minutes

  environment {
    variables = {
      AWS_BUCKET_NAME       = var.enable_aws_replication ? aws_s3_bucket.primary_data_lake[0].bucket : ""
      AZURE_STORAGE_ACCOUNT = var.enable_azure_replication ? azurerm_storage_account.primary_data_lake[0].name : ""
      GCP_BUCKET_NAME       = var.enable_gcp_replication ? google_storage_bucket.primary_data_lake[0].name : ""
      KMS_KEY_ID           = var.enable_aws_replication ? aws_kms_key.data_replication[0].id : ""
    }
  }

  tags = var.common_tags
}

# Create the Lambda deployment package
data "archive_file" "cross_cloud_sync_zip" {
  count       = var.enable_cross_cloud_sync ? 1 : 0
  type        = "zip"
  output_path = "cross_cloud_sync.zip"
  
  source {
    content = templatefile("${path.module}/templates/cross_cloud_sync.py", {
      aws_enabled   = var.enable_aws_replication
      azure_enabled = var.enable_azure_replication
      gcp_enabled   = var.enable_gcp_replication
    })
    filename = "index.py"
  }
}

# IAM role for Lambda cross-cloud sync
resource "aws_iam_role" "lambda_cross_cloud_sync" {
  count = var.enable_cross_cloud_sync ? 1 : 0
  name  = "${var.project_name}-lambda-cross-cloud-sync-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = var.common_tags
}

# Lambda execution policy
resource "aws_iam_policy" "lambda_cross_cloud_sync" {
  count = var.enable_cross_cloud_sync ? 1 : 0
  name  = "${var.project_name}-lambda-cross-cloud-sync-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.enable_aws_replication ? aws_s3_bucket.primary_data_lake[0].arn : "",
          var.enable_aws_replication ? "${aws_s3_bucket.primary_data_lake[0].arn}/*" : ""
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:Encrypt"
        ]
        Resource = var.enable_aws_replication ? aws_kms_key.data_replication[0].arn : ""
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_cross_cloud_sync" {
  count      = var.enable_cross_cloud_sync ? 1 : 0
  role       = aws_iam_role.lambda_cross_cloud_sync[0].name
  policy_arn = aws_iam_policy.lambda_cross_cloud_sync[0].arn
}

# EventBridge rule for scheduled synchronization
resource "aws_cloudwatch_event_rule" "cross_cloud_sync_schedule" {
  count               = var.enable_cross_cloud_sync ? 1 : 0
  name                = "${var.project_name}-cross-cloud-sync-schedule"
  description         = "Schedule for cross-cloud data synchronization"
  schedule_expression = "rate(${var.sync_schedule_minutes} minutes)"

  tags = var.common_tags
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  count     = var.enable_cross_cloud_sync ? 1 : 0
  rule      = aws_cloudwatch_event_rule.cross_cloud_sync_schedule[0].name
  target_id = "CrossCloudSyncLambdaTarget"
  arn       = aws_lambda_function.cross_cloud_sync[0].arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.enable_cross_cloud_sync ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cross_cloud_sync[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cross_cloud_sync_schedule[0].arn
}

# ============================================================================
# DATA BACKUP AND RECOVERY
# ============================================================================

# AWS Backup vault
resource "aws_backup_vault" "data_platform" {
  count       = var.enable_aws_backup ? 1 : 0
  name        = "${var.project_name}-data-platform-backup"
  kms_key_arn = aws_kms_key.data_replication[0].arn

  tags = merge(var.common_tags, {
    Purpose = "DataBackup"
  })
}

# Backup plan
resource "aws_backup_plan" "data_platform" {
  count = var.enable_aws_backup ? 1 : 0
  name  = "${var.project_name}-data-platform-backup-plan"

  rule {
    rule_name         = "daily_backup"
    target_vault_name = aws_backup_vault.data_platform[0].name
    schedule          = "cron(0 2 * * ? *)"  # Daily at 2 AM

    lifecycle {
      cold_storage_after = 30
      delete_after       = 365
    }

    recovery_point_tags = merge(var.common_tags, {
      BackupType = "Daily"
    })
  }

  rule {
    rule_name         = "weekly_backup"
    target_vault_name = aws_backup_vault.data_platform[0].name
    schedule          = "cron(0 3 ? * SUN *)"  # Weekly on Sunday at 3 AM

    lifecycle {
      cold_storage_after = 90
      delete_after       = 2555  # 7 years
    }

    recovery_point_tags = merge(var.common_tags, {
      BackupType = "Weekly"
    })
  }

  tags = var.common_tags
}

# Backup selection
resource "aws_backup_selection" "data_platform" {
  count        = var.enable_aws_backup ? 1 : 0
  iam_role_arn = aws_iam_role.backup[0].arn
  name         = "${var.project_name}-data-platform-backup-selection"
  plan_id      = aws_backup_plan.data_platform[0].id

  resources = compact([
    var.enable_aws_replication ? aws_s3_bucket.primary_data_lake[0].arn : "",
    var.enable_aws_replication ? aws_dynamodb_table.metadata_primary[0].arn : ""
  ])

  condition {
    string_equals {
      key   = "aws:ResourceTag/BackupEnabled"
      value = "true"
    }
  }
}

# IAM role for AWS Backup
resource "aws_iam_role" "backup" {
  count = var.enable_aws_backup ? 1 : 0
  name  = "${var.project_name}-backup-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "backup.amazonaws.com"
        }
      }
    ]
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForBackup",
    "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForRestores"
  ]

  tags = var.common_tags
}

# ============================================================================
# MONITORING AND ALERTING
# ============================================================================

# CloudWatch alarms for replication monitoring
resource "aws_cloudwatch_metric_alarm" "replication_failure" {
  count               = var.enable_aws_replication ? 1 : 0
  alarm_name          = "${var.project_name}-replication-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "ReplicationLatency"
  namespace           = "AWS/S3"
  period              = "300"
  statistic           = "Maximum"
  threshold           = "900"  # 15 minutes
  alarm_description   = "S3 replication latency is too high"
  alarm_actions       = [aws_sns_topic.data_alerts[0].arn]

  dimensions = {
    SourceBucket      = aws_s3_bucket.primary_data_lake[0].bucket
    DestinationBucket = aws_s3_bucket.replica_data_lake[0].bucket
  }

  tags = var.common_tags
}

# SNS topic for data platform alerts
resource "aws_sns_topic" "data_alerts" {
  count = var.enable_monitoring ? 1 : 0
  name  = "${var.project_name}-data-platform-alerts"

  tags = var.common_tags
}

# CloudWatch dashboard for data platform monitoring
resource "aws_cloudwatch_dashboard" "data_platform" {
  count          = var.enable_monitoring ? 1 : 0
  dashboard_name = "${var.project_name}-data-platform-monitoring"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/S3", "BucketSizeBytes", "BucketName", var.enable_aws_replication ? aws_s3_bucket.primary_data_lake[0].bucket : "", "StorageType", "StandardStorage"],
            [".", "NumberOfObjects", ".", ".", ".", "AllStorageTypes"]
          ]
          period = 86400  # 1 day
          stat   = "Average"
          region = var.aws_region
          title  = "Data Lake Storage Metrics"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = var.enable_aws_replication ? [
            ["AWS/S3", "ReplicationLatency", "SourceBucket", aws_s3_bucket.primary_data_lake[0].bucket, "DestinationBucket", aws_s3_bucket.replica_data_lake[0].bucket]
          ] : []
          period = 300
          stat   = "Average"
          region = var.aws_region
          title  = "Cross-Region Replication Metrics"
        }
      }
    ]
  })
}

# ============================================================================
# OUTPUTS
# ============================================================================

output "primary_data_lake_endpoints" {
  description = "Primary data lake endpoints across clouds"
  value = {
    aws_s3_bucket = var.enable_aws_replication ? aws_s3_bucket.primary_data_lake[0].bucket_domain_name : null
    azure_storage = var.enable_azure_replication ? azurerm_storage_account.primary_data_lake[0].primary_blob_endpoint : null
    gcp_bucket    = var.enable_gcp_replication ? google_storage_bucket.primary_data_lake[0].url : null
  }
}

output "replica_endpoints" {
  description = "Replica data lake endpoints"
  value = {
    aws_replica_bucket = var.enable_aws_replication ? aws_s3_bucket.replica_data_lake[0].bucket_domain_name : null
  }
}

output "backup_configuration" {
  description = "Backup configuration details"
  value = {
    aws_backup_vault_arn = var.enable_aws_backup ? aws_backup_vault.data_platform[0].arn : null
    backup_plan_id       = var.enable_aws_backup ? aws_backup_plan.data_platform[0].id : null
  }
}

output "encryption_keys" {
  description = "Encryption key information"
  value = {
    aws_kms_key_id = var.enable_aws_replication ? aws_kms_key.data_replication[0].id : null
    gcp_kms_key_id = var.enable_gcp_replication ? google_kms_crypto_key.data_replication[0].id : null
  }
  sensitive = true
}

output "monitoring_dashboard_url" {
  description = "CloudWatch dashboard URL for monitoring"
  value = var.enable_monitoring && var.enable_aws_replication ? "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.data_platform[0].dashboard_name}" : null
}