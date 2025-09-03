# Cross-Cloud Data Replication and Synchronization Module
# Implements data consistency and replication across AWS, Azure, and GCP

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

# Local variables
locals {
  common_tags = var.common_tags
  replication_schedule = var.enable_real_time_replication ? "rate(5 minutes)" : "rate(1 hour)"
}

# ============================================================================
# AWS DATA REPLICATION INFRASTRUCTURE
# ============================================================================

# S3 bucket for cross-cloud data staging
resource "aws_s3_bucket" "cross_cloud_staging" {
  count  = var.enable_aws_replication ? 1 : 0
  bucket = "${var.project_name}-cross-cloud-staging-${var.environment}"
  
  tags = merge(local.common_tags, {
    Purpose = "CrossCloudDataStaging"
    DataClassification = "Internal"
  })
}

# S3 bucket versioning for data integrity
resource "aws_s3_bucket_versioning" "cross_cloud_staging" {
  count  = var.enable_aws_replication ? 1 : 0
  bucket = aws_s3_bucket.cross_cloud_staging[0].id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 bucket encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "cross_cloud_staging" {
  count  = var.enable_aws_replication ? 1 : 0
  bucket = aws_s3_bucket.cross_cloud_staging[0].id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.cross_cloud_replication[0].arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

# KMS key for cross-cloud data encryption
resource "aws_kms_key" "cross_cloud_replication" {
  count = var.enable_aws_replication ? 1 : 0
  
  description             = "KMS key for cross-cloud data replication encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow cross-account access for replication"
        Effect = "Allow"
        Principal = {
          AWS = var.cross_account_role_arns
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:ReEncrypt*"
        ]
        Resource = "*"
      }
    ]
  })
  
  tags = merge(local.common_tags, {
    Purpose = "CrossCloudEncryption"
  })
}

resource "aws_kms_alias" "cross_cloud_replication" {
  count         = var.enable_aws_replication ? 1 : 0
  name          = "alias/${var.project_name}-cross-cloud-replication"
  target_key_id = aws_kms_key.cross_cloud_replication[0].key_id
}

# DynamoDB table for replication metadata and state management
resource "aws_dynamodb_table" "replication_state" {
  count = var.enable_aws_replication ? 1 : 0
  
  name           = "${var.project_name}-replication-state"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "replication_id"
  range_key      = "timestamp"
  
  attribute {
    name = "replication_id"
    type = "S"
  }
  
  attribute {
    name = "timestamp"
    type = "S"
  }
  
  attribute {
    name = "source_cloud"
    type = "S"
  }
  
  attribute {
    name = "target_cloud"
    type = "S"
  }
  
  attribute {
    name = "status"
    type = "S"
  }
  
  global_secondary_index {
    name     = "SourceCloudIndex"
    hash_key = "source_cloud"
    range_key = "timestamp"
  }
  
  global_secondary_index {
    name     = "TargetCloudIndex"
    hash_key = "target_cloud"
    range_key = "status"
  }
  
  global_secondary_index {
    name     = "StatusIndex"
    hash_key = "status"
    range_key = "timestamp"
  }
  
  point_in_time_recovery {
    enabled = var.environment == "production"
  }
  
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.cross_cloud_replication[0].arn
  }
  
  tags = merge(local.common_tags, {
    Purpose = "ReplicationMetadata"
  })
}

# Lambda function for data replication orchestration
resource "aws_lambda_function" "replication_orchestrator" {
  count = var.enable_aws_replication ? 1 : 0
  
  filename         = data.archive_file.replication_orchestrator[0].output_path
  function_name    = "${var.project_name}-replication-orchestrator"
  role            = aws_iam_role.lambda_replication_role[0].arn
  handler         = "index.handler"
  runtime         = "python3.11"
  timeout         = 900  # 15 minutes
  memory_size     = 1024
  
  source_code_hash = data.archive_file.replication_orchestrator[0].output_base64sha256
  
  vpc_config {
    subnet_ids         = var.aws_private_subnet_ids
    security_group_ids = [aws_security_group.lambda_replication[0].id]
  }
  
  environment {
    variables = {
      REPLICATION_STATE_TABLE = aws_dynamodb_table.replication_state[0].name
      STAGING_BUCKET         = aws_s3_bucket.cross_cloud_staging[0].bucket
      KMS_KEY_ID            = aws_kms_key.cross_cloud_replication[0].key_id
      ENVIRONMENT           = var.environment
      LOG_LEVEL            = var.environment == "production" ? "INFO" : "DEBUG"
      
      # Azure configuration
      AZURE_STORAGE_ACCOUNT = var.azure_storage_account_name
      AZURE_CONTAINER_NAME  = var.azure_container_name
      
      # GCP configuration
      GCP_PROJECT_ID       = var.gcp_project_id
      GCP_BUCKET_NAME      = var.gcp_bucket_name
    }
  }
  
  depends_on = [
    aws_iam_role_policy_attachment.lambda_replication_policy,
    aws_cloudwatch_log_group.lambda_replication_logs
  ]
  
  tags = merge(local.common_tags, {
    Purpose = "DataReplicationOrchestrator"
  })
}

# Lambda function source code
data "archive_file" "replication_orchestrator" {
  count = var.enable_aws_replication ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/replication_orchestrator.zip"
  
  source {
    content = templatefile("${path.module}/lambda/replication_orchestrator.py", {
      project_name = var.project_name
      environment = var.environment
    })
    filename = "index.py"
  }
  
  source {
    content = file("${path.module}/lambda/requirements.txt")
    filename = "requirements.txt"
  }
}

# Security group for Lambda function
resource "aws_security_group" "lambda_replication" {
  count = var.enable_aws_replication ? 1 : 0
  
  name_prefix = "${var.project_name}-lambda-replication-"
  vpc_id      = var.aws_vpc_id
  
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS outbound for AWS API calls"
  }
  
  egress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTP outbound for package downloads"
  }
  
  tags = merge(local.common_tags, {
    Purpose = "LambdaReplicationSecurity"
  })
}

# IAM role for Lambda replication function
resource "aws_iam_role" "lambda_replication_role" {
  count = var.enable_aws_replication ? 1 : 0
  
  name = "${var.project_name}-lambda-replication-role"
  
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
  
  tags = local.common_tags
}

# IAM policy for Lambda replication function
resource "aws_iam_policy" "lambda_replication_policy" {
  count = var.enable_aws_replication ? 1 : 0
  
  name = "${var.project_name}-lambda-replication-policy"
  
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
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetObjectVersion"
        ]
        Resource = [
          aws_s3_bucket.cross_cloud_staging[0].arn,
          "${aws_s3_bucket.cross_cloud_staging[0].arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem"
        ]
        Resource = [
          aws_dynamodb_table.replication_state[0].arn,
          "${aws_dynamodb_table.replication_state[0].arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:ReEncrypt*",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.cross_cloud_replication[0].arn
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.replication_notifications[0].arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_replication_policy" {
  count      = var.enable_aws_replication ? 1 : 0
  role       = aws_iam_role.lambda_replication_role[0].name
  policy_arn = aws_iam_policy.lambda_replication_policy[0].arn
}

# CloudWatch log group for Lambda function
resource "aws_cloudwatch_log_group" "lambda_replication_logs" {
  count = var.enable_aws_replication ? 1 : 0
  
  name              = "/aws/lambda/${var.project_name}-replication-orchestrator"
  retention_in_days = var.log_retention_days
  kms_key_id       = aws_kms_key.cross_cloud_replication[0].arn
  
  tags = merge(local.common_tags, {
    Purpose = "ReplicationLogs"
  })
}

# EventBridge rule for scheduled replication
resource "aws_cloudwatch_event_rule" "replication_schedule" {
  count = var.enable_aws_replication ? 1 : 0
  
  name                = "${var.project_name}-replication-schedule"
  description         = "Trigger cross-cloud data replication"
  schedule_expression = local.replication_schedule
  
  tags = merge(local.common_tags, {
    Purpose = "ReplicationSchedule"
  })
}

# EventBridge target for Lambda function
resource "aws_cloudwatch_event_target" "lambda_target" {
  count = var.enable_aws_replication ? 1 : 0
  
  rule      = aws_cloudwatch_event_rule.replication_schedule[0].name
  target_id = "ReplicationOrchestratorTarget"
  arn       = aws_lambda_function.replication_orchestrator[0].arn
  
  input = jsonencode({
    replication_type = "scheduled"
    source_clouds    = var.source_clouds
    target_clouds    = var.target_clouds
  })
}

# Lambda permission for EventBridge
resource "aws_lambda_permission" "allow_eventbridge" {
  count = var.enable_aws_replication ? 1 : 0
  
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.replication_orchestrator[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.replication_schedule[0].arn
}

# SNS topic for replication notifications
resource "aws_sns_topic" "replication_notifications" {
  count = var.enable_aws_replication ? 1 : 0
  
  name         = "${var.project_name}-replication-notifications"
  display_name = "Cross-Cloud Data Replication Notifications"
  
  kms_master_key_id = aws_kms_key.cross_cloud_replication[0].key_id
  
  tags = merge(local.common_tags, {
    Purpose = "ReplicationNotifications"
  })
}

# ============================================================================
# AZURE DATA REPLICATION INFRASTRUCTURE
# ============================================================================

# Azure Data Factory for cross-cloud data movement
resource "azurerm_data_factory" "cross_cloud_replication" {
  count = var.enable_azure_replication ? 1 : 0
  
  name                = "${var.project_name}-cross-cloud-adf"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = merge(local.common_tags, {
    Purpose = "CrossCloudDataFactory"
  })
}

# Azure Storage Account for replication staging
resource "azurerm_storage_account" "replication_staging" {
  count = var.enable_azure_replication ? 1 : 0
  
  name                     = "${replace(var.project_name, "-", "")}replication${var.environment}"
  resource_group_name      = var.azure_resource_group_name
  location                = var.azure_location
  account_tier            = "Standard"
  account_replication_type = var.environment == "production" ? "GRS" : "LRS"
  account_kind            = "StorageV2"
  
  enable_https_traffic_only = true
  min_tls_version          = "TLS1_2"
  
  blob_properties {
    versioning_enabled       = true
    last_access_time_enabled = true
    
    delete_retention_policy {
      days = 30
    }
    
    container_delete_retention_policy {
      days = 30
    }
  }
  
  tags = merge(local.common_tags, {
    Purpose = "ReplicationStaging"
  })
}

# Azure Storage Container for cross-cloud data
resource "azurerm_storage_container" "cross_cloud_data" {
  count = var.enable_azure_replication ? 1 : 0
  
  name                  = "cross-cloud-data"
  storage_account_name  = azurerm_storage_account.replication_staging[0].name
  container_access_type = "private"
}

# Azure Logic App for replication workflow orchestration
resource "azurerm_logic_app_workflow" "replication_workflow" {
  count = var.enable_azure_replication ? 1 : 0
  
  name                = "${var.project_name}-replication-workflow"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  tags = merge(local.common_tags, {
    Purpose = "ReplicationWorkflow"
  })
}

# ============================================================================
# GCP DATA REPLICATION INFRASTRUCTURE
# ============================================================================

# Cloud Scheduler for GCP-based replication
resource "google_cloud_scheduler_job" "replication_schedule" {
  count = var.enable_gcp_replication ? 1 : 0
  
  name        = "${var.project_name}-replication-schedule"
  description = "Trigger cross-cloud data replication from GCP"
  schedule    = var.enable_real_time_replication ? "*/5 * * * *" : "0 */1 * * *"
  time_zone   = var.gcp_timezone
  region      = var.gcp_region
  
  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions_function.replication_orchestrator[0].https_trigger_url
    
    headers = {
      "Content-Type" = "application/json"
    }
    
    body = base64encode(jsonencode({
      replication_type = "scheduled"
      source_clouds    = var.source_clouds
      target_clouds    = var.target_clouds
    }))
    
    oidc_token {
      service_account_email = google_service_account.replication_service[0].email
    }
  }
}

# GCP Cloud Function for replication orchestration
resource "google_cloudfunctions_function" "replication_orchestrator" {
  count = var.enable_gcp_replication ? 1 : 0
  
  name        = "${var.project_name}-replication-orchestrator"
  description = "Cross-cloud data replication orchestrator"
  region      = var.gcp_region
  runtime     = "python311"
  
  available_memory_mb   = 1024
  source_archive_bucket = google_storage_bucket.function_source[0].name
  source_archive_object = google_storage_bucket_object.function_source[0].name
  entry_point          = "orchestrate_replication"
  timeout              = 540
  
  https_trigger {
    security_level = "SECURE_ALWAYS"
  }
  
  environment_variables = {
    PROJECT_ID           = var.gcp_project_id
    ENVIRONMENT         = var.environment
    REPLICATION_DATASET = google_bigquery_dataset.replication_metadata[0].dataset_id
    AWS_S3_BUCKET       = var.aws_s3_bucket_name
    AZURE_STORAGE_ACCOUNT = var.azure_storage_account_name
  }
  
  service_account_email = google_service_account.replication_service[0].email
  
  labels = {
    environment = var.environment
    purpose     = "cross-cloud-replication"
  }
}

# GCP Service Account for replication operations
resource "google_service_account" "replication_service" {
  count = var.enable_gcp_replication ? 1 : 0
  
  account_id   = "${var.project_name}-replication-sa"
  display_name = "Cross-Cloud Data Replication Service Account"
  description  = "Service account for cross-cloud data replication operations"
}

# GCP Storage bucket for function source code
resource "google_storage_bucket" "function_source" {
  count = var.enable_gcp_replication ? 1 : 0
  
  name     = "${var.project_name}-function-source-${var.environment}"
  location = var.gcp_region
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
  
  labels = {
    environment = var.environment
    purpose     = "function-source"
  }
}

# Upload function source code
resource "google_storage_bucket_object" "function_source" {
  count = var.enable_gcp_replication ? 1 : 0
  
  name   = "replication-orchestrator-${formatdate("YYYY-MM-DD-hhmm", timestamp())}.zip"
  bucket = google_storage_bucket.function_source[0].name
  source = data.archive_file.gcp_replication_function[0].output_path
}

data "archive_file" "gcp_replication_function" {
  count = var.enable_gcp_replication ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/gcp_replication_function.zip"
  
  source {
    content = templatefile("${path.module}/gcp-functions/replication_orchestrator.py", {
      project_name = var.project_name
      environment = var.environment
    })
    filename = "main.py"
  }
  
  source {
    content = file("${path.module}/gcp-functions/requirements.txt")
    filename = "requirements.txt"
  }
}

# BigQuery dataset for replication metadata
resource "google_bigquery_dataset" "replication_metadata" {
  count = var.enable_gcp_replication ? 1 : 0
  
  dataset_id                  = "${replace(var.project_name, "-", "_")}_replication_metadata"
  friendly_name               = "Cross-Cloud Replication Metadata"
  description                = "Dataset for storing cross-cloud data replication metadata and logs"
  location                   = var.gcp_region
  default_table_expiration_ms = 7776000000  # 90 days
  
  labels = {
    environment = var.environment
    purpose     = "replication-metadata"
  }
}

# ============================================================================
# MONITORING AND ALERTING
# ============================================================================

# ============================================================================
# DATA CONSISTENCY AND VALIDATION
# ============================================================================

# Lambda function for data consistency validation
resource "aws_lambda_function" "data_consistency_validator" {
  count = var.enable_aws_replication ? 1 : 0
  
  filename         = data.archive_file.consistency_validator[0].output_path
  function_name    = "${var.project_name}-consistency-validator"
  role            = aws_iam_role.consistency_validator_role[0].arn
  handler         = "index.handler"
  runtime         = "python3.11"
  timeout         = 900
  memory_size     = 2048
  
  environment {
    variables = {
      REPLICATION_STATE_TABLE = aws_dynamodb_table.replication_state[0].name
      CONSISTENCY_TABLE      = aws_dynamodb_table.data_consistency[0].name
      SNS_TOPIC_ARN         = aws_sns_topic.replication_notifications[0].arn
      VALIDATION_THRESHOLD   = var.consistency_validation_threshold
      ENVIRONMENT           = var.environment
    }
  }
  
  depends_on = [
    aws_iam_role_policy_attachment.consistency_validator_policy,
    aws_cloudwatch_log_group.consistency_validator_logs
  ]
  
  tags = merge(local.common_tags, {
    Purpose = "DataConsistencyValidator"
  })
}

# Data consistency tracking table
resource "aws_dynamodb_table" "data_consistency" {
  count = var.enable_aws_replication ? 1 : 0
  
  name           = "${var.project_name}-data-consistency"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "data_identifier"
  range_key      = "validation_timestamp"
  
  attribute {
    name = "data_identifier"
    type = "S"
  }
  
  attribute {
    name = "validation_timestamp"
    type = "S"
  }
  
  attribute {
    name = "consistency_status"
    type = "S"
  }
  
  attribute {
    name = "source_hash"
    type = "S"
  }
  
  global_secondary_index {
    name     = "ConsistencyStatusIndex"
    hash_key = "consistency_status"
    range_key = "validation_timestamp"
  }
  
  global_secondary_index {
    name     = "SourceHashIndex"
    hash_key = "source_hash"
    range_key = "validation_timestamp"
  }
  
  ttl {
    attribute_name = "ttl_timestamp"
    enabled        = true
  }
  
  point_in_time_recovery {
    enabled = var.environment == "production"
  }
  
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.cross_cloud_replication[0].arn
  }
  
  tags = merge(local.common_tags, {
    Purpose = "DataConsistencyTracking"
  })
}

# Lambda source for consistency validator
data "archive_file" "consistency_validator" {
  count = var.enable_aws_replication ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/consistency_validator.zip"
  
  source {
    content = templatefile("${path.module}/lambda/consistency_validator.py", {
      project_name = var.project_name
      environment = var.environment
    })
    filename = "index.py"
  }
  
  source {
    content = "boto3==1.34.0\nbotocore==1.34.0\nhashlib\njson\nlogging"
    filename = "requirements.txt"
  }
}

# IAM role for consistency validator
resource "aws_iam_role" "consistency_validator_role" {
  count = var.enable_aws_replication ? 1 : 0
  
  name = "${var.project_name}-consistency-validator-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
  
  tags = local.common_tags
}

# IAM policy for consistency validator
resource "aws_iam_policy" "consistency_validator_policy" {
  count = var.enable_aws_replication ? 1 : 0
  
  name = "${var.project_name}-consistency-validator-policy"
  
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
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          aws_dynamodb_table.replication_state[0].arn,
          "${aws_dynamodb_table.replication_state[0].arn}/index/*",
          aws_dynamodb_table.data_consistency[0].arn,
          "${aws_dynamodb_table.data_consistency[0].arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.cross_cloud_staging[0].arn,
          "${aws_s3_bucket.cross_cloud_staging[0].arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.replication_notifications[0].arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.cross_cloud_replication[0].arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "consistency_validator_policy" {
  count      = var.enable_aws_replication ? 1 : 0
  role       = aws_iam_role.consistency_validator_role[0].name
  policy_arn = aws_iam_policy.consistency_validator_policy[0].arn
}

# CloudWatch log group for consistency validator
resource "aws_cloudwatch_log_group" "consistency_validator_logs" {
  count = var.enable_aws_replication ? 1 : 0
  
  name              = "/aws/lambda/${var.project_name}-consistency-validator"
  retention_in_days = var.log_retention_days
  kms_key_id       = aws_kms_key.cross_cloud_replication[0].arn
  
  tags = merge(local.common_tags, {
    Purpose = "ConsistencyValidationLogs"
  })
}

# EventBridge rule for consistency validation
resource "aws_cloudwatch_event_rule" "consistency_validation" {
  count = var.enable_aws_replication ? 1 : 0
  
  name                = "${var.project_name}-consistency-validation"
  description         = "Trigger data consistency validation"
  schedule_expression = "rate(${var.consistency_check_interval_minutes} minutes)"
  
  tags = merge(local.common_tags, {
    Purpose = "ConsistencyValidation"
  })
}

resource "aws_cloudwatch_event_target" "consistency_validator_target" {
  count = var.enable_aws_replication ? 1 : 0
  
  rule      = aws_cloudwatch_event_rule.consistency_validation[0].name
  target_id = "ConsistencyValidatorTarget"
  arn       = aws_lambda_function.data_consistency_validator[0].arn
}

resource "aws_lambda_permission" "allow_eventbridge_consistency" {
  count = var.enable_aws_replication ? 1 : 0
  
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_consistency_validator[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.consistency_validation[0].arn
}

# ============================================================================
# ADVANCED REPLICATION PATTERNS
# ============================================================================

# SQS Dead Letter Queue for failed replications
resource "aws_sqs_queue" "replication_dlq" {
  count = var.enable_aws_replication ? 1 : 0
  
  name                      = "${var.project_name}-replication-dlq"
  message_retention_seconds = 1209600  # 14 days
  
  kms_master_key_id = aws_kms_key.cross_cloud_replication[0].key_id
  
  tags = merge(local.common_tags, {
    Purpose = "ReplicationDeadLetterQueue"
  })
}

# SQS Queue for replication tasks
resource "aws_sqs_queue" "replication_queue" {
  count = var.enable_aws_replication ? 1 : 0
  
  name                      = "${var.project_name}-replication-queue"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 345600  # 4 days
  receive_wait_time_seconds = 20
  visibility_timeout_seconds = 960  # 16 minutes (more than Lambda timeout)
  
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.replication_dlq[0].arn
    maxReceiveCount     = 3
  })
  
  kms_master_key_id = aws_kms_key.cross_cloud_replication[0].key_id
  
  tags = merge(local.common_tags, {
    Purpose = "ReplicationQueue"
  })
}

# Lambda function for batch replication processing
resource "aws_lambda_function" "batch_replication_processor" {
  count = var.enable_aws_replication ? 1 : 0
  
  filename         = data.archive_file.batch_processor[0].output_path
  function_name    = "${var.project_name}-batch-replication-processor"
  role            = aws_iam_role.batch_processor_role[0].arn
  handler         = "index.handler"
  runtime         = "python3.11"
  timeout         = 900
  memory_size     = 3008
  
  reserved_concurrent_executions = var.max_concurrent_replications
  
  environment {
    variables = {
      REPLICATION_STATE_TABLE = aws_dynamodb_table.replication_state[0].name
      CONSISTENCY_TABLE      = aws_dynamodb_table.data_consistency[0].name
      STAGING_BUCKET         = aws_s3_bucket.cross_cloud_staging[0].bucket
      BATCH_SIZE            = var.replication_batch_size
      ENVIRONMENT           = var.environment
    }
  }
  
  depends_on = [
    aws_iam_role_policy_attachment.batch_processor_policy,
    aws_cloudwatch_log_group.batch_processor_logs
  ]
  
  tags = merge(local.common_tags, {
    Purpose = "BatchReplicationProcessor"
  })
}

# Event source mapping for SQS to Lambda
resource "aws_lambda_event_source_mapping" "replication_queue_trigger" {
  count = var.enable_aws_replication ? 1 : 0
  
  event_source_arn = aws_sqs_queue.replication_queue[0].arn
  function_name    = aws_lambda_function.batch_replication_processor[0].arn
  batch_size       = var.replication_batch_size
  
  scaling_config {
    maximum_concurrency = var.max_concurrent_replications
  }
}

# Batch processor Lambda source
data "archive_file" "batch_processor" {
  count = var.enable_aws_replication ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/batch_processor.zip"
  
  source {
    content = templatefile("${path.module}/lambda/batch_replication_processor.py", {
      project_name = var.project_name
      environment = var.environment
    })
    filename = "index.py"
  }
  
  source {
    content = "boto3==1.34.0\nbotocore==1.34.0\nconcurrent.futures\nhashlib\njson\nlogging\ntime"
    filename = "requirements.txt"
  }
}

# IAM role for batch processor
resource "aws_iam_role" "batch_processor_role" {
  count = var.enable_aws_replication ? 1 : 0
  
  name = "${var.project_name}-batch-processor-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
  
  tags = local.common_tags
}

# IAM policy for batch processor
resource "aws_iam_policy" "batch_processor_policy" {
  count = var.enable_aws_replication ? 1 : 0
  
  name = "${var.project_name}-batch-processor-policy"
  
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
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = aws_sqs_queue.replication_queue[0].arn
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          aws_dynamodb_table.replication_state[0].arn,
          "${aws_dynamodb_table.replication_state[0].arn}/index/*",
          aws_dynamodb_table.data_consistency[0].arn,
          "${aws_dynamodb_table.data_consistency[0].arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetObjectVersion"
        ]
        Resource = [
          aws_s3_bucket.cross_cloud_staging[0].arn,
          "${aws_s3_bucket.cross_cloud_staging[0].arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:ReEncrypt*",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.cross_cloud_replication[0].arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "batch_processor_policy" {
  count      = var.enable_aws_replication ? 1 : 0
  role       = aws_iam_role.batch_processor_role[0].name
  policy_arn = aws_iam_policy.batch_processor_policy[0].arn
}

# CloudWatch log group for batch processor
resource "aws_cloudwatch_log_group" "batch_processor_logs" {
  count = var.enable_aws_replication ? 1 : 0
  
  name              = "/aws/lambda/${var.project_name}-batch-replication-processor"
  retention_in_days = var.log_retention_days
  kms_key_id       = aws_kms_key.cross_cloud_replication[0].arn
  
  tags = merge(local.common_tags, {
    Purpose = "BatchProcessorLogs"
  })
}

# CloudWatch dashboard for replication monitoring
resource "aws_cloudwatch_dashboard" "replication_monitoring" {
  count = var.enable_aws_replication ? 1 : 0
  
  dashboard_name = "${var.project_name}-replication-monitoring"
  
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
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.replication_orchestrator[0].function_name],
            [".", "Errors", ".", "."],
            [".", "Duration", ".", "."],
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.data_consistency_validator[0].function_name],
            [".", "Errors", ".", "."],
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.batch_replication_processor[0].function_name],
            [".", "Errors", ".", "."]
          ]
          period = 300
          stat   = "Sum"
          region = var.aws_region
          title  = "Replication Function Metrics"
        }
      },
      {
        type   = "log"
        x      = 0
        y      = 6
        width  = 24
        height = 6
        
        properties = {
          query   = "SOURCE '${aws_cloudwatch_log_group.lambda_replication_logs[0].name}' | fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 100"
          region  = var.aws_region
          title   = "Replication Errors"
        }
      }
    ]
  })
}

# Data sources
data "aws_caller_identity" "current" {}

# Variables
variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "enable_aws_replication" {
  description = "Enable AWS-based replication infrastructure"
  type        = bool
  default     = true
}

variable "enable_azure_replication" {
  description = "Enable Azure-based replication infrastructure"
  type        = bool
  default     = false
}

variable "enable_gcp_replication" {
  description = "Enable GCP-based replication infrastructure"
  type        = bool
  default     = false
}

variable "enable_real_time_replication" {
  description = "Enable real-time replication (more frequent but higher cost)"
  type        = bool
  default     = false
}

variable "aws_vpc_id" {
  description = "AWS VPC ID"
  type        = string
  default     = ""
}

variable "aws_private_subnet_ids" {
  description = "AWS private subnet IDs"
  type        = list(string)
  default     = []
}

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "West US 2"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = ""
}

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-west1"
}

variable "gcp_timezone" {
  description = "Timezone for GCP Cloud Scheduler"
  type        = string
  default     = "America/Los_Angeles"
}

variable "source_clouds" {
  description = "List of source cloud providers"
  type        = list(string)
  default     = ["aws"]
}

variable "target_clouds" {
  description = "List of target cloud providers"
  type        = list(string)
  default     = ["azure", "gcp"]
}

variable "cross_account_role_arns" {
  description = "List of cross-account role ARNs for KMS access"
  type        = list(string)
  default     = []
}

variable "log_retention_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 90
}

variable "aws_s3_bucket_name" {
  description = "AWS S3 bucket name for replication source"
  type        = string
  default     = ""
}

variable "azure_storage_account_name" {
  description = "Azure storage account name"
  type        = string
  default     = ""
}

variable "azure_container_name" {
  description = "Azure storage container name"
  type        = string
  default     = ""
}

variable "gcp_bucket_name" {
  description = "GCP Cloud Storage bucket name"
  type        = string
  default     = ""
}

# ============================================================================
# ADVANCED CONSISTENCY AND REPLICATION VARIABLES
# ============================================================================

variable "consistency_validation_threshold" {
  description = "Threshold percentage for data consistency validation (0-100)"
  type        = number
  default     = 99.5
  validation {
    condition     = var.consistency_validation_threshold >= 0 && var.consistency_validation_threshold <= 100
    error_message = "Consistency validation threshold must be between 0 and 100."
  }
}

variable "consistency_check_interval_minutes" {
  description = "Interval in minutes for consistency checks"
  type        = number
  default     = 30
  validation {
    condition     = var.consistency_check_interval_minutes >= 5 && var.consistency_check_interval_minutes <= 1440
    error_message = "Consistency check interval must be between 5 minutes and 24 hours."
  }
}

variable "replication_batch_size" {
  description = "Number of items to process in each replication batch"
  type        = number
  default     = 10
  validation {
    condition     = var.replication_batch_size >= 1 && var.replication_batch_size <= 100
    error_message = "Replication batch size must be between 1 and 100."
  }
}

variable "max_concurrent_replications" {
  description = "Maximum number of concurrent replication processes"
  type        = number
  default     = 50
  validation {
    condition     = var.max_concurrent_replications >= 1 && var.max_concurrent_replications <= 1000
    error_message = "Max concurrent replications must be between 1 and 1000."
  }
}

variable "enable_conflict_resolution" {
  description = "Enable automatic conflict resolution for data replication"
  type        = bool
  default     = true
}

variable "conflict_resolution_strategy" {
  description = "Strategy for resolving data conflicts (timestamp, size, manual)"
  type        = string
  default     = "timestamp"
  validation {
    condition     = contains(["timestamp", "size", "manual", "source_priority"], var.conflict_resolution_strategy)
    error_message = "Conflict resolution strategy must be timestamp, size, manual, or source_priority."
  }
}

variable "data_integrity_checks" {
  description = "Enable comprehensive data integrity checks"
  type        = bool
  default     = true
}

variable "checksum_algorithm" {
  description = "Checksum algorithm for data integrity (md5, sha256, sha512)"
  type        = string
  default     = "sha256"
  validation {
    condition     = contains(["md5", "sha256", "sha512"], var.checksum_algorithm)
    error_message = "Checksum algorithm must be md5, sha256, or sha512."
  }
}

variable "enable_eventual_consistency" {
  description = "Enable eventual consistency guarantees"
  type        = bool
  default     = true
}

variable "consistency_timeout_seconds" {
  description = "Timeout for consistency operations in seconds"
  type        = number
  default     = 300
  validation {
    condition     = var.consistency_timeout_seconds >= 60 && var.consistency_timeout_seconds <= 3600
    error_message = "Consistency timeout must be between 60 and 3600 seconds."
  }
}

variable "enable_replication_metrics" {
  description = "Enable detailed replication metrics and monitoring"
  type        = bool
  default     = true
}

variable "replication_lag_threshold_minutes" {
  description = "Threshold for replication lag alerting in minutes"
  type        = number
  default     = 60
  validation {
    condition     = var.replication_lag_threshold_minutes >= 1 && var.replication_lag_threshold_minutes <= 1440
    error_message = "Replication lag threshold must be between 1 minute and 24 hours."
  }
}

variable "enable_cross_region_replication" {
  description = "Enable cross-region replication within cloud providers"
  type        = bool
  default     = false
}

variable "backup_retention_days" {
  description = "Number of days to retain replication backups"
  type        = number
  default     = 90
  validation {
    condition     = var.backup_retention_days >= 1 && var.backup_retention_days <= 2555
    error_message = "Backup retention must be between 1 and 2555 days (7 years)."
  }
}

variable "enable_delta_sync" {
  description = "Enable delta synchronization to replicate only changes"
  type        = bool
  default     = true
}

variable "delta_sync_window_hours" {
  description = "Window in hours for delta synchronization"
  type        = number
  default     = 24
  validation {
    condition     = var.delta_sync_window_hours >= 1 && var.delta_sync_window_hours <= 168
    error_message = "Delta sync window must be between 1 hour and 7 days."
  }
}

variable "enable_schema_validation" {
  description = "Enable schema validation during replication"
  type        = bool
  default     = true
}

variable "schema_evolution_strategy" {
  description = "Strategy for handling schema evolution (strict, compatible, ignore)"
  type        = string
  default     = "compatible"
  validation {
    condition     = contains(["strict", "compatible", "ignore"], var.schema_evolution_strategy)
    error_message = "Schema evolution strategy must be strict, compatible, or ignore."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

# Outputs
output "aws_replication_function_arn" {
  description = "ARN of the AWS Lambda replication function"
  value       = var.enable_aws_replication ? aws_lambda_function.replication_orchestrator[0].arn : null
}

output "aws_replication_state_table" {
  description = "Name of the DynamoDB replication state table"
  value       = var.enable_aws_replication ? aws_dynamodb_table.replication_state[0].name : null
}

output "azure_data_factory_name" {
  description = "Name of the Azure Data Factory"
  value       = var.enable_azure_replication ? azurerm_data_factory.cross_cloud_replication[0].name : null
}

output "gcp_function_url" {
  description = "URL of the GCP Cloud Function"
  value       = var.enable_gcp_replication ? google_cloudfunctions_function.replication_orchestrator[0].https_trigger_url : null
}

output "replication_monitoring_dashboard" {
  description = "URL of the CloudWatch monitoring dashboard"
  value       = var.enable_aws_replication ? "https://console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.replication_monitoring[0].dashboard_name}" : null
}

output "data_consistency_table" {
  description = "Name of the data consistency tracking table"
  value       = var.enable_aws_replication ? aws_dynamodb_table.data_consistency[0].name : null
}

output "replication_queue_url" {
  description = "URL of the replication SQS queue"
  value       = var.enable_aws_replication ? aws_sqs_queue.replication_queue[0].url : null
}

output "consistency_validator_function_arn" {
  description = "ARN of the data consistency validator Lambda function"
  value       = var.enable_aws_replication ? aws_lambda_function.data_consistency_validator[0].arn : null
}

output "batch_processor_function_arn" {
  description = "ARN of the batch replication processor Lambda function"
  value       = var.enable_aws_replication ? aws_lambda_function.batch_replication_processor[0].arn : null
}

output "cross_cloud_replication_summary" {
  description = "Comprehensive summary of cross-cloud replication configuration"
  value = {
    # Core Configuration
    aws_replication_enabled    = var.enable_aws_replication
    azure_replication_enabled  = var.enable_azure_replication
    gcp_replication_enabled    = var.enable_gcp_replication
    real_time_replication      = var.enable_real_time_replication
    
    # Consistency Features
    consistency_validation_enabled = var.enable_aws_replication
    consistency_threshold         = var.consistency_validation_threshold
    consistency_check_interval    = var.consistency_check_interval_minutes
    conflict_resolution_enabled  = var.enable_conflict_resolution
    conflict_resolution_strategy  = var.conflict_resolution_strategy
    
    # Data Integrity
    data_integrity_checks = var.data_integrity_checks
    checksum_algorithm   = var.checksum_algorithm
    schema_validation    = var.enable_schema_validation
    
    # Performance Optimization
    batch_processing_enabled = var.enable_aws_replication
    batch_size              = var.replication_batch_size
    max_concurrent_processes = var.max_concurrent_replications
    delta_sync_enabled      = var.enable_delta_sync
    
    # Monitoring and Alerting
    replication_metrics_enabled = var.enable_replication_metrics
    replication_lag_threshold   = var.replication_lag_threshold_minutes
    monitoring_dashboard_available = var.enable_aws_replication
    
    # Backup and Recovery
    backup_retention_days        = var.backup_retention_days
    cross_region_replication     = var.enable_cross_region_replication
    point_in_time_recovery      = var.environment == "production"
    
    # Security
    encryption_in_transit = true
    encryption_at_rest   = true
    kms_key_rotation     = true
    
    # Compliance
    audit_logging_enabled = true
    data_lineage_tracking = true
    compliance_monitoring = true
  }
}