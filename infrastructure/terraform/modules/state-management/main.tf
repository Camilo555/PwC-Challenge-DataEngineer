# Advanced Terraform State Management Module
# Implements workspace isolation, backup, and state security

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
  
  # State management configuration
  state_config = {
    encryption_enabled = var.enable_state_encryption
    versioning_enabled = var.enable_state_versioning
    backup_enabled = var.enable_state_backup
    cross_region_replication = var.enable_cross_region_replication
    workspace_isolation = var.enable_workspace_isolation
  }
  
  # Workspace configurations
  workspaces = {
    for env in var.environments : env => {
      name = "${var.project_name}-${env}"
      backend_key = "env:/${env}/terraform.tfstate"
      lock_table = "${var.project_name}-locks-${env}"
    }
  }
  
  # Backup schedule configuration
  backup_schedules = {
    hourly = "cron(0 * * * ? *)"
    daily = "cron(0 2 * * ? *)"
    weekly = "cron(0 2 ? * SUN *)"
    monthly = "cron(0 2 1 * ? *)"
  }
}

# ============================================================================
# AWS STATE MANAGEMENT INFRASTRUCTURE
# ============================================================================

# KMS Key for State Encryption
resource "aws_kms_key" "state_encryption" {
  count = var.deploy_to_aws ? 1 : 0
  
  description             = "KMS key for Terraform state encryption"
  deletion_window_in_days = 7
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow Terraform State Access"
        Effect = "Allow"
        Principal = {
          AWS = [
            for role in var.terraform_execution_roles :
            "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:role/${role}"
          ]
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-state-encryption-key"
    Purpose = "TerraformStateEncryption"
  })
}

resource "aws_kms_alias" "state_encryption" {
  count = var.deploy_to_aws ? 1 : 0
  
  name          = "alias/${local.name_prefix}-terraform-state"
  target_key_id = aws_kms_key.state_encryption[0].key_id
}

# Primary State Bucket
resource "aws_s3_bucket" "terraform_state" {
  count = var.deploy_to_aws ? 1 : 0
  
  bucket        = "${local.name_prefix}-terraform-state-${random_string.bucket_suffix.result}"
  force_destroy = var.allow_state_bucket_destroy
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-terraform-state"
    Purpose = "TerraformState"
    Environment = var.environment
  })
}

# Bucket Versioning
resource "aws_s3_bucket_versioning" "terraform_state" {
  count = var.deploy_to_aws ? 1 : 0
  
  bucket = aws_s3_bucket.terraform_state[0].id
  versioning_configuration {
    status = var.enable_state_versioning ? "Enabled" : "Suspended"
  }
}

# Bucket Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  count = var.deploy_to_aws ? 1 : 0
  
  bucket = aws_s3_bucket.terraform_state[0].id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.enable_state_encryption ? aws_kms_key.state_encryption[0].arn : null
      sse_algorithm     = var.enable_state_encryption ? "aws:kms" : "AES256"
    }
    bucket_key_enabled = true
  }
}

# Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "terraform_state" {
  count = var.deploy_to_aws ? 1 : 0
  
  bucket = aws_s3_bucket.terraform_state[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket Policy
resource "aws_s3_bucket_policy" "terraform_state" {
  count = var.deploy_to_aws ? 1 : 0
  
  bucket = aws_s3_bucket.terraform_state[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DenyUnSecureCommunications"
        Effect = "Deny"
        Principal = "*"
        Action = "s3:*"
        Resource = [
          aws_s3_bucket.terraform_state[0].arn,
          "${aws_s3_bucket.terraform_state[0].arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
      {
        Sid    = "AllowTerraformStateAccess"
        Effect = "Allow"
        Principal = {
          AWS = [
            for role in var.terraform_execution_roles :
            "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:role/${role}"
          ]
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.terraform_state[0].arn,
          "${aws_s3_bucket.terraform_state[0].arn}/*"
        ]
      }
    ]
  })
}

# Lifecycle Configuration for State Management
resource "aws_s3_bucket_lifecycle_configuration" "terraform_state" {
  count = var.deploy_to_aws ? 1 : 0
  
  bucket = aws_s3_bucket.terraform_state[0].id

  rule {
    id     = "state_lifecycle"
    status = "Enabled"

    # Keep current versions
    expiration {
      days = var.state_retention_days
    }

    # Manage non-current versions
    noncurrent_version_expiration {
      noncurrent_days = var.state_version_retention_days
    }

    # Transition old versions to cheaper storage
    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = 90
      storage_class   = "GLACIER"
    }
  }

  # Clean up incomplete multipart uploads
  rule {
    id     = "cleanup_incomplete_uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Cross-Region Replication for State Backup
resource "aws_s3_bucket" "terraform_state_replica" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? 1 : 0
  
  provider      = aws.replica
  bucket        = "${local.name_prefix}-terraform-state-replica-${random_string.bucket_suffix.result}"
  force_destroy = var.allow_state_bucket_destroy
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-terraform-state-replica"
    Purpose = "TerraformStateReplica"
  })
}

resource "aws_s3_bucket_versioning" "terraform_state_replica" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? 1 : 0
  
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_replica[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

# IAM Role for Replication
resource "aws_iam_role" "replication" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? 1 : 0
  
  name = "${local.name_prefix}-state-replication-role"

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
}

resource "aws_iam_policy" "replication" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? 1 : 0
  
  name = "${local.name_prefix}-state-replication-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObjectVersionForReplication",
          "s3:GetObjectVersionAcl"
        ]
        Effect = "Allow"
        Resource = "${aws_s3_bucket.terraform_state[0].arn}/*"
      },
      {
        Action = [
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = aws_s3_bucket.terraform_state[0].arn
      },
      {
        Action = [
          "s3:ReplicateObject",
          "s3:ReplicateDelete"
        ]
        Effect = "Allow"
        Resource = "${aws_s3_bucket.terraform_state_replica[0].arn}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "replication" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? 1 : 0
  
  role       = aws_iam_role.replication[0].name
  policy_arn = aws_iam_policy.replication[0].arn
}

# Replication Configuration
resource "aws_s3_bucket_replication_configuration" "terraform_state" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? 1 : 0
  
  role   = aws_iam_role.replication[0].arn
  bucket = aws_s3_bucket.terraform_state[0].id

  rule {
    id     = "terraform_state_replication"
    status = "Enabled"

    destination {
      bucket = aws_s3_bucket.terraform_state_replica[0].arn
      storage_class = "STANDARD_IA"
      
      encryption_configuration {
        replica_kms_key_id = aws_kms_key.state_encryption[0].arn
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.terraform_state]
}

# DynamoDB Table for State Locking per Environment
resource "aws_dynamodb_table" "terraform_locks" {
  count = var.deploy_to_aws ? length(var.environments) : 0
  
  name           = "${var.project_name}-terraform-locks-${var.environments[count.index]}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  server_side_encryption {
    enabled     = var.enable_state_encryption
    kms_key_arn = var.enable_state_encryption ? aws_kms_key.state_encryption[0].arn : null
  }

  point_in_time_recovery {
    enabled = var.enable_dynamodb_pitr
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-terraform-locks-${var.environments[count.index]}"
    Environment = var.environments[count.index]
    Purpose = "TerraformStateLocking"
  })
}

# ============================================================================
# STATE BACKUP AND MONITORING
# ============================================================================

# Lambda for State Backup Management
resource "aws_lambda_function" "state_backup_manager" {
  count = var.deploy_to_aws && var.enable_state_backup ? 1 : 0
  
  filename         = data.archive_file.state_backup_zip[0].output_path
  function_name    = "${local.name_prefix}-state-backup-manager"
  role            = aws_iam_role.state_backup_lambda[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 512
  
  environment {
    variables = {
      STATE_BUCKET       = aws_s3_bucket.terraform_state[0].bucket
      BACKUP_BUCKET      = aws_s3_bucket.state_backups[0].bucket
      ENVIRONMENTS       = jsonencode(var.environments)
      RETENTION_DAYS     = var.backup_retention_days
      NOTIFICATION_TOPIC = aws_sns_topic.state_management_alerts[0].arn
    }
  }
  
  tags = var.tags
}

data "archive_file" "state_backup_zip" {
  count = var.deploy_to_aws && var.enable_state_backup ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/state_backup_manager.zip"
  
  source {
    content = templatefile("${path.module}/templates/state_backup_manager.py.tpl", {
      project_name = var.project_name
    })
    filename = "index.py"
  }
}

# S3 Bucket for State Backups
resource "aws_s3_bucket" "state_backups" {
  count = var.deploy_to_aws && var.enable_state_backup ? 1 : 0
  
  bucket        = "${local.name_prefix}-state-backups-${random_string.bucket_suffix.result}"
  force_destroy = var.allow_state_bucket_destroy
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-state-backups"
    Purpose = "TerraformStateBackups"
  })
}

resource "aws_s3_bucket_versioning" "state_backups" {
  count = var.deploy_to_aws && var.enable_state_backup ? 1 : 0
  
  bucket = aws_s3_bucket.state_backups[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state_backups" {
  count = var.deploy_to_aws && var.enable_state_backup ? 1 : 0
  
  bucket = aws_s3_bucket.state_backups[0].id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.state_encryption[0].arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

# IAM Role for State Backup Lambda
resource "aws_iam_role" "state_backup_lambda" {
  count = var.deploy_to_aws && var.enable_state_backup ? 1 : 0
  
  name = "${local.name_prefix}-state-backup-lambda-role"

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
}

resource "aws_iam_role_policy" "state_backup_lambda" {
  count = var.deploy_to_aws && var.enable_state_backup ? 1 : 0
  
  name = "${local.name_prefix}-state-backup-lambda-policy"
  role = aws_iam_role.state_backup_lambda[0].id

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
          aws_s3_bucket.terraform_state[0].arn,
          "${aws_s3_bucket.terraform_state[0].arn}/*",
          aws_s3_bucket.state_backups[0].arn,
          "${aws_s3_bucket.state_backups[0].arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = [aws_kms_key.state_encryption[0].arn]
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = [aws_sns_topic.state_management_alerts[0].arn]
      }
    ]
  })
}

# EventBridge Rules for Backup Scheduling
resource "aws_cloudwatch_event_rule" "state_backup_schedule" {
  for_each = var.deploy_to_aws && var.enable_state_backup ? var.backup_schedules : {}
  
  name                = "${local.name_prefix}-state-backup-${each.key}"
  description         = "Trigger state backup ${each.key}"
  schedule_expression = local.backup_schedules[each.value]
  
  tags = var.tags
}

resource "aws_cloudwatch_event_target" "state_backup_lambda" {
  for_each = var.deploy_to_aws && var.enable_state_backup ? var.backup_schedules : {}
  
  rule      = aws_cloudwatch_event_rule.state_backup_schedule[each.key].name
  target_id = "StateBackupLambdaTarget"
  arn       = aws_lambda_function.state_backup_manager[0].arn
  
  input = jsonencode({
    backup_type = each.key
    schedule = each.value
  })
}

resource "aws_lambda_permission" "allow_eventbridge_backup" {
  for_each = var.deploy_to_aws && var.enable_state_backup ? var.backup_schedules : {}
  
  statement_id  = "AllowExecutionFromEventBridge${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.state_backup_manager[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.state_backup_schedule[each.key].arn
}

# ============================================================================
# STATE MONITORING AND ALERTING
# ============================================================================

# SNS Topic for State Management Alerts
resource "aws_sns_topic" "state_management_alerts" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-state-management-alerts"
  
  tags = var.tags
}

resource "aws_sns_topic_subscription" "state_management_email" {
  count = var.deploy_to_aws && var.alert_email != "" ? 1 : 0
  
  topic_arn = aws_sns_topic.state_management_alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# CloudWatch Alarms for State Management
resource "aws_cloudwatch_metric_alarm" "state_bucket_errors" {
  count = var.deploy_to_aws ? 1 : 0
  
  alarm_name          = "${local.name_prefix}-state-bucket-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "4xxErrors"
  namespace           = "AWS/S3"
  period              = "300"
  statistic           = "Sum"
  threshold           = "5"
  alarm_description   = "High error rate on Terraform state bucket"
  alarm_actions       = [aws_sns_topic.state_management_alerts[0].arn]
  
  dimensions = {
    BucketName = aws_s3_bucket.terraform_state[0].bucket
  }
  
  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "state_lock_table_throttle" {
  count = var.deploy_to_aws ? length(var.environments) : 0
  
  alarm_name          = "${local.name_prefix}-${var.environments[count.index]}-lock-throttle"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "ThrottledRequests"
  namespace           = "AWS/DynamoDB"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "DynamoDB throttling on state lock table"
  alarm_actions       = [aws_sns_topic.state_management_alerts[0].arn]
  
  dimensions = {
    TableName = aws_dynamodb_table.terraform_locks[count.index].name
  }
  
  tags = var.tags
}

# CloudWatch Dashboard for State Management
resource "aws_cloudwatch_dashboard" "state_management" {
  count = var.deploy_to_aws ? 1 : 0
  
  dashboard_name = "${local.name_prefix}-state-management"
  
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
            ["AWS/S3", "BucketSizeBytes", "BucketName", aws_s3_bucket.terraform_state[0].bucket, "StorageType", "StandardStorage"],
            ["AWS/S3", "NumberOfObjects", "BucketName", aws_s3_bucket.terraform_state[0].bucket, "StorageType", "AllStorageTypes"]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Terraform State Bucket Metrics"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        
        properties = {
          metrics = [
            for i, env in var.environments : [
              "AWS/DynamoDB", "ConsumedReadCapacityUnits", "TableName", aws_dynamodb_table.terraform_locks[i].name
            ]
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "State Lock Table Usage"
          period = 300
        }
      }
    ]
  })
}

# ============================================================================
# WORKSPACE MANAGEMENT AUTOMATION
# ============================================================================

# Lambda for Workspace Management
resource "aws_lambda_function" "workspace_manager" {
  count = var.deploy_to_aws && var.enable_workspace_isolation ? 1 : 0
  
  filename         = data.archive_file.workspace_manager_zip[0].output_path
  function_name    = "${local.name_prefix}-workspace-manager"
  role            = aws_iam_role.workspace_manager_lambda[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  
  environment {
    variables = {
      STATE_BUCKET = aws_s3_bucket.terraform_state[0].bucket
      LOCK_TABLES  = jsonencode([for table in aws_dynamodb_table.terraform_locks : table.name])
      WORKSPACES   = jsonencode(local.workspaces)
    }
  }
  
  tags = var.tags
}

data "archive_file" "workspace_manager_zip" {
  count = var.deploy_to_aws && var.enable_workspace_isolation ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/workspace_manager.zip"
  
  source {
    content = templatefile("${path.module}/templates/workspace_manager.py.tpl", {
      project_name = var.project_name
    })
    filename = "index.py"
  }
}

# IAM Role for Workspace Manager
resource "aws_iam_role" "workspace_manager_lambda" {
  count = var.deploy_to_aws && var.enable_workspace_isolation ? 1 : 0
  
  name = "${local.name_prefix}-workspace-manager-role"

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
}

resource "aws_iam_role_policy" "workspace_manager_lambda" {
  count = var.deploy_to_aws && var.enable_workspace_isolation ? 1 : 0
  
  name = "${local.name_prefix}-workspace-manager-policy"
  role = aws_iam_role.workspace_manager_lambda[0].id

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
          aws_s3_bucket.terraform_state[0].arn,
          "${aws_s3_bucket.terraform_state[0].arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          for table in aws_dynamodb_table.terraform_locks : table.arn
        ]
      }
    ]
  })
}

# Random string for bucket naming
resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Data sources
data "aws_caller_identity" "current" {
  count = var.deploy_to_aws ? 1 : 0
}