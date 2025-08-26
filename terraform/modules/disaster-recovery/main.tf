# Multi-Cloud Disaster Recovery Module
# Implements comprehensive DR strategy across cloud providers

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
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # DR strategy based on RTO/RPO requirements
  dr_strategy = var.rto_minutes <= 30 ? "active-active" : var.rto_minutes <= 120 ? "active-passive" : "backup-restore"
  
  # Replication configuration
  replication_config = {
    enabled = var.enable_cross_cloud_replication
    primary_cloud = var.primary_cloud
    secondary_cloud = var.secondary_cloud
    strategy = local.dr_strategy
  }
  
  # Recovery objectives
  recovery_objectives = {
    rto_minutes = var.rto_minutes
    rpo_minutes = var.rpo_minutes
    tier = var.rto_minutes <= 30 ? "tier1" : var.rto_minutes <= 240 ? "tier2" : "tier3"
  }
}

# ============================================================================
# AWS DISASTER RECOVERY
# ============================================================================

# AWS Backup Vault
resource "aws_backup_vault" "main" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name        = "${local.name_prefix}-backup-vault"
  kms_key_arn = aws_kms_key.backup[0].arn
  
  tags = var.tags
}

# AWS Backup Plan
resource "aws_backup_plan" "main" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-backup-plan"
  
  # Critical data - frequent backups
  rule {
    rule_name         = "critical_data_backup"
    target_vault_name = aws_backup_vault.main[0].name
    schedule          = "cron(0 */4 * * ? *)"  # Every 4 hours
    
    start_window      = 60
    completion_window = 300
    
    lifecycle {
      cold_storage_after = 7
      delete_after      = var.backup_retention_days
    }
    
    recovery_point_tags = merge(var.tags, {
      BackupType = "critical"
      RPO        = "${var.rpo_minutes}m"
    })
  }
  
  # Regular data - daily backups
  rule {
    rule_name         = "regular_data_backup"
    target_vault_name = aws_backup_vault.main[0].name
    schedule          = "cron(0 2 * * ? *)"  # Daily at 2 AM
    
    lifecycle {
      cold_storage_after = 30
      delete_after      = var.backup_retention_days
    }
    
    recovery_point_tags = merge(var.tags, {
      BackupType = "regular"
    })
  }
  
  # Archive data - weekly backups
  rule {
    rule_name         = "archive_data_backup"
    target_vault_name = aws_backup_vault.main[0].name
    schedule          = "cron(0 1 ? * SUN *)"  # Weekly on Sunday
    
    lifecycle {
      cold_storage_after = 1
      delete_after      = 2555  # 7 years
    }
    
    recovery_point_tags = merge(var.tags, {
      BackupType = "archive"
    })
  }
  
  tags = var.tags
}

# AWS Backup Selection
resource "aws_backup_selection" "main" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  iam_role_arn = aws_iam_role.backup[0].arn
  name         = "${local.name_prefix}-backup-selection"
  plan_id      = aws_backup_plan.main[0].id
  
  selection_tag {
    type  = "STRINGEQUALS"
    key   = "Environment"
    value = var.environment
  }
  
  selection_tag {
    type  = "STRINGEQUALS"
    key   = "BackupRequired"
    value = "true"
  }
}

# KMS Key for AWS Backup
resource "aws_kms_key" "backup" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  description = "KMS key for AWS Backup encryption"
  
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
        Sid    = "Allow AWS Backup to use the key"
        Effect = "Allow"
        Principal = {
          Service = "backup.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:CreateGrant"
        ]
        Resource = "*"
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_kms_alias" "backup" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name          = "alias/${local.name_prefix}-backup"
  target_key_id = aws_kms_key.backup[0].key_id
}

# IAM Role for AWS Backup
resource "aws_iam_role" "backup" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-backup-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "backup.amazonaws.com"
      }
    }]
  })
  
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForBackup",
    "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForRestores"
  ]
  
  tags = var.tags
}

# AWS Lambda for DR Orchestration
resource "aws_lambda_function" "dr_orchestrator" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  filename         = data.archive_file.dr_orchestrator_zip[0].output_path
  function_name    = "${local.name_prefix}-dr-orchestrator"
  role            = aws_iam_role.dr_orchestrator[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 900  # 15 minutes
  
  environment {
    variables = {
      PRIMARY_CLOUD     = var.primary_cloud
      SECONDARY_CLOUD   = var.secondary_cloud
      RTO_MINUTES      = var.rto_minutes
      RPO_MINUTES      = var.rpo_minutes
      DR_STRATEGY      = local.dr_strategy
      DATABASE_PRIMARY = var.database_endpoints.primary
      DATABASE_SECONDARY = var.database_endpoints.secondary
    }
  }
  
  dead_letter_config {
    target_arn = aws_sqs_queue.dr_dlq[0].arn
  }
  
  tags = var.tags
}

# Lambda function code for DR orchestration
data "archive_file" "dr_orchestrator_zip" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/dr_orchestrator.zip"
  
  source {
    content = templatefile("${path.module}/templates/dr_orchestrator.py.tpl", {
      primary_cloud   = var.primary_cloud
      secondary_cloud = var.secondary_cloud
      rto_minutes    = var.rto_minutes
    })
    filename = "index.py"
  }
}

# IAM Role for DR Orchestrator Lambda
resource "aws_iam_role" "dr_orchestrator" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-dr-orchestrator-role"
  
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
  
  tags = var.tags
}

# IAM Policy for DR Orchestrator
resource "aws_iam_role_policy" "dr_orchestrator" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-dr-orchestrator-policy"
  role = aws_iam_role.dr_orchestrator[0].id
  
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
          "backup:StartRestoreJob",
          "backup:DescribeRestoreJob",
          "backup:ListBackupJobs",
          "backup:ListRestoreJobs",
          "rds:CreateDBInstanceReadReplica",
          "rds:PromoteReadReplica",
          "rds:DescribeDBInstances",
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:GetBucketReplication",
          "ec2:DescribeInstances",
          "ec2:StartInstances",
          "ec2:StopInstances",
          "route53:ChangeResourceRecordSets",
          "route53:GetHostedZone",
          "sns:Publish",
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage"
        ]
        Resource = "*"
      }
    ]
  })
}

# SQS Dead Letter Queue for DR operations
resource "aws_sqs_queue" "dr_dlq" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-dr-dlq"
  
  message_retention_seconds = 1209600  # 14 days
  
  tags = var.tags
}

# ============================================================================
# AZURE DISASTER RECOVERY
# ============================================================================

# Azure Site Recovery Vault
resource "azurerm_recovery_services_vault" "main" {
  count = var.primary_cloud == "azure" || var.secondary_cloud == "azure" ? 1 : 0
  
  name                = "${local.name_prefix}-recovery-vault"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  sku                 = "Standard"
  
  storage_mode_type = "GeoRedundant"
  cross_region_restore_enabled = true
  
  tags = var.tags
}

# Azure Backup Policy for VMs
resource "azurerm_backup_policy_vm" "main" {
  count = var.primary_cloud == "azure" || var.secondary_cloud == "azure" ? 1 : 0
  
  name                = "${local.name_prefix}-vm-backup-policy"
  resource_group_name = var.azure_resource_group_name
  recovery_vault_name = azurerm_recovery_services_vault.main[0].name
  
  backup {
    frequency = "Daily"
    time      = "02:00"
  }
  
  retention_daily {
    count = var.backup_retention_days
  }
  
  retention_weekly {
    count    = 52  # 1 year
    weekdays = ["Sunday"]
  }
  
  retention_monthly {
    count    = 84  # 7 years
    weekdays = ["Sunday"]
    weeks    = ["First"]
  }
  
  tags = var.tags
}

# Azure Automation Account for DR
resource "azurerm_automation_account" "dr_automation" {
  count = var.primary_cloud == "azure" || var.secondary_cloud == "azure" ? 1 : 0
  
  name                = "${local.name_prefix}-dr-automation"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  sku_name           = "Basic"
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = var.tags
}

# Azure Runbook for DR Failover
resource "azurerm_automation_runbook" "dr_failover" {
  count = var.primary_cloud == "azure" || var.secondary_cloud == "azure" ? 1 : 0
  
  name                    = "${local.name_prefix}-dr-failover"
  location               = var.azure_location
  resource_group_name    = var.azure_resource_group_name
  automation_account_name = azurerm_automation_account.dr_automation[0].name
  log_verbose            = "true"
  log_progress           = "true"
  description           = "DR failover orchestration runbook"
  runbook_type          = "PowerShell"
  
  content = templatefile("${path.module}/templates/azure_dr_failover.ps1.tpl", {
    primary_cloud        = var.primary_cloud
    secondary_cloud      = var.secondary_cloud
    resource_group_name  = var.azure_resource_group_name
    recovery_vault_name  = azurerm_recovery_services_vault.main[0].name
  })
  
  tags = var.tags
}

# ============================================================================
# GCP DISASTER RECOVERY
# ============================================================================

# GCP Backup and DR Service
resource "google_compute_region_backend_service" "dr_backend" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name                  = "${local.name_prefix}-dr-backend"
  region                = var.gcp_region
  protocol              = "HTTP"
  timeout_sec           = 30
  load_balancing_scheme = "EXTERNAL"
  
  health_checks = [google_compute_health_check.dr_health_check[0].id]
  
  failover_policy {
    disable_connection_drain_on_failover = false
    drop_traffic_if_unhealthy           = true
    failover_ratio                      = 1.0
  }
}

# GCP Health Check for DR
resource "google_compute_health_check" "dr_health_check" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name               = "${local.name_prefix}-dr-health-check"
  check_interval_sec = 10
  timeout_sec        = 5
  
  http_health_check {
    port               = 80
    request_path       = "/health"
    proxy_header       = "NONE"
  }
}

# GCP Cloud Function for DR Orchestration
resource "google_cloudfunctions_function" "dr_orchestrator" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name        = "${local.name_prefix}-dr-orchestrator"
  description = "DR orchestration function"
  runtime     = "python39"
  
  available_memory_mb   = 512
  timeout              = 540
  source_archive_bucket = google_storage_bucket.dr_functions[0].name
  source_archive_object = google_storage_bucket_object.dr_orchestrator_source[0].name
  
  trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.dr_events[0].name
  }
  
  entry_point = "orchestrate_dr"
  
  environment_variables = {
    PRIMARY_CLOUD     = var.primary_cloud
    SECONDARY_CLOUD   = var.secondary_cloud
    RTO_MINUTES      = var.rto_minutes
    RPO_MINUTES      = var.rpo_minutes
    GCP_PROJECT      = var.gcp_project_id
  }
  
  labels = var.tags
}

# GCP Storage for Cloud Functions
resource "google_storage_bucket" "dr_functions" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name     = "${local.name_prefix}-dr-functions-${random_string.bucket_suffix.result}"
  location = var.gcp_region
  
  labels = var.tags
}

resource "google_storage_bucket_object" "dr_orchestrator_source" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name   = "dr-orchestrator-source.zip"
  bucket = google_storage_bucket.dr_functions[0].name
  source = data.archive_file.gcp_dr_orchestrator_zip[0].output_path
}

data "archive_file" "gcp_dr_orchestrator_zip" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/gcp_dr_orchestrator.zip"
  
  source {
    content = templatefile("${path.module}/templates/gcp_dr_orchestrator.py.tpl", {
      project_id = var.gcp_project_id
    })
    filename = "main.py"
  }
  
  source {
    content = "google-cloud-compute==1.14.0\ngoogle-cloud-sql==3.8.0\ngoogle-cloud-storage==2.10.0"
    filename = "requirements.txt"
  }
}

# GCP Pub/Sub for DR Events
resource "google_pubsub_topic" "dr_events" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name = "${local.name_prefix}-dr-events"
  
  labels = var.tags
}

# ============================================================================
# KUBERNETES DISASTER RECOVERY
# ============================================================================

# Velero Backup Configuration
resource "kubernetes_namespace" "velero" {
  count = var.enable_k8s_backup ? 1 : 0
  
  metadata {
    name = "velero"
    
    labels = {
      app = "velero"
      purpose = "disaster-recovery"
    }
  }
}

# Velero Backup Schedule
resource "kubernetes_config_map" "velero_backup_schedule" {
  count = var.enable_k8s_backup ? 1 : 0
  
  metadata {
    name      = "velero-backup-schedule"
    namespace = kubernetes_namespace.velero[0].metadata[0].name
  }
  
  data = {
    "backup-schedule.yaml" = templatefile("${path.module}/templates/velero-schedule.yaml.tpl", {
      environment     = var.environment
      backup_schedule = "0 2 * * *"  # Daily at 2 AM
      ttl_hours      = var.backup_retention_days * 24
    })
  }
}

# ============================================================================
# DR MONITORING AND ALERTING
# ============================================================================

# CloudWatch Dashboard for DR Metrics (AWS)
resource "aws_cloudwatch_dashboard" "dr_dashboard" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  dashboard_name = "${local.name_prefix}-dr-dashboard"
  
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
            ["AWS/Backup", "NumberOfBackupJobsCompleted"],
            ["AWS/Backup", "NumberOfBackupJobsFailed"],
            ["AWS/Lambda", "Duration", "FunctionName", "${local.name_prefix}-dr-orchestrator"],
            ["AWS/Lambda", "Errors", "FunctionName", "${local.name_prefix}-dr-orchestrator"]
          ]
          period = 300
          stat   = "Sum"
          region = data.aws_region.current[0].name
          title  = "DR Metrics"
        }
      }
    ]
  })
}

# SNS Topic for DR Alerts
resource "aws_sns_topic" "dr_alerts" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-dr-alerts"
  
  tags = var.tags
}

# SNS Topic Subscription for DR Alerts
resource "aws_sns_topic_subscription" "dr_alerts_email" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" && var.alert_email != null ? 1 : 0
  
  topic_arn = aws_sns_topic.dr_alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# CloudWatch Alarm for Backup Failures
resource "aws_cloudwatch_metric_alarm" "backup_failures" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  alarm_name          = "${local.name_prefix}-backup-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "NumberOfBackupJobsFailed"
  namespace           = "AWS/Backup"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors backup job failures"
  
  alarm_actions = [aws_sns_topic.dr_alerts[0].arn]
  
  tags = var.tags
}

# ============================================================================
# SHARED RESOURCES
# ============================================================================

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Data sources
data "aws_caller_identity" "current" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
}

data "aws_region" "current" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
}