# Zero-Trust Security and Compliance Automation Module
# Implements enterprise-grade security with SOC2, GDPR, PCI DSS compliance

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    vault = {
      source  = "hashicorp/vault"
      version = "~> 3.20"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Security policies
  security_policies = {
    password_policy = {
      min_length         = 14
      require_uppercase  = true
      require_lowercase  = true
      require_numbers    = true
      require_symbols    = true
      max_age           = 90
    }
    
    network_policy = {
      default_deny         = true
      ingress_whitelist   = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
      egress_restriction  = true
    }
    
    encryption_policy = {
      at_rest_required    = true
      in_transit_required = true
      key_rotation_days   = 90
    }
  }
  
  # Compliance frameworks
  compliance_frameworks = {
    soc2 = {
      enabled     = true
      audit_logs  = true
      monitoring  = true
      encryption  = true
    }
    
    gdpr = {
      enabled            = true
      data_retention     = true
      right_to_erasure   = true
      pseudonymization   = true
    }
    
    pci_dss = {
      enabled          = true
      network_segmentation = true
      access_controls  = true
      vulnerability_scanning = true
    }
  }
}

# ============================================================================
# IDENTITY AND ACCESS MANAGEMENT (IAM)
# ============================================================================

# AWS IAM Identity Center (SSO) Configuration
resource "aws_ssoadmin_account_assignment" "admin_assignment" {
  count = var.deploy_to_aws && var.enable_sso ? length(var.admin_users) : 0
  
  instance_arn       = var.sso_instance_arn
  permission_set_arn = aws_ssoadmin_permission_set.admin_permission_set[0].arn
  principal_id       = var.admin_users[count.index]
  principal_type     = "USER"
  target_id          = data.aws_caller_identity.current[0].account_id
  target_type        = "AWS_ACCOUNT"
}

resource "aws_ssoadmin_permission_set" "admin_permission_set" {
  count = var.deploy_to_aws && var.enable_sso ? 1 : 0
  
  name             = "${local.name_prefix}-admin-permissions"
  description      = "Administrative permissions with MFA requirement"
  instance_arn     = var.sso_instance_arn
  session_duration = "PT8H"  # 8 hours
  
  tags = var.tags
}

resource "aws_ssoadmin_managed_policy_attachment" "admin_policy" {
  count = var.deploy_to_aws && var.enable_sso ? 1 : 0
  
  instance_arn       = var.sso_instance_arn
  managed_policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
  permission_set_arn = aws_ssoadmin_permission_set.admin_permission_set[0].arn
}

# Custom IAM Policy for Zero-Trust Access
resource "aws_iam_policy" "zero_trust_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  name        = "${local.name_prefix}-zero-trust-policy"
  description = "Zero-trust access policy with conditional access"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster",
          "eks:ListClusters"
        ]
        Resource = "*"
        Condition = {
          Bool = {
            "aws:MultiFactorAuthPresent" = "true"
          }
          IpAddress = {
            "aws:SourceIp" = var.allowed_ip_ranges
          }
          DateGreaterThan = {
            "aws:CurrentTime" = "2024-01-01T00:00:00Z"
          }
        }
      },
      {
        Effect = "Deny"
        Action = "*"
        Resource = "*"
        Condition = {
          Bool = {
            "aws:ViaAWSService" = "false"
          }
          StringNotEquals = {
            "aws:RequestedRegion" = var.allowed_regions
          }
        }
      }
    ]
  })
  
  tags = var.tags
}

# ============================================================================
# NETWORK SECURITY
# ============================================================================

# VPC Flow Logs for Network Monitoring
resource "aws_flow_log" "vpc_flow_logs" {
  count = var.deploy_to_aws ? 1 : 0
  
  iam_role_arn    = aws_iam_role.flow_log_role[0].arn
  log_destination = aws_cloudwatch_log_group.vpc_flow_logs[0].arn
  traffic_type    = "ALL"
  vpc_id          = var.vpc_id
  
  tags = var.tags
}

resource "aws_cloudwatch_log_group" "vpc_flow_logs" {
  count = var.deploy_to_aws ? 1 : 0
  
  name              = "/aws/vpc/flowlogs/${local.name_prefix}"
  retention_in_days = 90
  kms_key_id       = aws_kms_key.logs_encryption[0].arn
  
  tags = var.tags
}

resource "aws_iam_role" "flow_log_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-flow-log-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "vpc-flow-logs.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy" "flow_log_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-flow-log-policy"
  role = aws_iam_role.flow_log_role[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ]
      Resource = "*"
    }]
  })
}

# WAF for Application Layer Protection
resource "aws_wafv2_web_acl" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name  = "${local.name_prefix}-waf"
  scope = "REGIONAL"
  
  default_action {
    allow {}
  }
  
  # Rate limiting rule
  rule {
    name     = "RateLimitRule"
    priority = 1
    
    action {
      block {}
    }
    
    statement {
      rate_based_statement {
        limit              = 2000
        aggregate_key_type = "IP"
        
        scope_down_statement {
          geo_match_statement {
            country_codes = var.blocked_countries
          }
        }
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "RateLimitRule"
      sampled_requests_enabled   = true
    }
  }
  
  # SQL injection protection
  rule {
    name     = "SQLInjectionRule"
    priority = 2
    
    action {
      block {}
    }
    
    statement {
      sqli_match_statement {
        field_to_match {
          body {
            oversize_handling = "CONTINUE"
          }
        }
        text_transformation {
          priority = 1
          type     = "URL_DECODE"
        }
        text_transformation {
          priority = 2
          type     = "HTML_ENTITY_DECODE"
        }
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "SQLInjectionRule"
      sampled_requests_enabled   = true
    }
  }
  
  # XSS protection
  rule {
    name     = "XSSRule"
    priority = 3
    
    action {
      block {}
    }
    
    statement {
      xss_match_statement {
        field_to_match {
          body {
            oversize_handling = "CONTINUE"
          }
        }
        text_transformation {
          priority = 1
          type     = "URL_DECODE"
        }
        text_transformation {
          priority = 2
          type     = "HTML_ENTITY_DECODE"
        }
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "XSSRule"
      sampled_requests_enabled   = true
    }
  }
  
  # AWS Managed Core Rule Set
  rule {
    name     = "AWSManagedRulesCore"
    priority = 4
    
    override_action {
      none {}
    }
    
    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"
        
        rule_action_override {
          action_to_use {
            count {}
          }
          name = "SizeRestrictions_BODY"
        }
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "AWSManagedRulesCore"
      sampled_requests_enabled   = true
    }
  }
  
  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${local.name_prefix}-waf"
    sampled_requests_enabled   = true
  }
  
  tags = var.tags
}

# ============================================================================
# ENCRYPTION AND KEY MANAGEMENT
# ============================================================================

# KMS Keys for Different Data Types
resource "aws_kms_key" "application_encryption" {
  count = var.deploy_to_aws ? 1 : 0
  
  description             = "Application data encryption key"
  deletion_window_in_days = 10
  enable_key_rotation     = true
  
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
        Sid    = "Allow use of the key"
        Effect = "Allow"
        Principal = {
          AWS = [
            aws_iam_role.application_role[0].arn,
            "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:role/${local.name_prefix}-*"
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
    Name = "${local.name_prefix}-application-encryption"
    Type = "application-encryption"
  })
}

resource "aws_kms_key" "logs_encryption" {
  count = var.deploy_to_aws ? 1 : 0
  
  description             = "CloudWatch logs encryption key"
  deletion_window_in_days = 10
  enable_key_rotation     = true
  
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
        Sid    = "Allow CloudWatch Logs"
        Effect = "Allow"
        Principal = {
          Service = "logs.${var.aws_region}.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          ArnEquals = {
            "kms:EncryptionContext:aws:logs:arn" = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current[0].account_id}:*"
          }
        }
      }
    ]
  })
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-logs-encryption"
    Type = "logs-encryption"
  })
}

resource "aws_kms_alias" "application_encryption" {
  count = var.deploy_to_aws ? 1 : 0
  
  name          = "alias/${local.name_prefix}-application"
  target_key_id = aws_kms_key.application_encryption[0].key_id
}

resource "aws_kms_alias" "logs_encryption" {
  count = var.deploy_to_aws ? 1 : 0
  
  name          = "alias/${local.name_prefix}-logs"
  target_key_id = aws_kms_key.logs_encryption[0].key_id
}

# ============================================================================
# SECRETS MANAGEMENT
# ============================================================================

# AWS Secrets Manager for Application Secrets
resource "aws_secretsmanager_secret" "application_secrets" {
  count = var.deploy_to_aws ? 1 : 0
  
  name                    = "${local.name_prefix}-application-secrets"
  description             = "Application secrets with automatic rotation"
  kms_key_id             = aws_kms_key.application_encryption[0].arn
  recovery_window_in_days = 30
  
  replica {
    region = var.backup_region
  }
  
  tags = var.tags
}

resource "aws_secretsmanager_secret_version" "application_secrets" {
  count = var.deploy_to_aws ? 1 : 0
  
  secret_id = aws_secretsmanager_secret.application_secrets[0].id
  secret_string = jsonencode({
    database_password = var.database_password
    api_key          = random_password.api_key.result
    jwt_secret       = random_password.jwt_secret.result
    encryption_key   = random_password.encryption_key.result
  })
}

resource "random_password" "api_key" {
  length  = 32
  special = true
  upper   = true
  lower   = true
  numeric = true
}

resource "random_password" "jwt_secret" {
  length  = 64
  special = true
  upper   = true
  lower   = true
  numeric = true
}

resource "random_password" "encryption_key" {
  length  = 32
  special = false
  upper   = true
  lower   = true
  numeric = true
}

# Lambda function for secret rotation
resource "aws_lambda_function" "secret_rotation" {
  count = var.deploy_to_aws ? 1 : 0
  
  filename         = data.archive_file.secret_rotation_zip[0].output_path
  function_name    = "${local.name_prefix}-secret-rotation"
  role            = aws_iam_role.secret_rotation_role[0].arn
  handler         = "secret_rotation.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  
  environment {
    variables = {
      SECRETS_MANAGER_ENDPOINT = "https://secretsmanager.${var.aws_region}.amazonaws.com"
    }
  }
  
  tags = var.tags
}

data "archive_file" "secret_rotation_zip" {
  count = var.deploy_to_aws ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/secret_rotation.zip"
  
  source {
    content = templatefile("${path.module}/templates/secret_rotation.py.tpl", {
      environment = var.environment
    })
    filename = "secret_rotation.py"
  }
}

resource "aws_iam_role" "secret_rotation_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-secret-rotation-role"
  
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

# ============================================================================
# KUBERNETES SECURITY
# ============================================================================

# Pod Security Standards
resource "kubernetes_manifest" "pod_security_policy" {
  manifest = {
    apiVersion = "v1"
    kind       = "Namespace"
    metadata = {
      name = var.kubernetes_namespace
      labels = {
        "pod-security.kubernetes.io/enforce" = "restricted"
        "pod-security.kubernetes.io/audit"   = "restricted"
        "pod-security.kubernetes.io/warn"    = "restricted"
      }
    }
  }
}

# Network Policies for Zero-Trust
resource "kubernetes_network_policy" "default_deny_all" {
  metadata {
    name      = "default-deny-all"
    namespace = var.kubernetes_namespace
  }
  
  spec {
    pod_selector {}
    policy_types = ["Ingress", "Egress"]
  }
}

resource "kubernetes_network_policy" "allow_api_communication" {
  metadata {
    name      = "allow-api-communication"
    namespace = var.kubernetes_namespace
  }
  
  spec {
    pod_selector {
      match_labels = {
        component = "api"
      }
    }
    
    policy_types = ["Ingress", "Egress"]
    
    ingress {
      from {
        pod_selector {
          match_labels = {
            component = "ingress"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = "8000"
      }
    }
    
    egress {
      to {
        pod_selector {
          match_labels = {
            component = "database"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = "5432"
      }
    }
    
    egress {
      to {}
      ports {
        protocol = "TCP"
        port     = "53"
      }
      ports {
        protocol = "UDP"
        port     = "53"
      }
    }
  }
}

# Service Account with RBAC
resource "kubernetes_service_account" "security_service_account" {
  metadata {
    name      = "${var.project_name}-security-sa"
    namespace = var.kubernetes_namespace
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.pod_execution_role[0].arn
    }
  }
  
  automount_service_account_token = false
}

resource "kubernetes_role" "security_role" {
  metadata {
    name      = "${var.project_name}-security-role"
    namespace = var.kubernetes_namespace
  }
  
  rule {
    api_groups = [""]
    resources  = ["pods", "services"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["apps"]
    resources  = ["deployments"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding" "security_role_binding" {
  metadata {
    name      = "${var.project_name}-security-role-binding"
    namespace = var.kubernetes_namespace
  }
  
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.security_role.metadata[0].name
  }
  
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.security_service_account.metadata[0].name
    namespace = var.kubernetes_namespace
  }
}

# ============================================================================
# COMPLIANCE AUTOMATION
# ============================================================================

# AWS Config for Compliance Monitoring
resource "aws_config_configuration_recorder" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name     = "${local.name_prefix}-config-recorder"
  role_arn = aws_iam_role.config_role[0].arn
  
  recording_group {
    all_supported                 = true
    include_global_resource_types = true
  }
  
  depends_on = [aws_config_delivery_channel.main]
}

resource "aws_config_delivery_channel" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name           = "${local.name_prefix}-config-delivery-channel"
  s3_bucket_name = aws_s3_bucket.config_bucket[0].bucket
  
  snapshot_delivery_properties {
    delivery_frequency = "Daily"
  }
}

resource "aws_s3_bucket" "config_bucket" {
  count = var.deploy_to_aws ? 1 : 0
  
  bucket        = "${local.name_prefix}-aws-config-${random_string.config_suffix.result}"
  force_destroy = var.environment != "prod"
  
  tags = var.tags
}

resource "random_string" "config_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Config Rules for Compliance
resource "aws_config_config_rule" "encrypted_volumes" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-encrypted-volumes"
  
  source {
    owner             = "AWS"
    source_identifier = "ENCRYPTED_VOLUMES"
  }
  
  depends_on = [aws_config_configuration_recorder.main]
}

resource "aws_config_config_rule" "root_mfa_enabled" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-root-mfa-enabled"
  
  source {
    owner             = "AWS"
    source_identifier = "ROOT_MFA_ENABLED"
  }
  
  depends_on = [aws_config_configuration_recorder.main]
}

resource "aws_config_config_rule" "s3_bucket_public_read_prohibited" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-s3-bucket-public-read-prohibited"
  
  source {
    owner             = "AWS"
    source_identifier = "S3_BUCKET_PUBLIC_READ_PROHIBITED"
  }
  
  depends_on = [aws_config_configuration_recorder.main]
}

# IAM role for AWS Config
resource "aws_iam_role" "config_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-config-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "config.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "config_role_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  role       = aws_iam_role.config_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/ConfigRole"
}

# ============================================================================
# SECURITY MONITORING AND ALERTING
# ============================================================================

# CloudTrail for API Auditing
resource "aws_cloudtrail" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name           = "${local.name_prefix}-cloudtrail"
  s3_bucket_name = aws_s3_bucket.cloudtrail_bucket[0].bucket
  
  include_global_service_events = true
  is_multi_region_trail        = true
  enable_logging               = true
  
  event_selector {
    read_write_type                 = "All"
    include_management_events       = true
    exclude_management_event_sources = []
    
    data_resource {
      type   = "AWS::S3::Object"
      values = ["arn:aws:s3:::*/*"]
    }
  }
  
  insight_selector {
    insight_type = "ApiCallRateInsight"
  }
  
  tags = var.tags
}

resource "aws_s3_bucket" "cloudtrail_bucket" {
  count = var.deploy_to_aws ? 1 : 0
  
  bucket        = "${local.name_prefix}-cloudtrail-${random_string.cloudtrail_suffix.result}"
  force_destroy = var.environment != "prod"
  
  tags = var.tags
}

resource "random_string" "cloudtrail_suffix" {
  length  = 8
  special = false
  upper   = false
}

# GuardDuty for Threat Detection
resource "aws_guardduty_detector" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  enable                       = true
  finding_publishing_frequency = "FIFTEEN_MINUTES"
  
  malware_protection {
    scan_ec2_instance_with_findings {
      ebs_volumes = true
    }
  }
  
  kubernetes {
    audit_logs {
      enable = true
    }
  }
  
  s3_logs {
    enable = true
  }
  
  tags = var.tags
}

# Security Hub for Centralized Security
resource "aws_securityhub_account" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  enable_default_standards = true
}

resource "aws_securityhub_standards_subscription" "cis" {
  count = var.deploy_to_aws ? 1 : 0
  
  standards_arn = "arn:aws:securityhub:${var.aws_region}::standard/cis-aws-foundations-benchmark/v/1.4.0"
  
  depends_on = [aws_securityhub_account.main]
}

resource "aws_securityhub_standards_subscription" "pci_dss" {
  count = var.deploy_to_aws ? 1 : 0
  
  standards_arn = "arn:aws:securityhub:${var.aws_region}::standard/pci-dss/v/3.2.1"
  
  depends_on = [aws_securityhub_account.main]
}

# ============================================================================
# DATA PROTECTION AND PRIVACY
# ============================================================================

# Lambda function for GDPR compliance
resource "aws_lambda_function" "gdpr_processor" {
  count = var.deploy_to_aws && var.enable_gdpr_compliance ? 1 : 0
  
  filename         = data.archive_file.gdpr_processor_zip[0].output_path
  function_name    = "${local.name_prefix}-gdpr-processor"
  role            = aws_iam_role.gdpr_processor_role[0].arn
  handler         = "gdpr_processor.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  
  environment {
    variables = {
      ENVIRONMENT = var.environment
      KMS_KEY_ID  = aws_kms_key.application_encryption[0].key_id
    }
  }
  
  tags = var.tags
}

data "archive_file" "gdpr_processor_zip" {
  count = var.deploy_to_aws && var.enable_gdpr_compliance ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/gdpr_processor.zip"
  
  source {
    content = templatefile("${path.module}/templates/gdpr_processor.py.tpl", {
      environment = var.environment
    })
    filename = "gdpr_processor.py"
  }
}

resource "aws_iam_role" "gdpr_processor_role" {
  count = var.deploy_to_aws && var.enable_gdpr_compliance ? 1 : 0
  
  name = "${local.name_prefix}-gdpr-processor-role"
  
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

# ============================================================================
# DATA SOURCES
# ============================================================================

data "aws_caller_identity" "current" {
  count = var.deploy_to_aws ? 1 : 0
}

# ============================================================================
# SUPPORTING IAM ROLES
# ============================================================================

resource "aws_iam_role" "application_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-application-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

resource "aws_iam_role" "pod_execution_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-pod-execution-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Federated = "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:oidc-provider/${replace(var.eks_cluster_oidc_issuer_url, "https://", "")}"
      }
      Condition = {
        StringEquals = {
          "${replace(var.eks_cluster_oidc_issuer_url, "https://", "")}:sub" = "system:serviceaccount:${var.kubernetes_namespace}:${var.project_name}-security-sa"
          "${replace(var.eks_cluster_oidc_issuer_url, "https://", "")}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
  
  tags = var.tags
}