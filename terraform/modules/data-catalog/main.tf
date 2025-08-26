# Multi-Cloud Data Catalog Module
# Implements unified data discovery and metadata management

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
  
  # Data classification levels
  classification_levels = ["public", "internal", "confidential", "restricted"]
  
  # PII categories for detection
  pii_categories = [
    "email", "phone", "ssn", "credit_card", "ip_address", 
    "driver_license", "passport", "bank_account"
  ]
}

# ============================================================================
# AWS DATA CATALOG (AWS GLUE)
# ============================================================================

# Enhanced Glue Catalog Database
resource "aws_glue_catalog_database" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name        = "${local.name_prefix}-unified-catalog"
  description = "Unified data catalog for ${var.project_name}"
  
  create_table_default_permission {
    permissions = ["ALL"]
    principal   = data.aws_caller_identity.current[0].arn
  }
  
  parameters = {
    "classification" = "unified-catalog"
    "compressionType" = "gzip"
    "typeOfData" = "file"
    "has_encrypted_data" = "true"
  }
}

# Glue Crawler for automatic schema discovery
resource "aws_glue_crawler" "data_lake_crawler" {
  count = var.deploy_to_aws && var.aws_data_lake_bucket != null ? 1 : 0
  
  database_name = aws_glue_catalog_database.main[0].name
  name          = "${local.name_prefix}-data-lake-crawler"
  role          = aws_iam_role.glue_crawler[0].arn
  
  s3_target {
    path = "s3://${var.aws_data_lake_bucket}/"
    
    exclusions = [
      "**/_temporary/**",
      "**/.spark/**",
      "**/checkpoints/**"
    ]
  }
  
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
  
  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
  
  lineage_configuration {
    crawler_lineage_settings = "ENABLE"
  }
  
  tags = var.tags
}

# Glue Data Quality Ruleset
resource "aws_glue_data_quality_ruleset" "main" {
  count = var.deploy_to_aws && var.enable_data_quality_monitoring ? 1 : 0
  
  name    = "${local.name_prefix}-data-quality-rules"
  ruleset = <<EOF
Rules = [
  ColumnCount > 0,
  IsComplete "id",
  IsUnique "id",
  ColumnDataType "created_at" = "timestamp",
  ColumnValues "status" in ["active", "inactive", "pending"],
  CustomSql "SELECT COUNT(*) FROM primary WHERE created_at > date_sub(current_date(), 1)" > 0
]
EOF
  
  target_table {
    database_name = aws_glue_catalog_database.main[0].name
    table_name    = "sales_data"
  }
  
  tags = var.tags
}

# IAM Role for Glue Crawler
resource "aws_iam_role" "glue_crawler" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-glue-crawler-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
  
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]
  
  tags = var.tags
}

# IAM Policy for S3 Access
resource "aws_iam_role_policy" "glue_s3_access" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-glue-s3-policy"
  role = aws_iam_role.glue_crawler[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        "arn:aws:s3:::${var.aws_data_lake_bucket}",
        "arn:aws:s3:::${var.aws_data_lake_bucket}/*"
      ]
    }]
  })
}

# ============================================================================
# AZURE DATA CATALOG (PURVIEW)
# ============================================================================

# Azure Purview Account
resource "azurerm_purview_account" "main" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                = "${local.name_prefix}-purview"
  resource_group_name = var.azure_resource_group_name
  location           = var.azure_location
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = var.tags
}

# ============================================================================
# GCP DATA CATALOG
# ============================================================================

# GCP Data Catalog Entry Group
resource "google_data_catalog_entry_group" "main" {
  count = var.deploy_to_gcp ? 1 : 0
  
  entry_group_id = "${local.name_prefix}-unified-catalog"
  display_name   = "Unified Data Catalog"
  description    = "Centralized data catalog for ${var.project_name}"
  
  region = var.gcp_region
}

# Data Catalog Tag Template for Data Classification
resource "google_data_catalog_tag_template" "data_classification" {
  count = var.deploy_to_gcp ? 1 : 0
  
  tag_template_id = "${local.name_prefix}-data-classification"
  display_name    = "Data Classification"
  region         = var.gcp_region
  
  fields {
    field_id     = "classification_level"
    display_name = "Classification Level"
    type {
      enum_type {
        dynamic "allowed_values" {
          for_each = local.classification_levels
          content {
            display_name = title(allowed_values.value)
          }
        }
      }
    }
    is_required = true
  }
  
  fields {
    field_id     = "data_owner"
    display_name = "Data Owner"
    type {
      primitive_type = "STRING"
    }
    is_required = true
  }
  
  fields {
    field_id     = "retention_period"
    display_name = "Retention Period (Days)"
    type {
      primitive_type = "DOUBLE"
    }
  }
  
  fields {
    field_id     = "contains_pii"
    display_name = "Contains PII"
    type {
      primitive_type = "BOOL"
    }
    is_required = true
  }
}

# Data Loss Prevention (DLP) Job for PII Detection
resource "google_data_loss_prevention_job_trigger" "pii_detection" {
  count = var.deploy_to_gcp && var.enable_pii_detection ? 1 : 0
  
  display_name = "${local.name_prefix}-pii-detection"
  description  = "Automated PII detection in data lake"
  
  triggers {
    schedule {
      recurrence_period_duration = "86400s" # Daily
    }
  }
  
  inspect_job {
    inspect_template_name = google_data_loss_prevention_inspect_template.pii_template[0].name
    
    storage_config {
      cloud_storage_options {
        file_set {
          url = "gs://${var.gcp_data_lake_bucket}/**"
        }
        
        bytes_limit_per_file = 1073741824 # 1GB
        file_types = ["CSV", "JSON", "AVRO", "PARQUET"]
      }
    }
    
    actions {
      pub_sub {
        topic = google_pubsub_topic.pii_detection_results[0].name
      }
    }
  }
  
  status = "HEALTHY"
}

# DLP Inspect Template for PII Detection
resource "google_data_loss_prevention_inspect_template" "pii_template" {
  count = var.deploy_to_gcp && var.enable_pii_detection ? 1 : 0
  
  display_name = "${local.name_prefix}-pii-template"
  description  = "Template for detecting PII in data lake"
  
  inspect_config {
    dynamic "info_types" {
      for_each = [
        "EMAIL_ADDRESS", "PHONE_NUMBER", "US_SOCIAL_SECURITY_NUMBER",
        "CREDIT_CARD_NUMBER", "IP_ADDRESS", "US_DRIVERS_LICENSE_NUMBER"
      ]
      content {
        name = info_types.value
      }
    }
    
    min_likelihood = "POSSIBLE"
    include_quote  = true
    
    limits {
      max_findings_per_request = 100
    }
  }
}

# Pub/Sub Topic for PII Detection Results
resource "google_pubsub_topic" "pii_detection_results" {
  count = var.deploy_to_gcp && var.enable_pii_detection ? 1 : 0
  
  name = "${local.name_prefix}-pii-detection-results"
  
  labels = var.tags
}

# ============================================================================
# UNIFIED METADATA MANAGEMENT
# ============================================================================

# Apache Atlas deployment on Kubernetes (for unified metadata)
resource "kubernetes_namespace" "atlas" {
  count = var.deploy_unified_catalog ? 1 : 0
  
  metadata {
    name = "${local.name_prefix}-atlas"
    
    labels = {
      purpose = "metadata-management"
      app     = "apache-atlas"
    }
  }
}

resource "kubernetes_deployment" "atlas" {
  count = var.deploy_unified_catalog ? 1 : 0
  
  metadata {
    name      = "apache-atlas"
    namespace = kubernetes_namespace.atlas[0].metadata[0].name
    
    labels = {
      app = "apache-atlas"
    }
  }
  
  spec {
    replicas = 1
    
    selector {
      match_labels = {
        app = "apache-atlas"
      }
    }
    
    template {
      metadata {
        labels = {
          app = "apache-atlas"
        }
      }
      
      spec {
        container {
          image = "apache/atlas:2.3.0"
          name  = "atlas"
          
          port {
            container_port = 21000
          }
          
          env {
            name  = "ATLAS_LOG_OPTS"
            value = "-Datlas.log.level=WARN"
          }
          
          resources {
            limits = {
              cpu    = "2000m"
              memory = "4Gi"
            }
            requests = {
              cpu    = "1000m"
              memory = "2Gi"
            }
          }
          
          volume_mount {
            name       = "atlas-config"
            mount_path = "/opt/atlas/conf"
          }
        }
        
        volume {
          name = "atlas-config"
          config_map {
            name = kubernetes_config_map.atlas_config[0].metadata[0].name
          }
        }
      }
    }
  }
  
  depends_on = [kubernetes_config_map.atlas_config]
}

# Atlas Configuration
resource "kubernetes_config_map" "atlas_config" {
  count = var.deploy_unified_catalog ? 1 : 0
  
  metadata {
    name      = "atlas-config"
    namespace = kubernetes_namespace.atlas[0].metadata[0].name
  }
  
  data = {
    "atlas-application.properties" = templatefile("${path.module}/templates/atlas-application.properties.tpl", {
      database_host     = var.database_connections.postgresql.host
      database_name     = var.database_connections.postgresql.database
      database_username = var.database_connections.postgresql.username
    })
  }
}

# Atlas Service
resource "kubernetes_service" "atlas" {
  count = var.deploy_unified_catalog ? 1 : 0
  
  metadata {
    name      = "apache-atlas"
    namespace = kubernetes_namespace.atlas[0].metadata[0].name
  }
  
  spec {
    selector = {
      app = "apache-atlas"
    }
    
    port {
      port        = 21000
      target_port = 21000
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# Data sources
data "aws_caller_identity" "current" {
  count = var.deploy_to_aws ? 1 : 0
}