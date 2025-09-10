# BMAD Platform - Medallion Data Lakehouse Architecture
# Bronze → Silver → Gold layers with ACID transactions and petabyte-scale processing

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
      version = "~> 4.80"
    }
  }
}

locals {
  # Data lake layers configuration
  layers = {
    bronze = {
      description = "Raw data ingestion layer"
      retention_days = var.bronze_retention_days
      storage_class = "STANDARD"
      compression = "GZIP"
    }
    silver = {
      description = "Cleaned and validated data layer"
      retention_days = var.silver_retention_days
      storage_class = "STANDARD_IA"
      compression = "SNAPPY"
    }
    gold = {
      description = "Business-ready aggregated data layer"
      retention_days = var.gold_retention_days
      storage_class = "STANDARD"
      compression = "PARQUET"
    }
  }
  
  # Data partitioning strategy
  partition_keys = ["year", "month", "day", "hour"]
  
  # Data lake zones
  zones = {
    landing    = "landing"
    processing = "processing"
    curated    = "curated"
    sandbox    = "sandbox"
    archive    = "archive"
  }
  
  common_tags = var.tags
}

# AWS S3 Data Lake Implementation
resource "aws_s3_bucket" "data_lake_bronze" {
  count  = var.aws_s3_bucket != null ? 1 : 0
  bucket = "${var.project_name}-${var.environment}-bronze-data-lake"
  
  tags = merge(local.common_tags, {
    Layer = "Bronze"
    Purpose = "Raw Data Ingestion"
  })
}

resource "aws_s3_bucket" "data_lake_silver" {
  count  = var.aws_s3_bucket != null ? 1 : 0
  bucket = "${var.project_name}-${var.environment}-silver-data-lake"
  
  tags = merge(local.common_tags, {
    Layer = "Silver"
    Purpose = "Cleaned and Validated Data"
  })
}

resource "aws_s3_bucket" "data_lake_gold" {
  count  = var.aws_s3_bucket != null ? 1 : 0
  bucket = "${var.project_name}-${var.environment}-gold-data-lake"
  
  tags = merge(local.common_tags, {
    Layer = "Gold"
    Purpose = "Business-Ready Data"
  })
}

# S3 Bucket Configurations
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  count  = var.enable_versioning && var.aws_s3_bucket != null ? 3 : 0
  bucket = [
    aws_s3_bucket.data_lake_bronze[0].id,
    aws_s3_bucket.data_lake_silver[0].id,
    aws_s3_bucket.data_lake_gold[0].id
  ][count.index]
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_encryption" "data_lake_encryption" {
  count  = var.enable_encryption && var.aws_s3_bucket != null ? 3 : 0
  bucket = [
    aws_s3_bucket.data_lake_bronze[0].id,
    aws_s3_bucket.data_lake_silver[0].id,
    aws_s3_bucket.data_lake_gold[0].id
  ][count.index]

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
      bucket_key_enabled = true
    }
  }
}

# Lifecycle Management for Cost Optimization
resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  count  = var.aws_s3_bucket != null ? 1 : 0
  bucket = aws_s3_bucket.data_lake_bronze[0].id

  rule {
    id     = "bronze_lifecycle"
    status = "Enabled"

    expiration {
      days = local.layers.bronze.retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 60
      storage_class = "GLACIER"
    }

    transition {
      days          = 180
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver_lifecycle" {
  count  = var.aws_s3_bucket != null ? 1 : 0
  bucket = aws_s3_bucket.data_lake_silver[0].id

  rule {
    id     = "silver_lifecycle"
    status = "Enabled"

    expiration {
      days = local.layers.silver.retention_days
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "gold_lifecycle" {
  count  = var.aws_s3_bucket != null ? 1 : 0
  bucket = aws_s3_bucket.data_lake_gold[0].id

  rule {
    id     = "gold_lifecycle"
    status = "Enabled"

    expiration {
      days = local.layers.gold.retention_days
    }

    transition {
      days          = 365
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 1095  # 3 years
      storage_class = "GLACIER"
    }
  }
}

# Azure Data Lake Storage Gen2
resource "azurerm_storage_account" "data_lake" {
  count               = var.azure_storage_account != null ? 1 : 0
  name                = "${var.project_name}${var.environment}datalake"
  resource_group_name = var.azure_resource_group_name
  location           = var.azure_location
  
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled          = true  # Hierarchical Namespace for Data Lake Gen2
  
  blob_properties {
    versioning_enabled = var.enable_versioning
    change_feed_enabled = true
    
    delete_retention_policy {
      days = 30
    }
    
    container_delete_retention_policy {
      days = 30
    }
  }
  
  tags = merge(local.common_tags, {
    Purpose = "Data Lake Storage"
  })
}

# Azure Storage Containers for each layer
resource "azurerm_storage_container" "bronze" {
  count                 = var.azure_storage_account != null ? 1 : 0
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.data_lake[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  count                 = var.azure_storage_account != null ? 1 : 0
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.data_lake[0].name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  count                 = var.azure_storage_account != null ? 1 : 0
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.data_lake[0].name
  container_access_type = "private"
}

# Google Cloud Storage Data Lake
resource "google_storage_bucket" "data_lake_bronze" {
  count    = var.gcp_storage_bucket != null ? 1 : 0
  name     = "${var.project_name}-${var.environment}-bronze-data-lake"
  location = var.gcp_region
  
  storage_class = "STANDARD"
  
  versioning {
    enabled = var.enable_versioning
  }
  
  encryption {
    default_kms_key_name = var.gcp_kms_key_name
  }
  
  lifecycle_rule {
    condition {
      age = local.layers.bronze.retention_days
    }
    action {
      type = "Delete"
    }
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
  
  labels = local.common_tags
}

resource "google_storage_bucket" "data_lake_silver" {
  count    = var.gcp_storage_bucket != null ? 1 : 0
  name     = "${var.project_name}-${var.environment}-silver-data-lake"
  location = var.gcp_region
  
  storage_class = "STANDARD"
  
  versioning {
    enabled = var.enable_versioning
  }
  
  encryption {
    default_kms_key_name = var.gcp_kms_key_name
  }
  
  lifecycle_rule {
    condition {
      age = local.layers.silver.retention_days
    }
    action {
      type = "Delete"
    }
  }
  
  labels = local.common_tags
}

resource "google_storage_bucket" "data_lake_gold" {
  count    = var.gcp_storage_bucket != null ? 1 : 0
  name     = "${var.project_name}-${var.environment}-gold-data-lake"
  location = var.gcp_region
  
  storage_class = "STANDARD"
  
  versioning {
    enabled = var.enable_versioning
  }
  
  encryption {
    default_kms_key_name = var.gcp_kms_key_name
  }
  
  lifecycle_rule {
    condition {
      age = local.layers.gold.retention_days
    }
    action {
      type = "Delete"
    }
  }
  
  labels = local.common_tags
}

# Delta Lake Configuration
resource "kubernetes_config_map" "delta_lake_config" {
  metadata {
    name      = "delta-lake-config"
    namespace = "data-platform"
  }

  data = {
    "delta-lake.conf" = templatefile("${path.module}/templates/delta-lake.conf.tpl", {
      aws_bronze_bucket    = var.aws_s3_bucket != null ? aws_s3_bucket.data_lake_bronze[0].bucket : ""
      aws_silver_bucket    = var.aws_s3_bucket != null ? aws_s3_bucket.data_lake_silver[0].bucket : ""
      aws_gold_bucket      = var.aws_s3_bucket != null ? aws_s3_bucket.data_lake_gold[0].bucket : ""
      azure_account_name   = var.azure_storage_account != null ? azurerm_storage_account.data_lake[0].name : ""
      gcp_bronze_bucket    = var.gcp_storage_bucket != null ? google_storage_bucket.data_lake_bronze[0].name : ""
      gcp_silver_bucket    = var.gcp_storage_bucket != null ? google_storage_bucket.data_lake_silver[0].name : ""
      gcp_gold_bucket      = var.gcp_storage_bucket != null ? google_storage_bucket.data_lake_gold[0].name : ""
    })
  }
}

# Apache Iceberg Configuration
resource "kubernetes_config_map" "iceberg_config" {
  metadata {
    name      = "iceberg-config"
    namespace = "data-platform"
  }

  data = {
    "iceberg.properties" = templatefile("${path.module}/templates/iceberg.properties.tpl", {
      warehouse_path      = "s3a://${var.aws_s3_bucket != null ? aws_s3_bucket.data_lake_gold[0].bucket : ""}/"
      catalog_type       = "hive"
      metastore_uri      = "thrift://hive-metastore.data-platform.svc.cluster.local:9083"
    })
  }
}

# Data Quality Framework Configuration
resource "kubernetes_config_map" "data_quality_config" {
  metadata {
    name      = "data-quality-config"
    namespace = "data-platform"
  }

  data = {
    "great-expectations.yml" = templatefile("${path.module}/templates/great-expectations.yml.tpl", {
      bronze_bucket = var.aws_s3_bucket != null ? aws_s3_bucket.data_lake_bronze[0].bucket : ""
      silver_bucket = var.aws_s3_bucket != null ? aws_s3_bucket.data_lake_silver[0].bucket : ""
      gold_bucket   = var.aws_s3_bucket != null ? aws_s3_bucket.data_lake_gold[0].bucket : ""
    })
  }
}

# Data Catalog Configuration (AWS Glue)
resource "aws_glue_catalog_database" "bronze" {
  count = var.aws_s3_bucket != null ? 1 : 0
  name  = "${var.project_name}_${var.environment}_bronze_catalog"
  
  description = "Bronze layer data catalog for raw data"
  
  parameters = {
    classification = "bronze"
    layer         = "raw"
  }
}

resource "aws_glue_catalog_database" "silver" {
  count = var.aws_s3_bucket != null ? 1 : 0
  name  = "${var.project_name}_${var.environment}_silver_catalog"
  
  description = "Silver layer data catalog for cleaned data"
  
  parameters = {
    classification = "silver"
    layer         = "cleaned"
  }
}

resource "aws_glue_catalog_database" "gold" {
  count = var.aws_s3_bucket != null ? 1 : 0
  name  = "${var.project_name}_${var.environment}_gold_catalog"
  
  description = "Gold layer data catalog for business-ready data"
  
  parameters = {
    classification = "gold"
    layer         = "curated"
  }
}

# Data Lineage Tracking
resource "kubernetes_deployment" "apache_atlas" {
  metadata {
    name      = "apache-atlas"
    namespace = "data-platform"
    labels = {
      app = "apache-atlas"
    }
  }

  spec {
    replicas = 2

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
            name  = "JAVA_OPTS"
            value = "-Xms2g -Xmx4g"
          }
          
          resources {
            requests = {
              cpu    = "1000m"
              memory = "4Gi"
            }
            limits = {
              cpu    = "2000m"
              memory = "8Gi"
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
            name = "atlas-config"
          }
        }
      }
    }
  }
}

# Data Processing Engine (Apache Spark)
resource "kubernetes_deployment" "spark_history_server" {
  metadata {
    name      = "spark-history-server"
    namespace = "data-platform"
    labels = {
      app = "spark-history-server"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "spark-history-server"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark-history-server"
        }
      }

      spec {
        container {
          image = "apache/spark:3.5.0"
          name  = "spark-history-server"
          
          command = [
            "/opt/spark/bin/spark-class",
            "org.apache.spark.deploy.history.HistoryServer"
          ]
          
          port {
            container_port = 18080
          }
          
          env {
            name  = "SPARK_HISTORY_OPTS"
            value = "-Dspark.history.fs.logDirectory=s3a://${var.aws_s3_bucket != null ? aws_s3_bucket.data_lake_bronze[0].bucket : ""}/spark-logs"
          }
          
          resources {
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
            limits = {
              cpu    = "1000m"
              memory = "2Gi"
            }
          }
        }
      }
    }
  }
}

# Data Schema Registry
resource "kubernetes_deployment" "schema_registry" {
  metadata {
    name      = "schema-registry"
    namespace = "data-platform"
    labels = {
      app = "schema-registry"
    }
  }

  spec {
    replicas = 3

    selector {
      match_labels = {
        app = "schema-registry"
      }
    }

    template {
      metadata {
        labels = {
          app = "schema-registry"
        }
      }

      spec {
        container {
          image = "confluentinc/cp-schema-registry:7.4.0"
          name  = "schema-registry"
          
          port {
            container_port = 8081
          }
          
          env {
            name  = "SCHEMA_REGISTRY_HOST_NAME"
            value = "schema-registry"
          }
          
          env {
            name  = "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"
            value = "kafka-cluster.kafka.svc.cluster.local:9092"
          }
          
          env {
            name  = "SCHEMA_REGISTRY_LISTENERS"
            value = "http://0.0.0.0:8081"
          }
          
          resources {
            requests = {
              cpu    = "200m"
              memory = "512Mi"
            }
            limits = {
              cpu    = "1000m"
              memory = "2Gi"
            }
          }
        }
      }
    }
  }
}

# ACID Transaction Coordinator (Delta Lake)
resource "kubernetes_deployment" "delta_sharing_server" {
  metadata {
    name      = "delta-sharing-server"
    namespace = "data-platform"
    labels = {
      app = "delta-sharing-server"
    }
  }

  spec {
    replicas = 2

    selector {
      match_labels = {
        app = "delta-sharing-server"
      }
    }

    template {
      metadata {
        labels = {
          app = "delta-sharing-server"
        }
      }

      spec {
        container {
          image = "deltaio/delta-sharing-server:1.0.1"
          name  = "delta-sharing-server"
          
          port {
            container_port = 8080
          }
          
          env {
            name  = "DELTA_SHARING_CONFIG_PATH"
            value = "/etc/delta-sharing/server.yaml"
          }
          
          resources {
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
            limits = {
              cpu    = "2000m"
              memory = "4Gi"
            }
          }
          
          volume_mount {
            name       = "delta-sharing-config"
            mount_path = "/etc/delta-sharing"
          }
        }
        
        volume {
          name = "delta-sharing-config"
          config_map {
            name = "delta-sharing-config"
          }
        }
      }
    }
  }
}