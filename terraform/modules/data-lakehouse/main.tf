# Medallion Data Lakehouse Architecture Module
# Implements Bronze → Silver → Gold data layers with petabyte-scale processing

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
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Medallion layers configuration
  medallion_layers = ["bronze", "silver", "gold"]
  
  # Data zones for enterprise governance
  data_zones = {
    raw        = "bronze"
    cleansed   = "silver"
    curated    = "gold"
    sandbox    = "sandbox"
    archive    = "archive"
    sensitive  = "sensitive"
  }
  
  # Partitioning strategy
  partition_config = {
    bronze = ["year", "month", "day", "hour"]
    silver = ["year", "month", "day"]
    gold   = ["year", "month"]
  }
}

# ============================================================================
# AWS DATA LAKEHOUSE (PRIMARY)
# ============================================================================

# S3 Buckets for Medallion Architecture
resource "aws_s3_bucket" "medallion_layers" {
  for_each = var.deploy_to_aws ? toset(local.medallion_layers) : []
  
  bucket = "${local.name_prefix}-${each.key}-${random_string.bucket_suffix.result}"
  
  tags = merge(var.tags, {
    Name         = "${local.name_prefix}-${each.key}-layer"
    Layer        = each.key
    DataZone     = each.key
    Lifecycle    = "managed"
    Replication  = "multi-region"
  })
}

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# S3 Bucket Versioning for Data Lineage
resource "aws_s3_bucket_versioning" "medallion_versioning" {
  for_each = aws_s3_bucket.medallion_layers
  
  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Intelligent Tiering for Cost Optimization
resource "aws_s3_bucket_intelligent_tiering_configuration" "medallion_tiering" {
  for_each = aws_s3_bucket.medallion_layers
  
  bucket = each.value.id
  name   = "EntireBucket"
  
  status = "Enabled"
  
  tiering {
    access_tier = "DEEP_ARCHIVE_ACCESS"
    days        = 180
  }
  
  tiering {
    access_tier = "ARCHIVE_ACCESS"
    days        = 90
  }
  
  optional_fields = ["BucketKeyStatus", "RequestPayer"]
}

# S3 Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "medallion_lifecycle" {
  for_each = aws_s3_bucket.medallion_layers
  
  bucket = each.value.id
  
  rule {
    id     = "medallion_lifecycle_${each.key}"
    status = "Enabled"
    
    expiration {
      days = each.key == "bronze" ? 2555 : (each.key == "silver" ? 1825 : 3650)  # 7, 5, 10 years
    }
    
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
    
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
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
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

# AWS Glue Data Catalog for Metadata Management
resource "aws_glue_catalog_database" "medallion_catalog" {
  for_each = var.deploy_to_aws ? toset(local.medallion_layers) : []
  
  name         = "${replace(local.name_prefix, "-", "_")}_${each.key}_catalog"
  description  = "Data catalog for ${each.key} layer of medallion architecture"
  
  create_table_default_permission {
    permissions = ["ALL"]
    
    principal = aws_iam_role.glue_service_role[0].arn
  }
  
  target_database {
    catalog_id    = data.aws_caller_identity.current[0].account_id
    database_name = "${replace(local.name_prefix, "-", "_")}_${each.key}_catalog"
  }
}

# Glue Crawlers for Schema Discovery
resource "aws_glue_crawler" "medallion_crawler" {
  for_each = var.deploy_to_aws ? toset(local.medallion_layers) : []
  
  database_name = aws_glue_catalog_database.medallion_catalog[each.key].name
  name          = "${local.name_prefix}-${each.key}-crawler"
  role          = aws_iam_role.glue_service_role[0].arn
  
  s3_target {
    path = "s3://${aws_s3_bucket.medallion_layers[each.key].bucket}"
    
    exclusions = [
      "**/_SUCCESS",
      "**/_started",
      "**/_committed*"
    ]
  }
  
  configuration = jsonencode({
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
    Version = 1.0
  })
  
  schedule = each.key == "bronze" ? "cron(0 6 * * ? *)" : "cron(0 8 * * ? *)"
  
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DELETE_FROM_DATABASE"
  }
  
  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
  
  lineage_configuration {
    crawler_lineage_settings = "ENABLE"
  }
  
  tags = var.tags
}

# Glue Service Role
resource "aws_iam_role" "glue_service_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-glue-service-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

# Glue Service Role Policies
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  role       = aws_iam_role.glue_service_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-glue-s3-access"
  role = aws_iam_role.glue_service_role[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = concat(
          [for bucket in aws_s3_bucket.medallion_layers : bucket.arn],
          [for bucket in aws_s3_bucket.medallion_layers : "${bucket.arn}/*"]
        )
      }
    ]
  })
}

# Lake Formation for Data Governance
resource "aws_lakeformation_data_lake_settings" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  admins = [aws_iam_role.glue_service_role[0].arn]
  
  default_database_permissions {
    permissions = ["ALL"]
    principal   = aws_iam_role.glue_service_role[0].arn
  }
  
  default_table_permissions {
    permissions = ["ALL"]
    principal   = aws_iam_role.glue_service_role[0].arn
  }
  
  create_database_default_permissions {
    permissions = ["ALL"]
    principal   = aws_iam_role.glue_service_role[0].arn
  }
  
  create_table_default_permissions {
    permissions = ["ALL"]
    principal   = aws_iam_role.glue_service_role[0].arn
  }
}

# EMR Cluster for Big Data Processing
resource "aws_emr_cluster" "medallion_processing" {
  count = var.deploy_to_aws ? 1 : 0
  
  name          = "${local.name_prefix}-emr-cluster"
  release_label = "emr-6.15.0"
  applications  = ["Hadoop", "Spark", "Hive", "Presto", "JupyterHub", "Livy"]
  
  termination_protection            = var.environment == "prod"
  keep_job_flow_alive_when_no_steps = true
  
  ec2_attributes {
    key_name                          = var.ec2_key_name
    subnet_id                         = var.private_subnet_ids[0]
    emr_managed_master_security_group = aws_security_group.emr_master[0].id
    emr_managed_slave_security_group  = aws_security_group.emr_slave[0].id
    instance_profile                  = aws_iam_instance_profile.emr_instance_profile[0].arn
  }
  
  master_instance_group {
    instance_type = var.emr_master_instance_type
    instance_count = 1
    
    ebs_config {
      size                 = 100
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }
  
  core_instance_group {
    instance_type  = var.emr_core_instance_type
    instance_count = var.emr_core_instance_count
    
    ebs_config {
      size                 = 500
      type                 = "gp3"
      volumes_per_instance = 2
    }
    
    autoscaling_policy = jsonencode({
      Constraints = {
        MinCapacity = var.emr_core_instance_count
        MaxCapacity = var.emr_max_instance_count
      }
      Rules = [
        {
          Name        = "ScaleOutMemoryPercentage"
          Description = "Scale out if YARNMemoryAvailablePercentage is less than 15"
          Action = {
            Market                 = "ON_DEMAND"
            SimpleScalingPolicyConfiguration = {
              AdjustmentType         = "CHANGE_IN_CAPACITY"
              ScalingAdjustment      = 1
              CoolDown              = 300
            }
          }
          Trigger = {
            CloudWatchAlarmDefinition = {
              ComparisonOperator = "LESS_THAN"
              EvaluationPeriods  = 1
              MetricName         = "YARNMemoryAvailablePercentage"
              Namespace          = "AWS/ElasticMapReduce"
              Period             = 300
              Statistic          = "AVERAGE"
              Threshold          = 15.0
              Unit               = "PERCENT"
            }
          }
        },
        {
          Name        = "ScaleInMemoryPercentage"
          Description = "Scale in if YARNMemoryAvailablePercentage is greater than 75"
          Action = {
            SimpleScalingPolicyConfiguration = {
              AdjustmentType    = "CHANGE_IN_CAPACITY"
              ScalingAdjustment = -1
              CoolDown         = 300
            }
          }
          Trigger = {
            CloudWatchAlarmDefinition = {
              ComparisonOperator = "GREATER_THAN"
              EvaluationPeriods  = 1
              MetricName         = "YARNMemoryAvailablePercentage"
              Namespace          = "AWS/ElasticMapReduce"
              Period             = 300
              Statistic          = "AVERAGE"
              Threshold          = 75.0
              Unit               = "PERCENT"
            }
          }
        }
      ]
    })
  }
  
  task_instance_group {
    instance_type  = var.emr_task_instance_type
    instance_count = 0
    
    ebs_config {
      size                 = 100
      type                 = "gp3"
      volumes_per_instance = 1
    }
    
    bid_price = "0.30"
  }
  
  bootstrap_action {
    path = "s3://aws-emr-resources-${data.aws_region.current[0].name}-prod/bootstrap-actions/run-if"
    name = "runif"
    args = ["instance.isMaster=true", "echo running on master node"]
  }
  
  configurations_json = jsonencode([
    {
      Classification = "hadoop-env"
      Configurations = [
        {
          Classification = "export"
          Properties = {
            JAVA_HOME = "/usr/lib/jvm/java-1.8.0"
          }
        }
      ]
    },
    {
      Classification = "spark-env"
      Configurations = [
        {
          Classification = "export"
          Properties = {
            JAVA_HOME = "/usr/lib/jvm/java-1.8.0"
          }
        }
      ]
    },
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.dynamicAllocation.enabled"          = "true"
        "spark.dynamicAllocation.minExecutors"     = "1"
        "spark.dynamicAllocation.maxExecutors"     = "50"
        "spark.dynamicAllocation.initialExecutors" = "2"
        "spark.sql.adaptive.enabled"               = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "spark.sql.adaptive.skewJoin.enabled"      = "true"
        "spark.serializer"                         = "org.apache.spark.serializer.KryoSerializer"
        "spark.sql.hive.convertMetastoreParquet"   = "true"
        "spark.sql.parquet.filterPushdown"         = "true"
        "spark.sql.parquet.mergeSchema"            = "false"
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" = "2"
        "spark.speculation"                        = "false"
        "spark.sql.streaming.metricsEnabled"       = "true"
        "spark.sql.streaming.streamingQueryListeners" = "org.apache.spark.sql.streaming.StreamingQueryListener"
        "spark.delta.logStore.class"               = "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
      }
    },
    {
      Classification = "hive-site"
      Properties = {
        "javax.jdo.option.ConnectionURL"      = "jdbc:mysql://${var.hive_metastore_endpoint}/hive?createDatabaseIfNotExist=true"
        "javax.jdo.option.ConnectionUserName" = var.hive_metastore_username
        "javax.jdo.option.ConnectionPassword" = var.hive_metastore_password
        "javax.jdo.option.ConnectionDriverName" = "org.mariadb.jdbc.Driver"
      }
    }
  ])
  
  service_role = aws_iam_role.emr_service_role[0].arn
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-emr-cluster"
    Type = "big-data-processing"
  })
  
  depends_on = [
    aws_iam_role_policy_attachment.emr_service_role,
    aws_iam_instance_profile.emr_instance_profile
  ]
}

# EMR Security Groups
resource "aws_security_group" "emr_master" {
  count = var.deploy_to_aws ? 1 : 0
  
  name_prefix = "${local.name_prefix}-emr-master"
  vpc_id      = var.vpc_id
  
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = var.tags
}

resource "aws_security_group" "emr_slave" {
  count = var.deploy_to_aws ? 1 : 0
  
  name_prefix = "${local.name_prefix}-emr-slave"
  vpc_id      = var.vpc_id
  
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = var.tags
}

# ============================================================================
# AZURE DATA LAKEHOUSE (SECONDARY)
# ============================================================================

# Azure Data Lake Storage Gen2
resource "azurerm_storage_account" "medallion_adls" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                     = "${replace(local.name_prefix, "-", "")}adls${random_string.bucket_suffix.result}"
  resource_group_name      = var.azure_resource_group_name
  location                 = var.azure_location
  account_tier             = "Standard"
  account_replication_type = "ZRS"
  account_kind             = "StorageV2"
  is_hns_enabled          = true
  
  network_rules {
    default_action             = "Allow"
    bypass                     = ["AzureServices"]
    virtual_network_subnet_ids = var.azure_subnet_ids
  }
  
  blob_properties {
    versioning_enabled       = true
    change_feed_enabled      = true
    change_feed_retention_in_days = 30
    last_access_time_enabled = true
    
    delete_retention_policy {
      days = 30
    }
    
    container_delete_retention_policy {
      days = 30
    }
  }
  
  lifecycle_management_policy {
    rule {
      name    = "medallion_lifecycle"
      enabled = true
      
      filters {
        prefix_match = ["bronze/", "silver/", "gold/"]
        blob_types   = ["blockBlob"]
      }
      
      actions {
        base_blob {
          tier_to_cool_after_days_since_modification_greater_than    = 30
          tier_to_archive_after_days_since_modification_greater_than = 90
          delete_after_days_since_modification_greater_than          = 2555
        }
        
        snapshot {
          delete_after_days_since_creation_greater_than = 90
        }
        
        version {
          delete_after_days_since_creation = 90
        }
      }
    }
  }
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-adls"
    Type = "data-lake-storage"
  })
}

# ADLS Gen2 Filesystems for Medallion Layers
resource "azurerm_storage_data_lake_gen2_filesystem" "medallion_layers" {
  for_each = var.deploy_to_azure ? toset(local.medallion_layers) : []
  
  name               = each.key
  storage_account_id = azurerm_storage_account.medallion_adls[0].id
  
  properties = {
    layer = each.key
  }
}

# Azure Synapse Analytics Workspace
resource "azurerm_synapse_workspace" "main" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                                 = "${local.name_prefix}-synapse"
  resource_group_name                  = var.azure_resource_group_name
  location                            = var.azure_location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.medallion_layers["gold"].id
  sql_administrator_login             = var.synapse_admin_login
  sql_administrator_login_password    = var.synapse_admin_password
  
  managed_virtual_network_enabled = true
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = var.tags
}

# ============================================================================
# GCP DATA LAKEHOUSE (TERTIARY)
# ============================================================================

# Cloud Storage Buckets for Medallion Architecture
resource "google_storage_bucket" "medallion_layers_gcp" {
  for_each = var.deploy_to_gcp ? toset(local.medallion_layers) : []
  
  name     = "${local.name_prefix}-${each.key}-${random_string.bucket_suffix.result}"
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
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
  
  labels = merge(var.tags, {
    layer = each.key
    zone  = each.key
  })
}

# BigQuery Dataset for Analytics
resource "google_bigquery_dataset" "medallion_analytics" {
  for_each = var.deploy_to_gcp ? toset(local.medallion_layers) : []
  
  dataset_id    = "${replace(local.name_prefix, "-", "_")}_${each.key}"
  friendly_name = "${title(each.key)} Layer - ${title(var.project_name)}"
  description   = "BigQuery dataset for ${each.key} layer of medallion architecture"
  location      = var.gcp_region
  
  default_table_expiration_ms = each.key == "bronze" ? 7776000000 : (each.key == "silver" ? 15552000000 : 31104000000)  # 90 days, 6 months, 1 year
  
  access {
    role          = "OWNER"
    user_by_email = var.gcp_service_account_email
  }
  
  access {
    role   = "READER"
    domain = var.organization_domain
  }
  
  labels = var.tags
}

# Dataproc Cluster for Spark Processing
resource "google_dataproc_cluster" "medallion_processing" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name   = "${local.name_prefix}-dataproc"
  region = var.gcp_region
  
  cluster_config {
    staging_bucket = google_storage_bucket.medallion_layers_gcp["bronze"].name
    
    master_config {
      num_instances = 1
      machine_type  = var.dataproc_master_machine_type
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 100
        num_local_ssds    = 1
      }
    }
    
    worker_config {
      num_instances = var.dataproc_worker_count
      machine_type  = var.dataproc_worker_machine_type
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
        num_local_ssds    = 1
      }
    }
    
    preemptible_worker_config {
      num_instances = var.dataproc_preemptible_count
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
      }
    }
    
    software_config {
      image_version = "2.1-debian11"
      
      properties = {
        "spark:spark.dynamicAllocation.enabled"                    = "true"
        "spark:spark.dynamicAllocation.minExecutors"               = "1"
        "spark:spark.dynamicAllocation.maxExecutors"               = "50"
        "spark:spark.sql.adaptive.enabled"                         = "true"
        "spark:spark.sql.adaptive.coalescePartitions.enabled"      = "true"
        "spark:spark.serializer"                                   = "org.apache.spark.serializer.KryoSerializer"
        "dataproc:dataproc.logging.stackdriver.enable"            = "true"
        "dataproc:jobs.file-backed-output.enable"                 = "true"
      }
    }
    
    gce_cluster_config {
      zone = var.gcp_zone
      
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform"
      ]
      
      metadata = {
        enable-oslogin = "true"
      }
    }
    
    autoscaling_config {
      max_instances = var.dataproc_max_instances
      min_instances = var.dataproc_min_instances
    }
  }
  
  labels = var.tags
}

# ============================================================================
# CROSS-CLOUD DATA REPLICATION
# ============================================================================

# AWS DataSync for Cross-Region Replication
resource "aws_datasync_task" "cross_region_replication" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? length(local.medallion_layers) : 0
  
  destination_location_arn = aws_datasync_location_s3.destination[count.index].arn
  name                     = "${local.name_prefix}-${local.medallion_layers[count.index]}-replication"
  source_location_arn      = aws_datasync_location_s3.source[count.index].arn
  
  options {
    bytes_per_second                = -1
    gid                            = "NONE"
    log_level                      = "TRANSFER"
    mtime                          = "PRESERVE"
    overwrite_mode                 = "ALWAYS"
    posix_permissions              = "NONE"
    preserve_deleted_files         = "PRESERVE"
    preserve_devices               = "NONE"
    task_queueing                  = "ENABLED"
    transfer_mode                  = "CHANGED"
    uid                           = "NONE"
    verify_mode                   = "POINT_IN_TIME_CONSISTENT"
  }
  
  schedule {
    schedule_expression = "cron(0 2 * * ? *)"  # Daily at 2 AM
  }
  
  tags = var.tags
}

# ============================================================================
# DATA QUALITY AND GOVERNANCE
# ============================================================================

# Great Expectations for Data Quality
resource "aws_lambda_function" "data_quality_validator" {
  count = var.deploy_to_aws ? 1 : 0
  
  filename         = data.archive_file.data_quality_zip[0].output_path
  function_name    = "${local.name_prefix}-data-quality-validator"
  role            = aws_iam_role.data_quality_role[0].arn
  handler         = "validator.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 1024
  
  environment {
    variables = {
      BRONZE_BUCKET = try(aws_s3_bucket.medallion_layers["bronze"].bucket, "")
      SILVER_BUCKET = try(aws_s3_bucket.medallion_layers["silver"].bucket, "")
      GOLD_BUCKET   = try(aws_s3_bucket.medallion_layers["gold"].bucket, "")
      GLUE_DATABASE = try(aws_glue_catalog_database.medallion_catalog["silver"].name, "")
    }
  }
  
  tags = var.tags
}

data "archive_file" "data_quality_zip" {
  count = var.deploy_to_aws ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/data_quality_validator.zip"
  
  source {
    content = templatefile("${path.module}/templates/data_quality_validator.py.tpl", {
      environment = var.environment
    })
    filename = "validator.py"
  }
  
  source {
    content = "great-expectations==0.17.23\nboto3==1.34.0\npandas==2.1.4"
    filename = "requirements.txt"
  }
}

# Data Quality Lambda IAM Role
resource "aws_iam_role" "data_quality_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-data-quality-role"
  
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

data "aws_region" "current" {
  count = var.deploy_to_aws ? 1 : 0
}

# ============================================================================
# EMR IAM ROLES AND POLICIES
# ============================================================================

resource "aws_iam_role" "emr_service_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-emr-service-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_service_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  role       = aws_iam_role.emr_service_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_instance_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-emr-instance-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_instance_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  role       = aws_iam_role.emr_instance_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_instance_profile" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-emr-instance-profile"
  role = aws_iam_role.emr_instance_role[0].name
  
  tags = var.tags
}

# DataSync Locations (placeholder - would need actual configuration)
resource "aws_datasync_location_s3" "source" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? length(local.medallion_layers) : 0
  
  s3_bucket_arn = aws_s3_bucket.medallion_layers[local.medallion_layers[count.index]].arn
  subdirectory  = "/"
  
  s3_config {
    bucket_access_role_arn = aws_iam_role.datasync_role[0].arn
  }
  
  tags = var.tags
}

resource "aws_datasync_location_s3" "destination" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? length(local.medallion_layers) : 0
  
  s3_bucket_arn = "arn:aws:s3:::${var.backup_bucket_name}-${local.medallion_layers[count.index]}"
  subdirectory  = "/"
  
  s3_config {
    bucket_access_role_arn = aws_iam_role.datasync_role[0].arn
  }
  
  tags = var.tags
}

resource "aws_iam_role" "datasync_role" {
  count = var.deploy_to_aws && var.enable_cross_region_replication ? 1 : 0
  
  name = "${local.name_prefix}-datasync-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "datasync.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}