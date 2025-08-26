# Multi-Cloud Messaging Infrastructure Module
# Implements Apache Kafka and RabbitMQ with enterprise features

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
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Kafka topic configuration for data streaming
  kafka_topics = [
    {
      name               = "${var.kafka_topic_prefix}-sales-events"
      partitions        = var.kafka_partition_count
      replication_factor = var.kafka_replication_factor
      config = {
        "retention.ms" = "604800000"  # 7 days
        "cleanup.policy" = "delete"
      }
    },
    {
      name               = "${var.kafka_topic_prefix}-customer-events"
      partitions        = var.kafka_partition_count
      replication_factor = var.kafka_replication_factor
      config = {
        "retention.ms" = "2592000000"  # 30 days
        "cleanup.policy" = "compact"
      }
    },
    {
      name               = "${var.kafka_topic_prefix}-data-quality-alerts"
      partitions        = 3
      replication_factor = var.kafka_replication_factor
      config = {
        "retention.ms" = "86400000"  # 1 day
        "cleanup.policy" = "delete"
      }
    },
    {
      name               = "${var.kafka_topic_prefix}-schema-registry"
      partitions        = 1
      replication_factor = var.kafka_replication_factor
      config = {
        "cleanup.policy" = "compact"
      }
    }
  ]
  
  # RabbitMQ queues for task orchestration
  rabbitmq_queues = [
    "etl-tasks",
    "data-validation",
    "model-training",
    "report-generation",
    "alert-notifications"
  ]
}

# ============================================================================
# AWS MESSAGING SERVICES
# ============================================================================

# Amazon MSK (Managed Streaming for Apache Kafka)
resource "aws_msk_cluster" "main" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  cluster_name           = "${local.name_prefix}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.kafka_broker_count
  
  broker_node_group_info {
    instance_type   = var.kafka_instance_type
    client_subnets  = var.private_subnets
    security_groups = [aws_security_group.msk[0].id]
    
    storage_info {
      ebs_storage_info {
        volume_size = var.kafka_storage_size
      }
    }
  }
  
  configuration_info {
    arn      = aws_msk_configuration.main[0].arn
    revision = aws_msk_configuration.main[0].latest_revision
  }
  
  encryption_info {
    encryption_at_rest_kms_key_id = aws_kms_key.msk[0].arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
  
  client_authentication {
    sasl {
      scram = true
    }
    tls {
      certificate_authority_arns = [aws_acmpca_certificate_authority.msk[0].arn]
    }
  }
  
  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = var.enable_logging
        log_group = aws_cloudwatch_log_group.msk[0].name
      }
      firehose {
        enabled         = var.enable_logging
        delivery_stream = aws_kinesis_firehose_delivery_stream.msk_logs[0].name
      }
      s3 {
        enabled = var.enable_logging
        bucket  = aws_s3_bucket.msk_logs[0].bucket
        prefix  = "kafka-logs/"
      }
    }
  }
  
  tags = var.tags
}

# MSK Configuration
resource "aws_msk_configuration" "main" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  kafka_versions = [var.kafka_version]
  name          = "${local.name_prefix}-kafka-config"
  
  server_properties = <<-EOT
    auto.create.topics.enable=false
    default.replication.factor=${var.kafka_replication_factor}
    min.insync.replicas=${max(1, var.kafka_replication_factor - 1)}
    num.partitions=${var.kafka_partition_count}
    log.retention.hours=168
    log.segment.bytes=1073741824
    log.retention.check.interval.ms=300000
    message.max.bytes=1048576
    replica.fetch.max.bytes=1048576
    group.initial.rebalance.delay.ms=3000
    compression.type=gzip
    unclean.leader.election.enable=false
  EOT
}

# MSK Security Group
resource "aws_security_group" "msk" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  name        = "${local.name_prefix}-msk-sg"
  description = "Security group for MSK cluster"
  vpc_id      = var.vpc_id
  
  # Kafka broker communication
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  # Kafka broker SSL
  ingress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  # Kafka broker SASL/SCRAM
  ingress {
    from_port   = 9096
    to_port     = 9096
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  # Zookeeper
  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-msk-sg"
  })
}

# KMS Key for MSK
resource "aws_kms_key" "msk" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  description = "KMS key for MSK cluster encryption"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "Enable IAM User Permissions"
      Effect = "Allow"
      Principal = {
        AWS = "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:root"
      }
      Action   = "kms:*"
      Resource = "*"
    }]
  })
  
  tags = var.tags
}

resource "aws_kms_alias" "msk" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  name          = "alias/${local.name_prefix}-msk"
  target_key_id = aws_kms_key.msk[0].key_id
}

# Private Certificate Authority for MSK TLS
resource "aws_acmpca_certificate_authority" "msk" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  certificate_authority_configuration {
    key_algorithm     = "RSA_2048"
    signing_algorithm = "SHA256WITHRSA"
    
    subject {
      common_name = "${local.name_prefix}-msk-ca"
    }
  }
  
  permanent_deletion_time_in_days = 7
  
  tags = var.tags
}

# Amazon MQ (Managed RabbitMQ)
resource "aws_mq_broker" "main" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  broker_name    = "${local.name_prefix}-rabbitmq"
  engine_type    = "RabbitMQ"
  engine_version = var.rabbitmq_version
  host_instance_type = var.rabbitmq_instance_type
  security_groups    = [aws_security_group.rabbitmq[0].id]
  
  deployment_mode = var.environment == "prod" ? "CLUSTER_MULTI_AZ" : "SINGLE_INSTANCE"
  subnet_ids      = var.environment == "prod" ? var.private_subnets : [var.private_subnets[0]]
  
  user {
    username = var.rabbitmq_username
    password = var.rabbitmq_password
  }
  
  configuration {
    id       = aws_mq_configuration.rabbitmq[0].id
    revision = aws_mq_configuration.rabbitmq[0].latest_revision
  }
  
  encryption_options {
    use_aws_owned_key = false
    kms_key_id       = aws_kms_key.rabbitmq[0].arn
  }
  
  logs {
    general = var.enable_logging
  }
  
  storage_type = "efs"
  
  tags = var.tags
}

# RabbitMQ Configuration
resource "aws_mq_configuration" "rabbitmq" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  description    = "RabbitMQ configuration for ${local.name_prefix}"
  name           = "${local.name_prefix}-rabbitmq-config"
  engine_type    = "RabbitMQ"
  engine_version = var.rabbitmq_version
  
  data = base64encode(templatefile("${path.module}/templates/rabbitmq.conf.tpl", {
    max_connections = 1000
    memory_high_watermark = "0.6"
    disk_free_limit = "2GB"
  }))
  
  tags = var.tags
}

# RabbitMQ Security Group
resource "aws_security_group" "rabbitmq" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  name        = "${local.name_prefix}-rabbitmq-sg"
  description = "Security group for RabbitMQ broker"
  vpc_id      = var.vpc_id
  
  # AMQP
  ingress {
    from_port   = 5672
    to_port     = 5672
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  # AMQPS
  ingress {
    from_port   = 5671
    to_port     = 5671
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  # Management Console
  ingress {
    from_port   = 15672
    to_port     = 15672
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-rabbitmq-sg"
  })
}

# KMS Key for RabbitMQ
resource "aws_kms_key" "rabbitmq" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
  
  description = "KMS key for RabbitMQ encryption"
  
  tags = var.tags
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "msk" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws && var.enable_logging ? 1 : 0
  
  name              = "/aws/msk/${local.name_prefix}"
  retention_in_days = 30
  
  tags = var.tags
}

# S3 Bucket for MSK Logs
resource "aws_s3_bucket" "msk_logs" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws && var.enable_logging ? 1 : 0
  
  bucket = "${local.name_prefix}-msk-logs-${random_string.bucket_suffix.result}"
  
  tags = var.tags
}

# Kinesis Firehose for MSK Logs
resource "aws_kinesis_firehose_delivery_stream" "msk_logs" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws && var.enable_logging ? 1 : 0
  
  name        = "${local.name_prefix}-msk-logs"
  destination = "s3"
  
  s3_configuration {
    role_arn   = aws_iam_role.firehose[0].arn
    bucket_arn = aws_s3_bucket.msk_logs[0].arn
    prefix     = "kafka-logs/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    
    buffer_size     = 5
    buffer_interval = 300
    
    compression_format = "GZIP"
  }
  
  tags = var.tags
}

# IAM Role for Firehose
resource "aws_iam_role" "firehose" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws && var.enable_logging ? 1 : 0
  
  name = "${local.name_prefix}-firehose-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "firehose.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  count = var.cloud_provider == "aws" || var.deploy_to_aws && var.enable_logging ? 1 : 0
  
  name = "${local.name_prefix}-firehose-policy"
  role = aws_iam_role.firehose[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.msk_logs[0].arn,
        "${aws_s3_bucket.msk_logs[0].arn}/*"
      ]
    }]
  })
}

# ============================================================================
# KUBERNETES-BASED KAFKA DEPLOYMENT
# ============================================================================

# Kafka Operator Namespace
resource "kubernetes_namespace" "kafka" {
  count = var.deploy_k8s_kafka ? 1 : 0
  
  metadata {
    name = "${local.name_prefix}-kafka"
    
    labels = {
      app     = "kafka"
      purpose = "messaging"
    }
  }
}

# Strimzi Kafka Operator
resource "helm_release" "kafka_operator" {
  count = var.deploy_k8s_kafka ? 1 : 0
  
  name       = "kafka-operator"
  repository = "https://strimzi.io/charts/"
  chart      = "strimzi-kafka-operator"
  version    = "0.38.0"
  namespace  = kubernetes_namespace.kafka[0].metadata[0].name
  
  values = [templatefile("${path.module}/templates/kafka-operator-values.yaml.tpl", {
    namespace = kubernetes_namespace.kafka[0].metadata[0].name
  })]
  
  depends_on = [kubernetes_namespace.kafka]
}

# Kafka Cluster CRD
resource "kubernetes_manifest" "kafka_cluster" {
  count = var.deploy_k8s_kafka ? 1 : 0
  
  manifest = yamldecode(templatefile("${path.module}/templates/kafka-cluster.yaml.tpl", {
    name                   = "${local.name_prefix}-kafka"
    namespace             = kubernetes_namespace.kafka[0].metadata[0].name
    replicas              = var.kafka_broker_count
    storage_size          = "${var.kafka_storage_size}Gi"
    kafka_version         = var.kafka_version
    replication_factor    = var.kafka_replication_factor
    min_insync_replicas   = max(1, var.kafka_replication_factor - 1)
  }))
  
  depends_on = [helm_release.kafka_operator]
}

# Schema Registry
resource "kubernetes_manifest" "schema_registry" {
  count = var.deploy_k8s_kafka ? 1 : 0
  
  manifest = yamldecode(templatefile("${path.module}/templates/schema-registry.yaml.tpl", {
    name      = "${local.name_prefix}-schema-registry"
    namespace = kubernetes_namespace.kafka[0].metadata[0].name
    kafka_cluster = "${local.name_prefix}-kafka"
  }))
  
  depends_on = [kubernetes_manifest.kafka_cluster]
}

# Kafka Connect
resource "kubernetes_manifest" "kafka_connect" {
  count = var.deploy_k8s_kafka ? 1 : 0
  
  manifest = yamldecode(templatefile("${path.module}/templates/kafka-connect.yaml.tpl", {
    name      = "${local.name_prefix}-kafka-connect"
    namespace = kubernetes_namespace.kafka[0].metadata[0].name
    kafka_cluster = "${local.name_prefix}-kafka"
    replicas  = 3
  }))
  
  depends_on = [kubernetes_manifest.kafka_cluster]
}

# ============================================================================
# KAFKA TOPICS CREATION
# ============================================================================

resource "kubernetes_manifest" "kafka_topics" {
  count = var.deploy_k8s_kafka ? length(local.kafka_topics) : 0
  
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    
    metadata = {
      name      = local.kafka_topics[count.index].name
      namespace = kubernetes_namespace.kafka[0].metadata[0].name
      labels = {
        "strimzi.io/cluster" = "${local.name_prefix}-kafka"
      }
    }
    
    spec = {
      partitions = local.kafka_topics[count.index].partitions
      replicas   = local.kafka_topics[count.index].replication_factor
      config     = local.kafka_topics[count.index].config
    }
  }
  
  depends_on = [kubernetes_manifest.kafka_cluster]
}

# ============================================================================
# RABBITMQ ON KUBERNETES
# ============================================================================

# RabbitMQ Cluster Operator
resource "helm_release" "rabbitmq_operator" {
  count = var.deploy_k8s_rabbitmq ? 1 : 0
  
  name       = "rabbitmq-operator"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "rabbitmq-cluster-operator"
  version    = "3.8.21"
  namespace  = kubernetes_namespace.rabbitmq[0].metadata[0].name
  
  depends_on = [kubernetes_namespace.rabbitmq]
}

resource "kubernetes_namespace" "rabbitmq" {
  count = var.deploy_k8s_rabbitmq ? 1 : 0
  
  metadata {
    name = "${local.name_prefix}-rabbitmq"
    
    labels = {
      app     = "rabbitmq"
      purpose = "messaging"
    }
  }
}

# RabbitMQ Cluster
resource "kubernetes_manifest" "rabbitmq_cluster" {
  count = var.deploy_k8s_rabbitmq ? 1 : 0
  
  manifest = yamldecode(templatefile("${path.module}/templates/rabbitmq-cluster.yaml.tpl", {
    name         = "${local.name_prefix}-rabbitmq"
    namespace    = kubernetes_namespace.rabbitmq[0].metadata[0].name
    replicas     = var.environment == "prod" ? 3 : 1
    storage_size = "${var.rabbitmq_storage_size}Gi"
  }))
  
  depends_on = [helm_release.rabbitmq_operator]
}

# ============================================================================
# MONITORING AND METRICS
# ============================================================================

# Prometheus ServiceMonitor for Kafka
resource "kubernetes_manifest" "kafka_service_monitor" {
  count = var.deploy_k8s_kafka && var.enable_monitoring ? 1 : 0
  
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    
    metadata = {
      name      = "${local.name_prefix}-kafka-metrics"
      namespace = kubernetes_namespace.kafka[0].metadata[0].name
      labels = {
        app = "kafka"
      }
    }
    
    spec = {
      selector = {
        matchLabels = {
          "strimzi.io/cluster" = "${local.name_prefix}-kafka"
          "strimzi.io/kind"    = "Kafka"
        }
      }
      endpoints = [
        {
          port = "tcp-prometheus"
          path = "/metrics"
        }
      ]
    }
  }
  
  depends_on = [kubernetes_manifest.kafka_cluster]
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
  count = var.cloud_provider == "aws" || var.deploy_to_aws ? 1 : 0
}