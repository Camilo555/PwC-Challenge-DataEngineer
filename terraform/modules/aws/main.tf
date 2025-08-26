# AWS Infrastructure Module for PwC Retail Data Platform
# Comprehensive AWS resources including EKS, RDS, S3, and networking

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure AWS Provider
provider "aws" {
  region = var.region
  
  default_tags {
    tags = var.tags
  }
}

# Data sources for AWS availability zones and AMI
data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# ============================================================================
# NETWORKING - VPC, SUBNETS, GATEWAYS
# ============================================================================

# Main VPC
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-vpc"
    Type = "vpc"
  })
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-igw"
  })
}

# Public Subnets
resource "aws_subnet" "public" {
  count = length(var.public_subnet_cidrs)
  
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  
  tags = merge(var.tags, {
    Name                     = "${var.project_name}-${var.environment}-public-subnet-${count.index + 1}"
    Type                     = "public"
    "kubernetes.io/role/elb" = "1"
  })
}

# Private Subnets
resource "aws_subnet" "private" {
  count = length(var.private_subnet_cidrs)
  
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = data.aws_availability_zones.available.names[count.index]
  
  tags = merge(var.tags, {
    Name                              = "${var.project_name}-${var.environment}-private-subnet-${count.index + 1}"
    Type                              = "private"
    "kubernetes.io/role/internal-elb" = "1"
  })
}

# Elastic IPs for NAT Gateways
resource "aws_eip" "nat" {
  count = length(aws_subnet.public)
  
  domain = "vpc"
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-nat-eip-${count.index + 1}"
  })
  
  depends_on = [aws_internet_gateway.main]
}

# NAT Gateways
resource "aws_nat_gateway" "main" {
  count = length(aws_subnet.public)
  
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-nat-gw-${count.index + 1}"
  })
  
  depends_on = [aws_internet_gateway.main]
}

# Route Table for Public Subnets
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-public-rt"
  })
}

# Route Table for Private Subnets
resource "aws_route_table" "private" {
  count = length(aws_subnet.private)
  
  vpc_id = aws_vpc.main.id
  
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main[count.index].id
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-private-rt-${count.index + 1}"
  })
}

# Route Table Associations
resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)
  
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)
  
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# ============================================================================
# SECURITY GROUPS
# ============================================================================

# EKS Cluster Security Group
resource "aws_security_group" "eks_cluster" {
  name_prefix = "${var.project_name}-${var.environment}-eks-cluster"
  vpc_id      = aws_vpc.main.id
  
  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-eks-cluster-sg"
  })
}

# EKS Node Group Security Group
resource "aws_security_group" "eks_nodes" {
  name_prefix = "${var.project_name}-${var.environment}-eks-nodes"
  vpc_id      = aws_vpc.main.id
  
  ingress {
    description = "All traffic from cluster"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    security_groups = [aws_security_group.eks_cluster.id]
  }
  
  ingress {
    description = "Node to node communication"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-eks-nodes-sg"
  })
}

# RDS Security Group
resource "aws_security_group" "rds" {
  name_prefix = "${var.project_name}-${var.environment}-rds"
  vpc_id      = aws_vpc.main.id
  
  ingress {
    description     = "PostgreSQL from EKS nodes"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-rds-sg"
  })
}

# ============================================================================
# EKS CLUSTER
# ============================================================================

# EKS Cluster IAM Role
resource "aws_iam_role" "eks_cluster" {
  name = "${var.project_name}-${var.environment}-eks-cluster-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

# Attach policies to EKS cluster role
resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_cluster.name
}

# EKS Cluster
resource "aws_eks_cluster" "main" {
  name     = "${var.project_name}-${var.environment}-cluster"
  role_arn = aws_iam_role.eks_cluster.arn
  version  = var.cluster_version
  
  vpc_config {
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs    = ["0.0.0.0/0"]
    subnet_ids = concat(
      aws_subnet.public[*].id,
      aws_subnet.private[*].id
    )
    security_group_ids = [aws_security_group.eks_cluster.id]
  }
  
  # Enable logging
  enabled_cluster_log_types = var.enable_logging ? [
    "api",
    "audit",
    "authenticator",
    "controllerManager",
    "scheduler"
  ] : []
  
  # Encryption at rest
  encryption_config {
    provider {
      key_arn = aws_kms_key.eks.arn
    }
    resources = ["secrets"]
  }
  
  tags = var.tags
  
  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
    aws_cloudwatch_log_group.eks
  ]
}

# CloudWatch Log Group for EKS
resource "aws_cloudwatch_log_group" "eks" {
  count = var.enable_logging ? 1 : 0
  
  name              = "/aws/eks/${var.project_name}-${var.environment}-cluster/cluster"
  retention_in_days = 7
  
  tags = var.tags
}

# KMS Key for EKS encryption
resource "aws_kms_key" "eks" {
  description             = "EKS Secret Encryption Key"
  deletion_window_in_days = 7
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-eks-kms"
  })
}

resource "aws_kms_alias" "eks" {
  name          = "alias/${var.project_name}-${var.environment}-eks"
  target_key_id = aws_kms_key.eks.key_id
}

# ============================================================================
# EKS NODE GROUP
# ============================================================================

# EKS Node Group IAM Role
resource "aws_iam_role" "eks_nodes" {
  name = "${var.project_name}-${var.environment}-eks-nodes-role"
  
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

# Attach policies to node group role
resource "aws_iam_role_policy_attachment" "eks_worker_node_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_nodes.name
}

resource "aws_iam_role_policy_attachment" "eks_cni_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_nodes.name
}

resource "aws_iam_role_policy_attachment" "eks_container_registry_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_nodes.name
}

# EKS Node Group
resource "aws_eks_node_group" "main" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.project_name}-${var.environment}-nodes"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = aws_subnet.private[*].id
  instance_types  = var.node_instance_types
  
  scaling_config {
    desired_size = var.node_desired_size
    max_size     = var.node_max_size
    min_size     = var.node_min_size
  }
  
  update_config {
    max_unavailable_percentage = 25
  }
  
  # Use latest EKS optimized AMI
  ami_type       = "AL2_x86_64"
  capacity_type  = "ON_DEMAND"
  disk_size      = 50
  
  tags = var.tags
  
  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_container_registry_policy,
  ]
}

# ============================================================================
# RDS POSTGRESQL DATABASE
# ============================================================================

# DB Subnet Group
resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-${var.environment}-db-subnet-group"
  subnet_ids = aws_subnet.private[*].id
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-db-subnet-group"
  })
}

# RDS Parameter Group
resource "aws_db_parameter_group" "main" {
  family = "postgres15"
  name   = "${var.project_name}-${var.environment}-postgres-params"
  
  parameter {
    name  = "log_statement"
    value = "all"
  }
  
  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }
  
  tags = var.tags
}

# RDS PostgreSQL Instance
resource "aws_db_instance" "main" {
  identifier = "${var.project_name}-${var.environment}-postgres"
  
  # Engine configuration
  engine         = "postgres"
  engine_version = "15.4"
  instance_class = var.db_instance_class
  
  # Storage configuration
  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = var.db_allocated_storage * 2
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id           = aws_kms_key.rds.arn
  
  # Database configuration
  db_name  = "${replace(var.project_name, "-", "_")}_${var.environment}"
  username = "admin"
  password = var.db_admin_password
  port     = 5432
  
  # Network and security
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
  publicly_accessible    = false
  
  # Backup and maintenance
  backup_retention_period   = var.db_backup_retention_period
  backup_window            = var.db_backup_window
  maintenance_window       = var.db_maintenance_window
  auto_minor_version_upgrade = true
  
  # Performance and monitoring
  parameter_group_name   = aws_db_parameter_group.main.name
  performance_insights_enabled = true
  performance_insights_retention_period = 7
  monitoring_interval    = 60
  monitoring_role_arn   = aws_iam_role.rds_monitoring.arn
  
  # Deletion protection
  deletion_protection = var.environment == "prod"
  skip_final_snapshot = var.environment != "prod"
  final_snapshot_identifier = var.environment == "prod" ? "${var.project_name}-${var.environment}-final-snapshot-${formatdate("YYYY-MM-DD-hhmm", timestamp())}" : null
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-postgres"
  })
  
  depends_on = [aws_cloudwatch_log_group.rds]
}

# KMS Key for RDS encryption
resource "aws_kms_key" "rds" {
  description             = "RDS encryption key"
  deletion_window_in_days = 7
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-rds-kms"
  })
}

resource "aws_kms_alias" "rds" {
  name          = "alias/${var.project_name}-${var.environment}-rds"
  target_key_id = aws_kms_key.rds.key_id
}

# RDS Enhanced Monitoring IAM Role
resource "aws_iam_role" "rds_monitoring" {
  name = "${var.project_name}-${var.environment}-rds-monitoring-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  role       = aws_iam_role.rds_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# CloudWatch Log Group for RDS
resource "aws_cloudwatch_log_group" "rds" {
  count = var.enable_logging ? 1 : 0
  
  name              = "/aws/rds/instance/${var.project_name}-${var.environment}-postgres/postgresql"
  retention_in_days = 7
  
  tags = var.tags
}

# ============================================================================
# S3 STORAGE
# ============================================================================

# S3 Bucket for data storage
resource "aws_s3_bucket" "data" {
  bucket        = "${var.project_name}-${var.environment}-data-${random_id.bucket_suffix.hex}"
  force_destroy = var.environment != "prod"
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-data-bucket"
    Type = "data-storage"
  })
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# S3 Bucket versioning
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

# S3 Bucket public access block
resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# KMS Key for S3 encryption
resource "aws_kms_key" "s3" {
  description             = "S3 bucket encryption key"
  deletion_window_in_days = 7
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-s3-kms"
  })
}

resource "aws_kms_alias" "s3" {
  name          = "alias/${var.project_name}-${var.environment}-s3"
  target_key_id = aws_kms_key.s3.key_id
}

# ============================================================================
# ECR CONTAINER REGISTRY
# ============================================================================

resource "aws_ecr_repository" "app_repositories" {
  for_each = toset(["api", "etl", "dbt", "dagster"])
  
  name                 = "${var.project_name}-${var.environment}-${each.key}"
  image_tag_mutability = "MUTABLE"
  
  image_scanning_configuration {
    scan_on_push = true
  }
  
  encryption_configuration {
    encryption_type = "KMS"
    kms_key        = aws_kms_key.ecr.arn
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-${each.key}-ecr"
  })
}

# ECR Lifecycle Policy
resource "aws_ecr_lifecycle_policy" "app_repositories" {
  for_each = aws_ecr_repository.app_repositories
  
  repository = each.value.name
  
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 10 images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["v"]
          countType     = "imageCountMoreThan"
          countNumber   = 10
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Delete untagged images older than 1 day"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 1
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# KMS Key for ECR encryption
resource "aws_kms_key" "ecr" {
  description             = "ECR encryption key"
  deletion_window_in_days = 7
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-ecr-kms"
  })
}

resource "aws_kms_alias" "ecr" {
  name          = "alias/${var.project_name}-${var.environment}-ecr"
  target_key_id = aws_kms_key.ecr.key_id
}

# ============================================================================
# RABBITMQ (AMAZON MQ)
# ============================================================================

# Amazon MQ Broker (RabbitMQ)
resource "aws_mq_broker" "rabbitmq" {
  broker_name        = "${var.project_name}-${var.environment}-rabbitmq"
  engine_type        = "RabbitMQ"
  engine_version     = "3.12.13"
  host_instance_type = var.rabbitmq_instance_type
  publicly_accessible = false
  
  # Security and networking
  security_groups    = [aws_security_group.rabbitmq.id]
  subnet_ids        = [aws_subnet.private[0].id]
  
  # Authentication
  user {
    username = var.rabbitmq_username
    password = var.rabbitmq_password
  }
  
  # Encryption
  encryption_options {
    use_aws_owned_key = false
    kms_key_id       = aws_kms_key.mq.arn
  }
  
  # Logging
  logs {
    general = true
  }
  
  # Maintenance
  maintenance_window_start_time {
    day_of_week = "SUNDAY"
    time_of_day = "03:00"
    time_zone   = "UTC"
  }
  
  # Configuration
  configuration {
    id       = aws_mq_configuration.rabbitmq.id
    revision = aws_mq_configuration.rabbitmq.latest_revision
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-rabbitmq"
    Type = "message-broker"
  })
}

# RabbitMQ Configuration
resource "aws_mq_configuration" "rabbitmq" {
  description    = "RabbitMQ configuration for ${var.project_name}-${var.environment}"
  name           = "${var.project_name}-${var.environment}-rabbitmq-config"
  engine_type    = "RabbitMQ"
  engine_version = "3.12.13"
  
  data = <<DATA
# Default RabbitMQ delivery acknowledgement timeout is 30 minutes
consumer_timeout = 1800000

# Memory high watermark
vm_memory_high_watermark.absolute = 1GB

# Disk space monitoring
disk_free_limit.absolute = 2GB

# Enable management plugin
management_agent.disable_metrics_collector = false

# Clustering
cluster_formation.peer_discovery_backend = rabbit_peer_discovery_aws
cluster_formation.aws.region = ${var.region}
cluster_formation.aws.use_autoscaling_group = true
DATA

  tags = var.tags
}

# Security Group for RabbitMQ
resource "aws_security_group" "rabbitmq" {
  name_prefix = "${var.project_name}-${var.environment}-rabbitmq"
  vpc_id      = aws_vpc.main.id
  
  ingress {
    description     = "AMQP from EKS nodes"
    from_port       = 5672
    to_port         = 5672
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
  }
  
  ingress {
    description     = "Management UI from EKS nodes"
    from_port       = 15672
    to_port         = 15672
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-rabbitmq-sg"
  })
}

# KMS Key for Amazon MQ encryption
resource "aws_kms_key" "mq" {
  description             = "Amazon MQ encryption key"
  deletion_window_in_days = 7
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-mq-kms"
  })
}

resource "aws_kms_alias" "mq" {
  name          = "alias/${var.project_name}-${var.environment}-mq"
  target_key_id = aws_kms_key.mq.key_id
}

# ============================================================================
# KAFKA (AMAZON MSK)
# ============================================================================

# Amazon MSK Cluster (Kafka)
resource "aws_msk_cluster" "kafka" {
  cluster_name           = "${var.project_name}-${var.environment}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = length(aws_subnet.private)
  
  broker_node_group_info {
    instance_type   = var.kafka_instance_type
    client_subnets  = aws_subnet.private[*].id
    storage_info {
      ebs_storage_info {
        volume_size = var.kafka_volume_size
      }
    }
    security_groups = [aws_security_group.kafka.id]
  }
  
  # Encryption
  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.kafka.arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
  
  # Enhanced monitoring
  enhanced_monitoring = "PER_TOPIC_PER_BROKER"
  
  # Open monitoring with Prometheus
  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }
  
  # Logging
  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = var.enable_logging
        log_group = var.enable_logging ? aws_cloudwatch_log_group.kafka[0].name : null
      }
      firehose {
        enabled = false
      }
      s3 {
        enabled = false
      }
    }
  }
  
  # Configuration
  configuration_info {
    arn      = aws_msk_configuration.kafka.arn
    revision = aws_msk_configuration.kafka.latest_revision
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-kafka"
    Type = "streaming-platform"
  })
}

# Kafka Configuration
resource "aws_msk_configuration" "kafka" {
  kafka_versions = [var.kafka_version]
  name           = "${var.project_name}-${var.environment}-kafka-config"
  
  server_properties = <<PROPERTIES
# Replication settings for high availability
default.replication.factor=3
min.insync.replicas=2

# Log retention settings
log.retention.hours=168
log.retention.bytes=1073741824
log.segment.bytes=1073741824

# Compression settings
compression.type=lz4

# Transaction settings
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# Auto create topics disabled for production
auto.create.topics.enable=false

# Performance tuning
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# JVM heap settings
heap.opts=-Xmx4G -Xms4G

# Metrics
metric.reporters=org.apache.kafka.common.metrics.JmxReporter
PROPERTIES
}

# Security Group for Kafka
resource "aws_security_group" "kafka" {
  name_prefix = "${var.project_name}-${var.environment}-kafka"
  vpc_id      = aws_vpc.main.id
  
  ingress {
    description     = "Kafka plaintext from EKS nodes"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
  }
  
  ingress {
    description     = "Kafka TLS from EKS nodes"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
  }
  
  ingress {
    description     = "Zookeeper from EKS nodes"
    from_port       = 2181
    to_port         = 2181
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
  }
  
  ingress {
    description     = "JMX monitoring"
    from_port       = 11001
    to_port         = 11002
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-kafka-sg"
  })
}

# KMS Key for Kafka encryption
resource "aws_kms_key" "kafka" {
  description             = "Kafka encryption key"
  deletion_window_in_days = 7
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-kafka-kms"
  })
}

resource "aws_kms_alias" "kafka" {
  name          = "alias/${var.project_name}-${var.environment}-kafka"
  target_key_id = aws_kms_key.kafka.key_id
}

# CloudWatch Log Group for Kafka
resource "aws_cloudwatch_log_group" "kafka" {
  count = var.enable_logging ? 1 : 0
  
  name              = "/aws/msk/${var.project_name}-${var.environment}-kafka"
  retention_in_days = 14
  
  tags = var.tags
}

# ============================================================================
# BACKUP AND DISASTER RECOVERY
# ============================================================================

# Backup vault for messaging services
resource "aws_backup_vault" "messaging" {
  name        = "${var.project_name}-${var.environment}-messaging-backup"
  kms_key_arn = aws_kms_key.backup.arn
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-messaging-backup"
  })
}

# KMS Key for backup encryption
resource "aws_kms_key" "backup" {
  description             = "Backup encryption key"
  deletion_window_in_days = 7
  
  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-backup-kms"
  })
}

resource "aws_kms_alias" "backup" {
  name          = "alias/${var.project_name}-${var.environment}-backup"
  target_key_id = aws_kms_key.backup.key_id
}

# IAM role for AWS Backup
resource "aws_iam_role" "backup" {
  name = "${var.project_name}-${var.environment}-backup-role"
  
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
  
  tags = var.tags
}

# Attach backup service role policy
resource "aws_iam_role_policy_attachment" "backup" {
  role       = aws_iam_role.backup.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForBackup"
}

# ============================================================================
# MONITORING AND ALERTING FOR MESSAGING SERVICES
# ============================================================================

# CloudWatch Dashboard for messaging services
resource "aws_cloudwatch_dashboard" "messaging" {
  count          = var.enable_monitoring ? 1 : 0
  dashboard_name = "${var.project_name}-${var.environment}-messaging"
  
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
            ["AWS/AmazonMQ", "TotalMessageCount", "Broker", aws_mq_broker.rabbitmq.broker_name],
            ["AWS/AmazonMQ", "MessageReadyCount", "Broker", aws_mq_broker.rabbitmq.broker_name],
            ["AWS/AmazonMQ", "MessageUnacknowledgedCount", "Broker", aws_mq_broker.rabbitmq.broker_name]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.region
          title   = "RabbitMQ Message Metrics"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        
        properties = {
          metrics = [
            ["AWS/Kafka", "MessagesInPerSec", "Cluster Name", aws_msk_cluster.kafka.cluster_name],
            ["AWS/Kafka", "BytesInPerSec", "Cluster Name", aws_msk_cluster.kafka.cluster_name],
            ["AWS/Kafka", "BytesOutPerSec", "Cluster Name", aws_msk_cluster.kafka.cluster_name]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.region
          title   = "Kafka Throughput Metrics"
          period  = 300
        }
      }
    ]
  })
}

# CloudWatch Alarms for RabbitMQ
resource "aws_cloudwatch_metric_alarm" "rabbitmq_connection_count" {
  count = var.enable_monitoring ? 1 : 0
  
  alarm_name          = "${var.project_name}-${var.environment}-rabbitmq-high-connections"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "ConnectionCount"
  namespace           = "AWS/AmazonMQ"
  period              = "300"
  statistic           = "Average"
  threshold           = "80"
  alarm_description   = "This metric monitors RabbitMQ connection count"
  alarm_actions       = [aws_sns_topic.alerts[0].arn]
  
  dimensions = {
    Broker = aws_mq_broker.rabbitmq.broker_name
  }
  
  tags = var.tags
}

# CloudWatch Alarms for Kafka
resource "aws_cloudwatch_metric_alarm" "kafka_cpu_utilization" {
  count = var.enable_monitoring ? 1 : 0
  
  alarm_name          = "${var.project_name}-${var.environment}-kafka-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CpuUser"
  namespace           = "AWS/Kafka"
  period              = "300"
  statistic           = "Average"
  threshold           = "80"
  alarm_description   = "This metric monitors Kafka CPU utilization"
  alarm_actions       = [aws_sns_topic.alerts[0].arn]
  
  dimensions = {
    "Cluster Name" = aws_msk_cluster.kafka.cluster_name
  }
  
  tags = var.tags
}

# SNS Topic for alerts
resource "aws_sns_topic" "alerts" {
  count = var.enable_monitoring ? 1 : 0
  
  name = "${var.project_name}-${var.environment}-messaging-alerts"
  
  tags = var.tags
}

# SNS Topic Policy
resource "aws_sns_topic_policy" "alerts" {
  count = var.enable_monitoring ? 1 : 0
  
  arn = aws_sns_topic.alerts[0].arn
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "cloudwatch.amazonaws.com"
        }
        Action = "sns:Publish"
        Resource = aws_sns_topic.alerts[0].arn
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })
}