# AWS Infrastructure Module for Data Platform
# Optimized for production workloads with cost optimization and security

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Data sources for availability zones and AMIs
data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ami" "eks_worker" {
  most_recent = true
  owners      = ["amazon"]
  
  filter {
    name   = "name"
    values = ["amazon-eks-node-1.28-*"]
  }
  
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

# VPC for secure networking
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-vpc"
    Type = "networking"
  })
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-igw"
  })
}

# Public Subnets (for load balancers and NAT gateways)
resource "aws_subnet" "public" {
  count = 3

  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 1}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  map_public_ip_on_launch = true

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-public-${count.index + 1}"
    Type = "public"
    "kubernetes.io/role/elb" = "1"
  })
}

# Private Subnets (for EKS nodes and databases)
resource "aws_subnet" "private" {
  count = 3

  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-private-${count.index + 1}"
    Type = "private"
    "kubernetes.io/role/internal-elb" = "1"
  })
}

# Data Subnets (for data services with additional isolation)
resource "aws_subnet" "data" {
  count = 3

  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 20}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-data-${count.index + 1}"
    Type = "data"
  })
}

# NAT Gateways for private subnet internet access
resource "aws_eip" "nat" {
  count = var.env_config.high_availability ? 3 : 1

  domain = "vpc"
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-nat-eip-${count.index + 1}"
  })

  depends_on = [aws_internet_gateway.main]
}

resource "aws_nat_gateway" "main" {
  count = var.env_config.high_availability ? 3 : 1

  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-nat-${count.index + 1}"
  })

  depends_on = [aws_internet_gateway.main]
}

# Route Tables
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-public-rt"
  })
}

resource "aws_route_table" "private" {
  count = var.env_config.high_availability ? 3 : 1

  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main[count.index].id
  }

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-private-rt-${count.index + 1}"
  })
}

# Route Table Associations
resource "aws_route_table_association" "public" {
  count = 3

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count = 3

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[var.env_config.high_availability ? count.index : 0].id
}

resource "aws_route_table_association" "data" {
  count = 3

  subnet_id      = aws_subnet.data[count.index].id
  route_table_id = aws_route_table.private[var.env_config.high_availability ? count.index : 0].id
}

# Security Groups
resource "aws_security_group" "eks_cluster" {
  name        = "${var.resource_prefix}-eks-cluster-sg"
  description = "Security group for EKS cluster"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-eks-cluster-sg"
  })
}

resource "aws_security_group" "eks_nodes" {
  name        = "${var.resource_prefix}-eks-nodes-sg"
  description = "Security group for EKS worker nodes"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 0
    to_port         = 65535
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_cluster.id]
    description     = "Allow cluster control plane"
  }

  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    self      = true
    description = "Allow nodes to communicate with each other"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-eks-nodes-sg"
  })
}

resource "aws_security_group" "data_services" {
  name        = "${var.resource_prefix}-data-services-sg"
  description = "Security group for data services (RDS, Redshift, etc.)"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
    description     = "PostgreSQL from EKS nodes"
  }

  ingress {
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
    description     = "Redshift from EKS nodes"
  }

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-data-services-sg"
  })
}

# KMS Key for encryption
resource "aws_kms_key" "main" {
  description             = "KMS key for ${var.resource_prefix} data platform"
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
      }
    ]
  })

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-kms-key"
  })
}

resource "aws_kms_alias" "main" {
  name          = "alias/${var.resource_prefix}-data-platform"
  target_key_id = aws_kms_key.main.key_id
}

data "aws_caller_identity" "current" {}

# EKS Cluster IAM Role
resource "aws_iam_role" "eks_cluster" {
  name = "${var.resource_prefix}-eks-cluster-role"

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

  tags = var.common_tags
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_cluster.name
}

# EKS Node Group IAM Role
resource "aws_iam_role" "eks_node_group" {
  name = "${var.resource_prefix}-eks-nodegroup-role"

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

  tags = var.common_tags
}

resource "aws_iam_role_policy_attachment" "eks_worker_node_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_node_group.name
}

resource "aws_iam_role_policy_attachment" "eks_cni_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_node_group.name
}

resource "aws_iam_role_policy_attachment" "eks_container_registry_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_node_group.name
}

# Additional permissions for data access
resource "aws_iam_role_policy" "eks_data_access" {
  name = "${var.resource_prefix}-eks-data-access"
  role = aws_iam_role.eks_node_group.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.main.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:*"
      }
    ]
  })
}

# EKS Cluster
resource "aws_eks_cluster" "main" {
  name     = "${var.resource_prefix}-eks"
  role_arn = aws_iam_role.eks_cluster.arn
  version  = "1.28"

  vpc_config {
    subnet_ids              = concat(aws_subnet.private[*].id, aws_subnet.public[*].id)
    endpoint_private_access = true
    endpoint_public_access  = var.environment == "production" ? false : true
    security_group_ids      = [aws_security_group.eks_cluster.id]
  }

  encryption_config {
    provider {
      key_arn = aws_kms_key.main.arn
    }
    resources = ["secrets"]
  }

  enabled_cluster_log_types = var.env_config.monitoring_level == "comprehensive" ? [
    "api", "audit", "authenticator", "controllerManager", "scheduler"
  ] : ["api", "audit"]

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-eks"
  })

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
    aws_cloudwatch_log_group.eks
  ]
}

# CloudWatch Log Group for EKS
resource "aws_cloudwatch_log_group" "eks" {
  name              = "/aws/eks/${var.resource_prefix}-eks/cluster"
  retention_in_days = var.env_config.backup_retention
  kms_key_id        = aws_kms_key.main.arn

  tags = var.common_tags
}

# EKS Node Group
resource "aws_eks_node_group" "main" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.resource_prefix}-nodes"
  node_role_arn   = aws_iam_role.eks_node_group.arn
  subnet_ids      = aws_subnet.private[*].id

  capacity_type  = var.enable_cost_optimization ? "SPOT" : "ON_DEMAND"
  instance_types = var.env_config.cluster_size == "small" ? ["t3.medium"] : (
    var.env_config.cluster_size == "medium" ? ["t3.large", "t3.xlarge"] : 
    ["m5.xlarge", "m5.2xlarge", "c5.2xlarge"]
  )

  scaling_config {
    desired_size = var.env_config.cluster_size == "small" ? 2 : (
      var.env_config.cluster_size == "medium" ? 3 : 5
    )
    max_size     = var.env_config.auto_scaling ? (var.env_config.cluster_size == "small" ? 4 : 
      (var.env_config.cluster_size == "medium" ? 8 : 20)) : (
      var.env_config.cluster_size == "small" ? 2 : (
        var.env_config.cluster_size == "medium" ? 3 : 5
      )
    )
    min_size = 1
  }

  update_config {
    max_unavailable_percentage = 25
  }

  # Launch template for additional configuration
  launch_template {
    name    = aws_launch_template.eks_nodes.name
    version = aws_launch_template.eks_nodes.latest_version
  }

  tags = var.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_container_registry_policy,
    aws_iam_role_policy.eks_data_access,
  ]
}

# Launch Template for EKS Nodes
resource "aws_launch_template" "eks_nodes" {
  name_prefix   = "${var.resource_prefix}-eks-nodes-"
  image_id      = data.aws_ami.eks_worker.id
  instance_type = "t3.medium"

  vpc_security_group_ids = [aws_security_group.eks_nodes.id]

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.env_config.cluster_size == "small" ? 30 : 50
      volume_type           = "gp3"
      encrypted             = true
      kms_key_id           = aws_kms_key.main.arn
      delete_on_termination = true
    }
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
    instance_metadata_tags      = "enabled"
  }

  monitoring {
    enabled = var.env_config.monitoring_level != "basic"
  }

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.common_tags, {
      Name = "${var.resource_prefix}-eks-node"
    })
  }
}

# Data Lake S3 Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.resource_prefix}-data-lake-${random_id.bucket_suffix.hex}"

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-data-lake"
    Type = "data-storage"
  })
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_encryption" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = aws_kms_key.main.arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  count  = var.enable_cost_optimization ? 1 : 0
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "transition_to_ia"
    status = "Enabled"

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

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.env_config.backup_retention
    }
  }
}

# Kinesis Streams for real-time data
resource "aws_kinesis_stream" "data_stream" {
  name             = "${var.resource_prefix}-data-stream"
  shard_count      = var.env_config.cluster_size == "small" ? 1 : (
    var.env_config.cluster_size == "medium" ? 3 : 5
  )
  retention_period = var.env_config.backup_retention > 24 ? 24 : var.env_config.backup_retention

  encryption_type = "KMS"
  kms_key_id     = aws_kms_key.main.arn

  shard_level_metrics = var.env_config.monitoring_level == "comprehensive" ? [
    "IncomingRecords",
    "OutgoingRecords",
  ] : []

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-kinesis"
  })
}

# Redshift Cluster for data warehousing
resource "aws_redshift_subnet_group" "main" {
  name       = "${var.resource_prefix}-redshift-subnet-group"
  subnet_ids = aws_subnet.data[*].id

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-redshift-subnet-group"
  })
}

resource "aws_redshift_cluster" "main" {
  count = var.env_config.cluster_size != "small" ? 1 : 0

  cluster_identifier     = "${var.resource_prefix}-redshift"
  database_name         = "datawarehouse"
  master_username       = "dwadmin"
  manage_master_password = true
  
  node_type       = var.env_config.cluster_size == "medium" ? "dc2.large" : "dc2.8xlarge"
  number_of_nodes = var.env_config.cluster_size == "medium" ? 2 : 4

  db_subnet_group_name   = aws_redshift_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.data_services.id]

  encrypted   = true
  kms_key_id = aws_kms_key.main.arn

  backup_retention_period = var.env_config.backup_retention
  preferred_backup_window = "03:00-04:00"

  skip_final_snapshot = var.environment != "production"
  final_snapshot_identifier = var.environment == "production" ? "${var.resource_prefix}-redshift-final-snapshot-${formatdate("YYYY-MM-DD-hhmm", timestamp())}" : null

  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-redshift"
  })
}

# CloudWatch Log Groups for centralized logging
resource "aws_cloudwatch_log_group" "application_logs" {
  name              = "/aws/application/${var.resource_prefix}"
  retention_in_days = var.env_config.backup_retention
  kms_key_id        = aws_kms_key.main.arn

  tags = var.common_tags
}

# Cost and Usage Budget (if cost optimization enabled)
resource "aws_budgets_budget" "main" {
  count = var.enable_cost_optimization ? 1 : 0

  name         = "${var.resource_prefix}-monthly-budget"
  budget_type  = "COST"
  limit_amount = var.env_config.cluster_size == "small" ? "500" : (
    var.env_config.cluster_size == "medium" ? "2000" : "5000"
  )
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filters = {
    Tag = ["Project:${var.common_tags.Project}"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = ["admin@company.com"] # Should be configurable
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = ["admin@company.com"]
  }

  tags = var.common_tags
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "resource_prefix" {
  description = "Resource naming prefix"
  type        = string
}

variable "common_tags" {
  description = "Common tags for all resources"
  type        = map(string)
}

variable "enable_disaster_recovery" {
  description = "Enable disaster recovery features"
  type        = bool
  default     = false
}

variable "enable_cost_optimization" {
  description = "Enable cost optimization features"
  type        = bool
  default     = true
}

variable "env_config" {
  description = "Environment-specific configuration"
  type = object({
    cluster_size        = string
    high_availability  = bool
    auto_scaling       = bool
    backup_retention   = number
    monitoring_level   = string
  })
}

# Outputs
output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "security_group_ids" {
  description = "Security group IDs"
  value = {
    eks_cluster    = aws_security_group.eks_cluster.id
    eks_nodes      = aws_security_group.eks_nodes.id
    data_services  = aws_security_group.data_services.id
  }
}

output "kms_key_id" {
  description = "KMS key ID"
  value       = aws_kms_key.main.id
}

output "eks_cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = aws_eks_cluster.main.endpoint
}

output "eks_cluster_ca" {
  description = "EKS cluster certificate authority"
  value       = aws_eks_cluster.main.certificate_authority[0].data
}

output "data_lake_bucket" {
  description = "Data lake S3 bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "kinesis_streams" {
  description = "Kinesis stream names"
  value = {
    data_stream = aws_kinesis_stream.data_stream.name
  }
}

output "redshift_cluster_endpoint" {
  description = "Redshift cluster endpoint"
  value       = var.env_config.cluster_size != "small" ? aws_redshift_cluster.main[0].endpoint : null
}

output "cloudwatch_log_groups" {
  description = "CloudWatch log groups"
  value = {
    eks_logs         = aws_cloudwatch_log_group.eks.name
    application_logs = aws_cloudwatch_log_group.application_logs.name
  }
}