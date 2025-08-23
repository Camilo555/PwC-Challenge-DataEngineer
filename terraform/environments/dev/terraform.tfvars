# Development Environment Configuration
# PwC Retail Data Platform - Development Environment

# General Configuration
project_name     = "pwc-retail-data-platform"
environment      = "dev"
cloud_provider   = "aws"
region          = "us-west-2"

# Network Configuration
vpc_cidr              = "10.0.0.0/16"
availability_zones    = ["us-west-2a", "us-west-2b", "us-west-2c"]
public_subnet_cidrs   = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
private_subnet_cidrs  = ["10.0.4.0/24", "10.0.5.0/24", "10.0.6.0/24"]

# Kubernetes Configuration
cluster_version       = "1.28"
node_instance_types   = ["t3.medium"]
node_desired_size     = 2
node_max_size        = 4
node_min_size        = 1

# Database Configuration
db_instance_class             = "db.t3.micro"
db_allocated_storage          = 20
db_backup_retention_period    = 3
db_backup_window             = "03:00-04:00"
db_maintenance_window        = "sun:04:00-sun:05:00"

# Kubernetes Resource Configuration
namespace = "pwc-dev"
api_cpu_request      = "100m"
api_memory_request   = "256Mi"
etl_cpu_request      = "250m"
etl_memory_request   = "512Mi"

# Container Images
api_image      = "pwc-retail-api:dev"
etl_image      = "pwc-retail-etl:dev"
dbt_image      = "pwc-retail-dbt:dev"
dagster_image  = "pwc-retail-dagster:dev"

# Feature Flags
enable_monitoring    = true
enable_logging      = true
enable_encryption   = true
enable_audit_logs   = false

# Storage
storage_class = "gp2"

# Secrets Management
secrets_backend = "kubernetes"

# Common Tags
tags = {
  Environment = "dev"
  Project     = "pwc-retail-data-platform"
  Owner       = "data-engineering-team"
  CostCenter  = "engineering"
  Terraform   = "true"
}