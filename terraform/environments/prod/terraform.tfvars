# Production Environment Configuration
# PwC Retail Data Platform - Production Environment

# General Configuration
project_name     = "pwc-retail-data-platform"
environment      = "prod"
cloud_provider   = "aws"
region          = "us-west-2"

# Network Configuration
vpc_cidr              = "10.2.0.0/16"
availability_zones    = ["us-west-2a", "us-west-2b", "us-west-2c"]
public_subnet_cidrs   = ["10.2.1.0/24", "10.2.2.0/24", "10.2.3.0/24"]
private_subnet_cidrs  = ["10.2.4.0/24", "10.2.5.0/24", "10.2.6.0/24"]

# Kubernetes Configuration
cluster_version       = "1.28"
node_instance_types   = ["m5.large", "m5.xlarge"]
node_desired_size     = 5
node_max_size        = 20
node_min_size        = 3

# Database Configuration
db_instance_class             = "db.r5.large"
db_allocated_storage          = 200
db_backup_retention_period    = 30
db_backup_window             = "03:00-04:00"
db_maintenance_window        = "sun:04:00-sun:05:00"

# Kubernetes Resource Configuration
namespace = "pwc-prod"
api_cpu_request      = "500m"
api_memory_request   = "1Gi"
etl_cpu_request      = "1000m"
etl_memory_request   = "2Gi"

# Container Images
api_image      = "pwc-retail-api:latest"
etl_image      = "pwc-retail-etl:latest"
dbt_image      = "pwc-retail-dbt:latest"
dagster_image  = "pwc-retail-dagster:latest"

# Feature Flags
enable_monitoring    = true
enable_logging      = true
enable_encryption   = true
enable_audit_logs   = true

# Storage
storage_class = "gp3"

# Secrets Management
secrets_backend = "aws-secrets-manager"

# Common Tags
tags = {
  Environment = "prod"
  Project     = "pwc-retail-data-platform"
  Owner       = "data-engineering-team"
  CostCenter  = "production"
  Terraform   = "true"
  Backup      = "required"
  Compliance  = "required"
}