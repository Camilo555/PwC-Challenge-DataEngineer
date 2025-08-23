# Staging Environment Configuration
# PwC Retail Data Platform - Staging Environment

# General Configuration
project_name     = "pwc-retail-data-platform"
environment      = "staging"
cloud_provider   = "aws"
region          = "us-west-2"

# Network Configuration
vpc_cidr              = "10.1.0.0/16"
availability_zones    = ["us-west-2a", "us-west-2b", "us-west-2c"]
public_subnet_cidrs   = ["10.1.1.0/24", "10.1.2.0/24", "10.1.3.0/24"]
private_subnet_cidrs  = ["10.1.4.0/24", "10.1.5.0/24", "10.1.6.0/24"]

# Kubernetes Configuration
cluster_version       = "1.28"
node_instance_types   = ["t3.large"]
node_desired_size     = 3
node_max_size        = 6
node_min_size        = 2

# Database Configuration
db_instance_class             = "db.t3.small"
db_allocated_storage          = 50
db_backup_retention_period    = 7
db_backup_window             = "03:00-04:00"
db_maintenance_window        = "sun:04:00-sun:05:00"

# Kubernetes Resource Configuration
namespace = "pwc-staging"
api_cpu_request      = "200m"
api_memory_request   = "512Mi"
etl_cpu_request      = "500m"
etl_memory_request   = "1Gi"

# Container Images
api_image      = "pwc-retail-api:staging"
etl_image      = "pwc-retail-etl:staging"
dbt_image      = "pwc-retail-dbt:staging"
dagster_image  = "pwc-retail-dagster:staging"

# Feature Flags
enable_monitoring    = true
enable_logging      = true
enable_encryption   = true
enable_audit_logs   = true

# Storage
storage_class = "gp3"

# Secrets Management
secrets_backend = "kubernetes"

# Common Tags
tags = {
  Environment = "staging"
  Project     = "pwc-retail-data-platform"
  Owner       = "data-engineering-team"
  CostCenter  = "engineering"
  Terraform   = "true"
}