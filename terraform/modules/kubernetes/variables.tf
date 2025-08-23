# Kubernetes Module Variables

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "namespace" {
  description = "Kubernetes namespace for the application"
  type        = string
}

variable "labels" {
  description = "Common labels to apply to all resources"
  type        = map(string)
  default     = {}
}

# Database Configuration
variable "database_host" {
  description = "Database host/endpoint"
  type        = string
}

variable "database_name" {
  description = "Database name"
  type        = string
}

variable "database_username" {
  description = "Database username"
  type        = string
  sensitive   = true
}

variable "database_password" {
  description = "Database password"
  type        = string
  sensitive   = true
}

# Container Images
variable "api_image" {
  description = "API container image"
  type        = string
  default     = "pwc-retail-api:latest"
}

variable "etl_image" {
  description = "ETL container image"
  type        = string
  default     = "pwc-retail-etl:latest"
}

variable "dbt_image" {
  description = "dbt container image"
  type        = string
  default     = "pwc-retail-dbt:latest"
}

variable "dagster_image" {
  description = "Dagster container image"
  type        = string
  default     = "pwc-retail-dagster:latest"
}

# Resource Requests
variable "api_cpu_request" {
  description = "API CPU request"
  type        = string
  default     = "100m"
}

variable "api_memory_request" {
  description = "API memory request"
  type        = string
  default     = "256Mi"
}

variable "etl_cpu_request" {
  description = "ETL CPU request"
  type        = string
  default     = "250m"
}

variable "etl_memory_request" {
  description = "ETL memory request"
  type        = string
  default     = "512Mi"
}

# Feature Flags
variable "enable_monitoring" {
  description = "Enable monitoring"
  type        = bool
  default     = true
}

variable "enable_logging" {
  description = "Enable logging"
  type        = bool
  default     = true
}

# Storage Configuration
variable "storage_provisioner" {
  description = "Storage provisioner for the cloud provider"
  type        = string
  default     = "kubernetes.io/aws-ebs"
}

variable "storage_parameters" {
  description = "Storage class parameters"
  type        = map(string)
  default = {
    type      = "gp3"
    fsType    = "ext4" 
    encrypted = "true"
  }
}

# Service Configuration
variable "service_annotations" {
  description = "Service annotations for cloud provider specific configurations"
  type        = map(string)
  default = {
    "service.beta.kubernetes.io/aws-load-balancer-type" = "nlb"
  }
}

# Elasticsearch Configuration
variable "enable_kibana" {
  description = "Enable Kibana deployment"
  type        = bool
  default     = false
}