# Messaging Module Variables

variable "project_name" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "cloud_provider" {
  description = "Primary cloud provider"
  type        = string
  validation {
    condition     = contains(["aws", "azure", "gcp"], var.cloud_provider)
    error_message = "Cloud provider must be one of: aws, azure, gcp."
  }
}

# Multi-cloud deployment flags
variable "deploy_to_aws" {
  description = "Deploy AWS messaging services"
  type        = bool
  default     = false
}

variable "deploy_to_azure" {
  description = "Deploy Azure messaging services"
  type        = bool
  default     = false
}

variable "deploy_to_gcp" {
  description = "Deploy GCP messaging services"
  type        = bool
  default     = false
}

variable "deploy_k8s_kafka" {
  description = "Deploy Kafka on Kubernetes"
  type        = bool
  default     = true
}

variable "deploy_k8s_rabbitmq" {
  description = "Deploy RabbitMQ on Kubernetes"
  type        = bool
  default     = true
}

# Network Configuration
variable "vpc_id" {
  description = "VPC ID for security groups"
  type        = string
}

variable "private_subnets" {
  description = "Private subnet IDs"
  type        = list(string)
}

variable "security_group_id" {
  description = "Existing security group ID"
  type        = string
  default     = null
}

# RabbitMQ Configuration
variable "rabbitmq_instance_type" {
  description = "RabbitMQ instance type"
  type        = string
  default     = "mq.t3.micro"
}

variable "rabbitmq_username" {
  description = "RabbitMQ admin username"
  type        = string
  default     = "admin"
}

variable "rabbitmq_password" {
  description = "RabbitMQ admin password"
  type        = string
  sensitive   = true
}

variable "rabbitmq_version" {
  description = "RabbitMQ version"
  type        = string
  default     = "3.11.28"
}

variable "rabbitmq_storage_size" {
  description = "RabbitMQ storage size in GB"
  type        = number
  default     = 20
}

# Kafka Configuration
variable "kafka_version" {
  description = "Kafka version"
  type        = string
  default     = "2.8.1"
}

variable "kafka_instance_type" {
  description = "Kafka instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "kafka_broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 3
  
  validation {
    condition     = var.kafka_broker_count >= 3
    error_message = "Kafka broker count must be at least 3 for production use."
  }
}

variable "kafka_partition_count" {
  description = "Default number of partitions per topic"
  type        = number
  default     = 3
}

variable "kafka_replication_factor" {
  description = "Default replication factor"
  type        = number
  default     = 3
  
  validation {
    condition     = var.kafka_replication_factor >= 3
    error_message = "Kafka replication factor must be at least 3 for production use."
  }
}

variable "kafka_storage_size" {
  description = "Kafka storage size in GB per broker"
  type        = number
  default     = 100
}

variable "kafka_topic_prefix" {
  description = "Prefix for Kafka topics"
  type        = string
  default     = "pwc-retail"
}

# Monitoring and Logging
variable "enable_monitoring" {
  description = "Enable monitoring and metrics collection"
  type        = bool
  default     = true
}

variable "enable_logging" {
  description = "Enable centralized logging"
  type        = bool
  default     = true
}

# Tags and Labels
variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

variable "labels" {
  description = "Resource labels (for Kubernetes resources)"
  type        = map(string)
  default     = {}
}