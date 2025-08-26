# Messaging Module Outputs

# ============================================================================
# AWS MESSAGING OUTPUTS
# ============================================================================

output "aws_msk_cluster_arn" {
  description = "Amazon MSK cluster ARN"
  value       = var.cloud_provider == "aws" || var.deploy_to_aws ? aws_msk_cluster.main[0].arn : null
}

output "aws_msk_bootstrap_brokers" {
  description = "Amazon MSK bootstrap brokers"
  value       = var.cloud_provider == "aws" || var.deploy_to_aws ? aws_msk_cluster.main[0].bootstrap_brokers : null
}

output "aws_msk_bootstrap_brokers_sasl_scram" {
  description = "Amazon MSK bootstrap brokers (SASL/SCRAM)"
  value       = var.cloud_provider == "aws" || var.deploy_to_aws ? aws_msk_cluster.main[0].bootstrap_brokers_sasl_scram : null
}

output "aws_msk_bootstrap_brokers_tls" {
  description = "Amazon MSK bootstrap brokers (TLS)"
  value       = var.cloud_provider == "aws" || var.deploy_to_aws ? aws_msk_cluster.main[0].bootstrap_brokers_tls : null
}

output "aws_msk_zookeeper_connect_string" {
  description = "Amazon MSK Zookeeper connection string"
  value       = var.cloud_provider == "aws" || var.deploy_to_aws ? aws_msk_cluster.main[0].zookeeper_connect_string : null
}

output "aws_rabbitmq_broker_id" {
  description = "Amazon MQ RabbitMQ broker ID"
  value       = var.cloud_provider == "aws" || var.deploy_to_aws ? aws_mq_broker.main[0].id : null
}

output "aws_rabbitmq_console_url" {
  description = "Amazon MQ RabbitMQ management console URL"
  value       = var.cloud_provider == "aws" || var.deploy_to_aws ? "https://${aws_mq_broker.main[0].instances[0].console_url}" : null
}

output "aws_rabbitmq_endpoint" {
  description = "Amazon MQ RabbitMQ endpoint"
  value       = var.cloud_provider == "aws" || var.deploy_to_aws ? aws_mq_broker.main[0].instances[0].endpoints[0] : null
}

# ============================================================================
# KUBERNETES MESSAGING OUTPUTS
# ============================================================================

output "k8s_kafka_namespace" {
  description = "Kubernetes namespace for Kafka"
  value       = var.deploy_k8s_kafka ? kubernetes_namespace.kafka[0].metadata[0].name : null
}

output "k8s_kafka_cluster_name" {
  description = "Kubernetes Kafka cluster name"
  value       = var.deploy_k8s_kafka ? "${var.project_name}-${var.environment}-kafka" : null
}

output "k8s_kafka_bootstrap_service" {
  description = "Kubernetes Kafka bootstrap service name"
  value       = var.deploy_k8s_kafka ? "${var.project_name}-${var.environment}-kafka-kafka-bootstrap" : null
}

output "k8s_kafka_bootstrap_servers" {
  description = "Kubernetes Kafka bootstrap servers"
  value = var.deploy_k8s_kafka ? (
    "${var.project_name}-${var.environment}-kafka-kafka-bootstrap.${kubernetes_namespace.kafka[0].metadata[0].name}.svc.cluster.local:9092"
  ) : null
}

output "k8s_schema_registry_service" {
  description = "Kubernetes Schema Registry service name"
  value       = var.deploy_k8s_kafka ? "${var.project_name}-${var.environment}-schema-registry" : null
}

output "k8s_kafka_connect_service" {
  description = "Kubernetes Kafka Connect service name"
  value       = var.deploy_k8s_kafka ? "${var.project_name}-${var.environment}-kafka-connect-api" : null
}

output "k8s_rabbitmq_namespace" {
  description = "Kubernetes namespace for RabbitMQ"
  value       = var.deploy_k8s_rabbitmq ? kubernetes_namespace.rabbitmq[0].metadata[0].name : null
}

output "k8s_rabbitmq_cluster_name" {
  description = "Kubernetes RabbitMQ cluster name"
  value       = var.deploy_k8s_rabbitmq ? "${var.project_name}-${var.environment}-rabbitmq" : null
}

output "k8s_rabbitmq_service" {
  description = "Kubernetes RabbitMQ service name"
  value = var.deploy_k8s_rabbitmq ? (
    "${var.project_name}-${var.environment}-rabbitmq.${kubernetes_namespace.rabbitmq[0].metadata[0].name}.svc.cluster.local"
  ) : null
}

# ============================================================================
# UNIFIED MESSAGING ENDPOINTS
# ============================================================================

output "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers (unified endpoint)"
  value = var.deploy_k8s_kafka ? (
    "${var.project_name}-${var.environment}-kafka-kafka-bootstrap.${kubernetes_namespace.kafka[0].metadata[0].name}.svc.cluster.local:9092"
  ) : var.cloud_provider == "aws" || var.deploy_to_aws ? (
    aws_msk_cluster.main[0].bootstrap_brokers
  ) : null
}

output "rabbitmq_endpoint" {
  description = "RabbitMQ endpoint (unified)"
  value = var.deploy_k8s_rabbitmq ? (
    "${var.project_name}-${var.environment}-rabbitmq.${kubernetes_namespace.rabbitmq[0].metadata[0].name}.svc.cluster.local:5672"
  ) : var.cloud_provider == "aws" || var.deploy_to_aws ? (
    aws_mq_broker.main[0].instances[0].endpoints[0]
  ) : null
}

output "schema_registry_endpoint" {
  description = "Schema Registry endpoint"
  value = var.deploy_k8s_kafka ? (
    "http://${var.project_name}-${var.environment}-schema-registry.${kubernetes_namespace.kafka[0].metadata[0].name}.svc.cluster.local:8081"
  ) : null
}

output "kafka_connect_endpoint" {
  description = "Kafka Connect endpoint"
  value = var.deploy_k8s_kafka ? (
    "http://${var.project_name}-${var.environment}-kafka-connect-api.${kubernetes_namespace.kafka[0].metadata[0].name}.svc.cluster.local:8083"
  ) : null
}

# ============================================================================
# TOPIC CONFIGURATION OUTPUTS
# ============================================================================

output "kafka_topics" {
  description = "Kafka topics configuration"
  value = var.deploy_k8s_kafka ? [
    for topic in local.kafka_topics : {
      name               = topic.name
      partitions        = topic.partitions
      replication_factor = topic.replication_factor
      config            = topic.config
    }
  ] : []
}

output "kafka_topic_names" {
  description = "List of Kafka topic names"
  value       = var.deploy_k8s_kafka ? [for topic in local.kafka_topics : topic.name] : []
}

# ============================================================================
# MONITORING OUTPUTS
# ============================================================================

output "monitoring_configuration" {
  description = "Messaging monitoring configuration"
  value = {
    kafka_monitoring_enabled    = var.deploy_k8s_kafka && var.enable_monitoring
    rabbitmq_monitoring_enabled = var.deploy_k8s_rabbitmq && var.enable_monitoring
    
    aws_cloudwatch_enabled = (var.cloud_provider == "aws" || var.deploy_to_aws) && var.enable_logging
    
    prometheus_endpoints = {
      kafka = var.deploy_k8s_kafka && var.enable_monitoring ? (
        "http://${var.project_name}-${var.environment}-kafka-kafka-exporter.${kubernetes_namespace.kafka[0].metadata[0].name}.svc.cluster.local:9308/metrics"
      ) : null
      
      rabbitmq = var.deploy_k8s_rabbitmq && var.enable_monitoring ? (
        "http://${var.project_name}-${var.environment}-rabbitmq.${kubernetes_namespace.rabbitmq[0].metadata[0].name}.svc.cluster.local:15692/metrics"
      ) : null
    }
    
    log_destinations = {
      aws_cloudwatch = var.cloud_provider == "aws" || var.deploy_to_aws && var.enable_logging ? (
        aws_cloudwatch_log_group.msk[0].name
      ) : null
      
      aws_s3 = var.cloud_provider == "aws" || var.deploy_to_aws && var.enable_logging ? (
        aws_s3_bucket.msk_logs[0].bucket
      ) : null
    }
  }
}

# ============================================================================
# SECURITY OUTPUTS
# ============================================================================

output "security_configuration" {
  description = "Messaging security configuration"
  value = {
    aws_msk_encryption = var.cloud_provider == "aws" || var.deploy_to_aws ? {
      kms_key_arn              = aws_kms_key.msk[0].arn
      encryption_in_transit    = "TLS"
      encryption_at_rest      = "KMS"
      client_authentication  = ["TLS", "SASL_SCRAM"]
    } : null
    
    aws_rabbitmq_encryption = var.cloud_provider == "aws" || var.deploy_to_aws ? {
      kms_key_arn = aws_kms_key.rabbitmq[0].arn
    } : null
    
    k8s_kafka_security = var.deploy_k8s_kafka ? {
      authentication_enabled = true
      authorization_enabled  = true
      tls_enabled           = true
    } : null
  }
}

# Local values for internal reference
locals {
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
}