# Data Lake Module Outputs

output "bronze_layer_paths" {
  description = "Bronze layer storage paths across clouds"
  value = {
    aws_s3     = var.aws_s3_bucket != null ? "s3://${aws_s3_bucket.data_lake_bronze[0].bucket}" : null
    azure_adls = var.azure_storage_account != null ? "abfss://bronze@${azurerm_storage_account.data_lake[0].name}.dfs.core.windows.net" : null
    gcp_gcs    = var.gcp_storage_bucket != null ? "gs://${google_storage_bucket.data_lake_bronze[0].name}" : null
  }
}

output "silver_layer_paths" {
  description = "Silver layer storage paths across clouds"
  value = {
    aws_s3     = var.aws_s3_bucket != null ? "s3://${aws_s3_bucket.data_lake_silver[0].bucket}" : null
    azure_adls = var.azure_storage_account != null ? "abfss://silver@${azurerm_storage_account.data_lake[0].name}.dfs.core.windows.net" : null
    gcp_gcs    = var.gcp_storage_bucket != null ? "gs://${google_storage_bucket.data_lake_silver[0].name}" : null
  }
}

output "gold_layer_paths" {
  description = "Gold layer storage paths across clouds"
  value = {
    aws_s3     = var.aws_s3_bucket != null ? "s3://${aws_s3_bucket.data_lake_gold[0].bucket}" : null
    azure_adls = var.azure_storage_account != null ? "abfss://gold@${azurerm_storage_account.data_lake[0].name}.dfs.core.windows.net" : null
    gcp_gcs    = var.gcp_storage_bucket != null ? "gs://${google_storage_bucket.data_lake_gold[0].name}" : null
  }
}

output "aws_glue_catalog_databases" {
  description = "AWS Glue catalog database names"
  value = var.aws_s3_bucket != null ? {
    bronze = aws_glue_catalog_database.bronze[0].name
    silver = aws_glue_catalog_database.silver[0].name
    gold   = aws_glue_catalog_database.gold[0].name
  } : null
}

output "delta_lake_config" {
  description = "Delta Lake configuration"
  value = {
    config_map_name = kubernetes_config_map.delta_lake_config.metadata[0].name
    namespace       = kubernetes_config_map.delta_lake_config.metadata[0].namespace
  }
}

output "iceberg_config" {
  description = "Apache Iceberg configuration"
  value = {
    config_map_name = kubernetes_config_map.iceberg_config.metadata[0].name
    namespace       = kubernetes_config_map.iceberg_config.metadata[0].namespace
  }
}

output "schema_registry_endpoint" {
  description = "Schema Registry service endpoint"
  value = "http://schema-registry.data-platform.svc.cluster.local:8081"
}

output "spark_history_server_endpoint" {
  description = "Spark History Server endpoint"
  value = "http://spark-history-server.data-platform.svc.cluster.local:18080"
}

output "delta_sharing_server_endpoint" {
  description = "Delta Sharing Server endpoint"
  value = "http://delta-sharing-server.data-platform.svc.cluster.local:8080"
}

output "apache_atlas_endpoint" {
  description = "Apache Atlas data lineage endpoint"
  value = "http://apache-atlas.data-platform.svc.cluster.local:21000"
}