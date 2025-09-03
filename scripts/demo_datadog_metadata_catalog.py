#!/usr/bin/env python3
"""
Datadog Metadata Catalog Demo

This script demonstrates the Datadog-integrated metadata catalog functionality,
showing how to register assets, track lineage, monitor quality, and integrate
with various data platform components.
"""

import asyncio
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.metadata.datadog_metadata_catalog import (
    DatadogMetadataCatalog,
    DataAssetMetadata,
    DataAssetType,
    DataQualityLevel,
    create_metadata_catalog
)
from core.metadata.catalog_integrations import (
    ETLMetadataCollector,
    MLMetadataCollector,
    APIMetadataCollector,
    DatabaseMetadataCollector,
    MetadataCatalogOrchestrator
)
from core.logging import get_logger

logger = get_logger(__name__)


async def demo_basic_catalog_operations():
    """Demonstrate basic catalog operations."""
    print("=== Basic Catalog Operations Demo ===")
    
    # Create catalog instance (in demo mode without actual Datadog keys)
    catalog = create_metadata_catalog(environment="demo")
    
    # Register sample data assets
    print("\n1. Registering sample data assets...")
    
    # Register a raw data table
    raw_sales_metadata = DataAssetMetadata(
        asset_id="raw_sales_data",
        name="Raw Sales Data",
        asset_type=DataAssetType.TABLE,
        description="Raw sales transactions from e-commerce platform",
        owner="john.doe@company.com",
        team="data_engineering",
        location="s3://data-lake/raw/sales/",
        schema={
            "columns": [
                {"name": "transaction_id", "type": "string", "nullable": False},
                {"name": "customer_id", "type": "string", "nullable": False},
                {"name": "product_id", "type": "string", "nullable": False},
                {"name": "quantity", "type": "integer", "nullable": False},
                {"name": "unit_price", "type": "decimal", "nullable": False},
                {"name": "transaction_date", "type": "timestamp", "nullable": False}
            ]
        },
        row_count=1500000,
        size_bytes=45000000,
        quality_level=DataQualityLevel.GOOD,
        quality_score=0.85,
        pii_fields={"customer_id"},
        compliance_tags={"gdpr", "pci_dss"}
    )
    
    success = await catalog.register_asset(raw_sales_metadata)
    print(f"[OK] Raw sales data registered: {success}")
    
    # Register a cleaned data table
    clean_sales_metadata = DataAssetMetadata(
        asset_id="clean_sales_data",
        name="Clean Sales Data",
        asset_type=DataAssetType.TABLE,
        description="Cleaned and validated sales data",
        owner="jane.smith@company.com",
        team="data_engineering",
        location="s3://data-lake/clean/sales/",
        quality_level=DataQualityLevel.EXCELLENT,
        quality_score=0.95,
        upstream_dependencies={"raw_sales_data"}
    )
    
    success = await catalog.register_asset(clean_sales_metadata)
    print(f"[OK] Clean sales data registered: {success}")
    
    # Register an ML model
    model_metadata = DataAssetMetadata(
        asset_id="sales_prediction_model",
        name="Sales Prediction Model",
        asset_type=DataAssetType.MODEL,
        description="XGBoost model for predicting sales trends",
        owner="alice.johnson@company.com",
        team="ml_engineering",
        quality_level=DataQualityLevel.GOOD,
        quality_score=0.87,
        upstream_dependencies={"clean_sales_data"},
        custom_properties={
            "model_type": "xgboost",
            "accuracy": 0.87,
            "features": ["customer_segment", "product_category", "seasonality", "price"]
        }
    )
    
    success = await catalog.register_asset(model_metadata)
    print(f"[OK] ML model registered: {success}")
    
    return catalog


async def demo_lineage_tracking(catalog: DatadogMetadataCatalog):
    """Demonstrate data lineage tracking."""
    print("\n=== Data Lineage Tracking Demo ===")
    
    # Track lineage relationships
    print("\n2. Tracking data lineage...")
    
    # ETL pipeline: raw -> clean -> aggregated -> model
    await catalog.track_lineage(
        downstream_asset_id="clean_sales_data",
        upstream_asset_ids=["raw_sales_data"],
        operation="data_cleaning"
    )
    
    await catalog.track_lineage(
        downstream_asset_id="sales_prediction_model",
        upstream_asset_ids=["clean_sales_data"],
        operation="model_training"
    )
    
    # Get lineage graph
    lineage_graph = await catalog.get_lineage_graph("sales_prediction_model", depth=3)
    print(f"✅ Lineage graph generated for sales_prediction_model:")
    print(f"   Upstream dependencies: {lineage_graph}")
    
    return True


async def demo_quality_monitoring(catalog: DatadogMetadataCatalog):
    """Demonstrate data quality monitoring."""
    print("\n=== Data Quality Monitoring Demo ===")
    
    print("\n3. Monitoring data quality...")
    
    # Simulate quality degradation
    await catalog.update_asset_quality(
        asset_id="raw_sales_data",
        quality_level=DataQualityLevel.FAIR,
        quality_score=0.72,
        quality_details={
            "null_percentage": 0.15,
            "duplicate_percentage": 0.08,
            "schema_violations": 25,
            "last_check": datetime.utcnow().isoformat()
        }
    )
    print("✅ Quality degradation recorded for raw_sales_data")
    
    # Simulate quality improvement
    await catalog.update_asset_quality(
        asset_id="clean_sales_data",
        quality_level=DataQualityLevel.EXCELLENT,
        quality_score=0.98,
        quality_details={
            "null_percentage": 0.01,
            "duplicate_percentage": 0.0,
            "validation_passed": True,
            "last_check": datetime.utcnow().isoformat()
        }
    )
    print("✅ Quality improvement recorded for clean_sales_data")
    
    return True


async def demo_search_and_discovery(catalog: DatadogMetadataCatalog):
    """Demonstrate asset search and discovery."""
    print("\n=== Asset Search and Discovery Demo ===")
    
    print("\n4. Searching and discovering assets...")
    
    # Search for sales-related assets
    sales_assets = await catalog.search_assets(
        query="sales",
        asset_types=[DataAssetType.TABLE, DataAssetType.MODEL],
        limit=10
    )
    print(f"✅ Found {len(sales_assets)} sales-related assets:")
    for asset in sales_assets:
        print(f"   - {asset.name} ({asset.asset_type.value}) - Quality: {asset.quality_score:.2f}")
    
    # Search by team
    data_eng_assets = await catalog.search_assets(
        teams=["data_engineering"],
        limit=10
    )
    print(f"✅ Found {len(data_eng_assets)} data engineering assets")
    
    # Search by quality level
    high_quality_assets = await catalog.search_assets(
        quality_levels=[DataQualityLevel.EXCELLENT, DataQualityLevel.GOOD],
        limit=10
    )
    print(f"✅ Found {len(high_quality_assets)} high-quality assets")
    
    return True


async def demo_etl_integration():
    """Demonstrate ETL metadata collection."""
    print("\n=== ETL Integration Demo ===")
    
    print("\n5. ETL metadata collection...")
    
    etl_collector = ETLMetadataCollector()
    
    # Register an ETL pipeline
    success = await etl_collector.register_etl_pipeline(
        pipeline_name="daily_sales_aggregation",
        source_tables=["raw_sales_data", "raw_customer_data"],
        target_tables=["daily_sales_summary", "customer_metrics"],
        transformation_logic="Aggregate sales by day, calculate customer lifetime value",
        owner="etl-team@company.com",
        team="data_engineering"
    )
    print(f"✅ ETL pipeline registered: {success}")
    
    # Demo dataframe transformation tracking
    # Create sample dataframes
    input_df = pd.DataFrame({
        'customer_id': range(1000),
        'purchase_amount': np.random.uniform(10, 1000, 1000),
        'purchase_date': pd.date_range('2024-01-01', periods=1000, freq='H')
    })
    
    # Simulate transformation
    output_df = input_df.groupby('customer_id').agg({
        'purchase_amount': ['sum', 'mean', 'count']
    }).reset_index()
    output_df.columns = ['customer_id', 'total_spent', 'avg_purchase', 'purchase_count']
    
    success = await etl_collector.register_dataframe_transformation(
        input_df=input_df,
        output_df=output_df,
        transformation_name="customer_aggregation",
        source_asset_id="raw_purchase_data",
        target_asset_id="customer_summary"
    )
    print(f"✅ Dataframe transformation registered: {success}")
    
    return True


async def demo_ml_integration():
    """Demonstrate ML metadata collection."""
    print("\n=== ML Integration Demo ===")
    
    print("\n6. ML metadata collection...")
    
    ml_collector = MLMetadataCollector()
    
    # Register ML model
    success = await ml_collector.register_ml_model(
        model_name="churn_prediction_v2",
        model_type="random_forest",
        training_data_sources=["customer_features", "behavioral_data"],
        features=["tenure", "monthly_charges", "total_charges", "contract_type", "payment_method"],
        target_variable="churn",
        performance_metrics={
            "accuracy": 0.89,
            "precision": 0.85,
            "recall": 0.82,
            "f1_score": 0.83,
            "auc_roc": 0.91
        },
        owner="ml-team@company.com",
        team="ml_engineering"
    )
    print(f"✅ ML model registered: {success}")
    
    # Update model performance (simulate retraining)
    success = await ml_collector.update_model_performance(
        model_name="churn_prediction_v2",
        performance_metrics={
            "accuracy": 0.91,
            "precision": 0.87,
            "recall": 0.85,
            "f1_score": 0.86,
            "auc_roc": 0.93
        },
        evaluation_data={
            "test_set_size": 5000,
            "evaluation_date": datetime.utcnow().isoformat(),
            "confusion_matrix": [[2100, 150], [200, 2550]]
        }
    )
    print(f"✅ Model performance updated: {success}")
    
    return True


async def demo_api_integration():
    """Demonstrate API metadata collection."""
    print("\n=== API Integration Demo ===")
    
    print("\n7. API metadata collection...")
    
    api_collector = APIMetadataCollector()
    
    # Mock API endpoint function
    async def get_sales_summary(start_date: str, end_date: str, team_id: int = None):
        """Get sales summary for a date range, optionally filtered by team."""
        pass
    
    # Register API endpoint
    success = await api_collector.register_api_endpoint(
        endpoint_path="/api/v1/sales/summary",
        method="GET",
        handler_function=get_sales_summary,
        data_sources=["daily_sales_summary", "team_metadata"],
        response_schema={
            "total_sales": "number",
            "transaction_count": "integer",
            "average_order_value": "number",
            "top_products": "array"
        },
        owner="api-team@company.com",
        team="api_engineering"
    )
    print(f"✅ API endpoint registered: {success}")
    
    return True


async def demo_database_integration():
    """Demonstrate database metadata collection."""
    print("\n=== Database Integration Demo ===")
    
    print("\n8. Database metadata collection...")
    
    db_collector = DatabaseMetadataCollector()
    
    # Register database table
    success = await db_collector.register_database_table(
        table_name="fact_sales",
        database_name="analytics_warehouse",
        schema_name="public",
        columns=[
            {"name": "sale_key", "type": "bigint", "nullable": False, "primary_key": True},
            {"name": "date_key", "type": "integer", "nullable": False},
            {"name": "customer_key", "type": "integer", "nullable": False},
            {"name": "product_key", "type": "integer", "nullable": False},
            {"name": "quantity", "type": "integer", "nullable": False},
            {"name": "unit_price", "type": "decimal(10,2)", "nullable": False},
            {"name": "line_amount", "type": "decimal(12,2)", "nullable": False},
            {"name": "discount_amount", "type": "decimal(10,2)", "nullable": True}
        ],
        row_count=15750000,
        table_size_bytes=2100000000,
        owner="dba-team@company.com",
        team="data_engineering"
    )
    print(f"✅ Database table registered: {success}")
    
    return True


async def demo_catalog_health_and_reporting(catalog: DatadogMetadataCatalog):
    """Demonstrate catalog health monitoring and reporting."""
    print("\n=== Catalog Health and Reporting Demo ===")
    
    print("\n9. Catalog health monitoring...")
    
    # Get catalog health metrics
    health_metrics = await catalog.get_catalog_health()
    print("✅ Catalog Health Metrics:")
    print(f"   Total assets: {health_metrics['total_assets']}")
    print(f"   Average quality score: {health_metrics['avg_quality_score']:.2f}")
    print(f"   Recent changes (24h): {health_metrics['recent_changes_24h']}")
    print(f"   Quality distribution: {health_metrics['quality_distribution']}")
    print(f"   Asset type distribution: {health_metrics['type_distribution']}")
    
    # Get comprehensive report using orchestrator
    orchestrator = MetadataCatalogOrchestrator(catalog)
    comprehensive_report = await orchestrator.get_comprehensive_report()
    
    print("\n✅ Comprehensive Report:")
    print(f"   Assets with PII: {comprehensive_report['compliance_summary']['assets_with_pii']}")
    print(f"   High quality assets: {comprehensive_report['quality_insights']['high_quality_assets']}")
    print(f"   Assets needing attention: {comprehensive_report['quality_insights']['needs_attention']}")
    print(f"   Average lineage depth: {comprehensive_report['lineage_analysis']['avg_lineage_depth']:.1f}")
    
    return True


async def demo_catalog_export(catalog: DatadogMetadataCatalog):
    """Demonstrate catalog export functionality."""
    print("\n=== Catalog Export Demo ===")
    
    print("\n10. Exporting catalog...")
    
    # Export catalog to JSON
    catalog_json = await catalog.export_catalog(format="json")
    
    # Save to file
    export_path = Path(__file__).parent.parent / "exports" / "metadata_catalog_export.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text(catalog_json, encoding='utf-8')
    
    print(f"✅ Catalog exported to: {export_path}")
    print(f"   Export size: {len(catalog_json):,} characters")
    
    return True


async def main():
    """Run all demos."""
    print("Datadog Metadata Catalog Demo")
    print("=" * 50)
    
    try:
        # Basic operations
        catalog = await demo_basic_catalog_operations()
        
        # Advanced features
        await demo_lineage_tracking(catalog)
        await demo_quality_monitoring(catalog)
        await demo_search_and_discovery(catalog)
        
        # Integration demos
        await demo_etl_integration()
        await demo_ml_integration()
        await demo_api_integration()
        await demo_database_integration()
        
        # Monitoring and reporting
        await demo_catalog_health_and_reporting(catalog)
        await demo_catalog_export(catalog)
        
        print("\n" + "=" * 50)
        print("🎉 All demos completed successfully!")
        print("=" * 50)
        print("\nKey Features Demonstrated:")
        print("✅ Asset registration and metadata management")
        print("✅ Data lineage tracking and visualization") 
        print("✅ Quality monitoring with Datadog integration")
        print("✅ Asset search and discovery")
        print("✅ ETL pipeline metadata collection")
        print("✅ ML model metadata and performance tracking")
        print("✅ API endpoint documentation")
        print("✅ Database schema management")
        print("✅ Health monitoring and reporting")
        print("✅ Catalog export and backup")
        print("\nDatadog Integration Benefits:")
        print("📊 Real-time metrics and alerting")
        print("📈 Performance monitoring and trends")
        print("🔍 Centralized logging and search")
        print("📋 Compliance and governance tracking")
        print("🚨 Quality degradation alerts")
        print("=" * 50)
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        print(f"\n❌ Demo failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))