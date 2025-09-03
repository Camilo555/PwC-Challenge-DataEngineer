#!/usr/bin/env python3
"""
Schema Evolution Demo Script

Demonstrates the schema evolution capabilities across medallion architecture layers,
showing how to handle schema changes gracefully as data structures evolve.
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from etl.schema_evolution import (
    create_medallion_handler,
    MedallionLayer,
    SchemaChangeType,
    CompatibilityLevel
)
from core.logging import get_logger

logger = get_logger(__name__)


def demo_bronze_schema_evolution():
    """Demonstrate bronze layer schema evolution."""
    print("=== Bronze Layer Schema Evolution Demo ===")
    
    # Create handler
    handler = create_medallion_handler()
    
    # Initial bronze data - raw sales data
    print("\n1. Initial bronze data ingestion...")
    initial_data = pd.DataFrame({
        'transaction_id': ['TXN001', 'TXN002', 'TXN003'],
        'customer_id': ['CUST001', 'CUST002', 'CUST001'],
        'product_id': ['PROD001', 'PROD002', 'PROD001'],
        'quantity': [2, 1, 3],
        'unit_price': [25.99, 15.50, 25.99],
        'transaction_date': ['2024-01-15', '2024-01-15', '2024-01-16']
    })
    
    # Evolve bronze schema - first version
    result = handler.evolve_bronze_schema(
        table_name='sales_transactions_bronze',
        new_data=initial_data,
        source_system='e_commerce_api',
        auto_register=True
    )
    
    print(f"[OK] Initial bronze schema registered: {result['schema_registered']}")
    print(f"     Metadata columns added: {result['evolved_data'].columns.tolist()[-4:]}")
    
    # Simulate new data with additional columns (schema evolution)
    print("\n2. Bronze data with new columns...")
    evolved_data = pd.DataFrame({
        'transaction_id': ['TXN004', 'TXN005'],
        'customer_id': ['CUST003', 'CUST004'],
        'product_id': ['PROD003', 'PROD001'],
        'quantity': [1, 2],
        'unit_price': [35.99, 25.99],
        'transaction_date': ['2024-01-17', '2024-01-17'],
        'discount_amount': [5.00, 0.00],  # New column
        'payment_method': ['credit_card', 'paypal'],  # New column
        'shipping_cost': [7.99, 9.99]  # New column
    })
    
    result = handler.evolve_bronze_schema(
        table_name='sales_transactions_bronze',
        new_data=evolved_data,
        source_system='e_commerce_api_v2',
        auto_register=True
    )
    
    print(f"[OK] Bronze schema evolved: {result['schema_evolved']}")
    print(f"     Changes detected: {len(result['changes'])}")
    for change in result['changes']:
        print(f"     - {change['type']}: {change['column']}")
    
    return handler


def demo_silver_schema_evolution(handler):
    """Demonstrate silver layer schema evolution."""
    print("\n=== Silver Layer Schema Evolution Demo ===")
    
    # Create cleaned silver data from bronze
    print("\n3. Silver layer data cleaning...")
    silver_data = pd.DataFrame({
        'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN004', 'TXN005'],
        'customer_id': ['CUST001', 'CUST002', 'CUST001', 'CUST003', 'CUST004'],
        'product_id': ['PROD001', 'PROD002', 'PROD001', 'PROD003', 'PROD001'],
        'quantity': [2, 1, 3, 1, 2],
        'unit_price': [25.99, 15.50, 25.99, 35.99, 25.99],
        'total_amount': [51.98, 15.50, 77.97, 35.99, 51.98],  # Calculated field
        'transaction_date': pd.to_datetime(['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-17', '2024-01-17']),
        'discount_amount': [0.00, 0.00, 0.00, 5.00, 0.00],
        'net_amount': [51.98, 15.50, 77.97, 30.99, 51.98],  # total - discount
        'is_discounted': [False, False, False, True, False]  # Business logic
    })
    
    # Business rules for silver layer
    business_rules = {
        'quantity': {'min_value': 1, 'max_value': 100, 'required': True},
        'unit_price': {'min_value': 0.01, 'required': True},
        'total_amount': {'min_value': 0.01, 'required': True},
        'discount_amount': {'min_value': 0.0, 'required': False}
    }
    
    result = handler.evolve_silver_schema(
        table_name='sales_transactions_silver',
        new_data=silver_data,
        business_rules=business_rules,
        validate_quality=True
    )
    
    print(f"[OK] Silver schema registered: {result['schema_registered']}")
    print(f"     Quality issues found: {len(result['quality_issues'])}")
    for issue in result['quality_issues']:
        print(f"     - {issue}")
    
    # Simulate schema change with business rule validation
    print("\n4. Silver schema with business rule changes...")
    updated_silver_data = silver_data.copy()
    updated_silver_data['customer_segment'] = ['Premium', 'Standard', 'Premium', 'Standard', 'Premium']  # New column
    updated_silver_data['lifetime_value'] = [1500.00, 250.00, 1500.00, 320.00, 890.00]  # New calculated field
    
    updated_business_rules = business_rules.copy()
    updated_business_rules['customer_segment'] = {'allowed_values': ['Premium', 'Standard', 'Gold'], 'required': True}
    updated_business_rules['lifetime_value'] = {'min_value': 0.0, 'required': False}
    
    result = handler.evolve_silver_schema(
        table_name='sales_transactions_silver',
        new_data=updated_silver_data,
        business_rules=updated_business_rules,
        validate_quality=True
    )
    
    print(f"[OK] Silver schema evolved: {result['schema_evolved']}")
    print(f"     Changes: {len(result['changes'])}")
    for change in result['changes']:
        print(f"     - {change['type']}: {change['column']}")


def demo_gold_schema_evolution(handler):
    """Demonstrate gold layer schema evolution."""
    print("\n=== Gold Layer Schema Evolution Demo ===")
    
    # Create aggregated gold data
    print("\n5. Gold layer aggregation...")
    gold_data = pd.DataFrame({
        'date_key': ['2024-01-15', '2024-01-16', '2024-01-17'],
        'total_transactions': [2, 1, 2],
        'total_revenue': [67.48, 77.97, 82.97],
        'total_quantity': [3, 3, 3],
        'avg_order_value': [33.74, 77.97, 41.49],
        'unique_customers': [2, 1, 2],
        'total_discount': [0.00, 0.00, 5.00],
        'premium_customers': [1, 1, 1],
        'standard_customers': [1, 0, 1]
    })
    
    # First attempt - should require approval
    result = handler.evolve_gold_schema(
        table_name='daily_sales_summary_gold',
        new_data=gold_data,
        approval_required=True,
        downstream_systems=['business_intelligence', 'reporting_api', 'executive_dashboard']
    )
    
    print(f"[BLOCKED] Gold schema registration blocked: approval required")
    print(f"          Approval required: {result['approval_required']}")
    print(f"          Success: {result['success']}")
    
    # Simulate approval and register
    print("\n6. Gold schema with approval...")
    result = handler.evolve_gold_schema(
        table_name='daily_sales_summary_gold',
        new_data=gold_data,
        approval_required=False,  # Simulate approval
        downstream_systems=['business_intelligence', 'reporting_api', 'executive_dashboard']
    )
    
    print(f"[OK] Gold schema registered: {result['schema_registered']}")
    
    # Attempt breaking change - should be blocked
    print("\n7. Gold schema breaking change attempt...")
    breaking_gold_data = gold_data.copy()
    breaking_gold_data = breaking_gold_data.drop(['total_discount'], axis=1)  # Remove column
    breaking_gold_data['revenue_category'] = ['Medium', 'High', 'Medium']  # Add new column
    
    result = handler.evolve_gold_schema(
        table_name='daily_sales_summary_gold',
        new_data=breaking_gold_data,
        approval_required=True,
        downstream_systems=['business_intelligence', 'reporting_api', 'executive_dashboard']
    )
    
    print(f"[BLOCKED] Gold breaking change blocked: {not result['success']}")
    print(f"          Warnings: {len(result['warnings'])}")
    for warning in result['warnings']:
        print(f"          - {warning}")
    
    if result['downstream_impact']:
        print(f"          Downstream systems impacted: {result['downstream_impact']}")


def demo_cross_layer_compatibility():
    """Demonstrate cross-layer schema compatibility checking."""
    print("\n=== Cross-Layer Compatibility Demo ===")
    
    handler = create_medallion_handler()
    
    # Register schemas for all layers
    print("\n8. Cross-layer compatibility checking...")
    
    # Bronze schema
    bronze_schema = {
        "columns": [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
            {"name": "value", "type": "float", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": False}
        ]
    }
    
    # Silver schema - should be compatible with bronze
    silver_schema = {
        "columns": [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "name", "type": "string", "nullable": False},  # Made required
            {"name": "value", "type": "float", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": False},
            {"name": "processed_at", "type": "timestamp", "nullable": False}  # Added
        ]
    }
    
    # Gold schema - aggregated, different structure
    gold_schema = {
        "columns": [
            {"name": "date_key", "type": "string", "nullable": False},
            {"name": "total_records", "type": "integer", "nullable": False},
            {"name": "avg_value", "type": "float", "nullable": True},
            {"name": "min_value", "type": "float", "nullable": True},
            {"name": "max_value", "type": "float", "nullable": True}
        ]
    }
    
    # Register schemas
    handler.schema_manager.register_schema(
        'test_table_bronze', bronze_schema, 'v1.0.0', MedallionLayer.BRONZE
    )
    handler.schema_manager.register_schema(
        'test_table_silver', silver_schema, 'v1.0.0', MedallionLayer.SILVER
    )
    handler.schema_manager.register_schema(
        'test_table_gold', gold_schema, 'v1.0.0', MedallionLayer.GOLD
    )
    
    print("[OK] All layer schemas registered")
    
    # Check compatibility
    compatibility = handler._check_layer_compatibility(
        bronze_schema, silver_schema,
        MedallionLayer.BRONZE, MedallionLayer.SILVER
    )
    
    print(f"[OK] Bronze -> Silver compatibility: {compatibility['compatible']}")
    if compatibility['warnings']:
        for warning in compatibility['warnings']:
            print(f"     Warning: {warning}")
    
    return handler


def demo_schema_migration():
    """Demonstrate schema migration between versions."""
    print("\n=== Schema Migration Demo ===")
    
    handler = create_medallion_handler()
    
    print("\n9. Schema version migration...")
    
    # Create test data with old schema
    old_data = pd.DataFrame({
        'id': ['1', '2', '3'],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    # Register initial schema
    initial_schema = {
        "columns": [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "name", "type": "string", "nullable": False},
            {"name": "age", "type": "integer", "nullable": True}
        ]
    }
    
    handler.schema_manager.register_schema(
        'users_test', initial_schema, 'v1.0.0', MedallionLayer.SILVER
    )
    
    # Create new data with evolved schema
    new_data = pd.DataFrame({
        'id': ['4', '5'],
        'name': ['David', 'Eve'],
        'age': [28, 32],
        'email': ['david@example.com', 'eve@example.com'],  # New column
        'department': ['Engineering', 'Marketing']  # New column
    })
    
    # Register new schema version
    evolved_schema = {
        "columns": [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "name", "type": "string", "nullable": False},
            {"name": "age", "type": "integer", "nullable": True},
            {"name": "email", "type": "string", "nullable": True},
            {"name": "department", "type": "string", "nullable": True}
        ]
    }
    
    handler.schema_manager.register_schema(
        'users_test', evolved_schema, 'v1.1.0', MedallionLayer.SILVER
    )
    
    print("[OK] Schema versions registered: v1.0.0 -> v1.1.0")
    
    # Migrate old data to new schema
    try:
        migrated_data = handler.schema_manager.migrate_dataframe(
            df=old_data,
            table_name='users_test',
            source_version='v1.0.0',
            target_version='v1.1.0',
            layer=MedallionLayer.SILVER
        )
        
        print(f"[OK] Data migrated successfully")
        print(f"     Original columns: {list(old_data.columns)}")
        print(f"     Migrated columns: {list(migrated_data.columns)}")
        print(f"     New columns filled with: {migrated_data[['email', 'department']].iloc[0].tolist()}")
        
    except Exception as e:
        print(f"[ERROR] Migration failed: {e}")


def demo_schema_health_monitoring():
    """Demonstrate schema health monitoring."""
    print("\n=== Schema Health Monitoring Demo ===")
    
    handler = create_medallion_handler()
    
    print("\n10. Schema health monitoring...")
    
    # Get health metrics for each layer
    for layer in [MedallionLayer.BRONZE, MedallionLayer.SILVER, MedallionLayer.GOLD]:
        health = handler.get_layer_schema_health(layer)
        print(f"\n{layer.value.upper()} Layer Health:")
        print(f"  Total tables: {health['total_tables']}")
        print(f"  Avg schema versions: {health['avg_schema_versions']:.1f}")
        print(f"  Recent changes: {health['recent_changes']}")
        print(f"  Compatibility issues: {health['compatibility_issues']}")
    
    # Get schema lineage
    lineage = handler.schema_manager.get_schema_lineage('sales_transactions_bronze')
    if lineage:
        print(f"\nSchema Lineage for 'sales_transactions_bronze':")
        for version_info in lineage[-2:]:  # Show last 2 versions
            print(f"  Version {version_info['version']}:")
            print(f"    Timestamp: {version_info['timestamp']}")
            print(f"    Changes: {len(version_info['changes'])}")
            print(f"    Compatibility: {version_info['compatibility_level']}")


def demo_schema_export():
    """Demonstrate schema registry export."""
    print("\n=== Schema Registry Export Demo ===")
    
    handler = create_medallion_handler()
    
    print("\n11. Exporting schema registry...")
    
    # Export schema registry
    export_path = Path(__file__).parent.parent / "exports" / "schema_evolution"
    success = handler.schema_manager.export_schema_registry(export_path)
    
    if success:
        print(f"[OK] Schema registry exported to: {export_path}")
        
        # List exported files
        if export_path.exists():
            files = list(export_path.glob("*.json"))
            print(f"     Exported files:")
            for file in files:
                size = file.stat().st_size
                print(f"     - {file.name} ({size:,} bytes)")
    else:
        print("[ERROR] Failed to export schema registry")


def main():
    """Run all schema evolution demos."""
    print("Schema Evolution Demonstration")
    print("=" * 50)
    
    try:
        # Run demos in sequence
        handler = demo_bronze_schema_evolution()
        demo_silver_schema_evolution(handler)
        demo_gold_schema_evolution(handler)
        demo_cross_layer_compatibility()
        demo_schema_migration()
        demo_schema_health_monitoring()
        demo_schema_export()
        
        print("\n" + "=" * 50)
        print("[OK] All schema evolution demos completed successfully!")
        print("=" * 50)
        print("\nKey Features Demonstrated:")
        print("[OK] Bronze layer permissive schema evolution")
        print("[OK] Silver layer business rule validation")
        print("[OK] Gold layer strict compatibility checking")
        print("[OK] Cross-layer compatibility validation")
        print("[OK] Automated schema migration")
        print("[OK] Schema version management")
        print("[OK] Health monitoring and reporting")
        print("[OK] Schema registry export/backup")
        print("\nSchema Evolution Benefits:")
        print("[-] Backward and forward compatibility")
        print("[-] Automated migration between versions")
        print("[-] Layer-specific evolution policies")
        print("[-] Business rule enforcement")
        print("[-] Breaking change detection")
        print("[-] Medallion architecture support")
        print("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"Schema evolution demo failed: {e}")
        print(f"\n[ERROR] Demo failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())